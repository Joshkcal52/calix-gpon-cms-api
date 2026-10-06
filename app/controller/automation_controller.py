"""
Automation Service
Runs Ansible playbooks for the heavy, multi-device work: baseline audits,
bulk syncs, changes that span other vendors' gear.

Rules:
  - Only playbooks on the allowlist below can run. A caller picks one by name
    and never supplies a path.
  - Each playbook has a Pydantic model for its variables. Unknown or invalid
    variables are rejected before anything is queued.
  - Playbooks reach Calix CMS only through this API (`api_url`), never
    directly, so CMS access keeps a single owner.

Two runners share one interface: AnsibleRunner uses the ansible-runner
package, SimulatedRunner plays back a scripted run for when Ansible is not
installed (the default in MOCK_MODE).
"""

import asyncio
import logging
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Type

from pydantic import BaseModel

from app.models.automation_model import BaselineAuditVars, PlaybookInfo
from app.worker.runner import JobContext, JobError

logger = logging.getLogger(__name__)

ANSIBLE_DIR = Path(__file__).resolve().parents[2] / "ansible"


class PlaybookSpec:
    def __init__(self, name: str, file: str, description: str, vars_model: Type[BaseModel]):
        self.name = name
        self.file = file
        self.description = description
        self.vars_model = vars_model

    def info(self) -> PlaybookInfo:
        return PlaybookInfo(name=self.name, description=self.description,
                            variables_schema=self.vars_model.model_json_schema())


# The allowlist. To add a playbook: put the file in ansible/project/, write a
# variables model in app/models/automation_model.py, and register it here.
PLAYBOOKS: Dict[str, PlaybookSpec] = {
    spec.name: spec for spec in (
        PlaybookSpec(
            name="baseline_audit",
            file="baseline_audit.yml",
            description="Check that the ONTs expected to be in service on a network are online.",
            vars_model=BaselineAuditVars),
    )
}

# Emit callback handed to a runner: emit(type, message, **data). Thread-safe.
Emit = Callable[..., None]


class PlaybookRunner(Protocol):
    name: str

    async def run(self, spec: PlaybookSpec, variables: Dict[str, Any], limit: Optional[str],
                  job_id: str, emit: Emit) -> Dict[str, Any]:
        """Run to completion and return a summary. Raise JobError if the run fails."""


class AnsibleRunner:
    """Runs playbooks with ansible-runner, in a worker thread so the event loop stays free"""

    name = "ansible-runner"

    def __init__(self, timeout: float = 600.0, project_dir: Optional[Path] = None,
                 inventory: Optional[Path] = None):
        self.timeout = timeout
        self.project_dir = project_dir or ANSIBLE_DIR / "project"
        self.inventory = inventory or ANSIBLE_DIR / "inventory" / "hosts.yml"

    async def run(self, spec: PlaybookSpec, variables: Dict[str, Any], limit: Optional[str],
                  job_id: str, emit: Emit) -> Dict[str, Any]:
        try:
            import ansible_runner  # noqa: F401
        except ImportError:
            raise JobError(
                "ansible_not_installed",
                "ansible-runner is not installed on this worker. "
                "Install requirements-ansible.txt, or set ANSIBLE_MOCK=true to simulate runs.") from None

        cancel = threading.Event()
        try:
            return await asyncio.to_thread(self._run_blocking, spec, variables, limit, job_id, emit, cancel)
        except asyncio.CancelledError:
            cancel.set()  # tells ansible-runner to stop the playbook process
            raise

    def _run_blocking(self, spec: PlaybookSpec, variables: Dict[str, Any], limit: Optional[str],
                      job_id: str, emit: Emit, cancel: threading.Event) -> Dict[str, Any]:
        import ansible_runner

        failures: List[str] = []
        stats: Dict[str, Any] = {}

        def on_event(event: Dict[str, Any]) -> bool:
            kind = event.get("event", "")
            data = event.get("event_data") or {}
            host, task = data.get("host"), data.get("task")
            if kind == "playbook_on_play_start":
                emit("log", f"PLAY [{data.get('name') or data.get('play')}]")
            elif kind == "playbook_on_task_start":
                emit("step", f"TASK [{task}]")
            elif kind == "runner_on_ok":
                changed = bool((data.get("res") or {}).get("changed"))
                emit("log", f"{'changed' if changed else 'ok'}: {host}", host=host, task=task, changed=changed)
            elif kind in ("runner_on_failed", "runner_on_unreachable"):
                # Only the message is forwarded: full results can contain secrets
                reason = str((data.get("res") or {}).get("msg", "no details"))[:500]
                label = "unreachable" if kind == "runner_on_unreachable" else "failed"
                failures.append(f"{host}: {task}: {reason}")
                emit("warning", f"{label}: {host}: {reason}", host=host, task=task)
            elif kind == "playbook_on_stats":
                stats.update({name: data.get(name) or {}
                              for name in ("ok", "changed", "failures", "dark", "skipped")})
            return False  # nothing needs to be kept on disk

        # Each run gets its own scratch directory, so runs never share state
        private_dir = tempfile.mkdtemp(prefix=f"rogue-ansible-{job_id}-")
        try:
            run = ansible_runner.run(
                private_data_dir=private_dir,
                project_dir=str(self.project_dir),
                playbook=spec.file,
                inventory=str(self.inventory),
                extravars=variables,
                limit=limit,
                ident=job_id,
                quiet=True,
                timeout=int(self.timeout),
                event_handler=on_event,
                cancel_callback=cancel.is_set,
            )
            summary = {"playbook": spec.name, "status": run.status, "rc": run.rc, "stats": stats}
        finally:
            shutil.rmtree(private_dir, ignore_errors=True)

        if run.status != "successful":
            if run.status == "timeout":
                message = f"Playbook {spec.name} was stopped after the {self.timeout:g} second limit."
            elif failures:
                message = f"Playbook {spec.name} failed. {failures[-1]}"
            else:
                message = f"Playbook {spec.name} ended with status '{run.status}' (exit code {run.rc})."
            raise JobError("playbook_failed", message, data={"playbook": summary})
        return summary


class SimulatedRunner:
    """Plays back a plausible run without Ansible. Changes nothing anywhere."""

    name = "simulated"

    def __init__(self, task_seconds: float = 0.3):
        self.task_seconds = task_seconds

    async def run(self, spec: PlaybookSpec, variables: Dict[str, Any], limit: Optional[str],
                  job_id: str, emit: Emit) -> Dict[str, Any]:
        emit("warning", "Simulated run: Ansible was not started and nothing was changed")
        emit("log", f"PLAY [{spec.description}]")
        for task in ("Check that the API can reach CMS", "Read the status of the expected ONTs",
                     "Report the audit result"):
            emit("step", f"TASK [{task}]")
            await asyncio.sleep(self.task_seconds)
            emit("log", "ok: localhost", host="localhost", task=task, changed=False)
        return {"playbook": spec.name, "status": "successful", "rc": 0, "simulated": True,
                "stats": {"ok": {"localhost": 3}}}


def build_playbook_runner(settings: Any) -> PlaybookRunner:
    simulate = settings.mock_mode if settings.ansible_mock is None else settings.ansible_mock
    if simulate:
        return SimulatedRunner()
    return AnsibleRunner(timeout=settings.ansible_timeout_seconds)


async def run_playbook(context: JobContext, runner: PlaybookRunner, payload: Dict[str, Any],
                       api_url: str) -> Dict[str, Any]:
    """Worker-side handler body for RUN_PLAYBOOK"""
    spec = PLAYBOOKS.get(payload.get("playbook", ""))
    if spec is None:
        raise JobError("unknown_playbook", f"Playbook '{payload.get('playbook')}' is not on the allowlist.")

    # Validate again on the worker: the queue is a trust boundary too
    variables = spec.vars_model.model_validate(payload.get("variables") or {}).model_dump()
    variables["api_url"] = api_url  # set by the server, never by the caller

    await context.emit("log", f"Running playbook {spec.name} with {runner.name}", runner=runner.name)
    return await runner.run(spec, variables, payload.get("limit"), context.job_id, context.emit_threadsafe)
