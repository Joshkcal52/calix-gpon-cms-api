"""
Simulated signups at scale, through the API, against the mocks only.

Signs up one customer per discovered ONT on one PON (SUB-0001, ...), follows
every job to the end, reads every ONT back, sends the whole set again to check
nothing new is sent, then runs a lifecycle round (port disable/enable, a
bandwidth profile change, four deletions). On the E7 path it also counts the
CLI commands the mock E7 received and can inject faults.

    python tools/sim_signups.py --backend cms --customers 32
    python tools/sim_signups.py --backend e7-cli --customers 64 --faults
    python tools/sim_signups.py --url http://localhost:8000 --backend e7-cli --customers 32

Without --url it starts the API in this process exactly as
`MOCK_PON_SIZE=<n> python main.py --mock [--backend e7-cli]` does (the
embedded worker, the in-memory queue, a mock E7 on 127.0.0.1), so it can see
the mock E7's command log and inject faults. With --url it drives an API
already running in mock mode; command counts and faults then need the
in-process mode.

MOCK ONLY. It refuses to start the API if CMS_HOST or E7_NODES is set, and
with --url it refuses any API that does not report mock mode.
"""

import argparse
import json
import logging
import os
import socket
import statistics
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

NETWORKS = {"cms": "NTWK-MOCK-E7", "e7-cli": "E7-MOCK"}
SIM_PONS = {"cms": "1/1/3", "e7-cli": "1/1/1"}  # the PON each mock fills (app/nbi/mock_pon.py)
PROFILE_OF_MODEL = {"844G": "844G-1", "812G": "812G-1", "716GE-I": "716GE-I"}
TAG_ACTION = "ADD-OUTER"  # a tag action with an outer VLAN on both mocks
WRITE_VERBS = ("create", "add", "remove", "delete")


# --- Running the API ------------------------------------------------------------------

class InProcessApi:
    """The API in a thread of this process, as python main.py --mock [--backend e7-cli] runs it"""

    def __init__(self, backend: str, pon_size: int):
        for name in ("CMS_HOST", "E7_NODES", "KAFKA_BOOTSTRAP_SERVERS"):
            if os.environ.get(name):
                raise SystemExit(f"{name} is set. This tool runs against the mocks only; unset it.")
        import uvicorn
        from config import settings
        from main import app

        for name, value in (("mock_mode", True), ("nbi_backend", "e7-cli" if backend == "e7-cli" else "cms"),
                            ("mock_pon_size", pon_size), ("e7_nodes", None), ("cms_host", None),
                            ("kafka_bootstrap_servers", None), ("api_keys", None),
                            ("nbi_timeout_seconds", 30.0)):
            setattr(settings, name, value)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.app = app
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 20
        while not self.server.started:
            if time.monotonic() > deadline or not self.thread.is_alive():
                raise SystemExit("The API did not start")
            time.sleep(0.05)
        self.url = f"http://127.0.0.1:{self.port}"

    @property
    def mock_e7(self):
        return getattr(self.app.state, "mock_e7", None)

    @property
    def backend(self):
        return self.app.state.gateway.backend

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


class SessionTimer:
    """Wraps the E7 session's run() to measure how long the one SSH session is busy"""

    def __init__(self, session):
        self.busy = 0.0
        self.calls = 0
        original = session.run

        async def timed(command: str) -> str:
            start = time.perf_counter()
            try:
                return await original(command)
            finally:
                self.busy += time.perf_counter() - start
                self.calls += 1

        session.run = timed


# --- The driver -----------------------------------------------------------------------

class Sim:
    def __init__(self, url: str, backend: str, api: Optional[InProcessApi] = None):
        self.http = httpx.Client(base_url=url, timeout=60)
        self.backend = backend
        self.net = NETWORKS[backend]
        self.base = f"/api/v2/networks/{self.net}"
        self.api = api
        self.problems: List[str] = []

    # HTTP helpers

    def get(self, path: str) -> httpx.Response:
        return self.http.get(path if path.startswith("/api") else self.base + path)

    def submit(self, method: str, path: str, body: Any = None) -> str:
        r = self.http.request(method, self.base + path, json=body)
        if r.status_code != 202:
            raise RuntimeError(f"{method} {path} answered {r.status_code}: {r.text}")
        return r.json()["job"]["id"]

    def wait(self, job_ids: List[str], timeout: float = 900) -> Dict[str, Dict[str, Any]]:
        """Poll every job until it has finished. Returns job_id -> job."""
        done: Dict[str, Dict[str, Any]] = {}
        deadline = time.monotonic() + timeout
        while len(done) < len(job_ids):
            for job_id in job_ids:
                if job_id in done:
                    continue
                job = self.get(f"/api/v2/jobs/{job_id}").json()
                if job["status"] in ("succeeded", "failed"):
                    done[job_id] = job
            if time.monotonic() > deadline:
                raise RuntimeError(f"{len(job_ids) - len(done)} job(s) did not finish within {timeout:g}s")
            if len(done) < len(job_ids):
                time.sleep(0.1)
        return done

    def events(self, job_id: str) -> List[Dict[str, Any]]:
        """Every event of a finished job (the SSE stream replays them and closes)"""
        found, kind = [], None
        with self.http.stream("GET", f"/api/v2/jobs/{job_id}/events") as r:
            for line in r.iter_lines():
                if line.startswith("event: "):
                    kind = line[7:]
                elif line.startswith("data: ") and kind != "end":
                    found.append(json.loads(line[6:]))
        return found

    def check(self, ok: bool, what: str) -> bool:
        if not ok:
            self.problems.append(what)
        return ok

    # The customers

    def plan(self, count: int, pon: str) -> List[Dict[str, Any]]:
        discovered = [d for d in self.get("/onts/discovered").json() if d["pon_port"] == pon]
        discovered.sort(key=lambda d: d["serial_number"])
        if len(discovered) < count:
            raise SystemExit(f"Only {len(discovered)} discovered ONTs on PON {pon}; start the API with "
                             f"MOCK_PON_SIZE={count} (or python main.py --mock --pon-size {count})")
        customers = []
        for n, ont in enumerate(discovered[:count], start=1):
            profile = PROFILE_OF_MODEL.get(ont["model"], ont["model"])
            side = "router" if profile == "844G-1" else "ethernet"  # routed on the 844G's G1, bridged on g1
            services = [dict(vlan=100, bandwidth_profile="RES-1G", tag_action=TAG_ACTION, attach_to=side,
                             description="HSI")]
            if n % 4 == 0:
                services.append(dict(vlan=300, bandwidth_profile="RES-1G", tag_action=TAG_ACTION, attach_to=side,
                                     description="IPTV"))
            if n % 10 == 3:  # a second data service, on its own port and VLAN
                services.append(dict(vlan=400, bandwidth_profile="RES-500M", tag_action=TAG_ACTION,
                                     attach_to="ethernet", ont_port=2, description="DATA2"))
            sub = f"SUB-{n:04d}"
            customers.append({"subscriber_id": sub, "serial_number": ont["serial_number"], "profile": profile,
                              "description": f"{sub} {n} Sim Lane",
                              "ports": [{"port": 1, "subscriber_id": sub}], "services": services})
        return customers

    def sign_up(self, customers: List[Dict[str, Any]], timeout: float = 900) -> Tuple[float, List[Dict[str, Any]]]:
        """POST every signup, then follow every job. Returns (wall seconds, one record per customer)."""
        start = time.perf_counter()
        ids = [self.submit("POST", "/signups", c) for c in customers]
        jobs = self.wait(ids, timeout)
        wall = time.perf_counter() - start
        records = []
        for customer, job_id in zip(customers, ids):
            job = jobs[job_id]
            records.append({"subscriber_id": customer["subscriber_id"], "job_id": job_id, "status": job["status"],
                            "run_seconds": _seconds(job.get("started_at"), job.get("finished_at")),
                            "total_seconds": _seconds(job.get("created_at"), job.get("finished_at")),
                            "result": job.get("result"), "error": job.get("error")})
        return wall, records

    def read_back(self, customers: List[Dict[str, Any]], records: List[Dict[str, Any]]) -> int:
        """Read every ONT and its ports back and compare them with what was asked. Returns how many match."""
        matched = 0
        for customer, record in zip(customers, records):
            if record["status"] != "succeeded":
                continue
            ont_id = record["result"]["ont_id"]
            ont = self.get(f"/onts/{ont_id}").json()
            sub = customer["subscriber_id"]
            want = sorted((s["attach_to"], s.get("ont_port", 1), s["vlan"], s["bandwidth_profile"])
                          for s in customer["services"])
            have = sorted((s["attachment"], s["ont_port"], s["vlan"], s["bandwidth_profile"])
                          for s in ont.get("services", []))
            ports = self.get(f"/onts/{ont_id}/ports").json()
            eth1 = next((p for p in ports["ethernet"] if p["number"] == 1), {})
            ok = all([
                self.check(have == want, f"{sub} ONT {ont_id}: services {have}, asked for {want}"),
                self.check(ont.get("serial_number") == customer["serial_number"],
                           f"{sub} ONT {ont_id}: serial {ont.get('serial_number')}"),
                self.check(ont.get("subscriber_id") == sub, f"{sub} ONT {ont_id}: subscriber {ont.get('subscriber_id')}"),
                self.check(ont.get("profile") == customer["profile"], f"{sub} ONT {ont_id}: profile {ont.get('profile')}"),
                self.check(eth1.get("subscriber_id") == sub,
                           f"{sub} ONT {ont_id}: eth1 subscriber {eth1.get('subscriber_id')}"),
            ])
            matched += ok
        return matched

    # E7 command log

    def commands(self) -> List[str]:
        return list(self.api.mock_e7.commands) if self.api and self.api.mock_e7 else []


def _seconds(start: Optional[str], end: Optional[str]) -> Optional[float]:
    from datetime import datetime
    if not start or not end:
        return None
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()


def writes(commands: List[str]) -> List[str]:
    return [c for c in commands if c.split()[:1] and c.split()[0] in WRITE_VERBS
            or c.startswith("set ont")]


def per_customer_commands(commands: List[str], customers: List[Dict[str, Any]]) -> Dict[str, Counter]:
    """
    Split the E7 command log of a signup round by customer. The worker runs one job at a time, so a job's
    commands are contiguous: each starts at its `show ont serial-number <sn>` (or the `show ont-profile`
    just before it, sent first for router-side services).
    """
    starts = []
    for c in customers:
        serial = c["serial_number"][4:]
        at = next((i for i, cmd in enumerate(commands) if cmd == f"show ont serial-number {serial}"), None)
        if at is None:
            continue
        if at > 0 and commands[at - 1] == "show ont-profile":
            at -= 1
        starts.append((at, c["subscriber_id"]))
    starts.sort()
    counts = {}
    for k, (at, sub) in enumerate(starts):
        end = starts[k + 1][0] if k + 1 < len(starts) else len(commands)
        counts[sub] = Counter(" ".join(cmd.split()[:2]) for cmd in commands[at:end])
    return counts


# --- Rounds ---------------------------------------------------------------------------

def signup_round(sim: Sim, customers: List[Dict[str, Any]], report: Dict[str, Any]) -> List[Dict[str, Any]]:
    timer = SessionTimer(sim.api.backend.sessions[sim.net]) if sim.api and sim.backend == "e7-cli" else None
    before = len(sim.commands())
    wall, records = sim.sign_up(customers)
    log = sim.commands()[before:]
    ok = [r for r in records if r["status"] == "succeeded"]
    runs = [r["run_seconds"] for r in ok if r["run_seconds"] is not None]
    slowest = sorted(records, key=lambda r: -(r["run_seconds"] or 0))[:5]
    report["signups"] = {
        "customers": len(customers), "succeeded": len(ok), "failed": len(records) - len(ok),
        "wall_seconds": round(wall, 2), "jobs_per_minute": round(len(records) / wall * 60, 1),
        "job_run_seconds": {"median": round(statistics.median(runs), 3) if runs else None,
                            "max": round(max(runs), 3) if runs else None},
        "slowest": [{"subscriber_id": r["subscriber_id"], "run_seconds": r["run_seconds"],
                     "total_seconds": r["total_seconds"]} for r in slowest],
        "failures": [{"subscriber_id": r["subscriber_id"], "error": r["error"]} for r in records
                     if r["status"] != "succeeded"],
        "services_asked": sum(len(c["services"]) for c in customers),
    }
    if log:
        split = per_customer_commands(log, customers)
        totals = [sum(c.values()) for c in split.values()]
        by_profile = {}
        for c in customers:
            if c["subscriber_id"] in split:
                by_profile.setdefault((c["profile"], len(c["services"])), []).append(sum(split[c["subscriber_id"]].values()))
        report["e7_commands"] = {
            "total": len(log), "writes": len(writes(log)),
            "per_customer": {"min": min(totals), "median": statistics.median(totals), "max": max(totals)},
            "by_profile_and_services": {f"{p} x{n}": sorted(set(v)) for (p, n), v in sorted(by_profile.items())},
            "kinds": dict(Counter(" ".join(c.split()[:2]) for c in log).most_common()),
            "example": next((dict(v) for s, v in split.items()
                             if next(c for c in customers if c["subscriber_id"] == s)["profile"] == "844G-1"), None),
        }
    if timer:
        report["e7_session"] = {"busy_seconds": round(timer.busy, 2), "commands": timer.calls,
                                "busy_share_of_wall": round(timer.busy / wall, 3),
                                "ms_per_command": round(timer.busy / max(1, timer.calls) * 1000, 2)}
    matched = sim.read_back(customers, records)
    report["read_back"] = {"checked": len(ok), "matched": matched}
    return records


def repeat_round(sim: Sim, customers: List[Dict[str, Any]], first: List[Dict[str, Any]], report: Dict[str, Any]) -> None:
    before = len(sim.commands())
    wall, records = sim.sign_up(customers)
    log = sim.commands()[before:]
    changed = []
    for customer, record, earlier in zip(customers, records, first):
        if record["status"] != "succeeded":
            changed.append(f"{customer['subscriber_id']}: {record['status']} {record['error']}")
            continue
        if earlier["status"] == "succeeded" and record["result"]["ont_id"] != earlier["result"]["ont_id"]:
            changed.append(f"{customer['subscriber_id']}: ONT {record['result']['ont_id']}, was {earlier['result']['ont_id']}")
        sent = [e["message"] for e in sim.events(record["job_id"])
                if e["type"] == "log" and e["message"].startswith(("Created ONT", "Bound ", "Set port"))]
        if sent:
            changed.append(f"{customer['subscriber_id']}: {sent}")
    report["repeat"] = {"wall_seconds": round(wall, 2), "succeeded": sum(r["status"] == "succeeded" for r in records),
                        "jobs_that_sent_something": changed,
                        "e7_writes": writes(log) if log or sim.api else None,
                        "e7_commands": len(log) if sim.api and sim.backend == "e7-cli" else None}
    sim.check(not changed, f"repeat round sent changes: {changed[:5]}")
    sim.check(not writes(log), f"repeat round sent E7 writes: {writes(log)[:5]}")


def lifecycle_round(sim: Sim, customers: List[Dict[str, Any]], records: List[Dict[str, Any]],
                    report: Dict[str, Any]) -> None:
    ok = [(c, r["result"]["ont_id"]) for c, r in zip(customers, records) if r["status"] == "succeeded"]
    steps = []

    def run(what: str, method: str, path: str, body: Any = None) -> Dict[str, Any]:
        start = time.perf_counter()
        job = sim.wait([sim.submit(method, path, body)])
        job = next(iter(job.values()))
        steps.append({"step": what, "status": job["status"], "seconds": round(time.perf_counter() - start, 3),
                      "error": (job.get("error") or {}).get("message")})
        sim.check(job["status"] == "succeeded", f"lifecycle {what}: {job.get('error')}")
        return job

    def eth1_state(ont_id: int) -> Optional[str]:
        ports = sim.get(f"/onts/{ont_id}/ports").json()
        return next((p["admin_state"] for p in ports["ethernet"] if p["number"] == 1), None)

    # Disable then enable a port
    customer, ont_id = ok[0]
    run(f"disable eth1 on ONT {ont_id}", "POST", f"/onts/{ont_id}/ports/eth1/disable")
    sim.check(eth1_state(ont_id) == "disabled", f"ONT {ont_id} eth1 reads {eth1_state(ont_id)} after disable")
    steps[-1]["verified"] = eth1_state(ont_id) == "disabled"
    run(f"enable eth1 on ONT {ont_id}", "POST", f"/onts/{ont_id}/ports/eth1/enable")
    steps[-1]["verified"] = eth1_state(ont_id) == "enabled"
    sim.check(steps[-1]["verified"], f"ONT {ont_id} eth1 does not read enabled after enable")

    # Change a bandwidth profile: the HSI of the first 844G (on the router side) and of the first other ONT
    for customer, ont_id in (next(x for x in ok if x[0]["profile"] == "844G-1"),
                             next(x for x in ok if x[0]["profile"] != "844G-1")):
        hsi = next(s for s in sim.get(f"/onts/{ont_id}").json()["services"] if s["vlan"] == 100)
        port = f"{'rg' if hsi['attachment'] == 'router' else 'eth'}{hsi['ont_port']}"
        run(f"HSI on ONT {ont_id} {port} RES-1G -> RES-500M", "PATCH",
            f"/onts/{ont_id}/ports/{port}/services/{hsi['service_index']}", {"bandwidth_profile": "RES-500M"})
        after = sim.get(f"/onts/{ont_id}").json()["services"]
        now = [s for s in after if s["vlan"] == 100]
        steps[-1]["verified"] = len(now) == 1 and now[0]["bandwidth_profile"] == "RES-500M" and len(after) == len(
            customer["services"])
        sim.check(steps[-1]["verified"], f"ONT {ont_id} services after the profile change: {after}")

    # Delete the last four customers
    for customer, ont_id in ok[-4:]:
        run(f"delete ONT {ont_id} ({customer['subscriber_id']})", "DELETE",
            f"/onts/{ont_id}?serial_number={customer['serial_number']}&force=true")
        gone = sim.get(f"/onts/{ont_id}").status_code == 404
        rediscovered = any(d["serial_number"] == customer["serial_number"] for d in sim.get("/onts/discovered").json())
        steps[-1]["verified"] = gone and rediscovered
        sim.check(gone, f"ONT {ont_id} still reads after delete")
        sim.check(rediscovered, f"{customer['serial_number']} not discovered again after delete")
    # A delete repeated is safe
    customer, ont_id = ok[-1]
    job = run(f"delete ONT {ont_id} again", "DELETE", f"/onts/{ont_id}?serial_number={customer['serial_number']}&force=true")
    steps[-1]["verified"] = (job.get("result") or {}).get("already_absent") is True
    report["lifecycle"] = steps


def fault_round(sim: Sim, customers: List[Dict[str, Any]], report: Dict[str, Any]) -> None:
    """E7 path only, in-process: idle close mid-run, a hang-up on add eth-svc, a fifth service on G1"""
    mock = sim.api.mock_e7
    faults = {}
    batches = [customers[i:i + 6] for i in range(0, 24, 6)]

    # 1. The E7 closes the session while it is idle between jobs, mid-run
    _, first = sim.sign_up(batches[0])
    logins = mock.logins
    mock.close_idle_sessions()  # what the E7's inactivity timeout does ("Session timed out.")
    time.sleep(0.5)
    before = len(mock.commands)
    _, second = sim.sign_up(batches[1])
    log = mock.commands[before:]
    dupes = [c for c, n in Counter(writes(log)).items() if n > 1]
    faults["idle_close_between_jobs"] = {
        "succeeded": sum(r["status"] == "succeeded" for r in first + second), "of": len(first + second),
        "new_logins": mock.logins - logins, "duplicated_writes": dupes,
        "failures": [r["error"] for r in second if r["status"] != "succeeded"]}
    sim.check(all(r["status"] == "succeeded" for r in second) and not dupes, "idle close: jobs failed or writes doubled")

    # 1b. While jobs are running, close the session whenever it is waiting for a command (every 50 ms):
    # between jobs, and between the commands of one job
    logins, closes, latency = mock.logins, mock.idle_closes, mock.latency
    mock.latency = max(latency, 0.01)
    before = len(mock.commands)
    ids = [sim.submit("POST", "/signups", c) for c in batches[2]]
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        mock.close_idle_sessions()
        time.sleep(0.05)
        if all(sim.get(f"/api/v2/jobs/{i}").json()["status"] in ("succeeded", "failed") for i in ids):
            break
    mock.latency = latency
    third = list(sim.wait(ids).values())
    log = mock.commands[before:]
    retried = {i: [e["message"] for e in sim.events(i) if e["type"] == "warning"] for i in ids}
    faults["idle_close_while_running"] = {
        "succeeded": sum(j["status"] == "succeeded" for j in third), "of": len(third),
        "sessions_closed": mock.idle_closes - closes, "new_logins": mock.logins - logins,
        "jobs_retried": {i[:8]: w for i, w in retried.items() if w},
        "duplicated_writes": [c for c, n in Counter(writes(log)).items() if n > 1],
        "failures": [j["error"] for j in third if j["status"] != "succeeded"]}
    sim.check(all(j["status"] == "succeeded" for j in third), "idle close while running: a job failed")

    # 2. Hang up on one add eth-svc: before it runs, then (the harder case) after it has run
    for when in ("before", "after"):
        customer = batches[3][0 if when == "before" else 1]
        mock.hang_up_on, mock.hang_up_once, mock.hang_up_after_run = "add eth-svc", True, when == "after"
        before = len(mock.commands)
        _, (record,) = sim.sign_up([customer])
        mock.hang_up_on, mock.hang_up_after_run = None, False
        log = mock.commands[before:]
        ont = sim.get(f"/onts/{record['result']['ont_id']}").json() if record["status"] == "succeeded" else None
        identities = Counter((s["attachment"], s["ont_port"], s["vlan"]) for s in (ont or {}).get("services", []))
        warnings = [e["message"] for e in sim.events(record["job_id"]) if e["type"] in ("warning", "error")]
        faults[f"hang_up_{when}_add_eth_svc"] = {
            "status": record["status"], "error": record["error"], "warnings": warnings,
            "attempts_logged": sum(1 for e in sim.events(record["job_id"])
                                   if e["type"] == "status" and "attempt" in e["message"]) + 1,
            "add_eth_svc_sent": [c for c in log if c.startswith("add eth-svc")],
            "services_on_ont": len(ont["services"]) if ont else None, "asked": len(customer["services"]),
            "duplicated": [k for k, n in identities.items() if n > 1]}
        sim.check(not any(n > 1 for n in identities.values()), f"hang-up {when}: a service was duplicated")

    # 3. A fifth service onto an 844G's G1
    target = next(c for c in batches[3][2:] if c["profile"] == "844G-1")
    four = [dict(vlan=v, bandwidth_profile="RES-1G", tag_action=TAG_ACTION, attach_to="router") for v in (100, 200, 300)]
    four.append(dict(bandwidth_profile="RES-1G", tag_action="MOCK-NO-VLAN", attach_to="router"))
    fifth = dict(vlan=400, bandwidth_profile="RES-1G", tag_action=TAG_ACTION, attach_to="router")
    _, (record,) = sim.sign_up([{**target, "services": four}])
    before = len(mock.commands)
    _, (refused,) = sim.sign_up([{**target, "services": four + [fifth]}])
    sent = writes(mock.commands[before:])
    faults["fifth_service_on_g1_existing_ont"] = {
        "four_first": record["status"], "status": refused["status"],
        "error": (refused["error"] or {}).get("message"), "writes_sent": sent}
    sim.check(refused["status"] == "failed" and not sent, "fifth service on G1 was not refused before sending")

    # 3b. The same five in one signup for a new ONT
    fresh = next(c for c in customers[24:] if c["profile"] == "844G-1")
    before = len(mock.commands)
    _, (new,) = sim.sign_up([{**fresh, "services": four + [fifth]}])
    sent = writes(mock.commands[before:])
    faults["fifth_service_on_g1_new_signup"] = {
        "status": new["status"], "error": (new["error"] or {}).get("message"), "writes_sent": sent,
        "ont_left_behind": any(o for o in mock.onts.values() if o["serial"] == fresh["serial_number"][4:])}
    report["faults"] = faults


# --- Main -----------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backend", choices=["cms", "e7-cli"], default="cms")
    parser.add_argument("--customers", type=int, default=32)
    parser.add_argument("--url", help="an API already running in mock mode (default: start one in this process)")
    parser.add_argument("--faults", action="store_true",
                        help="E7 path, in-process only: run the fault round (instead of the other rounds)")
    parser.add_argument("--e7-latency", type=float, default=0.0,
                        help="in-process E7 path: seconds the mock E7 waits before each answer (default 0)")
    parser.add_argument("--json", help="write the report here as JSON")
    args = parser.parse_args()
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per poll otherwise

    api = None if args.url else InProcessApi(args.backend, args.customers)
    sim = Sim(args.url or api.url, args.backend, api)
    report: Dict[str, Any] = {"backend": args.backend, "mode": "url" if args.url else "in-process"}
    if api and api.mock_e7:
        api.mock_e7.latency = report["e7_latency"] = args.e7_latency
    try:
        status = sim.get("/api/v2/status").json()
        if not status.get("mock_mode"):
            raise SystemExit("The API is not in mock mode. This tool runs against the mocks only.")
        if status["cms_backend"] != {"cms": "mock-cms", "e7-cli": "e7-cli"}[args.backend]:
            raise SystemExit(f"The API's backend is {status['cms_backend']}, not {args.backend}")
        customers = sim.plan(args.customers, SIM_PONS[args.backend])
        report["mix"] = dict(Counter(c["profile"] for c in customers))
        if args.faults:
            if not api or args.backend != "e7-cli" or args.customers < 30:
                raise SystemExit("--faults needs --backend e7-cli, no --url and --customers 30 or more")
            fault_round(sim, customers, report)
            return
        records = signup_round(sim, customers, report)
        repeat_round(sim, customers, records, report)
        lifecycle_round(sim, customers, records, report)
    finally:
        report["problems"] = sim.problems
        print(json.dumps(report, indent=2, default=str))
        if args.json:
            Path(args.json).write_text(json.dumps(report, indent=2, default=str))
        if api:
            api.stop()


if __name__ == "__main__":
    main()
