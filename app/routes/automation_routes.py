"""
Automation Routes
Start Ansible playbook runs. Only playbooks on the allowlist can be started,
and their variables are validated before anything is queued.
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from typing import List
import logging

from app.controller.automation_controller import PLAYBOOKS
from app.controller.job_controller import JobStore
from app.dependencies import get_jobs
from app.models.automation_model import PlaybookInfo, PlaybookRunRequest
from app.models.job_model import ACTION_RUN_PLAYBOOK, Command, JobAccepted, LANE_AUTOMATION
from app.routes.job_routes import accepted

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/automation", tags=["Automation (Ansible)"])


@router.get("/playbooks", response_model=List[PlaybookInfo])
async def list_playbooks() -> List[PlaybookInfo]:
    """Playbooks that can be run, with the variables each one accepts"""
    return [spec.info() for spec in PLAYBOOKS.values()]


@router.post("/playbooks/{name}/runs", response_model=JobAccepted, status_code=202)
async def run_playbook(name: str, request: PlaybookRunRequest, http_request: Request,
                       jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """
    Queue a playbook run and answer 202 straight away.

    Follow `events_url` for each task as it runs. A failed playbook is not
    retried automatically.
    """
    spec = PLAYBOOKS.get(name)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"Playbook '{name}' is not on the allowlist")

    try:
        variables = spec.vars_model.model_validate(request.variables)
    except ValidationError as e:
        # Same 422 shape FastAPI uses for the request body itself
        raise RequestValidationError([{**error, "loc": ("body", "variables", *error["loc"])}
                                      for error in e.errors(include_url=False)]) from None

    command = Command(
        action=ACTION_RUN_PLAYBOOK,
        lane=LANE_AUTOMATION,
        key=name,
        kind="playbook",
        summary=f"Run playbook {name}",
        payload={"playbook": name, "variables": variables.model_dump(), "limit": request.limit})
    logger.info(f"Queueing {command.summary} as job {command.id}")
    return accepted(await jobs.submit(command), http_request)
