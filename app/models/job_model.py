"""
Job Models
Pydantic models for queued work (ONT activations and changes, legacy events): the command
that travels through the queue, the progress updates workers publish, and the
job view the API serves.
"""

import uuid
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from enum import Enum

# Lanes: each has its own queue (Kafka topic) and its own workers
LANE_PROVISIONING = "provisioning"

ACTION_ACTIVATE_ONT = "ACTIVATE_ONT"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return uuid.uuid4().hex


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Command(BaseModel):
    """A unit of work written to the queue. Its id is also the job id."""
    id: str = Field(default_factory=new_id)
    action: str = Field(..., description="ACTIVATE_ONT, CHANGE_ONT, GPON_SIGNUP or XGS_CUTOVER")
    lane: str = Field(..., description="provisioning or legacy")
    key: str = Field(..., description="Ordering key: commands with the same key run one at a time, in order")
    kind: str = Field(..., description="Job kind shown to clients, e.g. activation, signup, port-change")
    summary: str
    payload: Dict[str, Any]
    created_at: datetime = Field(default_factory=_now)
    requested_by: Optional[str] = Field(None, description="Who asked for it: the API key's name (see app/audit.py)")


# An update recording what this job created in CMS (an ONT, a service), so a later attempt of the same
# job can tell its own work from someone else's. Published with a wait, like a status change.
OWNERSHIP = "ownership"


class JobUpdate(BaseModel):
    """A progress message published by the API (queued) or a worker (everything else)"""
    id: str = Field(default_factory=new_id)
    job_id: str
    kind: str
    summary: str
    type: str = Field(..., description="status, step, log, warning, error, result or ownership")
    message: str
    data: Optional[Dict[str, Any]] = None
    status: Optional[JobStatus] = Field(None, description="Set when this update changes the job status")
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=_now)
    requested_by: Optional[str] = None


class JobEvent(BaseModel):
    """One progress update as served to clients. `seq` increases by one per event within a job."""
    seq: int
    timestamp: datetime
    type: str = Field(..., description="status, step, log, warning, error or result")
    message: str
    data: Optional[Dict[str, Any]] = None


class Job(BaseModel):
    id: str
    kind: str = Field(..., description="activation, signup, cutover, port-change, ...")
    summary: str
    status: JobStatus = JobStatus.QUEUED
    requested_by: Optional[str] = Field(
        None, description="Who asked for it: the API key's name, or what a topic producer called itself")
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None


class JobAccepted(BaseModel):
    """Returned with HTTP 202 once the command is safely in the queue"""
    job: Job
    status_url: str
    events_url: str = Field(..., description="Server-Sent Events stream of progress")
