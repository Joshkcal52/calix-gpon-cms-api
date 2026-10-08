"""
Legacy GPON Routes
Subscriber-level operations for legacy GPON areas. This is the entry point a
billing system (Sonar or any other) calls.

Each request is validated, written to the legacy-gpon-events topic as an
event, and answered with 202 and a job to follow. Systems that already
publish to Kafka can write the same events to the topic directly and skip
these routes (see app/legacy/models.py for the event shapes).
"""

from fastapi import APIRouter, Depends, Path, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import List, Optional
import logging

from app import audit
from app.controller.job_controller import JobStore
from app.controller.provisioning_controller import check_bandwidth_profiles
from app.dependencies import get_jobs
from app.legacy.events import event_to_command
from app.legacy.models import GponSignupEvent, XgsCutoverEvent
from app.models.job_model import JobAccepted, new_id
from app.models.provisioning_model import (
    EthernetPortConfig, EthernetService, NETWORK_PATTERN, OntId, ProfileName, SerialNumber, SubscriberId,
    check_unique_ports, check_unique_services
)
from app.routes.job_routes import accepted
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/networks/{network}", tags=["Legacy GPON subscribers"])

NetworkPath = Path(..., pattern=NETWORK_PATTERN, description="CMS network name, e.g. NTWK-MOCK-E7")


class SignupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subscriber_id: SubscriberId
    serial_number: SerialNumber
    ont_id: Optional[OntId] = Field(None, description="Leave empty to take the next free ID")
    profile: ProfileName = Field(..., description="ONT profile")
    description: Optional[str] = Field(None, max_length=64)
    services: List[EthernetService] = Field(..., min_length=1, max_length=8)
    ports: List[EthernetPortConfig] = Field(
        default_factory=list, max_length=8,
        description="Ethernet port settings (admin state, subscriber ID, security profile)")
    wait_for_online: bool = Field(False, description="Hold the job open until the ONT comes online")

    _unique_ports = field_validator("ports")(check_unique_ports)
    _unique_services = field_validator("services")(check_unique_services)


class CutoverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subscriber_id: SubscriberId
    ont_id: Optional[OntId] = Field(None, description="Needed only if the subscriber has more than one ONT")
    xgs_serial_number: Optional[SerialNumber] = Field(
        None, description="Serial number of the replacement XGS ONT, if already known")


@router.post("/signups", response_model=JobAccepted, status_code=202)
async def signup(request: SignupRequest, http_request: Request, network: str = NetworkPath,
                 jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """
    New subscriber in a legacy GPON area: provision their ONT and services on CMS.

    The ONT does not have to be installed yet. CMS holds the configuration
    against the serial number and applies it when the ONT is first seen.
    """
    check_bandwidth_profiles((s.bandwidth_profile for s in request.services), settings.known_bandwidth_profiles())
    event = GponSignupEvent(event_id=new_id(), network=network, requested_by=audit.caller(http_request),
                            **request.model_dump())
    logger.info(f"Queueing signup of subscriber {event.subscriber_id} on {network} as job {event.event_id}")
    return accepted(await jobs.submit(event_to_command(event)), http_request)


@router.post("/cutovers", response_model=JobAccepted, status_code=202)
async def cutover(request: CutoverRequest, http_request: Request, network: str = NetworkPath,
                  jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """
    Hand a legacy GPON subscriber over to XGS.

    Reads the subscriber's ONT, VLANs and profiles from CMS, translates them,
    and publishes a migration request for the XGS side. CMS is not changed:
    the legacy ONT stays in service until someone retires it.
    """
    event = XgsCutoverEvent(event_id=new_id(), network=network, requested_by=audit.caller(http_request),
                            **request.model_dump())
    logger.info(f"Queueing cutover of subscriber {event.subscriber_id} on {network} as job {event.event_id}")
    return accepted(await jobs.submit(event_to_command(event)), http_request)
