"""
Provisioning Routes
ONT operations through the Calix CMS northbound interface.

Reads answer within the request (the fast lane). Changes are written to the
queue and answered with 202 Accepted and a job to follow.
"""

from fastapi import APIRouter, Depends, Path, Request
from typing import Any, Dict, List
import logging

from app.controller.job_controller import JobStore
from app.controller.provisioning_controller import check_bandwidth_profiles, check_ont_status
from app.dependencies import get_gateway, get_jobs
from app.models.job_model import ACTION_ACTIVATE_ONT, Command, JobAccepted, LANE_PROVISIONING
from app.models.provisioning_model import (
    ActivationRequest, DiscoveredOnt, NETWORK_PATTERN, OntState,
    StatusCheckRequest, StatusCheckResult
)
from app.nbi.gateway import NbiGateway
from app.routes.job_routes import accepted
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["Provisioning (CMS NBI)"])

NetworkPath = Path(..., pattern=NETWORK_PATTERN, description="CMS network name, e.g. NTWK-MOCK-E7")


@router.get("/nbi/health")
async def nbi_health(gateway: NbiGateway = Depends(get_gateway)) -> Dict[str, Any]:
    """
    Which CMS backend is in use and the state of its circuit breaker.

    Reports this API process only; each worker has its own breaker.
    """
    return gateway.health()


@router.get("/networks/{network}/onts/discovered", response_model=List[DiscoveredOnt])
async def list_discovered_onts(network: str = NetworkPath,
                               gateway: NbiGateway = Depends(get_gateway)) -> List[DiscoveredOnt]:
    """ONTs seen on a PON that are not provisioned yet"""
    return await gateway.list_discovered_onts(network)


@router.post("/networks/{network}/onts/status-check", response_model=StatusCheckResult)
async def status_check(request: StatusCheckRequest, network: str = NetworkPath,
                       gateway: NbiGateway = Depends(get_gateway)) -> StatusCheckResult:
    """
    Read up to 200 ONTs in parallel.

    One unreadable ONT does not fail the check: its entry carries an `error`
    instead of an `ont`.
    """
    return await check_ont_status(gateway, network, request.ont_ids)


@router.get("/networks/{network}/onts/{ont_id}", response_model=OntState)
async def get_ont(network: str = NetworkPath, ont_id: int = Path(..., ge=1),
                  gateway: NbiGateway = Depends(get_gateway)) -> OntState:
    """Current state of one provisioned ONT"""
    return await gateway.get_ont(network, ont_id)


@router.post("/networks/{network}/activations", response_model=JobAccepted, status_code=202)
async def activate(request: ActivationRequest, http_request: Request, network: str = NetworkPath,
                   jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """
    Activate a discovered ONT: create it, bind its services, wait for it to
    come online.

    The request is validated, written to the queue, and answered with 202
    straight away. Follow `events_url` (Server-Sent Events) for live progress
    or poll `status_url`. Sending the same activation twice is safe: the
    second run finds the ONT in place and only adds what is missing.
    """
    check_bandwidth_profiles((s.bandwidth_profile for s in request.services), settings.known_bandwidth_profiles())
    command = Command(
        action=ACTION_ACTIVATE_ONT,
        lane=LANE_PROVISIONING,
        key=network,  # one network's commands run in order, one at a time
        kind="activation",
        summary=f"Activate ONT {request.serial_number} on {network}",
        payload={"network": network, "request": request.model_dump()})
    logger.info(f"Queueing {command.summary} as job {command.id}")
    return accepted(await jobs.submit(command), http_request)
