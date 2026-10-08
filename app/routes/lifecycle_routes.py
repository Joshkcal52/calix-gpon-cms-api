"""
ONT Lifecycle Routes
Everything after an ONT is provisioned: read its ports and router side,
enable and disable ports and services, change or delete a service, delete
the ONT. Plus lookups of the ONT profiles and VLANs that exist on a network.

Reads answer within the request. Changes are queued as CHANGE_ONT jobs and
answered with 202 (see app/controller/lifecycle_controller.py).

Ports are named eth1..eth8 for Ethernet ports and rg1 for the built-in
router of ONTs such as the 844G. Router-side changes need the CMS_RG_*
settings on a real CMS (app/nbi/router_side.py); without them the job fails
with cms_unverified and nothing is sent.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, Path, Query, Request

from app.controller.job_controller import JobStore
from app.controller.provisioning_controller import check_bandwidth_profiles
from app.dependencies import get_gateway, get_jobs
from app.models.job_model import Command, JobAccepted, LANE_PROVISIONING
from app.models.lifecycle_model import (
    ACTION_CHANGE_ONT, CHANGE_KINDS, DeleteOnt, DeleteService, UpdatePort, UpdateService, describe
)
from app.models.provisioning_model import (
    NETWORK_PATTERN, PORT_PATTERN, OntPorts, OntProfile, PortChange, ServiceChange, Vlan
)
from app.nbi.gateway import NbiGateway
from app.routes.job_routes import accepted
from config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/networks/{network}", tags=["ONT lifecycle (CMS NBI)"])

NetworkPath = Path(..., pattern=NETWORK_PATTERN, description="CMS network name, e.g. NTWK-MOCK-E7")
OntPath = Path(..., ge=1, description="ONT ID")
PortPath = Path(..., pattern=PORT_PATTERN, description="eth1..eth8, or rg1 for the router side")
ServicePath = Path(..., ge=1, le=12, description="Service number on the port")


async def _queue(jobs: JobStore, http_request: Request, network: str, ont_id: int, change) -> JobAccepted:
    command = Command(
        action=ACTION_CHANGE_ONT, lane=LANE_PROVISIONING,
        key=network,  # one network's changes run in order, one at a time
        kind=CHANGE_KINDS[change.op], summary=describe(change, network, ont_id),
        payload={"network": network, "ont_id": ont_id, "change": change.model_dump(mode="json")})
    logger.info(f"Queueing {command.summary} as job {command.id}")
    return accepted(await jobs.submit(command), http_request)


# --- Lookups --------------------------------------------------------------------

@router.get("/ont-profiles", response_model=List[OntProfile])
async def list_ont_profiles(network: str = NetworkPath,
                            gateway: NbiGateway = Depends(get_gateway)) -> List[OntProfile]:
    """
    ONT profiles defined on the network, with their port counts. `rg_ports`
    above 0 means the model has a built-in router. The counts are read from the
    profile's geth/feth/pots/rg attributes; reading them as counts is inferred.
    """
    return await gateway.list_ont_profiles(network)


@router.get("/vlans", response_model=List[Vlan])
async def list_vlans(network: str = NetworkPath, gateway: NbiGateway = Depends(get_gateway)) -> List[Vlan]:
    """VLANs defined on the network"""
    return await gateway.list_vlans(network)


@router.get("/onts/{ont_id}/ports", response_model=OntPorts)
async def get_ont_ports(network: str = NetworkPath, ont_id: int = OntPath,
                        gateway: NbiGateway = Depends(get_gateway)) -> OntPorts:
    """
    Ethernet ports (admin state, subscriber ID, security profile) and what is
    known about the router side. One CMS call per port.
    """
    return await gateway.get_ont_ports(network, ont_id)


# --- Ports ----------------------------------------------------------------------

@router.post("/onts/{ont_id}/ports/{port}/enable", response_model=JobAccepted, status_code=202)
async def enable_port(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                      port: str = PortPath, jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """Set a port's admin state to enabled"""
    change = UpdatePort(port=port, change=PortChange(admin_state="enabled"))
    return await _queue(jobs, http_request, network, ont_id, change)


@router.post("/onts/{ont_id}/ports/{port}/disable", response_model=JobAccepted, status_code=202)
async def disable_port(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                       port: str = PortPath, jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """Set a port's admin state to disabled"""
    change = UpdatePort(port=port, change=PortChange(admin_state="disabled"))
    return await _queue(jobs, http_request, network, ont_id, change)


@router.patch("/onts/{ont_id}/ports/{port}", response_model=JobAccepted, status_code=202)
async def update_port(body: PortChange, http_request: Request, network: str = NetworkPath,
                      ont_id: int = OntPath, port: str = PortPath,
                      jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """Change an Ethernet port's admin state, subscriber ID, security profile or description"""
    return await _queue(jobs, http_request, network, ont_id, UpdatePort(port=port, change=body))


# --- Services ---------------------------------------------------------------------

@router.post("/onts/{ont_id}/ports/{port}/services/{service_index}/enable",
             response_model=JobAccepted, status_code=202)
async def enable_service(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                         port: str = PortPath, service_index: int = ServicePath,
                         jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    change = UpdateService(port=port, service_index=service_index, change=ServiceChange(admin_state="enabled"))
    return await _queue(jobs, http_request, network, ont_id, change)


@router.post("/onts/{ont_id}/ports/{port}/services/{service_index}/disable",
             response_model=JobAccepted, status_code=202)
async def disable_service(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                          port: str = PortPath, service_index: int = ServicePath,
                          jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    change = UpdateService(port=port, service_index=service_index, change=ServiceChange(admin_state="disabled"))
    return await _queue(jobs, http_request, network, ont_id, change)


@router.patch("/onts/{ont_id}/ports/{port}/services/{service_index}",
              response_model=JobAccepted, status_code=202)
async def update_service(body: ServiceChange, http_request: Request, network: str = NetworkPath,
                         ont_id: int = OntPath, port: str = PortPath, service_index: int = ServicePath,
                         jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """Change a service's bandwidth profile, tag action, VLAN or admin state. Only what differs is sent."""
    check_bandwidth_profiles([body.bandwidth_profile], settings.known_bandwidth_profiles())
    change = UpdateService(port=port, service_index=service_index, change=body)
    return await _queue(jobs, http_request, network, ont_id, change)


@router.delete("/onts/{ont_id}/ports/{port}/services/{service_index}",
               response_model=JobAccepted, status_code=202)
async def delete_service(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                         port: str = PortPath, service_index: int = ServicePath,
                         expect_vlan: Optional[int] = Query(None, ge=1, le=4094,
                                                            description="Refuse unless the service is on this VLAN"),
                         jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """Delete one service. Deleting one that is already gone succeeds."""
    change = DeleteService(port=port, service_index=service_index, expect_vlan=expect_vlan)
    return await _queue(jobs, http_request, network, ont_id, change)


# --- The ONT ------------------------------------------------------------------------

@router.delete("/onts/{ont_id}", response_model=JobAccepted, status_code=202)
async def delete_ont(http_request: Request, network: str = NetworkPath, ont_id: int = OntPath,
                     serial_number: str = Query(..., pattern=r"^[A-Za-z0-9]{4,16}$",
                                                description="The ONT's serial number, as a safety check"),
                     force: bool = Query(False, description="Also delete its data services"),
                     voice_checked: bool = Query(
                         False, description="Someone has checked in CMS that it has no voice service to keep. "
                                            "Required if its ONT profile has voice ports"),
                     jobs: JobStore = Depends(get_jobs)) -> JobAccepted:
    """
    Delete an ONT. Refused if the ONT ID now holds a different serial number,
    if it still has services and force is false, or if its ONT profile has
    voice ports and voice_checked is false (this API does not read voice
    services). Deleting one that is already gone succeeds.

    With force, the data services are deleted one by one first. The ONT
    itself is always deleted without CMS's force, so CMS refuses it if
    anything is still on it.
    """
    change = DeleteOnt(serial_number=serial_number, force=force, voice_checked=voice_checked)
    return await _queue(jobs, http_request, network, ont_id, change)
