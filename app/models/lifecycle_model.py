"""
Lifecycle Models
Changes to an ONT that already exists: port and service admin state, service
updates, deleting a service or the whole ONT. Each one is queued as a job
(action CHANGE_ONT) and applied by a worker.

Every change is written so that running it twice gives the same result: the
worker reads CMS first and sends only what is still different.
"""

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.models.provisioning_model import PortChange, PortName, SerialNumber, ServiceChange

ACTION_CHANGE_ONT = "CHANGE_ONT"


class UpdatePort(BaseModel):
    """Enable or disable a port, or change its subscriber ID, security profile or description"""
    model_config = ConfigDict(extra="forbid")

    op: Literal["update_port"] = "update_port"
    port: PortName
    change: PortChange


class UpdateService(BaseModel):
    """Enable or disable a service, or change its bandwidth profile, tag action or VLAN"""
    model_config = ConfigDict(extra="forbid")

    op: Literal["update_service"] = "update_service"
    port: PortName
    service_index: int = Field(..., ge=1, le=12)
    change: ServiceChange


class DeleteService(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["delete_service"] = "delete_service"
    port: PortName
    service_index: int = Field(..., ge=1, le=12)
    expect_vlan: Optional[int] = Field(
        None, ge=1, le=4094, description="Refuse to delete if the service is not on this VLAN")


class DeleteOnt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["delete_ont"] = "delete_ont"
    serial_number: SerialNumber = Field(
        ..., description="The ONT's serial number. Deletion is refused if the ONT ID now holds another serial")
    force: bool = Field(
        False, description="Delete the ONT's data services first, then the ONT. Without it, an ONT that still has "
                           "services is refused. Either way the ONT itself is deleted without CMS's force, so "
                           "CMS refuses it if anything this API cannot see (such as voice) is still on it")
    voice_checked: bool = Field(
        False, description="Confirms someone has checked in CMS that this ONT has no voice service to keep. "
                           "Required when its ONT profile has voice (POTS) ports, or when that cannot be read: "
                           "this API does not read voice services")


OntChange = Annotated[Union[UpdatePort, UpdateService, DeleteService, DeleteOnt], Field(discriminator="op")]
OntChangeAdapter: TypeAdapter = TypeAdapter(OntChange)

CHANGE_KINDS = {"update_port": "port-change", "update_service": "service-change",
                "delete_service": "service-delete", "delete_ont": "ont-delete"}


def describe(change: "UpdatePort | UpdateService | DeleteService | DeleteOnt", network: str, ont_id: int) -> str:
    where = f"ONT {ont_id} on {network}"
    if isinstance(change, UpdatePort):
        return f"Change port {change.port} of {where}"
    if isinstance(change, UpdateService):
        return f"Change service {change.service_index} on port {change.port} of {where}"
    if isinstance(change, DeleteService):
        return f"Delete service {change.service_index} on port {change.port} of {where}"
    return f"Delete {where} ({change.serial_number})"
