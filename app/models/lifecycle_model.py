"""
Lifecycle Models
Changes to an ONT that already exists: port and service admin state, service
updates, deleting a service or the whole ONT. Each one is queued as a job
(action CHANGE_ONT) and applied by a worker.

Every change is written so that running it twice gives the same result: the
worker reads CMS first and sends only what is still different, then reads it
back to check that CMS holds what was asked for.

ONT IDs and service numbers are reused after a delete, so every change carries
what proves it is about the same thing: serial_number (the ONT) and, for a
service, expect_vlan (its outer VLAN, or "none" for a service without one).
A change whose target no longer matches is refused.
dry_run runs every read and check and reports what would be sent, sending
nothing.
"""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.models.provisioning_model import PortChange, PortName, SerialNumber, ServiceChange

ExpectSerial = Field(..., description="The ONT's serial number. Refused if the ONT ID now holds another ONT "
                                     "(ONT IDs are reused)")
# The service's outer VLAN, or "none" for a service without one
ExpectedVlan = Union[Annotated[int, Field(ge=1, le=4094)], Literal["none"]]
ExpectVlan = Field(..., description="The service's outer VLAN, or none if it has none. Refused if the service "
                                    "number now holds another service (service numbers are reused)")
DryRun = Field(False, description="Read and check everything, report what would be sent, send nothing")

ACTION_CHANGE_ONT = "CHANGE_ONT"


class UpdatePort(BaseModel):
    """Enable or disable a port, or change its subscriber ID, security profile or description"""
    model_config = ConfigDict(extra="forbid")

    op: Literal["update_port"] = "update_port"
    port: PortName
    change: PortChange
    serial_number: SerialNumber = ExpectSerial
    dry_run: bool = DryRun


class UpdateService(BaseModel):
    """Enable or disable a service, or change its bandwidth profile, tag action or VLAN"""
    model_config = ConfigDict(extra="forbid")

    op: Literal["update_service"] = "update_service"
    port: PortName
    service_index: int = Field(..., ge=1, le=12)
    change: ServiceChange
    serial_number: SerialNumber = ExpectSerial
    expect_vlan: ExpectedVlan = ExpectVlan
    dry_run: bool = DryRun


class DeleteService(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["delete_service"] = "delete_service"
    port: PortName
    service_index: int = Field(..., ge=1, le=12)
    expect_vlan: ExpectedVlan = ExpectVlan
    serial_number: SerialNumber = ExpectSerial
    dry_run: bool = DryRun


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
    dry_run: bool = DryRun


OntChange = Annotated[Union[UpdatePort, UpdateService, DeleteService, DeleteOnt], Field(discriminator="op")]
OntChangeAdapter: TypeAdapter = TypeAdapter(OntChange)

CHANGE_KINDS = {"update_port": "port-change", "update_service": "service-change",
                "delete_service": "service-delete", "delete_ont": "ont-delete"}


def describe(change: "UpdatePort | UpdateService | DeleteService | DeleteOnt", network: str, ont_id: int) -> str:
    text = _describe(change, network, ont_id)
    return f"Dry run: {text[0].lower()}{text[1:]}" if change.dry_run else text


def _describe(change: "UpdatePort | UpdateService | DeleteService | DeleteOnt", network: str, ont_id: int) -> str:
    where = f"ONT {ont_id} on {network}"
    if isinstance(change, UpdatePort):
        return f"Change port {change.port} of {where}"
    if isinstance(change, UpdateService):
        return f"Change service {change.service_index} on port {change.port} of {where}"
    if isinstance(change, DeleteService):
        return f"Delete service {change.service_index} on port {change.port} of {where}"
    return f"Delete {where} ({change.serial_number})"
