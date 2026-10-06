"""
Legacy GPON Models
The events this layer consumes, what it reads from CMS, and what it sends to
the modern XGS/SMx side.

  LegacyGponEvent        what arrives on the legacy-gpon-events topic
  LegacySubscriberRecord what CMS holds for a subscriber (the "legacy" shape)
  XgsMigrationRequest    what is published for the XGS side (the "modern" shape)
  ProfileMap             how legacy profile IDs translate to modern names

XgsMigrationRequest is THIS project's proposal for the message. The schema of
your existing XGS/SMx topic is not known here, so match this model to that
contract before pointing XGS_MIGRATION_TOPIC at production.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from app.models.provisioning_model import (
    EthernetService, NetworkName, OntId, OntState, ProfileName, ProvisionedService,
    SerialNumber, SubscriberId
)

EventId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.:-]{8,64}$",
                               description="Unique per event. Reused as the job ID and to spot repeats")]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- Events on the legacy topic ------------------------------------------------

class _EventBase(BaseModel):
    event_id: EventId
    occurred_at: datetime = Field(default_factory=_now)
    network: NetworkName
    subscriber_id: SubscriberId

    @field_validator("occurred_at")
    @classmethod
    def _assume_utc(cls, value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


class GponSignupEvent(_EventBase):
    """Mode A: a new subscriber in a legacy GPON area. Provision their ONT on CMS."""
    type: Literal["GPON_SIGNUP"] = "GPON_SIGNUP"
    serial_number: SerialNumber
    ont_id: Optional[OntId] = Field(None, description="Leave empty to take the next free ID")
    profile: ProfileName = Field(..., description="ONT profile")
    description: Optional[str] = Field(None, max_length=64)
    services: List[EthernetService] = Field(..., min_length=1, max_length=8)
    wait_for_online: bool = Field(False, description="Hold the job open until the ONT comes online")


class XgsCutoverEvent(_EventBase):
    """Mode B: move a legacy GPON subscriber to XGS. Read their config from CMS and hand it over."""
    type: Literal["XGS_CUTOVER"] = "XGS_CUTOVER"
    ont_id: Optional[OntId] = Field(None, description="Needed only if the subscriber has more than one ONT")
    xgs_serial_number: Optional[SerialNumber] = Field(
        None, description="Serial number of the replacement XGS ONT, if already known")


LegacyGponEvent = Annotated[Union[GponSignupEvent, XgsCutoverEvent], Field(discriminator="type")]
LegacyGponEventAdapter: TypeAdapter = TypeAdapter(LegacyGponEvent)


# --- Legacy side: what CMS holds -------------------------------------------------

class LegacySubscriberRecord(BaseModel):
    """One subscriber's GPON ONT and services, as read from CMS"""
    network: str
    subscriber_id: str
    ont_id: int
    serial_number: str
    ont_profile: Optional[str] = None
    model: Optional[str] = None
    pon_port: Optional[str] = None
    description: Optional[str] = None
    admin_state: str = "enabled"
    services: List[ProvisionedService]

    @classmethod
    def from_ont(cls, network: str, subscriber_id: str, ont: OntState) -> "LegacySubscriberRecord":
        return cls(network=network, subscriber_id=subscriber_id, ont_id=ont.ont_id,
                   serial_number=ont.serial_number, ont_profile=ont.profile, model=ont.model,
                   pon_port=ont.pon_port, description=ont.description,
                   admin_state=ont.admin_state, services=ont.services)


# --- Translation table -------------------------------------------------------------

class BandwidthMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_profile: str = Field(..., min_length=1, description="Service profile name on the XGS side")
    service_type: Literal["data", "voice", "video", "other"] = "data"


class ProfileMap(BaseModel):
    """How legacy CMS identifiers translate to the modern side. Loaded from YAML."""
    model_config = ConfigDict(extra="forbid")

    bandwidth_profiles: Dict[str, BandwidthMapping] = Field(
        default_factory=dict, description="Legacy bandwidth profile -> modern service profile")
    tag_action_vlans: Dict[str, int] = Field(
        default_factory=dict,
        description="Legacy tag action -> VLAN, for services whose VLAN is set by the tag action")

    @classmethod
    def load(cls, path: Union[str, Path]) -> "ProfileMap":
        with open(path, encoding="utf-8") as f:
            return cls.model_validate(yaml.safe_load(f) or {})


# --- Modern side: what is sent to the XGS/SMx topic ----------------------------------

class XgsService(BaseModel):
    service_type: str
    service_profile: str
    vlan: int = Field(..., ge=1, le=4094)
    inner_vlan: Optional[int] = None
    ont_port: int
    enabled: bool = True
    description: Optional[str] = None


class LegacySource(BaseModel):
    """Where the subscriber is coming from, so the XGS side can trace and later retire it"""
    system: Literal["calix-cms"] = "calix-cms"
    network: str
    ont_id: int
    serial_number: str
    ont_profile: Optional[str] = None
    pon_port: Optional[str] = None


class XgsMigrationRequest(BaseModel):
    schema_version: Literal[1] = 1
    action: Literal["MIGRATE_GPON_TO_XGS"] = "MIGRATE_GPON_TO_XGS"
    migration_id: str = Field(..., description="Same as the cutover event ID. A repeat carries the same ID")
    requested_at: datetime
    subscriber_id: str
    description: Optional[str] = None
    xgs_serial_number: Optional[str] = None
    source: LegacySource
    services: List[XgsService]


class TranslationError(ValueError):
    """The legacy record cannot be expressed on the modern side. Lists every problem found."""

    def __init__(self, problems: List[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


def translate(record: LegacySubscriberRecord, event: XgsCutoverEvent,
              profile_map: ProfileMap) -> XgsMigrationRequest:
    """
    Turn what CMS holds into a migration request for the XGS side.

    Nothing is guessed. A bandwidth profile with no mapping, or a service
    whose VLAN cannot be determined, fails the whole translation with a
    message naming each one, so a subscriber is never migrated onto the wrong
    plan or VLAN.
    """
    problems: List[str] = []
    services: List[XgsService] = []

    if not record.services:
        problems.append(f"ONT {record.ont_id} has no services on CMS, so there is nothing to migrate")

    for service in record.services:
        label = f"service {service.service_index} on port {service.ont_port}"
        mapping = profile_map.bandwidth_profiles.get(service.bandwidth_profile or "")
        if mapping is None:
            problems.append(f"{label}: bandwidth profile '{service.bandwidth_profile}' has no entry "
                            f"under bandwidth_profiles in the profile map")

        vlan = service.vlan
        if vlan is None:
            vlan = profile_map.tag_action_vlans.get(service.tag_action or "")
            if vlan is None:
                problems.append(f"{label}: no VLAN on the service and tag action '{service.tag_action}' "
                                f"has no entry under tag_action_vlans in the profile map")

        if mapping is not None and vlan is not None:
            services.append(XgsService(
                service_type=mapping.service_type, service_profile=mapping.service_profile,
                vlan=vlan, inner_vlan=service.inner_vlan, ont_port=service.ont_port,
                enabled=service.admin_state.startswith("enabled"), description=service.description))

    if problems:
        raise TranslationError(problems)

    return XgsMigrationRequest(
        migration_id=event.event_id,
        requested_at=event.occurred_at,
        subscriber_id=record.subscriber_id,
        description=record.description,
        xgs_serial_number=event.xgs_serial_number,
        source=LegacySource(network=record.network, ont_id=record.ont_id,
                            serial_number=record.serial_number, ont_profile=record.ont_profile,
                            pon_port=record.pon_port),
        services=services)
