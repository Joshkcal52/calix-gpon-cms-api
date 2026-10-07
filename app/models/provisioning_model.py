"""
Provisioning Models
Pydantic models for ONT provisioning through the CMS northbound interface.

Every value that ends up in a CMS request is validated here first, so
nothing unchecked reaches the NBI client or a playbook.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple
from enum import Enum

# CMS network (node) name, e.g. NTWK-MOCK-E7
NETWORK_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"

NetworkName = Annotated[str, Field(pattern=NETWORK_PATTERN, description="CMS network name")]
SerialNumber = Annotated[str, Field(pattern=r"^[A-Za-z0-9]{4,16}$", description="ONT serial number")]
ProfileName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")]
OntId = Annotated[int, Field(ge=1, description="ONT ID on the network")]
SubscriberId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,32}$", description="Billing subscriber ID")]
# A security profile is a numeric database ID on CMS (1 to 16 per the cms-nbi-client docstring)
SecurityProfileId = Annotated[str, Field(pattern=r"^[0-9]{1,3}$", description="Ethernet security profile ID")]
AdminState = Literal["enabled", "disabled", "enabled-no-alarms"]

# A port on an ONT, as used in URLs: eth1..eth8 for Ethernet ports, rg1..rg8 for
# the router (residential gateway) side of ONTs that have one, such as the 844G.
PORT_PATTERN = r"^(eth|rg)[1-8]$"
PortName = Annotated[str, Field(pattern=PORT_PATTERN, description="eth1..eth8, or rg1 for the router side")]


class Attachment(str, Enum):
    """Where a service is attached on the ONT"""
    ETHERNET = "ethernet"  # a plain Ethernet port (CMS ONT slot 3)
    ROUTER = "router"  # the built-in router (RG) of an ONT such as the 844G
    UNKNOWN = "unknown"  # CMS reported an ONT slot this code does not know how to read


def port_name(attachment: "Attachment | str", number: int) -> str:
    return f"{'rg' if Attachment(attachment) == Attachment.ROUTER else 'eth'}{number}"


def parse_port(name: str) -> Tuple[Attachment, int]:
    """eth2 -> (ETHERNET, 2), rg1 -> (ROUTER, 1)"""
    if name.startswith("rg"):
        return Attachment.ROUTER, int(name[2:])
    return Attachment.ETHERNET, int(name[3:])


class OperState(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class EthernetService(BaseModel):
    """A service to bind to an ONT Ethernet port, or to its built-in router"""
    model_config = ConfigDict(extra="forbid")

    vlan: int = Field(..., ge=1, le=4094, description="Service VLAN (sent to CMS as the outer tag)")
    ont_port: int = Field(1, ge=1, le=8, description="Port number on the side given by attach_to")
    attach_to: Literal["ethernet", "router"] = Field(
        "ethernet", description="ethernet: a plain Ethernet port. router: the built-in router (RG) of an "
                                "ONT such as the 844G. Router-side services need CMS_RG_* settings on a "
                                "real CMS; see app/nbi/router_side.py")
    bandwidth_profile: ProfileName = Field(..., description="Bandwidth profile (a numeric ID on a real CMS)")
    tag_action: Optional[ProfileName] = Field(
        None, description="Service tag action (a numeric ID on a real CMS, which requires one)")
    description: Optional[str] = Field(None, max_length=64)


class ProvisionedService(BaseModel):
    """A service as CMS reports it on an ONT"""
    service_index: int = Field(1, description="Service number on the ONT port")
    ont_port: int = 1
    attachment: Attachment = Field(Attachment.ETHERNET, description="ethernet, router or unknown")
    ont_slot: Optional[str] = Field(None, description="The raw ONT slot CMS reported, kept for diagnosis")
    vlan: Optional[int] = Field(None, description="Outer tag. Empty when the tag action sets the VLAN")
    inner_vlan: Optional[int] = None
    bandwidth_profile: Optional[str] = None
    tag_action: Optional[str] = None
    admin_state: str = "enabled"
    description: Optional[str] = None


class DiscoveredOnt(BaseModel):
    """An ONT seen on a PON that has not been provisioned yet"""
    serial_number: str
    model: str
    pon_port: str = Field(..., description="GPON port as shelf/card/port")


class OntState(BaseModel):
    """A provisioned ONT as CMS reports it"""
    ont_id: int
    serial_number: str
    model: Optional[str] = None
    profile: Optional[str] = None
    admin_state: str = "enabled"
    oper_state: OperState = OperState.OFFLINE
    pon_port: Optional[str] = None
    description: Optional[str] = None
    subscriber_id: Optional[str] = None
    services: List[ProvisionedService] = []

    def service_at(self, attachment: Attachment, ont_port: int, service_index: int) -> Optional[ProvisionedService]:
        return next((s for s in self.services if (s.attachment, s.ont_port, s.service_index)
                     == (Attachment(attachment), ont_port, service_index)), None)


# --- Ports, router side and lookups ----------------------------------------------

class EthernetPortConfig(BaseModel):
    """Settings for one ONT Ethernet port, sent when an ONT is provisioned"""
    model_config = ConfigDict(extra="forbid")

    port: int = Field(..., ge=1, le=8, description="Ethernet port number")
    admin_state: Optional[AdminState] = None
    subscriber_id: Optional[SubscriberId] = None
    security_profile: Optional[SecurityProfileId] = None
    description: Optional[str] = Field(None, max_length=48)

    def changes(self) -> "PortChange":
        return PortChange(**self.model_dump(exclude={"port"}, exclude_none=True))


class PortChange(BaseModel):
    """New values for a port. Only the fields that are set are changed."""
    model_config = ConfigDict(extra="forbid")

    admin_state: Optional[AdminState] = None
    subscriber_id: Optional[SubscriberId] = None
    security_profile: Optional[SecurityProfileId] = None
    description: Optional[str] = Field(None, max_length=48)

    @model_validator(mode="after")
    def _something(self) -> "PortChange":
        if not self.model_dump(exclude_none=True):
            raise ValueError("Set at least one of admin_state, subscriber_id, security_profile, description")
        return self


class ServiceChange(BaseModel):
    """New values for a service. Only the fields that are set are changed."""
    model_config = ConfigDict(extra="forbid")

    admin_state: Optional[AdminState] = None
    bandwidth_profile: Optional[ProfileName] = None
    tag_action: Optional[ProfileName] = None
    vlan: Optional[int] = Field(None, ge=1, le=4094, description="New outer VLAN")

    @model_validator(mode="after")
    def _something(self) -> "ServiceChange":
        if not self.model_dump(exclude_none=True):
            raise ValueError("Set at least one of admin_state, bandwidth_profile, tag_action, vlan")
        return self


class PortState(BaseModel):
    """One ONT port as read back from CMS"""
    port: str = Field(..., description="eth1..eth8 or rg1")
    attachment: Attachment
    number: int
    admin_state: Optional[str] = None
    subscriber_id: Optional[str] = None
    security_profile: Optional[str] = None
    description: Optional[str] = None


class RouterState(BaseModel):
    """What is known about the ONT's built-in router (RG)"""
    present: Optional[bool] = Field(None, description="From the ONT profile's rg attribute. None if unknown")
    default_to_rg: Optional[bool] = Field(None, description="From the ONT profile's default-to-rg attribute")
    ports: List[PortState] = Field(default_factory=list, description="Router ports read from CMS")
    services: int = Field(0, description="Services attached to the router side")
    note: Optional[str] = Field(None, description="Why part of this could not be read")


class OntPorts(BaseModel):
    network: str
    ont_id: int
    profile: Optional[str] = None
    ethernet: List[PortState]
    router: RouterState


class OntProfile(BaseModel):
    """An ONT profile defined on the network. Port counts come from the profile's attributes."""
    id: str
    name: Optional[str] = None
    model: Optional[str] = None
    vendor: Optional[str] = None
    ge_ports: Optional[int] = None
    fe_ports: Optional[int] = None
    pots_ports: Optional[int] = None
    rg_ports: Optional[int] = Field(None, description="Router (RG) count. Above 0 means a built-in router")
    default_to_rg: Optional[bool] = None


class Vlan(BaseModel):
    id: int
    name: Optional[str] = None


def check_unique_ports(ports: List[EthernetPortConfig]) -> List[EthernetPortConfig]:
    numbers = [p.port for p in ports]
    if len(numbers) != len(set(numbers)):
        raise ValueError("Each Ethernet port may be listed once in ports")
    return ports


class OntSpec(BaseModel):
    """What the gateway needs to create an ONT"""
    serial_number: SerialNumber
    ont_id: Optional[OntId] = Field(None, description="Leave empty to take the next free ID")
    profile: ProfileName = Field(..., description="ONT profile name")
    description: Optional[str] = Field(None, max_length=64)
    subscriber_id: Optional[SubscriberId] = None


class ActivationRequest(OntSpec):
    """Activate a discovered ONT: create it, bind its services, wait for it to come online"""
    model_config = ConfigDict(extra="forbid")

    services: List[EthernetService] = Field(default_factory=list, max_length=8)
    ports: List[EthernetPortConfig] = Field(
        default_factory=list, max_length=8,
        description="Ethernet port settings (admin state, subscriber ID, security profile)")
    rollback_on_failure: bool = Field(
        True, description="Delete the ONT again if a service cannot be bound")
    require_discovery: bool = Field(
        True, description="Fail if the ONT has not been seen on a PON. Set false to pre-provision "
                          "by serial number before the ONT is installed")
    wait_for_online: bool = Field(True, description="Wait for the ONT to come online before finishing")

    _unique_ports = field_validator("ports")(check_unique_ports)


class StatusCheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ont_ids: List[OntId] = Field(..., min_length=1, max_length=200)


class StatusCheckItem(BaseModel):
    ont_id: int
    ont: Optional[OntState] = None
    error: Optional[Dict[str, Any]] = Field(None, description="Set when this ONT could not be read")


class StatusCheckResult(BaseModel):
    network: str
    checked: int
    online: int
    failed: int
    results: List[StatusCheckItem]
