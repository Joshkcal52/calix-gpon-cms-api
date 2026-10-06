"""
Provisioning Models
Pydantic models for ONT provisioning through the CMS northbound interface.

Every value that ends up in a CMS request is validated here first, so
nothing unchecked reaches the NBI client or a playbook.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import Annotated, Any, Dict, List, Optional
from enum import Enum

# CMS network (node) name, e.g. NTWK-MOCK-E7
NETWORK_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"

NetworkName = Annotated[str, Field(pattern=NETWORK_PATTERN, description="CMS network name")]
SerialNumber = Annotated[str, Field(pattern=r"^[A-Za-z0-9]{4,16}$", description="ONT serial number")]
ProfileName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")]
OntId = Annotated[int, Field(ge=1, description="ONT ID on the network")]


class OperState(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class EthernetService(BaseModel):
    """A VLAN bound to an ONT Ethernet port"""
    model_config = ConfigDict(extra="forbid")

    vlan: int = Field(..., ge=1, le=4094, description="Service VLAN")
    ont_port: int = Field(1, ge=1, le=8, description="ONT Ethernet port number")
    bandwidth_profile: ProfileName = Field(..., description="Bandwidth profile name")
    description: Optional[str] = Field(None, max_length=64)


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
    services: List[EthernetService] = []


class OntSpec(BaseModel):
    """What the gateway needs to create an ONT"""
    serial_number: SerialNumber
    ont_id: Optional[OntId] = Field(None, description="Leave empty to take the next free ID")
    profile: ProfileName = Field(..., description="ONT profile name")
    description: Optional[str] = Field(None, max_length=64)
    subscriber_id: Optional[str] = Field(None, pattern=r"^[A-Za-z0-9_.-]{1,32}$")


class ActivationRequest(OntSpec):
    """Activate a discovered ONT: create it, bind its services, wait for it to come online"""
    model_config = ConfigDict(extra="forbid")

    services: List[EthernetService] = Field(default_factory=list, max_length=8)
    rollback_on_failure: bool = Field(
        True, description="Delete the ONT again if a service cannot be bound")


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
