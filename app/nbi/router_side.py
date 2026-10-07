"""
THE ROUTER SIDE OF AN ONT: NOT KNOWN, ISOLATED HERE ON PURPOSE.

ONTs such as the 844G have a built-in router (Calix calls it the RG, the
residential gateway). Data services on those ONTs attach to the router side
instead of to a plain Ethernet port.

What the public sources say, and nothing more:

  - cms-nbi-client 0.1.0: an ONT profile (OntProf) has the attributes `rg` and
    `default-to-rg`. An Ethernet service (EthSvc) is addressed by
    ont / ontslot / ontethany / ethsvc, and the library documents only three
    ontslot values: 3 Gigabit Ethernet, 4 HPNA Ethernet, 5 Fast Ethernet.
  - The IDI billing CMS R15.x integration docs: "RG" is a port type that can be
    enabled and disabled (with an "RG Port" number); when an ONT is linked,
    each Gigabit and Fast Ethernet port can be set to "RG 1"; data services
    take a WAN protocol (dhcp or static).

What neither source shows, and so this project does NOT know:

  1. Which ontslot (and ontethany) value an EthSvc on the router side uses.
  2. Which object type and ID element CMS uses for the router port itself,
     for reading it and for setting its admin state.
  3. How an Ethernet port is bound to the router ("RG 1") in the XML.
  4. How the WAN protocol of a router-side service is expressed.

Items 3 and 4 are not built. Items 1 and 2 are answered by the ONE function
below, router_side_addressing(), and by nothing else in the code base. It
returns None unless an operator has set all three CMS_RG_* settings after
checking a real CMS (python -m app.tools.cms_check prints the ontslot of
every service it reads; run it against an 844G that already has router-side
services). While it returns None, every router-side write is refused with
cms_unverified and nothing is sent, and services in an ONT slot other than 3
are read back as attachment "unknown", never guessed to be router-side.

The mock CMS NBI server uses deliberately fake values (MOCK_RG_ADDRESSING) so
the code path can be tested end to end. They are NOT Calix values.
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class RouterAddressing:
    service_slot: str  # the <ontslot> of an EthSvc on the router side; <ontethany> carries the RG port number
    port_type: str  # the <type> of the router port object
    port_id_element: str  # the element inside <id> that carries the router port number


# Used by the mock NBI server only. Obviously fake so nobody mistakes them for Calix's.
MOCK_RG_ADDRESSING = RouterAddressing(
    service_slot="UNVERIFIED-RG", port_type="UnverifiedRgPort", port_id_element="unverifiedrgport")


def router_side_addressing(settings: Any) -> Optional[RouterAddressing]:
    """
    UNVERIFIED. How CMS addresses the router side of an ONT, from
    CMS_RG_SERVICE_SLOT, CMS_RG_PORT_TYPE and CMS_RG_PORT_ID_ELEMENT.

    Returns None (router side unsupported) unless all three are set.
    """
    slot = getattr(settings, "cms_rg_service_slot", None)
    port_type = getattr(settings, "cms_rg_port_type", None)
    id_element = getattr(settings, "cms_rg_port_id_element", None)
    if not (slot and port_type and id_element):
        return None
    return RouterAddressing(service_slot=slot, port_type=port_type, port_id_element=id_element)


NOT_CONFIGURED = (
    "Router-side (RG) services and ports are not verified against a real CMS, so this project does not "
    "know how CMS addresses them. Nothing was sent. Run python -m app.tools.cms_check against an ONT "
    "that already has router-side services, then set CMS_RG_SERVICE_SLOT, CMS_RG_PORT_TYPE and "
    "CMS_RG_PORT_ID_ELEMENT (see app/nbi/router_side.py).")
