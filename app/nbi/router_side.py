"""
THE ROUTER SIDE OF AN ONT: NOT KNOWN, ISOLATED HERE ON PURPOSE.

ONTs such as the 844G have a built-in router (Calix calls it the RG, the
residential gateway). Each of their Ethernet ports either sits behind that
router (router mode), with the data service attached to the router side, or
is a plain bridge to the customer's own router (bridge mode), with the service
attached to the port. The same model runs either way, so the mode must be
read for each port. The worst manual-cutover errors were 844Gs that ran as
bridges and were rebuilt as router/ONT combos: two routers in a row.

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
  3. How an Ethernet port's binding to the router ("RG 1", i.e. router mode
     versus bridge mode) appears in the XML, for reading or for setting it.
  4. How the WAN protocol of a router-side service is expressed.

Items 1 to 3 are answered by the ONE function below, router_side_addressing(),
and by nothing else in the code base. Each piece is its own setting, empty
until an operator has checked a real CMS:

  CMS_RG_SERVICE_SLOT        item 1
  CMS_RG_PORT_TYPE           item 2 (with CMS_RG_PORT_ID_ELEMENT)
  CMS_RG_PORT_ID_ELEMENT
  CMS_RG_PORT_MODE_ELEMENT   item 3, reading only: the child element of an
                             OntEthGe reply that names the router the port is
                             bound to. Present and not empty or "none" is read
                             as router mode, present and empty or "none" as
                             bridge mode, absent as unknown. That reading rule
                             is itself an assumption to confirm.

python -m app.tools.cms_check prints the ontslot of every service it reads and
every OntEthGe field it does not recognise; run it against an 844G running as
a router and one running as a bridge, and compare.

While a piece is not configured: router-side writes are refused with
cms_unverified and nothing is sent; services in an ONT slot other than 3 are
read back as attachment "unknown"; port mode is read back as "unknown". Nothing
is guessed, and the mode is never taken from the ONT model or profile.

Setting a port's mode through the NBI is not built at all (item 3, writing):
on a real CMS such a request is always refused. Item 4 is not built.

The mock CMS NBI server uses deliberately fake values (MOCK_RG_ADDRESSING) so
the code path can be tested end to end. They are NOT Calix values.
"""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class RouterAddressing:
    service_slot: Optional[str] = None  # <ontslot> of an EthSvc on the router side; <ontethany> is the RG number
    port_type: Optional[str] = None  # <type> of the router port object
    port_id_element: Optional[str] = None  # element inside <id> that carries the router port number
    port_mode_element: Optional[str] = None  # OntEthGe child naming the router a port is bound to

    @property
    def router_ports(self) -> bool:
        return bool(self.port_type and self.port_id_element)


# Used by the mock NBI server only. Obviously fake so nobody mistakes them for Calix's.
MOCK_RG_ADDRESSING = RouterAddressing(
    service_slot="UNVERIFIED-RG", port_type="UnverifiedRgPort", port_id_element="unverifiedrgport",
    port_mode_element="unverified-rg-binding")


def router_side_addressing(settings: Any) -> Optional[RouterAddressing]:
    """
    UNVERIFIED. How CMS addresses the router side of an ONT, from the
    CMS_RG_* settings. Returns None if none is set; otherwise each piece that
    is not set stays None and the code that needs it refuses or reports
    "unknown".
    """
    addressing = RouterAddressing(
        service_slot=getattr(settings, "cms_rg_service_slot", None) or None,
        port_type=getattr(settings, "cms_rg_port_type", None) or None,
        port_id_element=getattr(settings, "cms_rg_port_id_element", None) or None,
        port_mode_element=getattr(settings, "cms_rg_port_mode_element", None) or None)
    return addressing if addressing != RouterAddressing() else None


def read_port_mode(addressing: Optional[RouterAddressing], reply: Any) -> str:
    """
    UNVERIFIED reading rule for an Ethernet port's mode (see item 3 above).
    `reply` is the OntEthGe object as parsed by xmltodict.
    """
    if addressing is None or not addressing.port_mode_element or not isinstance(reply, dict):
        return "unknown"
    if addressing.port_mode_element not in reply:
        return "unknown"
    value = reply[addressing.port_mode_element]
    if isinstance(value, dict):
        value = value.get("#text") or ("set" if any(k for k in value if not k.startswith("@")) else None)
    text = str(value or "").strip().lower()
    return "bridge" if text in ("", "none") else "router"


MODE_NOT_SETTABLE = (
    "Setting an Ethernet port's bridge or router mode through the CMS API is not known, so it is not "
    "built. Nothing was sent. Set it in the CMS screens, then read it back here.")

NOT_CONFIGURED = (
    "Router-side (RG) services and ports are not verified against a real CMS, so this project does not "
    "know how CMS addresses them. Nothing was sent. Run python -m app.tools.cms_check against an ONT "
    "that already has router-side services, then set the CMS_RG_* settings (see app/nbi/router_side.py).")
