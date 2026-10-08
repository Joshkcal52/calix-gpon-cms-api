"""
CMS Backend
Talks to a real Calix CMS through the NBI client (app/nbi/client.py). Used
when CMS_HOST is set. It turns each operation into the CMS requests that
carry it out, and the replies into this project's models.

The client handles the transport: login and session, HTTP or HTTPS with
certificate verification, timeouts, paging, escaping and typed errors. The
NBI gateway in front of this backend provides retries, the circuit breaker
and the concurrency cap.

NOT VERIFIED AGAINST A REAL CMS. It is tested against the mock NBI server
(app/nbi/mock_cms_server.py). The request shapes are those cms-nbi-client
0.1.0 sends. CMS NBI documentation for an earlier release (not R15.x) has
since settled some of what was guessed:

  - show-ont answers each ONT as two objects, provisioning under get-config
    and state under get (app/nbi/client.py merges them).
  - op-stat takes the values enable, sys-disable and user-disable. That
    enable means the ONT is up is still inferred.
  - linked-pon is a GponPort with shelf, card and gponport.
  - The OntEthGe element names (admin, subscr-id, descr, sec/EthSecProf,
    speed, duplex, ...) are those of a port update.
  - A delete without force fails while the ONT still has services; with
    force, CMS deletes them too.
  - EthSvc: bw-prof 1-300, tag-action 1-255, out-tag and in-tag 2-4093 and
    read back as none when not sent, ethsvc 1-12, ontethany 1-8.

Still guesses: the rpc-error tags used to tell "not found" from "already
exists" (the documented error is operation-failed with the detail in
error-app-tag), the meaning of the OntProf values (port counts,
"true"/"false"), and the DiscOnt reply.

Two requests are deliberately not the library's: the library's Ethernet port
update (E7.Update.ont_geth) raised KeyError on every call and its service
update (E7.Update.ont_ethsvc) silently dropped tag action and bandwidth
profile changes. This project always sent those itself, and still does, in
the XML shape the library meant to produce.

The router side (RG) of ONTs such as the 844G is not known at all. Every
router-side request goes through app/nbi/router_side.py and is refused unless
an operator has configured it.

Every reply field this module does not recognise is recorded in
`unrecognised` (type -> field names), which python -m app.tools.cms_check
prints.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Set, Union

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, EthernetService, OntPorts, OntProfile, OntSpec, OntState, OperState,
    PortChange, PortState, ProvisionedService, RouterState, ServiceChange, Vlan, port_name
)
from app.nbi.client import CmsClient, element
from app.nbi.errors import NbiError, NbiNotFoundError, NbiNotVerifiedError, NbiRejectedError
from app.nbi.router_side import MODE_NOT_SETTABLE, NOT_CONFIGURED, RouterAddressing, read_port_mode
from app.nbi.serno import from_serno, to_serno

logger = logging.getLogger(__name__)

SYSTEM = "<type>System</type><id/>"

# Sent with every new ONT and every new service: the values cms-nbi-client sends
ONT_DEFAULTS = ("<pwe3prof><type>OntPwe3Prof</type><id><ontpwe3prof>1</ontpwe3prof></id></pwe3prof>{profile}"
                "<us-sdber-rate>5</us-sdber-rate><low-rx-opt-pwr-ne-thresh>-30.0</low-rx-opt-pwr-ne-thresh>"
                "<high-rx-opt-pwr-ne-thresh>-7.0</high-rx-opt-pwr-ne-thresh><battery-present>false</battery-present>"
                "<pse-max-power-budget>30</pse-max-power-budget><poe-class-control>disabled</poe-class-control>")
# The peak-rate overrides are "none" unless the service sets them
SERVICE_DEFAULTS = ("<in-tag>none</in-tag><mcast-prof></mcast-prof><pon-cos>derived</pon-cos>"
                    "<us-cir-override>none</us-cir-override>{us_pir}{ds_pir}<hot-swap>false</hot-swap>"
                    "<pppoe-force-discard>false</pppoe-force-discard>")

ONT_SLOT = "3"  # Gigabit Ethernet, per cms-nbi-client's documentation (4 is HPNA, 5 is Fast Ethernet)

# Reply values whose meaning is inferred, not taken from cms-nbi-client's documentation. Their raw
# values are collected in `observed` so a check against a real CMS shows them.
OBSERVED_FIELDS: Dict[str, tuple] = {
    "Ont": ("op-stat", "admin"),
    "EthSvc": ("admin", "out-tag", "in-tag", "us-pir-override", "ds-pir-override"),
    "OntEthGe": ("admin",),
    "OntProf": ("geth", "pots", "rg", "default-to-rg"),
}

# Every reply field the code knows by name: the ones it reads, plus the ones
# named in the requests and attr-lists this project sends (app/nbi/client.py). Anything else in a reply
# is reported as unrecognised.
KNOWN_FIELDS: Dict[str, Set[str]] = {
    "Ont": {"type", "id", "admin", "serno", "reg-id", "subscr-id", "descr", "ontprof", "op-stat", "model",
            "vendor", "linked-pon", "ontpwe3prof", "us-sdber-rate", "low-rx-opt-pwr-ne-thresh",
            "high-rx-opt-pwr-ne-thresh", "battery-present", "pse-max-power-budget", "poe-class-control",
            "crit", "maj", "min", "warn", "info", "derived-states", "prov-reg-id", "pon", "clei", "ont",
            "curr-sw-vers", "alt-sw-vers", "curr-committed", "mfg-serno", "product-code", "curr-cust-vers",
            "alt-cust-vers", "onu-mac", "mta-mac", "link-permit-status"},
    "EthSvc": {"type", "id", "admin", "descr", "tag-action", "bw-prof", "out-tag", "in-tag", "mcast-prof",
               "pon-cos", "us-cir-override", "us-pir-override", "ds-pir-override", "hot-swap",
               "pppoe-force-discard"},
    "OntEthGe": {"type", "id", "admin", "subscr-id", "descr", "sec", "accept-link-oam-loopbacks",
                 "dhcp-limit-override", "disable-on-batt", "duplex", "force-dot1x", "gos", "link-oam-events",
                 "pbit-map", "poe-class-control", "poe-power-priority", "policing", "ppte-power-control",
                 "role", "speed"},
    "OntProf": {"type", "id", "name", "vendor", "model", "pots", "feth", "geth", "hpnaeth", "ds1", "rfvid",
                "hotrfvid", "eth-oam-capable", "convert-mcast-capable", "rg", "fb", "default-to-rg"},
    "Vlan": {"type", "id", "name", "igmp-mode", "igmp-prof", "dhcp-mode", "mac-force-forw", "ip-src-verify",
             "mac-learn", "ae-ont-discovery", "pon-tlan", "pon-hairpin", "igmp-pbit", "dhcp-svc-prof",
             "option82-enable", "eth-opt82prof", "gpon-opt82prof", "mobility", "pppoe-prof"},
    "DiscOnt": {"type", "id", "model", "vendor", "pon", "clei", "mfg-serno", "product-code", "reg-id",
                "curr-sw-vers", "onu-mac"},
}


def _scalar(value: Any) -> Optional[str]:
    """A reply element is a dict if it has attributes or children, a str otherwise (client.to_data)"""
    if isinstance(value, dict):
        value = value.get("#text")
    if value is None:
        return None
    return str(value).strip() or None


def _dig(data: Any, *path: str) -> Optional[str]:
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return _scalar(data)


def _objects(data: Any, object_type: str) -> Iterator[Dict[str, Any]]:
    """
    Find every object of one type in a reply: a dict, a list, or nested lists
    depending on how many items CMS sent. This walks whatever it is given.
    """
    if isinstance(data, list):
        for item in data:
            yield from _objects(item, object_type)
    elif isinstance(data, dict):
        if data.get("type") == object_type and "id" in data:
            yield data
        else:
            for value in data.values():
                yield from _objects(value, object_type)


def _pon_port(reference: Any) -> Optional[str]:
    parts = [_dig(reference, "id", part) for part in ("shelf", "card", "gponport")]
    return "/".join(parts) if all(parts) else None


def _vlan(value: Any) -> Optional[int]:
    text = _scalar(value)
    return int(text) if text and text.isdigit() else None


def _number(value: Any) -> Optional[int]:
    text = _scalar(value)
    return int(text) if text and text.isdigit() else None


def _flag(value: Any) -> Optional[bool]:
    text = (_scalar(value) or "").lower()
    return {"true": True, "false": False}.get(text)


def _profile(obj: Dict[str, Any]) -> OntProfile:
    """OntProf. Attribute names from the attr-list sent; reading them as counts is inferred."""
    return OntProfile(
        id=_dig(obj, "id", "ontprof") or "", name=_scalar(obj.get("name")), model=_scalar(obj.get("model")),
        vendor=_scalar(obj.get("vendor")), ge_ports=_number(obj.get("geth")), fe_ports=_number(obj.get("feth")),
        pots_ports=_number(obj.get("pots")), rg_ports=_number(obj.get("rg")),
        default_to_rg=_flag(obj.get("default-to-rg")))


def _ge_port(obj: Dict[str, Any], number: int, router: Optional[RouterAddressing] = None) -> PortState:
    """
    OntEthGe. Element names from the write request; that a read uses them too
    is inferred. The mode comes from app/nbi/router_side.py and is
    "unknown" until that is configured.
    """
    return PortState(
        port=port_name(Attachment.ETHERNET, number), attachment=Attachment.ETHERNET, number=number,
        mode=read_port_mode(router, obj),
        admin_state=_scalar(obj.get("admin")), subscriber_id=_scalar(obj.get("subscr-id")),
        description=_scalar(obj.get("descr")), security_profile=_dig(obj, "sec", "id", "ethsecprof"))


def _ont_state(obj: Dict[str, Any], services: Optional[List[ProvisionedService]] = None) -> OntState:
    return OntState(
        ont_id=int(_dig(obj, "id", "ont")),
        serial_number=from_serno(_scalar(obj.get("serno")) or "0", _scalar(obj.get("vendor")) or ""),
        model=_scalar(obj.get("model")),
        profile=_dig(obj, "ontprof", "id", "ontprof"),
        admin_state=_scalar(obj.get("admin")) or "enabled",
        oper_state=OperState.ONLINE if _scalar(obj.get("op-stat")) == "enable" else OperState.OFFLINE,
        pon_port=_pon_port(obj.get("linked-pon")),
        description=_scalar(obj.get("descr")),
        subscriber_id=_scalar(obj.get("subscr-id")),
        registration_id=_scalar(obj.get("reg-id")),
        services=services or [])


def _attachment(slot: Optional[str], router: Optional[RouterAddressing]) -> Attachment:
    """
    Slot 3 (or none given) is a Gigabit Ethernet port. A slot equal to the
    configured router-side slot is the router. Anything else is reported as
    unknown rather than guessed. CMS NBI documentation lists 4 (HPNA
    Ethernet) and 5 (Fast Ethernet) for services on a port, 8 for the router
    (OntRg) and 9 for a full-bridge interface (OntFb), which carries services
    of its own: an 844G running as a bridge may hold its data service in slot
    9 rather than 3. Those read as unknown here.
    """
    if slot in (None, ONT_SLOT):
        return Attachment.ETHERNET
    if router is not None and router.service_slot and slot == router.service_slot:
        return Attachment.ROUTER
    return Attachment.UNKNOWN


def _service(obj: Dict[str, Any], router: Optional[RouterAddressing] = None) -> ProvisionedService:
    slot = _dig(obj, "id", "ontslot")
    return ProvisionedService(
        service_index=int(_dig(obj, "id", "ethsvc") or 1),
        ont_port=int(_dig(obj, "id", "ontethany") or 1),
        attachment=_attachment(slot, router), ont_slot=slot,
        vlan=_vlan(obj.get("out-tag")),
        inner_vlan=_vlan(obj.get("in-tag")),
        bandwidth_profile=_dig(obj, "bw-prof", "id", "bwprof"),
        tag_action=_dig(obj, "tag-action", "id", "svctagaction"),
        admin_state=_scalar(obj.get("admin")) or "enabled",
        description=_scalar(obj.get("descr")),
        # That a read answers with the request's element names and values ("100m", "none") is inferred
        us_pir_override=_scalar(obj.get("us-pir-override")), ds_pir_override=_scalar(obj.get("ds-pir-override")))


class CmsBackend:
    name = "cms-nbi"

    def __init__(self, host: str, port: int, username: str, password: str, timeout: float = 10.0,
                 router: Optional[RouterAddressing] = None, scheme: str = "http", verify: Union[bool, str] = True):
        self.client = CmsClient(host, port, username, password, scheme=scheme, verify=verify, timeout=timeout)
        self._router = router  # None: the router side is not verified, see app/nbi/router_side.py
        self.unrecognised: Dict[str, Set[str]] = defaultdict(set)  # reply fields nobody here knows
        self.slots_seen: Set[str] = set()  # every EthSvc ontslot value read, for cms_check
        self.observed: Dict[str, Set[str]] = defaultdict(set)  # "Ont.op-stat" -> raw values seen

    # --- NbiBackend operations --------------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        found = await self.client.children(network, SYSTEM, "DiscOnt", verb="get")
        return [DiscoveredOnt(
            serial_number=from_serno(_dig(obj, "id", "discont") or "0", _scalar(obj.get("vendor")) or ""),
            model=_scalar(obj.get("model")) or "unknown",
            pon_port=_pon_port(obj.get("pon")) or "unknown") for obj in self._audit(found, "DiscOnt")]

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        matches = await self._show_ont(network, {"ont": str(ont_id)})
        if not matches:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        return _ont_state(matches[0], await self._services(network, ont_id))

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        matches = await self._show_ont(network, {"serno": to_serno(serial_number)})
        if not matches:
            return None
        ont_id = int(_dig(matches[0], "id", "ont"))
        return _ont_state(matches[0], await self._services(network, ont_id))

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        return [_ont_state(obj) for obj in await self._show_ont(network, {"subscr-id": subscriber_id})]

    async def list_onts(self, network: str) -> List[OntState]:
        return [_ont_state(obj) for obj in await self._show_ont(network, {})]  # every page

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        profile = f"<ontprof><type>OntProf</type><id>{element('ontprof', spec.profile)}</id></ontprof>"
        created = await self.client.edit(network, "create", (
            f"<type>Ont</type><id>{element('ont', spec.ont_id or 0)}</id>"  # 0 asks CMS for the first free ID
            f"<admin>enabled</admin>{element('serno', to_serno(spec.serial_number))}<reg-id></reg-id>"
            f"{element('subscr-id', spec.subscriber_id or '')}{element('descr', spec.description or '')}"
            + ONT_DEFAULTS.format(profile=profile)), get_config=True)
        created = next(_objects(created, "Ont"), None)
        if created is None:
            raise NbiError("CMS accepted the request but did not return the new ONT.")
        return _ont_state(created)

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        if not service.tag_action:
            raise NbiRejectedError(
                f"Service on VLAN {service.vlan} has no tag_action. CMS needs a service tag action "
                f"for every Ethernet service.")
        # <out-tag> only when there is a VLAN: IDI's CMS R15.x docs say to leave the outer VLAN out when the
        # tag action does not need it. Which tag actions need one is set on CMS, not known here.
        out_tag = element("out-tag", service.vlan) if service.vlan is not None else ""
        attachment = Attachment(service.attach_to)
        slot = self._slot(attachment)
        used = {s.service_index for s in await self._services(network, ont_id)
                if (s.attachment, s.ont_port) == (attachment, service.ont_port)}
        index = next(n for n in range(1, len(used) + 2) if n not in used)
        await self.client.edit(network, "create", (
            f"{_service_id(ont_id, slot, service.ont_port, index)}<admin>enabled</admin>"
            f"{element('descr', service.description or '')}"
            f"<tag-action><type>SvcTagAction</type><id>{element('svctagaction', service.tag_action)}</id>"
            f"</tag-action><bw-prof><type>BwProf</type><id>{element('bwprof', service.bandwidth_profile)}</id>"
            f"</bw-prof>{out_tag}" + SERVICE_DEFAULTS.format(
                us_pir=element("us-pir-override", service.us_pir_override or "none"),
                ds_pir=element("ds-pir-override", service.ds_pir_override or "none"))), get_config=True)

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        body = _service_id(ont_id, self._slot(attachment), ont_port, service_index)
        if change.admin_state:
            body += element("admin", change.admin_state)
        if change.tag_action:
            body += (f"<tag-action><type>SvcTagAction</type><id>{element('svctagaction', change.tag_action)}"
                     f"</id></tag-action>")
        if change.bandwidth_profile:
            body += f"<bw-prof><type>BwProf</type><id>{element('bwprof', change.bandwidth_profile)}</id></bw-prof>"
        if change.vlan is not None:
            body += element("out-tag", change.vlan)
        if change.us_pir_override:
            body += element("us-pir-override", change.us_pir_override)
        if change.ds_pir_override:
            body += element("ds-pir-override", change.ds_pir_override)
        await self.client.edit(network, "merge", body)

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        await self.client.edit(network, "delete", _service_id(ont_id, self._slot(attachment), ont_port, service_index))

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        matches = await self._show_ont(network, {"ont": str(ont_id)})
        if not matches:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        profile_id = _dig(matches[0], "ontprof", "id", "ontprof")
        profile = await self._ont_profile(network, profile_id) if profile_id else None
        services = await self._services(network, ont_id)

        # Read as many Ethernet ports as the profile says it has; without a
        # profile, read upwards until CMS says the port does not exist.
        count = profile.ge_ports if profile and profile.ge_ports is not None else 8
        ethernet = []
        for number in range(1, count + 1):
            try:
                obj = await self.client.get(network, f"<type>OntEthGe</type><id>{element('ont', ont_id)}"
                                                     f"<ontslot>{ONT_SLOT}</ontslot>{element('ontethge', number)}</id>")
            except NbiNotFoundError:
                if profile is None or profile.ge_ports is None:
                    break
                raise
            found = self._audit(obj, "OntEthGe")
            ethernet.append(_ge_port(found[0] if found else {}, number, self._router))

        present = None if profile is None or profile.rg_ports is None else profile.rg_ports > 0
        router = RouterState(present=present, default_to_rg=profile.default_to_rg if profile else None,
                             services=sum(1 for s in services if s.attachment == Attachment.ROUTER))
        unknown = [s for s in services if s.attachment == Attachment.UNKNOWN]
        if present and (self._router is None or not self._router.router_ports):
            router.note = "Router ports not read: " + NOT_CONFIGURED
        elif present:
            for number in range(1, profile.rg_ports + 1):
                obj = await self._read_object(network, self._router_port_id(ont_id, number))
                router.ports.append(PortState(port=port_name(Attachment.ROUTER, number),
                                              attachment=Attachment.ROUTER, number=number,
                                              admin_state=_scalar(obj.get("admin")) if obj else None))
        if unknown:
            slots = sorted({s.ont_slot or "?" for s in unknown})
            router.note = ((router.note + " ") if router.note else "") + (
                f"{len(unknown)} service(s) are in ONT slot(s) {', '.join(slots)}, which this code does not "
                f"recognise. They may be on the router side; nothing is assumed.")
        return OntPorts(network=network, ont_id=ont_id, profile=profile_id, ethernet=ethernet, router=router,
                        voice_ports=profile.pots_ports if profile else None)

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        values = change.model_dump(exclude_none=True)
        if "mode" in values:
            raise NbiNotVerifiedError(MODE_NOT_SETTABLE)
        if Attachment(attachment) == Attachment.ROUTER:
            if set(values) - {"admin_state"}:
                raise NbiRejectedError("Only the admin state of a router port can be changed.")
            body = f"{self._router_port_id(ont_id, number)}{element('admin', values['admin_state'])}"
        else:
            # The XML E7.Update.ont_geth() was meant to send (it raised KeyError instead)
            body = (f"<type>OntEthGe</type><id>{element('ont', ont_id)}<ontslot>{ONT_SLOT}</ontslot>"
                    f"{element('ontethge', number)}</id>")
            if "admin_state" in values:
                body += element("admin", values["admin_state"])
            if "description" in values:
                body += element("descr", values["description"])
            if "security_profile" in values:
                body += (f"<sec><id>{element('ethsecprof', values['security_profile'])}</id>"
                         f"<type>EthSecProf</type></sec>")
            if "subscriber_id" in values:
                body += element("subscr-id", values["subscriber_id"])
        await self.client.edit(network, "merge", body)

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        found = await self.client.children(network, SYSTEM, "OntProf")
        return [_profile(obj) for obj in self._audit(found, "OntProf")]

    async def list_vlans(self, network: str) -> List[Vlan]:
        found = await self.client.children(network, SYSTEM, "Vlan")
        return [Vlan(id=int(_dig(obj, "id", "vlan")), name=_scalar(obj.get("name")))
                for obj in self._audit(found, "Vlan") if (_dig(obj, "id", "vlan") or "").isdigit()]

    async def delete_ont(self, network: str, ont_id: int) -> None:
        await self.client.edit(network, "delete", f"<type>Ont</type><id>{element('ont', ont_id)}</id>", force=True)

    async def close(self) -> None:
        await self.client.close()

    # --- Reads -------------------------------------------------------------------

    async def _show_ont(self, network: str, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        return self._audit(await self.client.show_ont(network, filters), "Ont")

    async def _services(self, network: str, ont_id: int) -> List[ProvisionedService]:
        found = await self.client.children(network, f"<type>Ont</type><id>{element('ont', ont_id)}</id>", "EthSvc")
        services = [_service(obj, self._router) for obj in self._audit(found, "EthSvc")]
        self.slots_seen.update(s.ont_slot or "(none)" for s in services)
        return services

    async def _ont_profile(self, network: str, profile_id: str) -> Optional[OntProfile]:
        try:
            obj = await self.client.get(network, f"<type>OntProf</type><id>{element('ontprof', profile_id)}</id>")
        except NbiNotFoundError:
            return None
        found = self._audit(obj, "OntProf")
        return _profile(found[0]) if found else None

    def _audit(self, result: Any, object_type: str) -> List[Dict[str, Any]]:
        """The objects of one type in a result, noting any field nobody here knows"""
        found = list(_objects(result, object_type))
        known = KNOWN_FIELDS.get(object_type, set())
        if self._router is not None and self._router.port_type and object_type == self._router.port_type:
            known = {"type", "id", "admin"}
        mode_element = self._router.port_mode_element if self._router else None
        if object_type == "OntEthGe" and mode_element:
            known = known | {mode_element}
        for obj in found:
            self.unrecognised[object_type].update(key for key in obj if key not in known and not key.startswith("@"))
            for field in OBSERVED_FIELDS.get(object_type, ()) + (
                    (mode_element,) if object_type == "OntEthGe" and mode_element else ()):
                self.observed[f"{object_type}.{field}"].add(_scalar(obj.get(field)) or "(empty)")
        return found

    # --- Router side -------------------------------------------------------------

    def _slot(self, attachment: Attachment) -> str:
        attachment = Attachment(attachment)
        if attachment == Attachment.ETHERNET:
            return ONT_SLOT
        if attachment == Attachment.ROUTER:
            if self._router is None or not self._router.service_slot:
                raise NbiNotVerifiedError(NOT_CONFIGURED)
            return self._router.service_slot
        raise NbiRejectedError("A service in an unrecognised ONT slot cannot be addressed.")

    def _router_port_id(self, ont_id: int, number: int) -> str:
        if self._router is None or not self._router.router_ports:
            raise NbiNotVerifiedError(NOT_CONFIGURED)
        name = self._router.port_id_element
        return f"{element('type', self._router.port_type)}<id>{element('ont', ont_id)}{element(name, number)}</id>"

    async def _read_object(self, network: str, object_xml: str) -> Optional[Dict[str, Any]]:
        obj = await self.client.get(network, object_xml)
        if obj is not None:
            self._audit(obj, _scalar(obj.get("type")) or "")
        return obj


def _service_id(ont_id: int, slot: str, ont_port: int, service_index: int) -> str:
    return (f"<type>EthSvc</type><id>{element('ont', ont_id)}{element('ontslot', slot)}"
            f"{element('ontethany', ont_port)}{element('ethsvc', service_index)}</id>")
