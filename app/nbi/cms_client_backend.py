"""
CMS Backend (cms-nbi-client)
Talks to a real Calix CMS through the cms-nbi-client library, which builds
the SOAP/XML requests. Used when CMS_HOST is set.

This is version 0.1.0 of the library, the only one that installs (2.0.0 does
not: see docs/ARCHITECTURE.md). What that means here:

  - It is synchronous, so every call runs in a worker thread.
  - It has no retries, pooling or circuit breaker. The NBI gateway in front of
    this backend provides the timeout, retries, breaker and concurrency cap.
  - It only speaks plain HTTP (port 18080). The CMS login and every request
    cross the network unencrypted. Keep this traffic on a trusted network.
  - It puts values into XML without escaping them, so this module escapes
    every value first.
  - On failure it returns the raw HTTP response instead of raising. This
    module turns those into the NbiError types the rest of the app uses.

  - Two of its update calls are broken, so this module sends those requests
    itself, in the XML shape the library would have produced (_rpc() below),
    using the library's session:
      E7.Update.ont_geth() (Ethernet ports) raises KeyError on every call: it
        looks up 'subscr-id' where its parameter is 'subscr_id'.
      E7.Update.ont_ethsvc() (services) silently drops the tag action and
        bandwidth profile: it calls pydash.get(path, obj) with the arguments
        swapped, so nested fields always look empty and are removed.

NOT VERIFIED AGAINST A REAL CMS. It is tested against the mock NBI server
(app/nbi/mock_cms_server.py), whose replies are modelled on what the library
parses. Anything the library does not parse is a guess: the op-stat values,
the linked-pon element, the rpc-error tags used to tell "not found" from
"already exists", the element names in an Ethernet port (OntEthGe) reply,
and the meaning of the OntProf values (port counts, "true"/"false").

The router side (RG) of ONTs such as the 844G is not known at all. Every
router-side request goes through app/nbi/router_side.py and is refused unless
an operator has configured it.

Every reply field this module does not recognise is recorded in
`unrecognised` (type -> field names), which python -m app.tools.cms_check
prints.
"""

import asyncio
import logging
import os
import random
import tempfile
from collections import defaultdict
from typing import Any, Callable, Dict, Iterator, List, Optional, Set
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, EthernetService, OntPorts, OntProfile, OntSpec, OntState, OperState,
    PortChange, PortState, ProvisionedService, RouterState, ServiceChange, Vlan, port_name
)
from app.nbi.errors import (
    NbiAuthError, NbiConflictError, NbiConnectionError, NbiError, NbiNotFoundError,
    NbiNotVerifiedError, NbiRejectedError, NbiTimeoutError
)
from app.nbi.router_side import MODE_NOT_SETTABLE, NOT_CONFIGURED, RouterAddressing, read_port_mode
from app.nbi.serno import from_serno, to_serno

logger = logging.getLogger(__name__)

ONT_SLOT = "3"  # Gigabit Ethernet, per the library's docstrings (4 is HPNA, 5 is Fast Ethernet)
SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"

# Reply values whose meaning is inferred, not taken from the library. Their raw
# values are collected in `observed` so a check against a real CMS shows them.
OBSERVED_FIELDS: Dict[str, tuple] = {
    "Ont": ("op-stat", "admin"),
    "EthSvc": ("admin", "out-tag", "in-tag"),
    "OntEthGe": ("admin",),
    "OntProf": ("geth", "rg", "default-to-rg"),
}

# Every reply field the code knows by name: the ones it reads, plus the ones the
# library itself names in its requests and attr-lists. Anything else in a reply
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


class _SessionExpired(Exception):
    pass


def _attr(value: str) -> str:
    """Escape a value that the library places inside a double-quoted XML attribute"""
    return escape(value, {'"': "&quot;"})


def _scalar(value: Any) -> Optional[str]:
    """xmltodict gives a dict for an element with attributes, a str otherwise"""
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
    Find every object of one type in a library result. The library returns a
    dict, a list or nested lists depending on how many items CMS sent, so this
    walks whatever it is given.
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
    """OntProf. Attribute names from the library's attr-list; reading them as counts is inferred."""
    return OntProfile(
        id=_dig(obj, "id", "ontprof") or "", name=_scalar(obj.get("name")), model=_scalar(obj.get("model")),
        vendor=_scalar(obj.get("vendor")), ge_ports=_number(obj.get("geth")), fe_ports=_number(obj.get("feth")),
        pots_ports=_number(obj.get("pots")), rg_ports=_number(obj.get("rg")),
        default_to_rg=_flag(obj.get("default-to-rg")))


def _ge_port(obj: Dict[str, Any], number: int, router: Optional[RouterAddressing] = None) -> PortState:
    """
    OntEthGe. Element names from the library's write request; that a read uses
    them too is inferred. The mode comes from app/nbi/router_side.py and is
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
    configured router-side slot is the router. Anything else, including the
    library's 4 (HPNA) and 5 (Fast Ethernet), is reported as unknown rather
    than guessed.
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
        description=_scalar(obj.get("descr")))


def _raise_rpc_error(text: str) -> None:
    """Raise the matching NbiError if a CMS reply carries an <rpc-error>"""
    if "rpc-error" not in text:
        return
    try:
        root = ElementTree.fromstring(text)
    except ElementTree.ParseError:
        raise NbiRejectedError("CMS returned an error that could not be read.") from None
    tag = message = ""
    for element in root.iter():
        name = element.tag.split("}", 1)[-1]
        if name == "error-tag":
            tag = (element.text or "").strip()
        elif name == "error-message":
            message = (element.text or "").strip()
    message = message or f"CMS refused the request ({tag or 'no reason given'})."
    if tag == "access-denied":
        raise _SessionExpired(message)
    if tag == "data-missing":
        raise NbiNotFoundError(message)
    if tag in ("data-exists", "in-use"):
        raise NbiConflictError(message)
    raise NbiRejectedError(message)


class CmsNbiClientBackend:
    name = "cms-nbi-client"

    def __init__(self, host: str, port: int, username: str, password: str, timeout: float = 10.0,
                 router: Optional[RouterAddressing] = None):
        try:
            import cmsnbiclient
            import requests
            import xmltodict
        except ImportError:
            raise RuntimeError(
                "CMS_HOST is set but cms-nbi-client is not installed. "
                "Install requirements-cms.txt.") from None
        self._lib = cmsnbiclient
        self._requests = requests
        self._xmltodict = xmltodict
        self._router = router  # None: the router side is not verified, see app/nbi/router_side.py
        self.unrecognised: Dict[str, Set[str]] = defaultdict(set)  # reply fields nobody here knows
        self.slots_seen: Set[str] = set()  # every EthSvc ontslot value read, for cms_check
        self.observed: Dict[str, Set[str]] = defaultdict(set)  # "Ont.op-stat" -> raw values seen
        self._host, self._port = host, str(port)
        self._username, self._password = username, password
        self._timeout = timeout
        self._logged_in = False
        self._login_lock = asyncio.Lock()

        # Client() reads cms_nbi_config.json from the working directory and writes
        # a default one if it is missing. Give it a scratch directory for that.
        previous = os.getcwd()
        with tempfile.TemporaryDirectory(prefix="cms-nbi-") as scratch:
            os.chdir(scratch)
            try:
                self._client = cmsnbiclient.Client()
            finally:
                os.chdir(previous)
        self._uri = self._client.cms_nbi_config["cms_netconf_uri"]["e7"]

    # --- NbiBackend operations --------------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        def work() -> List[DiscoveredOnt]:
            result = self._check(self._query(network).system_children_discont())
            return [DiscoveredOnt(
                serial_number=from_serno(_dig(obj, "id", "discont") or "0", _scalar(obj.get("vendor")) or ""),
                model=_scalar(obj.get("model")) or "unknown",
                pon_port=_pon_port(obj.get("pon")) or "unknown") for obj in self._audit(result, "DiscOnt")]
        return await self._run(work)

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        def work() -> OntState:
            matches = self._show_ont(network, {"ont": str(ont_id)})
            if not matches:
                raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
            return _ont_state(matches[0], self._services(network, ont_id))
        return await self._run(work)

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        def work() -> Optional[OntState]:
            matches = self._show_ont(network, {"serno": to_serno(serial_number)})
            if not matches:
                return None
            ont_id = int(_dig(matches[0], "id", "ont"))
            return _ont_state(matches[0], self._services(network, ont_id))
        return await self._run(work)

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        def work() -> List[OntState]:
            return [_ont_state(obj) for obj in self._show_ont(network, {"subscr-id": escape(subscriber_id)})]
        return await self._run(work)

    async def list_onts(self, network: str) -> List[OntState]:
        def work() -> List[OntState]:
            result = self._check(self._query(network).show_ont())  # the library pages through them all
            return [_ont_state(obj) for obj in self._audit(result, "Ont")]
        return await self._run(work)

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        def work() -> OntState:
            result = self._check(self._lib.E7.Create(self._client, _attr(network), self._timeout).ont(
                ont_id=str(spec.ont_id or 0),  # 0 asks CMS for the first free ID
                admin_state="enabled",
                ont_sn=to_serno(spec.serial_number),
                sub_id=escape(spec.subscriber_id or ""),
                ont_desc=escape(spec.description or ""),
                ontprof_id=escape(spec.profile)))
            created = next(_objects(result, "Ont"), None)
            if created is None:
                raise NbiError("CMS accepted the request but did not return the new ONT.")
            return _ont_state(created)
        return await self._run(work)

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        if not service.tag_action:
            raise NbiRejectedError(
                f"Service on VLAN {service.vlan} has no tag_action. CMS needs a service tag action "
                f"for every Ethernet service.")
        attachment = Attachment(service.attach_to)
        slot = self._slot(attachment)

        def work() -> None:
            used = {s.service_index for s in self._services(network, ont_id)
                    if (s.attachment, s.ont_port) == (attachment, service.ont_port)}
            index = next(n for n in range(1, len(used) + 2) if n not in used)
            self._check(self._lib.E7.Create(self._client, _attr(network), self._timeout).ont_ethsvc(
                ont_id=str(ont_id), ontslot=slot, ontethany=str(service.ont_port), ethsvc=str(index),
                admin_state="enabled",
                descr=escape(service.description or ""),
                svctagaction_id=escape(service.tag_action),
                bwprof_id=escape(service.bandwidth_profile),
                out_tag_id=str(service.vlan)))
        await self._run(work)

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        slot = self._slot(attachment)
        # The XML E7.Update.ont_ethsvc() would send if it worked: it silently drops
        # tag-action and bw-prof (pydash.get called with its arguments swapped).
        body = (f"<type>EthSvc</type><id><ont>{ont_id}</ont><ontslot>{escape(slot)}</ontslot>"
                f"<ontethany>{ont_port}</ontethany><ethsvc>{service_index}</ethsvc></id>")
        if change.admin_state:
            body += f"<admin>{escape(change.admin_state)}</admin>"
        if change.tag_action:
            body += (f"<tag-action><type>SvcTagAction</type><id><svctagaction>{escape(change.tag_action)}"
                     f"</svctagaction></id></tag-action>")
        if change.bandwidth_profile:
            body += (f"<bw-prof><type>BwProf</type><id><bwprof>{escape(change.bandwidth_profile)}</bwprof>"
                     f"</id></bw-prof>")
        if change.vlan is not None:
            body += f"<out-tag>{change.vlan}</out-tag>"
        await self._edit(network, body)

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        slot = self._slot(attachment)

        def work() -> None:
            self._check(self._lib.E7.Delete(self._client, _attr(network), self._timeout).ont_ethsvc(
                ont_id=str(ont_id), ontslot=slot, ontethany=str(ont_port), ethsvc=str(service_index)))
        await self._run(work)

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        def work() -> OntPorts:
            matches = self._show_ont(network, {"ont": str(ont_id)})
            if not matches:
                raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
            profile_id = _dig(matches[0], "ontprof", "id", "ontprof")
            profile = self._ont_profile(network, profile_id) if profile_id else None
            services = self._services(network, ont_id)

            # Read as many Ethernet ports as the profile says it has; without a
            # profile, read upwards until CMS says the port does not exist.
            count = profile.ge_ports if profile and profile.ge_ports is not None else 8
            ethernet = []
            for number in range(1, count + 1):
                try:
                    result = self._check(self._query(network).ont_geth(ont_id=str(ont_id), ontethge=str(number)))
                except NbiNotFoundError:
                    if profile is None or profile.ge_ports is None:
                        break
                    raise
                found = self._audit(result, "OntEthGe")
                ethernet.append(_ge_port(found[0] if found else {}, number, self._router))

            present = None if profile is None or profile.rg_ports is None else profile.rg_ports > 0
            router = RouterState(present=present, default_to_rg=profile.default_to_rg if profile else None,
                                 services=sum(1 for s in services if s.attachment == Attachment.ROUTER))
            unknown = [s for s in services if s.attachment == Attachment.UNKNOWN]
            if present and (self._router is None or not self._router.router_ports):
                router.note = "Router ports not read: " + NOT_CONFIGURED
            elif present:
                for number in range(1, profile.rg_ports + 1):
                    obj = self._read_object(network, self._router_port_id(ont_id, number))
                    router.ports.append(PortState(port=port_name(Attachment.ROUTER, number),
                                                  attachment=Attachment.ROUTER, number=number,
                                                  admin_state=_scalar(obj.get("admin")) if obj else None))
            if unknown:
                slots = sorted({s.ont_slot or "?" for s in unknown})
                router.note = ((router.note + " ") if router.note else "") + (
                    f"{len(unknown)} service(s) are in ONT slot(s) {', '.join(slots)}, which this code does not "
                    f"recognise. They may be on the router side; nothing is assumed.")
            return OntPorts(network=network, ont_id=ont_id, profile=profile_id, ethernet=ethernet, router=router)
        return await self._run(work)

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        values = change.model_dump(exclude_none=True)
        if "mode" in values:
            raise NbiNotVerifiedError(MODE_NOT_SETTABLE)
        if Attachment(attachment) == Attachment.ROUTER:
            if set(values) - {"admin_state"}:
                raise NbiRejectedError("Only the admin state of a router port can be changed.")
            body = f"{self._router_port_id(ont_id, number)}<admin>{escape(values['admin_state'])}</admin>"
        else:
            # The XML E7.Update.ont_geth() would send, if it did not raise KeyError
            body = (f"<type>OntEthGe</type><id><ont>{ont_id}</ont><ontslot>{ONT_SLOT}</ontslot>"
                    f"<ontethge>{number}</ontethge></id>")
            if "admin_state" in values:
                body += f"<admin>{escape(values['admin_state'])}</admin>"
            if "description" in values:
                body += f"<descr>{escape(values['description'])}</descr>"
            if "security_profile" in values:
                body += (f"<sec><id><ethsecprof>{escape(values['security_profile'])}</ethsecprof></id>"
                         f"<type>EthSecProf</type></sec>")
            if "subscriber_id" in values:
                body += f"<subscr-id>{escape(values['subscriber_id'])}</subscr-id>"

        await self._edit(network, body)

    async def _edit(self, network: str, object_body: str) -> None:
        def work() -> None:
            self._rpc(network, "<edit-config><target><running/></target><config><top>"
                               f'<object operation="merge">{object_body}</object></top></config></edit-config>')
        await self._run(work)

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        def work() -> List[OntProfile]:
            result = self._check(self._query(network).system_children_ontprof())
            return [_profile(obj) for obj in self._audit(result, "OntProf")]
        return await self._run(work)

    async def list_vlans(self, network: str) -> List[Vlan]:
        def work() -> List[Vlan]:
            result = self._check(self._query(network).system_children_vlan())
            return [Vlan(id=int(_dig(obj, "id", "vlan")), name=_scalar(obj.get("name")))
                    for obj in self._audit(result, "Vlan") if (_dig(obj, "id", "vlan") or "").isdigit()]
        return await self._run(work)

    async def delete_ont(self, network: str, ont_id: int) -> None:
        def work() -> None:
            self._check(self._lib.E7.Delete(self._client, _attr(network), self._timeout).ont(
                ont_id=str(ont_id), force="true"))
        await self._run(work)

    async def close(self) -> None:
        if not self._logged_in:
            return
        try:
            await asyncio.to_thread(
                self._client.logout_netconf, protocol="http", port=self._port,
                cms_node_ip=self._host, uri=self._uri, http_timeout=self._timeout)
        except Exception as e:
            logger.warning(f"CMS logout failed: {e}")
        self._logged_in = False

    # --- Session and error handling ---------------------------------------------

    async def _run(self, work: Callable[[], Any]) -> Any:
        """Run blocking library calls in a thread, logging in first and once more if the session expired"""
        for attempt in (1, 2):
            await self._ensure_session()
            try:
                return await asyncio.to_thread(self._translate, work)
            except _SessionExpired:
                self._logged_in = False
                if attempt == 2:
                    raise NbiAuthError("CMS keeps rejecting the session straight after login.") from None
                logger.info("CMS session expired; logging in again")

    async def _ensure_session(self) -> None:
        async with self._login_lock:
            if not self._logged_in:
                await asyncio.to_thread(self._translate, self._login)
                self._logged_in = True

    def _login(self) -> None:
        result = self._client.login_netconf(
            cms_user_nm=self._username, cms_user_pass=self._password, protocol="http",
            port=self._port, cms_node_ip=self._host, uri=self._uri, http_timeout=self._timeout)
        if result == "0":
            logger.info(f"Logged in to CMS at {self._host}:{self._port}")
            return
        response = result[1] if isinstance(result, tuple) else result
        if getattr(response, "status_code", 200) != 200:
            raise NbiConnectionError(f"CMS answered the login with HTTP {response.status_code}.")
        raise NbiAuthError("CMS rejected this API's username or password.")

    def _translate(self, work: Callable[[], Any]) -> Any:
        """Turn the HTTP library's exceptions into NbiErrors"""
        try:
            return work()
        except self._requests.exceptions.Timeout:
            raise NbiTimeoutError(f"CMS did not answer within {self._timeout:g} seconds.") from None
        except self._requests.exceptions.RequestException as e:
            raise NbiConnectionError(f"Could not connect to CMS at {self._host}:{self._port} "
                                     f"({type(e).__name__}).") from None

    def _check(self, result: Any) -> Any:
        """
        The library returns parsed data on success and the raw HTTP response
        otherwise. A raw response can be an error, or just an empty answer.
        Returns the data, or None for an empty answer.
        """
        if not isinstance(result, self._requests.Response):
            return result
        if result.status_code >= 500:
            raise NbiConnectionError(f"CMS answered with HTTP {result.status_code}.")
        if result.status_code != 200:
            raise NbiRejectedError(f"CMS refused the request with HTTP {result.status_code}.")
        _raise_rpc_error(result.text)
        return None

    def _query(self, network: str) -> Any:
        # A new object per call: the library keeps paging state on the instance
        return self._lib.E7.Query(self._client, _attr(network), self._timeout)

    def _show_ont(self, network: str, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        return list(self._audit(self._check(self._query(network).show_ont(action_args=filters)), "Ont"))

    def _services(self, network: str, ont_id: int) -> List[ProvisionedService]:
        result = self._check(self._query(network).ont_children_ethsvc(ont_id=str(ont_id)))
        services = [_service(obj, self._router) for obj in self._audit(result, "EthSvc")]
        self.slots_seen.update(s.ont_slot or "(none)" for s in services)
        return services

    def _ont_profile(self, network: str, profile_id: str) -> Optional[OntProfile]:
        try:
            result = self._check(self._query(network).ontprof(ontprof_id=escape(profile_id)))
        except NbiNotFoundError:
            return None
        found = list(self._audit(result, "OntProf"))
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

    # --- Router side and raw requests ------------------------------------------

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
        element = self._router.port_id_element
        return f"<type>{escape(self._router.port_type)}</type><id><ont>{ont_id}</ont><{element}>{number}</{element}></id>"

    def _read_object(self, network: str, object_xml: str) -> Optional[Dict[str, Any]]:
        obj = self._rpc(network, '<get-config><source><running/></source><filter type="subtree"><top>'
                                 f"<object>{object_xml}</object></top></filter></get-config>")
        if obj is not None:
            self._audit(obj, _scalar(obj.get("type")) or "")
        return obj

    def _rpc(self, network: str, body: str) -> Optional[Dict[str, Any]]:
        """
        Send one <rpc> the way the library does (same envelope, headers and
        session), for requests the library cannot make. Returns the reply's
        data/top/object, or None for a reply without one (such as <ok/>).
        """
        client = self._client
        payload = (f'<soapenv:Envelope xmlns:soapenv="{SOAP_NS}"><soapenv:Body>'
                   f'<rpc message-id="{random.getrandbits(31)}" nodename="{_attr(network)}" '
                   f'username="{_attr(client.cms_user_nm)}" sessionid="{client.session_id}">{body}</rpc>'
                   f"</soapenv:Body></soapenv:Envelope>")
        response = self._requests.post(
            url=client.cms_netconf_url, data=payload, timeout=self._timeout,
            headers={"Content-Type": "text/xml;charset=ISO-8859-1",
                     "User-Agent": f"CMS_NBI_CONNECT-{client.cms_user_nm}"})
        if response.status_code != 200:
            return self._check(response)
        _raise_rpc_error(response.text)
        reply = self._xmltodict.parse(response.content)
        for key in ("soapenv:Envelope", "soapenv:Body", "rpc-reply", "data", "top", "object"):
            if not isinstance(reply, dict) or key not in reply:
                return None
            reply = reply[key]
        return reply
