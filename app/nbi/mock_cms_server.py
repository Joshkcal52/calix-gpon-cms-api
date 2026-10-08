"""
Mock CMS NBI Server
A stand-in for the Calix CMS northbound interface that speaks SOAP over HTTP,
so the real client path (app/nbi/client.py and app/nbi/cms_backend.py) can be
exercised without a CMS.

    uvicorn app.nbi.mock_cms_server:app --port 18080

It listens where CMS does (POST /cmsexc/ex/netconf) and keeps its data in the
simulated CMS from app/nbi/mock_cms.py: one network, NTWK-MOCK-E7.

WHERE THE XML SHAPES COME FROM: every request shape, and every part of a
reply that the cms-nbi-client library (0.1.0) reads, was taken from that
library, which this project used to depend on. Fields the library never
looked at (op-stat, linked-pon, model, the error tags) are educated guesses.
Nothing here has been compared with a real CMS.

  - show-ont answers each ONT as two objects in one <match>, as CMS NBI
    documentation describes: the provisioning (admin, ontprof,
    serno, subscr-id, descr, linked-pon) under <get-config>, the state
    (op-stat, alarm counts, model, vendor) under <get>. Create, merge and
    get-config replies carry only the provisioning. Serial numbers are
    lowercase hexadecimal.

  - OntEthGe (Ethernet port) replies reuse the element names of the library's
    write request (admin, subscr-id, descr, sec/EthSecProf). That a read
    answers with the same names is inferred.
  - OntProf and Vlan replies use the attribute names the library asks for in
    its attr-list. Their values (port counts, "true"/"false") are inferred.
  - The router side (RG), and each Ethernet port's bridge/router mode, use the
    deliberately fake names in app/nbi/router_side.py:MOCK_RG_ADDRESSING.
    Calix's are not known.
  - A service without an outer VLAN is created with no <out-tag> element and
    read back as <out-tag>none</out-tag>, like <in-tag>. That a real CMS
    answers "none" there is inferred. Which tag actions need an outer VLAN is
    the simulation's own rule (MOCK_TAG_ACTIONS_NO_VLAN in app/nbi/mock_cms.py).
  - Paging is off unless MOCK_CMS_PAGE_SIZE is set. Then show-ont and every
    list of children answer that many items at a time, end with <more/> while
    there are more, and start after the item named by an <after> element. The
    shape is the one the library expected; a real CMS has not been seen paging.

Helper endpoints for demos and tests (not part of CMS):
    GET  /mock/state                  every ONT, as JSON
    GET  /mock/requests               what each NBI request was (auth, get, action, edit), in order
    POST /mock/damage?pon=1/1/1       simulate a bad change on one PON (pon=all for every PON)
    POST /mock/outage?mode=timeout    timeout | refused | none
    POST /mock/reset                  back to the starting data
"""

import logging
import os
import secrets
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree
from xml.sax.saxutils import escape, quoteattr

from fastapi import FastAPI, Request, Response

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, OntProfile, OntSpec, OntState, OperState, PortChange, PortState, ProvisionedService
)
from app.nbi.errors import (
    NbiConflictError, NbiConnectionError, NbiError, NbiNotFoundError, NbiRejectedError
)
from app.nbi.mock_cms import MockCmsBackend
from app.nbi.router_side import MOCK_RG_ADDRESSING as RG
from app.nbi.serno import DEFAULT_VENDOR as VENDOR, from_serno, to_serno

logger = logging.getLogger(__name__)

NBI_PATH = "/cmsexc/ex/netconf"
SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"
ONT_SLOT = "3"  # the ONT slot cms-nbi-client uses for Ethernet ports

app = FastAPI(title="Mock Calix CMS NBI", docs_url=None, redoc_url=None)


def _fresh_state() -> None:
    app.state.cms = MockCmsBackend(
        latency=float(os.getenv("MOCK_CMS_LATENCY", "0.02")),
        ranging_seconds=float(os.getenv("MOCK_CMS_RANGING_SECONDS", "1.5")),
        extra_onts=int(os.getenv("MOCK_CMS_EXTRA_ONTS", "0")),
        pon_size=int(os.getenv("MOCK_PON_SIZE", "0")))
    page_size = int(os.getenv("MOCK_CMS_PAGE_SIZE", "0"))
    app.state.page_size = page_size if page_size > 0 else None  # None: never page
    app.state.sessions = set()
    app.state.requests = []  # one entry per NBI request: what kind it was, for tests and the read-only check


_fresh_state()


def _username() -> str:
    return os.getenv("MOCK_CMS_USER", "nbi-demo")


def _password() -> str:
    return os.getenv("MOCK_CMS_PASSWORD", "demo-password")


# --- XML helpers ---------------------------------------------------------------

class RpcError(Exception):
    def __init__(self, tag: str, message: str):
        super().__init__(message)
        self.tag = tag
        self.message = message


def _strip_namespaces(root: ElementTree.Element) -> ElementTree.Element:
    for element in root.iter():
        if "}" in element.tag:
            element.tag = element.tag.split("}", 1)[1]
    return root


def _text(element: Optional[ElementTree.Element], path: str, default: str = "") -> str:
    found = element.find(path) if element is not None else None
    return (found.text or "").strip() if found is not None else default


def _has(element: ElementTree.Element, path: str) -> bool:
    return element.find(path) is not None


def _rpc_reply(inner: str, rpc: Optional[ElementTree.Element] = None) -> Response:
    attributes = ""
    if rpc is not None:
        attributes = (f" message-id={quoteattr(rpc.get('message-id', ''))}"
                      f" nodename={quoteattr(rpc.get('nodename', ''))}")
    body = (f'<soapenv:Envelope xmlns:soapenv="{SOAP_NS}"><soapenv:Body>'
            f"<rpc-reply{attributes}>{inner}</rpc-reply></soapenv:Body></soapenv:Envelope>")
    return Response(content=body, media_type="text/xml")


def _rpc_error(tag: str, message: str, rpc: Optional[ElementTree.Element] = None) -> Response:
    return _rpc_reply(
        "<rpc-error><error-type>application</error-type>"
        f"<error-tag>{escape(tag)}</error-tag><error-severity>error</error-severity>"
        f"<error-message>{escape(message)}</error-message></rpc-error>", rpc)


def _auth_reply(result_code: int, session_id: str = "") -> Response:
    # The login reply is SOAP 1.1 with no prefix: that is the shape cms-nbi-client parses
    session = f"<SessionId>{session_id}</SessionId>" if session_id else ""
    body = ('<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body>'
            f"<auth-reply><ResultCode>{result_code}</ResultCode>{session}</auth-reply></Body></Envelope>")
    return Response(content=body, media_type="text/xml")


def _pon_xml(tag: str, pon_port: Optional[str]) -> str:
    if not pon_port or pon_port.count("/") != 2:
        return ""
    shelf, card, port = pon_port.split("/")
    return (f"<{tag}><type>GponPort</type><id><shelf>{shelf}</shelf><card>{card}</card>"
            f"<gponport>{port}</gponport></id></{tag}>")


def _ont_xml(ont: OntState) -> str:
    """
    An Ont as get-config, create and merge replies carry it: its provisioning only, with no
    op-stat, model or vendor; show-ont adds those separately (see _ont_state_xml).
    """
    return (
        f"<type>Ont</type><id><ont>{ont.ont_id}</ont></id>"
        f"<admin>{escape(ont.admin_state)}</admin>"
        f"<ontprof><type>OntProf</type><id><ontprof>{escape(ont.profile or '')}</ontprof></id></ontprof>"
        f"<serno>{to_serno(ont.serial_number)}</serno><reg-id></reg-id>"
        f"<subscr-id>{escape(ont.subscriber_id or '')}</subscr-id>"
        f"<descr>{escape(ont.description or '')}</descr>"
        f"{_pon_xml('linked-pon', ont.pon_port)}")


def _ont_state_xml(ont: OntState) -> str:
    """The state half of a show-ont match"""
    online = ont.oper_state == OperState.ONLINE
    return (
        f"<type>Ont</type><id><ont>{ont.ont_id}</ont></id>"
        f"<op-stat>{'enable' if online else 'sys-disable'}</op-stat>"
        f"<crit>0</crit><maj>0</maj><min>0</min><warn>0</warn><info>0</info>"
        f"<model>{escape(ont.model or '')}</model><vendor>{VENDOR}</vendor>")


def _slot(attachment: Attachment) -> str:
    return RG.service_slot if attachment == Attachment.ROUTER else ONT_SLOT


def _attachment(slot: str) -> Attachment:
    if slot in ("", ONT_SLOT):
        return Attachment.ETHERNET
    if slot == RG.service_slot:
        return Attachment.ROUTER
    raise RpcError("invalid-value", f"The mock CMS has no ONT slot '{slot}'.")


def _optional(tag: str, value: Optional[str]) -> str:
    return f"<{tag}>{escape(value or '')}</{tag}>"


def _port_xml(ont_id: int, port: PortState) -> str:
    if port.attachment == Attachment.ROUTER:
        return (f"<type>{RG.port_type}</type><id><ont>{ont_id}</ont>"
                f"<{RG.port_id_element}>{port.number}</{RG.port_id_element}></id>"
                f"<admin>{escape(port.admin_state or '')}</admin>")
    security = (f"<sec><type>EthSecProf</type><id><ethsecprof>{escape(port.security_profile)}</ethsecprof></id></sec>"
                if port.security_profile else "")
    binding = "rg1" if port.mode == "router" else "none"  # fake element and values, see router_side.py
    return (f"<type>OntEthGe</type><id><ont>{ont_id}</ont><ontslot>{ONT_SLOT}</ontslot>"
            f"<ontethge>{port.number}</ontethge></id><admin>{escape(port.admin_state or '')}</admin>"
            f"{_optional('subscr-id', port.subscriber_id)}{_optional('descr', port.description)}{security}"
            f"<{RG.port_mode_element}>{binding}</{RG.port_mode_element}>")


def _profile_xml(profile: OntProfile) -> str:
    def number(value: Optional[int]) -> str:
        return "" if value is None else str(value)
    return (f'<type>OntProf</type><id><ontprof name="{escape(profile.name or "")}">{escape(profile.id)}</ontprof></id>'
            f"<name>{escape(profile.name or '')}</name><vendor>{escape(profile.vendor or '')}</vendor>"
            f"<model>{escape(profile.model or '')}</model><pots>{number(profile.pots_ports)}</pots>"
            f"<feth>{number(profile.fe_ports)}</feth><geth>{number(profile.ge_ports)}</geth><hpnaeth>0</hpnaeth>"
            f"<ds1>0</ds1><rfvid>0</rfvid><hotrfvid>0</hotrfvid><eth-oam-capable>false</eth-oam-capable>"
            f"<convert-mcast-capable>false</convert-mcast-capable><rg>{number(profile.rg_ports)}</rg><fb>0</fb>"
            f"<default-to-rg>{'true' if profile.default_to_rg else 'false'}</default-to-rg>")


def _service_xml(ont_id: int, service: ProvisionedService) -> str:
    return (
        f"<type>EthSvc</type><id><ont>{ont_id}</ont><ontslot>{_slot(service.attachment)}</ontslot>"
        f"<ontethany>{service.ont_port}</ontethany>"
        f'<ethsvc name="Data{service.service_index}">{service.service_index}</ethsvc></id>'
        f"<admin>{escape(service.admin_state)}</admin><descr>{escape(service.description or '')}</descr>"
        f"<tag-action><type>SvcTagAction</type><id><svctagaction>{escape(service.tag_action or '')}"
        f"</svctagaction></id></tag-action>"
        f"<bw-prof><type>BwProf</type><id><bwprof>{escape(service.bandwidth_profile or '')}</bwprof></id></bw-prof>"
        f"<out-tag>{service.vlan if service.vlan is not None else 'none'}</out-tag>"
        f"<in-tag>{service.inner_vlan if service.inner_vlan is not None else 'none'}</in-tag>"
        f"<us-pir-override>{escape(service.us_pir_override or 'none')}</us-pir-override>"
        f"<ds-pir-override>{escape(service.ds_pir_override or 'none')}</ds-pir-override>")


def _discovered_xml(ont: DiscoveredOnt) -> str:
    return (f"<type>DiscOnt</type><id><discont>{to_serno(ont.serial_number)}</discont></id>"
            f"<model>{escape(ont.model)}</model><vendor>{VENDOR}</vendor>{_pon_xml('pon', ont.pon_port)}")


def _tag_to_vlan(value: str) -> Optional[int]:
    return int(value) if value.isdigit() else None


def _int(value: str, what: str) -> int:
    if not value.isdigit():
        raise RpcError("invalid-value", f"{what} must be a number, got '{value}'.")
    return int(value)


# --- The NBI endpoint ------------------------------------------------------------

@app.post(NBI_PATH)
async def netconf(request: Request) -> Response:
    try:
        root = _strip_namespaces(ElementTree.fromstring(await request.body()))
    except ElementTree.ParseError as e:
        return Response(content=f"Malformed XML: {e}", status_code=400, media_type="text/plain")

    auth, rpc = root.find("./Body/auth"), root.find("./Body/rpc")
    app.state.requests.append(_describe(auth, rpc))
    if auth is not None:
        return _handle_auth(auth)
    if rpc is None:
        return Response(content="Expected an <auth> or <rpc> element", status_code=400, media_type="text/plain")

    if rpc.get("sessionid") not in app.state.sessions:
        return _rpc_error("access-denied", "Session is not valid or has expired.", rpc)

    network = rpc.get("nodename", "")
    try:
        if _has(rpc, "./action"):
            inner = await _handle_action(network, rpc.find("./action"))
        elif _has(rpc, "./get") or _has(rpc, "./get-config"):
            inner = await _handle_get(network, rpc)
        elif _has(rpc, "./edit-config"):
            inner = await _handle_edit(network, rpc.find("./edit-config"))
        else:
            raise RpcError("operation-not-supported", "The mock CMS does not support this request.")
    except RpcError as e:
        return _rpc_error(e.tag, e.message, rpc)
    except NbiConnectionError:
        return Response(content="Service unavailable", status_code=503, media_type="text/plain")
    except NbiNotFoundError as e:
        return _rpc_error("data-missing", e.message, rpc)
    except NbiConflictError as e:
        return _rpc_error("data-exists", e.message, rpc)
    except NbiError as e:
        return _rpc_error("invalid-value", e.message, rpc)
    return _rpc_reply(inner, rpc)


def _describe(auth: Optional[ElementTree.Element], rpc: Optional[ElementTree.Element]) -> Dict[str, str]:
    """One line of the request log: e.g. {"kind": "edit", "operation": "merge", "type": "OntEthGe"}"""
    if auth is not None:
        return {"kind": "login" if _has(auth, "./login") else "logout"}
    if rpc is None:
        return {"kind": "unknown"}
    if _has(rpc, "./action"):
        return {"kind": "action", "type": _text(rpc, "./action/action-type")}
    if _has(rpc, "./edit-config"):
        target = rpc.find("./edit-config//config/top/object")
        return {"kind": "edit", "operation": target.get("operation", "merge") if target is not None else "",
                "type": _text(target, "./type")}
    target = rpc.find(".//top/object")
    return {"kind": "get", "type": _text(target, "./type"), "children": _text(target, "./children/type")}


def _handle_auth(auth: ElementTree.Element) -> Response:
    if _has(auth, "./login"):
        ok = (_text(auth, "./login/UserName") == _username()
              and secrets.compare_digest(_text(auth, "./login/Password").encode(), _password().encode()))
        if not ok:
            return _auth_reply(6)
        session_id = str(secrets.randbelow(900_000_000) + 100_000_000)
        app.state.sessions.add(session_id)
        return _auth_reply(0, session_id)
    app.state.sessions.discard(_text(auth, "./logout/SessionId"))
    return _auth_reply(0)


async def _handle_action(network: str, action: ElementTree.Element) -> str:
    if _text(action, "./action-type") != "show-ont":
        raise RpcError("operation-not-supported", "The mock CMS only supports the show-ont action.")
    args = action.find("./action-args")
    onts = await app.state.cms.list_onts(network)

    ont_id, serno = _text(args, "./ont/id/ont"), _text(args, "./serno")
    subscriber, admin = _text(args, "./subscr-id"), _text(args, "./admin")
    pon = args.find("./linked-pon/id") if args is not None else None
    pon_port = "/".join(_text(pon, f"./{part}") for part in ("shelf", "card", "gponport")) if pon is not None else ""

    matches = [o for o in onts
               if (not ont_id or str(o.ont_id) == ont_id)
               and (not serno or to_serno(o.serial_number) == serno.lower())
               and (not subscriber or o.subscriber_id == subscriber)
               and (not admin or o.admin_state == admin)
               and (not pon_port or o.pon_port == pon_port)]
    if not matches:
        return "<action-reply/>"
    # Each match carries the ONT twice, as CMS NBI documentation describes: its provisioning
    # (serial, subscriber, profile, PON port) under get-config, its state (op-stat, model) under get
    page = _page([f"<match><get-config><object>{_ont_xml(o)}</object></get-config>"
                  f"<get><object>{_ont_state_xml(o)}</object></get></match>"
                  for o in sorted(matches, key=lambda o: o.ont_id)], args, "./get/object/id")
    return f"<action-reply>{page}</action-reply>"


def _canonical(element: Optional[ElementTree.Element]) -> str:
    if element is None:
        return ""
    for node in element.iter():
        node.text = (node.text or "").strip() or None
        node.tail = None
    return ElementTree.canonicalize(ElementTree.tostring(element, encoding="unicode"))


def _page(items: List[str], request: Optional[ElementTree.Element], id_path: str) -> str:
    """
    Items as one page: all of them unless paging is on (MOCK_CMS_PAGE_SIZE).
    `request` is the element that may hold <after>; `id_path` finds an item's
    <id> inside it.
    """
    size = app.state.page_size
    if size is None:
        return "".join(items)
    start = 0
    after = request.find("./after/id") if request is not None else None
    if after is not None:
        wanted = _canonical(after)
        ids = [_canonical(_strip_namespaces(ElementTree.fromstring(item)).find(id_path)) for item in items]
        if wanted not in ids:
            raise RpcError("invalid-value", "The <after> element names an item that is not in this list.")
        start = ids.index(wanted) + 1
    page = items[start:start + size]
    return "".join(page) + ("<more/>" if start + size < len(items) else "")


async def _handle_get(network: str, rpc: ElementTree.Element) -> str:
    target = rpc.find(".//top/object")
    object_type, child_type = _text(target, "./type"), _text(target, "./children/type")
    cms: MockCmsBackend = app.state.cms
    request = target.find("./children") if target is not None else None

    if object_type == "System" and child_type == "DiscOnt":
        children = _page([f"<child>{_discovered_xml(o)}</child>"
                          for o in await cms.list_discovered_onts(network)], request, "./id")
        return f"<data><top><object><type>System</type><id/><children>{children}</children></object></top></data>"

    if object_type == "DiscOnt":
        serno = _text(target, "./id/discont").lower()
        for ont in await cms.list_discovered_onts(network):
            if to_serno(ont.serial_number) == serno:
                return f"<data><top><object>{_discovered_xml(ont)}</object></top></data>"
        raise RpcError("data-missing", f"Discovered ONT {serno} does not exist on {network}.")

    if object_type == "System" and child_type == "OntProf":
        children = _page([f"<child>{_profile_xml(p)}</child>" for p in await cms.list_ont_profiles(network)],
                         request, "./id")
        return f"<data><top><object><type>System</type><id/><children>{children}</children></object></top></data>"

    if object_type == "System" and child_type == "Vlan":
        children = _page([f'<child><type>Vlan</type><id><vlan name="{escape(v.name or "")}">{v.id}</vlan></id>'
                          f"<name>{escape(v.name or '')}</name></child>" for v in await cms.list_vlans(network)],
                         request, "./id")
        return f"<data><top><object><type>System</type><id/><children>{children}</children></object></top></data>"

    if object_type == "OntProf":
        wanted = _text(target, "./id/ontprof")
        for profile in await cms.list_ont_profiles(network):
            if profile.id == wanted:
                return f"<data><top><object>{_profile_xml(profile)}</object></top></data>"
        raise RpcError("data-missing", f"ONT profile {wanted} does not exist on {network}.")

    if object_type in ("OntEthGe", RG.port_type):
        ont_id = _int(_text(target, "./id/ont"), "ONT ID")
        ports = await cms.get_ont_ports(network, ont_id)
        if object_type == "OntEthGe":
            number, found = _int(_text(target, "./id/ontethge"), "Ethernet port"), ports.ethernet
        else:
            number, found = _int(_text(target, f"./id/{RG.port_id_element}"), "Router port"), ports.router.ports
        port = next((p for p in found if p.number == number), None)
        if port is None:
            raise RpcError("data-missing", f"ONT {ont_id} has no {object_type} {number}.")
        return f"<data><top><object>{_port_xml(ont_id, port)}</object></top></data>"

    if object_type == "Ont":
        ont = await cms.get_ont(network, _int(_text(target, "./id/ont"), "ONT ID"))
        if child_type == "EthSvc":
            children = _page([f"<child>{_service_xml(ont.ont_id, s)}</child>" for s in ont.services],
                             request, "./id")
            return (f"<data><top><object><type>Ont</type><id><ont>{ont.ont_id}</ont></id>"
                    f"<children>{children}</children></object></top></data>")
        return f"<data><top><object>{_ont_xml(ont)}</object></top></data>"

    raise RpcError("operation-not-supported", f"The mock CMS cannot read objects of type '{object_type}'.")


async def _handle_edit(network: str, edit: ElementTree.Element) -> str:
    target = edit.find(".//config/top/object")
    if target is None:
        raise RpcError("missing-element", "edit-config has no object.")
    operation, object_type = target.get("operation", "merge"), _text(target, "./type")
    cms: MockCmsBackend = app.state.cms

    if object_type == "Ont":
        ont_id = _int(_text(target, "./id/ont") or "0", "ONT ID")
        if operation == "delete":
            await cms.delete_ont(network, ont_id)
            return "<ok/>"
        if operation == "create":
            serno = _text(target, "./serno")
            if not serno or serno == "0":
                raise RpcError("missing-element", "The mock CMS needs a serial number to create an ONT.")
            spec = OntSpec.model_construct(
                serial_number=from_serno(serno), ont_id=ont_id or None,
                profile=_text(target, "./ontprof/id/ontprof"),
                description=_text(target, "./descr") or None,
                subscriber_id=_text(target, "./subscr-id") or None)
            ont = await cms.create_ont(network, spec)
            admin = _text(target, "./admin")
            if admin and admin != ont.admin_state:
                ont = await cms.update_ont(network, ont.ont_id, {"admin_state": admin})
            return f"<data><top><object>{_ont_xml(ont)}</object></top></data>"
        if operation == "merge":
            changes: Dict[str, Any] = {}
            for field, path in (("admin_state", "./admin"), ("description", "./descr"),
                                ("subscriber_id", "./subscr-id"), ("profile", "./ontprof/id/ontprof")):
                if _has(target, path):
                    changes[field] = _text(target, path) or None
            if _has(target, "./serno"):
                changes["serial_number"] = from_serno(_text(target, "./serno"))
            ont = await cms.update_ont(network, ont_id, changes)
            return f"<data><top><object>{_ont_xml(ont)}</object></top></data>"

    if object_type in ("OntEthGe", RG.port_type) and operation == "merge":
        ont_id = _int(_text(target, "./id/ont"), "ONT ID")
        values: Dict[str, Any] = {}
        for field, path in (("admin_state", "./admin"), ("subscriber_id", "./subscr-id"),
                            ("description", "./descr"), ("security_profile", "./sec/id/ethsecprof")):
            if _has(target, path):
                values[field] = _text(target, path) or None
        if object_type == "OntEthGe":
            attachment, number = Attachment.ETHERNET, _int(_text(target, "./id/ontethge"), "Ethernet port")
        else:
            attachment = Attachment.ROUTER
            number = _int(_text(target, f"./id/{RG.port_id_element}"), "Router port")
        if not any(v is not None for v in values.values()):
            raise RpcError("missing-element", f"{object_type} merge has nothing to change.")
        await cms.update_port(network, ont_id, attachment, number, PortChange.model_construct(**values))
        ports = await cms.get_ont_ports(network, ont_id)
        port = next(p for p in ports.ethernet + ports.router.ports if (p.attachment, p.number) == (attachment, number))
        return f"<data><top><object>{_port_xml(ont_id, port)}</object></top></data>"

    if object_type == "EthSvc":
        ont_id = _int(_text(target, "./id/ont"), "ONT ID")
        attachment = _attachment(_text(target, "./id/ontslot"))
        port = _int(_text(target, "./id/ontethany"), "ONT port")
        index = _int(_text(target, "./id/ethsvc"), "Service number")
        if operation == "delete":
            await cms.delete_service(network, ont_id, attachment, port, index)
            return "<ok/>"
        if operation in ("create", "merge"):
            current: Optional[ProvisionedService] = None
            if operation == "merge":
                current = (await cms.get_ont(network, ont_id)).service_at(attachment, port, index)
                if current is None:
                    raise RpcError("data-missing", f"Service {index} on ONT {ont_id} port {port} does not exist.")
            base = current.model_dump() if current else {"service_index": index, "ont_port": port,
                                                         "attachment": attachment}
            for field, path in (("admin_state", "./admin"), ("description", "./descr"),
                                ("tag_action", "./tag-action/id/svctagaction"),
                                ("bandwidth_profile", "./bw-prof/id/bwprof"),
                                ("us_pir_override", "./us-pir-override"), ("ds_pir_override", "./ds-pir-override")):
                if _has(target, path):
                    base[field] = _text(target, path) or None
            if _has(target, "./out-tag"):
                base["vlan"] = _tag_to_vlan(_text(target, "./out-tag"))
            if _has(target, "./in-tag"):
                base["inner_vlan"] = _tag_to_vlan(_text(target, "./in-tag"))
            base["admin_state"] = base.get("admin_state") or "enabled"
            service = ProvisionedService(**base)
            await cms.put_service(network, ont_id, service, create=operation == "create")
            return f"<data><top><object>{_service_xml(ont_id, service)}</object></top></data>"

    raise RpcError("operation-not-supported",
                   f"The mock CMS cannot {operation} objects of type '{object_type}'.")


# --- Demo and test helpers (not part of CMS) ---------------------------------------

@app.get("/mock/state")
async def mock_state() -> List[Dict[str, Any]]:
    cms: MockCmsBackend = app.state.cms
    return [cms._snapshot(ont).model_dump(mode="json") for ont in cms._onts.values()]


@app.get("/mock/requests")
async def mock_requests() -> List[Dict[str, str]]:
    return app.state.requests


@app.post("/mock/damage")
async def mock_damage(pon: str = "1/1/1") -> Dict[str, Any]:
    """Break one PON, or every PON with pon=all"""
    cms: MockCmsBackend = app.state.cms
    pons = sorted({o.state.pon_port for o in cms._onts.values()}) if pon == "all" else [pon]
    return {"pon": pon, "damage": [change for p in pons for change in cms.damage(p)]}


@app.post("/mock/outage")
async def mock_outage(mode: str = "none") -> Dict[str, Any]:
    app.state.cms.outage = None if mode == "none" else mode
    return {"outage": app.state.cms.outage}


@app.post("/mock/reset")
async def mock_reset() -> Dict[str, str]:
    _fresh_state()
    return {"status": "reset"}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "healthy"}
