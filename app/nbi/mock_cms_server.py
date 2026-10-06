"""
Mock CMS NBI Server
A stand-in for the Calix CMS northbound interface that speaks SOAP over HTTP,
so the real client path (cms-nbi-client, or Ansible's uri module) can be
exercised without a CMS.

    uvicorn app.nbi.mock_cms_server:app --port 18080

It listens where CMS does (POST /cmsexc/ex/netconf) and keeps its data in the
simulated CMS from app/nbi/mock_cms.py: one network, NTWK-MOCK-E7.

WHERE THE XML SHAPES COME FROM: every request shape, and every part of a
reply that cms-nbi-client 0.1.0 reads, is taken from that library's source.
Fields the library never looks at (op-stat, linked-pon, model, the error
tags) are educated guesses. Nothing here has been compared with a real CMS.

Helper endpoints for demos and tests (not part of CMS):
    GET  /mock/state                  every ONT, as JSON
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

from app.models.provisioning_model import DiscoveredOnt, OntSpec, OntState, OperState, ProvisionedService
from app.nbi.errors import (
    NbiConflictError, NbiConnectionError, NbiError, NbiNotFoundError, NbiRejectedError
)
from app.nbi.mock_cms import MockCmsBackend
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
        extra_onts=int(os.getenv("MOCK_CMS_EXTRA_ONTS", "0")))
    app.state.sessions = set()


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
    online = ont.oper_state == OperState.ONLINE
    return (
        f"<type>Ont</type><id><ont>{ont.ont_id}</ont></id>"
        f"<admin>{escape(ont.admin_state)}</admin>"
        f"<serno>{to_serno(ont.serial_number)}</serno><reg-id></reg-id>"
        f"<subscr-id>{escape(ont.subscriber_id or '')}</subscr-id>"
        f"<descr>{escape(ont.description or '')}</descr>"
        f"<ontprof><type>OntProf</type><id><ontprof>{escape(ont.profile or '')}</ontprof></id></ontprof>"
        f"<op-stat>{'enable' if online else 'sys-disable'}</op-stat>"
        f"<model>{escape(ont.model or '')}</model><vendor>{VENDOR}</vendor>"
        f"{_pon_xml('linked-pon', ont.pon_port)}")


def _service_xml(ont_id: int, service: ProvisionedService) -> str:
    return (
        f"<type>EthSvc</type><id><ont>{ont_id}</ont><ontslot>{ONT_SLOT}</ontslot>"
        f"<ontethany>{service.ont_port}</ontethany>"
        f'<ethsvc name="Data{service.service_index}">{service.service_index}</ethsvc></id>'
        f"<admin>{escape(service.admin_state)}</admin><descr>{escape(service.description or '')}</descr>"
        f"<tag-action><type>SvcTagAction</type><id><svctagaction>{escape(service.tag_action or '')}"
        f"</svctagaction></id></tag-action>"
        f"<bw-prof><type>BwProf</type><id><bwprof>{escape(service.bandwidth_profile or '')}</bwprof></id></bw-prof>"
        f"<out-tag>{service.vlan if service.vlan is not None else 'none'}</out-tag>"
        f"<in-tag>{service.inner_vlan if service.inner_vlan is not None else 'none'}</in-tag>")


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


def _handle_auth(auth: ElementTree.Element) -> Response:
    if _has(auth, "./login"):
        ok = (_text(auth, "./login/UserName") == _username()
              and secrets.compare_digest(_text(auth, "./login/Password"), _password()))
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
               and (not serno or to_serno(o.serial_number) == serno.upper())
               and (not subscriber or o.subscriber_id == subscriber)
               and (not admin or o.admin_state == admin)
               and (not pon_port or o.pon_port == pon_port)]
    if not matches:
        return "<action-reply/>"
    return "<action-reply>" + "".join(
        f"<match><get><object>{_ont_xml(o)}</object></get></match>" for o in matches) + "</action-reply>"


async def _handle_get(network: str, rpc: ElementTree.Element) -> str:
    target = rpc.find(".//top/object")
    object_type, child_type = _text(target, "./type"), _text(target, "./children/type")
    cms: MockCmsBackend = app.state.cms

    if object_type == "System" and child_type == "DiscOnt":
        children = "".join(f"<child>{_discovered_xml(o)}</child>"
                           for o in await cms.list_discovered_onts(network))
        return f"<data><top><object><type>System</type><id/><children>{children}</children></object></top></data>"

    if object_type == "DiscOnt":
        serno = _text(target, "./id/discont").upper()
        for ont in await cms.list_discovered_onts(network):
            if to_serno(ont.serial_number) == serno:
                return f"<data><top><object>{_discovered_xml(ont)}</object></top></data>"
        raise RpcError("data-missing", f"Discovered ONT {serno} does not exist on {network}.")

    if object_type == "Ont":
        ont = await cms.get_ont(network, _int(_text(target, "./id/ont"), "ONT ID"))
        if child_type == "EthSvc":
            children = "".join(f"<child>{_service_xml(ont.ont_id, s)}</child>" for s in ont.services)
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

    if object_type == "EthSvc":
        ont_id = _int(_text(target, "./id/ont"), "ONT ID")
        port = _int(_text(target, "./id/ontethany"), "ONT port")
        index = _int(_text(target, "./id/ethsvc"), "Service number")
        if operation == "delete":
            await cms.delete_service(network, ont_id, port, index)
            return "<ok/>"
        if operation in ("create", "merge"):
            current: Optional[ProvisionedService] = None
            if operation == "merge":
                existing = (await cms.get_ont(network, ont_id)).services
                current = next((s for s in existing if (s.ont_port, s.service_index) == (port, index)), None)
            base = current.model_dump() if current else {"service_index": index, "ont_port": port}
            for field, path in (("admin_state", "./admin"), ("description", "./descr"),
                                ("tag_action", "./tag-action/id/svctagaction"),
                                ("bandwidth_profile", "./bw-prof/id/bwprof")):
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
