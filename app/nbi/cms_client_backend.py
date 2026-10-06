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

NOT VERIFIED AGAINST A REAL CMS. It is tested against the mock NBI server
(app/nbi/mock_cms_server.py), whose replies are modelled on what the library
parses. Anything the library does not parse is a guess: the op-stat values,
the linked-pon element, and the rpc-error tags used to tell "not found" from
"already exists".
"""

import asyncio
import logging
import os
import tempfile
from typing import Any, Callable, Dict, Iterator, List, Optional
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from app.models.provisioning_model import (
    DiscoveredOnt, EthernetService, OntSpec, OntState, OperState, ProvisionedService
)
from app.nbi.errors import (
    NbiAuthError, NbiConflictError, NbiConnectionError, NbiError, NbiNotFoundError,
    NbiRejectedError, NbiTimeoutError
)
from app.nbi.serno import from_serno, to_serno

logger = logging.getLogger(__name__)

ONT_SLOT = "3"  # the library's default ONT slot for Ethernet ports


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
        services=services or [])


def _service(obj: Dict[str, Any]) -> ProvisionedService:
    return ProvisionedService(
        service_index=int(_dig(obj, "id", "ethsvc") or 1),
        ont_port=int(_dig(obj, "id", "ontethany") or 1),
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

    def __init__(self, host: str, port: int, username: str, password: str, timeout: float = 10.0):
        try:
            import cmsnbiclient
            import requests
        except ImportError:
            raise RuntimeError(
                "CMS_HOST is set but cms-nbi-client is not installed. "
                "Install requirements-cms.txt.") from None
        self._lib = cmsnbiclient
        self._requests = requests
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
                pon_port=_pon_port(obj.get("pon")) or "unknown") for obj in _objects(result, "DiscOnt")]
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
            return [_ont_state(obj) for obj in _objects(result, "Ont")]
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

        def work() -> None:
            used = {s.service_index for s in self._services(network, ont_id) if s.ont_port == service.ont_port}
            index = next(n for n in range(1, len(used) + 2) if n not in used)
            self._check(self._lib.E7.Create(self._client, _attr(network), self._timeout).ont_ethsvc(
                ont_id=str(ont_id), ontslot=ONT_SLOT, ontethany=str(service.ont_port), ethsvc=str(index),
                admin_state="enabled",
                descr=escape(service.description or ""),
                svctagaction_id=escape(service.tag_action),
                bwprof_id=escape(service.bandwidth_profile),
                out_tag_id=str(service.vlan)))
        await self._run(work)

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
        return list(_objects(self._check(self._query(network).show_ont(action_args=filters)), "Ont"))

    def _services(self, network: str, ont_id: int) -> List[ProvisionedService]:
        result = self._check(self._query(network).ont_children_ethsvc(ont_id=str(ont_id)))
        return [_service(obj) for obj in _objects(result, "EthSvc")]
