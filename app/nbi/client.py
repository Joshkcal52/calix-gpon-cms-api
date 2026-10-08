"""
CMS NBI client
Sends requests to the Calix CMS northbound interface (R15.x: SOAP/XML over
HTTP or HTTPS, one URL) and turns the replies into plain Python data. This is
the only code in the project that talks to CMS over the network.

It knows the transport, not the network: the envelopes, the session, paging
and errors. Which objects to read or change, and what their replies mean, is
up to the backend (app/nbi/cms_backend.py).

The request shapes were learned from the cms-nbi-client project
(https://github.com/somenetworking/CMS-NBI-Client) by recording, against the
mock NBI server, what its 0.1.0 release sent for every operation this project
uses. This code was written fresh from those recordings; none of it is taken
from the library. tests/test_cms_client.py holds the recordings and checks
that the same requests are sent.

What it does:
  - Logs in on first use, keeps the session, and logs in again once if CMS
    says the session is no longer valid (rpc-error access-denied).
  - Escapes every value it is given into the XML: text and attributes.
  - Pages through long lists: when a reply ends with <more/>, it asks again
    with <after> set to the last item it received.
  - Raises the NbiError types the rest of the app uses: NbiTimeoutError,
    NbiConnectionError, NbiAuthError, NbiNotFoundError, NbiConflictError,
    NbiRejectedError.
  - Verifies the CMS server's certificate over HTTPS unless told not to.

NOT VERIFIED AGAINST A REAL CMS. The request shapes are the library's and the
reply shapes are the mock server's, which follows CMS NBI documentation where
it is clear (show-ont, delete, paging). Inferred, not known: the rpc-error tags
used to tell "not found" from "already exists", the access-denied signal for
an expired session, and how paging looks in a real reply (the mock server
can page; a real CMS has not been seen doing it).
"""

import asyncio
import logging
import random
from typing import Any, Dict, List, Optional, Union
from xml.etree import ElementTree
from xml.sax.saxutils import escape

import httpx

from app.nbi.errors import (
    NbiAuthError, NbiConflictError, NbiConnectionError, NbiError, NbiNotFoundError, NbiRejectedError,
    NbiTimeoutError
)

logger = logging.getLogger(__name__)

NBI_PATH = "/cmsexc/ex/netconf"
SOAP_1_1 = "http://schemas.xmlsoap.org/soap/envelope/"  # login and logout use SOAP 1.1
SOAP_1_2 = "http://www.w3.org/2003/05/soap-envelope"  # every <rpc> uses SOAP 1.2
MAX_PAGES = 10_000  # a list longer than this many pages is treated as a CMS that never stops paging

# The attribute lists sent when listing children, as cms-nbi-client sends them
ATTR_LISTS = {
    "DiscOnt": ("op-stat crit maj min warn info derived-states reg-id prov-reg-id pon model vendor clei ont ontprof "
                "subscr-id descr curr-sw-vers alt-sw-vers curr-committed mfg-serno product-code curr-cust-vers "
                "alt-cust-vers onu-mac mta-mac link-permit-status"),
    "OntProf": ("name vendor model pots feth geth hpnaeth ds1 rfvid hotrfvid eth-oam-capable convert-mcast-capable "
                "rg fb default-to-rg"),
    "Vlan": ("name igmp-mode igmp-prof dhcp-mode mac-force-forw ip-src-verify mac-learn ae-ont-discovery pon-tlan "
             "pon-hairpin igmp-pbit dhcp-svc-prof option82-enable eth-opt82prof gpon-opt82prof mobility pppoe-prof"),
    "EthSvc": ("admin descr tag-action bw-prof out-tag in-tag mcast-prof pon-cos us-cir-override us-pir-override "
               "ds-pir-override hot-swap pppoe-force-discard"),
}


class SessionExpired(Exception):
    """CMS refused the session (rpc-error access-denied)"""


def text(value: Any) -> str:
    """Escape a value for an XML element's text"""
    return escape(str(value))


def attr(value: Any) -> str:
    """Escape a value for a double-quoted XML attribute"""
    return escape(str(value), {'"': "&quot;"})


def element(tag: str, value: Any) -> str:
    """<tag>value</tag>, with the value escaped"""
    return f"<{tag}>{text(value)}</{tag}>"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def to_data(node: ElementTree.Element) -> Any:
    """
    One reply element as plain data: an element with neither attributes nor
    children is its text (None if empty); otherwise a dict with "@name" for
    attributes, "#text" for text, and one key per child element (a list when
    the child repeats). Namespace prefixes are dropped.
    """
    children = list(node)
    content = (node.text or "").strip() or None
    if not children and not node.attrib:
        return content
    data: Dict[str, Any] = {f"@{_local(k)}": v for k, v in node.attrib.items()}
    for child in children:
        name, value = _local(child.tag), to_data(child)
        if name in data:
            if not isinstance(data[name], list):
                data[name] = [data[name]]
            data[name].append(value)
        else:
            data[name] = value
    if content is not None:
        data["#text"] = content
    return data


def _strip(root: ElementTree.Element) -> ElementTree.Element:
    for node in root.iter():
        node.tag = _local(node.tag)
    return root


def raise_rpc_error(root: ElementTree.Element) -> None:
    """Raise the matching NbiError if a reply carries an <rpc-error>"""
    error = root.find(".//rpc-error")
    if error is None:
        return
    tag = (error.findtext("error-tag") or "").strip()
    message = ((error.findtext("error-message") or "").strip()
               or f"CMS refused the request ({tag or 'no reason given'}).")
    if tag == "access-denied":
        raise SessionExpired(message)
    if tag == "data-missing":
        raise NbiNotFoundError(message)
    if tag in ("data-exists", "in-use"):
        raise NbiConflictError(message)
    raise NbiRejectedError(message)


class CmsClient:
    """One CMS server, one login. Safe to share between tasks on one event loop."""

    def __init__(self, host: str, port: int, username: str, password: str, *, scheme: str = "http",
                 verify: Union[bool, str] = True, timeout: float = 10.0, path: str = NBI_PATH):
        if scheme not in ("http", "https"):
            raise ValueError(f"CMS scheme must be http or https, not '{scheme}'")
        self.host, self.port, self.scheme = host, int(port), scheme
        self.url = f"{scheme}://{host}:{self.port}{path}"
        self.username = username
        self._password = password
        self.timeout = timeout
        self._verify = verify  # True, False, or the path of a CA bundle
        self.session_id: Optional[str] = None
        self._login_lock = asyncio.Lock()
        self._http: Optional[httpx.AsyncClient] = None
        self._http_loop: Optional[asyncio.AbstractEventLoop] = None

    # --- Transport -----------------------------------------------------------------

    def _connection(self) -> httpx.AsyncClient:
        """The connection pool, made on first use (and again if the event loop changed)"""
        loop = asyncio.get_running_loop()
        if self._http is None or self._http_loop is not loop:
            self._http = httpx.AsyncClient(verify=self._verify, timeout=self.timeout, trust_env=False)
            self._http_loop = loop
        return self._http

    async def send(self, body: str) -> httpx.Response:
        """
        POST one SOAP document and return the HTTP response. Every request to
        CMS passes through here (the read-only check guards this method).
        The headers are the ones cms-nbi-client sends. The body goes out as
        ASCII, with any other character as a character reference: that reads
        the same whether CMS takes the header's ISO-8859-1 or the login
        envelope's UTF-8 declaration at its word.
        """
        try:
            return await self._connection().post(
                self.url, content=body.encode("ascii", "xmlcharrefreplace"),
                headers={"Content-Type": "text/xml;charset=ISO-8859-1",
                         "User-Agent": f"CMS_NBI_CONNECT-{self.username}"})
        except httpx.ConnectTimeout:
            # Nothing was sent, so unlike a timeout after connecting, a write cannot have been applied.
            # (On Windows a refused connection ends here too: it retries instead of failing at once.)
            raise NbiConnectionError(f"Could not connect to CMS at {self.host}:{self.port} (no connection "
                                     f"within {self.timeout:g} seconds). Nothing was sent.") from None
        except httpx.TimeoutException:
            raise NbiTimeoutError(f"CMS did not answer within {self.timeout:g} seconds.") from None
        except httpx.HTTPError as e:
            hint = (" The CMS server's TLS certificate could not be verified: set CMS_CA_FILE to the CA that "
                    "signed it." if "CERTIFICATE_VERIFY_FAILED" in str(e) else "")
            raise NbiConnectionError(f"Could not connect to CMS at {self.host}:{self.port} "
                                     f"({type(e).__name__}).{hint}") from None

    @staticmethod
    def _parse(response: httpx.Response) -> ElementTree.Element:
        try:
            return _strip(ElementTree.fromstring(response.content))
        except ElementTree.ParseError:
            raise NbiRejectedError("CMS answered with something that is not XML.") from None

    async def close(self) -> None:
        """Log out if logged in, and close the connections"""
        try:
            await self.logout()
        finally:
            if self._http is not None:
                try:
                    await self._http.aclose()
                except RuntimeError:  # its event loop is gone; nothing left to close
                    pass
                self._http = self._http_loop = None

    # --- Session -------------------------------------------------------------------

    def _auth(self, inner: str) -> str:
        return (f'<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="{SOAP_1_1}">'
                f'<soapenv:Body><auth message-id="{_message_id()}">{inner}</auth></soapenv:Body>'
                f"</soapenv:Envelope>")

    async def login(self) -> None:
        response = await self.send(self._auth(
            f"<login>{element('UserName', self.username)}{element('Password', self._password)}</login>"))
        if response.status_code != 200:
            raise NbiConnectionError(f"CMS answered the login with HTTP {response.status_code}.")
        reply = self._parse(response)
        code = (reply.findtext(".//auth-reply/ResultCode") or "").strip()
        session = (reply.findtext(".//auth-reply/SessionId") or "").strip()
        if code != "0" or not session:
            raise NbiAuthError("CMS rejected this API's username or password.")
        self.session_id = session
        logger.info(f"Logged in to CMS at {self.host}:{self.port}")

    async def logout(self) -> None:
        if self.session_id is None:
            return
        session, self.session_id = self.session_id, None
        try:
            await self.send(self._auth(
                f"<logout>{element('UserName', self.username)}{element('SessionId', session)}</logout>"))
        except NbiError as e:
            logger.warning(f"CMS logout failed: {e.message}")

    async def _ensure_session(self) -> str:
        async with self._login_lock:
            if self.session_id is None:
                await self.login()
            return self.session_id

    # --- Requests --------------------------------------------------------------------

    async def rpc(self, network: str, body: str) -> ElementTree.Element:
        """
        Send one <rpc> on the current session and return its <rpc-reply>.
        Logs in first if needed, and once more if CMS has dropped the session.
        """
        for attempt in (1, 2):
            session = await self._ensure_session()
            response = await self.send(
                f'<soapenv:Envelope xmlns:soapenv="{SOAP_1_2}"><soapenv:Body>'
                f'<rpc message-id="{_message_id()}" nodename="{attr(network)}" username="{attr(self.username)}" '
                f'sessionid="{attr(session)}">{body}</rpc></soapenv:Body></soapenv:Envelope>')
            if response.status_code >= 500:
                raise NbiConnectionError(f"CMS answered with HTTP {response.status_code}.")
            if response.status_code != 200:
                raise NbiRejectedError(f"CMS refused the request with HTTP {response.status_code}.")
            root = self._parse(response)
            try:
                raise_rpc_error(root)
            except SessionExpired:
                if self.session_id == session:
                    self.session_id = None
                if attempt == 2:
                    raise NbiAuthError("CMS keeps rejecting the session straight after login.") from None
                logger.info("CMS session expired; logging in again")
                continue
            reply = root.find(".//rpc-reply")
            if reply is None:
                raise NbiRejectedError("CMS answered without an rpc-reply.")
            return reply
        raise AssertionError("unreachable")

    async def show_ont(self, network: str, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        The show-ont action: every ONT matching the filters (ont, serno,
        subscr-id), as Ont objects. All pages.
        """
        args = ""
        for name, value in filters.items():
            args += (f"<ont><type>Ont</type><id>{element('ont', value)}</id></ont>" if name == "ont"
                     else element(name, value))
        found: List[Dict[str, Any]] = []
        after = ""
        for _ in range(MAX_PAGES):
            reply = await self.rpc(network, "<action><action-type>show-ont</action-type>"
                                            f"<action-args>{args}{after}</action-args></action>")
            answer = reply.find("action-reply")
            matches = []
            for match in answer.findall("match") if answer is not None else []:
                # Per CMS NBI documentation, each match carries the ONT twice: its provisioning (serno,
                # subscr-id, descr, ontprof, linked-pon) under get-config and its state (op-stat,
                # model, vendor) under get. Read both into one object; a match with only one half
                # still reads.
                halves = [o for o in (match.find("get-config/object"), match.find("get/object")) if o is not None]
                if not halves:
                    continue
                merged: Dict[str, Any] = {}
                for half in halves:
                    data = to_data(half)
                    if isinstance(data, dict):
                        merged.update(data)
                found.append(merged)
                matches.append(halves[0])
            if answer is None or answer.find("more") is None:
                return found
            after = self._after(matches, after)
        raise NbiRejectedError("CMS kept paging the ONT list without end.")

    async def children(self, network: str, parent: str, child_type: str,
                       verb: str = "get-config") -> List[Dict[str, Any]]:
        """
        Every child of one type under a parent object (System, or an Ont), all
        pages. `parent` is the parent's <type> and <id> XML.
        """
        source = "<source><running/></source>" if verb == "get-config" else ""
        attr_filter = "" if child_type == "Vlan" else "<attr-filter></attr-filter>"
        found: List[Dict[str, Any]] = []
        after = ""
        for _ in range(MAX_PAGES):
            reply = await self.rpc(
                network, f'<{verb}>{source}<filter type="subtree"><top><object>{parent}<children>'
                         f"{element('type', child_type)}{after}{attr_filter}"
                         f"<attr-list>{ATTR_LISTS[child_type]}</attr-list></children></object></top></filter>"
                         f"</{verb}>")
            listed = reply.find("data/top/object/children")
            items = listed.findall("child") if listed is not None else []
            found.extend(to_data(c) for c in items)
            if listed is None or listed.find("more") is None:
                return found
            after = self._after(items, after)
        raise NbiRejectedError(f"CMS kept paging the {child_type} list without end.")

    @staticmethod
    def _after(items: List[ElementTree.Element], previous: str) -> str:
        """<after> naming the last item received, so the next page starts after it"""
        if not items or items[-1].find("type") is None or items[-1].find("id") is None:
            raise NbiRejectedError("CMS said there were more items but did not say where the page ended.")
        last = items[-1]
        after = (f"<after>{ElementTree.tostring(last.find('type'), encoding='unicode')}"
                 f"{ElementTree.tostring(last.find('id'), encoding='unicode')}</after>")
        if after == previous:
            raise NbiRejectedError("CMS returned the same page twice.")
        return after

    async def get(self, network: str, object_xml: str) -> Optional[Dict[str, Any]]:
        """One object, read with get-config. `object_xml` is its <type> and <id>. None if CMS sent nothing."""
        reply = await self.rpc(network, '<get-config><source><running/></source><filter type="subtree"><top>'
                                        f"<object>{object_xml}</object></top></filter></get-config>")
        found = reply.find("data/top/object")
        return to_data(found) if found is not None else None

    async def edit(self, network: str, operation: str, object_xml: str, *, get_config: bool = False,
                   force: bool = False) -> Optional[Dict[str, Any]]:
        """
        Create, merge or delete one object. With get_config, CMS sends the
        object back as it now stands, and that is returned.
        """
        extra = (' get-config="true"' if get_config else "") + (' force="true"' if force else "")
        reply = await self.rpc(network, f'<edit-config><target><running/></target><config><top>'
                                        f'<object operation="{attr(operation)}"{extra}>{object_xml}</object>'
                                        f"</top></config></edit-config>")
        found = reply.find("data/top/object")
        return to_data(found) if found is not None else None


def _message_id() -> int:
    """CMS matches replies to requests by this; any positive 31-bit number"""
    return random.randint(1, 2**31 - 1)
