"""
E7 CLI Backend
Provisions through each E7 EXA node's own command line over SSH, for
networks whose CMS has no northbound interface (NBI) licence. Used when
E7_NODES is set. It sits behind the same NBI gateway as the CMS backend, so
the API, jobs, retries, circuit breaker and safety checks are unchanged.

A "network" in this API's URLs is one E7 node (E7_NODES maps names to
hosts). Each node gets one SSH session; its commands run one at a time.

The commands are those of Calix's public E7 EXA R3.x CLI reference:
  create ont <id> profile <p> serial-number <sn> [subscriber-id|description|admin-state]
  set ont <id> pon-port none  delete ont <id> [forced]
  show ont <id> detail        show ont detail        show ont discovered detail
  show ont serial-number <sn> show ont subscriber-id <s>
  add eth-svc <name> to-ont-port <ont>/g<n> bw-profile <bw> svc-tag-action <ta> [outer-vlan ...]
  remove eth-svc <name> from-ont-port <ont>/g<n>   (a service change is a remove and an add)
  show ont-port <ont> eth-svc   show ont-port <ont> detail   set ont-port <ont>/g<n> ...
  show ont-profile              show vlan
ONT ports are <ont>/g<n> for Gigabit Ethernet and <ont>/G<n> for the
residential gateway (router side), per the same reference.

NOT VERIFIED AGAINST A REAL E7. The commands' syntax is documented; what they
print mostly is not (Calix's one sample is a show mcast table), so every reply is read by the
lenient parsers in app/nbi/e7_parse.py. Also not documented, and so settings
or guesses here: the prompt, how a refused command reads (E7_ERROR_PATTERN),
the service names an E7 uses (E7_SERVICE_NAMES), and which state words mean
an ONT is up. Capture real output with python -m app.tools.e7_capture first.

Unlike CMS, the CLI addresses bandwidth profiles, tag actions and services by
name. This API's service_index maps to a service name through
E7_SERVICE_NAMES: index 1 is the first name, and so on.

CMS still manages these E7s. Whether CMS notices changes made on the CLI by
itself, or needs a resync of the node, is not known: check that an ONT
created here appears in CMS before relying on CMS screens.
"""

import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Set, Tuple

from app.models.provisioning_model import (
    Attachment, DiscoveredOnt, EthernetService, OntPorts, OntProfile, OntSpec, OntState, OperState,
    PortChange, PortState, ProvisionedService, RouterState, ServiceChange, Vlan, port_name
)
from app.nbi import e7_parse as parse
from app.nbi.e7_session import E7Node, E7Session
from app.nbi.errors import (
    NbiConflictError, NbiError, NbiNotFoundError, NbiNotVerifiedError, NbiRejectedError
)
from app.nbi.router_side import MODE_NOT_SETTABLE
from app.nbi.serno import from_serno, to_serno

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = r"\S+[>#] ?"
DEFAULT_PAGER_COMMAND = "set session pager disabled"
# Keep alarms and events from landing in the middle of a command's output, and turn the session's
# inactivity timeout off (set session timeout enabled|disabled; 30 minutes when on). An open-source E7 test
# library (DarcyChang/MyProjects, Teststeplib_e7.py) turns the same three off at login.
DEFAULT_SESSION_COMMANDS = ("set session timeout disabled", "set session alarm-notif disabled",
                            "set session event-notif disabled", "set session tca-notif disabled")
# The one CLI error Calix documents is "Invalid input detected at '^' marker". The rest is a guess, apart
# from "fail" and "not found", which the same test library checks for in real E7 replies.
DEFAULT_ERROR_PATTERN = (r"(?i)(\berror\b|invalid input|\binvalid\b|\bfail(?:ed|ure|s)?\b|cannot|can't|not allowed|denied|"
                         r"does not exist|doesn't exist|not found|no such|already exists|in use|unknown)")
NOT_FOUND = re.compile(r"(?i)(does not exist|doesn't exist|not found|no such|not provisioned)")
CONFLICT = re.compile(r"(?i)(already exists|already in use|in use|duplicate|exists)")
# Service names: E7s name services (show eth-svc-name lists them). Data1..Data8 is not confirmed for your
# E7s, but open-source scripts written against real E7s use Data1 to Data5 the same way.
DEFAULT_SERVICE_NAMES = tuple(f"Data{n}" for n in range(1, 9))
# ONT state words read as "up". Inferred; the raw words seen are kept in `observed`.
ONLINE_WORDS = {"present", "up", "online", "enabled", "enable", "in-service", "is", "active", "ranged", "working"}

# A CLI argument may not contain anything that could end the command or start another one
UNSAFE = re.compile(r'["\x00-\x1f\x7f;]')
CLI_MAX_LINE = 4000  # Calix: "a maximum of 4k characters for a single paste operation"
# create ont / set ont: "subscriber ID. This is a 1-63 character text string", "description ... 1-47 character"
ONT_SUBSCRIBER_ID_MAX = 63
ONT_DESCRIPTION_MAX = 47
# Services allowed per ONT interface. Calix, E-Series EXA R3.x Turn-Up and Transport Guide, System
# Support Capacities: "8 on an ONT Ethernet port, 4 on an ONT RG interface, 1 on an ONT FB interface".
# This backend adds services to g (Ethernet) and G (RG) ports only.
MAX_SERVICES = {Attachment.ETHERNET: 8, Attachment.ROUTER: 4}


class UnknownE7Error(NbiNotFoundError):
    """The network named is not in E7_NODES. Never read as "that ONT does not exist"."""


def arg(value: object) -> str:
    """One CLI argument, quoted if it has spaces (Calix: quote entity names that contain spaces)"""
    text = str(value)
    if UNSAFE.search(text):
        raise NbiRejectedError(f"{text!r} contains a character that cannot be sent on the E7 command line "
                               f"(quotes, semicolons and control characters are refused).")
    if not text:
        raise NbiRejectedError("An empty value cannot be sent on the E7 command line.")
    return f'"{text}"' if " " in text else text


def limited(value: str, what: str, most: int) -> str:
    """A value the E7 limits in length, refused here rather than by the E7"""
    if len(value) > most:
        raise NbiRejectedError(f"The E7 takes at most {most} characters for {what}; {value!r} has {len(value)}.")
    return value


def e7_serial(serial_number: str) -> str:
    """CXNK00A3F104 -> 00A3F104 (Calix: the ONT serial number is 1-8 hex digits)"""
    return to_serno(serial_number).upper().zfill(8)


class E7CliBackend:
    name = "e7-cli"

    def __init__(self, nodes: Sequence[E7Node], *, prompt: str = DEFAULT_PROMPT,
                 pager_command: Optional[str] = DEFAULT_PAGER_COMMAND,
                 session_commands: Sequence[str] = DEFAULT_SESSION_COMMANDS,
                 error_pattern: str = DEFAULT_ERROR_PATTERN, service_names: Sequence[str] = DEFAULT_SERVICE_NAMES,
                 connect_timeout: float = 10.0, sessions: Optional[Dict[str, E7Session]] = None):
        self.nodes = {node.name: node for node in nodes}
        self.sessions: Dict[str, E7Session] = sessions or {
            node.name: E7Session(node, prompt=prompt, pager_command=pager_command,
                                 setup_commands=session_commands, connect_timeout=connect_timeout)
            for node in nodes}
        self.error = re.compile(error_pattern)
        self.service_names = tuple(service_names)
        self.unrecognised: Dict[str, Set[str]] = defaultdict(set)  # command -> labels nobody here knows
        self.observed: Dict[str, Set[str]] = defaultdict(set)  # "oper-state" -> raw words seen

    # --- Reads -------------------------------------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        rows = self._audit("show ont discovered detail", parse.records(
            await self._show(network, "show ont discovered detail")))
        return [DiscoveredOnt(serial_number=_serial(row), model=parse.value(row, "model") or "unknown",
                              pon_port=_pon(row) or "unknown")
                for row in rows if parse.value(row, "serial-number")]

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        ont = await self._ont_detail(network, ont_id)
        if ont is None:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        ont.services = await self._services(network, ont_id)
        return ont

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        for ont_id in await self._ont_ids(network, f"show ont serial-number {arg(e7_serial(serial_number))}"):
            ont = await self._ont_detail(network, ont_id)
            if ont is not None and to_serno(ont.serial_number) == to_serno(serial_number):
                ont.services = await self._services(network, ont_id)
                return ont
        return None

    async def find_onts_by_subscriber(self, network: str, subscriber_id: str) -> List[OntState]:
        found = []
        for ont_id in await self._ont_ids(network, f"show ont subscriber-id {arg(subscriber_id)}"):
            ont = await self._ont_detail(network, ont_id)
            if ont is not None and ont.subscriber_id == subscriber_id:
                found.append(ont)
        return found

    async def list_onts(self, network: str) -> List[OntState]:
        rows = self._audit("show ont detail", parse.records(await self._show(network, "show ont detail")))
        return [self._ont_state(row) for row in rows if parse.number(row, "ont") is not None]

    async def get_ont_ports(self, network: str, ont_id: int) -> OntPorts:
        ont = await self._ont_detail(network, ont_id)
        if ont is None:
            raise NbiNotFoundError(f"ONT {ont_id} does not exist on {network}.")
        profile = next((p for p in await self.list_ont_profiles(network) if p.id == ont.profile), None)
        services = await self._services(network, ont_id)
        rows = self._audit("show ont-port detail", parse.records(
            await self._show(network, f"show ont-port {ont_id} detail")))

        ethernet, router_ports = [], []
        for row in rows:
            port = _port(parse.value(row, "port"))
            if port is None:
                continue
            letter, number = port
            state = PortState(port=port_name(Attachment.ROUTER if letter == "G" else Attachment.ETHERNET, number),
                              attachment=Attachment.ROUTER if letter == "G" else Attachment.ETHERNET, number=number,
                              admin_state=parse.value(row, "admin-state"),
                              subscriber_id=parse.value(row, "subscriber-id"),
                              description=parse.value(row, "description"),
                              security_profile=parse.value(row, "eth-sec-profile"), mode="unknown")
            if letter == "g":
                ethernet.append(state)
            elif letter == "G":
                router_ports.append(state)

        present = None if profile is None or profile.rg_ports is None else profile.rg_ports > 0
        router = RouterState(present=True if router_ports else present,
                             default_to_rg=profile.default_to_rg if profile else None, ports=router_ports,
                             services=sum(1 for s in services if s.attachment == Attachment.ROUTER))
        if any(s.attachment == Attachment.UNKNOWN for s in services):
            router.note = ("Some services are on ONT ports other than g (Ethernet) and G (residential gateway), "
                           "such as F (full bridge). They are listed as unknown.")
        return OntPorts(network=network, ont_id=ont_id, profile=ont.profile, ethernet=sorted(
            ethernet, key=lambda p: p.number), router=router, voice_ports=profile.pots_ports if profile else None)

    async def list_ont_profiles(self, network: str) -> List[OntProfile]:
        rows = self._audit("show ont-profile", parse.table(await self._show(network, "show ont-profile")))
        profiles = []
        for row in rows:
            name = parse.value(row, "name") or parse.value(row, "profile")
            if not name:
                continue
            # A column per port type, as Calix's M&T guide describes ("the G (RG) and F (FB) Ports columns"),
            # or, failing that, one cell listing them ("4g 2p 1G")
            counts = parse.port_columns(row) or parse.port_counts(parse.value(row, "ports"))
            caps = (parse.value(row, "capabilities") or "").split()
            profiles.append(OntProfile(
                id=name, name=name, model=parse.value(row, "model"), vendor=parse.value(row, "vendor"),
                # A port type the list leaves out is read as none of it, once the list has been read at all
                ge_ports=counts.get("g", 0) if counts else None, fe_ports=counts.get("f", 0) if counts else None,
                pots_ports=counts.get("p", 0) if counts else None, rg_ports=counts.get("G", 0) if counts else None,
                default_to_rg=("rg" in caps) if caps or counts else None))
        return profiles

    async def list_vlans(self, network: str) -> List[Vlan]:
        rows = self._audit("show vlan", parse.table(await self._show(network, "show vlan")))
        vlans = []
        for row in rows:
            vlan = parse.number(row, "outer-vlan") or parse.number(row, "ont") or parse.number(row, "name")
            if vlan is not None:
                name = parse.value(row, "name")
                vlans.append(Vlan(id=vlan, name=None if name == str(vlan) else name))
        return vlans

    # --- Writes ------------------------------------------------------------------------

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        ont_id = spec.ont_id or await self._free_ont_id(network)
        command = (f"create ont {ont_id} profile {arg(spec.profile)} "
                   f"serial-number {arg(e7_serial(spec.serial_number))}")
        if spec.subscriber_id:
            command += f" subscriber-id {arg(limited(spec.subscriber_id, 'an ONT subscriber ID', ONT_SUBSCRIBER_ID_MAX))}"
        if spec.description:
            command += f" description {arg(limited(spec.description, 'an ONT description', ONT_DESCRIPTION_MAX))}"
        await self._change(network, command)
        created = await self._ont_detail(network, ont_id)
        if created is None:
            raise NbiError(f"The E7 accepted create ont {ont_id} but does not show the ONT afterwards.")
        return created

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        if not service.tag_action:
            raise NbiRejectedError(f"Service on VLAN {service.vlan} has no tag_action. The E7 needs a service "
                                   f"tag action (svc-tag-action) for every Ethernet service.")
        if service.us_pir_override or service.ds_pir_override:
            raise NbiNotVerifiedError("Peak-rate overrides have no documented E7 CLI parameter on add eth-svc. "
                                      "Nothing was sent. Use a bandwidth profile with the rate instead.")
        attachment = Attachment(service.attach_to)
        port = self._ont_port(ont_id, attachment, service.ont_port)
        used = {s.service_index for s in await self._services(network, ont_id)
                if (s.attachment, s.ont_port) == (attachment, service.ont_port)}
        if len(used) >= MAX_SERVICES[attachment]:
            where = "an RG interface" if attachment == Attachment.ROUTER else "an ONT Ethernet port"
            raise NbiConflictError(f"ONT port {port} already has {len(used)} services; the E7 allows at most "
                                   f"{MAX_SERVICES[attachment]} on {where}.")
        free = [i for i in range(1, len(self.service_names) + 1) if i not in used]
        if not free:
            raise NbiConflictError(f"ONT port {port} already has a service under every name in E7_SERVICE_NAMES.")
        await self._change(network, _add_command(
            self.service_names[free[0] - 1], port, bw_profile=service.bandwidth_profile,
            tag_action=service.tag_action, outer_vlan=service.vlan, description=service.description))

    async def update_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int, change: ServiceChange) -> None:
        """
        Removes the service and adds it again under the same name with the new
        values, rather than set ont-port ... eth-svc, whose sub-options the CLI
        reference does not lay out clearly. Every value read back that add
        eth-svc takes is carried over. If the add is refused, the old service is
        added back before the refusal is raised.
        """
        if change.us_pir_override or change.ds_pir_override:
            raise NbiNotVerifiedError("Peak-rate overrides have no documented E7 CLI parameter. Nothing was sent.")
        if change.admin_state:
            _service_admin(change.admin_state)
        port = self._ont_port(ont_id, attachment, ont_port)
        current, row = await self._find_service(network, ont_id, attachment, ont_port, service_index)
        name = self._name_of(current, ont_id)
        keep = dict(bw_profile=current.bandwidth_profile, tag_action=current.tag_action, outer_vlan=current.vlan,
                    inner_vlan=current.inner_vlan, description=current.description,
                    mcast_profile=parse.value(row, "mcast-profile"), pon_cos=parse.value(row, "pon-cos"),
                    admin_state=current.admin_state.lower())
        new = dict(keep)
        for key, value in (("bw_profile", change.bandwidth_profile), ("tag_action", change.tag_action),
                           ("outer_vlan", change.vlan), ("admin_state", change.admin_state)):
            if value is not None:
                new[key] = value
        if new == keep:
            return
        if not keep["bw_profile"] or not keep["tag_action"]:
            raise NbiRejectedError(f"Service {service_index} on {port} reads back without a bandwidth profile or tag "
                                   f"action, so it could not be added back if the change failed. Nothing was sent.")
        # All three commands are built, and so checked, before anything is sent
        remove = f"remove eth-svc {arg(name)} from-ont-port {port}"
        add = _add_command(name, port, **new)
        restore = _add_command(name, port, **keep)
        await self._change(network, remove)
        try:
            await self._change(network, add)
        except NbiError:
            try:
                await self._change(network, restore)
            except NbiError as e:
                logger.error(f"E7 {network}: service {name} on {port} was removed, its change was refused, "
                             f"and adding it back failed too: {e}")
            raise

    async def delete_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                             service_index: int) -> None:
        current, _ = await self._find_service(network, ont_id, attachment, ont_port, service_index)
        name = self._name_of(current, ont_id)
        await self._change(network, f"remove eth-svc {arg(name)} from-ont-port {self._ont_port(ont_id, attachment, ont_port)}")

    async def update_port(self, network: str, ont_id: int, attachment: Attachment, number: int,
                          change: PortChange) -> None:
        values = change.model_dump(exclude_none=True)
        if "mode" in values:
            raise NbiNotVerifiedError(MODE_NOT_SETTABLE.replace("CMS API", "E7 CLI backend").replace(
                "CMS screens", "CMS screens or on the E7"))
        port = self._ont_port(ont_id, attachment, number)
        if Attachment(attachment) == Attachment.ROUTER and set(values) - {"admin_state"}:
            raise NbiRejectedError("Only the admin state of a router port can be changed.")
        parts = []
        if "admin_state" in values:
            parts.append(f"admin-state {_port_admin(values['admin_state'])}")
        if "description" in values:
            parts.append(f"description {arg(values['description'])}")
        if "subscriber_id" in values:
            parts.append(f"subscriber-id {arg(values['subscriber_id'])}")
        if "security_profile" in values:
            parts.append(f"eth-sec-profile {arg(values['security_profile'])}")
        if parts:
            await self._change(network, f"set ont-port {port} {' '.join(parts)}")

    async def delete_ont(self, network: str, ont_id: int) -> None:
        # Unlink it from its PON first (Calix, set ont: "pon-port ... none to unlink this ONT from the current
        # physical location"), then "forced - Deletes the ONT, even if ports are non-default or services exist"
        await self._change(network, f"set ont {ont_id} pon-port none")
        await self._change(network, f"delete ont {ont_id} forced")

    async def close(self) -> None:
        for session in self.sessions.values():
            await session.close()

    # --- Helpers -----------------------------------------------------------------------

    def _session(self, network: str) -> E7Session:
        session = self.sessions.get(network)
        if session is None:
            raise UnknownE7Error(f"No E7 named {network} is configured (E7_NODES).")
        return session

    async def _show(self, network: str, command: str) -> str:
        """A read. Output that looks like an error and holds no field this backend knows is an error."""
        output = await self._session(network).run(command)
        rows = parse.table(output) if _is_table(output) else parse.records(output)
        if self.error.search(output) and not any(parse.known(key) for row in rows for key in row):
            self._raise(network, command, output)
        return output

    async def _change(self, network: str, command: str) -> None:
        """A write. The CLI prints nothing on success, as far as is known; anything error-like is a refusal."""
        if len(command) > CLI_MAX_LINE:
            raise NbiRejectedError("The command is longer than the E7 command line accepts.")
        output = await self._session(network).run(command)
        if self.error.search(output):
            self._raise(network, command, output)
        if output.strip():
            logger.info(f"E7 {network}: {command!r} printed {output.strip()!r}")

    def _raise(self, network: str, command: str, output: str) -> None:
        text = " ".join(output.split())[:500]
        message = f"E7 {network} refused '{command}': {text}"
        if NOT_FOUND.search(output):
            raise NbiNotFoundError(message)
        if CONFLICT.search(output):
            raise NbiConflictError(message)
        raise NbiRejectedError(message)

    async def _ont_detail(self, network: str, ont_id: int) -> Optional[OntState]:
        try:
            output = await self._show(network, f"show ont {ont_id} detail")
        except UnknownE7Error:
            raise
        except NbiNotFoundError:
            return None
        row = parse.merged(output)
        self._audit("show ont detail", [row])
        if not parse.value(row, "serial-number") and parse.number(row, "ont") is None:
            return None
        row.setdefault("ont", str(ont_id))
        return self._ont_state(row)

    async def _ont_ids(self, network: str, command: str) -> List[int]:
        """ONT IDs listed by a show ont filter, as a table or as label/value blocks"""
        try:
            output = await self._show(network, command)
        except UnknownE7Error:
            raise
        except NbiNotFoundError:
            return []
        rows = parse.table(output) if _is_table(output) else parse.records(output)
        ids = [parse.number(row, "ont") for row in self._audit(" ".join(command.split()[:3]), rows)]
        return [i for i in dict.fromkeys(ids) if i is not None]

    async def _free_ont_id(self, network: str) -> int:
        used = {ont.ont_id for ont in await self.list_onts(network)}
        return next(n for n in range(1, len(used) + 2) if n not in used)

    async def _services(self, network: str, ont_id: int) -> List[ProvisionedService]:
        return [service for service, _ in await self._service_rows(network, ont_id)]

    async def _service_rows(self, network: str, ont_id: int) -> List[Tuple[ProvisionedService, Dict[str, str]]]:
        """Each service, with the row it was read from"""
        try:
            output = await self._show(network, f"show ont-port {ont_id} eth-svc")
        except UnknownE7Error:
            raise
        except NbiNotFoundError:
            return []
        rows = parse.table(output) if _is_table(output) else parse.records(output)
        services = []
        extra = 100
        for row in self._audit("show ont-port eth-svc", rows):
            port, name = _port(parse.value(row, "port")), parse.value(row, "name")
            if port is None or not name:
                continue
            letter, number = port
            if name in self.service_names:
                index = self.service_names.index(name) + 1
            else:
                extra += 1  # a name outside E7_SERVICE_NAMES: readable, numbered from 101 in listed order
                index = extra
            services.append((ProvisionedService(
                service_index=index, ont_port=number, ont_slot=letter,
                attachment={"g": Attachment.ETHERNET, "G": Attachment.ROUTER}.get(letter, Attachment.UNKNOWN),
                vlan=parse.number(row, "outer-vlan"), inner_vlan=parse.number(row, "inner-vlan"),
                bandwidth_profile=parse.value(row, "bw-profile"), tag_action=parse.value(row, "svc-tag-action"),
                admin_state=parse.value(row, "admin-state") or "enabled", description=parse.value(row, "description")),
                row))
        return services

    async def _find_service(self, network: str, ont_id: int, attachment: Attachment, ont_port: int,
                            service_index: int) -> Tuple[ProvisionedService, Dict[str, str]]:
        """A service as read back, so a service outside E7_SERVICE_NAMES is found too"""
        for service, row in await self._service_rows(network, ont_id):
            if (service.attachment, service.ont_port, service.service_index) == (
                    Attachment(attachment), ont_port, service_index):
                return service, row
        raise NbiNotFoundError(f"ONT {ont_id} has no service {service_index} on {port_name(attachment, ont_port)}.")

    def _name_of(self, service: ProvisionedService, ont_id: int) -> str:
        """The E7's name for a service read back"""
        if service.service_index <= len(self.service_names):
            return self.service_names[service.service_index - 1]
        raise NbiRejectedError(f"Service {service.service_index} on ONT {ont_id} has a name outside "
                               f"E7_SERVICE_NAMES; change it on the E7 or add its name to E7_SERVICE_NAMES.")

    def _ont_port(self, ont_id: int, attachment: Attachment, number: int) -> str:
        attachment = Attachment(attachment)
        if attachment == Attachment.ETHERNET:
            return f"{ont_id}/g{number}"
        if attachment == Attachment.ROUTER:
            return f"{ont_id}/G{number}"  # Calix: "ONT RG (Residential Gateway) Interface Designated as 'G1'"
        raise NbiRejectedError("A service on an unrecognised ONT port cannot be addressed.")

    def _ont_state(self, row: Dict[str, str]) -> OntState:
        oper = (parse.value(row, "oper-state") or "").lower()
        self.observed["oper-state"].add(oper or "(empty)")
        return OntState(
            ont_id=parse.number(row, "ont"), serial_number=_serial(row),
            model=parse.value(row, "model"), profile=parse.value(row, "profile"),
            admin_state=parse.value(row, "admin-state") or "enabled",
            oper_state=OperState.ONLINE if oper in ONLINE_WORDS else OperState.OFFLINE,
            pon_port=_pon(row), description=parse.value(row, "description"),
            subscriber_id=parse.value(row, "subscriber-id"), registration_id=parse.value(row, "reg-id"))

    def _audit(self, command: str, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        self.unrecognised[command].update(parse.unrecognised(rows))
        return rows


def _is_table(output: str) -> bool:
    lines = [line for line in output.splitlines() if line.strip()]
    return len(lines) > 1 and bool(parse.RULE.match(lines[1]))


def _serial(row: Dict[str, str]) -> str:
    """The serial as this API writes it (CXNK00A3F104), from CXNK00A3F104 or 00A3F104 plus a vendor field"""
    raw = parse.value(row, "serial-number") or "0"
    vendor = raw[:4] if raw[:4].isalpha() else (parse.value(row, "vendor") or "")
    return from_serno(to_serno(raw), vendor if vendor.isalpha() and len(vendor) == 4 else "")


def _pon(row: Dict[str, str]) -> Optional[str]:
    """'1/2/gp3' -> '1/2/3'. Anything without a shelf, card and port is returned as the E7 printed it."""
    text = parse.value(row, "pon-port")
    numbers = re.findall(r"\d+", text or "")
    return "/".join(numbers) if len(numbers) == 3 and re.fullmatch(r"[\w/ -]+", text) else text


def _port(text: Optional[str]) -> Optional[tuple]:
    """'101/g2' or 'g2' -> ('g', 2)"""
    match = re.search(r"(?:^|/)([gGFhfrRpdt])(\d+)$", (text or "").strip())
    return (match.group(1), int(match.group(2))) if match else None


def _port_admin(state: str) -> str:
    """Calix, set ont-port: admin-state "Valid values: enabled, disabled". no-alarms is for the ONT itself."""
    if state not in ("enabled", "disabled"):
        raise NbiRejectedError(f"An ONT port's admin state on the E7 is enabled or disabled, not {state}. "
                               f"Nothing was sent.")
    return state


def _service_admin(state: str) -> str:
    """Calix, add eth-svc and set ont-port eth-svc: admin-state "Valid values: enabled, disabled" """
    if state not in ("enabled", "disabled"):
        raise NbiRejectedError(f"A service's admin state on the E7 is enabled or disabled, not {state}. "
                               f"Nothing was sent.")
    return state


def _add_command(name: str, port: str, *, bw_profile: str, tag_action: str, outer_vlan: Optional[int] = None,
                 inner_vlan: Optional[int] = None, description: Optional[str] = None,
                 mcast_profile: Optional[str] = None, pon_cos: Optional[str] = None,
                 admin_state: Optional[str] = None) -> str:
    """add eth-svc <name> to-ont-port <port> bw-profile <bw> svc-tag-action <ta> [outer-vlan inner-vlan
    mcast-profile description pon-cos admin-state], per Calix's syntax line"""
    command = f"add eth-svc {arg(name)} to-ont-port {port} bw-profile {arg(bw_profile)} svc-tag-action {arg(tag_action)}"
    if outer_vlan is not None:
        command += f" outer-vlan {int(outer_vlan)}"
    if inner_vlan is not None:
        command += f" inner-vlan {int(inner_vlan)}"
    if mcast_profile:
        command += f" mcast-profile {arg(mcast_profile)}"
    if description:
        command += f" description {arg(description)}"
    if pon_cos:
        command += f" pon-cos {arg(pon_cos)}"
    if admin_state is not None:
        command += f" admin-state {_service_admin(admin_state)}"
    return command


def nodes_from_settings(settings) -> List[E7Node]:
    """E7_NODES=NAME=host[:port],... with the shared E7_USERNAME, E7_PASSWORD and E7_KNOWN_HOSTS"""
    from app.models.provisioning_model import NETWORK_PATTERN

    nodes = []
    for entry in (settings.e7_nodes or "").split(","):
        if not entry.strip():
            continue
        name, sep, address = entry.strip().partition("=")
        host, _, port = address.strip().rpartition(":") if ":" in address else (address.strip(), "", "22")
        if not sep or not host or not port.isdigit() or not re.match(NETWORK_PATTERN, name.strip()):
            raise ValueError(f"E7_NODES entry {entry.strip()!r} is not NAME=host or NAME=host:port")
        nodes.append(E7Node(name=name.strip(), host=host, port=int(port), username=settings.e7_username,
                            password=settings.e7_password or "", known_hosts=settings.e7_known_hosts))
    if len({n.name for n in nodes}) != len(nodes):
        raise ValueError("E7_NODES names the same E7 twice")
    return nodes


def backend_from_settings(settings) -> E7CliBackend:
    split = lambda text: tuple(part.strip() for part in (text or "").split(",") if part.strip())
    return E7CliBackend(
        nodes_from_settings(settings), prompt=settings.e7_prompt_pattern,
        pager_command=settings.e7_pager_command or None, session_commands=split(settings.e7_session_commands),
        error_pattern=settings.e7_error_pattern, service_names=split(settings.e7_service_names),
        connect_timeout=settings.nbi_timeout_seconds)
