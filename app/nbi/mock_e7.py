"""
Mock E7
A stand-in for an E7 EXA node's command line, for testing the E7 CLI
backend (app/nbi/e7_backend.py) over real SSH.

The commands it accepts follow Calix's public E7 EXA R3.x CLI reference.
WHAT IT PRINTS IS MOSTLY INVENTED: Calix publishes almost no sample output.
Its lists are tables laid out like the one sample in the E7 CLI reference
(show mcast): header words one space apart, and a line of dashes under the
header, one run per column, columns one space apart. The detail views
("Label : value") and the refusals ("Error: ...") are this project's guess. Passing tests against this mock says
the backend and the mock agree, not that a real E7 prints this. Replace the
layouts with captures from a real E7 (python -m app.tools.e7_capture).

Run it on its own to point the API at it:
    python -m app.nbi.mock_e7 --port 2222 [--pon-size 64]
    E7_NODES=E7-MOCK=127.0.0.1:2222 E7_USERNAME=e7 E7_PASSWORD=e7 E7_KNOWN_HOSTS=none python main.py
"""

import argparse
import asyncio
import shlex
from typing import Dict, List, Optional, Set, Tuple

from app.nbi.mock_pon import EXTRA_BW_PROFILES, EXTRA_VLANS, check_pon_size, sim_onts

NODE = "E7-MOCK"
PROMPT = f"{NODE}>"
PROFILES = {  # name -> (model, ports, capabilities). Counts invented, not Calix's figures.
    "716GE-I": ("716GE-I", "2g 2p", ""),
    "812G-1": ("812G", "2g 2p", ""),
    "844G-1": ("844G", "4g 2p 1G", "rg"),
}
VLANS = {100: "data", 200: "voice", 300: "video"}
BW_PROFILES = ("RES-1G", "VOICE")
TAG_ACTIONS = ("ADD-OUTER", "MOCK-NO-VLAN")  # MOCK-NO-VLAN takes no outer-vlan (a simulation rule)
SERVICE_NAMES = tuple(f"Data{n}" for n in range(1, 9))


class MockE7:
    def __init__(self, pon_size: int = 0) -> None:
        """pon_size: fill PON 1/1/1 with that many ONTs in all (1:32 or 1:64), the rest discovered and unprovisioned"""
        self.onts: Dict[int, Dict[str, str]] = {}
        self.services: Dict[int, List[Dict[str, str]]] = {}
        self.ports: Dict[Tuple[int, str], Dict[str, str]] = {}
        # Phone lines per ONT. Never shown by show ont-port eth-svc (the backend does not read voice), but a
        # delete without forced is refused while one is there, so tests can show voice is not deleted unseen
        self.voice_lines: Dict[int, int] = {}
        # Physically present on a PON: serial -> (model, pon)
        self.present: Dict[str, Tuple[str, str]] = {
            "00A3F104": ("844G", "1/1/1"), "00B2C201": ("812G", "1/1/2"), "00C0FFEE": ("716GE-I", "1/2/1")}
        self.commands: List[str] = []
        self.idle_timeout: Optional[float] = None  # seconds before an idle session is closed, as a real E7 can
        self.hang_up_on: Optional[str] = None  # a command start: the session closes on it, unanswered
        self.hang_up_once = False  # clear hang_up_on after the first hang-up
        self.hang_up_after_run = False  # run the command first, then hang up without answering
        self.latency = 0.0  # seconds before each answer, to stand in for a real E7's response time
        self.idle_closes = 0  # sessions closed by close_idle_sessions()
        self._idle: Set[asyncio.Event] = set()  # one per session waiting for a command; set it to close the session
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.vlans = dict(VLANS)
        self.bw_profiles = BW_PROFILES
        self.logins = 0
        self._create(101, "812G-1", "00B2C201", "SUB-1001", "Smith residence")
        self.services[101] = [{"port": "g1", "name": "Data1", "bw": "RES-1G", "ta": "ADD-OUTER", "outer": "100",
                               "inner": "none", "admin": "enabled", "desc": ""}]
        if check_pon_size(pon_size):
            on_pon = sum(pon == "1/1/1" for _, pon in self.present.values())
            for serial, profile in sim_onts(max(0, pon_size - on_pon)):
                self.present[serial] = (PROFILES[profile][0], "1/1/1")
            self.vlans.update(EXTRA_VLANS)
            self.bw_profiles = BW_PROFILES + EXTRA_BW_PROFILES

    def close_idle_sessions(self) -> None:
        """
        Close every session that is waiting for its next command, as an E7 does when its inactivity
        timeout runs out. Safe to call from another thread.
        """
        def close() -> None:
            for closer in list(self._idle):
                if not closer.is_set():
                    closer.set()
                    self.idle_closes += 1
        if self._loop is not None:
            self._loop.call_soon_threadsafe(close)

    # --- Dispatch ------------------------------------------------------------------

    def run(self, line: str) -> str:
        self.commands.append(line)
        try:
            words = shlex.split(line)
        except ValueError:
            return "Error: Invalid input detected at '^' marker."
        if not words:
            return ""
        try:
            return self._dispatch(words)
        except _Refused as e:
            return f"Error: {e}"

    def _dispatch(self, w: List[str]) -> str:
        if w[:2] == ["set", "session"]:
            return ""
        if w[:2] == ["show", "ont"] and len(w) >= 3:
            if w[2:] == ["discovered", "detail"]:
                return self._discovered()
            if w[2:] == ["detail"]:
                return "\n\n".join(self._ont_detail(i) for i in sorted(self.onts))
            if w[2] in ("serial-number", "subscriber-id") and len(w) == 4:
                key = "serial" if w[2] == "serial-number" else "subscriber"
                hits = [i for i, o in self.onts.items() if o[key].lower() == w[3].lower()]
                return _table(["ONT", "Serial Number", "Subscriber ID", "Profile"],
                              [[str(i), self.onts[i]["serial"], self.onts[i]["subscriber"], self.onts[i]["profile"]]
                               for i in hits])
            if w[2].isdigit() and w[3:] == ["detail"]:
                return self._ont_detail(self._ont(w[2]))
        if w[:2] == ["show", "ont-port"] and len(w) == 4 and w[2].isdigit():
            ont = self._ont(w[2])
            if w[3] == "eth-svc":
                return _table(["Port", "Service", "BW Profile", "Svc Tag Action", "Outer VLAN", "Inner VLAN",
                               "Admin State", "Description"],
                              [[f"{ont}/{s['port']}", s["name"], s["bw"], s["ta"], s["outer"], s["inner"], s["admin"],
                                s["desc"] or "-"] for s in self.services.get(ont, [])])
            if w[3] == "detail":
                return "\n\n".join(self._port_detail(ont, p) for p in self._ont_ports(ont))
        if w[:2] == ["show", "ont-profile"] and len(w) == 2:
            # A column per port type (Calix's M&T guide: 'the "G" (RG) and "F" (FB) Ports columns')
            letters = ["g", "p", "G", "F"]
            counts = {n: {c[-1]: c[:-1] for c in p.split()} for n, (_, p, _) in PROFILES.items()}
            return _table(["Name", "Model"] + letters + ["Capabilities"],
                          [[n, m] + [counts[n].get(x, "0") for x in letters] + [c or "-"]
                           for n, (m, _, c) in PROFILES.items()])
        if w[:2] == ["show", "vlan"] and len(w) == 2:
            return _table(["VLAN", "Name"], [[str(v), n] for v, n in self.vlans.items()])
        if w[:2] == ["create", "ont"] and len(w) >= 3:
            return self._create_cmd(w[2], _options(w[3:]))
        if w[:2] == ["set", "ont"] and len(w) >= 3:
            ont = self._ont(w[2])
            for key, val in _options(w[3:]).items():
                if key == "pon-port" and val == "none":
                    self.onts[ont]["linked"] = ""
                elif key == "pon-port":  # link it again, to the PON its serial number is seen on
                    serial = self.onts[ont]["serial"]
                    if self.present.get(serial, (None, None))[1] != val:
                        raise _Refused(f"ONT {ont} (serial {serial}) is not discovered on PON port {val}.")
                    self.onts[ont]["linked"] = serial
                elif key in ("subscriber-id", "description"):
                    self._check_length(key, val)
                    self.onts[ont][{"subscriber-id": "subscriber"}.get(key, key)] = val
                elif key == "admin-state" and val in ("enabled", "no-alarms", "disabled"):
                    self.onts[ont]["admin"] = val
                else:
                    raise _Refused(f"Invalid input detected at '^' marker ({key}).")
            return ""
        if w[:2] == ["delete", "ont"] and len(w) in (3, 4):
            ont = self._ont(w[2])
            # Calix's M&T guide: an ONT can only be deleted once its record is unlinked from the discovered ONT
            if self.onts[ont]["linked"]:
                raise _Refused(f"ONT {ont} is linked to a discovered ONT; unlink it first (set ont {ont} pon-port none).")
            if w[3:] != ["forced"] and (self.services.get(ont) or self.voice_lines.get(ont)):
                raise _Refused(f"ONT {ont} has services; remove them first or use forced.")
            del self.onts[ont]
            self.services.pop(ont, None)
            self.voice_lines.pop(ont, None)
            for key in [k for k in self.ports if k[0] == ont]:
                del self.ports[key]  # a new ONT under the same ID starts with default ports
            return ""
        if w[:2] == ["add", "eth-svc"] and len(w) >= 5 and w[3] == "to-ont-port":
            return self._add_service(w[2], w[4], _options(w[5:]))
        if w[:2] == ["remove", "eth-svc"] and len(w) == 5 and w[3] == "from-ont-port":
            ont, port = self._port(w[4])
            before = len(self.services.get(ont, []))
            self.services[ont] = [s for s in self.services.get(ont, []) if (s["port"], s["name"]) != (port, w[2])]
            if len(self.services[ont]) == before:
                raise _Refused(f"Service {w[2]} does not exist on {w[4]}.")
            return ""
        if w[:2] == ["set", "ont-port"] and len(w) >= 3:
            ont, port = self._port(w[2])
            if len(w) >= 5 and w[3] == "eth-svc":
                svc = next((s for s in self.services.get(ont, []) if (s["port"], s["name"]) == (port, w[4])), None)
                if svc is None:
                    raise _Refused(f"Service {w[4]} does not exist on {w[2]}.")
                for key, val in _options(w[5:]).items():
                    field = {"bw-profile": "bw", "svc-tag-action": "ta", "outer-vlan": "outer",
                             "admin-state": "admin"}.get(key)
                    if field == "admin":
                        self._check_admin(val)
                    if field is None:
                        raise _Refused(f"Invalid input detected at '^' marker ({key}).")
                    self._check(field, val)
                    svc[field] = val
                return ""
            settings = self.ports.setdefault((ont, port), {})
            for key, val in _options(w[3:]).items():
                if key not in ("admin-state", "description", "subscriber-id", "eth-sec-profile"):
                    raise _Refused(f"Invalid input detected at '^' marker ({key}).")
                if key == "admin-state":
                    self._check_admin(val)
                settings[key] = val
            return ""
        raise _Refused("Invalid input detected at '^' marker.")

    # --- Commands --------------------------------------------------------------------

    def _create(self, ont: int, profile: str, serial: str, subscriber: str, description: str) -> None:
        self.onts[ont] = {"profile": profile, "serial": serial, "subscriber": subscriber,
                          "description": description, "admin": "enabled", "linked": serial}

    def _create_cmd(self, ont_text: str, opts: Dict[str, str]) -> str:
        if not ont_text.isdigit() or not 1 <= int(ont_text) <= 64000000:
            raise _Refused("Invalid input detected at '^' marker.")
        ont = int(ont_text)
        if ont in self.onts:
            raise _Refused(f"ONT {ont} already exists.")
        if opts.get("profile") not in PROFILES:
            raise _Refused(f"ONT profile {opts.get('profile')} does not exist.")
        serial = opts.get("serial-number", "").upper()
        if any(o["serial"] == serial for o in self.onts.values()):
            raise _Refused(f"Serial number {serial} is already in use.")
        for key in ("subscriber-id", "description"):
            self._check_length(key, opts.get(key, ""))
        self._create(ont, opts["profile"], serial, opts.get("subscriber-id", ""), opts.get("description", ""))
        return ""

    def _add_service(self, name: str, port_text: str, opts: Dict[str, str]) -> str:
        ont, port = self._port(port_text)
        if name not in SERVICE_NAMES:
            raise _Refused(f"Service name {name} does not exist.")
        if any((s["port"], s["name"]) == (port, name) for s in self.services.get(ont, [])):
            raise _Refused(f"Service {name} already exists on {port_text}.")
        if port.startswith("G") and sum(s["port"] == port for s in self.services.get(ont, [])) >= 4:
            raise _Refused(f"{port_text} already has the most services a residential gateway port takes.")
        if "admin-state" in opts:
            self._check_admin(opts["admin-state"])
        for field, key in (("bw", "bw-profile"), ("ta", "svc-tag-action")):
            if key not in opts:
                raise _Refused(f"{key} is required.")
            self._check(field, opts[key])
        if opts["svc-tag-action"] == "MOCK-NO-VLAN" and "outer-vlan" in opts:
            raise _Refused("The service tag action does not take an outer VLAN.")
        if opts["svc-tag-action"] != "MOCK-NO-VLAN":
            self._check("outer", opts.get("outer-vlan", ""))
        self.services.setdefault(ont, []).append({
            "port": port, "name": name, "bw": opts["bw-profile"], "ta": opts["svc-tag-action"],
            "outer": opts.get("outer-vlan", "none"), "inner": opts.get("inner-vlan", "none"),
            "admin": opts.get("admin-state", "enabled"), "desc": opts.get("description", "")})
        return ""

    @staticmethod
    def _check_admin(val: str) -> None:
        if val not in ("enabled", "disabled"):  # no-alarms is for the ONT, not its ports or services
            raise _Refused(f"Invalid input detected at '^' marker ({val}).")

    @staticmethod
    def _check_length(key: str, val: str) -> None:
        most = {"subscriber-id": 63, "description": 47}[key]
        if len(val) > most:
            raise _Refused(f"{key} is limited to {most} characters.")

    def _check(self, field: str, val: str) -> None:
        if field == "bw" and val not in self.bw_profiles:
            raise _Refused(f"Bandwidth profile {val} does not exist.")
        if field == "ta" and val not in TAG_ACTIONS:
            raise _Refused(f"Service tag action {val} does not exist.")
        if field == "outer" and (not val.isdigit() or int(val) not in self.vlans):
            raise _Refused(f"VLAN {val} does not exist.")

    # --- Views -----------------------------------------------------------------------

    def _ont_detail(self, ont: int) -> str:
        o = self.onts[ont]
        model, pon = self.present.get(o["linked"], (PROFILES[o["profile"]][0], None))
        return _detail([("ONT", str(ont)), ("Profile", o["profile"]), ("Serial Number", f"CXNK{o['serial']}"),
                        ("Subscriber ID", o["subscriber"] or "-"), ("Description", o["description"] or "-"),
                        ("Admin State", o["admin"]), ("Oper State", "present" if pon else "missing"),
                        ("Model", model), ("PON Port", pon or "-")])

    def _discovered(self) -> str:
        provisioned = {o["serial"] for o in self.onts.values()}
        return "\n\n".join(_detail([("Serial Number", f"CXNK{s}"), ("Model", m), ("PON Port", p)])
                           for s, (m, p) in self.present.items() if s not in provisioned)

    def _ont_ports(self, ont: int) -> List[str]:
        counts = PROFILES[self.onts[ont]["profile"]][1].split()
        return [f"{c[-1]}{n}" for c in counts if c[-1] in "gG" for n in range(1, int(c[:-1]) + 1)]

    def _port_detail(self, ont: int, port: str) -> str:
        s = self.ports.get((ont, port), {})
        return _detail([("Port", f"{ont}/{port}"), ("Admin State", s.get("admin-state", "enabled")),
                        ("Subscriber ID", s.get("subscriber-id", "-")), ("Description", s.get("description", "-")),
                        ("Eth Sec Profile", s.get("eth-sec-profile", "-"))])

    def _ont(self, text: str) -> int:
        if not text.isdigit() or int(text) not in self.onts:
            raise _Refused(f"ONT {text} does not exist.")
        return int(text)

    def _port(self, text: str) -> Tuple[int, str]:
        ont_text, _, port = text.partition("/")
        ont = self._ont(ont_text)
        if port not in self._ont_ports(ont):
            raise _Refused(f"ONT port {text} does not exist.")
        return ont, port


class _Refused(Exception):
    pass


def _options(words: List[str]) -> Dict[str, str]:
    if len(words) % 2:
        raise _Refused("Invalid input detected at '^' marker.")
    return dict(zip(words[::2], words[1::2]))


def _detail(pairs: List[Tuple[str, str]]) -> str:
    width = max(len(k) for k, _ in pairs)
    return "\n".join(f"  {k.ljust(width)} : {v}" for k, v in pairs)


def _table(header: List[str], rows: List[List[str]]) -> str:
    """Laid out like show mcast in Calix's E7 CLI reference: one space between columns, dashes under each"""
    widths = [max(len(c) for c in col) for col in zip(header, *rows)]
    line = lambda cells: " ".join(c.ljust(w) for c, w in zip(cells, widths)).rstrip()
    return "\n".join([line(header), line(["-" * w for w in widths])] + [line(r) for r in rows])


# --- SSH server ------------------------------------------------------------------------

async def serve(port: int = 0, *, username: str = "e7", password: str = "e7",
                e7: Optional[MockE7] = None, host: str = "127.0.0.1"):
    """Start an SSH server for one MockE7. Returns (server, port, e7)."""
    import asyncssh

    e7 = e7 or MockE7()

    class Server(asyncssh.SSHServer):
        def begin_auth(self, user: str) -> bool:
            return True

        def password_auth_supported(self) -> bool:
            return True

        def validate_password(self, user: str, pw: str) -> bool:
            return (user, pw) == (username, password)

    async def shell(process: "asyncssh.SSHServerProcess") -> None:
        e7.logins += 1
        e7._loop = asyncio.get_running_loop()
        process.stdout.write(f"Calix E7 (mock)\n{PROMPT}")
        try:
            while True:
                # Wait for a command; give up on the idle timeout or when close_idle_sessions() is called
                closer = asyncio.Event()
                reader = asyncio.ensure_future(process.stdin.readline())
                closed = asyncio.ensure_future(closer.wait())
                e7._idle.add(closer)
                try:
                    await asyncio.wait({reader, closed}, timeout=e7.idle_timeout,
                                       return_when=asyncio.FIRST_COMPLETED)
                finally:
                    e7._idle.discard(closer)
                    closed.cancel()
                if not reader.done():
                    reader.cancel()
                    process.stdout.write("\nSession timed out.\n")
                    break
                line = reader.result()
                if not line:
                    break
                command = line.strip()
                if command in ("exit", "logout", "quit"):
                    break
                if e7.hang_up_on and command.startswith(e7.hang_up_on):
                    e7.run(command) if e7.hang_up_after_run else e7.commands.append(command)
                    if e7.hang_up_once:
                        e7.hang_up_on = None
                    break
                output = e7.run(command) if command else ""
                if e7.latency:
                    await asyncio.sleep(e7.latency)
                process.stdout.write((output + "\n" if output else "") + PROMPT)
        except (asyncssh.BreakReceived, asyncssh.TerminalSizeChanged, ConnectionError):
            pass
        process.exit(0)

    key = asyncssh.generate_private_key("ssh-ed25519")
    server = await asyncssh.create_server(Server, host, port, server_host_keys=[key], process_factory=shell)
    return server, server.sockets[0].getsockname()[1], e7


def main() -> None:  # pragma: no cover - manual use
    parser = argparse.ArgumentParser(description="Mock E7 EXA command line over SSH (invented output)")
    parser.add_argument("--port", type=int, default=2222)
    parser.add_argument("--pon-size", type=int, default=0, help="fill PON 1/1/1 with this many ONTs (up to 64)")
    args = parser.parse_args()

    async def run() -> None:
        _, port, _ = await serve(args.port, e7=MockE7(pon_size=args.pon_size))
        print(f"Mock E7 listening on 127.0.0.1:{port} (user e7, password e7)")
        await asyncio.Event().wait()

    asyncio.run(run())


if __name__ == "__main__":
    main()
