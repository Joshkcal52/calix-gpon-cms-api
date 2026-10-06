"""
Mock Calix E7-20
A simulated chassis so the API can be developed without lab hardware.

SSHController uses MockE7Connection when MOCK_MODE=true. It is a drop-in for
the Netmiko connection object (send_command / disconnect), so the controllers
and routes run exactly the same code they run against a real device. No
network socket is ever opened.

IMPORTANT: the output formats below are approximations. They cover the
commands the controllers send, the commands listed in notes.md, and they are
shaped so the regexes in app/utils/helpers.py can read them. They have NOT
been checked against a real E7-20. Before trusting a parser on live gear,
capture real output and replace these templates with it.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MockONT:
    """One simulated ONT. Counters are fixed so results are repeatable."""
    ont_id: int
    pon: str  # shelf/card/port, e.g. "1/1/1"
    serial: str
    model: str
    description: str
    online: bool = True
    admin: str = "enabled"
    distance_km: float = 0.0
    tx_power: float = 2.0  # dBm
    rx_power: float = -18.0  # dBm
    temperature: float = 40.0  # C
    missed_bursts: int = 0
    bip_errors: int = 0
    crc_errors: int = 0
    fec_errors: int = 0
    alarms: Tuple[str, ...] = ()

    @property
    def ref(self) -> str:
        """ONT reference in the 1/1/1:4 form used by helpers.py"""
        return f"{self.pon}:{self.ont_id}"

    @property
    def status(self) -> str:
        return "online" if self.online else "offline"


# The scenario: PON 1/1/1 has a rogue ONT (4) transmitting outside its
# timeslot. Its own counters are off the chart and its neighbours on the same
# PON show collateral errors, two of them (2 and 7) past the default
# thresholds. ONT 6 is dark. PON 1/1/2 is healthy apart from one low-light
# ONT (12), which should NOT be flagged as rogue.
MOCK_ONTS: Tuple[MockONT, ...] = (
    MockONT(1, "1/1/1", "CXNK00A3F101", "716GE-I", "101 Alder St", distance_km=1.82,
            tx_power=2.3, rx_power=-16.9, temperature=41.5,
            missed_bursts=38, bip_errors=21, crc_errors=4, fec_errors=310),
    MockONT(2, "1/1/1", "CXNK00A3F102", "716GE-I", "105 Alder St", distance_km=2.05,
            tx_power=2.1, rx_power=-17.4, temperature=39.0,
            missed_bursts=142, bip_errors=64, crc_errors=12, fec_errors=890),
    MockONT(3, "1/1/1", "CXNK00A3F103", "844G-1", "112 Alder St", distance_km=2.31,
            tx_power=2.4, rx_power=-17.8, temperature=44.0,
            missed_bursts=57, bip_errors=33, crc_errors=6, fec_errors=402),
    MockONT(4, "1/1/1", "CXNK00A3F104", "844G-1", "118 Alder St", distance_km=2.47,
            tx_power=4.9, rx_power=-18.1, temperature=71.5,
            missed_bursts=4821, bip_errors=913, crc_errors=207, fec_errors=15344,
            alarms=("MAJOR  Upstream BIP error threshold crossed",
                    "MAJOR  Missed burst threshold crossed",
                    "MINOR  ONT temperature high")),
    MockONT(5, "1/1/1", "CXNK00A3F105", "716GE-I", "124 Alder St", distance_km=2.60,
            tx_power=2.2, rx_power=-18.3, temperature=40.5,
            missed_bursts=76, bip_errors=41, crc_errors=9, fec_errors=655),
    MockONT(6, "1/1/1", "CXNK00A3F106", "812G-1", "131 Alder St", online=False,
            distance_km=2.88, alarms=("MAJOR  ONT missing (loss of signal)",)),
    MockONT(7, "1/1/1", "CXNK00A3F107", "844G-1", "137 Alder St", distance_km=3.10,
            tx_power=2.0, rx_power=-19.0, temperature=42.0,
            missed_bursts=119, bip_errors=48, crc_errors=8, fec_errors=720),
    MockONT(8, "1/1/1", "CXNK00A3F108", "716GE-I", "140 Alder St", distance_km=3.35,
            tx_power=2.3, rx_power=-19.4, temperature=38.5,
            missed_bursts=22, bip_errors=15, crc_errors=2, fec_errors=198),
    MockONT(9, "1/1/2", "CXNK00B7C209", "716GE-I", "12 Birch Ave", distance_km=0.94,
            tx_power=2.2, rx_power=-15.8, temperature=37.0, fec_errors=3),
    MockONT(10, "1/1/2", "CXNK00B7C210", "844G-1", "18 Birch Ave", distance_km=1.12,
            tx_power=2.4, rx_power=-16.2, temperature=40.0, bip_errors=1, fec_errors=9),
    MockONT(11, "1/1/2", "CXNK00B7C211", "716GE-I", "25 Birch Ave", distance_km=1.40,
            tx_power=2.1, rx_power=-16.7, temperature=38.0),
    MockONT(12, "1/1/2", "CXNK00B7C212", "812G-1", "31 Birch Ave", distance_km=6.75,
            tx_power=2.6, rx_power=-27.6, temperature=45.5,
            bip_errors=7, crc_errors=1, fec_errors=2210,
            alarms=("MINOR  ONT receive power low",)),
    MockONT(13, "1/1/2", "CXNK00B7C213", "716GE-I", "40 Birch Ave", distance_km=1.77,
            tx_power=2.3, rx_power=-17.0, temperature=39.5, fec_errors=14),
    MockONT(14, "1/1/2", "CXNK00B7C214", "844G-1", "46 Birch Ave", distance_km=1.93,
            tx_power=2.2, rx_power=-17.3, temperature=41.0, missed_bursts=1, fec_errors=6),
)

# ProCurve-style port counters for the bare "show interface" command, which
# hp_controller.py parses (the project connects with device_type hp_procurve).
MOCK_SWITCH_PORTS: Tuple[Tuple[str, int, int, int, int], ...] = (
    # port, total bytes, total frames, errors rx, drops tx
    ("1", 3_482_190_774, 4_120_338, 0, 0),
    ("2", 912_004_551, 1_033_207, 0, 0),
    ("3", 0, 0, 0, 0),
    ("4", 58_733_102_946, 61_904_415, 12, 3),
    ("5", 7_204_118, 51_873, 0, 0),
    ("6", 0, 0, 0, 0),
    ("7", 1_299_640_087, 1_877_502, 0, 148),
    ("8", 24_118_530_662, 30_215_946, 2, 0),
)


def _table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Render a fixed-width CLI table: header, dashed rule, rows"""
    cells = [[str(c) for c in row] for row in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h)
              for i, h in enumerate(headers)]

    def line(values: Sequence[str]) -> str:
        return "  ".join(v.ljust(w) for v, w in zip(values, widths)).rstrip()

    return "\n".join([line(headers), line(["-" * w for w in widths])] + [line(r) for r in cells])


def _fields(pairs: Sequence[Tuple[str, object]], indent: str = "  ") -> str:
    """Render 'Label : value' lines, the shape helpers.py regexes expect"""
    width = max(len(label) for label, _ in pairs)
    return "\n".join(f"{indent}{label.ljust(width)} : {value}" for label, value in pairs)


def _dbm(value: float, ont: MockONT) -> str:
    return f"{value:.1f} dBm" if ont.online else "--"


class MockE7Connection:
    """
    Stand-in for a Netmiko connection to a Calix E7-20.

    Only the two methods SSHController uses are implemented: send_command()
    and disconnect(). Extra Netmiko keyword arguments (expect_string,
    delay_factor, ...) are accepted and ignored.
    """

    hostname = "E7-20-MOCK"

    def __init__(self, host: str = "mock"):
        self.host = host
        self._alive = True
        pon = r"(?:gpon-)?(?P<pon>\d+/\d+/\d+)"
        ont = r"(?P<ont>\S+)"
        # (pattern, handler) pairs; the command must match a pattern in full.
        self._commands: List[Tuple[re.Pattern, Callable[..., str]]] = [
            (re.compile(p), h) for p, h in (
                (r"show version", self._version),
                (r"show system clock", self._clock),
                (r"show user", self._users),
                (r"show interface", self._switch_ports),
                # Commands the ONT controller sends
                (r"show ont (?:info|status)", self._ont_table),
                (rf"show ont (?:info|status) {ont}", self._ont_detail),
                (rf"show ont performance {ont}", self._ont_performance),
                (r"show pon port status", self._pon_status),
                (rf"show pon port status {pon}", self._pon_status),
                # Commands from notes.md
                (r"show interface gpon", self._ont_table),
                (rf"show interface {pon}", self._ont_table),
                (rf"show interface {pon} ont {ont} statistics", self._ont_performance),
                (rf"show gpon pon-errors {pon}", self._pon_errors),
                (rf"show gpon ont-provision {pon} {ont}", self._ont_provision),
                (rf"show gpon ont-optical {pon}", self._ont_optical),
                (r"show gpon serial-number", self._serial_numbers),
                (rf"show alarm {pon} ont {ont}", self._alarms),
            )
        ]

    # --- Netmiko-compatible surface -------------------------------------

    def send_command(self, command: str, **_netmiko_kwargs) -> str:
        """Return canned output for a CLI command, like Netmiko's send_command"""
        if not self._alive:
            raise ConnectionError("Mock E7 session is closed")

        normalized = re.sub(r"\s+", " ", command).strip().lower()
        for pattern, handler in self._commands:
            match = pattern.fullmatch(normalized)
            if match:
                return handler(**match.groupdict())

        logger.warning(f"Mock E7 has no canned output for command: {command!r}")
        return (f"% Invalid command: '{command.strip()}'\n"
                "% (mock E7-20: add this command in app/controller/mock_e7.py)")

    def disconnect(self) -> None:
        self._alive = False

    def is_alive(self) -> bool:
        return self._alive

    # --- Lookups ---------------------------------------------------------

    def _onts(self, pon: Optional[str] = None) -> List[MockONT]:
        return [o for o in MOCK_ONTS if pon is None or o.pon == pon]

    def _find(self, ont: str, pon: Optional[str] = None) -> Optional[MockONT]:
        """Resolve '4', 'ont-4' or '1/1/1:4' (optionally limited to one PON)"""
        match = re.fullmatch(r"(?:(?P<pon>\d+/\d+/\d+):|ont-)?(?P<id>\d+)", ont)
        if not match:
            return None
        pon = match.group("pon") or pon
        for candidate in self._onts(pon):
            if candidate.ont_id == int(match.group("id")):
                return candidate
        return None

    @staticmethod
    def _not_found(ont: str, pon: Optional[str] = None) -> str:
        where = f" on GPON port {pon}" if pon else ""
        return f"% ONT {ont} not found{where}"

    # --- System commands -------------------------------------------------

    def _version(self) -> str:
        return "\n".join([
            "*** MOCK DEVICE - simulated output, not a real chassis ***",
            "Calix E7-20",
            _fields([
                ("System Name", self.hostname),
                ("Software Version", "2.6.50.12"),
                ("Shelf 1 Card 1", "GPON-8 (active)"),
                ("Shelf 1 Card 2", "GPON-8 (standby)"),
                ("Uptime", "214 days, 6 hours, 41 minutes"),
            ]),
        ])

    def _clock(self) -> str:
        return datetime.now().strftime("System clock: %Y-%m-%d %H:%M:%S local")

    def _users(self) -> str:
        return _table(["User", "Role", "Session", "From"],
                      [["admin", "admin", "ssh", "mock-client"]])

    def _switch_ports(self) -> str:
        rows = [[port, f"{total_bytes:,}", f"{frames:,}", errors, drops, "off", 0]
                for port, total_bytes, frames, errors, drops in MOCK_SWITCH_PORTS]
        return " Status and Counters - Port Counters\n\n" + _table(
            ["Port", "Total Bytes", "Total Frames", "Errors Rx", "Drops Tx", "Flow Ctrl", "Bcast Limit"],
            rows)

    # --- ONT commands ----------------------------------------------------

    def _ont_table(self, pon: Optional[str] = None) -> str:
        """One row per ONT. Online rows read 'ONT <ref> ... online ...'"""
        onts = self._onts(pon)
        if not onts:
            return f"% No ONTs found on GPON port {pon}"
        rows = [[f"ONT {o.ref}", o.serial, o.model, o.status,
                 _dbm(o.rx_power, o), _dbm(o.tx_power, o),
                 o.missed_bursts, o.bip_errors, o.crc_errors, o.description]
                for o in onts]
        return _table(["ONT ID", "Serial Number", "Model", "Status", "Rx Power", "Tx Power",
                       "Missed Bursts", "BIP Errors", "CRC Errors", "Description"], rows)

    def _ont_detail(self, ont: str, pon: Optional[str] = None) -> str:
        found = self._find(ont, pon)
        if not found:
            return self._not_found(ont, pon)
        return f"ONT {found.ref}\n" + _fields([
            ("Serial Number", found.serial),
            ("Model", found.model),
            ("Admin State", found.admin),
            ("Oper State", found.status),
            ("PON Port", found.pon),
            ("Description", found.description),
            ("Distance", f"{found.distance_km:.2f} km"),
            ("Tx Power", _dbm(found.tx_power, found)),
            ("Rx Power", _dbm(found.rx_power, found)),
            ("Temperature", f"{found.temperature:.1f} C" if found.online else "--"),
            ("Missed Bursts", found.missed_bursts),
            ("BIP Errors", found.bip_errors),
            ("CRC Errors", found.crc_errors),
            ("FEC Errors", found.fec_errors),
        ])

    def _ont_performance(self, ont: str, pon: Optional[str] = None) -> str:
        found = self._find(ont, pon)
        if not found:
            return self._not_found(ont, pon)
        return f"ONT {found.ref} statistics ({found.status})\n" + _fields([
            ("Missed Bursts", found.missed_bursts),
            ("BIP Errors", found.bip_errors),
            ("CRC Errors", found.crc_errors),
            ("FEC Errors", found.fec_errors),
            ("Tx Power", _dbm(found.tx_power, found)),
            ("Rx Power", _dbm(found.rx_power, found)),
        ])

    def _ont_provision(self, pon: str, ont: str) -> str:
        found = self._find(ont, pon)
        if not found:
            return self._not_found(ont, pon)
        return f"ONT {found.ref} provisioning\n" + _fields([
            ("Model", found.model),
            ("Serial Number", found.serial),
            ("Admin State", found.admin),
            ("Description", found.description),
            ("Services", "data (VLAN 100), voice (VLAN 200)"),
        ])

    def _ont_optical(self, pon: str) -> str:
        onts = self._onts(pon)
        if not onts:
            return f"% No ONTs found on GPON port {pon}"
        rows = [[f"ONT {o.ref}", o.status, _dbm(o.tx_power, o), _dbm(o.rx_power, o),
                 f"{o.temperature:.1f} C" if o.online else "--"]
                for o in onts]
        return _table(["ONT ID", "Status", "Tx Power", "Rx Power", "Temperature"], rows)

    def _serial_numbers(self) -> str:
        return _table(["ONT ID", "Serial Number", "Model"],
                      [[f"ONT {o.ref}", o.serial, o.model] for o in self._onts() if o.online])

    def _alarms(self, pon: str, ont: str) -> str:
        found = self._find(ont, pon)
        if not found:
            return self._not_found(ont, pon)
        if not found.alarms:
            return f"ONT {found.ref}: no active alarms"
        return f"ONT {found.ref}: {len(found.alarms)} active alarm(s)\n" + "\n".join(
            f"  {alarm}" for alarm in found.alarms)

    # --- PON commands ----------------------------------------------------

    def _pon_status(self, pon: Optional[str] = None) -> str:
        """One row per GPON port. Rows read 'PON port <id> up ...'"""
        pons: Dict[str, List[MockONT]] = {}
        for o in self._onts(pon):
            pons.setdefault(o.pon, []).append(o)
        if not pons:
            return f"% GPON port {pon} not found"
        rows = []
        for name, onts in pons.items():
            online = sum(1 for o in onts if o.online)
            rows.append([f"PON port {name}", "up", len(onts), online,
                         f"{online / len(onts) * 100:.1f}%"])
        return _table(["Interface", "Status", "Total ONTs", "Online ONTs", "Utilization"], rows)

    def _pon_errors(self, pon: str) -> str:
        onts = self._onts(pon)
        if not onts:
            return f"% GPON port {pon} not found"
        missed = sum(o.missed_bursts for o in onts)
        return f"GPON port {pon} error counters\n" + _fields([
            ("bipErrors", sum(o.bip_errors for o in onts)),
            ("missedBursts", missed),
            ("crcErrors", sum(o.crc_errors for o in onts)),
            ("sdber", "1.0e-05" if missed > 1000 else "< 1.0e-09"),
            ("laserOnTime", "214d 06h 41m"),
        ])
