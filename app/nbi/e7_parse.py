"""
E7 CLI Output Parsing
Turns what the E7 EXA command line prints into plain data for the E7 CLI
backend (app/nbi/e7_backend.py).

NOT VERIFIED AGAINST A REAL E7. Calix's public E7 EXA R3.x CLI reference
documents the commands but shows no sample output for any of them, so the
layout of every reply here is unknown. The parsers are therefore lenient
rather than exact:

  - `records()` reads "label: value" (or "label = value", or a label and a
    value separated by two or more spaces) lines, in one or more blocks.
  - `table()` reads a table with a header row, by the columns' positions
    under the header.
  - Labels are normalised ("Serial Number" -> "serial-number") and looked up
    through ALIASES, so one parser accepts several plausible spellings.

Before pointing the backend at a real E7, capture real output with
python -m app.tools.e7_capture (show commands only), and adjust ALIASES or
the parsers until tests built from those captures pass.
"""

import re
from typing import Dict, Iterable, List, Optional, Tuple

# Field -> labels it may appear under, normalised. The first entry is the name this project uses.
ALIASES: Dict[str, Tuple[str, ...]] = {
    "ont": ("ont", "ont-id", "id", "ont-number"),
    "serial-number": ("serial-number", "serial", "serial-no", "sn", "serno"),
    "profile": ("profile", "ont-profile", "ont-prof"),
    "admin-state": ("admin-state", "admin", "admin-status"),
    "oper-state": ("oper-state", "operational-state", "op-state", "op-stat", "oper-status", "status", "state"),
    "pon-port": ("pon-port", "gpon-port", "linked-pon", "pon", "location", "on-gpon-port"),
    "subscriber-id": ("subscriber-id", "subscriber", "subscr-id"),
    "reg-id": ("reg-id", "registration-id"),
    "description": ("description", "descr", "desc"),
    "model": ("model", "ont-model", "product", "product-code"),
    "vendor": ("vendor", "vendor-id"),
    "port": ("port", "ont-port", "interface", "port-id"),
    "name": ("name", "service", "eth-svc", "service-name", "svc-name"),
    "bw-profile": ("bw-profile", "bandwidth-profile", "bw-prof"),
    "svc-tag-action": ("svc-tag-action", "tag-action", "svc-tag"),
    "outer-vlan": ("outer-vlan", "vlan", "s-vlan", "out-tag"),
    "inner-vlan": ("inner-vlan", "c-vlan", "in-tag"),
    "ports": ("ports", "port-types", "interfaces"),
    "capabilities": ("capabilities", "caps"),
    "eth-sec-profile": ("eth-sec-profile", "security-profile", "sec-profile"),
}

_LOOKUP = {label: field for field, labels in ALIASES.items() for label in labels}

KEY_VALUE = re.compile(r"^\s*(?P<key>[A-Za-z][\w ./()-]*?)\s*(?::|=|\s{2,})\s*(?P<value>.*?)\s*$")
RULE = re.compile(r"^[\s\-=+|]+$")  # a line of dashes under a table header


def normalise(label: str) -> str:
    """'Serial Number' -> 'serial-number', 'Admin State:' -> 'admin-state'"""
    return re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")


def field_name(label: str) -> str:
    """The project's name for a label, or the normalised label itself if no alias knows it"""
    key = normalise(label)
    return _LOOKUP.get(key, key)


def known(field: str) -> bool:
    return field in ALIASES


def records(text: str) -> List[Dict[str, str]]:
    """
    Label/value lines as one dict per block. A new block starts after a
    blank line, or when a label repeats within the current block.
    """
    blocks: List[Dict[str, str]] = []
    current: Dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip() or RULE.match(line):
            if current:
                blocks.append(current)
                current = {}
            continue
        match = KEY_VALUE.match(line)
        if not match:
            continue
        key = field_name(match.group("key"))
        if key in current:
            blocks.append(current)
            current = {}
        current[key] = match.group("value")
    if current:
        blocks.append(current)
    return blocks


def merged(text: str) -> Dict[str, str]:
    """Every label/value line as one dict, for output that describes one thing"""
    result: Dict[str, str] = {}
    for block in records(text):
        for key, value in block.items():
            result.setdefault(key, value)
    return result


def table(text: str) -> List[Dict[str, str]]:
    """
    A column-aligned table: the first line with two or more words followed
    by a rule of dashes, or failing that the first line, is the header. Each
    cell is the text under its header's column. Header words separated by a
    single space are one column ("Serial Number").
    """
    lines = [line.rstrip() for line in text.splitlines()]
    lines = [line for line in lines if line.strip()]
    if not lines:
        return []
    header_at = next((i for i in range(len(lines) - 1) if RULE.match(lines[i + 1]) and not RULE.match(lines[i])), 0)
    header = lines[header_at]
    starts = [m.start() for m in re.finditer(r"(?:(?<=\s\s)|^)(?=\S)", header)]
    names = [field_name(header[a:b].strip()) for a, b in zip(starts, starts[1:] + [None])]
    rows = []
    for line in lines[header_at + 1:]:
        if RULE.match(line) or line.strip().lower().startswith(("total", "number of")):
            continue
        cells = _cut(line, starts)
        row = {name: cell for name, cell in zip(names, cells) if cell}
        if row:
            rows.append(row)
    return rows


def _cut(line: str, starts: List[int]) -> List[str]:
    """
    Cells by header position, nudged to word boundaries: a value that starts a
    little left of its header (right-aligned numbers) stays whole.
    """
    cells = []
    bounds = starts + [len(line) + 1]
    for i in range(len(starts)):
        a, b = bounds[i], bounds[i + 1]
        while 0 < a < len(line) and line[a - 1] != " ":
            a -= 1
        while 0 < b < len(line) and line[b - 1] != " ":
            b -= 1
        cells.append(line[a:b].strip() if a < len(line) else "")
    return cells


def value(data: Dict[str, str], field: str) -> Optional[str]:
    text = (data.get(field) or "").strip().strip('"')
    return None if text.lower() in ("", "-", "--", "none", "n/a", "na") else text


def number(data: Dict[str, str], field: str) -> Optional[int]:
    text = value(data, field)
    match = re.match(r"^\d+$", text or "")
    return int(text) if match else None


# ONT port letters in Calix's show ont-profile legend: f=fast-eth, g=gig-eth, h=hpna-eth, r=video-rf,
# R=video-hot-rf, t=t1, p=pots, G=residential-gateway, F=full-bridge
PORT_COUNTS = re.compile(r"(\d+)\s*([fghrRtpGFd])\b")


def port_counts(text: Optional[str]) -> Dict[str, int]:
    """'4g 2p 1G' -> {'g': 4, 'p': 2, 'G': 1}. The layout is inferred from the legend, not seen."""
    counts: Dict[str, int] = {}
    for count, letter in PORT_COUNTS.findall(text or ""):
        counts[letter] = counts.get(letter, 0) + int(count)
    return counts


def unrecognised(rows: Iterable[Dict[str, str]]) -> List[str]:
    return sorted({key for row in rows for key in row if not known(key)})
