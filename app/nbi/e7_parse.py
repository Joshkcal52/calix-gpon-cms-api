"""
E7 CLI Output Parsing
Turns what the E7 EXA command line prints into plain data for the E7 CLI
backend (app/nbi/e7_backend.py).

NOT VERIFIED AGAINST A REAL E7. Calix's CLI references document the
commands but show sample output for almost none of them (one table, show
mcast, in the E7 CLI reference #220-00652), so the layout of every reply
here is mostly unknown. The parsers are therefore lenient rather than exact:

  - `records()` reads "label: value" (or "label = value", or a label and a
    value separated by two or more spaces) lines, in one or more blocks.
  - `table()` reads a table with a header row, by the columns the dashed
    line under the header marks out (the layout of Calix's one sample
    table, show mcast), or by the header's word positions if there is none.
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
    "mcast-profile": ("mcast-profile", "multicast-profile"),
    "pon-cos": ("pon-cos", "us-cos", "pon-us-cos"),
}

_LOOKUP = {label: field for field, labels in ALIASES.items() for label in labels}

# ONT port letters in Calix's show ont-profile legend: f=fast-eth, g=gig-eth, h=hpna-eth, r=video-rf,
# R=video-hot-rf, t=t1, p=pots, G=residential-gateway, F=full-bridge (and d=DS-1 in port names)
PORT_LETTERS = "fghrRtpGFd"

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
    return field in ALIASES or (field.startswith("ports-") and field[len("ports-"):] in PORT_LETTERS)


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
    A column-aligned table: the first line followed by a rule of dashes, or
    failing that the first line, is the header. Each cell is the text under
    its column.

    The columns are the runs of dashes in the rule, as in Calix's one sample
    table (show mcast in the E7 CLI reference):
        VLAN Multicast Group Interfaces, OLT Ports and ERPS Domains
        ---- --------------- ------------------------------------------
    Header words there are one space apart both within a column and between
    columns, so only the rule says where a column starts. Without a rule,
    header words separated by a single space are one column ("Serial Number").

    The same sample in the E7 OS R3.1-3.2 Maintenance and Troubleshooting
    Guide prints its rows one space apart, not padded under the dashes:
        25 225.1.1.1 12/g1, 13/1, traffic_domain
    Whether that is the E7 or the document's typesetting is not known, so a
    row whose words cross the column edges is read word by word instead:
    one word to each column, and the rest of the line to the last.
    """
    lines = [line.rstrip() for line in text.splitlines()]
    lines = [line for line in lines if line.strip()]
    if not lines:
        return []
    ruled = next((i for i in range(len(lines) - 1) if RULE.match(lines[i + 1]) and not RULE.match(lines[i])), None)
    header_at = ruled or 0
    header = lines[header_at]
    if ruled is not None:
        starts = [m.start() for m in re.finditer(r"[-=]+", lines[ruled + 1])]
    else:
        starts = [m.start() for m in re.finditer(r"(?:(?<=\s\s)|^)(?=\S)", header)]
    names = [column_name(header[a:b]) for a, b in zip(starts, starts[1:] + [None])]
    rows = []
    for line in lines[header_at + 1:]:
        if RULE.match(line) or line.strip().lower().startswith(("total", "number of")):
            continue
        cells = _cut(line, starts) if ruled is None or _aligned(line, starts) else _split(line, len(starts))
        row = {name: cell for name, cell in zip(names, cells) if cell}
        if row:
            rows.append(row)
    return rows


def column_name(label: str) -> str:
    """
    A table column's field name. A column headed by one port letter keeps its
    case ("G" -> "ports-G", "g" -> "ports-g"): Calix's show ont-profile has a
    column per port type (the M&T guide: 'review the "G" (RG) and "F" (FB)
    Ports columns'), and G (residential gateway) is not g (Gigabit Ethernet).
    """
    text = label.strip()
    return f"ports-{text}" if len(text) == 1 and text in PORT_LETTERS else field_name(text)


def _aligned(line: str, starts: List[int]) -> bool:
    """Every column edge inside the line falls on a space, as in a row padded under the dashes"""
    return all(s == 0 or s >= len(line) or line[s - 1] == " " for s in starts)


def _split(line: str, columns: int) -> List[str]:
    """An unpadded row: one word per column, and the rest of the line in the last"""
    words = line.split(None, columns - 1)
    return words + [""] * (columns - len(words))


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


PORT_COUNTS = re.compile(rf"(\d+)\s*([{PORT_LETTERS}])\b")


def port_counts(text: Optional[str]) -> Dict[str, int]:
    """'4g 2p 1G' -> {'g': 4, 'p': 2, 'G': 1}, for a profile listing its ports in one cell (not seen; a fallback)"""
    counts: Dict[str, int] = {}
    for count, letter in PORT_COUNTS.findall(text or ""):
        counts[letter] = counts.get(letter, 0) + int(count)
    return counts


def port_columns(row: Dict[str, str]) -> Dict[str, int]:
    """{'ports-g': '4', 'ports-G': '1'} -> {'g': 4, 'G': 1}: a profile with a column per port type"""
    return {key[len("ports-"):]: int(value) for key, value in row.items()
            if key.startswith("ports-") and key[len("ports-"):] in PORT_LETTERS and value.strip().isdigit()}


def unrecognised(rows: Iterable[Dict[str, str]]) -> List[str]:
    return sorted({key for row in rows for key in row if not known(key)})
