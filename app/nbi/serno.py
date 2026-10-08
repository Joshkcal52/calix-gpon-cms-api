"""
Serial number conversion.

CMS stores an ONT serial without the vendor prefix or leading zeros
(CXNK00A3F104 is held as a3f104). That is how the cms-nbi-client
documentation describes it. CMS NBI documentation describes <serno> as
hexadecimal, shows it in lowercase and says show-ont matches must be exact,
so the value is sent in lowercase. Whether CMS ignores case is not known.
Not yet checked against a real CMS.
"""

DEFAULT_VENDOR = "CXNK"


def to_serno(serial_number: str) -> str:
    """CXNK00A3F104 -> a3f104"""
    body = serial_number[4:] if serial_number[:4].isalpha() else serial_number
    return body.lstrip("0").lower() or "0"


def from_serno(serno: str, vendor: str = DEFAULT_VENDOR) -> str:
    """a3f104 -> CXNK00A3F104"""
    return f"{vendor or DEFAULT_VENDOR}{serno.upper().zfill(8)}"
