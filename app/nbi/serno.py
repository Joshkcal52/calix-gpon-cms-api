"""
Serial number conversion.

CMS stores an ONT serial without the vendor prefix or leading zeros
(CXNK00A3F104 is held as A3F104). That is how the cms-nbi-client
documentation describes it; it has not been checked against a real CMS.
"""

DEFAULT_VENDOR = "CXNK"


def to_serno(serial_number: str) -> str:
    """CXNK00A3F104 -> A3F104"""
    body = serial_number[4:] if serial_number[:4].isalpha() else serial_number
    return body.lstrip("0").upper() or "0"


def from_serno(serno: str, vendor: str = DEFAULT_VENDOR) -> str:
    """A3F104 -> CXNK00A3F104"""
    return f"{vendor or DEFAULT_VENDOR}{serno.upper().zfill(8)}"
