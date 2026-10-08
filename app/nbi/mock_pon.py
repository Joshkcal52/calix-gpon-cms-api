"""
Mock PON
A filled PON for the simulators (app/nbi/mock_e7.py and app/nbi/mock_cms.py),
for trying the API at scale: MockE7(pon_size=64), MockCmsBackend(pon_size=64),
or MOCK_PON_SIZE=64 python main.py --mock [--backend e7-cli].

The ONTs are discovered (connected, not provisioned), serials CXNK00A1xxxx,
in a fixed mix of half 844G-1 (with a router), a quarter 812G-1 and a quarter
716GE-I. A sized simulator also has a second data VLAN (400) and a second
residential bandwidth profile (RES-500M), so a customer can have a second data
service and a profile can be changed. Without pon_size nothing changes.

A GPON PON is split 1:32 or 1:64 at most, so pon_size is limited to 64.
"""

from typing import Dict, List, Tuple

PON_SPLIT_MAX = 64
MODEL_MIX = ("844G-1", "812G-1", "844G-1", "716GE-I")  # profile names, in the order they are handed out
EXTRA_VLANS: Dict[int, str] = {400: "data-2"}
EXTRA_BW_PROFILES: Tuple[str, ...] = ("RES-500M",)


def check_pon_size(pon_size: int) -> int:
    if not 0 <= pon_size <= PON_SPLIT_MAX:
        raise ValueError(f"pon_size must be 0 to {PON_SPLIT_MAX} (a GPON PON is split 1:{PON_SPLIT_MAX} at most), "
                         f"not {pon_size}")
    return pon_size


def sim_onts(count: int) -> List[Tuple[str, str]]:
    """(serial without the CXNK vendor prefix, profile) for each of `count` ONTs: 00A10001, 00A10002, ..."""
    return [(f"00A1{n:04X}", MODEL_MIX[(n - 1) % len(MODEL_MIX)]) for n in range(1, count + 1)]
