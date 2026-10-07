"""
Snapshot Exporter
Writes what CMS currently holds for one network to a YAML file: the format
the emergency rollback playbook (ansible/rollback/) restores from.

    python -m app.legacy.snapshot --olt OLT-04 --network NTWK-OLT-04 \\
        --output /path/to/network-backups/olts/OLT-04.yml

Run it on a schedule and commit the result to the network-backups
repository. It uses the same CMS settings as the API (CMS_HOST, CMS_USERNAME,
CMS_PASSWORD, or MOCK_MODE). It only reads from CMS.

A snapshot covers what this project manages: each ONT's ID, serial number,
profile, admin state, subscriber, description and Ethernet services. It is
not a full CMS backup: voice ports, multicast, Ethernet port settings and
everything else are not captured and so cannot be restored from it.

An ONT with any service that is not on a plain Ethernet port (the router side
of an 844G, or an ONT slot this code does not recognise) is LEFT OUT, with a
warning. The snapshot format has no ONT slot, so the rollback playbook would
recreate such a service on an Ethernet port: the wrong place. Leaving the ONT
out means the rollback never touches it.
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import yaml

from app.nbi.gateway import NbiGateway, build_gateway
from app.models.provisioning_model import Attachment
from config import settings

logger = logging.getLogger(__name__)

SNAPSHOT_FORMAT = 1


async def take_snapshot(gateway: NbiGateway, olt: str, network: str) -> Dict[str, Any]:
    onts = []
    for summary in sorted(await gateway.list_onts(network), key=lambda o: o.ont_id):
        ont = await gateway.get_ont(network, summary.ont_id)  # with its services
        elsewhere = [s for s in ont.services if s.attachment != Attachment.ETHERNET]
        if elsewhere:
            logger.warning(f"ONT {ont.ont_id} left out of the snapshot: {len(elsewhere)} service(s) are not on an "
                           f"Ethernet port (ONT slot {', '.join(sorted({s.ont_slot or '?' for s in elsewhere}))}), "
                           f"which the snapshot format cannot record. The rollback will not touch this ONT.")
            continue
        onts.append({
            "ont_id": ont.ont_id,
            "serial_number": ont.serial_number,
            "profile": ont.profile,
            "admin_state": ont.admin_state,
            "subscriber_id": ont.subscriber_id,
            "description": ont.description,
            "pon_port": ont.pon_port,
            "services": [{
                "ont_port": s.ont_port, "service_index": s.service_index, "vlan": s.vlan,
                "inner_vlan": s.inner_vlan, "bandwidth_profile": s.bandwidth_profile,
                "tag_action": s.tag_action, "admin_state": s.admin_state, "description": s.description,
            } for s in sorted(ont.services, key=lambda s: (s.ont_port, s.service_index))],
        })
    return {
        "format": SNAPSHOT_FORMAT,
        "olt": olt,
        "network": network,
        "captured_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "onts": onts,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export a CMS network to a rollback snapshot file")
    parser.add_argument("--olt", required=True, help="Name used for this OLT in the backups repository")
    parser.add_argument("--network", required=True, help="CMS network name")
    parser.add_argument("--output", default="-", help="File to write (default: standard output)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    gateway = build_gateway(settings)
    try:
        snapshot = await take_snapshot(gateway, args.olt, args.network)
    finally:
        await gateway.close()

    text = yaml.safe_dump(snapshot, sort_keys=False, default_flow_style=False)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Wrote {len(snapshot['onts'])} ONTs from {args.network} to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
