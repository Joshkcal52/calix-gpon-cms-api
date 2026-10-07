"""
Snapshots, and comparing one with CMS (read-only)
A snapshot is a YAML record of what CMS holds for a network, one card or one
PON port: each ONT's identity, profile, admin state, subscriber, description,
Ethernet ports (with their bridge/router mode) and data services. It is built
from the same reads as the cutover export (app/legacy/export.py), so one pass
over CMS can produce the export, the CSVs and the snapshot.

Take one:

    python -m app.legacy.snapshot --network NTWK-OLT-04 [--olt OLT-04] [--pon 1/2/3 | --card 1/2] \\
        --output snapshots/OLT-04.yml

(or GET /api/v2/networks/{network}/export?format=snapshot, or the export CLI,
which writes <name>.snapshot.yml next to its other files).

Compare a saved one with what CMS holds now:

    python -m app.legacy.snapshot --compare snapshots/OLT-04.yml

(or POST /api/v2/networks/{network}/snapshot-compare with the snapshot). It
lists ONTs that are gone, ONTs that are new, and every field, port and service
that differs. Exit code 0 if nothing differs, 1 if something does, 2 on error.

Both ONLY READ CMS. Nothing here changes CMS; there is no restore. Uses the
same CMS settings as the API (CMS_HOST, ... or MOCK_MODE).

Format 1 snapshots (from the retired rollback playbook) can still be
compared: they have no ports, so only ONTs and services are compared.
"""

import argparse
import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field

from app.legacy.export import CutoverExport, export_network
from app.nbi.errors import NbiError
from app.nbi.gateway import NbiGateway

logger = logging.getLogger(__name__)

SNAPSHOT_FORMAT = 2
ONT_FIELDS = ("serial_number", "registration_id", "profile", "admin_state", "subscriber_id", "description",
              "pon_port")
PORT_FIELDS = ("mode", "admin_state", "subscriber_id", "security_profile", "description")
# Fields a saved snapshot does not have (us/ds_pir_override before they were exported) are not compared
SERVICE_FIELDS = ("attachment", "ont_slot", "vlan", "inner_vlan", "bandwidth_profile", "tag_action",
                  "us_pir_override", "ds_pir_override", "admin_state", "description")


def snapshot_from_export(export: CutoverExport, olt: Optional[str] = None) -> Dict[str, Any]:
    onts = []
    for record in sorted(export.records, key=lambda r: r.ont.legacy.ont_id):
        legacy = record.ont.legacy
        ont: Dict[str, Any] = {
            "ont_id": legacy.ont_id, "serial_number": legacy.serial_number,
            "registration_id": legacy.registration_id, "profile": legacy.profile,
            "admin_state": legacy.admin_state, "subscriber_id": record.subscriber.account_number,
            "description": legacy.description, "pon_port": legacy.pon_port,
            "ports": [{"port": p.port, "mode": p.mode, "admin_state": p.admin_state,
                       "subscriber_id": p.subscriber_id, "security_profile": p.security_profile,
                       "description": p.description} for p in record.ports],
            "services": [{"port": s.legacy_port, "service_index": s.service_index, "attachment": s.attachment,
                          "ont_slot": s.ont_slot, "vlan": s.vlan, "inner_vlan": s.c_vlan,
                          "bandwidth_profile": s.bandwidth_profile, "tag_action": s.tag_action,
                          "us_pir_override": s.us_pir_override, "ds_pir_override": s.ds_pir_override,
                          "admin_state": s.admin_state, "description": s.description}
                         for s in sorted(record.data_services, key=lambda s: (s.legacy_port, s.service_index))],
        }
        unreadable = [n for n in record.notes if n.startswith("The ONT could not be read")]
        if unreadable:
            ont["unreadable"] = unreadable[0]
        onts.append(ont)
    return {
        "format": SNAPSHOT_FORMAT,
        "olt": olt,
        "network": export.network,
        "pon_port": export.pon_port,
        "card": export.card,
        "captured_at": export.exported_at.isoformat(),
        "onts": onts,
    }


async def take_snapshot(gateway: NbiGateway, network: str, olt: Optional[str] = None, pon_port: Optional[str] = None,
                        card: Optional[str] = None) -> Dict[str, Any]:
    export = await export_network(gateway, network, pon_port, include_ports=True, card=card)
    return snapshot_from_export(export, olt)


def to_yaml(snapshot: Dict[str, Any]) -> str:
    return yaml.safe_dump(snapshot, sort_keys=False, default_flow_style=False)


# --- Compare ----------------------------------------------------------------------------

class FieldChange(BaseModel):
    field: str
    saved: Any
    now: Any


class ItemChange(BaseModel):
    """A port or a service that exists in both, with what differs"""
    item: str
    changes: List[FieldChange]


class OntDifference(BaseModel):
    ont_id: int
    serial_number: Optional[str] = None
    changes: List[FieldChange] = Field(default_factory=list)
    ports_changed: List[ItemChange] = Field(default_factory=list)
    ports_missing: List[str] = Field(default_factory=list, description="In the snapshot, not on CMS now")
    ports_new: List[str] = Field(default_factory=list)
    services_changed: List[ItemChange] = Field(default_factory=list)
    services_missing: List[str] = Field(default_factory=list, description="In the snapshot, not on CMS now")
    services_new: List[str] = Field(default_factory=list, description="On CMS now, not in the snapshot")


class Comparison(BaseModel):
    read_only: bool = True
    network: str
    pon_port: Optional[str] = None
    card: Optional[str] = None
    snapshot_captured_at: Optional[str] = None
    compared_at: str
    same: bool
    onts_compared: int
    onts_missing: List[Dict[str, Any]] = Field(default_factory=list, description="In the snapshot, gone from CMS")
    onts_new: List[Dict[str, Any]] = Field(default_factory=list, description="On CMS, not in the snapshot")
    onts_unreadable: List[Dict[str, Any]] = Field(default_factory=list, description="Could not be read now")
    onts_changed: List[OntDifference] = Field(default_factory=list)


def _service_key(service: Dict[str, Any]) -> str:
    # Format 1 services have ont_port instead of port, and were always on Ethernet ports
    port = service.get("port") or f"eth{service.get('ont_port', 1)}"
    return f"{port} service {service.get('service_index', 1)}"


def _field_changes(saved: Dict[str, Any], now: Dict[str, Any], fields: Tuple[str, ...]) -> List[FieldChange]:
    return [FieldChange(field=f, saved=saved.get(f), now=now.get(f))
            for f in fields if f in saved and saved.get(f) != now.get(f)]


def _items(saved: List[Dict[str, Any]], now: List[Dict[str, Any]], key, fields: Tuple[str, ...]):
    saved_by, now_by = {key(i): i for i in saved}, {key(i): i for i in now}
    changed = [ItemChange(item=k, changes=c) for k in saved_by if k in now_by
               for c in [_field_changes(saved_by[k], now_by[k], fields)] if c]
    return changed, [k for k in saved_by if k not in now_by], [k for k in now_by if k not in saved_by]


def check_snapshot(saved: Any) -> None:
    if not isinstance(saved, dict) or saved.get("format") not in (1, 2):
        raise ValueError(f"Unknown snapshot format {saved.get('format') if isinstance(saved, dict) else None!r}")
    if not saved.get("network") or not isinstance(saved.get("onts"), list):
        raise ValueError("A snapshot needs a network and a list of onts")


def compare(saved: Dict[str, Any], current: Dict[str, Any]) -> Comparison:
    """What differs between a saved snapshot and a fresh one. Pure: no CMS calls."""
    check_snapshot(saved)
    saved_onts = {o["ont_id"]: o for o in saved.get("onts") or []}
    now_onts = {o["ont_id"]: o for o in current.get("onts") or []}
    result = Comparison(network=current["network"], pon_port=current.get("pon_port"), card=current.get("card"),
                        snapshot_captured_at=str(saved.get("captured_at")), compared_at=current["captured_at"],
                        same=True, onts_compared=0)

    for ont_id, then in saved_onts.items():
        now = now_onts.get(ont_id)
        if now is None:
            result.onts_missing.append({"ont_id": ont_id, "serial_number": then.get("serial_number")})
            continue
        if now.get("unreadable"):
            result.onts_unreadable.append({"ont_id": ont_id, "reason": now["unreadable"]})
            continue
        result.onts_compared += 1
        difference = OntDifference(ont_id=ont_id, serial_number=now.get("serial_number"),
                                   changes=_field_changes(then, now, ONT_FIELDS))
        if "ports" in then:  # format 1 has no ports
            difference.ports_changed, difference.ports_missing, difference.ports_new = _items(
                then["ports"], now.get("ports") or [], lambda p: p["port"], PORT_FIELDS)
        difference.services_changed, difference.services_missing, difference.services_new = _items(
            then.get("services") or [], now.get("services") or [], _service_key, SERVICE_FIELDS)
        if any((difference.changes, difference.ports_changed, difference.ports_missing, difference.ports_new,
                difference.services_changed, difference.services_missing, difference.services_new)):
            result.onts_changed.append(difference)

    result.onts_new = [{"ont_id": o["ont_id"], "serial_number": o.get("serial_number")}
                       for ont_id, o in now_onts.items() if ont_id not in saved_onts]
    result.same = not (result.onts_missing or result.onts_new or result.onts_changed or result.onts_unreadable)
    return result


async def compare_with_cms(gateway: NbiGateway, saved: Dict[str, Any]) -> Comparison:
    """Read CMS (same network and scope as the snapshot) and compare. Read-only."""
    check_snapshot(saved)
    current = await take_snapshot(gateway, saved["network"], saved.get("olt"), saved.get("pon_port"),
                                  saved.get("card"))
    return compare(saved, current)


def describe(comparison: Comparison) -> str:
    lines = [f"Snapshot of {comparison.snapshot_captured_at} vs CMS now ({comparison.compared_at}), "
             f"{comparison.network}" + (f" PON {comparison.pon_port}" if comparison.pon_port else "")
             + (f" card {comparison.card}" if comparison.card else "")]
    if comparison.same:
        lines.append(f"No differences across {comparison.onts_compared} ONTs.")
    for ont in comparison.onts_missing:
        lines.append(f"ONT {ont['ont_id']} ({ont['serial_number']}): in the snapshot, gone from CMS")
    for ont in comparison.onts_new:
        lines.append(f"ONT {ont['ont_id']} ({ont['serial_number']}): on CMS, not in the snapshot")
    for ont in comparison.onts_unreadable:
        lines.append(f"ONT {ont['ont_id']}: could not be read now ({ont['reason']})")
    for ont in comparison.onts_changed:
        prefix = f"ONT {ont.ont_id}"
        lines += [f"{prefix}: {c.field} {c.saved!r} -> {c.now!r}" for c in ont.changes]
        for kind, changed, missing, new in (("port", ont.ports_changed, ont.ports_missing, ont.ports_new),
                                            ("service", ont.services_changed, ont.services_missing,
                                             ont.services_new)):
            lines += [f"{prefix} {item.item}: {c.field} {c.saved!r} -> {c.now!r}" for item in changed
                      for c in item.changes]
            lines += [f"{prefix} {item}: {kind} in the snapshot, gone from CMS" for item in missing]
            lines += [f"{prefix} {item}: {kind} on CMS, not in the snapshot" for item in new]
    lines.append("Read-only: nothing was changed.")
    return "\n".join(lines)


async def main() -> None:
    from app.nbi.gateway import build_gateway
    from config import settings

    parser = argparse.ArgumentParser(description="Take a snapshot of CMS, or compare one with CMS. Read-only.")
    parser.add_argument("--compare", metavar="SNAPSHOT", help="Compare this saved snapshot with CMS now")
    parser.add_argument("--network", help="CMS network name (taking a snapshot)")
    parser.add_argument("--olt", help="Name of the OLT, recorded in the snapshot")
    where = parser.add_mutually_exclusive_group()
    where.add_argument("--pon", help="Only this PON port, as shelf/card/port")
    where.add_argument("--card", help="Only this card, as shelf/card")
    parser.add_argument("--output", default="-", help="File to write (default: standard output)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    if not args.compare and not args.network:
        parser.error("--network is required to take a snapshot")
    gateway = build_gateway(settings)
    try:
        if args.compare:
            with open(args.compare, encoding="utf-8") as f:
                saved = yaml.safe_load(f) or {}
            comparison = await compare_with_cms(gateway, saved)
            text = comparison.model_dump_json(indent=2) + "\n" if args.output != "-" else describe(comparison) + "\n"
        else:
            snapshot = await take_snapshot(gateway, args.network, args.olt, args.pon, args.card)
            text = to_yaml(snapshot)
    except (OSError, ValueError, KeyError, NbiError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)
    finally:
        await gateway.close()

    if args.output == "-":
        sys.stdout.write(text)
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
    if args.compare:
        if args.output != "-":
            print(describe(comparison), file=sys.stderr)
        sys.exit(0 if comparison.same else 1)
    print(f"Wrote {len(snapshot['onts'])} ONTs from {args.network} to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
