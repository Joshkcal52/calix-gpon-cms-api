"""
Cutover Export
Reads a whole CMS network, or one PON port, and lays it out the way SMx
thinks about it: a subscriber, their ONT, and the ONT's data services. This
replaces building the GPON-to-XGS cutover spreadsheet by hand.

    python -m app.legacy.export --network NTWK-OLT-04 [--pon 1/2/3] --output-dir ./cutover

writes three files: <name>.json (everything), <name>.services.csv (one row
per data service, for a person to fill in) and <name>.ports.csv (port
settings). The same data is served by GET /api/v2/networks/{network}/export.

Only reads CMS. Uses the same CMS settings as the API (CMS_HOST, ... or
MOCK_MODE).

Fields that cannot come from CMS are present but empty, and their names start
with fill_in_: the customer's name and address, the OLT and the serial
number and ONT profile of the new XGS ONT, and the SMx registration ID. Fill
them in, then hand the file to the translate step (app/legacy/smx.py).

The column names follow the SMx parameters as the IDI billing SMx R24.x
integration docs name them (account number, OLT, ONT profile, ONT port,
VLAN, c-VLAN). They are not SMx's own API field names, which are not known
here.
"""

import argparse
import asyncio
import csv
import io
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, Field

from app.models.provisioning_model import OntState, PortState, ProvisionedService, port_name
from app.nbi.errors import NbiError
from app.nbi.gateway import NbiGateway

logger = logging.getLogger(__name__)

EXPORT_FORMAT = 1

FILL_IN_SUBSCRIBER = ("fill_in_first_name", "fill_in_last_name", "fill_in_street", "fill_in_city",
                      "fill_in_state", "fill_in_zip")
FILL_IN_ONT = ("fill_in_olt", "fill_in_serial_number", "fill_in_ont_profile", "fill_in_registration_id")


# --- The record: one legacy ONT, in SMx's terms --------------------------------------

class ExportSubscriber(BaseModel):
    account_number: Optional[str] = Field(None, description="CMS subscriber ID of the ONT")
    fill_in_first_name: Optional[str] = None
    fill_in_last_name: Optional[str] = None
    fill_in_street: Optional[str] = None
    fill_in_city: Optional[str] = None
    fill_in_state: Optional[str] = None
    fill_in_zip: Optional[str] = None


class LegacyOnt(BaseModel):
    """What CMS holds for the ONT being replaced"""
    network: str
    ont_id: int
    serial_number: str
    profile: Optional[str] = None
    model: Optional[str] = None
    pon_port: Optional[str] = None
    admin_state: Optional[str] = None
    oper_state: Optional[str] = None
    description: Optional[str] = None
    router_present: Optional[bool] = None
    router_default_to_rg: Optional[bool] = None


class ExportOnt(BaseModel):
    fill_in_olt: Optional[str] = None
    fill_in_serial_number: Optional[str] = Field(None, description="Serial number of the new XGS ONT")
    fill_in_ont_profile: Optional[str] = Field(None, description="SMx ONT profile of the new ONT")
    fill_in_registration_id: Optional[str] = None
    description: Optional[str] = Field(None, description="Carried over from CMS")
    legacy: LegacyOnt


class ExportService(BaseModel):
    """One data service, as CMS holds it"""
    legacy_port: str = Field(..., description="eth1..eth8, rg1, or slotN-portM for an unrecognised slot")
    attachment: str
    ont_slot: Optional[str] = None
    service_index: int
    vlan: Optional[int] = Field(None, description="Outer tag on CMS. Empty when the tag action sets it")
    c_vlan: Optional[int] = Field(None, description="Inner tag on CMS")
    bandwidth_profile: Optional[str] = None
    tag_action: Optional[str] = None
    admin_state: str = "enabled"
    description: Optional[str] = None


class ExportRecord(BaseModel):
    subscriber: ExportSubscriber
    ont: ExportOnt
    data_services: List[ExportService] = Field(default_factory=list)
    ports: List[PortState] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list, description="What could not be read, and why")


class CutoverExport(BaseModel):
    format: int = EXPORT_FORMAT
    network: str
    pon_port: Optional[str] = None
    exported_at: datetime
    to_fill_in: List[str] = Field(default_factory=lambda: list(FILL_IN_SUBSCRIBER + FILL_IN_ONT))
    records: List[ExportRecord]


def _legacy_port(service: ProvisionedService) -> str:
    if service.attachment.value == "unknown":
        return f"slot{service.ont_slot or '?'}-port{service.ont_port}"
    return port_name(service.attachment, service.ont_port)


def _service(service: ProvisionedService) -> ExportService:
    return ExportService(
        legacy_port=_legacy_port(service), attachment=service.attachment.value, ont_slot=service.ont_slot,
        service_index=service.service_index, vlan=service.vlan, c_vlan=service.inner_vlan,
        bandwidth_profile=service.bandwidth_profile, tag_action=service.tag_action,
        admin_state=service.admin_state, description=service.description)


async def export_ont(gateway: NbiGateway, network: str, ont_id: int, include_ports: bool = True) -> ExportRecord:
    """One ONT with its services and ports, as an export record"""
    notes: List[str] = []
    ont: OntState = await gateway.get_ont(network, ont_id)  # with its services
    legacy = LegacyOnt(network=network, ont_id=ont.ont_id, serial_number=ont.serial_number, profile=ont.profile,
                       model=ont.model, pon_port=ont.pon_port, admin_state=ont.admin_state,
                       oper_state=ont.oper_state.value, description=ont.description)
    ports: List[PortState] = []
    if include_ports:
        try:
            read = await gateway.get_ont_ports(network, ont_id)
            ports = read.ethernet + read.router.ports
            legacy.router_present, legacy.router_default_to_rg = read.router.present, read.router.default_to_rg
            if read.router.note:
                notes.append(read.router.note)
        except NbiError as e:
            notes.append(f"Ports could not be read: {e.message}")
    unknown = [s for s in ont.services if s.attachment.value == "unknown"]
    if unknown and not any("ONT slot" in n for n in notes):
        notes.append(f"{len(unknown)} service(s) are in an ONT slot this code does not recognise.")
    if not ont.subscriber_id:
        notes.append("The ONT has no subscriber ID on CMS.")
    return ExportRecord(
        subscriber=ExportSubscriber(account_number=ont.subscriber_id),
        ont=ExportOnt(description=ont.description, legacy=legacy),
        data_services=[_service(s) for s in sorted(ont.services, key=lambda s: (s.attachment.value, s.ont_port,
                                                                               s.service_index))],
        ports=ports, notes=notes)


async def export_network(gateway: NbiGateway, network: str, pon_port: Optional[str] = None,
                         include_ports: bool = True) -> CutoverExport:
    """
    Read every ONT on the network (or one PON port) with its services and
    ports. An ONT that cannot be read is kept as a record with a note instead
    of failing the export. Reads run in parallel, capped by the gateway.
    """
    onts = sorted(await gateway.list_onts(network), key=lambda o: o.ont_id)
    if pon_port:
        onts = [o for o in onts if o.pon_port == pon_port]

    async def one(summary: OntState) -> ExportRecord:
        try:
            return await export_ont(gateway, network, summary.ont_id, include_ports)
        except NbiError as e:
            return ExportRecord(
                subscriber=ExportSubscriber(account_number=summary.subscriber_id),
                ont=ExportOnt(description=summary.description, legacy=LegacyOnt(
                    network=network, ont_id=summary.ont_id, serial_number=summary.serial_number,
                    profile=summary.profile, pon_port=summary.pon_port)),
                notes=[f"The ONT could not be read: {e.message}"])

    records = await asyncio.gather(*(one(o) for o in onts))
    return CutoverExport(network=network, pon_port=pon_port,
                         exported_at=datetime.now(timezone.utc).replace(microsecond=0), records=list(records))


# --- CSV ------------------------------------------------------------------------------

SERVICE_COLUMNS = (
    ("account_number",) + FILL_IN_SUBSCRIBER + FILL_IN_ONT + (
        "ont_description", "legacy_network", "legacy_ont_id", "legacy_serial_number", "legacy_profile",
        "legacy_model", "legacy_pon_port", "legacy_admin_state", "legacy_router_present",
        "legacy_router_default_to_rg", "service_legacy_port", "service_attachment", "service_ont_slot",
        "service_index", "service_vlan", "service_c_vlan", "service_bandwidth_profile", "service_tag_action",
        "service_admin_state", "service_description", "notes"))
PORT_COLUMNS = ("legacy_network", "legacy_ont_id", "account_number", "port", "attachment", "admin_state",
                "subscriber_id", "security_profile", "description")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def service_rows(export: CutoverExport) -> Iterable[Dict[str, str]]:
    """One row per data service. An ONT with no services still gets one row."""
    for record in export.records:
        legacy = record.ont.legacy
        base = {"account_number": record.subscriber.account_number,
                **{k: getattr(record.subscriber, k) for k in FILL_IN_SUBSCRIBER},
                **{k: getattr(record.ont, k) for k in FILL_IN_ONT},
                "ont_description": record.ont.description, "legacy_network": legacy.network,
                "legacy_ont_id": legacy.ont_id, "legacy_serial_number": legacy.serial_number,
                "legacy_profile": legacy.profile, "legacy_model": legacy.model,
                "legacy_pon_port": legacy.pon_port, "legacy_admin_state": legacy.admin_state,
                "legacy_router_present": legacy.router_present,
                "legacy_router_default_to_rg": legacy.router_default_to_rg, "notes": " ".join(record.notes)}
        for service in record.data_services or [None]:
            row = dict(base)
            if service is not None:
                row.update({f"service_{k}" if k != "service_index" else k: v
                            for k, v in service.model_dump().items()})
            yield {column: _cell(row.get(column)) for column in SERVICE_COLUMNS}


def port_rows(export: CutoverExport) -> Iterable[Dict[str, str]]:
    for record in export.records:
        for port in record.ports:
            yield {"legacy_network": record.ont.legacy.network, "legacy_ont_id": str(record.ont.legacy.ont_id),
                   "account_number": _cell(record.subscriber.account_number), "port": port.port,
                   "attachment": port.attachment.value, "admin_state": _cell(port.admin_state),
                   "subscriber_id": _cell(port.subscriber_id), "security_profile": _cell(port.security_profile),
                   "description": _cell(port.description)}


def to_csv(rows: Iterable[Dict[str, str]], columns: Tuple[str, ...]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=list(columns), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def from_services_csv(text: str) -> CutoverExport:
    """
    Read a services CSV back, typically after a person filled in the fill_in_
    columns. Rows are grouped back into one record per legacy ONT.
    """
    reader = csv.DictReader(io.StringIO(text))
    missing = [c for c in ("account_number", "legacy_network", "legacy_ont_id", "legacy_serial_number")
               if c not in (reader.fieldnames or [])]
    if missing:
        raise ValueError(f"Not a services export: missing column(s) {', '.join(missing)}")

    def value(row: Dict[str, str], column: str) -> Optional[str]:
        text = (row.get(column) or "").strip()
        return text or None

    def number(row: Dict[str, str], column: str) -> Optional[int]:
        text = value(row, column)
        return int(text) if text is not None else None

    records: Dict[Tuple[str, int], ExportRecord] = {}
    network = ""
    for line, row in enumerate(reader, start=2):
        try:
            key = (row["legacy_network"], int(row["legacy_ont_id"]))
        except (TypeError, ValueError):
            raise ValueError(f"Line {line}: legacy_ont_id must be a number") from None
        network = network or key[0]
        if key not in records:
            records[key] = ExportRecord(
                subscriber=ExportSubscriber(account_number=value(row, "account_number"),
                                            **{k: value(row, k) for k in FILL_IN_SUBSCRIBER}),
                ont=ExportOnt(description=value(row, "ont_description"),
                              **{k: value(row, k) for k in FILL_IN_ONT},
                              legacy=LegacyOnt(network=key[0], ont_id=key[1],
                                               serial_number=row["legacy_serial_number"],
                                               profile=value(row, "legacy_profile"),
                                               model=value(row, "legacy_model"),
                                               pon_port=value(row, "legacy_pon_port"),
                                               admin_state=value(row, "legacy_admin_state"))))
        if value(row, "service_legacy_port"):
            try:
                records[key].data_services.append(ExportService(
                    legacy_port=value(row, "service_legacy_port"),
                    attachment=value(row, "service_attachment") or "ethernet",
                    ont_slot=value(row, "service_ont_slot"), service_index=number(row, "service_index") or 1,
                    vlan=number(row, "service_vlan"), c_vlan=number(row, "service_c_vlan"),
                    bandwidth_profile=value(row, "service_bandwidth_profile"),
                    tag_action=value(row, "service_tag_action"),
                    admin_state=value(row, "service_admin_state") or "enabled",
                    description=value(row, "service_description")))
            except ValueError as e:
                raise ValueError(f"Line {line}: {e}") from None
    return CutoverExport(network=network, exported_at=datetime.now(timezone.utc).replace(microsecond=0),
                         records=list(records.values()))


def load_export(path: str) -> CutoverExport:
    """An export as JSON, or a (filled-in) services CSV"""
    text = Path(path).read_text(encoding="utf-8-sig")
    if path.lower().endswith(".csv"):
        return from_services_csv(text)
    return CutoverExport.model_validate_json(text)


# --- Command line ------------------------------------------------------------------------

async def main() -> None:
    from app.nbi.gateway import build_gateway
    from config import settings

    parser = argparse.ArgumentParser(description="Export a CMS network or PON port for an XGS cutover (read-only)")
    parser.add_argument("--network", required=True, help="CMS network name")
    parser.add_argument("--pon", help="Only this PON port, as shelf/card/port")
    parser.add_argument("--output-dir", default=".", help="Where to write the files (default: here)")
    parser.add_argument("--name", help="File name stem (default: the network, plus the PON port)")
    parser.add_argument("--no-ports", action="store_true", help="Skip reading port settings (fewer CMS calls)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    gateway = build_gateway(settings)
    try:
        export = await export_network(gateway, args.network, args.pon, include_ports=not args.no_ports)
    finally:
        await gateway.close()

    stem = args.name or args.network + (f"-pon-{args.pon.replace('/', '-')}" if args.pon else "")
    directory = Path(args.output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{stem}.json").write_text(export.model_dump_json(indent=2), encoding="utf-8")
    (directory / f"{stem}.services.csv").write_text(to_csv(service_rows(export), SERVICE_COLUMNS), encoding="utf-8")
    (directory / f"{stem}.ports.csv").write_text(to_csv(port_rows(export), PORT_COLUMNS), encoding="utf-8")
    services = sum(len(r.data_services) for r in export.records)
    with_notes = sum(1 for r in export.records if r.notes)
    print(f"Exported {len(export.records)} ONTs and {services} data services from {args.network} to "
          f"{directory}/{stem}.*  ({with_notes} ONT(s) have notes)", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
