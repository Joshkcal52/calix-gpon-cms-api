"""
SMx Cutover Plan (dry run)
Turns a cutover export (app/legacy/export.py) into the SMx requests that
would recreate each subscriber on XGS, for a person to review. NOTHING IS
SENT, and this project has no code that writes to SMx.

    python -m app.legacy.smx --input cutover/NTWK-OLT-04.services.csv \\
        [--map app/legacy/smx_map.yml] [--subscriber 100482] --output plan.json

The input is an export's JSON, or its services CSV after the fill_in_
columns were filled in. Also served by POST /api/v2/smx/plans and, for one
subscriber read live from CMS, GET /api/v2/networks/{network}/subscribers/{id}/smx-plan.

Per subscriber the plan holds three kinds of request, in order: Create/Update
Subscriber, Create/Update ONT, and Create/Update Data Service for each
service. The action and parameter names are the ones the IDI billing SMx
R24.x integration docs use. They are NOT SMx's own API names or JSON shape,
which are not known here: whoever builds the SMx client maps them.

Rules:
  - Each Ethernet port's bridge or router mode is carried over exactly as CMS
    reports it, on the ONT request and on each data service. A mode that could
    not be read fails the subscriber: it is never defaulted, and never to a
    router/ONT combo. (844Gs that ran as bridges and were rebuilt as combos
    gave customers two routers in a row.)
  - The existing serial number is used unless fill_in_replacement_serial_number
    is set, because the ONT may be kept or swapped. Each plan says which.
  - Every ONT profile, bandwidth profile, tag action, port, port mode and admin
    state is looked up in
    the mapping file (app/legacy/smx_map.yml or SMX_MAP_PATH). A value with no
    entry fails that subscriber with a message naming it, and it gets no
    requests. Other subscribers are unaffected.
  - Blank fill_in_ fields do not fail a subscriber: they are listed under
    to_fill_in and the subscriber is marked not ready.
  - A service in an ONT slot this code does not recognise always fails the
    subscriber: it is unknown which side of the ONT it was on.
  - Voice is not planned: only Ethernet data services are read from CMS. Each
    plan carries the export's voice status ("not checked": the ONT has voice
    ports; "unknown": their count could not be read; "none"), and the plan
    counts the first two under voice_not_checked. "ready" covers data
    services only.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.legacy.export import (
    BRIDGE_NOTE, FILL_IN_ONT, FILL_IN_SUBSCRIBER, OPTIONAL_FILL_IN, VOICE_NONE, CutoverExport, ExportRecord,
    VoiceStatus, load_export
)

NOT_SENT = ("Dry run. Nothing was sent to SMx. Parameter names follow the IDI billing SMx R24.x docs, "
            "not SMx's own API, whose request format is not known here. 'PON Port', 'Port Modes' and "
            "'Port Mode' are not in those docs: they carry what SMx must be told, under names chosen here.")


VOICE_NOT_PLANNED = ("Voice is not planned: only Ethernet data services are read from CMS. Check every subscriber "
                     "whose voice is 'not checked' (the ONT has voice ports) or 'unknown' by hand.")


class TagActionRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vlan: Union[Literal["outer"], int] = Field(..., description="outer, or a fixed VLAN")
    c_vlan: Union[Literal["inner", "none"], int] = "none"

    @field_validator("vlan", "c_vlan")
    @classmethod
    def _range(cls, value):
        if isinstance(value, int) and not 1 <= value <= 4094:
            raise ValueError("VLANs are 1 to 4094")
        return value


class SmxMap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ont_profiles: Dict[str, str] = Field(default_factory=dict, description="CMS ONT profile -> SMx ONT Profile ID")
    bandwidth_profiles: Dict[str, str] = Field(default_factory=dict, description="CMS bandwidth profile -> Policy Map")
    port_modes: Dict[str, str] = Field(default_factory=dict, description="bridge / router -> SMx value")
    tag_actions: Dict[str, TagActionRule] = Field(default_factory=dict)
    ont_ports: Dict[str, str] = Field(default_factory=dict, description="CMS port -> SMx ONT Port ID")
    admin_states: Dict[str, str] = Field(default_factory=dict)
    service_name: str = Field("{account}-{port}-{index}")
    subscriber_defaults: Dict[str, Any] = Field(default_factory=dict)
    ont_defaults: Dict[str, Any] = Field(default_factory=dict)
    data_service_defaults: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("service_name")
    @classmethod
    def _template(cls, value: str) -> str:
        try:
            value.format(account="A", port="P", index=1)
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"service_name may only use {{account}}, {{port}} and {{index}} ({e})") from None
        return value

    @classmethod
    def load(cls, path: Union[str, Path]) -> "SmxMap":
        with open(path, encoding="utf-8") as f:
            return cls.model_validate(yaml.safe_load(f) or {})


class SmxRequest(BaseModel):
    action: str = Field(..., description="Create/Update Subscriber, Create/Update ONT or Create/Update Data Service")
    parameters: Dict[str, Any]


class SubscriberPlan(BaseModel):
    account_number: Optional[str]
    legacy_network: str
    legacy_ont_id: int
    legacy_serial_number: str
    ok: bool = Field(..., description="False if a value had no mapping; then there are no requests")
    ready: bool = Field(..., description="ok and nothing left to fill in. Covers data services only, never voice")
    voice: VoiceStatus = Field(..., description="Voice is not planned. 'none': no voice ports. 'not checked': voice ports "
                                        "whose service was not read. 'unknown': voice port count not read")
    ont: Literal["kept", "replaced"] = Field(
        ..., description="kept: the existing serial number is used. replaced: fill_in_replacement_serial_number")
    problems: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list, description="Worth a person's attention; not failures")
    to_fill_in: List[str] = Field(default_factory=list)
    requests: List[SmxRequest] = Field(default_factory=list)


class SmxPlan(BaseModel):
    dry_run: Literal[True] = True
    note: str = NOT_SENT
    network: str
    subscribers: int
    ok: int
    ready: int
    failed: int
    voice_note: str = VOICE_NOT_PLANNED
    voice_not_checked: int = Field(..., description="Subscribers whose voice is 'not checked' or 'unknown'")
    plans: List[SubscriberPlan]


def plan_subscriber(record: ExportRecord, mapping: SmxMap) -> SubscriberPlan:
    legacy = record.ont.legacy
    account = record.subscriber.account_number
    problems: List[str] = []
    warnings: List[str] = []
    services: List[SmxRequest] = []

    if not account:
        problems.append("the ONT has no subscriber ID on CMS, so there is no account number")
    if not record.data_services:
        problems.append(f"ONT {legacy.ont_id} has no data services on CMS, so there is nothing to recreate")
    ont_profile = mapping.ont_profiles.get(legacy.profile or "")
    if ont_profile is None:
        problems.append(f"ONT profile '{legacy.profile}' has no entry under ont_profiles")

    # Each Ethernet port's mode, exactly as CMS has it. Never defaulted.
    port_modes: Dict[str, str] = {}
    if not legacy.port_modes:
        problems.append("the Ethernet ports' bridge/router modes were not read from CMS, so they cannot be "
                        "carried over (export with ports)")
    for port, mode in legacy.port_modes.items():
        if mode not in ("bridge", "router"):
            problems.append(f"port {port}: its bridge/router mode could not be read from CMS; it is not defaulted")
            continue
        smx_port, smx_mode = mapping.ont_ports.get(port), mapping.port_modes.get(mode)
        if smx_port is None:
            problems.append(f"port {port} has no entry under ont_ports")
        if smx_mode is None:
            problems.append(f"port mode '{mode}' has no entry under port_modes")
        if smx_port is not None and smx_mode is not None:
            port_modes[smx_port] = smx_mode
    if legacy.router_capable_running_as_bridge:
        warnings.append(BRIDGE_NOTE)

    for service in record.data_services:
        label = f"service {service.service_index} on {service.legacy_port}"
        if service.attachment not in ("ethernet", "router"):
            problems.append(f"{label}: it is in ONT slot '{service.ont_slot}', which this code does not "
                            f"recognise, so it is unknown which side of the ONT it was on")
            continue
        policy = mapping.bandwidth_profiles.get(service.bandwidth_profile or "")
        if policy is None:
            problems.append(f"{label}: bandwidth profile '{service.bandwidth_profile}' has no entry under "
                            f"bandwidth_profiles")
        port = mapping.ont_ports.get(service.legacy_port)
        if port is None:
            problems.append(f"{label}: port '{service.legacy_port}' has no entry under ont_ports")
        admin = mapping.admin_states.get(service.admin_state)
        if admin is None:
            problems.append(f"{label}: admin state '{service.admin_state}' has no entry under admin_states")
        mode = None
        if service.port_mode not in ("bridge", "router"):
            problems.append(f"{label}: the bridge/router mode of its port could not be read from CMS; it is "
                            f"not defaulted")
        else:
            mode = mapping.port_modes.get(service.port_mode)
            if mode is None:
                problems.append(f"{label}: port mode '{service.port_mode}' has no entry under port_modes")

        vlan: Optional[int] = None
        c_vlan: Optional[int] = None
        rule = mapping.tag_actions.get(service.tag_action or "")
        if rule is None:
            problems.append(f"{label}: tag action '{service.tag_action}' has no entry under tag_actions")
        else:
            vlan = service.vlan if rule.vlan == "outer" else rule.vlan
            if vlan is None:
                problems.append(f"{label}: tag action '{service.tag_action}' takes the outer VLAN, but the service "
                                f"has none on CMS")
            if rule.c_vlan == "inner":
                c_vlan = service.c_vlan
                if c_vlan is None:
                    problems.append(f"{label}: tag action '{service.tag_action}' takes the inner VLAN, but the "
                                    f"service has none on CMS")
            elif isinstance(rule.c_vlan, int):
                c_vlan = rule.c_vlan

        if problems:
            continue
        parameters = {
            **mapping.data_service_defaults,
            "Account Number": account,
            "OLT": record.ont.fill_in_olt,
            "ONT ID": None,  # assigned by SMx when the ONT is created
            "ONT Port ID": port,
            "Port Mode": mode,
            "Service Name": mapping.service_name.format(account=account, port=service.legacy_port,
                                                        index=service.service_index),
            "Service Description": service.description,
            "Policy Map": policy,
            "VLAN": vlan,
            "c-VLAN": c_vlan,
            "Admin State": admin,
        }
        services.append(SmxRequest(action="Create/Update Data Service", parameters=parameters))

    subscriber, ont = record.subscriber, record.ont
    disposition = "replaced" if ont.fill_in_replacement_serial_number else "kept"
    base = dict(account_number=account, legacy_network=legacy.network, legacy_ont_id=legacy.ont_id,
                legacy_serial_number=legacy.serial_number, ont=disposition, voice=record.voice, warnings=warnings)
    if problems:
        return SubscriberPlan(**base, ok=False, ready=False, problems=problems)

    if not ont.registration_id:
        warnings.append("CMS has no registration ID for this ONT; SMx asks for one.")
    to_fill_in = [f for f in FILL_IN_SUBSCRIBER + FILL_IN_ONT if f not in OPTIONAL_FILL_IN
                  and not getattr(subscriber if f in FILL_IN_SUBSCRIBER else ont, f)]
    requests = [
        SmxRequest(action="Create/Update Subscriber", parameters={
            **mapping.subscriber_defaults,
            "Account Number": account,
            "First Name": subscriber.fill_in_first_name, "Last Name": subscriber.fill_in_last_name,
            "Street Line 1": subscriber.fill_in_street, "City": subscriber.fill_in_city,
            "State": subscriber.fill_in_state, "Zip/Postal Code": subscriber.fill_in_zip}),
        SmxRequest(action="Create/Update ONT", parameters={
            **mapping.ont_defaults,
            "OLT": ont.fill_in_olt,
            "PON Port": ont.fill_in_pon_port,
            "Subscriber ID": account,
            "ONT Profile ID": ont_profile,
            "Serial Number": ont.fill_in_replacement_serial_number or ont.serial_number,
            "Registration ID": ont.registration_id,
            "ONT Description": ont.description,
            "Port Modes": port_modes}),
        *services]
    return SubscriberPlan(**base, ok=True, ready=not to_fill_in, to_fill_in=to_fill_in, requests=requests)


def plan_export(export: CutoverExport, mapping: SmxMap, subscriber_id: Optional[str] = None) -> SmxPlan:
    records = export.records
    if subscriber_id is not None:
        records = [r for r in records if r.subscriber.account_number == subscriber_id]
        if not records:
            raise LookupError(f"Subscriber {subscriber_id} is not in this export.")
    plans = [plan_subscriber(r, mapping) for r in records]
    return SmxPlan(network=export.network, subscribers=len(plans), ok=sum(p.ok for p in plans),
                   ready=sum(p.ready for p in plans), failed=sum(not p.ok for p in plans),
                   voice_not_checked=sum(p.voice != VOICE_NONE for p in plans), plans=plans)


def main() -> None:
    from config import settings

    parser = argparse.ArgumentParser(description="Plan the SMx requests for a cutover export. Sends nothing.")
    parser.add_argument("--input", required=True, help="Export JSON, or a filled-in services CSV")
    parser.add_argument("--map", default=settings.smx_map_path, help="Mapping file (default: SMX_MAP_PATH)")
    parser.add_argument("--subscriber", help="Only this subscriber (CMS subscriber ID)")
    parser.add_argument("--output", default="-", help="File to write (default: standard output)")
    args = parser.parse_args()

    try:
        plan = plan_export(load_export(args.input), SmxMap.load(args.map), args.subscriber)
    except (ValueError, LookupError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)
    text = json.dumps(plan.model_dump(mode="json"), indent=2) + "\n"
    if args.output == "-":
        sys.stdout.write(text)
    else:
        Path(args.output).write_text(text, encoding="utf-8")
    print(f"{plan.subscribers} subscriber(s): {plan.ready} ready, {plan.ok - plan.ready} need fields filled in, "
          f"{plan.failed} failed. Nothing was sent.", file=sys.stderr)
    if plan.voice_not_checked:
        print(f"Voice is NOT planned. {plan.voice_not_checked} subscriber(s) have voice ports, or a voice port count "
              f"that could not be read: check their voice service by hand.", file=sys.stderr)
    sys.exit(1 if plan.failed else 0)


if __name__ == "__main__":
    main()
