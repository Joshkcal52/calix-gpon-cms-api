"""
Read-only CMS check
One command that tells you, in minutes, whether this code understands YOUR
CMS. It logs in, lists the ONTs on one network, reads one ONT with its
services and ports, reads the ONT profiles and VLANs, logs out, and prints
what it understood and which reply fields it did not recognise.

    export CMS_HOST=cms.example.internal CMS_USERNAME=... CMS_PASSWORD=...
    python -m app.tools.cms_check --network NTWK-OLT-04 [--ont-id 1204] [--dump ./cms-replies]

IT CANNOT CHANGE ANYTHING. Every request the CMS client sends passes through
a guard that refuses anything but login, logout and reads (an <edit-config> is
never sent), and it only calls read operations. --dump saves every request and
reply, with the password and session ID blanked, for comparing against the
assumptions listed in docs/ARCHITECTURE.md.

Pick an --ont-id that has services, ideally an 844G with a router-side
service: the ONT slot values it prints are what app/nbi/router_side.py needs.

It uses the same CMS settings as the API, including CMS_SCHEME, CMS_VERIFY_TLS
and CMS_CA_FILE for HTTPS.

Exit code: 0 if every step worked, 1 if any step failed, 2 if it could not
start (no CMS_HOST, or a setting it cannot use).
"""

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

READ_ONLY_MARKERS = ("<auth", "<get-config", "<get>", "<get ", "<action")


class WriteBlocked(Exception):
    pass


def _redact(text: str) -> str:
    text = re.sub(r"(<Password>).*?(</Password>)", r"\1********\2", text, flags=re.S)
    text = re.sub(r'(sessionid=")[^"]*(")', r"\1********\2", text)
    return re.sub(r"(<SessionId>).*?(</SessionId>)", r"\1********\2", text, flags=re.S)


def install_guard(client: Any, dump: Optional[Path]) -> Callable[[], None]:
    """
    Wrap the CMS client's send() (app/nbi/client.py), which every request to
    CMS goes through, so only login, logout and reads can leave this process.
    Returns a function that removes the guard.
    """
    original = client.send
    counter = {"n": 0}

    async def guarded(body: str) -> Any:
        if "<edit-config" in body or not any(marker in body for marker in READ_ONLY_MARKERS):
            raise WriteBlocked("The read-only check refused to send a request that is not a read.")
        response = await original(body)
        if dump is not None:
            counter["n"] += 1
            stem = dump / f"{counter['n']:03d}"
            stem.with_suffix(".request.xml").write_text(_redact(body), encoding="utf-8")
            stem.with_suffix(".reply.xml").write_text(f"<!-- HTTP {response.status_code} -->\n"
                                                      f"{_redact(response.text)}", encoding="utf-8")
        return response

    client.send = guarded
    return lambda: delattr(client, "send")


class Report:
    def __init__(self) -> None:
        self.steps: List[Dict[str, Any]] = []
        # Bandwidth profile and tag action values as CMS wrote them on the ONT read: numeric IDs or names?
        self.service_values: Dict[str, List[str]] = {"bandwidth_profile": [], "tag_action": []}

    def step(self, name: str, ok: bool, detail: Any) -> None:
        self.steps.append({"step": name, "ok": ok, "detail": detail})

    @property
    def ok(self) -> bool:
        return all(s["ok"] for s in self.steps)


async def run_check(backend: Any, network: str, ont_id: Optional[int], report: Report) -> None:
    """Read-only calls only. Each step is reported; a failed step does not stop the rest."""

    async def attempt(name: str, call: Callable[[], Any], describe: Callable[[Any], Any]) -> Any:
        try:
            result = await call()
        except Exception as e:  # report everything, including unexpected crashes
            report.step(name, False, f"{type(e).__name__}: {getattr(e, 'message', e)}")
            return None
        report.step(name, True, describe(result))
        return result

    onts = await attempt(
        "login and list ONTs", lambda: backend.list_onts(network),
        lambda found: {"count": len(found), "first": [
            {"ont_id": o.ont_id, "serial_number": o.serial_number, "profile": o.profile, "model": o.model,
             "pon_port": o.pon_port, "admin_state": o.admin_state, "oper_state": o.oper_state.value,
             "subscriber_id": o.subscriber_id} for o in found[:5]]})
    if ont_id is None and onts:
        ont_id = onts[0].ont_id
    if ont_id is not None:
        ont = await attempt(f"read ONT {ont_id} with its services", lambda: backend.get_ont(network, ont_id),
                            lambda ont: ont.model_dump(mode="json"))
        for field in report.service_values if ont else ():
            report.service_values[field] = sorted({getattr(s, field) or "(empty)" for s in ont.services})
        await attempt(f"read ONT {ont_id} ports and router side", lambda: backend.get_ont_ports(network, ont_id),
                      lambda ports: ports.model_dump(mode="json"))
    else:
        report.step("read one ONT", False, "No ONT to read: the list was empty or failed. Pass --ont-id.")
    await attempt("list ONT profiles", lambda: backend.list_ont_profiles(network),
                  lambda found: [p.model_dump() for p in found])
    await attempt("list VLANs", lambda: backend.list_vlans(network),
                  lambda found: {"count": len(found), "first": [v.model_dump() for v in found[:10]]})
    await attempt("log out", backend.close, lambda _: "logged out")


def findings(backend: Any) -> Dict[str, Any]:
    return {
        "unrecognised_reply_fields": {t: sorted(f) for t, f in sorted(backend.unrecognised.items()) if f},
        "service_ont_slots_seen": sorted(backend.slots_seen),
        "raw_values_of_inferred_fields": {k: sorted(v) for k, v in sorted(backend.observed.items())},
        "router_side_configured": backend._router is not None,
    }


def print_text(report: Report, found: Dict[str, Any], out=sys.stdout) -> None:
    for step in report.steps:
        print(f"[{'ok' if step['ok'] else 'FAILED'}] {step['step']}", file=out)
        print("    " + json.dumps(step["detail"], indent=2, default=str).replace("\n", "\n    "), file=out)
    print("\nReply fields this code did not recognise (send these to whoever maintains it):", file=out)
    unknown = found["unrecognised_reply_fields"]
    print("    " + ("\n    ".join(f"{t}: {', '.join(f)}" for t, f in unknown.items()) if unknown else "none"),
          file=out)
    print("\nRaw values of fields whose meaning is inferred (check these by eye):", file=out)
    for field, values in found["raw_values_of_inferred_fields"].items():
        print(f"    {field}: {', '.join(values)}", file=out)
    print("\nBandwidth profile and tag action values on that ONT's services, as CMS wrote them. Numeric IDs or "
          "names? BANDWIDTH_PROFILES and the services this API sends must use the same form (see "
          "docs/KNOWN_UNKNOWNS.md):", file=out)
    for field, values in report.service_values.items():
        print(f"    {field}: {', '.join(values) or 'none read (pick an --ont-id with services)'}", file=out)
    print(f"\nService ONT slots seen: {', '.join(found['service_ont_slots_seen']) or 'none'}", file=out)
    print("    3 is Gigabit Ethernet. Any other value on an ONT with a router is a candidate for "
          "CMS_RG_SERVICE_SLOT (see app/nbi/router_side.py).", file=out)
    print(f"\nRouter side configured (CMS_RG_*): {'yes' if found['router_side_configured'] else 'no'}", file=out)
    print(f"\nResult: {'every step worked' if report.ok else 'SOME STEPS FAILED'}. Nothing was changed.", file=out)


def main() -> None:
    from config import settings
    from app.nbi.router_side import router_side_addressing

    parser = argparse.ArgumentParser(description="Read-only check of a real CMS. Changes nothing.")
    parser.add_argument("--network", required=True, help="CMS network name, e.g. NTWK-OLT-04")
    parser.add_argument("--ont-id", type=int, help="ONT to read in full (default: the first one listed)")
    parser.add_argument("--host", default=settings.cms_host, help="CMS host (default: CMS_HOST)")
    parser.add_argument("--port", type=int, default=settings.cms_port, help="NBI port (default: CMS_PORT)")
    parser.add_argument("--scheme", choices=("http", "https"), default=settings.cms_scheme,
                        help="http or https (default: CMS_SCHEME)")
    parser.add_argument("--user", default=settings.cms_username, help="CMS user (default: CMS_USERNAME)")
    parser.add_argument("--timeout", type=float, default=max(settings.nbi_timeout_seconds, 30.0))
    parser.add_argument("--dump", help="Directory to save every request and reply (password blanked)")
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    args = parser.parse_args()

    if not args.host:
        print("Set CMS_HOST (and CMS_USERNAME, CMS_PASSWORD) or pass --host. The password is only read from "
              "CMS_PASSWORD.", file=sys.stderr)
        sys.exit(2)
    from app.nbi.cms_backend import CmsBackend
    try:
        backend = CmsBackend(args.host, args.port, args.user, settings.cms_password or "", timeout=args.timeout,
                             router=router_side_addressing(settings), scheme=args.scheme,
                             verify=settings.cms_verify())
    except ValueError as e:
        print(str(e), file=sys.stderr)
        sys.exit(2)

    dump = Path(args.dump) if args.dump else None
    if dump:
        dump.mkdir(parents=True, exist_ok=True)
    remove_guard = install_guard(backend.client, dump)
    report = Report()
    try:
        asyncio.run(run_check(backend, args.network, args.ont_id, report))
    finally:
        remove_guard()

    found = findings(backend)
    if args.json:
        print(json.dumps({"ok": report.ok, "steps": report.steps, "service_values": report.service_values, **found},
                         indent=2, default=str))
    else:
        print(f"CMS read-only check: {backend.client.url}, network {args.network}\n")
        print_text(report, found)
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
