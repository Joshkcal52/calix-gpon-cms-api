"""
Read-only E7 capture
Saves what a real E7 EXA prints for every show command the E7 CLI backend
(app/nbi/e7_backend.py) reads, and shows what the backend's parsers make of
it. Calix publishes almost no sample output, so these captures are the only way to
check the parsers before the backend changes anything.

    export E7_NODES=OLT-04=10.0.0.4 E7_USERNAME=... E7_PASSWORD=...
    python -m app.tools.e7_capture --network OLT-04 --ont-id 1204 --out ./e7-captures

IT CANNOT CHANGE ANYTHING. Every command passes through a guard that refuses
anything but "show ..." and the "set session ..." commands that turn the
pager and notifications off for this login only (E7_PAGER_COMMAND and
E7_SESSION_COMMANDS). Each command's raw output is written to --out, one
file per command, for turning into test fixtures.

Pick an --ont-id that has services, ideally an 844G with a router-side (G1)
service. Read the "unrecognised labels" and "state words" it prints: those
are what app/nbi/e7_parse.py ALIASES and ONLINE_WORDS in app/nbi/e7_backend.py
need.

Exit code: 0 if every step worked, 1 if any step failed, 2 if it could not
start (no E7_NODES, or an unknown --network).
"""

import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any, List, Optional

ALLOWED = re.compile(r"^(show\s|set session\s)")
RAW_COMMANDS = ("show ont", "show ont discovered", "show ont discovered detail", "show ont-profile",
                "show vlan", "show bw-profile", "show svc-tag-action", "show eth-svc-name")
ONT_COMMANDS = ("show ont {ont} detail", "show ont-port {ont} eth-svc", "show ont-port {ont} detail",
                "show ont-port {ont}")
# An ONT ID that almost certainly does not exist, to see how the E7 words a refusal
MISSING_ONT = 63999999


class CommandBlocked(Exception):
    pass


def install_guard(session: Any, out: Optional[Path]) -> None:
    """Wrap the session's exchange so only show and set session commands reach the E7, and save each output"""
    original = session._exchange
    counter = {"n": 0}

    async def guarded(command: str) -> str:
        if not ALLOWED.match(command):
            raise CommandBlocked(f"The read-only capture refused to send {command!r}.")
        output = await original(command)
        if out is not None:
            counter["n"] += 1
            name = re.sub(r"[^A-Za-z0-9]+", "-", command).strip("-")
            (out / f"{counter['n']:02d}-{name}.txt").write_text(f"$ {command}\n{output}\n", encoding="utf-8")
        return output

    session._exchange = guarded


async def run(args: argparse.Namespace) -> int:
    from app.nbi.e7_backend import backend_from_settings
    from config import Settings

    settings = Settings()
    if not settings.e7_nodes:
        print("E7_NODES is not set (NAME=host[:port]).", file=sys.stderr)
        return 2
    backend = backend_from_settings(settings)
    network = args.network or next(iter(backend.nodes))
    if network not in backend.sessions:
        print(f"No E7 named {network} in E7_NODES ({', '.join(backend.nodes)}).", file=sys.stderr)
        return 2
    out = Path(args.out) if args.out else None
    if out:
        out.mkdir(parents=True, exist_ok=True)
    session = backend.sessions[network]
    install_guard(session, out)

    failures: List[str] = []

    async def step(title: str, action) -> Any:
        try:
            result = await action()
        except CommandBlocked:
            raise
        except Exception as e:
            failures.append(title)
            print(f"[FAIL] {title}: {type(e).__name__}: {getattr(e, 'message', e)}")
            return None
        print(f"[ OK ] {title}")
        return result

    try:
        for command in RAW_COMMANDS + tuple(c.format(ont=args.ont_id) for c in ONT_COMMANDS if args.ont_id) + (
                f"show ont {MISSING_ONT} detail",):
            output = await step(f"raw: {command}", lambda c=command: session.run(c))
            if output is not None and args.verbose:
                print(output)

        print("\nWhat the backend's parsers make of it:")
        profiles = await step("ONT profiles", lambda: backend.list_ont_profiles(network))
        for p in profiles or []:
            print(f"  {p.id}: model={p.model} ge={p.ge_ports} pots={p.pots_ports} rg={p.rg_ports} "
                  f"default_to_rg={p.default_to_rg}")
        vlans = await step("VLANs", lambda: backend.list_vlans(network))
        print(f"  {len(vlans or [])} VLANs: {', '.join(str(v.id) for v in (vlans or [])[:20])}")
        discovered = await step("Discovered ONTs", lambda: backend.list_discovered_onts(network))
        for d in (discovered or [])[:10]:
            print(f"  {d.serial_number} {d.model} on {d.pon_port}")
        if args.ont_id:
            ont = await step(f"ONT {args.ont_id}", lambda: backend.get_ont(network, args.ont_id))
            if ont:
                print("  " + ont.model_dump_json(indent=None))
                found = await step("Same ONT by serial number",
                                   lambda: backend.find_ont_by_serial(network, ont.serial_number))
                if found is not None and found.ont_id != ont.ont_id:
                    failures.append("serial lookup")
                    print(f"[FAIL] serial lookup found ONT {found.ont_id}")
            ports = await step(f"ONT {args.ont_id} ports", lambda: backend.get_ont_ports(network, args.ont_id))
            if ports:
                print("  " + ports.model_dump_json(indent=None))
        missing = await step(f"Missing ONT {MISSING_ONT} reads as not found",
                             lambda: backend._ont_detail(network, MISSING_ONT))
        if missing is not None:
            failures.append("missing ONT")
            print(f"[FAIL] ONT {MISSING_ONT} was read as existing: the error pattern does not match the E7's words")
    finally:
        await backend.close()

    print("\nUnrecognised labels, by command (add the ones that matter to ALIASES in app/nbi/e7_parse.py):")
    for command, labels in sorted(backend.unrecognised.items()):
        if labels:
            print(f"  {command}: {', '.join(sorted(labels))}")
    print("ONT state words seen (ONLINE_WORDS in app/nbi/e7_backend.py decides which mean up):")
    for field, words in sorted(backend.observed.items()):
        print(f"  {field}: {', '.join(sorted(words))}")
    if out:
        print(f"\nRaw output saved in {out}")
    print(f"\n{len(failures)} step(s) failed" if failures else "\nEvery step worked")
    return 1 if failures else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only capture of E7 CLI output for the E7 CLI backend")
    parser.add_argument("--network", help="E7 name from E7_NODES (default: the first)")
    parser.add_argument("--ont-id", type=int, help="An existing ONT with services, ideally an 844G")
    parser.add_argument("--out", help="Directory to save each command's raw output in")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print each raw output too")
    sys.exit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
