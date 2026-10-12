"""
Lab cycle: the first writes to a real CMS, on one spare lab ONT
Runs the checklist in HANDOFF.md as one command, through the same code the
workers use, and says after each step whether CMS did what was expected:

  1. Snapshot the PON (read-only)
  2. Activate with a bandwidth profile CMS does not have: CMS must refuse it,
     and the ONT this run created must be deleted again (rollback)
  3. Activate for real: create the ONT, bind one data service
  4. Run the same activation again: no second ONT or service
  5. Change it: disable and enable the Ethernet port and the service, and
     (with --change-bandwidth-profile) change the service's bandwidth profile
     by running the activation again
  6. Delete the ONT without CMS's force while its service is still on it:
     CMS must refuse. The API's delete and an activation's rollback rely on
     this to keep a voice service, or a service added since they read the
     ONT, from being removed unseen
  7. Delete it the way the API does: the service first, then the ONT without
     CMS's force
  8. Snapshot the PON again and compare: nothing else may have changed

    export CMS_HOST=... CMS_USERNAME=... CMS_PASSWORD=... [CMS_SCHEME=https ...]
    python -m app.tools.lab_cycle --network NTWK-LAB --pon 1/1/1 --serial CXNK0012AB34 \\
        --profile 844G-1 --vlan 100 --tag-action 1 --bandwidth-profile 17 --bad-bandwidth-profile 99999 \\
        [--change-bandwidth-profile 18] [--snapshot-dir ./lab] [--yes]

IT CHANGES CMS. It only ever touches the ONT it creates: it refuses to start
if the serial number is already provisioned, and every change and delete
names that ONT. Use a spare ONT with no voice. It asks for the serial number
to be typed back before the first write, unless --yes is given.

If a step fails after the ONT was created, the ONT is deleted with CMS's
force to leave the lab as it was (it was created by this run, so nothing
else is on it), and the report says so. Step 2 or 7 failing while that
cleanup works is itself a finding: both delete the way the API does (the
services, then the ONT without CMS's force), so it means CMS refuses a
delete without force even when no service is left, and the API's ONT delete
(app/controller/lifecycle_controller.py) and rollback
(app/controller/provisioning_controller.py) need changing before they can work.

With MOCK_MODE=true it runs against the simulated CMS, as a rehearsal.

Exit code: 0 if every step did what was expected, 1 if any did not, 2 if it
could not start (bad settings, serial already provisioned, not confirmed).
"""

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.controller.lifecycle_controller import apply_ont_change
from app.controller.provisioning_controller import activate_ont
from app.legacy.snapshot import compare, describe, take_snapshot, to_yaml
from app.models.lifecycle_model import DeleteOnt, DeleteService, UpdatePort, UpdateService
from app.models.provisioning_model import ActivationRequest, EthernetService, PortChange, ServiceChange
from app.nbi.errors import NbiError, NbiRejectedError
from app.nbi.gateway import NbiGateway


class Failed(Exception):
    """A step did not do what was expected"""


class PrintingContext:
    """Stands in for a job's context: prints each step and event as it happens"""
    job_id, attempt = "lab-cycle", 1

    def __init__(self, out=sys.stdout):
        self.out = out
        self.warnings: List[str] = []

    async def emit(self, type: str, message: str, /, **data: Any) -> None:
        if type == "warning":
            self.warnings.append(message)
        print(f"      {type}: {message}", file=self.out)

    async def step(self, number: int, total: int, message: str) -> None:
        print(f"      {number}/{total} {message}", file=self.out)

    async def record_ownership(self, message: str, /, **data: Any) -> None:
        await self.emit("ownership", message, **data)

    async def ownership_records(self) -> List[Dict[str, Any]]:
        return []  # one run, no earlier attempts


class LabCycle:
    def __init__(self, gateway: NbiGateway, args: argparse.Namespace, out=sys.stdout):
        self.gateway, self.args, self.out = gateway, args, out
        self.network = args.network
        self.results: List[Dict[str, Any]] = []
        self.ont_id: Optional[int] = None  # set once this run has created the ONT

    # --- The cycle ---------------------------------------------------------------------

    async def run(self) -> bool:
        before = None
        try:
            before = await self._step("Snapshot the PON before", self.snapshot, "before")
            await self._step("Bad bandwidth profile: refused, and rolled back", self.rollback)
            await self._step("Activate", self.activate)
            await self._step("Activate again: no duplicate", self.activate_again)
            await self._step("Disable and enable the Ethernet port", self.toggle_port)
            await self._step("Disable and enable the service", self.toggle_service)
            if self.args.change_bandwidth_profile:
                await self._step("Change the bandwidth profile by activating again", self.change_bandwidth)
            await self._step("Delete without force while a service is on it: refused", self.refused_delete)
            await self._step("Delete the service, then the ONT without force", self.delete)
        except Failed:
            await self.clean_up()
        if before is not None:
            try:
                await self._step("Snapshot the PON after and compare", self.compare_after, before)
            except Failed:
                pass
        return all(r["ok"] for r in self.results)

    async def snapshot(self, label: str) -> Dict[str, Any]:
        snapshot = await take_snapshot(self.gateway, self.network, pon_port=self.args.pon)
        if self.args.snapshot_dir:
            path = Path(self.args.snapshot_dir) / f"{label}.snapshot.yml"
            path.write_text(to_yaml(snapshot), encoding="utf-8")
            print(f"      saved {path}", file=self.out)
        print(f"      {len(snapshot['onts'])} ONT(s) on PON {self.args.pon}", file=self.out)
        return snapshot

    async def rollback(self) -> None:
        request = self._activation(self.args.bad_bandwidth_profile, wait_for_online=False)
        context = PrintingContext(self.out)
        try:
            await activate_ont(context, self.gateway, self.network, request, verify_timeout=0, poll_seconds=0)
        except NbiRejectedError as e:
            print(f"      CMS refused it, as expected: {e.message}", file=self.out)
        else:
            self.ont_id = await self._find()
            raise Failed(f"CMS accepted bandwidth profile '{self.args.bad_bandwidth_profile}'. Pick one that does "
                         f"not exist on this CMS.")
        if any("Rollback failed" in w for w in context.warnings):
            self.ont_id = await self._find()
            raise Failed("The rollback could not delete the ONT it created.")
        if await self._find() is not None:
            self.ont_id = await self._find()
            raise Failed("The ONT is still on CMS after the rollback.")

    async def activate(self) -> None:
        result = await self._activate(self.args.bandwidth_profile)
        self.ont_id = result["ont_id"]
        ont = await self.gateway.get_ont(self.network, self.ont_id)
        if len(ont.services) != 1:
            raise Failed(f"Expected 1 service on ONT {self.ont_id}, CMS reports {len(ont.services)}.")
        service = ont.services[0]
        print(f"      ONT {self.ont_id}: {ont.serial_number}, profile {ont.profile}, {ont.oper_state.value}; "
              f"service on {service.attachment.value} port {service.ont_port} (slot {service.ont_slot}), "
              f"VLAN {service.vlan}, bandwidth profile {service.bandwidth_profile}, "
              f"tag action {service.tag_action}", file=self.out)
        if ont.serial_number != self.args.serial:
            raise Failed(f"CMS reports serial number {ont.serial_number}, not {self.args.serial}: the serial "
                         f"conversion in app/nbi/serno.py does not match this CMS.")
        if service.bandwidth_profile != self.args.bandwidth_profile:
            raise Failed(f"CMS reports bandwidth profile {service.bandwidth_profile!r}, not "
                         f"{self.args.bandwidth_profile!r}.")
        if not result["online"]:
            print("      warning: CMS does not report it online. If the ONT is lit and ranged, the op-stat "
                  "values in app/nbi/cms_backend.py do not match this CMS.", file=self.out)

    async def activate_again(self) -> None:
        context = PrintingContext(self.out)
        result = await self._activate(self.args.bandwidth_profile, context=context)
        if result["ont_id"] != self.ont_id:
            raise Failed(f"The second run reports ONT {result['ont_id']}, not {self.ont_id}.")
        ont = await self.gateway.get_ont(self.network, self.ont_id)
        if len(ont.services) != 1:
            raise Failed(f"After a second run ONT {self.ont_id} has {len(ont.services)} services, not 1. "
                         f"The service identity (VLAN, tag action) read back from CMS does not match what was "
                         f"sent.")

    async def toggle_port(self) -> None:
        port = f"eth{self.args.ont_port}"
        for state in ("disabled", "enabled"):
            await self._change(UpdatePort(port=port, change=PortChange(admin_state=state),
                                          serial_number=self.args.serial))
            now = next(p for p in (await self.gateway.get_ont_ports(self.network, self.ont_id)).ethernet
                       if p.number == self.args.ont_port)
            if now.admin_state != state:
                raise Failed(f"Port {port} reads {now.admin_state!r} after setting it {state!r}.")

    async def toggle_service(self) -> None:
        for state in ("disabled", "enabled"):
            await self._change(UpdateService(port=f"eth{self.args.ont_port}", service_index=1,
                                             change=ServiceChange(admin_state=state),
                                             serial_number=self.args.serial, expect_vlan=self.args.vlan))
            service = (await self.gateway.get_ont(self.network, self.ont_id)).services[0]
            if service.admin_state != state:
                raise Failed(f"The service reads {service.admin_state!r} after setting it {state!r}.")

    async def change_bandwidth(self) -> None:
        await self._activate(self.args.change_bandwidth_profile)
        ont = await self.gateway.get_ont(self.network, self.ont_id)
        if [s.bandwidth_profile for s in ont.services] != [self.args.change_bandwidth_profile]:
            raise Failed(f"CMS reports {[s.bandwidth_profile for s in ont.services]} after changing the bandwidth "
                         f"profile to {self.args.change_bandwidth_profile!r}.")

    async def refused_delete(self) -> None:
        # Sent straight to the gateway: the API's own delete would refuse before sending. If CMS
        # accepts it the ONT is gone, so make sure first that the ONT ID still holds the ONT this
        # run created (start() refused a serial that was already provisioned)
        ont = await self.gateway.get_ont(self.network, self.ont_id)
        if ont.serial_number != self.args.serial:
            raise Failed(f"ONT {self.ont_id} now holds {ont.serial_number}, not {self.args.serial}. Not sent.")
        try:
            await self.gateway.delete_ont(self.network, self.ont_id, force=False)
        except NbiError as e:
            if e.retryable:
                raise
            print(f"      CMS refused it, as expected: {e.message}", file=self.out)
        else:
            if await self._find() is None:
                self.ont_id = None  # nothing left to clean up
            raise Failed("CMS deleted the ONT, service and all, without force. A delete through the API or a "
                         "rollback can then remove a voice service, or a service added since it read the ONT, "
                         "without seeing it. Do not use the API's delete or rollback on this CMS until that "
                         "is solved.")
        ont = await self.gateway.get_ont(self.network, self.ont_id)
        if len(ont.services) != 1:
            raise Failed(f"After the refused delete ONT {self.ont_id} has {len(ont.services)} services, not 1.")

    async def delete(self) -> None:
        # The way the API deletes: the service on its own, then the ONT without CMS's force. This ONT was
        # created by this run, so there is no voice on it to check for.
        await self._change(DeleteService(port=f"eth{self.args.ont_port}", service_index=1,
                                         serial_number=self.args.serial, expect_vlan=self.args.vlan))
        result = await self._change(DeleteOnt(serial_number=self.args.serial, voice_checked=True))
        if not result.get("deleted"):
            raise Failed(f"The delete reported {result}.")
        if await self._find() is not None:
            raise Failed("The ONT is still on CMS after the delete.")
        self.ont_id = None

    async def compare_after(self, before: Dict[str, Any]) -> None:
        after = await self.snapshot("after")
        comparison = compare(before, after)
        lines = [line for line in describe(comparison).splitlines() if not line.startswith("Read-only")]
        print("      " + "\n      ".join(lines), file=self.out)
        if not comparison.same:
            raise Failed("The PON is not as it was before the cycle.")

    async def clean_up(self) -> None:
        """
        After a failed step: delete the ONT this run created, with force, so the lab is as it was. Not if
        it holds a service this run did not add: someone else is using the lab ONT, and force would take
        their service (or a voice line, which this API cannot see) with it.
        """
        if self.ont_id is None:
            return
        print(f"\n  Cleaning up: deleting ONT {self.ont_id} ({self.args.serial}), which this run created, with "
              f"CMS's force", file=self.out)
        try:
            ont = await self.gateway.get_ont(self.network, self.ont_id)
            if ont.serial_number != self.args.serial:
                print(f"  NOT deleted: ONT {self.ont_id} holds {ont.serial_number}, not {self.args.serial}. "
                      f"Check it by hand.", file=self.out)
                return
            ours = self._activation(self.args.bandwidth_profile).services[0].identity()
            others = [s for s in ont.services if s.identity() != ours]
            if others:
                listed = ", ".join(f"port {s.ont_port} service {s.service_index} (VLAN {s.vlan})" for s in others)
                print(f"  NOT deleted: ONT {self.ont_id} also holds service(s) this run did not add ({listed}). "
                      f"Check it by hand.", file=self.out)
                return
            await self.gateway.delete_ont(self.network, self.ont_id, force=True)
            print("  Deleted.", file=self.out)
        except NbiError as e:
            print(f"  Cleanup FAILED ({e.message}). Delete ONT {self.ont_id} by hand.", file=self.out)

    # --- Helpers -----------------------------------------------------------------------

    async def _step(self, name: str, fn, *args: Any) -> Any:
        print(f"\n[{len(self.results) + 1}] {name}", file=self.out)
        try:
            result = await fn(*args)
        except Failed as e:
            self._record(name, False, str(e))
            raise
        except NbiError as e:
            self._record(name, False, f"{type(e).__name__}: {e.message}")
            raise Failed(e.message) from e
        self._record(name, True, "as expected")
        return result

    def _record(self, name: str, ok: bool, detail: str) -> None:
        self.results.append({"step": name, "ok": ok, "detail": detail})
        print(f"    {'ok' if ok else 'FAILED'}: {detail}", file=self.out)

    def _activation(self, bandwidth_profile: str, *, wait_for_online: bool = True) -> ActivationRequest:
        return ActivationRequest(
            serial_number=self.args.serial, profile=self.args.profile, ont_id=self.args.ont_id,
            description="gpon-cms lab cycle", require_discovery=not self.args.allow_undiscovered,
            wait_for_online=wait_for_online,
            services=[EthernetService(vlan=self.args.vlan, tag_action=self.args.tag_action,
                                      bandwidth_profile=bandwidth_profile, ont_port=self.args.ont_port)])

    async def _activate(self, bandwidth_profile: str, context: Optional[PrintingContext] = None) -> Dict[str, Any]:
        return await activate_ont(context or PrintingContext(self.out), self.gateway, self.network,
                                  self._activation(bandwidth_profile), verify_timeout=self.args.online_timeout,
                                  poll_seconds=2.0)

    async def _change(self, change) -> Dict[str, Any]:
        return await apply_ont_change(PrintingContext(self.out), self.gateway, self.network, self.ont_id, change)

    async def _find(self) -> Optional[int]:
        ont = await self.gateway.find_ont_by_serial(self.network, self.args.serial)
        return ont.ont_id if ont else None


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Activate, change and delete one spare lab ONT, checking each "
                                                 "step against CMS. THIS CHANGES CMS.")
    parser.add_argument("--network", required=True, help="CMS network name")
    parser.add_argument("--pon", required=True, help="The lab ONT's PON port, as shelf/card/port, for the snapshots")
    parser.add_argument("--serial", required=True, help="Serial number of a spare lab ONT that is not provisioned")
    parser.add_argument("--profile", required=True, help="Its ONT profile")
    parser.add_argument("--ont-id", type=int, help="ONT ID to create it as (default: the next free one)")
    parser.add_argument("--vlan", type=int, required=True, help="Outer VLAN of the test service")
    parser.add_argument("--tag-action", required=True, help="Tag action of the test service (a numeric ID)")
    parser.add_argument("--bandwidth-profile", required=True, help="A bandwidth profile ID that exists")
    parser.add_argument("--bad-bandwidth-profile", required=True,
                        help="A bandwidth profile ID that does NOT exist, to see the rollback run")
    parser.add_argument("--change-bandwidth-profile", help="A second bandwidth profile ID that exists, to change to")
    parser.add_argument("--ont-port", type=int, default=1, help="Ethernet port of the test service (default 1)")
    parser.add_argument("--online-timeout", type=float, default=120.0,
                        help="Seconds to wait for the ONT to come online (default 120)")
    parser.add_argument("--allow-undiscovered", action="store_true",
                        help="Provision even if CMS has not discovered the ONT on a PON")
    parser.add_argument("--snapshot-dir", help="Save the before and after snapshots here")
    parser.add_argument("--yes", action="store_true", help="Do not ask for the serial number to be typed back")
    return parser.parse_args(argv)


async def start(gateway: NbiGateway, args: argparse.Namespace, out=sys.stdout, confirm=input) -> int:
    """Run the cycle. Returns the exit code."""
    existing = await gateway.find_ont_by_serial(args.network, args.serial)
    if existing is not None:
        print(f"{args.serial} is already provisioned as ONT {existing.ont_id} on {args.network}. This tool only "
              f"works on an ONT it creates itself; pick a spare one. Nothing was changed.", file=out)
        return 2
    if not args.yes:
        print(f"This will create, change and delete ONT {args.serial} on {args.network}.", file=out)
        if confirm("Type the serial number to go ahead: ").strip() != args.serial:
            print("Not confirmed. Nothing was changed.", file=out)
            return 2

    cycle = LabCycle(gateway, args, out)
    ok = await cycle.run()
    print("\nSummary", file=out)
    for result in cycle.results:
        print(f"  [{'ok' if result['ok'] else 'FAILED'}] {result['step']}: {result['detail']}", file=out)
    print(f"\nResult: {'every step did what was expected' if ok else 'SOME STEPS FAILED'}.", file=out)
    return 0 if ok else 1


def main() -> None:
    from app.nbi.gateway import build_gateway
    from config import settings

    args = parse_args()
    if args.snapshot_dir:
        Path(args.snapshot_dir).mkdir(parents=True, exist_ok=True)
    if not settings.mock_mode and not settings.cms_host:
        print("Set CMS_HOST, CMS_USERNAME and CMS_PASSWORD (or MOCK_MODE=true for a rehearsal).", file=sys.stderr)
        sys.exit(2)

    async def run() -> int:
        gateway = build_gateway(settings)
        try:
            print(f"Lab cycle against {gateway.backend.name}, network {args.network}")
            return await start(gateway, args)
        finally:
            await gateway.close()

    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
