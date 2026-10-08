"""
Tests for the lab cycle tool (app/tools/lab_cycle.py), run against the
simulated CMS as a rehearsal of the first writes to a real one.
"""

import asyncio
import io

from app.nbi.breaker import CircuitBreaker
from app.nbi.errors import NbiConflictError
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend
from app.tools.lab_cycle import parse_args, start

SERIAL = "CXNK00C4D301"  # discovered on 1/1/1, not provisioned


def args(*extra):
    return parse_args(["--network", MOCK_NETWORK, "--pon", "1/1/1", "--serial", SERIAL, "--profile", "844G-1",
                       "--vlan", "100", "--tag-action", "1", "--bandwidth-profile", "RES-1G",
                       "--bad-bandwidth-profile", "NOPE-BW", "--online-timeout", "5", *extra])


def run(arguments, backend=None, confirm=lambda prompt: SERIAL):
    backend = backend or MockCmsBackend(latency=0, ranging_seconds=0)
    gateway = NbiGateway(backend, timeout=5, retry_backoff=0, breaker=CircuitBreaker())
    out = io.StringIO()
    code = asyncio.run(start(gateway, arguments, out=out, confirm=confirm))
    return code, out.getvalue(), backend


def test_the_full_cycle_passes_on_the_simulated_cms_and_leaves_the_pon_as_it_was(tmp_path):
    code, out, backend = run(args("--change-bandwidth-profile", "VOICE", "--snapshot-dir", str(tmp_path)))
    assert code == 0, out
    assert "Result: every step did what was expected." in out
    for step in ("Bad bandwidth profile: refused, and rolled back", "Activate again: no duplicate",
                 "Change the bandwidth profile by activating again",
                 "Delete without force while a service is on it: refused",
                 "Delete the service, then the ONT without force", "Snapshot the PON after and compare"):
        assert f"[ok] {step}" in out
    assert "CMS refused it, as expected: Bandwidth profile 'NOPE-BW' does not exist" in out
    assert "No differences across 8 ONTs." in out
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())
    assert (tmp_path / "before.snapshot.yml").exists() and (tmp_path / "after.snapshot.yml").exists()


def test_it_refuses_an_ont_that_is_already_provisioned():
    arguments = args()
    arguments.serial = "CXNK00A3F101"  # ONT 1
    code, out, backend = run(arguments)
    assert code == 2 and "already provisioned as ONT 1" in out and "Nothing was changed" in out
    assert 1 in backend._onts


def test_it_asks_for_the_serial_number_before_the_first_write():
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    code, out, _ = run(args(), backend=backend, confirm=lambda prompt: "no")
    assert code == 2 and "Not confirmed" in out
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())


def test_a_cms_that_refuses_every_delete_without_force_is_reported_and_cleaned_up():
    # If CMS insists on force even with nothing left on the ONT, the rollback (which deletes the way
    # the API does, without force) already fails at step 2. The ONT this run created is then deleted
    # with force, and the PON ends as it was.
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    real_delete = backend.delete_ont

    async def needs_force(network, ont_id, *, force):
        if not force:
            raise NbiConflictError("Object has children; use force.")
        await real_delete(network, ont_id, force=force)

    backend.delete_ont = needs_force
    code, out, _ = run(args("--yes"), backend=backend)
    assert code == 1
    assert "Rollback failed: ONT 15 could not be deleted (Object has children; use force.)" in out
    assert "[FAILED] Bad bandwidth profile: refused, and rolled back: The rollback could not delete" in out
    assert "Cleaning up: deleting ONT" in out and "Deleted." in out
    assert "[ok] Snapshot the PON after and compare" in out
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())


def test_a_cms_that_deletes_an_ont_with_its_services_without_force_is_reported():
    # The opposite finding, and the one the API's delete and rollback depend on: CMS quietly
    # removes the service with the ONT. The step fails, there is nothing left to clean up, and
    # the PON ends as it was.
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    real_delete = backend.delete_ont

    async def always_force(network, ont_id, *, force):
        await real_delete(network, ont_id, force=True)

    backend.delete_ont = always_force
    code, out, _ = run(args("--yes"), backend=backend)
    assert code == 1
    assert "[FAILED] Delete without force while a service is on it: refused: CMS deleted the ONT" in out
    assert "Cleaning up" not in out
    assert "[ok] Snapshot the PON after and compare" in out
    assert all(o.state.serial_number != SERIAL for o in backend._onts.values())
