"""
Tests for three risks found reviewing the code before its first run against a
real CMS:

  - deleting an ONT could take its voice service with it unseen: this API
    does not read voice, and the delete was always sent with CMS's force.
    Now a profile with voice ports needs voice_checked, and the ONT itself
    is deleted without force, after its data services, so CMS refuses it if
    anything is left (voice, or a service added since the read)
  - running an activation again skipped a service that was already bound,
    even when its bandwidth profile or peak rates had changed, and reported
    success. Now it changes the service to match
  - an unknown bandwidth profile was found out only when CMS refused it
    mid-job. Now BANDWIDTH_PROFILES is checked before anything is sent
    (the API refusing before queueing is in test_api_errors.py)

The simulated CMS keeps a hidden voice line on every third ONT (3, 6, 9, 12),
as a real CMS read through this API would: nothing reports it.
"""

import asyncio

import pytest

from app.controller.lifecycle_controller import apply_ont_change
from app.controller.provisioning_controller import activate_ont, service_differences
from app.models.lifecycle_model import DeleteOnt
from app.models.provisioning_model import (
    ActivationRequest, Attachment, EthernetService, OntProfile, ProvisionedService
)
from app.nbi.breaker import CircuitBreaker
from app.nbi.errors import NbiConflictError, NbiRejectedError
from app.nbi.gateway import NbiGateway
from app.nbi.mock_cms import MOCK_NETWORK, MockCmsBackend

NET = MOCK_NETWORK
NEW_SERIAL = "CXNK00C4D301"  # discovered, not provisioned, in the simulated CMS


class Recorder:
    job_id, attempt = "test", 1

    def __init__(self):
        self.messages = []

    async def emit(self, type, message, /, **data):
        self.messages.append(message)

    async def step(self, number, total, message):
        self.messages.append(message)


def make_gateway():
    backend = MockCmsBackend(latency=0, ranging_seconds=0)
    return NbiGateway(backend, timeout=1, retry_backoff=0, breaker=CircuitBreaker()), backend


def delete(gateway, ont_id, serial, **options):
    return apply_ont_change(Recorder(), gateway, NET, ont_id, DeleteOnt(serial_number=serial, **options))


# --- Deleting an ONT that may carry voice ---------------------------------------------

def test_a_profile_with_voice_ports_needs_voice_checked():
    async def scenario():
        gateway, backend = make_gateway()
        with pytest.raises(NbiConflictError, match="has 2 voice port") as refused:
            await delete(gateway, 4, "CXNK00A3F104", force=True)
        assert "voice_checked=true" in refused.value.message and "Nothing was deleted" in refused.value.message
        assert len(backend._onts[4].state.services) == 1  # untouched

        result = await delete(gateway, 4, "CXNK00A3F104", force=True, voice_checked=True)
        assert result["deleted"] is True and 4 not in backend._onts

    asyncio.run(scenario())


def test_a_hidden_voice_line_stops_the_delete_even_with_force_and_voice_checked():
    # Someone said voice_checked, but ONT 3 still has a phone line. Its data services go (the caller
    # asked for that with force), the ONT and its voice line stay, and the job fails saying why.
    async def scenario():
        gateway, backend = make_gateway()
        assert backend._onts[3].voice_lines == 1
        with pytest.raises(NbiConflictError, match="1 voice line"):
            await delete(gateway, 3, "CXNK00A3F103", force=True, voice_checked=True)
        assert 3 in backend._onts and backend._onts[3].voice_lines == 1
        assert backend._onts[3].state.services == []

    asyncio.run(scenario())


def test_a_profile_without_voice_ports_needs_no_check_and_an_unreadable_one_does():
    async def scenario(pots):
        gateway, backend = make_gateway()
        real = backend.list_ont_profiles

        async def profiles(network):
            return [p.model_copy(update={"pots_ports": pots}) if p.id == "716GE-I" else p
                    for p in await real(network)]

        backend.list_ont_profiles = profiles
        return await delete(gateway, 1, "CXNK00A3F101", force=True)

    assert asyncio.run(scenario(0))["deleted"] is True
    with pytest.raises(NbiConflictError, match="could not be read"):
        asyncio.run(scenario(None))


def test_a_service_added_during_the_delete_is_not_removed_with_the_ont():
    # Between the read and the ONT delete, someone adds a service in CMS. The ONT delete is sent
    # without force, so CMS refuses it rather than removing a service nobody saw.
    async def scenario():
        gateway, backend = make_gateway()
        real_delete_service = backend.delete_service

        async def delete_service_then_someone_adds_one(network, ont_id, attachment, ont_port, service_index):
            await real_delete_service(network, ont_id, attachment, ont_port, service_index)
            backend._onts[ont_id].state.services.append(ProvisionedService(
                service_index=1, ont_port=2, vlan=300, bandwidth_profile="RES-1G", tag_action="1"))

        backend.delete_service = delete_service_then_someone_adds_one
        with pytest.raises(NbiConflictError, match="1 data service"):
            await delete(gateway, 4, "CXNK00A3F104", force=True, voice_checked=True)
        assert [s.vlan for s in backend._onts[4].state.services] == [300]

    asyncio.run(scenario())


def rejected_request():
    """The second service is refused by CMS, after the ONT and the first service are in place"""
    return ActivationRequest(serial_number=NEW_SERIAL, profile="844G-1", wait_for_online=False, services=[
        EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1"),
        EthernetService(vlan=100, bandwidth_profile="NOPE-BW", tag_action="1", ont_port=2)])


def test_rollback_removes_the_ont_this_run_created_without_force():
    # The service this run added is deleted, then the ONT without CMS's force
    async def scenario():
        gateway, backend = make_gateway()
        real_delete_ont, forces = backend.delete_ont, []

        async def recording_delete_ont(network, ont_id, *, force):
            forces.append(force)
            await real_delete_ont(network, ont_id, force=force)

        backend.delete_ont = recording_delete_ont
        with pytest.raises(NbiRejectedError, match="NOPE-BW"):
            await activate_ont(Recorder(), gateway, NET, rejected_request(), verify_timeout=0, poll_seconds=0)
        assert all(o.state.serial_number != NEW_SERIAL for o in backend._onts.values())
        assert forces == [False]

    asyncio.run(scenario())


def test_rollback_leaves_an_ont_another_run_is_provisioning():
    # The same command running twice at once: while this run binds its services, the other run
    # binds one this run then finds already there. CMS refuses this run's add, and the rollback
    # must not delete the ONT the other run is finishing.
    async def scenario():
        gateway, backend = make_gateway()
        real_add = backend.add_ethernet_service

        async def other_run_gets_there_first(network, ont_id, service):
            if service.ont_port == 2:
                await real_add(network, ont_id, service.model_copy(update={"bandwidth_profile": "RES-1G"}))
            await real_add(network, ont_id, service)

        backend.add_ethernet_service = other_run_gets_there_first
        recorder = Recorder()
        request = rejected_request()
        request.services[1].bandwidth_profile = "RES-1G"
        with pytest.raises(NbiConflictError, match="already has a service"):
            await activate_ont(recorder, gateway, NET, request, verify_timeout=0, poll_seconds=0)
        ont = next(o.state for o in backend._onts.values() if o.state.serial_number == NEW_SERIAL)
        assert sorted(s.ont_port for s in ont.services) == [1, 2]
        assert any(m.startswith("Not rolled back") for m in recorder.messages)

    asyncio.run(scenario())


# --- Running an activation again with different service settings -------------------------

def test_activation_again_changes_a_bound_service_that_differs():
    async def scenario():
        gateway, backend = make_gateway()

        def request(**service):
            fields = dict(vlan=100, bandwidth_profile="RES-1G", tag_action="1")
            fields.update(service)
            return ActivationRequest(serial_number=NEW_SERIAL, profile="844G-1", wait_for_online=False,
                                     services=[EthernetService(**fields)])

        async def run(req):
            context = Recorder()
            result = await activate_ont(context, gateway, NET, req, verify_timeout=0, poll_seconds=0)
            ont = backend._onts[result["ont_id"]].state
            return context.messages, ont.services

        await run(request())
        messages, services = await run(request(bandwidth_profile="VOICE", ds_pir_override="500m"))
        assert len(services) == 1  # changed in place, not bound twice
        assert (services[0].bandwidth_profile, services[0].ds_pir_override, services[0].us_pir_override) == (
            "VOICE", "500m", "none")
        assert any("Changed eth1 service 1: bandwidth profile VOICE, ds pir override 500m" in m for m in messages)

        # Back to no override: an empty peak rate means "none", as when the service was first bound
        messages, services = await run(request(bandwidth_profile="VOICE"))
        assert services[0].ds_pir_override == "none"

        # Nothing differs: nothing is sent
        before = backend.calls
        messages, services = await run(request(bandwidth_profile="VOICE"))
        assert not any(m.startswith("Changed") for m in messages)
        assert "Binding 0 service(s) to ONT" in " ".join(messages) and "and changing" not in " ".join(messages)
        assert backend.calls - before == 2  # find it by serial, read it back: no write

    asyncio.run(scenario())


def test_service_differences():
    bound = ProvisionedService(vlan=100, bandwidth_profile="RES-1G", tag_action="1", us_pir_override=None,
                               ds_pir_override="none")
    same = EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1")
    assert service_differences(bound, same) == {}
    assert service_differences(bound, same.model_copy(update={"tag_action": "2"})) == {"tag_action": "2"}
    # A request without a tag action leaves the bound one alone
    assert service_differences(bound, same.model_copy(update={"tag_action": None})) == {}
    assert service_differences(bound, same.model_copy(update={"us_pir_override": "100m"})) == {
        "us_pir_override": "100m"}


# --- Bandwidth profiles checked by the worker, before anything is sent -------------------

def test_the_worker_refuses_an_unknown_bandwidth_profile_before_touching_cms():
    # A signup that arrives on the events topic never passes through the API's check
    async def scenario():
        gateway, backend = make_gateway()
        request = ActivationRequest(serial_number=NEW_SERIAL, profile="844G-1", require_discovery=False,
                                    services=[EthernetService(vlan=100, bandwidth_profile="RES-1G", tag_action="1"),
                                              EthernetService(vlan=300, bandwidth_profile="17", tag_action="1")])
        with pytest.raises(NbiRejectedError, match=r"Unknown bandwidth profile\(s\): 17\. .*RES-1G, VOICE"):
            await activate_ont(Recorder(), gateway, NET, request, verify_timeout=0, poll_seconds=0,
                               bandwidth_profiles=frozenset({"RES-1G", "VOICE"}))
        assert backend.calls == 0

    asyncio.run(scenario())
