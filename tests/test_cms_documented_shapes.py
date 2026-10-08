"""
Tests for the reply shapes and limits taken from CMS NBI documentation. It is
not for R15.x, so each of these is strong evidence rather than proof; a real
CMS has the last word.

  - show-ont answers each ONT as two objects in one <match>: its provisioning
    under get-config and its state under get. Both are read.
  - <serno> is lowercase hexadecimal.
  - CMS limits descriptions: ONT 48 characters, Ethernet port 47, service 31.
  - A network (node) name may contain spaces.
  - An Ethernet port's <intf> binding names OntRg (router) or OntFb (full
    bridge).
"""

import asyncio
from xml.etree import ElementTree

import pytest
from pydantic import ValidationError

from app.models.provisioning_model import (
    ActivationRequest, EthernetPortConfig, EthernetService, NETWORK_PATTERN, PortChange
)
from app.nbi.client import CmsClient
from app.nbi.router_side import RouterAddressing, read_port_mode
from app.nbi.serno import from_serno, to_serno

# A show-ont reply in the documented layout. Values are made up.
SHOW_ONT = """
<rpc-reply message-id="7" nodename="NTWK-TEST"><ok/><action-reply>
  <match>
    <get-config><object>
      <type>Ont</type><id><ont>1</ont></id><admin>enabled</admin>
      <ontprof><type>OntProf</type><id><ontprof name="844G">157</ontprof></id></ontprof>
      <serno>a1b2c</serno><reg-id></reg-id><subscr-id>SUB-1</subscr-id><descr>1 Main St</descr>
      <linked-pon><type>GponPort</type><id><shelf>1</shelf><card>2</card><gponport>3</gponport></id></linked-pon>
    </object></get-config>
    <get><object>
      <type>Ont</type><id><ont>1</ont></id><op-stat>enable</op-stat><crit>0</crit>
      <model>844G</model><vendor>CXNK</vendor>
    </object></get>
  </match>
</action-reply></rpc-reply>
"""


def show_ont_reply(xml: str):
    client = CmsClient("cms.invalid", 18080, "nbi", "secret")

    async def rpc(network, body):
        return ElementTree.fromstring(xml)
    client.rpc = rpc
    return asyncio.run(client.show_ont("NTWK-TEST", {"serno": "a1b2c"}))


def test_show_ont_reads_the_provisioning_and_the_state_of_each_ont():
    [ont] = show_ont_reply(SHOW_ONT)
    assert ont["serno"] == "a1b2c" and ont["subscr-id"] == "SUB-1" and ont["descr"] == "1 Main St"
    assert ont["ontprof"]["id"]["ontprof"]["#text"] == "157"
    assert ont["linked-pon"]["id"]["card"] == "2"
    assert ont["op-stat"] == "enable" and ont["model"] == "844G" and ont["vendor"] == "CXNK"


def test_a_match_with_only_the_state_half_still_reads():
    state_only = SHOW_ONT.replace(SHOW_ONT[SHOW_ONT.index("<get-config>"):
                                                       SHOW_ONT.index("</get-config>") + 13], "")
    [ont] = show_ont_reply(state_only)
    assert ont["op-stat"] == "enable" and "serno" not in ont


def test_serial_numbers_are_sent_in_lowercase_and_read_back_in_uppercase():
    assert to_serno("CXNK00A3F104") == "a3f104"
    assert from_serno("a1b2c", "CXNK") == "CXNK000A1B2C"
    assert to_serno(from_serno("a1b2c")) == "a1b2c"


@pytest.mark.parametrize("build, longest", [
    (lambda d: ActivationRequest(serial_number="CXNK00A3F104", profile="139", description=d), 48),
    (lambda d: EthernetPortConfig(port=1, description=d), 47),
    (lambda d: PortChange(description=d), 47),
    (lambda d: EthernetService(vlan=100, bandwidth_profile="17", tag_action="1", description=d), 31),
])
def test_descriptions_longer_than_cms_takes_are_refused(build, longest):
    build("x" * longest)
    with pytest.raises(ValidationError):
        build("x" * (longest + 1))


@pytest.mark.parametrize("name, ok", [
    ("NTWK-MOCK-E7", True),
    ("NTWK-West E7", True),  # spaces are allowed in a node name
    ("NTWK-e7_20", True),
    ("NTWK-" + "x" * 54, True),  # 59 characters, the longest allowed
    ("NTWK-" + "x" * 55, False),
    ("NTWK-West ", False),  # no space at either end
    (" NTWK-West", False),
])
def test_network_names_follow_the_cms_nodename_rule(name, ok):
    import re
    assert bool(re.match(NETWORK_PATTERN, name)) is ok


INTF = RouterAddressing(port_mode_element="intf")


@pytest.mark.parametrize("bound_to, mode", [("OntRg", "router"), ("OntFb", "bridge"), ("OntSomethingElse", "unknown")])
def test_a_ports_intf_binding_is_read_by_the_type_it_names(bound_to, mode):
    port = {"type": "OntEthGe", "intf": {"type": bound_to, "id": {"ont": "1", "ontslot": "8", "ontrg": "1"}}}
    assert read_port_mode(INTF, port) == mode


def test_a_port_without_an_intf_binding_is_unknown():
    assert read_port_mode(INTF, {"type": "OntEthGe", "admin": "enabled"}) == "unknown"
