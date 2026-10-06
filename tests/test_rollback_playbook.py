"""
The emergency rollback playbook, run for real with ansible-playbook against
the mock CMS NBI server, restoring from a real Git repository.
"""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.skipif(
    shutil.which("ansible-playbook") is None or shutil.which("git") is None,
    reason="needs ansible-playbook and git")

REPO = Path(__file__).resolve().parents[1]
ROLLBACK = REPO / "ansible" / "rollback"
SNAPSHOT = REPO / "deploy" / "demo" / "network-backups" / "olts" / "OLT-MOCK.yml"


def load_filters():
    spec = importlib.util.spec_from_file_location("cms_nbi_filters", ROLLBACK / "filter_plugins" / "cms_nbi.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- The decision logic, without Ansible --------------------------------------------

@pytest.fixture(scope="module")
def filters():
    pytest.importorskip("ansible")
    return load_filters()


SNAPSHOT_ONT = {
    "ont_id": 7, "serial_number": "CXNK00A3F107", "profile": "844G-1", "admin_state": "enabled",
    "subscriber_id": "SUB-1007", "description": "137 Alder St", "pon_port": "1/2/3",
    "services": [{"ont_port": 1, "service_index": 1, "vlan": 100, "inner_vlan": None,
                  "bandwidth_profile": "RES-1G", "tag_action": "1", "admin_state": "enabled", "description": None}],
}


def test_scope_narrows_by_card_interface_and_ont(filters):
    onts = [{"ont_id": 1, "pon_port": "1/1/1"}, {"ont_id": 2, "pon_port": "1/1/2"},
            {"ont_id": 3, "pon_port": "1/2/1"}, {"ont_id": 4, "pon_port": "1/12/1"}, {"ont_id": 5, "pon_port": None}]
    ids = lambda **scope: [o["ont_id"] for o in filters.rollback_scope(onts, **scope)]  # noqa: E731

    assert ids() == [1, 2, 3, 4, 5]
    assert ids(card="1/1") == [1, 2]  # and not card 1/12
    assert ids(interface="1/1/2") == [2]
    assert ids(ont="3") == [3]
    assert ids(card="1/2", ont="1") == []


def test_actions_for_a_matching_ont_are_empty(filters):
    live = {k: v for k, v in SNAPSHOT_ONT.items() if k != "services"}
    assert filters.rollback_actions(SNAPSHOT_ONT, live, SNAPSHOT_ONT["services"]) == []


def test_actions_create_a_missing_ont_then_its_services(filters):
    actions = filters.rollback_actions(SNAPSHOT_ONT, {}, [])
    assert [a["template"] for a in actions] == ["ont_create.xml.j2", "service_create.xml.j2"]


def test_actions_correct_differences_and_remove_strays_only_when_asked(filters):
    live = {**SNAPSHOT_ONT, "admin_state": "disabled", "description": None}
    live_services = [{**SNAPSHOT_ONT["services"][0], "vlan": 300},
                     {**SNAPSHOT_ONT["services"][0], "service_index": 9}]

    actions = filters.rollback_actions(SNAPSHOT_ONT, live, live_services)
    assert [a["template"] for a in actions] == ["ont_merge.xml.j2", "service_delete.xml.j2", "service_merge.xml.j2"]
    assert "admin_state: 'disabled' -> 'enabled'" in actions[0]["summary"]
    assert "vlan: 300 -> 100" in actions[2]["summary"]

    kept = filters.rollback_actions(SNAPSHOT_ONT, live, live_services, remove_extra_services=False)
    assert "service_delete.xml.j2" not in [a["template"] for a in kept]


def test_login_refusal_is_reported_plainly(filters):
    from ansible.errors import AnsibleFilterError
    refused = ('<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body><auth-reply>'
               "<ResultCode>6</ResultCode></auth-reply></Body></Envelope>")
    with pytest.raises(AnsibleFilterError, match="CMS refused the login"):
        filters.cms_session_id(refused)


# --- The playbook, end to end ---------------------------------------------------------

@pytest.fixture
def cms(nbi_server):
    server, port = nbi_server
    server._fresh_state()
    return server, f"http://127.0.0.1:{port}/cmsexc/ex/netconf"


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=test", "-c", "user.email=test@example.invalid", *args],
                   check=True, capture_output=True)


@pytest.fixture
def backups(tmp_path):
    repo = tmp_path / "network-backups"
    (repo / "olts").mkdir(parents=True)
    shutil.copy(SNAPSHOT, repo / "olts" / "OLT-MOCK.yml")
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    git(repo, "add", ".")
    git(repo, "commit", "-q", "-m", "Nightly snapshot")
    return repo


def state(server):
    cms = server.app.state.cms
    onts = [cms._snapshot(o).model_dump(mode="json", exclude={"oper_state"}) for o in cms._onts.values()]
    return sorted(onts, key=lambda o: o["ont_id"])


def rollback(url, backups, *args, password="demo-password"):
    env = {**os.environ, "CMS_NBI_URL": url, "CMS_NBI_USER": "nbi-demo", "CMS_NBI_PASSWORD": password,
           "NETWORK_BACKUPS_REPO": str(backups), "ANSIBLE_NOCOLOR": "1", "no_proxy": "127.0.0.1,localhost",
           "NO_PROXY": "127.0.0.1,localhost", "PATH": f"{Path(sys.executable).parent}:{os.environ['PATH']}"}
    return subprocess.run(["ansible-playbook", "rollback.yml", *args], cwd=ROLLBACK, env=env,
                          capture_output=True, text=True, timeout=300)


def test_rollback_restores_a_damaged_pon(cms, backups, tmp_path):
    server, url = cms
    server._fresh_state()
    baseline = state(server)
    damage = server.app.state.cms.damage("1/1/1")
    damaged = state(server)
    assert len(damage) == 4 and damaged != baseline

    # Dry run: prints the plan, changes nothing
    dry = rollback(url, backups, "-e", "target_olt=OLT-MOCK", "--check")
    assert dry.returncode == 0, dry.stdout + dry.stderr
    assert "DRY RUN" in dry.stdout and "would be restored: 4" in dry.stdout
    assert "create ONT 1 (CXNK00A3F101)" in dry.stdout
    assert state(server) == damaged

    # A scope that excludes the damage restores nothing
    other_pon = rollback(url, backups, "-e", "target_olt=OLT-MOCK", "-e", "target_interface=1/1/2")
    assert other_pon.returncode == 0 and "6 ONTs checked" in other_pon.stdout
    assert "already matching: 6" in other_pon.stdout
    assert state(server) == damaged

    # The real thing
    report = tmp_path / "report.json"
    run = rollback(url, backups, "-e", "target_olt=OLT-MOCK", "-e", f"report_path={report}")
    assert run.returncode == 0, run.stdout + run.stderr
    assert "restored: 4" in run.stdout and "failed: 0" in run.stdout
    assert state(server) == baseline

    record = json.loads(report.read_text())
    assert record["olt"] == "OLT-MOCK" and record["dry_run"] is False and len(record["results"]) == 14
    restored = {r["ont_id"]: r["changes"] for r in record["results"] if r["status"] == "restored"}
    assert sorted(restored) == [1, 2, 3, 4]
    assert restored[1] == ["create ONT 1 (CXNK00A3F101)", "create service 1 on port 1 of ONT 1 (VLAN 100)"]

    # Running it again sends nothing
    again = rollback(url, backups, "-e", "target_olt=OLT-MOCK")
    assert again.returncode == 0 and "already matching: 14" in again.stdout
    assert state(server) == baseline
    assert server.app.state.sessions == set()  # every run logged out


def test_one_failing_ont_does_not_stop_the_rest(cms, backups):
    server, url = cms
    server._fresh_state()
    # A snapshot that asks for something CMS will refuse on ONT 5...
    path = backups / "olts" / "OLT-MOCK.yml"
    snapshot = yaml.safe_load(path.read_text())
    next(o for o in snapshot["onts"] if o["ont_id"] == 5)["services"][0]["vlan"] = 999
    path.write_text(yaml.safe_dump(snapshot, sort_keys=False))
    git(backups, "commit", "-q", "-am", "Snapshot with a VLAN that does not exist")
    # ...while ONT 2 genuinely needs restoring
    server.app.state.cms._onts[2].state.admin_state = "disabled"

    run = rollback(url, backups, "-e", "target_olt=OLT-MOCK")

    assert run.returncode != 0
    assert "restored: 1" in run.stdout and "failed: 1" in run.stdout
    assert "VLAN 999 does not exist" in run.stdout
    assert server.app.state.cms._onts[2].state.admin_state == "enabled"
    assert server.app.state.sessions == set()

    # The older, good snapshot can still be chosen by commit
    first_commit = subprocess.run(["git", "-C", str(backups), "rev-list", "--max-parents=0", "HEAD"],
                                  capture_output=True, text=True, check=True).stdout.strip()
    pinned = rollback(url, backups, "-e", "target_olt=OLT-MOCK", "-e", f"snapshot_version={first_commit}")
    assert pinned.returncode == 0 and "already matching: 14" in pinned.stdout


def test_rollback_refuses_to_run_without_a_target_or_with_a_bad_login(cms, backups):
    server, url = cms
    server._fresh_state()
    server.app.state.cms.damage("1/1/1")
    damaged = state(server)

    no_target = rollback(url, backups)
    assert no_target.returncode != 0 and "Name the OLT to restore" in no_target.stdout

    traversal = rollback(url, backups, "-e", "target_olt=../../etc/passwd")
    assert traversal.returncode != 0 and "Name the OLT to restore" in traversal.stdout

    unknown = rollback(url, backups, "-e", "target_olt=OLT-NOPE")
    assert unknown.returncode != 0

    empty_scope = rollback(url, backups, "-e", "target_olt=OLT-MOCK", "-e", "target_card=9/9")
    assert empty_scope.returncode != 0 and "no ONTs matching that scope" in empty_scope.stdout

    bad_login = rollback(url, backups, "-e", "target_olt=OLT-MOCK", password="wrong")
    assert bad_login.returncode != 0 and "CMS refused the login" in bad_login.stdout
    assert "wrong" not in bad_login.stdout + bad_login.stderr  # the password is never printed

    assert state(server) == damaged  # none of those runs changed anything
