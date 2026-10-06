# Emergency rollback

Restores the ONTs on one OLT to a snapshot kept in the `network-backups` Git repository. It talks straight to the Calix CMS northbound interface with Ansible's `uri` module and needs nothing else from this project, so it still works when the API, Kafka or the workers are what is broken.

## Run it

```bash
export CMS_NBI_URL=http://cms.example.internal:18080/cmsexc/ex/netconf
export CMS_NBI_USER=...
export CMS_NBI_PASSWORD=...
export NETWORK_BACKUPS_REPO=git@git.example.internal:netops/network-backups.git
cd ansible/rollback

# 1. See what would change. Nothing is written.
ansible-playbook rollback.yml -e target_olt=OLT-04 --check

# 2. Restore.
ansible-playbook rollback.yml -e target_olt=OLT-04
```

Narrow the scope with any of these:

| Variable | Restores | Example |
|---|---|---|
| `target_olt` (required) | Every ONT in that OLT's snapshot | `-e target_olt=OLT-04` |
| `target_card` | One line card, as shelf/card | `-e target_card=1/2` |
| `target_interface` | One PON port, as shelf/card/port | `-e target_interface=1/2/3` |
| `target_ont` | One ONT | `-e target_ont=1204` |

Other options:

| Option | Meaning |
|---|---|
| `-e snapshot_version=3f2a9c1` | Restore from an older commit, tag or branch instead of the newest snapshot |
| `-e remove_extra_services=false` | Leave services that are not in the snapshot in place |
| `-e report_path=./rollback.json` | Write a JSON record of the run |
| `-f 200` | Restore more ONTs at once (default 50, set in `ansible.cfg`) |

The run always ends with a summary: how many ONTs already matched, how many were restored, how many failed and why. One ONT failing does not stop the others. The exit code is non-zero if any ONT failed.

## What it changes, and what it never does

For each ONT in the snapshot that is in scope:

- **Missing on CMS:** it is created, with its services.
- **Different on CMS:** serial number, profile, admin state, subscriber ID and description are set back to the snapshot.
- **Services:** missing ones are created, changed ones are corrected, and ones that are on the ONT but not in the snapshot are removed (unless `remove_extra_services=false`).
- **Already matching:** nothing is sent.

It never touches an ONT that is not in the snapshot. A customer provisioned after the snapshot was taken keeps their service, even if they are on the PON being restored.

**Check the snapshot date before you restore.** The plan printed at the start shows when the snapshot was committed. If snapshots are taken nightly and the damage happened yesterday, the newest snapshot may already contain the damage: use `snapshot_version` to pick an older one.

## The snapshot format

One YAML file per OLT at `olts/<OLT name>.yml`:

```yaml
format: 1
olt: OLT-04
network: NTWK-OLT-04          # CMS network name
captured_at: '2026-10-06T03:00:00+00:00'
onts:
  - ont_id: 1204
    serial_number: CXNK00A3F104
    profile: '162'
    admin_state: enabled
    subscriber_id: '100482'
    description: 118 Alder St
    pon_port: 1/2/3           # shelf/card/port, used for scoping only
    services:
      - ont_port: 1
        service_index: 1
        vlan: 100             # null when the tag action sets the VLAN
        inner_vlan: null
        bandwidth_profile: '12'
        tag_action: '3'
        admin_state: enabled
        description: null
```

Produce these files with the exporter in this repository, on a schedule, and commit them:

```bash
python -m app.legacy.snapshot --olt OLT-04 --network NTWK-OLT-04 --output olts/OLT-04.yml
```

## Limits you should know about

- **Not tested against a real CMS.** It has been run against the mock NBI server in this repository only. The request XML matches what `cms-nbi-client` sends; the reply fields it reads are partly inferred.
- **It is not a full CMS restore.** A snapshot holds ONT identity, profile, admin state, subscriber, description and Ethernet services. Voice ports, multicast, ONT port settings and everything else in CMS are not captured and not restored.
- **If your existing backups are in another format** (raw CMS or E7 exports), this playbook cannot read them. Either take snapshots with the exporter above or adapt `filter_plugins/cms_nbi.py`.
- **Speed is limited by the machine running Ansible.** Each ONT costs two CMS reads plus one request per change, and Ansible starts a process for each. On a 2-core machine, 414 ONTs took about 3 minutes regardless of `-f`. More cores help; forks beyond the core count do not.
- **Every request goes to the one CMS server.** High fork counts are load on CMS. Its limits are not known here.
- **The CMS login crosses the network in clear text** unless `CMS_NBI_URL` is an `https` URL.
- **A failed ONT may be partly restored.** The summary names the step that failed. Run the playbook again once the cause is fixed; it only sends what is still different.
