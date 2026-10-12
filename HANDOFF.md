# Handoff: gpon-cms, a provisioning API for legacy Calix GPON (CMS)

**What this is.** What the SMx API is for XGS, this is for CMS R15.x (SOAP over HTTP or HTTPS). It provisions an ONT piece by piece (the ONT, each Ethernet port, each service, on a plain port or on the built-in router of an 844G, with or without an outer VLAN, with optional peak-rate overrides), runs the rest of the lifecycle as repeat-safe jobs (enable/disable ports and services, change or delete a service, delete an ONT), and exports a PON, card or network for a GPON-to-XGS cutover in SMx's terms, with a dry-run plan of the SMx requests. It only ever talks to CMS: nothing here writes to SMx or Sonar. **Voice is out of scope**: only Ethernet data services are read, so the export and the SMx plan mark every ONT with voice ports `voice: not checked`, and `unknown` where the count could not be read. Those need checking by hand. The `XGS_CUTOVER` event's migration request has no voice flag yet. Design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Five minutes against the mock

```bash
pip install -r requirements.txt
MOCK_MODE=true python main.py          # one process, simulated CMS, no Kafka, no API key needed
python main.py --mock --backend e7-cli # the same over the E7 CLI path, against a mock E7 (network E7-MOCK)
```

In another terminal (`http://localhost:8000/docs` has every route):

```bash
A=http://localhost:8000/api/v2/networks/NTWK-MOCK-E7
# Provision an 844G: port 1 configured, the data service on the router side. Answers 202 with a job.
curl -s -X POST $A/signups -H 'content-type: application/json' -d '{"subscriber_id": "SUB-3001",
  "serial_number": "CXNK00C4D301", "profile": "844G-1", "ports": [{"port": 1, "security_profile": "2", "mode": "router"}],
  "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}]}'
curl -s $A/onts/15/ports                                       # ports, modes, router side, read back
curl -s -X POST "$A/onts/15/ports/rg1/services/1/disable?serial_number=CXNK00C4D301&expect_vlan=100"  # another job
curl -s "$A/export?pon=1/1/1&format=csv" > pon.csv            # the cutover spreadsheet, fill_in_ columns blank
curl -s $A/subscribers/SUB-1003/smx-plan                       # SMx requests for one subscriber (dry run)
```

In the export, ONTs 3, 4, 7 and others are 844Gs whose ports all run as bridges: they are flagged "keep it a bridge". Every simulated profile has voice ports, so every ONT reads `voice: not checked`.

## What is proven (against the mock, by the test suite on Python 3.10 and 3.12, run by CI on every push)

- Every CMS operation through the project's own CMS client (`app/nbi/client.py`) over real SOAP to the mock NBI server. Each sends the same requests the cms-nbi-client library sent, recorded before the library was removed: a test compares them. HTTPS is tested with a private CA (the certificate is checked by default), and so is paging, against the mock's.
- Every change is repeat-safe (each test runs it twice). A failed provisioning deletes the ONT it created. A service without a VLAN is known by its port and tag action, so a repeat neither duplicates nor skips one.
- The queue's delivery rules over the Kafka wire protocol (librdkafka's mock cluster), including a worker killed mid-job. The demo (`deploy/demo/`) has also run end to end in Docker against a single Apache Kafka 3.7.1 broker.
- The read-only check sends no write (a guard refuses one) and blanks the password in its dumps. API keys are enforced on every `/api/v2` route. The old `/api/v1` SSH routes are gone, and CORS allows no origin unless `CORS_ALLOW_ORIGINS` lists it.

## What is not proven

Every open risk, guess and gap is listed in **[docs/KNOWN_UNKNOWNS.md](docs/KNOWN_UNKNOWNS.md)**. The headlines:

- **Nothing has touched a real CMS, SMx, Sonar or production Kafka cluster.** Reply fields cms-nbi-client never parsed are guessed, and a wrong guess reads as empty ("no services"), not as an error. **The receiving team owns the first CMS check**: nobody on this project has had CMS access (what it needs is on that page).
- **The 844G router side is not known** (bridge or router per port, router-side services, WAN protocol): off until configured in `app/nbi/router_side.py`. **Voice is not read**, so an ONT with voice ports is only deleted with `voice_checked=true`, and never with CMS's force; that CMS then refuses a delete while a service is left is not confirmed.
- **Before it goes live**: set `BANDWIDTH_PROFILES` from CMS, `API_KEYS` and a real image; there is no shared circuit breaker and no metrics, and the limits on queued changes are per API process. Give each API key only the scopes it needs (`name:key:read+change`). Each change records the API key that asked for it (`requested_by`, audit lines in `app/audit.py`).
- **Licensed GPL-3.0**, the same as cms-nbi-client, whose requests it learned from; **most of the code was written by Claude** and should be reviewed as unfamiliar code. Both are explained on that page.
- **The E7 CLI backend (for a CMS without the NBI) has never met a real E7.** Its commands come from Calix's public E7 EXA R3.x CLI reference, but Calix shows sample output for only one table (show mcast, whose dashed-line layout the table parser follows), so its parsers, the prompt and the error wording are guesses that the mock E7 shares. Run `python -m app.tools.e7_capture` (show commands only) and fix the parsers against real captures before any write. See `docs/E7_CLI.md`.

## First three things to do with a real CMS

1. **Run the read-only check** against an 844G running as a router, then one running as a bridge: `CMS_HOST=... CMS_USERNAME=... CMS_PASSWORD=... [CMS_SCHEME=https CMS_PORT=... CMS_CA_FILE=...] python -m app.tools.cms_check --network NTWK-... --ont-id <id> --dump ./replies`. Read "unrecognised reply fields", "raw values of inferred fields" and "service ONT slots seen". Use them to set the four `CMS_RG_*` settings (instructions in `router_side.py`) and to fix any inferred field. It changes nothing.
2. **Export one PON read-only and check it against what the techs know**: `python -m app.legacy.export --network NTWK-... --pon 1/1/1`. Check serials, VLANs, profiles, and that each 844G's bridge/router mode matches the field. Keep the `.snapshot.yml`; `python -m app.legacy.snapshot --compare` shows any later drift.
3. **Make the first write on one spare lab ONT, with no voice**: `python -m app.tools.lab_cycle --network NTWK-... --pon 1/1/1 --serial <spare> --profile <profile> --vlan <v> --tag-action <id> --bandwidth-profile <id> --bad-bandwidth-profile <an id that does not exist> --change-bandwidth-profile <another id> --snapshot-dir ./lab`. It snapshots the PON, checks that a bad bandwidth profile is refused and rolled back, activates, runs the activation again, toggles the port and the service, changes the bandwidth profile, checks that CMS refuses a delete without force while the service is still on the ONT, deletes the ONT the way the API does (service first, then the ONT without CMS's force), and compares the PON with the first snapshot. It only touches an ONT it creates, asks for the serial number to be typed back, and cleans up with force if a step fails. Rehearse it first with `MOCK_MODE=true` (serial `CXNK00C4D301`, profile `844G-1`, VLAN 100, tag action 1, profiles `RES-1G` and `VOICE`). **If the refused-delete step fails, stop**: CMS removes services with an ONT even without force, so the API's delete and rollback could remove a voice service unseen. **If the rollback or delete step fails but the cleanup works**, CMS insists on force even with nothing left on the ONT: the ONT delete in `app/controller/lifecycle_controller.py` and the rollback in `app/controller/provisioning_controller.py` must change before either can work. Then set `BANDWIDTH_PROFILES` from CMS, and only then point Sonar or the portal at it, with `API_KEYS` set.

## When this project is done

This is a bridge: it carries the legacy GPON network until the cutover to SMx, and every CMS or E7 release it has to follow until then is maintenance with no end. It is done, and should be switched off rather than extended, when all of these hold:

- **Every PON is cut over.** Each one has been exported, its subscribers moved to SMx, and a snapshot compare shows it empty on CMS.
- **The sunset date has passed.** **Owner to set: ____.** From that date new signups go to SMx only, and billing stops sending `signups` here.
- **The supported releases are pinned.** **Owner to set: CMS ____ and E7 ____** (the ones with recordings in `tests/fixtures/`). A new release is a decision to make, not a patch to write.
- **Nothing new is added unless it shortens the cutover.** Lifecycle changes stay as they are until the last PON moves.

Until then, [docs/ANTI_PATTERNS.md](docs/ANTI_PATTERNS.md) lists what still needs doing before this carries live subscribers.

## Credits

The CMS request shapes were learned from [cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client) by **somenetworking**, which this project used until it had its own client.
