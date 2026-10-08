# Handoff: gpon-cms, a provisioning API for legacy Calix GPON (CMS)

**What this is.** What the SMx API is for XGS, this is for CMS R15.x (SOAP over HTTP or HTTPS). It provisions an ONT piece by piece (the ONT, each Ethernet port, each service, on a plain port or on the built-in router of an 844G, with or without an outer VLAN, with optional peak-rate overrides), runs the rest of the lifecycle as repeat-safe jobs (enable/disable ports and services, change or delete a service, delete an ONT), and exports a PON, card or network for a GPON-to-XGS cutover in SMx's terms, with a dry-run plan of the SMx requests. It only ever talks to CMS: nothing here writes to SMx or Sonar. **Voice is out of scope**: only Ethernet data services are read, so the export and the SMx plan mark every ONT with voice ports `voice: not checked`, and `unknown` where the count could not be read. Those need checking by hand. The `XGS_CUTOVER` event's migration request has no voice flag yet. Design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Five minutes against the mock

```bash
pip install -r requirements.txt
MOCK_MODE=true python main.py          # one process, simulated CMS, no Kafka, no API key needed
```

In another terminal (`http://localhost:8000/docs` has every route):

```bash
A=http://localhost:8000/api/v2/networks/NTWK-MOCK-E7
# Provision an 844G: port 1 configured, the data service on the router side. Answers 202 with a job.
curl -s -X POST $A/signups -H 'content-type: application/json' -d '{"subscriber_id": "SUB-3001",
  "serial_number": "CXNK00C4D301", "profile": "844G-1", "ports": [{"port": 1, "security_profile": "2", "mode": "router"}],
  "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}]}'
curl -s $A/onts/15/ports                                       # ports, modes, router side, read back
curl -s -X POST $A/onts/15/ports/rg1/services/1/disable        # a lifecycle change (another job)
curl -s "$A/export?pon=1/1/1&format=csv" > pon.csv            # the cutover spreadsheet, fill_in_ columns blank
curl -s $A/subscribers/SUB-1003/smx-plan                       # SMx requests for one subscriber (dry run)
```

In the export, ONTs 3, 4, 7 and others are 844Gs whose ports all run as bridges: they are flagged "keep it a bridge". Every simulated profile has voice ports, so every ONT reads `voice: not checked`.

## What is proven (against the mock, by the test suite on Python 3.10 and 3.12)

- Every CMS operation through the project's own CMS client (`app/nbi/client.py`) over real SOAP to the mock NBI server. Each sends the same requests the cms-nbi-client library sent, recorded before the library was removed: a test compares them. HTTPS is tested with a private CA (the certificate is checked by default), and so is paging, against the mock's.
- Every change is repeat-safe (each test runs it twice). A failed provisioning deletes the ONT it created. A service without a VLAN is known by its port and tag action, so a repeat neither duplicates nor skips one.
- The queue's delivery rules over the Kafka wire protocol (librdkafka's mock cluster), including a worker killed mid-job. The demo (`deploy/demo/`) has also run end to end in Docker against a single Apache Kafka 3.7.1 broker.
- The read-only check sends no write (a guard refuses one) and blanks the password in its dumps. API keys are enforced on every `/api/v2` route. The old `/api/v1` SSH routes are gone, and CORS allows no origin unless `CORS_ALLOW_ORIGINS` lists it.

## What is not proven

- **Nothing has touched a real CMS, SMx, Sonar or production Kafka cluster** (only one demo broker, with no replication). Request shapes come from cms-nbi-client; any reply field it did not parse is inferred (list in ARCHITECTURE.md). HTTPS and paging have only met the mock server.
- **The router side of the 844G is not confirmed.** CMS NBI documentation for an earlier release suggests router-side services in ONT slot 8, the router port as `OntRg`/`ontrg`, a full-bridge interface as `OntFb` in slot 9, and each Ethernet port's binding in an `<intf>` element (`OntRg` = router, `OntFb` = bridge). Those values are in `env_example.txt` as suggestions, not defaults, because they are not from R15.x and the slot is not certain. Until `cms_check` on a real 844G agrees and the `CMS_RG_*` settings are set, router writes are refused and modes read as `unknown` (`app/nbi/router_side.py`). Services in slots other than 3 (Fast Ethernet 5, full bridge 9) read as `unknown`. WAN protocol and static IP are not built.
- **Port numbers and VLAN rules.** IDI's "200 + the port number" most likely means E7 OS xDSL ports (201–248), not ONT Ethernet ports, which are sent as 1–8 (inferred; `cms_check` settles it). Which tag actions need an outer VLAN is set on each CMS; the mock's rule is invented.
- **Whether customers keep their ONT through a cutover is not settled.** Both are supported: the export carries the existing serial, and a replacement serial can be filled in.
- **Voice is not read at all.** The voice status comes from the profile's `pots` attribute, whose meaning as a port count is inferred (`cms_check` prints its raw values).
- SMx parameter names come from the IDI billing docs, not from SMx. All mapping files hold demo values. The Kubernetes manifests have never been applied.

- **The E7 CLI backend (for a CMS without the NBI) has never met a real E7.** Its commands come from Calix's public E7 EXA R3.x CLI reference, but Calix shows sample output for only one table (show mcast, whose dashed-line layout the table parser follows), so its parsers, the prompt and the error wording are guesses that the mock E7 shares. Run `python -m app.tools.e7_capture` (show commands only) and fix the parsers against real captures before any write. See `docs/E7_CLI.md`.

## First three things to do with a real CMS

1. **Run the read-only check** against an 844G running as a router, then one running as a bridge: `CMS_HOST=... CMS_USERNAME=... CMS_PASSWORD=... [CMS_SCHEME=https CMS_PORT=... CMS_CA_FILE=...] python -m app.tools.cms_check --network NTWK-... --ont-id <id> --dump ./replies`. Read "unrecognised reply fields", "raw values of inferred fields" and "service ONT slots seen". Use them to set the four `CMS_RG_*` settings (instructions in `router_side.py`) and to fix any inferred field. It changes nothing.
2. **Export one PON read-only and check it against what the techs know**: `python -m app.legacy.export --network NTWK-... --pon 1/1/1`. Check serials, VLANs, profiles, and that each 844G's bridge/router mode matches the field. Keep the `.snapshot.yml`; `python -m app.legacy.snapshot --compare` shows any later drift.
3. **Make the first write on one spare lab ONT**: a signup with a port and a service, each lifecycle change, then delete it. Compare a snapshot before and after. Only then point Sonar or the portal at it, with `API_KEYS` set.

## Credits

The CMS request shapes were learned from [cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client) by **somenetworking**, which this project used until it had its own client.
