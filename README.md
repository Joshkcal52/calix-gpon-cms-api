# gpon-cms: a provisioning API for legacy Calix GPON

A REST and Kafka front end for legacy Calix GPON (E7 EXA, managed by CMS). What the SMx API is for the newer network, this is for the old one: a billing system or portal makes one call when a customer signs up, and the configuration is pushed to the ONT. No screen-scraping and nobody typing into CMS.

**Two ways to reach the network; the API is the same either way:**

1. **Through the CMS northbound interface (NBI), if your CMS has it.** This is the preferred path: CMS stays the system of record. The NBI is a separately licensed part of CMS (R15.x, SOAP over HTTP or HTTPS). Set `CMS_HOST`.
2. **Straight to each E7's command line, if it does not.** Without an NBI licence there is no API on CMS to call, so the configuration is sent over SSH to each E7 EXA node's own CLI, using Calix's documented commands. Set `E7_NODES`. See [docs/E7_CLI.md](docs/E7_CLI.md).

The path is chosen once, at startup: whichever of `CMS_HOST` and `E7_NODES` is set. If both are set, pick one with `python main.py --backend cms` or `--backend e7-cli` (or `NBI_BACKEND`). It is not a per-request fallback: if the NBI is down, jobs wait and retry rather than switching to the CLI. Billing, routes, jobs, retries and safety checks do not change between the two.

It also covers the rest of an ONT's life (suspend, restore, change, disconnect) and takes the retyping out of a cutover to SMx: it exports what CMS holds for a PON in SMx's terms and plans the SMx requests as a dry run.

> **Status: tested against a simulated CMS and a mock E7 only.** Nothing here has run against a real CMS, E7 or SMx, or a production Kafka cluster. Start with [HANDOFF.md](HANDOFF.md) for what is proven, what is not, and the first three things to do with a real CMS; [docs/E7_CLI.md](docs/E7_CLI.md) has the same for an E7.

## What it does

- **Provisions an ONT piece by piece.** CMS has no single "set up this customer" command and its API cannot apply a port template, so the ONT, each Ethernet port and each service are sent as separate requests, in order. If CMS (or the E7) rejects a step, the ONT created in that run is removed again.
- **Runs the lifecycle as jobs that are safe to repeat.** Enable or disable a port or a service, change a service's bandwidth profile, tag action, VLAN or peak-rate overrides, delete a service, delete an ONT. Each change answers `202` with a job you can poll or follow live.
- **Handles ONTs with a built-in router, such as the 844G.** Each Ethernet port is either a plain bridge or belongs to the router. The mode is read from CMS for every port and never assumed from the ONT model.
- **Exports for cutovers.** One PON port, one card or a whole network, as CSV and JSON laid out for SMx, with the existing serial numbers carried over and blank columns for what only the new side knows. Router-capable ONTs running as bridges are flagged. **Voice is not exported** (see below): every ONT with voice ports is marked `voice: not checked`.
- **Plans the SMx requests as a dry run.** Nothing is sent to SMx. A profile with no mapping fails that subscriber with a message; nothing is guessed.
- **Compares a saved snapshot with CMS**, read-only, to show what has changed.
- **Checks a real CMS without changing it.** One command logs in, reads, logs out, and reports what it understood.
- **Survives a bad day.** Every CMS call has a timeout and goes through a circuit breaker. Queued work waits out a CMS outage and is picked up by another worker if one dies mid-job.

## Try it in five minutes

No CMS, Kafka or API key needed. Python 3.10 to 3.12.

```bash
pip install -r requirements.txt
MOCK_MODE=true python main.py
```

That simulates CMS. To try the E7 command-line path instead, `python main.py --mock --backend e7-cli` starts a mock E7 inside the API (its network is `E7-MOCK`).

Interactive docs for every route are at http://localhost:8000/docs. In another terminal:

```bash
A=http://localhost:8000/api/v2/networks/NTWK-MOCK-E7

# ONTs the simulated CMS has seen but not provisioned
curl -s $A/onts/discovered

# Sign a customer up: one port configured, one data service. Answers 202 with a job.
curl -s -X POST $A/signups -H 'content-type: application/json' -d '{
  "subscriber_id": "SUB-3001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
  "ports": [{"port": 1, "security_profile": "2"}],
  "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}'

# (RES-1G and tag action 1 are the simulated CMS's. A real CMS takes numeric IDs for both:
# bw-prof 1-300 and tag-action 1-255, as listed in BANDWIDTH_PROFILES)

# Watch it happen, using the events_url from that reply
curl -N http://localhost:8000/api/v2/jobs/<job id>/events

# Read it back, then suspend its service
curl -s $A/onts/15/ports
# (every change names the ONT's serial, and a service change its VLAN, or none: IDs and numbers are reused)
curl -s -X POST "$A/onts/15/ports/eth1/services/1/disable?serial_number=CXNK00C4D301&expect_vlan=100"

# The cutover sheet for one PON, and the SMx plan for one subscriber
curl -s "$A/export?pon=1/1/1&format=csv" > pon.csv
curl -s $A/subscribers/SUB-1003/smx-plan
```

## API overview

All routes are under `/api/v2`. Changes answer `202 Accepted` with a job; reads answer directly.

| Area | Routes |
|---|---|
| Sign up and hand over | `POST /networks/{network}/signups`, `POST /networks/{network}/cutovers`, `POST /networks/{network}/activations` |
| Read | `GET /networks/{network}/onts/discovered`, `GET .../onts/{ont_id}`, `GET .../onts/{ont_id}/ports`, `POST .../onts/status-check` |
| Lookups | `GET /networks/{network}/ont-profiles`, `GET /networks/{network}/vlans` |
| Ports | `POST .../onts/{ont_id}/ports/{port}/enable`, `.../disable`, `PATCH .../ports/{port}` |
| Services | `POST .../ports/{port}/services/{n}/enable`, `.../disable`, `PATCH` and `DELETE .../services/{n}` |
| | Every port and service change needs `serial_number`, and every service change `expect_vlan` (the service's outer VLAN, or `none`): ONT IDs and service numbers are reused, so a change whose target no longer matches is refused. Every change also takes `dry_run=true`. |
| Disconnect | `DELETE /networks/{network}/onts/{ont_id}` |
| Cutover | `GET /networks/{network}/export`, `GET .../subscribers/{subscriber_id}/smx-plan`, `POST /smx/plans`, `POST /networks/{network}/snapshot-compare` |
| Jobs | `GET /jobs/`, `GET /jobs/{job_id}`, `GET /jobs/{job_id}/events` (Server-Sent Events) |
| Health | `GET /nbi/health` (CMS backend and circuit breaker) |

Ports are named `eth1` to `eth8` for Ethernet ports and `rg1` for the router side. `GET /api/v2/status` reports the mode, queue and CMS backend in use; `GET /health` is open, without a key, for probes.

## What it does not do: voice

Only Ethernet data services are read from CMS. Voice (POTS ports and their services) is not provisioned, exported or planned. So that a cutover sheet never looks complete when it is not, every export record and every SMx plan carries a `voice` status:

| `voice` | Meaning |
|---|---|
| `not checked` | The ONT's profile has voice ports. Their service was not read: check it by hand. |
| `unknown` | The voice port count could not be read (the ONT or its ports could not be read, ports were skipped, or the profile has no count). Check by hand. |
| `none` | The profile has no voice ports. |

It is the second column of the services CSV. The export and the plan count the first two (`voice_not_checked`), and the command-line tools print that count. The SMx plan's `ready` covers data services only.

The `XGS_CUTOVER` event (`POST /networks/{network}/cutovers`) does not carry voice either, and its migration request has no voice flag yet.

Deleting an ONT whose profile has voice ports needs `voice_checked=true`: someone has checked in CMS that it carries no voice service to keep. The ONT itself is deleted without CMS's force, so a voice service still on it should make CMS refuse the delete rather than lose the line.

## Command-line tools

```bash
# Read-only check of a real CMS. Sends logins and reads only.
python -m app.tools.cms_check --network NTWK-... --ont-id <id>

# Export a PON, a card or a network for a cutover
python -m app.legacy.export --network NTWK-... --pon 1/1/1 --output-dir ./cutover

# Plan the SMx requests from a filled-in export (dry run)
python -m app.legacy.smx --input ./cutover/<file>.services.csv --output plan.json

# Compare a saved snapshot with what CMS holds now
python -m app.legacy.snapshot --compare ./cutover/<file>.snapshot.yml
```

## Trying it at scale (mocks only)

`MOCK_PON_SIZE` (or `python main.py --mock --pon-size 64`) fills one simulated PON with up to 64 discovered ONTs, a mix of 844G, 812G and 716GE-I. `tools/sim_signups.py` signs up a customer per ONT through the API, reads everything back, sends the set again to check nothing new is sent, runs a lifecycle round, and on the E7 path counts the CLI commands and injects session faults. It talks to the mocks only.

```bash
python tools/sim_signups.py --backend cms --customers 64
python tools/sim_signups.py --backend e7-cli --customers 64 --e7-latency 0.05
python tools/sim_signups.py --backend e7-cli --customers 32 --faults
```

The numbers and what they showed are in [docs/SIM_RESULTS.md](docs/SIM_RESULTS.md). They check the API's behaviour, not real CMS or E7 speed.

## Connecting to a real CMS

```bash
export CMS_HOST=cms.example.internal CMS_USERNAME=... CMS_PASSWORD=...
export CMS_SCHEME=https CMS_PORT=<the HTTPS NBI port>        # if your CMS offers HTTPS
export CMS_CA_FILE=/path/to/cms-ca.pem                        # if its certificate is from a private CA
export API_KEYS=portal:choose-a-long-random-key
python -m app.tools.cms_check --network NTWK-... --ont-id <id>   # do this first
```

Before the first change on a real CMS, know that:

- **Use HTTPS if CMS offers it.** The default is plain HTTP on port 18080, as CMS ships, and then the CMS login crosses the network unencrypted. With `CMS_SCHEME=https` the server's certificate is checked (against `CMS_CA_FILE` if set); `CMS_VERIFY_TLS=false` turns that off, with a warning. HTTPS has only been tested against the mock server.
- **Router-side support is switched off until configured.** How CMS addresses the router side of ONTs such as the 844G is not known here. Until the four `CMS_RG_*` settings are filled in from a real CMS, router-side changes are refused and port modes read as `unknown`. The read-only check prints what to set; details are in `app/nbi/router_side.py`.
- **Voice is not handled.** See "What it does not do: voice" above.
- **CMS expects database IDs, not names**, for profiles and tag actions, and network names in the form `NTWK-...`.
- **The mapping files hold demo values.** Replace `app/legacy/profile_map.yml` and `app/legacy/smx_map.yml` with your own.

## Without the CMS NBI: the E7 command line

If your CMS has no NBI licence, the API can provision through each E7 EXA's own CLI over SSH instead. Set `E7_NODES` (each name becomes a `{network}`). With only one of `CMS_HOST` and `E7_NODES` set, that one is used. With both set, choose at startup with `python main.py --backend cms` or `--backend e7-cli` (or `NBI_BACKEND` in `.env`). Everything above the backend works the same. The commands follow Calix's public E7 EXA CLI reference, but Calix publishes almost no sample output, so the parsers are unverified. Run the read-only capture first:

```bash
export E7_NODES=OLT-04=10.0.0.4 E7_USERNAME=... E7_PASSWORD=... NBI_TIMEOUT_SECONDS=30
python -m app.tools.e7_capture --network OLT-04 --ont-id <id> --out ./e7-captures   # show commands only
```

How each operation maps to a command, and what is still unknown, is in [docs/E7_CLI.md](docs/E7_CLI.md).

## Configuration

Settings come from environment variables or a `.env` file. `env_example.txt` has the common ones and `config.py` has every setting. The main ones:

| Variable | Purpose |
|---|---|
| `MOCK_MODE` | `true` uses the simulated CMS. Nothing real is touched. |
| `CMS_HOST`, `CMS_PORT`, `CMS_USERNAME`, `CMS_PASSWORD` | The real CMS. Ignored when `MOCK_MODE` is on. |
| `CMS_SCHEME`, `CMS_VERIFY_TLS`, `CMS_CA_FILE` | `http` (default) or `https`; the certificate check (on by default) and the CA to check it against. |
| `API_KEYS` | Comma-separated keys (or `name:key`) required on every `/api/v2` route. Optional only in mock mode. |
| `CORS_ALLOW_ORIGINS` | Browser origins allowed to call the API, comma-separated. Empty (the default) allows none. |
| `RELOAD` | `true` restarts `python main.py` on code changes. Off by default. |
| `KAFKA_BOOTSTRAP_SERVERS` | Set to queue work in Kafka. Empty queues inside the API process. |
| `WORKER_LANES` | Which queues a worker serves: `provisioning`, `legacy`. |
| `KAFKA_TOPIC_PREFIX` | Prefix of the command and job-update topics. Default `gpon-cms`. |
| `LEGACY_EVENTS_TOPIC`, `XGS_MIGRATION_TOPIC` | Topic for signup and cutover events in, and migration requests out. |
| `CMS_RG_*` | Router-side addressing, set from a real CMS. |
| `NBI_TIMEOUT_SECONDS`, `NBI_BREAKER_*` | Timeout and circuit breaker for CMS calls. |

## Running with Kafka and workers

By default the API queues work in its own process, which is enough to try it or to run small. With `KAFKA_BOOTSTRAP_SERVERS` set, the API only validates and queues, and stateless workers do the work:

```bash
pip install -r requirements-kafka.txt
WORKER_EMBEDDED=false uvicorn main:app --port 8000
WORKER_LANES=legacy,provisioning python -m app.worker
```

Workers commit a Kafka offset only once a command has succeeded or its failure has been recorded, so a worker that dies mid-job loses nothing. Producers other than the API can write signup and cutover events straight to the events topic.

- A demo with Kafka, the API, a worker and a mock CMS server: [deploy/demo/README.md](deploy/demo/README.md)
- Container image and Kubernetes manifests: `deploy/Dockerfile` and `deploy/k8s/gpon-cms.yaml`. The image has been built and the demo Compose file run with it; the Kubernetes manifests have not been applied to a cluster.

## Tests

In a virtualenv, so the pinned versions are the ones tested:

```bash
python -m venv venv && . venv/bin/activate      # Windows PowerShell: python -m venv venv; .\venv\Scripts\Activate.ps1
pip install -r requirements-test.txt
python -m pytest
```

CI (`.github/workflows/tests.yml`) runs the same on Linux with Python 3.10 and 3.12 on every push and pull request. The tests run every CMS operation through the CMS client over SOAP (HTTP and HTTPS) against the mock CMS server, check it sends the same requests cms-nbi-client did, and the queue rules over the Kafka wire protocol against an in-process Kafka mock.

## Project layout

```
app/
  routes/       REST routes
  controller/   provisioning workflows and job tracking
  nbi/          CMS gateway, CMS client and backend, E7 CLI backend, simulated CMS, mock CMS server and mock E7
  legacy/       signup and cutover events, export, SMx plan, snapshots
  messaging/    the queue: in-process or Kafka
  worker/       background worker (python -m app.worker)
  tools/        read-only CMS check and E7 capture
  models/       request and response models
deploy/         Dockerfile, demo, Kubernetes manifests
docs/           architecture, the E7 CLI, simulation results
tests/
tools/          sim_signups.py: signups at scale against the mocks
main.py         the API
config.py       settings
```

## Documentation

- [HANDOFF.md](HANDOFF.md): one page on what this is, what is proven, and where to start with a real CMS
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the design, the delivery rules, and the full list of what is inferred
- [docs/E7_CLI.md](docs/E7_CLI.md): provisioning through each E7's command line, and what is unverified
- [docs/CUTOVER.md](docs/CUTOVER.md): what the cutover tools hand to the XGS side, and what to know before relying on them
- [docs/SIM_RESULTS.md](docs/SIM_RESULTS.md): 32 and 64 simulated customers on both backends, with fault injection
- [docs/KNOWN_UNKNOWNS.md](docs/KNOWN_UNKNOWNS.md): every open risk, guess and gap in one place, who owns the CMS work, the license question, and how the code was written
- [deploy/demo/README.md](deploy/demo/README.md): the multi-process demo

## Credits

The shapes of the CMS requests were learned from [cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client) by somenetworking, which this project used before it had its own client (`app/nbi/client.py`). Thank you.
