# gpon-cms: a provisioning API for Calix CMS

A REST and Kafka front end for legacy Calix GPON. What the SMx API is for the newer network, this is for CMS: a billing system or portal makes one call when a customer signs up, and the configuration is pushed to the ONT through the CMS northbound interface (R15.x, SOAP over HTTP). No screen-scraping and nobody typing into CMS.

It also covers the rest of an ONT's life (suspend, restore, change, disconnect) and takes the retyping out of a cutover to SMx: it exports what CMS holds for a PON in SMx's terms and plans the SMx requests as a dry run.

> **Status: tested against a simulated CMS only.** Nothing here has run against a real CMS or SMx, or a production Kafka cluster. Start with [HANDOFF.md](HANDOFF.md) for what is proven, what is not, and the first three things to do with a real CMS.

## What it does

- **Provisions an ONT piece by piece.** CMS has no single "set up this customer" command and its API cannot apply a port template, so the ONT, each Ethernet port and each service are sent as separate requests, in order. If CMS rejects a step, the ONT created in that run is removed again.
- **Runs the lifecycle as jobs that are safe to repeat.** Enable or disable a port or a service, change a service's bandwidth profile, tag action or VLAN, delete a service, delete an ONT. Each change answers `202` with a job you can poll or follow live.
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

# Watch it happen, using the events_url from that reply
curl -N http://localhost:8000/api/v2/jobs/<job id>/events

# Read it back, then suspend its service
curl -s $A/onts/15/ports
curl -s -X POST $A/onts/15/ports/eth1/services/1/disable

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

```bash
pip install -r requirements-test.txt
pytest
```

Run on Python 3.10 and 3.12. The tests run every CMS operation through the CMS client over SOAP (HTTP and HTTPS) against the mock CMS server, check it sends the same requests cms-nbi-client did, and the queue rules over the Kafka wire protocol against an in-process Kafka mock.

## Project layout

```
app/
  routes/       REST routes
  controller/   provisioning workflows and job tracking
  nbi/          CMS gateway, CMS client and backend, simulated CMS and mock CMS server
  legacy/       signup and cutover events, export, SMx plan, snapshots
  messaging/    the queue: in-process or Kafka
  worker/       background worker (python -m app.worker)
  tools/        read-only CMS check
  models/       request and response models
deploy/         Dockerfile, demo, Kubernetes manifests
docs/           architecture
tests/
main.py         the API
config.py       settings
```

## Documentation

- [HANDOFF.md](HANDOFF.md): one page on what this is, what is proven, and where to start with a real CMS
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the design, the delivery rules, and the full list of what is inferred
- [deploy/demo/README.md](deploy/demo/README.md): the multi-process demo

## Credits

The shapes of the CMS requests were learned from [cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client) by somenetworking, which this project used before it had its own client (`app/nbi/client.py`). Thank you.
