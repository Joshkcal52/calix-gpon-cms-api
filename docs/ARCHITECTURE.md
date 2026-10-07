# Architecture

## What this project is for

A provisioning API for legacy Calix GPON: the CMS-side counterpart of what the SMx API provides for XGS. CMS (R15.x) speaks SOAP/XML over HTTP on port 18080. This service puts validated REST/JSON and Kafka events in front of it, so a billing system such as Sonar can sign a customer up and have the configuration land on the legacy ONT without anyone typing it into the CMS screens.

CMS provisions object by object: the ONT, each Ethernet port and each service are separate requests, and the API cannot apply the port templates techs use in the CMS screens. This service sends each piece itself.

Around that core sit the cutover tools: an export of a network in SMx's terms, a dry-run plan of the SMx requests, snapshots with a read-only compare, and a read-only check for a real CMS.

Every route is under `/api/v2` and needs an API key, except `/` and `/health`. The project's earlier `/api/v1` routes (SSH and CLI scraping) have been removed.

**Voice is out of scope.** Only Ethernet data services are read, provisioned, exported or planned. The cutover tools say so on every record rather than leaving it out silently (see "Cutover tools").

## The shape

```
                      reads (answer in the request)
   client ──HTTP──▶ ┌──────────────┐ ───────────────────────────▶ ┌─────────┐
   (API key)        │   FastAPI    │                              │   NBI   │──SOAP──▶ Calix CMS
   client ◀──SSE─── │  (the API)   │                              │ gateway │
                    └──────┬───────┘                              └─────────┘
              202 Accepted │ ▲ job updates                             ▲
                  commands ▼ │                                         │
                    ┌────────┴─────┐   one command   ┌──────────────┐  │
                    │  message bus │ ──────────────▶ │   workers    │──┘
                    │ (Kafka, or   │   at a time     │ (stateless)  │
                    │  in-process) │   per key       └──────────────┘
                    └──────────────┘
   command-line tools (export, snapshot compare, SMx plan, cms_check) ──▶ NBI gateway or backend, read-only
```

## Who owns what

| Part | Owns | Never does |
|---|---|---|
| **FastAPI** (`main.py`, `app/routes/`) | API keys. CORS (no origin allowed unless `CORS_ALLOW_ORIGINS` lists it). Validating every request with Pydantic. Answering reads. Writing commands to the bus and answering `202`. Serving job status and the SSE stream. | Executing a queued change itself. |
| **NBI gateway** (`app/nbi/gateway.py`) | Every call to CMS: timeout, read retries, circuit breaker, concurrency cap, typed errors. | Business logic. It runs one CMS operation per call. |
| **CMS backend** (`app/nbi/cms_client_backend.py`) | Turning one operation into one SOAP exchange through `cms-nbi-client`, and replies into models. Recording reply fields it does not recognise. | Retries or policy. |
| **Router side** (`app/nbi/router_side.py`) | Everything about the router (RG) side of an ONT that is not known: how router-side services and ports are addressed, and how a port's bridge/router mode appears. | Guessing. Unconfigured pieces are refused or read as "unknown". |
| **Message bus** (`app/messaging/`) | Carrying commands to workers and job updates back. Kafka in production, in-process queues for development. | Deciding what a command means. |
| **Workers** (`app/worker/`) | Running commands and acknowledging them only when the outcome is settled. | Serving HTTP, keeping state between commands. |
| **Legacy layer** (`app/legacy/`) | Signup and cutover events, the export, the SMx plan, snapshots and compare. | Its own CMS calls or queue handling. It reuses the gateway and the workers. Writing to SMx or Sonar. |

Two rules keep the boundary sharp:

1. **One owner for CMS.** Only the gateway calls CMS (the read-only check uses the backend directly, behind a guard that refuses writes).
2. **Reads are direct, changes are queued.** A read answers within the request. A change is written to the bus and answered with `202` and a job to follow.

## What happens to a request

**Read**, e.g. `GET /api/v2/networks/{network}/onts/{ont_id}/ports`: route → gateway → CMS → response.

**Change**, e.g. `POST /api/v2/networks/{network}/onts/{ont_id}/ports/eth2/disable`:

1. The API key is checked, then FastAPI validates the body. Bad input gets `422` and nothing is queued.
2. The API writes a `Command` to the bus and waits for the bus to confirm it is stored. Only then does it answer `202` with `status_url` and `events_url`. If the bus refuses, the answer is `503` and nothing was queued.
3. A worker takes the command, runs it through the gateway, and publishes each step as a job update.
4. Every API process follows the updates stream, so any replica can serve `GET /api/v2/jobs/{id}` or the SSE stream at `/api/v2/jobs/{id}/events`.

## One ONT, piece by piece

**Activation and signup** (`ACTIVATE_ONT`, `GPON_SIGNUP`) run five steps, each repeat-safe:

1. Find the ONT by serial number (or among discovered ONTs).
2. Create it, unless it exists.
3. Configure each Ethernet port in `ports` (admin state, subscriber ID, security profile, description), sending only what differs from what CMS holds.
4. Bind each service not already bound, on an Ethernet port (`attach_to: ethernet`) or on the router side (`attach_to: router`).
5. Check, or wait for, the ONT coming online.

If CMS rejects a port setting or a service, the ONT created in that run is deleted again. An ONT this run did not create is never deleted. A router-side service on a profile whose `rg` count is 0 is refused before anything is created.

**Lifecycle changes** (`CHANGE_ONT`): enable or disable a port or a service, change a port (subscriber ID, security profile, description), change a service (bandwidth profile, tag action, VLAN), delete a service, delete an ONT. Each reads CMS first and sends only what is still different, so a repeat finds nothing to do. Deleting an ONT needs its serial number and refuses if the ONT ID now holds another one; it refuses an ONT that still has services unless `force=true`. Each change is one CMS write, so nothing needs undoing.

**Lookups**: `GET /ont-profiles` (with port counts and router count) and `GET /vlans`.

## Bridge or router: the 844G problem

ONTs such as the 844G have a built-in router. Each Ethernet port either sits behind it (router mode, the data service on the router side) or is a plain bridge to the customer's own router. The worst errors in manual cutovers were 844Gs running as bridges that were rebuilt as router/ONT combos: two routers in a row, double NAT.

So the mode is read **for each port, from CMS**, and never taken from the ONT model or profile. How CMS represents it is not known, so the reading rule lives in `app/nbi/router_side.py` and is off until `CMS_RG_PORT_MODE_ELEMENT` is set; until then every mode reads as `unknown`. The export carries each mode and flags router-capable ONTs that run every port as a bridge; the SMx plan carries each mode exactly and fails a subscriber whose mode is unknown rather than defaulting it.

What is and is not known about the router side, and the four settings that configure it, are listed at the top of `router_side.py`. Setting a port's mode through the NBI is not built; on a real CMS it is refused.

## Cutover tools (read-only)

- **Export** (`app/legacy/export.py`, `GET /networks/{network}/export`, `python -m app.legacy.export`): a network, a card or a PON port, one record per ONT laid out as SMx thinks of it (subscriber, ONT, data services), as JSON, a services CSV, a ports CSV and a snapshot. The existing serial number and registration ID are carried over because the ONT may be kept. Columns starting `fill_in_` are left blank: the new OLT and PON port, a replacement serial if the ONT is swapped, the customer's name and address. Voice is not exported, so every record has a `voice` status (the second CSV column): `not checked` when the ONT profile has voice (POTS) ports, `unknown` when that count could not be read, `none` only when the profile has none. A CSV read back without a status reads as `unknown`, and a status that contradicts `legacy_voice_ports` is refused.
- **SMx plan** (`app/legacy/smx.py`, `POST /smx/plans`, `GET .../subscribers/{id}/smx-plan`, `python -m app.legacy.smx`): from an export or a filled-in CSV, the Create/Update Subscriber, ONT and Data Service requests that would recreate each subscriber. A mapping file (`app/legacy/smx_map.yml`, `SMX_MAP_PATH`) translates ONT profiles, bandwidth profiles, tag actions, ports, port modes and admin states. An unmapped value fails that subscriber only, with a message naming it. Each subscriber's plan carries the export's `voice` status, the plan counts those not `none` under `voice_not_checked`, and `ready` covers data services only. Nothing is sent. Parameter names follow the IDI billing SMx R24.x docs; SMx's own API format is not known here.
- **Snapshots and compare** (`app/legacy/snapshot.py`): a snapshot is YAML built from the export. `--compare FILE` (or `POST /networks/{network}/snapshot-compare`) lists ONTs gone, ONTs new and every changed field, port and service between the snapshot and CMS now. There is no restore.
- **Read-only CMS check** (`python -m app.tools.cms_check`): logs in, lists ONTs, reads one ONT with services and ports, reads profiles and VLANs, logs out. It prints what it understood, every reply field it did not recognise, the raw values of fields whose meaning is inferred, and every service ONT slot it saw. A guard on the HTTP layer refuses anything but login, logout and reads. `--dump` saves every request and reply with the password blanked.

## The legacy GPON events

Events arrive on the `legacy-gpon-events` topic as plain JSON, keyed by network name. The API writes them (`POST /networks/{network}/signups` and `/cutovers`), and so can any other producer. The shapes are in `app/legacy/models.py`; every event needs a unique `event_id`, which becomes the job ID.

**`GPON_SIGNUP`** provisions the subscriber's ONT, ports and services as above. The ONT does not have to be installed.

**`XGS_CUTOVER`** reads the subscriber's ONT and services, translates them with `app/legacy/profile_map.yml` and publishes an `XgsMigrationRequest` to `XGS_MIGRATION_TOPIC`. CMS is only read. `XgsMigrationRequest` is this project's proposal; match it to your topic's schema first. Nothing here writes to SMx.

## Delivery rules

Kafka topics, with the default prefix:

| Topic | Key | Purpose |
|---|---|---|
| `legacy-gpon-events` | network name | signup and cutover events, as plain event JSON |
| `xgs-migration-requests` | subscriber ID | produced by cutovers for the XGS side |
| `gpon-cms.commands.provisioning` | network name | ONT activations and lifecycle changes |
| `gpon-cms.commands.dlq` | original key | commands that failed for good, with the reason in headers |
| `gpon-cms.job-updates` | job id | progress, read by every API process |

Auto-commit is off. For each command a worker does exactly one of these:

| Outcome | What the worker does | Offset |
|---|---|---|
| Succeeded | Publishes the result, then commits. | committed |
| Failed for good (CMS rejected it, bad data, unverified, too old) | Publishes the error, copies the command to the dead-letter topic, then commits. | committed |
| Failed for now (CMS down, timeout, breaker open) | Publishes a warning, seeks back to the same command, waits with backoff, tries again. | **not** committed |
| Result could not be published (Kafka trouble) | Seeks back and tries again. | **not** committed |
| Worker dies or loses the partition | Nothing. Kafka gives the command to another worker. | **not** committed |

Consequences: commands can run twice, so every handler is repeat-safe. Commands with the same key share a partition and run one at a time, so one network's changes never overlap and the partition count caps parallel CMS work. A CMS outage queues work instead of failing it; commands older than `COMMAND_MAX_AGE_SECONDS` are failed instead of run. A worker refuses to start if its slowest command could exceed `KAFKA_MAX_POLL_INTERVAL_SECONDS`.

## Errors

Every CMS failure reaches the client, or the failed job, in one shape:

```json
{"error": {"code": "cms_unavailable", "message": "...", "retryable": true, "retry_after_seconds": 30, "operation": "get_ont"}}
```

| Code | HTTP | Meaning |
|---|---|---|
| `unauthorized` | 401 | No API key, or not a valid one. |
| `auth_not_configured` | 503 | `API_KEYS` is not set and `MOCK_MODE` is off. |
| `cms_timeout` | 504 | CMS did not answer in time. For a write the message says the change may have been applied. |
| `cms_unreachable` | 502 | Could not connect to CMS. |
| `cms_unavailable` | 503 + `Retry-After` | The circuit breaker is open. The call was not attempted. |
| `cms_auth_failed` | 502 | CMS refused this API's own credentials. |
| `cms_rejected` | 422 | CMS answered and said no (unknown VLAN, profile without a router). |
| `cms_unverified` | 501 | The request needs CMS behaviour not verified and not configured (router side, setting a port's mode). Nothing was sent. |
| `conflict` | 409 | Already exists, exists differently, wrong serial for an ONT ID, ONT still has services. |
| `not_found` | 404 | No such network, ONT, port or service. |
| `cms_not_configured` | 503 | `MOCK_MODE` is off and `CMS_HOST` is not set. |
| `untranslatable` | job only | A cutover found a profile or VLAN with no entry in the profile map. |
| `queue_unavailable` | 503 | The bus did not accept the command. Nothing was queued. |

## Running it

| Mode | How | Use |
|---|---|---|
| Single process | `MOCK_MODE=true python main.py` | Development. In-process queue, embedded worker, simulated CMS. No API key needed. Jobs are lost on restart. |
| Demo topology | `deploy/demo/` (Docker Compose, or four local processes) | Kafka, API, worker, mock CMS NBI server and a stand-in XGS listener. |
| Production shape | API: `uvicorn main:app`. Workers: `WORKER_LANES=provisioning` and `WORKER_LANES=legacy python -m app.worker`. `CMS_HOST` and `API_KEYS` set. | See `deploy/k8s/gpon-cms.yaml`. |

## The CMS client: cms-nbi-client 0.1.0

[cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client), by somenetworking, builds the SOAP requests. It is licensed **GPL-3.0**, which matters if this service is ever distributed outside your organisation. Version 2.0.0 does not install (it depends on `python-keyring`, which is not on PyPI) and does not import. 0.1.0 is what this project uses.

What 0.1.0 does not do, and where that is handled:

| Gap in the library | Handled by |
|---|---|
| Synchronous (`requests`) | The backend runs each call in a worker thread. |
| No retries, timeouts policy or circuit breaker | The NBI gateway. |
| No connection pooling | Not handled. Acceptable at provisioning volumes. |
| Returns the raw HTTP response on failure instead of raising | The backend maps replies to typed errors. |
| Values are placed into XML unescaped | The backend escapes every value first. |
| `E7.Update.ont_geth()` (Ethernet ports) raises `KeyError` on every call | The backend sends that request itself, in the XML shape the library would produce. A test pins the bug. |
| `E7.Update.ont_ethsvc()` silently drops tag action and bandwidth profile changes (`pydash.get` called with its arguments swapped) | The backend sends that request itself. A test pins the bug. |
| **HTTP only.** Its HTTPS branch is not implemented. | **Not handled.** The CMS login and all traffic are unencrypted. |
| Writes `cms_nbi_config.json` into the working directory | The backend gives it a scratch directory. |

## What is verified, and what is not

Verified by the test suite (`requirements-test.txt`, run on Python 3.10 and 3.12) and by running it:

- Gateway, circuit breaker, error mapping, validation, API keys on every `/api/v2` route.
- The worker's acknowledgement rules, on the in-process bus and over the real Kafka wire protocol against librdkafka's mock cluster, including a worker killed with SIGKILL mid-job.
- Every CMS operation through `cms-nbi-client` 0.1.0 over SOAP against the mock NBI server: ONTs, Ethernet ports, router-side services and ports (with the mock's fake addressing), service update and delete, profiles, VLANs, session expiry, timeouts.
- Activation, signup, every lifecycle change run twice, rollback of a new ONT, export (JSON, CSV, card and PON scope), the SMx plan from a filled-in CSV, snapshot compare, and the read-only check (including that it sends no edit and blanks the password in dumps).
- The voice status on every export record and plan, including `unknown` for an ONT, ports or profile count that could not be read.

Inferred, not verified, because nobody on this project has a real CMS:

- **Every reply field the library does not parse**: `op-stat` values, `linked-pon`, `reg-id`, the `rpc-error` tags, the expired-session signal, the serial-number short form, the element names in an Ethernet port (OntEthGe) reply, and the meaning of OntProf values (port counts, `true`/`false`).
- **The router side and port modes**: not known at all; see `app/nbi/router_side.py`.
- **The voice port count**: read from the OntProf `pots` attribute. That it is a count of voice ports is inferred.
- **The IDI docs say "the Calix API expects 200 + the port number" for Ethernet ports** (port 1 as 201). The library uses plain port numbers. Which one your CMS wants is unknown; `cms_check` prints the port IDs of real services.
- The SMx parameter names (taken from the IDI docs, not from SMx) and every entry in `smx_map.yml` and `profile_map.yml`, which are demo values.
- Apache Kafka itself (only the mock cluster), Docker and Kubernetes (written, parsed, never run).

## Before this touches a real network

- **Run `python -m app.tools.cms_check` first.** See HANDOFF.md for the first three things to do.
- **Encryption.** CMS traffic is plain HTTP (library limit) and Kafka is plaintext with no credentials.
- **API keys** are shared secrets in an environment variable. Browsers' EventSource cannot send them, so a browser following a job's SSE stream needs a proxy that adds the key.
- **Job history** is rebuilt from the updates topic on start, so it is bounded by that topic's retention.
- **Dead letters** are parked with their error; there is no tool yet to list or replay them.
- **One breaker per process.** Workers do not share breaker state.
