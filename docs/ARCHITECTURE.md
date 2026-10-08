# Architecture

## What this project is for

A provisioning API for legacy Calix GPON: the CMS-side counterpart of what the SMx API provides for XGS. CMS (R15.x) speaks SOAP/XML over HTTP on port 18080. This service puts validated REST/JSON and Kafka events in front of it, so a billing system such as Sonar can sign a customer up and have the configuration land on the legacy ONT without anyone typing it into the CMS screens.

CMS provisions object by object: the ONT, each Ethernet port and each service are separate requests, and the API cannot apply the port templates techs use in the CMS screens. This service sends each piece itself. The IDI billing vendor's certified CMS R15.x integration does the same: its "Link E7 GPON ONT" creates the ONT, configures the Ethernet ports one call per port, then sets the WAN protocol, and says "the API does not allow the template to be set".

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
| **CMS backend** (`app/nbi/cms_backend.py`) | Turning one operation into the CMS requests that carry it out, and replies into models. Recording reply fields it does not recognise. | Retries or policy. |
| **CMS client** (`app/nbi/client.py`) | The connection to CMS: HTTP or HTTPS with certificate checks, login and session, paging, escaping, typed errors. | Knowing what any object means. |
| **Router side** (`app/nbi/router_side.py`) | Everything about the router (RG) side of an ONT that is not known: how router-side services and ports are addressed, and how a port's bridge/router mode appears. | Guessing. Unconfigured pieces are refused or read as "unknown". |
| **Message bus** (`app/messaging/`) | Carrying commands to workers and job updates back. Kafka in production, in-process queues for development. | Deciding what a command means. |
| **Workers** (`app/worker/`) | Running commands and acknowledging them only when the outcome is settled. | Serving HTTP, keeping state between commands. |
| **Legacy layer** (`app/legacy/`) | Signup and cutover events, the export, the SMx plan, snapshots and compare. | Its own CMS calls or queue handling. It reuses the gateway and the workers. Writing to SMx or Sonar. |

Two rules keep the boundary sharp:

1. **One owner for CMS.** Only the gateway calls CMS (the read-only check uses the backend directly, behind a guard that refuses writes).
2. **Reads are direct, changes are queued.** A read answers within the request. A change is written to the bus and answered with `202` and a job to follow. A request is refused up front only for what can be checked without CMS (Pydantic). Checks that need CMS, such as whether this ONT has port eth5 or a router, run in the job, so a CMS outage queues work instead of refusing it; the job fails with a clear message.

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
4. Bind each service not already bound, on an Ethernet port (`attach_to: ethernet`) or on the router side (`attach_to: router`). A service is the same service if it has the same side, port and VLAN; a service without a VLAN, by its side, port and tag action. The two kinds never match each other, and two services in one request may not share an identity, so a repeat run neither adds a duplicate nor skips a wanted service.
5. Check, or wait for, the ONT coming online.

**The outer VLAN is optional** (2 to 4093), as in IDI's docs: "Do not include if the value is not required by Service Tag Action". Without one, no `<out-tag>` is sent and the service needs a tag action; requests for services with a VLAN are unchanged. Which tag actions need a VLAN is set up on each CMS and is not known here. The simulated CMS has its own rule, with fake names: tag actions `MOCK-NO-VLAN-A` and `MOCK-NO-VLAN-B` take none, every other one needs one (`app/nbi/mock_cms.py`). **Peak-rate overrides** (`us_pir_override`, `ds_pir_override`, e.g. `100m`, `1g` or `none`) go where `none` was always sent; left empty they are still `none`.

If CMS rejects a port setting or a service, the ONT created in that run is deleted again. An ONT this run did not create is never deleted. A router-side service on a profile whose `rg` count is 0 is refused before anything is created.

**Lifecycle changes** (`CHANGE_ONT`): enable or disable a port or a service, change a port (subscriber ID, security profile, description), change a service (bandwidth profile, tag action, VLAN, peak-rate overrides), delete a service, delete an ONT. Each reads CMS first and sends only what is still different, so a repeat finds nothing to do. Deleting an ONT needs its serial number and refuses if the ONT ID now holds another one; it refuses an ONT that still has services unless `force=true`. Each change is one CMS write, so nothing needs undoing.

**Lookups**: `GET /ont-profiles` (with port counts and router count) and `GET /vlans`.

## Bridge or router: the 844G problem

ONTs such as the 844G have a built-in router. Each Ethernet port either sits behind it (router mode, the data service on the router side) or is a plain bridge to the customer's own router. The worst errors in manual cutovers were 844Gs running as bridges that were rebuilt as router/ONT combos: two routers in a row, double NAT.

So the mode is read **for each port, from CMS**, and never taken from the ONT model or profile. IDI's docs give each Gigabit Ethernet port 1–8 and Fast Ethernet port 1–2 a "Data Port Default" of "Do Not Set / None / RG 1". That "None" is bridge mode and "RG 1" router mode is a strong reading, not confirmed, and the XML element is still unknown. How CMS represents it is not known, so the reading rule lives in `app/nbi/router_side.py` and is off until `CMS_RG_PORT_MODE_ELEMENT` is set; until then every mode reads as `unknown`. The export carries each mode and flags router-capable ONTs that run every port as a bridge; the SMx plan carries each mode exactly and fails a subscriber whose mode is unknown rather than defaulting it.

What is and is not known about the router side, and the four settings that configure it, are listed at the top of `router_side.py`. Setting a port's mode through the NBI is not built; on a real CMS it is refused.

## Cutover tools (read-only)

- **Export** (`app/legacy/export.py`, `GET /networks/{network}/export`, `python -m app.legacy.export`): a network, a card or a PON port, one record per ONT laid out as SMx thinks of it (subscriber, ONT, data services), as JSON, a services CSV, a ports CSV and a snapshot. The existing serial number and registration ID are carried over because the ONT may be kept. Columns starting `fill_in_` are left blank: the new OLT and PON port, a replacement serial if the ONT is swapped, the customer's name and address. Voice is not exported, so every record has a `voice` status (the second CSV column): `not checked` when the ONT profile has voice (POTS) ports, `unknown` when that count could not be read, `none` only when the profile has none. A CSV read back without a status reads as `unknown`, and a status that contradicts `legacy_voice_ports` is refused.
- **SMx plan** (`app/legacy/smx.py`, `POST /smx/plans`, `GET .../subscribers/{id}/smx-plan`, `python -m app.legacy.smx`): from an export or a filled-in CSV, the Create/Update Subscriber, ONT and Data Service requests that would recreate each subscriber. A mapping file (`app/legacy/smx_map.yml`, `SMX_MAP_PATH`) translates ONT profiles, bandwidth profiles, tag actions, ports, port modes and admin states. An unmapped value fails that subscriber only, with a message naming it; so does a tag action mapped to `outer` on a service with no outer VLAN. A peak-rate override other than `none` is carried only through `peak_rate_overrides` in the mapping file (SMx parameter names and values, set by hand); the shipped file has none, so such a service fails its subscriber rather than lose its rate or guess it. Each subscriber's plan carries the export's `voice` status, the plan counts those not `none` under `voice_not_checked`, and `ready` covers data services only. Nothing is sent. Parameter names follow the IDI billing SMx R24.x docs; SMx's own API format is not known here.
- **Snapshots and compare** (`app/legacy/snapshot.py`): a snapshot is YAML built from the export. `--compare FILE` (or `POST /networks/{network}/snapshot-compare`, which takes the snapshot as YAML exactly as `format=snapshot` gives it, as JSON, or a JSON export) lists ONTs gone, ONTs new and every changed field, port and service between the snapshot and CMS now. There is no restore.
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

Every error reaches the client, or the failed job, in one shape: CMS failures, API key and queue errors, request validation and plain HTTP errors such as an unknown route or job:

```json
{"error": {"code": "cms_unavailable", "message": "...", "retryable": true, "retry_after_seconds": 30, "operation": "get_ont"}}
```

| Code | HTTP | Meaning |
|---|---|---|
| `invalid_request` | 422 | The request is not valid (FastAPI's list of problems is under `details`), or a snapshot to compare is not one. Nothing was queued. |
| `unauthorized` | 401 | No API key, or not a valid one. |
| `auth_not_configured` | 503 | `API_KEYS` is not set and `MOCK_MODE` is off. |
| `cms_timeout` | 504 | CMS did not answer in time. For a write the message says the change may have been applied. |
| `cms_unreachable` | 502 | Could not connect to CMS. |
| `cms_unavailable` | 503 + `Retry-After` | The circuit breaker is open. The call was not attempted. |
| `cms_auth_failed` | 502 | CMS refused this API's own credentials. |
| `cms_rejected` | 422 | CMS answered and said no (unknown VLAN or bandwidth profile, profile without a router). |
| `cms_unverified` | 501 | The request needs CMS behaviour not verified and not configured (router side, setting a port's mode). Nothing was sent. |
| `conflict` | 409 | Already exists, exists differently, wrong serial for an ONT ID, ONT still has services. |
| `not_found` | 404 | No such network, ONT, port, service, subscriber, job or route. |
| `cms_not_configured` | 503 | `MOCK_MODE` is off and `CMS_HOST` is not set. |
| `untranslatable` | job only | A cutover found a profile or VLAN with no entry in the profile map. |
| `queue_unavailable` | 503 | The bus did not accept the command. Nothing was queued. |

## Running it

| Mode | How | Use |
|---|---|---|
| Single process | `MOCK_MODE=true python main.py` | Development. In-process queue, embedded worker, simulated CMS. No API key needed unless `API_KEYS` is set (then it is enforced, as it always is with `MOCK_MODE` off). Jobs are lost on restart. |
| Demo topology | `deploy/demo/` (Docker Compose, or four local processes) | Kafka, API, worker, mock CMS NBI server and a stand-in XGS listener. |
| Production shape | API: `uvicorn main:app`. Workers: `WORKER_LANES=provisioning` and `WORKER_LANES=legacy python -m app.worker`. `CMS_HOST` and `API_KEYS` set. | See `deploy/k8s/gpon-cms.yaml`. |

## The CMS client

`app/nbi/client.py` is the only code that talks to CMS over the network. It knows the transport, not the network: the SOAP envelopes, login and session, paging, escaping and errors. `app/nbi/cms_backend.py` decides which objects to read or change and what the replies mean.

| What | How |
|---|---|
| HTTP or HTTPS | `CMS_SCHEME` (default `http`) and `CMS_PORT`. Over HTTPS the server's certificate is checked against the system's CAs, or against `CMS_CA_FILE`. Only `CMS_VERIFY_TLS=false` turns the check off, and the API logs a warning when it is off or when the scheme is `http`. |
| Session | Logs in on first use. If CMS says the session is gone (`rpc-error` `access-denied`), logs in once more and resends; a second refusal is `cms_auth_failed`. Logs out on shutdown. |
| Errors | Timeouts, refused connections, TLS failures, HTTP errors and `rpc-error` replies become the typed errors in the table above. |
| Escaping | Every value is escaped into the XML, including the login. Requests go out as ASCII, with any other character as a character reference. |
| Paging | When a list ends with `<more/>`, asks again with `<after>` naming the last item, until there is no more. A CMS that repeats a page is refused, not looped over. |
| Async | One `httpx` connection pool per process; no worker threads. The gateway adds timeouts, read retries, the breaker and the concurrency cap. |

**Where the request shapes come from.** This project used to send its requests through [cms-nbi-client](https://github.com/somenetworking/CMS-NBI-Client) 0.1.0 by somenetworking, and that project is the reference the request shapes were learned from. Before it was removed, every request it sent for every operation used here was recorded against the mock NBI server (`tests/fixtures/cms_nbi_client_requests.json`). The client was written fresh from those recordings, and `tests/test_cms_client.py` checks that it sends the same requests: the same URL, headers and XML (compared in canonical form, so indentation aside), apart from the random message and session IDs. No code from the library is used.

Two requests always differed from the library's, on purpose: its Ethernet port update raised `KeyError` and its service update silently dropped tag-action and bandwidth-profile changes, so this project built those two itself, in the shape the library meant to send. Where the new client goes beyond the library, it is untested against a real CMS: paging (the library's `show-ont` paging could not have handled a page holding more than one ONT, and the mock never paged), HTTPS (the library had none), and escaping of the login (the library did not escape it).

## What is verified, and what is not

Verified by the test suite (`requirements-test.txt`, run on Python 3.10 and 3.12) and by running it:

- Gateway, circuit breaker, error mapping, validation, API keys on every `/api/v2` route.
- The worker's acknowledgement rules, on the in-process bus and over the real Kafka wire protocol against librdkafka's mock cluster, including a worker killed with SIGKILL mid-job.
- Every CMS operation through the CMS client over SOAP against the mock NBI server: ONTs, Ethernet ports, router-side services and ports (with the mock's fake addressing), service update and delete, profiles, VLANs, session expiry, timeouts. Each sends the same requests cms-nbi-client 0.1.0 sent (recorded before it was removed).
- HTTPS against the mock NBI server with a certificate from a private CA: refused by default, accepted with `CMS_CA_FILE`, accepted with `CMS_VERIFY_TLS=false`; the read-only check over HTTPS. Paging, against the mock's paging.
- Activation, signup, every lifecycle change run twice, rollback of a new ONT, export (JSON, CSV, card and PON scope), the SMx plan from a filled-in CSV, snapshot compare, and the read-only check (including that it sends no edit and blanks the password in dumps).
- The voice status on every export record and plan, including `unknown` for an ONT, ports or profile count that could not be read.
- Services without an outer VLAN, and peak-rate overrides. Through the API: provisioning run twice, lifecycle changes, export, CSV, snapshot and the SMx plan. Over SOAP: the create request is the recorded one without `<out-tag>`, or with the rates where `none` was; activation and changes run twice send nothing new; read-back.

Inferred, not verified, because nobody on this project has a real CMS:

- **Every reply field cms-nbi-client did not parse**: `op-stat` values, `linked-pon`, `reg-id`, the `rpc-error` tags, the expired-session signal, the serial-number short form, the element names in an Ethernet port (OntEthGe) reply, and the meaning of OntProf values (port counts, `true`/`false`).
- **The router side and port modes**: not known at all; see `app/nbi/router_side.py`.
- **The voice port count**: read from the OntProf `pots` attribute. That it is a count of voice ports is inferred.
- **"The Calix API expects 200 + the port number" when updating an Ethernet port** (IDI). This project, like cms-nbi-client, sends ONT Ethernet ports as 1–8. The evidence leans towards IDI's rule being E7 OS xDSL port numbering (the same page lists "DSL Port: Values 201-248"), not GPON ONT ports, but that is inferred. `cms_check` on a real CMS settles it: it reads OntEthGe 1, 2, … and prints each service's port.
- **Which tag actions need an outer VLAN** (set up on each CMS; the mock's rule is invented), that a service without one reads back as `<out-tag>none</out-tag>`, and the peak-rate format beyond IDI's example `100m` (this project accepts `none` or 1–9999 followed by `m` or `g`), and how both read back.
- **Each port's "None / RG 1" setting** (IDI) as bridge versus router mode: a strong reading, not confirmed; the XML is unknown. **WAN protocol (dhcp/static) and the static IP settings** are not built: IDI lists them, but their XML is shown nowhere.
- The SMx parameter names (taken from the IDI docs, not from SMx) and every entry in `smx_map.yml` and `profile_map.yml`, which are demo values.
- Apache Kafka beyond one demo broker (no replication, failover or rebalancing across real brokers), and Kubernetes (written and parsed, never applied). The image is built and the demo Compose file has run end to end.

## Before this touches a real network

- **Run `python -m app.tools.cms_check` first.** See HANDOFF.md for the first three things to do.
- **Encryption.** Set `CMS_SCHEME=https` (and the HTTPS port) if your CMS offers it: with the default `http` the CMS login crosses the network unencrypted. Kafka is plaintext with no credentials.
- **Paging and HTTPS have only met the mock.** If `cms_check` lists fewer ONTs than CMS holds, or HTTPS fails against a real CMS, start there.
- **API keys** are shared secrets in an environment variable. Browsers' EventSource cannot send them, so a browser following a job's SSE stream needs a proxy that adds the key.
- **Job history** is rebuilt from the updates topic on start, so it is bounded by that topic's retention.
- **Dead letters** are parked with their error; there is no tool yet to list or replay them.
- **One breaker per process.** Workers do not share breaker state.
