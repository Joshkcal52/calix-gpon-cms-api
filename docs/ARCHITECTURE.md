# Architecture

## What this project is for

A provisioning API for legacy Calix GPON: the CMS-side counterpart of what the SMx API provides for XGS. CMS only speaks SOAP/XML. This service puts validated REST/JSON and Kafka events in front of it, so a billing system such as Sonar can sign a customer up and have the configuration land on the legacy ONT with no screen-scraping.

Around that core sit two things: a hand-over path that moves a legacy subscriber's configuration to the XGS side, and an emergency rollback playbook.

This document covers the `/api/v2` layer: how FastAPI, the CMS northbound interface (NBI), Kafka and Ansible fit together, what each one owns, and what has and has not been proven to work. The `/api/v1` routes (SSH and CLI scraping for rogue ONT detection) are older, separate, and not built on.

## The shape

```
                      reads (answer in the request)
   client ──HTTP──▶ ┌──────────────┐ ───────────────────────────▶ ┌─────────┐
                    │   FastAPI    │                              │   NBI   │──▶ Calix CMS
   client ◀──SSE─── │  (the API)   │                              │ gateway │
                    └──────┬───────┘                              └─────────┘
              202 Accepted │ ▲ job updates                             ▲
                  commands ▼ │                                         │
                    ┌────────┴─────┐   one command   ┌──────────────┐  │
                    │  message bus │ ──────────────▶ │   workers    │──┘  provisioning lane
                    │ (Kafka, or   │   at a time     │ (stateless)  │
                    │  in-process) │   per key       └──────┬───────┘
                    └──────────────┘                        │ automation lane
                                                            ▼
                                                     ansible-runner ──▶ playbooks ──HTTP──▶ FastAPI
                                                                           └──▶ other vendors' devices
```

## Who owns what

| Part | Owns | Never does |
|---|---|---|
| **FastAPI** (`main.py`, `app/routes/`) | Validating every request with Pydantic. Answering reads. Writing commands to the bus and answering `202`. Serving job status and the SSE stream. | Executing a queued change itself. |
| **NBI gateway** (`app/nbi/`) | Every call to CMS: timeout, read retries, circuit breaker, concurrency cap, typed errors. | Business logic. It runs one CMS operation per call. |
| **Message bus** (`app/messaging/`) | Carrying commands to workers and job updates back. Kafka in production, in-process queues for development. | Deciding what a command means. |
| **Workers** (`app/worker/`) | Running commands and acknowledging them only when the outcome is settled. | Serving HTTP, keeping state between commands. |
| **Ansible** (`ansible/`, `app/controller/automation_controller.py`) | Multi-device, multi-vendor and bulk work, as allowlisted playbooks. | Talking to CMS directly. Playbooks started by the API reach Calix only through this API. |
| **Legacy layer** (`app/legacy/`) | The signup and cutover events, and the translation from legacy to XGS terms. | Its own CMS calls or queue handling. It reuses the gateway and the workers. |
| **Emergency rollback** (`ansible/rollback/`) | Restoring an OLT from a Git snapshot. Run by an engineer, by hand. | Depending on anything above. It talks to CMS directly so it works when the rest is down. |

Two rules keep the boundary sharp:

1. **One owner for CMS.** Only the gateway calls CMS. Ansible goes through the API, so there is one circuit breaker, one set of limits and one place to change when the CMS client changes. The one deliberate exception is the emergency rollback playbook.
2. **Reads are direct, changes are queued.** A read (discovered ONTs, ONT status, bulk status check) answers within the request. A change (activation, playbook run) is written to the bus and answered with `202` and a job to follow.

## What happens to a request

**Read**, e.g. `GET /api/v2/networks/{network}/onts/{ont_id}`: route → gateway → CMS → response. A bulk status check runs its reads in parallel, capped by `NBI_MAX_CONCURRENCY`, and reports per-ONT failures without failing the request.

**Change**, e.g. `POST /api/v2/networks/{network}/activations`:

1. FastAPI validates the body. Bad input gets `422` and nothing is queued.
2. The API writes a `Command` to the bus and waits for the bus to confirm it is stored. Only then does it answer `202` with `status_url` and `events_url`. If the bus refuses, the answer is `503` and nothing was queued.
3. A worker takes the command, runs it through the gateway, and publishes each step as a job update.
4. Every API process follows the updates stream, so any replica can serve `GET /api/v2/jobs/{id}` or the SSE stream at `/api/v2/jobs/{id}/events`. A reconnecting browser sends `Last-Event-ID` and resumes.

**Playbook**, `POST /api/v2/automation/playbooks/{name}/runs`: the same path on the automation lane. The name must be on the allowlist in `automation_controller.py` and the variables must pass that playbook's Pydantic model. `api_url` is set by the server, never by the caller.

## The legacy GPON layer

Events arrive on the `legacy-gpon-events` topic as plain JSON, keyed by network name. The API writes them (`POST /api/v2/networks/{network}/signups` and `/cutovers`), and so can any other producer, such as a Sonar webhook bridge. The shapes are in `app/legacy/models.py`; every event needs a unique `event_id`, which becomes the job ID.

**Mode A, `GPON_SIGNUP`:** provision the subscriber's ONT and services on CMS. The ONT does not have to be installed: CMS holds the configuration against the serial number. It is the same repeat-safe workflow as an activation, including rollback if CMS rejects a service.

**Mode B, `XGS_CUTOVER`:** hand a subscriber over to XGS.

1. Find the subscriber's ONT on CMS by subscriber ID. No match, or more than one without an `ont_id` to choose, fails the job.
2. Read its services (VLANs, bandwidth profiles, tag actions) into `LegacySubscriberRecord`.
3. Translate to `XgsMigrationRequest` using the profile map (`app/legacy/profile_map.yml`, or `LEGACY_PROFILE_MAP_PATH`).
4. Publish it to `XGS_MIGRATION_TOPIC`, keyed by subscriber ID.

Three things to know about Mode B:

- **Nothing is guessed.** A bandwidth profile with no entry in the profile map, or a service whose VLAN cannot be determined, fails the cutover with a message naming each one, and nothing is published.
- **CMS is only read.** The legacy ONT stays in service. Retiring it after the XGS side is up is a separate step that does not exist yet.
- **`XgsMigrationRequest` is this project's proposal.** The schema of your existing XGS/SMx topic is not known here. Match the model to that contract before setting `XGS_MIGRATION_TOPIC` to the production topic. A redelivered cutover publishes the same request again with the same `migration_id`, so the consumer should ignore repeats.

On a real CMS, profiles and tag actions are numeric IDs, and a service's VLAN may live in its tag action instead of on the service. The profile map covers both: `bandwidth_profiles` maps IDs to XGS service profiles, and `tag_action_vlans` supplies the VLAN when the service has none of its own.

## Emergency rollback

`ansible/rollback/rollback.yml` restores one OLT, card, PON port or ONT to a snapshot from the `network-backups` Git repository, using Ansible's `uri` module against the CMS NBI. Each ONT is an Ansible host, so `--forks` sets how many are restored at once. `ansible/rollback/README.md` has the commands, the exact rules and the limits. Snapshots are produced by `python -m app.legacy.snapshot`.

## Delivery rules

Kafka topics, with the default prefix:

| Topic | Key | Purpose |
|---|---|---|
| `legacy-gpon-events` | network name | signup and cutover events, as plain event JSON |
| `xgs-migration-requests` | subscriber ID | produced by cutovers for the XGS side |
| `rogue.commands.provisioning` | network name | ONT commands |
| `rogue.commands.automation` | playbook name | playbook runs |
| `rogue.commands.dlq` | original key | commands that failed for good, with the reason in headers |
| `rogue.job-updates` | job id | progress, read by every API process |

Auto-commit is off. For each command a worker does exactly one of these:

| Outcome | What the worker does | Offset |
|---|---|---|
| Succeeded | Publishes the result, then commits. | committed |
| Failed for good (CMS rejected it, bad data, playbook failed, too old) | Publishes the error, copies the command to the dead-letter topic, then commits. | committed |
| Failed for now (CMS down, timeout, breaker open) | Publishes a warning, seeks back to the same command, waits with backoff, tries again. | **not** committed |
| Result could not be published (Kafka trouble) | Seeks back and tries again. | **not** committed |
| Worker dies or loses the partition | Nothing. Kafka gives the command to another worker. | **not** committed |

This differs from "commit only after success" in one deliberate way: a command that can never succeed is committed once its failure is recorded in the dead-letter topic. Without that, one bad request (a VLAN that does not exist) would block its partition for ever. Nothing is dropped either way.

What follows from these rules:

- **Commands can run twice**, for example when a worker dies after CMS applied a change but before the commit. Handlers are therefore written to be repeat-safe: activation first looks the ONT up by serial number, skips creation if it exists, and binds only the services that are missing.
- **Order and rate.** Commands with the same key share a partition and run one at a time, in order. So one network's changes never overlap, and the partition count is the cap on parallel CMS work. Kafka does not rate-limit CMS by itself; this is the mechanism.
- **A CMS outage queues work instead of failing it.** Each partition waits on its current command. Queued commands older than `COMMAND_MAX_AGE_SECONDS` (default one hour) are failed instead of run, so a request does not fire long after someone gave up on it.
- **Slow commands and Kafka's patience.** Kafka takes a partition away from a consumer that does not poll within `max.poll.interval.ms`. A worker refuses to start if its slowest command could exceed `KAFKA_MAX_POLL_INTERVAL_SECONDS`.
- **Rollback.** If CMS rejects a service during activation, the worker deletes the ONT it created in that run. It never deletes an ONT it did not create, and it does not roll back when CMS is merely unreachable (the retry carries on instead).

## Errors

Every CMS failure reaches the client, or the failed job, in one shape:

```json
{"error": {"code": "cms_unavailable", "message": "CMS has failed 5 times in a row, so calls are paused. Try again in about 30 seconds.", "retryable": true, "retry_after_seconds": 30, "operation": "get_ont"}}
```

| Code | HTTP | Meaning |
|---|---|---|
| `cms_timeout` | 504 | CMS did not answer in time. For a write the message says the change may have been applied. |
| `cms_unreachable` | 502 | Could not connect to CMS. |
| `cms_unavailable` | 503 + `Retry-After` | The circuit breaker is open. The call was not attempted. |
| `cms_auth_failed` | 502 | CMS refused this API's own credentials. |
| `cms_rejected` | 422 | CMS answered and said no (unknown VLAN, unknown profile). |
| `conflict` | 409 | Already exists, or exists with different settings. |
| `not_found` | 404 | No such network or ONT. |
| `cms_not_configured` | 503 | `MOCK_MODE` is off and `CMS_HOST` is not set. |
| `untranslatable` | job only | A cutover found a profile or VLAN with no entry in the profile map. |
| `queue_unavailable` | 503 | The bus did not accept the command. Nothing was queued. |

The breaker opens after `NBI_BREAKER_FAILURE_THRESHOLD` consecutive transport failures, refuses calls for `NBI_BREAKER_RECOVERY_SECONDS`, then lets one probe through. A CMS "no" does not count as a failure. Each process has its own breaker; `GET /api/v2/nbi/health` shows the API's.

## Running it

| Mode | How | Use |
|---|---|---|
| Single process | `MOCK_MODE=true python main.py` | Development. In-process queue, embedded worker, simulated CMS. Jobs are lost on restart. |
| Demo topology | `deploy/demo/` (Docker Compose, or four local processes) | Kafka, API, worker, mock CMS NBI server and a stand-in XGS listener. |
| Production shape | API: `uvicorn main:app`. Workers: `WORKER_LANES=legacy python -m app.worker`. `CMS_HOST` set. | See `deploy/k8s/rogue-api.yaml`. |

Extras: `requirements-kafka.txt` for Kafka, `requirements-cms.txt` for a real CMS (or the mock NBI server), `requirements-ansible.txt` for real playbook runs.

Which CMS the gateway uses: `MOCK_MODE=true` gives the in-process simulated CMS; otherwise `CMS_HOST` gives the real client; otherwise the v2 CMS routes answer `503 cms_not_configured`.

## The CMS client

The brief assumed `cms-nbi-client` provides async calls, Pydantic validation, connection pooling and circuit breakers. Checked against PyPI on 2026-10-06:

- **2.0.0 cannot be installed.** It depends on `python-keyring`, which is not a package on PyPI. Forced in, it fails on import: it uses the Pydantic v1 `BaseSettings` import, and its client imports `E7Operations` and `RESTOperations`, which do not exist in the package.
- **0.1.0 (2022) is what `pip install cms-nbi-client` gives you**, and it is what this project uses (`app/nbi/cms_client_backend.py`). It builds the SOAP requests, which is the part worth having.

What 0.1.0 does not do, and where that is handled instead:

| Gap in the library | Handled by |
|---|---|
| Synchronous (`requests`) | The backend runs each call in a worker thread. |
| No retries, timeouts policy or circuit breaker | The NBI gateway. |
| No connection pooling: a new HTTP connection per request | Not handled. Acceptable at provisioning volumes. |
| Returns the raw HTTP response on failure instead of raising | The backend maps replies to typed errors. |
| Values are placed into XML unescaped | The backend escapes every value first. |
| **HTTP only.** Its HTTPS branch is not implemented. | **Not handled.** The CMS login and all traffic are unencrypted. |
| Writes `cms_nbi_config.json` into the working directory | The backend gives it a scratch directory. |

It is licensed GPL-3.0, which matters if this service is ever distributed outside your organisation.

## What is verified, and what is not

Verified, by tests (`pytest`, see `requirements-test.txt`) and by running it:

- **Gateway, circuit breaker, error mapping, validation.**
- **The worker's acknowledgement rules**, on the in-process bus and over the real Kafka wire protocol against librdkafka's mock cluster: offsets committed only after success, a failing command left uncommitted and retried in order, a command in hand when a worker stops going to another worker, dead-lettering, and a restarted API rebuilding its jobs from the updates topic.
- **A worker killed with SIGKILL mid-job**, as separate OS processes: a second worker picked the job up after the session timeout and finished it without creating a second ONT.
- **`cms-nbi-client` end to end** against the mock NBI server: every operation, error mapping, session expiry, timeouts.
- **Signup and cutover** through API, Kafka, a separate worker process, SOAP to the mock CMS, and out to the XGS topic. Also an event written straight to the topic by an outside producer.
- **The rollback playbook**, run with `ansible-playbook` from a real Git repository against the mock NBI: dry run, scoped runs, full restore compared with the baseline, a repeat run sending nothing, one failing ONT not stopping the rest, and 414 ONTs at once.
- **Playbook runs through `ansible-runner`** with task events streamed live.

Not verified:

- **Anything against a real CMS.** The mock NBI server's request shapes, and every reply field the library reads, come from the `cms-nbi-client` source. The rest is inferred: the `op-stat` values used for online/offline, the `linked-pon` element, the `rpc-error` tags used to tell "not found" from "already exists", how CMS signals an expired session, and the serial-number short form. Expect to adjust these on first contact with a real CMS.
- **Apache Kafka itself.** The mock cluster speaks the real protocol but is not a broker. In particular, two workers sharing one consumer group could not be tested: the mock rejects a second group member from this client.
- **Docker and Kubernetes.** The Dockerfile, both Compose files and the manifests parse, but have not been built, run or applied.
- **The XGS topic contract** (see above) and the profile map entries, which are demo values.

## Before this touches a real network

- **Authentication.** The API has none. Anyone who can reach it can provision ONTs and start playbooks.
- **Encryption.** CMS traffic is plain HTTP (library limit) and Kafka is plaintext with no credentials.
- **Job history.** The job table is rebuilt from the updates topic on start, so it is bounded by that topic's retention. Use a database if jobs must be kept.
- **Dead letters.** Commands are parked with their error, but there is no tool yet to list or replay them.
- **One breaker per process.** Workers do not share breaker state, so each pod discovers an outage on its own.
- **Billing lifecycle.** Signup and hand-over exist. Suspend, resume, speed change and disconnect do not.
