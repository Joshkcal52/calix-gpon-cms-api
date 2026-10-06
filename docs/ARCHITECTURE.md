# Architecture: the v2 integration layer

This describes the `/api/v2` layer: how FastAPI, the CMS northbound interface (NBI), Ansible and Kafka fit together, what each one owns, and what has and has not been proven to work.

The `/api/v1` routes (SSH and CLI scraping) are unchanged and separate.

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
| **Ansible** (`ansible/`, `app/controller/automation_controller.py`) | Multi-device, multi-vendor and bulk work, as allowlisted playbooks. | Talking to CMS directly. Playbooks reach Calix only through this API. |

Two rules keep the boundary sharp:

1. **One owner for CMS.** Only the gateway calls CMS. Ansible goes through the API, so there is one circuit breaker, one set of limits and one place to change when the CMS client changes.
2. **Reads are direct, changes are queued.** A read (discovered ONTs, ONT status, bulk status check) answers within the request. A change (activation, playbook run) is written to the bus and answered with `202` and a job to follow.

## What happens to a request

**Read**, e.g. `GET /api/v2/networks/{network}/onts/{ont_id}`: route → gateway → CMS → response. A bulk status check runs its reads in parallel, capped by `NBI_MAX_CONCURRENCY`, and reports per-ONT failures without failing the request.

**Change**, e.g. `POST /api/v2/networks/{network}/activations`:

1. FastAPI validates the body. Bad input gets `422` and nothing is queued.
2. The API writes a `Command` to the bus and waits for the bus to confirm it is stored. Only then does it answer `202` with `status_url` and `events_url`. If the bus refuses, the answer is `503` and nothing was queued.
3. A worker takes the command, runs it through the gateway, and publishes each step as a job update.
4. Every API process follows the updates stream, so any replica can serve `GET /api/v2/jobs/{id}` or the SSE stream at `/api/v2/jobs/{id}/events`. A reconnecting browser sends `Last-Event-ID` and resumes.

**Playbook**, `POST /api/v2/automation/playbooks/{name}/runs`: the same path on the automation lane. The name must be on the allowlist in `automation_controller.py` and the variables must pass that playbook's Pydantic model. `api_url` is set by the server, never by the caller.

## Delivery rules

Kafka topics, with the default prefix:

| Topic | Key | Purpose |
|---|---|---|
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
| `cms_not_configured` | 503 | `MOCK_MODE` is off and there is no real CMS backend yet. |
| `queue_unavailable` | 503 | The bus did not accept the command. Nothing was queued. |

The breaker opens after `NBI_BREAKER_FAILURE_THRESHOLD` consecutive transport failures, refuses calls for `NBI_BREAKER_RECOVERY_SECONDS`, then lets one probe through. A CMS "no" does not count as a failure. Each process has its own breaker; `GET /api/v2/nbi/health` shows the API's.

## Running it

| Mode | How | Use |
|---|---|---|
| Single process | `MOCK_MODE=true python main.py` | Development. In-process queue and an embedded worker. Jobs are lost on restart. |
| Kafka, one process | `deploy/docker-compose.kafka.yml`, then `KAFKA_BOOTSTRAP_SERVERS=localhost:9092 WORKER_EMBEDDED=true MOCK_MODE=true python main.py` | Trying the Kafka path locally. |
| Kafka, separate workers | API: `uvicorn main:app`. Workers: `WORKER_LANES=provisioning python -m app.worker` | Production shape. See `deploy/k8s/rogue-api.yaml`. |

Kafka needs `pip install -r requirements-kafka.txt`. Real playbook runs need `pip install -r requirements-ansible.txt` and `ANSIBLE_MOCK=false`.

## What is verified, and what is not

Verified here, by tests (`pytest`, 66 tests) and by running the API:

- Gateway, circuit breaker, error mapping and validation.
- The worker's acknowledgement rules, using the in-process bus.
- Repeat-safe activation, including a create whose answer is lost after CMS applied it.
- `202` plus SSE progress end to end, rollback, and waiting out a simulated CMS outage.
- A real `ansible-runner` run of `baseline_audit.yml`, calling back into the API, with task events streamed live.

Not verified:

- **The Kafka bus against a real broker.** `tests/test_kafka_bus.py` checks how the code drives the Kafka client using a stub. No broker could be run where this was built. Run the compose file and an activation before relying on it.
- **The Kubernetes manifests and Dockerfile.** They parse, but have not been applied or built.
- **Anything against a real CMS.** There is no real CMS backend yet (next section).

## The CMS client: an open decision

The brief assumed `cms-nbi-client` provides async calls, Pydantic validation and circuit breakers. Checked against PyPI on 2026-10-06:

- **2.0.0 cannot be installed.** It depends on `python-keyring`, which is not a package on PyPI. Forced in, it fails on import: it uses the Pydantic v1 `BaseSettings` import, and its client imports `E7Operations` and `RESTOperations`, which do not exist in the package.
- **0.1.0 (2022) is what `pip install cms-nbi-client` gives you.** It is synchronous (`requests`), HTTP only (the HTTPS branch is not implemented), has no validation or breaker, writes a `cms_nbi_config.json` into the working directory, and is GPL-3.0.

So the resilience lives in this project's gateway, and the CMS client is a backend behind it (`NbiBackend` in `app/nbi/gateway.py`, six methods). Today the only backend is the mock CMS. The options for a real one:

1. **A small async client of our own** (recommended): `httpx` plus the SOAP envelopes from the Calix NBI guide. A few hundred lines for the six operations, no GPL dependency, HTTPS from the start.
2. **Wrap 0.1.0** in a thread per call. Quickest to write, but inherits HTTP-only transport and the GPL licence.
3. **Fix 2.0.0** upstream or in a fork.

Whichever is chosen, it cannot be trusted until it has been run against a CMS or against captured CMS responses.

## Before this touches a real network

- **Authentication.** The API has none. Anyone who can reach it can provision ONTs and start playbooks.
- **Kafka security.** The bus connects in plaintext with no credentials.
- **Job history.** The job table is rebuilt from the updates topic on start, so it is bounded by that topic's retention. Use a database if jobs must be kept.
- **Dead letters.** Commands are parked with their error, but there is no tool yet to list or replay them.
- **One breaker per process.** Workers do not share breaker state, so each pod discovers an outage on its own.
