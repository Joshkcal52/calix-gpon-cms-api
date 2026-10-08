# Anti-patterns: where this project stands

A check of the code against fifteen anti-patterns common to tools that drive a network on behalf of another system. For each one: where it applies here, what already guards against it, and what is still open. Line numbers are as of the commit that added this page.

Status words: **handled** means the code already guards against it; **partly** means some paths are guarded and others are not; **open** means nothing guards against it yet. Like the rest of the project, "handled" means tested against the mocks only (see [KNOWN_UNKNOWNS.md](KNOWN_UNKNOWNS.md)).

| # | Anti-pattern | Status |
|---|---|---|
| 1 | Assuming you are the only writer | partly |
| 2 | A second source of truth | partly |
| 3 | Treating an old request as current intent | partly |
| 4 | Identifying things by position | open |
| 5 | Cleanup more dangerous than the failure | partly |
| 6 | Treating "timed out" as "failed" | handled |
| 7 | Retries with no cap and no isolation | open |
| 8 | Reporting success on "command accepted" | open |
| 9 | Parsers that fail open | open |
| 10 | Mocks that become the specification | open |
| 11 | Partial coverage that looks complete | partly |
| 12 | No limit on blast radius | open |
| 13 | Deployment settings with hidden consequences | partly |
| 14 | One all-powerful service account | open |
| 15 | A bridge tool that becomes permanent | open |

## State and ownership

### 1. Assuming you are the only writer: partly

Technicians keep using the CMS GUI and the E7 CLI, so anything built on "nothing else touched this since my read" eventually fails.

**Already guarded:**
- The rollback no longer uses CMS's force, re-reads the ONT, and leaves it alone if it holds a service this run did not add (`app/controller/provisioning_controller.py:320-353`).
- A service bound "meanwhile by another run" is detected from the conflict and not claimed (`provisioning_controller.py:183-190`).

**Still assumes it is the only writer:**
- **ONT delete deletes services from a stale read.** `_delete_ont` reads the ONT at step 1, then deletes each service by its index from that read (`app/controller/lifecycle_controller.py:110, 136-137`). If a technician deletes service 2 and adds a new one that CMS also numbers 2 in between, the new one is deleted.
- **The free service index is picked from a read.** `add_ethernet_service` reads the port's services and takes the first unused index (`app/nbi/cms_backend.py:298-300`). A technician adding a service on that port in between causes a conflict. The conflict is handled, but only because CMS refuses the duplicate.
- **The free ONT ID on the E7 path is picked from a read** (`app/nbi/e7_backend.py:451-453`). The CMS path asks CMS for the next ID (`cms_backend.py:279`), which is the right pattern.
- **`lab_cycle` cleans up with force** (`app/tools/lab_cycle.py:247-258`). It only touches an ONT it created, but on a shared lab PON a technician may have added something to it since.

**To do:** re-read immediately before each destructive write and compare it with the identity the job expected (see #4). Prefer CMS-assigned IDs over IDs picked from a read.

### 2. A second source of truth: partly

Reading CMS before every write keeps the main path honest. State kept on the side is:

| State | Where | Drift risk |
|---|---|---|
| `BANDWIDTH_PROFILES` | `config.py:69`, copied by hand from CMS | A tier added or retired on CMS is refused here, or a retired one is still allowed. Nothing re-checks it against CMS. |
| `E7_SERVICE_NAMES` | `config.py:88` | Service names on the E7 change and this list does not. Also see #13. |
| `profile_map.yml`, `smx_map.yml` | `app/legacy/` | Every entry is a demo value (KNOWN_UNKNOWNS). They map CMS profiles to SMx with no version or date, so nothing shows when they go stale. |
| Ownership records | Job history on the updates topic (`app/worker/runner.py:234-256`) | Only as durable as the topic's retention (see #13). |
| Snapshots | `app/legacy/snapshot.py` | Read-only, used only for comparison. This is the right pattern. |

**To do:** stamp each mapping file with the CMS release and the date it was checked, and have `cms_check` report any CMS profile or bandwidth profile that the maps or `BANDWIDTH_PROFILES` do not cover, and any that they list but CMS no longer has.

### 3. Treating an old request as current intent: partly

**Already guarded:** every command has an age limit, `COMMAND_MAX_AGE_SECONDS`, one hour by default (`config.py:108`, enforced in `runner.py:374-383`). The age is measured from the earlier of the producer's timestamp and the bus's, so a wrong clock cannot make an old command look new (`runner.py:319-326`).

**Open:** within that hour nothing checks that the request still matches what is on the ONT.
- `UpdateService` and `UpdatePort` carry only the target values, with no expected current state (`app/models/lifecycle_model.py:20-36`). A delayed "set bandwidth profile 17" overwrites a technician's upgrade to 22 made five minutes later in the GUI.
- **A replayed signup reverts changes.** The activation changes an already bound service to match the request (`provisioning_controller.py:153-176`), so a signup replayed within the hour undoes any bandwidth, tag action or peak-rate change made since.
- `DeleteService.expect_vlan` is optional (`lifecycle_model.py:45-46`).

**To do:** add optional `expect` fields (the current bandwidth profile, VLAN and admin state) to service and port changes, refused on a mismatch the way `DeleteOnt` refuses a serial mismatch. Make `expect_vlan` required on the API's service delete. On a signup's re-run, leave alone a service whose current values match neither the request nor what the earlier attempt recorded.

### 4. Identifying things by position: open

The serial check on `DeleteOnt` is the right pattern (`lifecycle_controller.py:111-123`), but it is the only place it is applied.

- **Every port and service change addresses an ONT by its ID alone.** Only `DeleteOnt` carries a serial (`lifecycle_model.py:49-54`). An ONT ID reused after a delete and re-provision gets the old customer's queued change.
- **Services are addressed by index** (`lifecycle_model.py:35, 44`; `lifecycle_controller.py:71, 92`). Indexes are reused after deletes.
- **On the E7 path, the index comes from the name's place in `E7_SERVICE_NAMES`.** Names outside the list are numbered from 101 in the order the E7 prints them (`e7_backend.py:474-478`). Reordering the setting, or a change in the E7's print order, renumbers services.
- **The CMS reader defaults a missing service index and port to 1** (`cms_backend.py:224-225`). If either element name is wrong, every service reads as port 1, service 1, and a delete or change by index hits whatever is really there.

**To do:** require `serial_number` on every ONT change, not only deletes, and check it the way `_delete_ont` does. Require `expect_vlan` (or the full service identity: side, port, VLAN or tag action) on service changes and deletes. Make a missing index or port a parse error (see #9).

## Failure handling

### 5. Cleanup more dangerous than the failure: partly

**Already guarded:** the activation's rollback undoes only what this job recorded creating (`provisioning_controller.py:115-121, 306-353`), never uses force, and leaves an ONT with someone else's service in place.

**Open:**
- **A forced API delete that CMS refuses leaves the customer with no data.** With `force=true`, `_delete_ont` deletes each data service, then sends the ONT delete without force (`lifecycle_controller.py:132-140`). If CMS then refuses because of a voice service, the customer has lost data and kept the ONT. That outage is worse than the refused delete alone. KNOWN_UNKNOWNS notes it; nothing prevents it.
- **The E7 delete unlinks the ONT from its PON before deleting it** (`e7_backend.py:345-378`). If the delete is refused and the relink fails, the ONT is out of service.
- **`lab_cycle` cleans up with force** (`lab_cycle.py:258`). See #1.

**To do:** run every refusal check before the first destructive write. For example, try the ONT delete without force first, or read voice where possible, and only then remove services. When a step fails partway, report exactly which services were removed in the job's error, not only in its log events.

### 6. Treating "timed out" as "failed": handled

The gateway never retries a write (`app/nbi/gateway.py:164-195`). A write's timeout says "may or may not have been applied" (`gateway.py:197-201`), and the worker's retry reads before it writes (`runner.py:180-181`; `lifecycle_controller.py:5-8`). A `create_ont` that timed out and was in fact applied is found by serial on the retry, and is treated conservatively as not this job's (`provisioning_controller.py:115-121, 210-215`).

### 7. Retries with no cap and no isolation: open

- **No attempt cap.** A retryable error is retried until the command reaches its age limit (`runner.py:404-414`): with the default backoff, about 60 attempts over an hour.
- **The pause is worker-wide.** `_pause` sleeps the whole worker (`runner.py:413, 432-437`), so every lane and partition it holds waits, not only the command that failed.
- **Ordering is per network.** Every command is keyed by its network (`app/routes/lifecycle_routes.py:49`, `app/routes/provisioning_routes.py:85`, `app/legacy/events.py:32`), so one ONT whose command keeps timing out holds up every other change on that network for up to an hour. Networks that hash to the same partition wait too.
- **One breaker per process for every network** (`gateway.py:79, 333-341`). On the E7 path each network is a separate E7 (`config.py:73`), so one unreachable E7 opens the breaker and stops every other E7 served by that process. The fleet-wide gap (6 workers, 6 breakers) is in KNOWN_UNKNOWNS.

**To do:** keep one breaker per network (a dict keyed by network in the gateway). Cap attempts per command, then dead-letter it. Pause only the partition that failed, not the whole worker. Consider keying by `network/ont_id` so that only changes to the same ONT are serialised.

### 8. Reporting success on "command accepted": open

- **An activation succeeds even when the ONT never came online.** The result carries `online: false` and a warning event (`provisioning_controller.py:229-236`), but its status is SUCCEEDED, which is what billing will look at.
- **Lifecycle changes do not read back.** Port and service updates and deletes return `changed: true` as soon as the write call returns (`lifecycle_controller.py:61-62, 81-83, 102-103, 140-141`).
- **On the E7 path, "no output that looks like an error" means success** (`e7_backend.py:406-414`), with a guessed error pattern (`config.py:85`; also see #13).

**To do:** read back after each write and compare it with what was asked for, failing the job (non-retryable, with what was found) on a mismatch. Report the activation as a distinct outcome, such as `succeeded_offline`, or make `wait_for_online` failures fail the job, so billing cannot treat it as live.

## Device interface

### 9. Parsers that fail open: open

KNOWN_UNKNOWNS already says that "an empty read looks the same as nothing there". The places where that happens:

**E7 path (`app/nbi/e7_backend.py`)**
- `_service_rows` skips any row whose port or name it cannot read (`470-472`), so services it cannot parse vanish. An activation then binds them again; an ONT delete thinks the ONT is empty.
- `_ont_detail` returns `None`, meaning "no such ONT", when it cannot find a serial or ONT number (`434-435`). An activation then creates the ONT again.
- `_show` raises only when the output looks like an error *and* holds no known field (`402`). Unrecognised fields are only collected (`523-525`), never refused.
- `_free_ont_id` on an empty parse picks ID 1 (`451-453`).

**CMS path (`app/nbi/cms_backend.py`)**
- `_objects` silently yields nothing when the reply has no `type` element or uses another name (`131-144`).
- `_service` defaults a missing index and port to 1 (`224-225`).
- A missing `admin` reads as `enabled` (`195, 231`), and a missing serial as `"0"` (`192`).

**No release check before writes.** Nothing reads the CMS or E7 software release. `cms_version` (`config.py:128`) is a static label.

**To do:** add a strict mode, on by default for writes, in which a required field that is missing, a row that cannot be parsed, or an unrecognised field on a write path raises a non-retryable parse error before anything is written. Read the release at login, and refuse writes on releases that have no recorded fixtures (#10).

### 10. Mocks that become the specification: open

`mock_cms.py`, `mock_cms_server.py` and `mock_e7.py` were written from the same guesses as the parsers, so the suite proves that the two agree with each other. The only recorded data in `tests/fixtures/` is `cms_nbi_client_requests.json`: requests, not replies, and recorded against this project's own mock.

**To do:** `cms_check --dump` and `e7_capture` already save real replies. Commit them under `tests/fixtures/<cms|e7>/<release>/` and add parser tests that read them. Once a release has captures, have the mocks answer from them instead of from invented output.

### 11. Partial coverage that looks complete: partly

**Already guarded:** voice is flagged in the export and SMx plan, and an ONT delete is refused without `voice_checked` (`lifecycle_controller.py:129-130, 144-158`).

**Not yet flagged the same way:**
- **Services in slots other than 3 or the configured router slot** read as `Attachment.UNKNOWN` (`cms_backend.py:204-218`). This includes slot 9 (full bridge), which is where an 844G running as a bridge may keep its data service. They are listed, but the export and SMx plan do not mark the ONT as incomplete.
- **Anything CMS or the E7 returns that this code does not model** (video RF, multicast profiles, T1, port-level settings beyond those read) is noted in `unrecognised` (`cms_backend.py:438-452`, `e7_backend.py:523-525`), but nothing carries that note to the export, the snapshot or the SMx plan.
- **The router side's port mode reads `unknown`** until it is configured. The SMx plan already fails on it, which is the right pattern.

**To do:** generalise the voice flag. Any ONT with an unknown-slot service or an unrecognised field gets `coverage: incomplete` in the export, the snapshot and the plan, and a delete or migration of it needs the same kind of explicit acknowledgement as `voice_checked`.

## Operations

### 12. No limit on blast radius: open

- **No per-key rate limit and no cap on queued changes.** KNOWN_UNKNOWNS, "No limit on queued jobs".
- **No per-run or per-day cap on destructive operations.** A billing bug that sends a delete for every ONT gets them applied, one network at a time.
- **No dry run for lifecycle changes.** The SMx plan is a dry run, but `DELETE /onts/{id}` and the service changes have none.
- **The legacy events topic has no limit at all.** Its producers are trusted.
- **Reads can load CMS heavily.** A network-wide export (`app/routes/cutover_routes.py:48`) and a 200-ONT status check (`app/models/provisioning_model.py:334`) are both bounded only by the gateway's concurrency cap.

**To do:** add a token bucket per API key on write routes; a per-network, per-hour cap on ONT and service deletes, beyond which jobs park in the dead-letter topic for a person to release; and `dry_run=true` on every change route, running the reads and checks and returning what would be sent.

### 13. Deployment settings with hidden consequences: partly

**Already guarded:**
- `WORKER_LANES` no longer changes the consumer group. Each lane has its own group whatever lane list a worker has (`app/messaging/kafka_bus.py:184-198`).
- `KAFKA_MAX_POLL_INTERVAL_SECONDS` is checked against the slowest command at start (`config.py:166-174`).

**Settings that still look like tuning but change behaviour:**

| Setting | What it really controls |
|---|---|
| `KAFKA_TOPIC_PREFIX` (`config.py:104`) | Topic names *and* consumer group IDs (`kafka_bus.py:196-198`). Changing it starts new groups that read from the earliest offset (`kafka_bus.py:155, 208`), so up to `COMMAND_MAX_AGE_SECONDS` of old commands run again. |
| `COMMAND_MAX_AGE_SECONDS` (`config.py:108`) | How old a replayed command may be and still run (#3). Raising it to drain a backlog widens the replay window for everything. |
| Updates topic `retention.ms` (`deploy/k8s/gpon-cms.yaml:21`) | How long ownership records last (`runner.py:252-256`). Below the age limit, a retry can no longer prove that it created an ONT and will not roll it back. |
| `E7_SERVICE_NAMES` (`config.py:88`) | Service index numbering on the E7 path (#4). Reordering it renumbers every service, and queued changes then hit other services. |
| `E7_ERROR_PATTERN` (`config.py:85`) | Whether a write succeeded (#8). A pattern that is too narrow reports refusals as success. |
| `MOCK_MODE` | Also opens every `/api/v2` route when `API_KEYS` is unset (`app/auth.py:73-75`). |

**To do:** refuse to start when `KAFKA_TOPIC_PREFIX` differs from what a marker on the topic records, unless an override is set. Check at start that the updates topic's retention exceeds `COMMAND_MAX_AGE_SECONDS`. Document each row above next to its setting in `config.py`.

### 14. One all-powerful service account: open

- **API keys have names but no scopes** (`app/auth.py:40-54, 66-94`). Any key, including one leaked from the portal, can delete ONTs.
- **One CMS user and one E7 user do everything** (`config.py:57-58, 75-76`), so CMS's own audit shows every change as that user.
- **On the events topic, `requested_by` is whatever the producer wrote** (`app/legacy/events.py:34`, `app/legacy/models.py:43-44`).

**To do:** extend `API_KEYS` to `name:key:scopes`, with the scopes `read`, `change` and `delete`, and check them per route. Use separate CMS and E7 users for reads and writes where CMS allows it. Use Kafka ACLs (or a signed field) so that only known producers can write to the events topic.

### 15. A bridge tool that becomes permanent: open

Nothing in README, HANDOFF or CUTOVER.md says when this project is finished. It already carries two backends, a cutover exporter and an SMx planner. Without an end state it will be maintained against each new CMS and E7 release for as long as one GPON ONT is left.

**To do:** add a "Done when" section to HANDOFF.md, for example:
- every PON is exported, cut over and confirmed empty on CMS;
- a sunset date, after which new signups go to SMx only and this API refuses `signups`;
- the CMS and E7 releases it supports are pinned, and a new release is a decision rather than a patch;
- no new features except those that shorten the cutover.
