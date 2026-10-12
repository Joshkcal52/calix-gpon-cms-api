# Anti-patterns: where this project stands

A check of the code against fifteen anti-patterns common to tools that drive a network on behalf of another system. For each one: where it applied, what now guards against it, and what is still open. Line numbers are as of the commit that last changed this page.

Status words:
- **handled**: guarded against before this check.
- **fixed**: guarded against by the changes made with this page.
- **partly**: some paths are guarded, others are listed as still open.

Like the rest of the project, all of these have been tested against the mocks only (see [KNOWN_UNKNOWNS.md](KNOWN_UNKNOWNS.md)); the new guards are covered by `tests/test_anti_patterns.py`.

| # | Anti-pattern | Status |
|---|---|---|
| 1 | Assuming you are the only writer | fixed, with two read-then-pick IDs left |
| 2 | A second source of truth | partly |
| 3 | Treating an old request as current intent | partly |
| 4 | Identifying things by position | fixed (opt-in on the API) |
| 5 | Cleanup more dangerous than the failure | partly |
| 6 | Treating "timed out" as "failed" | handled |
| 7 | Retries with no cap and no isolation | partly |
| 8 | Reporting success on "command accepted" | fixed |
| 9 | Parsers that fail open | partly: no release check yet |
| 10 | Mocks that become the specification | partly: no recordings yet |
| 11 | Partial coverage that looks complete | fixed |
| 12 | No limit on blast radius | partly: limits are per process |
| 13 | Deployment settings with hidden consequences | partly |
| 14 | One all-powerful service account | partly |
| 15 | A bridge tool that becomes permanent | partly: dates to set |

## State and ownership

### 1. Assuming you are the only writer: fixed, with two read-then-pick IDs left

Technicians keep using the CMS GUI and the E7 CLI, so anything built on "nothing else touched this since my read" eventually fails.

**Guarded:**
- **Rollback.** The activation's rollback never uses force, re-reads the ONT, and leaves it alone if it holds a service this run did not add (`app/controller/provisioning_controller.py:335`).
- **Services bound meanwhile.** A service bound "meanwhile by another run" is detected and not claimed (`provisioning_controller.py:184`).
- **ONT delete (new).** Each service is checked again just before it is deleted. If its number now holds a different service, the delete stops with a conflict (`app/controller/lifecycle_controller.py:193-200`).
- **`lab_cycle` cleanup (new).** It no longer forces the delete of an ONT that holds a service the run did not add (`app/tools/lab_cycle.py:263-269`).
- **Read-back (new).** Every lifecycle write is read back (#8), so a change a technician overwrote at the same moment is reported, not assumed.

**Still read-then-pick:**
- The next free service number on the CMS path (`app/nbi/cms_backend.py:309`).
- The next free ONT ID on the E7 path (`app/nbi/e7_backend.py:455`).

Both depend on the device refusing a duplicate. The CMS path's ONT ID is already assigned by CMS.

### 2. A second source of truth: partly

Reading CMS before every write keeps the main path honest. State kept on the side:

| State | Where | Drift risk |
|---|---|---|
| `BANDWIDTH_PROFILES` | `config.py:77`, copied by hand from CMS | A tier added or retired on CMS is refused here, or a retired one is still allowed. |
| `E7_SERVICE_NAMES` | `config.py:101` | The E7's service names change and this list does not. It also numbers services; its comment now says to only append. |
| `profile_map.yml`, `smx_map.yml` | `app/legacy/` | Demo values with no record of the CMS release or the date they were checked. |
| Ownership records | job history on the updates topic (`app/worker/runner.py:75-97`) | Only as durable as the topic's retention (#13). |
| Snapshots | `app/legacy/snapshot.py` | Read-only and used only for comparison. This is the right pattern. |

**Still open:**
- Stamp each mapping file with the CMS release and the date it was checked.
- Have `cms_check` report bandwidth and ONT profiles that the maps or `BANDWIDTH_PROFILES` do not cover. The API cannot list bandwidth profiles from CMS, so this needs a person or a CMS export.

### 3. Treating an old request as current intent: partly

**Guarded:**
- **Age limit.** Every command has one, `COMMAND_MAX_AGE_SECONDS` (`config.py:128`), measured from the earlier of the producer's and the bus's timestamps (`app/worker/runner.py:160`).
- **Expected-state guards (new).** Port and service changes and deletes can carry `serial_number` and `expect_vlan`, and are refused when the target no longer matches (`lifecycle_controller.py:239-250`).
- **Dry runs (new).** `dry_run=true` lets a caller see what a delayed or replayed command would do first.

**Still open:**
- **The guards are optional**, so existing clients keep working. Billing and the portal should always send them; making them required is a breaking API change for the owner to decide.
- **A replayed signup within the hour still reverts later changes.** The activation changes an already bound service to match the request (`provisioning_controller.py:155`).

### 4. Identifying things by position: fixed (opt-in on the API)

- **ONT IDs.** Every lifecycle change now takes `serial_number` (`app/models/lifecycle_model.py:39, 51, 63`), checked the way the ONT delete always was (`lifecycle_controller.py:239`).
- **Service numbers.** Service changes and deletes take `expect_vlan` (`lifecycle_model.py:52, 62`). The ONT delete re-checks each service's identity just before deleting it (#1).
- **The CMS reader no longer defaults a missing service number or port to 1.** That is now a parse error (`cms_backend.py:235-236`).
- **On the E7 path the service number still comes from the name's place in `E7_SERVICE_NAMES`** (`e7_backend.py:483`). The setting's comment now says so (`config.py:97-101`), and `expect_vlan` catches a renumbering.

### 5. Cleanup more dangerous than the failure: partly

**Guarded:**
- **Rollback.** The activation's rollback undoes only what the job recorded creating, never with force (`provisioning_controller.py:321-368`).
- **`lab_cycle` (new).** Its forced cleanup refuses an ONT holding someone else's service (#1).
- **Partial deletes are reported (new).** A forced ONT delete that CMS refuses now says in the job's error exactly which data services were already removed and that the ONT was not (`lifecycle_controller.py:205-211`). The customer impact is visible instead of buried in log events.

**Still open:**
- **The forced delete still removes data services before CMS has the chance to refuse the ONT** because of a voice service. Avoiding that needs either a way to read voice or confirmation that CMS refuses a non-forced delete up front (the `lab_cycle` steps 6 and 7 settle it).
- **The E7 delete unlinks the ONT from its PON before deleting it** (`e7_backend.py:345-375`). If the delete is refused and relinking fails, the ONT is out of service.

### 6. Treating "timed out" as "failed": handled

The gateway never retries a write (`app/nbi/gateway.py:181-214`). A write's timeout says "may or may not have been applied" (`gateway.py:216`), and the worker's retry reads before it writes (`runner.py:21-22`). A `create_ont` that timed out but was applied is found by serial on the retry, and is treated conservatively as not this job's (`provisioning_controller.py:211`).

### 7. Retries with no cap and no isolation: partly

**Fixed:**
- **One breaker per E7.** On the E7 path each network is its own device, so the gateway keeps a breaker per network. One unreachable E7 no longer pauses the others (`gateway.py:166-175, 360`). The CMS path keeps one breaker, because every network sits behind the same CMS. `/api/v2/nbi/health` lists each network's breaker.
- **An attempt cap.** A command that keeps failing for a passing reason is failed for good after `COMMAND_MAX_ATTEMPTS` (20) attempts instead of retrying for up to an hour (`runner.py:249-254`; `config.py:131`). An open breaker does not count, so a CMS outage is still waited out.

**Still open:**
- **The backoff pause is worker-wide** (`runner.py:263, 282`). Every partition a worker holds waits while one command backs off.
- **Commands are keyed per network** (`app/routes/lifecycle_routes.py:54`, `app/routes/provisioning_routes.py:85`, `app/legacy/events.py:32`). One sick ONT still holds up its network, for at most 20 attempts. Keying by `network/ont_id` would narrow that, at the cost of ordering between ONTs.

### 8. Reporting success on "command accepted": fixed

- **Every lifecycle write is read back.** That covers port changes, service changes, service deletes and ONT deletes. A write CMS accepted but does not show fails the job with `not_applied` (`app/nbi/errors.py`, `NbiVerifyError`; `lifecycle_controller.py:73, 120, 152, 218`). Results carry `verified: true`.
- **An activation checks its final read.** If a requested service is missing or differs, it fails with `not_applied` (`provisioning_controller.py:232-240`).
- **Activation results now carry `outcome`.** It is one of `online`, `provisioned_offline` or `provisioned_not_waited` (`provisioning_controller.py:248`), so billing cannot mistake a succeeded job for a live customer.
- **E7 writes are read back too.** On the E7 path, output that does not look like an error is still taken as "accepted" (`e7_backend.py:406`), but the read-back now catches a refusal the error pattern missed.

## Device interface

### 9. Parsers that fail open: partly

**Fixed:** a reply missing a field that a decision rests on is now a non-retryable `cms_unreadable` error (`NbiParseError`), never "nothing there".
- **CMS:** an ONT without its number or serial, and a service without its number or port (`cms_backend.py:189-201, 235-236, 278`).
- **E7, ONT detail:** detail with no serial number. It no longer reads as "no such ONT", which would have led an activation to create the ONT again (`e7_backend.py:437`).
- **E7, ONT list:** a row without an ONT number (`e7_backend.py:452`).
- **E7, services:** a service row without a port or a name. It is no longer skipped, which would have made the service vanish (`e7_backend.py:477`).

**Still open:**
- **Nothing reads the CMS or E7 software release before a write.** `cms_version` (`config.py:151`) is a static label. How each reports its release has to come from a real capture.
- **Optional fields still default.** `admin` reads as `enabled` when missing (`cms_backend.py:205, 242`).
- **Unrecognised fields are only collected** (`cms_backend.py:449`, `e7_backend.py:529`); they do not stop a write.

### 10. Mocks that become the specification: partly

`mock_cms.py`, `mock_cms_server.py` and `mock_e7.py` share the parsers' guesses.

**New:** `tests/fixtures/e7/README.md` sets out where captures from `python -m app.tools.e7_capture` go, one directory per release. `tests/test_recorded_output.py` runs every recording through the backend's parsers and fails on any it cannot read. It is skipped until the first capture is committed.

**Still open:**
- **No recordings exist yet.** They need access to a real E7.
- **CMS replies saved by `cms_check --dump` have no equivalent test yet.**
- **The mocks still answer from invented output.**

### 11. Partial coverage that looks complete: fixed

- **Voice was already flagged.** The export and SMx plan mark it, and an ONT delete is refused without `voice_checked` (`lifecycle_controller.py:222-236`).
- **New: every export record carries `coverage`.** It is `complete` only when every service is in a slot this code knows and the ports were read; otherwise `incomplete`, or `unknown` when read from a sheet without the column. Unreadable ONTs are `incomplete` (`app/legacy/export.py:144-150, 267-272`).
- **New: the export counts these.** `coverage_incomplete` (`export.py:183`) totals them, and the CSV has a `coverage` column.
- **New: the SMx plan warns.** It warns on every subscriber whose coverage is not `complete` (`app/legacy/smx.py:273`).

### 12. No limit on blast radius: partly

**Fixed:**
- **Per-key limits.** Each API key may queue `WRITE_REQUESTS_PER_MINUTE` (120) changes and `DELETES_PER_HOUR` (100) deletes per API process. Beyond that the API answers 429 and nothing is queued (`app/auth.py:123-157`; `config.py:39-40`).
- **Dry runs.** Every lifecycle route takes `dry_run=true`, which runs the reads and checks and reports what would be sent (`lifecycle_controller.py:65, 109, 144, 180`).

**Still open:**
- **The limits are per API process**, so 2 pods give double.
- **Events written straight to the events topic are not limited.**
- **A network-wide export can still load CMS heavily**, bounded only by the gateway's concurrency cap.

### 13. Deployment settings with hidden consequences: partly

**Guarded:**
- **`WORKER_LANES`** no longer changes the consumer group (`app/messaging/kafka_bus.py:184-198`).
- **`KAFKA_MAX_POLL_INTERVAL_SECONDS`** is checked against the slowest command at start.

**New:** each setting below now says what it really controls, next to its definition:

| Setting | What it really controls |
|---|---|
| `KAFKA_TOPIC_PREFIX` (`config.py:117-120`) | Topic names and consumer group IDs. A new prefix replays up to `COMMAND_MAX_AGE_SECONDS` of commands. |
| `COMMAND_MAX_AGE_SECONDS` (`config.py:124-128`) | The replay window, and how long ownership records must last. |
| Updates topic `retention.ms` (`deploy/k8s/gpon-cms.yaml`) | Whether a retry can still roll back what its earlier attempt created. |
| `E7_SERVICE_NAMES` (`config.py:97-101`) | Service numbering on the E7 path. |
| `E7_ERROR_PATTERN` (`config.py:93-95`) | Whether an E7 write counts as accepted (now backed by the read-back). |

**Still open:**
- **`MOCK_MODE` still opens `/api/v2` when `API_KEYS` is unset** (`auth.py:169`). It only does so in mock mode, and logs a warning.
- **No startup check enforces retention or a prefix change.** Both need the Kafka admin API.

### 14. One all-powerful service account: partly

**Fixed:** API keys take scopes: `name:key:read+change` (`auth.py:67-108`).
- **What each scope covers:**
  - `read`: GETs and the POSTs that only read (`auth.py:51`).
  - `change`: every other POST and PATCH.
  - `delete`: every DELETE.
- **A portal key without `delete` can no longer delete ONTs** (`auth.py:189`).
- **Unscoped keys still work.** They keep full rights for compatibility, with a warning at start.

**Still open:**
- **One CMS user and one E7 user do every read and write** (`config.py:65, 83`).
- **Producers on the events topic are trusted for `requested_by`** (`app/legacy/events.py:34`). Kafka ACLs on that topic are the fix.

### 15. A bridge tool that becomes permanent: partly

**New:** HANDOFF.md has a "When this project is done" section. It covers:
- every PON cut over and empty on CMS;
- a sunset date for signups;
- pinned CMS and E7 releases;
- no new features unless they shorten the cutover.

**Still open:** the sunset date and the pinned releases are blanks for the owner to fill in.
