# Known unknowns and open risks

Everything about this project that is untested, guessed, or not built, in one place. Read it before trusting the code with live subscribers. [HANDOFF.md](../HANDOFF.md) has the short version; [ARCHITECTURE.md](ARCHITECTURE.md) has the design. [ANTI_PATTERNS.md](ANTI_PATTERNS.md) maps fifteen common failure patterns of network automation tools onto this code.

Status words used below: **verified** means tested against the mock CMS or mock E7 (never a real one); **inferred** means guessed from public sources; **not built** means the code refuses or skips it.

## Who does the CMS work, and when

**Not done before handoff.** Nobody on this project has had access to a Calix CMS, so nothing below has been checked against one. **The receiving team owns it.** It needs:

- CMS NBI credentials (a read-only user is enough for the first step) and network access to the NBI port (18080 for HTTP, or the HTTPS port).
- A lab E7 or a test PON, with at least one 844G running as a router, one running as a bridge, and one spare ONT with no voice.
- About a day: half for the read-only check and fixing what it shows, half for the first write cycle.

The steps, in order, are in HANDOFF.md under "First three things to do with a real CMS": `python -m app.tools.cms_check` (read-only), a read-only export of one PON checked against what the techs know, then `python -m app.tools.lab_cycle` on the spare ONT.

## The CMS side has never met a real CMS

- **Request shapes** come from recordings of cms-nbi-client 0.1.0 against this project's mock server (`tests/fixtures/cms_nbi_client_requests.json`). The library's author presumably ran them against a CMS; this project never has. HTTPS, paging and escaping of the login go beyond what the library did and have only met the mock.
- **Reply fields the library never parsed are guessed**: `op-stat` values (online/offline), `linked-pon` (PON port), `reg-id`, the `rpc-error` tags used to tell "not found" from "already exists", the expired-session signal, the serial number's short form, every element name in an Ethernet port (OntEthGe) reply, the meaning of the ONT profile (OntProf) values as port counts, that a service without a VLAN reads back as `<out-tag>none</out-tag>`, and how peak rates read back. `cms_check` prints every reply field it does not recognise and the raw values of each guessed one.
- **An empty read looks the same as "nothing there".** If a guessed element name is wrong, the code does not fail: it reads an empty value. Then a port reads as having no security profile, an ONT reads as offline, or an ONT reads as having no services. The consequences: a repeat activation may add a service again (CMS should refuse the duplicate), the export and SMx plan may miss services, a snapshot compare may report services as gone, and a delete of an ONT may skip deleting its services (the ONT delete is sent without CMS's force, so CMS should then refuse it). `cms_check` is what tells you: compare its output with the CMS screens for the same ONT before any write.
- **Port numbers.** IDI's docs say "the Calix API expects 200 + the port number" when updating an Ethernet port. This project sends ONT Ethernet ports as 1 to 8, like cms-nbi-client. The evidence leans towards IDI meaning E7 OS xDSL ports (201 to 248), but that is inferred; `cms_check` settles it.
- **Which tag actions need an outer VLAN** is set up on each CMS. The mock's rule (two fake tag actions take none) is invented.
- **Peak-rate format**: IDI shows `100m`. This project also accepts `g` and `none`; anything else is refused before it is queued.

## The router side of the 844G

Not known at all, and off until configured (`app/nbi/router_side.py`, the four `CMS_RG_*` settings): router-side writes are refused, and each port's bridge/router mode reads as `unknown`, so the SMx plan fails rather than default it. IDI's docs give each Ethernet port a "None / RG 1" setting; reading that as bridge versus router is likely but unconfirmed, and its XML element is unknown. Setting a port's mode, and the WAN protocol and static IP of a router-side service, are **not built**: their XML is shown nowhere. This matters because 844Gs that ran as bridges and were rebuilt as router/ONT combos gave customers double NAT.

## Voice is not handled

Only Ethernet data services are read, provisioned, exported or planned. The voice status of each ONT comes from its profile's `pots` attribute, whose meaning as a voice port count is inferred. The export and SMx plan mark every ONT with voice ports `not checked` (or `unknown` if the count could not be read); those need checking by hand. The cutover's migration request has no voice flag yet.

## Deleting an ONT

The risk: CMS's "force" delete removes an ONT with everything on it, and this API cannot see voice services, so a phone line could go with it unseen. What the code does now:

- An ONT whose profile has voice ports, or whose count cannot be read, is refused unless `voice_checked=true` says someone has checked it in CMS.
- An ONT that still has data services is refused unless `force=true`. With it, the data services are deleted one by one, then the ONT is deleted **without** CMS's force, so CMS should refuse it while anything is left (a voice service, or a service added since the read).
- Nothing uses CMS's force on an ONT delete: a rollback also deletes the services its activation added, then the ONT without force.

**Inferred, not verified:** that CMS refuses a delete without force while services are left, and accepts one when nothing is left. If it insists on force even for an empty ONT, the API's delete and an activation's rollback fail (safely, leaving the ONT and saying so) and `app/controller/lifecycle_controller.py` and `_roll_back` in `app/controller/provisioning_controller.py` must change. The other direction matters more: if CMS removes the services with the ONT even without force, neither the voice protection nor the rollback's protection of another run's services works, and nothing fails to say so. `lab_cycle` checks both directions on a spare ONT (steps 6 and 7). Also see "an empty read" above.

## The E7 command-line path

For a CMS without the NBI licence, the API provisions through each E7's CLI over SSH (`app/nbi/e7_backend.py`, [E7_CLI.md](E7_CLI.md)). It has only met the mock E7 (`app/nbi/mock_e7.py`), whose output is invented.

- **Command syntax is from Calix's public CLI reference; the output is not documented.** The parsers, the prompt, the refusal wording and which words mean "online" are guesses the mock shares. `python -m app.tools.e7_capture` (show commands only) settles them; fix the parsers against its captures before any write.
- **Port bridge/router mode reads `unknown`** and cannot be set, so the SMx plan refuses every subscriber on this path until a capture from an 844G shows where the E7 prints it.
- **Deleting an ONT** removes its data services, unlinks it from its PON (Calix requires that before a delete), then sends `delete ont` without `forced`. That the E7 then refuses while a voice service is left, and that `set ont <id> pon-port <location>` links it again in the form `show ont <id> detail` prints, are both inferred. If linking it again fails, the error says the ONT is unlinked and out of service. A refused delete has already removed the data services, as on the CMS path.
- **Names, not IDs.** Bandwidth profiles, tag actions, ONT profiles and services are sent by name; `BANDWIDTH_PROFILES` and the mapping files need the E7's names, not CMS's IDs. Service names default to `Data1`..`Data8` (`E7_SERVICE_NAMES`).
- **One SSH session per E7, one job at a time.** In simulation the session is the limit for one E7 (see [SIM_RESULTS.md](SIM_RESULTS.md)); how long a real E7 takes per command is unknown.

## Bandwidth profiles

The bug: an unknown bandwidth profile was accepted, and on a real CMS it would only have been refused mid-job, or worse, a valid but wrong ID would have applied another speed tier. Now:

- `BANDWIDTH_PROFILES` (copied by hand from CMS, or the E7's names on the E7 path; this API cannot list them) is checked before an activation, signup or service change is queued, and again by the worker for signups from the events topic. **Unset, nothing is checked** before CMS. Set it before going live.
- The simulated CMS refuses an unknown profile too, as it does an unknown ONT profile or VLAN.
- A valid ID that is the wrong tier for the customer cannot be caught here; that is billing's data.

**Open question: numeric IDs or names?** CMS documentation for an earlier release (not R15.x) gives `bw-prof` as 1-300 and `tag-action` as 1-255, which suggests numeric IDs. The CMS backend sends whatever value it is given and compares what it reads back as text, so it works either way as long as `BANDWIDTH_PROFILES`, the services this API sends and what CMS reads back all use the same form. Nothing refuses a name on the CMS path: an assumption from older docs could reject values a real CMS accepts, and a value CMS does not know is refused by CMS and rolled back anyway. The API logs a warning at start when `BANDWIDTH_PROFILES` holds non-numeric values with `NBI_BACKEND=cms`. `python -m app.tools.cms_check --ont-id <an ONT with services>` prints the values CMS wrote, which answers the question on first contact. The simulated CMS and its NBI server use names (`RES-1G`, `VOICE`); they change once a real CMS has shown the form, not before.

## The SMx side

SMx parameter names come from the IDI billing SMx R24.x docs, not from SMx. Every entry in `smx_map.yml` and `profile_map.yml` is a demo value. Nothing here writes to SMx or Sonar. `XgsMigrationRequest` is this project's proposal for the migration topic, not a known schema. Whether customers keep their ONT through a cutover is not settled: the export carries the existing serial, and a replacement serial can be filled in. What the cutover tools hand over is in [CUTOVER.md](CUTOVER.md).

## Deployment gaps

- **The circuit breaker is per process.** `deploy/k8s/gpon-cms.yaml` runs 2 API pods and 6 workers (3 per lane), each with its own breaker (5 consecutive failures, then 30 seconds' pause). In a CMS outage that is up to 40 failed calls before every process has stopped, and each process probes again every 30 seconds. The concurrency cap (`NBI_MAX_CONCURRENCY`, 8) is per process too: up to 64 CMS calls at once across the fleet. A shared breaker and cap would need shared state (Redis or similar); until then size replicas with this in mind. On the E7 path each process keeps one breaker per E7, so one unreachable E7 does not pause the others.
- **No metrics or request IDs.** There are logs, `/health` and `/api/v2/nbi/health` (the breaker's state), nothing else. An ops team will want job success and failure rates, queue depth, CMS response times and breaker state exported (Prometheus or OpenTelemetry) and alerted on, plus a request ID carried from the HTTP request through the job to each CMS call. Job IDs already tie a job's log lines together.
- **Placeholders.** The Kubernetes file still says `image: REPLACE_WITH_YOUR_IMAGE` and has never been applied to a cluster. It already reads the CMS login and the API keys from Kubernetes Secrets (`cms-nbi-credentials`, `gpon-cms-api-keys`), not from a `.env` file; how those Secrets are filled from your secret store (External Secrets, Vault, sealed secrets) is yours to wire up. `.env` and `env_example.txt` are for development only.
- **Limits on queued jobs are per API process.** Each API key may queue `WRITE_REQUESTS_PER_MINUTE` changes (120) and `DELETES_PER_HOUR` deletes (100) per API process; beyond that it gets 429 and nothing is queued. With 2 API pods the fleet-wide limit is double that, and events written straight to the events topic are not limited at all. Commands for one network run one at a time (they share a Kafka partition). Commands older than `COMMAND_MAX_AGE_SECONDS` (1 hour) are failed rather than run, and one that keeps timing out is failed after `COMMAND_MAX_ATTEMPTS` (20). Every lifecycle change takes `dry_run=true`.
- **Audit trail retention.** Every job records the API key that asked for it (`requested_by`), and each accepted, succeeded and failed change writes one JSON line to the `gpon_cms.audit` logger (`app/audit.py`), also to `AUDIT_LOG_PATH` if set. Job history in the API itself only lasts as long as the updates topic keeps messages, so the lasting record is wherever those log lines are shipped. Events written straight to the topic carry whatever their producer put in `requested_by`; producers are trusted.
- **Kafka** has only run as one demo broker: no replication, failover or rebalancing across real brokers. Kafka traffic is plaintext with no credentials.
- **Dead letters** are parked with their error; there is no tool to list or replay them.

## License

This project is licensed under the **GNU General Public License v3.0** ([LICENSE](../LICENSE)). Anyone may use, change and share it; anyone who distributes a changed version must publish it under the same license.

Why GPL-3.0:

- **cms-nbi-client is licensed GPL-3.0** (its LICENSE file, and "GPL v3.0" on PyPI). This project depended on it until it replaced it with its own client (`app/nbi/client.py`). No code from the library is in the repository: the client was written from the requests the library sent to the mock server, which are kept as test data in `tests/fixtures/cms_nbi_client_requests.json`, and the docs credit the library throughout. Whether request formats learned that way carry any GPL obligation was never settled; publishing under the same license means the question does not need to be.
- **Third-party packages are not part of the repository.** An early commit tracked a whole `venv/` (about 60 packages: paramiko under the LGPL, netmiko, cryptography and others). It was removed from the entire history before the repository was published, so no copies of those packages are distributed with it. They are installed from PyPI under their own licenses (`requirements*.txt`).

## How it was written

Most of this code was written by Claude (Anthropic's AI model) in Claude Code sessions: every non-merge commit except the first and one that added a prompt file, and every line of the current application since the first commit (an earlier SSH-based project that has since been removed). The project's owner merged each change through a pull request after its tests passed. Review it as unfamiliar code: the tests show what was checked against the mocks, and this page shows what was not, but nobody should assume a person has read and understood every line.
