# Simulated GPON: signups at scale, against the mocks

**What this is and is not.** These numbers come from the simulated CMS (`app/nbi/mock_cms.py`) and the mock E7 (`app/nbi/mock_e7.py`). They show how the API behaves at 32 and 64 customers: its queue, retries, repeat-safety and E7 session handling. They are not a measure of real CMS or E7 speed. The mock CMS waits 50 ms per call and the mock E7 answers at once (or after a set delay), so every duration below depends on what the mock was told to do. Nothing here touched a real CMS or E7: no `CMS_HOST` or `E7_NODES`, and SSH only to 127.0.0.1.

Run 2026-10-08 on Windows 11, Python 3.12.7, branch `claude/gpon-sim`.

## How to run it

```bash
python tools/sim_signups.py --backend cms    --customers 64          # starts the API in-process, as python main.py --mock
python tools/sim_signups.py --backend e7-cli --customers 64 [--e7-latency 0.05]
python tools/sim_signups.py --backend e7-cli --customers 32 --faults  # the fault round (E7 path, in-process)

# or against a separately started API
MOCK_PON_SIZE=64 python main.py --mock --backend e7-cli    # or: python main.py --mock --backend e7-cli --pon-size 64
python tools/sim_signups.py --url http://127.0.0.1:8000 --backend e7-cli --customers 64
```

`MOCK_PON_SIZE` (or `--pon-size`, or `MockE7(pon_size=N)` / `MockCmsBackend(pon_size=N)`) fills one PON with up to 64 discovered ONTs, serials `CXNK00A1xxxx`, half 844G-1, a quarter 812G-1 and a quarter 716GE-I (`app/nbi/mock_pon.py`). On the mock E7 that PON is 1/1/1, and its existing 844G (`00A3F104`) counts toward the total, so 64 means 33 × 844G, 16 × 812G and 15 × 716GE-I. On the mock CMS it is 1/1/3, because 1/1/1 already holds eight provisioned ONTs. A sized mock also has VLAN 400 and a bandwidth profile RES-500M. Without the setting, both mocks are exactly as before.

## What the driver does

1. Lists the discovered ONTs on the PON and POSTs one `/signups` per customer, all at once: `SUB-0001`, and so on.
   - Everyone gets HSI on VLAN 100: on the router (G1 / rg1) of an 844G, bridged on g1 / eth1 for the others.
   - Every 4th customer also gets IPTV on VLAN 300, on the same side.
   - Customers 3, 13, 23, ... get a second data service on eth2 (VLAN 400, RES-500M).
   - Port eth1 gets the subscriber ID.
2. Polls every job to completion. It records each job's run time (started to finished) and total time (queued to finished), its result, and any error.
3. Reads every ONT and its ports back. It compares the services (side, port, VLAN, bandwidth profile), serial, profile, subscriber ID and eth1's subscriber ID with what was asked.
4. Sends the whole set again. It checks that every job succeeds on the same ONT ID, that no job logs "Created ONT", "Bound" or "Set port", and, on the E7, that the mock received no write at all.
5. Lifecycle, each change verified by reading it back:
   - disable and then enable eth1;
   - change HSI from RES-1G to RES-500M on an 844G's router side and on a bridged ONT;
   - delete the last four customers with force (each must read 404 and be discovered again);
   - delete one of them a second time (must succeed as `already_absent`).

   Since PR #10 was merged, deletes are sent with `voice_checked=true`, and the round also checks that a delete without it is refused and, on the in-process E7, that an ONT with a phone line the API cannot see is refused and linked to its PON again. The numbers below were taken before that change.

## Results

Times are wall clock from the first POST to the last job finishing. Every job runs through the one embedded worker, one after another.

| Run | Customers | Succeeded | Total time | Jobs/min | Job run time, median / max | Read back | Repeat round | Lifecycle |
|---|---|---|---|---|---|---|---|---|
| `--mock` (simulated CMS) | 32 | 32 | 13.7 s | 140 | 0.42 / 0.53 s | 32/32 match | 6.0 s, nothing sent | 9/9 verified |
| `--mock` (simulated CMS) | 64 | 64 | 27.0 s | 142 | 0.42 / 0.50 s | 64/64 match | 11.9 s, nothing sent | 9/9 verified |
| `--mock --backend e7-cli` | 32 | 32 | 1.15 s | 1,666 | 0.033 / 0.054 s | 32/32 match | 0.63 s, 0 writes | 9/9 verified |
| `--mock --backend e7-cli` | 64 | 64 | 2.31 s | 1,663 | 0.037 / 0.055 s | 64/64 match | 1.32 s, 0 writes | 9/9 verified |
| e7-cli, mock E7 waits 50 ms per command | 32 | 32 | 27.4 s | 70 | 0.85 / 0.98 s | 32/32 match | 17.2 s, 0 writes | 9/9 verified |
| e7-cli, mock E7 waits 50 ms per command | 64 | 64 | 53.8 s | 71 | 0.83 / 0.99 s | 64/64 match | 33.8 s, 0 writes | 9/9 verified |

**No failures in any run.** The same four main runs against a separately started `python main.py --mock [--backend e7-cli] --pon-size N` (driver with `--url`) also passed every check:

| Run | Total time | Jobs/min |
|---|---|---|
| CMS, 32 customers | 13.5 s | 142 |
| CMS, 64 customers | 26.0 s | 148 |
| E7, 32 customers | 2.9 s | 663 |
| E7, 64 customers | 5.5 s | 696 |

The E7 runs are slower there than in-process, most likely because of the server's per-request logging.

**Slowest jobs.** No customer stands out. In every run the slowest jobs are customers with two or three services, at most about 1.2 times the median. At 64 customers the slowest *total* time belongs to the last customers queued: SUB-0063 waited 52.6 s with the 50 ms mock E7, against 0.98 s of running. The wait is the queue, not the job.

**Lifecycle steps** take 0.11 to 0.22 s each on the mock CMS and 0.11 s on the mock E7. With the 50 ms E7 they take 0.23 to 0.43 s. A bandwidth profile change on the E7 is a `remove eth-svc` followed by an `add eth-svc`. Both changes read back correctly, and the ONT kept its other services.

### CLI commands per customer (E7 path)

The same in every E7 run:

| ONT and services | Commands | Of which writes |
|---|---|---|
| 812G-1, HSI only | 14 | 3 |
| 844G-1, HSI only | 15 | 3 |
| 716GE-I, HSI + DATA2 | 16 | 4 |
| 844G-1, HSI + IPTV | 17 | 4 |

The writes are `create ont`, `set ont-port` and one `add eth-svc` per service. The 15 commands of an 844G with one service:

- `show ont-profile` ×2: the router check, then reading the ports
- `show ont` ×6: by serial number; `discovered detail`; `detail` of every ONT, to find a free ID; the new ONT's detail ×3
- `show ont-port` ×4
- `create ont`, `set ont-port` and `add eth-svc` ×1 each

Totals: 487 commands for 32 customers and 975 for 64, about 80% of them reads. A repeat signup sends 9 or 10 reads and no writes (305 and 609 commands in all).

### Is the single SSH session per E7 the bottleneck?

**On one E7, yes: it is busy almost the whole time, and nothing could run beside it anyway.**

- With the instant mock E7, the session was busy 75% of the wall time, at 1.8 ms per command (mostly SSH round trips on localhost).
- With the mock waiting 50 ms per command, it was busy 98.4% of the time, at 54 to 55 ms per command.
- So throughput on one E7 is about one customer every (15 commands × the E7's time per command). That is 50 ms → about 70 jobs a minute, measured above. At an assumed 200 ms per command it would be about 20 a minute. That figure is arithmetic, not a measurement: real E7 timings are unknown.

**But more SSH sessions would not help today.**

- Each network's jobs share an ordering key (`key=network`), so they run one at a time by design.
- The embedded worker (`app/worker/runner.py`, `Worker.run`) handles one command at a time across every network. Two E7s behind one in-memory worker would not run in parallel either.
- Running E7s in parallel needs more worker processes (Kafka, `python -m app.worker`). Speeding up one E7 means sending fewer commands per job: see finding 2 below.

## Fault injection (E7 path)

Run four times with `--faults`. Every case behaved the same way in all four runs, apart from the counts noted for case 1b.

| Fault | What happened | Verdict |
|---|---|---|
| **1. The E7 closes the session while idle, between jobs, mid-run.** `close_idle_sessions()` after 6 signups, then 6 more. | The next command found the session closed, logged in once and was sent once. 12/12 succeeded. No warnings, no write sent twice. | As designed |
| **1b. Idle sessions closed every 50 ms while 6 signups ran.** This catches the gaps between the commands of one job, and logins. | 6/6 succeeded every time, with no write sent twice. 1 to 16 closes landed and caused 0 to 8 fresh logins. In three of the four runs, one or two jobs logged `E7 E7-MOCK closed the SSH session. Retrying in 2 seconds.`: the close raced the next command. Once a command has been written, the session doesn't resend it, so the whole job retried, and the retry read back what was already there. | Correct: safe to repeat |
| **2a. Hang up on one `add eth-svc` before it runs.** | Job failed as retryable, retried after 2 s. The retry found the ONT, saw no service and sent the add once more. The ONT has 1 service, as asked. | Correct |
| **2b. Hang up on one `add eth-svc` after it has run** (the reply is lost). | Same retry. The retry read back the service the lost command had created, did not add it again, and went on to the next one. The ONT has exactly the 2 services asked for, none twice. | Correct, and the case that matters |
| **3a. A fifth service onto an 844G that already has 4 on G1** (VLANs 100/200/300 + a no-VLAN tag action, then VLAN 400). | Failed: `ONT port 21/G1 already has 4 services; the E7 allows at most 4 on an RG interface.` **No write was sent.** | Refused before sending |
| **3b. The same five services in one signup for a new 844G.** | Refused at the fifth service, but only after `create ont`, `set ont-port` and four `add eth-svc` were sent. The rollback then sent `set ont … pon-port none` and `delete ont … forced`. Nothing was left behind, but 8 writes reached the E7 for a request that could never succeed. | **Not** refused before sending (finding 1) |

Case 2b is now a test: `test_a_lost_reply_to_add_eth_svc_does_not_duplicate_the_service_when_the_signup_runs_again` in `tests/test_mock_pon.py`.

## Findings and surprises

1. **A signup that asks for more services than an E7 port takes is refused only partway through.** The E7 limits (8 per g port, 4 per G) are checked per `add eth-svc`, against what is already on the port. For a new ONT, the ONT is created and the first services added before the limit is hit, and then everything is rolled back (case 3b). The request could be refused before anything is sent: count the services the signup asks for per port and side, add those already there, and compare with the limits. This is not changed here.
   - The simulated CMS has no such limit and would accept five router-side services. What a real CMS allows is not known.
2. **Every `create ont` without an `ont_id` lists every ONT on the E7** (`show ont detail`, in `_free_ont_id`) to find a free ID. That costs nothing against the mock. On a real E7 carrying hundreds or thousands of ONTs, it is likely to be the slowest command of each signup, and it gets slower as the E7 fills. Billing could pass an `ont_id`, or the backend could list IDs more cheaply. The ONT profiles are also read twice per 844G signup.
3. **A retryable failure is retried until the command is an hour old** (`COMMAND_MAX_AGE_SECONDS`, default 3600), with backoff up to 60 s, and the circuit breaker opens for 30 s at a time. My first attempt at fault 1b set the mock's idle timeout to 2 ms, so short that logins could not finish. The job then sat in `running`, logging a warning each time, until I stopped it. That is the intended behaviour, but whatever watches the jobs should treat a long run of warnings as an outage, not wait for `failed`.
4. **Log wording on the E7 path.** The gateway and breaker log `CMS call find_ont_by_serial failed` and `CMS circuit breaker opened` even when the backend is the E7 CLI.
5. **Fixed: `main.py` never stopped the message bus.** An `await bus.stop()` had ended up after the `return` in `start_mock_e7_if_needed`, so it never ran. It is now at the end of the app's shutdown. This is harmless with the in-memory bus, but a Kafka bus was left open at shutdown.
6. **Fixed in the mock E7: an ONT's port settings outlived the ONT.** After `delete ont 7`, a new ONT 7 started with the old ONT's port settings (a disabled g1, say). The real E7 presumably does not do this; the mock now clears them.
7. **The mock E7's `idle_timeout` only applies from the next wait.** It is read when the mock starts waiting for a command, so setting it while a session is already idle closes nothing (my first run of fault 1 showed 0 new logins). I added `close_idle_sessions()` to close idle sessions on demand.
8. **The two mocks' speeds are invented and very different:** 50 ms per mock CMS call, 0 ms per mock E7 command. The first two rows of each backend in the table compare the mocks, not the backends. The 50 ms E7 rows put both on the same footing, and there the E7 path is about half the CMS path's speed: about 15 commands per signup against about 9 CMS calls.

## Changes made

- `app/nbi/mock_pon.py` (new): the filled PON's serials, model mix, extra VLAN and bandwidth profile.
- `app/nbi/mock_e7.py`:
  - `MockE7(pon_size=N)`, and `--pon-size` when it is run on its own;
  - VLANs and bandwidth profiles per instance;
  - `hang_up_once`, `hang_up_after_run`, `latency` and `close_idle_sessions()`;
  - an ONT's port settings are cleared when it is deleted.
- `app/nbi/mock_cms.py`: `MockCmsBackend(pon_size=N)` fills 1/1/3; VLANs and bandwidth profiles per instance. `mock_cms_server.py` reads `MOCK_PON_SIZE` too.
- `config.py` / `app/nbi/gateway.py` / `main.py`:
  - the `MOCK_PON_SIZE` setting and `python main.py --pon-size`;
  - the app keeps the mock E7 it started at `app.state.mock_e7`, for the driver;
  - the `bus.stop()` fix.
- `tools/sim_signups.py` (new): the driver.
- `tests/test_mock_pon.py` (new): 10 tests covering the filled PON on both mocks, the defaults unchanged, the 1:64 limit, the setting reaching both mocks, port settings cleared on delete, the idle close, and the lost-reply retry.

## Test suite

`python -m pytest`: **200 passed, 2 skipped, 1 failed.** The failure is `tests/test_legacy_layer.py::test_cms_client_backend_survives_an_expired_session_and_reports_outages`, which fails the same way on `main` before these changes (190 passed, 2 skipped, 1 failed there). The 10 extra passes are `tests/test_mock_pon.py`.
