# Provisioning without the CMS NBI: the E7 command line

CMS's northbound interface (NBI) is a separately licensed part of CMS. Without it, there is no API on CMS to call. The E7 EXA nodes themselves have a full provisioning command line, documented publicly by Calix, and the E7 CLI backend (`app/nbi/e7_backend.py`) drives it over SSH.

Nothing above the backend changes: the same routes, jobs, retries, circuit breaker, rollback and safety checks run, because the backend sits behind the same NBI gateway as the CMS backend. SNMP is not an alternative: Calix's OSS integration guide for the E-Series says the E7 cannot be provisioned over SNMP.

## Turning it on

```bash
export E7_NODES="OLT-04=10.0.0.4,OLT-05=10.0.0.5:22"   # NAME=host[:port]; NAME is the {network} in URLs
export E7_USERNAME=... E7_PASSWORD=...                  # one CLI account, on every E7 listed
export NBI_TIMEOUT_SECONDS=30                           # an E7 call runs several commands
python -m app.tools.e7_capture --network OLT-04 --ont-id <an 844G with services> --out ./e7-captures
```

Each E7's SSH host key must be in `~/.ssh/known_hosts` (or the file `E7_KNOWN_HOSTS` names). Add it with `ssh-keyscan`. `E7_KNOWN_HOSTS=none` turns the check off; use that in a lab only. If `CMS_HOST` is also set, choose the path with `NBI_BACKEND=cms|e7-cli` or `python main.py --backend cms|e7-cli`; with both set and no choice, the API refuses to start. `python main.py --mock --backend e7-cli` runs the whole API against a mock E7 it starts itself (network `E7-MOCK`). A separate worker process (`python -m app.worker`) does not start one: point its `E7_NODES` at `python -m app.nbi.mock_e7`.

## How the API maps onto the CLI

The commands are taken from Calix's public E7 EXA R3.x CLI reference and GPON applications guide.

| API operation | E7 command |
|---|---|
| Discovered ONTs | `show ont discovered detail` |
| Read an ONT | `show ont <id> detail` and `show ont-port <id> eth-svc` |
| Find by serial or subscriber | `show ont serial-number <sn>` or `show ont subscriber-id <s>`, then the read above |
| Create an ONT | `create ont <id> profile <p> serial-number <sn> [subscriber-id <s>] [description "<d>"]` |
| Add a service | `add eth-svc <name> to-ont-port <id>/g<n> bw-profile <bw> svc-tag-action <ta> [outer-vlan <v>] [description <d>]` |
| Router-side service (844G) | the same command on `<id>/G<n>`: Calix designates the residential gateway "G1" |
| Change a service | `remove eth-svc <name> from-ont-port <id>/g<n>`, then `add eth-svc` with the same name and the new values. Every value read back that `add eth-svc` takes (VLANs, description, multicast profile, PON CoS, admin state) is carried over. If the E7 refuses the add, the old service is added back and the refusal is reported |
| Remove a service | `remove eth-svc <name> from-ont-port <id>/g<n>` |
| Change a port | `set ont-port <id>/g<n> [admin-state ...] [description ...] [subscriber-id ...] [eth-sec-profile ...]` |
| Delete an ONT | The API removes each data service first (`remove eth-svc`), then `set ont <id> pon-port none` (unlink it from its PON: Calix's M&T guide says an ONT can only be deleted when its record is not linked to a discovered ONT), then `delete ont <id>` **without** `forced`, so the E7 refuses while anything this API cannot see is left, such as a voice line. If it refuses, the ONT is linked to its PON again (`set ont <id> pon-port <the location show ont printed>`) and the refusal is reported. A rollback of an ONT the same signup just created does the same, removing only the services that signup added; nothing on this path sends `delete ont <id> forced`. An ONT whose profile has voice ports needs `voice_checked=true` |
| Ports, profiles, VLANs | `show ont-port <id> detail`, `show ont-profile`, `show vlan` |

What differs from CMS:

- **One SSH session per E7.** Commands to one E7 run one at a time (Calix allows five CLI sessions per E7; this uses one). A command that times out or is cancelled closes its session, because the E7's next output might belong to the abandoned command. The next call logs in again. Calix's capacities table: when a sixth CLI login is accepted, the E7 drops the oldest session as most likely idle, and the optional inactivity timeout is 30 minutes. A session the E7 closed while idle is noticed before the next command is sent; that command logs in again and is sent once. A session lost after a command was sent is never retried, because the command may have run.
- **Names, not IDs.** Bandwidth profiles, tag actions and ONT profiles are sent by name. Services have names too: `service_index` 1 is the first name in `E7_SERVICE_NAMES`, 2 the second, and so on. A service whose name is not in that list is still read (as index 101 and up) but cannot be changed through the API.
- **Peak-rate overrides are refused.** `add eth-svc` has no documented peak-rate parameter. Use a bandwidth profile with the rate.
- **Admin states.** `no-alarms` is for the ONT itself (`set ont`). Calix gives ONT ports and services only `enabled` and `disabled`, so `enabled-no-alarms` on a port or a service is refused before anything is sent.
- **Services per ONT port, as Calix's capacities table gives them:** 8 on an Ethernet (g) port, 4 on a residential-gateway (G) interface (and 1 on a full-bridge interface, which this backend does not use). One more is refused before anything is sent, whatever `E7_SERVICE_NAMES` holds.
- **Lengths are checked before sending.** An ONT's subscriber ID takes 1-63 characters and its description 1-47 (`create ont`); the API's own limit on an ONT description is 48, for CMS.
- **Every argument is checked before it is sent.** Quotes, semicolons, newlines and other control characters are refused, so no value can end the command or start another. Values with spaces are quoted, as Calix documents.

## What is not verified

**Calix shows sample output for almost none of these commands.** The command syntax is documented. What the commands print is not, so the parsers in `app/nbi/e7_parse.py` are lenient rather than exact. They read "label: value" lines and column-aligned tables, and look labels up through `ALIASES`. The one sample table in Calix's E7 CLI reference (#220-00652, `show mcast`) puts header words one space apart both within and between columns, with a line of dashes under the header, one run per column. Tables are therefore read by the dashed line's columns, and the mock prints its tables that way (a test is built from that sample). The mock E7's output (`app/nbi/mock_e7.py`) is invented, so passing tests show that the backend and the mock agree, not that a real E7 parses.

These are settings or guesses, all checked by the capture tool:

| What | Where | Default |
|---|---|---|
| The prompt | `E7_PROMPT_PATTERN` | any word ending in `>` or `#` |
| Pager and notifications off | `E7_PAGER_COMMAND`, `E7_SESSION_COMMANDS` | `set session pager disabled`, the inactivity timeout off (`set session timeout disabled`), and the same for alarm, event and TCA notifications (documented commands) |
| Table rows | `table()` in `e7_parse.py` | read under the dashed line's columns; a row whose words cross the column edges (as Calix's R3.1-3.2 M&T guide prints the show mcast sample) is read one word per column, the rest of the line in the last |
| How a refusal reads | `E7_ERROR_PATTERN` | words such as error, invalid, fail, not found, already exists. Calix documents only "Invalid input detected at '^' marker"; an open-source E7 test library checks real replies for "fail" and "not found", and sees "success" after many writes |
| Service names | `E7_SERVICE_NAMES` | `Data1` to `Data8`. Run `show eth-svc-name` on the E7 to see the real ones |
| ONT state words meaning up | `ONLINE_WORDS` in `e7_backend.py` | present, up, online, enabled, ... |
| Port counts in `show ont-profile` | `port_columns` and `port_counts` in `e7_parse.py` | a column per port letter (Calix's M&T guide refers to "the G (RG) and F (FB) Ports columns"; G and g are kept apart), or failing that one cell such as "4g 2p 1G" |

Also unknown:

- **Whether CMS notices CLI changes by itself.** CMS still manages these E7s. Create one ONT through the API and check that CMS shows it. If it doesn't, CMS needs a resync of that node.
- **Port modes.** Bridge or router mode is read as `unknown` and cannot be set. Calix documents `add ont-port <id>/g1 to-res-gw` and `remove ont-port <id>/g1 from-res-gw`, but these are not built until a real 844G has been checked.

## Before the first change on a real E7

1. **Run the capture.** `python -m app.tools.e7_capture --network <E7> --ont-id <id> --out ./e7-captures -v`. It sends only `show ...` and `set session ...` commands (a guard refuses anything else). It saves each output and prints what the parsers made of it, the labels nobody recognised, and the ONT state words seen. Point it at an 844G with a router-side service, and at a 716GE-I or 812G.
2. **Fix the parsers against the captures.** Add the captures to `tests/fixtures/`, write tests that parse them, and adjust `ALIASES`, the parsers or the settings until those tests pass. Check that the missing-ONT step reads as not found: if it doesn't, the error pattern does not match the E7's wording.
3. **Make the first write on one spare lab ONT.** Run an activation with one service, change it, remove it, delete the ONT. After each step, compare against `show ont <id> detail` on the E7 and against CMS.

## Sources

- Calix, E-Series EXA R3.x Turn-Up and Transport Guide, System Support Capacities: services per ONT interface (8 Ethernet, 4 RG, 1 FB), five CLI sessions, the oldest dropped on a sixth login, 30-minute timeout, 4,000-character paste
- Calix, E-Series (E7 OS R3.1-3.2) Maintenance and Troubleshooting Guide: show ont-profile's G and F Ports columns, `set ont <id> pon-port none` to unlink, deleting an ONT only once unlinked, the show mcast sample with unpadded rows
- DarcyChang/MyProjects on GitHub (`Teststeplib_e7.py`, `E7_base.robot`): an automation library run against real E7s. Its checks show `show ont <id>` with a `Location:` field, `Label : value` detail lines (`File Vers :`, `Management Mode :`), "not found" for a missing object, "fail" in refusals, `Data1`.. service names, and `set session timeout disable` at login
- Calix, E7 EXA R3.x GPON Applications Guide: interface groups RG `G1`, full bridge `F1`, half-bridge `g1`

- Calix, E7 EXA R3.x CLI Reference: [add eth-svc](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36169.htm), [create svc-tag-action](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36190.htm), [create ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36182.htm), [delete ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36211.htm), [remove eth-svc](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36262.htm), [show ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36360.htm), [show ont-port](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/41014.htm), [set ont-port](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/105316.htm), [show ont-profile](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36367.htm), [About the E7 CLI](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/41308.htm)
- DarcyChang/MyProjects on GitHub (`Teststeplib_e7.py`, `E7_base.robot`): an automation library run against real E7s. Its checks show `show ont <id>` with a `Location:` field, `Label : value` detail lines (`File Vers :`, `Management Mode :`), "not found" for a missing object, "fail" in refusals, `Data1`.. service names, and `set session timeout disable` at login
- Calix, E7 EXA R3.x GPON Applications Guide: [creating an ONT](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/36650.htm), [ONT interface groups (RG "G1", full bridge "F1")](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/66248.htm), [GPON L2 data services](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/37525.htm)
