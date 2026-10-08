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

Each E7's SSH host key must be in `~/.ssh/known_hosts` (or the file `E7_KNOWN_HOSTS` names). Add it with `ssh-keyscan`. `E7_KNOWN_HOSTS=none` turns the check off; use that in a lab only. `E7_NODES` and `CMS_HOST` cannot both be set.

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
| Change a service | `set ont-port <id>/g<n> eth-svc <name> [bw-profile ...] [svc-tag-action ...] [outer-vlan ...] [admin-state ...]` |
| Remove a service | `remove eth-svc <name> from-ont-port <id>/g<n>` |
| Change a port | `set ont-port <id>/g<n> [admin-state ...] [description ...] [subscriber-id ...] [eth-sec-profile ...]` |
| Delete an ONT | `delete ont <id> forced` (the API itself still refuses unless no services remain or `force=true`) |
| Ports, profiles, VLANs | `show ont-port <id> detail`, `show ont-profile`, `show vlan` |

What differs from CMS:

- **One SSH session per E7.** Commands to one E7 run one at a time (Calix allows five CLI sessions per E7; this uses one). A command that times out or is cancelled closes its session, because the E7's next output might belong to the abandoned command. The next call logs in again.
- **Names, not IDs.** Bandwidth profiles, tag actions and ONT profiles are sent by name. Services have names too: `service_index` 1 is the first name in `E7_SERVICE_NAMES`, 2 the second, and so on. A service whose name is not in that list is still read (as index 101 and up) but cannot be changed through the API.
- **Peak-rate overrides are refused.** `add eth-svc` has no documented peak-rate parameter. Use a bandwidth profile with the rate.
- **Every argument is checked before it is sent.** Quotes, semicolons, newlines and other control characters are refused, so no value can end the command or start another. Values with spaces are quoted, as Calix documents.

## What is not verified

**Calix shows no sample output for any of these commands.** The command syntax is documented. What the commands print is not, so the parsers in `app/nbi/e7_parse.py` are lenient rather than exact. They read "label: value" lines and column-aligned tables, and look labels up through `ALIASES`. The mock E7's output (`app/nbi/mock_e7.py`) is invented, so passing tests show that the backend and the mock agree, not that a real E7 parses.

These are settings or guesses, all checked by the capture tool:

| What | Where | Default |
|---|---|---|
| The prompt | `E7_PROMPT_PATTERN` | any word ending in `>` or `#` |
| Pager and notifications off | `E7_PAGER_COMMAND`, `E7_SESSION_COMMANDS` | `set session pager disabled`, and the same for alarm, event and TCA notifications (documented commands) |
| How a refusal reads | `E7_ERROR_PATTERN` | words such as error, invalid, failed, not found, already exists. Calix documents only "Invalid input detected at '^' marker" |
| Service names | `E7_SERVICE_NAMES` | `Data1` to `Data8`. Run `show eth-svc-name` on the E7 to see the real ones |
| ONT state words meaning up | `ONLINE_WORDS` in `e7_backend.py` | present, up, online, enabled, ... |
| Port counts in `show ont-profile` | `port_counts` in `e7_parse.py` | read from Calix's legend letters (g, p, G, F, ...) as "4g 2p 1G" |

Also unknown:

- **Whether CMS notices CLI changes by itself.** CMS still manages these E7s. Create one ONT through the API and check that CMS shows it. If it doesn't, CMS needs a resync of that node.
- **Port modes.** Bridge or router mode is read as `unknown` and cannot be set. Calix documents `add ont-port <id>/g1 to-res-gw` and `remove ont-port <id>/g1 from-res-gw`, but these are not built until a real 844G has been checked.

## Before the first change on a real E7

1. **Run the capture.** `python -m app.tools.e7_capture --network <E7> --ont-id <id> --out ./e7-captures -v`. It sends only `show ...` and `set session ...` commands (a guard refuses anything else). It saves each output and prints what the parsers made of it, the labels nobody recognised, and the ONT state words seen. Point it at an 844G with a router-side service, and at a 716GE-I or 812G.
2. **Fix the parsers against the captures.** Add the captures to `tests/fixtures/`, write tests that parse them, and adjust `ALIASES`, the parsers or the settings until those tests pass. Check that the missing-ONT step reads as not found: if it doesn't, the error pattern does not match the E7's wording.
3. **Make the first write on one spare lab ONT.** Run an activation with one service, change it, remove it, delete the ONT. After each step, compare against `show ont <id> detail` on the E7 and against CMS.

## Sources

- Calix, E7 EXA R3.x CLI Reference: [add eth-svc](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36169.htm), [create svc-tag-action](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36190.htm), [create ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36182.htm), [delete ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36211.htm), [remove eth-svc](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36262.htm), [show ont](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36360.htm), [show ont-port](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/41014.htm), [set ont-port](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/105316.htm), [show ont-profile](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/36367.htm), [About the E7 CLI](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/cli/41308.htm)
- Calix, E7 EXA R3.x GPON Applications Guide: [creating an ONT](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/36650.htm), [ONT interface groups (RG "G1", full bridge "F1")](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/66248.htm), [GPON L2 data services](https://www.calix.com/content/dam/calix/mycalix-misc/lib/iae/exa/r3x/gag/37525.htm)
