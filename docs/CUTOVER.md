# Taking legacy GPON config to the XGS side

This repo reads the legacy network (CMS through its NBI, or each E7's CLI) and hands its configuration over in SMx's terms. It never writes to SMx: creating subscribers, ONTs and services on the XGS side is left to whichever system already talks to SMx's API.

## What it provides

- **The export.** One PON, card or network as JSON and CSV, laid out the way SMx thinks of it (subscriber, ONT, data services). Serial numbers are carried over, and columns starting `fill_in_` are left for what only the new side knows. Get it from `GET /api/v2/networks/{network}/export` or `python -m app.legacy.export`.
- **The `XGS_CUTOVER` migration request.** One subscriber at a time, published to Kafka (`xgs-migration-requests`) by `POST /api/v2/networks/{network}/cutovers`. Its shape was designed here, not matched to any consumer, so adapt it to what the receiving system expects, or read the export instead.
- **The SMx plan.** A dry run of the SMx requests that would recreate each subscriber (`/smx-plan`, `python -m app.legacy.smx`). Nothing is sent.
- **Snapshots and compare.** A snapshot saved with each export, and `python -m app.legacy.snapshot --compare` to show what changed since.

## Before you rely on it

1. **Export and snapshot each PON before the CO upgrade.** EXA may not be readable afterwards, so work from the saved files, not from a live read.
2. **On the E7 path, port bridge/router mode reads `unknown`** until someone captures the output from a real 844G (`python -m app.tools.e7_capture`, see [E7_CLI.md](E7_CLI.md)). On the CMS path it is read normally. The SMx plan refuses ports whose mode is unknown rather than guess.
3. **Voice isn't exported.** ONTs with phone ports are flagged `voice: not checked` and need moving by hand.
4. **The mapping files hold demo values.** `app/legacy/smx_map.yml` and `app/legacy/profile_map.yml` translate legacy profiles, tag actions and ports into SMx's names; if you use the SMx plan or the migration request, put your own values in them.
