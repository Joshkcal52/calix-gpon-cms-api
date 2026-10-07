# Demo: the legacy GPON layer on one machine

Nothing here touches real hardware. CMS is the mock NBI server from this repository, and "the XGS side" is a listener that prints what it receives.

```
 curl ──▶ api ──▶ Kafka: legacy-gpon-events, gpon-cms.commands.provisioning ──▶ worker ──SOAP──▶ mock-cms
           ▲                                                                     │
           └──── Kafka: gpon-cms.job-updates ◀───────────────────────────────────┤
                                                                                 └──▶ Kafka: xgs-migration-requests ──▶ xgs-listener
```

The quickest path needs neither Docker nor Kafka: `MOCK_MODE=true python main.py`, as in `HANDOFF.md`. This page is for the multi-process topology.

## With Docker

This Compose file has been run end to end once, with Apache Kafka 3.7.1: every step below worked, including the outage and the worker restart. The same processes can also run without Docker; see "Without Docker".

```bash
docker compose -f deploy/demo/docker-compose.yml up --build
```

The Compose file sets a demo API key and the mock NBI server's deliberately fake router-side names (`CMS_RG_*`). Never copy those names to a real CMS.

Then, in another terminal:

```bash
API=http://localhost:8000
NET=NTWK-MOCK-E7
KEY='X-API-Key: demo-key-0123456789abcdef'

# 1. ONTs the mock CMS has seen but not provisioned
curl -H "$KEY" $API/api/v2/networks/$NET/onts/discovered

# 2. A new subscriber on an 844G: ports configured, the data service on the router side
curl -H "$KEY" -X POST $API/api/v2/networks/$NET/signups -H 'content-type: application/json' -d '{
  "subscriber_id": "SUB-3001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
  "description": "9 Elm Rd", "wait_for_online": true,
  "ports": [{"port": 1, "subscriber_id": "SUB-3001", "security_profile": "2"}],
  "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1", "attach_to": "router"}]}'

# 3. Watch it happen (use the events_url from the reply)
curl -N -H "$KEY" $API/api/v2/jobs/<job id>/events

# 4. Read the ports back; disable and re-enable a service
curl -H "$KEY" $API/api/v2/networks/$NET/onts/15/ports
curl -H "$KEY" -X POST $API/api/v2/networks/$NET/onts/15/ports/rg1/services/1/disable

# 5. Export PON 1/1/1 for a cutover, and plan the SMx requests (dry run, nothing is sent).
#    Voice is not exported: the voice column reads "not checked" for every ONT that has voice ports.
curl -H "$KEY" "$API/api/v2/networks/$NET/export?pon=1/1/1&format=csv" > pon-1-1-1.csv
curl -H "$KEY" $API/api/v2/networks/$NET/subscribers/SUB-1003/smx-plan

# 6. Hand a subscriber over to XGS through Kafka, and see it arrive on the XGS side
curl -H "$KEY" -X POST $API/api/v2/networks/$NET/cutovers -H 'content-type: application/json' \
  -d '{"subscriber_id": "SUB-1003", "xgs_serial_number": "CXNK10AA0001"}'
docker compose -f deploy/demo/docker-compose.yml logs xgs-listener
```

Things to try:

```bash
# CMS goes down: reads fail fast with a clear error, queued jobs wait and then finish
curl -X POST 'http://localhost:18080/mock/outage?mode=refused'
curl -X POST 'http://localhost:18080/mock/outage?mode=none'

# A worker dies mid-job: another one picks the job up and finishes it
docker compose -f deploy/demo/docker-compose.yml kill -s SIGKILL worker
docker compose -f deploy/demo/docker-compose.yml up -d worker

# Something changes on CMS: compare the saved snapshot with what CMS holds now (read-only)
curl -X POST 'http://localhost:18080/mock/damage?pon=1/1/1'
python -c 'import json,sys,yaml; print(json.dumps(yaml.safe_load(open(sys.argv[1]))))' \
  deploy/demo/snapshots/OLT-MOCK.yml > /tmp/snapshot.json
curl -H "$KEY" -X POST $API/api/v2/networks/$NET/snapshot-compare -H 'content-type: application/json' \
  -d @/tmp/snapshot.json
```

`snapshots/OLT-MOCK.yml` is the mock CMS's starting state, taken with only `CMS_RG_PORT_MODE_ELEMENT` set (so port modes are read and router ports are not). The compare reports the deleted ONT, the disabled ONT, the moved VLAN and the stray service, plus anything you provisioned in step 2. It changes nothing; there is no restore.

## Without Docker

This is the path that was run and tested. It uses an in-process Kafka mock (real Kafka wire protocol, no Java) from the `confluent-kafka` package.

```bash
pip install -r requirements-test.txt

# Terminal 1: Kafka. Prints KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:<port>
python -m tests.support.mock_kafka

# Terminal 2: the mock CMS
uvicorn app.nbi.mock_cms_server:app --port 18080

# Terminals 3 to 5: set these in each, using the address from terminal 1
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:<port>
export MOCK_MODE=false CMS_HOST=127.0.0.1 CMS_USERNAME=nbi-demo CMS_PASSWORD=demo-password
export API_KEYS=demo:demo-key-0123456789abcdef
export CMS_RG_SERVICE_SLOT=UNVERIFIED-RG CMS_RG_PORT_TYPE=UnverifiedRgPort \
       CMS_RG_PORT_ID_ELEMENT=unverifiedrgport CMS_RG_PORT_MODE_ELEMENT=unverified-rg-binding

WORKER_EMBEDDED=false uvicorn main:app --port 8000      # terminal 3: the API
WORKER_LANES=legacy,provisioning python -m app.worker   # terminal 4: the worker
python deploy/demo/xgs_listener.py                      # terminal 5: the XGS side
```

Then use the same `curl` commands as above. The command-line tools work against the same mock CMS with the same variables:

```bash
python -m app.tools.cms_check --network NTWK-MOCK-E7 --ont-id 3
python -m app.legacy.export --network NTWK-MOCK-E7 --card 1/1 --output-dir ./cutover
python -m app.legacy.smx --input ./cutover/NTWK-MOCK-E7-card-1-1.services.csv --output plan.json
python -m app.legacy.snapshot --compare deploy/demo/snapshots/OLT-MOCK.yml
```

The Kafka mock allows only one worker per consumer group. To run two workers side by side, use the Docker path or a real broker.
