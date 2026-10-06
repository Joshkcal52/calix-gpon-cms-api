# Demo: the legacy GPON layer on one machine

Nothing here touches real hardware. CMS is the mock NBI server from this repository, and "the XGS side" is a listener that prints what it receives.

```
 curl ──▶ api ──▶ Kafka: legacy-gpon-events ──▶ worker ──SOAP──▶ mock-cms
           ▲                                      │
           └──── Kafka: rogue.job-updates ◀───────┤
                                                  └──▶ Kafka: xgs-migration-requests ──▶ xgs-listener
```

## With Docker

This Compose file has not been run yet (it was written without access to Docker). The same processes were run and tested directly; see "Without Docker".

```bash
docker compose -f deploy/demo/docker-compose.yml up --build
```

Then, in another terminal:

```bash
API=http://localhost:8000
NET=NTWK-MOCK-E7

# 1. ONTs the mock CMS has seen but not provisioned
curl $API/api/v2/networks/$NET/onts/discovered

# 2. Mode A: a new subscriber signs up. Answers 202 with a job.
curl -X POST $API/api/v2/networks/$NET/signups -H 'content-type: application/json' -d '{
  "subscriber_id": "SUB-3001", "serial_number": "CXNK00C4D301", "profile": "844G-1",
  "description": "9 Elm Rd", "wait_for_online": true,
  "services": [{"vlan": 100, "bandwidth_profile": "RES-1G", "tag_action": "1"}]}'

# 3. Watch it happen (use the events_url from the reply)
curl -N $API/api/v2/jobs/<job id>/events

# 4. Mode B: hand that subscriber over to XGS
curl -X POST $API/api/v2/networks/$NET/cutovers -H 'content-type: application/json' \
  -d '{"subscriber_id": "SUB-3001", "xgs_serial_number": "CXNK10AA0001"}'

# 5. See the migration request arrive on the XGS side
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

# Emergency rollback: break a PON, look at the plan, restore it
curl -X POST 'http://localhost:18080/mock/damage?pon=1/1/1'
docker compose -f deploy/demo/docker-compose.yml run --rm rollback -e target_olt=OLT-MOCK --check
docker compose -f deploy/demo/docker-compose.yml run --rm rollback -e target_olt=OLT-MOCK
```

The rollback restores the snapshot in `network-backups/olts/OLT-MOCK.yml`, which is the mock CMS's starting state. Anything you provisioned in steps 2 to 4 is not in that snapshot and is left alone.

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

WORKER_EMBEDDED=false uvicorn main:app --port 8000      # terminal 3: the API
WORKER_LANES=legacy,provisioning python -m app.worker   # terminal 4: the worker
python deploy/demo/xgs_listener.py                      # terminal 5: the XGS side
```

Then use the same `curl` commands as above. For the rollback:

```bash
git init -q -b main /tmp/network-backups && cp -r deploy/demo/network-backups/. /tmp/network-backups/
git -C /tmp/network-backups add . && git -C /tmp/network-backups commit -q -m "Nightly snapshot"

export CMS_NBI_URL=http://127.0.0.1:18080/cmsexc/ex/netconf CMS_NBI_USER=nbi-demo CMS_NBI_PASSWORD=demo-password
export NETWORK_BACKUPS_REPO=/tmp/network-backups
cd ansible/rollback && ansible-playbook rollback.yml -e target_olt=OLT-MOCK --check
```

The Kafka mock allows only one worker per consumer group. To run two workers side by side, use the Docker path or a real broker.
