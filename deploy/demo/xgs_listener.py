"""
Demo stand-in for the modern XGS/SMx side: prints every migration request
that arrives on the XGS topic. It changes nothing anywhere.

    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python deploy/demo/xgs_listener.py
"""

import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer


async def main() -> None:
    topic = os.getenv("XGS_MIGRATION_TOPIC", "xgs-migration-requests")
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        group_id=None, auto_offset_reset="earliest")
    await consumer.start()
    print(f"Listening on {topic}", flush=True)
    try:
        async for record in consumer:
            request = json.loads(record.value)
            print(f"--- migration request for subscriber {record.key.decode()} ---", flush=True)
            print(json.dumps(request, indent=2), flush=True)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
