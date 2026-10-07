"""
Worker entry point: `python -m app.worker`

Runs one stateless worker until SIGTERM/SIGINT, then finishes the command in
hand and leaves the consumer group. This is what the Kubernetes worker pods
run. Environment:

  KAFKA_BOOTSTRAP_SERVERS   required
  WORKER_LANES              comma-separated: provisioning, legacy
"""

import asyncio
import logging
import signal

from app.messaging.bus import build_bus
from app.nbi.gateway import build_gateway
from app.worker.handlers import build_handlers
from app.worker.runner import Worker
from config import settings

logger = logging.getLogger("app.worker")


async def main() -> None:
    if not settings.kafka_bootstrap_servers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS is not set. A standalone worker needs Kafka; "
                         "without it the API runs its own embedded worker.")
    lanes = settings.worker_lane_list()
    settings.check_worker_timeouts(lanes)

    bus = build_bus(settings)
    gateway = build_gateway(settings)
    worker = Worker(
        bus, lanes, build_handlers(settings, gateway, bus),
        max_command_age=settings.command_max_age_seconds,
        heartbeat_file=settings.worker_heartbeat_file)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)

    await bus.start()
    try:
        await worker.run()
    finally:
        await gateway.close()
        await bus.stop()


if __name__ == "__main__":
    logging.basicConfig(level=settings.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
