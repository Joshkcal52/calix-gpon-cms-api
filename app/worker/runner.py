"""
Worker
Takes commands off the bus, runs them, publishes progress, and acknowledges
each command only when its outcome is settled.

For every command exactly one of these happens:

  success             publish the result, then acknowledge (commit the offset)
  failed for good     publish the error, copy the command to the dead-letter
                      queue, then acknowledge. Nothing is dropped, and one bad
                      command cannot block the ones behind it.
  failed for now      (CMS down, timeout, breaker open) do NOT acknowledge.
                      Wait, then take the same command again. Work queues up
                      behind it until CMS is back.
  worker dies         nothing was acknowledged, so the bus hands the command
                      to another worker.

Because a command can run more than once, handlers must check what is already
done before changing anything.

Workers hold no state between commands, so any number can run side by side.
The API embeds one for single-process use (in-memory bus); with Kafka they
run as their own pods: `python -m app.worker`.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from app.messaging.bus import BusUnavailableError, CommandConsumer, Delivery, MessageBus
from app.models.job_model import Command, JobStatus, JobUpdate

logger = logging.getLogger(__name__)


class JobError(Exception):
    """Raise inside a handler to fail the command with a structured error"""

    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 data: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.data = data or {}

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message, "retryable": self.retryable, **self.data}


class JobContext:
    """Handed to a handler so it can report progress on its job"""

    def __init__(self, bus: MessageBus, command: Command, attempt: int):
        self._bus = bus
        self._loop = asyncio.get_running_loop()
        self.command = command
        self.job_id = command.id
        self.attempt = attempt
        self._outbox: Optional[asyncio.Queue] = None
        self._pump: Optional[asyncio.Task] = None

    async def emit(self, type: str, message: str, /, **data: Any) -> None:
        """Publish a progress event for this job"""
        await self._publish(type, message, data)

    async def step(self, number: int, total: int, message: str) -> None:
        await self.emit("step", message, step=number, total_steps=total)

    def emit_threadsafe(self, type: str, message: str, /, **data: Any) -> None:
        """
        Same as emit(), for code that cannot await, such as ansible-runner's
        callback in a worker thread. Events are published in the order given.
        """
        self._loop.call_soon_threadsafe(self._enqueue, type, message, data)

    def _enqueue(self, type: str, message: str, data: Dict[str, Any]) -> None:
        if self._outbox is None:
            self._outbox = asyncio.Queue()
            self._pump = asyncio.create_task(self._pump_outbox())
        self._outbox.put_nowait((type, message, data))

    async def _pump_outbox(self) -> None:
        while True:
            type, message, data = await self._outbox.get()
            try:
                await self._publish(type, message, data)
            except BusUnavailableError as e:
                logger.warning(f"Dropped a progress event for job {self.job_id}: {e.message}")
            finally:
                self._outbox.task_done()

    async def close(self) -> None:
        """Flush events queued by emit_threadsafe(), so they land before the final status"""
        await asyncio.sleep(0)  # let callbacks already scheduled from other threads run
        if self._outbox is not None:
            await self._outbox.join()
            self._pump.cancel()

    async def _publish(self, type: str, message: str, data: Optional[Dict[str, Any]] = None, *,
                       status: Optional[JobStatus] = None, result: Optional[Dict[str, Any]] = None,
                       error: Optional[Dict[str, Any]] = None) -> None:
        await self._bus.publish_update(JobUpdate(
            job_id=self.job_id, kind=self.command.kind, summary=self.command.summary,
            type=type, message=message, data=data or None,
            status=status, result=result, error=error))


Handler = Callable[[JobContext, Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]]


def describe_error(exc: BaseException) -> Dict[str, Any]:
    """Turn any exception into the error body stored on the job"""
    if hasattr(exc, "to_dict"):  # NbiError, JobError, BusUnavailableError
        return exc.to_dict()
    return {"code": "internal_error", "message": f"Unexpected error: {exc}", "retryable": False}


class Worker:
    def __init__(self, bus: MessageBus, lanes: Sequence[str], handlers: Dict[str, Handler], *,
                 max_command_age: float = 3600.0, retry_backoff: float = 2.0,
                 max_retry_backoff: float = 60.0, heartbeat_file: Optional[str] = None):
        self._bus = bus
        self.lanes = list(lanes)
        self._handlers = handlers
        self.max_command_age = max_command_age
        self.retry_backoff = retry_backoff
        self.max_retry_backoff = max_retry_backoff
        self._heartbeat_file = heartbeat_file
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """Finish the command in hand, then exit run()"""
        self._stop.set()

    async def run(self) -> None:
        consumer = self._bus.consume_commands(self.lanes)
        await consumer.start()
        heartbeat = asyncio.create_task(self._beat()) if self._heartbeat_file else None
        logger.info(f"Worker started on lanes: {', '.join(self.lanes)}")
        try:
            while not self._stop.is_set():
                delivery = await consumer.next(timeout=1.0)
                if delivery is not None:
                    await self._process(consumer, delivery)
        finally:
            if heartbeat:
                heartbeat.cancel()
            await consumer.stop()
            logger.info("Worker stopped")

    async def _process(self, consumer: CommandConsumer, delivery: Delivery) -> None:
        try:
            await self._handle(consumer, delivery)
        except BusUnavailableError as e:
            # The outcome could not be recorded, so the command stays in the queue
            logger.warning(f"Bus unavailable while handling command {delivery.command.id}: {e.message}")
            await consumer.retry(delivery)
            await self._pause(self.retry_backoff)

    async def _handle(self, consumer: CommandConsumer, delivery: Delivery) -> None:
        command = delivery.command
        context = JobContext(self._bus, command, delivery.attempt)
        age = (datetime.now(timezone.utc) - command.created_at).total_seconds()
        expired = age > self.max_command_age

        try:
            if expired:
                raise JobError(
                    "command_expired",
                    f"This request waited {age / 60:.0f} minutes in the queue, longer than the "
                    f"{self.max_command_age / 60:.0f} minute limit, so it was not run. "
                    f"Submit it again if it is still wanted.")
            handler = self._handlers.get(command.action)
            if handler is None:
                raise JobError("unknown_action", f"This worker has no handler for action '{command.action}'.")

            started = f"Started: {command.summary}"
            if delivery.attempt > 1:
                started += f" (attempt {delivery.attempt})"
            await context._publish("status", started, {"attempt": delivery.attempt}, status=JobStatus.RUNNING)
            try:
                result = await handler(context, command.payload) or {}
            finally:
                await context.close()

        except (asyncio.CancelledError, BusUnavailableError):
            raise  # not acknowledged, so the command is delivered again
        except Exception as exc:
            error = describe_error(exc)
            if error["code"] == "internal_error":
                logger.exception(f"Command {command.id} ({command.action}) crashed")

            if error.get("retryable") and not expired:
                delay = min(self.max_retry_backoff, self.retry_backoff * 2 ** (delivery.attempt - 1))
                delay = max(delay, float(error.get("retry_after_seconds") or 0))
                await context._publish("warning", f"{error['message']} Retrying in {delay:.0f} seconds.",
                                       {"attempt": delivery.attempt, "error": error})
                await consumer.retry(delivery)  # NOT acknowledged
                await self._pause(delay)
                return

            await context._publish("error", error["message"], error, status=JobStatus.FAILED, error=error)
            await consumer.dead_letter(delivery, error)
            await consumer.ack(delivery)  # the outcome is recorded, safe to move on
            return

        # Publish the result first. If we die before the ack, the command runs
        # again and the (repeat-safe) handler reports success a second time.
        await context._publish("result", f"Finished: {command.summary}", result,
                               status=JobStatus.SUCCEEDED, result=result)
        await consumer.ack(delivery)

    async def _pause(self, seconds: float) -> None:
        """Sleep, but wake at once if the worker is asked to stop"""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _beat(self) -> None:
        """Touch a file every few seconds so Kubernetes can tell the event loop is alive"""
        while True:
            with open(self._heartbeat_file, "w") as f:
                f.write(datetime.now(timezone.utc).isoformat())
            await asyncio.sleep(10)
