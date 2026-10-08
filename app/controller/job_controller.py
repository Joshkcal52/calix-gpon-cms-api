"""
Job Service
The API's view of queued work. It puts commands on the bus, follows the
updates stream, and keeps each job's status and ordered event log so clients
can poll a job or follow it live over Server-Sent Events.

The job table lives in memory and is rebuilt from the updates stream. With
the in-memory bus that means jobs are lost on restart; with Kafka each API
process replays the updates topic when it starts.
"""

import asyncio
import logging
from collections import OrderedDict
from typing import AsyncIterator, List, Optional, Set

from app import audit
from app.messaging.bus import BusUnavailableError, MessageBus
from app.models.job_model import Command, Job, JobEvent, JobStatus, JobUpdate

logger = logging.getLogger(__name__)

TERMINAL = (JobStatus.SUCCEEDED, JobStatus.FAILED)


class JobNotFound(KeyError):
    pass


class _JobRecord:
    def __init__(self, job: Job):
        self.job = job
        self.events: List[JobEvent] = []
        self.seen: Set[str] = set()  # update ids already applied
        self.waiters: Set[asyncio.Event] = set()

    @property
    def done(self) -> bool:
        return self.job.status in TERMINAL


class JobStore:
    def __init__(self, bus: MessageBus, max_jobs: int = 500):
        self._bus = bus
        self.max_jobs = max_jobs
        self._records: "OrderedDict[str, _JobRecord]" = OrderedDict()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._follow_updates(self._bus.updates()), name="job-updates")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def submit(self, command: Command) -> Job:
        """
        Queue a command. Returns once the bus has stored it; raises
        BusUnavailableError if it could not be stored.
        """
        queued = JobUpdate(job_id=command.id, kind=command.kind, summary=command.summary,
                           type="status", message="Queued", status=JobStatus.QUEUED,
                           timestamp=command.created_at, requested_by=command.requested_by)
        self.apply(queued)  # visible on this API process straight away
        try:
            await self._bus.publish_update(queued)  # ...and on the others
            await self._bus.publish_command(command)
        except BusUnavailableError as e:
            failed = JobUpdate(job_id=command.id, kind=command.kind, summary=command.summary,
                               type="error", message=e.message, status=JobStatus.FAILED,
                               error=e.to_dict())
            self.apply(failed)
            try:
                await self._bus.publish_update(failed)
            except BusUnavailableError:
                pass
            raise
        audit.record("accepted", command)
        return self.get(command.id)

    def apply(self, update: JobUpdate) -> None:
        """Fold one update into the job table. Safe to call twice with the same update."""
        record = self._records.get(update.job_id)
        if record is None:
            record = _JobRecord(Job(id=update.job_id, kind=update.kind, summary=update.summary,
                                    created_at=update.timestamp))
            self._records[update.job_id] = record
            self._evict_old_jobs()
        if update.id in record.seen:
            return
        record.seen.add(update.id)

        job = record.job
        if update.requested_by and not job.requested_by:
            job.requested_by = update.requested_by
        if update.status is not None:
            job.status = update.status
            if update.status == JobStatus.RUNNING:
                job.started_at = job.started_at or update.timestamp
                job.finished_at = None  # a retry after a finished attempt
            elif update.status in TERMINAL:
                job.finished_at = update.timestamp
            job.result = update.result
            job.error = update.error

        record.events.append(JobEvent(seq=len(record.events) + 1, timestamp=update.timestamp,
                                      type=update.type, message=update.message, data=update.data))
        for waiter in record.waiters:
            waiter.set()

    def get(self, job_id: str) -> Job:
        return self._record(job_id).job

    def list(self) -> List[Job]:
        return [record.job for record in reversed(self._records.values())]

    async def events(self, job_id: str, after: int = 0,
                     heartbeat: float = 15.0) -> AsyncIterator[Optional[JobEvent]]:
        """
        Yield a job's events in order, starting after sequence number `after`,
        and finish once the job has ended and everything has been delivered.
        Yields None when nothing happened for `heartbeat` seconds, so the
        caller can send a keep-alive.
        """
        record = self._record(job_id)
        position = max(0, after)

        while True:
            while position < len(record.events):
                yield record.events[position]
                position += 1
            if record.done:
                return

            waiter = asyncio.Event()
            record.waiters.add(waiter)
            try:
                await asyncio.wait_for(waiter.wait(), timeout=heartbeat)
            except asyncio.TimeoutError:
                yield None
            finally:
                record.waiters.discard(waiter)

    # --- Internals -------------------------------------------------------

    async def _follow_updates(self, stream: AsyncIterator[JobUpdate]) -> None:
        while True:
            try:
                async for update in stream:
                    self.apply(update)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lost the job-updates stream; reconnecting in 5 seconds")
                await asyncio.sleep(5)
            stream = self._bus.updates()

    def _record(self, job_id: str) -> _JobRecord:
        try:
            return self._records[job_id]
        except KeyError:
            raise JobNotFound(job_id) from None

    def _evict_old_jobs(self) -> None:
        for job_id in list(self._records):
            if len(self._records) <= self.max_jobs:
                break
            if self._records[job_id].done:
                del self._records[job_id]
