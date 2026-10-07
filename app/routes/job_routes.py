"""
Job Routes
Follow queued work: poll a job, or stream its progress live with
Server-Sent Events (SSE).
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from typing import AsyncIterator, List, Optional
import logging

from app.controller.job_controller import JobNotFound, JobStore
from app.dependencies import get_jobs
from app.models.job_model import Job, JobAccepted

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/jobs", tags=["Jobs"])


def accepted(job: Job, request: Request) -> JobAccepted:
    """The 202 body for a queued job, with links to follow it"""
    return JobAccepted(
        job=job,
        status_url=str(request.url_for("get_job", job_id=job.id)),
        events_url=str(request.url_for("stream_job_events", job_id=job.id)))


def _find(jobs: JobStore, job_id: str) -> Job:
    try:
        return jobs.get(job_id)
    except JobNotFound:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found") from None


@router.get("/", response_model=List[Job])
async def list_jobs(jobs: JobStore = Depends(get_jobs)) -> List[Job]:
    """Recent jobs, newest first"""
    return jobs.list()


@router.get("/{job_id}", response_model=Job, name="get_job")
async def get_job(job_id: str, jobs: JobStore = Depends(get_jobs)) -> Job:
    """Status of one job, with its result or error once it has finished"""
    return _find(jobs, job_id)


@router.get("/{job_id}/events", name="stream_job_events")
async def stream_job_events(
    job_id: str,
    after: Optional[int] = Query(None, ge=0, description="Only send events after this sequence number"),
    last_event_id: Optional[int] = Header(None, description="Set by browsers when an SSE stream reconnects"),
    jobs: JobStore = Depends(get_jobs),
) -> StreamingResponse:
    """
    Live progress as Server-Sent Events.

    Replays what already happened, then sends each new event as it arrives.
    Each message has `id` (the sequence number), `event` (status, step, log,
    warning, error or result) and JSON `data`. A final `end` event carries the
    finished job, then the stream closes. A reconnecting browser sends
    `Last-Event-ID` and resumes where it left off.
    """
    _find(jobs, job_id)
    start = after if after is not None else (last_event_id or 0)

    async def stream() -> AsyncIterator[str]:
        async for event in jobs.events(job_id, after=start):
            if event is None:
                yield ": keep-alive\n\n"  # stops proxies closing a quiet stream
                continue
            yield f"id: {event.seq}\nevent: {event.type}\ndata: {event.model_dump_json()}\n\n"
        yield f"event: end\ndata: {jobs.get(job_id).model_dump_json()}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
