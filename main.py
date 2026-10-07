"""
gpon-cms
A provisioning API for legacy Calix GPON through CMS (R15.x, SOAP over HTTP).
"""

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
import uvicorn
from typing import Dict, List
import logging

# Import routes
from app.routes import (
    provisioning_routes, job_routes, legacy_routes, lifecycle_routes, cutover_routes, status_routes
)

# Import services for the v2 integration layer
from app.auth import ApiAuthError, check_configuration, require_api_key
from app.controller.job_controller import JobStore
from app.messaging.bus import BusUnavailableError, build_bus
from app.nbi.errors import NbiError
from app.nbi.gateway import build_gateway
from app.worker.handlers import build_handlers
from app.worker.runner import Worker

# Import configuration
from config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start and stop the services behind the v2 routes:
    the CMS gateway, the message bus, the job store and (unless workers run
    as separate pods) an embedded worker.
    """
    check_configuration(settings)
    bus = build_bus(settings)
    await bus.start()
    gateway = build_gateway(settings)
    jobs = JobStore(bus)
    await jobs.start()

    app.state.bus = bus
    app.state.gateway = gateway
    app.state.jobs = jobs
    app.state.worker = None

    worker_task = None
    if settings.run_embedded_worker():
        lanes = settings.worker_lane_list()
        settings.check_worker_timeouts(lanes)
        worker = Worker(
            bus, lanes, build_handlers(settings, gateway, bus),
            max_command_age=settings.command_max_age_seconds)
        app.state.worker = worker
        worker_task = asyncio.create_task(worker.run(), name="embedded-worker")
    logger.info(f"Queue: {bus.name}. CMS backend: {gateway.backend.name}. "
                f"Embedded worker: {'yes' if worker_task else 'no'}")

    yield

    if worker_task:
        # Cancel rather than wait: a command cut short is not acknowledged,
        # so nothing is recorded as done that was not
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
    await jobs.stop()
    await gateway.close()
    await bus.stop()


# Create FastAPI application instance
app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    docs_url="/docs",  # Swagger UI documentation
    redoc_url="/redoc",  # ReDoc documentation
    lifespan=lifespan
)


def cors_origins(value: str) -> List[str]:
    """CORS_ALLOW_ORIGINS, comma-separated, as a list. Empty: no origin is allowed."""
    origins = [origin.strip() for origin in (value or "").split(",") if origin.strip()]
    if "*" in origins:
        raise ValueError("CORS_ALLOW_ORIGINS must list each origin; '*' is not accepted")
    return origins


def configure_cors(app: FastAPI, value: str) -> None:
    """
    Allow browser pages from the listed origins to call the API. With none
    listed, no CORS headers are sent and browsers block cross-origin calls.
    The API key travels in a header, so cookies (credentials) are not allowed.
    """
    origins = cors_origins(value)
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=False,
            allow_methods=["GET", "POST", "PATCH", "DELETE"],
            allow_headers=["Authorization", "X-API-Key", "Content-Type", "Last-Event-ID"],
        )


configure_cors(app, settings.cors_allow_origins)

# Every /api/v2 route needs an API key (app/auth.py)
for v2 in (provisioning_routes, legacy_routes, lifecycle_routes, cutover_routes, job_routes, status_routes):
    app.include_router(v2.router, dependencies=[Depends(require_api_key)])


@app.exception_handler(NbiError)
async def nbi_error_handler(request: Request, exc: NbiError) -> JSONResponse:
    """
    Every CMS failure reaches the client in one shape:
    {"error": {"code", "message", "retryable", ...}} with a fitting status,
    plus Retry-After when the circuit breaker says when to come back.
    """
    headers = {}
    if exc.retry_after is not None:
        headers["Retry-After"] = str(max(1, round(exc.retry_after)))
    return JSONResponse(status_code=exc.http_status, content={"error": exc.to_dict()}, headers=headers)


@app.exception_handler(ApiAuthError)
async def auth_error_handler(request: Request, exc: ApiAuthError) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if exc.http_status == 401 else {}
    return JSONResponse(status_code=exc.http_status, content={"error": exc.to_dict()}, headers=headers)


@app.exception_handler(BusUnavailableError)
async def bus_error_handler(request: Request, exc: BusUnavailableError) -> JSONResponse:
    """The queue did not accept the command, so nothing was queued"""
    return JSONResponse(status_code=exc.http_status, content={"error": exc.to_dict()},
                        headers={"Retry-After": "5"})


@app.get("/")
async def root() -> Dict[str, str]:
    """
    Root endpoint - provides basic API information
    """
    return {
        "message": "gpon-cms: provisioning API for Calix CMS",
        "version": settings.api_version,
        "docs": "/docs",
    }


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint - used to verify the API is running
    """
    return {"status": "healthy"}

# This allows us to run the app directly with: python main.py
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,  # RELOAD=true restarts on code changes (development only)
        log_level=settings.log_level.lower()
    )
