"""
Rogue ONT Detection API
A FastAPI application for detecting rogue ONTs on Calix E7-20 GPON systems.
"""

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import asyncio
import uvicorn
from typing import Dict, Any
import logging

# Import routes
from app.routes import ssh_routes, ont_routes, hp_routes
from app.routes import provisioning_routes, job_routes, legacy_routes, lifecycle_routes, cutover_routes

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

# Add CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ssh_routes.router)
app.include_router(ont_routes.router)
app.include_router(hp_routes.router)
# Every /api/v2 route needs an API key (app/auth.py)
for v2 in (provisioning_routes, legacy_routes, lifecycle_routes, cutover_routes, job_routes):
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
        "message": "Rogue ONT Detection API",
        "version": settings.api_version,
        "status": "running",
        "target_platform": settings.target_platform,
        "cms_version": settings.cms_version
    }


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint - used to verify the API is running
    """
    return {"status": "healthy"}


@app.get("/api/v1/status")
async def api_status() -> Dict[str, Any]:
    """
    API status endpoint - provides detailed status information
    """
    return {
        "api_version": settings.api_version,
        "status": "operational",
        "mock_mode": settings.mock_mode,
        "queue": "kafka" if settings.kafka_bootstrap_servers else "in-memory",
        "target_platform": settings.target_platform,
        "cms_version": settings.cms_version,
        "endpoints": {
            "ssh_management": "/api/v1/ssh",
            "ont_management": "/api/v1/onts",
            "provisioning": "/api/v2/networks/{network}",
            "jobs": "/api/v2/jobs",
            "documentation": "/docs"
        }
    }

# This allows us to run the app directly with: python main.py
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,  # Auto-reload on code changes (development only)
        log_level=settings.log_level.lower()
    )
