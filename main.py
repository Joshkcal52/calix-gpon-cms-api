"""
gpon-cms
A provisioning API for legacy Calix GPON through CMS (R15.x, SOAP over HTTP).
"""

from fastapi import Depends, FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
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
from app import audit
from app.auth import ApiAuthError, WriteLimiter, check_configuration, require_api_key
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
    audit.configure(settings.audit_log_path)
    bus = build_bus(settings)
    await bus.start()
    gateway_settings, mock_e7_server = await start_mock_e7_if_needed(settings)
    gateway = build_gateway(gateway_settings)
    jobs = JobStore(bus)
    await jobs.start()

    app.state.bus = bus
    app.state.gateway = gateway
    app.state.jobs = jobs
    app.state.limiter = WriteLimiter(settings.write_requests_per_minute, settings.deletes_per_hour)
    app.state.worker = None
    app.state.mock_e7 = mock_e7_server.e7 if mock_e7_server is not None else None  # for tools/sim_signups.py

    worker_task = None
    if settings.run_embedded_worker():
        lanes = settings.worker_lane_list()
        settings.check_worker_timeouts(lanes)
        worker = Worker(
            bus, lanes, build_handlers(settings, gateway, bus),
            max_command_age=settings.command_max_age_seconds,
            max_attempts=settings.command_max_attempts)
        app.state.worker = worker
        worker_task = asyncio.create_task(worker.run(), name="embedded-worker")
    logger.info(f"Queue: {bus.name}. Backend: {gateway.backend.name}. "
                f"Embedded worker: {'yes' if worker_task else 'no'}")

    yield

    if worker_task:
        # Cancel rather than wait: a command cut short is not acknowledged,
        # so nothing is recorded as done that was not
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
    await jobs.stop()
    await gateway.close()
    if mock_e7_server is not None:
        mock_e7_server.close()
    await bus.stop()


async def start_mock_e7_if_needed(settings):
    """
    MOCK_MODE with NBI_BACKEND=e7-cli and no E7_NODES: start a mock E7 (app/nbi/mock_e7.py) on a
    free local port, and return settings pointing the gateway at it, with the server to close later.
    """
    if not (settings.mock_mode and settings.nbi_backend == "e7-cli" and not settings.e7_nodes):
        return settings, None
    from app.nbi import mock_e7
    server, port, e7 = await mock_e7.serve(0, e7=mock_e7.MockE7(pon_size=settings.mock_pon_size))
    server.e7 = e7
    logger.info(f"Mock E7 {mock_e7.NODE} listening on 127.0.0.1:{port}")
    return settings.model_copy(update={
        "e7_nodes": f"{mock_e7.NODE}=127.0.0.1:{port}", "e7_username": "e7", "e7_password": "e7",
        "e7_known_hosts": "none"}), server


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


# Codes for errors raised as plain HTTP errors (FastAPI's own 404s and 405s, and HTTPException in routes)
HTTP_ERROR_CODES = {400: "bad_request", 404: "not_found", 405: "method_not_allowed", 409: "conflict",
                    422: "invalid_request", 503: "unavailable"}


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Plain HTTP errors in the same shape as every other error, not FastAPI's {"detail": ...}"""
    error = {"code": HTTP_ERROR_CODES.get(exc.status_code, f"http_{exc.status_code}"),
             "message": str(exc.detail), "retryable": exc.status_code == 503}
    return JSONResponse(status_code=exc.status_code, content={"error": error}, headers=getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    Request validation in the same shape: {"error": {"code": "invalid_request",
    "message", "retryable": false, "details": [FastAPI's list of problems]}}
    """
    problems = exc.errors()
    summary = "; ".join(f"{'.'.join(str(part) for part in p.get('loc', ()))}: {p.get('msg')}" for p in problems[:5])
    more = f" (and {len(problems) - 5} more)" if len(problems) > 5 else ""
    return JSONResponse(status_code=422, content={"error": {
        "code": "invalid_request", "message": f"The request is not valid: {summary}{more}", "retryable": False,
        "details": jsonable_encoder(problems)}})


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
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Run the gpon-cms API")
    parser.add_argument("--backend", choices=["auto", "cms", "e7-cli"],
                        help="provision through the CMS NBI (cms) or each E7's CLI (e7-cli); overrides NBI_BACKEND")
    parser.add_argument("--mock", action="store_true",
                        help="use the simulated CMS, or with --backend e7-cli a mock E7; same as MOCK_MODE=true")
    parser.add_argument("--pon-size", type=int,
                        help="with --mock, fill one simulated PON with this many ONTs (up to 64); same as MOCK_PON_SIZE")
    args = parser.parse_args()
    # Set on the shared settings object (uvicorn imports main again in this process) and in the
    # environment (for the fresh process RELOAD=true starts)
    if args.backend:
        settings.nbi_backend = args.backend
        os.environ["NBI_BACKEND"] = args.backend
    if args.mock:
        settings.mock_mode = True
        os.environ["MOCK_MODE"] = "true"
    if args.pon_size is not None:
        settings.mock_pon_size = args.pon_size
        os.environ["MOCK_PON_SIZE"] = str(args.pon_size)
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.reload,  # RELOAD=true restarts on code changes (development only)
        log_level=settings.log_level.lower()
    )
