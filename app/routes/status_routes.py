"""
Status route for /api/v2: what this API process is running against.
Needs an API key like every /api/v2 route. /health stays open for probes.
"""

from typing import Any, Dict

from fastapi import APIRouter, Request

from config import settings

router = APIRouter(prefix="/api/v2", tags=["Status"])


@router.get("/status")
async def api_status(request: Request) -> Dict[str, Any]:
    return {
        "api_version": settings.api_version,
        "status": "operational",
        "mock_mode": settings.mock_mode,
        "queue": request.app.state.bus.name,
        "cms_backend": request.app.state.gateway.backend.name,
        "target_platform": settings.target_platform,
        "cms_version": settings.cms_version,
        "endpoints": {
            "provisioning": "/api/v2/networks/{network}",
            "jobs": "/api/v2/jobs",
            "cms_health": "/api/v2/nbi/health",
            "documentation": "/docs",
        },
    }
