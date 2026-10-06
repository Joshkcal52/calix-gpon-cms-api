"""
Rogue ONT Detection API
A FastAPI application for detecting rogue ONTs on Calix E7-20 GPON systems.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from typing import Dict, Any
import logging

# Import routes
from app.routes import ssh_routes, ont_routes, hp_routes

# Import configuration
from config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI application instance
app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    docs_url="/docs",  # Swagger UI documentation
    redoc_url="/redoc"  # ReDoc documentation
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
        "target_platform": settings.target_platform,
        "cms_version": settings.cms_version,
        "endpoints": {
            "ssh_management": "/api/v1/ssh",
            "ont_management": "/api/v1/onts",
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
