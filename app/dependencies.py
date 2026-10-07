"""
Shared services for the v2 routes. They are created once when the app starts
(see lifespan in main.py) and handed to routes through FastAPI dependencies.
"""

from fastapi import Request

from app.controller.job_controller import JobStore
from app.nbi.gateway import NbiGateway


def get_gateway(request: Request) -> NbiGateway:
    return request.app.state.gateway


def get_jobs(request: Request) -> JobStore:
    return request.app.state.jobs
