"""
ONT Routes
API endpoints for ONT data management and rogue ONT detection
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from app.models.ont_model import (
    ONTData, ONTList, PONStatus, RogueAnalysisRequest,
    RogueAnalysisResult, PerformanceMetrics
)
from app.controller.ont_controller import ont_controller
from app.controller.ssh_controller import ssh_controller
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/onts", tags=["ONT Management"])


@router.get("/", response_model=ONTList)
async def get_all_onts() -> ONTList:
    """
    Get all ONTs from the Calix E7-20

    This endpoint retrieves information about all ONTs connected to the system.
    Requires an active SSH connection.
    """
    try:
        # Check if we have an active SSH connection
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info("Retrieving all ONT information")

        return await ont_controller.get_all_onts()

    except Exception as e:
        logger.error(f"Error getting ONT list: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ont_id}", response_model=ONTData)
async def get_ont_by_id(ont_id: str) -> ONTData:
    """
    Get specific ONT by ID

    This endpoint retrieves detailed information about a specific ONT.
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info(f"Retrieving ONT information for ID: {ont_id}")

        ont_data = await ont_controller.get_ont_by_id(ont_id)

        if not ont_data:
            raise HTTPException(
                status_code=404,
                detail=f"ONT with ID {ont_id} not found"
            )

        return ont_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting ONT {ont_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{ont_id}/performance", response_model=PerformanceMetrics)
async def get_ont_performance(ont_id: str) -> PerformanceMetrics:
    """
    Get performance metrics for a specific ONT

    This endpoint retrieves detailed performance metrics including
    missed bursts, BIP errors, CRC errors, and FEC errors.
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info(f"Retrieving performance metrics for ONT: {ont_id}")

        metrics = await ont_controller.get_ont_performance_metrics(ont_id)

        if not metrics:
            raise HTTPException(
                status_code=404,
                detail=f"Performance metrics not found for ONT {ont_id}"
            )

        return metrics

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting performance metrics for ONT {ont_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pon/status", response_model=List[PONStatus])
async def get_pon_status(
    pon_port: Optional[str] = Query(None, description="Specific PON port to query")
) -> List[PONStatus]:
    """
    Get PON port status information

    This endpoint retrieves status information for PON ports.
    If no specific port is provided, returns status for all PON ports.
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info(f"Retrieving PON status for port: {pon_port or 'all'}")

        return await ont_controller.get_pon_status(pon_port)

    except Exception as e:
        logger.error(f"Error getting PON status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze-rogue", response_model=RogueAnalysisResult)
async def analyze_rogue_onts(request: RogueAnalysisRequest) -> RogueAnalysisResult:
    """
    Analyze ONTs for rogue behavior

    This endpoint analyzes ONTs based on specified criteria to identify
    potentially rogue or misbehaving ONTs. The analysis considers:
    - Missed bursts threshold
    - BIP errors threshold
    - CRC errors threshold
    - PON port filtering
    - Online/offline status filtering
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info("Starting rogue ONT analysis")

        result = await ont_controller.analyze_rogue_onts(request)

        logger.info(f"Analysis complete. Found {len(result.suspicious_onts)} suspicious ONTs")

        return result

    except Exception as e:
        logger.error(f"Error analyzing rogue ONTs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=Dict[str, Any])
async def get_ont_summary() -> Dict[str, Any]:
    """
    Get ONT summary statistics

    This endpoint provides a summary of ONT statistics including:
    - Total ONT count
    - Online/offline counts
    - PON utilization
    - Recent performance issues
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info("Retrieving ONT summary")

        # Get all ONTs
        ont_list = await ont_controller.get_all_onts()

        # Calculate summary statistics
        total_onts = len(ont_list.onts)
        online_onts = len([ont for ont in ont_list.onts if ont.status.value == "online"])
        offline_onts = len([ont for ont in ont_list.onts if ont.status.value == "offline"])

        # Group by PON port
        pon_groups = {}
        for ont in ont_list.onts:
            if ont.pon_port:
                if ont.pon_port not in pon_groups:
                    pon_groups[ont.pon_port] = []
                pon_groups[ont.pon_port].append(ont)

        # Calculate PON utilization
        pon_utilization = {}
        for pon_port, onts in pon_groups.items():
            online_count = len([ont for ont in onts if ont.status.value == "online"])
            utilization = (online_count / len(onts)) * 100 if onts else 0
            pon_utilization[pon_port] = {
                "total_onts": len(onts),
                "online_onts": online_count,
                "utilization_percent": round(utilization, 2)
            }

        return {
            "total_onts": total_onts,
            "online_onts": online_onts,
            "offline_onts": offline_onts,
            "online_percentage": round((online_onts / total_onts) * 100, 2) if total_onts > 0 else 0,
            "pon_utilization": pon_utilization,
            "total_pon_ports": len(pon_groups),
            "timestamp": ont_list.timestamp
        }

    except Exception as e:
        logger.error(f"Error getting ONT summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", response_model=List[ONTData])
async def search_onts(
    status: Optional[str] = Query(None, description="Filter by ONT status"),
    pon_port: Optional[str] = Query(None, description="Filter by PON port"),
    description: Optional[str] = Query(None, description="Search in ONT description")
) -> List[ONTData]:
    """
    Search and filter ONTs

    This endpoint allows searching and filtering ONTs based on various criteria.
    """
    try:
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info("Searching ONTs with filters")

        # Get all ONTs
        ont_list = await ont_controller.get_all_onts()

        # Apply filters
        filtered_onts = ont_list.onts

        if status:
            filtered_onts = [ont for ont in filtered_onts if ont.status.value == status.lower()]

        if pon_port:
            filtered_onts = [ont for ont in filtered_onts if ont.pon_port == pon_port]

        if description:
            filtered_onts = [ont for ont in filtered_onts
                             if ont.description and description.lower() in ont.description.lower()]

        return filtered_onts

    except Exception as e:
        logger.error(f"Error searching ONTs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
