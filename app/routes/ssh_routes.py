"""
SSH Routes
API endpoints for SSH connection management
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from app.models.ssh_model import (
    SSHConnectionRequest, SSHConnectionStatus,
    SSHCommandRequest, SSHCommandResponse
)
from app.controller.ssh_controller import ssh_controller
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ssh", tags=["SSH Management"])


@router.post("/connect", response_model=SSHConnectionStatus)
async def connect_ssh(request: SSHConnectionRequest) -> SSHConnectionStatus:
    """
    Establish SSH connection to Calix E7-20

    This endpoint creates a new SSH connection to the specified Calix E7-20 device.
    The connection will be maintained for subsequent operations.
    """
    try:
        logger.info(f"Attempting SSH connection to {request.host}")

        status = await ssh_controller.connect(
            host=request.host,
            username=request.username,
            password=request.password,
            port=request.port,
            timeout=request.timeout
        )

        if not status.connected:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to connect: {status.error_message}"
            )

        return status

    except Exception as e:
        logger.error(f"SSH connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/disconnect")
async def disconnect_ssh() -> Dict[str, str]:
    """
    Disconnect from SSH session

    This endpoint closes the current SSH connection to the Calix E7-20 device.
    """
    try:
        success = await ssh_controller.disconnect()

        if success:
            return {"message": "SSH connection disconnected successfully"}
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to disconnect SSH connection"
            )

    except Exception as e:
        logger.error(f"SSH disconnect error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=SSHConnectionStatus)
async def get_ssh_status() -> SSHConnectionStatus:
    """
    Get current SSH connection status

    This endpoint returns the current status of the SSH connection to the Calix E7-20.
    """
    try:
        return await ssh_controller.get_connection_status()

    except Exception as e:
        logger.error(f"Error getting SSH status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/command", response_model=SSHCommandResponse)
async def execute_command(request: SSHCommandRequest) -> SSHCommandResponse:
    """
    Execute a command on the Calix E7-20

    This endpoint executes a CLI command on the connected Calix E7-20 device.
    The command output and execution details are returned.
    """
    try:
        # Check if we have an active connection
        if not await ssh_controller.is_connected():
            raise HTTPException(
                status_code=400,
                detail="No active SSH connection. Please connect first."
            )

        logger.info(f"Executing command: {request.command}")

        response = await ssh_controller.execute_command(
            command=request.command,
            timeout=request.timeout
        )

        if response.exit_code != 0:
            logger.warning(f"Command failed: {response.error}")

        return response

    except Exception as e:
        logger.error(f"Command execution error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test-connection")
async def test_connection() -> Dict[str, Any]:
    """
    Test SSH connection with a simple command

    This endpoint tests the SSH connection by executing a simple command
    to verify the connection is working properly.
    """
    try:
        if not await ssh_controller.is_connected():
            return {
                "connected": False,
                "message": "No active SSH connection"
            }

        # Execute a simple test command
        response = await ssh_controller.execute_command("show version")

        return {
            "connected": True,
            "test_command": "show version",
            "success": response.exit_code == 0,
            "output_preview": response.output[:200] + "..." if len(response.output) > 200 else response.output,
            "execution_time": response.execution_time
        }

    except Exception as e:
        logger.error(f"Connection test error: {str(e)}")
        return {
            "connected": False,
            "error": str(e)
        }
