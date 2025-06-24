"""
SSH Connection Models
Pydantic models for SSH connection and authentication
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class SSHConnectionRequest(BaseModel):
    """Model for SSH connection request"""
    host: str = Field(..., description="SSH host IP address")
    username: str = Field(..., description="SSH username")
    password: str = Field(..., description="SSH password")
    port: int = Field(22, description="SSH port number")
    timeout: int = Field(30, description="Connection timeout in seconds")


class SSHConnectionStatus(BaseModel):
    """Model for SSH connection status"""
    connected: bool = Field(..., description="Whether SSH connection is active")
    host: str = Field(..., description="Connected host")
    username: str = Field(..., description="Connected username")
    port: int = Field(..., description="Connected port")
    connected_at: Optional[datetime] = Field(None, description="Connection timestamp")
    last_command: Optional[str] = Field(None, description="Last command executed")
    error_message: Optional[str] = Field(None, description="Error message if connection failed")


class SSHCommandRequest(BaseModel):
    """Model for SSH command execution request"""
    command: str = Field(..., description="Command to execute")
    timeout: int = Field(60, description="Command execution timeout")


class SSHCommandResponse(BaseModel):
    """Model for SSH command execution response"""
    command: str = Field(..., description="Executed command")
    output: str = Field(..., description="Command output")
    error: Optional[str] = Field(None, description="Error output if any")
    exit_code: int = Field(..., description="Command exit code")
    execution_time: float = Field(..., description="Execution time in seconds")
    timestamp: datetime = Field(default_factory=datetime.now, description="Execution timestamp")
