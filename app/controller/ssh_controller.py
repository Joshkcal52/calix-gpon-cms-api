"""
SSH Service
Handles SSH connections to Calix E7-20 using Netmiko
"""

import logging
from typing import Optional, Dict, Any
from netmiko import ConnectHandler, NetMikoTimeoutException, NetMikoAuthenticationException
from paramiko import SSHException
from app.models.ssh_model import SSHConnectionStatus, SSHCommandResponse
from config import settings
from datetime import datetime
import time

logger = logging.getLogger(__name__)


class SSHController:
    """Service for managing SSH connections to Calix E7-20"""

    def __init__(self):
        self.connection = None
        self.connection_status = SSHConnectionStatus(
            connected=False,
            host="",
            username="",
            port=22
        )

    async def connect(self, host: str, username: str, password: str, port: int = 22, timeout: int = 30) -> SSHConnectionStatus:
        """
        Establish SSH connection to Calix E7-20

        Args:
            host: Target host IP address
            username: SSH username
            password: SSH password
            port: SSH port (default: 22)
            timeout: Connection timeout in seconds

        Returns:
            SSHConnectionStatus: Connection status information
        """
        try:
            # Disconnect existing connection if any
            if self.connection:
                await self.disconnect()

            # Configure connection parameters for Calix E7-20
            device_config = {
                'device_type': 'hp_procurve',  # Use the HP ProCurve device type
                'host': host,
                'username': username,
                'password': password,
                'port': port,
                'timeout': timeout,
                'global_delay_factor': 2,
                'fast_cli': False,
                'session_log': None
            }

            logger.info(f"Attempting SSH connection to {host}:{port}")

            # Establish connection
            self.connection = ConnectHandler(**device_config)

            # Test connection with a simple command
            output = self.connection.send_command("show version", expect_string=r"#")

            # Update connection status
            self.connection_status = SSHConnectionStatus(
                connected=True,
                host=host,
                username=username,
                port=port,
                connected_at=datetime.now(),
                last_command="show version"
            )

            logger.info(f"Successfully connected to {host}")
            return self.connection_status

        except NetMikoAuthenticationException as e:
            error_msg = f"Authentication failed for {host}: {str(e)}"
            logger.error(error_msg)
            self.connection_status.error_message = error_msg
            return self.connection_status

        except NetMikoTimeoutException as e:
            error_msg = f"Connection timeout to {host}: {str(e)}"
            logger.error(error_msg)
            self.connection_status.error_message = error_msg
            return self.connection_status

        except SSHException as e:
            error_msg = f"SSH error connecting to {host}: {str(e)}"
            logger.error(error_msg)
            self.connection_status.error_message = error_msg
            return self.connection_status

        except Exception as e:
            error_msg = f"Unexpected error connecting to {host}: {str(e)}"
            logger.error(error_msg)
            self.connection_status.error_message = error_msg
            return self.connection_status

    async def disconnect(self) -> bool:
        """
        Disconnect from SSH session

        Returns:
            bool: True if disconnected successfully
        """
        try:
            if self.connection:
                self.connection.disconnect()
                self.connection = None
                self.connection_status.connected = False
                logger.info("SSH connection disconnected")
                return True
            return True
        except Exception as e:
            logger.error(f"Error disconnecting SSH: {str(e)}")
            return False

    async def execute_command(self, command: str, timeout: int = 60) -> SSHCommandResponse:
        """
        Execute a command on the Calix E7-20

        Args:
            command: Command to execute
            timeout: Command timeout in seconds

        Returns:
            SSHCommandResponse: Command execution results
        """
        if not self.connection or not self.connection_status.connected:
            return SSHCommandResponse(
                command=command,
                output="",
                error="No active SSH connection",
                exit_code=1,
                execution_time=0.0
            )

        start_time = time.time()

        try:
            logger.info(f"Executing command: {command}")

            # Execute command with timeout
            output = self.connection.send_command(
                command,
                expect_string=r"#",
                delay_factor=2
            )

            execution_time = time.time() - start_time

            # Update last command in status
            self.connection_status.last_command = command

            logger.info(f"Command executed successfully in {execution_time:.2f}s")

            return SSHCommandResponse(
                command=command,
                output=output,
                error=None,
                exit_code=0,
                execution_time=execution_time
            )

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Error executing command '{command}': {str(e)}"
            logger.error(error_msg)

            return SSHCommandResponse(
                command=command,
                output="",
                error=error_msg,
                exit_code=1,
                execution_time=execution_time
            )

    async def get_connection_status(self) -> SSHConnectionStatus:
        """
        Get current connection status

        Returns:
            SSHConnectionStatus: Current connection status
        """
        return self.connection_status

    async def is_connected(self) -> bool:
        """
        Check if SSH connection is active

        Returns:
            bool: True if connected
        """
        return self.connection_status.connected and self.connection is not None


# Global SSH service instance
ssh_controller = SSHController()
