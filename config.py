"""
Configuration settings for the Rogue ONT Detection API
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings using Pydantic for validation
    """

    # API Settings
    api_title: str = "Rogue ONT Detection API"
    api_version: str = "1.0.0"
    api_description: str = "API for detecting rogue ONTs on Calix E7-20 GPON systems"

    # Server Settings
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = True  # Set to False in production

    # SSH Settings for Calix E7-20
    ssh_host: Optional[str] = None
    ssh_port: int = 22
    ssh_username: str = "admin"
    ssh_password: Optional[str] = None
    ssh_timeout: int = 30

    # Mock mode: simulate the E7-20 instead of opening an SSH connection.
    # For development without lab hardware. Set MOCK_MODE=true in .env
    mock_mode: bool = False

    # Target Platform Settings
    target_platform: str = "Calix E7-20 (GPON)"
    cms_version: str = "15.01.772"

    # Logging
    log_level: str = "INFO"

    class Config:
        # Load settings from .env file if it exists
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create a global settings instance
settings = Settings()
