"""
Configuration settings for the Rogue ONT Detection API
"""

import os
from pathlib import Path
from typing import List, Optional
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

    # API keys for every /api/v2 route, comma-separated. Each entry is a key, or name:key
    # (the name is logged with each request). See app/auth.py.
    api_keys: Optional[str] = None

    # Mock mode: simulate the E7-20 instead of opening an SSH connection.
    # For development without lab hardware. Set MOCK_MODE=true in .env
    mock_mode: bool = False

    # --- CMS northbound interface (the fast lane) ---
    # The real CMS, reached with cms-nbi-client over plain HTTP. Ignored when MOCK_MODE is on.
    cms_host: Optional[str] = None
    cms_port: int = 18080
    cms_username: str = "nbi"
    cms_password: Optional[str] = None
    # UNVERIFIED: how CMS addresses the router (RG) side of ONTs such as the 844G.
    # Leave empty until checked against your CMS; see app/nbi/router_side.py.
    cms_rg_service_slot: Optional[str] = None
    cms_rg_port_type: Optional[str] = None
    cms_rg_port_id_element: Optional[str] = None
    cms_rg_port_mode_element: Optional[str] = None  # read-only: an Ethernet port's bridge/router mode
    nbi_timeout_seconds: float = 10.0  # per CMS call
    nbi_max_retries: int = 2  # extra attempts for reads; writes are retried by the worker
    nbi_max_concurrency: int = 8  # CMS calls in flight per process
    nbi_breaker_failure_threshold: int = 5  # consecutive failures that pause CMS calls
    nbi_breaker_recovery_seconds: float = 30.0  # how long calls stay paused

    # ONT activation
    activation_verify_timeout_seconds: float = 120.0  # wait for the ONT to come online
    activation_poll_seconds: float = 2.0

    # --- Queue ---
    # Leave empty to queue inside the API process (single process, nothing
    # survives a restart). Set to use Kafka, e.g. KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic_prefix: str = "rogue"
    # Must be longer than the slowest command, or Kafka takes the partition away mid-task
    kafka_max_poll_interval_seconds: float = 900.0
    kafka_session_timeout_seconds: float = 30.0  # how long before a dead worker's work is reassigned
    command_max_age_seconds: float = 3600.0  # older queued commands are failed, not run

    # --- Legacy GPON layer (signup and GPON-to-XGS cutover) ---
    legacy_events_topic: str = "legacy-gpon-events"  # consumed: signup and cutover events
    # Produced: migration requests for the XGS side. Set this to your existing
    # XGS/SMx topic only once XgsMigrationRequest matches that topic's schema.
    xgs_migration_topic: str = "xgs-migration-requests"
    legacy_profile_map_path: str = str(Path(__file__).resolve().parent / "app" / "legacy" / "profile_map.yml")
    # Mapping for SMx cutover plans (dry run, nothing is sent): app/legacy/smx.py
    smx_map_path: str = str(Path(__file__).resolve().parent / "app" / "legacy" / "smx_map.yml")

    # --- Workers ---
    # Run a worker inside the API process. Default: yes without Kafka, no with it
    # (with Kafka, workers run as their own pods: python -m app.worker).
    worker_embedded: Optional[bool] = None
    worker_lanes: str = "provisioning,legacy"
    worker_heartbeat_file: Optional[str] = None  # touched every 10s for a liveness probe

    # Target Platform Settings
    target_platform: str = "Calix E7-20 (GPON)"
    cms_version: str = "15.01.772"

    # Logging
    log_level: str = "INFO"

    def run_embedded_worker(self) -> bool:
        if self.worker_embedded is not None:
            return self.worker_embedded
        return not self.kafka_bootstrap_servers

    def worker_lane_list(self) -> List[str]:
        lanes = [lane.strip() for lane in self.worker_lanes.split(",") if lane.strip()]
        unknown = set(lanes) - {"provisioning", "legacy"}
        if unknown or not lanes:
            raise ValueError(f"WORKER_LANES must list provisioning and/or legacy, "
                             f"got '{self.worker_lanes}'")
        return lanes

    def check_worker_timeouts(self, lanes: List[str]) -> None:
        """Refuse to start a worker whose slowest command could outlast Kafka's patience"""
        slowest = self.activation_verify_timeout_seconds + 6 * self.nbi_timeout_seconds
        if self.kafka_bootstrap_servers and slowest + 60 > self.kafka_max_poll_interval_seconds:
            raise ValueError(
                f"KAFKA_MAX_POLL_INTERVAL_SECONDS ({self.kafka_max_poll_interval_seconds:g}) must be at least "
                f"60 seconds longer than the slowest command ({slowest:g} seconds). Otherwise Kafka reassigns "
                f"the partition mid-task and the command runs twice.")

    class Config:
        # Load settings from .env file if it exists
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create a global settings instance
settings = Settings()
