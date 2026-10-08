"""
Configuration settings for gpon-cms
"""

import os
from pathlib import Path
from typing import FrozenSet, List, Literal, Optional, Union
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings using Pydantic for validation
    """

    # API Settings
    api_title: str = "gpon-cms"
    api_version: str = "1.0.0"
    api_description: str = "Provisioning API for legacy Calix GPON through CMS (R15.x NBI)"

    # Server Settings (python main.py)
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False  # RELOAD=true restarts on code changes; development only

    # Browser origins allowed to call the API (CORS), comma-separated, e.g.
    # https://portal.example.internal. Empty (the default) allows none.
    cors_allow_origins: str = ""

    # API keys for every /api/v2 route, comma-separated. Each entry is a key, or name:key
    # (the name is logged with each request). See app/auth.py.
    api_keys: Optional[str] = None

    # Which backend provisions: cms (the CMS northbound interface, CMS_HOST), e7-cli (each E7's command line
    # over SSH, E7_NODES), or auto: whichever of the two is set (an error if both are). With both configured,
    # pick one here or at startup: python main.py --backend cms|e7-cli
    nbi_backend: Literal["auto", "cms", "e7-cli"] = "auto"

    # Mock mode: a simulated CMS (app/nbi/mock_cms.py), or with NBI_BACKEND=e7-cli a mock E7
    # (app/nbi/mock_e7.py) that python main.py starts itself. For development without CMS or E7 access.
    # Set MOCK_MODE=true in .env, or python main.py --mock
    mock_mode: bool = False
    # Mock mode only: fill one PON of the simulated CMS (1/1/3) or the mock E7 (1/1/1) with this many
    # discovered ONTs (up to 64), for trying the API at scale. See app/nbi/mock_pon.py. 0: the usual small network
    mock_pon_size: int = 0

    # --- CMS northbound interface (the fast lane) ---
    # The real CMS, reached through app/nbi/client.py. Ignored when MOCK_MODE is on.
    cms_host: Optional[str] = None
    # http or https. Plain HTTP sends the CMS login unencrypted: use https wherever CMS offers it.
    cms_scheme: Literal["http", "https"] = "http"
    cms_port: int = 18080  # the NBI port for the scheme above; ask your CMS admin for the HTTPS port
    # HTTPS only: check the CMS server's certificate (on unless set to false), against
    # CMS_CA_FILE if set (for a private CA or a self-signed CMS), else the system's CAs.
    cms_verify_tls: bool = True
    cms_ca_file: Optional[str] = None
    cms_username: str = "nbi"
    cms_password: Optional[str] = None
    # UNVERIFIED: how CMS addresses the router (RG) side of ONTs such as the 844G.
    # Leave empty until checked against your CMS; see app/nbi/router_side.py.
    cms_rg_service_slot: Optional[str] = None
    cms_rg_port_type: Optional[str] = None
    cms_rg_port_id_element: Optional[str] = None
    cms_rg_port_mode_element: Optional[str] = None  # read-only: an Ethernet port's bridge/router mode
    # The bandwidth profiles a service may use, comma-separated, exactly as CMS names them (numeric IDs
    # on a real CMS), e.g. BANDWIDTH_PROFILES=17,18,22. Anything else is refused before a job is queued,
    # so a typo cannot reach CMS mid-job or pick a valid but wrong speed tier. Empty: not checked here
    # (the simulated CMS's own list in MOCK_MODE). This API cannot list them from CMS.
    bandwidth_profiles: Optional[str] = None

    # --- E7 command line (for a CMS without the NBI licence) ---
    # Provision through each E7 EXA node's own CLI over SSH instead of CMS. See app/nbi/e7_backend.py.
    # NAME=host[:port],... Each NAME is a network in the API's URLs. Not used together with CMS_HOST.
    e7_nodes: Optional[str] = None
    e7_username: str = ""
    e7_password: Optional[str] = None
    # SSH host keys are checked against ~/.ssh/known_hosts unless this names another file.
    # "none" turns the check off (a test lab only: anyone on the path could pose as the E7).
    e7_known_hosts: Optional[str] = None
    # UNVERIFIED against a real E7: Calix documents none of these four. Check them with
    # python -m app.tools.e7_capture before provisioning.
    e7_prompt_pattern: str = r"\S+[>#] ?"  # the whole last line of output when the E7 is waiting
    e7_pager_command: str = "set session pager disabled"
    e7_session_commands: str = "set session timeout disabled,set session alarm-notif disabled,set session event-notif disabled,set session tca-notif disabled"
    e7_error_pattern: str = (r"(?i)(\berror\b|invalid input|\binvalid\b|\bfail(?:ed|ure|s)?\b|cannot|can't|not allowed|denied|"
                             r"does not exist|doesn't exist|not found|no such|already exists|in use|unknown)")
    # Service names, in service_index order: show eth-svc-name on the E7 lists the real ones
    e7_service_names: str = "Data1,Data2,Data3,Data4,Data5,Data6,Data7,Data8"

    nbi_timeout_seconds: float = 10.0  # per CMS (or E7) call; 30 or more for E7, whose calls run several commands
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
    kafka_topic_prefix: str = "gpon-cms"
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
    audit_log_path: Optional[str] = None  # also append the audit trail (app/audit.py) to this file, as JSON lines

    def cms_verify(self) -> Union[bool, str]:
        """What the CMS client checks the server's certificate against: a CA file, the system's CAs, or nothing"""
        if not self.cms_verify_tls:
            return False
        return self.cms_ca_file or True

    def known_bandwidth_profiles(self) -> Optional[FrozenSet[str]]:
        """The bandwidth profiles services may use, or None if they are not checked here"""
        listed = frozenset(p.strip() for p in (self.bandwidth_profiles or "").split(",") if p.strip())
        if listed:
            return listed
        if self.mock_mode:  # the list of the mock in use: the simulated CMS or the mock E7, and a filled PON's extra
            from app.nbi.mock_cms import MOCK_BANDWIDTH_PROFILES
            from app.nbi.mock_e7 import BW_PROFILES
            from app.nbi.mock_pon import EXTRA_BW_PROFILES
            mock = BW_PROFILES if self.nbi_backend == "e7-cli" else MOCK_BANDWIDTH_PROFILES
            return frozenset(mock + (EXTRA_BW_PROFILES if self.mock_pon_size else ()))
        return None

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
        from app.worker.runner import HISTORY_TIMEOUT  # an activation may read its job's history first
        slowest = self.activation_verify_timeout_seconds + 6 * self.nbi_timeout_seconds + HISTORY_TIMEOUT
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
