"""
Automation Models
Pydantic models for Ansible playbook runs. Each playbook on the allowlist has
its own variables model; unknown variables are rejected.
"""

from pydantic import BaseModel, ConfigDict, Field
from typing import Any, Dict, List, Optional

from app.models.provisioning_model import NetworkName, OntId


class BaselineAuditVars(BaseModel):
    """Variables for the baseline_audit playbook"""
    model_config = ConfigDict(extra="forbid")

    network: NetworkName
    ont_ids: List[OntId] = Field(..., min_length=1, max_length=500,
                                 description="ONTs that are expected to be online")


class PlaybookRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    variables: Dict[str, Any] = Field(default_factory=dict,
                                      description="Checked against the playbook's own model")
    limit: Optional[str] = Field(None, pattern=r"^[A-Za-z0-9_.:,!&-]{1,200}$",
                                 description="Ansible host limit pattern")


class PlaybookInfo(BaseModel):
    name: str
    description: str
    variables_schema: Dict[str, Any]
