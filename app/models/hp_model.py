from pydantic import BaseModel
from typing import List, Optional


class HPInterface(BaseModel):
    name: str
    total_bytes: str  # Or int, if you remove the commas
    total_frames: str  # Or int
    errors_rx: int
    drops_tx: int
    # status and speed are now optional
    status: Optional[str] = None
    speed: Optional[str] = None


class HPInterfaceList(BaseModel):
    interfaces: List[HPInterface]
