"""
ONT Data Models
Pydantic models for ONT (Optical Network Terminal) data structures
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class ONTStatus(str, Enum):
    """Enumeration for ONT status values"""
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class OpticalLevels(BaseModel):
    """Model for optical power levels"""
    tx_power: Optional[float] = Field(None, description="Transmit power in dBm")
    rx_power: Optional[float] = Field(None, description="Receive power in dBm")
    temperature: Optional[float] = Field(None, description="Temperature in Celsius")


class PerformanceMetrics(BaseModel):
    """Model for ONT performance metrics"""
    missed_bursts: Optional[int] = Field(None, description="Number of missed bursts")
    bip_errors: Optional[int] = Field(None, description="BIP (Bit Interleaved Parity) errors")
    crc_errors: Optional[int] = Field(None, description="CRC (Cyclic Redundancy Check) errors")
    fec_errors: Optional[int] = Field(None, description="Forward Error Correction errors")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp of measurement")


class ONTData(BaseModel):
    """Complete ONT data model"""
    ont_id: str = Field(..., description="ONT identifier")
    serial_number: Optional[str] = Field(None, description="ONT serial number")
    status: ONTStatus = Field(ONTStatus.UNKNOWN, description="Current ONT status")
    pon_port: Optional[str] = Field(None, description="PON port number")
    optical_levels: Optional[OpticalLevels] = Field(None, description="Optical power levels")
    performance_metrics: Optional[PerformanceMetrics] = Field(None, description="Performance metrics")
    description: Optional[str] = Field(None, description="ONT description")
    distance: Optional[float] = Field(None, description="Distance in kilometers")
    last_updated: datetime = Field(default_factory=datetime.now, description="Last update timestamp")


class ONTList(BaseModel):
    """Model for list of ONTs"""
    onts: List[ONTData] = Field(..., description="List of ONT data")
    total_count: int = Field(..., description="Total number of ONTs")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp of data collection")


class PONStatus(BaseModel):
    """Model for PON port status"""
    pon_port: str = Field(..., description="PON port identifier")
    status: str = Field(..., description="PON port status")
    total_onts: int = Field(..., description="Total ONTs on this PON")
    online_onts: int = Field(..., description="Number of online ONTs")
    utilization_percent: float = Field(..., description="PON utilization percentage")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp of measurement")


class RogueAnalysisRequest(BaseModel):
    """Model for rogue ONT analysis request"""
    pon_ports: Optional[List[str]] = Field(None, description="Specific PON ports to analyze")
    threshold_missed_bursts: int = Field(100, description="Threshold for missed bursts")
    threshold_bip_errors: int = Field(50, description="Threshold for BIP errors")
    threshold_crc_errors: int = Field(10, description="Threshold for CRC errors")
    include_offline: bool = Field(False, description="Include offline ONTs in analysis")


class RogueAnalysisResult(BaseModel):
    """Model for rogue ONT analysis results"""
    suspicious_onts: List[ONTData] = Field(..., description="List of suspicious ONTs")
    analysis_criteria: RogueAnalysisRequest = Field(..., description="Criteria used for analysis")
    total_analyzed: int = Field(..., description="Total ONTs analyzed")
    timestamp: datetime = Field(default_factory=datetime.now, description="Analysis timestamp")
