"""
ONT Service
Handles ONT data collection and analysis for rogue ONT detection
"""

import logging
import re
from typing import List, Optional, Dict, Any
from app.models.ont_model import (
    ONTData, ONTList, PONStatus, RogueAnalysisRequest,
    RogueAnalysisResult, ONTStatus, OpticalLevels, PerformanceMetrics
)
from app.controller.ssh_controller import ssh_controller
from datetime import datetime
import statistics

logger = logging.getLogger(__name__)


class ONTController:
    """Service for ONT data collection and analysis"""

    def __init__(self):
        self.cache = {}  # Simple cache for ONT data
        self.cache_timeout = 300  # 5 minutes cache timeout

    async def get_all_onts(self) -> ONTList:
        """
        Get all ONTs from the Calix E7-20

        Returns:
            ONTList: List of all ONTs with their data
        """
        try:
            # Check if we have an active SSH connection
            if not await ssh_controller.is_connected():
                raise Exception("No active SSH connection")

            # Execute command to get ONT information
            # Note: This command may need adjustment based on actual Calix CLI
            command = "show ont info"
            response = await ssh_controller.execute_command(command)

            if response.exit_code != 0:
                raise Exception(f"Failed to get ONT info: {response.error}")

            # Parse the output to extract ONT data
            onts = await self._parse_ont_output(response.output)

            return ONTList(
                onts=onts,
                total_count=len(onts)
            )

        except Exception as e:
            logger.error(f"Error getting ONT data: {str(e)}")
            raise

    async def get_ont_by_id(self, ont_id: str) -> Optional[ONTData]:
        """
        Get specific ONT by ID

        Args:
            ont_id: ONT identifier

        Returns:
            ONTData: ONT data if found, None otherwise
        """
        try:
            # Check cache first
            if ont_id in self.cache:
                cached_data = self.cache[ont_id]
                if (datetime.now() - cached_data['timestamp']).seconds < self.cache_timeout:
                    return cached_data['data']

            # Get specific ONT information
            command = f"show ont info {ont_id}"
            response = await ssh_controller.execute_command(command)

            if response.exit_code != 0:
                logger.warning(f"Failed to get ONT {ont_id}: {response.error}")
                return None

            # Parse single ONT output
            ont_data = await self._parse_single_ont_output(response.output, ont_id)

            if ont_data:
                # Cache the result
                self.cache[ont_id] = {
                    'data': ont_data,
                    'timestamp': datetime.now()
                }

            return ont_data

        except Exception as e:
            logger.error(f"Error getting ONT {ont_id}: {str(e)}")
            return None

    async def get_pon_status(self, pon_port: Optional[str] = None) -> List[PONStatus]:
        """
        Get PON port status information

        Args:
            pon_port: Specific PON port (optional)

        Returns:
            List[PONStatus]: PON status information
        """
        try:
            if not await ssh_controller.is_connected():
                raise Exception("No active SSH connection")

            # Get PON status information
            command = "show pon port status"
            if pon_port:
                command += f" {pon_port}"

            response = await ssh_controller.execute_command(command)

            if response.exit_code != 0:
                raise Exception(f"Failed to get PON status: {response.error}")

            # Parse PON status output
            pon_status_list = await self._parse_pon_status_output(response.output)

            return pon_status_list

        except Exception as e:
            logger.error(f"Error getting PON status: {str(e)}")
            raise

    async def analyze_rogue_onts(self, request: RogueAnalysisRequest) -> RogueAnalysisResult:
        """
        Analyze ONTs for rogue behavior

        Args:
            request: Analysis criteria and parameters

        Returns:
            RogueAnalysisResult: Analysis results with suspicious ONTs
        """
        try:
            # Get all ONTs
            ont_list = await self.get_all_onts()

            # Filter ONTs based on request criteria
            onts_to_analyze = ont_list.onts

            if request.pon_ports:
                onts_to_analyze = [ont for ont in onts_to_analyze
                                   if ont.pon_port in request.pon_ports]

            if not request.include_offline:
                onts_to_analyze = [ont for ont in onts_to_analyze
                                   if ont.status == ONTStatus.ONLINE]

            # Analyze for rogue behavior
            suspicious_onts = []

            for ont in onts_to_analyze:
                if await self._is_ont_suspicious(ont, request):
                    suspicious_onts.append(ont)

            return RogueAnalysisResult(
                suspicious_onts=suspicious_onts,
                analysis_criteria=request,
                total_analyzed=len(onts_to_analyze)
            )

        except Exception as e:
            logger.error(f"Error analyzing rogue ONTs: {str(e)}")
            raise

    async def _parse_ont_output(self, output: str) -> List[ONTData]:
        """
        Parse ONT information from CLI output

        Args:
            output: Raw CLI output

        Returns:
            List[ONTData]: Parsed ONT data
        """
        onts = []

        # This is a simplified parser - you'll need to adjust based on actual Calix output format
        lines = output.strip().split('\n')

        for line in lines:
            # Example parsing logic - adjust based on actual output format
            if 'ONT' in line and 'online' in line.lower():
                # Extract ONT ID, status, and other information
                # This is a placeholder - implement based on actual output
                ont_data = ONTData(
                    ont_id="placeholder_id",
                    status=ONTStatus.ONLINE,
                    pon_port="1/1/1:1"
                )
                onts.append(ont_data)

        return onts

    async def _parse_single_ont_output(self, output: str, ont_id: str) -> Optional[ONTData]:
        """
        Parse single ONT information from CLI output

        Args:
            output: Raw CLI output
            ont_id: ONT identifier

        Returns:
            ONTData: Parsed ONT data
        """
        # Implement parsing logic for single ONT output
        # This is a placeholder - implement based on actual Calix output format
        return ONTData(
            ont_id=ont_id,
            status=ONTStatus.ONLINE,
            pon_port="1/1/1:1"
        )

    async def _parse_pon_status_output(self, output: str) -> List[PONStatus]:
        """
        Parse PON status from CLI output

        Args:
            output: Raw CLI output

        Returns:
            List[PONStatus]: Parsed PON status data
        """
        pon_status_list = []

        # This is a simplified parser - adjust based on actual Calix output format
        lines = output.strip().split('\n')

        for line in lines:
            # Example parsing logic - implement based on actual output
            if 'PON' in line and 'port' in line.lower():
                # Extract PON port information
                # This is a placeholder
                pon_status = PONStatus(
                    pon_port="1/1/1:1",
                    status="up",
                    total_onts=32,
                    online_onts=28,
                    utilization_percent=87.5
                )
                pon_status_list.append(pon_status)

        return pon_status_list

    async def _is_ont_suspicious(self, ont: ONTData, criteria: RogueAnalysisRequest) -> bool:
        """
        Determine if an ONT is suspicious based on analysis criteria

        Args:
            ont: ONT data to analyze
            criteria: Analysis criteria

        Returns:
            bool: True if ONT is suspicious
        """
        if not ont.performance_metrics:
            return False

        metrics = ont.performance_metrics

        # Check if ONT exceeds any thresholds
        if (metrics.missed_bursts and
                metrics.missed_bursts > criteria.threshold_missed_bursts):
            return True

        if (metrics.bip_errors and
                metrics.bip_errors > criteria.threshold_bip_errors):
            return True

        if (metrics.crc_errors and
                metrics.crc_errors > criteria.threshold_crc_errors):
            return True

        return False

    async def get_ont_performance_metrics(self, ont_id: str) -> Optional[PerformanceMetrics]:
        """
        Get detailed performance metrics for a specific ONT

        Args:
            ont_id: ONT identifier

        Returns:
            PerformanceMetrics: Performance data
        """
        try:
            # Execute command to get performance metrics
            command = f"show ont performance {ont_id}"
            response = await ssh_controller.execute_command(command)

            if response.exit_code != 0:
                logger.warning(f"Failed to get performance metrics for ONT {ont_id}: {response.error}")
                return None

            # Parse performance metrics output
            return await self._parse_performance_metrics(response.output)

        except Exception as e:
            logger.error(f"Error getting performance metrics for ONT {ont_id}: {str(e)}")
            return None

    async def _parse_performance_metrics(self, output: str) -> PerformanceMetrics:
        """
        Parse performance metrics from CLI output

        Args:
            output: Raw CLI output

        Returns:
            PerformanceMetrics: Parsed performance data
        """
        # This is a placeholder - implement based on actual Calix output format
        # You'll need to extract missed_bursts, bip_errors, crc_errors, etc.

        return PerformanceMetrics(
            missed_bursts=0,
            bip_errors=0,
            crc_errors=0,
            fec_errors=0
        )


# Global ONT service instance
ont_controller = ONTController()
