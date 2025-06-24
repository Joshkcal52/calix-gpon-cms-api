"""
Helper Utilities
Common utility functions for the Rogue ONT Detection API
"""

import re
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def parse_ont_id_from_string(text: str) -> Optional[str]:
    """
    Extract ONT ID from a string using regex patterns

    Args:
        text: String containing ONT ID

    Returns:
        str: Extracted ONT ID or None if not found
    """
    # Common patterns for ONT IDs in Calix systems
    patterns = [
        r'ONT\s+(\d+/\d+/\d+:\d+)',  # Format: ONT 1/1/1:1
        r'(\d+/\d+/\d+:\d+)',        # Format: 1/1/1:1
        r'ONT-(\d+)',                # Format: ONT-123
        r'(\d{1,3}/\d{1,3}/\d{1,3}:\d{1,3})'  # Generic pattern
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)

    return None


def parse_optical_levels(text: str) -> Dict[str, Optional[float]]:
    """
    Parse optical power levels from CLI output

    Args:
        text: CLI output containing optical level information

    Returns:
        Dict: Parsed optical levels with tx_power, rx_power, temperature
    """
    result = {
        'tx_power': None,
        'rx_power': None,
        'temperature': None
    }

    # Patterns for optical power levels
    tx_pattern = r'[Tt]x\s*[Pp]ower[:\s]*([+-]?\d+\.?\d*)\s*dBm'
    rx_pattern = r'[Rr]x\s*[Pp]ower[:\s]*([+-]?\d+\.?\d*)\s*dBm'
    temp_pattern = r'[Tt]emperature[:\s]*([+-]?\d+\.?\d*)\s*[Cc]'

    # Extract values
    tx_match = re.search(tx_pattern, text)
    if tx_match:
        try:
            result['tx_power'] = float(tx_match.group(1))
        except ValueError:
            logger.warning(f"Could not parse TX power: {tx_match.group(1)}")

    rx_match = re.search(rx_pattern, text)
    if rx_match:
        try:
            result['rx_power'] = float(rx_match.group(1))
        except ValueError:
            logger.warning(f"Could not parse RX power: {rx_match.group(1)}")

    temp_match = re.search(temp_pattern, text)
    if temp_match:
        try:
            result['temperature'] = float(temp_match.group(1))
        except ValueError:
            logger.warning(f"Could not parse temperature: {temp_match.group(1)}")

    return result


def parse_performance_metrics(text: str) -> Dict[str, Optional[int]]:
    """
    Parse performance metrics from CLI output

    Args:
        text: CLI output containing performance metrics

    Returns:
        Dict: Parsed performance metrics
    """
    result = {
        'missed_bursts': None,
        'bip_errors': None,
        'crc_errors': None,
        'fec_errors': None
    }

    # Patterns for performance metrics
    patterns = {
        'missed_bursts': [
            r'[Mm]issed\s*[Bb]ursts[:\s]*(\d+)',
            r'[Bb]urst\s*[Ee]rrors[:\s]*(\d+)'
        ],
        'bip_errors': [
            r'[Bb]IP\s*[Ee]rrors[:\s]*(\d+)',
            r'[Bb]it\s*[Ii]nterleaved\s*[Pp]arity[:\s]*(\d+)'
        ],
        'crc_errors': [
            r'[Cc]RC\s*[Ee]rrors[:\s]*(\d+)',
            r'[Cc]yclic\s*[Rr]edundancy\s*[Cc]heck[:\s]*(\d+)'
        ],
        'fec_errors': [
            r'[Ff]EC\s*[Ee]rrors[:\s]*(\d+)',
            r'[Ff]orward\s*[Ee]rror\s*[Cc]orrection[:\s]*(\d+)'
        ]
    }

    # Extract values for each metric type
    for metric, pattern_list in patterns.items():
        for pattern in pattern_list:
            match = re.search(pattern, text)
            if match:
                try:
                    result[metric] = int(match.group(1))
                    break  # Use first match found
                except ValueError:
                    logger.warning(f"Could not parse {metric}: {match.group(1)}")

    return result


def format_pon_port(pon_port: str) -> str:
    """
    Format PON port string to standard format

    Args:
        pon_port: Raw PON port string

    Returns:
        str: Formatted PON port string
    """
    # Remove extra spaces and normalize format
    formatted = re.sub(r'\s+', '', pon_port)

    # Ensure proper format (e.g., 1/1/1:1)
    if ':' not in formatted and '/' in formatted:
        # Add default ONT number if missing
        formatted += ':1'

    return formatted


def calculate_utilization_percentage(online_count: int, total_count: int) -> float:
    """
    Calculate PON utilization percentage

    Args:
        online_count: Number of online ONTs
        total_count: Total number of ONTs

    Returns:
        float: Utilization percentage (0-100)
    """
    if total_count == 0:
        return 0.0

    return round((online_count / total_count) * 100, 2)


def is_suspicious_ont(metrics: Dict[str, Optional[int]], thresholds: Dict[str, int]) -> bool:
    """
    Determine if an ONT is suspicious based on thresholds

    Args:
        metrics: ONT performance metrics
        thresholds: Threshold values for each metric

    Returns:
        bool: True if ONT is suspicious
    """
    for metric, threshold in thresholds.items():
        if metric in metrics and metrics[metric] is not None:
            if metrics[metric] > threshold:
                return True

    return False


def format_timestamp(timestamp: datetime) -> str:
    """
    Format timestamp for API responses

    Args:
        timestamp: Datetime object

    Returns:
        str: Formatted timestamp string
    """
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def validate_ont_id(ont_id: str) -> bool:
    """
    Validate ONT ID format

    Args:
        ont_id: ONT identifier to validate

    Returns:
        bool: True if valid format
    """
    # Common ONT ID patterns
    patterns = [
        r'^\d+/\d+/\d+:\d+$',  # Format: 1/1/1:1
        r'^ONT-\d+$',           # Format: ONT-123
        r'^\d+$'                # Format: 123
    ]

    for pattern in patterns:
        if re.match(pattern, ont_id):
            return True

    return False


def sanitize_command(command: str) -> str:
    """
    Sanitize CLI command to prevent injection

    Args:
        command: Raw command string

    Returns:
        str: Sanitized command
    """
    # Remove potentially dangerous characters
    dangerous_chars = [';', '&', '|', '`', '$', '(', ')', '{', '}']

    sanitized = command
    for char in dangerous_chars:
        sanitized = sanitized.replace(char, '')

    # Remove multiple spaces
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()

    return sanitized


def parse_calix_output_section(output: str, section_name: str) -> List[str]:
    """
    Parse a specific section from Calix CLI output

    Args:
        output: Full CLI output
        section_name: Name of section to extract

    Returns:
        List[str]: Lines from the specified section
    """
    lines = output.split('\n')
    section_lines = []
    in_section = False

    for line in lines:
        if section_name.lower() in line.lower():
            in_section = True
            continue

        if in_section:
            if line.strip() == '' or line.startswith('#'):
                break
            section_lines.append(line.strip())

    return section_lines
