"""
Circuit Breaker
Stops calling CMS while it is down, so requests fail in milliseconds with a
clear message instead of each waiting out a full timeout.

closed    -> calls go through. N consecutive failures open the breaker.
open      -> calls are refused until recovery_seconds have passed.
half_open -> one probe call is let through. Success closes the breaker,
             failure opens it again.

Everything here is synchronous and runs on the event loop, so no locking is
needed.
"""

import logging
import time
from typing import Any, Callable, Dict, Optional

from app.nbi.errors import NbiCircuitOpenError

logger = logging.getLogger(__name__)

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_seconds: float = 30.0,
                 clock: Callable[[], float] = time.monotonic):
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._clock = clock
        self._state = CLOSED
        self._failures = 0  # consecutive failures
        self._opened_at: Optional[float] = None
        self._probe_in_flight = False

    @property
    def state(self) -> str:
        return self._state

    def allow(self, operation: Optional[str] = None) -> None:
        """Call before each attempt. Raises NbiCircuitOpenError if CMS is being rested."""
        if self._state == CLOSED:
            return

        if self._state == OPEN:
            remaining = self.recovery_seconds - (self._clock() - self._opened_at)
            if remaining > 0:
                raise self._refusal(operation, remaining)
            self._state = HALF_OPEN
            self._probe_in_flight = True
            logger.info("CMS circuit breaker half-open: letting one probe call through")
            return

        # Half-open: only one probe at a time
        if self._probe_in_flight:
            raise self._refusal(operation, 1.0)
        self._probe_in_flight = True

    def record_success(self) -> None:
        if self._state != CLOSED:
            logger.info("CMS circuit breaker closed: CMS is answering again")
        self._state = CLOSED
        self._failures = 0
        self._opened_at = None
        self._probe_in_flight = False

    def record_failure(self) -> None:
        self._failures += 1
        self._probe_in_flight = False
        if self._state == HALF_OPEN or self._failures >= self.failure_threshold:
            if self._state != OPEN:
                logger.warning(
                    f"CMS circuit breaker opened after {self._failures} consecutive failures; "
                    f"pausing CMS calls for {self.recovery_seconds:.0f}s")
            self._state = OPEN
            self._opened_at = self._clock()

    def abandon(self) -> None:
        """An attempt ended without a verdict (e.g. it was cancelled). Free the probe slot."""
        self._probe_in_flight = False

    def snapshot(self) -> Dict[str, Any]:
        retry_after = None
        if self._state == OPEN:
            retry_after = max(0.0, self.recovery_seconds - (self._clock() - self._opened_at))
        return {
            "state": self._state,
            "consecutive_failures": self._failures,
            "failure_threshold": self.failure_threshold,
            "recovery_seconds": self.recovery_seconds,
            "retry_after_seconds": None if retry_after is None else round(retry_after, 1),
        }

    def _refusal(self, operation: Optional[str], retry_after: float) -> NbiCircuitOpenError:
        return NbiCircuitOpenError(
            f"CMS has failed {self._failures} times in a row, so calls are paused. "
            f"Try again in about {max(1, round(retry_after))} seconds.",
            operation=operation,
            retry_after=retry_after)
