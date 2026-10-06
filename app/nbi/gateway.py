"""
NBI Gateway
The only door to CMS. Routes and jobs call the gateway; the gateway calls a
backend (the mock CMS today, a real CMS client later).

The gateway adds, in one place:
  - a timeout on every call
  - retries with backoff, for reads only (writes are retried by the worker,
    which first checks what was already applied)
  - a circuit breaker, so a CMS outage fails fast instead of stalling the API
  - a cap on concurrent CMS calls
  - typed errors (app/nbi/errors.py) that map to clean API responses

Backends only translate one operation into one CMS exchange and raise
NbiError subclasses. They need no resilience logic of their own.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Protocol

from app.models.provisioning_model import DiscoveredOnt, EthernetService, OntSpec, OntState
from app.nbi.breaker import CircuitBreaker, OPEN
from app.nbi.errors import NbiError, NbiNotConfiguredError, NbiTimeoutError, NbiTransportError

logger = logging.getLogger(__name__)


class NbiBackend(Protocol):
    """What a CMS client has to provide to sit behind the gateway"""

    name: str

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]: ...

    async def get_ont(self, network: str, ont_id: int) -> OntState: ...

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]: ...

    async def create_ont(self, network: str, spec: OntSpec) -> OntState: ...

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None: ...

    async def delete_ont(self, network: str, ont_id: int) -> None: ...

    async def close(self) -> None: ...


class NbiGateway:
    def __init__(self, backend: NbiBackend, *, timeout: float = 10.0, max_retries: int = 2,
                 retry_backoff: float = 0.25, max_concurrency: int = 8,
                 breaker: Optional[CircuitBreaker] = None):
        self.backend = backend
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.breaker = breaker or CircuitBreaker()
        self._slots = asyncio.Semaphore(max_concurrency)

    # --- Operations ------------------------------------------------------

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        return await self._call("list_discovered_onts",
                                lambda: self.backend.list_discovered_onts(network), idempotent=True)

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        return await self._call("get_ont",
                                lambda: self.backend.get_ont(network, ont_id), idempotent=True)

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        return await self._call("find_ont_by_serial",
                                lambda: self.backend.find_ont_by_serial(network, serial_number),
                                idempotent=True)

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        return await self._call("create_ont",
                                lambda: self.backend.create_ont(network, spec), idempotent=False)

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        return await self._call("add_ethernet_service",
                                lambda: self.backend.add_ethernet_service(network, ont_id, service),
                                idempotent=False)

    async def delete_ont(self, network: str, ont_id: int) -> None:
        return await self._call("delete_ont",
                                lambda: self.backend.delete_ont(network, ont_id), idempotent=False)

    def health(self) -> Dict[str, Any]:
        return {"backend": self.backend.name, "breaker": self.breaker.snapshot()}

    async def close(self) -> None:
        await self.backend.close()

    # --- Resilience ------------------------------------------------------

    async def _call(self, operation: str, fn: Callable[[], Awaitable[Any]], *, idempotent: bool) -> Any:
        attempts = 1 + (self.max_retries if idempotent else 0)

        for attempt in range(1, attempts + 1):
            self.breaker.allow(operation)  # raises NbiCircuitOpenError while CMS is rested

            try:
                async with self._slots:
                    result = await asyncio.wait_for(fn(), timeout=self.timeout)
            except asyncio.TimeoutError:
                error: NbiTransportError = NbiTimeoutError(self._timeout_message(operation, idempotent))
            except NbiTransportError as e:
                error = e
            except NbiError as e:
                # CMS answered, it just said no. That is not an outage.
                self.breaker.record_success()
                e.operation = operation
                raise
            except BaseException:
                self.breaker.abandon()
                raise
            else:
                self.breaker.record_success()
                return result

            error.operation = operation
            self.breaker.record_failure()
            logger.warning(f"CMS call {operation} failed (attempt {attempt}/{attempts}): {error.message}")

            if attempt == attempts or self.breaker.state == OPEN:
                raise error
            await asyncio.sleep(self.retry_backoff * 2 ** (attempt - 1))

    def _timeout_message(self, operation: str, idempotent: bool) -> str:
        message = f"CMS did not answer within {self.timeout:g} seconds while running {operation}."
        if not idempotent:
            message += " The change may or may not have been applied; check the ONT before retrying."
        return message


class NotConfiguredBackend:
    """Placeholder used when MOCK_MODE is off and no real CMS client exists yet"""

    name = "not-configured"

    def _refuse(self) -> NbiNotConfiguredError:
        return NbiNotConfiguredError(
            "No CMS connection is configured. Set MOCK_MODE=true to use the simulated CMS; "
            "a real CMS backend has not been added yet (see docs/ARCHITECTURE.md).")

    async def list_discovered_onts(self, network: str) -> List[DiscoveredOnt]:
        raise self._refuse()

    async def get_ont(self, network: str, ont_id: int) -> OntState:
        raise self._refuse()

    async def find_ont_by_serial(self, network: str, serial_number: str) -> Optional[OntState]:
        raise self._refuse()

    async def create_ont(self, network: str, spec: OntSpec) -> OntState:
        raise self._refuse()

    async def add_ethernet_service(self, network: str, ont_id: int, service: EthernetService) -> None:
        raise self._refuse()

    async def delete_ont(self, network: str, ont_id: int) -> None:
        raise self._refuse()

    async def close(self) -> None:
        return None


def build_gateway(settings: Any) -> NbiGateway:
    """Create the gateway for the current configuration"""
    if settings.mock_mode:
        from app.nbi.mock_cms import MockCmsBackend
        backend: NbiBackend = MockCmsBackend()
        logger.warning("MOCK_MODE is on: CMS calls go to a simulated CMS, not a real one")
    else:
        backend = NotConfiguredBackend()

    return NbiGateway(
        backend,
        timeout=settings.nbi_timeout_seconds,
        max_retries=settings.nbi_max_retries,
        max_concurrency=settings.nbi_max_concurrency,
        breaker=CircuitBreaker(
            failure_threshold=settings.nbi_breaker_failure_threshold,
            recovery_seconds=settings.nbi_breaker_recovery_seconds),
    )
