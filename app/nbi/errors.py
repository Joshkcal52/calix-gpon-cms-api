"""
NBI Errors
One exception type per way a CMS call can fail. Each carries the HTTP status
and the machine-readable code the API returns, so routes and background jobs
report failures the same way.
"""

from typing import Any, Dict, Optional


class NbiError(Exception):
    """Base class for every failure talking to CMS"""

    code = "cms_error"
    http_status = 502
    retryable = False  # is it worth the caller trying the same request again?

    def __init__(self, message: str, *, operation: Optional[str] = None,
                 retry_after: Optional[float] = None):
        super().__init__(message)
        self.message = message
        self.operation = operation
        self.retry_after = retry_after

    def to_dict(self) -> Dict[str, Any]:
        """The error body sent to API clients and stored on failed jobs"""
        body: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.operation:
            body["operation"] = self.operation
        if self.retry_after is not None:
            body["retry_after_seconds"] = max(1, round(self.retry_after))
        return body


class NbiTransportError(NbiError):
    """CMS could not be reached or did not answer. Counts against the circuit breaker."""
    retryable = True


class NbiTimeoutError(NbiTransportError):
    code = "cms_timeout"
    http_status = 504


class NbiConnectionError(NbiTransportError):
    code = "cms_unreachable"
    http_status = 502


class NbiCircuitOpenError(NbiError):
    """The breaker is open: CMS has been failing, so the call was not attempted"""
    code = "cms_unavailable"
    http_status = 503
    retryable = True


class NbiNotConfiguredError(NbiError):
    """No real CMS backend is set up (and MOCK_MODE is off)"""
    code = "cms_not_configured"
    http_status = 503


class NbiAuthError(NbiError):
    """CMS refused this API's own credentials. A server-side configuration problem."""
    code = "cms_auth_failed"
    http_status = 502


class NbiNotFoundError(NbiError):
    code = "not_found"
    http_status = 404


class NbiConflictError(NbiError):
    code = "conflict"
    http_status = 409


class NbiRejectedError(NbiError):
    """CMS answered and refused the request (bad VLAN, unknown profile, ...)"""
    code = "cms_rejected"
    http_status = 422
