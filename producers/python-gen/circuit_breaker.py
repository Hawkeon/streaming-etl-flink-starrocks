"""
Circuit Breaker - prevents cascade failures when downstream is unhealthy.
"""

import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreaker:
    """
    Circuit breaker that opens after failure_threshold failures,
    stays open for recovery_timeout seconds, then allows one test request.
    """
    failure_threshold: int = 5
    recovery_timeout: int = 60  # seconds
    half_open_max_calls: int = 1

    _failures: int = field(default=0, init=False)
    _last_failure_time: Optional[float] = field(default=None, init=False)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _half_open_calls: int = field(default=0, init=False)
    _lock: bool = field(default=False, init=False)

    def record_success(self) -> None:
        """Call on successful operation."""
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._half_open_calls = 0

    def record_failure(self) -> None:
        """Call on failed operation."""
        self._failures += 1
        self._last_failure_time = time.time()
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
        elif self._failures >= self.failure_threshold:
            self._state = CircuitState.OPEN

    def is_open(self) -> bool:
        """
        Returns True if circuit is OPEN (fail fast).
        If OPEN and recovery timeout passed, transitions to HALF_OPEN.
        """
        if self._state == CircuitState.OPEN:
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    return False  # allow test call
            return True
        return False

    def is_half_open(self) -> bool:
        """Returns True if in half-open state (allowing test calls)."""
        return self._state == CircuitState.HALF_OPEN

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failures(self) -> int:
        return self._failures
