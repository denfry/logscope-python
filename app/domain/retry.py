from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    base_seconds: float = 0.5
    max_seconds: float = 30.0
    jitter_seconds: float = 0.2

    def __post_init__(self) -> None:
        if self.base_seconds <= 0 or self.max_seconds <= 0 or self.base_seconds > self.max_seconds:
            raise ValueError("retry bounds are invalid")
        if self.jitter_seconds < 0:
            raise ValueError("jitter must not be negative")

    def next_delay(self, attempt: int) -> float:
        if attempt < 1:
            raise ValueError("attempt must be positive")
        exponent = min(attempt - 1, 31)
        delay = min(self.max_seconds, self.base_seconds * (2**exponent))
        if self.jitter_seconds:
            delay = min(self.max_seconds, delay + random.uniform(0, self.jitter_seconds))
        return delay
