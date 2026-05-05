import time
from dataclasses import dataclass, field


@dataclass
class RushStats:
    total: int = 0
    successes: int = 0
    errors: int = 0
    start_time: float = 0.0
    last_bizid: str | None = None
    last_response: dict | None = None

    @property
    def elapsed_ms(self) -> float:
        return (time.time() * 1000 - self.start_time) if self.start_time else 0

    @property
    def rate(self) -> float:
        if self.elapsed_ms <= 0:
            return 0
        return self.total / (self.elapsed_ms / 1000)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "successes": self.successes,
            "errors": self.errors,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "rate": round(self.rate, 1),
            "last_bizid": self.last_bizid,
        }
