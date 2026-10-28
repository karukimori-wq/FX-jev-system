from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum


class Mode(StrEnum):
    DEMO = "demo"
    LIVE = "live"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderState(StrEnum):
    SUBMITTING = "SUBMITTING"
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    FILLED = "FILLED"
    UNKNOWN = "UNKNOWN"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class Run:
    run_id: str
    logic_id: str
    mode: Mode
    max_quantity: int
    enabled: bool = True
    ends_at: datetime | None = None


@dataclass(frozen=True)
class Intent:
    decision_id: str
    run_id: str
    pair: str
    side: Side
    quantity: int


@dataclass
class Position:
    position_id: str
    run_id: str
    logic_id: str
    pair: str
    side: Side
    quantity: int
    stop_order_id: str | None = None


@dataclass
class Order:
    internal_order_id: str
    decision_id: str
    client_order_id: str
    run_id: str
    logic_id: str
    pair: str
    side: Side
    quantity: int
    filled_quantity: int = 0
    canceled_quantity: int = 0
    state: OrderState = OrderState.SUBMITTING
    external_order_id: str | None = None
    execution_ids: set[str] = field(default_factory=set)
    reject_reason: str | None = None

    @property
    def remaining_quantity(self) -> int:
        return max(0, self.quantity - self.filled_quantity - self.canceled_quantity)


@dataclass(frozen=True)
class QueuedIntent:
    intent: Intent
    queued_at: datetime
