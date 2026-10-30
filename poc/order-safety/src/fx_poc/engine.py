from __future__ import annotations

from collections import deque
from datetime import datetime
from threading import RLock

from .models import Intent, Mode, Order, OrderState, Position, QueuedIntent, Run, Side


class SafetyError(RuntimeError):
    """The requested action violates a local safety invariant."""


class AmbiguousSubmit(RuntimeError):
    """The broker may have accepted an order but its response was lost."""


class Ledger:
    """In-memory stand-in for a durable transactional ledger."""

    def __init__(self) -> None:
        self.lock = RLock()
        self.runs: dict[str, Run] = {}
        self.orders: dict[str, Order] = {}
        self.positions: dict[str, Position] = {}
        self.decision_to_order: dict[str, str] = {}
        self.frozen_runs: set[str] = set()
        self.queued: deque[QueuedIntent] = deque()
        self.close_requests: list[tuple[str, str, int]] = []
        self.event_ids: set[str] = set()


class FakeBroker:
    """Deterministic broker double. It never makes network requests."""

    def __init__(self) -> None:
        self.orders: dict[str, dict[str, str | int]] = {}
        self.close_requests: list[tuple[str, str, int]] = []
        self.submit_count = 0
        self.lose_next_submit_response = False

    def submit_open(self, order: Order) -> str:
        self.submit_count += 1
        external_id = f"broker-{self.submit_count}"
        self.orders[external_id] = {
            "client_order_id": order.client_order_id,
            "state": "OPEN",
            "quantity": order.quantity,
        }
        if self.lose_next_submit_response:
            self.lose_next_submit_response = False
            raise AmbiguousSubmit("simulated response loss after broker acceptance")
        return external_id

    def request_cancel(self, external_order_id: str | None) -> None:
        if external_order_id is None or external_order_id not in self.orders:
            raise SafetyError("cannot cancel an order without a known broker order ID")
        # Acknowledgement is deliberately not a final cancel state.
        self.orders[external_order_id]["cancel_requested"] = 1

    def submit_close(self, run_id: str, position_id: str, quantity: int) -> None:
        self.close_requests.append((run_id, position_id, quantity))

    def find_by_client_id(self, client_order_id: str) -> str | None:
        for external_id, data in self.orders.items():
            if data["client_order_id"] == client_order_id:
                return external_id
        return None


class Engine:
    def __init__(self, ledger: Ledger, broker: FakeBroker) -> None:
        self.ledger = ledger
        self.broker = broker

    def add_run(self, run: Run) -> None:
        if run.max_quantity < 0:
            raise ValueError("max_quantity must be non-negative")
        with self.ledger.lock:
            if run.run_id in self.ledger.runs:
                raise SafetyError(f"run already exists: {run.run_id}")
            self.ledger.runs[run.run_id] = run

    def seed_position(self, position: Position) -> None:
        """Test helper to load a known, already reconciled position."""
        if position.quantity <= 0:
            raise ValueError("position quantity must be positive")
        with self.ledger.lock:
            run = self._run(position.run_id)
            if run.logic_id != position.logic_id:
                raise SafetyError("position logic owner does not match run")
            if position.position_id in self.ledger.positions:
                raise SafetyError("position already exists")
            self.ledger.positions[position.position_id] = position

    def enqueue(self, intent: Intent, now: datetime) -> None:
        with self.ledger.lock:
            self.ledger.queued.append(QueuedIntent(intent=intent, queued_at=now))

    def stop_run(self, run_id: str) -> None:
        with self.ledger.lock:
            run = self._run(run_id)
            self.ledger.runs[run_id] = Run(
                run_id=run.run_id,
                logic_id=run.logic_id,
                mode=run.mode,
                max_quantity=run.max_quantity,
                enabled=False,
                ends_at=run.ends_at,
            )
            self.ledger.queued = deque(q for q in self.ledger.queued if q.intent.run_id != run_id)

    def dispatch_one(self, now: datetime) -> Order | None:
        with self.ledger.lock:
            if not self.ledger.queued:
                return None
            queued = self.ledger.queued.popleft()
        # submit_intent rechecks all run/cap/dedupe conditions at dispatch time.
        return self.submit_intent(queued.intent, now)

    def submit_intent(self, intent: Intent, now: datetime) -> Order:
        with self.ledger.lock:
            existing_id = self.ledger.decision_to_order.get(intent.decision_id)
            if existing_id:
                existing = self.ledger.orders[existing_id]
                if (existing.run_id, existing.pair, existing.side, existing.quantity) != (
                    intent.run_id,
                    intent.pair,
                    intent.side,
                    intent.quantity,
                ):
                    raise SafetyError("decision_id was reused with different order intent")
                return existing

            run = self._run(intent.run_id)
            self._validate_intent(intent, run, now)
            if run.run_id in self.ledger.frozen_runs:
                raise SafetyError("run is frozen while an order result is UNKNOWN")
            self._check_capacity(run, intent.quantity)

            internal_id = f"order-{len(self.ledger.orders) + 1}"
            order = Order(
                internal_order_id=internal_id,
                decision_id=intent.decision_id,
                client_order_id=self._client_order_id(intent.decision_id),
                run_id=run.run_id,
                logic_id=run.logic_id,
                pair=intent.pair,
                side=intent.side,
                quantity=intent.quantity,
                state=OrderState.SUBMITTING,
            )
            # Reservation and idempotency key commit together under this lock.
            self.ledger.orders[internal_id] = order
            self.ledger.decision_to_order[intent.decision_id] = internal_id

        try:
            external_id = self.broker.submit_open(order)
        except AmbiguousSubmit:
            with self.ledger.lock:
                order.state = OrderState.UNKNOWN
                self.ledger.frozen_runs.add(run.run_id)
            return order
        except Exception as exc:
            # An unclassified transport error is ambiguous unless a definitive reject exists.
            with self.ledger.lock:
                order.state = OrderState.UNKNOWN
                order.reject_reason = type(exc).__name__
                self.ledger.frozen_runs.add(run.run_id)
            return order
        with self.ledger.lock:
            order.external_order_id = external_id
            order.state = OrderState.OPEN
            return order

    def apply_fill(self, internal_order_id: str, execution_id: str, quantity: int) -> Position | None:
        if quantity <= 0:
            raise ValueError("fill quantity must be positive")
        with self.ledger.lock:
            if execution_id in self.ledger.event_ids:
                return None
            order = self.ledger.orders[internal_order_id]
            if quantity > order.remaining_quantity:
                raise SafetyError("fill exceeds the outstanding order quantity")
            self.ledger.event_ids.add(execution_id)
            order.execution_ids.add(execution_id)
            order.filled_quantity += quantity
            run = self._run(order.run_id)
            position_id = f"position-{execution_id}"
            position = Position(
                position_id=position_id,
                run_id=run.run_id,
                logic_id=run.logic_id,
                pair=order.pair,
                side=order.side,
                quantity=quantity,
            )
            self.ledger.positions[position_id] = position
            if order.state == OrderState.CANCEL_PENDING:
                # A fill may arrive after a cancel request; keep the order pending
                # until the broker reports its final canceled quantity.
                pass
            elif order.remaining_quantity == 0:
                order.state = OrderState.FILLED
            elif order.filled_quantity:
                order.state = OrderState.PARTIAL
            return position

    def request_cancel(self, internal_order_id: str) -> None:
        with self.ledger.lock:
            order = self.ledger.orders[internal_order_id]
            if order.state not in {OrderState.OPEN, OrderState.PARTIAL, OrderState.CANCEL_PENDING}:
                raise SafetyError(f"order cannot be cancelled in state {order.state}")
            order.state = OrderState.CANCEL_PENDING
            external_id = order.external_order_id
        self.broker.request_cancel(external_id)

    def confirm_cancel(self, internal_order_id: str) -> int:
        """Apply a final broker reconciliation; only now is remaining reserve released."""
        with self.ledger.lock:
            order = self.ledger.orders[internal_order_id]
            if order.state != OrderState.CANCEL_PENDING:
                raise SafetyError("cancel confirmation requires CANCEL_PENDING")
            canceled = order.remaining_quantity
            order.canceled_quantity += canceled
            order.state = OrderState.CANCELED
            return canceled

    def close_position(self, run_id: str, position_id: str, quantity: int) -> None:
        with self.ledger.lock:
            position = self.ledger.positions.get(position_id)
            if position is None:
                raise SafetyError("position does not exist")
            if position.run_id != run_id:
                raise SafetyError("run does not own this position")
            if quantity <= 0 or quantity > position.quantity:
                raise SafetyError("close quantity is outside the owned position size")
            self.broker.submit_close(run_id, position_id, quantity)
            self.ledger.close_requests.append((run_id, position_id, quantity))

    def resolve_unknown(self, internal_order_id: str) -> bool:
        """Attach a discovered external ID; this method never resubmits an order."""
        with self.ledger.lock:
            order = self.ledger.orders[internal_order_id]
            if order.state != OrderState.UNKNOWN:
                raise SafetyError("only UNKNOWN orders can be reconciled")
            external_id = self.broker.find_by_client_id(order.client_order_id)
            if external_id is None:
                return False
            order.external_order_id = external_id
            order.state = OrderState.OPEN
            # Reconciliation of order existence alone does not release the run freeze.
            return True

    def exposure(self, run_id: str) -> int:
        with self.ledger.lock:
            run = self._run(run_id)
            return self._run_position_qty(run_id) + self._run_reserved_qty(run_id)

    def _check_capacity(self, run: Run, requested: int) -> None:
        if run.mode == Mode.DEMO:
            if self._run_position_qty(run.run_id) + self._run_reserved_qty(run.run_id) + requested > run.max_quantity:
                raise SafetyError("demo run quantity cap exceeded")
            return
        logic_runs = [r for r in self.ledger.runs.values() if r.logic_id == run.logic_id and r.mode == Mode.LIVE]
        logic_cap = min((r.max_quantity for r in logic_runs), default=run.max_quantity)
        position_qty = sum(self._run_position_qty(r.run_id) for r in logic_runs)
        reserved_qty = sum(self._run_reserved_qty(r.run_id) for r in logic_runs)
        if position_qty + reserved_qty + requested > logic_cap:
            raise SafetyError("live logic-wide quantity cap exceeded")
        if self._run_position_qty(run.run_id) + self._run_reserved_qty(run.run_id) + requested > run.max_quantity:
            raise SafetyError("live run quantity cap exceeded")

    def _run_position_qty(self, run_id: str) -> int:
        return sum(p.quantity for p in self.ledger.positions.values() if p.run_id == run_id)

    def _run_reserved_qty(self, run_id: str) -> int:
        return sum(
            o.remaining_quantity
            for o in self.ledger.orders.values()
            if o.run_id == run_id
            and o.state in {
                OrderState.SUBMITTING,
                OrderState.OPEN,
                OrderState.PARTIAL,
                OrderState.CANCEL_PENDING,
                OrderState.UNKNOWN,
            }
        )

    def _validate_intent(self, intent: Intent, run: Run, now: datetime) -> None:
        if intent.quantity <= 0:
            raise SafetyError("quantity must be positive")
        if not intent.pair or "_" not in intent.pair:
            raise SafetyError("pair must use canonical BASE_QUOTE form")
        if not run.enabled:
            raise SafetyError("run is stopped")
        if run.ends_at is not None and now >= run.ends_at:
            raise SafetyError("run end time has passed")
        if run.mode == Mode.LIVE and run.max_quantity == 0:
            raise SafetyError("a zero-cap run cannot send live orders")

    def _run(self, run_id: str) -> Run:
        try:
            return self.ledger.runs[run_id]
        except KeyError as exc:
            raise SafetyError(f"unknown run: {run_id}") from exc

    @staticmethod
    def _client_order_id(decision_id: str) -> str:
        # Deterministic local correlation ID; no broker idempotency is assumed.
        safe = "".join(ch for ch in decision_id if ch.isalnum()) or "decision"
        return safe[:36]

