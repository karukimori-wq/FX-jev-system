from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import unittest

from fx_poc import Engine, FakeBroker, Intent, Ledger, Mode, Position, Run, Side
from fx_poc.engine import AmbiguousSubmit, SafetyError
from fx_poc.models import OrderState


NOW = datetime(2026, 10, 24, 0, 0, tzinfo=timezone.utc)


def make_engine() -> tuple[Engine, Ledger, FakeBroker]:
    ledger = Ledger()
    broker = FakeBroker()
    return Engine(ledger, broker), ledger, broker


def add_run(engine: Engine, run_id: str = "run-A", logic_id: str = "logic-A", cap: int = 10_000) -> None:
    engine.add_run(Run(run_id, logic_id, Mode.LIVE, cap))


def intent(decision_id: str, run_id: str = "run-A", quantity: int = 2_000, side: Side = Side.BUY) -> Intent:
    return Intent(decision_id, run_id, "USD_JPY", side, quantity)


class AcceptanceTests(unittest.TestCase):
    # M03: atomic capacity reservation for concurrent new decisions.
    def test_m03_concurrent_decisions_cannot_exceed_quantity_cap(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine)
        engine.seed_position(Position("held-A", "run-A", "logic-A", "USD_JPY", Side.BUY, 6_000))

        def submit(decision_id: str) -> str:
            try:
                return engine.submit_intent(intent(decision_id, quantity=2_500), NOW).state.value
            except SafetyError:
                return "REJECTED"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(submit, ["decision-one", "decision-two"]))

        self.assertEqual(outcomes.count("OPEN"), 1)
        self.assertEqual(outcomes.count("REJECTED"), 1)
        self.assertEqual(engine.exposure("run-A"), 8_500)
        self.assertEqual(broker.submit_count, 1)

    # M06: owner-only close request leaves the other strategy untouched.
    def test_m06_close_is_scoped_to_the_owning_run(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine, "run-A", "logic-A")
        add_run(engine, "run-B", "logic-B")
        engine.seed_position(Position("pos-A", "run-A", "logic-A", "USD_JPY", Side.BUY, 5_000, "stop-A"))
        engine.seed_position(Position("pos-B", "run-B", "logic-B", "USD_JPY", Side.SELL, 5_000, "stop-B"))

        with self.assertRaisesRegex(SafetyError, "does not own"):
            engine.close_position("run-A", "pos-B", 5_000)
        engine.close_position("run-A", "pos-A", 5_000)

        self.assertEqual(broker.close_requests, [("run-A", "pos-A", 5_000)])
        self.assertEqual(ledger.positions["pos-B"].quantity, 5_000)
        self.assertEqual(ledger.positions["pos-B"].stop_order_id, "stop-B")

    # M07: persistent decision key survives engine recreation when ledger is reused.
    def test_m07_duplicate_decision_is_idempotent_across_engine_recreation(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine)
        first = engine.submit_intent(intent("stable-decision"), NOW)
        restarted_engine = Engine(ledger, broker)
        second = restarted_engine.submit_intent(intent("stable-decision"), NOW)

        self.assertEqual(first.internal_order_id, second.internal_order_id)
        self.assertEqual(len(ledger.orders), 1)
        self.assertEqual(broker.submit_count, 1)

    # M08: reservation is retained while cancellation is pending and fills race it.
    def test_m08_cancel_ack_does_not_release_unconfirmed_quantity(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine)
        order = engine.submit_intent(intent("cancel-race", quantity=10_000), NOW)
        engine.apply_fill(order.internal_order_id, "exec-1", 4_000)
        engine.request_cancel(order.internal_order_id)
        self.assertEqual(engine.exposure("run-A"), 10_000)

        engine.apply_fill(order.internal_order_id, "exec-2", 2_000)
        self.assertEqual(engine.exposure("run-A"), 10_000)
        canceled = engine.confirm_cancel(order.internal_order_id)

        self.assertEqual(canceled, 4_000)
        self.assertEqual(order.state, OrderState.CANCELED)
        self.assertEqual(order.filled_quantity, 6_000)
        self.assertEqual(engine.exposure("run-A"), 6_000)

    # M09: broker acceptance with lost response freezes the run; it never resends.
    def test_m09_ambiguous_response_becomes_unknown_without_resend(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine)
        broker.lose_next_submit_response = True
        order = engine.submit_intent(intent("lost-response"), NOW)

        self.assertEqual(order.state, OrderState.UNKNOWN)
        self.assertIn("run-A", ledger.frozen_runs)
        self.assertIs(engine.submit_intent(intent("lost-response"), NOW), order)
        with self.assertRaisesRegex(SafetyError, "UNKNOWN"):
            engine.submit_intent(intent("second-decision"), NOW)
        self.assertEqual(broker.submit_count, 1)

    # M16: a queued signal is revalidated after the run is stopped.
    def test_m16_stop_discards_queued_new_entry(self) -> None:
        engine, ledger, broker = make_engine()
        add_run(engine)
        engine.enqueue(intent("queued-before-stop"), NOW)
        engine.stop_run("run-A")

        dispatched = engine.dispatch_one(NOW)
        self.assertIsNone(dispatched)
        self.assertEqual(broker.submit_count, 0)
        self.assertEqual(len(ledger.queued), 0)

    def test_duplicate_execution_event_does_not_double_position(self) -> None:
        engine, ledger, _ = make_engine()
        add_run(engine)
        order = engine.submit_intent(intent("execution-idempotency"), NOW)
        position = engine.apply_fill(order.internal_order_id, "exec-unique", 2_000)
        duplicate = engine.apply_fill(order.internal_order_id, "exec-unique", 2_000)

        self.assertIsNotNone(position)
        self.assertIsNone(duplicate)
        self.assertEqual(sum(p.quantity for p in ledger.positions.values()), 2_000)


if __name__ == "__main__":
    unittest.main()
