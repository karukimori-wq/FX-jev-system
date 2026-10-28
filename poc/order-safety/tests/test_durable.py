from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from fx_poc import Intent, Mode, Run, Side
from fx_poc.durable import DurableEngine, SQLiteLedger
from fx_poc.engine import AmbiguousSubmit, SafetyError
from fx_poc.models import OrderState


class CountingBroker:
    def __init__(self):
        self.submit_count = 0
        self.lose_response = False

    def submit_open(self, order):
        self.submit_count += 1
        if self.lose_response:
            self.lose_response = False
            raise AmbiguousSubmit("accepted, response lost")
        return f"broker-{self.submit_count}"


class DurableAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "ledger.sqlite3"
        self.ledger = SQLiteLedger(self.db_path)
        self.ledger.set_logic_limit("logic-A", 10_000)
        self.ledger.add_run(Run("run-A", "logic-A", Mode.LIVE, 10_000))
        self.broker = CountingBroker()

    def tearDown(self):
        self.tmp.cleanup()

    def test_m07_decision_is_idempotent_after_database_and_engine_restart(self):
        intent = Intent("stable-decision", "run-A", "USD_JPY", Side.BUY, 2_000)
        first = DurableEngine(self.ledger, self.broker).submit_intent(intent)

        restarted_ledger = SQLiteLedger(self.db_path)
        second = DurableEngine(restarted_ledger, self.broker).submit_intent(intent)

        self.assertEqual(first, second)
        self.assertEqual(second.state, OrderState.OPEN)
        self.assertEqual(self.broker.submit_count, 1)

    def test_unknown_survives_restart_without_resubmission(self):
        intent = Intent("lost-response", "run-A", "USD_JPY", Side.BUY, 2_000)
        self.broker.lose_response = True
        first = DurableEngine(self.ledger, self.broker).submit_intent(intent)
        second = DurableEngine(SQLiteLedger(self.db_path), self.broker).submit_intent(intent)

        self.assertEqual(first.state, OrderState.UNKNOWN)
        self.assertEqual(second.state, OrderState.UNKNOWN)
        self.assertEqual(self.broker.submit_count, 1)

    def test_m03_parallel_reservations_cannot_cross_cap(self):
        self.ledger.seed_position("held", "run-A", 7_000)
        intents = [Intent(f"decision-{i}", "run-A", "USD_JPY", Side.BUY, 2_500) for i in range(2)]

        def submit(intent):
            try:
                return DurableEngine(SQLiteLedger(self.db_path), self.broker).submit_intent(intent).state
            except SafetyError:
                return "REJECTED"

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(submit, intents))
        self.assertEqual(outcomes.count(OrderState.OPEN), 1)
        self.assertEqual(outcomes.count("REJECTED"), 1)
        self.assertEqual(self.broker.submit_count, 1)

    def test_fill_cancel_and_stop_survive_reopen(self):
        intent = Intent("race", "run-A", "USD_JPY", Side.BUY, 10_000)
        DurableEngine(self.ledger, self.broker).submit_intent(intent)
        self.ledger.apply_fill("race", "exec1", 4_000)
        self.ledger.set_state("race", OrderState.CANCEL_PENDING)
        reopened = SQLiteLedger(self.db_path)
        reopened.apply_fill("race", "exec2", 2_000)
        self.assertEqual(reopened.get_order("race").state, OrderState.CANCEL_PENDING)
        self.assertFalse(reopened.apply_fill("race", "exec2", 2_000))
        reopened.confirm_cancel("race")
        reopened.stop_run("run-A")
        reopened = SQLiteLedger(self.db_path)
        self.assertEqual(reopened.get_order("race").state, OrderState.CANCELED)
        with self.assertRaisesRegex(SafetyError, "stopped"):
            reopened.reserve(Intent("later", "run-A", "USD_JPY", Side.BUY, 1_000))

    def test_owned_close_reservation_and_fill_survive_restart(self):
        self.ledger.seed_position("pos", "run-A", 5_000)
        with self.assertRaisesRegex(SafetyError, "own"):
            self.ledger.reserve_close("bad", "other-run", "pos", 1_000)
        self.assertTrue(self.ledger.reserve_close("close1", "run-A", "pos", 4_000))
        reopened = SQLiteLedger(self.db_path)
        self.assertFalse(reopened.reserve_close("close1", "run-A", "pos", 4_000))
        with self.assertRaisesRegex(SafetyError, "exceeds"):
            reopened.reserve_close("close2", "run-A", "pos", 2_000)
        self.assertTrue(reopened.apply_close_fill("close1", "close-exec", 2_000))
        self.assertFalse(SQLiteLedger(self.db_path).apply_close_fill("close1", "close-exec", 2_000))
        with reopened._connect() as db:
            self.assertEqual(db.execute("SELECT quantity FROM positions WHERE position_id='pos'").fetchone()[0], 3_000)

    def test_terminal_order_cannot_be_reopened(self):
        DurableEngine(self.ledger, self.broker).submit_intent(Intent("terminal", "run-A", "USD_JPY", Side.BUY, 1_000))
        self.ledger.apply_fill("terminal", "terminal-exec", 1_000)
        with self.assertRaisesRegex(SafetyError, "transition"):
            self.ledger.set_state("terminal", OrderState.OPEN)

    def test_close_unknown_is_not_resent_after_reopen(self):
        from fx_poc.durable import DurableCloseEngine
        class LostResponse:
            calls = 0
            def submit_close(self, *args):
                self.calls += 1
                raise TimeoutError()
        broker = LostResponse()
        self.ledger.seed_position("owned", "run-A", 5_000)
        self.assertEqual(DurableCloseEngine(self.ledger, broker).submit("c", "run-A", "owned", 5_000), "UNKNOWN")
        self.assertEqual(DurableCloseEngine(SQLiteLedger(self.db_path), broker).submit("c", "run-A", "owned", 5_000), "UNKNOWN")
        self.assertEqual(broker.calls, 1)
        with self.assertRaises(SafetyError):
            self.ledger.reserve_close("c2", "run-A", "owned", 1)

    def test_cancel_releases_only_unfilled_close_reservation(self):
        self.ledger.seed_position("owned", "run-A", 5_000)
        self.ledger.reserve_close("c", "run-A", "owned", 5_000)
        self.ledger.transition_close("c", "OPEN")
        self.ledger.transition_close("c", "CANCEL_PENDING")
        self.ledger.apply_close_fill("c", "cx", 2_000)
        with self.assertRaises(SafetyError):
            self.ledger.reserve_close("c2", "run-A", "owned", 1)
        self.ledger.transition_close("c", "CANCELED")
        self.assertTrue(SQLiteLedger(self.db_path).reserve_close("c2", "run-A", "owned", 3_000))
        self.ledger.transition_close("c2", "REJECTED")
        self.assertTrue(self.ledger.reserve_close("c3", "run-A", "owned", 3_000))

    def test_unknown_close_reconciliation_requires_unique_exact_match(self):
        self.ledger.seed_position("owned", "run-A", 5_000)
        self.ledger.reserve_close("rc", "run-A", "owned", 5_000)
        self.ledger.transition_close("rc", "UNKNOWN")
        self.assertFalse(self.ledger.reconcile_close("rc", []))
        with self.assertRaisesRegex(SafetyError, "UNKNOWN"):
            self.ledger.reserve(Intent("blocked", "run-A", "USD_JPY", Side.BUY, 1))
        evidence = dict(close_id="rc", run_id="run-A", position_id="owned", quantity=5_000, filled=0, state="OPEN", external_id="external-rc")
        self.assertFalse(self.ledger.reconcile_close("rc", [evidence, evidence]))
        self.assertTrue(self.ledger.reconcile_close("rc", [evidence]))
        reopened = SQLiteLedger(self.db_path)
        self.assertEqual(reopened.close_state("rc"), "OPEN")
        with reopened._connect() as db:
            self.assertEqual(db.execute("SELECT external_id FROM closes WHERE close_id='rc'").fetchone()[0], "external-rc")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM close_reconciliations").fetchone()[0], 3)

    def test_unknown_and_stop_rollback_together_on_db_failure(self):
        import sqlite3
        self.ledger.seed_position("atomic", "run-A", 1_000)
        self.ledger.reserve_close("atomic-close", "run-A", "atomic", 1_000)
        with self.ledger._connect() as db:
            db.execute("CREATE TRIGGER fail_stop BEFORE UPDATE ON runs BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ledger.transition_close("atomic-close", "UNKNOWN")
        self.assertEqual(SQLiteLedger(self.db_path).close_state("atomic-close"), "SUBMITTING")

    def test_partial_reconciliation_and_explicit_mock_resume(self):
        self.ledger.seed_position("partial", "run-A", 5_000)
        self.ledger.reserve_close("pc", "run-A", "partial", 5_000)
        self.ledger.apply_close_fill("pc", "pf", 2_000)
        self.ledger.transition_close("pc", "UNKNOWN")
        with self.assertRaises(SafetyError):
            self.ledger.approve_mock_resume("run-A", "operator", "reviewed")
        evidence = dict(close_id="pc", run_id="run-A", position_id="partial", quantity=5_000, filled=3_000, state="PARTIAL", external_id="ep")
        self.assertFalse(self.ledger.reconcile_close("pc", [evidence]))
        evidence["filled"] = 2_000
        self.assertTrue(self.ledger.reconcile_close("pc", [evidence]))
        with self.ledger._connect() as db:
            self.assertEqual(db.execute("SELECT enabled FROM runs WHERE run_id='run-A'").fetchone()[0], 0)
        self.ledger.approve_mock_resume("run-A", "operator", "mock evidence reviewed")
        self.assertTrue(SQLiteLedger(self.db_path).reserve(Intent("after-review", "run-A", "USD_JPY", Side.BUY, 1_000))[1])

    def test_multiple_stop_reasons_survive_restart_and_block_resume(self):
        self.ledger.stop_run("run-A", "MANUAL")
        self.ledger.stop_run("run-A", "DATA_FAILURE")
        reopened = SQLiteLedger(self.db_path)
        reopened.clear_mock_stop("run-A", "DATA_FAILURE", "operator")
        with self.assertRaisesRegex(SafetyError, "stop reasons"):
            reopened.approve_mock_resume("run-A", "operator", "reviewed")
        reopened.clear_mock_stop("run-A", "MANUAL", "operator")
        reopened.approve_mock_resume("run-A", "operator", "all mock reasons cleared")

    def test_startup_detects_submitting_without_resend(self):
        self.ledger.reserve(Intent("before-crash", "run-A", "USD_JPY", Side.BUY, 1_000))
        reopened = SQLiteLedger(self.db_path)
        self.assertIn(("orders", "before-crash", "run-A"), reopened.startup_scan())
        with self.assertRaisesRegex(SafetyError, "unresolved"):
            reopened.clear_mock_stop("run-A", "STARTUP_REVIEW", "operator")
        with self.assertRaisesRegex(SafetyError, "stopped"):
            reopened.reserve(Intent("new", "run-A", "USD_JPY", Side.BUY, 1_000))
        self.assertEqual(self.broker.submit_count, 0)

    def test_worker_startup_review_and_send_flow(self):
        from fx_poc.worker import MockWorker
        worker = MockWorker(self.ledger, self.broker)
        intent = Intent("worker-entry", "run-A", "USD_JPY", Side.BUY, 1_000)
        with self.assertRaisesRegex(SafetyError, "stopped"):
            worker.submit(intent)
        token = worker.review_token("run-A")
        self.ledger.stop_run("run-A", "DATA_FAILURE")
        with self.assertRaisesRegex(SafetyError, "stale"):
            worker.approve_resume("run-A", "operator", "mock review", token)
        with self.assertRaisesRegex(SafetyError, "other stop"):
            worker.approve_resume("run-A", "operator", "mock review", worker.review_token("run-A"))
        self.ledger.clear_mock_stop("run-A", "DATA_FAILURE", "operator")
        worker.approve_resume("run-A", "operator", "mock review", worker.review_token("run-A"))
        self.assertEqual(worker.submit(intent).state, OrderState.OPEN)
        restarted = MockWorker(SQLiteLedger(self.db_path), self.broker)
        with self.assertRaisesRegex(SafetyError, "stopped"):
            restarted.submit(Intent("after-restart", "run-A", "USD_JPY", Side.BUY, 1_000))
        self.assertEqual(self.broker.submit_count, 1)


if __name__ == "__main__":
    unittest.main()
