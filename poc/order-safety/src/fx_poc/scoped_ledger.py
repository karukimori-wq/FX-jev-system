"""Run-bound namespace boundary for new callers; legacy APIs remain available."""
from .identifiers import scoped_id
from .models import Intent
from .engine import SafetyError


class RunLedger:
    def __init__(self, ledger, run_id):
        self.ledger = ledger
        self.run_id = run_id
        self.mode = ledger.run_mode(run_id)

    def identifier(self, kind, source):
        return scoped_id(self.mode, self.run_id, kind, source)

    def reserve(self, decision, pair, side, quantity):
        return self.ledger.reserve(Intent(self.identifier('decision',decision),self.run_id,pair,side,quantity))

    def _owned(self, table, key, identifier):
        with self.ledger._connect() as db:
            row = db.execute(f'SELECT run_id FROM {table} WHERE {key}=?',(identifier,)).fetchone()
        if row != (self.run_id,):
            raise SafetyError('unknown or foreign owned record')

    def ingest_entry_fill(self, decision, execution, quantity):
        order = self.identifier('decision',decision)
        self._owned('orders','decision_id',order)
        return self.ledger.ingest_entry_fill(order,self.identifier('execution',execution),quantity)

    def filled_position(self, execution):
        position = 'fill-' + self.identifier('execution',execution)
        self._owned('positions','position_id',position)
        return position

    def reserve_close(self, close, position_id, quantity):
        self._owned('positions','position_id',position_id)
        return self.ledger.reserve_close(self.identifier('close',close),self.run_id,position_id,quantity)

    def ingest_close_fill(self, close, execution, quantity):
        close_id = self.identifier('close',close)
        self._owned('closes','close_id',close_id)
        return self.ledger.ingest_close_fill(close_id,self.identifier('execution',execution),quantity)


    def reconcile_terminal_entry(self, decision, candidates):
        order = self.identifier('decision', decision)
        self._owned('orders', 'decision_id', order)
        # Evidence must already contain internal IDs; never rewrite a foreign
        # candidate into evidence for this run.
        return self.ledger.reconcile_terminal_entry(order, candidates)

    def reconcile_terminal_close(self, close, candidates):
        close_id = self.identifier('close', close)
        self._owned('closes', 'close_id', close_id)
        return self.ledger.reconcile_terminal_close(close_id, candidates)


class RunEngine:
    """Run-bound submission path using the durable engines' existing gates."""
    def __init__(self, ledger, broker, run_id):
        from .durable import DurableEngine, DurableCloseEngine
        self.scope = RunLedger(ledger, run_id)
        self.entries = DurableEngine(ledger, broker)
        self.closes = DurableCloseEngine(ledger, broker)

    def submit_entry(self, decision, pair, side, quantity):
        scope = self.scope
        return self.entries.submit_intent(Intent(
            scope.identifier('decision', decision), scope.run_id, pair, side, quantity))

    def submit_close(self, close, position_id, quantity):
        scope = self.scope
        scope._owned('positions', 'position_id', position_id)
        return self.closes.submit(scope.identifier('close', close), scope.run_id, position_id, quantity)

    def stop(self, reason='MANUAL'):
        self.scope.ledger.stop_run(self.scope.run_id, reason)

    def process_cancellations(self):
        """Process only this run's queued entry cancellations, including legacy IDs."""
        ledger = self.scope.ledger
        with ledger._connect() as db:
            pending = db.execute(
                "SELECT c.decision_id FROM cancel_requests c JOIN orders o ON o.decision_id=c.decision_id WHERE o.run_id=? AND c.status='PENDING' ORDER BY c.decision_id",
                (self.scope.run_id,)).fetchall()
        sent = 0
        for (decision_id,) in pending:
            self.scope._owned('orders', 'decision_id', decision_id)
            sent += bool(ledger.send_cancel(decision_id, self.entries.broker))
        return sent
