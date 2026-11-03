"""Offline diagnostics of known v0.27 gaps; these are not acceptance passes."""
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from fx_poc import Intent, Mode, Run, Side
from fx_poc.durable import SQLiteLedger
from fx_poc.engine import SafetyError

results = {}
with tempfile.TemporaryDirectory() as tmp:
    ledger = SQLiteLedger(Path(tmp)/'demo-scope.db')
    ledger.set_logic_limit('L',100)
    ledger.add_run(Run('live','L',Mode.LIVE,100))
    ledger.add_run(Run('demo','L',Mode.DEMO,0),demo_quantity_limit=100)
    with ledger._connect() as db:
        db.execute("INSERT INTO orders(decision_id,run_id,logic_id,pair,side,quantity,state) VALUES ('demo-order','demo','L','USD_JPY','BUY',10,'CANCELED')")
        db.execute("CREATE TABLE late_entry_fills(execution_id TEXT PRIMARY KEY,decision_id TEXT,quantity INTEGER)")
        db.execute("INSERT INTO late_entry_fills VALUES ('demo-late','demo-order',1)")
    try:
        ledger.reserve(Intent('live-order','live','USD_JPY',Side.BUY,1))
        results['demo_quarantine_blocks_live'] = False
    except SafetyError as exc:
        results['demo_quarantine_blocks_live'] = True
        results['demo_quarantine_message'] = str(exc)
    ledger = SQLiteLedger(Path(tmp)/'expired.db')
    ledger.set_logic_limit('L',100)
    ledger.add_run(Run('expired','L',Mode.LIVE,100,ends_at=datetime(2000,1,1,tzinfo=timezone.utc)))
    results['expired_run_reservation_accepted'] = ledger.reserve(Intent('expired-order','expired','USD_JPY',Side.BUY,1))[1]
print(json.dumps(results,indent=2))
