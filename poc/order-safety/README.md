# FX order safety PoC v0.11.0

Python 3.11+, standard library only. Local, keyless prototype. No account API,
Jev client, price feed, web UI, or real trading is connected.

## Run

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Durable SQLite path

`SQLiteLedger` persists runs, entry/close orders, partial fills, owned positions,
stop reasons, startup review and approval evidence. Unknown orders retain their
reservation and repeated decisions never resend. `MockWorker` stops all existing
runs on startup; mock review tokens guard against local snapshot changes.

### D067 aggregate live quantity limit

Configure `ledger.set_logic_limit(logic_id, live_cap)` explicitly (integer
0..1,000,000). This setting is distinct from each live run's positive
`Run.max_quantity`. Missing configuration rejects new live entries, including
on existing databases; migration never infers a cap from old run settings.

Under one `BEGIN IMMEDIATE` transaction, each entry checks both its run cap and
the same logic's live exposure across **all** runs, including stopped old runs:

- sum of owned remaining positions;
- plus unfilled quantity of SUBMITTING, UNKNOWN, OPEN, PARTIAL, CANCEL_PENDING entries;
- no buy/sell or currency-pair netting; demo runs excluded.

Partial fills move quantity from reservation into positions without double
counting. Only confirmed cancellation releases the unfilled remainder. Pending
closes do not release held exposure until close fills arrive. Cap changes are
persisted with history and invalidate worker review tokens. Lowering a cap blocks
new additions while preserving positions and permitting owned closes. Cap reduction now records cancellation requests for all outstanding live entries of the logic.

### Demo isolation

Create a separate run with `Mode.DEMO`, `max_quantity=0`, and explicit
`demo_quantity_limit` (positive integer <=1,000,000) for simulation sizing. Zero
selects demo; it does not imply unlimited simulated quantity. Live runs cannot
have zero cap or demo settings. Run mode/ownership are immutable through the API;
mode changes require a new run ID. Demo orders and closes route to a built-in,
no-I/O `DemoBroker`, never the injected external adapter. Their orders, fills,
positions, close reservations and simulation settings remain run-scoped in the
same database. Demo fills are explicit test events, not market simulation.

The in-memory `Engine` remains the original acceptance-test reference; the new
aggregate and demo guarantees apply to `SQLiteLedger` + `DurableEngine` /
`DurableCloseEngine` and `MockWorker`, not the old in-memory path.

## Validation and limits

Baseline v0.9 existed at the handoff path and passed all 21 tests before edits.
v0.10 adds 11 targeted tests (32 total), covering cross-run parallel connections,
stopped old runs, gross opposite-side/pair exposure, partial fills/cancellation,
independent run cap, missing/zero aggregate limits, cap reduction with closes,
demo open/close isolation and reopening, simulation settings, invalid input,
legacy database migration and stale review after settings change.

Existing 21 scenarios are retained; the durable fixture now explicitly sets its
logic cap. Tests use SQLite reopening and threads, not power-loss or multi-host
validation. Startup review is mock-only and proves no external reconciliation.
No profitability, real broker behavior, production authorization, market-based
demo execution, performance accounting, account-wide yen limits, mandatory stop
protection, end-time enforcement or production deployment is established.


## v0.11: stop / send serialization and cancellation queue

A canonical-path adjacent Unix `flock` gate serializes open dispatch, run stop,
startup stop scan and live-cap changes. A stop/cap reduction that wins first
locally cancels an unattempted reservation without calling an adapter. If send
wins first, control waits for that mock adapter call to finish and then records
a cancellation request. This is local same-file Unix coordination; it is not a
multi-host protocol, and a hung adapter can delay stop completion. Adapters must
have bounded I/O and must not recursively invoke control APIs. Hard-link aliases,
network filesystems and bypassing this API are unsupported.

A dispatch claim commits before adapter I/O. Reopening after that claim never
resends, including the crash-before-actual-send ambiguity. Existing databases
are conservatively migrated by marking every preexisting order attempted. Only
new, provably unattempted SUBMITTING reservations may be locally discarded.

`DurableEngine.process_cancellations()` drives persisted requests through
PENDING -> SENDING -> ACKNOWLEDGED / UNKNOWN. No automatic retry follows a claim,
acknowledgement or ambiguity. The adapter ACK never frees the quantity; an
explicit confirmed cancellation or full fill does. Partial fills during pending
cancellation remain counted. Full entry fills take precedence over cancellation
and preserve the resulting position. Demo cancellations use the no-I/O adapter.
Review tokens include cancellation and dispatch evidence.

Eight additional tests bring the total to 40. They verify stop-before-send,
cap reduction before send with demo exclusion, ACK plus partial fill, full fill
winning the cancel race, unknown cancellation after reopen, durable dispatch
claim replay prevention, a blocked send concurrent with stop, and persisted
pending cancellation after reopen. These are thread/connection tests, not
power-loss or multi-process crash tests.

Remaining: trusted terminal-order reconciliation (including UNKNOWN entries and
SENDING/UNKNOWN cancels), out-of-order fill/terminal events, real adapter timeout
behavior and distributed control. Callers must run the cancellation processor;
there is no autonomous scheduler/service yet. An unknown entry without an
external order ID is queued for reconciliation, never blindly canceled or resent.

## v0.12 terminal entry reconciliation
Restored from the saved v0.11 ZIP; baseline 40 tests passed. Now 42 tests pass.
reconcile_terminal_entry accepts exactly one trusted mock terminal candidate matching
decision, owning run, quantity, cumulative recorded fills and external ID.
No missing fills are synthesized. Rejected evidence retains reservations. Valid
cancellation releases unfilled quantity while preserving owned positions.
Audit evidence is persisted transactionally. This is not a real broker lookup.
Missing executions, delayed events and terminal close reconciliation remain pending.

## v0.13 terminal close reconciliation
44 tests pass. reconcile_terminal_close requires a unique trusted mock candidate
matching close ID, run owner, position ID, requested quantity, recorded cumulative
fills and external ID. Missing executions are not synthesized. Cancellation frees
only the unfilled close reservation, never adjusts holdings again. Evidence and
state commit atomically. Live broker lookup and delayed fill recovery remain pending.

## v0.14 late entry event quarantine
Baseline 44 tests verified; 46 tests pass. ingest_entry_fill records previously
unseen terminal-order fills in late_entry_fills and stops the affected logic.
Recorded execution replays remain idempotent. New entries for that logic are
blocked even for new runs or after mock stop clearance. No quantities are guessed
or silently overwritten. This is quarantine only: evidence-based correction and
release, close-fill quarantine, and routing all adapter events here remain pending.

## v0.15 quarantined canceled-entry fill repair
48 tests pass. repair_late_entry_fill matches execution/order/run/external IDs,
quantity, cumulative fills and terminal state to trusted mock evidence. It adds
the missing execution and owned position atomically, retaining quarantine and
new-entry stops. Replays are idempotent; injected position-write failure rolls
back the execution too. Only CANCELED entries with known external IDs are supported.
Automatic clearance, rejected-order repair, close-fill repair, multi-fill history
reconciliation and real broker verification remain pending.

## v0.16 late close-fill quarantine
Baseline 48 verified; 50 tests pass. ingest_close_fill persists unseen terminal
close executions without guessing remaining position changes, and stops all runs
of the affected logic. New-run reservations are also blocked by quarantine.
Recorded fills remain idempotent; conflicting duplicate payloads are rejected.
Close-fill repair/clearance and adapter-wide event routing remain pending.

## v0.17 canceled close late-fill repair
52 tests pass. repair_late_close_fill requires exact trusted mock execution,
close, run, position, external ID, quantity, cumulative fill and terminal evidence.
It atomically records the execution, reduces owned holdings and updates close
state with repair evidence. Replays do not reduce holdings again. Insufficient
holdings or mismatches reject repair. Quarantine and new-entry stops remain.
Only CANCELED closes with known external IDs are supported. Competing close
reservations, explicit clearance and live account verification remain pending.

## v0.18 competing close reservation detection
53 tests pass. Late close repair atomically records pending close quantity versus
remaining holdings when competing reservations exceed the repaired position.
Further reserve_close requests for that position reject until review. Existing
orders are retained; automatic cancellation, dispatch fencing for already-reserved
closes and conflict clearance remain pending. This is conflict detection and
additional reservation blocking, not complete broker-side over-close prevention.

## v0.19 close pre-dispatch recheck
54 tests pass. DurableCloseEngine uses the same local dispatch gate as late
repairs, checks position ownership, remaining quantity versus all active close
reservations and persisted conflicts before sending. Unsafe new reservations
become locally REJECTED without broker I/O. Gate remains held through mock send.
Legacy crash ambiguity, already-sent cancellation, direct lower-level mutations
and real broker concurrency remain outside this guarantee.

## v0.20 conflicting sent-close cancellation
55 tests pass. Explicit queue_conflicting_close_cancels records requests for
known-external-ID OPEN/PARTIAL/CANCEL_PENDING closes on a recorded conflict.
send_close_cancel persists SENDING before I/O, records ACKNOWLEDGED/UNKNOWN and
never automatically retries. ACK does not free reservations. Demo performs no
external call. Automatic scheduling, UNKNOWN-ID lookup, request-status terminal
synchronization and conflict clearance remain pending.

## v0.21 terminal close / cancel-request synchronization
Baseline 55 verified; 56 tests pass. Exact terminal close reconciliation also
sets the associated cancel request to CONFIRMED_CANCELED/REJECTED/FILLED in the
same transaction. Mismatches preserve request state. Reopening never resends
confirmed cancellations. Conflict records and review stops remain unchanged.
Direct transition_close events are not yet routed through this evidence API.

## v0.22 explicit close conflict clearance
57 tests pass. clear_close_conflict requires actor/reason, no SUBMITTING/UNKNOWN
closes, no unresolved cancellation requests and pending quantity <= holdings.
Clearance evidence and conflict removal commit atomically. New-entry stops and
late-fill quarantine are unchanged. Local mock review only, not external account
reconciliation or production authorization.

## v0.23 clearance audit hardening
Restored saved v0.22 and verified 57 tests. Now 59 tests pass. Clearance audit
records UTC database timestamp with migration of existing audit tables. Injected
conflict-delete failure rolls back the entire clearance; retry records one audit.
Unknown cancellation blocks clearance even when reservation quantity fits.
Production authentication and external reconciliation remain unimplemented.

## v0.24.0: logic-wide worker review evidence

Resume review tokens now cover all runs sharing the logic, including holdings,
reservations, cancellation status, late fills, repair evidence, reconciliation,
close conflicts and clearance audits. Optional legacy tables may be absent.
Evidence changes invalidate a previously issued token in the approval transaction;
unrelated logic changes do not. This does not clear quarantines or authorize live trading.

Validation: `PYTHONPATH=src python -m unittest discover -s tests -v` — 63 tests.

## v0.25.0: logic-wide resume blocking

A current review token is necessary but insufficient for approval: unresolved
SUBMITTING/UNKNOWN/CANCEL_PENDING orders or closes, non-startup active stops,
unconfirmed entry/close cancellations, late-fill quarantine or close conflicts
in any run sharing the logic reject resume. Rejection rolls back without clearing
startup stops, enabling runs or writing approvals. Confirmed cancellations permit
review; approval enables only the requested run. Checks intentionally cover all
modes sharing a logic conservatively; demo/live quantities and broker routing
remain separate. Late-fill repair does not automatically release quarantine.

Validation: 68 tests pass. No real broker or GitHub integration performed.

## v0.26.0: serialized resume and startup review generations

Worker approval, legacy mock resume and mock stop clearance now acquire the same
local dispatch gate as stop/startup and broker dispatch before opening a write
transaction. Review is rechecked after acquiring the gate. Each startup increments
a durable singleton generation in the startup transaction; review tokens include
this generation so identical stopped states across restarts cannot reuse old review.
Approval-before-stop still allows the later stop to disable the run and discard
unsent entries. These guarantees require the same local SQLite path and cooperative
APIs; this is not distributed locking or production broker authorization.

Validation: 71 tests pass; evidence/tests-v0.26.txt includes the full result.

## v0.27.0: shared resume risk predicate

Worker approval and legacy mock resume share one transaction-local predicate for
all runs of a logic: unresolved entry/close states, active stops, cancellation
uncertainty, late-fill quarantine and close conflicts block both entry points.
Worker approval alone may clear STARTUP_REVIEW after validating a current token.
Once worker startup has occurred, legacy tokenless resume is refused even after
all startup stop reasons were manually cleared. Pre-worker clean mock workflows
remain supported. This retains the conservative all-mode review scope.

Validation: 75 tests pass; full evidence is in evidence/tests-v0.27.txt.

## v0.28.0: mode-scoped quarantine and resume

Late entry/close fills stop only runs with the same logic and mode. Reservation
quarantine checks and resume risks use the same scope. Review snapshots exclude
the other mode and exclude live cap history from demo review.

`MockWorker(..., mode=Mode.LIVE)` or `Mode.DEMO` starts/reviews only that mode and
refuses cross-mode use. Omitting mode deliberately starts both modes for backward
compatibility. Startup generations are per mode; legacy global startup evidence
is conservatively migrated to both modes. Existing live multi-run caps remain.

83 mock tests pass. The v0.27 isolation diagnostic now reports false for demo
quarantine blocking live; expired-run acceptance remains true (not addressed).
Global ID keys, shared local DB/dispatch lock, P&L and real broker infrastructure
remain outside complete demo/live isolation. Issue #1 stays open.
