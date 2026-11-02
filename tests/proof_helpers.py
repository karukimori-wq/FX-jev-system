import time
from fxjev.reconciliation import (BrokerOrder,BrokerFill,OwnedPosition,ReconciliationProof)
from fxjev.ledger import Mode,OrderStatus


def proof(client_id="o1", *, qty=5, filled=0, status=OrderStatus.CANCELED,
          strategy="s", run="r1", mode=Mode.LIVE, broker_id=None,
          observed_at_ns=None, **changes):
    now=time.time_ns()
    broker_id=broker_id or "broker-"+client_id
    order=BrokerOrder(client_id,broker_id,strategy,run,mode,qty,status,now-2_000_000)
    fills=((BrokerFill(broker_id,"exec-"+client_id,filled,now-1_000_000),)
           if filled else ())
    position=OwnedPosition(client_id,broker_id,strategy,run,mode,filled,
                           now if observed_at_ns is None else observed_at_ns)
    fields=dict(order=order,fills=fills,position=position,
                order_history_complete=True,fill_history_complete=True,
                position_snapshot_complete=True)
    fields.update(changes)
    return ReconciliationProof(**fields)
