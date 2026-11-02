"""Broker-free demo bootstrap: always fences old sessions before doing anything.

This entrypoint performs NO trade execution, authentication, or broker I/O.
It intentionally never automatically grants order-admission approval.
"""
import argparse
import json

from .startup import TradingRuntime


def main(argv=None) -> int:
    parser=argparse.ArgumentParser(
        description="Initialize fail-closed SQLite demo runtime (no broker connections)")
    parser.add_argument("--db",required=True,
                        help="Path to local SQLite P0 test ledger")
    args=parser.parse_args(argv)
    runtime=TradingRuntime(args.db)
    print(json.dumps({
        "engine":"broker-free P0 demo only",
        "generation":runtime.generation,
        "recovered_intent_count":len(runtime.recovered_ids),
        "admissions":"blocked until explicit mock DEMO approval",
        "live_trading":"disabled",
    },sort_keys=True))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
