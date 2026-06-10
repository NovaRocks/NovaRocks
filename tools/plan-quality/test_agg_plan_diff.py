#!/usr/bin/env python3
"""Standalone unit test for agg_plan_diff.agg_counts (no live server needed).

Run: python3 tools/plan-quality/test_agg_plan_diff.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from agg_plan_diff import agg_counts  # noqa: E402

NR_EXPLAIN = """
PLAN FRAGMENT 0
  HASH AGGREGATE (GLOBAL) stats={rows=3}
    HASH EXCHANGE (source: ShuffleAgg)
      HASH AGGREGATE (LOCAL) stats={rows=3}
        OLAP SCAN (t)
"""

FE_EXPLAIN = """
PLAN FRAGMENT 0
  3:AGGREGATE (merge finalize)
  |  group by: 1: k
  2:EXCHANGE
  1:AGGREGATE (update serialize)
  0:OlapScanNode
"""


def main() -> int:
    nr = agg_counts(NR_EXPLAIN, "nr")
    assert nr == {"single": 0, "local": 1, "global": 1}, nr

    fe = agg_counts(FE_EXPLAIN, "fe")
    assert fe == {"single": 0, "update": 1, "merge": 1}, fe

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
