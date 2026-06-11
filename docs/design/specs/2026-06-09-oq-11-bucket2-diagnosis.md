# OQ-11 — split-selection investigation findings (corrected)

Date: 2026-06-09 → 2026-06-10
Method: systematic-debugging against the standalone server, then re-validated after rebasing onto updated `main`.

## TL;DR

The deliverable that survives is the **capability-driven eligibility oracle** that makes
`avg` (and other decomposable functions) *eligible* for two-phase Local/Global split — the
original OQ-11 Bucket-1 goal. An intermediate **cost-model hypothesis was tested and rejected**
(see below); it is NOT part of the final change.

## What was investigated

Why grouped two-phase split (and `avg` in particular) did not appear in plans.

## Misdiagnosis and correction (recorded so it isn't repeated)

- **Observed (on the standalone managed-lake config):** grouped `SUM`/`AVG` never produced a
  Local/Global split; scans reported `stats={rows=100000}` because `ANALYZE` did not register
  real row counts for managed-lake tables. At that (fake) size, the flat 16 MiB
  `DISTRIBUTION_STARTUP_COST` made the Local→Global shuffle enforcer dominate, so the CBO chose
  single-phase.
- **Hypothesis (prototyped, then REJECTED):** waive the startup for `ShuffleAgg` enforcers so
  split wins. A controlled experiment confirmed the constant *controls* split selection — but it
  did **not** validate that split was the *correct* choice for that data.
- **Correction (from `main` #277 "migrate sql-test suite to iceberg base tables"):** the
  optimizer suite now runs on iceberg base tables with **real stats**, and explicitly establishes
  that *on small data the optimizer correctly chooses single-phase; two-phase split triggers at
  scale and is covered by the ssb/tpc-* benchmark suites*. So `rows=100000` was the real artifact
  (fixed at the root by #277), and single-phase **is** correct for small tables. Forcing the
  `ShuffleAgg` exemption made split appear on tiny tables and **broke 8 optimizer goldens**
  (with-fix = 9 failures; reverted = 1). The exemption was reverted.

## The valid contribution (final change)

- **Capability oracle** (`src/sql/agg_mergeability.rs`): single source of truth for two-phase
  split eligibility, replacing the hardcoded `sum|min|max|count` whitelist in
  `SplitAggregateRule`. Enables `avg` (distinct/ordered/order-sensitive guarded; drift-guarded
  against the planning-layer type inference). At scale, the unmodified cost model two-phases
  `avg` like `sum` (FE parity for Bucket-1). The eligibility is unit-tested
  (`splits_grouped_avg_aggregate`); split *at scale* is covered by the benchmark suites.
- **Measurement harness** (`tools/plan-quality/agg_plan_diff.py`): FE-vs-NR aggregate split-marker
  diff for future re-measurement.
- **Cleanup**: unified the duplicated `NETWORK_COST` / `DISTRIBUTION_STARTUP_COST` constants into
  `cost.rs` (the duplication was a real drift footgun surfaced during the investigation).

## Remaining OQ-11 work (follow-ups)

1. **Set-op branch-local aggregate pushdown** (push partials *into* `UNION` branches; q75 count
   parity) — a distinct AggregatePushdown feature; its own plan.
2. **FE-count re-measurement at scale** with the harness (loaded tpch/tpcds + a live FE) to size
   any remaining tail now that `avg` is eligible.
