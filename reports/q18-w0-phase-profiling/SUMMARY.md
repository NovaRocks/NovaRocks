# Q18 W0 Phase Profiling

Run date: 2026-06-23

Workspace: `/Users/harbor/.codex/worktrees/0e15/NovaRocks`

Branch: `codex/q18-w0-operator-phase-profiling`

Command shape: current `target/dev-opt/novarocks`, `sql-tests` dev-opt runner, cross-process `1 FE + 3 BE`, TPC-H q18 only, `EXPLAIN ANALYZE`, `--query-timeout 300`.

Result:

- Suite/case: `tpc-h/q18`
- Status: PASS
- Runner CPU time: 78.82s
- Runner wall time: 80.76s
- Plan header: `Planning: 100.7ms / Execution: 78.72s / Rows: 9`
- Profile header: `operator_active=315.83s network=37.69s scan_io=8.62s`
- Full plan: `reports/q18-w0-phase-profiling/plans/analyze/tpc-h/q18.result`
- Runner log: `reports/q18-w0-phase-profiling/logs/analyze-tpc-h-q18.log`

Hot join phase split:

| Node | Operator | Rows | Time | Max | Min | build_ht | search | output | Peak |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 9 | HASH JOIN PARTITIONED INNER `l_orderkey = o_orderkey` | 13,502,430 | 39.2s | 40.0s | 38.3s | 171.8ms | 212.8ms | 9.0s | 617.0MB |
| 18 | HASH JOIN PARTITIONED LEFT SEMI `o_orderkey = lineitem.l_orderkey` | 6,001,287 | 35.3s | 37.4s | 32.3s | 3.5us | 19.3us | 185.8us | 84.4KB |
| 7 | HASH JOIN PARTITIONED INNER `o_custkey = c_custkey` | 3,150,000 | 3.6s | 4.0s | 3.5s | 3.4ms | 66.3ms | 347.7ms | 55.3MB |

W1 signal:

- Node 9 has a visible `output=9.0s` component, so output/gather materialization is the clearest phase target among the three joins.
- Node 18 still spends most of its `time=35.3s` outside the newly split build/search/output sub-counters, so the next attribution step should inspect driver/blocking/exchange or uninstrumented semi-join work before optimizing a specific phase.
- The rerun did not emit negative per-driver min timings; `min` now reflects the selected operator-time source consistently.
