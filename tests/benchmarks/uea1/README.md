# UEA-1 performance protocol

This directory contains the frozen workload manifest and comparison tools for
UEA-1. The system-test runner must use the `performance` launch profile and an
explicit manifest path. The three scenarios are intentionally excluded from
the default fault-scenario run.

`workloads.json` describes the formal 1FE+3BE run. Manifest version 2 rejects
unknown fields and arbitrary producer SQL. Each of the closed `mv-refresh`,
`analyze`, and `optimize` producers owns a finite sequence of independent,
identically seeded targets. The controller prepares all targets before each
window. MV targets start unrefreshed; ANALYZE targets disable collect-on-write
and have no statistics/job history; OPTIMIZE targets contain multiple real
small files plus live v3 deletion vectors and no job history. Foreground
traffic uses a separate table.

The scenario must start and retain an isolated REST/MinIO fixture, project its
static credentials through normal FE/BE configuration, create one private
catalog, and pass `Some(MixedFixtureBinding::new(catalog)?)` to `performance::run`.
The mixed scenario rejects a missing binding. Short and slow-output scenarios
pass `None`. No controller launches an alternative server stack. The controller
creates an exclusive namespace per window and cleans its exact objects after
all jobs finish. On failure it retains them until the scenario stops the
cluster and tears down its owned storage fixture; it does not drop the target
of an unknown in-flight job.

Business duration starts before the command and ends after its success oracle:

- MV: synchronous refresh, previously absent publication marker becomes present,
  exact refreshed-row count and output aggregate, unchanged input snapshots.
- ANALYZE: the only job for its fresh target is `SUCCEEDED`; a provider Theta
  artifact describes the same input, with nonempty expected NDV and unchanged
  input snapshots. SQL acknowledgement alone is insufficient.
- OPTIMIZE: `ALTER TABLE ... OPTIMIZE`, the exact fresh job is `FINISHED`, with
  nonzero input files, a direct replacement snapshot, entirely new live data
  paths, removed live deletion vectors, fewer current physical artifacts, and
  unchanged row values. The current connector hard-codes zero in the SHOW
  output-count field, so that defect is retained as a diagnostic while
  `$files` is the physical output oracle. Failed or empty jobs do not count.

`mixed-fixture-N.json` records the initial input facts and `mixed-business-N.json`
records each proven completion, identity and publication. `mixed-business.json`
combines successful windows. Jobs completing after the window are reported as
drain work and excluded from window completions. Exhausting a producer's finite
sequence before a formal window ends invalidates the window; it never loops over
already refreshed/analyzed/optimized objects. Freeze adequate job counts from
baseline pilots before candidate measurement. These files contain fixture
identities and aggregate counts, never credentials or source rows.

`uea1-performance.json` records every measured window in the same monotonic
elapsed-millisecond domain as `process-resources.json`. Resource comparisons
select samples between `started_elapsed_millis` and `ended_elapsed_millis`;
setup, warmup, cleanup, and post-window drain samples remain diagnostic only.
Every performance and startup run also writes `run-manifest.json`. Formal runs
require a clean checkout and that checkout's exact
`target/release/novarocks`; the manifest records source, binary, config,
workload, fixture, tool tree, Cargo.lock, toolchain, platform, power mode, and
start/end identity. Formal extraction rejects a missing preparation event,
any incomplete FE/three-BE resource window, and any artifact that does not
reference the exact completed run manifest.

`workloads-smoke.json` uses the same three real business operations and oracles
with small fixtures and one job of each kind. Its closed producers may finish
before the foreground window because it is a facility self-test, not
performance evidence. `SELECT 1` cannot stand in for a producer or its completion.
Formal short-query, mixed, and slow-output workloads require at least five
fixed 120-second windows. Slow-output records control and foreground queries
as distinct metric cohorts while the throttled client remains a diagnostic
delivery observation.

`compare.py` derives the allowed relative noise from two baseline A/A sample
sets using pooled median and MAD. It rejects zero-valued positive metrics and
noise above five percent. Formal comparison uses four non-overlapping runs in
the exact order B0-A, candidate-A, B0-B, candidate-B; it verifies both source
and binary repeats before pooling the two candidate samples. First create one
descriptor beside each run's artifacts with only `artifacts`, `expected`,
`metric_resolutions`, and the closed sampled thread gates. Then run:

```bash
python3 tests/benchmarks/uea1/artifact_protocol.py extract \
  --descriptor <run>/descriptor.json --output <run>/comparison-input.json
python3 tests/benchmarks/uea1/compare.py --structured \
  --baseline-a <b0-a>/comparison-input.json \
  --candidate-a <candidate-a>/comparison-input.json \
  --baseline-b <b0-b>/comparison-input.json \
  --candidate-b <candidate-b>/comparison-input.json
```

`build_feedback.py` samples one same-tick process snapshot repeatedly and
records command duration plus peak RSS for the isolated process group and its
complete visible descendant tree. Missing samples stay unavailable rather
than becoming zero.
