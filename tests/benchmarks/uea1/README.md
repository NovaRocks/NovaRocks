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

`mixed-fixture-N.json` records the initial input facts, `mixed-business-N.json`
records each proven completion, identity and publication, and `mixed-query-N.json`
preserves foreground samples even when a worker fails. `mixed-business.json`
combines successful windows. Jobs completing after the window are reported as
drain work and excluded from window completions. Exhausting a producer's finite
sequence before a formal window ends invalidates the window; it never loops over
already refreshed/analyzed/optimized objects. Freeze adequate job counts from
baseline pilots before candidate measurement. These files contain fixture
identities and aggregate counts, never credentials or source rows.

`uea1-performance.json` records every measured window in the same monotonic
time domain as `process-resources.json`, with microsecond window boundaries and
millisecond process samples. Resource comparisons
select query samples against the exact `started_elapsed_micros` and
`ended_elapsed_micros` boundaries; process-resource samples retain their native
millisecond resolution;
setup, warmup, cleanup, and post-window drain samples remain diagnostic only.
Every performance and startup run also writes `run-manifest.json`. Formal runs
require a clean checkout and that checkout's exact
`target/release/novarocks`. After the real cluster starts, the runner reads the
structured `SHOW BACKENDS` projection, requires all three live BEs to report
one embedded native build identity equal to the source revision, and records
that identity in run-manifest schema 4. Smoke runs record one uniform live-BE
identity without requiring source equality. The manifest also records source,
binary, the canonical runner executable and its hash, config, workload, fixture,
tool tree, raw Cargo.lock, third-party build graph, toolchain, platform,
power mode, and start/end identity. Raw Cargo.lock may differ between B0 and
candidate but must repeat exactly within each source; the provider-independent
third-party build graph must match across all four runs. That graph is the
current-target `normal,build` closure reached from the server and system-test
runner roots as reported by `cargo tree --locked`. It binds each reachable
external package's version, source, checksum, active feature set, and external
dependency edges. Workspace packages are transparent so a crate split or rename
does not change the graph by itself; a reachable non-workspace path dependency
is instead bound by a content hash. This deliberately describes the two measured
build roots rather than Cargo metadata's workspace-wide unified feature set.
Formal runs require the runner itself to be this checkout's release executable.
The tool-tree hash covers the complete UEA-1 benchmark tree, system-test runner
source and manifest, and cluster-harness source and manifest. Formal extraction rejects a missing preparation event,
any incomplete FE/three-BE resource window, and any artifact that does not
reference the exact completed run manifest. Performance runs also bind the
canonical descriptor, a secret-free semantic projection of the rendered FE/BE
configs, and a recomputable fixture realization. `run-completion.json` is written
last and binds every required artifact hash; its absence means the run is incomplete.
`process-resources.json` schema 2 contains the run identity, the exact
`(role, pid, process_start_token)` process set, and its samples. The run manifest
also binds every role to the frozen executable hash, size, mtime, and process
birth token. Every sample must name that exact process instance, so PID reuse is
rejected. Both the performance report and run manifest bind the resource file
hash. If execution fails, partial resource samples remain available for diagnosis
without producing a completion marker.

Preparation measurements use an authenticated, run-scoped diagnostic prelude.
The runner arms the otherwise absent collector with the run-manifest identity,
executes the scenario's real preparation paths, drains and disarms it, and only
then starts timed work. `uea1-performance.json` records the diagnostic range in
the same monotonic domain as every timed window. Formal extraction requires the
exact run token and rejects any overlap. The launch secret is process-local and
is never written to the artifact.

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
and binary repeats before pooling the two candidate samples. Place one
descriptor beside each run's artifacts with only `artifacts`, `expected`,
`metric_resolutions`, and the closed sampled thread gates. The extract command
creates a derived report for review. Formal comparison reads the descriptors
and re-extracts every value from the hash-bound raw artifacts; it never accepts
the editable derived report as evidence. All four runs must pass every absolute
gate, including the two B0 preflight runs:

```bash
python3 tests/benchmarks/uea1/artifact_protocol.py extract \
  --descriptor <run>/descriptor.json --output <run>/comparison-input.json
python3 tests/benchmarks/uea1/compare.py --structured \
  --baseline-a <b0-a>/descriptor.json \
  --candidate-a <candidate-a>/descriptor.json \
  --baseline-b <b0-b>/descriptor.json \
  --candidate-b <candidate-b>/descriptor.json
```

`build_feedback.py` samples one same-tick process snapshot repeatedly and
records command duration plus peak RSS for the isolated process group and its
complete visible descendant tree. Missing samples stay unavailable rather
than becoming zero.

The checked-in descriptors under `descriptors/` freeze the formal scenario
shape, resolutions, role set, and current-source thread ceilings. The runner
copies the matching canonical bytes into each performance artifact directory as
`descriptor.json`; formal extraction resolves the closed scenario mapping and
rejects any descriptor whose bytes differ from that checkout's canonical file.
Do not rewrite its relative artifact paths. The thread ceilings follow
`1 + configured pool maxima + fixed service threads`: FE is
`1 + 1106 + 7 = 1114`, and each BE is `1 + 103570 + 7 = 103578`. Legacy
per-query, per-Task, per-Fragment, and provider bridge threads are excluded from
the fixed-service term but remain visible in the sampled process total. Tighter
candidate architecture limits require their own concurrency gate after those
dynamic thread sources are removed.

Before comparing a candidate, validate each scenario's two B0 runs on their
own:

```bash
python3 tests/benchmarks/uea1/compare.py --baseline-noise \
  --baseline-a <b0-a>/descriptor.json \
  --baseline-b <b0-b>/descriptor.json
```
