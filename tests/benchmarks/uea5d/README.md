# UEA-5D startup baseline

This directory analyzes the startup part of D00. It is not an LP, CREATE,
EXCHANGE, or CAPACITY acceptance gate by itself.

The shared native 1FE+3BE system scenario
`task-execution/uea5d-startup-baseline` uses the existing shallow and deep SQL
fixtures plus fixed UNION shapes with 4/8/16 branches for static plan width.
These constant inputs do not establish a multi-BE or DOP scaling baseline.
It records five warm-ups and 200 measured runs per fixture,
including every first-row and total latency. Its performance launch profile requires a
clean checkout and the checkout-local release server and runner binaries.
Set `ARTIFACT_ROOT_A` and `ARTIFACT_ROOT_B` to distinct absolute directories
outside the checkout; formal provenance requires a clean source tree.

```bash
cargo build --locked --release -p novarocks-server -p novarocks-system-test-runner

target/release/novarocks-system-tests \
  --only task-execution/uea5d-startup-baseline \
  --binary target/release/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$ARTIFACT_ROOT_A" \
  --cluster-size 3 --timeout-secs 1800 --launch-profile performance

target/release/novarocks-system-tests \
  --only task-execution/uea5d-startup-baseline \
  --binary target/release/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$ARTIFACT_ROOT_B" \
  --cluster-size 3 --timeout-secs 1800 --launch-profile performance

python3 tests/benchmarks/uea5d/analyze_startup.py \
  --a "$ARTIFACT_ROOT_A/task-execution-uea5d-startup-baseline" \
  --b "$ARTIFACT_ROOT_B/task-execution-uea5d-startup-baseline" \
  --output "$ARTIFACT_ROOT_B/startup-aa.json"
```

The analysis rejects incomplete samples, duplicate run identities, and changed
server/runner binary SHA256, source/configuration identity, frozen SQL, or plan
shapes. It uses nearest-rank p50/p95/p99, matching the system scenario report.
It reports A/A spread without choosing a pass threshold.
Freeze numeric candidate gates only after examining these two independent
runs and the other D00 capacity and phase measurements. Keep raw artifacts and
their hashes with the plan execution receipt.
