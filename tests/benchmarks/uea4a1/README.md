# UEA-4A-1 warehouse-concurrency performance protocol

`workload.json` is the frozen P00 load record for UEA-4A-1. The native
system-test runner owns the controller at
`tests/system-test-runner/src/scenarios/query_concurrency.rs`. It executes the
same workload against a B0 binary and a candidate binary; only the profile's
documented FE workload policy differs.

Each run starts a native 1FE+3BE cluster and executes three 120-second normal
windows followed by three 120-second saturated windows. The normal workload
has one client and a fixed 20 ms think time. The saturated workload has four clients while the candidate
warehouse limit is three, so it must observe the one fair query-admission
queue. The one-row `sleep` query bounds BE memory and makes queueing
observable without introducing a CPU benchmark.

The scenario writes `uea4a1-performance.json`, process thread/RSS samples in
`process-resources.json`, and the runner's hash-bound `scenario-evidence.json`.
The latter records binary path, source revision/tree state, platform, rendered
role configuration and the native process identities. Run the controller with
release binaries and the performance launch profile:

```bash
target/release/novarocks-system-tests \
  --binary <b0-release-novarocks> \
  --config "$PWD/tools/ci/fixtures/system-scenarios-base.toml" \
  --cluster-size 3 --timeout-secs 1200 --launch-profile performance \
  --artifact-root "$PWD/reports/uea4a1-b0" \
  --only query-concurrency/uea4a1-b0-performance

target/release/novarocks-system-tests \
  --binary "$PWD/target/release/novarocks" \
  --config "$PWD/tools/ci/fixtures/system-scenarios-base.toml" \
  --cluster-size 3 --timeout-secs 1200 --launch-profile performance \
  --artifact-root "$PWD/reports/uea4a1-candidate" \
  --only query-concurrency/uea4a1-candidate-performance
```

Compare normal windows as the regression signal. Saturated windows report the
different policies: B0 may admit more logical work while the candidate must
keep admitted queries at or below three and report waiting work. Do not treat
their throughput as equivalent capacity.
