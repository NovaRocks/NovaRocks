# NovaRocks system scenarios

`novarocks-system-tests` is the imperative, process-boundary test frontend.
It owns scenario discovery, actors, bounded deadlines, oracle composition and
failure reporting. `novarocks-cluster-harness` remains the only owner of
1FE+NBE configuration, spawn, readiness, topology, faults, restart, logs and
cleanup.

List all registered scenarios (including explicit stages):

```bash
cargo run -p novarocks-system-test-runner -- --list
```

List the default functional baseline:

```bash
cargo run -p novarocks-system-test-runner -- --list-default
```

The default list uses the same registry classification as a run without
`--only`. Explicit stages, including performance baselines and external
fixtures, remain discoverable through `--list` and selectable by exact `--only`.

Run one scenario against the native 1FE+3BE default:

```bash
cargo build --workspace --profile dev-opt
cargo run -p novarocks-system-test-runner --profile dev-opt -- \
  --binary target/dev-opt/novarocks \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" \
  --cluster-size 3 \
  --timeout-secs 300 \
  --only query-lifecycle/mysql-disconnect
```

Most scenarios build their Iceberg warehouse on the local filesystem under
the harness runtime directory. The vended credential scenarios start an
isolated Iceberg REST and MinIO fixture and require the fixture image and
Docker service. The base config supplies SQLite StateStore and no shared
`[connector.object_store]` credentials. The generated
`$NOVAROCKS_FE_CONFIG` / `$NOVAROCKS_BE_CONFIG` pair also works
when started through the exact `--role all-in-one --fe-config ... --be-config
...` command; it adds object-store settings that local-filesystem scenarios
do not read.

The consumer credential acceptance cases are
`connector/vended-credential-refresh`,
`connector/vended-credential-targeted-deadline`,
`connector/vended-credential-late-close`, and
`connector/vended-credential-unreachable`. Run each with an exact `--only`
filter and `--cluster-size 3 --timeout-secs 180`. The fixture identifies the
BE principal and holds its refresh only after that request arrives. The
late-close case enables a debug-only, exact-authority trigger on BE[0].

Scenarios run sequentially. On failure the runner prints action history,
process diagnostics, the retained runtime/log directory and an exact rerun
command. Successful scenarios explicitly stop their own process group and
remove the generated runtime directory.

## In CI

`tools/ci/local-full-ci.sh` runs this registry as its own stable stage,
between the server binary smoke and the SQL suites. The stage discovers
default scenarios through `--list-default` and runs each one with a single `--only`
invocation, so every scenario gets an independent summary row, log and
artifact directory. It selects and reports only — `tools/ci/lib/system_scenarios.sh`
holds no cluster lifecycle of its own.

The same `query-lifecycle/distributed-baseline` scenario is reused by the
FoundationDB and MySQL StateStore provider gates as a feature-binary
coexistence smoke. Read it narrowly: it proves a feature-enabled binary still
completes a standard native topology, query and cleanup, not that any query
used that provider.

The `native-ingress/*` scenarios require the real 1FE+3BE launch. They send
authenticated raw gRPC requests to a BE to check header-stage refusal,
pre-prost resource checks, and the ordinary/control message boundaries. The
current-gate case reads the BE management `/metrics?type=json` surface before,
during, and after a runner-held ordinary Worker closure, so an available gate
can be distinguished from an unknown, unimplemented source. The explicit
`blocking-saturation-control` case fills the ordinary blocking pool and checks
that a small Cancel still completes while a ninth ordinary request waits.
`resident-envelope-calibration` records coarse BE process RSS before, during,
and after one legal 48 MiB outer request plus seven small-body closures in
`process-resources.json`. Its padding is a legal protobuf unknown field, not a
retained FrozenFragment carrier; it does not establish a hard RSS bound or a
mixed ordinary/control peak.
`async-scheduling-pressure` runs four distributed SQL queries, waits for real
Exchange shuffle bytes while one remains active, and checks a small control
receipt; it is a liveness check under ordinary async work, not proof that all
async worker threads can be saturated safely.
`partial-body-deadline` leaves a gRPC request body half open and checks bounded
termination, the `running_deadline` counter, holder release, and a later
control receipt. HTTP/2 may end that half-open stream with a reset, leaving no
readable gRPC status; a fully received unary request has separate status checks.
`registry-contention-control` uses a runner-owned, debug-only rendezvous while
the Worker registry mutex is held. It distinguishes an already-started control
executor job waiting for that mutex from a control job still queued at ingress,
then checks the receipt and lock-wait observation after release.
Each selected case records its individual probes in `scenario-evidence.json`;
the scenario must execute at least one probe to count as passed. These are
correctness and coarse regression checks, not throughput benchmarks.

The `native-creation/*` scenarios also require the real 1FE+3BE launch and
check frozen task creation across the process boundary. An Accepted receipt
proves Worker ownership; Installed is a separate runtime-installation fact.
The `accepted-preparation-control-races` case checks preparation/control races
and authenticated covered-observation refusals. Native subscriptions require
nonzero generations; cursor-only requests cannot enter the production stream.
Preparation count, byte and per-context position charges remain until the job
actually exits. FE deployment positions remain occupied after Accepted or an
unknown RPC outcome, and its window W must fit the exact backend's advertised
per-context preparation capacity P. These are scenario and implementation
contracts; a listed scenario does not establish a passing acceptance gate.
`frozen-replay-and-membership` sends authenticated raw creates to one BE: a
legal create, the identical request, and the same task identity with every
body fact changed. Both replays must retain the winner's exact entity and
return its current monotonic status, with no second apply marker and no lease
renewal. Creates under another
frontend process, another attempt or another backend's identity must read no
receipt. An initial domain naming an edge the descriptor never froze fails
preparation after Accepted; that spent identity remains retained, so a later
legal body cannot replace it. A replay after the context is released must
apply nothing.
`creation-payload-lifetime` reads the FE task-creation gauges, which fall only
when a payload's last holder drops it. An answered create must release its FE
replay payload while its statement still runs, and the static plans must stay until
the statement ends. With `create-task-ack-drop` armed, the lost
acknowledgement must be resent as the same frozen create and answered by
identity; every task must be priced, frozen and applied exactly once. A
cancelled statement must release everything it froze. BE preparation input and
its P/count/bytes charge remain owned until the actual preparation job exits;
these FE gauges do not prove BE budget release.
`fixed-plan-recovery` runs one delayed read cleanly and once with its admitted
BE killed before any row is read. The recovered run must complete on attempt
2 and freeze exactly as many static plans as the clean run, so the recovery
attempt encoded none of its own. A full per-target transport window cannot be
produced at the default task transport budget without a capacity probe. That
cross-target admission is therefore checked by the frontend's real admission
pass and transport supervisor composition tests, not by these scenarios.
