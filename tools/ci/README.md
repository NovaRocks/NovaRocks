<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Rust test entry and retained execution materials

Project build/test entries default Cargo compilation to
`max(1, logical_cpu_count // 2)` jobs, computed on each host. For example, a
10-core host defaults to five jobs. An explicitly set `CARGO_BUILD_JOBS` is
preserved, and native Cargo `-j` / `--jobs` keeps its usual precedence. Set
`CARGO_BUILD_JOBS=8 tools/ci/dev-test.sh test` to override compilation jobs;
nextest's `--test-threads` / `-j` separately controls test execution. Bare Cargo
commands retain Cargo's own defaults. The development profile remains unchanged.

`tools/ci/dev-test.sh test` prepares the workspace's development graph and then
executes native nextest in the foreground. Nextest owns per-test processes,
parallel scheduling, TTY progress, input and job control. It requires nextest
**at least 0.9.148**, using the native `nextest-version.required` configuration.
`NOVA_CI_NEXTEST_BIN` can select an isolated `cargo-nextest` executable. Formal
tool preparation and the governance workflow pin **0.9.148**; ordinary development
also accepts compatible newer versions.

```bash
tools/ci/dev-test.sh test
tools/ci/dev-test.sh list -E 'package(novarocks-test-support)'
tools/ci/dev-test.sh test -E 'package(novarocks-test-support) & test(caller_exit)'
tools/ci/dev-test.sh test -E 'binary(process_monitor)'
tools/ci/dev-test.sh test --run-ignored only -E \
  'package(novarocks-cluster-harness) & test(native_proxy_cluster_keeps_advertisement_across_process_restarts)'
tools/ci/dev-test.sh test -E 'rdeps(novarocks-test-support)' --test-threads 2
tools/ci/dev-test.sh doc
tools/ci/dev-test.sh check --target-dir target/ide
```

`package()`, `binary()`, `test()` and `rdeps()` retain nextest's native meaning.
Package, feature, target and Cargo profile switches cannot replace the prepared
workspace graph; use runtime filters for selection. `cargo test -p ...` and bare
`cargo nextest run` remain useful local debugging paths. They do not establish
that the entry's last joint preparation was performed. Pure tests do not create
test-support material roots or retention guards. The first managed operation in
an ordinary nextest call lazily establishes its Rust-owned run lease; this does
not require Python. Nested runners must inherit the context after their outer
test explicitly ensures the lease.

Preparation saves native Cargo metadata, native nextest binaries metadata, the
last `cargo build --workspace --tests --bins --profile dev --locked` JSONL,
the four program bindings/build information, native full selection and actual
executable hashes. The binding checker matches package ID, target name/kind,
test profile and executable path against that final Cargo stream. It validates
the native host and checks hashes again immediately before execution. List and
run then reuse both native metadata files without invoking Cargo. Daily copies
under `target/nextest-prepared/<unique>/` are disposable caches, not retained
formal evidence. Do not concurrently rebuild the same target during a run.

The default configuration has four slots, no retries, no fail-fast, a failing
one-second leaked-handle deadline, and a five-minute watchdog with ten-second
termination grace. Native Server smoke/cluster targets consume two slots with
a two-test group limit and a ten-minute watchdog. Process-monitor mechanics use
their own two-test group. These limits prevent runaway tests; they are not
performance requirements and do not extend functional operation deadlines.
Doctests remain a separate Cargo command.

The ignored example is a public native-cluster integration case; the entry
supplies its required `NOVAROCKS_BIN` from the current prepared receipt. Internal
ignored mechanics helpers require their outer owner and private handshake inputs,
so ordinary examples do not select all ignored mechanics helpers.

## Formal execution

`tools/ci/local-full-ci.sh --cargo-only` runs the dependency/lint stages described
below and invokes `test-run.py batch`. The same wrapper can be invoked explicitly:

```bash
python3 tools/ci/test-run.py batch --output /absolute/local/unique-attempt
python3 tools/ci/test-run.py batch --output /absolute/local/another-attempt -- \
  -E 'package(novarocks-test-support)'
```

The destination must not exist. The containing filesystem must be local APFS,
ext, XFS, Btrfs or tmpfs. Containers must bind-mount a supported local state
directory; overlay root filesystems are rejected. The Python wrapper uses
capability checks, `os.posix_spawn(setpgroup=0, file_actions=..., setsigdef=...,
setsigmask=())` and `flock`; it supports system Python 3.9.6 without a Python
3.11 version gate. It does not require Python pidfd/kqueue APIs.

The batch retains distinct `rust-run/` and `doctest-run/` formal registration
contexts. Each contains `.formal`, `.pin`, `.gate`, the `closing`/`Closed` facts,
and its own monitor records. Closing a context never reopens it. The nextest
context is closed and checked before admitting independent doctests. An ordinary
nextest assertion failure (native exit 100) still runs doctests. Cancellation,
execution/environment failure, missing JUnit or incomplete/violated cleanup
gates later stages and records them as not started. A new final joint build and
build-info capture refresh the program bindings after doctest's Cargo command.
Its receipt is separate from the inputs describing what actually ran.

Each executor remains in the wrapper's session with an independent process
group. Nextest receives each observed INT/TERM/HUP/QUIT event exactly once at
its actual Child PID; Cargo/doctest receives group signals. TSTP is forwarded
to nextest; TTIN/TTOU normalize to TSTP. The wrapper stops itself only after
observing the child's stopped wait status. CONT resumes the actual executor;
cancellation while paused first resumes it. Pause confirmation has a five-second
deadline. Independently hosted payload monitors continue while the wrapper and
nextest are stopped. An already stopped wrapper requires CONT before Python can
process a pending HUP/TERM. SIGKILL/OOM/power loss cannot promise user-space
summarization; incomplete materials remain for the platform containment owner.

The local-full stage preserves the wrapper's inherited terminal descriptors.
Its `rust-batch.log` is an execution-index pointer and wrapper exit record;
executor raw output is already retained inside the batch. It does not redirect
display into that pointer log or add a second tee/capture layer. Closed inherited
stdio is reserved with owned null descriptors before record opens; spawn sources
are always separate CLOEXEC descriptors above 2. A post-spawn record/display
failure retains the original control error, cancels and actually waits the exact
executor, then still attempts normal registration sealing and summarization.
A pre-spawn failure also seals its domain independently, records `started=false`
with the original error, and retains any additional cleanup error.

Batch stdout/stderr go directly to exclusive raw files. Bounded daemon raw-FD
writers display incremental copies without blocking signal handling, wait or
cleanup. A broken/backpressured terminal does not change the raw result or
require a display flush at exit. The executor record reports display byte counts
and errors; the complete raw files remain authoritative. Cleanup uses one
thirty-second deadline from observed executor exit or handled cancellation,
covering gate closure and every record lock/read. It never kills payloads by
historical PID; test-support's independent monitors own those children.

## G15 interface: `execution-index.json`, schema 1

The batch root contains the schema-1 index from startup, initially
`complete=false`. Index updates are atomic. Monitor record inodes are never
replaced. Consumers use these declared references and native formats, rather
than human log layout or an invented test-result classification.

| Index field | Meaning |
| --- | --- |
| `schema` | `1`; reject unsupported versions. |
| `source` | Exact source commit, dirty flag and scoped Git status; no full environment dump. |
| `arguments`, `cargo_profile`, `nextest_profile`, `nextest_version` | Original native selection, actual profiles and raw tool self-report. |
| `materials` | Relative `path`, `format`, existence/readability facts for metadata, selection, configuration, bindings, JUnit and both cleanup summaries. |
| `stages` | Preparation, manifest prerequisite, nextest, independent doctest, final preparation. Each has `started` and native `returncode`, or the reason it did not start. |
| Executor stage | Actual argv/cwd, `material_root`, local stdout/stderr names, native wait status/exit, signal events, observed stopped/continued events and display completeness. The same facts are in that stage's `executor.json`. |
| `cancelled`, `error`, `exit_code` | Original cancellation events, stage failure explanation and wrapper outcome. |
| `complete` | Required execution/report/cleanup facts were collected. It does not mean tests passed; native nonzero exits remain nonzero. |

Native inputs are in `inputs/`: `cargo-metadata.json`,
`binaries-metadata.json`, `selected-tests.json`, `artifacts.json.cargo.jsonl`,
`artifacts.json`, `test-artifacts.json`, raw `build-info/`, and actual configuration
copies/hashes in `prepared.json`. `final-prepare/` owns the later joint build and
program receipt used by subsequent SQL/System consumers. JUnit is exclusively
`nextest/<profile>/junit.xml`, selected with an attempt-specific tool config's
`[store].dir`. A missing or truncated JUnit is explicitly incomplete; the wrapper
never fabricates or rewrites native JUnit. Doctest retains Cargo/rustdoc raw text
and native exit; it does not manufacture stable Cargo doctest JSON or JUnit.

Each domain's `cleanup-summary.json` schema 1 declares `registration_closed`,
`complete`, `ok`, `records`, `late_registrations` and `errors`. Each record entry
references `monitor/<launch>/record.jsonl`, stdout/stderr, the original `native`
identity map, its `Final`, completeness and violations. A separate caller
diagnostic may be retained at
`monitor/<launch>/caller-drop-error.txt`. If this regular file exists, the entry's
`caller_drop_error` references its relative path and `violations` records the
caller cleanup failure, even when the monitor later publishes a successful
`Final`. The wrapper retains the original file without reading, parsing or
copying its text. A symlink or another nonregular diagnostic is an explicit
incomplete-material error. The producer's JSONL
events all use schema 1: `Pending`, `Spawned`, optional nonterminal `Liveness`,
`ExitObserved`, and `Final`; `CleanupIncomplete` or a missing/partial Final stays
incomplete. `Final` preserves nullable `status_raw`/`startup_error`, `owner_lost`,
`errors` and `cleanup_complete`. A payload's nonzero exit alone is not a cleanup
violation. Owner loss/startup failure/cleanup errors remain distinct from the
test's native result. Shared flock descriptions are only closed, never unlocked.
Formal contexts are excluded from ordinary release/retention GC.

The ownership ruling is recorded in
[ADR-0184](../../docs/adr/ADR-0184-native-test-execution-and-process-monitor-ownership.md).

G15 stores derived selection/reuse decisions beside these inputs. It must not
write its conclusions into native results or this G6a-owned index. A path on one
machine is local evidence until the full referenced materials have an explicit
shared retention owner.

## Development selection and evidence composition

`dev-verify.sh` consumes canonical preparation, native SQL/System
`--list-declarations`, Cargo metadata and nextest discovery, plus the named stages
from `local-full-ci.sh --list-stages`. Rust selection uses native `rdeps()`;
[impact rules](verification/impact-rules.json) supplement owner declarations.
Mapping gaps remain visible and expand to a bounded owner domain or broad
coverage. This selector is development-only. Admission derives the fixed regular
collection, without consulting the impact graph. Admission activates a declared
subset of that collection; current-attempt provenance and the merge-queue check
are supplied by the Admission workflow. Post-merge full scheduling and shared
registration storage remain separate deliveries.

```bash
tools/ci/dev-verify.sh plan --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh run --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh resume --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh compose --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh check-declarations --root /absolute/local/declaration-evidence
tools/ci/dev-verify.sh confirm-cleanup /absolute/local/task-evidence/actual-attempt
```

Without `--prepared`, these commands first prepare the canonical workspace graph.
`--prepared /absolute/path/to/inputs` revalidates an existing canonical preparation.
`plan` reports selection/scopes, reasons, expansion gaps and heavy author
obligations; `run` creates an exclusive attempt; `resume` recomposes old records
and executes unsatisfied scopes. `compose` writes a fresh derived record. A root
can hold multiple independent attempts and compositions; preserve the whole set.
Actual SQL execution requires `NOVAROCKS_SQL_TEST_CONFIG` and the declared fixture
publication/dataset inputs. Do not rebuild the same target concurrently.

Development selection retains individual Rust test and workspace doctest
obligations. The native `workspace nextest and doctests` stage remains in the
inventory as an admission/full obligation; its development projection is
explicitly deferred to admission before reading results. Deferred stage failures
and unresolved cleanup remain visible in `deferred_scopes` and can still block
the development conclusion. Individual passes never manufacture a stage PASS.

Current G6a batch always includes nextest and independent doctest. If a resume's
entire unmet target set consists only of workspace doctest, the tool may run one
normal batch with one already satisfied, identity-matching regular nextest scope.
Before launch, `selection.json.resume_exception` records the reason, pending and
repeated scopes, prior qualifying execution, exact filterset and batch output.
The derived composition retains this reference. New failures in the repeated
scope enter the normal conflict rules; old cancellation and cleanup facts remain.
With no eligible regular scope, or another unmet target, the gap stays `not_run`.
This exception never uses helpers/manual/heavy tests to fill an empty selection,
creates no doctest executor/index, and does not retry the batch in a loop.
The CLI's `compose --layer admission` is labelled `local-rehearsal`, never a
formal admission receipt. A wrapper's formal process-registration context does
not establish a trusted formal source. Full-layer composition and registration
are library interfaces for G17; this entry does not schedule full runs.

### G15 retained materials

| Material | Owner facts and consumer boundary |
|---|---|
| `identity/source-start.json`, `source-end.json` | Exact commit, dirty/untracked status and source tree snapshots; they establish provenance, not executable identity. Dirty local attempts remain usable only within development rules. |
| Rust `identity/nextest.json`, `doctest.json` | Per-scope input snapshots at start/end, declared runtime/environment inputs, actual program/test/config hashes, profiles and stability confirmation. Missing declarations prevent historical reuse. The separately executed monitor binary is an explicit runtime input. |
| `execution-index.json` checkpoints | Atomically committed after nextest and its cleanup/identity confirmation, then after doctest. A killed later domain leaves the earlier independent checkpoint available; missing whole-attempt completion cannot invent the later result. |
| `invocations/*.started.json`, final `*.json` | Immutable pre-execution selection/argv/cwd/source and start identity; final native exit, end identity/stability, raw log and material references. A started record without a final remains incomplete. |
| System `scenario-teardown.json` | Schema 1 fixture/cluster cleanup facts, partial startup monitor responsibility and external resources, separate from schema 5 scenario evidence. Total process exit is not cleanup proof. |
| External resource registration | Atomic pre-side-effect owner/id/label/PID fact and released/cleanup_failed settlement; recursive collection preserves unsettled or corrupt records. Registration is no deletion authority. See [System owner rule](../../tests/system-test-runner/README.md#声明与收束材料). |
| SQL `--results-file` | Schema 1 JSONL run, full selection, terminal case, suite lifecycle and final end facts. Case identity, declared/executed oracle and policy, effective verification, failure fingerprints and snapshots remain native producer facts. Missing end or required cleanup prevents reuse of the affected scope. |
| `selection.json`, `compositions/*/composition.json` | Native units and scopes selected before execution; derived source/result/completeness/applicability/disposition, retained counterexamples, deferrals and cleanup obligations. They never modify JUnit, SQL records, scenario evidence or the G6a index. |
| `cleanup-confirmation.json` | Independent observation of exact recorded resources or monitor facts. `confirm-cleanup` queries containers, networks and volumes with the recorded Compose label; it performs no cleanup. `--observation` preserves supplemental manual material but does not itself discharge obligations. |
| Registration delta | `verification/registration.py` derives new/still-failing/last-pass-open/closed/infrastructure/quarantine states. A later pass alone does not close a failure; closure needs a linked fix and qualifying revalidation. No shared issue service is created. |

[dispositions.json](baselines/dispositions.json) supplies expiring, owned exact SQL
failure fingerprints and quarantine records. It does not rewrite raw failures or
excuse missing execution, diagnostic-only verification or cleanup violations.
[Layer rules](../../docs/guides/development/test-governance.md#三层验证与证据组合)
define eligible evidence and conclusion wording. Local paths and local admission
rehearsals remain local. Formal current-attempt evidence comes only from the
Admission workflow and its GitHub check; cross-run formal reuse is not enabled.

## PR admission

```bash
tools/ci/admission.sh --output /absolute/new/admission-output
```

The output must be an absent absolute directory. This entry runs the existing
cargo-only native chain once, then composes its current attempt. Exit 0 means the
enabled scope passed; 1 means it did not; 2 means an entry/configuration error.
The native exit is retained independently, including when no attempt directory
was produced. Failed preparation/enumeration writes a blocking composition with
explicit unavailable kinds and preserves observations obtained before failure.

`summary.md`, `composition.json` and `entry.json` retain the verdict, current
attempt path, raw exit, scope declaration digest and run provenance. Native logs
remain in `logs/ci-full/<attempt>`; composition never rewrites them. The workflow
uploads both roots, including hidden diagnostics, under a run/attempt-specific
artifact with a configured 90-day retention. Arrange recoverable handoff before
expiry for unresolved failures and formally referenced evidence.

`baselines/admission-scope.json` activates named stages and regular Rust test /
workspace doctest scopes. It does not change native identities. SQL suites and
System scenarios initially remain pending. Unknown names are rejected against
fresh native declarations; pending scopes and enumeration gaps remain visible.
Deleting the file restores the original complete regular admission collection.
To enable a unit, first establish its main baseline in the admission environment
or obtain a valid disposition, then submit the declaration change through PR.

`verification.admission.compose_attempt` has formal and rehearsal modes. Formal
mode requires the exact head SHA, clean and matching source start/end records,
and a run identity supplied by the designated hosted Admission workflow on
`merge_group` or a main dispatch. Only the current attempt is read. Other GitHub
contexts are diagnostic; local calls are rehearsal and never acquire formal
provenance. Environment fields in downloaded materials do not grant authority.
The existing `dev-verify.sh compose --layer admission` interface remains local.

Admission/full derive the Rust aggregate verdict from member outcomes plus
complete, uncancelled batch and cleanup facts, retaining its native exit.
Admission quarantine can exclude member assertions; full still gates every
member. Nextest exit 100 can represent ordinary test failure. Cargo doctest
nonzero exits cannot distinguish assertions from infrastructure and therefore
cannot establish aggregate success. Development's deferred aggregate is unchanged.
Internal prepare/error-manifest/final-prepare records supply the registered
preparation, error manifest and build-info stage observations.

The workflow runs `admission` for merge groups and dispatches and skips that job
on PRs. On `pull_request`, the independent `pr-validation` check runs the same
cargo-only entry, pinned tools, environment, summary publication and raw-material
upload through shared YAML anchors. Default checkout tests the PR merge SHA;
its evidence is `diagnostic`, even when every enabled scope passes. A separate
`precheck` supplies fast repository feedback. Each PR check has its own
PR-number concurrency group that cancels superseded runs without cancelling
merge-group or main-dispatch admission.

`pr-validation` is intentionally non-required: its real pending, running,
success, failure or cancelled state appears in the PR Checks box. No job or
step ignores a failure. Keep it out of required-check settings so its failure
does not itself block merging. This early feedback costs an additional complete
cargo-only run per PR update, alongside any later formal admission. See
[ADR-0188](../../docs/adr/ADR-0188-optional-pr-validation-and-formal-admission.md).

Shared admission/PR preparation restores two separate caches. Cargo download
paths are limited to `~/.cargo/registry/index`, `registry/cache` and `git/db`;
their key includes OS/architecture, lockfiles and pinned tool versions, with an
OS/architecture prefix fallback to reuse existing packages after dependency
changes. Tools are installed under `$RUNNER_TEMP/admission-tools`, including
their Cargo installation metadata. That cache requires an exact match for
OS/architecture, runner image label, Rust toolchain configuration and all three tool
versions (cargo-deny, cargo-nextest and cargo-machete); restored tool versions
are checked before use. The separate tool directory is added to PATH without caching rustup shims or credentials.

The workflow explicitly saves successfully prepared tools immediately after
version verification, then fetches locked workspace dependencies for the actual
Rust host and saves their download cache before running admission. Later test
failures do not prevent these saves. Exact cache hits are not saved again;
misses use normal installation/fetching. GitHub cache scope and eviction still
apply: PRs can restore base-branch caches, but their own saved caches are scoped
to that PR merge ref. There is no project `target/` or verification-result cache;
native validation runs on every invocation. Cache hit rates and speedups require
observation across actual runs.

Required-check/merge-queue settings
need explicit rollout authorization after a successful formal main baseline.
Real successful, rejected and repaired/re-enqueued PRs are required activation
evidence; local validation alone does not mean the gate is active.
The entry and composition tests run in the existing `verification-*-test.py`
CI stage. For focused validation:

```bash
for test in tools/ci/tests/verification-*-test.py; do python3 "$test" || exit 1; done
```

## Dependency and lint policy

Dependency direction belongs to crate structure and review, following the
[SPI](../../docs/guides/development/spi-boundary.md),
[StateStore](../../docs/guides/development/state-store-boundary.md) and
[Memory](../../docs/guides/development/memory-boundary.md) guides and
[ADR-0190](../../docs/adr/ADR-0190-dependency-direction-by-structure-and-review.md). CI does not add dependency-boundary,
source-shape or product-prerequisite checks.

`Cargo dependency policy` runs `cargo deny --locked check bans licenses sources`
under the root `deny.toml`. It remains an admission stage in the fail-fast
`--cargo-only` chain. It does not fetch the advisory database or check registry
index yanked status. Versions remain owned by the root manifest and `Cargo.lock`.
This supply-chain policy does not establish architectural direction, reject local
path replacements or prove product compatibility; source requirements outside
its coverage need review and behavioral evidence.

`Cargo dependency advisories` is a separate `full`-tier native stage, following
[ADR-0200](../../docs/adr/ADR-0200-advisory-discovery-is-independent-of-admission.md).
It runs `cargo deny --format json --locked check advisories` against the latest
online database and the same `deny.toml`, including yanked checks and unused
exception checks. It is absent from `--cargo-only` and the admission enabled
scope. Its three discovery entries share the same native stage:

- PRs run the independent `dependency-advisories` workflow as a non-required
  check. Findings and input errors retain a failed check; required-check settings
  must keep it outside admission. No `continue-on-error` masks the result.
- The same workflow supports `workflow_dispatch` with an optional ref. It has
  read-only permissions, publishes a summary and raw artifacts, and opens no
  issues. Scheduled discovery remains future work.
- Locally, run the stage with a fresh output directory:

  ```bash
  tools/ci/local-full-ci.sh --run-stage "Cargo dependency advisories" \
    /absolute/unused-artifact-receipt.json /absolute/new/advisories-attempt
  ```

The stage stores `dependency-advisories/cargo-deny.stdout`, `cargo-deny.stderr`,
`exit`, `summary.json` and `summary.md` below that output directory. It preserves
raw streams and the cargo-deny exit code. The summary distinguishes `pass`,
`findings` and `input-error`; removable exceptions are listed separately from
advisory findings, and input diagnostics remain visible even with findings.
Database commit, commit time and fetch time describe the observed input; an
unreadable or ambiguous local database is recorded as unknown. They do not pin
the database or change the result. A nonempty result directory is rejected.

The stage domain covers `Cargo.lock`, `deny.toml`, its script and its workflow.
`dev-verify` selects it as a heavy author obligation when those inputs change;
the full layer includes it. Online findings remain raw failures in composition,
even when the CI change itself behaves correctly. The database is outside the
stage identity, so development reuse in one verification root can retain an
older observation: inspect the summary timestamp and database commit, or use a
new root for fresh discovery. Local results do not prove remote trigger,
permission or required-check configuration.

`NOVA_CI_CARGO_DENY_VERSION` must stay identical in `admission.yml` and
`dependency-advisories.yml`; update and verify both when changing cargo-deny.

The next two stages, in order, are `unused dependencies` and
`unused dependencies tests`. They run in local full CI, admission and
`pr-validation`. The first uses **cargo-machete 0.9.2** with `--with-metadata`,
scanning only top-level directories containing root-workspace members derived
from locked Cargo metadata. The virtual root and `vendor/` packages with
independent workspaces/locks are excluded. The stage checks that `Cargo.lock`
remains unchanged. Nonzero tool exits fail the stage. Any stderr line beginning
`error when handling ` also fails it, because this version can return 0 after a
manifest analysis error. The second stage tests the integration, including that
partial-analysis case. Stage logs are `cargo-machete.log` and
`cargo-machete-test.log`.

Remove unused dependencies. For a genuine false positive, use the owning
package's `[package.metadata.cargo-machete] ignored` with a TOML comment
explaining the macro/generated-code use or required feature activation. Use
`renamed` when the dependency and imported crate names differ. Do not remove a
feature-activating dependency without checking feature resolution.

Install the pinned local tool with:

```bash
cargo install cargo-machete --version 0.9.2 --locked
```

The `cargo clippy` stage is strict for workspace members:
`ci_main_cargo clippy --all-targets -- -D warnings`. The wrapper fixes
`--workspace --profile dev --locked` and the worktree `target/`. Its effective
Cargo command is:

```bash
cargo clippy --workspace --profile dev --locked --target-dir target --all-targets -- -D warnings
```

Strictness is scoped to this CI stage, not a workspace-wide
`warnings = "deny"`, `#![deny(warnings)]`, or `RUSTFLAGS` policy. The arguments
after `--` are passed to Clippy; dependencies outside the workspace are not
thereby made Clippy targets.

The shared baseline follows only the relaxations common to Databend and
RisingWave: `[workspace.lints.clippy] result_large_err = "allow"` and the root
`clippy.toml` value `too-many-arguments-threshold = 10`. `large_enum_variant`
and `type_complexity` retain Clippy's defaults; project-specific relaxations
from only one comparison project are not adopted. New local exceptions use
`#[expect(lint, reason = "...")]`, not `#[allow]`, so an obsolete exception
becomes visible. A `rust-toolchain.toml` upgrade must fix newly emitted lints
or justify a baseline adjustment under the same alignment rule.

`novarocks-native-trust` separately denies `clippy::disallowed_methods` and
`clippy::disallowed_types` at the crate root; its local `clippy.toml` forbids
configuration-source/filesystem APIs and direct Tonic endpoint connections.
Clippy reads the nearest `clippy.toml`; configurations do not merge. The root
file sets the argument threshold to 10, and native-trust's nearer file repeats
that value alongside its local restrictions. If the root configuration changes,
copy its shared entries into native-trust's configuration too.

## Focused verification

```bash
bash tools/ci/tests/local-full-ci-classification-test.sh
bash tools/ci/tests/system-scenarios-stage-test.sh
python3 tools/ci/tests/test-run-test.py
```

The Python suite uses controlled executors for lifecycle/control-flow checks and,
when nextest is installed, a dependency-free real Rust fixture with its own
target. Mocked scripts prove command/selection control flow, not NovaRocks product
execution. Native Linux x86_64 remains a separate required validation target;
Linux arm64, QEMU or cross-compilation does not satisfy that requirement.
Set `NOVA_CI_TEST_EVIDENCE_ROOT` to a unique directory to retain generated fixture
sources, actual commands, raw outputs and indices; rebuildable fixture target
caches stay in the worktree and are removed after verification. The governance
entry retains these regression materials under its own run directory.

When simple native tests are unexpectedly slow on macOS, follow the official
[nextest macOS Developer Tools guidance](https://nexte.st/docs/installation/macos/)
and inspect actual executions as needed. The entry does not automatically change
system authorization or treat a microbenchmark as a performance gate.
