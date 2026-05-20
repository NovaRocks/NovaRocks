# OPT-5 · Optimizer Observability and Regression Harness — Design

Date: 2026-05-20
Status: Draft (pending implementation plan)
Roadmap item: TODO List → Optimizer 路线图 → OPT-5
Reference doc: `NovaRocks TODO/OPT-5-optimizer-observability-regression.md`

## 1. Goal

Build the **scaffolding** that lets future optimizer work (OPT-1 aggregate
pushdown, OPT-2 count-join rule, OPT-3 histogram cardinality, OPT-4 join
strategy costing) be verified as plan-shape changes rather than result-only
checks. Avoid bloating EXPLAIN content in this PR; pick the framework hooks
that later PRs can extend without re-plumbing.

Out of scope for this PR:

- Adding NDV / histogram / distribution property to EXPLAIN VERBOSE output
  (the *hooks* are added; concrete fields land in OPT-3 / OPT-4).
- Merging per-operator runtime stats into EXPLAIN ANALYZE output (NovaRocks
  pipeline has no systematic per-operator runtime profile today; ANALYZE in
  this PR is a query-level summary only).
- Rule-trace mode (showing which rules fired). Deferred.

## 2. Reference Survey (StarRocks)

Confirmed by inspection of `/Users/harbor/project/starrocks/fe/`:

- Modes enum `StatementBase.ExplainLevel` covers
  `NORMAL | LOGICAL | ANALYZE | VERBOSE | COSTS | SCHEDULER | OPTIMIZER | REWRITE | PLAN_ADVISOR`.
- Session variable for rule bisection: **`cbo_disabled_rules`** (string,
  comma-separated). Defined in
  `fe/fe-core/.../qe/SessionVariable.java`; plumbed into
  `OptimizerOptions.applyDisableRuleFromSessionVariable()`; checked via
  `isRuleDisable(RuleType)` against a `BitSet ruleSwitches`.
- `EXPLAIN ANALYZE` runs the query, collects `RuntimeProfile` per fragment,
  and merges it into the plan tree (`fe-core/.../sql/ExplainAnalyzer.java`).
  Heavy machinery; not justified for NovaRocks single-node today.
- Test harness uses substring assertions on `Explain.toString(plan, ...)`
  output. Simple and proven; we copy the pattern.

Decisions adopted from the survey:

- Session variable name **`disable_optimizer_rules`** with **`cbo_disabled_rules`**
  as a write-side alias. New code prefers the unprefixed name; SR-shaped
  scripts keep working.
- Visitor-style EXPLAIN formatter with a single trailer slot per node where
  future fields (NDV, distribution, stats source) plug in without touching
  callers. NovaRocks already has the right shape in `src/sql/explain.rs`;
  we extend rather than rewrite.
- EXPLAIN ANALYZE in this PR is a **query-level** summary; per-operator merge
  is filed as a follow-up.

## 3. Current Baseline (verified)

- `ExplainLevel { Normal, Verbose, Costs }` in `src/sql/explain.rs:21`,
  formatter at `src/sql/explain.rs:36` (`format_node`) and
  `src/sql/explain.rs:252` (`explain_physical_plan`).
- Verbose currently shows: `columns`, `predicates`, some `local_scan` decode
  hints, `min_max_stats` toggle. No NDV, no distribution property line, no
  stats-source attribution.
- `EXPLAIN COSTS` is hand-parsed in `src/engine/mod.rs:2926`
  (`split_explain_costs_sql`) because sqlparser doesn't know `COSTS`.
  `EXPLAIN` and `EXPLAIN VERBOSE` go through sqlparser native.
- `OptimizerOptions::disabled_rules: HashSet<String>` and
  `is_enabled(rule_name)` already exist in
  `src/sql/optimizer/options.rs:43-71`; `disable()` is `#[allow(dead_code)]`
  because nothing populates it from session today.
- Both rule traits already define `name()`:
  - CBO: `Rule::name(&self) -> &str` in `src/sql/optimizer/rule.rs:31`.
  - RBO: `RewriteRule::name(&self) -> &'static str` in
    `src/sql/optimizer/rbo/rule.rs:15`, with docstring stating uniqueness
    across both namespaces.
- `SessionOptimizerSettings` thread-local in
  `src/sql/optimizer/options.rs:8-35` with bool flags but no `disabled_rules`
  field.
- `tests/sql-test-runner/src/` has `runner.rs` / `parser.rs` / `config.rs`;
  zero EXPLAIN-related code today.
- NovaRocks pipeline has `runtime_profile_report_interval_ns` plumbed
  (`src/exec/pipeline/fragment_context.rs:55`) but no per-operator
  rows-in / rows-out / elapsed metrics surfaced upward — confirming the
  out-of-scope decision for full ANALYZE.

## 4. Design

### 4.1 Session-level rule disable

**Settings field.** Add `disabled_rules: Vec<String>` to
`SessionOptimizerSettings` (`src/sql/optimizer/options.rs`).

**SET parser.** In `src/server/mod.rs` SET-statement handling, accept:

- `SET disable_optimizer_rules = '<comma-list>'`
- `SET cbo_disabled_rules = '<comma-list>'` (alias, identical semantics)

Both write to the same session field. Empty string / `NULL` / explicit
unset reverts to empty list. Unknown rule names are accepted silently with
a single `warn!("unknown optimizer rule disabled via session: {name}")`
log line — rationale: this is a debug knob, not part of any application
contract; failing the SET on a stale name would lock users out after rule
renames.

**Wire-up.** Change `OptimizerOptions::default_settings()` → keep it for
tests, and add `OptimizerOptions::from_session(&SessionOptimizerSettings)`
which copies `disabled_rules` into the existing `HashSet<String>`. The
top-level `optimize()` in `src/sql/optimizer/mod.rs` calls
`from_session(&current_session_optimizer_settings())` instead of
`default_settings()`.

**Verification audit.** Read every RBO driver loop and CBO search step that
picks a rule from a `&[Rule]` / `&[RewriteRule]` slice. Confirm each call
site already gates on `options.is_enabled(rule.name())`. Anywhere this gate
is missing, add it as part of this PR — these are bugs masked by the
fact that `disabled_rules` was empty before.

### 4.2 ExplainLevel expansion + EXPLAIN ANALYZE

**Enum.** Add `Analyze` variant. Drop `#[allow(dead_code)]`.

**Parser.** sqlparser already parses `EXPLAIN ANALYZE <query>` natively
and exposes it as `Statement::Explain { analyze: true, ... }`. Today the
match arm at `src/engine/mod.rs:705` hard-codes `analyze: false`, so
`EXPLAIN ANALYZE` currently falls through to the catch-all error. We just
add an `analyze: true` arm. `split_explain_costs_sql` keeps its existing
shape and remains the only manual prefix split (sqlparser doesn't
understand `COSTS`); rename to clarify scope is not required.

**Dispatch.** In `src/engine/mod.rs` statement_dispatch:

- `Statement::Explain { analyze: false, verbose, .. }` plus
  `forced_explain_level == Some(Costs)` → existing `explain_query` path
  (`Normal | Verbose | Costs`).
- `Statement::Explain { analyze: true, .. }` → new `explain_analyze_query`:
  1. `let t_plan_start = Instant::now()`; run analyzer + planner +
     optimizer; `planning_ms = t_plan_start.elapsed()`.
  2. `let t_exec_start = Instant::now()`; run normal `execute_query`,
     consume the result set counting rows, discard column data;
     `execution_ms = t_exec_start.elapsed()`; `rows = count`.
  3. Emit `Verbose`-style plan text (reuse `explain_physical_plan(plan,
     Analyze)`) with a prepended header line:
     `Planning: {planning_ms} ms / Execution: {execution_ms} ms / Rows: {rows}`.
     Use integer ms; sub-millisecond precision is noise here and
     simpler to normalize in golden files.
  4. On execute error, return the SQL error directly — no half-plan
     output. Rationale: avoid false positives in OPT-1 / OPT-2
     verification.

Reject `EXPLAIN ANALYZE` on non-Query bodies the same way the existing
arm rejects non-Query: `"EXPLAIN ANALYZE only supports SELECT queries"`.

**No per-operator runtime stats.** Pipeline has no systematic profile
collection. Filed as follow-up `OPT-5-followup-per-operator-analyze`.

### 4.3 EXPLAIN node-stats trailer slot

`PhysicalPlanNode` already carries `stats: Statistics` with
`output_row_count: f64` (see `src/sql/optimizer/physical_plan.rs:12`,
`src/sql/optimizer/statistics.rs:30`). The trailer just surfaces what's
already there.

Add a uniform trailer in `format_node` and `explain_physical_plan`:
whenever `level ∈ {Verbose, Costs, Analyze}`, append at the end of the
node line a `stats={...}` trailer built from a single helper
`format_stats_trailer(stats: &Statistics, level)`.

For this PR the helper emits only `rows=N` where `N` is
`output_row_count.round() as i64` (clamped at ≥0). When stats are
unset / zero / NaN, emit `rows=?` literal. Future PRs add `ndv=`,
`distribution=`, `source=` keys to the same helper without touching call
sites.

Output ordering of trailer keys is stable: future PRs **append** new
keys after `rows=`, never insert ahead of it. This keeps existing
golden files diff-clean as new fields land.

Rationale for the trailer existing now: OPT-3 / OPT-4 PRs will need this
exact slot; standing it up here means future PRs are pure additions, and
plan-golden files written today won't churn when the trailer gains keys
(new keys append to the end, existing keys keep order).

### 4.4 sql-test-runner: golden + inline assertions

The existing harness already supports per-suite result-golden under
`sql-tests/<suite>/sql/*.sql` + `sql-tests/<suite>/result/*.result` and
per-statement directives via `-- @<key>=<value>` (see `QueryMeta` in
`tests/sql-test-runner/src/parser.rs`). OPT-5 plugs into both, no new
file format.

**Path A — golden EXPLAIN output (primary).**

- New suite `sql-tests/optimizer/sql/*.sql` + `sql-tests/optimizer/result/*.result`.
- Each `.sql` contains one or more `EXPLAIN [VERBOSE|COSTS|ANALYZE] <query>`
  statements. The string-column EXPLAIN output lands in the `.result`
  golden via the existing result-diff machinery; nothing new on the diff
  side.
- For files that use `EXPLAIN ANALYZE`, add a top-of-file directive
  `-- @normalize_explain_timing=true`. When set, the runner applies a
  small normalize pass to the result rows **before diff and before
  record**:
  - In any line matching `^Planning: \d+(?:\.\d+)? ?ms /
    Execution: \d+(?:\.\d+)? ?ms / Rows: (\d+)$`, replace the two timing
    numbers with `<MS>`; keep the row count untouched.
  - **No other normalizations.** No whitespace collapsing, no list
    sorting, no plan-node-id rewrite (none appear in current output).
    Resist the urge to add more — silent normalize is how plan-shape
    drift hides.
- Default for the directive is `false`. A `.result` file without it gets
  bit-exact diff like every other suite.

**Path B — inline `@explain_contains` directive (auxiliary).**

- Extend `QueryMeta` and `parse_meta` with `explain_contains: Vec<String>`,
  shape mirroring the existing `result_contains` field exactly. Suite
  directive form: `-- @explain_contains=<substring>` placed in the
  per-statement meta block above a SQL statement.
- For each substring in `explain_contains`, the runner issues
  `EXPLAIN VERBOSE <body>` of the **same** statement (in addition to the
  original execute), and asserts the explain text contains the substring.
  This lets existing result-golden cases annotate plan-shape facts
  without rewriting the case as an EXPLAIN case.
- Failure mode: emit unified-diff-style block showing
  `expected substring: ...` and the full EXPLAIN VERBOSE output, then
  fail the statement.
- Scope is the immediately preceding meta block of the statement, same
  as `@result_contains`. Multiple directives accumulate (AND-ed).
- Not supported in this PR: regex, multi-line, `@explain_not_contains`.
  Add later if a real case demands it.

### 4.5 PR-internal verification cases

In the new `sql-tests/optimizer/` suite:

1. `disable_join_commutativity.sql` — two `EXPLAIN VERBOSE <same query>`
   statements in one file, the second preceded by
   `SET disable_optimizer_rules = 'JoinCommutativity';`. Single `.result`
   captures both plans; diff asserts the two plans differ in the
   expected way.
2. `baseline_inner_join.sql` — plain inner join `EXPLAIN VERBOSE`; locks
   in current baseline plan so OPT-1 / OPT-2 PRs see a diff.
3. `analyze_header_present.sql` — `EXPLAIN ANALYZE` of a trivial query,
   top-of-file `-- @normalize_explain_timing=true`; the `Planning: <MS> /
   Execution: <MS> / Rows: N` header line is golden.

Plus one `-- @explain_contains=...` example in any existing suite
(e.g. `filter`) to exercise Path B end-to-end.

Unit tests:

- `tests/sql-test-runner` Rust-level unit tests for: normalize pipeline,
  inline directive parsing, attachment-to-next-statement semantics.
- `src/sql/optimizer/options.rs`: tests that a disabled rule name does
  not fire; unknown name doesn't poison the session.

## 5. Data Flow

```text
mysql client / sql-tests
  |
  v
SET disable_optimizer_rules = '...'    (session)
  |
  v
SessionOptimizerSettings { disabled_rules: Vec<String>, ... }   (thread-local)
  |
  v
optimize()  →  OptimizerOptions::from_session(...)              (per-query)
  |                            |
  |                            +---> RBO driver: is_enabled(rule.name())
  |                            +---> CBO search:  is_enabled(rule.name())
  |
  v
PhysicalPlanNode
  |
  v
explain_physical_plan(plan, level)
  |
  +-- Normal  → existing per-node lines
  +-- Verbose → existing lines + stats={rows=...} trailer
  +-- Costs   → existing lines + stats trailer + statistics header
  +-- Analyze → execute(), measure, emit "Planning:.../Execution:.../Rows:..."
                header above the Verbose plan
  |
  v
QueryResult (string column) → MySQL wire / sql-tests differ
```

## 6. Failure Modes & Edge Cases

- **Unknown rule name in `disable_optimizer_rules`.** Warn-log, ignore;
  do not fail the SET. Test covered.
- **Empty `disable_optimizer_rules`.** Treated as no-disable; explicit
  test.
- **`EXPLAIN ANALYZE` on a DDL / non-SELECT.** Reject with the same error
  shape as existing EXPLAIN-on-DDL handling; do not execute.
- **`EXPLAIN ANALYZE` execution error.** Surface SQL error, no
  partial plan.
- **Stats trailer when row count is unavailable.** Emit `rows=?` literal;
  golden files must lock this in so OPT-3 PRs adding NDV don't drift the
  unknown-rows formatting.
- **`@explain_contains` on last statement in file.** Allowed; the extra
  EXPLAIN VERBOSE runs after the original statement executes.
- **Multiple `@explain_contains` on one statement.** All AND-ed, matching
  `@result_contains` semantics.
- **`@explain_contains` on a DDL / non-queryable statement.** Reject at
  parse time with `@explain_contains is only valid on SELECT / EXPLAIN
  statements` — DDL has no EXPLAIN.

## 7. Test Plan

Categories:

1. **Rust unit tests** under `src/sql/optimizer/options.rs` and
   `src/server/`:
   - Default session has empty disabled_rules.
   - Setting a known rule name disables only that rule (verified via
     `OptimizerOptions::is_enabled`).
   - Setting an unknown rule name does **not** fail the SET; the warn
     log fires once; other rules remain enabled.
   - `SET disable_optimizer_rules = ''` clears the list.
   - Aliases `disable_optimizer_rules` and `cbo_disabled_rules` produce
     identical session state.

2. **Rust unit tests** under `src/sql/explain.rs`:
   - `format_stats_trailer` emits `rows=?` when no estimate (zero / NaN
     / negative); emits `rows=N` when estimate present.
   - `Verbose` and `Costs` levels both include the trailer; `Normal` does
     not.
   - Existing `EXPLAIN COSTS` parser (`split_explain_costs_sql`) still
     routes correctly — no regression from adding the `analyze: true`
     arm.
   - Header line emitted by `explain_analyze_query` matches
     `Planning: \d+ ms / Execution: \d+ ms / Rows: \d+` (integer ms).

3. **sql-test-runner Rust tests** under `tests/sql-test-runner/src/`:
   - `@explain_contains` directive parsed into `QueryMeta`, behaves like
     `@result_contains` in scoping.
   - `@normalize_explain_timing=true` rewrites only the
     `Planning: / Execution: / Rows:` line and leaves everything else
     untouched (test feeds a synthetic explain string with multiple
     `Planning:` substrings and confirms only the header is rewritten).
   - Multiple `@explain_contains` directives accumulate (AND-ed).

4. **End-to-end suite `optimizer/`** as listed in §4.5.

5. **Regression check.** Run `tpc-h`, `tpc-ds`, `ssb` suites once to
   confirm no result regression. Run `cargo build` and `cargo clippy`
   clean.

## 8. Roll-out Sequencing (becomes the writing-plans input)

Recommended commit order inside the PR (each must compile and pass
existing tests):

1. Add `Analyze` to `ExplainLevel` (drop `#[allow(dead_code)]`); no
   behavior change yet, but `explain_physical_plan` is taught to accept
   `Analyze` and treat it like `Verbose` for the node-level format.
2. Implement `format_stats_trailer` and thread it through `format_node` /
   `explain_physical_plan` for `Verbose | Costs | Analyze` levels.
   Lock down current Verbose output as the first golden case (the
   `baseline_inner_join.sql` case in the optimizer suite — recorded in a
   later commit, but the trailer behavior is established here).
3. Wire `SessionOptimizerSettings::disabled_rules` end-to-end + SET
   parser + `cbo_disabled_rules` alias; audit RBO/CBO sites for
   `is_enabled` gates and fix any missing; unit tests.
4. Add the `analyze: true` arm to the `Statement::Explain` match in
   `src/engine/mod.rs`; implement `explain_analyze_query` query-level
   summary (planning / execution / row count); reject non-Query bodies;
   tests.
5. Extend `QueryMeta` with `explain_contains: Vec<String>` and parser /
   runner wiring for `@explain_contains` + `@normalize_explain_timing`;
   sql-test-runner Rust unit tests.
6. Add the `sql-tests/optimizer/` suite with the three verification
   cases listed in §4.5; add one `@explain_contains` example in an
   existing suite (e.g. `filter`).
7. Documentation: update `CLAUDE.md` "Suggested Starting Points" with a
   pointer to the disable knob, the trailer, the `Analyze` mode, and the
   new suite.

## 9. Non-Goals (explicit)

- No per-operator runtime profile.
- No rule-trace mode.
- No fragment-level explain.
- No regex / negation in inline directives.
- No new EXPLAIN VERBOSE content fields (NDV, distribution, stats
  source) — slot exists, fields land in OPT-3 / OPT-4.

## 10. Follow-up Tickets to File on Merge

- `OPT-5-followup-per-operator-analyze` — pipeline-level operator profile
  collection and merge into EXPLAIN ANALYZE.
- `OPT-5-followup-rule-trace` — rule-fire trace mode (StarRocks
  `EXPLAIN TRACE`-style).
- `OPT-5-followup-fragment-explain` — multi-fragment EXPLAIN once the
  optimizer/codegen stabilizes fragment ids.
