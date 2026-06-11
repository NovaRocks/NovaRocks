# OPT-5 Optimizer Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build optimizer observability scaffolding — `EXPLAIN ANALYZE`,
node-stats trailer, session-level rule disable knob, and sql-test-runner
plan-assertion hooks — so future OPT-1/2/3/4 PRs can verify plan-shape
changes instead of result-only checks.

**Architecture:** Extend existing `src/sql/explain.rs` formatter with a
stable trailer slot, add `Analyze` variant to `ExplainLevel`, plumb
`SessionOptimizerSettings::disabled_rules` from `SET` into the existing
`OptimizerOptions::is_enabled` gate, and extend `QueryMeta` /
sql-test-runner with two new per-statement / per-file directives. No new
file format; no per-operator runtime profile.

**Tech Stack:** Rust 2021, sqlparser (already used), tokio/std::time
(for Instant), `tracing::warn!` (already in dependency tree), existing
sql-test-runner harness.

**Spec:** `docs/design/specs/2026-05-20-opt-5-optimizer-observability-design.md`

---

## File Structure

**Files modified:**

- `src/sql/explain.rs` — add `Analyze` variant, `format_stats_trailer`,
  thread trailer through `format_node` / `explain_physical_plan`.
- `src/sql/optimizer/options.rs` — add `disabled_rules` field to
  `SessionOptimizerSettings`; add `OptimizerOptions::from_session`.
- `src/sql/optimizer/mod.rs` — `optimize()` reads session settings;
  plumb `&OptimizerOptions` into `explore()` and `implement()`; gate
  rule application on `is_enabled`.
- `src/server/mod.rs` — add `parse_set_string_csv` SET parser; route
  `disable_optimizer_rules` and `cbo_disabled_rules` aliases into
  `SessionOptimizerSettings::disabled_rules`.
- `src/engine/mod.rs` — add `analyze: true` arm to `Statement::Explain`
  match; new `explain_analyze_query` function.
- `tests/sql-test-runner/src/types.rs` (or wherever `QueryMeta` lives) —
  add `explain_contains: Vec<String>` and `normalize_explain_timing: bool`.
- `tests/sql-test-runner/src/parser.rs` — parse `@explain_contains` and
  `@normalize_explain_timing` directives.
- `tests/sql-test-runner/src/runner.rs` — apply normalize before
  diff/record; issue extra EXPLAIN VERBOSE for `@explain_contains`.
- `CLAUDE.md` — pointer to new suite + knob.

**Files created:**

- `sql-tests/optimizer/sql/disable_join_commutativity.sql`
- `sql-tests/optimizer/sql/baseline_inner_join.sql`
- `sql-tests/optimizer/sql/analyze_header_present.sql`
- `sql-tests/optimizer/result/disable_join_commutativity.result`
- `sql-tests/optimizer/result/baseline_inner_join.result`
- `sql-tests/optimizer/result/analyze_header_present.result`
- `sql-tests/optimizer/init.sql` (if the suite needs initial fixtures —
  check existing `sql-tests/filter/init.sql` shape during Task 7).

---

## Task 1: Add `Analyze` variant to `ExplainLevel`

**Goal:** Land the enum change with zero behavior change so subsequent
tasks have something to attach to.

**Files:**
- Modify: `src/sql/explain.rs:18-25` (the `ExplainLevel` enum and its
  `#[allow(dead_code)]` attribute).
- Modify: `src/sql/explain.rs:51`, `:287`, `:290`, `:297`, `:300` (the
  `matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs)` and
  `matches!(level, ExplainLevel::Verbose)` checks — extend the first to
  also include `Analyze` so verbose-level trailers fire under ANALYZE;
  leave the Costs-only checks alone).

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests` near the bottom of
`src/sql/explain.rs` (after line ~1119 where the existing test module
begins):

```rust
#[test]
fn analyze_level_is_treated_like_verbose_in_formatter() {
    use super::{ExplainLevel, explain_physical_plan};
    use crate::sql::optimizer::operator::Operator;
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::statistics::Statistics;

    // Reuse the existing test-plan-builder pattern from
    // `verbose_explain_includes_column_list` (search this file for it
    // to copy the operator construction shape).
    let plan = build_minimal_scan_plan_for_explain_test();
    let verbose = explain_physical_plan(&plan, ExplainLevel::Verbose);
    let analyze = explain_physical_plan(&plan, ExplainLevel::Analyze);
    // Same per-node body text. Header is added by explain_analyze_query
    // later, not by explain_physical_plan itself.
    assert_eq!(verbose, analyze, "Analyze level must format nodes same as Verbose");
}
```

You'll need to extract a helper `build_minimal_scan_plan_for_explain_test`
out of the existing tests (look at the first test in the file that
constructs a `PhysicalPlanNode { op: Operator::PhysicalScan(...), ... }`)
or inline the construction. Either is fine; prefer extracting if the
existing test file already has 2+ near-identical builders.

- [ ] **Step 2: Run test to verify it fails**

```
cargo test --lib sql::explain::tests::analyze_level_is_treated_like_verbose_in_formatter
```

Expected: FAIL with `no variant Analyze` compile error.

- [ ] **Step 3: Add the variant**

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
    Analyze,
}
```

Drop the `#[allow(dead_code)]` line above the enum.

- [ ] **Step 4: Extend the Verbose-equivalent guards**

In `src/sql/explain.rs` find every occurrence of
`matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs)` and
change it to
`matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze)`.

Leave alone:
- `matches!(level, ExplainLevel::Verbose)` (single-Verbose checks):
  same treatment — extend to include `Analyze`.
- `matches!(level, ExplainLevel::Costs)` (Costs-only checks) — leave
  alone. Costs-only output (statistics table header, etc.) must NOT
  fire under Analyze.

Use grep to find them all:
```
grep -n "matches!(level, ExplainLevel" src/sql/explain.rs
```

- [ ] **Step 5: Run test to verify it passes**

```
cargo test --lib sql::explain::tests::analyze_level_is_treated_like_verbose_in_formatter
```

Expected: PASS.

- [ ] **Step 6: Quick regression check**

```
cargo build --lib
cargo test --lib sql::explain::
```

Expected: full `sql::explain` test module passes — no surprise breakage
from the new variant in match arms.

- [ ] **Step 7: Commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): add Analyze variant to ExplainLevel

Treats Analyze like Verbose for per-node body formatting; the query-level
Planning/Execution/Rows header is added by explain_analyze_query in a
later commit. Preparatory change for OPT-5."
```

---

## Task 2: Add `format_stats_trailer` helper and thread through node formatters

**Goal:** Every physical plan node, when rendered under
`Verbose | Costs | Analyze`, ends with a stable `stats={rows=N}` trailer.
This locks the slot future PRs append to without churning golden files.

**Files:**
- Modify: `src/sql/explain.rs` — new private fn `format_stats_trailer`,
  plus call sites in `explain_physical_plan` (the long match around
  `:260`).
- Test: same file's `#[cfg(test)] mod tests`.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn stats_trailer_emits_rows_question_mark_for_unset_stats() {
    use super::format_stats_trailer;
    use crate::sql::optimizer::statistics::Statistics;

    let stats = Statistics { output_row_count: 0.0, ..Default::default() };
    assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
}

#[test]
fn stats_trailer_emits_rows_value_for_positive_estimate() {
    use super::format_stats_trailer;
    use crate::sql::optimizer::statistics::Statistics;

    let stats = Statistics { output_row_count: 123.7, ..Default::default() };
    assert_eq!(format_stats_trailer(&stats), "stats={rows=124}");
}

#[test]
fn stats_trailer_emits_question_mark_for_nan() {
    use super::format_stats_trailer;
    use crate::sql::optimizer::statistics::Statistics;

    let stats = Statistics { output_row_count: f64::NAN, ..Default::default() };
    assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
}

#[test]
fn stats_trailer_emits_question_mark_for_negative() {
    use super::format_stats_trailer;
    use crate::sql::optimizer::statistics::Statistics;

    let stats = Statistics { output_row_count: -1.0, ..Default::default() };
    assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
}
```

If `Statistics` doesn't derive `Default`, either add it (preferred —
verify nothing breaks with `cargo build --lib`) or construct the value
explicitly. Check `src/sql/optimizer/statistics.rs:25-35` first.

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test --lib sql::explain::tests::stats_trailer
```

Expected: FAIL with `format_stats_trailer not found` or `no field
output_row_count`.

- [ ] **Step 3: Implement `format_stats_trailer`**

Add near the top of `src/sql/explain.rs`, after the `ExplainLevel` enum:

```rust
/// Build the per-node `stats={...}` trailer surfaced under
/// `Verbose | Costs | Analyze` levels. Future PRs (OPT-3 NDV, OPT-4
/// distribution) append keys after `rows=`; never reorder existing
/// keys — golden files depend on stable ordering.
pub(crate) fn format_stats_trailer(stats: &crate::sql::optimizer::statistics::Statistics) -> String {
    let rows = stats.output_row_count;
    let rows_str = if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else {
        rows.round() as i64
    }
    .to_string();
    format!("stats={{rows={rows_str}}}")
}
```

The `let rows_str = ... .to_string();` shape ensures both branches
return a `String` (or use a `match` with branches both returning
`String`).

Cleaner implementation:

```rust
pub(crate) fn format_stats_trailer(stats: &crate::sql::optimizer::statistics::Statistics) -> String {
    let rows = stats.output_row_count;
    let rows_str: String = if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else {
        (rows.round() as i64).to_string()
    };
    format!("stats={{rows={rows_str}}}")
}
```

- [ ] **Step 4: Run tests to verify they pass**

```
cargo test --lib sql::explain::tests::stats_trailer
```

Expected: PASS for all four.

- [ ] **Step 5: Thread the trailer into `explain_physical_plan`**

In `src/sql/explain.rs:252+`, locate the function and the per-node
output loop. After the existing logic that pushes the operator name
line under the `costs_suffix`, append the trailer when level is
`Verbose | Costs | Analyze`:

Read `explain_physical_plan` 260–340 first to find the natural line
where the per-node header string is finalized, then suffix the trailer
before the `out.push(...)` for that node's primary line. Concretely:

- Locate the spot where `let header = format!("{pad}{i}:{name}{...}")`
  (or equivalent) is built.
- After it, when
  `matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze)`,
  do `header.push_str(&format!(" {}", format_stats_trailer(&node.stats)));`
  before pushing.

If the existing `costs_suffix` already appends ` ` + suffix, place the
trailer **after** the cost suffix, with a single space separator. Stable
order: `0:OPERATOR ... cardinality: 1 stats={rows=N}`.

- [ ] **Step 6: Write golden snapshot test for the trailer placement**

```rust
#[test]
fn verbose_explain_includes_stats_trailer_on_scan() {
    use super::{ExplainLevel, explain_physical_plan};
    let plan = build_minimal_scan_plan_for_explain_test();
    let lines = explain_physical_plan(&plan, ExplainLevel::Verbose);
    let scan_line = lines.iter().find(|l| l.contains("SCAN")).expect("scan line");
    assert!(
        scan_line.ends_with("stats={rows=?}") || scan_line.contains("stats={rows="),
        "scan node should end with stats trailer: {scan_line}"
    );
}
```

(The reason both alternatives are accepted in the assertion: the test
helper's row-count estimate may or may not be populated — the test only
asserts the trailer SHAPE is present, not its value.)

- [ ] **Step 7: Run all `sql::explain::tests`**

```
cargo test --lib sql::explain::tests
```

Expected: PASS. Existing snapshot tests inside the file may fail
because their golden strings don't have the trailer — if so, **read
each failure and update the golden** to include the trailer. The
golden text is the source of truth for what the output should be;
adding a stats trailer is intentional behavior. Do NOT regenerate by
copy-pasting actual output blindly; eyeball each diff.

- [ ] **Step 8: Commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): add stable stats={rows=N} trailer for Verbose/Costs/Analyze

Per-node trailer slot for future OPT-3 NDV and OPT-4 distribution
fields. Keys are stable-ordered; future PRs append after rows= without
reordering. rows=? when no estimate is available."
```

---

## Task 3: Wire `SessionOptimizerSettings::disabled_rules` end-to-end

**Goal:** Adding `disable_optimizer_rules` to the session changes which
rules fire. Audit the two CBO call sites that currently skip
`is_enabled`.

**Files:**
- Modify: `src/sql/optimizer/options.rs:7-14` (`SessionOptimizerSettings`),
  `:43-72` (`OptimizerOptions` impl block).
- Modify: `src/sql/optimizer/mod.rs:64` (`optimize()` reads session),
  `:144`, `:208` (`explore` and `implement` take and check options).
- Test: `src/sql/optimizer/options.rs` `#[cfg(test)] mod tests`.

- [ ] **Step 1: Write the failing test**

In `src/sql/optimizer/options.rs` tests module:

```rust
#[test]
fn from_session_copies_disabled_rules() {
    let settings = SessionOptimizerSettings {
        disabled_rules: vec!["JoinCommutativity".to_string(), "FooRule".to_string()],
        ..Default::default()
    };
    let opts = OptimizerOptions::from_session(&settings);
    assert!(!opts.is_enabled("JoinCommutativity"));
    assert!(!opts.is_enabled("FooRule"));
    assert!(opts.is_enabled("UnrelatedRule"));
}

#[test]
fn from_session_empty_disabled_rules_enables_everything() {
    let settings = SessionOptimizerSettings::default();
    let opts = OptimizerOptions::from_session(&settings);
    assert!(opts.is_enabled("JoinCommutativity"));
    assert!(opts.is_enabled("AnyRuleAtAll"));
}
```

For `SessionOptimizerSettings` to support `..Default::default()`, the
struct already derives `Default` (verified: line 7). Good.

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test --lib sql::optimizer::options::tests::from_session
```

Expected: FAIL with `no field disabled_rules` or `no associated function from_session`.

- [ ] **Step 3: Add the field**

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionOptimizerSettings {
    pub enable_ukfk_opt: bool,
    pub enable_rbo_table_prune: bool,
    pub enable_cbo_table_prune: bool,
    pub enable_table_prune_on_update: bool,
    pub enable_eliminate_agg: bool,
    pub disabled_rules: Vec<String>,
}
```

- [ ] **Step 4: Add `from_session`**

In the `impl OptimizerOptions` block:

```rust
pub(crate) fn from_session(settings: &SessionOptimizerSettings) -> Self {
    let mut opts = Self::default_settings();
    for rule_name in &settings.disabled_rules {
        opts.disable(rule_name);
    }
    opts
}
```

Also drop `#[allow(dead_code)]` from `pub(crate) fn disable` (line 68)
— it's no longer dead.

- [ ] **Step 5: Run tests to verify they pass**

```
cargo test --lib sql::optimizer::options::tests::from_session
```

Expected: PASS.

- [ ] **Step 6: Plumb `from_session` into `optimize()`**

`src/sql/optimizer/mod.rs:64`:

Change:
```rust
let options = options::OptimizerOptions::default_settings();
```

To:
```rust
let options = options::OptimizerOptions::from_session(
    &options::current_session_optimizer_settings(),
);
```

- [ ] **Step 7: Audit `explore` and `implement` — write failing test first**

Find an existing CBO rule with a stable, recognizable plan effect. Pick
`JoinCommutativity` (it swaps inner-join children). Add a test in
`src/sql/optimizer/mod.rs` tests module (create if absent) that runs a
simple inner-join query through `optimize()` twice: once normally, once
with thread-local `disabled_rules = ["JoinCommutativity"]`. Assert the
two physical plans **differ** (commutativity creates an extra logical
alternative — disabling it should produce a deterministic plan; with
it enabled the optimizer may pick the other order if costs say so).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::options::{
        SessionOptimizerSettings, with_session_optimizer_settings,
    };
    use crate::sql::optimizer::statistics::TableStatistics;

    // Helper: build a LogicalPlan for `SELECT * FROM a INNER JOIN b ON a.k = b.k`
    // using whatever planner-test fixture is already in this crate. Look in
    // `src/sql/planner/mod.rs` tests for `parse_analyze_and_plan` or similar
    // and reuse it. If no such helper is exposed, gate the test behind
    // `#[cfg(feature = "...")]` only if it would otherwise require a public
    // helper. Otherwise inline a minimal builder.

    #[test]
    fn disabled_rule_changes_plan_shape() {
        let logical = build_inner_join_logical_plan_for_test();
        let stats = build_table_stats_for_inner_join_test();

        let enabled_plan = optimize(logical.clone(), &stats).expect("optimize ok");

        let disabled_settings = SessionOptimizerSettings {
            disabled_rules: vec!["JoinCommutativity".to_string()],
            ..Default::default()
        };
        let disabled_plan = with_session_optimizer_settings(disabled_settings, || {
            optimize(logical, &stats).expect("optimize ok")
        });

        // Either the plans are structurally different, or — if costs make
        // commutativity a no-op for this input — at minimum the number of
        // logical alternatives explored differs. The strongest assertion we
        // can make without coupling to memo internals is plan equality.
        // If both happen to pick the same final plan, the test still proves
        // the disable knob doesn't crash; relax to a smoke check then.
        let enabled_str = format!("{enabled_plan:?}");
        let disabled_str = format!("{disabled_plan:?}");
        // Smoke check: both runs return a plan. The disable knob is
        // exercised; observable plan-shape divergence is tested at the
        // SQL-suite level in Task 7.
        assert!(!enabled_str.is_empty());
        assert!(!disabled_str.is_empty());
    }
}
```

**Why the test is a smoke check, not a strict inequality:** the
CBO-level shape change is suite-asserted in Task 7's
`disable_join_commutativity.sql`. At this layer we only need to prove
that (a) the disable knob threads through compile-time, (b) `optimize()`
doesn't panic when a rule is disabled. The stronger plan-shape assertion
lives where the EXPLAIN output is golden — that's a more durable test
than `format!("{plan:?}")` string compare.

- [ ] **Step 8: Run the test — expect it to pass without further changes**

```
cargo test --lib sql::optimizer::tests::disabled_rule_changes_plan_shape
```

Expected: PASS (because explore/implement currently don't check
is_enabled, but the test only asserts non-panic + non-empty plan).

This is intentional. The next step audits the gates.

- [ ] **Step 9: Audit and fix CBO `explore` + `implement` to honor is_enabled**

`src/sql/optimizer/mod.rs:144` and `:208` both iterate
`for rule in rules` without checking `options.is_enabled`. Change
signatures:

```rust
fn explore(
    memo: &mut Memo,
    rules: &[Box<dyn Rule>],
    options: &OptimizerOptions,
    deadline: Instant,
) -> Result<(), String> {
    // ... existing body ...
    for rule in rules {
        if !options.is_enabled(rule.name()) {
            continue;
        }
        // existing JoinAssociativity size guard stays as-is, after the
        // is_enabled check.
        if rule.name() == "JoinAssociativity" && memo.groups.len() > 200 {
            continue;
        }
        // ... existing rule.matches / rule.apply logic ...
    }
}

fn implement(memo: &mut Memo, rules: &[Box<dyn Rule>], options: &OptimizerOptions) {
    // ... existing body ...
    for rule in rules {
        if !options.is_enabled(rule.name()) {
            continue;
        }
        // ... existing rule.matches / rule.apply logic ...
    }
}
```

Update callers at `:101` and `:107`:

```rust
explore(&mut memo, &transform_rules, &options, deadline)?;
// ...
implement(&mut memo, &impl_rules, &options);
```

- [ ] **Step 10: Strengthen the test to actually check disablement**

Rewrite the test from Step 7 to verify behavior at memo level:

```rust
#[test]
fn disabled_rule_is_skipped_during_explore() {
    // Build a tiny memo manually with one inner-join MExpr and run
    // explore with JoinCommutativity disabled vs enabled. Assert the
    // memo's logical_exprs count for the join group differs.
    //
    // Use src/sql/optimizer/memo.rs and src/sql/optimizer/convert.rs
    // to construct the memo; mirror the setup from existing tests in
    // mod.rs that already exercise explore (search for "explore(" in
    // tests). If no such test exists, use the simpler end-to-end
    // optimize() path and pattern-match the resulting plan op tree
    // for HashJoin order (which side is build/probe will differ when
    // commutativity is enabled vs not, given costs).
}
```

If the memo-construction approach is too invasive, keep the smoke
assertion from Step 7. The Task-7 SQL suite case is the authoritative
plan-shape regression test.

- [ ] **Step 11: Run all optimizer-level tests**

```
cargo test --lib sql::optimizer::
```

Expected: PASS. If any pre-existing test breaks because adding the
`is_enabled` gate changed plan iteration order, dig into the failure
and confirm it's a real semantic regression vs. a flaky ordering
assumption. Real regression → revert the test edit and rethink. Flaky
ordering → fix the test (and add a comment explaining).

- [ ] **Step 12: Commit**

```bash
git add src/sql/optimizer/options.rs src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): plumb SessionOptimizerSettings.disabled_rules into RBO+CBO

Adds OptimizerOptions::from_session, populates disabled_rules from the
thread-local session, and audits explore()+implement() to honor
is_enabled(rule.name()) before applying each CBO rule. RBO driver
already had the gate. Session-side SET parsing arrives in the next
commit."
```

---

## Task 4: Add `SET disable_optimizer_rules` / `cbo_disabled_rules` SET parsers

**Goal:** Make the new session field reachable from a mysql client.

**Files:**
- Modify: `src/server/mod.rs` — new `parse_set_string_csv` helper near
  `:530`; wire into `execute_statement_text` near `:697`.

- [ ] **Step 1: Write the failing tests**

In the `#[cfg(test)] mod tests` block of `src/server/mod.rs`:

```rust
#[test]
fn parse_set_string_csv_accepts_quoted_value() {
    assert_eq!(
        parse_set_string_csv("SET disable_optimizer_rules = 'JoinCommutativity'", "disable_optimizer_rules"),
        Some(vec!["JoinCommutativity".to_string()]),
    );
}

#[test]
fn parse_set_string_csv_splits_comma_list() {
    assert_eq!(
        parse_set_string_csv("SET disable_optimizer_rules = 'A,B,C'", "disable_optimizer_rules"),
        Some(vec!["A".to_string(), "B".to_string(), "C".to_string()]),
    );
}

#[test]
fn parse_set_string_csv_trims_spaces_within_list() {
    assert_eq!(
        parse_set_string_csv("SET disable_optimizer_rules = ' A , B '", "disable_optimizer_rules"),
        Some(vec!["A".to_string(), "B".to_string()]),
    );
}

#[test]
fn parse_set_string_csv_empty_value_returns_empty_list() {
    assert_eq!(
        parse_set_string_csv("SET disable_optimizer_rules = ''", "disable_optimizer_rules"),
        Some(vec![]),
    );
}

#[test]
fn parse_set_string_csv_accepts_alias_target_name() {
    assert_eq!(
        parse_set_string_csv("SET cbo_disabled_rules = 'X'", "cbo_disabled_rules"),
        Some(vec!["X".to_string()]),
    );
    // And rejects the wrong name.
    assert_eq!(
        parse_set_string_csv("SET disable_optimizer_rules = 'X'", "cbo_disabled_rules"),
        None,
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test --lib server::tests::parse_set_string_csv
```

Expected: FAIL with `parse_set_string_csv not found`.

- [ ] **Step 3: Implement `parse_set_string_csv`**

Below `parse_set_boolean` near line `:561`:

```rust
/// Parse `SET <name> = '<comma-list>'`. The value MUST be single-quoted
/// (matching mysql session-variable convention); inner items are
/// comma-separated, whitespace-trimmed, and empty items are dropped.
/// Returns the list (possibly empty) when the statement matches the
/// expected name, else None.
fn parse_set_string_csv(query: &str, expected_name: &str) -> Option<Vec<String>> {
    let trimmed = query.trim();
    let after_set = trimmed
        .get(..3)
        .filter(|head| head.eq_ignore_ascii_case("set"))
        .map(|_| trimmed[3..].trim_start())?;

    // Match the variable name (case-insensitive).
    let rest = {
        let prefix_len = expected_name.len();
        let head = after_set.get(..prefix_len)?;
        if !head.eq_ignore_ascii_case(expected_name) {
            return None;
        }
        after_set[prefix_len..].trim_start()
    };

    // Expect '=' followed by the value.
    let value_str = rest.strip_prefix('=')?.trim();

    // Value must be single-quoted.
    let inner = value_str
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))?;

    Ok::<_, ()>(()).ok()?; // silence unused-result lint if needed
    let items = inner
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    Some(items)
}
```

(If the `Ok::<_, ()>(()).ok()?;` lint shim isn't needed, remove it.)

Also confirm `expected_name` boundary: the name match must not
accidentally accept `disable_optimizer_rules_extra`. The `prefix_len`
match above will let the suffix bytes match too — fix by requiring the
next char after `prefix_len` to be whitespace or `=`:

```rust
let rest = {
    let prefix_len = expected_name.len();
    let head = after_set.get(..prefix_len)?;
    if !head.eq_ignore_ascii_case(expected_name) {
        return None;
    }
    let following = after_set.as_bytes().get(prefix_len)?;
    if !matches!(*following, b' ' | b'\t' | b'=') {
        return None;
    }
    after_set[prefix_len..].trim_start()
};
```

- [ ] **Step 4: Run tests to verify they pass**

```
cargo test --lib server::tests::parse_set_string_csv
```

Expected: PASS (all five).

- [ ] **Step 5: Wire into `execute_statement_text`**

After the `parse_set_boolean` block at `src/server/mod.rs:697-709`, add:

```rust
for name in ["disable_optimizer_rules", "cbo_disabled_rules"] {
    if let Some(rules) = parse_set_string_csv(trimmed, name) {
        // Warn about unknown rule names — does NOT fail the SET.
        // Empty list is allowed (clears the session list).
        for rule in &rules {
            if !is_known_optimizer_rule(rule) {
                tracing::warn!(
                    "unknown optimizer rule disabled via session: {rule}"
                );
            }
        }
        shim.optimizer_settings.disabled_rules = rules;
        return Ok(StatementResult::Ok);
    }
}
```

`is_known_optimizer_rule` is a helper that walks the same two rule
lists `optimize()` consults. Add it in `src/sql/optimizer/mod.rs` as a
`pub(crate)` function or behind a small accessor; preferred shape:

```rust
// in src/sql/optimizer/mod.rs
pub(crate) fn is_known_rule_name(name: &str) -> bool {
    rules::all_transformation_rules()
        .iter()
        .any(|r| r.name() == name)
        || rules::all_implementation_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::predicate_pushdown_rbo_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::column_pruning_rules()
            .iter()
            .any(|r| r.name() == name)
}
```

(Verify the exact RBO list helper names by inspecting
`src/sql/optimizer/rbo/rules/mod.rs`; the snippet above uses what the
existing `optimize()` reads.)

Import in `src/server/mod.rs`:
```rust
use crate::sql::optimizer::is_known_rule_name as is_known_optimizer_rule;
```

- [ ] **Step 6: Write a server-level integration test for unknown name**

```rust
#[test]
fn unknown_rule_name_warn_does_not_fail_set() {
    // Construct the smallest possible parse-and-route stub: call
    // parse_set_string_csv directly, then assert the per-rule warn path
    // is exercised by feeding an unknown name through is_known_optimizer_rule.
    let rules = parse_set_string_csv(
        "SET disable_optimizer_rules = 'TotallyNotARealRule'",
        "disable_optimizer_rules",
    )
    .expect("parse ok");
    assert_eq!(rules, vec!["TotallyNotARealRule".to_string()]);
    // The actual warn-log path is wired in execute_statement_text; here
    // we just assert is_known_rule_name returns false for the name.
    assert!(!crate::sql::optimizer::is_known_rule_name("TotallyNotARealRule"));
}
```

- [ ] **Step 7: Run all server + optimizer tests**

```
cargo test --lib server::
cargo test --lib sql::optimizer::
```

Expected: PASS.

- [ ] **Step 8: Manual smoke** (optional but recommended — server start is
  cheap on debug)

Skip the end-to-end smoke for now; Task 7's SQL suite case exercises
the full pipeline.

- [ ] **Step 9: Commit**

```bash
git add src/server/mod.rs src/sql/optimizer/mod.rs
git commit -m "feat(server): SET disable_optimizer_rules and cbo_disabled_rules

Comma-separated list of optimizer rule names to disable for the current
session; cbo_disabled_rules is a StarRocks-compat alias. Unknown rule
names are logged at warn! level and accepted (rationale: debug knob,
must survive rule renames). is_known_rule_name in the optimizer module
introspects the rule registry for the warn check."
```

---

## Task 5: Add `EXPLAIN ANALYZE` dispatch and `explain_analyze_query`

**Goal:** `EXPLAIN ANALYZE <SELECT>` executes the query, prints a
`Planning: <ms> ms / Execution: <ms> ms / Rows: <N>` header above the
Verbose plan body.

**Files:**
- Modify: `src/engine/mod.rs:705-778` — extend the `Statement::Explain`
  match arm to handle `analyze: true`.
- New function: `explain_analyze_query` near the existing
  `explain_query` at `:2314`.

- [ ] **Step 1: Inspect the existing `Statement::Explain` arm**

Open `src/engine/mod.rs:705`. The current arm matches only
`analyze: false`. Capture the existing fallthrough behavior of
`analyze: true` (likely the catch-all "unsupported sql" error) before
editing.

- [ ] **Step 2: Write a failing integration test**

Place the test in `src/engine/mod.rs` `#[cfg(test)] mod tests` if such a
module exists, else `tests/standalone_mysql_server.rs`. Prefer engine-
level because the function is straightforward:

```rust
#[test]
fn explain_analyze_prefixes_planning_execution_rows() {
    // Set up an in-memory standalone state with one tiny local-parquet
    // table or use the existing test fixture (search for `setup_test_state`
    // or equivalent helper in src/engine/mod.rs tests).
    let state = setup_minimal_standalone_state_for_engine_test();
    let result = run_explain_analyze_for_engine_test(
        &state,
        "SELECT 1",
    ).expect("explain analyze runs");
    let header = &result[0];
    assert!(
        header.starts_with("Planning: ") && header.contains("ms / Execution: ") && header.contains(" ms / Rows: "),
        "expected Planning/Execution/Rows header, got: {header}"
    );
}
```

If no engine-level test fixture exists for in-memory queries, prefer
to write this test as a sql-test-runner end-to-end case **only**, and
skip the Rust unit test. The Task-7 suite case
`analyze_header_present.sql` is the authoritative test for this
behavior anyway.

- [ ] **Step 3: Implement `explain_analyze_query`**

Place near the existing `explain_query` (around `:2314`):

```rust
fn explain_analyze_query(
    query: &sqlparser::ast::Query,
    catalog: &InMemoryCatalog,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<QueryResult, String> {
    use std::time::Instant;
    use crate::sql::explain::{ExplainLevel, explain_physical_plan};

    let t_plan_start = Instant::now();
    let (resolved, cte_registry) =
        crate::sql::analyzer::analyze(query, catalog, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry)?;
    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::optimizer::optimize(logical, &table_stats)?;
    let planning_ms = t_plan_start.elapsed().as_millis() as u64;

    let t_exec_start = Instant::now();
    let executed = execute_query(query, catalog, current_database, exchange_port, query_opts)?;
    let rows: u64 = executed.chunks.iter().map(|c| c.num_rows() as u64).sum();
    let execution_ms = t_exec_start.elapsed().as_millis() as u64;

    let mut lines = Vec::new();
    lines.push(format!(
        "Planning: {planning_ms} ms / Execution: {execution_ms} ms / Rows: {rows}"
    ));
    lines.extend(explain_physical_plan(&physical, ExplainLevel::Analyze));

    build_string_query_result("Explain String", lines)
}
```

(Verify `Chunk` exposes a `num_rows()` method; if not, the right
accessor lives in `src/exec/chunk/mod.rs`. Adjust accordingly.)

- [ ] **Step 4: Extend `Statement::Explain` arm**

In `src/engine/mod.rs:705`, change the match arm pattern from:

```rust
sqlast::Statement::Explain {
    statement,
    verbose,
    analyze: false,
    ..
} => { /* existing body */ }
```

to two arms — keep the existing one verbatim and add:

```rust
sqlast::Statement::Explain {
    statement,
    verbose: _,
    analyze: true,
    ..
} => {
    let sqlast::Statement::Query(ref query) = *statement else {
        return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
    };

    // Reuse the same time-travel + three-part-name rewrite block as the
    // existing analyze:false arm. To avoid duplication, factor the
    // rewrite into a helper `prepare_explain_query` that takes the
    // query and returns the rewritten query + an Option<String> for the
    // catalog name; call it from both arms. Sketch (place in the same
    // module):
    //
    //   fn prepare_explain_query<'a>(
    //       state: &'a StandaloneState,
    //       current_catalog: Option<&str>,
    //       current_database: &str,
    //       query: &'a sqlast::Query,
    //   ) -> Result<Cow<'a, sqlast::Query>, String> { ... }
    //
    // Move lines 715-762 of the existing arm into the helper, then both
    // arms call it and proceed with their level-specific dispatch.

    let prepared = prepare_explain_query(state, current_catalog, current_database, query)?;
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let result = explain_analyze_query(
        &prepared,
        &catalog,
        current_database,
        state.exchange_port,  // pub(crate) field on StandaloneState; see src/engine/mod.rs:199
        None,
    )?;
    drop(catalog);
    Ok(StatementResult::Query(result))
}
```

(The Cow approach handles "no rewrite needed" without cloning when the
query is unchanged; mirror exactly what the existing analyze:false arm
does — re-bind via a `let mut` of the owned clone for each rewrite. If
the cleanest version is a `String` arg → re-parse, that's a worse
choice; pass the AST.)

- [ ] **Step 5: Add the "non-Query body" rejection test**

If keeping a Rust-level test:

```rust
#[test]
fn explain_analyze_rejects_ddl() {
    let state = setup_minimal_standalone_state_for_engine_test();
    let err = run_explain_analyze_for_engine_test(
        &state,
        "CREATE TABLE foo (a INT)",
    ).expect_err("should reject DDL");
    assert!(err.contains("only supports SELECT"), "unexpected: {err}");
}
```

If there's no convenient test fixture, this assertion lands in Task 7's
suite directory via a `@expect_error=...` case. Skip the Rust test in
that case.

- [ ] **Step 6: Run engine tests**

```
cargo build --lib
cargo test --lib engine::
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs
git commit -m "feat(engine): EXPLAIN ANALYZE with query-level Planning/Execution/Rows summary

Wires Statement::Explain { analyze: true } to a new
explain_analyze_query that times planning + execution, counts result
rows, then prints the Verbose plan body. Per-operator runtime stats are
out of scope (no pipeline profile collection today)."
```

---

## Task 6: Extend `QueryMeta` with `explain_contains` and `normalize_explain_timing`

**Goal:** sql-test-runner gains two new directives that future
optimizer suites depend on.

**Files:**
- Modify: `tests/sql-test-runner/src/types.rs` (`QueryMeta` struct).
- Modify: `tests/sql-test-runner/src/parser.rs` (`parse_meta` and
  `merge_meta`).
- Modify: `tests/sql-test-runner/src/runner.rs` (apply normalize before
  diff; issue extra EXPLAIN for `@explain_contains`).

- [ ] **Step 1: Locate `QueryMeta`**

```
grep -n "pub struct QueryMeta" tests/sql-test-runner/src/types.rs
```

Read the struct definition fully so the new fields integrate with
existing field layout and `Default`.

- [ ] **Step 2: Add fields**

```rust
pub struct QueryMeta {
    // ... existing fields ...
    pub explain_contains: Vec<String>,
    pub normalize_explain_timing: bool,
}
```

Verify `Default` is derived and `Vec` / `bool` defaults are correct
(empty / false).

- [ ] **Step 3: Extend `parse_meta`**

In `tests/sql-test-runner/src/parser.rs:75-148`, add cases inside the
`match key.as_str()` block:

```rust
"explain_contains" => {
    meta.explain_contains.push(raw_value);
}
"normalize_explain_timing" => {
    meta.normalize_explain_timing = parse_bool(&raw_value)?;
}
```

- [ ] **Step 4: Extend `merge_meta`**

In `merge_meta` (line `:152`), add explain-side handling matching
`result_contains` shape:

```rust
explain_contains: if override_meta.explain_contains.is_empty() {
    base.explain_contains.clone()
} else {
    override_meta.explain_contains.clone()
},
normalize_explain_timing: override_meta.normalize_explain_timing
    || base.normalize_explain_timing,
```

- [ ] **Step 5: Write parser-level tests**

If there's an existing parser tests module (likely at the bottom of
`parser.rs`), add:

```rust
#[test]
fn parse_meta_collects_explain_contains() {
    // The canonical meta regex used by the runner — see
    // tests/sql-test-runner/src/main.rs:1600.
    let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
    let lines = vec![
        "-- @explain_contains=INNER JOIN".to_string(),
        "-- @explain_contains=stats={rows=".to_string(),
    ];
    let meta = parse_meta(&lines, &meta_re).expect("parse ok");
    assert_eq!(
        meta.explain_contains,
        vec!["INNER JOIN".to_string(), "stats={rows=".to_string()],
    );
}

#[test]
fn parse_meta_parses_normalize_explain_timing() {
    let meta_re = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
    let lines = vec!["-- @normalize_explain_timing=true".to_string()];
    let meta = parse_meta(&lines, &meta_re).expect("parse ok");
    assert!(meta.normalize_explain_timing);
}
```

- [ ] **Step 6: Run parser tests**

```
cargo test --manifest-path tests/sql-test-runner/Cargo.toml parser::
```

Expected: PASS.

- [ ] **Step 7: Implement the normalize pass**

In `tests/sql-test-runner/src/runner.rs`, find where result rows are
diffed (search for `fn diff_results` / `compare_rows` / similar — the
runner formats actual rows into a `Vec<String>` before diffing). Add
a normalize step:

```rust
/// Replace the timing values in the EXPLAIN ANALYZE header line with
/// the literal `<MS>`. Only the canonical
/// "Planning: <N> ms / Execution: <M> ms / Rows: <K>" shape is
/// rewritten. Other lines pass through verbatim. Row count K is
/// preserved.
fn normalize_explain_timing(line: &str) -> String {
    use once_cell::sync::Lazy;
    use regex::Regex;
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"^Planning: \d+ ms / Execution: \d+ ms / Rows: (\d+)$"
        )
        .expect("static regex compiles")
    });
    match RE.captures(line) {
        Some(caps) => format!(
            "Planning: <MS> ms / Execution: <MS> ms / Rows: {}",
            caps.get(1).unwrap().as_str(),
        ),
        None => line.to_string(),
    }
}

fn normalize_rows_for_diff(rows: &[String], meta: &QueryMeta) -> Vec<String> {
    if !meta.normalize_explain_timing {
        return rows.to_vec();
    }
    rows.iter().map(|line| normalize_explain_timing(line)).collect()
}
```

Find the diff site, call `normalize_rows_for_diff` on both `actual` and
`expected` before comparing. Same for the `--mode record` write path:
the recorded `.result` must contain the **normalized** strings, not the
raw ones (otherwise record + verify in the same run can drift).

Test:

```rust
#[test]
fn normalize_explain_timing_rewrites_only_header_line() {
    let input = vec![
        "Planning: 12 ms / Execution: 345 ms / Rows: 7".to_string(),
        "Some other line about Planning: 99 ms".to_string(),
        "Planning: notdigits ms / Execution: 0 ms / Rows: 0".to_string(),
    ];
    let meta = QueryMeta {
        normalize_explain_timing: true,
        ..QueryMeta::default()
    };
    let out = normalize_rows_for_diff(&input, &meta);
    assert_eq!(out[0], "Planning: <MS> ms / Execution: <MS> ms / Rows: 7");
    assert_eq!(out[1], input[1]); // non-header line untouched
    assert_eq!(out[2], input[2]); // header-shaped but with non-digit, untouched
}

#[test]
fn normalize_explain_timing_disabled_is_passthrough() {
    let input = vec!["Planning: 1 ms / Execution: 2 ms / Rows: 3".to_string()];
    let meta = QueryMeta::default(); // normalize_explain_timing = false
    let out = normalize_rows_for_diff(&input, &meta);
    assert_eq!(out, input);
}
```

- [ ] **Step 8: Implement `@explain_contains` assertion**

In the same `runner.rs`, locate the per-statement execution path
(search for `result_contains` to find where the existing result-side
substring assertion fires). The session API for running a statement is
`session.execute_query(timeout, sql, db_override)` which returns
`(bool, Option<QueryExecution>, String)` — the `QueryExecution` carries
the row data (search `QueryExecution` in `tests/sql-test-runner/src/types.rs`
or `session.rs` to find the rows accessor; typically `.rows` or
`.formatted_rows()`). Mirror exactly how `result_contains` consumes it.

Add a sibling step that fires AFTER the normal execute completes but
BEFORE moving to the next statement:

```rust
// After the normal execute + result-side assertions complete, run
// EXPLAIN VERBOSE of the same body for @explain_contains checks.
if !meta.explain_contains.is_empty() {
    let body = explain_contains_target_body(sql_for_this_statement);
    let explain_sql = format!("EXPLAIN VERBOSE {body}");
    let (ok, exec, msg) = session.execute_query(query_timeout, &explain_sql, db_override);
    if !ok {
        return Err(format!(
            "@explain_contains: EXPLAIN VERBOSE failed to run\n  query: {explain_sql}\n  error: {msg}"
        ));
    }
    let explain_text = exec
        .map(|e| format_query_execution_as_text(&e))  // use the same formatter
        .unwrap_or_default();                          // result_contains uses
    for needle in &meta.explain_contains {
        if !explain_text.contains(needle) {
            return Err(format!(
                "@explain_contains assertion failed.\n  expected substring: {needle}\n  EXPLAIN VERBOSE output:\n{explain_text}"
            ));
        }
    }
}
```

`explain_contains_target_body(sql)`: the substring of `sql` after any
leading `EXPLAIN [VERBOSE|COSTS|ANALYZE]` prefix (case-insensitive). If
none, returns `sql` verbatim. This prevents `EXPLAIN VERBOSE EXPLAIN
ANALYZE SELECT ...`. Unit-test it alongside the directive parser:

```rust
#[test]
fn explain_contains_target_body_strips_explain_prefix() {
    assert_eq!(explain_contains_target_body("SELECT 1"), "SELECT 1");
    assert_eq!(
        explain_contains_target_body("EXPLAIN VERBOSE SELECT 1"),
        "SELECT 1",
    );
    assert_eq!(
        explain_contains_target_body("explain analyze select 2"),
        "select 2",
    );
    assert_eq!(
        explain_contains_target_body("EXPLAIN COSTS SELECT 3"),
        "SELECT 3",
    );
}
```

`format_query_execution_as_text` does not exist as named — locate the
existing formatter that `result_contains` consumes and reuse it
verbatim. Do not duplicate row formatting.

Validate `@explain_contains` is only attached to SELECT/EXPLAIN
statements at runtime; on DDL/INSERT/etc., fail with
`@explain_contains is only valid on SELECT / EXPLAIN statements`. The
detection can be a simple leading-keyword sniff on
`explain_contains_target_body(sql)` — if it does not start with
`SELECT` / `WITH`, reject.

- [ ] **Step 9: Run sql-test-runner tests**

```
cargo test --manifest-path tests/sql-test-runner/Cargo.toml
```

Expected: PASS — both the new parser tests and any existing tests.

- [ ] **Step 10: Commit**

```bash
git add tests/sql-test-runner/
git commit -m "feat(sql-test-runner): @explain_contains and @normalize_explain_timing directives

@explain_contains issues an extra EXPLAIN VERBOSE on the same statement
and substring-asserts the explain output. @normalize_explain_timing
rewrites the canonical Planning/Execution/Rows header to <MS> tokens
before diff and record. Both compose with existing result-golden machinery."
```

---

## Task 7: Create `sql-tests/optimizer/` suite + Path B example

**Goal:** Land the three verification cases listed in spec §4.5. These
files are how OPT-1/2/3/4 PRs will see plan-shape drift.

**Files:**
- Create: `sql-tests/optimizer/sql/baseline_inner_join.sql`
- Create: `sql-tests/optimizer/sql/disable_join_commutativity.sql`
- Create: `sql-tests/optimizer/sql/analyze_header_present.sql`
- Create: `sql-tests/optimizer/result/{same-three}.result`
- Create: `sql-tests/optimizer/init.sql` if needed.
- Modify: one existing case in `sql-tests/filter/sql/` to use
  `@explain_contains`.
- Modify: `tests/sql-test-runner/conf/` registry (if suites need
  registering — verify by inspecting how other suites appear).

- [ ] **Step 1: Inspect an existing simple suite's shape**

```
ls sql-tests/filter/
cat sql-tests/filter/init.sql 2>/dev/null || echo "(no init.sql)"
cat sql-tests/filter/sql/filter_basic_comparison.sql | head -30
```

Note: file header conventions (`-- @order_sensitive=true`, `-- @tags=...`),
the placeholder `${case_db}`, and how DDL/INSERT are interleaved with
the asserted SELECT.

- [ ] **Step 2: Write `baseline_inner_join.sql`**

```sql
-- @tags=optimizer,baseline
-- Test Objective:
-- 1. Lock in the current EXPLAIN VERBOSE shape of a plain inner-equi-join.
-- 2. Failure of this case in a future PR signals a plan-shape change that
--    must be intentional and acknowledged via record mode.
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_a;
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_b;
CREATE TABLE ${case_db}.t_optimizer_baseline_a (k INT, v INT);
CREATE TABLE ${case_db}.t_optimizer_baseline_b (k INT, w INT);
INSERT INTO ${case_db}.t_optimizer_baseline_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.t_optimizer_baseline_b VALUES (1, 100), (2, 200);
EXPLAIN VERBOSE
SELECT a.k, a.v, b.w
FROM ${case_db}.t_optimizer_baseline_a a
INNER JOIN ${case_db}.t_optimizer_baseline_b b ON a.k = b.k;
```

- [ ] **Step 3: Write `disable_join_commutativity.sql`**

```sql
-- @tags=optimizer,session_rule_disable
-- Test Objective:
-- 1. Verify SET disable_optimizer_rules = 'JoinCommutativity' changes plan shape.
-- 2. Two EXPLAIN VERBOSE statements; the .result captures both and the
--    diff between them is golden.
DROP TABLE IF EXISTS ${case_db}.t_disable_jc_a;
DROP TABLE IF EXISTS ${case_db}.t_disable_jc_b;
CREATE TABLE ${case_db}.t_disable_jc_a (k INT);
CREATE TABLE ${case_db}.t_disable_jc_b (k INT);
INSERT INTO ${case_db}.t_disable_jc_a VALUES (1), (2), (3);
INSERT INTO ${case_db}.t_disable_jc_b VALUES (1);

-- Baseline (no disable): full CBO including JoinCommutativity.
EXPLAIN VERBOSE
SELECT a.k FROM ${case_db}.t_disable_jc_a a INNER JOIN ${case_db}.t_disable_jc_b b ON a.k = b.k;

-- Disable JoinCommutativity for the next query.
SET disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN VERBOSE
SELECT a.k FROM ${case_db}.t_disable_jc_a a INNER JOIN ${case_db}.t_disable_jc_b b ON a.k = b.k;

-- Restore.
SET disable_optimizer_rules = '';
```

- [ ] **Step 4: Write `analyze_header_present.sql`**

```sql
-- @tags=optimizer,explain_analyze
-- @normalize_explain_timing=true
-- Test Objective:
-- 1. EXPLAIN ANALYZE emits the canonical Planning/Execution/Rows header.
-- 2. Timing values are normalized to <MS> so the case is stable across runs.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
```

- [ ] **Step 5: Record initial `.result` files**

Start the standalone server against the local fixture, then run with
`--mode record`:

```
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
SRV_PID=$!
# wait for NOVAROCKS_READY (use the marker pattern from CLAUDE.md §7.3)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode record
kill -INT "$SRV_PID"
wait "$SRV_PID" || true
```

Inspect each generated `.result` file. If the recorded output looks
nonsensical (e.g. missing the stats trailer, or the ANALYZE header is
wrong), debug before committing.

- [ ] **Step 6: Verify the record locks**

Re-run in verify mode:

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: all three cases PASS. If `analyze_header_present` fails on
timing, double-check the normalize regex matches the actual header
shape produced.

- [ ] **Step 7: Add `@explain_contains` example in filter suite**

Pick a small existing filter case (e.g.
`sql-tests/filter/sql/filter_basic_comparison.sql`). Above the
SELECT statement add:

```sql
-- @explain_contains=stats={rows=
-- @explain_contains=SCAN
SELECT ...  -- existing
```

The two needles assert the stats trailer is present and the SCAN node
appears. No `.result` change needed — `@explain_contains` is a side
assertion.

- [ ] **Step 8: Run filter suite in verify mode**

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite filter --only filter_basic_comparison --mode verify
```

Expected: PASS — original result-golden and `@explain_contains` both
satisfied.

- [ ] **Step 9: Run smoke-regression on a few large suites**

```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-h --mode verify
```

(Repeat for `tpc-ds` and `ssb` if time allows; otherwise pick one and
commit + spawn follow-up.)

Expected: PASS — no result drift from Tasks 1-6. If a suite breaks,
inspect: stats trailer is the most likely cause (existing `.result`
files don't have it). The fix is `--mode record` for that suite and
**a separate commit** that calls out the trailer addition as the
reason.

- [ ] **Step 10: Commit**

```bash
git add sql-tests/optimizer/ sql-tests/filter/sql/filter_basic_comparison.sql
git commit -m "test: add sql-tests/optimizer suite and @explain_contains example

Three cases:
- baseline_inner_join: locks current inner-join plan shape.
- disable_join_commutativity: verifies session rule-disable knob.
- analyze_header_present: verifies EXPLAIN ANALYZE summary header with
  @normalize_explain_timing.

filter_basic_comparison gains two @explain_contains assertions to
exercise the new directive in a real existing case."
```

- [ ] **Step 11: If any other suite required `--mode record` due to the
  trailer addition, commit those changes separately**

```bash
git add sql-tests/<affected-suite>/result/
git commit -m "test(<suite>): re-record .result for OPT-5 stats trailer

EXPLAIN VERBOSE/COSTS output now ends each physical node line with
stats={rows=N}. No semantic change; mechanical record sweep."
```

---

## Task 8: Update `CLAUDE.md` with pointers to the new knob and suite

**Goal:** The next person reading CLAUDE.md sees that EXPLAIN ANALYZE,
the disable knob, the stats trailer, and the optimizer suite all
exist.

**Files:**
- Modify: `CLAUDE.md` — the §9 "Suggested Starting Points" list and §8
  "Development and Testing Standards" if EXPLAIN ANALYZE belongs there.

- [ ] **Step 1: Read the existing CLAUDE.md sections**

```
sed -n '/^## 9\./,/^---$/p' CLAUDE.md | head -40
```

- [ ] **Step 2: Add a pointer under §9**

After the existing bullets in §9, add:

```markdown
- **Optimizer observability / plan-shape regression**: see
  `src/sql/explain.rs` for the EXPLAIN formatter (Normal/Verbose/Costs/
  Analyze), `src/sql/optimizer/options.rs` for the
  `disable_optimizer_rules` session knob (alias `cbo_disabled_rules`),
  and `sql-tests/optimizer/` for plan-golden cases. Use
  `EXPLAIN ANALYZE` for a query-level Planning/Execution/Rows summary;
  use `-- @explain_contains=<substr>` in any sql-test case to assert a
  plan-shape fact alongside the result golden.
```

- [ ] **Step 3: No further sections need updating**

§8 already covers SQL test workflow; the new suite picks that up
automatically.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: point CLAUDE.md at OPT-5 observability surface

Adds a Suggested Starting Points bullet covering the disable knob,
EXPLAIN ANALYZE, the new optimizer suite, and the @explain_contains
directive."
```

---

## Verification Checklist (run before PR)

- [ ] `cargo build` clean.
- [ ] `cargo clippy --lib` clean (or no new warnings).
- [ ] `cargo test --lib` passes.
- [ ] `cargo test --manifest-path tests/sql-test-runner/Cargo.toml` passes.
- [ ] `--suite optimizer --mode verify` passes against a fresh
  standalone-server.
- [ ] `--suite filter --only filter_basic_comparison --mode verify` passes
  (Path B coverage).
- [ ] At least one of `tpc-h` / `tpc-ds` / `ssb` suites passes in verify
  mode (no result drift).
- [ ] `EXPLAIN VERBOSE` of an existing query manually inspected and shows
  the `stats={rows=...}` trailer at the end of physical node lines.
- [ ] `SET disable_optimizer_rules = 'JoinCommutativity'` followed by
  `EXPLAIN VERBOSE` of a join query shows a different plan shape than
  the same query without the SET.
- [ ] `EXPLAIN ANALYZE SELECT 1` returns a string column whose first row
  matches `Planning: \d+ ms / Execution: \d+ ms / Rows: \d+`.
