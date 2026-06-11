# Phase 4 Utils + Selectivity Move — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pure file move with no behavior change. Relocate `src/sql/optimizer/expr_utils.rs` to `src/sql/cascades/rbo/utils.rs`; relocate `estimate_selectivity` (and its private helpers `estimate_eq_selectivity`, `estimate_range_selectivity`, `extract_column_name`, `extract_literal_f64`) from `src/sql/optimizer/cardinality.rs` to `src/sql/cascades/stats.rs`. Rewrite all importers to the new paths. Delete `expr_utils.rs`. Keep `cardinality.rs` (its `estimate_statistics` still consumed by legacy `join_reorder`; Phase 5 moves that too).

**Architecture:** File move only — no signatures change, no logic touched. After this phase, the cascades layer owns its own utility module, severing the last cross-module dependency that Phase 3 bridged via `pub(crate)`. `cardinality.rs` remains but imports `estimate_selectivity` back from cascades.

**Tech Stack:** Rust 2021.

**Spec reference:** `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md` §4.4.

**Validation:** `cargo test -p novarocks --lib` count must match Phase 3 baseline (928 passed / 19 failed ± flake). End-to-end EXPLAIN diff is deferred — Phase 4 is pure relocation with no semantic change.

---

## Task 1: Create `src/sql/cascades/rbo/utils.rs` and register in `rbo/mod.rs`

Move `src/sql/optimizer/expr_utils.rs` verbatim into `src/sql/cascades/rbo/utils.rs`. Register the new module in `rbo/mod.rs`.

**Files:**
- Create: `src/sql/cascades/rbo/utils.rs`
- Modify: `src/sql/cascades/rbo/mod.rs`

- [ ] **Step 1: Copy the file.**

```bash
cd /Users/harbor/project/NovaRocks
cp src/sql/optimizer/expr_utils.rs src/sql/cascades/rbo/utils.rs
```

- [ ] **Step 2: Update the file header doc.**

Replace the first line (if it has one) or prepend a module-level doc comment at the top of `src/sql/cascades/rbo/utils.rs`:

```rust
//! Shared expression / plan utilities for the RBO rules and any cascades
//! code that needs small AST helpers. Moved from
//! `src/sql/optimizer/expr_utils.rs` in Phase 4 of the optimizer
//! unification; contents unchanged.
```

Preserve all existing function bodies exactly.

- [ ] **Step 3: Register the module.**

Edit `src/sql/cascades/rbo/mod.rs`. Add, near the other submodule declarations:

```rust
pub(crate) mod utils;
```

- [ ] **Step 4: Verify build.**

The crate should NOT build cleanly yet — `src/sql/optimizer/expr_utils.rs` still exists with `pub(crate)` visibility on the same names, and there is no consumer of the new module. That's fine. What must succeed: `cargo check --lib --quiet` reports no new errors beyond "unused module" / "unused import" warnings tied to the new file being unconsumed.

```bash
cargo build 2>&1 | tail -3
```

Expected: clean build (the new file is valid Rust; the old file is still the source of truth for current importers).

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/cascades/rbo/utils.rs src/sql/cascades/rbo/mod.rs
git commit -m "Phase 4 Task 1: copy expr_utils into src/sql/cascades/rbo/utils.rs

Pure relocation; contents unchanged. Subsequent Task 2 rewires importers
to the new path; Task 3 deletes the legacy copy.
"
```

---

## Task 2: Rewrite Importers to Use the New Path

Seven files currently `use crate::sql::optimizer::expr_utils::*`. Change each of them to `use crate::sql::cascades::rbo::utils::*`. The import lists themselves do not change — only the module path.

**Files (all modified):**
- `src/sql/optimizer/join_reorder.rs`
- `src/sql/cascades/rbo/rules/column_pruning.rs`
- `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_scan.rs`
- `src/sql/cascades/rbo/rules/predicate_pushdown/push_through_project.rs`
- `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_aggregate.rs`
- `src/sql/cascades/rbo/rules/predicate_pushdown/push_to_join.rs`
- `src/sql/cascades/rbo/rules/predicate_pushdown/semi_anti_condition.rs`

- [ ] **Step 1: Enumerate exact current import lines.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'use crate::sql::optimizer::expr_utils' --include='*.rs' src/ | tee /tmp/phase4-imports.txt
wc -l /tmp/phase4-imports.txt
```

Expected: 7 lines, one per file above. The exact import shape differs per file (some import a single symbol, others import several in `{…}` form).

- [ ] **Step 2: Replace each import path.**

For each line in `/tmp/phase4-imports.txt`, replace `crate::sql::optimizer::expr_utils` with `crate::sql::cascades::rbo::utils`. Preserve the rest of each `use` statement (the `::{…}` list or the trailing symbol) exactly.

Example edits:

- `use crate::sql::optimizer::expr_utils::{collect_column_refs, merge_needed};`
  becomes
  `use crate::sql::cascades::rbo::utils::{collect_column_refs, merge_needed};`

- `use crate::sql::optimizer::expr_utils::{split_and, wrap_remaining_filter};`
  becomes
  `use crate::sql::cascades::rbo::utils::{split_and, wrap_remaining_filter};`

The `Edit` tool with `replace_all=false` is the right mechanism per file since each change is a single occurrence. No logic edits; do not reflow the brace list.

- [ ] **Step 3: Verify build.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
```

Expected: clean build. Rustc will report "unused file" style warnings IF the new file is unconsumed; but with the imports rewired, every public symbol in the new file now has a consumer, while the OLD `src/sql/optimizer/expr_utils.rs` still defines the same names unused. Warnings about unused public items in `expr_utils.rs` are acceptable for this step — Task 3 deletes the file.

- [ ] **Step 4: Run cascades tests to confirm no semantic drift.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib sql::cascades 2>&1 | tail -5
```

Expected: pass count unchanged vs Phase 3 baseline (100 passed / 2 failed, same pre-existing flakes).

- [ ] **Step 5: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/optimizer/join_reorder.rs src/sql/cascades/rbo/rules/
git commit -m "Phase 4 Task 2: rewire expr_utils imports to cascades::rbo::utils

Seven files updated. No logic changes; only the module path component
changes. Task 3 deletes the legacy src/sql/optimizer/expr_utils.rs.
"
```

---

## Task 3: Delete Legacy `expr_utils.rs`

**Files:**
- Delete: `src/sql/optimizer/expr_utils.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Confirm no remaining importers.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'crate::sql::optimizer::expr_utils' --include='*.rs' src/ ; echo "done"
```

Expected: no matches (echo "done" is the only visible line). If any match appears, stop — Task 2 missed a file.

- [ ] **Step 2: Delete the file and its module declaration.**

```bash
cd /Users/harbor/project/NovaRocks
rm src/sql/optimizer/expr_utils.rs
```

Edit `src/sql/optimizer/mod.rs` and delete the line:

```rust
pub(crate) mod expr_utils;
```

- [ ] **Step 3: Verify build + tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -3
cargo test -p novarocks --lib 2>&1 | tail -3
```

Expected: clean build; 928 passed / 19 failed (Phase 3 baseline). Any new failure beyond pre-existing 19 is a regression — stop and investigate.

- [ ] **Step 4: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add -u src/sql/optimizer/
git commit -m "Phase 4 Task 3: delete legacy src/sql/optimizer/expr_utils.rs

All importers moved to src/sql/cascades/rbo/utils.rs in Task 2. The
legacy file is now unused.
"
```

---

## Task 4: Move `estimate_selectivity` into `src/sql/cascades/stats.rs`

Move `estimate_selectivity` and the four private helpers it calls (`estimate_eq_selectivity`, `estimate_range_selectivity`, `extract_column_name`, `extract_literal_f64`) from `src/sql/optimizer/cardinality.rs` into `src/sql/cascades/stats.rs`. Leave `estimate_statistics` (and its helpers `estimate_scan`, `estimate_filter`, ..., `get_expr_ndv`) in place — Phase 5 moves those with join_reorder. Import the moved helpers back into `cardinality.rs` and anywhere else that calls them.

**Files:**
- Modify: `src/sql/cascades/stats.rs` (add functions)
- Modify: `src/sql/optimizer/cardinality.rs` (remove functions, add import of the moved ones)

- [ ] **Step 1: Read current call sites.**

```bash
cd /Users/harbor/project/NovaRocks
grep -rn 'estimate_selectivity' --include='*.rs' src/ | tee /tmp/phase4-selectivity.txt
```

Expected: calls from
- `src/sql/optimizer/cardinality.rs` (internal: `estimate_scan`, `estimate_filter`, `estimate_join` — 4 call sites near lines 81, 123, 248, 256)
- `src/sql/cascades/stats.rs` (existing `use crate::sql::optimizer::cardinality::estimate_selectivity` per spec line 453)
- Maybe test modules inside `cardinality.rs` (lines ~745, 769, 797)

List the exact files. This plan assumes those are the only consumers; if grep reveals additional callers, extend Step 3 accordingly.

- [ ] **Step 2: Copy the five functions into `src/sql/cascades/stats.rs`.**

Append to `src/sql/cascades/stats.rs` (near the bottom of the existing file, above any test module if present). Copy the bodies verbatim from `src/sql/optimizer/cardinality.rs` lines 349-500 (approximate — confirm by reading the file):

- `pub(crate) fn estimate_selectivity(expr: &TypedExpr, column_stats: &HashMap<String, ColumnStatistic>) -> f64` (legacy line 350)
- `fn estimate_eq_selectivity(left: &TypedExpr, right: &TypedExpr, column_stats: &HashMap<String, ColumnStatistic>) -> f64` (line 427) — make `pub(crate)` so cardinality.rs can import it IF still needed there; otherwise keep private. Grep verifies: if only `estimate_selectivity` calls it, keep it private in stats.rs.
- `fn estimate_range_selectivity(...)` (line 445) — same guidance.
- `fn extract_column_name(expr: &TypedExpr) -> Option<&str>` (line 479) — **MUST** be `pub(crate)` because `get_expr_ndv` in the remaining `cardinality.rs` calls it (legacy line 503).
- `fn extract_literal_f64(expr: &TypedExpr) -> Option<f64>` (line 489) — private unless `cardinality.rs` uses it elsewhere (grep to confirm; likely private).

Imports needed at the top of `src/sql/cascades/stats.rs` for the moved code:

```rust
use crate::sql::ir::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::statistics::{ColumnStatistic, IS_NULL_FILTER, PREDICATE_UNKNOWN_FILTER};
```

Check whether any of these are already imported in `stats.rs` before adding duplicates.

- [ ] **Step 3: Remove the moved functions from `src/sql/optimizer/cardinality.rs` and add the re-import.**

Delete the five function definitions from `src/sql/optimizer/cardinality.rs`. Near the top of the file, add:

```rust
use crate::sql::cascades::stats::{estimate_selectivity, extract_column_name};
```

(Only include `extract_column_name` if `cardinality.rs` still references it — which `get_expr_ndv` does. If `estimate_eq_selectivity`, `estimate_range_selectivity`, or `extract_literal_f64` are also referenced elsewhere in `cardinality.rs` outside the now-deleted functions, either (a) promote them to `pub(crate)` in `stats.rs` and re-import, or (b) keep copies in `cardinality.rs`. Grep in Step 1 tells you which case you're in.)

- [ ] **Step 4: Update the existing line in `src/sql/cascades/stats.rs`.**

The spec Task 4.3 says `src/sql/cascades/stats.rs` currently reads:

```rust
use crate::sql::optimizer::cardinality::estimate_selectivity;
```

Since `estimate_selectivity` now lives in the same file, delete that `use` statement. If `stats.rs` has other `use crate::sql::optimizer::cardinality::...` items besides `estimate_selectivity`, leave those alone.

- [ ] **Step 5: Verify build + tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo build 2>&1 | tail -5
cargo test -p novarocks --lib 2>&1 | tail -3
```

Expected: clean build; 928 passed / 19 failed.

- [ ] **Step 6: Commit.**

```bash
cd /Users/harbor/project/NovaRocks
git add -u src/sql/
git commit -m "Phase 4 Task 4: move estimate_selectivity into src/sql/cascades/stats.rs

Moves estimate_selectivity and four private helpers (estimate_eq_selectivity,
estimate_range_selectivity, extract_column_name, extract_literal_f64) from
src/sql/optimizer/cardinality.rs to src/sql/cascades/stats.rs. The remaining
cardinality.rs (estimate_statistics + its helpers, used by legacy join_reorder
until Phase 5) imports the moved functions back. Logic unchanged.
"
```

---

## Task 5: Phase-4 Landing Verification + Spec Note

**Files:**
- Modify: `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md`
- Update: `/Users/harbor/.claude/projects/-Users-harbor-project-NovaRocks/memory/project_optimizer_unification_progress.md`

- [ ] **Step 1: Re-run full unit tests.**

```bash
cd /Users/harbor/project/NovaRocks
cargo test -p novarocks --lib 2>&1 | tail -3
```

Record the exact count. Expected: 928 passed / 19 failed (Phase 3 baseline). Tolerate ±1 on pass / fail due to the known flaky tests observed during Phase 3.

- [ ] **Step 2: Record the landing SHA.**

```bash
cd /Users/harbor/project/NovaRocks
git rev-parse HEAD
```

- [ ] **Step 3: Append landing note to the spec.**

Edit `docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md`. After the line
`**Commit:** \`Phase 4: move shared utilities and selectivity into cascades\``
(end of §4.4), add:

```markdown

**Phase 4 landed.** Date: 2026-04-13. HEAD at landing: <PASTE SHA>. Pure file move, no behavior change. Relocated `src/sql/optimizer/expr_utils.rs` → `src/sql/cascades/rbo/utils.rs` (file deleted, 7 importers rewired). Relocated `estimate_selectivity` + 4 private helpers from `src/sql/optimizer/cardinality.rs` → `src/sql/cascades/stats.rs`; `cardinality.rs` imports them back (remains in place for legacy `join_reorder` until Phase 5). Unit tests: <N> passed / <F> failed (matches Phase 3 baseline). Phase 5 (wrap join_reorder as a cascades Rule) is unblocked.
```

Fill in `<PASTE SHA>`, `<N>`, `<F>` from Steps 1 and 2.

- [ ] **Step 4: Update memory entry.**

Edit `/Users/harbor/.claude/projects/-Users-harbor-project-NovaRocks/memory/project_optimizer_unification_progress.md`. Change the Phase 4 line from `NEXT` to `LANDED 2026-04-13 at <SHA>`. Change the Phase 5 line from `not started` to `NEXT`.

- [ ] **Step 5: Commit the landing note.**

```bash
cd /Users/harbor/project/NovaRocks
git add docs/design/specs/2026-04-13-unify-rbo-cbo-optimizer-design.md docs/design/plans/2026-04-13-phase4-utils-and-selectivity-move.md
git commit -m "$(cat <<'EOF'
Phase 4 utils + selectivity move: mark spec section 4.4 as landed

Pure file move: src/sql/optimizer/expr_utils.rs → src/sql/cascades/rbo/utils.rs
(deleted legacy), estimate_selectivity + helpers → src/sql/cascades/stats.rs.
cardinality.rs imports the moved functions back until Phase 5 moves
estimate_statistics with join_reorder. Tests: matches Phase 3 baseline.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Invoke the finishing-a-development-branch skill for completion handoff.**
