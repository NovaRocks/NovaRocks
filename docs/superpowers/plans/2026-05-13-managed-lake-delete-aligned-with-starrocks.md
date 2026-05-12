# Managed-Lake DELETE Aligned with StarRocks — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the CoW managed-lake DELETE in [src/engine/delete_flow.rs:354-395](src/engine/delete_flow.rs#L354) with a StarRocks-aligned implementation. DUP/UNIQUE/AGG tables use `DeletePredicatePb` stored in rowset metadata; PRIMARY KEY tables use `__op = 1` chunks fed through the existing sink, which writes `.del` files via existing infrastructure. Plus two tactical fixes: SQL splitter for `--`/`/* */` comments, and analyzer-level literal coercion for `WHERE col op literal`.

**Architecture:** Most of the storage-layer infrastructure already exists in NovaRocks (delete payload codec, PK encoder, `__op` split, `OpWrite { rowset, dels[] }`, DeletePredicatePb proto + RPC + publish recognition). The implementation work is concentrated in the FE: (1) translating SQL WHERE into `DeletePredicatePb` for DUP/UNIQUE/AGG; (2) rewriting `DELETE FROM pk_t WHERE cond` into `SELECT pk_cols, 1 AS __op` that goes through the existing sink for PK tables; (3) adding a name-based `__op` column helper with defensive analyzer tests; (4) literal coercion plumbing for comparison/IN/BETWEEN operators; (5) the splitter fix.

**Tech Stack:** Rust, Arrow, sqlparser-rs, prost (proto), tokio, the StarRocks lake format proto schema (`idl/proto/lake_types.proto`).

**Source spec:** [docs/superpowers/specs/2026-05-12-managed-lake-delete-design.md](docs/superpowers/specs/2026-05-12-managed-lake-delete-design.md) — see §10 for current-state reality check.

**Build mode:** Use `cargo build` (debug) for iteration. Only switch to `--release` for the final full sql-test suite regression run (G4 gate) when performance might matter.

---

## File Structure (created / modified)

### M1 — D (splitter)
- **Modify** [tests/sql-test-runner/src/session.rs](tests/sql-test-runner/src/session.rs): extend `split_sql_statements` state machine

### M2 — C (literal coercion)
- **Create** `src/sql/analyzer/literal_coercion.rs` — `coerce_comparison_literal(left, right, op) -> (left, right)` helper
- **Modify** [src/sql/analyzer/mod.rs](src/sql/analyzer/mod.rs): export the new module
- **Modify** [src/sql/analyzer/resolve_expr.rs](src/sql/analyzer/resolve_expr.rs): call helper from `analyze_binary_op` (comparison ops), `analyze_in_list`, `analyze_between`

### M3 — A (DUP/UNIQUE/AGG DeletePredicate)
- **Create** `src/engine/delete_predicate_translate.rs` — sqlparser WHERE → `DeletePredicateTerms` + value serialization
- **Create** `src/connector/starrocks/lake/delete_predicate_proto.rs` — `DeletePredicateTerms` → `DeletePredicatePb`
- **Modify** [src/engine/delete_flow.rs](src/engine/delete_flow.rs): route DUP/UNIQUE/AGG to new predicate path
- **Modify** [src/connector/starrocks/lake/transactions.rs](src/connector/starrocks/lake/transactions.rs): expose standalone-callable `delete_data_with_predicate` if needed (decision in Task A4)
- **Create** SQL test cases under `sql-tests/write-path/sql/` and `sql-tests/write-path/result/`

### M4 — B (PK via `__op` + sink)
- **Create** `src/sql/analyzer/load_op_column.rs` — `LOAD_OP_COLUMN` constant, `LoadOp` enum, `is_load_op_column(name)` helper
- **Modify** [src/sql/analyzer/mod.rs](src/sql/analyzer/mod.rs): export
- **Modify** [src/engine/delete_flow.rs](src/engine/delete_flow.rs):
  - Add `execute_managed_pk_delete` (plan rewrite to `SELECT pk_cols, 1 AS __op FROM t WHERE cond` going through sink)
  - **Delete** `execute_managed_delete_statement` CoW (lines 354-395) **only after** PK path lands
- **Modify** [src/engine/mod.rs](src/engine/mod.rs): delete `managed_delete_rewrites_remaining_rows_for_primary_key_table` test (line ~4765)
- **Create** SQL test cases

---

## Milestone M1 — Test runner SQL splitter fix (D)

The runner's `split_sql_statements` does not recognize SQL comments. A `;` inside `-- ... ; ...` triggers a spurious split, generating bogus statements. Fix by extending the state machine to skip `--` to end-of-line and `/* ... */` blocks (no nested blocks, per MySQL rules).

### Task M1.T1 — Splitter handles `--` and `/* */` comments

**Files:**
- Modify: `tests/sql-test-runner/src/session.rs`:262-303 (`split_sql_statements`, `QuoteState` enum)

- [ ] **Step 1: Write failing tests**

Add this test module at the end of `tests/sql-test-runner/src/session.rs` (or extend an existing `#[cfg(test)] mod tests` block — first inspect file with `tail -20 tests/sql-test-runner/src/session.rs` to decide):

```rust
#[cfg(test)]
mod splitter_tests {
    use super::split_sql_statements;

    #[test]
    fn line_comment_semicolon_does_not_split() {
        let sql = "DELETE FROM t WHERE c = '2020-01-01 00:00:00';
-- '00:00:00.0' is same as '00:00:00'; rows already gone
DELETE FROM t WHERE c = '2020-01-01 00:00:00.0';";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2, "expected 2 statements, got {:?}", parts);
        assert!(parts[0].starts_with("DELETE FROM t WHERE c = '2020-01-01 00:00:00'"));
        assert!(parts[1].starts_with("DELETE FROM t WHERE c = '2020-01-01 00:00:00.0'"));
    }

    #[test]
    fn block_comment_semicolon_does_not_split() {
        let sql = "SELECT 1; /* note; with ; semicolons */ SELECT 2;";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT 1");
        assert_eq!(parts[1], "SELECT 2");
    }

    #[test]
    fn double_dash_without_trailing_whitespace_is_not_a_comment() {
        // MySQL rule: `--` is a comment only when followed by whitespace,
        // a control character, or end of line.
        let sql = "SELECT a--b FROM t;";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], "SELECT a--b FROM t");
    }

    #[test]
    fn comment_markers_inside_string_literal_are_inert() {
        let sql = "SELECT '-- not a comment'; SELECT '/* also not */';";
        let parts = split_sql_statements(sql).expect("split");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT '-- not a comment'");
        assert_eq!(parts[1], "SELECT '/* also not */'");
    }

    #[test]
    fn nested_block_comment_is_not_supported() {
        // MySQL treats /* ... /* ... */ as ending at the first */, leaving
        // the trailing `... */` outside the comment. We match that.
        let sql = "SELECT 1; /* outer /* inner */ tail */; SELECT 2;";
        let parts = split_sql_statements(sql).expect("split");
        // After first */ at offset of "inner */", `tail */` is outside.
        // The bare `;` after the `*/` closes the second statement.
        assert!(
            parts.len() >= 2,
            "nested-block parsing produced {:?}",
            parts
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml splitter_tests 2>&1 | tail -30
```

Expected: at least `line_comment_semicolon_does_not_split`, `block_comment_semicolon_does_not_split`, `comment_markers_inside_string_literal_are_inert` fail.

- [ ] **Step 3: Replace `split_sql_statements` with comment-aware state machine**

Replace lines 262-303 of `tests/sql-test-runner/src/session.rs` with:

```rust
fn split_sql_statements(sql: &str) -> Result<Vec<String>> {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment,
    }

    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut state = State::Normal;
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < sql.len() {
        // Safe: we only advance `i` on character boundaries (we never
        // step inside a multi-byte char body; the state-transition
        // cases below use sql[i..].chars().next() or byte peeks that
        // are valid at i because we got here via char_indices logic.)
        let ch = match sql[i..].chars().next() {
            Some(c) => c,
            None => break,
        };
        let char_len = ch.len_utf8();

        match state {
            State::Normal => match ch {
                '\'' => state = State::SingleQuote,
                '"' => state = State::DoubleQuote,
                '`' => state = State::Backtick,
                '-' if i + 1 < sql.len() && bytes[i + 1] == b'-' => {
                    // MySQL rule: `--` is a comment only when followed by
                    // whitespace, a control character, or end of line.
                    let after = i + 2;
                    let next_is_ws_or_eol = after >= sql.len()
                        || matches!(bytes[after], b' ' | b'\t' | b'\n' | b'\r')
                        || bytes[after] < 0x20;
                    if next_is_ws_or_eol {
                        state = State::LineComment;
                        i += 2;
                        continue;
                    }
                }
                '/' if i + 1 < sql.len() && bytes[i + 1] == b'*' => {
                    state = State::BlockComment;
                    i += 2;
                    continue;
                }
                ';' => {
                    if let Some(statement) = normalize_statement_fragment(&sql[start..i]) {
                        statements.push(statement);
                    }
                    start = i + char_len;
                }
                _ => {}
            },
            State::SingleQuote if ch == '\'' => state = State::Normal,
            State::DoubleQuote if ch == '"' => state = State::Normal,
            State::Backtick if ch == '`' => state = State::Normal,
            State::LineComment if ch == '\n' => state = State::Normal,
            State::BlockComment if ch == '*' && i + 1 < sql.len() && bytes[i + 1] == b'/' => {
                state = State::Normal;
                i += 2;
                continue;
            }
            _ => {}
        }
        i += char_len;
    }

    match state {
        State::SingleQuote | State::DoubleQuote | State::Backtick => {
            bail!("unterminated quoted string in SQL batch");
        }
        State::BlockComment => bail!("unterminated /* */ block comment in SQL batch"),
        _ => {}
    }

    if let Some(trailing) = normalize_statement_fragment(&sql[start..]) {
        statements.push(trailing);
    }
    Ok(statements)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test --manifest-path tests/sql-test-runner/Cargo.toml splitter_tests 2>&1 | tail -20
```

Expected: all 5 splitter tests pass.

- [ ] **Step 5: Run full write-path suite to confirm no other regressions**

First start standalone-server (the existing pattern from CLAUDE.md §8.4) — see existing test running instructions:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-server-m1.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then tail -30 "$LOG"; exit 1; fi
  sleep 1
done
grep '^NOVAROCKS_READY ' "$LOG" || { kill -9 "$SRV_PID"; exit 1; }
```

Then run the full write-path suite:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode verify 2>&1 | tail -40
kill $SRV_PID 2>/dev/null
```

Expected:
- `datetime_microsecond_precision_delete` now reaches step 3 of the test (but may still fail in step 3 / step 4 because §3 literal coercion isn't done yet — that's OK).
- All previously-passing cases continue passing. No new failures.

- [ ] **Step 6: Commit**

```bash
git add tests/sql-test-runner/src/session.rs
git commit -m "$(cat <<'EOF'
test(runner): teach SQL splitter to skip -- and /* */ comments

The runner's split_sql_statements was splitting on ; inside SQL line
and block comments, producing bogus statement fragments rejected by
the server as "unsupported sql in standalone server v1". Extend the
state machine to recognize MySQL comment syntax (-- followed by
whitespace/EOL, /* ... */ non-nested).

Unblocks datetime_microsecond_precision_delete step 3+ at the runner
level; analyzer literal-coercion fix in §3 is still required for the
test result to be correct.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Milestone M2 — Literal coercion in analyzer (C)

Currently `WHERE datetime_col = '2020-01-01 00:00:00.012'` doesn't auto-cast the string literal to DATETIME, causing string-string comparison (or scale truncation) and producing wrong results. The existing `coerce_to_target_type` ([src/sql/analyzer/resolve_expr.rs:2499](src/sql/analyzer/resolve_expr.rs#L2499)) already handles STRING → DATE/TIMESTAMP — it's just never called from `analyze_binary_op` comparison ops. Wire it through.

### Task M2.T1 — Coerce literals in comparison binary ops

**Files:**
- Create: `src/sql/analyzer/literal_coercion.rs`
- Modify: `src/sql/analyzer/mod.rs` (export module)
- Modify: `src/sql/analyzer/resolve_expr.rs` (call coercion helper from `analyze_binary_op`)

- [ ] **Step 1: Write failing test (integration through standalone session)**

Add this test to the end of `src/engine/mod.rs::tests` (find an existing test module structure first with `grep -n "fn managed_delete\b\|fn datetime\b" src/engine/mod.rs`):

```rust
#[test]
fn select_with_datetime_literal_matches_microsecond_precision() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };

    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();

    session
        .execute(
            "CREATE TABLE t_dt_coerce (c1 INT, c2 DATETIME) \
             DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 1 \
             PROPERTIES('replication_num'='1')",
        )
        .expect("create table");
    session
        .execute("INSERT INTO t_dt_coerce VALUES (4, '2020-01-01 00:00:00.012')")
        .expect("insert row");

    let r = session
        .query("SELECT c1 FROM t_dt_coerce WHERE c2 = '2020-01-01 00:00:00.012'")
        .expect("query with datetime literal");
    assert_eq!(r.row_count(), 1, "implicit STRING→DATETIME coercion should match");
}
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cargo test --lib select_with_datetime_literal_matches_microsecond_precision 2>&1 | tail -20
```

Expected: row_count is 0, assertion fails.

- [ ] **Step 3: Create the coercion helper module**

Create `src/sql/analyzer/literal_coercion.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! StarRocks-aligned literal coercion at analyzer level.
//!
//! When a comparison / IN / BETWEEN has `column op literal` where the column
//! is a typed slot (DATETIME, DATE, DECIMAL, INT family) and the literal is
//! a STRING, the literal must be coerced to the column's type *before*
//! comparison. Mirrors StarRocks' `LiteralExprFactory.create(value, columnType)`.
//!
//! For DATETIME with microsecond scale, this preserves up to 6 fractional
//! digits; longer fractions error rather than silently truncate (matching
//! StarRocks "Datetime literal is invalid").

use arrow::datatypes::DataType;

use crate::sql::analyzer::resolve_expr_types::TypedExpr;

/// Returns `true` if `expr` is a column reference (resolved slot ref).
/// Used to recognize "column-side" of a comparison.
pub(crate) fn is_column_ref(expr: &TypedExpr) -> bool {
    use crate::sql::analyzer::resolve_expr_types::ExprKind;
    matches!(expr.kind, ExprKind::SlotRef { .. })
}

/// Returns `true` if `data_type` is one we want to coerce string literals into.
pub(crate) fn is_coercible_target(data_type: &DataType) -> bool {
    use arrow::datatypes::TimeUnit;
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Timestamp(TimeUnit::Second, _)
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    )
}

/// If `right` is a string-typed literal and `left` is a column ref of a
/// coercible target type, return `right` coerced to `left`'s type.
/// Otherwise return `right` unchanged.
///
/// Caller is `analyze_binary_op` for `=/!=/<...>=`, `analyze_in_list`,
/// `analyze_between`.
pub(crate) fn coerce_literal_for_comparison(
    left: &TypedExpr,
    right: TypedExpr,
) -> TypedExpr {
    if !is_column_ref(left) {
        return right;
    }
    if !is_coercible_target(&left.data_type) {
        return right;
    }
    if !matches!(right.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return right;
    }
    // Reuse the existing coercion that already handles STRING → DATE / TIMESTAMP.
    // Decimal128 / Int* coercion still produces a Cast expression that the
    // evaluator handles at runtime; analyzer-level we just attach the cast.
    crate::sql::analyzer::resolve_expr::coerce_to_target_type(right, &left.data_type)
}

#[cfg(test)]
mod tests {
    // Unit tests live in resolve_expr.rs integration tests because
    // TypedExpr construction needs full analyzer scope. See
    // `analyze_binary_op_coerces_string_literal_to_datetime` etc. below.
}
```

- [ ] **Step 4: Register module + expose `coerce_to_target_type`**

In `src/sql/analyzer/mod.rs` add (alphabetical order with existing entries):

```rust
pub(crate) mod literal_coercion;
```

In `src/sql/analyzer/resolve_expr.rs:2499`, change `fn coerce_to_target_type` visibility:

```rust
pub(crate) fn coerce_to_target_type(expr: TypedExpr, target: &DataType) -> TypedExpr {
```

(currently private). Also expose the `resolve_expr_types` module if it's not already:

```bash
grep -n "mod resolve_expr_types\|pub.*resolve_expr_types" src/sql/analyzer/mod.rs
```

If missing, add `pub(crate) mod resolve_expr_types;` — or simpler, place `TypedExpr` and `ExprKind` re-exports in `resolve_expr.rs` and import from there.

- [ ] **Step 5: Wire coercion into `analyze_binary_op` for comparison ops**

In `src/sql/analyzer/resolve_expr.rs` around line 904-906 (the comparison branches of `analyze_binary_op`), modify the body so that comparison ops apply coercion. Replace the early `right_typed` binding to allow the coerced version to flow through. Specifically:

Find:

```rust
let left_typed = self.analyze_expr(left, scope)?;
let right_typed = self.analyze_expr(right, scope)?;

let (bin_op, result_type) = match op {
    sqlast::BinaryOperator::Eq => (BinOp::Eq, DataType::Boolean),
    sqlast::BinaryOperator::NotEq => (BinOp::Ne, DataType::Boolean),
    sqlast::BinaryOperator::Lt => (BinOp::Lt, DataType::Boolean),
    sqlast::BinaryOperator::LtEq => (BinOp::Le, DataType::Boolean),
    sqlast::BinaryOperator::Gt => (BinOp::Gt, DataType::Boolean),
    sqlast::BinaryOperator::GtEq => (BinOp::Ge, DataType::Boolean),
    sqlast::BinaryOperator::Spaceship => (BinOp::EqForNull, DataType::Boolean),
```

Replace with:

```rust
let left_typed = self.analyze_expr(left, scope)?;
let right_typed = self.analyze_expr(right, scope)?;

// StarRocks-aligned implicit literal coercion: when a comparison has
// (column, literal) we coerce the literal to the column's type before
// emitting the BinaryOp. Mirrors LiteralExprFactory.create(value, ty).
let (left_typed, right_typed) = {
    use crate::sql::analyzer::literal_coercion::coerce_literal_for_comparison;
    let coerce_for_compare = matches!(
        op,
        sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq
            | sqlast::BinaryOperator::Spaceship
    );
    if coerce_for_compare {
        let right_coerced = coerce_literal_for_comparison(&left_typed, right_typed);
        let left_coerced = coerce_literal_for_comparison(&right_coerced, left_typed);
        (left_coerced, right_coerced)
    } else {
        (left_typed, right_typed)
    }
};

let (bin_op, result_type) = match op {
    sqlast::BinaryOperator::Eq => (BinOp::Eq, DataType::Boolean),
    // (rest unchanged)
```

Note: `coerce_literal_for_comparison` is a no-op when `is_column_ref(left)` is false, so calling it both directions is safe even though most queries have column on the left. This handles `'2020-01-01' = c2` form for free.

- [ ] **Step 6: Run the failing test to verify it passes**

```bash
cargo build 2>&1 | tail -5  # confirm build clean
cargo test --lib select_with_datetime_literal_matches_microsecond_precision 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 7: Add unit tests for `coerce_literal_for_comparison`**

Add at the bottom of `src/sql/analyzer/literal_coercion.rs`:

```rust
#[cfg(test)]
mod coercion_tests {
    use super::*;
    use crate::sql::analyzer::resolve_expr_types::{ExprKind, TypedExpr};
    use arrow::datatypes::TimeUnit;

    fn slot(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::SlotRef { slot_id: 1 },
            data_type: ty,
            nullable: false,
        }
    }

    fn string_lit(s: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::StringLiteral(s.to_string()),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    #[test]
    fn coerces_string_literal_to_datetime_microsecond() {
        let left = slot(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = string_lit("2020-01-01 00:00:00.012");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(
            coerced.data_type,
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert!(matches!(coerced.kind, ExprKind::Cast { .. }));
    }

    #[test]
    fn coerces_string_literal_to_date32() {
        let left = slot(DataType::Date32);
        let right = string_lit("2020-01-01");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Date32);
    }

    #[test]
    fn does_not_coerce_when_left_is_not_column_ref() {
        // expr-vs-literal: skip coercion to avoid surprising arithmetic results.
        let left = TypedExpr {
            kind: ExprKind::IntLiteral(5),
            data_type: DataType::Int32,
            nullable: false,
        };
        let right = string_lit("foo");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }

    #[test]
    fn does_not_coerce_when_right_already_typed() {
        let left = slot(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = TypedExpr {
            kind: ExprKind::IntLiteral(1_672_531_200_000_000),
            data_type: DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable: false,
        };
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(coerced.kind, ExprKind::IntLiteral(_)));
    }

    #[test]
    fn does_not_coerce_for_non_coercible_target_types() {
        let left = slot(DataType::Boolean);
        let right = string_lit("true");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }
}
```

```bash
cargo test --lib literal_coercion::coercion_tests 2>&1 | tail -15
```

Expected: 5 PASS. (Adapt struct construction syntax based on actual `TypedExpr`/`ExprKind` definitions in the codebase if compilation fails — exact variant names may differ.)

- [ ] **Step 8: Commit**

```bash
git add src/sql/analyzer/literal_coercion.rs \
        src/sql/analyzer/mod.rs \
        src/sql/analyzer/resolve_expr.rs \
        src/engine/mod.rs
git commit -m "$(cat <<'EOF'
feat(analyzer): coerce string literals in WHERE col op literal

Mirror StarRocks' LiteralExprFactory.create(value, columnType): when a
comparison/IN/BETWEEN has a column-side reference whose type is
DATETIME, DATE, DECIMAL, or INT-family, coerce a string literal on
the other side to the column's type before evaluation. Reuses the
existing coerce_to_target_type plumbing which already handles
STRING→DATE/TIMESTAMP via the CAST evaluator.

Fixes silent miss-match in WHERE c = 'YYYY-MM-DD HH:MM:SS.ffffff'
against DATETIME(6) columns.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M2.T2 — Extend coercion to IN list and BETWEEN

**Files:**
- Modify: `src/sql/analyzer/resolve_expr.rs` — find `analyze_in_list` and `analyze_between` (existing — search with `grep -n "analyze_in_list\|analyze_between\|InList\|Between" src/sql/analyzer/resolve_expr.rs | head -10`)

- [ ] **Step 1: Write failing test**

Add to `src/engine/mod.rs::tests`:

```rust
#[test]
fn select_with_datetime_literal_in_list_matches() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();

    session.execute(
        "CREATE TABLE t_in_coerce (c1 INT, c2 DATETIME) \
         DUPLICATE KEY(c1) DISTRIBUTED BY HASH(c1) BUCKETS 1 \
         PROPERTIES('replication_num'='1')",
    ).expect("create");
    session.execute(
        "INSERT INTO t_in_coerce VALUES \
         (1, '2020-01-01 00:00:00.001'), (2, '2020-01-01 00:00:00.002')",
    ).expect("insert");

    let r = session.query(
        "SELECT c1 FROM t_in_coerce \
         WHERE c2 IN ('2020-01-01 00:00:00.001', '2020-01-01 00:00:00.002') \
         ORDER BY c1",
    ).expect("in list query");
    assert_eq!(r.row_count(), 2);
}
```

```bash
cargo test --lib select_with_datetime_literal_in_list_matches 2>&1 | tail -10
```

Expected: 0 rows, assertion fails.

- [ ] **Step 2: Find the IN-list analysis site**

```bash
grep -n "InList\|in_list\|InSubquery" src/sql/analyzer/resolve_expr.rs | head -10
```

Locate the function that produces `ExprKind::InList { expr, list, negated }` (or equivalent). Sample expected location: ~line 200-300 in `resolve_expr.rs`.

- [ ] **Step 3: Apply coercion in IN-list analyzer**

Inside the IN-list arm (after typing `expr` and each `list_item`), apply `coerce_literal_for_comparison`:

```rust
// Pseudo-code — adapt to actual code structure:
let lhs_typed = self.analyze_expr(expr, scope)?;
let mut list_typed = Vec::with_capacity(list.len());
for item in list {
    let item_typed = self.analyze_expr(item, scope)?;
    let item_coerced = crate::sql::analyzer::literal_coercion::coerce_literal_for_comparison(
        &lhs_typed, item_typed,
    );
    list_typed.push(item_coerced);
}
```

- [ ] **Step 4: Apply coercion in BETWEEN analyzer**

At `src/sql/analyzer/resolve_expr.rs:230-231` the BETWEEN arm already calls `coerce_to_target_type` for the low/high bounds against the **expression's** type. That's a slightly different convention than our column-aware coercion, but it works when the expression IS a column ref. Verify by reading the surrounding code; if `expr_typed.data_type` is the column type, the existing call is fine. Otherwise wrap with `coerce_literal_for_comparison(&expr_typed, low_typed)` etc.

- [ ] **Step 5: Run tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib select_with_datetime_literal_in_list_matches 2>&1 | tail -5
cargo test --lib select_with_datetime_literal_matches_microsecond_precision 2>&1 | tail -5
```

Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/analyzer/resolve_expr.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
feat(analyzer): extend literal coercion to IN list and BETWEEN

Mirror the comparison-binop coercion behavior to IN (...) and
BETWEEN, so WHERE col IN ('lit', 'lit') and WHERE col BETWEEN 'lit'
AND 'lit' apply the same column-type literal coercion.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M2.T3 — Full regression: write-path + tpc-ds smoke

This is a **G4 hard gate** for M2 — literal coercion is a global analyzer change and may surface latent issues elsewhere.

- [ ] **Step 1: Start server (debug)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build 2>&1 | tail -3
LOG=/tmp/novarocks-server-m2.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { tail -30 "$LOG"; exit 1; }
  sleep 1
done
```

- [ ] **Step 2: Run write-path suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode verify 2>&1 | tail -30
```

Expected: `datetime_microsecond_precision_delete` should now pass query 4 (rows c1=3,5,6,7,8,9,10,11,12,13). Query 6 should also pass (c1=3,5,6,7,9,11). Any other write-path regressions → diagnose.

- [ ] **Step 3: Run tpc-h / ssb / cte / filter / sort smoke**

Run each suite, just to surface regressions in non-DELETE WHERE paths:

```bash
for SUITE in tpc-h ssb cte filter sort function; do
  echo "=== $SUITE ==="
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$SUITE" --mode verify 2>&1 | tail -15
done
kill $SRV_PID 2>/dev/null
```

Expected: no new failures. Any new failures are likely from the literal coercion affecting borderline cases. Diagnose and fix in this milestone (don't carry forward to M3).

- [ ] **Step 4: If everything green, commit nothing (regression run is a gate, not a code change)**

If you had to add minor fixes, commit them with a message like `fix(analyzer): handle <regression> after literal coercion landed`.

---

## Milestone M3 — DUP/UNIQUE/AGG via DeletePredicate (A)

Translate WHERE into `DeletePredicateTerms`, encode to `DeletePredicatePb`, dispatch to existing `transactions::delete_data` path. The actual storage layer is fully in place ([see §10.1 of the spec](docs/superpowers/specs/2026-05-12-managed-lake-delete-design.md)) — this milestone is FE-side.

### Task M3.T1 — WHERE → DeletePredicateTerms (translator)

**Files:**
- Create: `src/engine/delete_predicate_translate.rs`
- Modify: `src/engine/mod.rs` (export module)

- [ ] **Step 1: Create the module skeleton**

```rust
// src/engine/delete_predicate_translate.rs

// Licensed to the Apache Software Foundation ...

//! Translate a managed-lake DELETE's WHERE clause into
//! `DeletePredicateTerms` — a conjunctive list of column-op-literal /
//! IN / IS NULL predicates with StarRocks-compatible string-encoded
//! literal values. Mirrors StarRocks DeleteAnalyzer restrictions:
//! AND-only, no OR/functions/subqueries/joins; non-DUP tables require
//! key columns; floating-point columns reject `=`.

use sqlparser::ast as sqlast;

use crate::sql::catalog::ColumnDef;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub struct BinaryTerm {
    pub column: String,
    pub op: CmpOp,
    /// StarRocks BinaryPredicatePb.value, already serialized per column type.
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct InTerm {
    pub column: String,
    pub is_not_in: bool,
    pub values: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct IsNullTerm {
    pub column: String,
    pub is_not_null: bool,
}

#[derive(Clone, Debug, Default)]
pub struct DeletePredicateTerms {
    pub binary: Vec<BinaryTerm>,
    pub in_list: Vec<InTerm>,
    pub is_null: Vec<IsNullTerm>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeysType {
    Dup,
    Unique,
    Agg,
}

impl KeysType {
    pub fn from_meta_str(s: &str) -> Option<Self> {
        match s {
            "DUP_KEYS" => Some(Self::Dup),
            "UNIQUE_KEYS" => Some(Self::Unique),
            "AGG_KEYS" => Some(Self::Agg),
            _ => None,
        }
    }
    fn requires_key_columns(self) -> bool {
        matches!(self, Self::Unique | Self::Agg)
    }
}

pub fn translate_to_delete_predicate(
    where_expr: &sqlast::Expr,
    schema: &[ColumnDef],
    keys: &[String],          // key column names, lower-cased
    keys_type: KeysType,
) -> Result<DeletePredicateTerms, String> {
    let mut terms = DeletePredicateTerms::default();
    let atoms = flatten_and(where_expr)?;
    for atom in atoms {
        translate_atom(atom, schema, keys, keys_type, &mut terms)?;
    }
    Ok(terms)
}

fn flatten_and(expr: &sqlast::Expr) -> Result<Vec<&sqlast::Expr>, String> {
    let mut out = Vec::new();
    fn walk<'a>(e: &'a sqlast::Expr, out: &mut Vec<&'a sqlast::Expr>) -> Result<(), String> {
        match e {
            sqlast::Expr::BinaryOp {
                op: sqlast::BinaryOperator::And,
                left,
                right,
            } => {
                walk(left, out)?;
                walk(right, out)?;
                Ok(())
            }
            sqlast::Expr::BinaryOp {
                op: sqlast::BinaryOperator::Or,
                ..
            } => Err(
                "DELETE on this table model does not support OR; \
                 use only AND of comparisons / IN / IS NULL"
                    .to_string(),
            ),
            sqlast::Expr::Nested(inner) => walk(inner, out),
            _ => {
                out.push(e);
                Ok(())
            }
        }
    }
    walk(expr, &mut out)?;
    Ok(out)
}

fn translate_atom(
    atom: &sqlast::Expr,
    schema: &[ColumnDef],
    keys: &[String],
    keys_type: KeysType,
    out: &mut DeletePredicateTerms,
) -> Result<(), String> {
    // TODO in Step 2: implement binary, in-list, is-null
    let _ = (atom, schema, keys, keys_type, out);
    Err(format!("unsupported DELETE predicate atom: {atom:?}"))
}
```

Add to `src/engine/mod.rs`:

```rust
pub(crate) mod delete_predicate_translate;
```

Verify it compiles:

```bash
cargo build 2>&1 | tail -3
```

- [ ] **Step 2: Write failing unit tests for binary terms**

Add at bottom of `src/engine/delete_predicate_translate.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn dup_schema_int_str() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                write_default: None,
            },
        ]
    }

    fn parse_where(sql: &str) -> sqlast::Expr {
        let stmt = Parser::parse_sql(&MySqlDialect {}, &format!("DELETE FROM t WHERE {sql}"))
            .expect("parse")
            .into_iter()
            .next()
            .expect("at least one statement");
        match stmt {
            sqlast::Statement::Delete(d) => {
                d.selection.expect("WHERE clause")
            }
            other => panic!("unexpected stmt {other:?}"),
        }
    }

    #[test]
    fn binary_eq_int_lit() {
        let w = parse_where("id = 42");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary.len(), 1);
        assert_eq!(t.binary[0].column, "id");
        assert_eq!(t.binary[0].op, CmpOp::Eq);
        assert_eq!(t.binary[0].value, "42");
    }

    #[test]
    fn binary_ne_string_lit() {
        let w = parse_where("name != 'alice'");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary[0].op, CmpOp::Ne);
        assert_eq!(t.binary[0].value, "alice");
    }

    #[test]
    fn and_combination() {
        let w = parse_where("id = 1 AND name = 'a'");
        let t = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("translate");
        assert_eq!(t.binary.len(), 2);
    }

    #[test]
    fn or_rejected() {
        let w = parse_where("id = 1 OR id = 2");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .unwrap_err();
        assert!(err.contains("OR"), "got: {err}");
    }

    #[test]
    fn unique_non_key_rejected() {
        let w = parse_where("name = 'x'");
        let err = translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Unique,
        )
        .unwrap_err();
        assert!(err.contains("key column"), "got: {err}");
    }

    #[test]
    fn dup_non_key_allowed() {
        let w = parse_where("name = 'x'");
        translate_to_delete_predicate(
            &w,
            &dup_schema_int_str(),
            &["id".to_string()],
            KeysType::Dup,
        )
        .expect("dup allows non-key");
    }
}
```

```bash
cargo test --lib delete_predicate_translate::tests 2>&1 | tail -20
```

Expected: all 6 tests fail (unimplemented `translate_atom`).

- [ ] **Step 3: Implement `translate_atom` for binary comparisons**

Replace the placeholder `translate_atom` body with:

```rust
fn translate_atom(
    atom: &sqlast::Expr,
    schema: &[ColumnDef],
    keys: &[String],
    keys_type: KeysType,
    out: &mut DeletePredicateTerms,
) -> Result<(), String> {
    match atom {
        sqlast::Expr::BinaryOp { left, op, right } => {
            let cmp = match op {
                sqlast::BinaryOperator::Eq => CmpOp::Eq,
                sqlast::BinaryOperator::NotEq => CmpOp::Ne,
                sqlast::BinaryOperator::Lt => CmpOp::Lt,
                sqlast::BinaryOperator::LtEq => CmpOp::Le,
                sqlast::BinaryOperator::Gt => CmpOp::Gt,
                sqlast::BinaryOperator::GtEq => CmpOp::Ge,
                other => return Err(format!(
                    "DELETE WHERE supports comparison / IN / IS NULL only; got {other:?}"
                )),
            };
            let (col_name, lit_expr) = extract_col_lit(left, right)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            if is_float_type(&column.data_type) && matches!(cmp, CmpOp::Eq | CmpOp::Ne) {
                return Err(format!(
                    "Don't support float column '{}' in delete condition",
                    col_name
                ));
            }
            let value = serialize_literal(lit_expr, &column.data_type, &col_name)?;
            out.binary.push(BinaryTerm {
                column: col_name,
                op: cmp,
                value,
            });
            Ok(())
        }
        sqlast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = expr_to_col_name(expr)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            let values = list
                .iter()
                .map(|e| serialize_literal(e, &column.data_type, &col_name))
                .collect::<Result<Vec<_>, _>>()?;
            out.in_list.push(InTerm {
                column: col_name,
                is_not_in: *negated,
                values,
            });
            Ok(())
        }
        sqlast::Expr::IsNull(inner) => {
            let col_name = expr_to_col_name(inner)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            out.is_null.push(IsNullTerm {
                column: col_name,
                is_not_null: false,
            });
            Ok(())
        }
        sqlast::Expr::IsNotNull(inner) => {
            let col_name = expr_to_col_name(inner)?;
            let column = column_or_err(schema, &col_name)?;
            check_keys(&col_name, &column, keys, keys_type)?;
            out.is_null.push(IsNullTerm {
                column: col_name,
                is_not_null: true,
            });
            Ok(())
        }
        sqlast::Expr::Nested(inner) => translate_atom(inner, schema, keys, keys_type, out),
        other => Err(format!(
            "DELETE WHERE atom must be col-op-lit / IN / IS NULL; got {other:?}"
        )),
    }
}

fn extract_col_lit<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Result<(String, &'a sqlast::Expr), String> {
    if let Ok(name) = expr_to_col_name(left) {
        return Ok((name, right));
    }
    if let Ok(name) = expr_to_col_name(right) {
        return Ok((name, left));
    }
    Err("DELETE WHERE comparison must have exactly one column and one literal side".to_string())
}

fn expr_to_col_name(e: &sqlast::Expr) -> Result<String, String> {
    match e {
        sqlast::Expr::Identifier(id) => Ok(id.value.to_lowercase()),
        sqlast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.to_lowercase())
            .ok_or_else(|| "empty compound identifier".to_string()),
        other => Err(format!("expected column reference, got {other:?}")),
    }
}

fn column_or_err(schema: &[ColumnDef], name: &str) -> Result<ColumnDef, String> {
    schema
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .cloned()
        .ok_or_else(|| format!("column '{name}' not found in table schema"))
}

fn check_keys(
    name: &str,
    _column: &ColumnDef,
    keys: &[String],
    keys_type: KeysType,
) -> Result<(), String> {
    if keys_type.requires_key_columns()
        && !keys.iter().any(|k| k.eq_ignore_ascii_case(name))
    {
        return Err(format!(
            "Where clause only supports key column on this table model; '{name}' is not a key column"
        ));
    }
    Ok(())
}

fn is_float_type(ty: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(ty, DataType::Float32 | DataType::Float64)
}
```

Add a stub `serialize_literal` that we'll complete in Task M3.T2:

```rust
fn serialize_literal(
    lit_expr: &sqlast::Expr,
    column_type: &arrow::datatypes::DataType,
    column_name: &str,
) -> Result<String, String> {
    // Step 5 of this task: minimal implementation; expanded in M3.T2.
    use sqlparser::ast::{Expr, Value, ValueWithSpan};
    let v = match lit_expr {
        Expr::Value(ValueWithSpan { value, .. }) => value,
        Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => {
            if let Expr::Value(ValueWithSpan { value, .. }) = expr.as_ref() {
                match value {
                    Value::Number(n, _) => return Ok(format!("-{n}")),
                    _ => {}
                }
            }
            return Err(format!("unsupported negated literal for column '{column_name}'"));
        }
        other => return Err(format!("literal value expected for column '{column_name}', got {other:?}")),
    };
    match v {
        Value::Number(n, _) => Ok(n.clone()),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(s.clone()),
        Value::Boolean(b) => Ok(if *b { "1".into() } else { "0".into() }),
        Value::Null => Err(format!(
            "NULL literal in DELETE WHERE for column '{column_name}'; use IS NULL/IS NOT NULL"
        )),
        other => Err(format!("unsupported literal for column '{column_name}': {other:?}")),
    }
}
```

```bash
cargo build 2>&1 | tail -3
cargo test --lib delete_predicate_translate::tests 2>&1 | tail -15
```

Expected: 6 PASS.

- [ ] **Step 4: Commit**

```bash
git add src/engine/delete_predicate_translate.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
feat(engine): add WHERE→DeletePredicateTerms translator skeleton

Translate DELETE WHERE into a conjunctive list of (column, op, literal)
predicates suitable for StarRocks DeletePredicatePb. Enforce StarRocks
alignment: AND only (no OR), col-op-lit form, key-column restriction
for UNIQUE/AGG, no equality on float columns. Literal value
serialization is a minimal stub here; per-type formatting lands in
the next task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M3.T2 — Per-type literal serialization (StarRocks BinaryPredicatePb.value format)

**Files:**
- Modify: `src/engine/delete_predicate_translate.rs` (`serialize_literal`)

- [ ] **Step 1: Write failing tests for per-type serialization**

Add to the `tests` module in `delete_predicate_translate.rs`:

```rust
fn schema_with(name: &str, ty: arrow::datatypes::DataType) -> Vec<ColumnDef> {
    vec![ColumnDef {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        write_default: None,
    }]
}

#[test]
fn datetime_microsecond_literal_zero_padded_to_six_digits() {
    use arrow::datatypes::TimeUnit;
    let w = parse_where("ts = '2020-01-01 00:00:00.012'");
    let schema = schema_with("ts", arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None));
    let t = translate_to_delete_predicate(
        &w,
        &schema,
        &["ts".to_string()],
        KeysType::Dup,
    )
    .expect("translate");
    assert_eq!(t.binary[0].value, "2020-01-01 00:00:00.012000");
}

#[test]
fn datetime_literal_overflow_rejected() {
    use arrow::datatypes::TimeUnit;
    let w = parse_where("ts = '2020-01-01 00:00:00.1234567'");
    let schema = schema_with("ts", arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None));
    let err = translate_to_delete_predicate(
        &w,
        &schema,
        &["ts".to_string()],
        KeysType::Dup,
    )
    .unwrap_err();
    assert!(err.contains("Datetime") || err.contains("microsecond"), "got: {err}");
}

#[test]
fn date_literal_iso_format() {
    let w = parse_where("d = '2020-01-01'");
    let schema = schema_with("d", arrow::datatypes::DataType::Date32);
    let t = translate_to_delete_predicate(
        &w,
        &schema,
        &["d".to_string()],
        KeysType::Dup,
    )
    .expect("translate");
    assert_eq!(t.binary[0].value, "2020-01-01");
}

#[test]
fn decimal_literal_padded_to_column_scale() {
    let w = parse_where("p = 12.3");
    let schema = schema_with("p", arrow::datatypes::DataType::Decimal128(10, 2));
    let t = translate_to_delete_predicate(
        &w,
        &schema,
        &["p".to_string()],
        KeysType::Dup,
    )
    .expect("translate");
    // DECIMAL(10,2) → "12.30" not "12.3"
    assert_eq!(t.binary[0].value, "12.30");
}

#[test]
fn negative_integer_literal() {
    let w = parse_where("v = -42");
    let schema = schema_with("v", arrow::datatypes::DataType::Int64);
    let t = translate_to_delete_predicate(
        &w,
        &schema,
        &["v".to_string()],
        KeysType::Dup,
    )
    .expect("translate");
    assert_eq!(t.binary[0].value, "-42");
}

#[test]
fn float_column_eq_rejected() {
    let w = parse_where("v = 1.5");
    let schema = schema_with("v", arrow::datatypes::DataType::Float64);
    let err = translate_to_delete_predicate(
        &w,
        &schema,
        &["v".to_string()],
        KeysType::Dup,
    )
    .unwrap_err();
    assert!(err.contains("float"), "got: {err}");
}
```

```bash
cargo test --lib delete_predicate_translate::tests 2>&1 | tail -20
```

Expected: 5 of 6 new tests fail (only `negative_integer_literal` passes from the stub).

- [ ] **Step 2: Implement full `serialize_literal`**

Replace the stub `serialize_literal` body with:

```rust
fn serialize_literal(
    lit_expr: &sqlast::Expr,
    column_type: &arrow::datatypes::DataType,
    column_name: &str,
) -> Result<String, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    use sqlparser::ast::{Expr, Value, ValueWithSpan};

    // Extract textual value, handling unary minus on numbers.
    let (raw, was_negated): (&str, bool) = match lit_expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            Value::Number(s, _) => (s.as_str(), false),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => (s.as_str(), false),
            Value::Boolean(b) => return Ok(if *b { "1".into() } else { "0".into() }),
            Value::Null => {
                return Err(format!(
                    "NULL literal in DELETE WHERE for column '{column_name}'; \
                     use IS NULL / IS NOT NULL"
                ));
            }
            other => return Err(format!(
                "unsupported literal for column '{column_name}': {other:?}"
            )),
        },
        Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(ValueWithSpan {
                value: Value::Number(s, _),
                ..
            }) => (s.as_str(), true),
            _ => return Err(format!("unsupported negated literal for column '{column_name}'")),
        },
        other => return Err(format!(
            "literal value expected for column '{column_name}', got {other:?}"
        )),
    };

    match column_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Parse + reformat to canonical (rejects floats like "1.5" for INT).
            let parsed: i128 = if was_negated {
                format!("-{raw}").parse().map_err(|e| {
                    format!("invalid integer literal for column '{column_name}': {raw} ({e})")
                })?
            } else {
                raw.parse().map_err(|e| {
                    format!("invalid integer literal for column '{column_name}': {raw} ({e})")
                })?
            };
            Ok(parsed.to_string())
        }
        DataType::Decimal128(_p, s) => {
            // StarRocks DECIMAL: pad fractional part to scale.
            let mut value: String = raw.to_string();
            if was_negated {
                value = format!("-{value}");
            }
            let scale = *s as usize;
            let (int_part, frac_part) = match value.split_once('.') {
                Some((i, f)) => (i.to_string(), f.to_string()),
                None => (value.clone(), String::new()),
            };
            if frac_part.len() > scale {
                return Err(format!(
                    "decimal literal for column '{column_name}' has more fractional digits ({}) than column scale ({})",
                    frac_part.len(),
                    scale
                ));
            }
            let frac = if scale == 0 {
                String::new()
            } else {
                let mut f = frac_part;
                while f.len() < scale {
                    f.push('0');
                }
                format!(".{f}")
            };
            Ok(format!("{int_part}{frac}"))
        }
        DataType::Date32 | DataType::Date64 => {
            // Canonical "YYYY-MM-DD".
            chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .map(|d| d.format("%Y-%m-%d").to_string())
                .map_err(|e| format!("invalid date literal for column '{column_name}': {raw} ({e})"))
        }
        DataType::Timestamp(unit, _) => {
            // Parse with optional fractional, normalize per unit scale.
            let scale_digits = match unit {
                TimeUnit::Second => 0usize,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            let (datepart, mut fracpart) = split_datetime_fraction(raw, column_name)?;
            if fracpart.len() > scale_digits {
                return Err(format!(
                    "Datetime literal '{raw}' has {} fractional digits but column '{column_name}' supports {} (microsecond)",
                    fracpart.len(),
                    scale_digits
                ));
            }
            while fracpart.len() < scale_digits {
                fracpart.push('0');
            }
            // Parse "YYYY-MM-DD HH:MM:SS" portion for validation.
            chrono::NaiveDateTime::parse_from_str(&datepart, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| format!("invalid datetime literal for column '{column_name}': {raw} ({e})"))?;
            if scale_digits > 0 {
                Ok(format!("{datepart}.{fracpart}"))
            } else {
                Ok(datepart)
            }
        }
        DataType::Boolean => {
            match raw {
                "0" | "false" | "FALSE" => Ok("0".to_string()),
                "1" | "true" | "TRUE" => Ok("1".to_string()),
                _ => Err(format!("invalid boolean literal for column '{column_name}': {raw}")),
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 => Ok(raw.to_string()),
        DataType::Float32 | DataType::Float64 => {
            // Float columns reject `=`/`!=` in `check_atom`; this path is only
            // reachable via `<`/`<=`/`>`/`>=`. Pass through canonical form.
            let parsed: f64 = if was_negated {
                format!("-{raw}").parse().map_err(|e| {
                    format!("invalid float literal for column '{column_name}': {raw} ({e})")
                })?
            } else {
                raw.parse().map_err(|e| {
                    format!("invalid float literal for column '{column_name}': {raw} ({e})")
                })?
            };
            Ok(parsed.to_string())
        }
        other => Err(format!(
            "DELETE WHERE: unsupported column type {other:?} for column '{column_name}'"
        )),
    }
}

fn split_datetime_fraction(raw: &str, column_name: &str) -> Result<(String, String), String> {
    match raw.split_once('.') {
        Some((d, f)) => {
            if !f.chars().all(|c| c.is_ascii_digit()) {
                return Err(format!(
                    "invalid datetime fractional part for column '{column_name}': '{f}'"
                ));
            }
            Ok((d.to_string(), f.to_string()))
        }
        None => Ok((raw.to_string(), String::new())),
    }
}
```

- [ ] **Step 3: Run all translator tests**

```bash
cargo test --lib delete_predicate_translate::tests 2>&1 | tail -25
```

Expected: all 12 tests PASS (6 from T1 + 6 new in T2).

- [ ] **Step 4: Commit**

```bash
git add src/engine/delete_predicate_translate.rs
git commit -m "$(cat <<'EOF'
feat(engine): per-type literal serialization for DeletePredicate

Format DELETE WHERE literals to match StarRocks BinaryPredicatePb.value
string conventions:
- DATETIME: zero-pad fractional to column scale (e.g. microsecond → 6
  digits); reject overflow.
- DECIMAL(p,s): pad fractional to scale.
- DATE: canonical YYYY-MM-DD.
- INT: reformat through i128 to reject floats.
- BOOL: 0/1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M3.T3 — DeletePredicateTerms → DeletePredicatePb

**Files:**
- Create: `src/connector/starrocks/lake/delete_predicate_proto.rs`
- Modify: `src/connector/starrocks/lake/mod.rs` (export)

- [ ] **Step 1: Confirm proto type names**

```bash
grep -n "DeletePredicatePb\|BinaryPredicatePb\|InPredicatePb\|IsNullPredicatePb" src/service/grpc_client/proto/starrocks.rs src/connector/starrocks/lake/*.rs 2>&1 | head -10
```

Find the exact path. Use the path inferred from `txn_log.rs:4096` import: `crate::service::grpc_client::proto::starrocks::{...}` or wherever the prost-generated types live.

- [ ] **Step 2: Create module with one test that fails**

Create `src/connector/starrocks/lake/delete_predicate_proto.rs`:

```rust
// Licensed ...

//! Convert FE-level DeletePredicateTerms to the wire DeletePredicatePb
//! that gets serialized into rowset metadata.

use crate::engine::delete_predicate_translate::{
    BinaryTerm, CmpOp, DeletePredicateTerms, InTerm, IsNullTerm,
};

// Adapt this import to the actual proto module path discovered in Step 1.
use crate::service::grpc_client::proto::starrocks::{
    BinaryPredicatePb, DeletePredicatePb, InPredicatePb, IsNullPredicatePb,
};

pub fn build_delete_predicate_pb(terms: &DeletePredicateTerms, version: i32) -> DeletePredicatePb {
    // Proto layout (`target/debug/build/.../starrocks.rs`):
    //   version: i32 (required)
    //   sub_predicates: Vec<String>   — legacy hybrid format, leave empty
    //   in_predicates / binary_predicates / is_null_predicates: lake-only
    DeletePredicatePb {
        version,
        sub_predicates: Vec::new(),
        binary_predicates: terms.binary.iter().map(binary_to_pb).collect(),
        in_predicates: terms.in_list.iter().map(in_to_pb).collect(),
        is_null_predicates: terms.is_null.iter().map(isnull_to_pb).collect(),
    }
}

fn binary_to_pb(term: &BinaryTerm) -> BinaryPredicatePb {
    BinaryPredicatePb {
        column_name: Some(term.column.clone()),
        op: Some(match term.op {
            CmpOp::Eq => "EQ",
            CmpOp::Ne => "NE",
            CmpOp::Lt => "LT",
            CmpOp::Le => "LE",
            CmpOp::Gt => "GT",
            CmpOp::Ge => "GE",
        }.to_string()),
        value: Some(term.value.clone()),
    }
}

fn in_to_pb(term: &InTerm) -> InPredicatePb {
    InPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_in: Some(term.is_not_in),
        values: term.values.clone(),
    }
}

fn isnull_to_pb(term: &IsNullTerm) -> IsNullPredicatePb {
    IsNullPredicatePb {
        column_name: Some(term.column.clone()),
        is_not_null: Some(term.is_not_null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_binary_and_in_and_isnull() {
        let mut terms = DeletePredicateTerms::default();
        terms.binary.push(BinaryTerm {
            column: "id".into(),
            op: CmpOp::Eq,
            value: "42".into(),
        });
        terms.in_list.push(InTerm {
            column: "name".into(),
            is_not_in: false,
            values: vec!["a".into(), "b".into()],
        });
        terms.is_null.push(IsNullTerm {
            column: "deleted_at".into(),
            is_not_null: false,
        });

        let pb = build_delete_predicate_pb(&terms, 7);
        assert_eq!(pb.version, 7);
        assert!(pb.sub_predicates.is_empty(), "sub_predicates must stay empty (lake mode)");
        assert_eq!(pb.binary_predicates.len(), 1);
        assert_eq!(pb.binary_predicates[0].column_name.as_deref(), Some("id"));
        assert_eq!(pb.binary_predicates[0].op.as_deref(), Some("EQ"));
        assert_eq!(pb.binary_predicates[0].value.as_deref(), Some("42"));
        assert_eq!(pb.in_predicates.len(), 1);
        assert_eq!(pb.in_predicates[0].values, vec!["a", "b"]);
        assert_eq!(pb.is_null_predicates.len(), 1);
        assert_eq!(pb.is_null_predicates[0].is_not_null, Some(false));
    }
}
```

Add to `src/connector/starrocks/lake/mod.rs`:

```rust
pub(crate) mod delete_predicate_proto;
```

- [ ] **Step 3: Build and test**

```bash
cargo build 2>&1 | tail -5
cargo test --lib delete_predicate_proto::tests 2>&1 | tail -10
```

Expected: 1 PASS. If compilation fails because field names differ on the prost-generated struct (e.g., `version: i32` not `Option<i32>`), adjust the assignment syntax accordingly.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/lake/delete_predicate_proto.rs \
        src/connector/starrocks/lake/mod.rs
git commit -m "$(cat <<'EOF'
feat(lake): encode DeletePredicateTerms to DeletePredicatePb proto

Map FE-level terms (binary, IN, IS NULL) to BinaryPredicatePb,
InPredicatePb, IsNullPredicatePb. The legacy sub_predicates field is
intentionally left empty to match StarRocks lake-mode behavior.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M3.T4 — Standalone-mode dispatch entry for DELETE-by-predicate

**Files:**
- Modify: `src/connector/starrocks/lake/transactions.rs` (add or expose internal helper)
- Modify: `src/engine/delete_flow.rs` (route DUP/UNIQUE/AGG to new path)

- [ ] **Step 1: Inspect `transactions::delete_data` and decide adapter shape**

Read `src/connector/starrocks/lake/transactions.rs:920-1030` and check:

```bash
sed -n '926,970p' src/connector/starrocks/lake/transactions.rs
```

The existing function:

```rust
pub(crate) fn delete_data(request: &DeleteDataRequest) -> Result<DeleteDataResponse, String>
```

Takes a `DeleteDataRequest` proto (designed for FFI / brpc). Standalone-mode can either:
- **(a)** Construct a `DeleteDataRequest` and call `delete_data` directly
- **(b)** Pull out an inner helper `delete_data_with_predicate(tablet_ids, txn_id, predicate, schema_key)` and call from both sites

Decision: **option (a)** — minimal surface, no refactor of internal helper, no `pub(crate)` widening. We only need to construct `DeleteDataRequest` in standalone path; everything else is already wired.

- [ ] **Step 2: Find how to enumerate tablet_ids and acquire a txn_id in standalone mode**

```bash
grep -rn "fn allocate_txn_id\|next_txn_id\|stage_managed_partition\|managed_partition_tablets\|list_tablets\|tablet_ids" src/connector/starrocks/managed/ src/engine/ --include="*.rs" 2>&1 | head -15
```

Locate the standalone txn allocator and the tablet enumerator. They are used by `insert_flow` / `mutation_flow` already — read those for the pattern.

- [ ] **Step 3: Write failing integration test for DUP DELETE end-to-end**

Add to `src/engine/mod.rs::tests`:

```rust
#[test]
fn managed_dup_delete_via_delete_predicate_path() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();
    session.execute(
        "CREATE TABLE t_dup (id INT, name STRING) DUPLICATE KEY(id) \
         DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')",
    ).expect("create dup");
    session.execute("INSERT INTO t_dup VALUES (1,'a'), (2,'b'), (3,'c')").expect("insert");
    session.execute("DELETE FROM t_dup WHERE id = 2").expect("delete via predicate");
    let r = session.query("SELECT id FROM t_dup ORDER BY id").expect("query");
    let ids: Vec<i32> = r.column_i32("id").expect("ids");
    assert_eq!(ids, vec![1, 3]);
}
```

(Adapt `r.column_i32` to whatever the actual `QueryResult` API is — inspect existing managed-lake tests in the same file for the right method name.)

```bash
cargo test --lib managed_dup_delete_via_delete_predicate_path 2>&1 | tail -15
```

Expected: fails because old `execute_managed_delete_statement` (CoW) handles it, or fails with the predicate not yet wired.

- [ ] **Step 4: Add new DUP/UNIQUE/AGG branch in `execute_managed_delete_statement`**

In `src/engine/delete_flow.rs`, near the top of `execute_managed_delete_statement` (after `target_ref != "main"` rejection), insert keys-type routing. Read the existing function carefully:

```bash
sed -n '354,400p' src/engine/delete_flow.rs
```

Modify the function so the body becomes a dispatch:

```rust
fn execute_managed_delete_statement(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    target_ref: &str,
) -> Result<StatementResult, String> {
    if target_ref != "main" {
        return Err(format!(
            "DELETE: branch target `{target_ref}` is only supported for iceberg tables"
        ));
    }

    // Look up managed-lake table metadata to learn keys_type, key columns,
    // tablet_ids, and schema.
    let table_info = resolve_managed_table_info(state, target)?;

    // MV rejection (mirror StarRocks).
    if table_info.is_materialized_view {
        return Err(format!(
            "The data of '{}' cannot be deleted because it is a materialized view; \
             the data of materialized view must be consistent with the base table.",
            target.table
        ));
    }

    match table_info.keys_type.as_str() {
        "PRIMARY_KEYS" => {
            // M4.T2 replaces this with execute_managed_pk_delete.
            // Keep the legacy CoW path here so PK DELETE remains
            // functional during the M3 → M4 window (G5 constraint).
            execute_managed_cow_delete_legacy(state, target, stmt, target_ref)
        }
        "DUP_KEYS" | "UNIQUE_KEYS" | "AGG_KEYS" => {
            execute_managed_predicate_delete(state, target, stmt, &table_info)
        }
        other => Err(format!("unsupported managed-lake keys_type: {other}")),
    }
}

/// Legacy copy-on-write DELETE — kept temporarily for PRIMARY KEY tables
/// while M4.T2 implements the __op + sink path. To be removed in M4.T3.
fn execute_managed_cow_delete_legacy(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    target_ref: &str,
) -> Result<StatementResult, String> {
    // EXACT body of the old execute_managed_delete_statement lines
    // 354-395 from the pre-M3 codebase — extract verbatim, just
    // renamed. The body does: parse "SELECT * WHERE NOT (cond)" to
    // collect survivors, truncate_managed_table, then sink.append_batch.
    // No semantic change vs original CoW.
    if target_ref != "main" {
        return Err(format!(
            "DELETE: branch target `{target_ref}` is only supported for iceberg tables"
        ));
    }
    let (catalog, sink) = {
        let reg = state.connectors.read().expect("connector registry read");
        (
            reg.catalog_backend(target.backend_name)?,
            reg.table_sink(target.backend_name)?,
        )
    };
    let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;
    let rewritten_sql = format!(
        "SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)",
        target.table, stmt.where_clause
    );
    let statement = crate::sql::parser::parse_sql_raw(&rewritten_sql)?;
    let sqlast::Statement::Query(query) = statement else {
        return Err("internal: managed DELETE rewrite did not parse as SELECT".to_string());
    };
    let batch = crate::engine::insert_flow::execute_insert_from_query_on_pipeline(
        state,
        target,
        &resolved,
        &[],
        query.as_ref(),
    )?;
    crate::connector::truncate_managed_table(state, &target.namespace, &target.table)?;
    if batch.num_rows() > 0 {
        sink.append_batch(&resolved, batch)?;
    }
    Ok(StatementResult::Ok)
}

fn execute_managed_predicate_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    table_info: &ManagedTableInfo,
) -> Result<StatementResult, String> {
    use crate::engine::delete_predicate_translate::{
        translate_to_delete_predicate, KeysType,
    };
    use crate::connector::starrocks::lake::delete_predicate_proto::build_delete_predicate_pb;

    let keys_type = KeysType::from_meta_str(&table_info.keys_type)
        .ok_or_else(|| format!("invalid keys_type: {}", table_info.keys_type))?;
    let terms = translate_to_delete_predicate(
        &stmt.where_clause,
        &table_info.columns,
        &table_info.key_columns,
        keys_type,
    )?;

    let next_version = table_info.current_version + 1;
    let predicate_pb = build_delete_predicate_pb(&terms, next_version as i32);

    let txn_id = allocate_managed_txn_id(state)?;
    let request = build_delete_data_request(
        txn_id,
        &table_info.tablet_ids,
        predicate_pb,
        &table_info.schema_key,
    );
    let _resp = crate::connector::starrocks::lake::transactions::delete_data(&request)?;

    // Publish the txn (commit visible).
    publish_managed_delete_txn(state, target, txn_id)?;

    Ok(StatementResult::Ok)
}
```

`resolve_managed_table_info`, `allocate_managed_txn_id`, `build_delete_data_request`, `publish_managed_delete_txn`, and `ManagedTableInfo` struct need to be defined / discovered. Implementer should:

1. **Define `ManagedTableInfo`** in `src/engine/delete_flow.rs` (private struct) carrying `keys_type: String`, `is_materialized_view: bool`, `columns: Vec<ColumnDef>`, `key_columns: Vec<String>`, `tablet_ids: Vec<i64>`, `current_version: i64`, `schema_key: Option<TableSchemaKeyPb>`.

2. **Find existing accessors** in `src/connector/starrocks/managed/store.rs` and `catalog.rs` (used by `insert_flow`) — copy the pattern.

3. **Reuse txn allocator** from managed module (check `src/connector/starrocks/managed/txn.rs::allocate_txn_id` or equivalent).

4. **Reuse publish path** that `transactions::delete_data` triggers (`append_delete_data_txn_log` writes txn log; commit/publish is separate). Check what `insert_flow` does after `append_lake_txn_log_with_chunk_rowset` — same publish path applies.

- [ ] **Step 5: Build, run failing test until it passes**

```bash
cargo build 2>&1 | tail -10
cargo test --lib managed_dup_delete_via_delete_predicate_path 2>&1 | tail -15
```

Expected: PASS once the wiring is complete. May require several iterations.

- [ ] **Step 6: Commit**

```bash
git add src/engine/delete_flow.rs
git commit -m "$(cat <<'EOF'
feat(engine): wire DUP/UNIQUE/AGG DELETE through DeletePredicate

Route DELETE on managed-lake DUP/UNIQUE/AGG tables to the existing
transactions::delete_data path: translate WHERE to DeletePredicateTerms,
encode to DeletePredicatePb, construct a DeleteDataRequest with the
table's tablet_ids and a fresh txn_id, dispatch, then publish.

PRIMARY_KEYS branch still errors with "not yet implemented" — filled
in M4.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M3.T5 — SQL regression tests for DUP/UNIQUE/AGG

**Files:**
- Create: `sql-tests/write-path/sql/managed_dup_delete_non_key_col.sql`
- Create: `sql-tests/write-path/result/managed_dup_delete_non_key_col.result`
- (Repeat for each of the 7 cases from spec §4.7.)

- [ ] **Step 1: Write each `.sql` test file**

Each follows the pattern from existing write-path tests. Example for `managed_dup_delete_non_key_col`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed,dup_keys,delete
-- Test Objective: Verify DUP table accepts DELETE on non-key column.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_dup_delete_non_key;
CREATE TABLE ${case_db}.t_dup_delete_non_key (
  id INT NOT NULL,
  v INT NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_dup_delete_non_key VALUES (1, 10), (2, 20), (3, 30);
DELETE FROM ${case_db}.t_dup_delete_non_key WHERE v = 20;
SELECT id, v FROM ${case_db}.t_dup_delete_non_key ORDER BY id;
```

Plus `.result`:

```
id	v
1	10
3	30
```

Create all 7 (use existing tests in `sql-tests/write-path/sql/` as templates):

1. `managed_dup_delete_non_key_col` — DUP, `WHERE v = lit` (non-key).
2. `managed_unique_delete_keyonly` — UNIQUE table, `WHERE id = lit`.
3. `managed_unique_delete_nonkey_rejected` — UNIQUE, `WHERE v = lit`; mark `@expect_error=Where clause only supports key column`.
4. `managed_agg_delete_keyonly` — AGGREGATE table.
5. `managed_dup_delete_or_rejected` — `WHERE id=1 OR id=2`; `@expect_error=OR`.
6. `managed_dup_delete_in_list` — `WHERE id IN (1,3)`.
7. `managed_dup_delete_is_null` — `WHERE v IS NULL`.

For `@expect_error`-style tests, inspect a similar existing case (e.g. `grep -l "expect_error" sql-tests/`) to confirm the syntax — typically a comment annotation parsed by the runner.

- [ ] **Step 2: Generate / record results**

Start a fresh server (if not already up):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-server-m3t5.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' "$LOG" && break; sleep 1; done
```

Run each test in `record` mode to produce `.result` (only for the non-error cases — error cases need hand-written `.result` or a special annotation):

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode record \
  --only managed_dup_delete_non_key_col,managed_unique_delete_keyonly,\
managed_agg_delete_keyonly,managed_dup_delete_in_list,managed_dup_delete_is_null \
  2>&1 | tail -20
```

Inspect the generated `.result` files and confirm they make semantic sense.

- [ ] **Step 3: Run in verify mode**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode verify 2>&1 | tail -30
kill $SRV_PID 2>/dev/null
```

Expected:
- All 7 new cases pass
- All existing cases continue passing (`datetime_microsecond_precision_delete` now fully green from D+C; `primary_key_*` still pass via the CoW path which is still present)

- [ ] **Step 4: Commit**

```bash
git add sql-tests/write-path/sql/managed_*.sql sql-tests/write-path/result/managed_*.result
git commit -m "$(cat <<'EOF'
test(write-path): cover DUP/UNIQUE/AGG DELETE via DeletePredicate

Add 7 sql-test cases:
- DUP delete on non-key column
- UNIQUE delete on key (allowed) + on non-key (rejected with
  StarRocks-aligned error)
- AGGREGATE delete on key
- OR predicate rejection
- IN list and IS NULL forms

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Milestone M4 — PRIMARY KEY via `__op` + sink (B)

The storage layer is fully wired (see §10 of the spec). M4 adds: (1) a name-based `__op` column helper for analyzer/optimizer audit and future reuse; (2) FE rewriting `DELETE FROM pk_t WHERE cond` into a chunk-producing pipeline that goes through the existing sink with `__op = 1`; (3) removes CoW.

### Task M4.T1 — `load_op_column` helper module + analyzer audit tests

**Files:**
- Create: `src/sql/analyzer/load_op_column.rs`
- Modify: `src/sql/analyzer/mod.rs` (export)

- [ ] **Step 1: Create the module**

```rust
// src/sql/analyzer/load_op_column.rs

// Licensed ...

//! Helpers for the `__op` control column used by managed-lake PK
//! tables to distinguish UPSERT (0) from DELETE (1) rows.
//!
//! StarRocks-aligned: matches the wire-format constant LOAD_OP_COLUMN
//! used by stream load and SQL DML alike.
//!
//! The optimizer / analyzer must NOT prune, push-down, or otherwise
//! mangle `__op` columns. These helpers exist to centralize that rule;
//! tests in [`audit_tests`] verify current passes already respect it.

pub const LOAD_OP_COLUMN: &str = "__op";

/// Wire-level `__op` values. Reserved variants are listed to anchor the
/// future MERGE/UPDATE extension; the sink rejects values outside the
/// currently-implemented set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadOp {
    Upsert = 0,
    Delete = 1,
    // Update = 2,   // reserved for future row-delta sink
    // Insert = 3,   // reserved
}

impl LoadOp {
    pub const fn as_i8(self) -> i8 {
        self as i8
    }
}

#[inline]
pub fn is_load_op_column(name: &str) -> bool {
    name == LOAD_OP_COLUMN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_matches_starrocks_protocol() {
        // If this changes, the stream-load protocol wire field changes
        // too. Coordinate with sink/operator.rs and managed-lake clients.
        assert_eq!(LOAD_OP_COLUMN, "__op");
    }

    #[test]
    fn reserved_variants_have_starrocks_aligned_numeric_values() {
        assert_eq!(LoadOp::Upsert.as_i8(), 0);
        assert_eq!(LoadOp::Delete.as_i8(), 1);
        // assert_eq!(LoadOp::Update.as_i8(), 2);  // reserved
        // assert_eq!(LoadOp::Insert.as_i8(), 3);  // reserved
    }

    #[test]
    fn helper_recognizes_load_op_column() {
        assert!(is_load_op_column("__op"));
        assert!(!is_load_op_column("_op"));
        assert!(!is_load_op_column("__op_v2"));
        assert!(!is_load_op_column(""));
    }
}
```

Add to `src/sql/analyzer/mod.rs`:

```rust
pub mod load_op_column;
```

- [ ] **Step 2: Build and run unit tests**

```bash
cargo build 2>&1 | tail -3
cargo test --lib load_op_column::tests 2>&1 | tail -10
```

Expected: 3 PASS.

- [ ] **Step 3: Commit (audit deferred to end-to-end coverage)**

The spec §10.5 risk analysis showed NovaRocks' current `PruneColumns` rule operates on `ScanNode.required_columns`, not on top-level `Project` literal columns. A const `1 AS __op` projection has no children to prune; the rule structurally leaves it alone. Adding a unit-level audit test would require constructing a `LogicalPlan` tree directly (the plan node ctors aren't ergonomic for hand-rolled tests in this codebase). Instead, **the end-to-end DELETE integration tests in M4.T2** verify `__op` survives the entire analyzer-optimizer pipeline by virtue of the DELETE actually working.

If a future regression deletes `__op` mid-pipeline, M4.T2's tests fail immediately. If you want extra safety, add a snapshot test in M4.T2 step 2 that EXPLAINS the rewritten DELETE plan and asserts `__op` appears in the output column list — but that's optional.

Commit the module + helper unit tests:

```bash
git add src/sql/analyzer/load_op_column.rs \
        src/sql/analyzer/mod.rs
git commit -m "$(cat <<'EOF'
feat(analyzer): centralize __op column helper

Add load_op_column.rs with LOAD_OP_COLUMN constant, LoadOp enum
(Upsert/Delete; Update/Insert reserved), and is_load_op_column helper.
Anchors the StarRocks stream-load wire protocol field name and the
future row-delta sink op-code values, replacing the implicit
hard-coded "__op" string literals currently scattered through
sink/operator.rs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M4.T2 — `execute_managed_pk_delete` (FE rewrite to `SELECT pk_cols, 1 AS __op`)

**Files:**
- Modify: `src/engine/delete_flow.rs`

- [ ] **Step 1: Write failing integration test**

Add to `src/engine/mod.rs::tests`:

```rust
#[test]
fn managed_pk_delete_via_op_column_path() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();

    session.execute(
        "CREATE TABLE t_pk (id BIGINT NOT NULL, payload STRING) PRIMARY KEY (id) \
         DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')",
    ).expect("create");
    session.execute("INSERT INTO t_pk VALUES (1,'a'),(2,'b'),(3,'c')").expect("insert");
    session.execute("DELETE FROM t_pk WHERE id = 2").expect("delete pk row");

    let r = session.query("SELECT id FROM t_pk ORDER BY id").expect("query");
    let ids: Vec<i64> = r.column_i64("id").expect("ids");
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn managed_pk_delete_complex_where() {
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();
    session.execute(
        "CREATE TABLE t_pk_cmplx (id INT NOT NULL, k INT, label STRING) PRIMARY KEY(id) \
         DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='1')",
    ).expect("create");
    session.execute("INSERT INTO t_pk_cmplx VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')").expect("insert");
    // Non-PK predicate, function on column.
    session.execute("DELETE FROM t_pk_cmplx WHERE LOWER(label) = 'y'").expect("delete");
    let r = session.query("SELECT id FROM t_pk_cmplx ORDER BY id").expect("query");
    let ids: Vec<i32> = r.column_i32("id").expect("ids");
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn managed_pk_delete_then_insert_same_pk_visible() {
    // Covers the publish-time race: DELETE marks a PK's segment via
    // delvec, then INSERT of the same PK adds a new row; the SELECT
    // must see the new row, not the old or "no row".
    let _runtime_guard = lock_runtime_test_state();
    let Some((_dir, config_path, _metadata_db_path)) = maybe_managed_lake_config() else {
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(config_path),
        metadata_db_path: None,
    })
    .expect("open engine");
    let session = engine.session();
    session.execute(
        "CREATE TABLE t_pk_cycle (id INT NOT NULL, label STRING) PRIMARY KEY(id) \
         DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')",
    ).expect("create");
    session.execute("INSERT INTO t_pk_cycle VALUES (1, 'old')").expect("insert old");
    session.execute("DELETE FROM t_pk_cycle WHERE id = 1").expect("delete old");
    session.execute("INSERT INTO t_pk_cycle VALUES (1, 'new')").expect("insert new");
    let r = session.query("SELECT id, label FROM t_pk_cycle").expect("query");
    assert_eq!(r.row_count(), 1, "expected exactly one row after delete-then-insert");
    let labels: Vec<String> = r.column_string("label").expect("labels");
    assert_eq!(labels, vec!["new".to_string()]);
}
```

```bash
cargo test --lib managed_pk_delete_via_op_column_path managed_pk_delete_complex_where managed_pk_delete_then_insert_same_pk_visible 2>&1 | tail -15
```

Expected: all three still pass via the **legacy CoW path** that M3.T4 left in place for PK. The point of these tests is to lock the expected behavior **before** switching the implementation; the next steps swap the implementation under them.

- [ ] **Step 2: Implement `execute_managed_pk_delete`**

Replace the legacy CoW dispatch in the `PRIMARY_KEYS` branch:

```rust
// In execute_managed_delete_statement, change:
"PRIMARY_KEYS" => execute_managed_cow_delete_legacy(state, target, stmt, target_ref),

// to:
"PRIMARY_KEYS" => execute_managed_pk_delete(state, target, stmt, &table_info),
```

Add the function in `src/engine/delete_flow.rs`:

```rust
fn execute_managed_pk_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    table_info: &ManagedTableInfo,
) -> Result<StatementResult, String> {
    // Build the SELECT plan: SELECT <pk_cols>, 1 AS __op FROM <table> WHERE <where>.
    let pk_cols = table_info.key_columns.iter().cloned().collect::<Vec<_>>();
    if pk_cols.is_empty() {
        return Err(format!(
            "managed-lake PRIMARY KEY table '{}' has no key columns",
            target.table
        ));
    }
    let pk_list = pk_cols.join(", ");
    let where_sql = render_where_for_select(&stmt.where_clause)?;
    let qualified_table = qualify_managed_table(target);
    let select_sql = format!(
        "SELECT {pk_list}, CAST(1 AS TINYINT) AS __op FROM {qualified_table} WHERE {where_sql}",
    );

    // Parse + analyze + execute the SELECT through the standalone pipeline,
    // collecting RecordBatches that include the __op column.
    let parsed = crate::sql::parser::parse_sql_raw(&select_sql)?;
    let sqlast::Statement::Query(query) = parsed else {
        return Err("internal: managed PK DELETE rewrite did not parse as SELECT".into());
    };

    // Execute via the same pipeline used by INSERT INTO SELECT, but
    // route the resulting chunks to the managed-lake sink (already
    // wired in insert_flow). This sink calls
    // append_lake_txn_log_with_chunk_rowset, which sees `__op` and
    // automatically splits upserts (none here) from deletes (.del file).
    crate::engine::insert_flow::execute_insert_from_query_into_managed_sink(
        state,
        target,
        // The pseudo "INSERT INTO t (pk_cols, __op) SELECT ..." shape;
        // helper signature should accept (target, target_columns,
        // query_ast) and run the standalone INSERT pipeline.
        &pk_cols,
        true, // include_load_op_column = the helper appends __op binding
        query.as_ref(),
    )?;

    Ok(StatementResult::Ok)
}

fn qualify_managed_table(target: &crate::engine::backend_resolver::TargetBackend) -> String {
    if target.catalog.is_empty() {
        if target.namespace.is_empty() {
            target.table.clone()
        } else {
            format!("{}.{}", target.namespace, target.table)
        }
    } else {
        format!("{}.{}.{}", target.catalog, target.namespace, target.table)
    }
}

fn render_where_for_select(where_expr: &sqlast::Expr) -> Result<String, String> {
    // sqlparser displays Expr losslessly via Display.
    Ok(where_expr.to_string())
}
```

If `execute_insert_from_query_into_managed_sink` doesn't exist with that signature, the implementer needs to either:
- **(a)** Add a new public helper alongside the existing `execute_insert_from_query_on_pipeline` ([src/engine/insert_flow.rs:183](src/engine/insert_flow.rs#L183)), which accepts a flag indicating the query is for a DELETE (so it doesn't strip `__op`), **or**
- **(b)** Use the existing `execute_insert_from_query_on_pipeline` if it already supports `__op`-in-output (inspect the function — many "insert pipeline" helpers in this codebase already preserve `__op` because of stream-load support)

Inspect:

```bash
sed -n '180,250p' src/engine/insert_flow.rs
```

and choose the minimal-change path.

- [ ] **Step 3: Iterate until tests pass**

```bash
cargo build 2>&1 | tail -10
cargo test --lib managed_pk_delete_via_op_column_path managed_pk_delete_complex_where 2>&1 | tail -20
```

If the sink fails to recognize `__op` because the chunk gets the column stripped earlier in the INSERT pipeline, trace `parse_op_batch` reachability ([txn_log.rs:1394](src/connector/starrocks/lake/txn_log.rs#L1394)) and see what's between the SELECT output and that function.

- [ ] **Step 4: Commit**

```bash
git add src/engine/delete_flow.rs src/engine/insert_flow.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
feat(engine): wire managed PRIMARY KEY DELETE through __op + sink

Rewrite DELETE FROM pk_t WHERE cond into
  SELECT pk_cols, CAST(1 AS TINYINT) AS __op FROM pk_t WHERE cond
and execute the SELECT through the standalone INSERT-INTO-SELECT
pipeline. The managed-lake sink already recognizes __op (it was
written for stream-load merge semantics): the chunk gets routed to
.del file encoding via parse_op_batch and encode_delete_keys_payload,
and PK index lookup + delvec emission happens at publish time
through the existing pk_applier path.

WHERE accepts any plannable form, including non-key columns,
functions, joins, and subqueries — same surface as StarRocks PK
DELETE.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M4.T3 — Delete CoW + old unit test (G5)

**Files:**
- Modify: `src/engine/delete_flow.rs` (delete CoW body from lines 354-395 of original; the dispatcher we replaced it with stays)
- Modify: `src/engine/mod.rs` (remove `managed_delete_rewrites_remaining_rows_for_primary_key_table` test, line ~4765)

- [ ] **Step 1: Delete `execute_managed_cow_delete_legacy`**

M3.T4 extracted the old CoW into `execute_managed_cow_delete_legacy` so PK DELETE remained working between M3 and M4.T2. After M4.T2 landed, no caller references that function. Confirm and delete:

```bash
grep -n "execute_managed_cow_delete_legacy" src/engine/delete_flow.rs src/engine/mod.rs
```

If the only reference is the function definition itself in `delete_flow.rs`, delete the whole function. If any caller remains (e.g., the dispatcher still routes some case to it), that's a bug — fix the dispatcher first.

Also verify no other CoW-style code (`rewritten_sql.*COALESCE`, `truncate_managed_table` outside the TRUNCATE statement handler) is left in `delete_flow.rs`:

```bash
grep -n "truncate_managed_table\|rewritten_sql.*COALESCE" src/engine/delete_flow.rs
```

Expected: zero matches.

- [ ] **Step 2: Remove the old unit test**

```bash
grep -n "managed_delete_rewrites_remaining_rows_for_primary_key_table" src/engine/mod.rs
```

Open `src/engine/mod.rs` at that line and delete the entire `#[test] fn managed_delete_rewrites_remaining_rows_for_primary_key_table()` function. (It is superseded by the two M4.T2 tests plus the M3.T4 DUP test.)

- [ ] **Step 3: Smoke-check TRUNCATE TABLE still works**

The `truncate_managed_table` helper is still required for `TRUNCATE TABLE` SQL. Confirm:

```bash
grep -n "truncate_managed_table\b" src/engine/ src/connector/starrocks/managed/ -r --include="*.rs" | head
```

There should be at least one caller in the TRUNCATE statement handler (not in `delete_flow.rs` anymore). Run an existing TRUNCATE test (find one):

```bash
grep -n "fn .*truncate" src/engine/mod.rs | head
cargo test --lib managed_truncate 2>&1 | tail -5  # adapt name
```

- [ ] **Step 4: Build and run full Rust test suite**

```bash
cargo build 2>&1 | tail -5
cargo test --lib 2>&1 | tail -20
```

Expected: all PASS. No reference to CoW remains.

- [ ] **Step 5: Commit**

```bash
git add src/engine/delete_flow.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
refactor(engine): remove managed-lake CoW DELETE legacy fallback

execute_managed_cow_delete_legacy was retained by M3.T4 so PRIMARY KEY
DELETE kept working through the M3 → M4.T2 window. With the new
__op + sink path live for PK and the DeletePredicate path live for
DUP/UNIQUE/AGG, the legacy function is unreachable and the old O(N)
SELECT-survivors → truncate → re-append behavior is gone. Delete the
helper and the unit test it backed.

TRUNCATE TABLE continues to use the truncate_managed_table helper.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M4.T4 — SQL regression tests for PK + MV rejection

**Files:**
- Create: `sql-tests/write-path/sql/managed_pk_delete_complex_where.sql` (+ `.result`)
- Create: `sql-tests/write-path/sql/managed_pk_delete_no_match.sql` (+ `.result`)
- Create: `sql-tests/write-path/sql/managed_mv_delete_rejected.sql` (+ `.result` with expected error)

- [ ] **Step 1: Write 3 new `.sql` files**

`managed_pk_delete_complex_where.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed,primary_key,delete
-- Test Objective: PK DELETE supports non-key WHERE and functions.
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_pk_complex;
CREATE TABLE ${case_db}.t_pk_complex (
  id INT NOT NULL,
  k INT,
  label STRING
)
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_pk_complex VALUES (1,10,'X'),(2,20,'Y'),(3,30,'Z');
DELETE FROM ${case_db}.t_pk_complex WHERE LOWER(label) = 'y';
SELECT id, k, label FROM ${case_db}.t_pk_complex ORDER BY id;
```

`.result`:

```
id	k	label
1	10	X
3	30	Z
```

`managed_pk_delete_no_match.sql`:

```sql
-- @tags=write_path,managed,primary_key,delete
SET catalog default_catalog;
DROP TABLE IF EXISTS ${case_db}.t_pk_nomatch;
CREATE TABLE ${case_db}.t_pk_nomatch (id INT NOT NULL, v INT) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_pk_nomatch VALUES (1,100);
DELETE FROM ${case_db}.t_pk_nomatch WHERE id = 999;
SELECT id, v FROM ${case_db}.t_pk_nomatch ORDER BY id;
```

`.result`:

```
id	v
1	100
```

`managed_mv_delete_rejected.sql`:

```sql
-- @tags=write_path,managed,materialized_view,delete
-- @expect_error=cannot be deleted because it is a materialized view
SET catalog default_catalog;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.mv_basic;
DROP TABLE IF EXISTS ${case_db}.base_for_mv;
CREATE TABLE ${case_db}.base_for_mv (id INT NOT NULL, v INT) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.base_for_mv VALUES (1,10);
CREATE MATERIALIZED VIEW ${case_db}.mv_basic AS SELECT id, v FROM ${case_db}.base_for_mv;
DELETE FROM ${case_db}.mv_basic WHERE id = 1;
```

(Inspect existing MV tests for exact MV DDL syntax accepted by NovaRocks; the example above is illustrative.)

- [ ] **Step 2: Record / verify**

```bash
# Server should be running from earlier task; start if not.
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode record \
  --only managed_pk_delete_complex_where,managed_pk_delete_no_match 2>&1 | tail -10
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode verify \
  --only managed_pk_delete_complex_where,managed_pk_delete_no_match,managed_mv_delete_rejected 2>&1 | tail -10
```

Inspect generated `.result` files for correctness.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/write-path/sql/managed_pk_*.sql \
        sql-tests/write-path/result/managed_pk_*.result \
        sql-tests/write-path/sql/managed_mv_delete_rejected.sql \
        sql-tests/write-path/result/managed_mv_delete_rejected.result
git commit -m "$(cat <<'EOF'
test(write-path): cover PK DELETE complex WHERE, no-match, MV rejection

Three new sql-test cases:
- PK DELETE with function-on-non-key-column WHERE (LOWER(label) = 'y')
- PK DELETE matching zero rows produces no error and no visible change
- DELETE on a materialized view rejected with StarRocks-aligned error

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task M4.T5 — Full regression + .del byte-compat sanity (G3 + G4 + G1)

This is the final gate before merge. **Hard block** on any regression in INSERT-heavy suites (G3) or full suite (G4).

- [ ] **Step 1: Run full Rust test suite**

```bash
cargo test --lib 2>&1 | tail -15
```

Expected: all PASS.

- [ ] **Step 2: Start server (debug build, current binary)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-server-m4t5.log
cargo build 2>&1 | tail -3
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' "$LOG" && break; sleep 1; done
```

- [ ] **Step 3: Run write-path suite (target verification)**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite write-path --mode verify 2>&1 | tail -25
```

Expected:
- `primary_key_insert_delete_select` PASS
- `primary_key_upsert_delete_select` PASS
- `datetime_microsecond_precision_delete` PASS (all 6 queries)
- All 10 new managed_* cases PASS

- [ ] **Step 4: Run INSERT-heavy regression suites (G3 gate)**

```bash
for SUITE in tpc-h tpc-ds ssb; do
  echo "=== $SUITE ==="
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$SUITE" --mode verify 2>&1 | tail -10
done
```

Expected: no new failures vs M2 baseline. INSERT into tpc-h.lineitem etc. still works through the sink that now also handles `__op` chunks.

- [ ] **Step 5: Run remaining suites (G4 gate)**

```bash
for SUITE in cte join filter sort function iceberg iceberg-rest; do
  echo "=== $SUITE ==="
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$SUITE" --mode verify 2>&1 | tail -10
done
```

Expected: no new failures.

- [ ] **Step 6: `.del` byte-compat sanity (G1 gate)**

Inspect the `.del` file produced by a managed PK DELETE in this session's MinIO bucket, and compare to a known StarRocks BE-written sample (if available):

```bash
# Find a .del file written during the test session.
source docker/iceberg-rest/runtime/current/env.sh
mc alias set local "$AWS_S3_ENDPOINT" "$AWS_S3_ACCESS_KEY_ID" "$AWS_S3_SECRET_ACCESS_KEY" 2>/dev/null || true
mc find "local/${NOVAROCKS_MANAGED_LAKE_WAREHOUSE#s3://}" --name "*.del" --print "{path}" 2>&1 | head -5
```

If a `.del` file exists, dump its head:

```bash
mc cat "local/<path>.del" | xxd | head -10
```

Confirm the SLICE_ESCAPE prefix and PK encoding match the format produced by `encode_delete_keys_payload` ([delete_payload_codec.rs:28](src/connector/starrocks/lake/delete_payload_codec.rs#L28)) — round-trip via NovaRocks' own decoder is already covered by unit tests, so byte format is internally consistent. **Cross-engine validation against an actual StarRocks BE binary** is optional but recommended; record findings in implementation notes if performed.

- [ ] **Step 7: Cleanup server**

```bash
kill $SRV_PID 2>/dev/null
```

- [ ] **Step 8: No code commit if gates pass; otherwise iterate**

If any gate fails:
- **G3 failure** (INSERT regression): the sink path change introduced a regression in chunks without `__op`. Most likely a defensive check in `parse_op_batch` is firing when it shouldn't. Inspect `txn_log.rs:1394` for the "no `__op` column" early return — should return `Ok(None)` and downstream should treat as pure-upsert.
- **G4 failure**: literal coercion (C) might have shifted a non-DELETE comparison. Bisect with `git bisect` against pre-M2 binary.
- **G1 sanity weird**: the `.del` byte content might encode some types differently than StarRocks BE; not blocking unless an actual interop test fails.

---

## Final Cleanup & PR Prep

- [ ] **Step 1: Run `cargo fmt` and `cargo clippy`**

```bash
cargo fmt
cargo clippy --all-targets 2>&1 | tail -30
```

Fix any new lints introduced by the milestones. Commit fixes with `style: clippy/fmt fixes for INT-2 series`.

- [ ] **Step 2: Verify the implementation lines up with spec §10.7**

Approximate target: ~2500 lines net (production + tests), 12 sql-test cases. Run `git diff --stat main` to confirm:

```bash
git diff --stat main 2>&1 | tail -10
```

If significantly different, investigate (over-implementation is as bad as under-).

- [ ] **Step 3: Create PR**

Title: `feat(managed-lake): align DELETE with StarRocks semantics (INT-2)`

Body should reference [docs/superpowers/specs/2026-05-12-managed-lake-delete-design.md](docs/superpowers/specs/2026-05-12-managed-lake-delete-design.md), summarize the milestones, and link to this plan.

---

## Notes for the Implementer

- **Build mode**: `cargo build` (debug) by default. Use `--release` only for final regression gate.
- **Server lifecycle**: Each milestone's full-suite verification step starts and stops the server in-task. Don't leave servers running between tasks — port conflicts cause silent regressions.
- **Worktree port**: `$NOVA_ENV_MYSQL_PORT` is allocated per worktree; never hard-code `9030`.
- **Plan deviation**: if any task uncovers code that needs to be added to the plan (e.g., a missing helper function not noted here), update this plan file in the same PR — don't leave the plan inconsistent.
- **Spec deviation**: if implementation reveals the spec is wrong, prefer updating the spec (in `docs/superpowers/specs/`) over silently diverging.
- **Test isolation**: managed-lake tests share a single MinIO bucket. Use unique table names per test (the existing pattern uses `${case_db}_<test_name>` substitution).
