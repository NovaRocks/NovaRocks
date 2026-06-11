# Iceberg Schema Evolution Phase 2 PR-3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ALTER TABLE ... SET / UNSET TBLPROPERTIES` SQL with an explicit denylist for NovaRocks-private and Iceberg-internal keys, reusing the PR-2 `commit_with_retry` helper for conflict tolerance. Closes the 10th and final item of §5 of the completion checklist.

**Architecture:** A new statement type `AlterIcebergPropertiesStmt` parses alongside the existing schema DDL but routes through a separate engine handler `handle_alter_iceberg_properties` and a separate executor `alter_table_properties`. The executor wraps `iceberg::Transaction::update_table_properties()` in a closure passed to `commit_with_retry` (already in the file from PR-2). The denylist is a single `is_reserved_property_key(&str) -> Option<PropertyDenialReason>` predicate consulted by the analyzer before any commit attempt.

**Tech Stack:** Rust 2021, vendored `iceberg 0.9.0` (`Transaction::update_table_properties()` returns `UpdatePropertiesAction` with chainable `.set(k, v)` / `.remove(k)`), no new crates.

**Spec:** [docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md](../specs/2026-05-06-iceberg-schema-evolution-phase2-design.md) §6 (PR-3).

**Branch:** `claude/iceberg-set-tblproperties` stacked on `claude/iceberg-ddl-commit-retry` (PR-2 / #88). Worktree `/Users/harbor/worktree/NovaRocks/iceberg-set-tblproperties`.

**Note on stacking:** Until PR #88 merges, this PR's diff against `main` will include PR-2's commits. After PR #88 merges, a `git rebase upstream/main` will collapse those commits and leave only PR-3's diff. No special handling needed during execution.

---

## Scope decisions vs spec

The brainstorming-time choices for PR-3 are unchanged from spec §6:

- **Denylist semantic** (option C from brainstorm): blacklist `novarocks.*` (full prefix), `format-version`, `identifier-field-ids`, `current-schema-id`, `default-spec-id`, `default-sort-order-id`, `last-column-id`, `last-partition-id`, `last-sequence-number`. Default-allow everything else.
- **No mixed SET + UNSET** in one statement (Spark / Hive idiom). Two statements required.
- **Strict UNSET by default**, `IF EXISTS` clause for silent-skip.
- **`format-version` rejection** with a forward-pointing error message ("use UPGRADE TABLE syntax (not yet implemented)").

This PR does NOT touch:

- The `UPGRADE TABLE` syntax for format-version. That belongs in §3 of the checklist.
- Cancellation in the retry sleep gap. PR-2's `TODO(cancellation)` carries forward; nothing new to plumb.
- Cross-engine fixture for property roundtrip (§17 work).

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/engine/statement.rs` | Add `looks_like_alter_iceberg_properties`, `AlterIcebergPropertiesStmt`, `PropertiesOp`, `parse_alter_iceberg_properties_sql`, parser tests |
| Modify | `src/engine/mod.rs` | Add new dispatcher branch + `handle_alter_iceberg_properties` method |
| Modify | `src/connector/iceberg/catalog/schema_update.rs` | Add `is_reserved_property_key` predicate, `alter_table_properties` executor (reuses `commit_with_retry`), denylist tests, executor unit tests |
| Modify | `src/connector/iceberg/catalog/mod.rs` (if needed) | Re-export `alter_table_properties` for engine import |
| Create | `sql-tests/iceberg/sql/iceberg_table_properties_set_unset.sql` | Happy path SET / UNSET / overwrite |
| Create | `sql-tests/iceberg/sql/iceberg_table_properties_unset_if_exists.sql` | UNSET IF EXISTS behavior |
| Create | `sql-tests/iceberg/sql/iceberg_table_properties_reject_reserved.sql` | Each denylist category produces explicit error |
| Create | `sql-tests/iceberg/sql/iceberg_table_properties_combined_reject.sql` | One statement with SET + UNSET both rejected; duplicate keys rejected |
| Create | `sql-tests/iceberg/result/iceberg_table_properties_*.result` | Golden results for each suite (recorded via `--mode record`) |

Boundaries:

- `is_reserved_property_key` is a free function so unit tests don't have to spin up a parser.
- `alter_table_properties` lives in `schema_update.rs` (with the schema commit helpers) rather than a new file: the file already owns the catalog/cache/transaction integration and the new code is small (~80 lines). YAGNI on a separate file.
- Parser-side AST (`AlterIcebergPropertiesStmt` + `PropertiesOp`) lives next to `AlterIcebergSchemaStmt` for proximity; no new module required.

---

## Pre-flight

- [ ] **Step 0.1: Verify branch and clean tree**

```
cd /Users/harbor/worktree/NovaRocks/iceberg-set-tblproperties
git rev-parse --abbrev-ref HEAD
git status
git log --oneline upstream/main..HEAD | head -10
```

Expected: branch `claude/iceberg-set-tblproperties`; clean tree; 6 commits ahead of `upstream/main` (5 PR-2 commits + 1 PR-2 plan doc).

- [ ] **Step 0.2: Verify PR-2 baseline tests pass**

```
cargo test -p novarocks --lib schema_update 2>&1 | tail -5
```
Expected: 100 passing (PR-2 baseline).

---

## Phase A: Parser

### Task A1: Add `AlterIcebergPropertiesStmt`, `PropertiesOp`, `looks_like_alter_iceberg_properties`, `parse_alter_iceberg_properties_sql`, and parser tests

**Files:**
- Modify: `src/engine/statement.rs`

The parser handles 3 surface forms:

```sql
ALTER TABLE ice.db.t SET TBLPROPERTIES ('k1' = 'v1', 'k2' = 'v2');
ALTER TABLE ice.db.t UNSET TBLPROPERTIES ('k1', 'k2');
ALTER TABLE ice.db.t UNSET TBLPROPERTIES IF EXISTS ('k1', 'k2');
```

Hard rejects (parser-level):

- Duplicate keys in SET (`SET TBLPROPERTIES ('k'='v1', 'k'='v2')`) — analyzer-level reject (we check after parse).
- Empty parens (`SET TBLPROPERTIES ()`) — parser-level reject.
- Mixed SET and UNSET in one statement — disallowed by grammar (the parser sees one keyword or the other, never both).
- Non-string key (e.g. `SET TBLPROPERTIES (foo = 'bar')` without quotes) — parser-level reject; require quoted strings on both sides of SET, quoted strings for UNSET keys.

- [ ] **Step 1: Write failing tests**

Add inside the existing parser tests module of `statement.rs` (search `mod parse_alter_iceberg_schema_*` and place adjacent). Pick a fresh module name `mod parse_alter_iceberg_properties_tests` to keep namespace clean:

```rust
#[cfg(test)]
mod parse_alter_iceberg_properties_tests {
    use super::{
        AlterIcebergPropertiesStmt, PropertiesOp, looks_like_alter_iceberg_properties,
        parse_alter_iceberg_properties_sql,
    };

    #[test]
    fn looks_like_set_tblproperties() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t SET TBLPROPERTIES ('k' = 'v')"
        ));
    }

    #[test]
    fn looks_like_unset_tblproperties() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t UNSET TBLPROPERTIES ('k')"
        ));
    }

    #[test]
    fn looks_like_unset_tblproperties_if_exists() {
        assert!(looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t UNSET TBLPROPERTIES IF EXISTS ('k')"
        ));
    }

    #[test]
    fn looks_like_does_not_match_alter_column() {
        assert!(!looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t ADD COLUMN c INT"
        ));
        assert!(!looks_like_alter_iceberg_properties(
            "ALTER TABLE ice.db.t ALTER COLUMN c FIRST"
        ));
    }

    #[test]
    fn parse_set_one_pair() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE ice.db.t SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd')"
        ).expect("parse");
        assert_eq!(stmt.table.parts, vec!["ice", "db", "t"]);
        let PropertiesOp::Set { entries } = stmt.op else { panic!() };
        assert_eq!(
            entries,
            vec![("write.parquet.compression-codec".to_string(), "zstd".to_string())]
        );
    }

    #[test]
    fn parse_set_multiple_pairs() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES ('a' = 'x', 'b' = 'y', 'c' = 'z')"
        ).expect("parse");
        let PropertiesOp::Set { entries } = stmt.op else { panic!() };
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], ("a".to_string(), "x".to_string()));
        assert_eq!(entries[2], ("c".to_string(), "z".to_string()));
    }

    #[test]
    fn parse_unset_strict() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t UNSET TBLPROPERTIES ('a', 'b')"
        ).expect("parse");
        let PropertiesOp::Unset { keys, if_exists } = stmt.op else { panic!() };
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
        assert!(!if_exists);
    }

    #[test]
    fn parse_unset_if_exists() {
        let stmt = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t UNSET TBLPROPERTIES IF EXISTS ('a')"
        ).expect("parse");
        let PropertiesOp::Unset { keys, if_exists } = stmt.op else { panic!() };
        assert_eq!(keys, vec!["a".to_string()]);
        assert!(if_exists);
    }

    #[test]
    fn parse_set_empty_parens_rejected() {
        assert!(parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES ()"
        ).is_err());
    }

    #[test]
    fn parse_unset_empty_parens_rejected() {
        assert!(parse_alter_iceberg_properties_sql(
            "ALTER TABLE t UNSET TBLPROPERTIES ()"
        ).is_err());
    }

    #[test]
    fn parse_set_duplicate_key_rejected() {
        let res = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES ('a' = 'x', 'a' = 'y')"
        );
        assert!(res.is_err());
        assert!(res.unwrap_err().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn parse_unset_duplicate_key_rejected() {
        let res = parse_alter_iceberg_properties_sql(
            "ALTER TABLE t UNSET TBLPROPERTIES ('a', 'a')"
        );
        assert!(res.is_err());
        assert!(res.unwrap_err().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn parse_unquoted_key_rejected() {
        // Keys must be string literals, not identifiers.
        assert!(parse_alter_iceberg_properties_sql(
            "ALTER TABLE t SET TBLPROPERTIES (foo = 'bar')"
        ).is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

```
cargo test -p novarocks --lib parse_alter_iceberg_properties 2>&1 | tail -10
```
Expected: cannot find types/functions.

- [ ] **Step 3: Implement the AST + parser**

Place the new types adjacent to the existing `AlterIcebergSchemaStmt` (around line 1089-1092 in `statement.rs`):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlterIcebergPropertiesStmt {
    pub(crate) table: ObjectName,
    pub(crate) op: PropertiesOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PropertiesOp {
    Set { entries: Vec<(String, String)> },
    Unset { keys: Vec<String>, if_exists: bool },
}
```

Add the dispatcher guard (mirror the shape of `looks_like_alter_iceberg_schema` near line 1421):

```rust
pub(crate) fn looks_like_alter_iceberg_properties(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    if parser.parse_keyword(Keyword::SET)
        && crate::sql::parser::dialect::peek_word_eq(&parser, 0, "TBLPROPERTIES")
    {
        return true;
    }
    // After failed SET match, restart from a fresh parser since parse_keyword consumed token.
    let Ok(normalized2) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser2) = Parser::new(&StarRocksDialect).try_with_sql(&normalized2) else {
        return false;
    };
    if !parser2.parse_keyword(Keyword::ALTER) || !parser2.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser2.parse_object_name(false).is_err() {
        return false;
    }
    parser2.parse_keyword(Keyword::UNSET)
        && crate::sql::parser::dialect::peek_word_eq(&parser2, 0, "TBLPROPERTIES")
}
```

**Note on the double-parser:** `parse_keyword(Keyword::SET)` advances the parser even on success, so if SET matches but TBLPROPERTIES doesn't, restarting is the simplest correct behavior. There may be a cleaner approach using `peek_word_eq` for SET/UNSET both — feel free to refactor to a single pass if you find a clean idiom.

Add the main parser (mirror `parse_alter_iceberg_schema_sql` shape):

```rust
pub(crate) fn parse_alter_iceberg_properties_sql(
    sql: &str,
) -> Result<AlterIcebergPropertiesStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE TBLPROPERTIES DDL: {e}"))?;

    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;

    let op = if parser.parse_keyword(Keyword::SET) {
        if !parser.parse_keyword_if_present_via_peek("TBLPROPERTIES") {
            // Helper above is pseudocode; in real code use:
            //   peek_word_eq(&parser, 0, "TBLPROPERTIES") && consume next token
            return Err("expected TBLPROPERTIES after SET".to_string());
        }
        let entries = parse_property_entries(&mut parser)?;
        PropertiesOp::Set { entries }
    } else if parser.parse_keyword(Keyword::UNSET) {
        if !crate::sql::parser::dialect::peek_word_eq(&parser, 0, "TBLPROPERTIES") {
            return Err("expected TBLPROPERTIES after UNSET".to_string());
        }
        parser.next_token(); // consume TBLPROPERTIES
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let keys = parse_property_keys(&mut parser)?;
        PropertiesOp::Unset { keys, if_exists }
    } else {
        return Err("expected SET or UNSET TBLPROPERTIES".to_string());
    };

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported trailing tokens at {}",
            parser.peek_token_ref().token
        ));
    }
    Ok(AlterIcebergPropertiesStmt { table, op })
}

fn parse_property_entries(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    parser.expect_token(&Token::LParen).map_err(|e| e.to_string())?;
    let mut entries = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    loop {
        let key = parse_string_literal(parser)?;
        parser.expect_token(&Token::Eq).map_err(|e| e.to_string())?;
        let value = parse_string_literal(parser)?;
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate key '{key}' in SET TBLPROPERTIES"));
        }
        entries.push((key, value));
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        break;
    }
    parser.expect_token(&Token::RParen).map_err(|e| e.to_string())?;
    if entries.is_empty() {
        return Err("SET TBLPROPERTIES requires at least one key=value pair".to_string());
    }
    Ok(entries)
}

fn parse_property_keys(parser: &mut Parser<'_>) -> Result<Vec<String>, String> {
    parser.expect_token(&Token::LParen).map_err(|e| e.to_string())?;
    let mut keys = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    loop {
        let key = parse_string_literal(parser)?;
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate key '{key}' in UNSET TBLPROPERTIES"));
        }
        keys.push(key);
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        break;
    }
    parser.expect_token(&Token::RParen).map_err(|e| e.to_string())?;
    if keys.is_empty() {
        return Err("UNSET TBLPROPERTIES requires at least one key".to_string());
    }
    Ok(keys)
}

fn parse_string_literal(parser: &mut Parser<'_>) -> Result<String, String> {
    use sqlparser::tokenizer::Token;
    let tok = parser.next_token();
    match tok.token {
        Token::SingleQuotedString(s) => Ok(s),
        Token::DoubleQuotedString(s) => Ok(s),
        other => Err(format!(
            "TBLPROPERTIES key/value must be a string literal, got `{other}`"
        )),
    }
}
```

**Note**: `parse_keyword_if_present_via_peek` is pseudocode in the snippet above. Replace with the actual idiom: check `peek_word_eq(&parser, 0, "TBLPROPERTIES")` then `parser.next_token()` to consume. The point: `TBLPROPERTIES` is not a sqlparser-recognized keyword, so we can't use `parse_keyword`. The dialect helper `peek_word_eq` (already used elsewhere) is the right tool.

Verify the helper signature against existing usage in `statement.rs` (e.g., the MODIFY branch in `parse_alter_iceberg_schema_sql`).

- [ ] **Step 4: Run tests to verify pass**

```
cargo test -p novarocks --lib parse_alter_iceberg_properties 2>&1 | tail -15
cargo test -p novarocks --lib statement::looks_like_alter 2>&1 | tail -10
cargo fmt --check 2>&1 | tail -3
```

Expected: 12 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(iceberg): parse ALTER TABLE SET/UNSET TBLPROPERTIES syntax"
```

---

## Phase B: Denylist predicate

### Task B1: Add `is_reserved_property_key` + tests

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

The predicate returns `Option<&'static str>` where `Some(reason)` is a human-readable category for the error message.

- [ ] **Step 1: Write failing tests**

Append to `#[cfg(test)] mod tests { ... }` block in `schema_update.rs`:

```rust
#[test]
fn reserved_key_format_version() {
    let reason = is_reserved_property_key("format-version").expect("denied");
    let lower = reason.to_lowercase();
    assert!(lower.contains("upgrade table") || lower.contains("format-version"));
}

#[test]
fn reserved_key_identifier_field_ids() {
    assert!(is_reserved_property_key("identifier-field-ids").is_some());
}

#[test]
fn reserved_key_internal_schema_id() {
    assert!(is_reserved_property_key("current-schema-id").is_some());
    assert!(is_reserved_property_key("default-spec-id").is_some());
    assert!(is_reserved_property_key("default-sort-order-id").is_some());
}

#[test]
fn reserved_key_internal_counters() {
    assert!(is_reserved_property_key("last-column-id").is_some());
    assert!(is_reserved_property_key("last-partition-id").is_some());
    assert!(is_reserved_property_key("last-sequence-number").is_some());
}

#[test]
fn reserved_key_novarocks_logical_type_prefix() {
    assert!(is_reserved_property_key("novarocks.logical_type.foo").is_some());
}

#[test]
fn reserved_key_novarocks_column_agg_prefix() {
    assert!(is_reserved_property_key("novarocks.column_agg.bar").is_some());
}

#[test]
fn reserved_key_novarocks_table_key_columns() {
    assert!(is_reserved_property_key("novarocks.table.key_columns").is_some());
}

#[test]
fn reserved_key_novarocks_nullability_attested_prefix() {
    assert!(is_reserved_property_key("novarocks.nullability.attested.address.street").is_some());
}

#[test]
fn reserved_key_novarocks_unknown_prefix_blocked() {
    // Forward-compat: any unknown novarocks.* key is reserved.
    assert!(is_reserved_property_key("novarocks.future.feature").is_some());
    assert!(is_reserved_property_key("novarocks.x").is_some());
}

#[test]
fn reserved_key_allows_iceberg_write_props() {
    assert!(is_reserved_property_key("write.parquet.compression-codec").is_none());
    assert!(is_reserved_property_key("write.format.default").is_none());
    assert!(is_reserved_property_key("write.target-file-size-bytes").is_none());
    assert!(is_reserved_property_key("history.expire.max-snapshot-age-ms").is_none());
    assert!(is_reserved_property_key("commit.retry.num-retries").is_none());
    assert!(is_reserved_property_key("gc.enabled").is_none());
}

#[test]
fn reserved_key_allows_user_custom_keys() {
    assert!(is_reserved_property_key("my.custom.key").is_none());
    assert!(is_reserved_property_key("foo").is_none());
    assert!(is_reserved_property_key("comment").is_none());
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p novarocks --lib schema_update::tests::reserved_key 2>&1 | tail -10
```
Expected: `cannot find function is_reserved_property_key`.

- [ ] **Step 3: Implement the predicate**

Place near `is_retryable_commit_conflict` in `schema_update.rs`:

```rust
/// Whether a property key is reserved (cannot be set/unset by SET TBLPROPERTIES).
/// Returns `None` if the key is user-modifiable, or `Some(reason)` containing a
/// human-readable category to include in the error message.
fn is_reserved_property_key(key: &str) -> Option<&'static str> {
    if key == "format-version" {
        return Some(
            "format-version is reserved; use UPGRADE TABLE syntax (not yet implemented in NovaRocks)"
        );
    }
    if matches!(
        key,
        "identifier-field-ids"
            | "current-schema-id"
            | "default-spec-id"
            | "default-sort-order-id"
            | "last-column-id"
            | "last-partition-id"
            | "last-sequence-number"
    ) {
        return Some("Iceberg internal metadata key, not user-settable");
    }
    if key.starts_with("novarocks.") {
        return Some("novarocks.* namespace is reserved for engine-managed properties");
    }
    None
}
```

- [ ] **Step 4: Run to verify pass**

```
cargo test -p novarocks --lib schema_update::tests::reserved_key 2>&1 | tail -15
```
Expected: 11 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(iceberg): is_reserved_property_key denylist predicate"
```

---

## Phase C: Executor

### Task C1: Add `alter_table_properties` reusing `commit_with_retry`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs` (add `pub use ... alter_table_properties` if existing module re-exports `alter_table_schema`; verify pattern)

The executor:
1. Validates ALL keys against denylist BEFORE any commit attempt (fail-fast UX).
2. Validates strict UNSET keys exist on the FIRST attempt's loaded metadata. Subsequent retries also revalidate per spec §5.3 strict semantics.
3. Builds `Transaction::update_table_properties()` action with `.set(k,v)` / `.remove(k)` for each entry/key.
4. Calls `commit_with_retry` with a closure that re-loads + re-builds + commits.
5. Pre/post invalidates `entry.table_cache` mirroring `alter_table_schema`.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn properties_op_collect_denylist_hits_on_set() {
    let op = PropertiesOp::Set {
        entries: vec![
            ("write.parquet.compression-codec".to_string(), "zstd".to_string()),
            ("format-version".to_string(), "3".to_string()),
            ("novarocks.logical_type.foo".to_string(), "TINYINT".to_string()),
            ("my.user.key".to_string(), "value".to_string()),
        ],
    };
    let denied = collect_property_denylist_hits(&op);
    assert_eq!(denied.len(), 2);
    assert!(denied.iter().any(|(k, _)| k == "format-version"));
    assert!(denied.iter().any(|(k, _)| k == "novarocks.logical_type.foo"));
}

#[test]
fn properties_op_collect_denylist_hits_on_unset() {
    let op = PropertiesOp::Unset {
        keys: vec![
            "comment".to_string(),
            "identifier-field-ids".to_string(),
            "novarocks.table.key_columns".to_string(),
        ],
        if_exists: false,
    };
    let denied = collect_property_denylist_hits(&op);
    assert_eq!(denied.len(), 2);
    assert!(denied.iter().any(|(k, _)| k == "identifier-field-ids"));
    assert!(denied.iter().any(|(k, _)| k == "novarocks.table.key_columns"));
}

#[test]
fn properties_op_validate_unset_strict_missing_key() {
    use std::collections::HashMap;
    let mut existing = HashMap::new();
    existing.insert("a".to_string(), "1".to_string());
    let op = PropertiesOp::Unset {
        keys: vec!["a".to_string(), "b".to_string()],
        if_exists: false,
    };
    let res = validate_unset_keys_present(&op, &existing);
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("'b'"));
}

#[test]
fn properties_op_validate_unset_if_exists_skips_missing() {
    use std::collections::HashMap;
    let mut existing = HashMap::new();
    existing.insert("a".to_string(), "1".to_string());
    let op = PropertiesOp::Unset {
        keys: vec!["a".to_string(), "b".to_string()],
        if_exists: true,
    };
    assert!(validate_unset_keys_present(&op, &existing).is_ok());
}

#[test]
fn properties_op_compute_remove_keys_filters_missing_when_if_exists() {
    use std::collections::HashMap;
    let mut existing = HashMap::new();
    existing.insert("a".to_string(), "1".to_string());
    let op = PropertiesOp::Unset {
        keys: vec!["a".to_string(), "b".to_string()],
        if_exists: true,
    };
    let computed = compute_remove_keys(&op, &existing);
    assert_eq!(computed, vec!["a".to_string()]);
}
```

- [ ] **Step 2: Run to verify failure**

```
cargo test -p novarocks --lib schema_update::tests::properties_op 2>&1 | tail -10
```
Expected: cannot find functions.

- [ ] **Step 3: Implement helpers**

Place near the predicate (above `commit_with_retry`):

```rust
fn collect_property_denylist_hits(op: &PropertiesOp) -> Vec<(String, &'static str)> {
    let mut hits = Vec::new();
    match op {
        PropertiesOp::Set { entries } => {
            for (k, _) in entries {
                if let Some(reason) = is_reserved_property_key(k) {
                    hits.push((k.clone(), reason));
                }
            }
        }
        PropertiesOp::Unset { keys, .. } => {
            for k in keys {
                if let Some(reason) = is_reserved_property_key(k) {
                    hits.push((k.clone(), reason));
                }
            }
        }
    }
    hits
}

fn validate_unset_keys_present(
    op: &PropertiesOp,
    existing: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if let PropertiesOp::Unset { keys, if_exists } = op {
        if !*if_exists {
            for k in keys {
                if !existing.contains_key(k) {
                    return Err(format!(
                        "UNSET TBLPROPERTIES key '{k}' does not exist; use IF EXISTS to silently skip"
                    ));
                }
            }
        }
    }
    Ok(())
}

fn compute_remove_keys(
    op: &PropertiesOp,
    existing: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    if let PropertiesOp::Unset { keys, if_exists } = op {
        if *if_exists {
            return keys.iter().filter(|k| existing.contains_key(*k)).cloned().collect();
        } else {
            return keys.clone();
        }
    }
    Vec::new()
}
```

Add at top of file: `use crate::engine::statement::PropertiesOp;` (or qualify inline).

- [ ] **Step 4: Run helpers tests**

```
cargo test -p novarocks --lib schema_update::tests::properties_op 2>&1 | tail -15
```
Expected: 5 new tests pass.

- [ ] **Step 5: Implement `alter_table_properties`**

Below the helpers, mirror `alter_table_schema`:

```rust
pub(crate) fn alter_table_properties(
    state: &Arc<StandaloneState>,
    stmt: &AlterIcebergPropertiesStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    // 1. Resolve target backend; must be iceberg.
    let target = crate::engine::backend_resolver::resolve_iceberg_target_for_alter(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;

    // 2. Denylist check (fail-fast, before any IO).
    let denied = collect_property_denylist_hits(&stmt.op);
    if !denied.is_empty() {
        let mut msgs: Vec<String> = denied
            .iter()
            .map(|(k, reason)| format!("`{k}`: {reason}"))
            .collect();
        msgs.sort();
        return Err(format!(
            "ALTER TABLE TBLPROPERTIES rejected reserved key(s): {}",
            msgs.join("; ")
        ));
    }

    // 3. Get catalog entry.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("read iceberg catalog registry: {e}"))?;
        registry
            .get(&target.catalog)
            .ok_or_else(|| format!("unknown iceberg catalog `{}`", target.catalog))?
    };

    // 4. Pre-commit invalidate.
    entry.invalidate_table_cache(&target.namespace, &target.table);

    // 5. Drive commit_with_retry. Each retry re-invalidates + re-loads + re-applies + commits.
    let entry_for_retry = entry.clone();
    let namespace_for_retry = target.namespace.clone();
    let table_for_retry = target.table.clone();
    let op_for_retry = stmt.op.clone();

    let result: Result<(), String> = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        commit_with_retry(|_attempt| {
            let entry_inner = entry_for_retry.clone();
            let namespace_inner = namespace_for_retry.clone();
            let table_inner = table_for_retry.clone();
            let op_inner = op_for_retry.clone();
            async move {
                // Each retry must start with a fresh metadata read; otherwise load_table()
                // would serve the stale cached state that just produced the conflict.
                entry_inner.invalidate_table_cache(&namespace_inner, &table_inner);
                let loaded_inner = crate::connector::iceberg::catalog::registry::load_table_async(
                    &entry_inner,
                    &namespace_inner,
                    &table_inner,
                )
                .await
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("reload table for tblproperties retry: {e}"),
                    )
                })?;

                // Strict UNSET: validate every key is present in the LATEST metadata.
                let existing = loaded_inner.table.metadata().properties().clone();
                if let Err(msg) = validate_unset_keys_present(&op_inner, &existing) {
                    return Err(iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        msg,
                    ));
                }

                // HadoopFileSystemCatalog is not Clone; rebuild per attempt rather than
                // share a stale instance.
                let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(
                    &entry_inner,
                )
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("rebuild catalog for tblproperties retry: {e}"),
                    )
                })?;

                let tx = Transaction::new(&loaded_inner.table);
                let mut action = tx.update_table_properties();
                match &op_inner {
                    PropertiesOp::Set { entries } => {
                        for (k, v) in entries {
                            action = action.set(k.clone(), v.clone());
                        }
                    }
                    PropertiesOp::Unset { .. } => {
                        for k in compute_remove_keys(&op_inner, &existing) {
                            action = action.remove(k);
                        }
                    }
                }
                let tx = action.apply(tx).map_err(|e| {
                    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                })?;
                tx.commit(&catalog).await.map(|_| ())
            }
        })
        .await
    })
    .map_err(|e| format!("alter table properties failed: {e}"))?;

    // 6. Post-commit invalidate.
    entry.invalidate_table_cache(&target.namespace, &target.table);
    crate::connector::iceberg::catalog::registry::invalidate_iceberg_caches(&entry);
    Ok(())
}
```

**API name verification**: the survey reported `Transaction::update_table_properties()` returns `UpdatePropertiesAction` with `.set(k, v)` and `.remove(k)` (chainable). Confirm at `~/.cargo/registry/src/index.crates.io-*/iceberg-0.9.0/src/transaction/update_properties.rs` and `transaction/mod.rs:135`. If the method returns `&mut self` instead of `mut self → Self`, adjust the `let mut action = ...` style.

**`load_table_async` vs `load_table`**: the schema-update path uses a sync wrapper. Look at the existing `alter_table_schema` to see whether its closure calls `load_table` directly or an async variant. Mirror exactly.

**`backend_resolver::resolve_iceberg_target_for_alter`** is a pseudo-name. Look at how `alter_table_schema` resolves the target (via `target_backend` or similar) and use the same exact helper.

- [ ] **Step 6: Wire up the public re-export**

If `src/connector/iceberg/catalog/mod.rs` re-exports `alter_table_schema` (e.g. `pub use schema_update::alter_table_schema;`), add `alter_table_properties` next to it. Otherwise the engine layer imports via the full path.

- [ ] **Step 7: Build to confirm no compile errors**

```
cargo build 2>&1 | tail -10
```

Expected: clean build. Existing tests untouched at this point.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(iceberg): alter_table_properties executor for SET/UNSET TBLPROPERTIES"
```

---

## Phase D: Engine wiring

### Task D1: Add dispatcher + handler in `src/engine/mod.rs`

**Files:**
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Add dispatcher branch BEFORE the existing schema dispatcher**

In `src/engine/mod.rs` near line 553 (the existing `looks_like_alter_iceberg_schema` block), prepend a new branch:

```rust
// ALTER TABLE ... SET / UNSET TBLPROPERTIES
if looks_like_alter_iceberg_properties(&normalized) {
    return self.handle_alter_iceberg_properties(
        &normalized,
        current_catalog,
        current_database,
    );
}
```

Update the import block at line 55 to add `looks_like_alter_iceberg_properties`.

- [ ] **Step 2: Add `handle_alter_iceberg_properties` method**

Mirror `handle_alter_iceberg_schema` (~line 893-907):

```rust
fn handle_alter_iceberg_properties(
    &self,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let stmt = crate::engine::statement::parse_alter_iceberg_properties_sql(sql)?;
    crate::connector::iceberg::catalog::alter_table_properties(
        &self.inner,
        &stmt,
        current_catalog,
        current_database,
    )?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 3: Build + run all unit tests**

```
cargo build 2>&1 | tail -5
cargo test -p novarocks --lib schema_update 2>&1 | tail -5
cargo test -p novarocks --lib parse_alter_iceberg 2>&1 | tail -10
cargo fmt --check 2>&1 | tail -3
```

Expected: clean build; existing tests pass; no regressions.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(iceberg): wire ALTER TABLE TBLPROPERTIES into engine dispatcher"
```

---

## Phase E: SQL test suites

### Task E1: Add 4 SQL test files

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_table_properties_set_unset.sql` + `result/...`
- Create: `sql-tests/iceberg/sql/iceberg_table_properties_unset_if_exists.sql` + `result/...`
- Create: `sql-tests/iceberg/sql/iceberg_table_properties_reject_reserved.sql` + `result/...`
- Create: `sql-tests/iceberg/sql/iceberg_table_properties_combined_reject.sql` + `result/...`

- [ ] **Step 1: Write `iceberg_table_properties_set_unset.sql`**

```sql
-- @order_sensitive=true
-- ALTER TABLE SET / UNSET TBLPROPERTIES happy path.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");
INSERT INTO p VALUES (1);
SELECT id FROM p ORDER BY id;

-- query 2
ALTER TABLE p SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');
SHOW CREATE TABLE p;

-- query 3
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'hello', 'gc.enabled' = 'true');
SHOW CREATE TABLE p;

-- query 4
-- Overwrite an existing key.
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'world');
SHOW CREATE TABLE p;

-- query 5
ALTER TABLE p UNSET TBLPROPERTIES ('comment');
SHOW CREATE TABLE p;

-- query 6
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
```

If `SHOW CREATE TABLE` doesn't include the properties block in NovaRocks today, fall back to a simpler sanity check (e.g. INSERT/SELECT continues to work) and rely on the unit tests in Phase C to assert property presence. Verify `SHOW CREATE TABLE` output by running it against the standalone-server during recording.

- [ ] **Step 2: Write `iceberg_table_properties_unset_if_exists.sql`**

```sql
-- @order_sensitive=true
-- UNSET TBLPROPERTIES strict vs IF EXISTS.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");
ALTER TABLE p SET TBLPROPERTIES ('a' = '1', 'b' = '2');

-- query 2
-- Strict: missing key fails.
-- @expect_error=UNSET TBLPROPERTIES key 'c' does not exist
ALTER TABLE p UNSET TBLPROPERTIES ('a', 'c');

-- query 3
-- Existing keys unchanged after the failed strict UNSET.
SHOW CREATE TABLE p;

-- query 4
-- IF EXISTS: missing keys silently skipped, present keys still removed.
ALTER TABLE p UNSET TBLPROPERTIES IF EXISTS ('a', 'c');
SHOW CREATE TABLE p;

-- query 5
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
```

- [ ] **Step 3: Write `iceberg_table_properties_reject_reserved.sql`**

One `@expect_error` per denylist category:

```sql
-- @order_sensitive=true
-- Denylist coverage: each reserved category errors clearly.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=format-version is reserved
ALTER TABLE p SET TBLPROPERTIES ('format-version' = '3');

-- query 3
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('identifier-field-ids' = '[1]');

-- query 4
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('current-schema-id' = '5');

-- query 5
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET TBLPROPERTIES ('novarocks.logical_type.foo' = 'TINYINT');

-- query 6
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET TBLPROPERTIES ('novarocks.future' = 'whatever');

-- query 7
-- UNSET path covered too.
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p UNSET TBLPROPERTIES ('last-column-id');

-- query 8
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_reject_${uuid0};
```

- [ ] **Step 4: Write `iceberg_table_properties_combined_reject.sql`**

```sql
-- @order_sensitive=true
-- Parser-level rejects: empty parens, duplicate keys, unsupported grammar.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=at least one
ALTER TABLE p SET TBLPROPERTIES ();

-- query 3
-- @expect_error=at least one
ALTER TABLE p UNSET TBLPROPERTIES ();

-- query 4
-- @expect_error=duplicate
ALTER TABLE p SET TBLPROPERTIES ('a' = '1', 'a' = '2');

-- query 5
-- @expect_error=duplicate
ALTER TABLE p UNSET TBLPROPERTIES ('a', 'a');

-- query 6
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_grammar_${uuid0};
```

- [ ] **Step 5: Record golden results**

```
# Make sure standalone-server is built with PR-3 changes
cargo build 2>&1 | tail -3

# Kill any running server
lsof -i :9030 -sTCP:LISTEN 2>&1 | awk 'NR>1 {print $2}' | xargs -r kill 2>/dev/null
sleep 2

# Start standalone-server
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/nr-pr3-record.log 2>&1 &
SERVER_PID=$!
until lsof -i :9030 -sTCP:LISTEN 2>/dev/null | grep -q LISTEN; do sleep 1; done

# Record results
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_table_properties_set_unset,iceberg_table_properties_unset_if_exists,iceberg_table_properties_reject_reserved,iceberg_table_properties_combined_reject \
  --mode record

# Verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_table_properties_set_unset,iceberg_table_properties_unset_if_exists,iceberg_table_properties_reject_reserved,iceberg_table_properties_combined_reject \
  --mode verify

kill $SERVER_PID 2>/dev/null
sleep 2
```

Expected: 4/4 PASS in verify mode after record.

- [ ] **Step 6: Commit each test file separately or in one combined commit**

```bash
git add sql-tests/iceberg/sql/iceberg_table_properties_*.sql sql-tests/iceberg/result/iceberg_table_properties_*.result
git commit -m "test(iceberg): SET/UNSET TBLPROPERTIES SQL suites"
```

---

## Phase F: Final verification + checklist + PR

### Task F1: fmt + clippy

- [ ] **Step 1: Format check**

```
cargo fmt
cargo fmt --check 2>&1 | tail -3
```
Expected: clean.

- [ ] **Step 2: Clippy on touched files**

```
cargo clippy -p novarocks --lib --tests 2>&1 | grep -E "(warning|error).*(schema_update|statement|engine/mod).rs" | head -10
```
Expected: no new warnings/errors attributable to PR-3 files.

### Task F2: Full lib test pass

```
cargo test -p novarocks --lib 2>&1 | tail -10
```
Expected: total tests bump by ~28 (12 parser + 11 denylist + 5 helper). Same 4 pre-existing MinIO failures (`mv_refresh::aggregate_mv_incremental_refresh_*`) that PR-1 / PR-2 also produced.

### Task F3: SQL suite regression

```
lsof -i :9030 -sTCP:LISTEN 2>&1 | awk 'NR>1 {print $2}' | xargs -r kill 2>/dev/null
sleep 2
NO_PROXY=127.0.0.1,localhost ./target/debug/novarocks standalone-server --port 9030 > /tmp/nr-pr3-final.log 2>&1 &
SERVER_PID=$!
until lsof -i :9030 -sTCP:LISTEN 2>/dev/null | grep -q LISTEN; do sleep 1; done

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_schema_evolution_local,iceberg_schema_evolution_nested,iceberg_schema_evolution_array_map_widen,iceberg_schema_evolution_decimal_widen,iceberg_schema_evolution_date_to_timestamp_widen,iceberg_schema_evolution_reorder,iceberg_schema_evolution_nullability,iceberg_schema_evolution_widen_reject,iceberg_table_properties_set_unset,iceberg_table_properties_unset_if_exists,iceberg_table_properties_reject_reserved,iceberg_table_properties_combined_reject \
  --mode verify

kill $SERVER_PID 2>/dev/null
sleep 2
```
Expected: 12/12 PASS (8 PR-1 schema-evolution + 4 new properties suites).

### Task F4: Update `NovaRocks Iceberg v3 完成度清单.md`

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md`

- [ ] **Step 1: Flip §5 item 10**

Find:

```
- [ ] `ALTER TABLE ... SET TBLPROPERTIES`（已有部分 props 支持，需要全量审计） ← phase 2 PR-3
```

Replace with (use actual PR number after creation):

```
- [x] `ALTER TABLE ... SET / UNSET TBLPROPERTIES`（denylist：novarocks.* 全前缀 + format-version + Iceberg 内部 schema/spec/last-* 键；strict UNSET + IF EXISTS；复用 PR-2 commit_with_retry） ← phase 2 PR-3（2026-05-08 · #TBD）
```

- [ ] **Step 2: Add changelog row**

Append:

```
| 2026-05-08 | PR-3（schema-evolution phase 2 §5 之三）#TBD：ALTER TABLE SET / UNSET TBLPROPERTIES SQL；denylist 拒绝 novarocks.* 全前缀 + format-version + identifier/schema/spec/last-* 内部键；strict UNSET 缺键 fail，IF EXISTS 静默跳过；复用 PR-2 commit_with_retry 走重试。新增 28 个单测（parser × 12 + denylist × 11 + helper × 5）+ 4 个 SQL 套件。§5 全部 10 项落地，phase 2 收官。Spec：[[2026-05-06-iceberg-schema-evolution-phase2-design]] §6。 |
```

### Task F5: Push + open PR

- [ ] **Step 1: Push**

```bash
git push -u origin claude/iceberg-set-tblproperties
```

- [ ] **Step 2: Open the PR**

If PR #88 is still OPEN, target this PR's base at `claude/iceberg-ddl-commit-retry` (the PR-2 branch) so GitHub renders only PR-3's diff. After PR-2 merges, GitHub auto-rebases the base to `main`. If PR-2 already merged, target `main` directly.

```bash
# If PR #88 is still OPEN: stack against the PR-2 branch
gh pr create --base claude/iceberg-ddl-commit-retry --head HangyuanLiu:claude/iceberg-set-tblproperties --title "feat(iceberg): schema evolution phase 2 PR-3 (SET / UNSET TBLPROPERTIES)" --body "$(cat <<'EOF'
## Summary

Closes the 10th and final item of §5 of `NovaRocks Iceberg v3 完成度清单`: `ALTER TABLE ... SET / UNSET TBLPROPERTIES`. After this PR merges, §5 schema evolution Phase 2 is fully done.

This PR adds:

- **`ALTER TABLE t SET TBLPROPERTIES ('k' = 'v', ...)`**: insert or overwrite multiple key/value pairs in one statement.
- **`ALTER TABLE t UNSET TBLPROPERTIES ('k', ...)`**: strict-by-default removal; missing key produces a clear error.
- **`ALTER TABLE t UNSET TBLPROPERTIES IF EXISTS ('k', ...)`**: silently skip missing keys.
- **Denylist** (rejected before any commit attempt):
  - `novarocks.*` full prefix (engine-managed property namespace)
  - `format-version` (with forward-pointing message about future `UPGRADE TABLE` syntax)
  - `identifier-field-ids`, `current-schema-id`, `default-spec-id`, `default-sort-order-id`, `last-column-id`, `last-partition-id`, `last-sequence-number` (Iceberg internal counters / identity)

Mixed `SET` and `UNSET` in one statement is grammatically disallowed (Spark / Hive parity). Duplicate keys within a single SET or UNSET are rejected.

The executor reuses PR-2's `commit_with_retry` for conflict tolerance: each retry attempt invalidates the entry cache, reloads the table, re-validates strict UNSET key existence, and rebuilds `Transaction::update_table_properties()` against the latest metadata.

## References

- Spec: `docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md` §6
- Plan: `docs/design/plans/2026-05-08-iceberg-schema-evolution-phase2-pr3.md`
- Closes §5 item 10 in `NovaRocks Iceberg v3 完成度清单.md`
- Stacked on PR #88 (PR-2 commit conflict retry)

## Test plan

- [x] cargo unit tests: 12 parser tests (looks_like / SET / UNSET / IF EXISTS / empty parens / duplicate keys / unquoted)
- [x] cargo unit tests: 11 denylist tests (each reserved category + several allowed user keys)
- [x] cargo unit tests: 5 executor helper tests (denylist collection, strict UNSET validation, IF EXISTS filtering, remove-key computation)
- [x] iceberg SQL suite: 4 new suites (set_unset / unset_if_exists / reject_reserved / combined_reject) plus 8 PR-1 schema-evolution suites = 12/12 PASS
- [x] cargo fmt --check clean
- [x] cargo clippy: no new warnings/errors on PR-3 files
- [x] Full lib test pass (4 pre-existing MinIO mv_refresh failures unchanged)
- [ ] release-build + full SQL suite (deferred to merge gate)

## Out of scope

- `format-version` upgrade via `UPGRADE TABLE` syntax — this PR rejects the key with a forward-pointing message; full upgrade work is §3 of the checklist.
- Cancellation in retry sleep gap — carries forward PR-2's `TODO(cancellation)`.
- Cross-engine fixture (Spark / Trino read NovaRocks-set props) — §17 work.
EOF
)"
```

- [ ] **Step 3: Update checklist with actual PR number**

After PR opens, edit Task F4 Step 1 + Step 2 to replace `#TBD` with the actual PR number from `gh pr create` output.

---

## Self-Review Output

Spec coverage check:

- §6.1 SQL syntax — Phase A (parser tests cover all 3 forms + grammar rejects)
- §6.2 denylist — Phase B (predicate) + Phase C (executor enforces) + SQL suite (each category exercised)
- §6.3 default-allow — Phase B `reserved_key_allows_*` tests
- §6.4 implementation path — Phases A/C/D align (parser → analyzer → executor → engine wiring)
- §6.5 testing — Phase B/C unit tests + Phase E SQL suites

Type / signature consistency: `AlterIcebergPropertiesStmt`, `PropertiesOp`, `parse_alter_iceberg_properties_sql`, `looks_like_alter_iceberg_properties`, `is_reserved_property_key`, `alter_table_properties`, `handle_alter_iceberg_properties` — referenced consistently across phases.

Placeholder check: `#TBD` in checklist updates is intentional, filled in Task F5 Step 3. No other placeholders.

Open caveats embedded in plan (engineer must verify at runtime):

- Task A1: `parse_keyword_if_present_via_peek` is pseudocode; use `peek_word_eq` + `next_token` per existing dialect helper idiom.
- Task A1 `looks_like_alter_iceberg_properties`: the double-parser to handle backtracking after a failed SET match is the simplest correct approach; a single-pass rewrite is welcome if cleaner.
- Task C1: `Transaction::update_table_properties()` API shape (`mut self → Self` vs `&mut self`) — verify against the vendored 0.9.0 source before coding.
- Task C1: `load_table_async` vs `load_table` — mirror exactly what `alter_table_schema` does in the same file (PR-2's retry closure).
- Task C1: `backend_resolver::resolve_iceberg_target_for_alter` is a placeholder name. Use the actual helper used by `alter_table_schema` (likely `target_backend` or similar; grep for the name).
- Task E1: `SHOW CREATE TABLE` may not surface table properties in NovaRocks today. If not, the SQL test must rely on a different verification (e.g. `INSERT/SELECT` continues to work) — document this in the SQL test or fall back to unit-test coverage.
