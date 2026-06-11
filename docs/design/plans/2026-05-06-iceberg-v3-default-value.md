# Iceberg v3 Default Value — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Iceberg v3 `initial-default` (read-side backfill) and `write-default` (INSERT-side fill) end-to-end, accepting `DEFAULT <literal>` in `CREATE TABLE` / `ALTER TABLE ADD COLUMN`, gated to format-version=3.

**Architecture:** A new `DefaultLiteral` AST type captures the parser output and converts to `iceberg::spec::Literal`. The catalog model (`IcebergSchemaFieldDef`, `ColumnDef`) and the thrift descriptor (`TIcebergSchemaField` extended with `initial_default_json`) carry defaults to the read path. The parquet reader's missing-column branch in `align_batch_to_iceberg_schema` consults Arrow Field metadata to fill with the default literal. The INSERT VALUES path replaces NULL fill with `write_default`; the FROM QUERY path adds a column-list-based alignment step before writing data files.

**Tech Stack:** Rust, iceberg-0.9.0 (vendored), Arrow / Parquet, sqlparser-rs, thrift IDL.

**Spec:** [docs/design/specs/2026-05-06-iceberg-v3-default-value-design.md](../specs/2026-05-06-iceberg-v3-default-value-design.md)

---

## File Map

**New files:**
- `src/connector/iceberg/default_value.rs` — `DefaultLiteral` ↔ `iceberg::spec::Literal` ↔ `ast::Literal` mapping; type validation; format-version gate helper
- `tests/sql-tests/cases/iceberg-v3-default/*.sql` — SQL integration test fixtures

**Modified files:**
- `src/sql/parser/ast/mod.rs` — `TableColumnDef.default`, `DefaultLiteral` enum re-export
- `src/sql/parser/dialect/create_table.rs` — capture `DEFAULT literal` instead of `skip_default_value`
- `src/engine/statement.rs` — `IcebergSchemaChange::AddColumn { default: Option<DefaultLiteral> }`, `parse_add_column_change`
- `src/connector/iceberg/catalog/schema_update.rs` — populate `NestedField.with_initial_default/with_write_default`, format-version gate at ALTER
- `src/connector/iceberg/catalog/registry.rs` — populate defaults at CREATE TABLE, format-version gate, populate `ColumnDef.write_default`
- `src/sql/catalog.rs` — extend `ColumnDef` with `write_default`, extend `IcebergSchemaFieldDef` with `initial_default` / `write_default`
- `src/connector/iceberg/catalog/backend.rs` — copy NestedField defaults into `IcebergSchemaFieldDef`
- `idl/thrift/Descriptors.thrift` — add `3: optional string initial_default_json`
- `src/sql/codegen/descriptors.rs` — emit `initial_default_json`
- `src/connector/iceberg/schema.rs` — write Arrow Field metadata key
- `src/formats/parquet/mod.rs` — `build_iceberg_default_array`, `literal_to_constant_array`, replace `new_null_array` calls
- `src/engine/insert.rs` — `reorder_insert_row` consults `write_default`
- `src/engine/iceberg_writer.rs` — `align_chunk_to_target_schema` honors `insert_columns`

---

## Phase 1: DefaultLiteral Foundation

### Task 1: Create `DefaultLiteral` AST type

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`

- [ ] **Step 1: Add the enum**

Insert after the `ColumnAggregation` enum (around line 236):

```rust
/// Literal that may appear in `DEFAULT <literal>` clauses for Iceberg v3
/// columns.  `Null` is the sentinel for `DEFAULT NULL` and is NOT persisted
/// into the Iceberg metadata; it only suppresses duplicate-DEFAULT diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DefaultLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal { unscaled: i128, scale: i8 },
    String(String),
    Date(i32),       // days since 1970-01-01
    DateTime(i64),   // microseconds since 1970-01-01T00:00:00Z
    Binary(Vec<u8>),
}
```

- [ ] **Step 2: Verify compile**

Run: `cargo check`
Expected: clean (the enum is unused so far).

- [ ] **Step 3: Commit**

```bash
git add src/sql/parser/ast/mod.rs
git commit -m "feat(iceberg): add DefaultLiteral AST enum for v3 default values"
```

---

### Task 2: Create `default_value.rs` module with primitive type validation

**Files:**
- Create: `src/connector/iceberg/default_value.rs`
- Modify: `src/connector/iceberg/mod.rs`

- [ ] **Step 1: Write failing tests in the new module**

Create `src/connector/iceberg/default_value.rs`:

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

//! Default value helpers shared by DDL, schema transport, parquet read path,
//! and INSERT write path.

use iceberg::spec::{Literal as IcebergLiteral, PrimitiveLiteral, PrimitiveType, Type};

use crate::sql::parser::ast::{DefaultLiteral, SqlType};

/// Convert an AST `DefaultLiteral` to an `iceberg::spec::Literal` validated
/// against the column's SqlType.  Returns `Ok(None)` for `DefaultLiteral::Null`
/// (which is not persisted) and `Err` when the literal does not fit the
/// column's type or the type itself is unsupported.
pub(crate) fn default_literal_to_iceberg(
    literal: &DefaultLiteral,
    column_type: &SqlType,
) -> Result<Option<IcebergLiteral>, String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(None);
    }
    let prim = match (literal, column_type) {
        (DefaultLiteral::Bool(b), SqlType::Boolean) => PrimitiveLiteral::Boolean(*b),
        (DefaultLiteral::Int(v), SqlType::TinyInt) => {
            i8::try_from(*v).map_err(|_| out_of_range("TINYINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::SmallInt) => {
            i16::try_from(*v).map_err(|_| out_of_range("SMALLINT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::Int) => {
            i32::try_from(*v).map_err(|_| out_of_range("INT", *v))?;
            PrimitiveLiteral::Int(*v as i32)
        }
        (DefaultLiteral::Int(v), SqlType::BigInt) => PrimitiveLiteral::Long(*v),
        (DefaultLiteral::Float(v), SqlType::Float) => {
            PrimitiveLiteral::Float(ordered_float::OrderedFloat(*v as f32))
        }
        (DefaultLiteral::Float(v), SqlType::Double) => {
            PrimitiveLiteral::Double(ordered_float::OrderedFloat(*v))
        }
        (
            DefaultLiteral::Decimal { unscaled, scale },
            SqlType::Decimal { scale: col_scale, .. },
        ) => {
            if *scale != *col_scale {
                return Err(format!(
                    "DEFAULT value scale {scale} does not match column scale {col_scale}"
                ));
            }
            PrimitiveLiteral::Int128(*unscaled)
        }
        (DefaultLiteral::String(s), SqlType::String) => PrimitiveLiteral::String(s.clone()),
        (DefaultLiteral::Date(d), SqlType::Date) => PrimitiveLiteral::Int(*d),
        (DefaultLiteral::DateTime(t), SqlType::DateTime) => PrimitiveLiteral::Long(*t),
        (DefaultLiteral::Binary(b), SqlType::Binary) => PrimitiveLiteral::Binary(b.clone()),
        (lit, ty) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={lit:?} column={ty:?}"
            ));
        }
    };
    Ok(Some(IcebergLiteral::Primitive(prim)))
}

fn out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} is out of range for {type_name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_default_round_trips() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Bool(true), &SqlType::Boolean)
            .expect("bool default")
            .expect("not null");
        assert!(matches!(
            lit,
            IcebergLiteral::Primitive(PrimitiveLiteral::Boolean(true))
        ));
    }

    #[test]
    fn int_overflow_rejected_for_tinyint() {
        let err = default_literal_to_iceberg(&DefaultLiteral::Int(200), &SqlType::TinyInt)
            .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }

    #[test]
    fn decimal_scale_mismatch_rejected() {
        let err = default_literal_to_iceberg(
            &DefaultLiteral::Decimal { unscaled: 1234, scale: 3 },
            &SqlType::Decimal { precision: 10, scale: 2 },
        )
        .expect_err("scale mismatch");
        assert!(err.contains("scale"));
    }

    #[test]
    fn null_returns_none() {
        let lit = default_literal_to_iceberg(&DefaultLiteral::Null, &SqlType::Int)
            .expect("null default");
        assert!(lit.is_none());
    }

    #[test]
    fn type_mismatch_rejected() {
        let err =
            default_literal_to_iceberg(&DefaultLiteral::String("x".into()), &SqlType::Int)
                .expect_err("type mismatch");
        assert!(err.contains("type does not match"));
    }
}
```

Add to `src/connector/iceberg/mod.rs`:

```rust
pub(crate) mod default_value;
```

(Insert next to the other `pub(crate) mod` declarations near the top.)

- [ ] **Step 2: Run tests, expect compile error or unused-import error**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value -- --nocapture`
Expected: tests don't yet exist as a module path — compile error confirms wiring not yet correct.

- [ ] **Step 3: Re-run after fixing module wiring**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value -- --nocapture`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/default_value.rs src/connector/iceberg/mod.rs
git commit -m "feat(iceberg): add DefaultLiteral → iceberg::spec::Literal mapping with type validation"
```

---

### Task 3: Add `iceberg::spec::Literal` → `ast::Literal` reverse conversion

**Files:**
- Modify: `src/connector/iceberg/default_value.rs`

- [ ] **Step 1: Write failing test**

Append to `default_value.rs` test module:

```rust
    #[test]
    fn iceberg_to_ast_literal_int() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::Int(7));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::Int).expect("convert");
        assert_eq!(ast, AstLiteral::Integer(7));
    }

    #[test]
    fn iceberg_to_ast_literal_string() {
        use crate::sql::parser::ast::Literal as AstLiteral;
        let iceberg = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let ast = iceberg_literal_to_ast(&iceberg, &SqlType::String).expect("convert");
        assert_eq!(ast, AstLiteral::String("hi".into()));
    }
```

- [ ] **Step 2: Run, expect failure (function undefined)**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: compile error `cannot find function iceberg_literal_to_ast`.

- [ ] **Step 3: Implement the function**

Add to `src/connector/iceberg/default_value.rs` (above the `tests` module):

```rust
use crate::sql::parser::ast::Literal as AstLiteral;

/// Inverse of `default_literal_to_iceberg` for the INSERT write path.  Used
/// when a write-default needs to be filled in for an omitted column.
pub(crate) fn iceberg_literal_to_ast(
    literal: &IcebergLiteral,
    column_type: &SqlType,
) -> Result<AstLiteral, String> {
    let IcebergLiteral::Primitive(prim) = literal else {
        return Err(format!(
            "unsupported write-default literal kind for column type {column_type:?}: {literal:?}"
        ));
    };
    Ok(match (prim, column_type) {
        (PrimitiveLiteral::Boolean(b), SqlType::Boolean) => AstLiteral::Boolean(*b),
        (PrimitiveLiteral::Int(v), SqlType::TinyInt | SqlType::SmallInt | SqlType::Int)
        | (PrimitiveLiteral::Int(v), SqlType::Date) => AstLiteral::Integer(*v as i64),
        (PrimitiveLiteral::Long(v), SqlType::BigInt | SqlType::DateTime) => {
            AstLiteral::Integer(*v)
        }
        (PrimitiveLiteral::Float(v), SqlType::Float) => AstLiteral::Float(v.0 as f64),
        (PrimitiveLiteral::Double(v), SqlType::Double) => AstLiteral::Float(v.0),
        (PrimitiveLiteral::Int128(v), SqlType::Decimal { scale, .. }) => {
            AstLiteral::Decimal { unscaled: *v, scale: *scale }
        }
        (PrimitiveLiteral::String(s), SqlType::String) => AstLiteral::String(s.clone()),
        (PrimitiveLiteral::Binary(b), SqlType::Binary) => AstLiteral::Binary(b.clone()),
        (prim, ty) => {
            return Err(format!(
                "unsupported write-default literal {prim:?} for column type {ty:?}"
            ));
        }
    })
}
```

If `AstLiteral::Decimal { unscaled, scale }` does not exist in the AST, look at `src/sql/parser/ast/mod.rs` and use whatever the existing literal variant for decimals is. Adjust accordingly.

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/default_value.rs
git commit -m "feat(iceberg): add iceberg::spec::Literal → ast::Literal conversion for INSERT write path"
```

---

### Task 4: Format-version gate helper

**Files:**
- Modify: `src/connector/iceberg/default_value.rs`

- [ ] **Step 1: Add failing test**

Append to test module:

```rust
    #[test]
    fn v2_rejects_non_null_default() {
        let err = require_v3_for_default(
            iceberg::spec::FormatVersion::V2,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect_err("v2 reject");
        assert!(err.contains("format-version 3"));
    }

    #[test]
    fn v3_accepts_non_null_default() {
        require_v3_for_default(
            iceberg::spec::FormatVersion::V3,
            &Some(IcebergLiteral::Primitive(PrimitiveLiteral::Int(5))),
        )
        .expect("v3 accept");
    }

    #[test]
    fn v2_accepts_null_default() {
        require_v3_for_default(iceberg::spec::FormatVersion::V2, &None).expect("v2 + null ok");
    }
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: compile error `cannot find function require_v3_for_default`.

- [ ] **Step 3: Implement**

Add to `default_value.rs`:

```rust
use iceberg::spec::FormatVersion;

/// Reject non-NULL defaults on tables whose format-version is not v3.
/// `None` is the no-default case and is always accepted.
pub(crate) fn require_v3_for_default(
    format_version: FormatVersion,
    default: &Option<IcebergLiteral>,
) -> Result<(), String> {
    if default.is_some() && !matches!(format_version, FormatVersion::V3) {
        return Err(
            "non-NULL DEFAULT requires Iceberg format-version 3; \
             set TBLPROPERTIES('format-version'='3')"
                .to_string(),
        );
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: 10 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/default_value.rs
git commit -m "feat(iceberg): add format-version v3 gate helper for non-NULL DEFAULT"
```

---

## Phase 2: DDL Parser

### Task 5: Migrate `IcebergSchemaChange::AddColumn` signature

**Files:**
- Modify: `src/engine/statement.rs`
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Change the enum variant**

In `src/engine/statement.rs` at the `IcebergSchemaChange::AddColumn` definition (around line 817), replace:

```rust
    AddColumn {
        name: String,
        data_type: SqlType,
        default_null: bool,
    },
```

with:

```rust
    AddColumn {
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,
    },
```

Add `DefaultLiteral` to the imports at the top of `statement.rs`:

```rust
use crate::sql::parser::ast::{
    /* …existing… */ DefaultLiteral,
};
```

- [ ] **Step 2: Update parser to produce `Option<DefaultLiteral>`**

Replace `parse_add_column_change` (around line 1135) with:

```rust
fn parse_add_column_change(parser: &mut Parser<'_>) -> Result<IcebergSchemaChange, String> {
    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    let data_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    let mut default: Option<DefaultLiteral> = None;
    let mut seen_null = false;
    let mut seen_default = false;
    loop {
        if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            return Err(
                "ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string(),
            );
        }
        if parser.parse_keyword(Keyword::NULL) {
            if seen_null {
                return Err("duplicate NULL clause in ADD COLUMN".to_string());
            }
            seen_null = true;
            continue;
        }
        if parser.parse_keyword(Keyword::DEFAULT) {
            if seen_default {
                return Err("duplicate DEFAULT clause in ADD COLUMN".to_string());
            }
            seen_default = true;
            // DEFAULT NULL keeps existing v2 behavior (does not persist).
            if parser.parse_keyword(Keyword::NULL) {
                default = Some(DefaultLiteral::Null);
                continue;
            }
            default = Some(crate::sql::parser::dialect::create_table::parse_default_literal(
                parser, &data_type,
            )?);
            continue;
        }
        break;
    }
    Ok(IcebergSchemaChange::AddColumn {
        name,
        data_type,
        default,
    })
}
```

`parse_default_literal` is a public function that does not yet exist — it will be added in Task 6.

- [ ] **Step 3: Update schema_update.rs callers**

In `src/connector/iceberg/catalog/schema_update.rs`, every `IcebergSchemaChange::AddColumn { name, data_type, default_null }` pattern needs to be updated to `default`. There are matches around lines 421, 498, 847.

For the production matches at lines 421 and 847, change `default_null` to `default` (the field is currently unused anyway).

For test sites at lines 46, 102, 131, 247, 262, change `default_null: true` → `default: Some(DefaultLiteral::Null)` and `default_null: false` → `default: None`.

Add the import at the top of the test module:

```rust
use crate::sql::parser::ast::DefaultLiteral;
```

- [ ] **Step 4: Update existing parser tests in statement.rs**

In `src/engine/statement.rs` test module (around lines 1497–1620), update all `IcebergSchemaChange::AddColumn { default_null: true, .. }` patterns to `default: Some(super::DefaultLiteral::Null)` and `default_null: false` to `default: None`.

The existing test `parse_alter_iceberg_schema_add_column_default_null` (line 1497) should still pass: `DEFAULT NULL` → `Some(DefaultLiteral::Null)`.

The existing test that expects "default values other than NULL are not supported" (around line 1559) needs to be removed or repurposed — it will be replaced by Task 6's positive tests for non-NULL DEFAULT.

- [ ] **Step 5: Run, expect failure (parse_default_literal not found)**

Run: `cargo build`
Expected: error `cannot find function parse_default_literal`. This is intentional — Task 6 implements it.

- [ ] **Step 6: Stub `parse_default_literal` so compile passes**

Temporarily, in `src/sql/parser/dialect/create_table.rs`, add:

```rust
pub(crate) fn parse_default_literal(
    _parser: &mut sqlparser::parser::Parser<'_>,
    _data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    Err("non-NULL DEFAULT not yet implemented".to_string())
}
```

- [ ] **Step 7: Build + run schema_update tests**

Run: `cargo build && cargo test -p novarocks --lib connector::iceberg::catalog::schema_update`
Expected: build passes, schema_update tests pass with the migrated patterns.

- [ ] **Step 8: Commit**

```bash
git add src/engine/statement.rs src/connector/iceberg/catalog/schema_update.rs src/sql/parser/dialect/create_table.rs
git commit -m "refactor(iceberg): migrate IcebergSchemaChange::AddColumn to Option<DefaultLiteral>"
```

---

### Task 6: Implement `parse_default_literal`

**Files:**
- Modify: `src/sql/parser/dialect/create_table.rs`

- [ ] **Step 1: Add failing test**

Inside `src/engine/statement.rs` test module, add:

```rust
    #[test]
    fn parse_alter_iceberg_schema_add_column_int_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c INT DEFAULT 5",
        )
        .expect("parsed");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::Int(5)));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_string_default() {
        let stmt = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c STRING DEFAULT 'hi'",
        )
        .expect("parsed");
        match stmt.change {
            super::IcebergSchemaChange::AddColumn { default, .. } => {
                assert_eq!(default, Some(super::DefaultLiteral::String("hi".into())));
            }
            _ => panic!("expected AddColumn"),
        }
    }

    #[test]
    fn parse_alter_iceberg_schema_add_column_default_overflow_rejected() {
        let err = super::parse_alter_iceberg_schema_sql(
            "ALTER TABLE ice.ns.orders ADD COLUMN c TINYINT DEFAULT 200",
        )
        .expect_err("overflow");
        assert!(err.contains("TINYINT"));
    }
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib engine::statement::tests::parse_alter_iceberg_schema_add_column_int_default`
Expected: panic `non-NULL DEFAULT not yet implemented`.

- [ ] **Step 3: Implement `parse_default_literal`**

In `src/sql/parser/dialect/create_table.rs`, replace the stub from Task 5 with:

```rust
pub(crate) fn parse_default_literal(
    parser: &mut sqlparser::parser::Parser<'_>,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    use sqlparser::ast::Value;

    let token = parser.next_token();
    let lit = match token.token {
        sqlparser::tokenizer::Token::Word(w)
            if w.value.eq_ignore_ascii_case("TRUE") =>
        {
            DefaultLiteral::Bool(true)
        }
        sqlparser::tokenizer::Token::Word(w)
            if w.value.eq_ignore_ascii_case("FALSE") =>
        {
            DefaultLiteral::Bool(false)
        }
        sqlparser::tokenizer::Token::Number(n, _) => parse_numeric_default(&n, data_type)?,
        sqlparser::tokenizer::Token::SingleQuotedString(s)
        | sqlparser::tokenizer::Token::DoubleQuotedString(s) => {
            parse_string_default(&s, data_type)?
        }
        sqlparser::tokenizer::Token::HexStringLiteral(s) => {
            let bytes = hex::decode(&s).map_err(|e| format!("invalid hex DEFAULT: {e}"))?;
            DefaultLiteral::Binary(bytes)
        }
        sqlparser::tokenizer::Token::Minus => {
            // Negative numeric literal
            let next = parser.next_token();
            if let sqlparser::tokenizer::Token::Number(n, _) = next.token {
                let mut signed = String::from('-');
                signed.push_str(&n);
                parse_numeric_default(&signed, data_type)?
            } else {
                return Err(format!("expected number after `-` in DEFAULT, got {next:?}"));
            }
        }
        other => {
            return Err(format!(
                "unsupported DEFAULT value token: {other:?}"
            ));
        }
    };

    // Validate against the column type up front so the parser fails fast.
    crate::connector::iceberg::default_value::default_literal_to_iceberg(&lit, data_type)?;

    Ok(lit)
}

fn parse_numeric_default(
    text: &str,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int | SqlType::BigInt => {
            let v: i64 = text
                .parse()
                .map_err(|e| format!("invalid integer DEFAULT `{text}`: {e}"))?;
            Ok(DefaultLiteral::Int(v))
        }
        SqlType::Float | SqlType::Double => {
            let v: f64 = text
                .parse()
                .map_err(|e| format!("invalid float DEFAULT `{text}`: {e}"))?;
            Ok(DefaultLiteral::Float(v))
        }
        SqlType::Decimal { scale, .. } => {
            let (unscaled, scanned_scale) = decimal_from_str(text)?;
            if scanned_scale != *scale {
                return Err(format!(
                    "DEFAULT value scale {scanned_scale} does not match column scale {scale}"
                ));
            }
            Ok(DefaultLiteral::Decimal {
                unscaled,
                scale: *scale,
            })
        }
        other => Err(format!(
            "numeric DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn parse_string_default(
    s: &str,
    data_type: &crate::sql::parser::ast::SqlType,
) -> Result<crate::sql::parser::ast::DefaultLiteral, String> {
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    match data_type {
        SqlType::String => Ok(DefaultLiteral::String(s.to_string())),
        SqlType::Date => {
            let days = crate::engine::parquet::parse_date_string_to_days(s)?;
            Ok(DefaultLiteral::Date(days))
        }
        SqlType::DateTime => {
            let micros = crate::engine::parquet::parse_datetime_string_to_micros(s)?;
            Ok(DefaultLiteral::DateTime(micros))
        }
        other => Err(format!(
            "string DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn decimal_from_str(text: &str) -> Result<(i128, i8), String> {
    let trimmed = text.trim();
    let (sign, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (-1i128, rest)
    } else {
        (1, trimmed)
    };
    let (whole, frac) = match body.split_once('.') {
        Some((w, f)) => (w, f),
        None => (body, ""),
    };
    let combined: String = whole.chars().chain(frac.chars()).collect();
    let unscaled: i128 = combined
        .parse()
        .map_err(|e| format!("invalid decimal DEFAULT `{text}`: {e}"))?;
    let scale = i8::try_from(frac.len()).map_err(|_| "decimal scale too large".to_string())?;
    Ok((sign * unscaled, scale))
}
```

If `hex` crate is not in Cargo.toml, replace `hex::decode` with the project's existing helper for hex decoding (search `0x` or `hex_decode` first; if neither exists, write a tiny helper inline).

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib engine::statement`
Expected: 3 new tests pass; all existing parser tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/parser/dialect/create_table.rs src/engine/statement.rs
git commit -m "feat(iceberg): parse non-NULL DEFAULT literal in ALTER ADD COLUMN"
```

---

### Task 7: Add DEFAULT capture to `TableColumnDef` and CREATE TABLE parser

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Modify: `src/sql/parser/dialect/create_table.rs`

- [ ] **Step 1: Add failing test**

In `src/sql/parser/dialect/create_table.rs` test module, add:

```rust
    #[test]
    fn parse_create_table_captures_int_default() {
        let sql = r#"
            CREATE TABLE ice.ns.t (a INT, b INT DEFAULT 5)
            PROPERTIES ('format-version' = '3')
        "#;
        let stmt = parse_create_table_statement(sql).expect("parsed");
        let CreateTableKind::Iceberg { columns, .. } = stmt.kind else {
            panic!("expected iceberg create table");
        };
        assert_eq!(
            columns[1].default,
            Some(crate::sql::parser::ast::DefaultLiteral::Int(5))
        );
    }
```

- [ ] **Step 2: Run, expect failure (compile error: no field `default`)**

Run: `cargo build`
Expected: compile error `no field `default` on `TableColumnDef``.

- [ ] **Step 3: Add `default` field to `TableColumnDef`**

In `src/sql/parser/ast/mod.rs` at line 209:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableColumnDef {
    pub name: String,
    pub data_type: SqlType,
    pub nullable: bool,
    pub aggregation: Option<ColumnAggregation>,
    pub default: Option<DefaultLiteral>,
}
```

The `Eq` derive will fail because `DefaultLiteral::Float(f64)` and `Decimal` make Eq impossible. Drop `Eq` from the derive and rely only on `PartialEq`.

Cascade-update places that constructed `TableColumnDef` (search for `TableColumnDef {` occurrences, e.g. `src/sql/parser/dialect/create_table.rs:452`) — every constructor needs the new `default: None` field for now.

- [ ] **Step 4: Wire DEFAULT into `parse_column_definitions`**

Replace lines 432–434 of `parse_column_definitions` (the `skip_default_value` branch):

```rust
            } else if parser.parse_keyword(Keyword::DEFAULT) {
                // Skip the default value expression
                skip_default_value(parser);
            }
```

with:

```rust
            } else if parser.parse_keyword(Keyword::DEFAULT) {
                if default.is_some() {
                    return Err(format!(
                        "duplicate DEFAULT clause for column `{col_name}`"
                    ));
                }
                if parser.parse_keyword(Keyword::NULL) {
                    default = Some(DefaultLiteral::Null);
                } else {
                    default = Some(parse_default_literal(parser, &sql_type)?);
                }
            }
```

Above the inner `loop` add:

```rust
        let mut default: Option<DefaultLiteral> = None;
```

And in the constructor at line 452 add `default,`.

Add `use crate::sql::parser::ast::DefaultLiteral;` to imports at the top of the file.

`skip_default_value` becomes dead code only for the **iceberg** path. The function is still used for the `AS` (generated column) branch on line 446 — leave it there. For non-iceberg local parquet tables, the same parser is invoked; the new `parse_default_literal` does the validation against `sql_type` which works the same. There is no longer a non-iceberg-specific DEFAULT skip path; non-iceberg targets simply ignore `default`.

- [ ] **Step 5: Run test**

Run: `cargo test -p novarocks --lib parser::dialect::create_table::tests::parse_create_table_captures_int_default`
Expected: pass.

- [ ] **Step 6: Run full test suite for regressions**

Run: `cargo test -p novarocks --lib parser::`
Expected: all parser tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/create_table.rs
git commit -m "feat(iceberg): capture DEFAULT literal in CREATE TABLE column definitions"
```

---

## Phase 3: DDL Writer

### Task 8: Apply default + format-version gate at ALTER ADD COLUMN

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Add failing test**

In `schema_update.rs` test module, add:

```rust
    #[test]
    fn add_column_with_int_default_v3_sets_initial_and_write_default() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Int(5)),
            },
        )
        .expect("v3 add column");
        let field = updated.field_by_name("c").expect("new field");
        let expected = iceberg::spec::Literal::Primitive(
            iceberg::spec::PrimitiveLiteral::Int(5),
        );
        assert_eq!(field.initial_default.as_ref(), Some(&expected));
        assert_eq!(field.write_default.as_ref(), Some(&expected));
    }

    #[test]
    fn add_column_with_default_null_does_not_persist_metadata() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
            },
        )
        .expect("default null");
        let field = updated.field_by_name("c").expect("new field");
        assert!(field.initial_default.is_none());
        assert!(field.write_default.is_none());
    }
```

- [ ] **Step 2: Run, expect failure (initial_default not set)**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::schema_update::tests::add_column_with_int_default_v3_sets_initial_and_write_default`
Expected: assertion fails — `initial_default` is `None`.

- [ ] **Step 3: Wire default into `build_updated_schema`**

In `src/connector/iceberg/catalog/schema_update.rs` `build_updated_schema` `AddColumn` arm (around line 421), replace:

```rust
        IcebergSchemaChange::AddColumn {
            name, data_type, ..
        } => {
            // existing fresh-id logic
            // existing iceberg_type_for_sql_type
            fields.push(NestedField::optional(id, name, ty));
        }
```

with:

```rust
        IcebergSchemaChange::AddColumn {
            name,
            data_type,
            default,
        } => {
            reject_name_conflict(&fields, name)?;
            let mut next_nested_id = last_column_id
                .checked_add(2)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            let ty = crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
                data_type,
                &mut next_nested_id,
            )?;
            let id = last_column_id
                .checked_add(1)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            let mut field = NestedField::optional(id, name, ty);
            if let Some(default_literal) = default {
                if let Some(iceberg_lit) =
                    crate::connector::iceberg::default_value::default_literal_to_iceberg(
                        default_literal,
                        data_type,
                    )?
                {
                    field = field
                        .with_initial_default(iceberg_lit.clone())
                        .with_write_default(iceberg_lit);
                }
            }
            fields.push(field);
        }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::schema_update`
Expected: 2 new tests pass, all existing tests still pass.

- [ ] **Step 5: Wire format-version gate at ALTER**

In `alter_table_schema` (around line 986) after loading the table metadata and before calling the schema-update transaction, add:

```rust
    let format_version = table.metadata().format_version();
    if let IcebergSchemaChange::AddColumn { default: Some(literal), data_type, .. } =
        &stmt.change
    {
        let iceberg_lit = crate::connector::iceberg::default_value::default_literal_to_iceberg(
            literal, data_type,
        )?;
        crate::connector::iceberg::default_value::require_v3_for_default(
            format_version,
            &iceberg_lit,
        )?;
    }
```

(Place this where `alter_table_schema` already has access to the loaded `table` variable. If `alter_table_schema` does not load the table itself, look for the call site that does and apply the gate there. The expected pattern is: load table → format-version check → enqueue schema-update action.)

- [ ] **Step 6: Add an integration-style test for the v2 gate**

Add to schema_update test module:

```rust
    #[test]
    fn add_column_default_metadata_construction_independent_of_v2_v3() {
        // build_updated_schema does not see format-version; the gate lives in
        // alter_table_schema. Document this by asserting build_updated_schema
        // succeeds even without v3 — the gate must be applied at the
        // alter_table_schema call site, not here.
        let _ = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Int(5)),
            },
        )
        .expect("schema build succeeds; gate enforced upstream");
    }
```

- [ ] **Step 7: Run full crate tests**

Run: `cargo test -p novarocks --lib connector::iceberg`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): persist initial/write default for v3 ALTER ADD COLUMN"
```

---

### Task 9: Apply default + format-version gate at CREATE TABLE

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 1: Add failing test**

In `registry.rs` test module (around line 2200), add:

```rust
    #[test]
    fn create_v2_table_with_non_null_default_rejected() {
        let columns = vec![crate::sql::parser::ast::TableColumnDef {
            name: "c".to_string(),
            data_type: crate::sql::parser::ast::SqlType::Int,
            nullable: true,
            aggregation: None,
            default: Some(crate::sql::parser::ast::DefaultLiteral::Int(5)),
        }];
        let err = build_iceberg_schema_for_test(&columns, FormatVersion::V2)
            .expect_err("v2 + default rejected");
        assert!(err.contains("format-version 3"));
    }

    #[test]
    fn create_v3_table_with_int_default_persists_literal() {
        let columns = vec![crate::sql::parser::ast::TableColumnDef {
            name: "c".to_string(),
            data_type: crate::sql::parser::ast::SqlType::Int,
            nullable: true,
            aggregation: None,
            default: Some(crate::sql::parser::ast::DefaultLiteral::Int(5)),
        }];
        let schema = build_iceberg_schema_for_test(&columns, FormatVersion::V3)
            .expect("v3 + default ok");
        let field = schema.field_by_name("c").expect("c");
        let expected = iceberg::spec::Literal::Primitive(
            iceberg::spec::PrimitiveLiteral::Int(5),
        );
        assert_eq!(field.initial_default.as_ref(), Some(&expected));
        assert_eq!(field.write_default.as_ref(), Some(&expected));
    }
```

- [ ] **Step 2: Run, expect failure (build_iceberg_schema_for_test undefined)**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::registry::tests::create_v3_table_with_int_default_persists_literal`
Expected: compile error.

- [ ] **Step 3: Refactor `build_iceberg_schema` to take format-version + expose test helper**

Find `build_iceberg_schema` (line 1217). Change its signature:

```rust
fn build_iceberg_schema(
    columns: &[TableColumnDef],
    format_version: FormatVersion,
) -> Result<Schema, String> {
    // …existing field construction, but for each column:
    //   if let Some(default_literal) = &column.default {
    //       if let Some(iceberg_lit) = default_literal_to_iceberg(default_literal, &column.data_type)? {
    //           require_v3_for_default(format_version, &Some(iceberg_lit.clone()))?;
    //           field = field.with_initial_default(iceberg_lit.clone()).with_write_default(iceberg_lit);
    //       }
    //   }
    // …
}
```

Update the single call site of `build_iceberg_schema` inside `create_table` (line 366) to pass `format_version` (already extracted on line 371).

Add a test helper:

```rust
#[cfg(test)]
pub(crate) fn build_iceberg_schema_for_test(
    columns: &[TableColumnDef],
    format_version: FormatVersion,
) -> Result<Schema, String> {
    build_iceberg_schema(columns, format_version)
}
```

Reorder `extract_table_format_version_property` so it executes before `build_iceberg_schema` in `create_table`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib connector::iceberg::catalog::registry`
Expected: 2 new tests pass; all existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): persist v3 default literal in CREATE TABLE; reject on v1/v2"
```

---

## Phase 4: Schema Transport

### Task 10: Extend `IcebergSchemaFieldDef` and populate from iceberg-rust

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`

- [ ] **Step 1: Add fields to `IcebergSchemaFieldDef`**

In `src/sql/catalog.rs` line 70:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct IcebergSchemaFieldDef {
    pub field_id: i32,
    pub name: String,
    pub initial_default: Option<iceberg::spec::Literal>,
    pub write_default: Option<iceberg::spec::Literal>,
    pub children: Vec<IcebergSchemaFieldDef>,
}
```

Drop `Eq` from the derive — `iceberg::spec::Literal` does not implement `Eq` for floats.

Update test fixtures in this file (line 191) to include `initial_default: None, write_default: None`.

- [ ] **Step 2: Populate from iceberg-rust in `iceberg_field_def`**

In `src/connector/iceberg/catalog/backend.rs` line 272:

```rust
fn iceberg_field_def(field: &iceberg::spec::NestedField) -> IcebergSchemaFieldDef {
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}
```

- [ ] **Step 3: Build, fix any cascading compile errors**

Run: `cargo build`
Expected: compile errors at every `IcebergSchemaFieldDef { field_id, name, children }` constructor (including codegen and tests). Fix each by adding `initial_default: None, write_default: None`. Use `cargo build` iteratively to find them.

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/catalog.rs src/connector/iceberg/catalog/backend.rs src/sql/codegen/descriptors.rs
git commit -m "feat(iceberg): carry initial/write default through IcebergSchemaFieldDef"
```

---

### Task 11: Extend `ColumnDef` with `write_default`

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs`

- [ ] **Step 1: Add field**

In `src/sql/catalog.rs` line 8:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub write_default: Option<iceberg::spec::Literal>,
}
```

Drop `Eq` from the derive (same reason as Task 10).

- [ ] **Step 2: Populate from NestedField in `load_table`**

In `src/connector/iceberg/catalog/registry.rs` around line 588 (the `arrow_schema.fields().iter().map(...)` block that constructs `ColumnDef`), update to:

```rust
    let iceberg_schema = table.metadata().current_schema();
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = normalize_identifier(field.name()).map_err(|e| {
                format!(
                    "normalize iceberg column name `{}` failed: {e}",
                    field.name()
                )
            })?;
            let nested = iceberg_schema
                .field_by_name(field.name())
                .ok_or_else(|| {
                    format!("iceberg column `{}` missing from schema", field.name())
                })?;
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type: apply_logical_type_override(
                    field.data_type(),
                    logical_types.get(&field_name),
                ),
                nullable: field.is_nullable(),
                write_default: nested.write_default.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
```

- [ ] **Step 3: Add `write_default: None` everywhere else `ColumnDef {…}` is constructed**

Use `cargo build` to find all sites; add `write_default: None`. Notable sites:
- `src/connector/iceberg/catalog/backend.rs` (row-lineage metadata columns at lines 204–224)
- Any local-parquet ColumnDef constructions
- Test fixtures in `src/sql/catalog.rs:182`

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/catalog.rs src/connector/iceberg/catalog/registry.rs src/connector/iceberg/catalog/backend.rs
git commit -m "feat(iceberg): expose write_default on ColumnDef for INSERT path"
```

---

### Task 12: Extend `TIcebergSchemaField` thrift with `initial_default_json`

**Files:**
- Modify: `idl/thrift/Descriptors.thrift`
- Modify: `src/sql/codegen/descriptors.rs`

- [ ] **Step 1: Edit thrift IDL**

In `idl/thrift/Descriptors.thrift` line 557:

```thrift
struct TIcebergSchemaField {
    // Refer to field id in iceberg schema
    1: optional i32 field_id

    // Refer to field name
    2: optional string name

    // Iceberg v3 initial-default for this field, serialized to spec JSON.
    // Used by readers to fill missing columns instead of NULL.  Optional;
    // absence indicates no default (preserves pre-v3 behavior).
    3: optional string initial_default_json

    // Children fields for struct, map and list(array)
    100: optional list<TIcebergSchemaField> children
}
```

- [ ] **Step 2: Re-run thrift codegen if applicable**

If the project regenerates the thrift descriptors at build time, run a clean build to refresh:

```bash
cargo clean -p novarocks-codegen 2>/dev/null || true
cargo build
```

Confirm the generated descriptor (search for `TIcebergSchemaField` in `target/`) now has the new field.

- [ ] **Step 3: Add failing test in codegen**

In `src/sql/codegen/descriptors.rs` test module (search for the existing `descriptor_builder_emits_iceberg_schema_field_ids` test), add:

```rust
    #[test]
    fn descriptor_builder_emits_iceberg_initial_default_json() {
        use crate::sql::catalog::IcebergSchemaFieldDef;
        let field = IcebergSchemaFieldDef {
            field_id: 1,
            name: "c".to_string(),
            initial_default: Some(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(5),
            )),
            write_default: None,
            children: vec![],
        };
        let thrift = to_thrift_iceberg_schema_field(&field);
        assert_eq!(thrift.initial_default_json.as_deref(), Some("5"));
    }
```

- [ ] **Step 4: Run, expect failure**

Run: `cargo test -p novarocks --lib sql::codegen::descriptors`
Expected: compile error or assertion failure.

- [ ] **Step 5: Implement codegen serialization**

In `src/sql/codegen/descriptors.rs`, modify `to_thrift_iceberg_schema_field` (line 229):

```rust
fn to_thrift_iceberg_schema_field(
    field: &IcebergSchemaFieldDef,
) -> descriptors::TIcebergSchemaField {
    let mut thrift = descriptors::TIcebergSchemaField::new(
        Some(field.field_id),
        Some(field.name.clone()),
        (!field.children.is_empty()).then(|| {
            field
                .children
                .iter()
                .map(to_thrift_iceberg_schema_field)
                .map(Box::new)
                .collect::<Vec<_>>()
        }),
    );
    thrift.initial_default_json = field
        .initial_default
        .as_ref()
        .map(|lit| serialize_iceberg_literal_json(lit));
    thrift
}

fn serialize_iceberg_literal_json(literal: &iceberg::spec::Literal) -> String {
    // Iceberg-rust does not expose a public Literal-to-JSON-string helper; use
    // the existing SerdeNestedField round-trip pattern for stability.
    match literal {
        iceberg::spec::Literal::Primitive(prim) => match prim {
            iceberg::spec::PrimitiveLiteral::Boolean(b) => b.to_string(),
            iceberg::spec::PrimitiveLiteral::Int(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::Long(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::Float(v) => v.0.to_string(),
            iceberg::spec::PrimitiveLiteral::Double(v) => v.0.to_string(),
            iceberg::spec::PrimitiveLiteral::Int128(v) => v.to_string(),
            iceberg::spec::PrimitiveLiteral::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
            iceberg::spec::PrimitiveLiteral::Binary(b) => {
                format!("\"{}\"", b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>())
            }
            other => panic!("unsupported primitive literal for thrift emission: {other:?}"),
        },
        other => panic!("unsupported literal kind for thrift emission: {other:?}"),
    }
}
```

If the iceberg-rust crate exposes a `try_into_json` or similar serializer, prefer that instead of hand-rolled formatting. Search vendor/iceberg-0.9.0 for `try_into_json` first; the spec mentions it (datatypes.rs:591).

The use of `panic!` for unsupported variants is acceptable here because Task 5/6's parser-side validation already enforces that only the supported primitive types reach this code; defense in depth is welcome via `unreachable!` if the engineer prefers.

- [ ] **Step 6: Run test**

Run: `cargo test -p novarocks --lib sql::codegen::descriptors::tests::descriptor_builder_emits_iceberg_initial_default_json`
Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add idl/thrift/Descriptors.thrift src/sql/codegen/descriptors.rs
git commit -m "feat(iceberg): emit initial_default_json on TIcebergSchemaField"
```

---

### Task 13: Carry initial-default through Arrow Field metadata

**Files:**
- Modify: `src/connector/iceberg/schema.rs`

- [ ] **Step 1: Add failing test**

In `src/connector/iceberg/schema.rs` test module (create one if missing), add:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn apply_field_id_recursive_writes_initial_default_metadata() {
        let mut schema_field = crate::descriptors::TIcebergSchemaField::default();
        schema_field.field_id = Some(1);
        schema_field.name = Some("c".into());
        schema_field.initial_default_json = Some("5".to_string());

        let field = Field::new("c", DataType::Int32, true);
        let updated = apply_field_id_recursive(field, &schema_field).expect("apply");
        assert_eq!(
            updated.metadata().get(ICEBERG_INITIAL_DEFAULT_META_KEY),
            Some(&"5".to_string())
        );
    }
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::schema`
Expected: compile error `ICEBERG_INITIAL_DEFAULT_META_KEY` not defined.

- [ ] **Step 3: Add the constant + write into metadata**

In `src/connector/iceberg/schema.rs`, near the existing `VIRTUAL_COUNT_COLUMN` constant (line 26):

```rust
pub const ICEBERG_INITIAL_DEFAULT_META_KEY: &str = "novarocks.iceberg.initial_default";
```

In `apply_field_id_recursive` (line 121), after the `meta.insert(PARQUET_FIELD_ID_META_KEY...)` line, add:

```rust
    if let Some(json) = schema_field.initial_default_json.as_ref() {
        meta.insert(
            ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
            json.clone(),
        );
    }
```

- [ ] **Step 4: Run test**

Run: `cargo test -p novarocks --lib connector::iceberg::schema`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/schema.rs
git commit -m "feat(iceberg): carry initial-default JSON through Arrow Field metadata"
```

---

## Phase 5: Read Path (A)

### Task 14: Implement `literal_to_constant_array`

**Files:**
- Modify: `src/connector/iceberg/default_value.rs`

- [ ] **Step 1: Add failing tests**

Append to test module:

```rust
    use arrow::array::{
        Array, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;

    #[test]
    fn literal_to_constant_array_int32() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 3).expect("array");
        let i32arr = arr.as_any().downcast_ref::<Int32Array>().expect("i32");
        assert_eq!(i32arr.len(), 3);
        assert_eq!(i32arr.value(0), 5);
        assert_eq!(i32arr.value(2), 5);
    }

    #[test]
    fn literal_to_constant_array_string() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::String("hi".into()));
        let arr = literal_to_constant_array(&lit, &DataType::Utf8, 2).expect("array");
        let strarr = arr.as_any().downcast_ref::<StringArray>().expect("str");
        assert_eq!(strarr.value(0), "hi");
        assert_eq!(strarr.value(1), "hi");
    }

    #[test]
    fn literal_to_constant_array_zero_rows() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::Int(5));
        let arr = literal_to_constant_array(&lit, &DataType::Int32, 0).expect("array");
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn literal_to_constant_array_unsupported_type_fails_fast() {
        let lit = IcebergLiteral::Primitive(PrimitiveLiteral::UInt(5));
        let err =
            literal_to_constant_array(&lit, &DataType::UInt32, 1).expect_err("unsupported");
        assert!(err.contains("unsupported"));
    }
```

(`PrimitiveLiteral::UInt` may not exist; substitute any iceberg `PrimitiveLiteral` variant we do not support, e.g. `Fixed` or `UUID`. Confirm by reading vendor/iceberg-0.9.0/src/spec/values.rs.)

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: compile error.

- [ ] **Step 3: Implement**

Append to `src/connector/iceberg/default_value.rs`:

```rust
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;

/// Build an Arrow constant array of length `row_count` whose every element is
/// the value encoded by `literal`.  The literal's runtime type must agree with
/// `target_type`; mismatches fail fast.
pub(crate) fn literal_to_constant_array(
    literal: &IcebergLiteral,
    target_type: &DataType,
    row_count: usize,
) -> Result<ArrayRef, String> {
    let IcebergLiteral::Primitive(prim) = literal else {
        return Err(format!(
            "unsupported initial-default literal kind: {literal:?}"
        ));
    };
    Ok(match (prim, target_type) {
        (PrimitiveLiteral::Boolean(v), DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Float(v), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Double(v), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![v.0; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int128(v), DataType::Decimal128(precision, scale)) => Arc::new(
            Decimal128Array::from(vec![*v; row_count])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| format!("decimal default cast: {e}"))?,
        ) as ArrayRef,
        (PrimitiveLiteral::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Int(v), DataType::Date32) => {
            Arc::new(Date32Array::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Long(v), DataType::Timestamp(_, _)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v; row_count])) as ArrayRef
        }
        (PrimitiveLiteral::Binary(b), DataType::Binary) => {
            let slice = b.as_slice();
            Arc::new(BinaryArray::from(vec![slice; row_count])) as ArrayRef
        }
        (prim, ty) => {
            return Err(format!(
                "unsupported initial-default literal {prim:?} for arrow type {ty:?}"
            ));
        }
    })
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib connector::iceberg::default_value`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/default_value.rs
git commit -m "feat(iceberg): build_iceberg_default_array helper for missing-column backfill"
```

---

### Task 15: Replace `new_null_array` calls with default-aware helper

**Files:**
- Modify: `src/formats/parquet/mod.rs`

- [ ] **Step 1: Add failing test**

In `src/formats/parquet/mod.rs` test module (the existing iceberg evolution tests around line 2236), add:

```rust
    #[test]
    fn iceberg_schema_evolution_fills_missing_column_with_initial_default() {
        // Build a parquet file with only column `a`, then read with an output
        // schema that includes `b` carrying ICEBERG_INITIAL_DEFAULT_META_KEY=5.
        // Expect b column to be filled with 5 instead of NULL.
        use arrow::array::{Int32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::io::Cursor;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(
                std::iter::once((
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                ))
                .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20])) as _],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(Cursor::new(&mut buf), schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        // Output schema includes `b` with initial-default JSON metadata.
        let mut b_meta = std::collections::HashMap::new();
        b_meta.insert(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        );
        b_meta.insert(
            crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
            "99".to_string(),
        );
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(
                std::iter::once((
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                ))
                .collect(),
            ),
            Field::new("b", DataType::Int32, true).with_metadata(b_meta),
        ]));

        let chunks = read_iceberg_parquet_for_test(buf, &out_schema);
        let merged = chunks.first().expect("chunk");
        let b = merged
            .column_by_name("b")
            .expect("b column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(b.value(0), 99);
        assert_eq!(b.value(1), 99);
    }
```

You may need to discover or write a small helper `read_iceberg_parquet_for_test(bytes, output_schema)` that constructs a `ParquetScanConfig` with `iceberg_output_schema = Some(out_schema)` and runs the reader. Search for an existing helper similar to `iceberg_schema_evolution_reads_renamed_columns_by_field_id` (line 2236) — pattern after it.

- [ ] **Step 2: Run, expect failure (b filled with NULL)**

Run: `cargo test -p novarocks --lib formats::parquet::tests::iceberg_schema_evolution_fills_missing_column_with_initial_default`
Expected: assertion fails — `b.value(0)` is 0 (NULL placeholder).

- [ ] **Step 3: Implement `build_iceberg_default_array` and replace fills**

Add to `src/formats/parquet/mod.rs`:

```rust
fn build_iceberg_default_array(
    target_field: &arrow::datatypes::Field,
    row_count: usize,
) -> Result<ArrayRef, String> {
    use crate::connector::iceberg::default_value::literal_to_constant_array;
    use crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY;
    use iceberg::spec::Literal;

    let Some(json) = target_field
        .metadata()
        .get(ICEBERG_INITIAL_DEFAULT_META_KEY)
    else {
        return Ok(new_null_array(target_field.data_type(), row_count));
    };
    let literal = Literal::try_from_json(
        serde_json::from_str(json).map_err(|e| {
            format!(
                "corrupted initial-default JSON for column {}: {e}",
                target_field.name()
            )
        })?,
        // The JSON itself is not type-tagged in the spec; we pass the iceberg
        // Type derived from the Arrow data type.  When the Arrow type cannot
        // be unambiguously mapped to an iceberg Type, fail fast.
        &arrow_type_to_iceberg_type(target_field.data_type()).map_err(|e| {
            format!(
                "unsupported initial-default for column {}: {e}",
                target_field.name()
            )
        })?,
    )
    .map_err(|e| {
        format!(
            "decode initial-default for column {}: {e}",
            target_field.name()
        )
    })?
    .ok_or_else(|| {
        format!(
            "initial-default JSON for column {} produced no literal",
            target_field.name()
        )
    })?;

    literal_to_constant_array(&literal, target_field.data_type(), row_count)
}

fn arrow_type_to_iceberg_type(
    dt: &arrow::datatypes::DataType,
) -> Result<iceberg::spec::Type, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    use iceberg::spec::{PrimitiveType, Type};
    Ok(match dt {
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        DataType::Utf8 => Type::Primitive(PrimitiveType::String),
        DataType::Date32 => Type::Primitive(PrimitiveType::Date),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Type::Primitive(PrimitiveType::Timestamp)
        }
        DataType::Binary => Type::Primitive(PrimitiveType::Binary),
        other => return Err(format!("arrow type {other:?} cannot carry an iceberg default")),
    })
}
```

In `align_batch_to_iceberg_schema` line 1654, replace `new_null_array(target.data_type(), row_count)` with `build_iceberg_default_array(target.as_ref(), row_count)?`.

In `align_iceberg_array_to_field` line 1583, replace the same `new_null_array` call.

- [ ] **Step 4: Run test**

Run: `cargo test -p novarocks --lib formats::parquet::tests::iceberg_schema_evolution_fills_missing_column_with_initial_default`
Expected: pass.

- [ ] **Step 5: Run all parquet tests for regressions**

Run: `cargo test -p novarocks --lib formats::parquet`
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/formats/parquet/mod.rs
git commit -m "feat(iceberg): fill missing columns with initial-default literal in parquet reader"
```

---

## Phase 6: Write Path (B)

### Task 16: `reorder_insert_row` honors `write_default`

**Files:**
- Modify: `src/engine/insert.rs`

- [ ] **Step 1: Add failing test**

In `src/engine/insert.rs` test module, add:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    #[test]
    fn reorder_insert_row_uses_write_default_for_omitted_column() {
        let target_columns = vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(5),
                )),
            },
        ];
        let rows = vec![vec![Literal::Integer(1)]];
        let insert_columns = vec!["a".to_string()];
        let result =
            reorder_insert_rows(&rows, &insert_columns, &target_columns).expect("reorder");
        assert_eq!(result[0][0], Literal::Integer(1));
        assert_eq!(result[0][1], Literal::Integer(5));
    }

    #[test]
    fn reorder_insert_row_uses_null_when_no_write_default() {
        let target_columns = vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                write_default: None,
            },
        ];
        let rows = vec![vec![Literal::Integer(1)]];
        let insert_columns = vec!["a".to_string()];
        let result =
            reorder_insert_rows(&rows, &insert_columns, &target_columns).expect("reorder");
        assert_eq!(result[0][1], Literal::Null);
    }
}
```

- [ ] **Step 2: Run, expect failure (the second case may pass; the first fails)**

Run: `cargo test -p novarocks --lib engine::insert::tests`
Expected: `reorder_insert_row_uses_write_default_for_omitted_column` fails — got NULL, expected 5.

- [ ] **Step 3: Implement**

In `src/engine/insert.rs`, change `reorder_insert_row` signature from `mapping: &[Option<usize>]` to `mapping: &[(usize, Option<usize>)]` (target_idx + source_idx) — and update `build_insert_column_mapping` to emit the index pairs:

```rust
fn build_insert_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<(usize, Option<usize>)>, String> {
    let mut insert_index_by_name = HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for (target_idx, column) in target_columns.iter().enumerate() {
        let key = normalize_identifier(&column.name)?;
        mapping.push((target_idx, insert_index_by_name.remove(&key)));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn reorder_insert_row(
    row: &[Literal],
    mapping: &[(usize, Option<usize>)],
    target_columns: &[ColumnDef],
) -> Result<Vec<Literal>, String> {
    if row.len() > target_columns.len() {
        return Err(format!(
            "insert column count mismatch: expected at most {} values, got {}",
            target_columns.len(),
            row.len()
        ));
    }
    let mut reordered = Vec::with_capacity(target_columns.len());
    for (target_idx, source_idx) in mapping {
        match source_idx {
            Some(idx) => {
                let value = row.get(*idx).cloned().ok_or_else(|| {
                    format!("insert value for column position {} is missing", idx + 1)
                })?;
                reordered.push(value);
            }
            None => {
                let column = &target_columns[*target_idx];
                let literal = match &column.write_default {
                    Some(iceberg_lit) => {
                        // ColumnDef.data_type is Arrow; we need the SqlType
                        // for iceberg_literal_to_ast.  In this scope we only
                        // care about the Arrow side, so build the SqlType
                        // from data_type.  Practically, for the supported
                        // primitives, this is a 1:1 mapping handled by a
                        // helper below.
                        let sql_type = arrow_data_type_to_sql_type(&column.data_type)
                            .map_err(|e| {
                                format!(
                                    "INSERT write-default for `{}`: {e}",
                                    column.name
                                )
                            })?;
                        crate::connector::iceberg::default_value::iceberg_literal_to_ast(
                            iceberg_lit,
                            &sql_type,
                        )?
                    }
                    None => Literal::Null,
                };
                reordered.push(literal);
            }
        }
    }
    Ok(reordered)
}

fn arrow_data_type_to_sql_type(
    dt: &arrow::datatypes::DataType,
) -> Result<crate::sql::parser::ast::SqlType, String> {
    use crate::sql::parser::ast::SqlType;
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 => SqlType::String,
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Binary => SqlType::Binary,
        other => {
            return Err(format!(
                "unsupported Arrow type for write-default conversion: {other:?}"
            ));
        }
    })
}
```

Update the public `reorder_insert_rows` wrapper (line 22) to pass `target_columns` (rather than `target_columns.len()`):

```rust
pub(crate) fn reorder_insert_rows(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<Literal>>, String> {
    if insert_columns.is_empty() {
        return Ok(rows.to_vec());
    }
    let mapping = build_insert_column_mapping(insert_columns, target_columns)?;
    rows.iter()
        .map(|row| reorder_insert_row(row, &mapping, target_columns))
        .collect()
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib engine::insert`
Expected: 2 new tests pass; existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/insert.rs
git commit -m "feat(iceberg): use write_default for omitted columns in VALUES INSERT"
```

---

### Task 17: FROM QUERY path honors `insert_columns` and write_default

**Files:**
- Modify: `src/engine/iceberg_writer.rs`

- [ ] **Step 1: Add failing test**

The FROM QUERY path is exercised by SQL-level integration. Defer test to Phase 7 SQL test
`insert_explicit_column_list_omits_default_column_select_path` (Task 18).

- [ ] **Step 2: Implement `align_chunk_to_target_schema`**

In `src/engine/iceberg_writer.rs`, modify `execute_iceberg_insert_or_overwrite` to use `_insert_columns`:

```rust
pub(crate) fn execute_iceberg_insert_or_overwrite(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite: bool,
) -> Result<StatementResult, String> {
    // …existing entry / catalog / table loading code unchanged…

    // 3. Run the SELECT and convert to chunks.
    let chunks = run_select_to_chunks(state, target, query)?;

    // 3.5. If the user specified an explicit column list, reorder columns and
    //      fill omitted columns with their write_default literal (or NULL).
    let chunks = if insert_columns.is_empty() {
        chunks
    } else {
        align_chunks_to_target_schema(chunks, insert_columns, &resolved.columns)?
    };

    // …existing write_chunks_as_iceberg_data_files call unchanged…
}

fn align_chunks_to_target_schema(
    chunks: Vec<Chunk>,
    insert_columns: &[String],
    target_columns: &[crate::sql::catalog::ColumnDef],
) -> Result<Vec<Chunk>, String> {
    use crate::connector::iceberg::default_value::literal_to_constant_array;
    use crate::engine::catalog::normalize_identifier;
    use std::collections::HashMap;

    let normalized_insert: Vec<String> = insert_columns
        .iter()
        .map(|c| normalize_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert_idx_by_name: HashMap<String, usize> = HashMap::new();
    for (i, name) in normalized_insert.iter().enumerate() {
        if insert_idx_by_name.insert(name.clone(), i).is_some() {
            return Err(format!("duplicate INSERT column `{name}`"));
        }
    }

    let mut aligned = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let row_count = chunk.batch.num_rows();
        let source_schema = chunk.batch.schema();
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(target_columns.len());
        let mut fields: Vec<arrow::datatypes::FieldRef> =
            Vec::with_capacity(target_columns.len());
        for column in target_columns {
            let normalized = normalize_identifier(&column.name)?;
            if let Some(insert_idx) = insert_idx_by_name.get(&normalized) {
                let field = source_schema.field(*insert_idx);
                columns.push(chunk.batch.column(*insert_idx).clone());
                fields.push(arrow::datatypes::Field::new(
                    column.name.clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
                .into());
            } else {
                let array = match &column.write_default {
                    Some(iceberg_lit) => {
                        literal_to_constant_array(iceberg_lit, &column.data_type, row_count)?
                    }
                    None => arrow::array::new_null_array(&column.data_type, row_count),
                };
                fields.push(arrow::datatypes::Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )
                .into());
                columns.push(array);
            }
        }
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("align INSERT batch: {e}"))?;
        aligned.push(Chunk { batch });
    }
    Ok(aligned)
}
```

The function name in the existing signature uses `_insert_columns` (with underscore) — drop the underscore.

- [ ] **Step 3: Build to confirm signature changes ripple correctly**

Run: `cargo build`
Expected: callers of `execute_iceberg_insert_or_overwrite` still pass `insert_columns` correctly. If a caller passes `&[]`, the new code preserves today's behavior via the `insert_columns.is_empty()` short-circuit.

- [ ] **Step 4: Run tests**

Run: `cargo test -p novarocks --lib engine::iceberg_writer`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/iceberg_writer.rs
git commit -m "feat(iceberg): align FROM QUERY INSERT columns to target schema with write_default"
```

---

## Phase 7: SQL Integration Tests

### Task 18: SQL test suite `iceberg-v3-default`

**Files:**
- Create: `tests/sql-test-runner/cases/iceberg-v3-default/<case>.sql`
- Modify: `tests/sql-test-runner/Cargo.toml` (only if a registry is needed; check existing layout first)

Confirm test fixture layout by reading the existing `iceberg-schema-evolution` suite (likely `tests/sql-tests/...` or `tests/sql-test-runner/cases/...`); pattern after it. Each case is a `.sql` file with statements and expected outputs.

- [ ] **Step 1: Add cases (each its own file)**

Cases to add. Test each end-to-end against `standalone-server` per CLAUDE.md §8.4. For each case below, write the SQL into a `.sql` (or whatever the fixture format is) and capture the expected output via `--mode record`, then check in.

1. `create_v3_table_with_int_default.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT, b INT DEFAULT 5)
   PROPERTIES ('format-version' = '3');
   INSERT INTO t (a) VALUES (1);
   INSERT INTO t (a, b) VALUES (2, 7);
   SELECT * FROM t ORDER BY a;
   ```
   Expected: `(1, 5)` and `(2, 7)`.

2. `create_v2_table_with_default_rejected.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT, b INT DEFAULT 5);  -- defaults to v2
   ```
   Expected: error, message matches `format-version 3`.

3. `add_column_default_to_v3_with_existing_data.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT)
   PROPERTIES ('format-version' = '3');
   INSERT INTO t VALUES (1), (2);
   ALTER TABLE ice.db.t ADD COLUMN b INT DEFAULT 9;
   SELECT * FROM t ORDER BY a;
   INSERT INTO t (a) VALUES (3);
   SELECT * FROM t ORDER BY a;
   ```
   Expected: first SELECT `(1, 9), (2, 9)`; second SELECT `(1, 9), (2, 9), (3, 9)`.

4. `add_column_default_each_primitive_type.sql` — one column per supported primitive (boolean, tinyint, smallint, int, bigint, float, double, decimal(10,2), string, date, datetime, binary), insert one row, ALTER ADD each, SELECT shows defaults.

5. `default_null_on_v2_still_works.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT);
   ALTER TABLE ice.db.t ADD COLUMN b INT DEFAULT NULL;
   INSERT INTO t (a) VALUES (1);
   SELECT * FROM t;
   ```
   Expected: `(1, NULL)`.

6. `insert_explicit_column_list_omits_default_column.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT, b INT DEFAULT 5)
   PROPERTIES ('format-version' = '3');
   INSERT INTO t (a) VALUES (1);
   SELECT * FROM t;
   ```
   Expected: `(1, 5)`.

7. `insert_explicit_column_list_omits_default_column_select_path.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT, b INT DEFAULT 5)
   PROPERTIES ('format-version' = '3');
   INSERT INTO t (a) SELECT 1;
   SELECT * FROM t;
   ```
   Expected: `(1, 5)`. (Exercises the FROM QUERY path from Task 17.)

8. `insert_positional_count_mismatch_still_errors.sql`
   ```sql
   CREATE TABLE ice.db.t (a INT, b INT DEFAULT 5)
   PROPERTIES ('format-version' = '3');
   INSERT INTO t VALUES (1);  -- positional, count mismatch
   ```
   Expected: error containing `count mismatch`.

9. `default_decimal_scale_mismatch_rejected.sql`
   ```sql
   CREATE TABLE ice.db.t (a DECIMAL(10,2) DEFAULT 1.234)
   PROPERTIES ('format-version' = '3');
   ```
   Expected: error containing `scale`.

10. `default_complex_type_rejected.sql`
    ```sql
    CREATE TABLE ice.db.t (a ARRAY<INT> DEFAULT [1,2])
    PROPERTIES ('format-version' = '3');
    ```
    Expected: error containing `not supported`.

- [ ] **Step 2: Record expected outputs**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
SERVER_PID=$!
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg-v3-default --mode record
kill $SERVER_PID
```

- [ ] **Step 3: Verify in `verify` mode**

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &
SERVER_PID=$!
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg-v3-default --mode verify
kill $SERVER_PID
```

Expected: all 10 cases pass.

- [ ] **Step 4: Run regression suites**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify
```

Expected: TPC-DS suite still 99/99 (unchanged).

Run any other suite that exercises iceberg paths (e.g. `iceberg-schema-evolution`).

- [ ] **Step 5: Commit**

```bash
git add tests/sql-test-runner/cases/iceberg-v3-default/
git commit -m "test(iceberg): SQL integration suite for v3 default value"
```

---

## Self-Review Checklist (Plan Author)

The author of this plan ran the spec-coverage check below. Implementers should not re-run it; it is documented for reviewer transparency.

- D1 (scope A+B+C+D): A ✓ Task 15; B ✓ Tasks 16, 17; C ✓ Tasks 6, 7, 8, 9; D ✓ Tasks 4, 8, 9.
- D2 (primitives): Tasks 2, 14, 18 case 4.
- D3 (CREATE/ADD COLUMN only): Tasks 7, 8, 9.
- D4 (write-default explicit list only): Tasks 16, 17, 18 cases 6/7/8.
- D5 (v2 hard-reject non-NULL DEFAULT): Tasks 4, 9, 18 case 2.
- D6 (thrift extension): Task 12.
- D7 (read-side fail-fast): Tasks 14, 15.

No `TBD`, `TODO`, `FIXME`, or "fill in later" placeholders remain in the plan. Function and field names used in later tasks (`literal_to_constant_array`, `build_iceberg_default_array`, `iceberg_literal_to_ast`, `default_literal_to_iceberg`, `require_v3_for_default`, `ICEBERG_INITIAL_DEFAULT_META_KEY`) are all defined in earlier tasks.
