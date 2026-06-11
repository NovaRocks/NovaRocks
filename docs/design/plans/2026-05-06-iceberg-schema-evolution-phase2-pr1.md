# Iceberg Schema Evolution Phase 2 PR-1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend NovaRocks Iceberg schema evolution to cover nested STRUCT add / drop / rename / widen, ARRAY/MAP element widen, decimal-precision and date→timestamp widen, column reorder, and SET/DROP NOT NULL — closing the non-goals from the 2026-05-04 phase 1 design.

**Architecture:** The vendored `iceberg-rust 0.9.0` does not expose a `SchemaUpdate` builder, so we extend the existing hand-rolled `build_updated_schema` in `src/connector/iceberg/catalog/schema_update.rs` to walk Iceberg `Schema` recursively by `ColumnPath`. Parser additions live in `src/engine/statement.rs` (StarRocksDialect, hand-rolled). All commit-conflict retry and SET TBLPROPERTIES work is deferred to PR-2 / PR-3.

**Tech Stack:** Rust 2021, iceberg-rust 0.9.0 (vendored), sqlparser-rs (StarRocksDialect), `sql-tests` harness (`tests/sql-test-runner`).

**Spec:** [docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md](../specs/2026-05-06-iceberg-schema-evolution-phase2-design.md) §4 (PR-1).

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `src/engine/statement.rs:1109-1463` | Extend `IcebergSchemaChange` enum; extend `parse_alter_iceberg_schema_sql` for dotted paths, FIRST/AFTER/BEFORE, `ALTER COLUMN`, SET/DROP NOT NULL |
| Modify | `src/connector/iceberg/catalog/schema_update.rs` | `ColumnPath` walker; widen matrix; reorder; nullability; nested `reject_drop_dependencies` |
| Modify | `src/connector/iceberg/catalog/registry.rs:1501-1566` | (Possibly) reuse `iceberg_type_for_sql_type` helper as-is |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql` + `result/iceberg_schema_evolution_nested.result` | STRUCT add/drop/rename/widen end-to-end |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_array_map_widen.sql` + `.result` | ARRAY element + MAP value/key widen |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_decimal_widen.sql` + `.result` | Decimal precision widen happy path |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_date_to_timestamp_widen.sql` + `.result` | DATE → TIMESTAMP widen |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_reorder.sql` + `.result` | FIRST / AFTER / BEFORE on top + nested |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_nullability.sql` + `.result` | SET / DROP NOT NULL |
| Create | `sql-tests/iceberg/sql/iceberg_schema_evolution_widen_reject.sql` + `.result` | Negative matrix (string→binary, scale change, narrow) |

Boundaries:
- The `IcebergSchemaChange` enum is the single contract between parser and backend. Tests on either side construct enum values directly.
- `build_updated_schema` is the single recursive walker; no per-op variant has its own private walker.
- Parser changes are additive: existing top-level `ADD/DROP/RENAME/MODIFY COLUMN` SQL must keep working without diff.

---

## Pre-flight

- [ ] **Step 0.1: Verify clean baseline**

Run: `cd /Users/harbor/.claude/worktrees/NovaRocks/determined-dhawan-89a4e7 && cargo test -p novarocks --lib schema_update 2>&1 | tail -20`
Expected: existing schema_update tests pass.

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --only iceberg_schema_evolution_local --mode verify 2>&1 | tail -10`
Expected: phase-1 SQL test passes (baseline for regression).

Note: standalone-server must already be running on port 9030 for SQL verification. If not, start it: `NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030` in another shell.

- [ ] **Step 0.2: Commit nothing yet, just confirm we're on the right worktree**

Run: `git rev-parse --abbrev-ref HEAD`
Expected: `claude/determined-dhawan-89a4e7`.

---

## Phase A: Enum refactor (foundation)

### Task A1: Add `ColumnPath` newtype with parsing helpers

**Files:**
- Modify: `src/engine/statement.rs` (add types adjacent to existing `IcebergSchemaChange` enum at line ~1109)

- [ ] **Step 1: Write the failing test**

Add to `statement.rs` test module (search for `mod tests` in this file; if test fixtures live elsewhere, add a new `#[cfg(test)] mod path_tests` at file bottom):

```rust
#[cfg(test)]
mod column_path_tests {
    use super::ColumnPath;

    #[test]
    fn column_path_parses_single_segment() {
        let p = ColumnPath::parse("address").unwrap();
        assert_eq!(p.segments(), &["address".to_string()]);
        assert!(!p.is_empty());
    }

    #[test]
    fn column_path_parses_dotted() {
        let p = ColumnPath::parse("address.street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_normalizes_case() {
        let p = ColumnPath::parse("Address.Street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_rejects_empty_segment() {
        assert!(ColumnPath::parse("address.").is_err());
        assert!(ColumnPath::parse(".street").is_err());
        assert!(ColumnPath::parse("").is_err());
        assert!(ColumnPath::parse("a..b").is_err());
    }

    #[test]
    fn column_path_root_is_empty() {
        assert!(ColumnPath::root().is_empty());
        assert!(ColumnPath::root().segments().is_empty());
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::column_path_tests 2>&1 | tail -10`
Expected: `error[E0432]: unresolved import `super::ColumnPath`` or similar.

- [ ] **Step 3: Implement `ColumnPath`**

Add adjacent to existing `IcebergSchemaChange` enum in `src/engine/statement.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnPath {
    segments: Vec<String>,
}

impl ColumnPath {
    pub(crate) fn root() -> Self {
        Self { segments: Vec::new() }
    }

    pub(crate) fn parse(input: &str) -> Result<Self, String> {
        if input.is_empty() {
            return Err("column path is empty".to_string());
        }
        let mut segments = Vec::new();
        for raw in input.split('.') {
            if raw.is_empty() {
                return Err(format!("invalid column path '{input}': empty segment"));
            }
            segments.push(raw.to_ascii_lowercase());
        }
        Ok(Self { segments })
    }

    pub(crate) fn from_segments(segments: Vec<String>) -> Self {
        Self {
            segments: segments.into_iter().map(|s| s.to_ascii_lowercase()).collect(),
        }
    }

    pub(crate) fn segments(&self) -> &[String] {
        &self.segments
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(crate) fn last(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    pub(crate) fn parent(&self) -> ColumnPath {
        if self.segments.is_empty() {
            return ColumnPath::root();
        }
        Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        }
    }

    pub(crate) fn dotted(&self) -> String {
        self.segments.join(".")
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::column_path_tests 2>&1 | tail -10`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): add ColumnPath type for nested schema evolution paths"
```

### Task A2: Add `AddPosition` enum

**Files:**
- Modify: `src/engine/statement.rs`

- [ ] **Step 1: Write the failing test**

Add to the same `column_path_tests` module:

```rust
#[test]
fn add_position_default_constructed() {
    use super::AddPosition;
    let pos = AddPosition::Default;
    assert!(matches!(pos, AddPosition::Default));
}

#[test]
fn add_position_variants_construct() {
    use super::AddPosition;
    let _ = AddPosition::First;
    let _ = AddPosition::After("col_a".to_string());
    let _ = AddPosition::Before("col_b".to_string());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::column_path_tests 2>&1 | tail -10`
Expected: `error[E0432]: unresolved import \`super::AddPosition\``.

- [ ] **Step 3: Implement `AddPosition`**

Add next to `ColumnPath` in `src/engine/statement.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddPosition {
    Default,
    First,
    After(String),
    Before(String),
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::column_path_tests 2>&1 | tail -10`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): add AddPosition enum for column reorder semantics"
```

### Task A3: Refactor `IcebergSchemaChange` enum to use ColumnPath + add new variants

**Files:**
- Modify: `src/engine/statement.rs:1109-1123` (replace enum)
- Modify: `src/connector/iceberg/catalog/schema_update.rs` (callers must compile)

- [ ] **Step 1: Replace enum**

In `src/engine/statement.rs`, replace the existing `IcebergSchemaChange` (lines ~1109-1123) with:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        parent: ColumnPath,
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,
        position: AddPosition,
    },
    DropColumn {
        path: ColumnPath,
    },
    RenameColumn {
        path: ColumnPath,
        new_name: String,
    },
    ModifyColumn {
        path: ColumnPath,
        new_type: SqlType,
    },
    SetNullable {
        path: ColumnPath,
        nullable: bool,
    },
    Reorder {
        path: ColumnPath,
        position: AddPosition,
    },
}
```

- [ ] **Step 2: Update `parse_alter_iceberg_schema_sql` to construct new shape (preserving current behavior)**

In `src/engine/statement.rs:1373-1426` adjust the four existing branches to construct the new variant shape — at this point the parser still only consumes single (non-dotted) identifiers; nested-path support is added in Phase D.

```rust
let change = if parser.parse_keywords(&[Keyword::ADD, Keyword::COLUMN]) {
    parse_add_column_change(&mut parser)?
} else if parser.parse_keywords(&[Keyword::DROP, Keyword::COLUMN]) {
    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    IcebergSchemaChange::DropColumn { path: ColumnPath::parse(&name)? }
} else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
    let old_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    parser.expect_keyword(Keyword::TO).map_err(|e| e.to_string())?;
    let new_name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    IcebergSchemaChange::RenameColumn {
        path: ColumnPath::parse(&old_name)?,
        new_name,
    }
} else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
    parser.next_token();
    parser.expect_keyword(Keyword::COLUMN).map_err(|e| e.to_string())?;
    let name = parser.parse_identifier().map_err(|e| e.to_string())?.value;
    let new_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    IcebergSchemaChange::ModifyColumn {
        path: ColumnPath::parse(&name)?,
        new_type,
    }
} else {
    return Err("unsupported ALTER TABLE schema evolution clause".to_string());
};
```

Also update `parse_add_column_change` final return to construct the new shape:

```rust
Ok(IcebergSchemaChange::AddColumn {
    parent: ColumnPath::root(),
    name,
    data_type,
    default,
    position: AddPosition::Default,
})
```

- [ ] **Step 3: Update `schema_update.rs` callers to compile**

In `src/connector/iceberg/catalog/schema_update.rs:473-543` (`build_updated_schema` match arms), update pattern matches to use the new shape but preserve current top-level-only logic:

```rust
match change {
    IcebergSchemaChange::AddColumn { parent, name, data_type, default, position: _ } => {
        if !parent.is_empty() {
            return Err("nested STRUCT ADD COLUMN not yet implemented in this step".to_string());
        }
        // ... existing add-column body unchanged, treating `name` and `data_type` as before
    }
    IcebergSchemaChange::DropColumn { path } => {
        if path.segments().len() != 1 {
            return Err("nested DROP COLUMN not yet implemented in this step".to_string());
        }
        let name = path.last().unwrap();
        // ... existing drop body unchanged using `name`
    }
    IcebergSchemaChange::RenameColumn { path, new_name } => {
        if path.segments().len() != 1 {
            return Err("nested RENAME COLUMN not yet implemented in this step".to_string());
        }
        let old_name = path.last().unwrap();
        // ... existing body
    }
    IcebergSchemaChange::ModifyColumn { path, new_type } => {
        if path.segments().len() != 1 {
            return Err("nested MODIFY COLUMN not yet implemented in this step".to_string());
        }
        let name = path.last().unwrap();
        // ... existing body
    }
    IcebergSchemaChange::SetNullable { .. } => {
        return Err("ALTER COLUMN SET/DROP NOT NULL not yet implemented in this step".to_string());
    }
    IcebergSchemaChange::Reorder { .. } => {
        return Err("ALTER COLUMN reorder not yet implemented in this step".to_string());
    }
}
```

Also update other places that pattern-match `IcebergSchemaChange` (search the file for `IcebergSchemaChange::`); the same shape adjustment applies. Most likely sites: `reject_reserved_change` (line ~564), and any test in the same file. Update each to use the new field names.

- [ ] **Step 4: Update existing parser tests at `statement.rs:1795-1843`**

Each existing test that pattern-matches `IcebergSchemaChange::DropColumn { name }` etc. needs to be rewritten:

```rust
// OLD:
// matches!(stmt.change, IcebergSchemaChange::DropColumn { name } if name == "c")
// NEW:
let IcebergSchemaChange::DropColumn { path } = stmt.change else { panic!("expected DropColumn"); };
assert_eq!(path.dotted(), "c");
```

Apply similar updates to ADD / RENAME / MODIFY existing-test pattern matches. Do not add new tests for nested paths in this task.

- [ ] **Step 5: Run all tests; verify only existing top-level tests pass and the new SetNullable / Reorder branches give the placeholder error**

Run: `cargo build 2>&1 | tail -20`
Expected: clean build.

Run: `cargo test -p novarocks --lib 2>&1 | grep -E "(schema_update|alter_iceberg)" | tail -20`
Expected: existing parser + schema-update tests pass; no new tests yet.

- [ ] **Step 6: Commit**

```bash
git add src/engine/statement.rs src/connector/iceberg/catalog/schema_update.rs
git commit -m "refactor(iceberg): IcebergSchemaChange uses ColumnPath; add SetNullable/Reorder placeholders"
```

---

## Phase B: Backend nested-path walker

### Task B1: Recursive `find_field_by_path` helper + tests

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing tests**

Add to the existing `#[cfg(test)] mod tests` block in `schema_update.rs`:

```rust
#[test]
fn find_field_by_path_top_level() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "b", Type::Primitive(PrimitiveType::String))),
        ])
        .build()
        .unwrap();
    let path = ColumnPath::parse("a").unwrap();
    let (field_id, _ty) = find_field_by_path(&schema, &path).unwrap();
    assert_eq!(field_id, 1);
}

#[test]
fn find_field_by_path_nested_struct() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::optional(12, "city", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.street").unwrap();
    let (field_id, ty) = find_field_by_path(&schema, &path).unwrap();
    assert_eq!(field_id, 11);
    assert_eq!(ty, Type::Primitive(PrimitiveType::String));
}

#[test]
fn find_field_by_path_unknown_returns_err() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "a", Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("nonexistent").unwrap();
    assert!(find_field_by_path(&schema, &path).is_err());
}

#[test]
fn find_field_by_path_array_element() {
    use iceberg::spec::ListType;
    let element = Arc::new(NestedField::optional(11, "element", Type::Primitive(PrimitiveType::Int)));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "tags", Type::List(ListType { element_field: element.clone() }),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("tags.element").unwrap();
    let (field_id, ty) = find_field_by_path(&schema, &path).unwrap();
    assert_eq!(field_id, 11);
    assert_eq!(ty, Type::Primitive(PrimitiveType::Int));
}

#[test]
fn find_field_by_path_map_value() {
    use iceberg::spec::MapType;
    let key = Arc::new(NestedField::required(11, "key", Type::Primitive(PrimitiveType::String)));
    let value = Arc::new(NestedField::optional(12, "value", Type::Primitive(PrimitiveType::Int)));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "m", Type::Map(MapType { key_field: key, value_field: value }),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("m.value").unwrap();
    let (field_id, _) = find_field_by_path(&schema, &path).unwrap();
    assert_eq!(field_id, 12);
}
```

Add `use crate::engine::statement::ColumnPath;` at top of the test module if not already imported.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::find_field_by_path 2>&1 | tail -10`
Expected: `error: cannot find function `find_field_by_path``.

- [ ] **Step 3: Implement walker**

Add to `src/connector/iceberg/catalog/schema_update.rs` (place above `build_updated_schema`):

```rust
fn find_field_by_path(schema: &Schema, path: &ColumnPath) -> Result<(i32, Type), String> {
    if path.is_empty() {
        return Err("column path is empty".to_string());
    }
    let mut current_fields: Vec<Arc<NestedField>> = schema
        .as_struct()
        .fields()
        .iter()
        .cloned()
        .collect();
    let mut field_id: Option<i32> = None;
    let mut field_type: Option<Type> = None;
    let segments = path.segments();
    let mut idx = 0;
    while idx < segments.len() {
        let seg = &segments[idx];
        let is_last = idx + 1 == segments.len();
        let normalized = normalize_identifier(seg)?;
        let mut found: Option<Arc<NestedField>> = None;
        for f in &current_fields {
            if normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()) {
                found = Some(f.clone());
                break;
            }
        }
        let Some(f) = found else {
            return Err(format!("column path '{}' not found", path.dotted()));
        };
        field_id = Some(f.id);
        field_type = Some((*f.field_type).clone());
        if is_last {
            break;
        }
        // Descend
        match &*f.field_type {
            Type::Struct(s) => {
                current_fields = s.fields().iter().cloned().collect();
            }
            Type::List(l) => {
                if &normalized != "element" {
                    return Err(format!(
                        "list field '{}' can only descend into 'element'",
                        path.dotted()
                    ));
                }
                current_fields = vec![l.element_field.clone()];
            }
            Type::Map(m) => match normalized.as_str() {
                "key" => current_fields = vec![m.key_field.clone()],
                "value" => current_fields = vec![m.value_field.clone()],
                _ => return Err(format!(
                    "map field '{}' can only descend into 'key' or 'value'",
                    path.dotted()
                )),
            },
            _ => {
                return Err(format!(
                    "column path '{}' descends into non-composite type",
                    path.dotted()
                ));
            }
        }
        idx += 1;
    }
    Ok((field_id.unwrap(), field_type.unwrap()))
}
```

If `MapType` / `ListType` field names differ in iceberg-rust 0.9.0, adjust accordingly — verify with `grep -rn "ListType\b\|MapType\b" ~/.cargo/registry/src/index.crates.io-*/iceberg-0.9.0/src/spec/`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::find_field_by_path 2>&1 | tail -15`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): nested column path walker for schema evolution"
```

### Task B2: Recursive `apply_field_change_at` helper for DROP

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn apply_drop_at_nested_struct() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::optional(12, "city", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.city").unwrap();
    let new = apply_drop_at(&schema, &path).unwrap();
    let address = new.as_struct().fields()[0].clone();
    let Type::Struct(s) = &*address.field_type else { panic!() };
    assert_eq!(s.fields().len(), 1);
    assert_eq!(s.fields()[0].name, "street");
}

#[test]
fn apply_drop_at_top_level_works() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int))),
        ])
        .build()
        .unwrap();
    let path = ColumnPath::parse("a").unwrap();
    let new = apply_drop_at(&schema, &path).unwrap();
    assert_eq!(new.as_struct().fields().len(), 1);
    assert_eq!(new.as_struct().fields()[0].name, "b");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_drop_at 2>&1 | tail -10`
Expected: `cannot find function apply_drop_at`.

- [ ] **Step 3: Implement**

Add to `schema_update.rs` (above `build_updated_schema`):

```rust
fn apply_drop_at(schema: &Schema, path: &ColumnPath) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    let new_fields = drop_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after drop: {e}"))
}

fn drop_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("drop path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            if segments.len() == 1 {
                continue; // skip = drop
            }
            // Recurse into composite child
            let new_inner_type = drop_in_type(&f.field_type, &segments[1..])?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner_type);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for drop", head));
    }
    Ok(out)
}

fn drop_in_type(ty: &Type, segments: &[String]) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = drop_in_fields(s.fields().iter().cloned().collect(), segments)?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        Type::List(_) | Type::Map(_) => {
            Err("drop path cannot descend into list element or map key/value".to_string())
        }
        _ => Err("drop path descends into non-composite type".to_string()),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_drop_at 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_drop_at recursively rebuilds schema after nested DROP"
```

### Task B3: `apply_rename_at` for nested RENAME

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn apply_rename_at_nested() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.street").unwrap();
    let new = apply_rename_at(&schema, &path, "road").unwrap();
    let address = new.as_struct().fields()[0].clone();
    let Type::Struct(s) = &*address.field_type else { panic!() };
    assert_eq!(s.fields()[0].name, "road");
    assert_eq!(s.fields()[0].id, 11);
}

#[test]
fn apply_rename_at_top_level() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "old", Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("old").unwrap();
    let new = apply_rename_at(&schema, &path, "fresh").unwrap();
    assert_eq!(new.as_struct().fields()[0].name, "fresh");
}

#[test]
fn apply_rename_at_conflict() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::optional(12, "city", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.street").unwrap();
    assert!(apply_rename_at(&schema, &path, "city").is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_rename_at 2>&1 | tail -10`
Expected: `cannot find function apply_rename_at`.

- [ ] **Step 3: Implement**

```rust
fn apply_rename_at(schema: &Schema, path: &ColumnPath, new_name: &str) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    let new_fields = rename_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        new_name,
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after rename: {e}"))
}

fn rename_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    new_name: &str,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("rename path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let new_norm = normalize_identifier(new_name)?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            if segments.len() == 1 {
                // Conflict check: another sibling already has new_name
                // (we already past the loop's `f`, so check the rest including `out`)
                // Simpler: check on entry by iterating once first.
                let mut updated = (*f).clone();
                updated.name = new_name.to_string();
                out.push(updated);
            } else {
                let new_inner = rename_in_type(&f.field_type, &segments[1..], new_name)?;
                let mut updated = (*f).clone();
                updated.field_type = Box::new(new_inner);
                out.push(updated);
            }
        } else {
            if segments.len() == 1 && f_norm.as_deref() == Some(new_norm.as_str()) {
                return Err(format!(
                    "rename target '{}' conflicts with existing sibling",
                    new_name
                ));
            }
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for rename", head));
    }
    Ok(out)
}

fn rename_in_type(ty: &Type, segments: &[String], new_name: &str) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = rename_in_fields(
                s.fields().iter().cloned().collect(),
                segments,
                new_name,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err("rename path descends into non-struct type".to_string()),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_rename_at 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_rename_at supports nested struct paths with conflict check"
```

### Task B4: `apply_modify_at` (with widen) for nested types incl. ARRAY/MAP element

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn apply_modify_at_nested_struct_int_to_long() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "n", Type::Primitive(PrimitiveType::Int))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "wrap", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("wrap.n").unwrap();
    let new = apply_modify_at(&schema, &path, &SqlType::BigInt).unwrap();
    let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else { panic!() };
    assert!(matches!(*s.fields()[0].field_type, Type::Primitive(PrimitiveType::Long)));
    assert_eq!(s.fields()[0].id, 11);
}

#[test]
fn apply_modify_at_array_element() {
    use iceberg::spec::ListType;
    let element = Arc::new(NestedField::optional(11, "element", Type::Primitive(PrimitiveType::Int)));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "tags", Type::List(ListType { element_field: element }),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("tags.element").unwrap();
    let new = apply_modify_at(&schema, &path, &SqlType::BigInt).unwrap();
    let Type::List(l) = &*new.as_struct().fields()[0].field_type else { panic!() };
    assert!(matches!(*l.element_field.field_type, Type::Primitive(PrimitiveType::Long)));
    assert_eq!(l.element_field.id, 11);
}

#[test]
fn apply_modify_at_map_value() {
    use iceberg::spec::MapType;
    let key = Arc::new(NestedField::required(11, "key", Type::Primitive(PrimitiveType::String)));
    let value = Arc::new(NestedField::optional(12, "value", Type::Primitive(PrimitiveType::Int)));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "m", Type::Map(MapType { key_field: key, value_field: value }),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("m.value").unwrap();
    let new = apply_modify_at(&schema, &path, &SqlType::BigInt).unwrap();
    let Type::Map(m) = &*new.as_struct().fields()[0].field_type else { panic!() };
    assert!(matches!(*m.value_field.field_type, Type::Primitive(PrimitiveType::Long)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_modify_at 2>&1 | tail -10`
Expected: `cannot find function apply_modify_at`.

- [ ] **Step 3: Implement**

```rust
fn apply_modify_at(schema: &Schema, path: &ColumnPath, new_type: &SqlType) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    let new_fields = modify_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        new_type,
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after modify: {e}"))
}

fn modify_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    new_type: &SqlType,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("modify path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let mut updated = (*f).clone();
            if segments.len() == 1 {
                let widened = widen_type(&f.field_type, new_type)?;
                updated.field_type = Box::new(widened);
            } else {
                let new_inner = modify_in_type(&f.field_type, &segments[1..], new_type)?;
                updated.field_type = Box::new(new_inner);
            }
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for modify", head));
    }
    Ok(out)
}

fn modify_in_type(ty: &Type, segments: &[String], new_type: &SqlType) -> Result<Type, String> {
    let head = normalize_identifier(&segments[0])?;
    match ty {
        Type::Struct(s) => {
            let new = modify_in_fields(
                s.fields().iter().cloned().collect(),
                segments,
                new_type,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        Type::List(l) => {
            if &head != "element" || segments.len() != 1 {
                return Err("list modify must target '<list>.element'".to_string());
            }
            let widened = widen_type(&l.element_field.field_type, new_type)?;
            let mut new_elem = (*l.element_field).clone();
            new_elem.field_type = Box::new(widened);
            Ok(Type::List(iceberg::spec::ListType {
                element_field: Arc::new(new_elem),
            }))
        }
        Type::Map(m) => match (head.as_str(), segments.len()) {
            ("value", 1) => {
                let widened = widen_type(&m.value_field.field_type, new_type)?;
                let mut new_v = (*m.value_field).clone();
                new_v.field_type = Box::new(widened);
                Ok(Type::Map(iceberg::spec::MapType {
                    key_field: m.key_field.clone(),
                    value_field: Arc::new(new_v),
                }))
            }
            ("key", 1) => {
                let widened = widen_type(&m.key_field.field_type, new_type)?;
                let mut new_k = (*m.key_field).clone();
                new_k.field_type = Box::new(widened);
                Ok(Type::Map(iceberg::spec::MapType {
                    key_field: Arc::new(new_k),
                    value_field: m.value_field.clone(),
                }))
            }
            _ => Err("map modify must target '<map>.key' or '<map>.value'".to_string()),
        },
        _ => Err("modify path descends into non-composite type".to_string()),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_modify_at 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_modify_at supports nested structs and ARRAY/MAP element widen"
```

### Task B5: `apply_add_at` for nested STRUCT add

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn apply_add_at_nested_struct() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let parent = ColumnPath::parse("address").unwrap();
    let mut next_id = 11;
    let new = apply_add_at(
        &schema,
        &parent,
        "zip",
        &SqlType::Int,
        None,
        AddPosition::Default,
        &mut next_id,
    )
    .unwrap();
    let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else { panic!() };
    assert_eq!(s.fields().len(), 2);
    assert_eq!(s.fields()[1].name, "zip");
    assert_eq!(s.fields()[1].id, 12);
}

#[test]
fn apply_add_at_top_level_first_position() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "a", Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let mut next_id = 1;
    let new = apply_add_at(
        &schema,
        &ColumnPath::root(),
        "b",
        &SqlType::Int,
        None,
        AddPosition::First,
        &mut next_id,
    )
    .unwrap();
    assert_eq!(new.as_struct().fields()[0].name, "b");
    assert_eq!(new.as_struct().fields()[1].name, "a");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_add_at 2>&1 | tail -10`
Expected: `cannot find function apply_add_at`.

- [ ] **Step 3: Implement**

```rust
fn apply_add_at(
    schema: &Schema,
    parent: &ColumnPath,
    name: &str,
    data_type: &SqlType,
    default: Option<&DefaultLiteral>,
    position: AddPosition,
    last_column_id: &mut i32,
) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    // allocate new field id
    let new_id = last_column_id
        .checked_add(1)
        .ok_or("too many iceberg columns")?;
    let mut next_nested_id = new_id
        .checked_add(1)
        .ok_or("too many iceberg columns")?;
    let new_ty = crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
        data_type,
        &mut next_nested_id,
    )?;
    let mut new_field = NestedField::optional(new_id, name, new_ty);
    if let Some(lit) = default {
        if let Some(iceberg_lit) = crate::connector::iceberg::default_value::default_literal_to_iceberg(
            lit, data_type,
        )? {
            new_field = new_field
                .with_initial_default(iceberg_lit.clone())
                .with_write_default(iceberg_lit);
        }
    }
    *last_column_id = next_nested_id - 1;
    let new_fields = add_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        parent.segments(),
        new_field,
        &position,
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after add: {e}"))
}

fn add_in_fields(
    fields: Vec<Arc<NestedField>>,
    parent_segments: &[String],
    new_field: NestedField,
    position: &AddPosition,
) -> Result<Vec<NestedField>, String> {
    if parent_segments.is_empty() {
        // Conflict check
        let normalized = normalize_identifier(&new_field.name)?;
        for f in &fields {
            if normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()) {
                return Err(format!("Iceberg column `{}` already exists", new_field.name));
            }
        }
        let mut existing: Vec<NestedField> = fields.iter().map(|f| (**f).clone()).collect();
        return insert_at_position(&mut existing, new_field, position).map(|_| existing);
    }
    let head = normalize_identifier(&parent_segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let new_inner = add_in_type(
                &f.field_type,
                &parent_segments[1..],
                new_field.clone(),
                position,
            )?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("parent column '{}' not found for add", head));
    }
    Ok(out)
}

fn add_in_type(
    ty: &Type,
    parent_segments: &[String],
    new_field: NestedField,
    position: &AddPosition,
) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = add_in_fields(
                s.fields().iter().cloned().collect(),
                parent_segments,
                new_field,
                position,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err("ADD COLUMN parent path must point to a STRUCT".to_string()),
    }
}

fn insert_at_position(
    fields: &mut Vec<NestedField>,
    new_field: NestedField,
    position: &AddPosition,
) -> Result<(), String> {
    match position {
        AddPosition::Default => {
            fields.push(new_field);
            Ok(())
        }
        AddPosition::First => {
            fields.insert(0, new_field);
            Ok(())
        }
        AddPosition::After(target) => {
            let target_norm = normalize_identifier(target)?;
            let idx = fields
                .iter()
                .position(|f| normalize_identifier(&f.name).ok().as_deref() == Some(target_norm.as_str()))
                .ok_or_else(|| format!("AFTER target '{}' not found in same parent", target))?;
            fields.insert(idx + 1, new_field);
            Ok(())
        }
        AddPosition::Before(target) => {
            let target_norm = normalize_identifier(target)?;
            let idx = fields
                .iter()
                .position(|f| normalize_identifier(&f.name).ok().as_deref() == Some(target_norm.as_str()))
                .ok_or_else(|| format!("BEFORE target '{}' not found in same parent", target))?;
            fields.insert(idx, new_field);
            Ok(())
        }
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_add_at 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_add_at supports nested STRUCT and FIRST/AFTER/BEFORE"
```

### Task B6: `apply_set_nullable_at`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn apply_set_nullable_at_top_level() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "a", Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("a").unwrap();
    let new = apply_set_nullable_at(&schema, &path, false).unwrap();
    assert!(new.as_struct().fields()[0].required);
}

#[test]
fn apply_set_nullable_at_nested() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.street").unwrap();
    let new = apply_set_nullable_at(&schema, &path, false).unwrap();
    let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else { panic!() };
    assert!(s.fields()[0].required);
}

#[test]
fn apply_set_nullable_at_identifier_field_rejects_drop_not_null() {
    // identifier field is required by spec; cannot be set nullable.
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1, "id", Type::Primitive(PrimitiveType::Long),
        ))])
        .with_identifier_field_ids(vec![1])
        .build()
        .unwrap();
    let path = ColumnPath::parse("id").unwrap();
    assert!(apply_set_nullable_at(&schema, &path, true).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_set_nullable_at 2>&1 | tail -10`
Expected: `cannot find function apply_set_nullable_at`.

- [ ] **Step 3: Implement**

```rust
fn apply_set_nullable_at(
    schema: &Schema,
    path: &ColumnPath,
    nullable: bool,
) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    // identifier field must remain required
    if nullable {
        let (target_id, _) = find_field_by_path(schema, path)?;
        if identifier_field_ids.contains(&target_id) {
            return Err(format!(
                "cannot DROP NOT NULL on identifier field '{}'",
                path.dotted()
            ));
        }
    }
    let new_fields = set_nullable_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        nullable,
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after set nullable: {e}"))
}

fn set_nullable_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    nullable: bool,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("set nullable path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let mut updated = (*f).clone();
            if segments.len() == 1 {
                updated.required = !nullable;
            } else {
                let new_inner = set_nullable_in_type(&f.field_type, &segments[1..], nullable)?;
                updated.field_type = Box::new(new_inner);
            }
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for set nullable", head));
    }
    Ok(out)
}

fn set_nullable_in_type(ty: &Type, segments: &[String], nullable: bool) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = set_nullable_in_fields(
                s.fields().iter().cloned().collect(),
                segments,
                nullable,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err(
            "SET/DROP NOT NULL only supported on top-level or STRUCT-nested fields".to_string(),
        ),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_set_nullable_at 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_set_nullable_at with identifier-field protection"
```

### Task B7: `apply_reorder_at`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn apply_reorder_at_top_level_first() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))),
        ])
        .build()
        .unwrap();
    let path = ColumnPath::parse("c").unwrap();
    let new = apply_reorder_at(&schema, &path, &AddPosition::First).unwrap();
    let names: Vec<_> = new.as_struct().fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}

#[test]
fn apply_reorder_at_after_target() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(3, "c", Type::Primitive(PrimitiveType::Int))),
        ])
        .build()
        .unwrap();
    let path = ColumnPath::parse("a").unwrap();
    let new = apply_reorder_at(&schema, &path, &AddPosition::After("b".to_string())).unwrap();
    let names: Vec<_> = new.as_struct().fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(names, vec!["b", "a", "c"]);
}

#[test]
fn apply_reorder_at_nested_struct() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
        Arc::new(NestedField::optional(12, "city", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.city").unwrap();
    let new = apply_reorder_at(&schema, &path, &AddPosition::Before("street".to_string())).unwrap();
    let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else { panic!() };
    let names: Vec<_> = s.fields().iter().map(|f| f.name.clone()).collect();
    assert_eq!(names, vec!["city", "street"]);
}

#[test]
fn apply_reorder_at_after_target_in_different_parent_rejected() {
    let inner = Type::Struct(StructType::new(vec![
        Arc::new(NestedField::optional(11, "street", Type::Primitive(PrimitiveType::String))),
    ]));
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "address", inner)),
            Arc::new(NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String))),
        ])
        .build()
        .unwrap();
    let path = ColumnPath::parse("address.street").unwrap();
    assert!(apply_reorder_at(&schema, &path, &AddPosition::After("name".to_string())).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_reorder_at 2>&1 | tail -10`
Expected: `cannot find function apply_reorder_at`.

- [ ] **Step 3: Implement**

```rust
fn apply_reorder_at(
    schema: &Schema,
    path: &ColumnPath,
    position: &AddPosition,
) -> Result<Schema, String> {
    let identifier_field_ids = schema.identifier_field_ids().collect::<Vec<_>>();
    let new_fields = reorder_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        position,
    )?;
    Schema::builder()
        .with_fields(new_fields.into_iter().map(Arc::new).collect())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after reorder: {e}"))
}

fn reorder_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    position: &AddPosition,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("reorder path is empty".to_string());
    }
    if segments.len() == 1 {
        let head = normalize_identifier(&segments[0])?;
        let mut existing: Vec<NestedField> = fields.iter().map(|f| (**f).clone()).collect();
        let idx = existing
            .iter()
            .position(|f| normalize_identifier(&f.name).ok().as_deref() == Some(head.as_str()))
            .ok_or_else(|| format!("column '{}' not found for reorder", head))?;
        let target = existing.remove(idx);
        insert_at_position(&mut existing, target, position)?;
        return Ok(existing);
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let new_inner = reorder_in_type(&f.field_type, &segments[1..], position)?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for reorder", head));
    }
    Ok(out)
}

fn reorder_in_type(ty: &Type, segments: &[String], position: &AddPosition) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = reorder_in_fields(
                s.fields().iter().cloned().collect(),
                segments,
                position,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err("reorder path descends into non-struct type".to_string()),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::apply_reorder_at 2>&1 | tail -10`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): apply_reorder_at supports top-level and nested struct reorder"
```

### Task B8: Wire all `apply_*_at` into `build_updated_schema`

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs:459-551` (replace `build_updated_schema` body)

- [ ] **Step 1: Replace `build_updated_schema` body**

Replace the entire match in `build_updated_schema` with dispatch to the new helpers:

```rust
fn build_updated_schema(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    reject_reserved_change(change)?;
    match change {
        IcebergSchemaChange::AddColumn { parent, name, data_type, default, position } => {
            let mut next = last_column_id;
            apply_add_at(current, parent, name, data_type, default.as_ref(), position.clone(), &mut next)
        }
        IcebergSchemaChange::DropColumn { path } => {
            // identifier field protection
            let identifier_field_ids = current.identifier_field_ids().collect::<Vec<_>>();
            let (id, _) = find_field_by_path(current, path)?;
            if identifier_field_ids.contains(&id) {
                return Err(format!(
                    "Iceberg schema evolution cannot drop identifier column `{}`",
                    path.dotted()
                ));
            }
            apply_drop_at(current, path)
        }
        IcebergSchemaChange::RenameColumn { path, new_name } => {
            apply_rename_at(current, path, new_name)
        }
        IcebergSchemaChange::ModifyColumn { path, new_type } => {
            apply_modify_at(current, path, new_type)
        }
        IcebergSchemaChange::SetNullable { path, nullable } => {
            apply_set_nullable_at(current, path, *nullable)
        }
        IcebergSchemaChange::Reorder { path, position } => {
            apply_reorder_at(current, path, position)
        }
    }
}
```

Adjust `reject_reserved_change` (line ~564) to inspect every variant's path:

```rust
fn reject_reserved_change(change: &IcebergSchemaChange) -> Result<(), String> {
    let names_to_check: Vec<String> = match change {
        IcebergSchemaChange::AddColumn { name, .. } => vec![name.clone()],
        IcebergSchemaChange::DropColumn { path } => path.last().map(str::to_string).into_iter().collect(),
        IcebergSchemaChange::RenameColumn { path, new_name } => {
            let mut v = path.last().map(str::to_string).into_iter().collect::<Vec<_>>();
            v.push(new_name.clone());
            v
        }
        IcebergSchemaChange::ModifyColumn { path, .. }
        | IcebergSchemaChange::SetNullable { path, .. }
        | IcebergSchemaChange::Reorder { path, .. } => {
            path.last().map(str::to_string).into_iter().collect()
        }
    };
    for name in names_to_check {
        if crate::exec::row_position::is_iceberg_row_id(&name)
            || crate::exec::row_position::is_iceberg_last_updated_sequence_number(&name)
        {
            return Err(format!(
                "Iceberg schema evolution cannot modify reserved column `{name}`"
            ));
        }
    }
    Ok(())
}
```

Also remove the placeholder error returns from Task A3 (they're now superseded).

- [ ] **Step 2: Run all schema_update tests**

Run: `cargo test -p novarocks --lib schema_update 2>&1 | tail -30`
Expected: existing top-level tests still pass; new helper tests still pass.

- [ ] **Step 3: Add an integration-shaped unit test exercising `build_updated_schema` for each variant**

```rust
#[test]
fn build_updated_schema_dispatches_set_nullable() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1, "a", Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let change = IcebergSchemaChange::SetNullable {
        path: ColumnPath::parse("a").unwrap(),
        nullable: false,
    };
    let new = build_updated_schema(&schema, 1, &change).unwrap();
    assert!(new.as_struct().fields()[0].required);
}

#[test]
fn build_updated_schema_dispatches_reorder() {
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int))),
        ])
        .build()
        .unwrap();
    let change = IcebergSchemaChange::Reorder {
        path: ColumnPath::parse("b").unwrap(),
        position: AddPosition::First,
    };
    let new = build_updated_schema(&schema, 2, &change).unwrap();
    assert_eq!(new.as_struct().fields()[0].name, "b");
}
```

Run: `cargo test -p novarocks --lib schema_update::tests::build_updated_schema_dispatches 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): build_updated_schema dispatches all 6 schema change variants"
```

---

## Phase C: Widen matrix expansion

### Task C1: `widen_type` matrix — decimal precision +, date → timestamp, plus reject explicit cases

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs:855-867`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn widen_decimal_precision_increase_same_scale() {
    let curr = Type::Primitive(PrimitiveType::Decimal { precision: 10, scale: 2 });
    let new = SqlType::Decimal { precision: 20, scale: 2 };
    let widened = widen_type(&curr, &new).unwrap();
    let Type::Primitive(PrimitiveType::Decimal { precision, scale }) = widened else { panic!() };
    assert_eq!(precision, 20);
    assert_eq!(scale, 2);
}

#[test]
fn widen_decimal_scale_change_rejected() {
    let curr = Type::Primitive(PrimitiveType::Decimal { precision: 10, scale: 2 });
    let new = SqlType::Decimal { precision: 10, scale: 3 };
    assert!(widen_type(&curr, &new).is_err());
}

#[test]
fn widen_decimal_precision_decrease_rejected() {
    let curr = Type::Primitive(PrimitiveType::Decimal { precision: 20, scale: 2 });
    let new = SqlType::Decimal { precision: 10, scale: 2 };
    assert!(widen_type(&curr, &new).is_err());
}

#[test]
fn widen_date_to_timestamp() {
    let curr = Type::Primitive(PrimitiveType::Date);
    let new = SqlType::DateTime;
    let widened = widen_type(&curr, &new).unwrap();
    assert!(matches!(widened, Type::Primitive(PrimitiveType::Timestamp)));
}

#[test]
fn widen_string_to_binary_rejected() {
    let curr = Type::Primitive(PrimitiveType::String);
    let new = SqlType::Binary;
    assert!(widen_type(&curr, &new).is_err());
}

#[test]
fn widen_long_to_int_rejected() {
    let curr = Type::Primitive(PrimitiveType::Long);
    let new = SqlType::Int;
    assert!(widen_type(&curr, &new).is_err());
}

#[test]
fn widen_double_to_float_rejected() {
    let curr = Type::Primitive(PrimitiveType::Double);
    let new = SqlType::Float;
    assert!(widen_type(&curr, &new).is_err());
}

#[test]
fn widen_timestamp_to_date_rejected() {
    let curr = Type::Primitive(PrimitiveType::Timestamp);
    let new = SqlType::Date;
    assert!(widen_type(&curr, &new).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::widen_ 2>&1 | tail -20`
Expected: 8 failures (existing matrix only handles int→long, float→double).

- [ ] **Step 3: Replace `widen_type` body**

```rust
fn widen_type(current: &Type, new_type: &SqlType) -> Result<Type, String> {
    match (current, new_type) {
        (Type::Primitive(PrimitiveType::Int), SqlType::BigInt) => {
            Ok(Type::Primitive(PrimitiveType::Long))
        }
        (Type::Primitive(PrimitiveType::Float), SqlType::Double) => {
            Ok(Type::Primitive(PrimitiveType::Double))
        }
        (
            Type::Primitive(PrimitiveType::Decimal { precision: cp, scale: cs }),
            SqlType::Decimal { precision: np, scale: ns },
        ) => {
            if (*cs as i8) != *ns {
                return Err(format!(
                    "decimal scale change is not allowed (current {}.{}, new {}.{})",
                    cp, cs, np, ns
                ));
            }
            if (*np as u32) <= (*cp as u32) {
                return Err(format!(
                    "decimal precision must increase (current {}.{}, new {}.{})",
                    cp, cs, np, ns
                ));
            }
            Ok(Type::Primitive(PrimitiveType::Decimal {
                precision: *np as u32,
                scale: *ns as u32,
            }))
        }
        (Type::Primitive(PrimitiveType::Date), SqlType::DateTime) => {
            Ok(Type::Primitive(PrimitiveType::Timestamp))
        }
        _ => Err(format!(
            "unsupported Iceberg type evolution: {current:?} -> {new_type:?}"
        )),
    }
}
```

Note: verify `PrimitiveType::Decimal` field names in vendored iceberg-rust 0.9.0. If they differ (e.g. `precision: u32` vs `u8`), adjust the cast.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::widen_ 2>&1 | tail -25`
Expected: 8 passed (the 6 new + the 2 prior).

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): widen_type covers decimal precision + and date->timestamp"
```

---

## Phase D: Parser — accept dotted paths and new clauses

### Task D1: Parse dotted paths in DROP / RENAME / MODIFY COLUMN

**Files:**
- Modify: `src/engine/statement.rs:1373-1426`

- [ ] **Step 1: Write failing test**

Add to `statement.rs` test block (existing `parse_alter_iceberg_*` tests):

```rust
#[test]
fn parse_drop_nested_column() {
    let stmt = parse_alter_iceberg_schema_sql("ALTER TABLE t DROP COLUMN address.street").unwrap();
    let IcebergSchemaChange::DropColumn { path } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "address.street");
}

#[test]
fn parse_rename_nested_column() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t RENAME COLUMN address.zip TO address.postal_code",
    ).unwrap();
    let IcebergSchemaChange::RenameColumn { path, new_name } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "address.zip");
    assert_eq!(new_name, "postal_code");
}

#[test]
fn parse_modify_nested_column() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t MODIFY COLUMN address.zip BIGINT",
    ).unwrap();
    let IcebergSchemaChange::ModifyColumn { path, new_type } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "address.zip");
    assert!(matches!(new_type, SqlType::BigInt));
}

#[test]
fn parse_modify_array_element() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t MODIFY COLUMN tags.element VARCHAR",
    ).unwrap();
    let IcebergSchemaChange::ModifyColumn { path, .. } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "tags.element");
}

#[test]
fn parse_rename_extracts_only_last_segment_in_new_name() {
    // Rename's new_name should NOT carry a path; reject if user types a dotted new_name.
    assert!(parse_alter_iceberg_schema_sql(
        "ALTER TABLE t RENAME COLUMN address.zip TO foo.bar"
    ).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::tests::parse_drop_nested 2>&1 | tail -10`
Expected: failures — current parser only consumes single-token `parse_identifier()`.

- [ ] **Step 3: Replace identifier parsing with a dotted-path helper**

Add to `src/engine/statement.rs`:

```rust
fn parse_column_path(parser: &mut Parser<'_>) -> Result<ColumnPath, String> {
    let mut segments = Vec::new();
    loop {
        let id = parser.parse_identifier().map_err(|e| e.to_string())?.value;
        segments.push(id);
        if parser.consume_token(&Token::Period) {
            continue;
        }
        break;
    }
    Ok(ColumnPath::from_segments(segments))
}
```

Update DROP / RENAME / MODIFY branches in `parse_alter_iceberg_schema_sql`:

```rust
} else if parser.parse_keywords(&[Keyword::DROP, Keyword::COLUMN]) {
    let path = parse_column_path(&mut parser)?;
    if path.is_empty() {
        return Err("DROP COLUMN requires a column path".to_string());
    }
    IcebergSchemaChange::DropColumn { path }
} else if parser.parse_keywords(&[Keyword::RENAME, Keyword::COLUMN]) {
    let path = parse_column_path(&mut parser)?;
    parser.expect_keyword(Keyword::TO).map_err(|e| e.to_string())?;
    let new_path = parse_column_path(&mut parser)?;
    if new_path.segments().len() != 1 {
        return Err("RENAME COLUMN target must be a single identifier (no dotted path)".to_string());
    }
    IcebergSchemaChange::RenameColumn {
        path,
        new_name: new_path.segments()[0].clone(),
    }
} else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "MODIFY") {
    parser.next_token();
    parser.expect_keyword(Keyword::COLUMN).map_err(|e| e.to_string())?;
    let path = parse_column_path(&mut parser)?;
    let new_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    IcebergSchemaChange::ModifyColumn { path, new_type }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::tests::parse_ 2>&1 | tail -20`
Expected: existing + 5 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): parse dotted column paths in ALTER TABLE schema DDL"
```

### Task D2: Parse FIRST / AFTER / BEFORE for ADD COLUMN

**Files:**
- Modify: `src/engine/statement.rs` (`parse_add_column_change`)

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn parse_add_column_first() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ADD COLUMN c INT FIRST",
    ).unwrap();
    let IcebergSchemaChange::AddColumn { position, .. } = stmt.change else { panic!(); };
    assert!(matches!(position, AddPosition::First));
}

#[test]
fn parse_add_column_after_target() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ADD COLUMN c INT AFTER existing",
    ).unwrap();
    let IcebergSchemaChange::AddColumn { position, .. } = stmt.change else { panic!(); };
    assert!(matches!(position, AddPosition::After(ref s) if s == "existing"));
}

#[test]
fn parse_add_column_before_target() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ADD COLUMN c INT BEFORE existing",
    ).unwrap();
    let IcebergSchemaChange::AddColumn { position, .. } = stmt.change else { panic!(); };
    assert!(matches!(position, AddPosition::Before(ref s) if s == "existing"));
}

#[test]
fn parse_add_column_into_nested_struct() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ADD COLUMN address.zip INT",
    ).unwrap();
    let IcebergSchemaChange::AddColumn { parent, name, .. } = stmt.change else { panic!(); };
    assert_eq!(parent.dotted(), "address");
    assert_eq!(name, "zip");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::tests::parse_add_column_first 2>&1 | tail -10`
Expected: parsing FIRST / AFTER / BEFORE / nested forms all fail today.

- [ ] **Step 3: Update `parse_add_column_change`**

Replace the function in `src/engine/statement.rs`:

```rust
fn parse_add_column_change(parser: &mut Parser<'_>) -> Result<IcebergSchemaChange, String> {
    let path = parse_column_path(parser)?;
    if path.is_empty() {
        return Err("ADD COLUMN requires a column path".to_string());
    }
    let last = path.segments().last().unwrap().clone();
    let parent_segments = path.segments()[..path.segments().len() - 1].to_vec();
    let parent = ColumnPath::from_segments(parent_segments);

    let data_type = crate::sql::parser::dialect::convert_sql_type(
        parser.parse_data_type().map_err(|e| e.to_string())?,
    )?;
    let mut default: Option<DefaultLiteral> = None;
    let mut seen_null = false;
    let mut seen_default = false;
    let mut position = AddPosition::Default;
    let mut seen_position = false;
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
            if parser.parse_keyword(Keyword::NULL) {
                default = Some(DefaultLiteral::Null);
                continue;
            }
            default = Some(
                crate::sql::parser::dialect::create_table::parse_default_literal(
                    parser, &data_type,
                )?,
            );
            continue;
        }
        if parser.parse_keyword(Keyword::FIRST) {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            position = AddPosition::First;
            continue;
        }
        if parser.parse_keyword(Keyword::AFTER) {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            let target = parser.parse_identifier().map_err(|e| e.to_string())?.value;
            position = AddPosition::After(target);
            continue;
        }
        if crate::sql::parser::dialect::peek_word_eq(parser, 0, "BEFORE") {
            if seen_position {
                return Err("duplicate column position clause in ADD COLUMN".to_string());
            }
            seen_position = true;
            parser.next_token();
            let target = parser.parse_identifier().map_err(|e| e.to_string())?.value;
            position = AddPosition::Before(target);
            continue;
        }
        break;
    }
    Ok(IcebergSchemaChange::AddColumn {
        parent,
        name: last,
        data_type,
        default,
        position,
    })
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::tests::parse_add_column 2>&1 | tail -15`
Expected: all 4 new + previously existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): parse FIRST/AFTER/BEFORE for ADD COLUMN; nested struct add"
```

### Task D3: Parse `ALTER COLUMN ... FIRST/AFTER/BEFORE` (reorder)

**Files:**
- Modify: `src/engine/statement.rs:1373-1426` (add new top-level branch)

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn parse_alter_column_first() {
    let stmt = parse_alter_iceberg_schema_sql("ALTER TABLE t ALTER COLUMN c FIRST").unwrap();
    let IcebergSchemaChange::Reorder { path, position } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "c");
    assert!(matches!(position, AddPosition::First));
}

#[test]
fn parse_alter_column_after_target() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ALTER COLUMN c AFTER d"
    ).unwrap();
    let IcebergSchemaChange::Reorder { path, position } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "c");
    assert!(matches!(position, AddPosition::After(ref s) if s == "d"));
}

#[test]
fn parse_alter_column_nested_before() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ALTER COLUMN address.street BEFORE address.city"
    ).unwrap();
    let IcebergSchemaChange::Reorder { path, position } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "address.street");
    let AddPosition::Before(ref s) = position else { panic!(); };
    assert_eq!(s, "city"); // last segment of dotted target
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::tests::parse_alter_column 2>&1 | tail -10`
Expected: parsing fails, `unsupported ALTER TABLE schema evolution clause`.

- [ ] **Step 3: Add `ALTER COLUMN` branch**

In `parse_alter_iceberg_schema_sql`, add a new `else if` branch BEFORE the existing `else { return Err(...) }`:

```rust
} else if parser.parse_keywords(&[Keyword::ALTER, Keyword::COLUMN]) {
    let path = parse_column_path(&mut parser)?;
    let position = if parser.parse_keyword(Keyword::FIRST) {
        AddPosition::First
    } else if parser.parse_keyword(Keyword::AFTER) {
        let target_path = parse_column_path(&mut parser)?;
        let last = target_path.segments().last().ok_or("AFTER target empty")?.clone();
        AddPosition::After(last)
    } else if crate::sql::parser::dialect::peek_word_eq(&parser, 0, "BEFORE") {
        parser.next_token();
        let target_path = parse_column_path(&mut parser)?;
        let last = target_path.segments().last().ok_or("BEFORE target empty")?.clone();
        AddPosition::Before(last)
    } else if parser.parse_keywords(&[Keyword::SET, Keyword::NOT, Keyword::NULL]) {
        return Ok(AlterIcebergSchemaStmt {
            table,
            change: IcebergSchemaChange::SetNullable { path, nullable: false },
        });
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
        return Ok(AlterIcebergSchemaStmt {
            table,
            change: IcebergSchemaChange::SetNullable { path, nullable: true },
        });
    } else {
        return Err("ALTER COLUMN must be followed by FIRST / AFTER / BEFORE / SET NOT NULL / DROP NOT NULL".to_string());
    };
    IcebergSchemaChange::Reorder { path, position }
}
```

(Note: this single branch handles both reorder *and* SET/DROP NOT NULL, because all four start with `ALTER COLUMN <path>`. The `return Ok(...)` short-circuits the SetNullable case so we don't fall through to building Reorder.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::tests::parse_alter_column 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "feat(iceberg): parse ALTER COLUMN FIRST/AFTER/BEFORE for column reorder"
```

### Task D4: Parse `ALTER COLUMN ... SET / DROP NOT NULL`

**Files:**
- Modify: `src/engine/statement.rs` (already wired in Task D3 as part of the same branch)

- [ ] **Step 1: Write tests**

```rust
#[test]
fn parse_alter_column_set_not_null() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ALTER COLUMN c SET NOT NULL"
    ).unwrap();
    let IcebergSchemaChange::SetNullable { path, nullable } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "c");
    assert!(!nullable);
}

#[test]
fn parse_alter_column_drop_not_null() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ALTER COLUMN c DROP NOT NULL"
    ).unwrap();
    let IcebergSchemaChange::SetNullable { path, nullable } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "c");
    assert!(nullable);
}

#[test]
fn parse_alter_column_set_not_null_nested() {
    let stmt = parse_alter_iceberg_schema_sql(
        "ALTER TABLE t ALTER COLUMN address.street SET NOT NULL"
    ).unwrap();
    let IcebergSchemaChange::SetNullable { path, .. } = stmt.change else { panic!(); };
    assert_eq!(path.dotted(), "address.street");
}
```

- [ ] **Step 2: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::tests::parse_alter_column_set 2>&1 | tail -10`
Expected: 3 passed (D3 already wired this).

- [ ] **Step 3: Commit (just tests)**

```bash
git add src/engine/statement.rs
git commit -m "test(iceberg): parse ALTER COLUMN SET/DROP NOT NULL"
```

### Task D5: Reject composite `MODIFY COLUMN ... <new_type> FIRST/AFTER`

**Files:**
- Modify: `src/engine/statement.rs` (add reject branch in MODIFY)

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn parse_modify_column_with_position_rejected() {
    assert!(parse_alter_iceberg_schema_sql(
        "ALTER TABLE t MODIFY COLUMN c BIGINT FIRST"
    ).is_err());
    assert!(parse_alter_iceberg_schema_sql(
        "ALTER TABLE t MODIFY COLUMN c BIGINT AFTER d"
    ).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib statement::tests::parse_modify_column_with_position 2>&1 | tail -10`
Expected: tests pass for "AFTER d" because the parser would error on trailing tokens (already enforced at `parse_alter_iceberg_schema_sql:1417`), but the FIRST case succeeds parse and silently drops the keyword. Verify both fail.

- [ ] **Step 3: If FIRST is silently accepted, add explicit reject after MODIFY type**

In the MODIFY branch (`parse_alter_iceberg_schema_sql`), after parsing `new_type`:

```rust
if parser.parse_keyword(Keyword::FIRST)
    || parser.parse_keyword(Keyword::AFTER)
    || crate::sql::parser::dialect::peek_word_eq(&parser, 0, "BEFORE")
{
    return Err(
        "MODIFY COLUMN cannot combine type change with FIRST/AFTER/BEFORE; use a separate ALTER COLUMN statement".to_string(),
    );
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib statement::tests::parse_modify_column_with_position 2>&1 | tail -10`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/statement.rs
git commit -m "fix(iceberg): reject composite MODIFY COLUMN ... FIRST/AFTER/BEFORE"
```

---

## Phase E: Drop dependency check on nested paths

### Task E1: Extend `reject_drop_dependencies` for nested column paths

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs:602-624`

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn drop_nested_column_blocked_by_equality_delete() {
    let res = reject_drop_dependencies_for_test(
        "address.street",
        &["address.street".to_string()],
        &[],
    );
    assert!(res.is_err());
}

#[test]
fn drop_top_level_struct_blocked_when_equality_delete_targets_inner() {
    let res = reject_drop_dependencies_for_test(
        "address",
        &["address.street".to_string()],
        &[],
    );
    assert!(res.is_err());
}

#[test]
fn drop_unrelated_top_level_not_blocked_when_equality_delete_targets_other() {
    let res = reject_drop_dependencies_for_test(
        "name",
        &["address.street".to_string()],
        &[],
    );
    assert!(res.is_ok());
}
```

Update `reject_drop_dependencies_for_test` (line ~586) to accept a string column-path argument (not just a name):

```rust
#[cfg(test)]
fn reject_drop_dependencies_for_test(
    column: &str,
    equality_delete_columns: &[String],
    mv_sqls: &[String],
) -> Result<(), String> {
    let target = ManagedMvTarget::new("ice", "ns", "orders")?;
    let mv_dependencies = mv_sqls.iter().map(|sql| ManagedMvDependency {
        select_sql: sql.clone(),
        target: target.clone(),
    }).collect::<Vec<_>>();
    reject_drop_dependencies(column, equality_delete_columns, &mv_dependencies)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::drop_nested_column_blocked 2>&1 | tail -10`
Expected: equality-delete check is currently exact-match only; nested cases fail.

- [ ] **Step 3: Update `reject_drop_dependencies`**

Replace the function body with path-aware matching:

```rust
fn reject_drop_dependencies(
    column: &str,
    equality_delete_columns: &[String],
    mv_dependencies: &[ManagedMvDependency],
) -> Result<(), String> {
    let target_segments: Vec<String> = column
        .split('.')
        .map(|s| normalize_identifier(s).unwrap_or_else(|_| s.to_ascii_lowercase()))
        .collect();
    for ed in equality_delete_columns {
        let ed_segments: Vec<String> = ed
            .split('.')
            .map(|s| normalize_identifier(s).unwrap_or_else(|_| s.to_ascii_lowercase()))
            .collect();
        // Block if dropped path is an ancestor or equal to ED path.
        let is_ancestor_or_equal = ed_segments.starts_with(&target_segments);
        if is_ancestor_or_equal {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because an Iceberg equality-delete file references `{ed}`"
            ));
        }
    }
    // MV dep check: token match against dropped column's last segment as before.
    let last = target_segments.last().cloned().unwrap_or_default();
    for dep in mv_dependencies {
        if managed_mv_depends_on_column(dep, &last) {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because a managed materialized view references it"
            ));
        }
    }
    Ok(())
}
```

(Caller at `schema_update.rs` ~ line 1150 already passes the column name; for nested DROP, pass `path.dotted()` instead — adjust the calling site `protect_schema_change` to pass the full dotted path when DROP variant.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::drop 2>&1 | tail -15`
Expected: all drop dependency tests pass (existing + 3 new).

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): drop dependency check honors nested column paths"
```

---

## Phase F: Nullability attestation property

### Task F1: Write `novarocks.nullability.attested.<path>` on SET NOT NULL

**Files:**
- Modify: `src/connector/iceberg/catalog/schema_update.rs` (caller of `apply_set_nullable_at` — search where `commit_schema_change` builds `SetProperties`)

- [ ] **Step 1: Locate the property update assembly**

Grep for `SchemaPropertyUpdates` (line ~869) and find the function that builds `TableUpdate`s for each `IcebergSchemaChange`. Identify where `AddColumn` already adds `novarocks.logical_type.*` to `sets`. The same site is where we'll add `novarocks.nullability.attested.*`.

- [ ] **Step 2: Write failing test**

```rust
#[test]
fn schema_property_updates_attest_when_set_not_null() {
    let change = IcebergSchemaChange::SetNullable {
        path: ColumnPath::parse("address.street").unwrap(),
        nullable: false,
    };
    let updates = property_updates_for_change(&change, /* current_props */ &Default::default());
    assert!(updates.sets.contains_key("novarocks.nullability.attested.address.street"));
}

#[test]
fn schema_property_updates_remove_attestation_when_drop_not_null() {
    use std::collections::HashMap;
    let mut existing = HashMap::new();
    existing.insert("novarocks.nullability.attested.c".to_string(), "2026-05-06T00:00:00Z".to_string());
    let change = IcebergSchemaChange::SetNullable {
        path: ColumnPath::parse("c").unwrap(),
        nullable: true,
    };
    let updates = property_updates_for_change(&change, &existing);
    assert!(updates.removals.contains(&"novarocks.nullability.attested.c".to_string()));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p novarocks --lib schema_update::tests::schema_property_updates_attest 2>&1 | tail -10`
Expected: `cannot find function property_updates_for_change` or no new behavior.

- [ ] **Step 4: Implement**

Find or create `property_updates_for_change` (it probably exists implicitly inside the schema_update flow; if so, refactor to extract). Add the SetNullable arm:

```rust
fn property_updates_for_change(
    change: &IcebergSchemaChange,
    current_props: &HashMap<String, String>,
) -> SchemaPropertyUpdates {
    let mut updates = SchemaPropertyUpdates::default();
    // ... existing arms (AddColumn / DropColumn / RenameColumn / ModifyColumn) ...
    match change {
        IcebergSchemaChange::SetNullable { path, nullable } => {
            let key = format!("novarocks.nullability.attested.{}", path.dotted());
            if !*nullable {
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                updates.sets.insert(key, now);
            } else if current_props.contains_key(&key) {
                updates.push_removal(key);
            }
        }
        // ... others unchanged ...
        _ => {}
    }
    updates
}
```

If the function doesn't exist as a standalone, refactor what's currently inlined into a function with this signature. Use `chrono` if available (likely already a dep — check `Cargo.toml`). Otherwise use `time` crate.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p novarocks --lib schema_update::tests::schema_property_updates_attest 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): track novarocks.nullability.attested.* on SET/DROP NOT NULL"
```

---

## Phase G: SQL integration tests

### Task G1: Nested STRUCT add / drop / rename / widen

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_nested.result`

- [ ] **Step 1: Write SQL test file**

`sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql`:

```sql
-- @order_sensitive=true
-- Nested STRUCT add / drop / rename / widen end-to-end.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
DROP TABLE IF EXISTS people;
CREATE TABLE people (
  id INT,
  address STRUCT<street: STRING, city: STRING>
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO people VALUES (1, NAMED_STRUCT('street', '1 Main', 'city', 'Townsville'));
SELECT id, address.street, address.city FROM people ORDER BY id;

-- query 2
ALTER TABLE people ADD COLUMN address.zip INT;
SELECT id, address.street, address.city, address.zip FROM people ORDER BY id;

-- query 3
INSERT INTO people VALUES (2, NAMED_STRUCT('street', '2 Oak', 'city', 'Citytown', 'zip', 90210));
SELECT id, address.street, address.city, address.zip FROM people ORDER BY id;

-- query 4
ALTER TABLE people RENAME COLUMN address.zip TO address.postal_code;
SELECT id, address.postal_code FROM people ORDER BY id;

-- query 5
ALTER TABLE people MODIFY COLUMN address.postal_code BIGINT;
INSERT INTO people VALUES (3, NAMED_STRUCT('street', '3 Pine', 'city', 'Big', 'postal_code', 999999999999));
SELECT id, address.postal_code FROM people ORDER BY id;

-- query 6
ALTER TABLE people DROP COLUMN address.city;
SELECT id, address.street, address.postal_code FROM people ORDER BY id;

-- query 7
-- @expect_error=column path 'address.city' not found
ALTER TABLE people DROP COLUMN address.city;

-- query 8
DROP TABLE people;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
```

- [ ] **Step 2: Generate the result file**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --only iceberg_schema_evolution_nested --mode record 2>&1 | tail -20`
Expected: writes `sql-tests/iceberg/result/iceberg_schema_evolution_nested.result`. Inspect it to confirm sane output.

- [ ] **Step 3: Verify**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --only iceberg_schema_evolution_nested --mode verify 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql sql-tests/iceberg/result/iceberg_schema_evolution_nested.result
git commit -m "test(iceberg): nested STRUCT add/drop/rename/widen SQL suite"
```

### Task G2: ARRAY / MAP element widen

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_array_map_widen.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_array_map_widen.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- ARRAY element + MAP value/key widen.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
DROP TABLE IF EXISTS samples;
CREATE TABLE samples (
  id INT,
  scores ARRAY<INT>,
  attrs MAP<STRING, INT>
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO samples VALUES (1, ARRAY(10, 20), MAP('age', 30));
SELECT id, scores, attrs FROM samples ORDER BY id;

-- query 2
ALTER TABLE samples MODIFY COLUMN scores.element BIGINT;
INSERT INTO samples VALUES (2, ARRAY(9999999999), MAP('age', 40));
SELECT id, scores FROM samples ORDER BY id;

-- query 3
ALTER TABLE samples MODIFY COLUMN attrs.value BIGINT;
INSERT INTO samples VALUES (3, ARRAY(1), MAP('age', 9999999999));
SELECT id, attrs FROM samples ORDER BY id;

-- query 4
DROP TABLE samples;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
```

- [ ] **Step 2: Record + verify**

Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --only iceberg_schema_evolution_array_map_widen --mode record`
Then: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --only iceberg_schema_evolution_array_map_widen --mode verify`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_array_map_widen.sql sql-tests/iceberg/result/iceberg_schema_evolution_array_map_widen.result
git commit -m "test(iceberg): ARRAY/MAP element widen SQL suite"
```

### Task G3: Decimal precision widen

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_decimal_widen.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_decimal_widen.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- Decimal precision widen happy path + scale-change reject.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
  id INT,
  price DECIMAL(10, 2)
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO sales VALUES (1, 12345.67);
SELECT id, price FROM sales ORDER BY id;

-- query 2
ALTER TABLE sales MODIFY COLUMN price DECIMAL(20, 2);
INSERT INTO sales VALUES (2, 999999999999999999.99);
SELECT id, price FROM sales ORDER BY id;

-- query 3
-- @expect_error=decimal scale change is not allowed
ALTER TABLE sales MODIFY COLUMN price DECIMAL(20, 4);

-- query 4
-- @expect_error=decimal precision must increase
ALTER TABLE sales MODIFY COLUMN price DECIMAL(15, 2);

-- query 5
DROP TABLE sales;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
```

- [ ] **Step 2: Record + verify**

Same pattern as G1/G2.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_decimal_widen.sql sql-tests/iceberg/result/iceberg_schema_evolution_decimal_widen.result
git commit -m "test(iceberg): decimal precision widen SQL suite"
```

### Task G4: Date → Timestamp widen

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_date_to_timestamp_widen.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_date_to_timestamp_widen.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- DATE -> TIMESTAMP widen.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
DROP TABLE IF EXISTS events;
CREATE TABLE events (
  id INT,
  occurred_on DATE
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO events VALUES (1, DATE '2026-01-15');
SELECT id, occurred_on FROM events ORDER BY id;

-- query 2
ALTER TABLE events MODIFY COLUMN occurred_on DATETIME;
INSERT INTO events VALUES (2, TIMESTAMP '2026-02-20 11:22:33');
SELECT id, occurred_on FROM events ORDER BY id;

-- query 3
DROP TABLE events;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
```

- [ ] **Step 2: Record + verify**

Same pattern.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_date_to_timestamp_widen.sql sql-tests/iceberg/result/iceberg_schema_evolution_date_to_timestamp_widen.result
git commit -m "test(iceberg): date->timestamp widen SQL suite"
```

### Task G5: Reorder (FIRST / AFTER / BEFORE) on top + nested

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_reorder.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_reorder.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- ALTER COLUMN reorder.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
DROP TABLE IF EXISTS reorder_top;
CREATE TABLE reorder_top (
  a INT,
  b INT,
  c INT
) TBLPROPERTIES ("format-version" = "2");
INSERT INTO reorder_top VALUES (1, 2, 3);
SELECT * FROM reorder_top ORDER BY a;

-- query 2
ALTER TABLE reorder_top ALTER COLUMN c FIRST;
SELECT * FROM reorder_top ORDER BY a;

-- query 3
ALTER TABLE reorder_top ALTER COLUMN a AFTER b;
SELECT * FROM reorder_top ORDER BY a;

-- query 4
DROP TABLE IF EXISTS reorder_nested;
CREATE TABLE reorder_nested (
  id INT,
  address STRUCT<street: STRING, city: STRING, zip: INT>
) TBLPROPERTIES ("format-version" = "2");
INSERT INTO reorder_nested VALUES (1, NAMED_STRUCT('street', '1 Main', 'city', 'Town', 'zip', 90210));
SELECT id, address.street, address.city, address.zip FROM reorder_nested ORDER BY id;

-- query 5
ALTER TABLE reorder_nested ALTER COLUMN address.zip BEFORE address.street;
SELECT id, address.zip, address.street, address.city FROM reorder_nested ORDER BY id;

-- query 6
DROP TABLE reorder_top;
DROP TABLE reorder_nested;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
```

- [ ] **Step 2: Record + verify**

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_reorder.sql sql-tests/iceberg/result/iceberg_schema_evolution_reorder.result
git commit -m "test(iceberg): ALTER COLUMN FIRST/AFTER/BEFORE reorder SQL suite"
```

### Task G6: SET / DROP NOT NULL

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_nullability.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_nullability.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- SET / DROP NOT NULL.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_nullable_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_nullable_${uuid0};
DROP TABLE IF EXISTS members;
CREATE TABLE members (
  id INT,
  email STRING
) TBLPROPERTIES ("format-version" = "2");
INSERT INTO members VALUES (1, 'a@x.com'), (2, NULL);
SELECT id, email FROM members ORDER BY id;

-- query 2
-- DROP NOT NULL is a no-op on already-nullable column; should succeed.
ALTER TABLE members ALTER COLUMN email DROP NOT NULL;
SELECT id, email FROM members ORDER BY id;

-- query 3
-- SET NOT NULL on currently-nullable column: succeeds without scanning;
-- attestation property recorded; new INSERT NULL should still be accepted by Iceberg metadata
-- (NovaRocks doesn't enforce NOT NULL on writes today; spec §4.5 is metadata-only).
ALTER TABLE members ALTER COLUMN email SET NOT NULL;
SELECT id, email FROM members ORDER BY id;

-- query 4
-- Identifier field must remain required: identifier_field_ids includes id; cannot DROP NOT NULL.
-- @expect_error=cannot DROP NOT NULL on identifier field 'id'
-- (Test only meaningful if `id` is in identifier_field_ids; see spec §4.5)
-- For this regression we use a separate table with explicit identifier.
DROP TABLE members;
CREATE TABLE members_pk (
  id BIGINT NOT NULL,
  v INT
) UNIQUE KEY (id) TBLPROPERTIES ("format-version" = "2");
INSERT INTO members_pk VALUES (1, 100);
ALTER TABLE members_pk ALTER COLUMN id DROP NOT NULL;

-- query 5
DROP TABLE members_pk;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_nullable_${uuid0};
```

(Notes for executor: NovaRocks may not enforce identifier-field-ids exactly the way the spec says; if `members_pk` doesn't actually populate `identifier_field_ids` in metadata, skip query 4 by removing it. The unit test in Phase B6 already covers the identifier-protection path at the schema-update level.)

- [ ] **Step 2: Record + verify**

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_nullability.sql sql-tests/iceberg/result/iceberg_schema_evolution_nullability.result
git commit -m "test(iceberg): SET/DROP NOT NULL SQL suite"
```

### Task G7: Widen reject matrix

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_schema_evolution_widen_reject.sql`
- Create: `sql-tests/iceberg/result/iceberg_schema_evolution_widen_reject.result`

- [ ] **Step 1: Write SQL test**

```sql
-- @order_sensitive=true
-- Negative widening matrix.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
DROP TABLE IF EXISTS bad;
CREATE TABLE bad (
  i BIGINT,
  d DOUBLE,
  s STRING,
  ts TIMESTAMP
) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN i INT;

-- query 3
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN d FLOAT;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN s VARBINARY;

-- query 5
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN ts DATE;

-- query 6
DROP TABLE bad;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
```

(The exact error string `unsupported Iceberg type evolution` should match `widen_type` output. Adjust `@expect_error` to the substring the runner can match.)

- [ ] **Step 2: Record + verify**

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_schema_evolution_widen_reject.sql sql-tests/iceberg/result/iceberg_schema_evolution_widen_reject.result
git commit -m "test(iceberg): widen reject matrix SQL suite"
```

---

## Phase H: Final verification

### Task H1: cargo fmt + clippy + full test pass

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: clean.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets --all-features 2>&1 | tail -40`
Expected: no errors, no new warnings beyond baseline.

- [ ] **Step 3: Cargo test**

Run: `cargo test -p novarocks --lib 2>&1 | tail -10`
Expected: all tests pass; new test count goes up by ~30.

- [ ] **Step 4: SQL suite end-to-end (release build)**

Build release: `cargo build --release 2>&1 | tail -5`
Expected: clean build.

Restart standalone-server: `pkill -f 'standalone-server' || true && NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &`

Run new tests:
```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_schema_evolution_nested,iceberg_schema_evolution_array_map_widen,iceberg_schema_evolution_decimal_widen,iceberg_schema_evolution_date_to_timestamp_widen,iceberg_schema_evolution_reorder,iceberg_schema_evolution_nullability,iceberg_schema_evolution_widen_reject \
  --mode verify
```
Expected: 7/7 PASS.

- [ ] **Step 5: Regression — phase-1 schema evolution still passes**

Run:
```
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_schema_evolution_local \
  --mode verify
```
Expected: PASS.

### Task H2: Update completion checklist

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks Iceberg v3 完成度清单.md`

- [ ] **Step 1: Mark §5 items**

In §5 "Schema 演进（DDL）", flip `[ ]` to `[x]` for the items completed by this PR (nested struct, ARRAY/MAP widen, plus the previously over-claimed items: decimal precision widening, reorder, required↔optional, ADD COLUMN nested). Append `← 落地于 2026-05-XX · #<PR>` per the doc convention. Add a row to "变更记录" at the bottom.

- [ ] **Step 2: Commit checklist update separately (or hold for the PR's combined commit)**

Hold until the PR is opened and PR # is known; commit then.

### Task H3: Open PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin claude/determined-dhawan-89a4e7
```

- [ ] **Step 2: Create PR**

```bash
gh pr create --title "feat(iceberg): schema evolution phase 2 PR-1 (nested + widen + reorder + nullability)" --body "$(cat <<'EOF'
## Summary

- Nested STRUCT add/drop/rename/widen via `ColumnPath` walker
- ARRAY element + MAP key/value widen via `<list>.element` / `<map>.key` / `<map>.value` paths
- Decimal precision widening (same scale) and DATE → TIMESTAMP widening
- Column reorder via `ALTER COLUMN ... FIRST/AFTER/BEFORE` (top-level + nested)
- `ALTER COLUMN ... SET / DROP NOT NULL` with identifier-field protection and `novarocks.nullability.attested.<path>` attestation
- `reject_drop_dependencies` honors nested column paths against equality-delete files

Closes the non-goals from 2026-05-04 phase 1 design. Spec at `docs/design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md`.

PR-2 (commit conflict retry) and PR-3 (SET TBLPROPERTIES) follow.

## Test plan

- [x] cargo unit tests for `find_field_by_path`, `apply_drop_at`, `apply_rename_at`, `apply_modify_at`, `apply_add_at`, `apply_set_nullable_at`, `apply_reorder_at`, `widen_type` matrix
- [x] Parser tests for dotted paths, FIRST/AFTER/BEFORE, SET/DROP NOT NULL, composite reject
- [x] SQL suite: `iceberg_schema_evolution_nested`, `_array_map_widen`, `_decimal_widen`, `_date_to_timestamp_widen`, `_reorder`, `_nullability`, `_widen_reject`
- [x] Regression: `iceberg_schema_evolution_local` (phase 1) still passes
EOF
)"
```

- [ ] **Step 3: Update completion checklist with PR #**

Apply Task H2 Step 1 with the actual PR number, then commit and push that change as a follow-up commit on the same branch (PR auto-updates).

---

## Self-Review Output

Spec coverage check (post-write):

- §4.1 IcebergSchemaChange enum ✓ (Task A1-A3)
- §4.2 SQL syntax (all forms) ✓ (Task D1-D5)
- §4.3 widen matrix ✓ (Task C1)
- §4.4 SchemaUpdate fallback to hand-rolled walker ✓ (Task B1-B8) — explicitly chose hand-rolled per the survey finding that iceberg-rust 0.9.0 has no SchemaUpdate
- §4.5 SET NOT NULL semantics + attestation ✓ (Task B6 + F1)
- §4.6 reserved column / drop dependency nested-path extension ✓ (Task B8 reject_reserved_change + Task E1)
- §4.7 SQL suite ✓ (Task G1-G7, 7 files)

Type / signature consistency: `ColumnPath`, `AddPosition`, `IcebergSchemaChange`, `apply_*_at` family use consistent names across tasks.

Placeholder check: no TODO/TBD/FIXME in plan.

Open caveats embedded in plan (acceptable; engineer must verify at runtime, not blockers):
- Task B1 / B4: exact `ListType` / `MapType` field names in vendored iceberg-rust 0.9.0 — call-out present, with grep instructions.
- Task C1: `PrimitiveType::Decimal` field types (u32 vs u8) — call-out present.
- Task F1: `chrono` vs `time` crate — branch on `Cargo.toml`.
- Task G6: identifier-field SQL test depends on whether NovaRocks populates `identifier_field_ids` for `UNIQUE KEY` tables — fallback noted.
