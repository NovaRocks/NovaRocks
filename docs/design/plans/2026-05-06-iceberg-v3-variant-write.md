# Iceberg v3 Variant Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable `INSERT INTO ... SELECT/VALUES` to write Iceberg v3 tables that contain variant columns, producing parquet files with `LogicalType::Variant` plus two required binary leaves (`metadata`, `value`); keep all other write paths fail-fast with actionable errors.

**Architecture:** Add two vendor patches to `iceberg-0.9.0` (PATCH 6 introduces `PrimitiveType::Variant` + Arrow Struct mapping + `arrow.parquet.variant` extension metadata; PATCH 7 bumps arrow/parquet to 58.2). Add a thin `variant_write` module that transforms NovaRocks' internal `LargeBinary` variant column into a `StructArray{metadata, value}` immediately before handing the batch to iceberg's `ParquetWriter`. Replace the blanket `ensure_no_variant_columns` guard with precise spec-driven checks (partition spec / sort order) and add a single helper `ensure_no_variant_columns_for_row_level_mutation` wired into the four non-INSERT entry points.

**Tech Stack:** Rust, Arrow 58.2, parquet-rs 58.2 (`variant_experimental` feature), iceberg-rust 0.9.0 (vendored), sql-test-runner (NovaRocks).

**Companion spec:** [docs/design/specs/2026-05-06-iceberg-v3-variant-write-design.md](../specs/2026-05-06-iceberg-v3-variant-write-design.md).

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `vendor/iceberg-0.9.0/Cargo.toml` | modify | bump `arrow-* = "57.1"` → `"58.2"`, `parquet = "57.1"` → `"58.2"` |
| `Cargo.toml` (root) | modify | bump arrow/parquet to 58.2.0; enable `variant_experimental` feature on parquet |
| `vendor/iceberg-0.9.0/src/spec/datatypes.rs` | modify | add `PrimitiveType::Variant` arm + serde |
| `vendor/iceberg-0.9.0/src/arrow/schema.rs` | modify | map `Variant` → `Struct{metadata, value}`; attach `arrow.parquet.variant` extension metadata on the parent field in `ToArrowSchemaConverter::field` |
| `vendor/iceberg-0.9.0/PATCH.md` | modify | document PATCH 6 + PATCH 7 |
| `src/connector/iceberg/variant_write.rs` | create | `metadata_byte_len`, `variant_field_indices`, `transform_variant_columns_for_write` |
| `src/connector/iceberg/mod.rs` | modify | declare new `variant_write` module |
| `src/connector/iceberg/data_writer.rs` | modify | call `transform_variant_columns_for_write` before `annotate_batch`; add round-trip integration test |
| `src/connector/iceberg/commit/validation.rs` | modify | replace `ensure_no_variant_columns` with `ensure_no_variant_in_partition_spec` + `ensure_no_variant_in_sort_order`; allow `LargeBinary` ↔ `Variant` in `arrow_iceberg_types_compatible`; delete `type_contains_variant`; add `ensure_no_variant_columns_for_row_level_mutation` |
| `src/engine/delete_flow.rs` | modify | call mutation guard before `classify_sql_delete_strategy` |
| `src/engine/mutation_flow.rs` | modify | call mutation guard at UPDATE entry (line 102 area) and MERGE entry (line 1229 area) |
| `src/engine/iceberg_writer.rs` | modify | call mutation guard inside the `if overwrite { … }` branch |
| `src/engine/equality_delete_flow.rs` | modify | call mutation guard after table load (line 70 area) |
| `sql-tests/iceberg/sql/iceberg_v3_variant_insert.sql` | create | positive end-to-end SQL coverage |
| `sql-tests/iceberg/result/iceberg_v3_variant_insert.result` | create | recorded baseline |
| `sql-tests/iceberg/sql/iceberg_v3_variant_unsupported.sql` | create | negative cases (mutation rejections, partition / sort by variant) |
| `sql-tests/iceberg/result/iceberg_v3_variant_unsupported.result` | create | recorded baseline |

Each task below is fully self-contained — no cross-task placeholders. Code blocks contain literal text to write or replace.

---

## Task 1: Bump arrow / parquet to 58.2 (PATCH 7)

**Files:**
- Modify: `vendor/iceberg-0.9.0/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `vendor/iceberg-0.9.0/PATCH.md`

This is a mechanical version bump. We do it before any feature work because PATCH 6 (next task) targets parquet 58.2's `variant_experimental` feature.

- [ ] **Step 1: Bump vendor `arrow-*` and `parquet` constraints**

In `vendor/iceberg-0.9.0/Cargo.toml`, replace every line that pins these crates to `"57.1"` with `"58.2"`. The relevant entries (verified earlier in spec discovery) are:

```toml
[dependencies.arrow-arith]
version = "58.2"

[dependencies.arrow-array]
version = "58.2"

[dependencies.arrow-buffer]
version = "58.2"

[dependencies.arrow-cast]
version = "58.2"

[dependencies.arrow-ord]
version = "58.2"

[dependencies.arrow-schema]
version = "58.2"

[dependencies.arrow-select]
version = "58.2"

[dependencies.arrow-string]
version = "58.2"

[dependencies.parquet]
version = "58.2"
features = ["async"]
```

Use `Edit` per entry — do not lose the `features = ["async"]` for parquet.

- [ ] **Step 2: Bump root `Cargo.toml`**

In `Cargo.toml` at the worktree root, replace these lines exactly:

```toml
arrow = { version = "58.2.0", features = ["prettyprint", "ipc", "ipc_compression"] }
arrow-buffer = { version = "58.2.0" }
arrow-data = { version = "58.2.0" }
parquet = { version = "58.2.0", features = ["arrow", "variant_experimental"] }
```

If you see other `arrow-*` direct-dependency lines, bump them all to `"58.2.0"`.

- [ ] **Step 3: Verify the workspace builds**

Run: `cargo build`
Expected: success. Compilation may take a few minutes due to the dependency change. Address any 58.x API breakage you find by reading the breaking-changes section of the parquet/arrow CHANGELOG entries; in our experience the 57→58 bump is source-compatible for the surfaces we use, but if you hit a real issue, fix it in the offending file rather than reverting the bump.

- [ ] **Step 4: Verify the existing test suite still passes**

Run: `cargo test --lib`
Expected: all currently-passing tests still pass. If anything fails that did not fail before the bump, treat it as a regression to fix in this same task.

- [ ] **Step 5: Document PATCH 7**

Append the following entry to `vendor/iceberg-0.9.0/PATCH.md` (after PATCH 5):

```markdown
## Patch 7 — bump arrow / parquet to 58.2

Files: `Cargo.toml` (vendor copy only; root is bumped in lock-step).

iceberg-rust 0.9.0 originally pinned `arrow-* = "57.1"` and
`parquet = "57.1"`. NovaRocks needs parquet 58.x to reach the
`variant_experimental` feature (used by PATCH 6 to emit
`LogicalType::Variant`). The diff is mechanical — every `"57.1"` literal
in `[dependencies.arrow-*]` and `[dependencies.parquet]` becomes `"58.2"`.

When upstream iceberg-rust 0.10 lands with its own arrow/parquet bump,
this entry is removed by the same path that already retires PATCH 1–5.
```

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock vendor/iceberg-0.9.0/Cargo.toml vendor/iceberg-0.9.0/PATCH.md
git commit -m "deps: bump arrow/parquet to 58.2 (vendor PATCH 7)"
```

---

## Task 2: Add `PrimitiveType::Variant` to vendor iceberg datatypes (PATCH 6 part 1)

**Files:**
- Modify: `vendor/iceberg-0.9.0/src/spec/datatypes.rs`

The existing `PrimitiveType` enum lives at `vendor/iceberg-0.9.0/src/spec/datatypes.rs:210-250`. The `compatible` method is at lines 252-275. The serde plumbing (custom `Serialize`/`Deserialize` for `decimal` / `fixed`) is at 285-318 — we route `Variant` through the simple path so we do not touch those custom branches.

- [ ] **Step 1: Write the failing test**

Append this test to the `#[cfg(test)] mod tests { … }` block already at the bottom of `vendor/iceberg-0.9.0/src/spec/datatypes.rs`:

```rust
#[test]
fn primitive_variant_serde_roundtrip() {
    use serde_json;
    let json = r#""variant""#;
    let parsed: PrimitiveType = serde_json::from_str(json).expect("parse variant");
    assert_eq!(parsed, PrimitiveType::Variant);
    let serialized = serde_json::to_string(&parsed).expect("serialize variant");
    assert_eq!(serialized, json);
}

#[test]
fn primitive_variant_compatible_rejects_all_literals() {
    use crate::spec::PrimitiveLiteral;
    assert!(!PrimitiveType::Variant.compatible(&PrimitiveLiteral::Boolean(true)));
    assert!(!PrimitiveType::Variant.compatible(&PrimitiveLiteral::String("x".to_string())));
    assert!(!PrimitiveType::Variant.compatible(&PrimitiveLiteral::Binary(b"x".to_vec())));
}
```

- [ ] **Step 2: Run tests, expect failure**

Run: `cargo test -p iceberg --lib spec::datatypes::tests::primitive_variant -- --nocapture`
Expected: compilation error — `PrimitiveType::Variant` is not a variant.

- [ ] **Step 3: Add the `Variant` arm to `PrimitiveType`**

Edit `vendor/iceberg-0.9.0/src/spec/datatypes.rs` at the `PrimitiveType` enum. The current end of the enum is:

```rust
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}
```

Replace with:

```rust
    /// Fixed length byte array
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
    /// Iceberg v3 unshredded variant. Physical layout in parquet is a
    /// group of two binary leaves (`metadata`, `value`) annotated with
    /// `LogicalType::Variant`. NovaRocks vendor PATCH 6.
    Variant,
}
```

Because the enum carries `#[serde(rename_all = "lowercase", remote = "Self")]` and the custom `Deserialize` for `PrimitiveType` only diverts strings starting with `"decimal"` or `"fixed"` (`vendor/iceberg-0.9.0/src/spec/datatypes.rs:293-305`), the new arm gets `"variant"` ↔ `Variant` automatically through the default lowercase rename.

- [ ] **Step 4: Update `compatible` for `Variant`**

The current `compatible` body (lines 252-275) is a single `matches!` macro on `(self, literal)` with arms for each non-Variant primitive. Variant has no entry in that `matches!`, so it already returns `false` for every `PrimitiveLiteral`. Verify by reading the function and confirming there is no need to change it. If the test still fails because of a fall-through behavior you didn't expect, add an explicit `(PrimitiveType::Variant, _) => false` arm at the start.

- [ ] **Step 5: Confirm `Serialize` doesn't need a special branch**

The custom `Serialize` impl (`vendor/iceberg-0.9.0/src/spec/datatypes.rs:307-318`) only overrides `Decimal` and `Fixed`; everything else delegates to the derived rename. `Variant` flows through the derived `_ => PrimitiveType::serialize(self, serializer)` arm — no edit needed.

- [ ] **Step 6: Run tests, expect pass**

Run: `cargo test -p iceberg --lib spec::datatypes::tests::primitive_variant -- --nocapture`
Expected: both new tests PASS.

- [ ] **Step 7: Run the full vendor crate test**

Run: `cargo test -p iceberg --lib spec::datatypes`
Expected: all tests in this module pass (sanity check that adding a variant did not break exhaustiveness elsewhere).

- [ ] **Step 8: Address any non-exhaustive `match`es**

Run: `cargo build`
Expected: the compiler will name every `match self { … }` over `PrimitiveType` that is now non-exhaustive. For each, add a `PrimitiveType::Variant => …` arm that produces a clearly-rejecting outcome:
- in functions returning `Result`: `Err(Error::new(ErrorKind::FeatureUnsupported, "variant primitive type cannot be used in this context yet"))`
- in functions returning a default value (e.g. partition transforms): an explicit panic with `unreachable!("variant cannot reach <context>")` plus the relevant context string

Compile until clean.

- [ ] **Step 9: Commit**

```bash
git add vendor/iceberg-0.9.0/src/spec/datatypes.rs
git commit -m "vendor: add PrimitiveType::Variant arm (PATCH 6 part 1)"
```

---

## Task 3: Map `Variant` to Arrow Struct + extension metadata (PATCH 6 part 2)

**Files:**
- Modify: `vendor/iceberg-0.9.0/src/arrow/schema.rs`

The `ToArrowSchemaConverter` is at `vendor/iceberg-0.9.0/src/arrow/schema.rs:490-693`. The `field` method (515-535) is where `PARQUET_FIELD_ID_META_KEY` is attached; we'll attach the variant extension metadata in the same place. The `primitive` method (608-690) is where we add the `Variant` arm.

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)] mod tests { … }` block at the bottom of `vendor/iceberg-0.9.0/src/arrow/schema.rs`:

```rust
#[test]
fn variant_primitive_maps_to_struct() {
    let arrow_ty = type_to_arrow_type(&Type::Primitive(PrimitiveType::Variant)).expect("ok");
    let DataType::Struct(fields) = arrow_ty else {
        panic!("expected Struct, got {arrow_ty:?}");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].name(), "metadata");
    assert_eq!(fields[0].data_type(), &DataType::Binary);
    assert!(!fields[0].is_nullable());
    assert_eq!(fields[1].name(), "value");
    assert_eq!(fields[1].data_type(), &DataType::Binary);
    assert!(!fields[1].is_nullable());
}

#[test]
fn variant_field_attaches_parquet_variant_extension_metadata() {
    let schema = crate::spec::Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            crate::spec::NestedField::optional(7, "v", Type::Primitive(PrimitiveType::Variant))
                .into(),
        ])
        .build()
        .expect("schema");
    let arrow_schema = schema_to_arrow_schema(&schema).expect("convert");
    let field = arrow_schema.field(0);
    assert_eq!(field.name(), "v");
    assert_eq!(
        field.metadata().get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
        Some("7"),
    );
    assert_eq!(
        field.metadata().get("ARROW:extension:name").map(String::as_str),
        Some("arrow.parquet.variant"),
    );
    assert_eq!(
        field.metadata().get("ARROW:extension:metadata").map(String::as_str),
        Some(""),
    );
    assert!(field.is_nullable(), "optional iceberg field becomes nullable arrow field");
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p iceberg --lib arrow::schema::tests::variant`
Expected: failure — `primitive` does not yet handle `Variant`, and `field` does not attach the extension metadata.

- [ ] **Step 3: Add the `Variant` arm in `ToArrowSchemaConverter::primitive`**

The current end of `fn primitive` looks like:

```rust
            crate::spec::PrimitiveType::Binary => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::LargeBinary))
            }
        }
    }
}
```

Replace the inner closing `}`s with the new arm preserved in place:

```rust
            crate::spec::PrimitiveType::Binary => {
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::LargeBinary))
            }
            crate::spec::PrimitiveType::Variant => {
                // NovaRocks PATCH 6: Iceberg v3 variant becomes a Struct with
                // two required binary leaves; the parent field carries the
                // `arrow.parquet.variant` Arrow extension type so parquet writes
                // emit `LogicalType::Variant` (see ToArrowSchemaConverter::field).
                let metadata_field = Field::new("metadata", DataType::Binary, false);
                let value_field = Field::new("value", DataType::Binary, false);
                Ok(ArrowSchemaOrFieldOrType::Type(DataType::Struct(
                    Fields::from(vec![metadata_field, value_field]),
                )))
            }
        }
    }
}
```

If `Fields` is not in scope at the top of the file, add `use arrow_schema::Fields;` to the imports near `use arrow_schema::{...}`. (Verify it's already imported — `Field` is, so `Fields` typically is too in 58.x.)

- [ ] **Step 4: Attach the `arrow.parquet.variant` extension metadata in `ToArrowSchemaConverter::field`**

The current `field` body (515-534) builds a `metadata: HashMap<String, String>` based on whether `field.doc` is set. Replace it with a version that ALSO appends the extension keys when `field.field_type` is `Type::Primitive(PrimitiveType::Variant)`:

```rust
fn field(
    &mut self,
    field: &crate::spec::NestedFieldRef,
    value: ArrowSchemaOrFieldOrType,
) -> crate::Result<ArrowSchemaOrFieldOrType> {
    let ty = match value {
        ArrowSchemaOrFieldOrType::Type(ty) => ty,
        _ => unreachable!(),
    };
    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field.id.to_string());
    if let Some(doc) = &field.doc {
        metadata.insert(ARROW_FIELD_DOC_KEY.to_string(), doc.clone());
    }
    if matches!(
        field.field_type.as_ref(),
        crate::spec::Type::Primitive(crate::spec::PrimitiveType::Variant)
    ) {
        // NovaRocks PATCH 6: parquet-rs 58.x reads ARROW:extension:name to
        // emit LogicalType::Variant on the parent group when feature
        // `variant_experimental` is enabled.
        metadata.insert("ARROW:extension:name".to_string(), "arrow.parquet.variant".to_string());
        metadata.insert("ARROW:extension:metadata".to_string(), String::new());
    }
    Ok(ArrowSchemaOrFieldOrType::Field(
        Field::new(field.name.clone(), ty, !field.required).with_metadata(metadata),
    ))
}
```

- [ ] **Step 5: Run, expect pass**

Run: `cargo test -p iceberg --lib arrow::schema::tests::variant`
Expected: both new tests PASS.

- [ ] **Step 6: Run the full arrow::schema test module**

Run: `cargo test -p iceberg --lib arrow::schema`
Expected: all tests pass — sanity check that the rewritten `field` method did not regress doc-string handling.

- [ ] **Step 7: Commit**

```bash
git add vendor/iceberg-0.9.0/src/arrow/schema.rs
git commit -m "vendor: map iceberg Variant to Arrow Struct + arrow.parquet.variant extension (PATCH 6 part 2)"
```

---

## Task 4: Document PATCH 6 in PATCH.md

**Files:**
- Modify: `vendor/iceberg-0.9.0/PATCH.md`

- [ ] **Step 1: Append the PATCH 6 section**

Append before the existing `## Verification after rebase` section:

```markdown
## Patch 6 — `PrimitiveType::Variant` + Arrow Struct mapping

Files: `src/spec/datatypes.rs`, `src/arrow/schema.rs`.

iceberg-rust 0.9.0 has no `PrimitiveType::Variant` arm, so any
`metadata.json` field with `"type": "variant"` fails to deserialize.
NovaRocks needs to read AND write Iceberg v3 tables that carry variant
columns. This patch adds:

* `PrimitiveType::Variant` on the `PrimitiveType` enum, going through
  the default lowercase rename so serde reads/writes `"variant"` as
  expected. The compatibility table never matches a literal — variant
  default values / partition / stats are all out-of-scope for now.
* `ToArrowSchemaConverter::primitive` returns
  `DataType::Struct{ metadata: Binary req, value: Binary req }` for
  `Variant`. Subfields deliberately carry no `PARQUET:field_id` —
  spec assigns one iceberg field id to the variant column itself.
* `ToArrowSchemaConverter::field` attaches
  `ARROW:extension:name = "arrow.parquet.variant"` (with empty
  `ARROW:extension:metadata`) when the underlying iceberg type is
  `Variant`. parquet-rs 58.2 reads these keys and emits
  `LogicalType::Variant` automatically when the consumer enables the
  `variant_experimental` feature.

When upstream iceberg-rust 0.10/0.11 ships native variant support,
this whole block becomes redundant; remove the enum arm, the primitive
arm, and the metadata-key attachments together.

Spec ref: <https://iceberg.apache.org/spec/#variant> and
parquet's `LogicalType::Variant` (parquet-rs source
`src/arrow/schema/extension.rs::logical_type_for_struct`).
```

- [ ] **Step 2: Commit**

```bash
git add vendor/iceberg-0.9.0/PATCH.md
git commit -m "docs: document vendor PATCH 6 (variant) in PATCH.md"
```

---

## Task 5: `variant_write` module — helpers (`metadata_byte_len`, `variant_field_indices`)

**Files:**
- Create: `src/connector/iceberg/variant_write.rs`
- Modify: `src/connector/iceberg/mod.rs`

Reference for the variant byte layout: `src/exec/variant.rs::serialize` (`size: u32 LE | metadata bytes | value bytes`). Reference for metadata header parsing: `src/exec/variant.rs::load_metadata`.

- [ ] **Step 1: Declare the module**

In `src/connector/iceberg/mod.rs`, after the `pub(crate) mod data_writer;` line (currently line 23), add:

```rust
pub(crate) mod variant_write;
```

- [ ] **Step 2: Create the module file with the failing tests**

Create `src/connector/iceberg/variant_write.rs` with this initial contents (tests + skeleton functions). The tests are designed to fail because the implementations are stubs:

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
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Write-side glue for Iceberg v3 variant columns.
//!
//! NovaRocks carries variants in execution as a single `LargeBinary`
//! per column whose bytes are `[size:u32 LE | metadata | value]` (the
//! `VariantValue::serialize` form). Iceberg parquet writers expect the
//! parent column to be a `StructArray { metadata: BinaryArray (req),
//! value: BinaryArray (req) }`; this module bridges the two right
//! before `iceberg::ParquetWriter::write`.

use iceberg::spec::SchemaRef;

/// Returns the offsets within a `[metadata|value]` payload at which the
/// metadata segment ends. Mirrors the parsing in
/// `src/exec/variant.rs::load_metadata` but only computes the length —
/// it deliberately does not validate the value segment.
///
/// `payload` must be the bytes AFTER the leading `u32` size header.
pub(crate) fn metadata_byte_len(payload: &[u8]) -> Result<usize, String> {
    todo!("implement in Step 4")
}

/// Returns the *top-level* arrow indices in the iceberg current
/// schema that correspond to `PrimitiveType::Variant` fields. Order
/// matches `iceberg_schema.as_struct().fields()`.
pub(crate) fn variant_field_indices(iceberg_schema: &SchemaRef) -> Vec<usize> {
    todo!("implement in Step 6")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_metadata_empty() -> Vec<u8> {
        // Version 1, dict_size = 0, single offset entry of 0.
        vec![0x01, 0x00, 0x00]
    }

    #[test]
    fn metadata_byte_len_empty_dict() {
        let m = build_metadata_empty();
        let payload: Vec<u8> = m.iter().copied().chain([/* value */ 0x00].iter().copied()).collect();
        assert_eq!(metadata_byte_len(&payload).expect("ok"), m.len());
    }

    #[test]
    fn metadata_byte_len_rejects_short_input() {
        let err = metadata_byte_len(&[0x01]).expect_err("must reject");
        assert!(
            err.to_lowercase().contains("metadata") || err.to_lowercase().contains("short"),
            "{err}"
        );
    }

    #[test]
    fn variant_field_indices_finds_variant_columns() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                    NestedField::optional(3, "s", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "v2", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert_eq!(variant_field_indices(&schema), vec![1, 3]);
    }

    #[test]
    fn variant_field_indices_returns_empty_when_no_variants() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        assert!(variant_field_indices(&schema).is_empty());
    }
}
```

- [ ] **Step 3: Run tests, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::variant_write::tests`
Expected: failures — `todo!()` panics at runtime.

- [ ] **Step 4: Implement `metadata_byte_len`**

Replace the `todo!()` body with:

```rust
pub(crate) fn metadata_byte_len(payload: &[u8]) -> Result<usize, String> {
    // Mirror src/exec/variant.rs::load_metadata, but stop at the metadata
    // segment instead of returning the full slice.
    const HEADER: usize = 1;
    const VERSION_MASK: u8 = 0b0000_1111;
    const OFFSET_SIZE_MASK: u8 = 0b1100_0000;
    const OFFSET_SIZE_SHIFT: u8 = 6;

    if payload.len() < HEADER + 1 {
        return Err(format!(
            "variant metadata too short: {} bytes",
            payload.len()
        ));
    }
    let header = payload[0];
    let version = header & VERSION_MASK;
    if version != 1 {
        return Err(format!("unsupported variant metadata version: {version}"));
    }
    let offset_size = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    if !(1..=4).contains(&offset_size) {
        return Err(format!("invalid variant metadata offset size: {offset_size}"));
    }
    if payload.len() < HEADER + offset_size as usize {
        return Err("variant metadata too short to contain dict_size".to_string());
    }
    let dict_size = read_le_u32(&payload[HEADER..], offset_size)? as usize;
    let offset_list_offset = HEADER + offset_size as usize;
    let last_offset_pos = offset_list_offset + dict_size * offset_size as usize;
    if last_offset_pos + offset_size as usize > payload.len() {
        return Err("variant metadata too short to contain offset list".to_string());
    }
    let last_data_size = read_le_u32(&payload[last_offset_pos..], offset_size)? as usize;
    let data_offset = offset_list_offset + (1 + dict_size) * offset_size as usize;
    let end = data_offset + last_data_size;
    if end > payload.len() {
        return Err(format!(
            "variant metadata end {end} exceeds payload {}",
            payload.len()
        ));
    }
    Ok(end)
}

fn read_le_u32(data: &[u8], size: u8) -> Result<u32, String> {
    if size == 0 || size > 4 {
        return Err("invalid little-endian size".to_string());
    }
    if data.len() < size as usize {
        return Err("variant metadata: not enough bytes for u32 read".to_string());
    }
    let mut out: u32 = 0;
    for (i, byte) in data.iter().copied().enumerate().take(size as usize) {
        out |= (byte as u32) << (8 * i);
    }
    Ok(out)
}
```

- [ ] **Step 5: Run, expect `metadata_byte_len` tests pass**

Run: `cargo test -p novarocks --lib connector::iceberg::variant_write::tests::metadata_byte_len`
Expected: PASS for both metadata_byte_len tests.

- [ ] **Step 6: Implement `variant_field_indices`**

Replace the `todo!()` body with:

```rust
pub(crate) fn variant_field_indices(iceberg_schema: &SchemaRef) -> Vec<usize> {
    use iceberg::spec::{PrimitiveType, Type};
    iceberg_schema
        .as_struct()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, f)| match f.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Variant) => Some(idx),
            _ => None,
        })
        .collect()
}
```

- [ ] **Step 7: Run, expect all helper tests pass**

Run: `cargo test -p novarocks --lib connector::iceberg::variant_write::tests`
Expected: 4 tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/mod.rs src/connector/iceberg/variant_write.rs
git commit -m "feat(iceberg): add variant_write helpers (metadata_byte_len, variant_field_indices)"
```

---

## Task 6: `variant_write` — `transform_variant_columns_for_write`

**Files:**
- Modify: `src/connector/iceberg/variant_write.rs`

- [ ] **Step 1: Add tests for the transform**

Append to the `mod tests { … }` block in `src/connector/iceberg/variant_write.rs`:

```rust
fn build_variant_payload_with_string(s: &str) -> Vec<u8> {
    // Replicates VariantValue with a short-string value but no metadata
    // dict (dict_size = 0). Sufficient for round-trip tests.
    let metadata = build_metadata_empty();
    // Short-string value: basic_type = 1 (ShortString), header byte =
    // (len << 2) | 0b01.
    let mut value = Vec::with_capacity(1 + s.len());
    let len = s.len();
    assert!(len < 64, "test helper limit");
    value.push(((len as u8) << 2) | 0b01);
    value.extend_from_slice(s.as_bytes());

    let total = (metadata.len() + value.len()) as u32;
    let mut out = Vec::with_capacity(4 + metadata.len() + value.len());
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&metadata);
    out.extend_from_slice(&value);
    out
}

fn make_iceberg_schema(fields: Vec<iceberg::spec::NestedFieldRef>) -> iceberg::spec::SchemaRef {
    use std::sync::Arc;
    Arc::new(
        iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("schema"),
    )
}

fn make_annotated_arrow_schema(iceberg_schema: &iceberg::spec::SchemaRef) -> arrow::datatypes::SchemaRef {
    use std::sync::Arc;
    Arc::new(
        iceberg::arrow::schema_to_arrow_schema(iceberg_schema).expect("convert"),
    )
}

#[test]
fn transform_single_variant_column_one_row() {
    use arrow::array::{LargeBinaryArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;

    let iceberg_schema = make_iceberg_schema(vec![
        NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Variant)).into(),
    ]);
    let annotated = make_annotated_arrow_schema(&iceberg_schema);
    let raw = build_variant_payload_with_string("hi");
    let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::LargeBinary, true)]));
    let arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(arr)]).expect("batch");

    let out = transform_variant_columns_for_write(&batch, &annotated, &[0]).expect("ok");
    assert_eq!(out.num_columns(), 1);
    let col = out.column(0);
    let s = col.as_any().downcast_ref::<arrow::array::StructArray>().expect("struct");
    assert_eq!(s.fields().len(), 2);
    assert_eq!(s.fields()[0].name(), "metadata");
    assert_eq!(s.fields()[1].name(), "value");
    let meta_arr = s.column(0).as_any().downcast_ref::<arrow::array::BinaryArray>().expect("binary");
    let val_arr = s.column(1).as_any().downcast_ref::<arrow::array::BinaryArray>().expect("binary");
    // metadata = empty dict (3 bytes), value = short-string "hi" (1 + 2 bytes).
    assert_eq!(meta_arr.value(0), &[0x01, 0x00, 0x00]);
    assert_eq!(val_arr.value(0).len(), 3);
    assert_eq!(val_arr.value(0)[1..], *b"hi");
}

#[test]
fn transform_handles_null_row_with_zero_length_children() {
    use arrow::array::{LargeBinaryArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;

    let iceberg_schema = make_iceberg_schema(vec![
        NestedField::optional(1, "v", Type::Primitive(PrimitiveType::Variant)).into(),
    ]);
    let annotated = make_annotated_arrow_schema(&iceberg_schema);
    let raw = build_variant_payload_with_string("a");
    let input_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::LargeBinary, true)]));
    let arr = LargeBinaryArray::from(vec![Some(raw.as_slice()), None, Some(raw.as_slice())]);
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(arr)]).expect("batch");

    let out = transform_variant_columns_for_write(&batch, &annotated, &[0]).expect("ok");
    let s = out.column(0).as_any().downcast_ref::<arrow::array::StructArray>().expect("struct");
    assert_eq!(s.len(), 3);
    assert!(s.is_valid(0));
    assert!(!s.is_valid(1)); // parent null
    assert!(s.is_valid(2));
    let meta = s.column(0).as_any().downcast_ref::<arrow::array::BinaryArray>().expect("b");
    let val = s.column(1).as_any().downcast_ref::<arrow::array::BinaryArray>().expect("b");
    // Children must NOT be marked null at the leaf level (Required) —
    // null parent rows carry zero-length placeholders.
    assert!(meta.is_valid(1));
    assert!(val.is_valid(1));
    assert_eq!(meta.value(1), &[] as &[u8]);
    assert_eq!(val.value(1), &[] as &[u8]);
}

#[test]
fn transform_passes_through_non_variant_columns_unchanged() {
    use arrow::array::{Int32Array, LargeBinaryArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;

    let iceberg_schema = make_iceberg_schema(vec![
        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
    ]);
    let annotated = make_annotated_arrow_schema(&iceberg_schema);
    let raw = build_variant_payload_with_string("x");
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("v", DataType::LargeBinary, true),
    ]));
    let id_arr = Int32Array::from(vec![Some(7)]);
    let v_arr = LargeBinaryArray::from_iter_values([raw.as_slice()]);
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(id_arr), Arc::new(v_arr)])
        .expect("batch");
    let out = transform_variant_columns_for_write(&batch, &annotated, &[1]).expect("ok");
    assert_eq!(out.num_columns(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int32Array>().expect("i32");
    assert_eq!(id.value(0), 7);
    let v = out.column(1).as_any().downcast_ref::<arrow::array::StructArray>().expect("struct");
    assert_eq!(v.fields().len(), 2);
}

#[test]
fn transform_handles_two_adjacent_variant_columns() {
    use arrow::array::{LargeBinaryArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use std::sync::Arc;

    let iceberg_schema = make_iceberg_schema(vec![
        NestedField::optional(1, "v1", Type::Primitive(PrimitiveType::Variant)).into(),
        NestedField::optional(2, "v2", Type::Primitive(PrimitiveType::Variant)).into(),
    ]);
    let annotated = make_annotated_arrow_schema(&iceberg_schema);
    let raw1 = build_variant_payload_with_string("a");
    let raw2 = build_variant_payload_with_string("bcd");
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::LargeBinary, true),
        Field::new("v2", DataType::LargeBinary, true),
    ]));
    let v1 = LargeBinaryArray::from_iter_values([raw1.as_slice()]);
    let v2 = LargeBinaryArray::from_iter_values([raw2.as_slice()]);
    let batch = RecordBatch::try_new(input_schema, vec![Arc::new(v1), Arc::new(v2)]).expect("batch");
    let out = transform_variant_columns_for_write(&batch, &annotated, &[0, 1]).expect("ok");
    let s1 = out.column(0).as_any().downcast_ref::<arrow::array::StructArray>().unwrap();
    let s2 = out.column(1).as_any().downcast_ref::<arrow::array::StructArray>().unwrap();
    assert_eq!(s1.len(), 1);
    assert_eq!(s2.len(), 1);
}
```

- [ ] **Step 2: Add the function signature stub**

Add to `src/connector/iceberg/variant_write.rs` (above the `#[cfg(test)]` line):

```rust
use arrow::record_batch::RecordBatch;

pub(crate) fn transform_variant_columns_for_write(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
    variant_indices: &[usize],
) -> Result<RecordBatch, String> {
    todo!("implement in Step 4")
}
```

- [ ] **Step 3: Run tests, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::variant_write::tests::transform`
Expected: failures (todo! panic).

- [ ] **Step 4: Implement the transform**

Replace the `todo!` body with:

```rust
pub(crate) fn transform_variant_columns_for_write(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
    variant_indices: &[usize],
) -> Result<RecordBatch, String> {
    use std::collections::HashSet;
    use std::sync::Arc;
    use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, LargeBinaryArray, StructArray};
    use arrow::buffer::NullBuffer;

    if batch.num_columns() != annotated_schema.fields().len() {
        return Err(format!(
            "variant_write: column count mismatch: batch={} annotated={}",
            batch.num_columns(),
            annotated_schema.fields().len()
        ));
    }

    let variant_set: HashSet<usize> = variant_indices.iter().copied().collect();
    let mut out_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, col) in batch.columns().iter().enumerate() {
        if !variant_set.contains(&idx) {
            out_columns.push(col.clone());
            continue;
        }
        let lb = col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                format!(
                    "variant_write: column {idx} expected LargeBinary, got {:?}",
                    col.data_type()
                )
            })?;

        let n = lb.len();
        let mut meta_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut nulls = vec![true; n];

        for row in 0..n {
            if lb.is_null(row) {
                nulls[row] = false;
                meta_builder.append_value([] as &[u8]);
                value_builder.append_value([] as &[u8]);
                continue;
            }
            let raw = lb.value(row);
            if raw.len() < 4 {
                return Err(format!(
                    "variant_write: row {row} payload too short ({} bytes)",
                    raw.len()
                ));
            }
            let total = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
            if 4 + total > raw.len() {
                return Err(format!(
                    "variant_write: row {row} declared total {total} exceeds payload {}",
                    raw.len() - 4
                ));
            }
            let payload = &raw[4..4 + total];
            let m_len = metadata_byte_len(payload)?;
            meta_builder.append_value(&payload[..m_len]);
            value_builder.append_value(&payload[m_len..]);
        }

        let meta_arr: BinaryArray = meta_builder.finish();
        let value_arr: BinaryArray = value_builder.finish();
        let null_buffer = NullBuffer::from(nulls);

        // Use the annotated schema's variant field as the StructArray
        // type — this carries the `arrow.parquet.variant` extension metadata
        // that PATCH 6 attaches.
        let struct_field = annotated_schema.field(idx);
        let arrow::datatypes::DataType::Struct(child_fields) = struct_field.data_type() else {
            return Err(format!(
                "variant_write: annotated schema for variant index {idx} is not Struct"
            ));
        };
        let struct_arr = StructArray::new(
            child_fields.clone(),
            vec![Arc::new(meta_arr) as ArrayRef, Arc::new(value_arr) as ArrayRef],
            Some(null_buffer),
        );
        out_columns.push(Arc::new(struct_arr));
    }

    RecordBatch::try_new(annotated_schema.clone(), out_columns)
        .map_err(|e| format!("variant_write: rebuild RecordBatch: {e}"))
}
```

- [ ] **Step 5: Run, expect all transform tests pass**

Run: `cargo test -p novarocks --lib connector::iceberg::variant_write`
Expected: 8 tests PASS (4 helper tests from Task 5 + 4 transform tests).

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/variant_write.rs
git commit -m "feat(iceberg): variant_write transform LargeBinary -> Struct{metadata, value}"
```

---

## Task 7: Wire transform into `data_writer.rs` + integration round-trip test

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`

The function to modify is `write_record_batches_as_data_files_with_writer` at `src/connector/iceberg/data_writer.rs:74-139`. Two batch-loop bodies (unpartitioned, lines 87-94; partitioned, lines 114-137) both call `annotate_batch(&batch, …)`. We insert the transform before each `annotate_batch` call.

- [ ] **Step 1: Add an integration test that writes a variant batch and reads it back**

Append to the `#[cfg(test)] mod tests { … }` block at the bottom of `src/connector/iceberg/data_writer.rs`:

```rust
#[tokio::test]
async fn write_variant_column_round_trips_through_local_parquet() {
    use arrow::array::{Int32Array, LargeBinaryArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    let dir = tempdir().expect("tempdir");
    let location = format!("file://{}", dir.path().display());

    let iceberg_schema = Arc::new(
        iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
            ])
            .build()
            .expect("schema"),
    );
    let metadata = iceberg::spec::TableMetadataBuilder::new(
        iceberg_schema.as_ref().clone(),
        iceberg::spec::PartitionSpec::unpartition_spec(),
        iceberg::spec::SortOrder::unsorted_order(),
        location.clone(),
        iceberg::spec::FormatVersion::V3,
        std::collections::HashMap::new(),
    )
    .expect("builder")
    .build()
    .expect("metadata")
    .metadata;
    let table = iceberg::table::Table::builder()
        .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
        .file_io(iceberg::io::FileIO::from_path(location.as_str()).unwrap().build().unwrap())
        .metadata(metadata)
        .build()
        .expect("table");

    // Build a 1-row record batch where `v` holds a serialized variant
    // (short string "hello").
    let payload = {
        let metadata = vec![0x01u8, 0x00, 0x00];
        let mut value = Vec::new();
        let s = b"hello";
        value.push(((s.len() as u8) << 2) | 0b01);
        value.extend_from_slice(s);
        let total = (metadata.len() + value.len()) as u32;
        let mut out = Vec::new();
        out.extend_from_slice(&total.to_le_bytes());
        out.extend_from_slice(&metadata);
        out.extend_from_slice(&value);
        out
    };
    let input_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::LargeBinary, true),
    ]));
    let batch = RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(LargeBinaryArray::from_iter_values([payload.as_slice()])),
        ],
    )
    .expect("batch");

    let data_files = write_record_batches_as_data_files(&table, vec![batch])
        .await
        .expect("write");
    assert_eq!(data_files.len(), 1);
    let path = data_files[0].file_path().to_string();
    let on_disk = path.strip_prefix("file://").unwrap_or(&path);

    // Re-open the parquet file with the standard parquet-rs reader and
    // assert the physical layout matches the spec.
    let f = File::open(on_disk).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("builder");
    let parquet_schema = builder.parquet_schema();
    let v_node = parquet_schema
        .columns()
        .iter()
        .find(|c| c.path().string().starts_with("v"))
        .expect("v column");
    assert!(
        v_node.path().string() == "v.metadata" || v_node.path().string() == "v.value",
        "expected leaf path under v.*; got {}",
        v_node.path().string()
    );
    // Look at the parent group's logical type via the parquet schema descr.
    let root = builder.parquet_schema().root_schema();
    let v_field = root
        .get_fields()
        .iter()
        .find(|f| f.name() == "v")
        .expect("v");
    assert!(
        format!("{:?}", v_field.get_basic_info().logical_type()).to_lowercase().contains("variant"),
        "v parent group must carry LogicalType::Variant; got {:?}",
        v_field.get_basic_info().logical_type()
    );
}
```

If `tempfile` is not yet a dev-dependency, add it to `Cargo.toml`'s `[dev-dependencies]` section: `tempfile = "3"`. Verify by running `cargo build --tests`.

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::data_writer::tests::write_variant_column_round_trips`
Expected: failure — the unmodified writer produces a parquet file where `v` is a `LargeBinary` column rather than a `LogicalType::Variant` group.

- [ ] **Step 3: Wire `transform_variant_columns_for_write` into the unpartitioned loop**

In `src/connector/iceberg/data_writer.rs` near the function `write_record_batches_as_data_files_with_writer` (line 74), import the helpers near the top of the file:

```rust
use super::variant_write::{transform_variant_columns_for_write, variant_field_indices};
```

Replace the unpartitioned block (currently lines 82-106 — the `if metadata.default_partition_spec().fields().is_empty()` branch) with:

```rust
    let variant_indices = variant_field_indices(metadata.current_schema());

    if metadata.default_partition_spec().fields().is_empty() {
        let mut writer = data_file_builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let staged = if variant_indices.is_empty() {
                batch
            } else {
                transform_variant_columns_for_write(&batch, &annotated_schema, &variant_indices)?
            };
            writer
                .write(annotate_batch(&staged, &annotated_schema)?)
                .await
                .map_err(|e| format!("iceberg data file write failed: {e}"))?;
        }
        let data_files = writer
            .close()
            .await
            .map_err(|e| format!("iceberg data file writer close failed: {e}"))?;
        return data_files
            .into_iter()
            .map(|data_file| {
                retag_data_file_partition_spec_id(data_file, metadata.default_partition_spec_id())
            })
            .collect();
    }
```

- [ ] **Step 4: Wire transform into the partitioned loop**

Replace the partitioned block body (the `for batch in batches { … }` from line 114 onward) with:

```rust
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let staged = if variant_indices.is_empty() {
            batch
        } else {
            transform_variant_columns_for_write(&batch, &annotated_schema, &variant_indices)?
        };
        let annotated = annotate_batch(&staged, &annotated_schema)?;
        let partitioned = splitter
            .split(&annotated)
            .map_err(|e| format!("split iceberg batch by partition spec failed: {e}"))?;
        for (partition_key, partition_batch) in partitioned {
            let mut writer = data_file_builder
                .build(Some(partition_key))
                .await
                .map_err(|e| format!("build iceberg partitioned data file writer failed: {e}"))?;
            writer
                .write(partition_batch)
                .await
                .map_err(|e| format!("iceberg partitioned data file write failed: {e}"))?;
            data_files.extend(
                writer.close().await.map_err(|e| {
                    format!("iceberg partitioned data file writer close failed: {e}")
                })?,
            );
        }
    }
```

- [ ] **Step 5: Run, expect pass**

Run: `cargo test -p novarocks --lib connector::iceberg::data_writer::tests::write_variant_column_round_trips`
Expected: PASS. The on-disk parquet file's `v` group must show `LogicalType::Variant`.

- [ ] **Step 6: Run the full data_writer test module**

Run: `cargo test -p novarocks --lib connector::iceberg::data_writer`
Expected: all existing tests still pass plus the new round-trip.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg/data_writer.rs Cargo.toml
git commit -m "feat(iceberg): write variant columns as parquet LogicalType::Variant"
```

---

## Task 8: Validation — `ensure_no_variant_in_partition_spec` + `ensure_no_variant_in_sort_order`

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Add tests for the two new helpers**

Append to the `#[cfg(test)] mod tests { … }` block at the bottom of `src/connector/iceberg/commit/validation.rs`:

```rust
fn make_table_with(
    fields: Vec<iceberg::spec::NestedFieldRef>,
    partition_fields: Vec<iceberg::spec::PartitionField>,
    sort_fields: Vec<iceberg::spec::SortField>,
) -> iceberg::table::Table {
    use std::sync::Arc;
    let schema = Arc::new(
        iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("schema"),
    );
    let mut spec_builder = iceberg::spec::PartitionSpec::builder(schema.as_ref())
        .with_spec_id(0);
    for f in partition_fields {
        spec_builder = spec_builder.add_partition_field(&f.name, f.transform).expect("add");
    }
    let partition_spec = spec_builder.build().expect("spec");
    let mut order_builder = iceberg::spec::SortOrder::builder().with_order_id(0);
    for f in sort_fields {
        order_builder = order_builder.with_sort_field(f);
    }
    let sort_order = order_builder.build_unbound().expect("sort");
    let metadata = iceberg::spec::TableMetadataBuilder::new(
        schema.as_ref().clone(),
        partition_spec,
        sort_order,
        "file:///tmp/x".to_string(),
        iceberg::spec::FormatVersion::V3,
        std::collections::HashMap::new(),
    )
    .expect("builder")
    .build()
    .expect("metadata")
    .metadata;
    iceberg::table::Table::builder()
        .identifier(iceberg::TableIdent::from_strs(["d", "t"]).unwrap())
        .file_io(iceberg::io::FileIO::from_path("file:///tmp/x").unwrap().build().unwrap())
        .metadata(metadata)
        .build()
        .expect("table")
}

#[test]
fn ensure_no_variant_in_partition_spec_rejects_variant_partition_column() {
    use iceberg::spec::{NestedField, PartitionField, PrimitiveType, Transform, Type};
    let table = make_table_with(
        vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ],
        vec![PartitionField {
            source_id: 2,
            field_id: 1000,
            name: "v_part".to_string(),
            transform: Transform::Identity,
        }],
        vec![],
    );
    let err = ensure_no_variant_in_partition_spec(&table).expect_err("must reject");
    assert!(err.contains("'v'"), "{err}");
    assert!(err.contains("partition"), "{err}");
}

#[test]
fn ensure_no_variant_in_partition_spec_accepts_clean_table() {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let table = make_table_with(
        vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ],
        vec![],
        vec![],
    );
    ensure_no_variant_in_partition_spec(&table).expect("clean");
}

#[test]
fn ensure_no_variant_in_sort_order_rejects_variant_sort_column() {
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, SortDirection, SortField, Transform, Type,
    };
    let table = make_table_with(
        vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ],
        vec![],
        vec![SortField {
            source_id: 2,
            transform: Transform::Identity,
            direction: SortDirection::Ascending,
            null_order: NullOrder::First,
        }],
    );
    let err = ensure_no_variant_in_sort_order(&table).expect_err("must reject");
    assert!(err.contains("'v'"), "{err}");
    assert!(err.contains("sort"), "{err}");
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::ensure_no_variant_in`
Expected: failure — these helpers don't exist yet.

- [ ] **Step 3: Implement both helpers**

Add to `src/connector/iceberg/commit/validation.rs` (just below the existing `ensure_iceberg_write_supported` function, around line 64):

```rust
pub fn ensure_no_variant_in_partition_spec(table: &Table) -> Result<(), String> {
    use iceberg::spec::{PrimitiveType, Type};
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    for f in metadata.default_partition_spec().fields() {
        let source = schema.field_by_id(f.source_id).ok_or_else(|| {
            format!(
                "iceberg table partition field '{name}' references missing source id {sid}",
                name = f.name,
                sid = f.source_id
            )
        })?;
        if matches!(source.field_type.as_ref(), Type::Primitive(PrimitiveType::Variant)) {
            return Err(format!(
                "iceberg table column '{name}' is variant; variant columns cannot appear in the partition spec. \
                 Drop the partition transform on '{name}' before writing.",
                name = source.name,
            ));
        }
    }
    Ok(())
}

pub fn ensure_no_variant_in_sort_order(table: &Table) -> Result<(), String> {
    use iceberg::spec::{PrimitiveType, Type};
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    for f in metadata.default_sort_order().fields.iter() {
        let source = schema.field_by_id(f.source_id).ok_or_else(|| {
            format!(
                "iceberg table sort field references missing source id {}",
                f.source_id
            )
        })?;
        if matches!(source.field_type.as_ref(), Type::Primitive(PrimitiveType::Variant)) {
            return Err(format!(
                "iceberg table column '{name}' is variant; variant columns cannot appear in the sort order. \
                 Drop the sort key on '{name}' before writing.",
                name = source.name,
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Run, expect pass**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::ensure_no_variant_in`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/validation.rs
git commit -m "feat(iceberg): add precise variant guards for partition spec / sort order"
```

---

## Task 9: Validation — wire new guards + delete dead code

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Update `ensure_iceberg_write_supported` to call the new helpers and remove the blanket variant check**

Replace the existing function body (lines 60-63) with:

```rust
pub fn ensure_iceberg_write_supported(table: &Table) -> Result<IcebergWriteMode, String> {
    ensure_no_variant_in_partition_spec(table)?;
    ensure_no_variant_in_sort_order(table)?;
    Ok(classify_iceberg_write_mode(table))
}
```

- [ ] **Step 2: Update `ensure_update_requires_v3_row_lineage`**

That function currently calls `ensure_no_variant_columns(table)?;` (line 73). Remove that line entirely — variant checks for UPDATE are handled by the new mutation guard (Task 11). The body becomes:

```rust
pub fn ensure_update_requires_v3_row_lineage(table: &Table) -> Result<(), String> {
    let metadata = table.metadata();
    ensure_update_properties_require_v3_row_lineage(
        metadata.format_version(),
        metadata.properties(),
    )
}
```

- [ ] **Step 3: Delete `ensure_no_variant_columns` and `type_contains_variant`**

Delete the entire function bodies (currently lines 122-153) and any `use` lines they introduced that are now unused. Leave the surrounding `pub fn ensure_single_partition_spec` etc. intact.

- [ ] **Step 4: Run cargo build and chase down any breakage from removed functions**

Run: `cargo build`
Expected: clean. If anything else (downstream test or callsite) used `ensure_no_variant_columns` or `type_contains_variant`, the compiler will name it. Replace each callsite according to context — e.g. an INSERT-side caller is already handled by the new `ensure_iceberg_write_supported`; a row-level caller should be handled in the mutation guard tasks below, so for now just delete the obsolete call.

- [ ] **Step 5: Run validation module tests**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation`
Expected: all tests pass (the existing positive tests around `row_lineage_property_parser`, `write_mode_classifies_*`, `update_mode_*` should still pass — they don't depend on the deleted helpers).

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/commit/validation.rs
git commit -m "refactor(iceberg): replace blanket variant guard with precise spec checks"
```

---

## Task 10: Validation — accept `LargeBinary` for `Variant` in schema match

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

The function to change is `arrow_iceberg_types_compatible` at lines 277-285 (post-Task 9 numbering will drift; locate by name).

- [ ] **Step 1: Add a test**

Append to the `#[cfg(test)] mod tests { … }`:

```rust
#[test]
fn variant_iceberg_type_matches_largebinary_arrow_type() {
    use arrow::datatypes::DataType;
    use iceberg::spec::{PrimitiveType, Type};
    let iceberg_ty = Type::Primitive(PrimitiveType::Variant);
    assert!(arrow_iceberg_types_compatible(&DataType::LargeBinary, &iceberg_ty));
    assert!(!arrow_iceberg_types_compatible(&DataType::Binary, &iceberg_ty));
    assert!(!arrow_iceberg_types_compatible(&DataType::Utf8, &iceberg_ty));
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::variant_iceberg_type_matches_largebinary`
Expected: failure — the existing function delegates to iceberg's `type_to_arrow_type` which now returns Struct.

- [ ] **Step 3: Insert the variant special case**

Replace `fn arrow_iceberg_types_compatible` body with:

```rust
fn arrow_iceberg_types_compatible(
    arrow_ty: &arrow::datatypes::DataType,
    iceberg_ty: &iceberg::spec::Type,
) -> bool {
    use iceberg::spec::{PrimitiveType, Type};
    if matches!(iceberg_ty, Type::Primitive(PrimitiveType::Variant)) {
        // NovaRocks execution layer carries variants as LargeBinary
        // (see src/lower/type_lowering.rs:89,170). The full struct shape
        // is materialized later by transform_variant_columns_for_write.
        return matches!(arrow_ty, arrow::datatypes::DataType::LargeBinary);
    }
    match iceberg::arrow::type_to_arrow_type(iceberg_ty) {
        Ok(expected) => &expected == arrow_ty,
        Err(_) => false,
    }
}
```

- [ ] **Step 4: Run, expect pass**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::variant_iceberg_type_matches_largebinary`
Expected: PASS.

- [ ] **Step 5: Run the full validation test module**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/commit/validation.rs
git commit -m "feat(iceberg): allow LargeBinary input column to match variant target"
```

---

## Task 11: Mutation guard helper

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Add a test**

Append to the validation test module:

```rust
#[test]
fn ensure_no_variant_columns_for_row_level_mutation_rejects_variant_table() {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let table = make_table_with(
        vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
        ],
        vec![],
        vec![],
    );
    let err = ensure_no_variant_columns_for_row_level_mutation(&table).expect_err("reject");
    assert!(err.contains("variant"), "{err}");
    assert!(err.contains("INSERT"), "{err}");
}

#[test]
fn ensure_no_variant_columns_for_row_level_mutation_accepts_plain_table() {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    let table = make_table_with(
        vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ],
        vec![],
        vec![],
    );
    ensure_no_variant_columns_for_row_level_mutation(&table).expect("ok");
}
```

- [ ] **Step 2: Run, expect failure**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::ensure_no_variant_columns_for_row_level_mutation`
Expected: failure — function does not exist.

- [ ] **Step 3: Implement the helper**

Add to `src/connector/iceberg/commit/validation.rs` (just above `ensure_no_variant_in_partition_spec`):

```rust
/// Used by the four non-INSERT write entry points (DELETE, UPDATE / MERGE,
/// INSERT OVERWRITE, ADD EQUALITY DELETE) to reject variant-bearing tables
/// while only the INSERT happy path supports them.
pub fn ensure_no_variant_columns_for_row_level_mutation(table: &Table) -> Result<(), String> {
    use iceberg::spec::{PrimitiveType, Type};
    let schema = table.metadata().current_schema();
    for f in schema.as_struct().fields() {
        if matches!(f.field_type.as_ref(), Type::Primitive(PrimitiveType::Variant)) {
            return Err(format!(
                "iceberg table column '{name}' is variant; row-level mutation of variant tables is not supported in this release. \
                 INSERT (without OVERWRITE) is supported.",
                name = f.name,
            ));
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Run, expect pass**

Run: `cargo test -p novarocks --lib connector::iceberg::commit::validation::tests::ensure_no_variant_columns_for_row_level_mutation`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/validation.rs
git commit -m "feat(iceberg): add row-level mutation guard for variant tables"
```

---

## Task 12: Wire mutation guard into DELETE

**Files:**
- Modify: `src/engine/delete_flow.rs`

- [ ] **Step 1: Locate the entry point**

The DELETE entry function currently calls `classify_sql_delete_strategy(&table)?;` at line 125 (verified). The guard must execute before that call so a variant table fails with the precise message rather than the generic delete-strategy classification.

- [ ] **Step 2: Add the import**

Add to the existing `use super::*` / `use crate::connector::iceberg::commit::*` import block at the top of `src/engine/delete_flow.rs`. If the file already imports symbols from `commit::validation` via the re-export at `src/connector/iceberg/commit/mod.rs`, add `ensure_no_variant_columns_for_row_level_mutation` to the re-export list there. Otherwise, add directly:

```rust
use crate::connector::iceberg::commit::validation::ensure_no_variant_columns_for_row_level_mutation;
```

(Verify the import path by reading the existing imports first; pick the style that matches.)

- [ ] **Step 3: Insert the guard before `classify_sql_delete_strategy`**

Edit lines 124-125:

```rust
    // 3. Validation.
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("DELETE: {e}"))?;
    let delete_strategy = classify_sql_delete_strategy(&table)?;
```

- [ ] **Step 4: Confirm the project still builds**

Run: `cargo build`
Expected: clean build.

- [ ] **Step 5: Confirm existing delete tests still pass**

Run: `cargo test -p novarocks --lib engine::delete`
Expected: pass — no test in this module exercises a variant table, so the new guard never fires.

- [ ] **Step 6: Commit**

```bash
git add src/engine/delete_flow.rs src/connector/iceberg/commit/mod.rs
git commit -m "feat(iceberg): fail-fast DELETE on variant tables"
```

(Drop `src/connector/iceberg/commit/mod.rs` from the add list if you didn't need to touch it.)

---

## Task 13: Wire mutation guard into UPDATE / MERGE

**Files:**
- Modify: `src/engine/mutation_flow.rs`

The UPDATE entry calls `select_iceberg_update_mode(&table)?;` at line 102. The MERGE entry calls it again at line 1229. We add the guard before each.

- [ ] **Step 1: Add the import**

Mirror Task 12 step 2 — either through the re-export or a direct import.

- [ ] **Step 2: Guard the UPDATE entry**

Before the existing line 102 (`let mode = select_iceberg_update_mode(&table)?;`), insert:

```rust
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("UPDATE: {e}"))?;
```

- [ ] **Step 3: Guard the MERGE entry**

Before the existing line 1229 (`let _ = select_iceberg_update_mode(&table)?;`), insert:

```rust
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("MERGE INTO: {e}"))?;
```

- [ ] **Step 4: Build & test**

Run: `cargo build && cargo test -p novarocks --lib engine::mutation`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mutation_flow.rs
git commit -m "feat(iceberg): fail-fast UPDATE / MERGE on variant tables"
```

---

## Task 14: Wire mutation guard into INSERT OVERWRITE

**Files:**
- Modify: `src/engine/iceberg_writer.rs`

The OVERWRITE branch is at `src/engine/iceberg_writer.rs:97-100`. The guard goes inside the `if overwrite { … }` block — INSERT (no overwrite) must remain allowed.

- [ ] **Step 1: Add the import**

Mirror Task 12 step 2.

- [ ] **Step 2: Insert the guard inside the OVERWRITE branch**

Replace lines 97-100 with:

```rust
    if overwrite {
        ensure_no_variant_columns_for_row_level_mutation(&table)
            .map_err(|e| format!("INSERT OVERWRITE: {e}"))?;
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }
```

- [ ] **Step 3: Build & test**

Run: `cargo build && cargo test -p novarocks --lib engine::iceberg_writer`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/iceberg_writer.rs
git commit -m "feat(iceberg): fail-fast INSERT OVERWRITE on variant tables"
```

---

## Task 15: Wire mutation guard into ADD EQUALITY DELETE

**Files:**
- Modify: `src/engine/equality_delete_flow.rs`

The entry function `execute_add_equality_delete_statement` loads the iceberg table at line 70. The guard goes immediately after the load.

- [ ] **Step 1: Add the import**

Mirror Task 12 step 2.

- [ ] **Step 2: Insert the guard**

After line 70 (`let table = block_on_iceberg(...)`) and before the existing `let metadata = table.metadata();` at line 72, insert:

```rust
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("ADD EQUALITY DELETE: {e}"))?;
```

- [ ] **Step 3: Build & test**

Run: `cargo build && cargo test -p novarocks --lib engine::equality_delete`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/equality_delete_flow.rs
git commit -m "feat(iceberg): fail-fast ADD EQUALITY DELETE on variant tables"
```

---

## Task 16: SQL test — positive variant INSERT round-trip

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_variant_insert.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_variant_insert.result`

The sql-test runner uses `${case_db}` to scope tables per case. Existing v3 fixtures (e.g. `sql-tests/iceberg/sql/iceberg_v3_default_insert_select.sql`) demonstrate the conventions: `DROP TABLE IF EXISTS … FORCE; CREATE TABLE … TBLPROPERTIES ("format-version" = "3"); INSERT …; SELECT …; DROP TABLE … FORCE;`.

- [ ] **Step 1: Write the SQL fixture**

Create `sql-tests/iceberg/sql/iceberg_v3_variant_insert.sql`:

```sql
-- @order_sensitive=true
-- Test Point: INSERT into a v3 iceberg table with a VARIANT column
-- round-trips through parquet write + read.
-- Method: CREATE … (id INT, v VARIANT) USING iceberg WITH ("format-version"="3"),
--         INSERT VALUES with parse_json, SELECT id, v.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant FORCE;
CREATE TABLE ${case_db}.t_v3_variant (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_variant VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10, 20, 30]')),
  (3, parse_json('null')),
  (4, NULL);

-- query 2
SELECT id, v FROM ${case_db}.t_v3_variant ORDER BY id;

-- query 3
SELECT id, get_json_string(v, '$.b') FROM ${case_db}.t_v3_variant WHERE id = 1;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant FORCE;
```

- [ ] **Step 2: Start a standalone server in another terminal (or background)**

Run: `NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030`
(Use `--release` for the SQL suite per CLAUDE.md §8.2.)
Wait until the server logs `serving on port 9030`.

- [ ] **Step 3: Record the result baseline**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_variant_insert --mode record
```
Expected: `sql-tests/iceberg/result/iceberg_v3_variant_insert.result` is created. Open it and visually confirm:
- query 2 has rows id=1..4, ordered ascending; row 4 shows NULL or empty for v.
- query 3 returns `"x"`.

If the recorded output looks wrong (e.g. variant-decoder emits placeholder for a row that should be valid), DO NOT commit the bad result — go back and debug the writer/reader chain.

- [ ] **Step 4: Verify reproducibility**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_variant_insert --mode verify
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_variant_insert.sql \
        sql-tests/iceberg/result/iceberg_v3_variant_insert.result
git commit -m "test(iceberg): positive sql-test for v3 variant INSERT round-trip"
```

---

## Task 17: SQL test — negative cases

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_variant_unsupported.sql`
- Create: `sql-tests/iceberg/result/iceberg_v3_variant_unsupported.result`

- [ ] **Step 1: Write the negative-case SQL fixture**

Create `sql-tests/iceberg/sql/iceberg_v3_variant_unsupported.sql`:

```sql
-- @order_sensitive=true
-- Test Point: variant tables reject DELETE / UPDATE / MERGE / INSERT
-- OVERWRITE / ADD EQUALITY DELETE with the matching guard message;
-- partition-by-variant and sort-by-variant DDL is rejected on writes.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_neg FORCE;
CREATE TABLE ${case_db}.t_v3_variant_neg (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_variant_neg VALUES (1, parse_json('{"a":1}'));

-- query 2
-- @expect_error=variant
DELETE FROM ${case_db}.t_v3_variant_neg WHERE id = 1;

-- query 3
-- @expect_error=variant
UPDATE ${case_db}.t_v3_variant_neg SET id = 2 WHERE id = 1;

-- query 4
-- @expect_error=variant
INSERT OVERWRITE ${case_db}.t_v3_variant_neg VALUES (5, parse_json('{}'));

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant_neg FORCE;
```

If the runner does not support `--expect_error`, replace each `@expect_error=variant` line with `-- @skip_result_check=true` and visually verify the recorded `.result` file shows the `variant` error string.

- [ ] **Step 2: Record + verify**

Mirror Task 16 steps 2-4, substituting `iceberg_v3_variant_unsupported`.

Open the recorded `.result` file and confirm each negative query yields a message containing `variant` and the expected path label (`DELETE`, `UPDATE`, `INSERT OVERWRITE`).

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_variant_unsupported.sql \
        sql-tests/iceberg/result/iceberg_v3_variant_unsupported.result
git commit -m "test(iceberg): negative sql-tests for unsupported variant write paths"
```

---

## Task 18: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: no diff. If anything reformats, stage and commit with `style: cargo fmt`.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. Address any new warnings the variant changes introduced.

- [ ] **Step 3: Full test suite**

Run: `cargo test`
Expected: all tests pass.

- [ ] **Step 4: SQL suite verify**

Make sure the standalone-server is running (release build per CLAUDE.md §8.4):

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030
```

Then run the iceberg suite in verify mode:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify -j 4
```

Expected: PASS, including the two new variant cases.

- [ ] **Step 5: Final commit (if any cleanup needed)**

If `cargo fmt` or clippy fixes left a residual diff, commit:

```bash
git add -A
git commit -m "style: cargo fmt / clippy clean-up after variant write path"
```

If there is no residual diff, no commit is needed.

---

## Self-Review Checklist (already performed by plan author — kept for reference)

- **Spec coverage:** every spec section maps to a task. § 1 architecture → all tasks; § 2.1 PATCH 6 → Tasks 2-3; § 2.2 PATCH 7 → Task 1; § 2.3 root Cargo.toml → Task 1; § 3 transform → Tasks 5-7; § 4 validation → Tasks 8-9; § 5 schema match → Task 10; § 6 mutation guards → Tasks 11-15; § 7 testing → Tasks 16-17; § 8 compatibility notes → covered through verifications in Task 7 and the sql-tests in Task 16.
- **Placeholder scan:** none. Every code/diff block is literal. The "spike for slot_types" mentioned in spec § 5 is explicitly deferred and documented in the spec itself; the plan adopts the default behavior unconditionally.
- **Type consistency:** function names (`metadata_byte_len`, `variant_field_indices`, `transform_variant_columns_for_write`, `ensure_no_variant_in_partition_spec`, `ensure_no_variant_in_sort_order`, `ensure_no_variant_columns_for_row_level_mutation`) are consistent across all task references.
