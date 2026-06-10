# IV3-6 PR-2: `variant_get` / `try_variant_get` Functions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Spark-aligned `variant_get(v, path[, type])` and `try_variant_get(v, path[, type])` scalar functions, kernel-backed via `parquet-variant-compute`, with literal-driven return types — the SQL surface and single-semantics evaluator the later optimizer pushdown (PR-4) rewrites onto.

**Architecture:** A new exec module `src/exec/expr/function/variant/variant_get.rs` converts the engine-internal `LargeBinary [size|metadata|value]` rows into an upstream `VariantArray` (zero-copy slicing via a new `split_serialized` helper in `src/exec/variant.rs`), then evaluates the upstream `variant_get` kernel with `GetOptions{path, as_type, cast_options}`. Strictness differs only in `CastOptions.safe`: `variant_get` errors on cast failure, `try_variant_get` yields NULL (Spark semantics). Path and type arguments must be string literals (read from `ExprArena::node`, not evaluated — Spark requires foldable args too). Return-type inference: analyzer base entry `LargeBinary` + a literal-driven post-pass in `resolve_expr.rs` (the `named_struct` precedent); codegen takes the analyzer's `type_hint` (verified: hint overrides at `expr_compiler.rs:1256-1261`), needing only a legacy-table entry so the eager fallback doesn't error on the unknown name.

**Tech Stack:** Rust; `parquet::variant` re-exports (`VariantArray`, `variant_get`, `GetOptions`, `json_to_variant`, `unshred_variant`, `VariantPath`, `VariantPathElement` — all available via the existing `parquet = { version = "58.2.0", features = ["arrow", "variant_experimental"] }`); `arrow::compute::CastOptions`.

**Background you must know (verified, with refs):**
- Exec dispatch: `FunctionKind::Variant(&'static str)`; `VARIANT_FUNCTIONS` alias table + `VARIANT_METADATA` arg counts + `eval_variant_function` match (`src/exec/expr/function/variant/dispatch.rs`). Both tables must be updated together (missing metadata for a registered name panics, `src/exec/expr/function/mod.rs:456-466`).
- `parse_json` exec **returns normalized JSON Utf8 text, not variant binary** (`src/exec/expr/function/variant/parse_json.rs:74-100`; analyzer type Utf8 at `src/sql/analyzer/functions.rs:1063`). Therefore `variant_get(parse_json(s), ...)` arrives with a Utf8 arg0 — the JSON-string input branch is mandatory, implemented via the upstream `json_to_variant` kernel.
- Analyzer return types: `infer_scalar_return_type(name, arg_types)` table at `src/sql/analyzer/functions.rs:1060-1064`, unknown defaults to Utf8 (permissive — no existence gate). Literal-driven refinement precedent: `named_struct` / `round` post-passes in `src/sql/analyzer/resolve_expr.rs:2055-2101`, where `args_typed` (analyzer IR, `ExprKind::Literal(LiteralValue::String(_))`) is in scope.
- Codegen: `infer_scalar_function_return_type` (legacy table) is called eagerly with `?` (`src/sql/codegen/expr_compiler.rs:1250`) — an unknown name errors even though the analyzer `type_hint` would override afterwards (`:1256-1261`). So add legacy entries; do NOT add signature-registry entries (`src/sql/functions/registry.rs`) — the registry has no LargeBinary `TypeSpec` and an entry would drag the two functions into the registry-vs-legacy parity test for no benefit.
- Lowering arg validation: variant allow-list block in `src/lower/expr/function_call.rs:345-380` (assumes exactly 2 args) — add a separate block for the 2-or-3-arg forms.
- Exec literal access: `ExprArena::node(id) -> Option<&ExprNode>` (`src/exec/expr/mod.rs:158`); exec literal string variant is `LiteralValue::Utf8(String)` (`src/exec/expr/mod.rs:56`). Analyzer literal string variant is `LiteralValue::String(String)` (different enum — do not confuse the two).
- Kernel facts (spike-verified 2026-06-10): `variant_get(&ArrayRef /*StructArray*/, GetOptions)` is shredding-aware and row-identical across shredded/unshredded inputs; `Variant::Null` always yields NULL (never an error) even in strict mode; cast failures yield NULL when `CastOptions.safe=true`, `ArrowError::CastError` when `safe=false`. `VariantPathElement::field(...)` / `::index(...)` constructors exist (`parquet-variant-58.2.0/src/path.rs:174-181`).
- Test harness: `tests/function/variant.rs` (wired via `tests/function/main.rs`), helpers `variant_primitive_serialized(type_id, payload)` (primitive variant bytes; type id 6 = Int64, 7 = Double) and `make_variant_chunk` (single-row LargeBinary chunk + arena).

**Commit policy:** every task ends in its own commit. Full gate in the final task.

---

### Task 1: `split_serialized` zero-copy helper in `src/exec/variant.rs`

**Files:**
- Modify: `src/exec/variant.rs`

- [ ] **Step 1: Write the failing test**

`src/exec/variant.rs` has a `#[cfg(test)] mod tests` at the bottom (it contains tests like the metadata/path ones); append:

```rust
    #[test]
    fn split_serialized_round_trips_create_inputs() {
        let metadata = VariantMetadata::empty();
        let value = vec![6u8 << 2, 123, 0, 0, 0, 0, 0, 0, 0]; // int64 primitive 123
        let v = VariantValue::create(metadata.raw(), &value).expect("create");
        let serialized = v.serialize();
        let (m, val) = split_serialized(&serialized).expect("split");
        assert_eq!(m, metadata.raw());
        assert_eq!(val, value.as_slice());
    }

    #[test]
    fn split_serialized_rejects_truncated_input() {
        assert!(split_serialized(&[0u8, 0, 0]).is_err());
        // size header claims more bytes than present
        assert!(split_serialized(&[200u8, 0, 0, 0, 1]).is_err());
    }
```

Run: `cargo test --lib exec::variant -- split_serialized --nocapture`
Expected: FAIL to compile — `split_serialized` not defined.

- [ ] **Step 2: Implement, and DRY `from_serialized` onto it**

In `src/exec/variant.rs`, add a free function next to `VariantValue` and refactor `from_serialized` (currently lines ~203-229) to delegate:

```rust
/// Split the engine-internal serialized variant form
/// `[size:u32 LE | metadata | value]` into zero-copy `(metadata, value)`
/// slices. Validation mirrors `VariantValue::from_serialized`.
pub fn split_serialized(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if data.len() < 4 {
        return Err("Invalid variant slice: too small to contain size header".to_string());
    }
    let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if size > VARIANT_MAX_SIZE {
        return Err(format!(
            "Variant size exceeds maximum limit: {} > {}",
            size, VARIANT_MAX_SIZE
        ));
    }
    if size > data.len().saturating_sub(4) {
        return Err(format!(
            "Invalid variant size: {} exceeds available data: {}",
            size,
            data.len().saturating_sub(4)
        ));
    }
    let payload = &data[4..4 + size];
    let metadata = load_metadata(payload)?;
    let metadata_len = metadata.len();
    Ok((&payload[..metadata_len], &payload[metadata_len..]))
}
```

And `from_serialized` becomes:

```rust
    pub fn from_serialized(data: &[u8]) -> Result<Self, String> {
        let (metadata, value) = split_serialized(data)?;
        Self::create(metadata, value)
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test --lib exec::variant -- --nocapture`
Expected: the 2 new tests PASS and all existing variant tests still PASS (from_serialized refactor is behavior-preserving).

- [ ] **Step 4: Commit**

```bash
git add src/exec/variant.rs
git commit -m "feat(variant): zero-copy split_serialized helper; from_serialized delegates to it"
```

---

### Task 2: Kernel-backed eval module `variant_get.rs`

**Files:**
- Create: `src/exec/expr/function/variant/variant_get.rs`
- Modify: `src/exec/expr/function/variant/mod.rs` (declare + export)

- [ ] **Step 1: Create the module**

`src/exec/expr/function/variant/variant_get.rs` (license header as siblings):

```rust
// (Apache 2.0 license header — copy verbatim from dispatch.rs)

//! Spark-aligned `variant_get` / `try_variant_get`, evaluated through the
//! upstream parquet-variant-compute kernel so expression-layer results are
//! row-identical with the scan-layer shredded fast path (IV3-6 decision B).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBufferBuilder, LargeBinaryArray, LargeBinaryBuilder,
    StringArray, StructArray, new_empty_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, TimeUnit};
use parquet::variant::{
    GetOptions, VariantArray, json_to_variant, unshred_variant, variant_get as kernel_variant_get,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::variant::{
    VariantPathSegment, VariantValue, parse_variant_path, split_serialized,
};

/// Map a `variant_get` type-string literal to the engine arrow type.
/// v1 whitelist per the IV3-6 design (decision E + §4).
pub fn variant_get_target_type(type_str: &str) -> Result<DataType, String> {
    match type_str.trim().to_ascii_lowercase().as_str() {
        "boolean" | "bool" => Ok(DataType::Boolean),
        "int" | "integer" | "int32" => Ok(DataType::Int32),
        "bigint" | "long" | "int64" => Ok(DataType::Int64),
        "float" | "float32" => Ok(DataType::Float32),
        "double" | "float64" => Ok(DataType::Float64),
        "string" | "varchar" => Ok(DataType::Utf8),
        "date" => Ok(DataType::Date32),
        "datetime" | "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => Err(format!(
            "variant_get: unsupported type '{other}' \
             (supported: boolean, int, bigint, float, double, string, date, datetime)"
        )),
    }
}

/// Read a required string-literal argument directly from the arena.
/// Spark requires these arguments to be foldable; we require literals.
fn literal_utf8_arg(arena: &ExprArena, id: ExprId, what: &str) -> Result<String, String> {
    match arena.node(id) {
        Some(ExprNode::Literal(LiteralValue::Utf8(s))) => Ok(s.clone()),
        _ => Err(format!(
            "variant_get requires a constant string literal for the {what} argument"
        )),
    }
}

fn kernel_path(path_str: &str) -> Result<parquet::variant::VariantPath<'static>, String> {
    let parsed = parse_variant_path(path_str)?;
    let elems: Vec<parquet::variant::VariantPathElement<'static>> = parsed
        .segments
        .iter()
        .map(|seg| match seg {
            VariantPathSegment::ObjectKey(k) => {
                parquet::variant::VariantPathElement::field(k.clone())
            }
            VariantPathSegment::ArrayIndex(i) => {
                parquet::variant::VariantPathElement::index(*i as usize)
            }
        })
        .collect();
    Ok(parquet::variant::VariantPath::from(elems))
}

fn binary_value_at_any(arr: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    use arrow::array::{BinaryArray, BinaryViewArray};
    if arr.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(a.value(row)));
    }
    Err(format!(
        "variant_get: expected binary metadata/value arrays, got {:?}",
        arr.data_type()
    ))
}

/// Engine LargeBinary `[size|metadata|value]` rows -> upstream VariantArray.
fn large_binary_to_variant_array(arr: &LargeBinaryArray) -> Result<VariantArray, String> {
    let mut metadata_builder = BinaryBuilder::new();
    let mut value_builder = BinaryBuilder::new();
    let mut validity = BooleanBufferBuilder::new(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            metadata_builder.append_value([]);
            value_builder.append_value([]);
            validity.append(false);
            continue;
        }
        let (m, v) = split_serialized(arr.value(row))
            .map_err(|e| format!("variant_get: invalid variant input at row {row}: {e}"))?;
        metadata_builder.append_value(m);
        value_builder.append_value(v);
        validity.append(true);
    }
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let struct_arr = StructArray::new(
        fields,
        vec![
            Arc::new(metadata_builder.finish()) as ArrayRef,
            Arc::new(value_builder.finish()) as ArrayRef,
        ],
        Some(NullBuffer::new(validity.finish())),
    );
    VariantArray::try_new(&struct_arr)
        .map_err(|e| format!("variant_get: failed to assemble variant input: {e}"))
}

/// Upstream VariantArray -> engine LargeBinary rows (2-arg return form).
fn variant_array_to_large_binary(va: VariantArray) -> Result<ArrayRef, String> {
    let va = if va.typed_value_field().is_some() {
        unshred_variant(&va).map_err(|e| format!("variant_get: unshred failed: {e}"))?
    } else {
        va
    };
    let metadata = va.metadata_field().clone();
    let value = va
        .value_field()
        .cloned()
        .ok_or_else(|| "variant_get: kernel result missing value column".to_string())?;
    let len = metadata.len();
    let mut builder = LargeBinaryBuilder::new();
    for row in 0..len {
        // Row-level nullability lives on the VariantArray's inner struct.
        if va.is_null(row) {
            builder.append_null();
            continue;
        }
        let (m, v) = match (
            binary_value_at_any(&metadata, row)?,
            binary_value_at_any(&value, row)?,
        ) {
            (Some(m), Some(v)) => (m, v),
            _ => {
                builder.append_null();
                continue;
            }
        };
        let serialized = VariantValue::create(m, v)
            .map_err(|e| format!("variant_get: kernel result row {row}: {e}"))?
            .serialize();
        builder.append_value(serialized.as_slice());
    }
    Ok(Arc::new(builder.finish()))
}

fn eval_variant_get_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    strict: bool,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    if !(2..=3).contains(&args.len()) {
        return Err(format!(
            "{fn_name} expects 2 or 3 arguments, got {}",
            args.len()
        ));
    }
    let path_str = literal_utf8_arg(arena, args[1], "path")?;
    let target_type = if args.len() == 3 {
        Some(variant_get_target_type(&literal_utf8_arg(
            arena, args[2], "type",
        )?)?)
    } else {
        None
    };
    let result_type = target_type.clone().unwrap_or(DataType::LargeBinary);
    if chunk.len() == 0 {
        return Ok(new_empty_array(&result_type));
    }

    let input = arena.eval(args[0], chunk)?;
    let variant_array = if let Some(bin) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        large_binary_to_variant_array(bin)?
    } else if let Some(json) = input.as_any().downcast_ref::<StringArray>() {
        // JSON-string input mode (e.g. variant_get(parse_json(s), ...)).
        // Malformed JSON is a query error in both strict and try modes —
        // try_ only relaxes the *cast*, mirroring Spark.
        let json_ref: ArrayRef = Arc::new(json.clone());
        json_to_variant(&json_ref).map_err(|e| format!("{fn_name}: invalid JSON input: {e}"))?
    } else {
        return Err(format!(
            "{fn_name} expects VARIANT or JSON/VARCHAR as first argument, got {:?}",
            input.data_type()
        ));
    };

    let mut opts = GetOptions::new_with_path(kernel_path(&path_str)?).with_cast_options(
        CastOptions {
            safe: !strict,
            format_options: Default::default(),
        },
    );
    if let Some(dt) = &target_type {
        opts = opts.with_as_type(Some(Arc::new(Field::new("", dt.clone(), true))));
    }

    let input_ref: ArrayRef = Arc::new(variant_array.into_inner());
    let result = kernel_variant_get(&input_ref, opts).map_err(|e| format!("{fn_name}: {e}"))?;

    match target_type {
        Some(_) => Ok(result),
        None => {
            let va = VariantArray::try_new(result.as_ref())
                .map_err(|e| format!("{fn_name}: unexpected kernel result: {e}"))?;
            variant_array_to_large_binary(va)
        }
    }
}

pub fn eval_variant_get(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_variant_get_impl(arena, args, chunk, true, "variant_get")
}

pub fn eval_try_variant_get(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_variant_get_impl(arena, args, chunk, false, "try_variant_get")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_type_whitelist() {
        assert_eq!(variant_get_target_type("bigint"), Ok(DataType::Int64));
        assert_eq!(variant_get_target_type(" INT "), Ok(DataType::Int32));
        assert_eq!(variant_get_target_type("string"), Ok(DataType::Utf8));
        assert_eq!(
            variant_get_target_type("datetime"),
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert!(variant_get_target_type("decimal(10,2)").is_err());
        assert!(variant_get_target_type("variant").is_err());
    }
}
```

- [ ] **Step 2: Declare and export in `mod.rs`**

In `src/exec/expr/function/variant/mod.rs`, add `mod variant_get;` to the module list and append to the exports:

```rust
pub use variant_get::{eval_try_variant_get, eval_variant_get, variant_get_target_type};
```

- [ ] **Step 3: Compile and run the module unit test**

Run: `cargo test --lib exec::expr::function::variant::variant_get -- --nocapture`
Expected: `target_type_whitelist` PASS, everything compiles. (If `VariantArray::is_null` doesn't exist on 58.2, swap `va.is_null(row)` for keeping a clone of the inner struct: `let inner = va.clone().into_inner();` before the loop and use `inner.is_null(row)` — semantics identical.)

- [ ] **Step 4: Commit**

```bash
git add src/exec/expr/function/variant/variant_get.rs src/exec/expr/function/variant/mod.rs
git commit -m "feat(variant): kernel-backed variant_get/try_variant_get eval module"
```

---

### Task 3: Dispatch wiring + behavior tests

**Files:**
- Modify: `src/exec/expr/function/variant/dispatch.rs`
- Test: `tests/function/variant.rs`

- [ ] **Step 1: Write the failing tests**

Append to `tests/function/variant.rs` (helpers `variant_primitive_serialized`, `make_variant_chunk`, `slot_id_expr`, `common::typed_null` already exist in this file; exec literal enum is `LiteralValue::Utf8`):

```rust
fn utf8_lit(arena: &mut ExprArena, s: &str) -> ExprId {
    arena.push(ExprNode::Literal(LiteralValue::Utf8(s.to_string())))
}

#[test]
fn test_variant_get_bigint_root() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = utf8_lit(&mut arena, "$");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let out = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 123);
}

#[test]
fn test_variant_get_two_arg_returns_variant() {
    use novarocks::exec::variant::{VariantValue, variant_to_i64};
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = utf8_lit(&mut arena, "$");
    let expr = common::typed_null(&mut arena, DataType::LargeBinary);
    let out = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1], &chunk).unwrap();
    let out = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let v = VariantValue::from_serialized(out.value(0)).unwrap();
    assert_eq!(variant_to_i64(&v).unwrap(), 123);
}

#[test]
fn test_try_variant_get_cast_failure_is_null() {
    // double 1.5 -> bigint is a lossy cast: strict errors, try yields NULL.
    let variant = variant_primitive_serialized(7, &1.5_f64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = utf8_lit(&mut arena, "$");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let out =
        eval_variant_function("try_variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
            .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0));
}

#[test]
fn test_variant_get_strict_cast_failure_errors() {
    let variant = variant_primitive_serialized(7, &1.5_f64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    let arg1 = utf8_lit(&mut arena, "$");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let err = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
        .expect_err("strict cast failure must error");
    assert!(
        err.to_lowercase().contains("cast"),
        "error mentions the cast: {err}"
    );
}

fn make_json_chunk(json: &str) -> (Chunk, ExprId, ExprArena) {
    let arr = Arc::new(StringArray::from(vec![Some(json)])) as ArrayRef;
    let field = Field::new("j", DataType::Utf8, true);
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![arr]).unwrap();
    let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        batch.schema().as_ref(),
        &[SlotId::new(1)],
    )
    .expect("chunk schema");
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    let arg0 = slot_id_expr(&mut arena, 1, DataType::Utf8);
    (chunk, arg0, arena)
}

#[test]
fn test_variant_get_json_string_input() {
    let (chunk, arg0, mut arena) = make_json_chunk(r#"{"a": 42}"#);
    let arg1 = utf8_lit(&mut arena, "$.a");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let out = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(out.value(0), 42);
}

#[test]
fn test_variant_get_missing_path_is_null() {
    let (chunk, arg0, mut arena) = make_json_chunk(r#"{"a": 42}"#);
    let arg1 = utf8_lit(&mut arena, "$.b");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let out = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
        .unwrap();
    let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(out.is_null(0), "missing path is NULL even in strict mode");
}

#[test]
fn test_variant_get_non_literal_path_errors() {
    let variant = variant_primitive_serialized(6, &123_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant);
    // Path given as a slot ref instead of a literal must be rejected.
    let arg1 = slot_id_expr(&mut arena, 1, DataType::Utf8);
    let expr = common::typed_null(&mut arena, DataType::LargeBinary);
    let err = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1], &chunk)
        .expect_err("non-literal path must error");
    assert!(err.contains("constant"), "{err}");
}

#[test]
fn test_variant_get_matches_get_variant_int_on_exact_types() {
    let variant = variant_primitive_serialized(6, &7_i64.to_le_bytes());
    let (chunk, arg0, mut arena) = make_variant_chunk(variant.clone());
    let arg1 = utf8_lit(&mut arena, "$");
    let arg2 = utf8_lit(&mut arena, "bigint");
    let expr = common::typed_null(&mut arena, DataType::Int64);
    let via_new = eval_variant_function("variant_get", &arena, expr, &[arg0, arg1, arg2], &chunk)
        .unwrap();
    let (chunk2, b0, mut arena2) = make_variant_chunk(variant);
    let b1 = utf8_lit(&mut arena2, "$");
    let expr2 = common::typed_null(&mut arena2, DataType::Int64);
    let via_old =
        eval_variant_function("get_variant_int", &arena2, expr2, &[b0, b1], &chunk2).unwrap();
    assert_eq!(
        via_new.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        via_old.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    );
}
```

(If `RecordBatch`/`Schema`/`ChunkSchema`/`SlotId` are not yet in this file's imports, they are — see the existing import block at lines 19-33.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test main variant -- --nocapture` (or the repo's harness name: `cargo test --test function variant`; check `tests/function/main.rs` binding — the suite target is the directory name, so `cargo test --test main` from `tests/function/` wiring; use `cargo test variant_get` to filter if simpler)
Expected: FAIL — `eval_variant_function("variant_get", ...)` returns `Err("unsupported variant function: variant_get")`.

- [ ] **Step 3: Wire dispatch**

In `src/exec/expr/function/variant/dispatch.rs`:

1. `VARIANT_FUNCTIONS` — add two rows:

```rust
    ("variant_get", "variant_get"),
    ("try_variant_get", "try_variant_get"),
```

2. `VARIANT_METADATA` — add:

```rust
    FunctionMeta {
        name: "variant_get",
        min_args: 2,
        max_args: 3,
    },
    FunctionMeta {
        name: "try_variant_get",
        min_args: 2,
        max_args: 3,
    },
```

3. `eval_variant_function` match — add before the `other =>` arm:

```rust
        "variant_get" => super::variant_get::eval_variant_get(arena, expr, args, chunk),
        "try_variant_get" => super::variant_get::eval_try_variant_get(arena, expr, args, chunk),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test variant_get -- --nocapture`
Expected: all 8 new tests PASS. Also run `cargo test --lib exec::expr::function` and the registry test (`tests/function/variant.rs::test_register_variant_functions` if present) — PASS.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/function/variant/dispatch.rs tests/function/variant.rs
git commit -m "feat(variant): dispatch variant_get/try_variant_get with Spark-aligned semantics"
```

---

### Task 4: Analyzer, codegen, and lowering registration

**Files:**
- Modify: `src/sql/analyzer/functions.rs` (~line 1060)
- Modify: `src/sql/analyzer/resolve_expr.rs` (after the round/truncate post-pass, ~line 2101)
- Modify: `src/sql/codegen/expr_compiler.rs` (~line 2437)
- Modify: `src/lower/expr/function_call.rs` (after the existing variant block, ~line 380)

- [ ] **Step 1: Analyzer base return type**

In `src/sql/analyzer/functions.rs`, in the scalar return-type match (next to the existing variant entries at ~1060):

```rust
        "variant_get" | "try_variant_get" => DataType::LargeBinary,
```

- [ ] **Step 2: Analyzer literal-driven post-pass**

In `src/sql/analyzer/resolve_expr.rs`, directly after the round/truncate refinement block (~line 2101, same scope — `name`, `args_typed`, `return_type` all in scope; this block may `return Err`, the enclosing function returns `Result`):

```rust
            // variant_get / try_variant_get: the optional 3rd argument is a
            // string literal naming the result type (Spark-aligned). Surface
            // it as the expression's static type; reject non-literal type
            // arguments up front (fail fast, CLAUDE.md rule 2).
            if matches!(name.as_str(), "variant_get" | "try_variant_get") {
                if !(2..=3).contains(&args_typed.len()) {
                    return Err(format!(
                        "{name} expects 2 or 3 arguments, got {}",
                        args_typed.len()
                    ));
                }
                if args_typed.len() == 3 {
                    match &args_typed[2].kind {
                        ExprKind::Literal(LiteralValue::String(t)) => {
                            return_type =
                                crate::exec::expr::function::variant::variant_get_target_type(t)?;
                        }
                        _ => {
                            return Err(format!(
                                "{name} type argument must be a string literal"
                            ));
                        }
                    }
                }
            }
```

- [ ] **Step 3: Codegen legacy-table entry**

In `src/sql/codegen/expr_compiler.rs`, in `infer_scalar_function_return_type`'s match (next to the variant group at ~2437):

```rust
        "variant_get" | "try_variant_get" => Ok(DataType::LargeBinary),
```

(The analyzer's `type_hint` overrides this for the 3-arg form at `expr_compiler.rs:1256-1261`; this entry only prevents the eager `unknown scalar function` error. Do NOT add signature-registry entries — see plan header.)

- [ ] **Step 4: Lowering arg validation**

In `src/lower/expr/function_call.rs`, after the existing variant validation block (which assumes exactly 2 args; ~line 380), add:

```rust
        if matches!(
            kind,
            function::FunctionKind::Variant(name)
                if matches!(name, "variant_get" | "try_variant_get")
        ) {
            let arg0 = arena
                .data_type(children[0])
                .ok_or_else(|| "variant_get missing arg0 type".to_string())?;
            if !matches!(arg0, DataType::LargeBinary | DataType::Utf8) {
                return Err(
                    "variant_get expects VARIANT or JSON/VARCHAR as first argument".to_string()
                );
            }
            for (i, child) in children.iter().enumerate().skip(1) {
                let t = arena
                    .data_type(*child)
                    .ok_or_else(|| format!("variant_get missing arg{i} type"))?;
                if !matches!(t, DataType::Utf8) {
                    return Err(format!(
                        "variant_get expects VARCHAR for argument {}",
                        i + 1
                    ));
                }
            }
        }
```

(Arg-count bounds come from `VARIANT_METADATA` via the generic check above this block.)

- [ ] **Step 5: Build and run targeted tests**

Run: `cargo build && cargo test --lib sql:: -- variant_get --nocapture && cargo test --lib expr_compiler`
Expected: builds clean; the expr_compiler consistency tests (`expr_compiler.rs:3240+`) still PASS — `variant_get` is not in the probe list, no parity interaction.

- [ ] **Step 6: Commit**

```bash
git add src/sql/analyzer/functions.rs src/sql/analyzer/resolve_expr.rs \
        src/sql/codegen/expr_compiler.rs src/lower/expr/function_call.rs
git commit -m "feat(sql): register variant_get/try_variant_get with literal-driven return types"
```

---

### Task 5: SQL regression case

**Files:**
- Create: `sql-tests/iceberg-dml/sql/variant_get.sql`
- Create: `sql-tests/iceberg-dml/result/variant_get.result`

- [ ] **Step 1: Write the case**

`sql-tests/iceberg-dml/sql/variant_get.sql` (note: never `SELECT v` raw — the binary breaks the runner's MySQL text transport; 2-arg results are displayed through `variant_typeof`):

```sql
-- @order_sensitive=true
-- Test Point: Spark-aligned variant_get / try_variant_get over a v3 iceberg
-- variant column: typed extraction, 2-arg variant return, try_ cast-failure
-- NULL, strict cast-failure error, missing-path NULL, WHERE usage.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_variant_get FORCE;
CREATE TABLE ${case_db}.t_variant_get (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_variant_get VALUES
  (1, parse_json('{"a": 1, "b": "x"}')),
  (2, parse_json('{"a": 99, "b": "y"}')),
  (3, parse_json('{"b": "no-a"}')),
  (4, parse_json('{"a": 1.5}')),
  (5, NULL);

-- query 2 — typed extraction; missing path and SQL NULL row are NULL.
SELECT id, try_variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get ORDER BY id;

-- query 3 — strict extraction over clean rows only.
SELECT id, variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get WHERE id <= 2 ORDER BY id;

-- query 4 — strict extraction over a lossy-cast row must fail.
-- @expect_error=cast
SELECT variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get WHERE id = 4;

-- query 5 — 2-arg form returns variant; display via variant_typeof.
SELECT id, variant_typeof(variant_get(v, '$.a')) FROM ${case_db}.t_variant_get WHERE id <= 2 ORDER BY id;

-- query 6 — predicate usage (the PR-4 pushdown target shape).
SELECT id FROM ${case_db}.t_variant_get WHERE try_variant_get(v, '$.a', 'bigint') > 5 ORDER BY id;

-- query 7 — string extraction.
SELECT id, variant_get(v, '$.b', 'string') FROM ${case_db}.t_variant_get WHERE id <= 3 ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_variant_get FORCE;
```

`sql-tests/iceberg-dml/result/variant_get.result`:

```
-- query 2
id	try_variant_get(v, '$.a', 'bigint')
1	1
2	99
3	NULL
4	NULL
5	NULL

-- query 3
id	variant_get(v, '$.a', 'bigint')
1	1
2	99

-- query 5
id	variant_typeof(variant_get(v, '$.a'))
1	Int64
2	Int64

-- query 6
id
2

-- query 7
id	variant_get(v, '$.b', 'string')
1	x
2	y
3	no-a
```

Note on query 2 row 4: JSON `1.5` decodes as double; `try_` cast double→bigint is lossy → NULL. Note on query 5: `variant_typeof` renders the kernel-rebuilt primitive — if the actual label differs (e.g. `Integer` vs `Int64` depending on how `parse_json`'s INSERT path encodes small ints), record the real output (Step 2) and fix the golden, asserting only that both rows carry the same scalar type label.

- [ ] **Step 2: Run, reconcile golden, verify**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG" &
# wait for the NOVAROCKS_READY line per CLAUDE.md §7.3
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --only variant_get --mode verify
```
Expected: PASS. If a golden cell differs for a *typed-label* reason (see note above), inspect with `--mode diff`, correct the golden by hand to the verified-correct output, and re-verify. Any *value* difference (e.g. query 2 row 1 ≠ 1) is a bug — stop and debug, do not record.

If query 1 fails with an unknown-function error from the analyzer (function existence gate — not expected per analysis, but contingency): add to `src/sql/functions/registry.rs::register_json_fns`:

```rust
    for name in ["variant_get", "try_variant_get"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Any("T")], TypeSpec::Binary),
        );
    }
```

and re-run `cargo test --lib expr_compiler` to confirm the parity test is still silent on these names.

- [ ] **Step 3: Commit**

```bash
git add sql-tests/iceberg-dml/sql/variant_get.sql sql-tests/iceberg-dml/result/variant_get.result
git commit -m "test(sql): variant_get/try_variant_get regression over iceberg v3 variant column"
```

---

### Task 6: Docs and full quality gate

**Files:**
- Modify: `docs/iceberg-v3/variant.md`

- [ ] **Step 1: Document the functions**

In `docs/iceberg-v3/variant.md`, add a section after the function list / before the unsupported sections:

```markdown
## ✅ variant_get / try_variant_get（Spark 对齐）

- `variant_get(v, path[, type])`：`path` 与 `type` 必须是字符串字面量。2 参返回 variant;
  3 参返回指定类型，`type` ∈ {boolean, int, bigint, float, double, string, date, datetime}。
- 语义：missing path → NULL；variant null → NULL；cast 失败 → `variant_get` 报错、
  `try_variant_get` 返回 NULL。
- 求值经由上游 parquet-variant-compute kernel（与 scan 层 shredded 快路径同一实现，
  IV3-6 决策 B）；旧 `get_variant_*` 家族保持原有宽松强转语义不变，不参与下推优化。
- 第一个参数也接受 JSON 字符串（如 `variant_get(parse_json(s), ...)`），坏 JSON 在两种
  模式下都是查询错误。
```

- [ ] **Step 2: Full quality gate**

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo build
cargo test
```
Expected: all green.

- [ ] **Step 3: Re-run the full iceberg-dml suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-dml --mode verify
```
Expected: PASS (server from Task 5 still up, or restart per CLAUDE.md §7.3).

- [ ] **Step 4: Commit**

```bash
git add docs/iceberg-v3/variant.md
git commit -m "docs: variant_get/try_variant_get function reference (IV3-6 PR-2)"
```

---

## Self-review checklist (completed at plan-write time)

- Spec coverage: design §4 in full — signature, literal type strings + whitelist + Int32/Int64 mapping, Spark null/error semantics, kernel-backed single evaluator, JSON-string fallback (mandated by the `parse_json`-returns-Utf8 finding), constant-args requirement, three-table registration (analyzer table, codegen legacy table, lowering allow-list) plus the exec double-table.
- No placeholders: every step carries complete code or exact commands with expected outcomes; the two genuinely environment-dependent outcomes (variant_typeof label in query 5; analyzer existence-gate contingency) ship with their concrete resolution code inline.
- Type consistency: `eval_variant_get(arena, _expr, args, chunk)` signature matches dispatch arms; `variant_get_target_type` is the single type-string source used by both exec (Task 2) and analyzer (Task 4); exec literal enum `LiteralValue::Utf8` vs analyzer `LiteralValue::String` used correctly in their respective files.
- Deliberate scope cuts (per design): no pushdown (PR-4), no signature-registry entry, no decimal/array type strings, datetime kernel semantics documented as-is.
