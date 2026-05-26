# IVM VARBINARY State + Distinct-Count Aggregates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the IVM detail-state framework from Arrow `Map<K, Int64>` to opaque `VARBINARY` state columns dispatched per `AggregateFunctionKind`, and add IVM support for `COUNT(DISTINCT col)` and `APPROX_COUNT_DISTINCT(col)`.

**Architecture:** Per-kind state combinator SQL function families (`<kind>_state`, `<kind>_state_signed`, `<kind>_state_union`, `<kind>_state_visible`). State bytes prefixed with `version = 0x01`. Multiset kinds (Min/Max/CountDistinct/ApproxCountDistinct) share the §3.3 sorted-multiset encoding. ApproxCountDistinct's visible reuses the plain HLL hash and estimator from `hll_raw.rs` for bit-equality with non-MV `approx_count_distinct` queries.

**Tech Stack:** Rust, Apache Arrow (`Binary` DataType), `sqlparser-rs`, NovaRocks IVM / MV / iceberg-ivm framework.

**Spec:** [`docs/superpowers/specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md`](../specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md)

---

## File Structure

### Created

| Path | Responsibility |
|---|---|
| `src/connector/starrocks/managed/state_codec.rs` | Central byte encode/decode helpers per `AggregateFunctionKind`: version byte, fixed-size LE primitives, ULEB128/SLEB128, multiset encoding with key serialization. |
| `src/exec/expr/agg/functions/state_combinators/mod.rs` | Module gate for per-kind `_state` / `_state_signed` aggregate functions. |
| `src/exec/expr/agg/functions/state_combinators/{count,sum,avg,min_max,bool_or_and,count_distinct,approx_count_distinct}.rs` | One file per kind family; each defines its `<kind>_state` and `<kind>_state_signed` aggregate function impls. |
| `src/exec/expr/function/mv_state/mod.rs` | Module gate for scalar combinators (`_state_union`, `_state_visible`, `DEBUG_DUMP_MV_STATE`). |
| `src/exec/expr/function/mv_state/{count,sum,avg,min_max,bool_or_and,count_distinct,approx_count_distinct}_visible.rs` | One file per kind: `_state_union` and `_state_visible` scalar function impls. |
| `src/exec/expr/function/mv_state/debug_dump.rs` | `DEBUG_DUMP_MV_STATE(mv_table_name, row_id)` scalar function — JSON decode per kind. |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_count_distinct_*.sql` | ~12 new CountDistinct fixtures. |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_approx_count_distinct_*.sql` | ~9 new ApproxCountDistinct fixtures. |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_count_vs_approx_state_equality.sql` | Cross-kind byte symmetry fixture. |
| `sql-tests/iceberg-ivm/sql/debug_dump_mv_state_*.sql` | 3 DEBUG_DUMP_MV_STATE fixtures. |

### Modified

| Path | Change |
|---|---|
| `src/connector/starrocks/managed/mv_shape.rs` | Add `CountDistinct` / `ApproxCountDistinct` to `AggregateFunctionKind`; extend classifier with new function-name routing and `count(DISTINCT)` syntax handling; add error messages. |
| `src/connector/starrocks/managed/mv_agg_state.rs` | Simplify `validate_state_column_type` to require VARBINARY; remove all `Map<K, Int64>`-specific accumulator and Parquet field-id paths; add `load_from_existing_mv` legacy-format detection. |
| `src/connector/starrocks/managed/ivm_delta_aggregate.rs` | Replace hard-coded `map_value_count` projection with per-kind `<kind>_state_signed` dispatch. |
| `src/exec/expr/agg/functions/mod.rs` | Declare `state_combinators` module; register `<kind>_state` and `<kind>_state_signed` aggregates; remove `map_value_count` / `map_value_count_signed`. |
| `src/exec/expr/function/mod.rs` | Declare `mv_state` module; register scalar combinator functions. |
| `src/sql/analyzer/functions.rs` | Add `<kind>_state_union`, `<kind>_state_visible`, `DEBUG_DUMP_MV_STATE` to scalar function whitelist; remove `map_value_count` entries. |
| Existing `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_{bool_or,bool_and,min_max_*}.result` schema-reflection sections | Update state column type from `Map<...>` to `BINARY`. |

---

## Phase 1: State codec primitives

Build the byte-level encode/decode foundation **with no MV-framework integration yet**. Pure logic, unit-testable in isolation.

### Task 1.1: Create state_codec module skeleton

**Files:**
- Create: `src/connector/starrocks/managed/state_codec.rs`
- Modify: `src/connector/starrocks/managed/mod.rs:<existing module list>`

- [ ] **Step 1: Create the file with module header**

```rust
// src/connector/starrocks/managed/state_codec.rs
//! Per-kind VARBINARY state codec for IVM detail-state aggregates.
//!
//! All non-empty states begin with `STATE_VERSION_V1 = 0x01`. Empty state
//! is a zero-length byte slice (no version byte) and is treated as `is_empty`
//! by every kind.
//!
//! Layout by kind: see docs/superpowers/specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md §3.

pub(crate) const STATE_VERSION_V1: u8 = 0x01;

/// Returns `true` iff `bytes` is the empty state (zero-length).
#[inline]
pub(crate) fn is_empty_state(bytes: &[u8]) -> bool {
    bytes.is_empty()
}
```

- [ ] **Step 2: Wire into `mod.rs`**

In `src/connector/starrocks/managed/mod.rs`, add `pub(crate) mod state_codec;` alphabetized with the existing module declarations.

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p novarocks 2>&1 | tail -20`
Expected: no new errors; `state_codec` module visible.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/state_codec.rs src/connector/starrocks/managed/mod.rs
git commit -m "ivm/state-codec: introduce per-kind VARBINARY state codec module skeleton"
```

### Task 1.2: ULEB128 / SLEB128 varint helpers

**Files:**
- Modify: `src/connector/starrocks/managed/state_codec.rs`

- [ ] **Step 1: Add failing tests at the bottom of the file**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uleb128_roundtrips() {
        for &v in &[0u64, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut buf = Vec::new();
            write_uleb128(&mut buf, v);
            let mut cursor = &buf[..];
            assert_eq!(read_uleb128(&mut cursor).unwrap(), v, "value {v}");
            assert!(cursor.is_empty(), "all bytes consumed for {v}");
        }
    }

    #[test]
    fn sleb128_roundtrips() {
        for &v in &[0i64, 1, -1, 63, -64, 64, -65, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            write_sleb128(&mut buf, v);
            let mut cursor = &buf[..];
            assert_eq!(read_sleb128(&mut cursor).unwrap(), v, "value {v}");
            assert!(cursor.is_empty(), "all bytes consumed for {v}");
        }
    }

    #[test]
    fn uleb128_short_buffer_errors() {
        let mut cursor: &[u8] = &[0x80];  // continuation bit set, no follow-up
        assert!(read_uleb128(&mut cursor).is_err());
    }
}
```

Run: `cargo test -p novarocks state_codec::tests 2>&1 | tail -10`
Expected: compilation error — `write_uleb128`, `read_uleb128`, `write_sleb128`, `read_sleb128` not found.

- [ ] **Step 2: Implement the four helpers**

Add above `#[cfg(test)] mod tests`:

```rust
pub(crate) fn write_uleb128(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn read_uleb128(cursor: &mut &[u8]) -> Result<u64, String> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: ULEB128 truncated".to_string())?;
        *cursor = rest;
        result |= ((byte & 0x7F) as u64)
            .checked_shl(shift)
            .ok_or_else(|| "state_codec: ULEB128 overflow".to_string())?;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("state_codec: ULEB128 too long".to_string());
        }
    }
}

pub(crate) fn write_sleb128(out: &mut Vec<u8>, mut value: i64) {
    loop {
        let byte = (value as u8) & 0x7F;
        let high_bit_set = byte & 0x40 != 0;
        value >>= 7;
        let done = (value == 0 && !high_bit_set) || (value == -1 && high_bit_set);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(crate) fn read_sleb128(cursor: &mut &[u8]) -> Result<i64, String> {
    let mut result: i64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: SLEB128 truncated".to_string())?;
        *cursor = rest;
        result |= ((byte & 0x7F) as i64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            // Sign-extend the high bit of the last group.
            if shift < 64 && byte & 0x40 != 0 {
                result |= -1i64 << shift;
            }
            return Ok(result);
        }
        if shift >= 64 {
            return Err("state_codec: SLEB128 too long".to_string());
        }
    }
}
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p novarocks state_codec::tests 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/state_codec.rs
git commit -m "ivm/state-codec: add ULEB128/SLEB128 varint helpers + roundtrip tests"
```

### Task 1.3: Key serialization for all V1 allowed Arrow types

**Files:**
- Modify: `src/connector/starrocks/managed/state_codec.rs`

- [ ] **Step 1: Add failing tests**

```rust
#[cfg(test)]
mod key_tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    fn key_round_trip(array: ArrayRef, idx: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        write_key_at(&mut buf, &array, idx).unwrap();
        buf
    }

    #[test]
    fn key_boolean() {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false)]));
        assert_eq!(key_round_trip(arr.clone(), 0), vec![1u8]);
        assert_eq!(key_round_trip(arr, 1), vec![0u8]);
    }

    #[test]
    fn key_int64_le() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(0x0123_4567_89AB_CDEFi64)]));
        assert_eq!(key_round_trip(arr, 0), vec![0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]);
    }

    #[test]
    fn key_float64_nan_canonicalized() {
        let arr1: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN)]));
        let arr2: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::from_bits(0x7FF8_0000_0000_0001))]));
        assert_eq!(key_round_trip(arr1, 0), key_round_trip(arr2, 0),
            "all NaNs must canonicalize to one byte pattern");
    }

    #[test]
    fn key_float64_neg_zero_canonicalized() {
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![Some(-0.0_f64)]));
        let pos: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.0_f64)]));
        assert_eq!(key_round_trip(arr, 0), key_round_trip(pos, 0));
    }

    #[test]
    fn key_utf8_length_prefixed() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("ab")]));
        let bytes = key_round_trip(arr, 0);
        assert_eq!(bytes, vec![2u8, b'a', b'b']);
    }
}
```

Run: `cargo test -p novarocks state_codec::key_tests 2>&1 | tail -10`
Expected: compilation error — `write_key_at` not found.

- [ ] **Step 2: Implement `write_key_at`**

```rust
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array,
    Float32Array, Float64Array, Decimal128Array, Date32Array, TimestampMicrosecondArray,
    StringArray, LargeStringArray};
use arrow::datatypes::DataType;

pub(crate) fn write_key_at(out: &mut Vec<u8>, array: &ArrayRef, idx: usize) -> Result<(), String> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            out.push(if a.value(idx) { 1 } else { 0 });
        }
        DataType::Int8 => out.extend_from_slice(&[array.as_any().downcast_ref::<Int8Array>().unwrap().value(idx) as u8]),
        DataType::Int16 => out.extend_from_slice(&array.as_any().downcast_ref::<Int16Array>().unwrap().value(idx).to_le_bytes()),
        DataType::Int32 => out.extend_from_slice(&array.as_any().downcast_ref::<Int32Array>().unwrap().value(idx).to_le_bytes()),
        DataType::Int64 => out.extend_from_slice(&array.as_any().downcast_ref::<Int64Array>().unwrap().value(idx).to_le_bytes()),
        DataType::Float32 => {
            let v = array.as_any().downcast_ref::<Float32Array>().unwrap().value(idx);
            let canonical = canonicalize_f32(v);
            out.extend_from_slice(&canonical.to_le_bytes());
        }
        DataType::Float64 => {
            let v = array.as_any().downcast_ref::<Float64Array>().unwrap().value(idx);
            let canonical = canonicalize_f64(v);
            out.extend_from_slice(&canonical.to_le_bytes());
        }
        DataType::Decimal128(_, _) => {
            let v = array.as_any().downcast_ref::<Decimal128Array>().unwrap().value(idx);
            out.extend_from_slice(&v.to_le_bytes());
        }
        DataType::Date32 => out.extend_from_slice(&array.as_any().downcast_ref::<Date32Array>().unwrap().value(idx).to_le_bytes()),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            out.extend_from_slice(&array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(idx).to_le_bytes());
        }
        DataType::Utf8 => {
            let s = array.as_any().downcast_ref::<StringArray>().unwrap().value(idx);
            write_uleb128(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        DataType::LargeUtf8 => {
            let s = array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(idx);
            write_uleb128(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        other => return Err(format!("state_codec: unsupported key type {other:?}")),
    }
    Ok(())
}

fn canonicalize_f32(v: f32) -> f32 {
    if v.is_nan() { f32::NAN }
    else if v == 0.0 { 0.0_f32 }  // -0.0 → +0.0
    else { v }
}

fn canonicalize_f64(v: f64) -> f64 {
    if v.is_nan() { f64::NAN }
    else if v == 0.0 { 0.0_f64 }
    else { v }
}
```

- [ ] **Step 3: Add corresponding `read_key` for known type**

Append:

```rust
pub(crate) fn read_key(cursor: &mut &[u8], dtype: &DataType) -> Result<KeyValue, String> {
    match dtype {
        DataType::Boolean => {
            let (&b, rest) = cursor.split_first().ok_or("read_key: truncated Boolean")?;
            *cursor = rest;
            Ok(KeyValue::Bool(b != 0))
        }
        DataType::Int64 => {
            if cursor.len() < 8 { return Err("read_key: truncated Int64".into()); }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&cursor[..8]);
            *cursor = &cursor[8..];
            Ok(KeyValue::Int64(i64::from_le_bytes(bytes)))
        }
        // ... (Int8/16/32, Float32/64, Decimal128, Date32, Timestamp, Utf8/LargeUtf8 — same shape)
        other => Err(format!("read_key: unsupported type {other:?}")),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyValue {
    Bool(bool),
    Int8(i8), Int16(i16), Int32(i32), Int64(i64),
    Float32(u32),  // raw bits, NaN/0.0 canonicalized
    Float64(u64),
    Decimal128(i128),
    Date32(i32),
    Timestamp(i64),
    Utf8(String),
}
```

Implement the remaining match arms following the Int64 pattern (8 bytes for Int64/Float64/Timestamp; 4 for Int32/Date32/Float32; 2 for Int16; 1 for Int8/Boolean; 16 for Decimal128; ULEB128 + bytes for Utf8/LargeUtf8).

- [ ] **Step 4: Add roundtrip test for read_key**

```rust
#[test]
fn key_int64_write_read_roundtrip() {
    let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(42i64), Some(-1)]));
    let mut buf = Vec::new();
    write_key_at(&mut buf, &arr, 0).unwrap();
    write_key_at(&mut buf, &arr, 1).unwrap();
    let mut cursor = &buf[..];
    assert_eq!(read_key(&mut cursor, &DataType::Int64).unwrap(), KeyValue::Int64(42));
    assert_eq!(read_key(&mut cursor, &DataType::Int64).unwrap(), KeyValue::Int64(-1));
}
```

Run: `cargo test -p novarocks state_codec 2>&1 | tail -10`
Expected: all key tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/state_codec.rs
git commit -m "ivm/state-codec: add key write/read for all V1-allowed Arrow types with NaN/zero canonicalization"
```

### Task 1.4: Fixed-size kind encoders (Count / Sum / Avg / BoolOr / BoolAnd)

**Files:**
- Modify: `src/connector/starrocks/managed/state_codec.rs`

- [ ] **Step 1: Add failing tests**

```rust
#[cfg(test)]
mod fixed_size_tests {
    use super::*;

    #[test]
    fn count_state_encodes_to_9_bytes() {
        let bytes = encode_count_state(42);
        assert_eq!(bytes.len(), 9);
        assert_eq!(bytes[0], STATE_VERSION_V1);
        assert_eq!(decode_count_state(&bytes).unwrap(), 42);
    }

    #[test]
    fn count_state_empty() {
        assert_eq!(decode_count_state(&[]).unwrap(), 0);
    }

    #[test]
    fn bool_or_state_encode_decode() {
        let bytes = encode_bool_state(3, 5);
        assert_eq!(bytes.len(), 17);
        let (ct, cf) = decode_bool_state(&bytes).unwrap();
        assert_eq!((ct, cf), (3, 5));
    }

    #[test]
    fn sum_int64_encode_decode() {
        let bytes = encode_sum_int64(10, 100);
        let (rc, sum) = decode_sum_int64(&bytes).unwrap();
        assert_eq!((rc, sum), (10, 100));
    }

    #[test]
    fn sum_decimal128_encode_decode() {
        let bytes = encode_sum_decimal128(7, 12345i128);
        let (rc, sum) = decode_sum_decimal128(&bytes).unwrap();
        assert_eq!((rc, sum), (7, 12345));
    }
}
```

Run: `cargo test -p novarocks state_codec::fixed_size_tests 2>&1 | tail -10`
Expected: compilation errors for the 5 missing functions.

- [ ] **Step 2: Implement the encoders/decoders**

```rust
pub(crate) fn encode_count_state(count: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(STATE_VERSION_V1);
    buf.extend_from_slice(&count.to_le_bytes());
    buf
}

pub(crate) fn decode_count_state(bytes: &[u8]) -> Result<i64, String> {
    if bytes.is_empty() { return Ok(0); }
    if bytes.len() != 9 || bytes[0] != STATE_VERSION_V1 {
        return Err(format!("decode_count_state: invalid bytes (len={})", bytes.len()));
    }
    let mut le = [0u8; 8];
    le.copy_from_slice(&bytes[1..9]);
    Ok(i64::from_le_bytes(le))
}

pub(crate) fn encode_bool_state(count_true: i64, count_false: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    buf.push(STATE_VERSION_V1);
    buf.extend_from_slice(&count_true.to_le_bytes());
    buf.extend_from_slice(&count_false.to_le_bytes());
    buf
}

pub(crate) fn decode_bool_state(bytes: &[u8]) -> Result<(i64, i64), String> {
    if bytes.is_empty() { return Ok((0, 0)); }
    if bytes.len() != 17 || bytes[0] != STATE_VERSION_V1 {
        return Err(format!("decode_bool_state: invalid bytes (len={})", bytes.len()));
    }
    let mut ct = [0u8; 8]; ct.copy_from_slice(&bytes[1..9]);
    let mut cf = [0u8; 8]; cf.copy_from_slice(&bytes[9..17]);
    Ok((i64::from_le_bytes(ct), i64::from_le_bytes(cf)))
}

pub(crate) fn encode_sum_int64(row_count: i64, sum: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    buf.push(STATE_VERSION_V1);
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&sum.to_le_bytes());
    buf
}

pub(crate) fn decode_sum_int64(bytes: &[u8]) -> Result<(i64, i64), String> {
    if bytes.is_empty() { return Ok((0, 0)); }
    if bytes.len() != 17 || bytes[0] != STATE_VERSION_V1 {
        return Err(format!("decode_sum_int64: invalid bytes (len={})", bytes.len()));
    }
    let mut rc = [0u8; 8]; rc.copy_from_slice(&bytes[1..9]);
    let mut sm = [0u8; 8]; sm.copy_from_slice(&bytes[9..17]);
    Ok((i64::from_le_bytes(rc), i64::from_le_bytes(sm)))
}

pub(crate) fn encode_sum_decimal128(row_count: i64, sum: i128) -> Vec<u8> {
    let mut buf = Vec::with_capacity(25);
    buf.push(STATE_VERSION_V1);
    buf.extend_from_slice(&row_count.to_le_bytes());
    buf.extend_from_slice(&sum.to_le_bytes());
    buf
}

pub(crate) fn decode_sum_decimal128(bytes: &[u8]) -> Result<(i64, i128), String> {
    if bytes.is_empty() { return Ok((0, 0)); }
    if bytes.len() != 25 || bytes[0] != STATE_VERSION_V1 {
        return Err(format!("decode_sum_decimal128: invalid bytes (len={})", bytes.len()));
    }
    let mut rc = [0u8; 8]; rc.copy_from_slice(&bytes[1..9]);
    let mut sm = [0u8; 16]; sm.copy_from_slice(&bytes[9..25]);
    Ok((i64::from_le_bytes(rc), i128::from_le_bytes(sm)))
}

// Avg uses the same shape as Sum (count + sum); reuse Sum encoders.
pub(crate) use encode_sum_int64 as encode_avg_int64;
pub(crate) use decode_sum_int64 as decode_avg_int64;
pub(crate) use encode_sum_decimal128 as encode_avg_decimal128;
pub(crate) use decode_sum_decimal128 as decode_avg_decimal128;
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p novarocks state_codec::fixed_size_tests 2>&1 | tail -10`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/state_codec.rs
git commit -m "ivm/state-codec: add fixed-size encoders for Count/Sum/Avg/BoolOr/BoolAnd"
```

### Task 1.5: Multiset encoder/decoder (Min/Max/CountDistinct/ApproxCountDistinct)

**Files:**
- Modify: `src/connector/starrocks/managed/state_codec.rs`

- [ ] **Step 1: Add failing tests**

```rust
#[cfg(test)]
mod multiset_tests {
    use super::*;

    fn entry_int(k: i64, c: i64) -> MultisetEntry {
        MultisetEntry { key_bytes: k.to_le_bytes().to_vec(), count: c }
    }

    #[test]
    fn multiset_empty_round_trip() {
        let bytes = encode_multiset(&[]);
        assert_eq!(bytes, Vec::<u8>::new());  // empty state = 0 bytes
        assert_eq!(decode_multiset(&bytes).unwrap(), Vec::<MultisetEntry>::new());
    }

    #[test]
    fn multiset_single_entry_round_trip() {
        let entries = vec![entry_int(42, 3)];
        let bytes = encode_multiset(&entries);
        assert_eq!(bytes[0], STATE_VERSION_V1);
        assert_eq!(decode_multiset(&bytes).unwrap(), entries);
    }

    #[test]
    fn multiset_union_sums_counts_at_shared_keys() {
        let a = vec![entry_int(1, 2), entry_int(3, 1)];
        let b = vec![entry_int(1, 1), entry_int(2, 5)];
        let merged = union_multisets(&a, &b);
        assert_eq!(merged, vec![entry_int(1, 3), entry_int(2, 5), entry_int(3, 1)]);
    }

    #[test]
    fn multiset_union_drops_canceled_entries() {
        let a = vec![entry_int(1, 2)];
        let b = vec![entry_int(1, -2), entry_int(2, 4)];
        let merged = union_multisets(&a, &b);
        assert_eq!(merged, vec![entry_int(2, 4)]);
    }

    #[test]
    fn multiset_union_preserves_sort_order() {
        let a = vec![entry_int(5, 1), entry_int(1, 1)];  // intentionally unsorted input
        let b = vec![entry_int(3, 1)];
        let merged = union_multisets(&a, &b);
        // canonical output sorted ascending
        let keys: Vec<i64> = merged.iter().map(|e| i64::from_le_bytes(e.key_bytes[..8].try_into().unwrap())).collect();
        assert_eq!(keys, vec![1, 3, 5]);
    }
}
```

Run: `cargo test -p novarocks state_codec::multiset_tests 2>&1 | tail -10`
Expected: compilation errors for `MultisetEntry`, `encode_multiset`, `decode_multiset`, `union_multisets`.

- [ ] **Step 2: Implement multiset types and operations**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultisetEntry {
    pub(crate) key_bytes: Vec<u8>,
    pub(crate) count: i64,
}

pub(crate) fn encode_multiset(entries: &[MultisetEntry]) -> Vec<u8> {
    if entries.is_empty() {
        return Vec::new();  // empty state
    }
    let mut buf = Vec::with_capacity(1 + 4 + entries.len() * 16);
    buf.push(STATE_VERSION_V1);
    write_uleb128(&mut buf, entries.len() as u64);
    for e in entries {
        buf.extend_from_slice(&e.key_bytes);
        write_sleb128(&mut buf, e.count);
    }
    buf
}

/// Decodes a multiset, but the caller must know the key type to walk entries.
/// For union/visible purposes we operate on raw key bytes; the key type is
/// only needed when we actually need to inspect keys (e.g., visible Min/Max).
pub(crate) fn decode_multiset_with_key_type(
    bytes: &[u8],
    key_dtype: &DataType,
) -> Result<Vec<MultisetEntry>, String> {
    if bytes.is_empty() { return Ok(Vec::new()); }
    if bytes[0] != STATE_VERSION_V1 {
        return Err(format!("decode_multiset: unknown version 0x{:02x}", bytes[0]));
    }
    let mut cursor = &bytes[1..];
    let n = read_uleb128(&mut cursor)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let key_start = cursor;
        // Advance cursor past one key by re-reading with read_key; capture the bytes.
        let _key = read_key(&mut cursor, key_dtype)?;
        let key_len = key_start.len() - cursor.len();
        let key_bytes = key_start[..key_len].to_vec();
        let count = read_sleb128(&mut cursor)?;
        out.push(MultisetEntry { key_bytes, count });
    }
    Ok(out)
}

/// Convenience: decode without key-type awareness (used in tests where keys are i64).
#[cfg(test)]
pub(crate) fn decode_multiset(bytes: &[u8]) -> Result<Vec<MultisetEntry>, String> {
    decode_multiset_with_key_type(bytes, &DataType::Int64)
}

pub(crate) fn union_multisets(a: &[MultisetEntry], b: &[MultisetEntry]) -> Vec<MultisetEntry> {
    // Collect into a BTreeMap keyed by raw key bytes for sorted iteration.
    use std::collections::BTreeMap;
    let mut map: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for e in a.iter().chain(b.iter()) {
        *map.entry(e.key_bytes.clone()).or_insert(0) += e.count;
    }
    map.into_iter()
        .filter(|(_, c)| *c > 0)
        .map(|(key_bytes, count)| MultisetEntry { key_bytes, count })
        .collect()
}
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p novarocks state_codec::multiset_tests 2>&1 | tail -10`
Expected: 5 passed.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/state_codec.rs
git commit -m "ivm/state-codec: multiset encode/decode/union with sorted canonicalization"
```

---

## Phase 2: Aggregate combinators for existing kinds

For each kind in {Count, Sum, Avg, Min, Max, BoolOr, BoolAnd}, implement two aggregate functions: `<kind>_state(args) -> VARBINARY` and `<kind>_state_signed(args, __change_op) -> VARBINARY`. These produce per-group partial state from delta rows.

Tasks 2.1-2.7 each handle one kind family. Files live under `src/exec/expr/agg/functions/state_combinators/`.

### Task 2.1: Create state_combinators module skeleton

**Files:**
- Create: `src/exec/expr/agg/functions/state_combinators/mod.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

- [ ] **Step 1: Create the submodule directory and gate file**

```rust
// src/exec/expr/agg/functions/state_combinators/mod.rs
//! Per-kind state combinator aggregate functions for IVM detail-state.
//!
//! Each kind family has two aggregate functions:
//!   - <kind>_state(args)                    -> VARBINARY  (partial state from INSERT-only delta)
//!   - <kind>_state_signed(args, __op TINYINT) -> VARBINARY (with INSERT/DELETE sign)
//!
//! All produce VARBINARY columns with byte layout defined in
//! src/connector/starrocks/managed/state_codec.rs

pub(super) mod count;
pub(super) mod bool_or_and;
pub(super) mod sum;
pub(super) mod avg;
pub(super) mod min_max;
pub(super) mod count_distinct;
pub(super) mod approx_count_distinct;
```

Create empty files for each `pub(super) mod` entry (e.g., `touch src/exec/expr/agg/functions/state_combinators/count.rs`) — they will be filled in subsequent tasks. Use the same module-header comment for each empty file:

```rust
// src/exec/expr/agg/functions/state_combinators/<kind>.rs
//! <KindName> state combinator aggregate functions.
```

- [ ] **Step 2: Declare the submodule in agg functions root**

In `src/exec/expr/agg/functions/mod.rs`, alphabetized near the existing `mod` declarations:

```rust
mod state_combinators;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build -p novarocks 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/ src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: scaffold state_combinators module tree"
```

### Task 2.2: `count_state` and `count_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/count.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// src/exec/expr/agg/functions/state_combinators/count.rs
use crate::connector::starrocks::managed::state_codec;
use crate::exec::expr::agg::{AggregateFunction, AggKind, AggSpec};

pub(super) struct CountStateAgg;
pub(super) struct CountStateSignedAgg;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use std::sync::Arc;

    fn run_count_state(values: &[Option<i64>]) -> Vec<u8> {
        let spec = CountStateAgg.spec(&[arrow::datatypes::DataType::Int64], None).unwrap();
        let mut state = vec![0u8; CountStateAgg.state_size(&spec)];
        unsafe { CountStateAgg.init_state(&spec, state.as_mut_ptr()); }
        let input: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        unsafe { CountStateAgg.update_batch(&spec, state.as_mut_ptr(), &[input]).unwrap(); }
        let mut out_builder = BinaryBuilder::new();
        unsafe { CountStateAgg.finalize_to(&spec, state.as_mut_ptr(), &mut out_builder); }
        unsafe { CountStateAgg.drop_state(&spec, state.as_mut_ptr()); }
        let arr = out_builder.finish();
        arr.value(0).to_vec()
    }

    #[test]
    fn count_state_counts_non_null_rows() {
        let bytes = run_count_state(&[Some(1), Some(2), None, Some(3)]);
        // count_state(col) on 3 non-null rows = encode_count_state(3)
        assert_eq!(state_codec::decode_count_state(&bytes).unwrap(), 3);
    }

    #[test]
    fn count_state_star_counts_all_rows() {
        // count_state with Wildcard input counts every row including NULLs.
        // Implementation: when args is empty (count(*)), no NULL filter; count = batch size.
        // Test driven through aggregator state - see implementation below.
    }
}
```

Run: `cargo test -p novarocks state_combinators::count 2>&1 | tail -10`
Expected: compilation error — `CountStateAgg` has no methods.

- [ ] **Step 2: Implement `CountStateAgg` and `CountStateSignedAgg`**

Refer to `src/exec/expr/agg/functions/bool_and.rs` for the `AggregateFunction` trait usage pattern. Adapt it to:
- State type: `i64` count
- `init_state`: write 0
- `update_batch`: for each non-NULL row in input array (or every row if Wildcard), increment count by 1
- `update_batch_signed`: same but +1 for INSERT (__op=0), -1 for DELETE (__op=1)
- `merge_batch`: take VARBINARY-encoded `count_state` bytes, decode, sum into current
- `finalize_to(BinaryBuilder)`: emit `encode_count_state(count)` if non-empty; else emit empty byte slice
- `drop_state`: no-op for inline i64

Add `count_state_signed` impl with the additional `__change_op` arg. The two structs share state representation.

- [ ] **Step 3: Register in `mod.rs`**

In `src/exec/expr/agg/functions/mod.rs`:

```rust
use state_combinators::count::{CountStateAgg, CountStateSignedAgg};

static COUNT_STATE: CountStateAgg = CountStateAgg;
static COUNT_STATE_SIGNED: CountStateSignedAgg = CountStateSignedAgg;
```

Add to `resolve_by_func`:

```rust
"count_state" => Ok(&COUNT_STATE),
"count_state_signed" => Ok(&COUNT_STATE_SIGNED),
```

Add corresponding `AggKind` variants (`CountState`, `CountStateSigned`) and update `resolve_by_kind`.

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::count 2>&1 | tail -10`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/count.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add count_state and count_state_signed aggregate functions"
```

### Task 2.3: `bool_or_state`, `bool_or_state_signed`, `bool_and_state`, `bool_and_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/bool_or_and.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

- [ ] **Step 1: Write failing tests for all four functions**

```rust
// In bool_or_and.rs test module:

#[test]
fn bool_or_state_counts_true_and_false() {
    let bytes = run_bool_or_state(&[Some(true), Some(false), Some(false), None, Some(true)]);
    let (ct, cf) = state_codec::decode_bool_state(&bytes).unwrap();
    assert_eq!((ct, cf), (2, 2));  // NULLs skipped
}

#[test]
fn bool_or_state_signed_handles_delete() {
    // INSERT (true), INSERT (false), DELETE (true)
    let bytes = run_bool_or_state_signed(
        &[Some(true), Some(false), Some(true)],
        &[0, 0, 1],
    );
    let (ct, cf) = state_codec::decode_bool_state(&bytes).unwrap();
    assert_eq!((ct, cf), (0, 1));
}

// Similar pattern for bool_and_state and bool_and_state_signed.
```

Run: expected compile fail.

- [ ] **Step 2: Implement the four structs**

State representation: `(i64 count_true, i64 count_false)`. For BOOL_OR and BOOL_AND, the state representation is identical — only the visible function (not state computation) differs. So the `_state` aggregate function is shared:

```rust
pub(super) struct BoolStateAgg;        // serves both bool_or_state and bool_and_state
pub(super) struct BoolStateSignedAgg;  // serves both signed variants

impl AggregateFunction for BoolStateAgg {
    // state = (ct, cf) struct
    // update_batch: for each non-NULL Boolean row, increment ct if true else cf
    // finalize_to: emit encode_bool_state(ct, cf) if either > 0, else empty
    // ...
}

impl AggregateFunction for BoolStateSignedAgg {
    // update_batch_with_op: increment by +1 for INSERT, -1 for DELETE
    // ...
}
```

- [ ] **Step 3: Register `bool_or_state` and `bool_and_state` aliases pointing to the same struct**

In `mod.rs`:

```rust
"bool_or_state"          => Ok(&BOOL_STATE),
"bool_and_state"         => Ok(&BOOL_STATE),
"bool_or_state_signed"   => Ok(&BOOL_STATE_SIGNED),
"bool_and_state_signed"  => Ok(&BOOL_STATE_SIGNED),
```

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::bool_or_and 2>&1 | tail -10`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/bool_or_and.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add bool_or/bool_and _state and _state_signed aggregate functions"
```

### Task 2.4: `sum_state` and `sum_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/sum.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

Sum has two specialization paths based on input type: Int64 (encodes via `encode_sum_int64`) and Decimal128 (via `encode_sum_decimal128`). At `spec(arg_types)` time, the impl branches on input type.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn sum_state_int64_skips_nulls() {
    let bytes = run_sum_state_int64(&[Some(10), None, Some(20)]);
    let (rc, sum) = state_codec::decode_sum_int64(&bytes).unwrap();
    assert_eq!((rc, sum), (2, 30));
}

#[test]
fn sum_state_signed_int64_handles_delete() {
    let bytes = run_sum_state_signed_int64(&[Some(10), Some(5)], &[0, 1]);  // INSERT 10, DELETE 5
    let (rc, sum) = state_codec::decode_sum_int64(&bytes).unwrap();
    assert_eq!((rc, sum), (0, 5));  // row_count: +1 -1 = 0; sum: +10 -5 = 5
}

#[test]
fn sum_state_decimal128() {
    let bytes = run_sum_state_decimal128(&[Some(1_000_000i128), Some(2_000_000i128)]);
    let (rc, sum) = state_codec::decode_sum_decimal128(&bytes).unwrap();
    assert_eq!((rc, sum), (2, 3_000_000));
}
```

Run: expected compile fail.

- [ ] **Step 2: Implement `SumStateAgg` and `SumStateSignedAgg`**

Branch on `spec.input_type`:
- Int64 / Int8/16/32 (widening allowed): state is `(i64 row_count, i64 sum)`
- Decimal128: state is `(i64 row_count, i128 sum)`

Reject Float32/Float64 inputs (not in spec §6.1 for Sum).

- [ ] **Step 3: Register**

```rust
"sum_state"          => Ok(&SUM_STATE),
"sum_state_signed"   => Ok(&SUM_STATE_SIGNED),
```

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::sum 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/sum.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add sum_state and sum_state_signed for Int64/Decimal128"
```

### Task 2.5: `avg_state` and `avg_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/avg.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

Avg uses the same state layout as Sum (per §3.2: `(row_count, sum)`); only visible differs. Aggregate function impl can `pub use` Sum's structs with renaming.

- [ ] **Step 1: Write a single integration test confirming Avg uses Sum's state**

```rust
#[test]
fn avg_state_state_bytes_equal_sum_state_bytes() {
    let avg_bytes = run_avg_state_int64(&[Some(10), Some(20)]);
    let sum_bytes = run_sum_state_int64(&[Some(10), Some(20)]);
    assert_eq!(avg_bytes, sum_bytes,
        "Avg and Sum state must be byte-identical (visible differs only)");
}
```

- [ ] **Step 2: Implement `AvgStateAgg` and `AvgStateSignedAgg` as thin wrappers reusing Sum logic**

```rust
// avg.rs
use super::sum::{SumStateAgg, SumStateSignedAgg};

pub(super) struct AvgStateAgg;
pub(super) struct AvgStateSignedAgg;

impl AggregateFunction for AvgStateAgg {
    // Delegate to SumStateAgg for all state machine ops; the only
    // user-observable difference (visible value) is computed in the
    // scalar function `avg_state_visible`, not here.
    // Implementation: explicitly forward each method to the inner Sum impl.
}
// Same delegation for AvgStateSignedAgg.
```

- [ ] **Step 3: Register**

```rust
"avg_state"          => Ok(&AVG_STATE),
"avg_state_signed"   => Ok(&AVG_STATE_SIGNED),
```

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::avg 2>&1 | tail -10`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/avg.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add avg_state / avg_state_signed delegating to sum state layout"
```

### Task 2.6: `min_state`, `max_state`, `min_state_signed`, `max_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/min_max.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

Min/Max share the multiset state encoding (§3.3). Both produce identical state; visible differs.

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn min_state_collects_multiset_entries() {
    // Input: 5, 5, 3, NULL, 5
    let bytes = run_min_state_int64(&[Some(5), Some(5), Some(3), None, Some(5)]);
    let entries = state_codec::decode_multiset_with_key_type(&bytes, &DataType::Int64).unwrap();
    // Expected: sorted ascending: [(3, 1), (5, 3)]
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].count, 1);  // key=3 once
    assert_eq!(entries[1].count, 3);  // key=5 three times
}

#[test]
fn min_state_and_max_state_produce_identical_bytes() {
    let min_bytes = run_min_state_int64(&[Some(1), Some(2), Some(3)]);
    let max_bytes = run_max_state_int64(&[Some(1), Some(2), Some(3)]);
    assert_eq!(min_bytes, max_bytes,
        "Min and Max state are byte-identical; visible differs only");
}

#[test]
fn min_state_signed_handles_delete() {
    let bytes = run_min_state_signed_int64(&[Some(5), Some(5)], &[0, 1]);  // INSERT 5, DELETE 5
    let entries = state_codec::decode_multiset_with_key_type(&bytes, &DataType::Int64).unwrap();
    assert!(entries.is_empty(), "INSERT then DELETE of same key cancels");
}
```

- [ ] **Step 2: Implement `MinMaxStateAgg` and `MinMaxStateSignedAgg`**

Shared struct for Min/Max (state is identical):

```rust
pub(super) struct MinMaxStateAgg;
pub(super) struct MinMaxStateSignedAgg;

// State during accumulation: BTreeMap<Vec<u8>, i64>  (key_bytes -> count)
// finalize_to: convert map to sorted Vec<MultisetEntry>, encode_multiset.
```

The struct also stores the input column's Arrow type (captured in `spec`) so `update_batch` knows how to serialize each row's value via `state_codec::write_key_at`.

- [ ] **Step 3: Register**

```rust
"min_state"          => Ok(&MIN_MAX_STATE),
"max_state"          => Ok(&MIN_MAX_STATE),
"min_state_signed"   => Ok(&MIN_MAX_STATE_SIGNED),
"max_state_signed"   => Ok(&MIN_MAX_STATE_SIGNED),
```

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::min_max 2>&1 | tail -10`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/min_max.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add min_state/max_state/_signed sharing multiset encoding"
```

---

## Phase 3: Scalar combinators for existing kinds

For each kind, implement `<kind>_state_union(a, b) -> VARBINARY` and `<kind>_state_visible(s) -> <original return type>` scalar functions. These run during the LEFT JOIN merge step and final visible derivation respectively.

### Task 3.1: Create mv_state scalar module skeleton

**Files:**
- Create: `src/exec/expr/function/mv_state/mod.rs`
- Modify: `src/exec/expr/function/mod.rs`

- [ ] **Step 1: Create scalar module gate**

```rust
// src/exec/expr/function/mv_state/mod.rs
//! Scalar functions for IVM materialized view state combinator operations.
//!
//! Each kind has:
//!   - <kind>_state_union(a, b)  -> VARBINARY  (merge two states)
//!   - <kind>_state_visible(s)   -> <return type> (finalize to user-visible)
//!
//! Plus the debug helper:
//!   - DEBUG_DUMP_MV_STATE(mv_name, row_id) -> Utf8 (JSON representation)

pub(super) mod count;
pub(super) mod bool_or_and;
pub(super) mod sum;
pub(super) mod avg;
pub(super) mod min_max;
pub(super) mod count_distinct;
pub(super) mod approx_count_distinct;
pub(super) mod debug_dump;
```

Create empty per-kind files with consistent header.

- [ ] **Step 2: Declare in scalar functions root**

In `src/exec/expr/function/mod.rs`, add:

```rust
pub(crate) mod mv_state;
```

- [ ] **Step 3: Verify build**

Run: `cargo build -p novarocks 2>&1 | tail -10`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/exec/expr/function/mv_state/ src/exec/expr/function/mod.rs
git commit -m "ivm/scalar: scaffold mv_state scalar function module tree"
```

### Task 3.2: `count_state_union` and `count_state_visible`

**Files:**
- Modify: `src/exec/expr/function/mv_state/count.rs`
- Modify: `src/sql/analyzer/functions.rs`

- [ ] **Step 1: Failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::state_codec;

    #[test]
    fn count_state_union_sums() {
        let a = state_codec::encode_count_state(10);
        let b = state_codec::encode_count_state(5);
        let merged = count_state_union(&a, &b).unwrap();
        assert_eq!(state_codec::decode_count_state(&merged).unwrap(), 15);
    }

    #[test]
    fn count_state_union_empty_left() {
        let b = state_codec::encode_count_state(7);
        let merged = count_state_union(&[], &b).unwrap();
        assert_eq!(merged, b);
    }

    #[test]
    fn count_state_union_canceled_returns_empty() {
        let a = state_codec::encode_count_state(5);
        let b = state_codec::encode_count_state(-5);
        let merged = count_state_union(&a, &b).unwrap();
        assert!(merged.is_empty(), "canceled count canonicalizes to empty state");
    }

    #[test]
    fn count_state_visible_returns_count() {
        let s = state_codec::encode_count_state(42);
        assert_eq!(count_state_visible(&s).unwrap(), 42);
    }

    #[test]
    fn count_state_visible_empty_returns_zero() {
        assert_eq!(count_state_visible(&[]).unwrap(), 0);
    }
}
```

Run: expected fail.

- [ ] **Step 2: Implement the two functions**

```rust
use crate::connector::starrocks::managed::state_codec;

pub(crate) fn count_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    let ca = state_codec::decode_count_state(a)?;
    let cb = state_codec::decode_count_state(b)?;
    let total = ca + cb;
    if total == 0 {
        Ok(Vec::new())
    } else {
        Ok(state_codec::encode_count_state(total))
    }
}

pub(crate) fn count_state_visible(s: &[u8]) -> Result<i64, String> {
    state_codec::decode_count_state(s)
}
```

- [ ] **Step 3: Register in scalar function registry**

In `src/sql/analyzer/functions.rs`, add to the scalar function whitelist:

```rust
"count_state_union"          // (VARBINARY, VARBINARY) -> VARBINARY
"count_state_visible"        // (VARBINARY) -> BIGINT
```

Add corresponding entries in `validate_scalar_function_call_impl` for type checking.

The scalar function dispatch in NovaRocks (likely in `src/exec/expr/function/`) needs to be wired to invoke these helpers — follow the pattern of an existing scalar like `map_value_count`'s scalar variant if one exists, or any binary→binary scalar like `from_binary`.

- [ ] **Step 4: Verify tests pass**

Run: `cargo test -p novarocks mv_state::count 2>&1 | tail -10`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/function/mv_state/count.rs src/sql/analyzer/functions.rs
git commit -m "ivm/scalar: add count_state_union and count_state_visible"
```

### Task 3.3 — 3.7: Repeat for {BoolOrAnd, Sum, Avg, MinMax} kinds

For each kind, follow the same pattern as Task 3.2:

**Task 3.3: `bool_or_state_union`, `bool_or_state_visible`, `bool_and_state_union`, `bool_and_state_visible`**
- Bool*_union: sum both `count_true` and `count_false`; if both zero → empty
- bool_or_visible: NULL if empty; true if count_true > 0; false if count_false > 0; else NULL
- bool_and_visible: NULL if empty; false if count_false > 0; true if count_true > 0; else NULL
- Tests must cover all 4 visible paths per spec §5.1

**Task 3.4: `sum_state_union`, `sum_state_visible`**
- _union: sum row_counts and sums (Int64 or Decimal128 based on type metadata)
- _visible: if row_count == 0 → NULL; else sum

**Task 3.5: `avg_state_union`, `avg_state_visible`**
- _union: identical to sum_state_union (shared layout)
- _visible: if row_count == 0 → NULL; else sum / row_count

**Task 3.6: `min_state_union`, `min_state_visible`, `max_state_union`, `max_state_visible`**
- min/max_union: identical (`union_multisets` already does the work). Register both names to same impl.
- min_visible: scan multiset from front, return first key with count > 0; else NULL
- max_visible: scan multiset from back, return first key with count > 0; else NULL
- Both must decode key bytes back to the original Arrow type (visible needs the actual scalar value)

Each task follows 5 steps: tests → impl → register → verify → commit. Commit messages parallel Task 3.2's.

After all subtasks: `cargo test -p novarocks mv_state 2>&1 | tail -10` should show all scalar combinator tests passing (~25 tests).

---

## Phase 4: New aggregate kind — CountDistinct

### Task 4.1: Add `AggregateFunctionKind::CountDistinct` enum variant

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs:94-107` (the `AggregateFunctionKind` enum)

- [ ] **Step 1: Add the variant**

In the `AggregateFunctionKind` enum:

```rust
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolOr,
    BoolAnd,
    /// `count(DISTINCT col)` / `count_distinct(col)` / `multi_distinct_count(col)`.
    /// Uses shared §3.3 multiset state encoding; visible counts positive entries.
    CountDistinct,
}
```

- [ ] **Step 2: Add a failing match-exhaustiveness check**

Find all `match` statements on `AggregateFunctionKind` in `mv_shape.rs`, `mv_agg_state.rs`, `ivm_delta_aggregate.rs`. Run `cargo build -p novarocks 2>&1 | grep -A2 "non-exhaustive"` to enumerate them.

Expected: compiler errors at every match site.

- [ ] **Step 3: Add stub arms returning `unimplemented!("CountDistinct: see Task 4.x")` at each match**

This intentionally keeps the codebase compiling but ensures we hit the unimplemented at runtime if CountDistinct slips through. Later tasks remove these stubs.

- [ ] **Step 4: Verify build**

Run: `cargo build -p novarocks 2>&1 | tail -5`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/ivm_delta_aggregate.rs
git commit -m "ivm/shape: add CountDistinct variant to AggregateFunctionKind with unimplemented stubs"
```

### Task 4.2: `count_distinct_state` and `count_distinct_state_signed`

**Files:**
- Modify: `src/exec/expr/agg/functions/state_combinators/count_distinct.rs`
- Modify: `src/exec/expr/agg/functions/mod.rs`

CountDistinct shares the same aggregate state computation as Min/Max (multiset encoding). The agg function impl can be aliased:

```rust
// In count_distinct.rs
pub(super) use super::min_max::{MinMaxStateAgg as CountDistinctStateAgg,
                                 MinMaxStateSignedAgg as CountDistinctStateSignedAgg};
```

- [ ] **Step 1: Add tests confirming byte equality with min_state**

```rust
#[test]
fn count_distinct_state_byte_equal_to_min_state() {
    let cd = run_count_distinct_state_int64(&[Some(5), Some(5), Some(3)]);
    let mn = run_min_state_int64(&[Some(5), Some(5), Some(3)]);
    assert_eq!(cd, mn, "CountDistinct state must be byte-identical to Min state on same input");
}
```

- [ ] **Step 2: Register in `mod.rs`**

```rust
"count_distinct_state"        => Ok(&MIN_MAX_STATE),  // shared impl
"count_distinct_state_signed" => Ok(&MIN_MAX_STATE_SIGNED),
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p novarocks state_combinators::count_distinct 2>&1 | tail -10`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/exec/expr/agg/functions/state_combinators/count_distinct.rs src/exec/expr/agg/functions/mod.rs
git commit -m "ivm/agg: add count_distinct_state aliased to multiset state encoding"
```

### Task 4.3: `count_distinct_state_union` and `count_distinct_state_visible`

**Files:**
- Modify: `src/exec/expr/function/mv_state/count_distinct.rs`
- Modify: `src/sql/analyzer/functions.rs`

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn count_distinct_state_visible_counts_positive_entries() {
    // Multiset with 3 distinct keys, all positive
    let entries = vec![
        MultisetEntry { key_bytes: 1i64.to_le_bytes().to_vec(), count: 5 },
        MultisetEntry { key_bytes: 2i64.to_le_bytes().to_vec(), count: 1 },
        MultisetEntry { key_bytes: 3i64.to_le_bytes().to_vec(), count: 2 },
    ];
    let s = encode_multiset(&entries);
    assert_eq!(count_distinct_state_visible(&s).unwrap(), 3);
}

#[test]
fn count_distinct_state_visible_skips_zero_or_negative() {
    let entries = vec![
        MultisetEntry { key_bytes: 1i64.to_le_bytes().to_vec(), count: 5 },
        MultisetEntry { key_bytes: 2i64.to_le_bytes().to_vec(), count: 0 },   // shouldn't normally appear, but be defensive
        MultisetEntry { key_bytes: 3i64.to_le_bytes().to_vec(), count: -1 },  // ditto
    ];
    let s = encode_multiset(&entries);
    assert_eq!(count_distinct_state_visible(&s).unwrap(), 1);
}

#[test]
fn count_distinct_state_visible_empty_returns_zero() {
    assert_eq!(count_distinct_state_visible(&[]).unwrap(), 0);
}

#[test]
fn count_distinct_state_union_shares_multiset_union() {
    // min_state_union and count_distinct_state_union must produce identical bytes
    let a_entries = vec![MultisetEntry { key_bytes: 1i64.to_le_bytes().to_vec(), count: 2 }];
    let b_entries = vec![MultisetEntry { key_bytes: 2i64.to_le_bytes().to_vec(), count: 3 }];
    let a = encode_multiset(&a_entries);
    let b = encode_multiset(&b_entries);
    let cd_merged = count_distinct_state_union(&a, &b).unwrap();
    let min_merged = crate::exec::expr::function::mv_state::min_max::min_state_union(&a, &b).unwrap();
    assert_eq!(cd_merged, min_merged);
}
```

- [ ] **Step 2: Implement**

```rust
pub(crate) fn count_distinct_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    // Identical to min_state_union / max_state_union: union multisets, drop non-positive.
    crate::exec::expr::function::mv_state::min_max::min_state_union(a, b)
}

pub(crate) fn count_distinct_state_visible(s: &[u8]) -> Result<i64, String> {
    if s.is_empty() { return Ok(0); }
    // We don't need the actual key values — only counts. So we can iterate
    // without knowing the key type.
    let mut cursor = &s[1..];  // skip version byte
    let n = state_codec::read_uleb128(&mut cursor)?;
    let mut positive = 0i64;
    // We need to skip each key. Since key types vary, we need the spec's
    // input type at union call. For visible, the type is encoded in the SQL
    // function's argument type metadata — captured at scalar function dispatch.
    // For now, we operate on entries already in normalized form: walk via
    // generic decoder.
    // PROBLEM at this point: the visible function needs to walk entries, but
    // the key type isn't known from the state bytes alone. See Step 3 below
    // for the resolution: add a type tag byte to the multiset encoding.
    return Err("walk keys requires key_type_tag byte — see Step 3".into())
}
```

Note: The visible function needs the key type metadata. NovaRocks scalar functions receive their `FunctionContext` which carries the input/output types. The type metadata must be plumbed from the MV's layout into the scalar function call. Alternative: pass the key type as a constant parameter (but that's awkward in SQL).

**Resolution adopted by this plan:** store a `key_type_tag: u8` byte after the version byte in the multiset encoding. This adds 1 byte per state but makes decoders self-contained. This is a small spec deviation from §3.3 — flagged in the plan's Notes section at the bottom; spec author should update §3.3 to match.

- [ ] **Step 3: Update spec §3.3 to add type tag**

This is a deviation worth noting; the implementer should consult with the spec author. The simplest resolution is to record the type tag inline. The encoding becomes:

```text
multiset_state :=
    u8       version = 0x01
    u8       key_type_tag       -- 0=Boolean, 1=Int8, 2=Int16, ..., 9=Utf8, etc.
    ULEB128  num_entries
    entry[num_entries]
```

Update `encode_multiset` to take a `key_type_tag: u8` parameter; update all callers. Refactor existing tests.

- [ ] **Step 4: Implement using the type tag**

```rust
pub(crate) fn count_distinct_state_visible(s: &[u8]) -> Result<i64, String> {
    if s.is_empty() { return Ok(0); }
    let entries = state_codec::decode_multiset_self_describing(s)?;
    Ok(entries.iter().filter(|e| e.count > 0).count() as i64)
}
```

`decode_multiset_self_describing` reads the type tag byte and dispatches to the right key decoder. Implement it in state_codec.rs.

- [ ] **Step 5: Run tests, register, commit**

Run: `cargo test -p novarocks mv_state::count_distinct 2>&1 | tail -10`
Expected: 4 passed.

Register:

```rust
"count_distinct_state_union"   // (VARBINARY, VARBINARY) -> VARBINARY
"count_distinct_state_visible" // (VARBINARY) -> BIGINT
```

```bash
git add src/exec/expr/function/mv_state/count_distinct.rs src/sql/analyzer/functions.rs src/connector/starrocks/managed/state_codec.rs
git commit -m "ivm/scalar: add count_distinct_state_union/visible + self-describing multiset codec"
```

### Task 4.4: Classifier dispatch for `count_distinct` aliases

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs:616-637` (the function-name match in `classify_aggregate_call`)
- Modify: `src/connector/starrocks/managed/mv_shape.rs:611` (the DISTINCT modifier reject)

- [ ] **Step 1: Failing tests at the bottom of mv_shape.rs**

```rust
#[test]
fn classify_count_distinct_function_name() {
    let q = "SELECT region, count_distinct(user_id) FROM events GROUP BY region";
    let shape = classify_mv_query(q).unwrap();
    let agg_shape = match shape {
        IncrementalMvShape::Aggregate(s) => s,
        _ => panic!("expected aggregate shape"),
    };
    assert_eq!(agg_shape.aggregates[0].function, AggregateFunctionKind::CountDistinct);
}

#[test]
fn classify_count_distinct_via_distinct_modifier() {
    let q = "SELECT region, count(DISTINCT user_id) FROM events GROUP BY region";
    let shape = classify_mv_query(q).unwrap();
    let agg_shape = match shape {
        IncrementalMvShape::Aggregate(s) => s,
        _ => panic!("expected aggregate shape"),
    };
    assert_eq!(agg_shape.aggregates[0].function, AggregateFunctionKind::CountDistinct);
}

#[test]
fn classify_multi_distinct_count() {
    let q = "SELECT region, multi_distinct_count(user_id) FROM events GROUP BY region";
    let shape = classify_mv_query(q).unwrap();
    let agg_shape = match shape {
        IncrementalMvShape::Aggregate(s) => s,
        _ => panic!("expected aggregate shape"),
    };
    assert_eq!(agg_shape.aggregates[0].function, AggregateFunctionKind::CountDistinct);
}

#[test]
fn classify_count_distinct_multi_arg_rejected() {
    let q = "SELECT region, count(DISTINCT user_id, session_id) FROM events GROUP BY region";
    let err = classify_mv_query(q).unwrap_err();
    assert!(err.contains("multi-column DISTINCT"), "got: {err}");
}

#[test]
fn classify_distinct_on_non_count_rejected() {
    let q = "SELECT region, sum(DISTINCT amount) FROM events GROUP BY region";
    let err = classify_mv_query(q).unwrap_err();
    assert!(err.contains("DISTINCT") || err.contains("not supported"));
}
```

Where `classify_mv_query` is a small helper that wraps the existing `classify_aggregate_mv_query` (add it to the test module if not present).

- [ ] **Step 2: Update classifier**

Replace the function-name match arm:

```rust
"count" => classify_count_input(&args)?,
```

to handle both regular and DISTINCT forms; add the new function-name aliases:

```rust
let function_name = function.name.to_string().to_ascii_lowercase();

// Handle count(DISTINCT col) syntax first, before duplicate_treatment rejection
if function_name == "count" {
    if let Some(dup) = args.duplicate_treatment {
        match dup {
            sqlparser::ast::DuplicateTreatment::Distinct => {
                return classify_count_distinct_from_distinct_syntax(&args.args, output_name);
            }
            sqlparser::ast::DuplicateTreatment::All => {
                return Err(aggregate_error());  // count(ALL ...) is unusual; reject for safety
            }
        }
    }
}

// Reject DISTINCT on any non-count function name
if args.duplicate_treatment.is_some() {
    return Err(format!(
        "DISTINCT modifier is not supported on `{function_name}` in incremental \
         materialized views; only `count(DISTINCT col)` is supported. \
         Consider using a separate `{function_name}_distinct` form if available."
    ));
}

let (function, input) = match function_name.as_str() {
    "count" => classify_count_input(&args.args)?,
    "sum"   => (AggregateFunctionKind::Sum, classify_sum_input(&args.args)?),
    "avg"   => (AggregateFunctionKind::Avg, classify_avg_input(&args.args)?),
    "min"   => (AggregateFunctionKind::Min, classify_min_max_input(&args.args)?),
    "max"   => (AggregateFunctionKind::Max, classify_min_max_input(&args.args)?),
    "bool_or" | "boolor_agg"  => (AggregateFunctionKind::BoolOr, classify_bool_or_and_input(&args.args)?),
    "bool_and" | "booland_agg" => (AggregateFunctionKind::BoolAnd, classify_bool_or_and_input(&args.args)?),
    "count_distinct" | "multi_distinct_count" => (
        AggregateFunctionKind::CountDistinct,
        classify_count_distinct_input(&args.args)?,
    ),
    _ => return Err(aggregate_error()),
};
```

Add `classify_count_distinct_input` and `classify_count_distinct_from_distinct_syntax`:

```rust
fn classify_count_distinct_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "COUNT(DISTINCT) with {} arguments is not supported in incremental materialized views; \
             multi-column DISTINCT cannot be incrementally maintained. \
             Consider concatenating the columns: COUNT(DISTINCT CONCAT(col1, '|', col2))",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("COUNT(DISTINCT) requires exactly one column expression".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("COUNT(DISTINCT *) is not supported".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}

fn classify_count_distinct_from_distinct_syntax(
    args: &[sqlparser::ast::FunctionArg],
    output_name: String,
) -> Result<AggregateCallShape, String> {
    let input = classify_count_distinct_input(args)?;
    Ok(AggregateCallShape {
        output_name,
        function: AggregateFunctionKind::CountDistinct,
        input,
    })
}
```

- [ ] **Step 3: Verify tests pass**

Run: `cargo test -p novarocks mv_shape::tests 2>&1 | tail -15`
Expected: 5 new tests pass.

- [ ] **Step 4: Update `is_aggregate_function` (line 1253)**

Already contains `count_distinct` and `multi_distinct_count` — no change.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs
git commit -m "ivm/shape: route count(DISTINCT)/count_distinct/multi_distinct_count to CountDistinct kind"
```

### Task 4.5: Type-domain validation for CountDistinct in AggregateMvLayout

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs` (find layout construction sites for `AggregateFunctionKind::Min/Max`)

CountDistinct reuses the same type-domain rules as Min/Max (per §6.1) — both allow the same scalar Arrow types and reject nested/binary/dictionary. Find the validation function that gates Min/Max key types and extend to also accept CountDistinct.

- [ ] **Step 1: Failing test**

```rust
#[test]
fn count_distinct_rejects_struct_key() {
    let shape = aggregate_shape_with_struct_key_count_distinct();  // helper
    let err = AggregateMvLayout::new(&shape).unwrap_err();
    assert!(err.contains("struct"), "got: {err}");
}

#[test]
fn count_distinct_accepts_int64_key() {
    let shape = aggregate_shape_with_int64_key_count_distinct();
    AggregateMvLayout::new(&shape).unwrap();
}
```

- [ ] **Step 2: Extend the existing Min/Max key-type validator to include CountDistinct**

Find the match in `mv_agg_state.rs` (look near line 184 where BoolOr is handled) and add CountDistinct to the same branch as Min/Max.

- [ ] **Step 3: Verify, commit**

Run: `cargo test -p novarocks mv_agg_state 2>&1 | tail -10`
Expected: pass.

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs
git commit -m "ivm/layout: accept CountDistinct in scalar-key type-domain validator"
```

---

## Phase 5: New aggregate kind — ApproxCountDistinct

### Task 5.1: Add `AggregateFunctionKind::ApproxCountDistinct` enum variant

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs` (the `AggregateFunctionKind` enum)
- Modify: all match sites on `AggregateFunctionKind` flagged by the compiler

- [ ] **Step 1: Add the variant**

```rust
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolOr,
    BoolAnd,
    CountDistinct,
    /// `approx_count_distinct(col)` / `ndv(col)` / `hll_ndv(col)`.
    /// Shares §3.3 multiset state with CountDistinct; visible computes HLL estimate
    /// over positive entries (reusing plain HLL hash + estimator for cross-path equality).
    ApproxCountDistinct,
}
```

- [ ] **Step 2: Hit the compiler-flagged match sites**

Run: `cargo build -p novarocks 2>&1 | grep "non-exhaustive\|missing match arm" | head -10`
Expected: enumerated list of match sites in `mv_shape.rs`, `mv_agg_state.rs`, `ivm_delta_aggregate.rs`.

At each match site, add an arm returning `unimplemented!("ApproxCountDistinct: see Task 5.x")`. Phase 5 fills these in.

- [ ] **Step 3: Verify build**

Run: `cargo build -p novarocks 2>&1 | tail -5`
Expected: clean (no unmatched patterns).

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/ivm_delta_aggregate.rs
git commit -m "ivm/shape: add ApproxCountDistinct variant with unimplemented stubs"
```

### Task 5.2: `approx_count_distinct_state` and `_state_signed`

Alias to the same multiset state aggregate as Min/Max/CountDistinct:

```rust
// src/exec/expr/agg/functions/state_combinators/approx_count_distinct.rs
pub(super) use super::min_max::{MinMaxStateAgg as ApproxCountDistinctStateAgg,
                                 MinMaxStateSignedAgg as ApproxCountDistinctStateSignedAgg};
```

Register:

```rust
"approx_count_distinct_state"        => Ok(&MIN_MAX_STATE),
"approx_count_distinct_state_signed" => Ok(&MIN_MAX_STATE_SIGNED),
```

Test: byte equality with `count_distinct_state` on same input.

Commit: `ivm/agg: alias approx_count_distinct_state to multiset state encoding`.

### Task 5.3: `approx_count_distinct_state_union`

Identical to `count_distinct_state_union`. Alias:

```rust
pub(crate) fn approx_count_distinct_state_union(a: &[u8], b: &[u8]) -> Result<Vec<u8>, String> {
    crate::exec::expr::function::mv_state::min_max::min_state_union(a, b)
}
```

Register, test (byte equality with count_distinct_state_union), commit.

### Task 5.4: `approx_count_distinct_state_visible` — HLL estimation over multiset

**Files:**
- Modify: `src/exec/expr/function/mv_state/approx_count_distinct.rs`
- Modify: `src/exec/expr/agg/functions/hll_raw.rs` — expose `update_register_from_hash` and `estimate_cardinality_from_registers` as `pub(crate)` if not already

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn approx_count_distinct_visible_matches_plain_hll() {
    // Build a multiset of 100 distinct Int64 keys, each with count 1.
    let entries: Vec<MultisetEntry> = (0..100).map(|i| MultisetEntry {
        key_bytes: (i as i64).to_le_bytes().to_vec(),
        count: 1,
    }).collect();
    let s = encode_multiset_with_type(&entries, KeyTypeTag::Int64);

    let mv_estimate = approx_count_distinct_state_visible(&s).unwrap();

    // Compute plain HLL estimate on the same data for comparison
    let mut registers = [0u8; HLL_REGISTERS_COUNT];
    for i in 0..100i64 {
        let hash = hash_i64_for_hll(i);
        update_register_from_hash(&mut registers, hash);
    }
    let plain_estimate = estimate_cardinality_from_registers(&registers);

    assert_eq!(mv_estimate, plain_estimate,
        "MV state visible must produce bit-identical estimate to plain HLL");
}

#[test]
fn approx_count_distinct_visible_ignores_multiplicity() {
    // Same key with different counts should produce same estimate
    let s1 = encode_multiset_with_type(&[MultisetEntry { key_bytes: 7i64.to_le_bytes().to_vec(), count: 1 }], KeyTypeTag::Int64);
    let s100 = encode_multiset_with_type(&[MultisetEntry { key_bytes: 7i64.to_le_bytes().to_vec(), count: 100 }], KeyTypeTag::Int64);
    assert_eq!(
        approx_count_distinct_state_visible(&s1).unwrap(),
        approx_count_distinct_state_visible(&s100).unwrap(),
    );
}

#[test]
fn approx_count_distinct_visible_empty_returns_zero() {
    assert_eq!(approx_count_distinct_state_visible(&[]).unwrap(), 0);
}

#[test]
fn approx_count_distinct_visible_skips_non_positive() {
    let entries = vec![
        MultisetEntry { key_bytes: 1i64.to_le_bytes().to_vec(), count: 1 },
        MultisetEntry { key_bytes: 2i64.to_le_bytes().to_vec(), count: 0 },
        MultisetEntry { key_bytes: 3i64.to_le_bytes().to_vec(), count: -1 },
    ];
    let s = encode_multiset_with_type(&entries, KeyTypeTag::Int64);
    // Only key=1 contributes to HLL
    assert!(approx_count_distinct_state_visible(&s).unwrap() <= 2);  // typically 1 with bias correction
}
```

- [ ] **Step 2: Implement**

```rust
use crate::connector::starrocks::managed::state_codec;
use crate::exec::expr::agg::functions::hll_raw::{
    HLL_REGISTERS_COUNT,
    update_register_from_hash,
    estimate_cardinality_from_registers,
    hash_key_bytes,  // see Step 3
};

pub(crate) fn approx_count_distinct_state_visible(s: &[u8]) -> Result<i64, String> {
    if s.is_empty() { return Ok(0); }

    let entries = state_codec::decode_multiset_self_describing(s)?;

    // Thread-local reusable register buffer (16 KB) to avoid per-row allocation.
    thread_local! {
        static REGISTERS: std::cell::RefCell<[u8; HLL_REGISTERS_COUNT]>
            = std::cell::RefCell::new([0u8; HLL_REGISTERS_COUNT]);
    }

    REGISTERS.with(|cell| {
        let mut regs = cell.borrow_mut();
        regs.fill(0);  // reset for this state

        for entry in &entries {
            if entry.count <= 0 { continue; }
            // Hash the canonical key bytes directly with the same hash plain HLL uses.
            let hash = hash_key_bytes(&entry.key_bytes);
            update_register_from_hash(&mut *regs, hash);
        }

        Ok(estimate_cardinality_from_registers(&*regs))
    })
}
```

- [ ] **Step 3: Expose / add `hash_key_bytes` in `hll_raw.rs`**

Find or add a function that takes `&[u8]` and produces the same `u64` hash that plain HLL produces for an equivalent Arrow value. If plain HLL hashes values through Arrow-specific paths (e.g., separately for Int64 vs Utf8), we may need to ensure the multiset's canonical key bytes match the byte representation plain HLL uses.

**Verify cross-path equivalence**: write an integration test that runs `approx_count_distinct(col)` on a base table and `approx_count_distinct_state_visible(mv_state)` on the corresponding MV, and asserts bit-identical results.

- [ ] **Step 4: Run tests**

```bash
cargo test -p novarocks mv_state::approx_count_distinct 2>&1 | tail -15
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/exec/expr/function/mv_state/approx_count_distinct.rs src/exec/expr/agg/functions/hll_raw.rs
git commit -m "ivm/scalar: add approx_count_distinct_state_visible with HLL estimator + buffer reuse"
```

### Task 5.5: Classifier dispatch for `approx_count_distinct` aliases

Mirror Task 4.4 but for the new kind. Add to the function-name match:

```rust
"approx_count_distinct" | "ndv" | "hll_ndv" => (
    AggregateFunctionKind::ApproxCountDistinct,
    classify_approx_count_distinct_input(&args.args)?,
),
```

`classify_approx_count_distinct_input`:

```rust
fn classify_approx_count_distinct_input(args: &[sqlparser::ast::FunctionArg]) -> Result<AggregateInput, String> {
    if args.len() > 1 {
        return Err(format!(
            "APPROX_COUNT_DISTINCT with {} arguments is not supported in incremental materialized views; \
             the precision hint argument is not supported in IVM. \
             Please use the single-argument form: APPROX_COUNT_DISTINCT(col)",
            args.len()
        ));
    }
    let [arg] = args else {
        return Err("APPROX_COUNT_DISTINCT requires exactly one column expression".to_string());
    };
    let sqlparser::ast::FunctionArgExpr::Expr(expr) = simple_aggregate_arg_expr(arg)? else {
        return Err("APPROX_COUNT_DISTINCT(*) is not supported".to_string());
    };
    reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    Ok(AggregateInput::Expr(Box::new(expr.clone())))
}
```

Tests for: `approx_count_distinct(col)`, `ndv(col)`, `hll_ndv(col)` all routing to `ApproxCountDistinct`; hint argument `approx_count_distinct(col, 14)` rejected with specific error.

Commit: `ivm/shape: route approx_count_distinct/ndv/hll_ndv to ApproxCountDistinct kind`.

### Task 5.6: Type-domain validation for ApproxCountDistinct

Same as Task 4.5 for CountDistinct. Extend the Min/Max key-type validator to also accept ApproxCountDistinct.

Commit: `ivm/layout: accept ApproxCountDistinct in scalar-key type-domain validator`.

---

## Phase 6: Framework wiring — replace `map_value_count*` with per-kind dispatch

### Task 6.1: Per-kind dispatch in `ivm_delta_aggregate.rs::signed_delta_projection`

**Files:**
- Modify: `src/connector/starrocks/managed/ivm_delta_aggregate.rs:159-200` (the projection emit logic)

- [ ] **Step 1: Failing test**

```rust
#[test]
fn signed_delta_projection_emits_per_kind_combinator() {
    // Build a shape with CountDistinct on `user_id`, ApproxCountDistinct on `session_id`,
    // and BoolOr on `flag`.
    let shape = sample_shape_three_kinds();
    let select_sql = "SELECT region, COUNT(DISTINCT user_id) AS u, APPROX_COUNT_DISTINCT(session_id) AS s, BOOL_OR(flag) AS f FROM events GROUP BY region";
    let rewritten = rewrite_select_sql_for_signed_delta_state(select_sql, &shape).unwrap();

    assert!(rewritten.contains("count_distinct_state_signed(user_id, __change_op)"));
    assert!(rewritten.contains("approx_count_distinct_state_signed(session_id, __change_op)"));
    assert!(rewritten.contains("bool_or_state_signed(flag, __change_op)"));
    assert!(!rewritten.contains("map_value_count_signed"), "legacy combinator must be replaced");
}
```

- [ ] **Step 2: Replace `map_value_count_signed` hardcoding with per-kind dispatch**

In `signed_delta_projection`, replace the hard-coded function name lookup with:

```rust
fn combinator_name_for_kind(kind: AggregateFunctionKind, signed: bool) -> &'static str {
    match (kind, signed) {
        (AggregateFunctionKind::Count,                false) => "count_state",
        (AggregateFunctionKind::Count,                true)  => "count_state_signed",
        (AggregateFunctionKind::Sum,                  false) => "sum_state",
        (AggregateFunctionKind::Sum,                  true)  => "sum_state_signed",
        (AggregateFunctionKind::Avg,                  false) => "avg_state",
        (AggregateFunctionKind::Avg,                  true)  => "avg_state_signed",
        (AggregateFunctionKind::Min,                  false) => "min_state",
        (AggregateFunctionKind::Min,                  true)  => "min_state_signed",
        (AggregateFunctionKind::Max,                  false) => "max_state",
        (AggregateFunctionKind::Max,                  true)  => "max_state_signed",
        (AggregateFunctionKind::BoolOr,               false) => "bool_or_state",
        (AggregateFunctionKind::BoolOr,               true)  => "bool_or_state_signed",
        (AggregateFunctionKind::BoolAnd,              false) => "bool_and_state",
        (AggregateFunctionKind::BoolAnd,              true)  => "bool_and_state_signed",
        (AggregateFunctionKind::CountDistinct,        false) => "count_distinct_state",
        (AggregateFunctionKind::CountDistinct,        true)  => "count_distinct_state_signed",
        (AggregateFunctionKind::ApproxCountDistinct,  false) => "approx_count_distinct_state",
        (AggregateFunctionKind::ApproxCountDistinct,  true)  => "approx_count_distinct_state_signed",
    }
}
```

Use it in projection emission for every agg call. Also remove the explicit `AggregateFunctionKind` allow-list near line 1401 of `mv_shape.rs` that previously restricted to BoolOr/BoolAnd/Min/Max — now all kinds use the same generic dispatch.

- [ ] **Step 3: Verify**

Run: `cargo test -p novarocks ivm_delta_aggregate 2>&1 | tail -10`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/ivm_delta_aggregate.rs src/connector/starrocks/managed/mv_shape.rs
git commit -m "ivm/rewriter: emit per-kind state combinator in delta projection"
```

### Task 6.2: Visible expression rewrite at MV CREATE

**Files:**
- Modify: the MV view-definition rewriter (likely `src/engine/mv_flow.rs` or `src/connector/starrocks/managed/mv_agg_state.rs`)

When a user creates `CREATE MATERIALIZED VIEW m AS SELECT key, COUNT(DISTINCT col) FROM t GROUP BY key`, NovaRocks stores a transformed view definition that reads `__agg_state_0__` from the MV. The transformed expression for the agg call must be `count_distinct_state_visible(__agg_state_0__)`.

Find the existing visible-rewrite logic for BoolOr (it currently uses something like `derive_bool_or_from_detail_map(...)` per spec exploration of `mv_agg_state.rs:2231`). Replace per-kind:

- BoolOr → `bool_or_state_visible(<state>)`
- BoolAnd → `bool_and_state_visible(<state>)`
- Min → `min_state_visible(<state>)`
- Max → `max_state_visible(<state>)`
- Count → `count_state_visible(<state>)`
- Sum → `sum_state_visible(<state>)`
- Avg → `avg_state_visible(<state>)`
- CountDistinct → `count_distinct_state_visible(<state>)`
- ApproxCountDistinct → `approx_count_distinct_state_visible(<state>)`

Tests cover that the rewritten view definition contains the right `_state_visible` call per kind.

Commit: `ivm/view: rewrite agg call to per-kind state_visible at MV CREATE`.

### Task 6.3: Simplify `validate_state_column_type` to VARBINARY-only

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs::validate_state_column_type`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn validate_state_column_type_accepts_binary() {
    let field = Field::new("__agg_state_0__", DataType::Binary, true);
    assert!(validate_state_column_type(&field, AggregateFunctionKind::BoolOr).is_ok());
}

#[test]
fn validate_state_column_type_rejects_map() {
    let field = legacy_map_state_field("__agg_state_0__", DataType::Int64);
    let err = validate_state_column_type(&field, AggregateFunctionKind::BoolOr).unwrap_err();
    assert!(err.contains("VARBINARY"));
}

#[test]
fn validate_state_column_type_rejects_arbitrary_type() {
    let field = Field::new("__agg_state_0__", DataType::Int64, true);
    let err = validate_state_column_type(&field, AggregateFunctionKind::BoolOr).unwrap_err();
    assert!(err.contains("VARBINARY"));
}
```

- [ ] **Step 2: Rewrite the function**

```rust
pub(crate) fn validate_state_column_type(
    column: &arrow::datatypes::Field,
    _kind: AggregateFunctionKind,
) -> Result<(), String> {
    match column.data_type() {
        arrow::datatypes::DataType::Binary => Ok(()),
        other => Err(format!(
            "expected VARBINARY state column type for `{}`, got: {:?}",
            column.name(), other
        )),
    }
}
```

- [ ] **Step 3: Remove all `Map<K, Int64>`-specific code paths**

Search `mv_agg_state.rs` for `DataType::Map`, `Field::new_list`, `PARQUET:field_id`, `accumulate_map_entry`, `derive_visible_from_detail_map`, `pick_min_max_scalar`, `pick_bool_or_visible`, `pick_bool_and_visible`. **Delete all of them.** The `_state_visible` scalar functions added in Phase 3 replace these.

This is a substantial deletion (~2000 lines). Pay attention to:
- Test modules using these helpers — delete or rewrite tests
- Public API surface that other modules import — replace imports with calls to `mv_state::<kind>_state_visible`

- [ ] **Step 4: Run full test suite to find broken callers**

```bash
cargo build -p novarocks 2>&1 | tail -30
```

Fix compile errors one by one. They will surface in:
- `ivm_delta_aggregate.rs` — uses old helpers
- `mv_flow.rs` — uses old visible rewrite
- Possibly `src/engine/mv_flow.rs` — depends on cross-cutting

- [ ] **Step 5: Run mv_agg_state tests**

```bash
cargo test -p novarocks mv_agg_state 2>&1 | tail -10
```

Expected: 3 new tests pass; pre-existing tests for deleted helpers are removed.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs
git commit -m "ivm/state: simplify validate_state_column_type to VARBINARY-only; remove legacy Map paths"
```

### Task 6.4: Legacy P5 MV detection at open

**Files:**
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs::AggregateMvLayout::load_from_existing_mv`

- [ ] **Step 1: Failing test**

```rust
#[test]
fn load_legacy_map_state_mv_returns_clear_error() {
    let mv_schema = build_schema_with_legacy_map_state_column("mv_legacy");
    let err = AggregateMvLayout::load_from_existing_mv("mv_legacy", &mv_schema).unwrap_err();
    assert!(err.contains("legacy state column format"), "got: {err}");
    assert!(err.contains("DROP MATERIALIZED VIEW"), "missing DROP guidance: {err}");
    assert!(err.contains("CREATE MATERIALIZED VIEW"), "missing CREATE guidance: {err}");
    assert!(err.contains("mv_legacy"), "missing MV name in error: {err}");
}
```

- [ ] **Step 2: Implement detection**

```rust
impl AggregateMvLayout {
    pub(crate) fn load_from_existing_mv(
        mv_name: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Self, String> {
        let first_state = schema.fields().iter()
            .find(|f| f.name().starts_with(AGG_STATE_COLUMN_PREFIX));

        if let Some(field) = first_state {
            match field.data_type() {
                DataType::Binary => { /* new format, proceed */ }
                DataType::Map(_, _) | DataType::List(_) => {
                    return Err(format!(
                        "materialized view `{mv_name}` was created with a legacy state column \
                         format ({:?}). This format is no longer supported. Please recreate \
                         the materialized view:\n\
                         \n\
                           DROP MATERIALIZED VIEW {mv_name};\n\
                           CREATE MATERIALIZED VIEW {mv_name} AS <original SELECT>;\n",
                        field.data_type()
                    ));
                }
                other => {
                    return Err(format!(
                        "materialized view `{mv_name}` has unexpected state column type {other:?}; \
                         expected VARBINARY"
                    ));
                }
            }
        }

        // ... rest of load logic
        Ok(layout)
    }
}
```

- [ ] **Step 3: Verify, commit**

Run: `cargo test -p novarocks mv_agg_state::load 2>&1 | tail -10`
Expected: pass.

```bash
git add src/connector/starrocks/managed/mv_agg_state.rs
git commit -m "ivm/state: detect legacy Map MV on load with copy-executable migration guidance"
```

### Task 6.5: Remove `map_value_count` and `map_value_count_signed` aggregate functions

**Files:**
- Modify: `src/exec/expr/agg/functions/mod.rs`
- Delete: any file in `src/exec/expr/agg/functions/` named `map_value_count.rs` if it exists
- Modify: `src/connector/starrocks/managed/mv_shape.rs::is_aggregate_function` (line 1278-1279)

- [ ] **Step 1: Remove from `is_aggregate_function`**

Delete `| "map_value_count"` and `| "map_value_count_signed"`.

- [ ] **Step 2: Remove resolver entries in `src/exec/expr/agg/functions/mod.rs`**

```rust
// DELETE:
"map_value_count"        => ...,
"map_value_count_signed" => ...,
```

- [ ] **Step 3: Delete the impl file if present**

```bash
find src/exec/expr/agg/functions -name "map_value*"
git rm <found files>
```

- [ ] **Step 4: Run build**

```bash
cargo build -p novarocks 2>&1 | tail -10
```

Expected: clean. Any remaining references indicate Phase 6 missed a callsite.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "ivm/agg: remove map_value_count and map_value_count_signed (superseded by per-kind state combinators)"
```

---

## Phase 7: DEBUG_DUMP_MV_STATE tool

### Task 7.1: Implement DEBUG_DUMP_MV_STATE scalar

**Files:**
- Modify: `src/exec/expr/function/mv_state/debug_dump.rs`
- Modify: `src/sql/analyzer/functions.rs`

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn debug_dump_count_state() {
    let s = encode_count_state(42);
    let json = debug_dump_mv_state_bytes(&s, AggregateFunctionKind::Count).unwrap();
    assert!(json.contains("\"kind\":\"Count\""));
    assert!(json.contains("\"count\":42"));
}

#[test]
fn debug_dump_count_distinct_state_lists_entries() {
    let entries = vec![
        MultisetEntry { key_bytes: 1i64.to_le_bytes().to_vec(), count: 2 },
        MultisetEntry { key_bytes: 2i64.to_le_bytes().to_vec(), count: 5 },
    ];
    let s = encode_multiset_with_type(&entries, KeyTypeTag::Int64);
    let json = debug_dump_mv_state_bytes(&s, AggregateFunctionKind::CountDistinct).unwrap();
    assert!(json.contains("\"kind\":\"CountDistinct\""));
    assert!(json.contains("\"key\":1") && json.contains("\"count\":2"));
    assert!(json.contains("\"key\":2") && json.contains("\"count\":5"));
}

#[test]
fn debug_dump_empty_state() {
    let json = debug_dump_mv_state_bytes(&[], AggregateFunctionKind::CountDistinct).unwrap();
    assert!(json.contains("\"kind\":\"CountDistinct\""));
    assert!(json.contains("\"empty\":true"));
}
```

- [ ] **Step 2: Implement using `serde_json` (already in Cargo.toml per `roaring` lookup pattern)**

```rust
use serde_json::{json, Value};

pub(crate) fn debug_dump_mv_state_bytes(
    bytes: &[u8],
    kind: AggregateFunctionKind,
) -> Result<String, String> {
    if bytes.is_empty() {
        return Ok(json!({
            "kind": format!("{:?}", kind),
            "empty": true,
        }).to_string());
    }

    let value: Value = match kind {
        AggregateFunctionKind::Count => {
            let c = state_codec::decode_count_state(bytes)?;
            json!({"kind": "Count", "count": c})
        }
        AggregateFunctionKind::Sum => {
            // ...
        }
        AggregateFunctionKind::Avg => {
            // ...
        }
        AggregateFunctionKind::Min | AggregateFunctionKind::Max
        | AggregateFunctionKind::CountDistinct | AggregateFunctionKind::ApproxCountDistinct => {
            let entries = state_codec::decode_multiset_self_describing(bytes)?;
            let kind_label = match kind {
                AggregateFunctionKind::Min => "Min",
                AggregateFunctionKind::Max => "Max",
                AggregateFunctionKind::CountDistinct => "CountDistinct",
                AggregateFunctionKind::ApproxCountDistinct => "ApproxCountDistinct",
                _ => unreachable!(),
            };
            let entries_json: Vec<Value> = entries.iter().map(|e| {
                // Decode key based on key_type_tag (stored in bytes; recovered by decode_multiset_self_describing into KeyValue)
                json!({"key": format!("{:?}", e.key_value), "count": e.count})
            }).collect();
            json!({"kind": kind_label, "entries": entries_json})
        }
        AggregateFunctionKind::BoolOr | AggregateFunctionKind::BoolAnd => {
            let (ct, cf) = state_codec::decode_bool_state(bytes)?;
            let kind_label = if matches!(kind, AggregateFunctionKind::BoolOr) { "BoolOr" } else { "BoolAnd" };
            json!({"kind": kind_label, "count_true": ct, "count_false": cf})
        }
    };

    Ok(value.to_string())
}
```

- [ ] **Step 3: SQL surface**

Implement the SQL-facing entry `DEBUG_DUMP_MV_STATE(mv_table_name: VARCHAR, row_id: BIGINT) -> VARCHAR`:

The function takes a MV table name and a row ID, opens the MV, looks up the row, iterates over each `__agg_state_<n>__` column, and emits a JSON object containing all kinds' decoded states.

This requires runtime metadata lookup (the kind for each agg state column is in `AggregateMvLayout`). Implement as a special scalar that performs a small read.

- [ ] **Step 4: Register in `src/sql/analyzer/functions.rs`**

```rust
"debug_dump_mv_state"  // (VARCHAR, BIGINT) -> VARCHAR
```

- [ ] **Step 5: Run, commit**

```bash
cargo test -p novarocks mv_state::debug_dump 2>&1 | tail -10
```

```bash
git add src/exec/expr/function/mv_state/debug_dump.rs src/sql/analyzer/functions.rs
git commit -m "ivm/tool: add DEBUG_DUMP_MV_STATE scalar for per-kind state introspection"
```

---

## Phase 8: SQL fixtures

Each fixture follows the pattern of existing `iceberg_ivm_aggregate_*.sql` files. The fixtures live under `sql-tests/iceberg-ivm/sql/` with companion `.result` files captured via record mode.

### Task 8.1: CountDistinct fixtures

For each fixture file in spec §7.1:

- [ ] **Step 1: Write the fixture SQL**

Follow the pattern of `iceberg_ivm_aggregate_bool_or.sql` (the closest analog). Example for `iceberg_ivm_aggregate_count_distinct_insert_only.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,count_distinct,detail_state
-- Test Point (IVM-CountDistinct 2026-05-26): COUNT(DISTINCT col) over a BIGINT
-- column round-trips through the multiset state and visible counts distinct
-- positive entries. INSERT-only base; no DELETE yet.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_cd_db_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_cd_db_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_cd_db_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_cd_db_${uuid0}.ns_${uuid0}.events (
  id BIGINT,
  region STRING,
  user_id BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_cd_db_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW cd_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region,
       COUNT(DISTINCT user_id) AS distinct_users,
       COUNT(*) AS total_rows
FROM ice_ivm_cd_db_${uuid0}.ns_${uuid0}.events
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_cd_db_${uuid0}.ns_${uuid0}.events VALUES
  (1, 'east', 100), (2, 'east', 100), (3, 'east', 200),  -- east: distinct {100, 200}
  (4, 'west', 300), (5, 'west', 300), (6, 'west', 300);  -- west: distinct {300}
REFRESH MATERIALIZED VIEW cd_mv_${uuid0};

-- query 3
-- MV expected: east distinct_users=2 total_rows=3; west distinct_users=1 total_rows=3
SELECT region, distinct_users, total_rows
FROM cd_mv_${uuid0}
ORDER BY region;

-- query 4
-- Plain GROUP BY verification.
SELECT region,
       COUNT(DISTINCT user_id) AS distinct_users,
       COUNT(*) AS total_rows
FROM ice_ivm_cd_db_${uuid0}.ns_${uuid0}.events
GROUP BY region
ORDER BY region;

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW cd_mv_${uuid0};
DROP TABLE ice_ivm_cd_db_${uuid0}.ns_${uuid0}.events FORCE;
DROP DATABASE ice_ivm_cd_db_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_cd_db_${uuid0};
```

- [ ] **Step 2: Record the expected result**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_count_distinct_insert_only \
  --mode record
```

- [ ] **Step 3: Verify in `--mode verify`**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_count_distinct_insert_only \
  --mode verify
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_count_distinct_insert_only.{sql,result}
git commit -m "test(ivm): count_distinct INSERT-only fixture"
```

Repeat steps 1-4 for each remaining CountDistinct fixture (§7.1 has 13 files total). The patterns are:
- `_delete_boundary` — INSERT + DELETE that removes last row of a distinct value
- `_delete_non_boundary` — INSERT + DELETE that leaves the value's count > 0
- `_null_skipped` — NULL rows
- `_string` — Utf8 column
- `_decimal` — Decimal128 column
- `_float_nan` — NaN canonicalize
- `_date` — Date32 column
- `_timestamp` — Timestamp(μs) column
- `_syntax_aliases` — verify all three SQL forms produce the same MV state via DEBUG_DUMP
- `_partitioned` — partitioned base, partition evolution
- `_reject_nested_key` — CREATE fails with struct/list/map key
- `_reject_multi_arg` — CREATE fails with `count(DISTINCT a, b)`

### Task 8.2: ApproxCountDistinct fixtures

Repeat Task 8.1's pattern for the 9 fixtures in spec §7.2. Key fixture is `iceberg_ivm_aggregate_approx_count_distinct_cross_check_with_plain.sql` which asserts bit-equal results between plain HLL query and MV state visible.

### Task 8.3: Cross-kind symmetry fixture

`iceberg_ivm_aggregate_count_vs_approx_state_equality.sql` builds two MVs on the same base table and compares their state bytes via `DEBUG_DUMP_MV_STATE`:

```sql
-- Build two MVs on same base
CREATE MATERIALIZED VIEW cd_mv AS SELECT region, COUNT(DISTINCT user_id) FROM events GROUP BY region;
CREATE MATERIALIZED VIEW acd_mv AS SELECT region, APPROX_COUNT_DISTINCT(user_id) FROM events GROUP BY region;

-- After REFRESH, dump state bytes and assert equality
SELECT
  DEBUG_DUMP_MV_STATE('cd_mv', 1) = DEBUG_DUMP_MV_STATE('acd_mv', 1) AS state_bytes_equal;
```

Expected output: `state_bytes_equal = true`.

### Task 8.4: DEBUG_DUMP_MV_STATE fixtures

Three fixtures per spec §7.6 — verify the tool decodes each kind family correctly.

### Task 8.5: Update existing P5 fixture schema-reflection results

Find existing fixtures that include `SHOW CREATE MATERIALIZED VIEW` or `DESCRIBE` queries:

```bash
grep -l "SHOW CREATE MATERIALIZED VIEW\|DESCRIBE" sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_{bool_or,bool_and,min_max_*}.sql
```

For each, run `--mode record` to regenerate the `.result` file with the new VARBINARY schema. Verify no semantic regression by running `--mode verify` for the full iceberg-ivm suite.

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --mode verify
```

Expected: every test passes after record update.

Commit per group of fixtures.

---

## Phase 9: Verification + cleanup

### Task 9.1: Full build + clippy + format

- [ ] Run `cargo build -p novarocks 2>&1 | tail -10` — expect clean
- [ ] Run `cargo clippy -p novarocks --all-targets -- -D warnings 2>&1 | tail -10` — expect clean
- [ ] Run `cargo fmt --all -- --check` — expect clean (run `cargo fmt --all` if not)
- [ ] Commit any format/clippy fixes

### Task 9.2: Full Rust test suite

- [ ] Run `cargo test -p novarocks 2>&1 | tail -20`
- [ ] Expected: all existing tests pass; ~50 new state_codec / state_combinators / mv_state tests pass.

### Task 9.3: Full iceberg-ivm SQL suite

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --mode verify
```

- [ ] All ~85 fixtures (61 existing + ~25 new) pass.

### Task 9.4: Final commit + PR creation

- [ ] Squash review fixes if any; ensure commit history is logical (one commit per task is fine)
- [ ] Push branch and create PR with the release-notes template body from spec §8.6

---

## Notes

- This plan deviates slightly from spec §3.3 by adding a `key_type_tag: u8` byte after the version byte in the multiset encoding (see Task 4.3, Step 3 discussion). This is needed so `_state_visible` and `DEBUG_DUMP_MV_STATE` can decode entries without external type metadata. The deviation should be communicated back to the spec author and §3.3 updated to match.

- Phase 6's deletion of legacy Map-state code paths in `mv_agg_state.rs` is the largest mechanical task — expect ~2000 lines of deletions plus cascading callsite fixes. Allow extra time for finding all callers.

- Phase 8's fixture work is high-volume but mechanical. Each fixture takes ~30 minutes (write SQL, record, verify, commit). Total ~13 hours for fixtures alone.

- The plan is structured so each phase ends in a green build + green Rust tests. Phase 8 is the only phase where end-to-end SQL fixtures gate "done"; everything before that is unit-test-driven.

