//! Per-kind VARBINARY state codec for IVM detail-state aggregates.
//!
//! All non-empty states begin with `STATE_VERSION_V1 = 0x01`. Empty state
//! is a zero-length byte slice (no version byte) and is treated as `is_empty`
//! by every kind.
//!
//! Layout by kind: see docs/superpowers/specs/2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md §3.
//!
//! Serialized key bytes are canonical equality/storage identities only. They
//! are not SQL value-order-preserving sort keys; callers that need SQL Min/Max
//! ordering must decode keys to typed values or use a typed comparator.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

pub(crate) const STATE_VERSION_V1: u8 = 0x01;
const CANONICAL_F32_NAN_BITS: u32 = 0x7FC0_0000;
const CANONICAL_F64_NAN_BITS: u64 = 0x7FF8_0000_0000_0000;

/// Returns `true` iff `bytes` is the empty state (zero-length).
#[inline]
pub(crate) fn is_empty_state(bytes: &[u8]) -> bool {
    bytes.is_empty()
}

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

/// Reads one canonical ULEB128 value emitted by `write_uleb128`.
///
/// Bytes already inspected are consumed even on error.
pub(crate) fn read_uleb128(cursor: &mut &[u8]) -> Result<u64, String> {
    let original = *cursor;
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: ULEB128 truncated".to_string())?;
        *cursor = rest;
        let payload = (byte & 0x7F) as u64;
        if shift >= 64 || payload > (u64::MAX >> shift) {
            return Err("state_codec: ULEB128 overflow".to_string());
        }
        result |= payload << shift;
        if byte & 0x80 == 0 {
            let consumed_len = original.len() - cursor.len();
            let consumed = &original[..consumed_len];
            let mut canonical = Vec::new();
            write_uleb128(&mut canonical, result);
            if consumed != canonical {
                return Err("state_codec: ULEB128 non-canonical".to_string());
            }
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    // Canonical raw bits keep NaN equality stable across persisted state.
    Float32(u32),
    // Canonical raw bits keep NaN equality stable across persisted state.
    Float64(u64),
    Decimal128(i128),
    Date32(i32),
    Timestamp(i64),
    Utf8(String),
}

pub(crate) fn write_key_at(out: &mut Vec<u8>, array: &ArrayRef, idx: usize) -> Result<(), String> {
    if idx >= array.len() {
        return Err(format!(
            "state_codec: key index {idx} out of bounds for len {}",
            array.len()
        ));
    }
    if array.is_null(idx) {
        return Err("state_codec: cannot encode NULL key".to_string());
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = downcast_array::<BooleanArray>(array, "BooleanArray")?;
            out.push(u8::from(arr.value(idx)));
            Ok(())
        }
        DataType::Int8 => {
            let arr = downcast_array::<Int8Array>(array, "Int8Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Int16 => {
            let arr = downcast_array::<Int16Array>(array, "Int16Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Int32 => {
            let arr = downcast_array::<Int32Array>(array, "Int32Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Int64 => {
            let arr = downcast_array::<Int64Array>(array, "Int64Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Float32 => {
            let arr = downcast_array::<Float32Array>(array, "Float32Array")?;
            out.extend_from_slice(&canonical_f32_bits(arr.value(idx)).to_le_bytes());
            Ok(())
        }
        DataType::Float64 => {
            let arr = downcast_array::<Float64Array>(array, "Float64Array")?;
            out.extend_from_slice(&canonical_f64_bits(arr.value(idx)).to_le_bytes());
            Ok(())
        }
        DataType::Decimal128(_, _) => {
            let arr = downcast_array::<Decimal128Array>(array, "Decimal128Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(array, "Date32Array")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr =
                downcast_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
            out.extend_from_slice(&arr.value(idx).to_le_bytes());
            Ok(())
        }
        DataType::Utf8 => {
            let arr = downcast_array::<StringArray>(array, "StringArray")?;
            let bytes = arr.value(idx).as_bytes();
            write_uleb128(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
            Ok(())
        }
        DataType::LargeUtf8 => {
            let arr = downcast_array::<LargeStringArray>(array, "LargeStringArray")?;
            let bytes = arr.value(idx).as_bytes();
            write_uleb128(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
            Ok(())
        }
        other => Err(format!("state_codec: unsupported key type {other:?}")),
    }
}

pub(crate) fn read_key(cursor: &mut &[u8], dtype: &DataType) -> Result<KeyValue, String> {
    let mut local = *cursor;
    let value =
        match dtype {
            DataType::Boolean => {
                let b = read_fixed::<1>(&mut local, "Boolean")?[0];
                match b {
                    0 => Ok(KeyValue::Bool(false)),
                    1 => Ok(KeyValue::Bool(true)),
                    _ => Err("read_key: non-canonical Boolean".to_string()),
                }
            }
            DataType::Int8 => Ok(KeyValue::Int8(i8::from_le_bytes(read_fixed::<1>(
                &mut local, "Int8",
            )?))),
            DataType::Int16 => Ok(KeyValue::Int16(i16::from_le_bytes(read_fixed::<2>(
                &mut local, "Int16",
            )?))),
            DataType::Int32 => Ok(KeyValue::Int32(i32::from_le_bytes(read_fixed::<4>(
                &mut local, "Int32",
            )?))),
            DataType::Int64 => Ok(KeyValue::Int64(i64::from_le_bytes(read_fixed::<8>(
                &mut local, "Int64",
            )?))),
            DataType::Float32 => {
                let bits = u32::from_le_bytes(read_fixed::<4>(&mut local, "Float32")?);
                validate_f32_bits(bits)?;
                Ok(KeyValue::Float32(bits))
            }
            DataType::Float64 => {
                let bits = u64::from_le_bytes(read_fixed::<8>(&mut local, "Float64")?);
                validate_f64_bits(bits)?;
                Ok(KeyValue::Float64(bits))
            }
            DataType::Decimal128(_, _) => Ok(KeyValue::Decimal128(i128::from_le_bytes(
                read_fixed::<16>(&mut local, "Decimal128")?,
            ))),
            DataType::Date32 => Ok(KeyValue::Date32(i32::from_le_bytes(read_fixed::<4>(
                &mut local, "Date32",
            )?))),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(KeyValue::Timestamp(
                i64::from_le_bytes(read_fixed::<8>(&mut local, "Timestamp")?),
            )),
            DataType::Utf8 | DataType::LargeUtf8 => {
                let len = read_uleb128(&mut local)?;
                let len = usize::try_from(len)
                    .map_err(|_| "read_key: Utf8 length does not fit usize".to_string())?;
                if local.len() < len {
                    return Err("read_key: truncated Utf8".to_string());
                }
                let bytes = &local[..len];
                local = &local[len..];
                let value = std::str::from_utf8(bytes)
                    .map_err(|e| format!("read_key: invalid Utf8: {e}"))?
                    .to_string();
                Ok(KeyValue::Utf8(value))
            }
            other => Err(format!("read_key: unsupported type {other:?}")),
        }?;
    *cursor = local;
    Ok(value)
}

fn downcast_array<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    name: &str,
) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("state_codec: failed to downcast key array to {name}"))
}

fn canonical_f32_bits(value: f32) -> u32 {
    if value.is_nan() {
        CANONICAL_F32_NAN_BITS
    } else if value == 0.0 {
        0.0f32.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        CANONICAL_F64_NAN_BITS
    } else if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn validate_f32_bits(bits: u32) -> Result<(), String> {
    if bits == (-0.0f32).to_bits() {
        return Err("read_key: non-canonical Float32 negative zero".to_string());
    }
    if f32::from_bits(bits).is_nan() && bits != CANONICAL_F32_NAN_BITS {
        return Err("read_key: non-canonical Float32 NaN".to_string());
    }
    Ok(())
}

fn validate_f64_bits(bits: u64) -> Result<(), String> {
    if bits == (-0.0f64).to_bits() {
        return Err("read_key: non-canonical Float64 negative zero".to_string());
    }
    if f64::from_bits(bits).is_nan() && bits != CANONICAL_F64_NAN_BITS {
        return Err("read_key: non-canonical Float64 NaN".to_string());
    }
    Ok(())
}

fn read_fixed<const N: usize>(cursor: &mut &[u8], name: &str) -> Result<[u8; N], String> {
    if cursor.len() < N {
        return Err(format!("read_key: truncated {name}"));
    }
    let mut bytes = [0u8; N];
    bytes.copy_from_slice(&cursor[..N]);
    *cursor = &cursor[N..];
    Ok(bytes)
}

/// Reads one canonical SLEB128 value emitted by `write_sleb128`.
///
/// Bytes already inspected are consumed even on error.
pub(crate) fn read_sleb128(cursor: &mut &[u8]) -> Result<i64, String> {
    let original = *cursor;
    let mut result: i64 = 0;
    let mut shift = 0u32;
    loop {
        let (&byte, rest) = cursor
            .split_first()
            .ok_or_else(|| "state_codec: SLEB128 truncated".to_string())?;
        *cursor = rest;
        let payload = byte & 0x7F;
        if shift == 63 && byte & 0x80 == 0 && payload != 0x00 && payload != 0x7F {
            return Err("state_codec: SLEB128 overflow".to_string());
        }
        if shift >= 64 {
            return Err("state_codec: SLEB128 too long".to_string());
        }
        result |= (payload as i64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            // Sign-extend the high bit of the last group.
            if shift < 64 && byte & 0x40 != 0 {
                result |= -1i64 << shift;
            }
            let consumed_len = original.len() - cursor.len();
            let consumed = &original[..consumed_len];
            let mut canonical = Vec::new();
            write_sleb128(&mut canonical, result);
            if consumed != canonical {
                return Err("state_codec: SLEB128 non-canonical".to_string());
            }
            return Ok(result);
        }
        if shift >= 64 {
            return Err("state_codec: SLEB128 too long".to_string());
        }
    }
}

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
        let mut cursor: &[u8] = &[0x80]; // continuation bit set, no follow-up
        assert!(read_uleb128(&mut cursor).is_err());
    }

    #[test]
    fn uleb128_malformed_inputs_error() {
        let mut overflow: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        assert!(read_uleb128(&mut overflow).is_err());

        let mut too_long: &[u8] = &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        assert!(read_uleb128(&mut too_long).is_err());

        let mut truncated: &[u8] = &[0x81, 0x80];
        assert!(read_uleb128(&mut truncated).is_err());
    }

    #[test]
    fn uleb128_rejects_non_canonical_encodings() {
        for bytes in [&[0x80, 0x00][..], &[0x81, 0x00][..]] {
            let mut cursor = bytes;
            assert!(read_uleb128(&mut cursor).is_err());
        }
    }

    #[test]
    fn sleb128_malformed_inputs_error() {
        let mut positive_overflow: &[u8] =
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert!(read_sleb128(&mut positive_overflow).is_err());

        let mut negative_underflow: &[u8] =
            &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        assert!(read_sleb128(&mut negative_underflow).is_err());

        let mut too_long: &[u8] = &[
            0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
        ];
        assert!(read_sleb128(&mut too_long).is_err());

        let mut truncated: &[u8] = &[0x80, 0x80];
        assert!(read_sleb128(&mut truncated).is_err());
    }

    #[test]
    fn sleb128_rejects_non_canonical_encodings() {
        for bytes in [&[0x80, 0x00][..], &[0xff, 0x7f][..]] {
            let mut cursor = bytes;
            assert!(read_sleb128(&mut cursor).is_err());
        }
    }
}

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
        assert_eq!(
            key_round_trip(arr, 0),
            vec![0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
        );
    }

    #[test]
    fn key_bytes_are_not_value_order_keys() {
        // Raw key bytes are canonical equality identities, not SQL value-order keys.
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(42i64), Some(-1i64)]));
        let forty_two = key_round_trip(ints.clone(), 0);
        let minus_one = key_round_trip(ints, 1);
        assert!(forty_two < minus_one);
        assert!(42i64 > -1i64);

        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("aa")]));
        let b = key_round_trip(strings.clone(), 0);
        let aa = key_round_trip(strings, 1);
        assert!(b < aa);
        assert!("b" > "aa");
    }

    #[test]
    fn key_float64_nan_canonicalized() {
        let arr1: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN)]));
        let arr2: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::from_bits(
            0x7FF8_0000_0000_0001,
        ))]));
        assert_eq!(
            key_round_trip(arr1, 0),
            key_round_trip(arr2, 0),
            "all NaNs must canonicalize to one byte pattern"
        );
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

    #[test]
    fn key_int64_write_read_roundtrip() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(42i64), Some(-1)]));
        let mut buf = Vec::new();
        write_key_at(&mut buf, &arr, 0).unwrap();
        write_key_at(&mut buf, &arr, 1).unwrap();
        let mut cursor = &buf[..];
        assert_eq!(
            read_key(&mut cursor, &DataType::Int64).unwrap(),
            KeyValue::Int64(42)
        );
        assert_eq!(
            read_key(&mut cursor, &DataType::Int64).unwrap(),
            KeyValue::Int64(-1)
        );
    }

    #[test]
    fn key_write_read_roundtrips_all_v1_allowed_types() {
        let cases: Vec<(ArrayRef, DataType, KeyValue)> = vec![
            (
                Arc::new(BooleanArray::from(vec![Some(true)])),
                DataType::Boolean,
                KeyValue::Bool(true),
            ),
            (
                Arc::new(Int8Array::from(vec![Some(-7i8)])),
                DataType::Int8,
                KeyValue::Int8(-7),
            ),
            (
                Arc::new(Int16Array::from(vec![Some(-1024i16)])),
                DataType::Int16,
                KeyValue::Int16(-1024),
            ),
            (
                Arc::new(Int32Array::from(vec![Some(123_456i32)])),
                DataType::Int32,
                KeyValue::Int32(123_456),
            ),
            (
                Arc::new(Int64Array::from(vec![Some(-123_456_789i64)])),
                DataType::Int64,
                KeyValue::Int64(-123_456_789),
            ),
            (
                Arc::new(Float32Array::from(vec![Some(f32::NAN)])),
                DataType::Float32,
                KeyValue::Float32(CANONICAL_F32_NAN_BITS),
            ),
            (
                Arc::new(Float64Array::from(vec![Some(f64::NAN)])),
                DataType::Float64,
                KeyValue::Float64(CANONICAL_F64_NAN_BITS),
            ),
            (
                Arc::new(Decimal128Array::from(vec![Some(-123_456_789i128)])),
                DataType::Decimal128(38, 0),
                KeyValue::Decimal128(-123_456_789),
            ),
            (
                Arc::new(Date32Array::from(vec![Some(19_000i32)])),
                DataType::Date32,
                KeyValue::Date32(19_000),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![Some(123_456_789i64)])),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                KeyValue::Timestamp(123_456_789),
            ),
            (
                Arc::new(StringArray::from(vec![Some("hello")])),
                DataType::Utf8,
                KeyValue::Utf8("hello".to_string()),
            ),
            (
                Arc::new(LargeStringArray::from(vec![Some("large")])),
                DataType::LargeUtf8,
                KeyValue::Utf8("large".to_string()),
            ),
        ];

        for (array, dtype, expected) in cases {
            let mut buf = Vec::new();
            write_key_at(&mut buf, &array, 0).unwrap();
            let mut cursor = &buf[..];
            assert_eq!(read_key(&mut cursor, &dtype).unwrap(), expected);
            assert!(cursor.is_empty());
        }
    }

    #[test]
    fn key_float64_read_rejects_non_canonical_nan() {
        let mut cursor = &0x7FF8_0000_0000_0001u64.to_le_bytes()[..];
        let err = read_key(&mut cursor, &DataType::Float64).unwrap_err();
        assert!(err.contains("non-canonical"));
    }

    #[test]
    fn key_float64_read_rejects_negative_zero() {
        let mut cursor = &(-0.0_f64).to_bits().to_le_bytes()[..];
        let err = read_key(&mut cursor, &DataType::Float64).unwrap_err();
        assert!(err.contains("non-canonical"));
    }

    #[test]
    fn read_key_leaves_cursor_unchanged_on_errors() {
        let non_canonical_nan = 0x7FF8_0000_0000_0001u64.to_le_bytes();
        let cases: Vec<(&[u8], DataType)> = vec![
            (&[2u8], DataType::Boolean),
            (&non_canonical_nan, DataType::Float64),
            (&[1, 2, 3], DataType::Int64),
            (&[3, b'a'], DataType::Utf8),
            (&[2, 0xff, 0xff], DataType::Utf8),
            (&[0x80, 0x00, b'a'], DataType::Utf8),
        ];

        for (bytes, dtype) in cases {
            let original = bytes;
            let mut cursor = original;
            assert!(
                read_key(&mut cursor, &dtype).is_err(),
                "expected read_key error for {dtype:?}"
            );
            assert_eq!(cursor, original, "cursor changed for {dtype:?}");
        }
    }

    #[test]
    fn key_write_rejects_null_and_out_of_bounds() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64), None]));

        let mut null_buf = vec![0xAA];
        let err = write_key_at(&mut null_buf, &arr, 1).unwrap_err();
        assert!(err.contains("NULL key"));
        assert_eq!(null_buf, vec![0xAA]);

        let mut oob_buf = vec![0xBB];
        let err = write_key_at(&mut oob_buf, &arr, 2).unwrap_err();
        assert!(err.contains("out of bounds"));
        assert_eq!(oob_buf, vec![0xBB]);
    }

    #[test]
    fn key_unsupported_type_returns_error() {
        let arr: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"ab"[..])]));
        let mut buf = Vec::new();
        let err = write_key_at(&mut buf, &arr, 0).unwrap_err();
        assert!(err.contains("unsupported key type"));

        let mut cursor: &[u8] = &[];
        let err = read_key(&mut cursor, &DataType::Binary).unwrap_err();
        assert!(err.contains("unsupported type"));
    }
}
