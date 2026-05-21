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

//! Pure-Rust JSON-to-Variant binary encoder.
//!
//! Mirrors StarRocks' `VariantEncoder` (`be/src/column/variant_encoder.cpp`)
//! but works against `serde_json::Value` instead of velocypack. Output bytes
//! are accepted by the existing reader in `src/exec/variant.rs`.
//!
//! Final layout produced by [`encode_json_text_to_variant_bytes`]:
//!   `[size:u32 LE | metadata | value]`
//! (matches `VariantValue::serialize`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use crate::exec::variant::VariantValue;

/// Encode JSON text into NovaRocks' size-prefixed variant payload.
pub(crate) fn encode_json_text_to_variant_bytes(json_text: &str) -> Result<Vec<u8>, String> {
    let value: Value =
        serde_json::from_str(json_text).map_err(|e| format!("parse_json: invalid JSON: {e}"))?;
    encode_json_value_to_variant_bytes(&value)
}

/// Encode an already-parsed JSON value. Same output shape as
/// [`encode_json_text_to_variant_bytes`].
pub(crate) fn encode_json_value_to_variant_bytes(value: &Value) -> Result<Vec<u8>, String> {
    let mut keys = BTreeSet::new();
    collect_object_keys(value, &mut keys);
    let (metadata, key_to_id) = build_metadata(&keys);
    let mut value_bytes = Vec::new();
    encode_value(value, &key_to_id, &mut value_bytes)?;
    let variant = VariantValue::create(&metadata, &value_bytes)?;
    Ok(variant.serialize())
}

// --- Variant constants (mirror src/exec/variant.rs) -----------------------

/// BasicType (low 2 bits of value header byte).
const BT_PRIMITIVE: u8 = 0;
const BT_SHORT_STRING: u8 = 1;
const BT_OBJECT: u8 = 2;
const BT_ARRAY: u8 = 3;

/// VariantPrimitiveType (upper 6 bits of value header byte).
const PT_NULL: u8 = 0;
const PT_BOOLEAN_TRUE: u8 = 1;
const PT_BOOLEAN_FALSE: u8 = 2;
const PT_INT8: u8 = 3;
const PT_INT16: u8 = 4;
const PT_INT32: u8 = 5;
const PT_INT64: u8 = 6;
const PT_DOUBLE: u8 = 7;
const PT_STRING: u8 = 16;

/// Metadata header bits.
const METADATA_VERSION: u8 = 0x01;
const METADATA_SORTED_MASK: u8 = 0x10;
const METADATA_OFFSET_SIZE_SHIFT: u8 = 6;

// --- helpers -------------------------------------------------------------

fn primitive_header(primitive_type: u8) -> u8 {
    (primitive_type << 2) | BT_PRIMITIVE
}

/// Smallest u8 in 1..=4 needed to represent `value` little-endian.
fn minimal_uint_size(value: u32) -> u8 {
    if value <= 0xFF {
        1
    } else if value <= 0xFFFF {
        2
    } else if value <= 0x00FF_FFFF {
        3
    } else {
        4
    }
}

fn append_uint_le(out: &mut Vec<u8>, value: u32, size: u8) {
    let bytes = value.to_le_bytes();
    out.extend_from_slice(&bytes[..size as usize]);
}

/// Build sorted-dictionary metadata bytes plus `key -> field_id` mapping.
fn build_metadata(keys: &BTreeSet<String>) -> (Vec<u8>, HashMap<String, u32>) {
    let dict_size = keys.len() as u32;
    let total_string_size: u32 = keys.iter().map(|k| k.len() as u32).sum();
    let max_value = std::cmp::max(dict_size, total_string_size);
    let offset_size = minimal_uint_size(max_value);

    let header =
        METADATA_VERSION | METADATA_SORTED_MASK | ((offset_size - 1) << METADATA_OFFSET_SIZE_SHIFT);

    let mut metadata = Vec::with_capacity(
        1 + offset_size as usize * (dict_size as usize + 2) + total_string_size as usize,
    );
    metadata.push(header);
    append_uint_le(&mut metadata, dict_size, offset_size);

    // Offsets: [0, len(k0), len(k0)+len(k1), ...]
    let mut offset: u32 = 0;
    append_uint_le(&mut metadata, offset, offset_size);
    for key in keys {
        offset += key.len() as u32;
        append_uint_le(&mut metadata, offset, offset_size);
    }

    // String heap.
    for key in keys {
        metadata.extend_from_slice(key.as_bytes());
    }

    let mut key_to_id = HashMap::with_capacity(keys.len());
    for (idx, key) in keys.iter().enumerate() {
        key_to_id.insert(key.clone(), idx as u32);
    }
    (metadata, key_to_id)
}

/// Recursively walk the JSON value collecting every object key.
fn collect_object_keys(value: &Value, keys: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                keys.insert(k.clone());
                collect_object_keys(v, keys);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_object_keys(item, keys);
            }
        }
        _ => {}
    }
}

fn encode_value(
    value: &Value,
    key_to_id: &HashMap<String, u32>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    match value {
        Value::Null => {
            out.push(primitive_header(PT_NULL));
        }
        Value::Bool(true) => {
            out.push(primitive_header(PT_BOOLEAN_TRUE));
        }
        Value::Bool(false) => {
            out.push(primitive_header(PT_BOOLEAN_FALSE));
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                encode_integer(i, out);
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    encode_integer(u as i64, out);
                } else {
                    encode_double(u as f64, out);
                }
            } else if let Some(f) = n.as_f64() {
                encode_double(f, out);
            } else {
                return Err(format!("parse_json: unsupported numeric literal: {n}"));
            }
        }
        Value::String(s) => {
            encode_string(s, out);
        }
        Value::Array(items) => {
            encode_array(items, key_to_id, out)?;
        }
        Value::Object(map) => {
            encode_object(map, key_to_id, out)?;
        }
    }
    Ok(())
}

fn encode_integer(v: i64, out: &mut Vec<u8>) {
    if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
        out.push(primitive_header(PT_INT8));
        out.extend_from_slice(&(v as i8).to_le_bytes());
    } else if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
        out.push(primitive_header(PT_INT16));
        out.extend_from_slice(&(v as i16).to_le_bytes());
    } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        out.push(primitive_header(PT_INT32));
        out.extend_from_slice(&(v as i32).to_le_bytes());
    } else {
        out.push(primitive_header(PT_INT64));
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn encode_double(v: f64, out: &mut Vec<u8>) {
    out.push(primitive_header(PT_DOUBLE));
    out.extend_from_slice(&v.to_le_bytes());
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    if bytes.len() <= 63 {
        // ShortString: header = (len << 2) | BT_SHORT_STRING.
        let header = ((bytes.len() as u8) << 2) | BT_SHORT_STRING;
        out.push(header);
        out.extend_from_slice(bytes);
    } else {
        // Primitive String: u32 LE length + bytes.
        out.push(primitive_header(PT_STRING));
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
}

fn encode_array(
    items: &[Value],
    key_to_id: &HashMap<String, u32>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let mut payload = Vec::new();
    let mut end_offsets = Vec::with_capacity(items.len());
    for item in items {
        encode_value(item, key_to_id, &mut payload)?;
        end_offsets.push(payload.len() as u32);
    }

    let num_elements = items.len() as u32;
    let is_large = num_elements > 255;
    let num_elements_size: u8 = if is_large { 4 } else { 1 };
    // Offsets must fit the largest end_offset (== payload size).
    let field_offset_size = minimal_uint_size(payload.len() as u32);

    // Array header: bits[0..=1]=BT_ARRAY, bits[2..=3]=offset_size-1, bit 4=is_large.
    let vheader = (field_offset_size - 1) | ((is_large as u8) << 2);
    let header = (vheader << 2) | BT_ARRAY;
    out.push(header);
    append_uint_le(out, num_elements, num_elements_size);
    // (num_elements + 1) offsets, leading 0.
    append_uint_le(out, 0, field_offset_size);
    for end in &end_offsets {
        append_uint_le(out, *end, field_offset_size);
    }
    out.extend_from_slice(&payload);
    Ok(())
}

fn encode_object(
    map: &serde_json::Map<String, Value>,
    key_to_id: &HashMap<String, u32>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    // Sort fields by field_id (matches StarRocks: std::map<uint32_t, ...>).
    let mut ordered: BTreeMap<u32, &Value> = BTreeMap::new();
    for (k, v) in map {
        let id = *key_to_id
            .get(k)
            .ok_or_else(|| format!("parse_json: variant metadata missing field: {k}"))?;
        ordered.insert(id, v);
    }

    let mut payload = Vec::new();
    let mut field_ids = Vec::with_capacity(ordered.len());
    let mut end_offsets = Vec::with_capacity(ordered.len());
    let mut max_field_id: u32 = 0;
    for (id, child) in &ordered {
        encode_value(child, key_to_id, &mut payload)?;
        field_ids.push(*id);
        end_offsets.push(payload.len() as u32);
        if *id > max_field_id {
            max_field_id = *id;
        }
    }

    let num_elements = ordered.len() as u32;
    let is_large = num_elements > 255;
    let num_elements_size: u8 = if is_large { 4 } else { 1 };
    let field_id_size = minimal_uint_size(max_field_id);
    let field_offset_size = minimal_uint_size(payload.len() as u32);

    // Object header: bits[0..=1]=BT_OBJECT, bits[2..=3]=offset_size-1,
    // bits[4..=5]=field_id_size-1, bit 6=is_large.
    let vheader = (field_offset_size - 1) | ((field_id_size - 1) << 2) | ((is_large as u8) << 4);
    let header = (vheader << 2) | BT_OBJECT;
    out.push(header);
    append_uint_le(out, num_elements, num_elements_size);
    for id in &field_ids {
        append_uint_le(out, *id, field_id_size);
    }
    append_uint_le(out, 0, field_offset_size);
    for end in &end_offsets {
        append_uint_le(out, *end, field_offset_size);
    }
    out.extend_from_slice(&payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::variant::{
        VariantValue, is_variant_null, parse_variant_path, variant_query, variant_to_bool,
        variant_to_f64, variant_to_i64, variant_to_string,
    };

    fn round_trip(json: &str) -> VariantValue {
        let bytes = encode_json_text_to_variant_bytes(json).expect("encode");
        VariantValue::from_serialized(&bytes).expect("decode")
    }

    fn query(json: &str, path: &str) -> VariantValue {
        let v = round_trip(json);
        let p = parse_variant_path(path).expect("parse path");
        variant_query(&v, &p).expect("query")
    }

    #[test]
    fn encode_null_json_value() {
        let v = round_trip("null");
        assert!(is_variant_null(&v).unwrap());
    }

    #[test]
    fn encode_bool_true_and_false() {
        let t = round_trip("true");
        let f = round_trip("false");
        assert!(variant_to_bool(&t).unwrap());
        assert!(!variant_to_bool(&f).unwrap());
    }

    #[test]
    fn encode_int_picks_smallest_width() {
        let cases: &[(i64, &str)] = &[
            (0, "0"),
            (127, "127"),
            (128, "128"),
            (32767, "32767"),
            (32768, "32768"),
            (2_147_483_647, "2147483647"),
            (2_147_483_648, "2147483648"),
            (-128, "-128"),
            (-129, "-129"),
        ];
        for (expected, text) in cases {
            let v = round_trip(text);
            assert_eq!(
                variant_to_i64(&v).unwrap(),
                *expected,
                "i64 round-trip failed for {text}"
            );
        }

        // Spot-check Int8 width: bytes after the 4-byte size prefix and metadata
        // should be `[primitive_header(PT_INT8), 0]`.
        let bytes = encode_json_text_to_variant_bytes("0").unwrap();
        // size:u32 LE
        let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        // empty metadata is [0x11, 0, 0] (sorted, dict_size=0, one zero offset).
        let value_start = 4 + (bytes.len() - 4 - 2); // value follows metadata, 2 bytes for Int8
        let _ = size;
        // Locate value bytes: empty metadata is exactly 3 bytes for an empty dict.
        let value = &bytes[4 + 3..];
        assert_eq!(value[0], primitive_header(PT_INT8));

        // Spot-check Int64 width: 0x8000_0000 forces 64-bit.
        let bytes = encode_json_text_to_variant_bytes("2147483648").unwrap();
        let value = &bytes[4 + 3..];
        assert_eq!(value[0], primitive_header(PT_INT64));
        let n = i64::from_le_bytes([
            value[1], value[2], value[3], value[4], value[5], value[6], value[7], value[8],
        ]);
        assert_eq!(n, 2_147_483_648);
        assert_eq!(value.len(), 1 + 8);
    }

    #[test]
    fn encode_double_for_floats() {
        let v = round_trip("1.25");
        let f = variant_to_f64(&v).unwrap();
        assert!((f - 1.25).abs() < 1e-9);
    }

    #[test]
    fn encode_short_string_inline() {
        let v = round_trip("\"hi\"");
        assert_eq!(variant_to_string(&v).unwrap(), "hi");

        // Spot-check ShortString header: ((2 << 2) | 1) == 0x09.
        let bytes = encode_json_text_to_variant_bytes("\"hi\"").unwrap();
        let value = &bytes[4 + 3..];
        assert_eq!(value[0], (2u8 << 2) | BT_SHORT_STRING);
        assert_eq!(&value[1..], b"hi");
    }

    #[test]
    fn encode_long_string_falls_to_primitive_string() {
        let s = "a".repeat(100);
        let json = format!("\"{}\"", s);
        let v = round_trip(&json);
        assert_eq!(variant_to_string(&v).unwrap(), s);

        // Spot-check primitive STRING header.
        let bytes = encode_json_text_to_variant_bytes(&json).unwrap();
        let value = &bytes[4 + 3..];
        assert_eq!(value[0], primitive_header(PT_STRING));
        let len = u32::from_le_bytes([value[1], value[2], value[3], value[4]]) as usize;
        assert_eq!(len, 100);
    }

    #[test]
    fn encode_simple_object() {
        let json = r#"{"a":1,"b":"x"}"#;
        let v = round_trip(json);
        let a = query(json, "$.a");
        let b = query(json, "$.b");
        assert_eq!(variant_to_i64(&a).unwrap(), 1);
        assert_eq!(variant_to_string(&b).unwrap(), "x");
        assert_eq!(v.metadata().dict_size(), 2);
        // Keys are sorted in metadata.
        assert_eq!(v.metadata().get_key(0).unwrap(), "a");
        assert_eq!(v.metadata().get_key(1).unwrap(), "b");
    }

    #[test]
    fn encode_simple_array() {
        let json = "[10,20,30]";
        assert_eq!(variant_to_i64(&query(json, "$[0]")).unwrap(), 10);
        assert_eq!(variant_to_i64(&query(json, "$[1]")).unwrap(), 20);
        assert_eq!(variant_to_i64(&query(json, "$[2]")).unwrap(), 30);
    }

    #[test]
    fn encode_nested_object_array_mix() {
        let json = r#"{"a":[1,2,{"k":"v"}]}"#;
        let v = query(json, "$.a[2].k");
        assert_eq!(variant_to_string(&v).unwrap(), "v");
        // Sanity for first element of the array.
        let first = query(json, "$.a[0]");
        assert_eq!(variant_to_i64(&first).unwrap(), 1);
    }

    #[test]
    fn encode_metadata_dict_is_sorted() {
        // Insert keys out of natural order.
        let json = r#"{"zeta":1,"alpha":2,"mid":{"beta":3}}"#;
        let bytes = encode_json_text_to_variant_bytes(json).unwrap();
        let value = VariantValue::from_serialized(&bytes).unwrap();
        let meta = value.metadata();
        assert_eq!(meta.dict_size(), 4);
        // Header sorted bit is set: low nibble version=1, sorted bit=0x10.
        assert_eq!(meta.raw()[0] & METADATA_SORTED_MASK, METADATA_SORTED_MASK);
        let keys: Vec<String> = (0..meta.dict_size())
            .map(|i| meta.get_key(i).unwrap())
            .collect();
        assert_eq!(keys, vec!["alpha", "beta", "mid", "zeta"]);
    }
}
