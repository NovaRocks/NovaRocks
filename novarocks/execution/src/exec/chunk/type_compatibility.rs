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

//! Runtime descriptor type compatibility checks for the execution layer.
//!
//! This is the keystone of the distributed-execution target architecture
//! (pillar P1, see `docs/design/specs/2026-06-12-distributed-execution-target-architecture.md`).
//! It is the one place that answers "does column type `actual` satisfy the
//! authoritative descriptor type `expected`?". Most types must match exactly;
//! the explicit C0 carrier exception is that `Dictionary(Int32, Utf8)` and
//! `Dictionary(Int32, LargeUtf8)` are valid physical representations for
//! matching string slots. It replaces the five hand-rolled
//! copies of that predicate that drifted apart:
//!   - `exec::chunk::schema::is_compatible_chunk_field_type` / `reconcile_chunk_data_type`
//!   - `exec::operators::sort::is_compatible_sort_field_type`
//!   - `runtime::exchange::is_compatible_exchange_arrow_type` / `merge_exchange_field_type`
//!   - `exec::expr::agg::functions::array_agg::reconcile_data_type`
//!
//! Deliberate decisions encoded here (resolved divergences across those copies):
//!   - decimal is precision-and-scale strict.
//!   - `Map` `ordered` flags must match.
//!   - `List` and `LargeList` are never compatible with each other.
//!   - structs are checked by POSITION, ignoring field names (Arrow field names are
//!     not part of the StarRocks logical type; cf. `struct_column` serde).
//!   - the historical `List <-> Struct[len==1]` collapse is DROPPED: it papered
//!     over an aggregate-state shape inconsistency that pillar P5 makes
//!     deterministic instead.
//!   - `Dictionary(Int32, Utf8/LargeUtf8)` is accepted as a carrier for matching
//!     string slots, but retagging must not decode it to a plain string array.
//!
//! Type only: this check says nothing about nullability. Field-level
//! nullability reconciliation (and the root-boundary `required -> null`
//! fail-fast) is a separate concern layered on top at descriptor boundaries.

use arrow::array::{Array, ArrayData, ArrayRef, make_array};
use arrow::datatypes::DataType;

/// One step on the path from a top-level type to a nested mismatch, for
/// diagnostics that can name `col.field[2].list.item` precisely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NestedStep {
    ListItem,
    LargeListItem,
    MapKey,
    MapValue,
    StructField(usize),
}

/// Why two types fail compatibility. Carried so CI / engine error classification can
/// discriminate without parsing free text (pillar P8 embeds this as the
/// type-mismatch arm of the engine error enum).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeMismatchKind {
    /// Non-compatible scalars (e.g. Int32 vs Int64; Utf8 vs Binary).
    ScalarMismatch,
    /// Decimal scales differ (never permitted under any policy).
    DecimalScaleMismatch,
    /// Decimal physical width differs (Decimal128 vs Decimal256).
    DecimalWidthCross,
    /// A list-kind type met a different kind (List vs LargeList, or list vs non-list).
    ListKindMismatch,
    /// `Map` `ordered` flags differ.
    MapOrderingMismatch,
    /// Struct field counts differ.
    StructArityMismatch,
}

/// A structured type mismatch produced by [`check_exact`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeMismatch {
    pub nested_path: Vec<NestedStep>,
    pub expected: DataType,
    pub actual: DataType,
    pub kind: TypeMismatchKind,
}

/// The one recursive descriptor check. Returns `Ok(())` when `actual`
/// is compatible with the authoritative `expected` descriptor, or a structured
/// [`TypeMismatch`] otherwise. Compatibility is exact except for the explicit
/// `Dictionary(Int32, Utf8/LargeUtf8)` physical carrier allowed for matching
/// string slots.
pub fn check_exact(expected: &DataType, actual: &DataType) -> Result<(), TypeMismatch> {
    let mut path = Vec::new();
    check_exact_inner(expected, actual, &mut path)
}

fn check_exact_inner(
    expected: &DataType,
    actual: &DataType,
    path: &mut Vec<NestedStep>,
) -> Result<(), TypeMismatch> {
    use DataType::*;
    use TypeMismatchKind::*;

    if expected == actual {
        return Ok(());
    }

    let mismatch = |kind: TypeMismatchKind, path: &[NestedStep]| TypeMismatch {
        nested_path: path.to_vec(),
        expected: expected.clone(),
        actual: actual.clone(),
        kind,
    };

    match (expected, actual) {
        (Decimal128(ep, es), Decimal128(ap, as_)) | (Decimal256(ep, es), Decimal256(ap, as_)) => {
            if es != as_ {
                Err(mismatch(DecimalScaleMismatch, path))
            } else if ep == ap {
                Ok(())
            } else {
                Err(mismatch(ScalarMismatch, path))
            }
        }
        (Decimal128(..), Decimal256(..)) | (Decimal256(..), Decimal128(..)) => {
            Err(mismatch(DecimalWidthCross, path))
        }
        (Timestamp(_, _), Timestamp(_, _)) | (Utf8, Binary) | (Binary, Utf8) => {
            Err(mismatch(ScalarMismatch, path))
        }
        (Utf8 | LargeUtf8, Dictionary(key, value))
            if key.as_ref() == &Int32 && value.as_ref() == expected =>
        {
            Ok(())
        }
        (List(ef), List(af)) => {
            path.push(NestedStep::ListItem);
            let r = check_exact_inner(ef.data_type(), af.data_type(), path);
            path.pop();
            r
        }
        (LargeList(ef), LargeList(af)) => {
            path.push(NestedStep::LargeListItem);
            let r = check_exact_inner(ef.data_type(), af.data_type(), path);
            path.pop();
            r
        }
        (List(_) | LargeList(_), _) | (_, List(_) | LargeList(_)) => {
            Err(mismatch(ListKindMismatch, path))
        }
        (Map(ef, eo), Map(af, ao)) => {
            if eo != ao {
                return Err(mismatch(MapOrderingMismatch, path));
            }
            let (ek, ev) = map_key_value(ef).ok_or_else(|| mismatch(ScalarMismatch, path))?;
            let (ak, av) = map_key_value(af).ok_or_else(|| mismatch(ScalarMismatch, path))?;
            path.push(NestedStep::MapKey);
            let rk = check_exact_inner(ek, ak, path);
            path.pop();
            rk?;
            path.push(NestedStep::MapValue);
            let rv = check_exact_inner(ev, av, path);
            path.pop();
            rv
        }
        (Struct(ef), Struct(af)) => {
            if ef.len() != af.len() {
                return Err(mismatch(StructArityMismatch, path));
            }
            for (idx, (e, a)) in ef.iter().zip(af.iter()).enumerate() {
                path.push(NestedStep::StructField(idx));
                let r = check_exact_inner(e.data_type(), a.data_type(), path);
                path.pop();
                r?;
            }
            Ok(())
        }
        _ => Err(mismatch(ScalarMismatch, path)),
    }
}

/// Retag `array` so its type equals `target`, changing only metadata — never a
/// single value. This is an explicit metadata-only rebuild primitive, not a
/// compatibility policy. Descriptor-bound runtime callers must run
/// [`check_exact`] before using it.
///
/// The legitimate retag cases are: identity; a decimal precision change at the
/// SAME scale within the same physical width (an `i128`/`i256` buffer is
/// reinterpreted, values untouched); `Utf8` <-> `Binary` (identical physical
/// layout); and recursion into `List` / `LargeList` / `Struct` / `Map` children.
/// Any difference that is not a pure relabel (e.g. a timestamp unit change, or
/// `Decimal128` <-> `Decimal256`) returns `Err`.
pub fn retag_column(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, TypeMismatch> {
    let data = retag_data(array.to_data(), target, &mut Vec::new())?;
    Ok(make_array(data))
}

fn retag_data(
    data: ArrayData,
    target: &DataType,
    path: &mut Vec<NestedStep>,
) -> Result<ArrayData, TypeMismatch> {
    use DataType::*;
    use TypeMismatchKind::*;

    if data.data_type() == target {
        return Ok(data);
    }
    let source = data.data_type().clone();

    match (&source, target) {
        (Decimal128(_, ss), Decimal128(_, ts)) | (Decimal256(_, ss), Decimal256(_, ts)) => {
            if ss != ts {
                return Err(retag_mismatch(path, target, &source, DecimalScaleMismatch));
            }
            finish_retag(data, target, Vec::new(), path, &source)
        }
        (Decimal128(..), Decimal256(..)) | (Decimal256(..), Decimal128(..)) => {
            Err(retag_mismatch(path, target, &source, DecimalWidthCross))
        }
        (Timestamp(source_unit, _), Timestamp(target_unit, _)) if source_unit == target_unit => {
            finish_retag(data, target, Vec::new(), path, &source)
        }
        (Timestamp(_, _), Timestamp(_, _)) => {
            Err(retag_mismatch(path, target, &source, ScalarMismatch))
        }
        (Utf8, Binary) | (Binary, Utf8) => finish_retag(data, target, Vec::new(), path, &source),
        (List(_), List(tf)) => {
            path.push(NestedStep::ListItem);
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (LargeList(_), LargeList(tf)) => {
            path.push(NestedStep::LargeListItem);
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (List(_) | LargeList(_), _) | (_, List(_) | LargeList(_)) => {
            Err(retag_mismatch(path, target, &source, ListKindMismatch))
        }
        (Map(_, so), Map(tf, to)) => {
            if so != to {
                return Err(retag_mismatch(path, target, &source, MapOrderingMismatch));
            }
            // A Map's single child is the `entries` struct; recurse it as a struct.
            path.push(NestedStep::StructField(0));
            let child = retag_data(data.child_data()[0].clone(), tf.data_type(), path);
            path.pop();
            finish_retag(data, target, vec![child?], path, &source)
        }
        (Struct(_), Struct(tfields)) => {
            let n = data.child_data().len();
            if n != tfields.len() {
                return Err(retag_mismatch(path, target, &source, StructArityMismatch));
            }
            let mut children = Vec::with_capacity(n);
            for (idx, tf) in tfields.iter().enumerate() {
                path.push(NestedStep::StructField(idx));
                let c = retag_data(data.child_data()[idx].clone(), tf.data_type(), path);
                path.pop();
                children.push(c?);
            }
            finish_retag(data, target, children, path, &source)
        }
        _ => Err(retag_mismatch(path, target, &source, ScalarMismatch)),
    }
}

/// Rebuild `data` with `target` as its type, reusing the original buffers/nulls
/// (metadata-only) and substituting `children` when retagging a nested type.
fn finish_retag(
    data: ArrayData,
    target: &DataType,
    children: Vec<ArrayData>,
    path: &[NestedStep],
    source: &DataType,
) -> Result<ArrayData, TypeMismatch> {
    let mut builder = data.into_builder().data_type(target.clone());
    if !children.is_empty() {
        builder = builder.child_data(children);
    }
    builder
        .build()
        .map_err(|_| retag_mismatch(path, target, source, TypeMismatchKind::ScalarMismatch))
}

fn retag_mismatch(
    path: &[NestedStep],
    expected: &DataType,
    actual: &DataType,
    kind: TypeMismatchKind,
) -> TypeMismatch {
    TypeMismatch {
        nested_path: path.to_vec(),
        expected: expected.clone(),
        actual: actual.clone(),
        kind,
    }
}

pub fn nested_path_label(root: &str, path: &[NestedStep]) -> String {
    let mut out = root.to_string();
    for step in path {
        match step {
            NestedStep::ListItem => out.push_str(".list.item"),
            NestedStep::LargeListItem => out.push_str(".large_list.item"),
            NestedStep::MapKey => out.push_str(".map.key"),
            NestedStep::MapValue => out.push_str(".map.value"),
            NestedStep::StructField(idx) => {
                out.push_str(".field[");
                out.push_str(&idx.to_string());
                out.push(']');
            }
        }
    }
    out
}

/// Extract the (key, value) child data types from a `Map` entries field, which
/// Arrow models as a 2-field `Struct<key, value>`.
fn map_key_value(entries: &arrow::datatypes::FieldRef) -> Option<(&DataType, &DataType)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => {
            Some((fields[0].data_type(), fields[1].data_type()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::TypeMismatchKind::*;
    use super::{NestedStep, check_exact, nested_path_label, retag_column};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, Decimal128Array, DictionaryArray, Int32Array, Int64Array,
        LargeStringDictionaryBuilder, ListArray, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, TimeUnit};
    use std::sync::Arc;

    fn list(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }
    fn large_list(item: DataType) -> DataType {
        DataType::LargeList(Arc::new(Field::new("item", item, true)))
    }
    fn strukt(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        ))
    }
    fn map(key: DataType, value: DataType, ordered: bool) -> DataType {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ])),
            false,
        );
        DataType::Map(Arc::new(entries), ordered)
    }
    fn d128(p: u8, s: i8) -> DataType {
        DataType::Decimal128(p, s)
    }
    fn d256(p: u8, s: i8) -> DataType {
        DataType::Decimal256(p, s)
    }
    fn dict_utf8() -> ArrayRef {
        Arc::new(
            vec!["PAID", "NEW", "PAID"]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }
    fn dict_large_utf8() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_value("NEW");
        builder.append_value("PAID");
        Arc::new(builder.finish())
    }

    #[test]
    fn identical_scalars_pass_exact_check() {
        assert!(check_exact(&DataType::Int64, &DataType::Int64).is_ok());
    }

    #[test]
    fn distinct_scalars_are_a_scalar_mismatch() {
        let err = check_exact(&DataType::Int32, &DataType::Int64).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
        assert!(err.nested_path.is_empty());
    }

    #[test]
    fn exact_check_rejects_decimal_precision_difference() {
        let err = check_exact(&d128(20, 2), &d128(38, 2)).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
    }

    #[test]
    fn decimal_scale_difference_is_not_exact() {
        let err = check_exact(&d128(20, 2), &d128(20, 3)).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
    }

    #[test]
    fn decimal128_and_decimal256_are_not_exact() {
        let err = check_exact(&d128(20, 2), &d256(20, 2)).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
        let err = check_exact(&d256(20, 2), &d128(20, 2)).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
    }

    #[test]
    fn timestamp_unit_difference_is_not_exact() {
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(check_exact(&us, &ns).unwrap_err().kind, ScalarMismatch);
    }

    #[test]
    fn utf8_binary_difference_is_not_exact() {
        assert_eq!(
            check_exact(&DataType::Utf8, &DataType::Binary)
                .unwrap_err()
                .kind,
            ScalarMismatch
        );
        assert_eq!(
            check_exact(&DataType::Binary, &DataType::Utf8)
                .unwrap_err()
                .kind,
            ScalarMismatch
        );
    }

    #[test]
    fn dict_int32_string_is_compatible_with_string_slot() {
        for (slot_type, dict) in [
            (DataType::Utf8, dict_utf8()),
            (DataType::LargeUtf8, dict_large_utf8()),
        ] {
            let dict_type = dict.data_type().clone();

            assert!(
                check_exact(&slot_type, &dict_type).is_ok(),
                "Dictionary(Int32, {:?}) must be compatible with a {:?} slot",
                slot_type,
                slot_type
            );
            assert!(
                retag_column(&dict, &slot_type).is_err(),
                "retag must refuse to silently decode a dict column to {:?}",
                slot_type
            );
        }
    }

    #[test]
    fn list_recurses_into_item_with_path() {
        let err = check_exact(&list(d128(20, 2)), &list(d128(20, 3))).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::ListItem]);
    }

    #[test]
    fn list_and_large_list_are_never_compatible() {
        let err = check_exact(&list(DataType::Int64), &large_list(DataType::Int64)).unwrap_err();
        assert_eq!(err.kind, ListKindMismatch);
    }

    #[test]
    fn list_struct_collapse_is_dropped() {
        // The historical List<->Struct[len==1] tolerance is deliberately removed.
        let err = check_exact(
            &list(DataType::Int32),
            &strukt(vec![("f", DataType::Int32)]),
        )
        .unwrap_err();
        assert_eq!(err.kind, ListKindMismatch);
    }

    #[test]
    fn struct_is_checked_by_position_ignoring_names() {
        let a = strukt(vec![("a", DataType::Int64), ("b", d128(20, 2))]);
        let b = strukt(vec![("x", DataType::Int64), ("y", d128(20, 2))]);
        assert!(check_exact(&a, &b).is_ok());
    }

    #[test]
    fn struct_arity_mismatch_is_reported() {
        let a = strukt(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let b = strukt(vec![("a", DataType::Int64)]);
        assert_eq!(check_exact(&a, &b).unwrap_err().kind, StructArityMismatch);
    }

    #[test]
    fn struct_child_mismatch_carries_field_path() {
        let a = strukt(vec![("a", DataType::Int64), ("b", d128(20, 2))]);
        let b = strukt(vec![("a", DataType::Int64), ("b", d128(20, 3))]);
        let err = check_exact(&a, &b).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::StructField(1)]);
    }

    #[test]
    fn nested_path_label_formats_struct_list_path() {
        let expected = strukt(vec![("items", list(DataType::Int32))]);
        let actual = strukt(vec![("items", list(DataType::Int64))]);

        let err = check_exact(&expected, &actual).unwrap_err();

        assert_eq!(
            err.nested_path,
            vec![NestedStep::StructField(0), NestedStep::ListItem]
        );
        assert_eq!(
            nested_path_label("field[0]", &err.nested_path[1..]),
            "field[0].list.item"
        );
        assert_eq!(
            nested_path_label("root", &err.nested_path),
            "root.field[0].list.item"
        );
    }

    #[test]
    fn map_ordering_must_match() {
        let ordered = map(DataType::Utf8, DataType::Int64, true);
        let unordered = map(DataType::Utf8, DataType::Int64, false);
        assert_eq!(
            check_exact(&ordered, &unordered).unwrap_err().kind,
            MapOrderingMismatch
        );
    }

    #[test]
    fn map_recurses_into_key_and_value() {
        let a = map(DataType::Utf8, d128(20, 2), false);
        let b = map(DataType::Utf8, d128(38, 2), false);
        let err = check_exact(&a, &b).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::MapValue]);

        let bad_value = map(DataType::Utf8, d128(20, 3), false);
        let err = check_exact(&a, &bad_value).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::MapValue]);

        let bad_key = map(DataType::Int64, d128(20, 2), false);
        let err = check_exact(&a, &bad_key).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
        assert_eq!(err.nested_path, vec![NestedStep::MapKey]);
    }

    #[test]
    fn check_ignores_field_nullability() {
        // The check is about type structure only; child nullability differs
        // but the item types match.
        let non_null_item = DataType::List(Arc::new(Field::new("item", DataType::Int64, false)));
        let null_item = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        assert!(check_exact(&non_null_item, &null_item).is_ok());
    }

    fn decimal128(values: Vec<i128>, p: u8, s: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.into_iter().map(Some).collect::<Vec<_>>())
                .with_precision_and_scale(p, s)
                .expect("decimal type"),
        )
    }

    #[test]
    fn retag_column_identity_is_a_noop() {
        let arr = decimal128(vec![123], 38, 2);
        let out = retag_column(&arr, &DataType::Decimal128(38, 2)).expect("retag");
        assert_eq!(out.data_type(), &DataType::Decimal128(38, 2));
    }

    #[test]
    fn retag_column_decimal_widens_precision_keeps_values() {
        let arr = decimal128(vec![123, -45], 18, 2);
        let out = retag_column(&arr, &DataType::Decimal128(38, 2)).expect("retag");
        assert_eq!(out.data_type(), &DataType::Decimal128(38, 2));
        assert!(check_exact(&DataType::Decimal128(38, 2), out.data_type()).is_ok());
        let d = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(d.value(0), 123);
        assert_eq!(d.value(1), -45);
    }

    #[test]
    fn retag_column_decimal_scale_mismatch_errors() {
        let arr = decimal128(vec![123], 18, 2);
        let err = retag_column(&arr, &DataType::Decimal128(38, 3)).unwrap_err();
        assert_eq!(err.kind, DecimalScaleMismatch);
    }

    #[test]
    fn retag_column_decimal_width_cross_errors() {
        let arr = decimal128(vec![123], 18, 2);
        let err = retag_column(&arr, &DataType::Decimal256(40, 2)).unwrap_err();
        assert_eq!(err.kind, DecimalWidthCross);
    }

    #[test]
    fn retag_column_utf8_to_binary_keeps_bytes() {
        let arr = Arc::new(StringArray::from(vec!["ab", "cd"])) as ArrayRef;
        let out = retag_column(&arr, &DataType::Binary).expect("retag");
        assert_eq!(out.data_type(), &DataType::Binary);
        let b = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(b.value(0), b"ab");
        assert_eq!(b.value(1), b"cd");
    }

    #[test]
    fn retag_column_timestamp_same_unit_keeps_values() {
        let arr =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_234_i64), None])) as ArrayRef;
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        let out = retag_column(&arr, &target).expect("retag");
        assert_eq!(out.data_type(), &target);
        let ts = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp micros");
        assert_eq!(ts.value(0), 1_234);
        assert!(ts.is_null(1));
    }

    #[test]
    fn retag_column_recurses_struct_child() {
        let d = decimal128(vec![123], 18, 2);
        let i = Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef;
        let src = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("d", DataType::Decimal128(18, 2), true)),
                d,
            ),
            (Arc::new(Field::new("i", DataType::Int64, true)), i),
        ])) as ArrayRef;
        let target = DataType::Struct(Fields::from(vec![
            Field::new("d", DataType::Decimal128(38, 2), true),
            Field::new("i", DataType::Int64, true),
        ]));
        let out = retag_column(&src, &target).expect("retag");
        assert_eq!(out.data_type(), &target);
        let s = out.as_any().downcast_ref::<StructArray>().unwrap();
        let dcol = s
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(dcol.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(dcol.value(0), 123);
    }

    #[test]
    fn retag_column_recurses_list_item() {
        let values = decimal128(vec![123, 456], 18, 2);
        let src = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Decimal128(18, 2), true)),
            OffsetBuffer::from_lengths([2]),
            values,
            None,
        )) as ArrayRef;
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(38, 2),
            true,
        )));
        let out = retag_column(&src, &target).expect("retag");
        assert_eq!(out.data_type(), &target);
        let l = out.as_any().downcast_ref::<ListArray>().unwrap();
        let items = l
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(items.value(0), 123);
        assert_eq!(items.value(1), 456);
    }

    #[test]
    fn retag_column_non_retaggable_scalar_errors() {
        let arr = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let err = retag_column(&arr, &DataType::Int64).unwrap_err();
        assert_eq!(err.kind, ScalarMismatch);
    }
}
