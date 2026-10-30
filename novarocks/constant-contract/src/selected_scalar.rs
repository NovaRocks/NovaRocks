// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Observed borrowed scalar reads for exact binder-owned type decisions.
//! Carrier reads do not grant a logical domain or perform numeric casts.

use super::{
    ConstantError, ConstantValue, Row, logical_null, primitive_bytes, resolve_row, variable_bytes,
};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

impl ConstantValue {
    /// Borrow actual selected UTF8 text, resolving dictionary/run encoding.
    /// NULL and other terminal carriers return None. The caller must validate
    /// the complete logical FVT separately; this never infers Physical/JSON.
    pub fn utf8_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<&str>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let Some(row) = selected_row(self, &mut work)? else {
            work.finish()?;
            return Ok(None);
        };
        if !matches!(
            row.data.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            work.finish()?;
            return Ok(None);
        }
        let bytes = variable_bytes(row)?;
        // The backing is already admitted. Observe actual selected length
        // before the finite opaque standard-library UTF8 validation call.
        for _ in bytes.chunks(1024) {
            work.step()?;
        }
        work.finish()?;
        control.checkpoint(phase, 0)?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| ConstantError::Invalid("selected constant text is not UTF8"));
        control.checkpoint(phase, 0)?;
        Ok(Some(text?))
    }

    /// Read only the selected terminal Int64 carrier, without casts or
    /// temporal reinterpretation. NULL and every other carrier return None.
    pub fn int64_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<i64>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let value = match selected_row(self, &mut work)? {
            Some(row) if row.data.data_type() == &DataType::Int64 => {
                work.step()?;
                let bytes = primitive_bytes(row, 8)?.try_into().map_err(|_| {
                    ConstantError::Invalid("selected Int64 constant has wrong width")
                })?;
                Some(i64::from_ne_bytes(bytes))
            }
            _ => None,
        };
        work.finish()?;
        Ok(value)
    }

    /// Read an exact terminal signed integer carrier losslessly into i64.
    /// This preserves source Field/FVT identity; it does not retag a value or
    /// convert unsigned, temporal, decimal, fixed-binary or LARGEINT carriers.
    pub fn signed_integer_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<i64>, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let value = if let Some(row) = selected_row(self, &mut work)? {
            macro_rules! signed {
                ($native:ty, $width:expr) => {{
                    work.step()?;
                    let bytes = primitive_bytes(row, $width)?.try_into().map_err(|_| {
                        ConstantError::Invalid("selected signed integer constant has wrong width")
                    })?;
                    Some(i64::from(<$native>::from_ne_bytes(bytes)))
                }};
            }
            match row.data.data_type() {
                DataType::Int8 => signed!(i8, 1),
                DataType::Int16 => signed!(i16, 2),
                DataType::Int32 => signed!(i32, 4),
                DataType::Int64 => signed!(i64, 8),
                _ => None,
            }
        } else {
            None
        };
        work.finish()?;
        Ok(value)
    }
}

pub(super) fn selected_row<'a>(
    value: &'a ConstantValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Row<'a>>, ConstantError> {
    let row = Row {
        data: &value.pool.0.data,
        index: value.ordinal as usize,
    };
    if logical_null(row.data, row.index, work)? {
        return Ok(None);
    }
    resolve_row(row, work)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConstantPolicy, ConstantPool};
    use arrow_array::types::{Int8Type, Int32Type};
    use arrow_array::{
        Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, LargeStringArray,
        NullArray, RunArray, StringArray, StringViewArray, TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::Field;
    use novarocks_type_contract::{
        CompileControlError, FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType,
    };
    use std::sync::{Arc, Mutex};

    struct Control {
        error: Option<CompileControlError>,
        at_positive: bool,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                error: None,
                at_positive: false,
                calls: Mutex::new(vec![]),
            }
        }
        fn failing(error: CompileControlError, at_positive: bool) -> Self {
            Self {
                error: Some(error),
                at_positive,
                calls: Mutex::new(vec![]),
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.calls.lock().unwrap().push((phase, units));
            if let Some(error) = self.error.clone()
                && (!self.at_positive || units > 0)
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn policy() -> ConstantPolicy {
        ConstantPolicy {
            max_rows: 1_000_000,
            max_array_nodes: 4096,
            max_logical_elements: 100_000_000,
            max_retained_buffer_bytes: 64 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 1024 * 1024,
            max_library_validation_work: 100_000_000,
            max_library_validation_bytes: 100_000_000,
        }
    }
    fn pool(array: ArrayRef, nullable: bool) -> ConstantPool {
        ConstantPool::try_new(
            Arc::new(Field::new("literal", array.data_type().clone(), nullable)),
            FunctionValueType::new(array.data_type().clone(), nullable),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
    }
    fn text(value: &ConstantValue) -> Option<&str> {
        value
            .utf8_observed(CompilePhase::FunctionSpecialization, &Control::good())
            .unwrap()
    }
    fn int(value: &ConstantValue) -> Option<i64> {
        value
            .int64_observed(CompilePhase::FunctionSpecialization, &Control::good())
            .unwrap()
    }

    #[test]
    fn text_reads_selected_ordinal_and_borrows_actual_utf8_backing() {
        let original = Arc::new(StringArray::from(vec!["unselected", "雪☃", "other"]));
        let p = pool(original.clone(), false);
        let selected = p.value(1).unwrap();
        let actual = text(&selected).unwrap();
        assert_eq!(actual, "雪☃");
        assert!(std::ptr::eq(actual.as_ptr(), original.value(1).as_ptr()));
        assert_eq!(int(&selected), None);
        let sliced = pool(Arc::new(original.slice(1, 1)), false);
        assert_eq!(text(&sliced.value(0).unwrap()), Some("雪☃"));
        for array in [
            Arc::new(LargeStringArray::from(vec!["large"])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["view longer than inline bytes"])) as ArrayRef,
        ] {
            let p = pool(array.clone(), false);
            let expected = if array.data_type() == &DataType::LargeUtf8 {
                "large"
            } else {
                "view longer than inline bytes"
            };
            assert_eq!(text(&p.value(0).unwrap()), Some(expected));
        }
    }

    #[test]
    fn int64_is_exact_and_does_not_cast_signed_unsigned_or_temporal_carriers() {
        let p = pool(
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, -7])),
            false,
        );
        assert_eq!(int(&p.value(0).unwrap()), Some(i64::MIN));
        assert_eq!(int(&p.value(1).unwrap()), Some(i64::MAX));
        assert_eq!(text(&p.value(1).unwrap()), None);
        let sliced = pool(
            Arc::new(Int64Array::from(vec![99, -7, 100]).slice(1, 1)),
            false,
        );
        assert_eq!(int(&sliced.value(0).unwrap()), Some(-7));
        for array in [
            Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![7])) as ArrayRef,
            Arc::new(StringArray::from(vec!["7"])) as ArrayRef,
        ] {
            assert_eq!(int(&pool(array, false).value(0).unwrap()), None);
        }
    }

    #[test]
    fn selected_dictionary_and_run_end_values_are_resolved_for_both_accessors() {
        let dict = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![1, 0]),
            Arc::new(StringArray::from(vec!["unused", "selected"])) as ArrayRef,
        )
        .unwrap();
        let p = pool(Arc::new(dict), false);
        assert_eq!(text(&p.value(0).unwrap()), Some("selected"));
        assert_eq!(text(&p.value(1).unwrap()), Some("unused"));
        let dict = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![1]),
            Arc::new(Int64Array::from(vec![88, -123])) as ArrayRef,
        )
        .unwrap();
        assert_eq!(
            int(&pool(Arc::new(dict), false).value(0).unwrap()),
            Some(-123)
        );
        let run = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 4]),
            &StringArray::from(vec!["other", "selected"]),
        )
        .unwrap();
        assert_eq!(
            text(&pool(Arc::new(run), false).value(3).unwrap()),
            Some("selected")
        );
        let run = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2, 4]),
            &Int64Array::from(vec![9, i64::MIN]),
        )
        .unwrap();
        assert_eq!(
            int(&pool(Arc::new(run), false).value(2).unwrap()),
            Some(i64::MIN)
        );
    }

    #[test]
    fn typed_and_encoded_nulls_return_none_without_payload_conversion() {
        for array in [
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(NullArray::new(1)) as ArrayRef,
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0]),
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![2]),
                    &Int64Array::from(vec![None]),
                )
                .unwrap(),
            ) as ArrayRef,
        ] {
            let selected = pool(array, true).value(0).unwrap();
            assert_eq!(text(&selected), None);
            assert_eq!(int(&selected), None);
        }
    }

    #[test]
    fn carrier_read_does_not_change_or_infer_source_logical_identity() {
        let array = StringArray::from(vec!["{\"a\":1}"]);
        let value_type =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let field = Arc::new(Field::new("literal", DataType::Utf8, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "json".into(),
            )]),
        ));
        let json = ConstantPool::try_new(
            field,
            value_type.clone(),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        let plain = pool(Arc::new(array), false).value(0).unwrap();
        assert_eq!(text(&json), text(&plain));
        assert_eq!(json.value_type(), &value_type);
        assert_eq!(
            plain.value_type().logical_type(),
            ValueLogicalType::Physical
        );
        let binary = pool(
            Arc::new(arrow_array::BinaryArray::from(vec![b"text".as_slice()])),
            false,
        )
        .value(0)
        .unwrap();
        assert_eq!(text(&binary), None);
    }

    #[test]
    fn utf8_opaque_validation_has_typed_before_and_after_checks() {
        struct StopAtCall {
            error: CompileControlError,
            at: usize,
            calls: Mutex<Vec<(CompilePhase, u32)>>,
        }
        impl PureCompileControl for StopAtCall {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                let mut calls = self.calls.lock().unwrap();
                calls.push((phase, units));
                if calls.len() == self.at {
                    return Err(self.error.clone());
                }
                Ok(())
            }
        }
        let selected = pool(Arc::new(StringArray::from(vec!["雪"])), false)
            .value(0)
            .unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in [3, 4] {
                let control = StopAtCall {
                    error: error.clone(),
                    at,
                    calls: Mutex::new(vec![]),
                };
                assert_eq!(
                    selected.utf8_observed(CompilePhase::FunctionSpecialization, &control),
                    Err(ConstantError::Control(error.clone()))
                );
                let calls = control.calls.lock().unwrap();
                assert_eq!(calls.len(), at);
                assert!(
                    calls
                        .iter()
                        .all(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
                );
                assert_eq!(calls[0].1, 0);
                assert!(calls[1].1 > 0 && calls[1].1 < 256);
                assert!(calls[2..].iter().all(|(_, units)| *units == 0));
            }
        }
    }

    #[test]
    fn controls_remain_typed_at_entry_mid_text_and_finish() {
        let long = pool(
            Arc::new(StringArray::from(vec!["雪".repeat(150_000)])),
            false,
        )
        .value(0)
        .unwrap();
        let short = pool(Arc::new(Int64Array::from(vec![7])), false)
            .value(0)
            .unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at_positive in [false, true] {
                let control = Control::failing(error.clone(), at_positive);
                assert_eq!(
                    long.utf8_observed(CompilePhase::LowerProgram, &control),
                    Err(ConstantError::Control(error.clone()))
                );
                assert_eq!(
                    *control.calls.lock().unwrap(),
                    if at_positive {
                        vec![
                            (CompilePhase::LowerProgram, 0),
                            (CompilePhase::LowerProgram, 256),
                        ]
                    } else {
                        vec![(CompilePhase::LowerProgram, 0)]
                    }
                );
            }
            let control = Control::failing(error.clone(), true);
            assert_eq!(
                short.int64_observed(CompilePhase::ProviderValidation, &control),
                Err(ConstantError::Control(error.clone()))
            );
            let calls = control.calls.lock().unwrap();
            assert_eq!(calls[0], (CompilePhase::ProviderValidation, 0));
            assert_eq!(calls.len(), 2);
            assert!(calls[1].1 > 0 && calls[1].1 < 256);
            drop(calls);
            // Even a carrier mismatch observes the mandatory entry control.
            let control = Control::failing(error.clone(), false);
            assert_eq!(
                short.utf8_observed(CompilePhase::Encode, &control),
                Err(ConstantError::Control(error))
            );
        }
    }

    #[test]
    fn signed_integer_access_preserves_all_widths_bounds_and_selected_ordinals() {
        for (array, lower, upper) in [
            (
                Arc::new(Int8Array::from(vec![7, i8::MIN, i8::MAX])) as ArrayRef,
                i64::from(i8::MIN),
                i64::from(i8::MAX),
            ),
            (
                Arc::new(arrow_array::Int16Array::from(vec![7, i16::MIN, i16::MAX])) as ArrayRef,
                i64::from(i16::MIN),
                i64::from(i16::MAX),
            ),
            (
                Arc::new(Int32Array::from(vec![7, i32::MIN, i32::MAX])) as ArrayRef,
                i64::from(i32::MIN),
                i64::from(i32::MAX),
            ),
            (
                Arc::new(Int64Array::from(vec![7, i64::MIN, i64::MAX])) as ArrayRef,
                i64::MIN,
                i64::MAX,
            ),
        ] {
            let p = pool(array, false);
            for (ordinal, expected) in [(1, lower), (2, upper)] {
                let selected = p.value(ordinal).unwrap();
                let original_type = selected.value_type().clone();
                assert_eq!(
                    selected
                        .signed_integer_observed(
                            CompilePhase::FunctionSpecialization,
                            &Control::good()
                        )
                        .unwrap(),
                    Some(expected)
                );
                assert_eq!(selected.value_type(), &original_type);
                if original_type.data_type() != &DataType::Int64 {
                    assert_eq!(int(&selected), None);
                }
            }
        }
        let sliced = pool(
            Arc::new(arrow_array::Int16Array::from(vec![0, -123, 42]).slice(1, 1)),
            false,
        )
        .value(0)
        .unwrap();
        assert_eq!(
            sliced
                .signed_integer_observed(CompilePhase::Validate, &Control::good())
                .unwrap(),
            Some(-123)
        );
    }

    #[test]
    fn signed_integer_access_resolves_int32_dictionary_runs_and_typed_nulls() {
        let dict = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(Int32Array::from(vec![88, i32::MIN])) as ArrayRef,
        )
        .unwrap();
        let p = pool(Arc::new(dict), false);
        assert_eq!(
            p.value(1)
                .unwrap()
                .signed_integer_observed(CompilePhase::Validate, &Control::good())
                .unwrap(),
            Some(i64::from(i32::MIN))
        );
        let run = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 4]),
            &Int32Array::from(vec![99, -777]),
        )
        .unwrap();
        assert_eq!(
            pool(Arc::new(run), false)
                .value(3)
                .unwrap()
                .signed_integer_observed(CompilePhase::Validate, &Control::good())
                .unwrap(),
            Some(-777)
        );
        for array in [
            Arc::new(Int8Array::from(vec![None])) as ArrayRef,
            Arc::new(arrow_array::Int16Array::from(vec![None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0]),
                    Arc::new(Int32Array::from(vec![None])) as ArrayRef,
                )
                .unwrap(),
            ) as ArrayRef,
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![2]),
                    &Int32Array::from(vec![None]),
                )
                .unwrap(),
            ) as ArrayRef,
        ] {
            assert_eq!(
                pool(array, true)
                    .value(0)
                    .unwrap()
                    .signed_integer_observed(CompilePhase::Validate, &Control::good())
                    .unwrap(),
                None
            );
        }
    }

    #[test]
    fn signed_integer_access_refuses_unsigned_temporal_decimal_and_largeint_carriers() {
        for array in [
            Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![7])) as ArrayRef,
            Arc::new(arrow_array::Date32Array::from(vec![7])) as ArrayRef,
            Arc::new(
                arrow_array::Decimal128Array::from(vec![7])
                    .with_precision_and_scale(10, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(arrow_array::Float64Array::from(vec![7.0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["7"])) as ArrayRef,
        ] {
            assert_eq!(
                pool(array, false)
                    .value(0)
                    .unwrap()
                    .signed_integer_observed(CompilePhase::Validate, &Control::good())
                    .unwrap(),
                None
            );
        }
        let value_type = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let bytes = 7i128.to_le_bytes();
        let array = arrow_array::FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap();
        let largeint = ConstantPool::try_new(
            Arc::new(value_type.try_to_field("literal").unwrap()),
            value_type,
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert_eq!(
            largeint
                .signed_integer_observed(CompilePhase::Validate, &Control::good())
                .unwrap(),
            None
        );
    }

    #[test]
    fn signed_integer_access_propagates_entry_interior_and_final_typed_controls() {
        // A real selected scalar traverses nested run searches. This reaches
        // an interior quantum without adding work or a synthetic callback.
        let ends = Int32Array::from((1..=512).collect::<Vec<i32>>());
        let mut values = vec![0i32; 512];
        values[511] = -777;
        let mut encoded: ArrayRef = Arc::new(Int32Array::from(values));
        for _ in 0..16 {
            encoded = Arc::new(RunArray::<Int32Type>::try_new(&ends, encoded.as_ref()).unwrap());
        }
        let selected = pool(encoded, false).value(511).unwrap();
        assert_eq!(
            selected
                .signed_integer_observed(CompilePhase::LowerProgram, &Control::good())
                .unwrap(),
            Some(-777)
        );
        let short = pool(Arc::new(Int8Array::from(vec![-7])), false)
            .value(0)
            .unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at_positive in [false, true] {
                let control = Control::failing(error.clone(), at_positive);
                assert_eq!(
                    selected.signed_integer_observed(CompilePhase::LowerProgram, &control),
                    Err(ConstantError::Control(error.clone()))
                );
                assert_eq!(
                    *control.calls.lock().unwrap(),
                    if at_positive {
                        vec![
                            (CompilePhase::LowerProgram, 0),
                            (CompilePhase::LowerProgram, 256),
                        ]
                    } else {
                        vec![(CompilePhase::LowerProgram, 0)]
                    }
                );
            }
            let control = Control::failing(error.clone(), true);
            assert_eq!(
                short.signed_integer_observed(CompilePhase::FunctionSpecialization, &control),
                Err(ConstantError::Control(error))
            );
            let calls = control.calls.lock().unwrap();
            assert_eq!(calls[0], (CompilePhase::FunctionSpecialization, 0));
            assert_eq!(calls.len(), 2);
            assert!(calls[1].1 > 0 && calls[1].1 < 256);
        }
    }
}
