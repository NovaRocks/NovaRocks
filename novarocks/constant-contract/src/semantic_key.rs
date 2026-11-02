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

//! Observed local bucketing over one admitted constant's semantic value.
//! No key is a wire identity or an equality proof. Field names, nested type
//! facts and metadata deliberately collide; callers must use equals_observed.

use super::{
    ConstantError, ConstantValue, Row, list_range, logical_null, primitive_bytes, resolve_row,
    union_row, variable_bytes,
};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

/// Process-local bucket key only. Equal keys require observed exact comparison.
/// The algorithm and output are not a persistent or protocol contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConstantSemanticKey(u64);

impl ConstantValue {
    /// Hash the selected semantic value, observing the supplied phase/control.
    /// Dictionary numbering, unused rows/backing, and NULL payload are ignored.
    /// Floating values retain their exact bits, including NaNs and signed zero.
    /// This borrows already admitted backing; traversal scratch is not a MEM grant.
    pub fn semantic_key_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantSemanticKey, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let mut stream = SemanticStream::new();
        // Coarse type buckets are sufficient: equality still checks complete
        // semantic binding and normalized value layout. No opaque DataType Hash or Debug traversal occurs.
        work.step()?;
        stream
            .hasher
            .write_u8(logical_bucket(self.value_type().logical_type()));
        stream
            .hasher
            .write_u8(u8::from(self.value_type().nullable()));
        stream
            .hasher
            .write_u8(carrier_bucket(self.value_type().data_type()));
        hash_value(
            Row {
                data: &self.pool.0.data,
                index: self.ordinal as usize,
            },
            &mut stream,
            &mut work,
        )?;
        stream.flush_nulls();
        work.finish()?;
        Ok(ConstantSemanticKey(stream.hasher.finish()))
    }
}

fn logical_bucket(logical: ValueLogicalType) -> u8 {
    match logical {
        ValueLogicalType::Physical => 0,
        ValueLogicalType::Json => 1,
        ValueLogicalType::Variant => 2,
        ValueLogicalType::Hll => 3,
        ValueLogicalType::Bitmap => 4,
        ValueLogicalType::Object => 5,
        ValueLogicalType::Percentile => 6,
        ValueLogicalType::LargeInt => 7,
        ValueLogicalType::Uuid => 8,
    }
}

fn carrier_bucket(carrier: &DataType) -> u8 {
    match carrier {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float16 => 10,
        DataType::Float32 => 11,
        DataType::Float64 => 12,
        DataType::Timestamp(_, _) => 13,
        DataType::Date32 => 14,
        DataType::Date64 => 15,
        DataType::Time32(_) => 16,
        DataType::Time64(_) => 17,
        DataType::Duration(_) => 18,
        DataType::Interval(_) => 19,
        DataType::Binary => 20,
        DataType::FixedSizeBinary(_) => 21,
        DataType::LargeBinary => 22,
        DataType::BinaryView => 23,
        DataType::Utf8 => 24,
        DataType::LargeUtf8 => 25,
        DataType::Utf8View => 26,
        DataType::List(_) => 27,
        DataType::ListView(_) => 28,
        DataType::FixedSizeList(_, _) => 29,
        DataType::LargeList(_) => 30,
        DataType::LargeListView(_) => 31,
        DataType::Struct(_) => 32,
        DataType::Union(_, _) => 33,
        DataType::Dictionary(_, _) => 34,
        DataType::Decimal32(_, _) => 35,
        DataType::Decimal64(_, _) => 36,
        DataType::Decimal128(_, _) => 37,
        DataType::Decimal256(_, _) => 38,
        DataType::Map(_, _) => 39,
        DataType::RunEndEncoded(_, _) => 40,
    }
}

struct SemanticStream {
    hasher: DefaultHasher,
    nulls: u64,
}
impl SemanticStream {
    fn new() -> Self {
        Self {
            hasher: DefaultHasher::new(),
            nulls: 0,
        }
    }
    fn add_nulls(&mut self, count: usize) -> Result<(), ConstantError> {
        self.nulls = self
            .nulls
            .checked_add(
                u64::try_from(count)
                    .map_err(|_| ConstantError::Invalid("constant NULL run exceeds key extent"))?,
            )
            .ok_or(ConstantError::Invalid(
                "constant NULL run exceeds key extent",
            ))?;
        Ok(())
    }
    fn flush_nulls(&mut self) {
        if self.nulls != 0 {
            self.hasher.write_u8(0);
            self.hasher.write_u64(self.nulls);
            self.nulls = 0;
        }
    }
    fn token(&mut self, token: u8) {
        self.flush_nulls();
        self.hasher.write_u8(token);
    }
    fn count(&mut self, count: usize) -> Result<(), ConstantError> {
        self.hasher.write_u64(
            u64::try_from(count)
                .map_err(|_| ConstantError::Invalid("constant key extent exceeds u64"))?,
        );
        Ok(())
    }
    fn bytes(
        &mut self,
        bytes: &[u8],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ConstantError> {
        self.count(bytes.len())?;
        // Each opaque standard Hasher call covers at most the same 1024-byte
        // quantum used by exact observed byte equality.
        for chunk in bytes.chunks(1024) {
            work.step()?;
            self.hasher.write(chunk);
        }
        Ok(())
    }
}

enum Frame<'a> {
    Row(Row<'a>),
    Range(&'a ArrayData, usize, usize),
    Fields(&'a ArrayData, usize, usize),
    End(u8),
}
fn hash_value(
    row: Row<'_>,
    stream: &mut SemanticStream,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    // Continuation frames keep scratch proportional to admitted nesting, not
    // list length or Struct width. It is not first-allocation authorization.
    let mut pending = vec![Frame::Row(row)];
    while let Some(frame) = pending.pop() {
        work.step()?;
        let row = match frame {
            Frame::Row(row) => row,
            Frame::End(token) => {
                stream.token(token);
                continue;
            }
            Frame::Range(data, start, end) => {
                if start == end {
                    continue;
                }
                if matches!(data.data_type(), DataType::Null) {
                    // Canonical NULL-run aggregation gives the same stream as
                    // visiting those NULLs individually, without expansion.
                    stream.add_nulls(end - start)?;
                    continue;
                }
                pending.push(Frame::Range(data, start + 1, end));
                Row { data, index: start }
            }
            Frame::Fields(data, index, field) => {
                if field == data.child_data().len() {
                    continue;
                }
                pending.push(Frame::Fields(data, index, field + 1));
                Row {
                    data: &data.child_data()[field],
                    index,
                }
            }
        };
        if logical_null(row.data, row.index, work)? {
            // Union NULL ignores its tag, matching exact equality.
            stream.add_nulls(1)?;
            continue;
        }
        let row = resolve_row(row, work)?
            .ok_or(ConstantError::Invalid("inconsistent constant key NULL"))?;
        if let Some(width) = row.data.data_type().primitive_width() {
            stream.token(1);
            stream.bytes(primitive_bytes(row, width)?, work)?;
            continue;
        }
        match row.data.data_type() {
            DataType::Boolean => {
                stream.token(2);
                stream
                    .hasher
                    .write_u8(u8::from(arrow_buffer::bit_util::get_bit(
                        row.data.buffers()[0].as_slice(),
                        row.data.offset() + row.index,
                    )));
            }
            DataType::FixedSizeBinary(width) => {
                let width = usize::try_from(*width)
                    .map_err(|_| ConstantError::Invalid("negative constant key binary width"))?;
                stream.token(3);
                stream.bytes(primitive_bytes(row, width)?, work)?;
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView => {
                stream.token(4);
                stream.bytes(variable_bytes(row)?, work)?;
            }
            DataType::Struct(fields) => {
                stream.token(5);
                stream.count(fields.len())?;
                pending.push(Frame::End(6));
                pending.push(Frame::Fields(row.data, row.index, 0));
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _) => {
                let (start, end) = list_range(row)?;
                stream.token(7);
                stream.count(end - start)?;
                pending.push(Frame::End(8));
                pending.push(Frame::Range(&row.data.child_data()[0], start, end));
            }
            DataType::Union(_, _) => {
                let (tag, child, _) = union_row(row.data, row.index, work)?;
                stream.token(9);
                stream.hasher.write_i8(tag);
                pending.push(Frame::End(10));
                pending.push(Frame::Row(child));
            }
            _ => {
                return Err(ConstantError::Invalid(
                    "unsupported constant semantic key carrier",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConstantPolicy, ConstantPool};
    use arrow_array::types::{Int8Type, Int32Type};
    use arrow_array::{
        Array, ArrayRef, DictionaryArray, Float32Array, Float64Array, Int8Array, Int64Array,
        ListArray, NullArray, RunArray, StringArray,
    };
    use arrow_schema::Field;
    use novarocks_type_contract::{CompileControlError, FunctionValueType};
    use std::sync::{Arc, Mutex};

    struct Control {
        error: Option<CompileControlError>,
        after: u64,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                error: None,
                after: u64::MAX,
                calls: Mutex::new(vec![]),
            }
        }
        fn failing(error: CompileControlError, after: u64) -> Self {
            Self {
                error: Some(error),
                after,
                calls: Mutex::new(vec![]),
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            if let Some(error) = self.error.clone()
                && calls
                    .iter()
                    .map(|(_, units)| u64::from(*units))
                    .sum::<u64>()
                    >= self.after
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
        let field = Arc::new(Field::new("literal", array.data_type().clone(), nullable));
        ConstantPool::try_new(
            field,
            FunctionValueType::new(array.data_type().clone(), nullable),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
    }
    fn value(array: ArrayRef, nullable: bool) -> ConstantValue {
        pool(array, nullable).value(0).unwrap()
    }
    fn key(value: &ConstantValue) -> ConstantSemanticKey {
        value
            .semantic_key_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    }
    fn assert_equal_keys(left: &ConstantValue, right: &ConstantValue) {
        assert!(
            left.equals_observed(right, CompilePhase::Validate, &Control::good())
                .unwrap()
        );
        assert_eq!(key(left), key(right));
    }

    #[test]
    fn selected_value_ignores_pool_rows_slices_and_null_payload() {
        let whole = pool(Arc::new(Int64Array::from(vec![99, 7, 123])), false);
        let selected = whole.value(1).unwrap();
        let same = value(Arc::new(Int64Array::from(vec![7])), false);
        assert_equal_keys(&selected, &same);
        let sliced = value(
            Arc::new(Int64Array::from(vec![88, 7, 66]).slice(1, 1)),
            false,
        );
        assert_equal_keys(&same, &sliced);
        assert_ne!(key(&selected), key(&whole.value(0).unwrap()));
        let null = value(Arc::new(Int64Array::from(vec![None])), true);
        let masked = Int64Array::from(vec![8675309])
            .to_data()
            .into_builder()
            .nulls(Some(arrow_buffer::NullBuffer::new_null(1)))
            .build()
            .unwrap();
        let masked = ConstantPool::try_new(
            Arc::new(Field::new("literal", DataType::Int64, true)),
            FunctionValueType::new(DataType::Int64, true),
            masked,
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert_equal_keys(&null, &masked);
        assert_ne!(
            key(&null),
            key(&value(
                Arc::new(StringArray::from(vec![None::<&str>])),
                true
            ))
        );
    }

    #[test]
    fn dictionary_keys_unused_values_and_run_layout_are_not_payload() {
        let left = value(
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0]),
                    Arc::new(StringArray::from(vec!["selected", "unused"])) as ArrayRef,
                )
                .unwrap(),
            ),
            false,
        );
        let right = value(
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![1]),
                    Arc::new(StringArray::from(vec!["other", "selected", "ignored"])) as ArrayRef,
                )
                .unwrap(),
            ),
            false,
        );
        assert_equal_keys(&left, &right);
        let left = pool(
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &arrow_array::Int32Array::from(vec![2, 3]),
                    &StringArray::from(vec!["selected", "x"]),
                )
                .unwrap(),
            ),
            false,
        );
        let right = pool(
            Arc::new(
                RunArray::<Int32Type>::try_new(
                    &arrow_array::Int32Array::from(vec![1, 3]),
                    &StringArray::from(vec!["selected", "selected"]),
                )
                .unwrap(),
            ),
            false,
        );
        assert_equal_keys(&left.value(1).unwrap(), &right.value(1).unwrap());
    }

    #[test]
    fn float_payload_bits_preserve_signed_zero_and_nan_identity() {
        for bits in [0x7fc0_0011u32, 0x7fc0_0012, 0x8000_0000] {
            let left = value(
                Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
                false,
            );
            let right = value(
                Arc::new(Float32Array::from(vec![f32::from_bits(bits)])),
                false,
            );
            assert_equal_keys(&left, &right);
        }
        let p = pool(
            Arc::new(Float32Array::from(vec![
                0.0,
                -0.0,
                f32::from_bits(0x7fc0_0011),
                f32::from_bits(0x7fc0_0012),
            ])),
            false,
        );
        assert_ne!(key(&p.value(0).unwrap()), key(&p.value(1).unwrap()));
        assert_ne!(key(&p.value(2).unwrap()), key(&p.value(3).unwrap()));
        let double = value(
            Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0001,
            )])),
            false,
        );
        assert_equal_keys(
            &double,
            &value(
                Arc::new(Float64Array::from(vec![f64::from_bits(
                    0x7ff8_0000_0000_0001,
                )])),
                false,
            ),
        );
    }

    #[test]
    fn source_field_attributes_do_not_change_typed_value_equality() {
        let left = value(Arc::new(StringArray::from(vec!["same"])), false);
        let field = Arc::new(
            Field::new("different", DataType::Utf8, false).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "annotation".into(),
                    "different".into(),
                )]),
            ),
        );
        let right = ConstantPool::try_new(
            field,
            FunctionValueType::new(DataType::Utf8, false),
            StringArray::from(vec!["same"]).to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert_eq!(key(&left), key(&right));
        assert!(
            left.equals_observed(&right, CompilePhase::Validate, &Control::good())
                .unwrap()
        );
    }

    #[test]
    fn explicit_root_domains_are_preserved_in_typed_value_buckets() {
        let array = StringArray::from(vec!["{\"v\":1}"]);
        let physical = value(Arc::new(array.clone()), false);
        let json = ConstantPool::try_new(
            Arc::new(Field::new("literal", DataType::Utf8, false)),
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap(),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert!(
            !physical
                .equals_observed(&json, CompilePhase::Validate, &Control::good())
                .unwrap()
        );
        assert_ne!(key(&physical), key(&json));
        assert_equal_keys(&json, &json.clone());
    }

    #[test]
    fn compact_null_ranges_are_canonical_and_do_not_expand() {
        let make = || {
            value(
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Null, true)),
                    arrow_buffer::OffsetBuffer::new(vec![0i32, 1_000_000].into()),
                    Arc::new(NullArray::new(1_000_000)),
                    None,
                )),
                false,
            )
        };
        let left = make();
        let right = make();
        let control = Control::good();
        assert_eq!(
            left.semantic_key_observed(CompilePhase::Validate, &control)
                .unwrap(),
            key(&right)
        );
        assert!(
            control
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|(_, units)| *units)
                .sum::<u32>()
                < 32
        );
        let mut compact = SemanticStream::new();
        compact.token(7);
        compact.count(320).unwrap();
        compact.add_nulls(320).unwrap();
        compact.token(8);
        let mut walked = SemanticStream::new();
        walked.token(7);
        walked.count(320).unwrap();
        for _ in 0..320 {
            walked.add_nulls(1).unwrap();
        }
        walked.token(8);
        assert_eq!(compact.hasher.finish(), walked.hasher.finish());
        let nested = || {
            value(
                Arc::new(ListArray::from_iter_primitive::<
                    arrow_array::types::Int64Type,
                    _,
                    _,
                >(vec![Some(vec![Some(1), None, Some(3)])])),
                false,
            )
        };
        assert_equal_keys(&nested(), &nested());
    }

    #[test]
    fn union_logical_null_ignores_selected_variant_tag() {
        use arrow_schema::{UnionFields, UnionMode};
        let fields = UnionFields::try_new(
            vec![1, 7],
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ],
        )
        .unwrap();
        let data = ArrayData::builder(DataType::Union(fields, UnionMode::Dense))
            .len(4)
            .buffers(vec![
                arrow_buffer::Buffer::from_slice_ref([1i8, 7, 1, 7]),
                arrow_buffer::Buffer::from_slice_ref([0i32, 0, 1, 1]),
            ])
            .child_data(vec![
                Int64Array::from(vec![None, Some(7)]).to_data(),
                Int64Array::from(vec![None, Some(7)]).to_data(),
            ])
            .build()
            .unwrap();
        let p = ConstantPool::try_new(
            Arc::new(Field::new("literal", data.data_type().clone(), true)),
            FunctionValueType::new(data.data_type().clone(), true),
            data,
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap();
        assert_equal_keys(&p.value(0).unwrap(), &p.value(1).unwrap());
        assert!(
            !p.value(2)
                .unwrap()
                .equals_observed(
                    &p.value(3).unwrap(),
                    CompilePhase::Validate,
                    &Control::good()
                )
                .unwrap()
        );
        assert_ne!(key(&p.value(2).unwrap()), key(&p.value(3).unwrap()));
    }

    #[test]
    fn typed_controls_preserve_phase_and_do_not_publish_partial_keys() {
        let long = value(
            Arc::new(StringArray::from(vec!["x".repeat(400_000)])),
            false,
        );
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for after in [0, 256] {
                let control = Control::failing(error.clone(), after);
                assert_eq!(
                    long.semantic_key_observed(CompilePhase::LowerProgram, &control),
                    Err(ConstantError::Control(error.clone()))
                );
                let calls = control.calls.lock().unwrap();
                assert!(
                    calls
                        .iter()
                        .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
                );
                assert_eq!(
                    calls.iter().map(|(_, units)| *units).collect::<Vec<_>>(),
                    if after == 0 { vec![0] } else { vec![0, 256] }
                );
            }
            let short = value(Arc::new(Int64Array::from(vec![7])), false);
            let control = Control::failing(error.clone(), 1);
            assert_eq!(
                short.semantic_key_observed(CompilePhase::Encode, &control),
                Err(ConstantError::Control(error))
            );
            let calls = control.calls.lock().unwrap();
            assert_eq!(calls[0], (CompilePhase::Encode, 0));
            assert_eq!(calls.len(), 2);
            assert!(calls[1].1 > 0 && calls[1].1 < 256);
            // Failure does not mutate the admitted value or prevent a fresh
            // controlled call from producing the complete same key.
            assert_eq!(key(&short), key(&short.clone()));
        }
    }
}
