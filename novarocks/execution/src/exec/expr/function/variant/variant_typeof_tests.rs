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
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::exec::expr::{ExprEvaluation, ExprNode};
use arrow::array::{LargeBinaryArray, StringArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::FunctionValueType;
use novarocks_types::SlotId;

fn input(array: ArrayRef, ty: FunctionValueType) -> (ExprArena, ExprId, Chunk) {
    let schema = Arc::new(Schema::new(vec![ty.try_to_field("document").unwrap()]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let mut arena = ExprArena::default();
    let argument = arena.push_value_typed(ExprNode::SlotId(SlotId::new(1)), ty);
    (arena, argument, chunk)
}
fn labels(arena: &ExprArena, argument: ExprId, chunk: &Chunk) -> Vec<Option<String>> {
    let result = eval_variant_typeof(arena, argument, &[argument], chunk).unwrap();
    result
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}
#[test]
fn variant_typeof_declared_json_slices_keep_row_local_nulls_and_original_width_labels() {
    let rows = [
        (Some("{}"), Some("Object")),
        (Some("[1,2]"), Some("Array")),
        (Some("null"), Some("Null")),
        (Some("1"), Some("Int8")),
        (Some("127"), Some("Int8")),
        (Some("128"), Some("Int16")),
        (Some("-129"), Some("Int16")),
        (Some("32768"), Some("Int32")),
        (Some("2147483648"), Some("Int64")),
        (Some("9223372036854775807"), Some("Int64")),
        (Some("18446744073709551615"), Some("Double")),
        (Some("1.5"), Some("Double")),
        (Some("true"), Some("Boolean(true)")),
        (Some("false"), Some("Boolean(false)")),
        (Some("\"abc\""), Some("ShortString")),
        (Some("not-json"), None),
        (None, None),
        (Some("[]"), Some("Array")),
        (Some("1e9999"), None),
        (Some("-128"), Some("Int8")),
    ];
    let mut source = vec![Some("poison prefix")];
    source.extend(rows.iter().map(|(value, _)| *value));
    source.push(Some("poison suffix"));
    let array: ArrayRef = Arc::new(StringArray::from(source));
    let ty = FunctionValueType::try_with_logical_type(
        arrow::datatypes::DataType::Utf8,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let (arena, argument, chunk) = input(array.slice(1, rows.len()), ty.clone());
    assert_eq!(
        labels(&arena, argument, &chunk),
        rows.iter()
            .map(|(_, value)| value.map(str::to_owned))
            .collect::<Vec<_>>()
    );
    let empty: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    let (arena, argument, chunk) = input(empty, ty);
    assert!(labels(&arena, argument, &chunk).is_empty());
}
#[test]
fn variant_typeof_nominal_variant_keeps_original_bytes_and_damaged_row_null_policy() {
    use novarocks_types::value::variant::VariantMetadata;
    let metadata = VariantMetadata::empty();
    let mut integer = vec![6 << 2];
    integer.extend_from_slice(&123_i64.to_le_bytes());
    let integer = VariantValue::create(metadata.raw(), &integer)
        .unwrap()
        .serialize();
    let json_null = VariantValue::create(metadata.raw(), &[0])
        .unwrap()
        .serialize();
    let array: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(integer.as_slice()),
        None,
        Some(&[255][..]),
        Some(json_null.as_slice()),
    ]));
    let ty = FunctionValueType::try_with_logical_type(
        arrow::datatypes::DataType::LargeBinary,
        true,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let (arena, argument, chunk) = input(array, ty);
    assert_eq!(
        labels(&arena, argument, &chunk),
        vec![Some("Int64".into()), None, None, Some("Null".into())]
    );
}
#[test]
fn variant_typeof_cannot_invent_json_from_plain_utf8_or_untyped_text() {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{}"]));
    let (arena, argument, chunk) = input(
        array,
        FunctionValueType::new(arrow::datatypes::DataType::Utf8, false),
    );
    assert!(eval_variant_typeof(&arena, argument, &[argument], &chunk).is_err());
    let mut untyped = ExprArena::default();
    let argument = untyped.push_typed(
        ExprNode::SlotId(SlotId::new(1)),
        arrow::datatypes::DataType::Utf8,
    );
    assert!(eval_variant_typeof(&untyped, argument, &[argument], &chunk).is_err());
}
#[test]
fn variant_typeof_never_turns_an_argument_runtime_refusal_into_a_null_row() {
    use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
    struct Failing {
        arena: ExprArena,
        original: ExecutionFailure,
        calls: std::cell::Cell<usize>,
    }
    impl ExprEvaluation for Failing {
        type Error = ExecutionFailure;
        fn arena(&self) -> &ExprArena {
            &self.arena
        }
        fn eval(&self, _: ExprId, _: &Chunk) -> Result<ArrayRef, Self::Error> {
            self.calls.set(self.calls.get() + 1);
            Err(self.original.clone())
        }
        fn slice_input(&self, _: &Chunk, _: usize, _: usize) -> Result<Chunk, Self::Error> {
            panic!("no retry after argument failure")
        }
        fn recoverable_error(&self, _: &Self::Error) -> bool {
            false
        }
        fn annotate_error(
            &self,
            error: Self::Error,
            _: impl FnOnce(&str) -> String,
        ) -> Self::Error {
            error
        }
    }
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{}"]));
    let ty = FunctionValueType::try_with_logical_type(
        arrow::datatypes::DataType::Utf8,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let (arena, argument, chunk) = input(array, ty);
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled)
        .at_operator(13, PipelineOperation::Pull);
    let failing = Failing {
        arena,
        original: original.clone(),
        calls: std::cell::Cell::new(0),
    };
    assert_eq!(
        eval_variant_typeof(&failing, argument, &[argument], &chunk).unwrap_err(),
        original
    );
    assert_eq!(failing.calls.get(), 1);
}
