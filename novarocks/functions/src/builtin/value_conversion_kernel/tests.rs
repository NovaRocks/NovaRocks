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
use crate::{
    ConstantPolicy, ConstantPool, FunctionValueType, KernelDiagnostic, ScalarEvaluationInstance,
};
use arrow_array::{
    DictionaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;
use novarocks_type_contract::{CompileControlError, NR_LOGICAL_TYPE_KEY, ValueLogicalType};
use std::{collections::HashMap, sync::Mutex, time::Duration};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = self.refusal
            && trace.len() == at + 1
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.refusal
            && trace.len() == at + 1
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure conversion must not wait")
    }
}
fn failures() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original invalid"),
        internal("original internal"),
        KernelFailure::Operational(KernelDiagnostic::new("original operation")),
        KernelFailure::InstanceFailed,
    ]
}
fn physical(ty: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(ty, nullable)
}
fn large_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn prepared(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Arc<dyn crate::PreparedScalarKernel> {
    super::super::value_conversion_owner::prepared_for_test(source, target).unwrap()
}
fn run<'a>(
    source: &FunctionValueType,
    target: &FunctionValueType,
    argument: EvaluatedArgument<'a>,
    selection: Selection<'a>,
) -> ArrayRef {
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(source, target)).unwrap();
    let normalized;
    let argument = match argument {
        EvaluatedArgument::Column(array) => {
            normalized = crate::test_value_ingress::array_for_binding(array, source).unwrap();
            EvaluatedArgument::Column(&normalized)
        }
        other => other,
    };
    let args = [argument];
    let output = instance
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    Arc::clone(output.values())
}
fn large(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn json_field(name: &str, json: bool) -> Arc<Field> {
    let mut metadata = HashMap::from([("provider.fact".to_owned(), "keep-original".to_owned())]);
    if json {
        metadata.insert(NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned());
    }
    Arc::new(Field::new(name, DataType::Utf8, true).with_metadata(metadata))
}

#[test]
fn signed_four_widths_extend_to_exact_signed_big_endian_largeint_without_row_errors() {
    let arrays: Vec<(ArrayRef, Vec<Option<i128>>)> = vec![
        (
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
            vec![Some(-128), None, Some(127)],
        ),
        (
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
            vec![Some(-32768), None, Some(32767)],
        ),
        (
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
            vec![Some(-2147483648), None, Some(2147483647)],
        ),
        (
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            vec![Some(-9223372036854775808), None, Some(9223372036854775807)],
        ),
    ];
    for (array, expected) in arrays {
        let output = run(
            &physical(array.data_type().clone(), true),
            &large_type(true),
            EvaluatedArgument::Column(&array),
            Selection::all(3),
        );
        let output = output
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (row, expected) in expected.into_iter().enumerate() {
            match expected {
                None => assert!(output.is_null(row)),
                Some(value) => assert_eq!(output.value(row), value.to_be_bytes()),
            }
        }
    }
}

#[test]
fn largeint_signed_range_null_and_direct_float_rounding_use_independent_expected_values() {
    let source = large(&[
        Some(i128::MIN),
        Some(-129),
        Some(-128),
        Some(127),
        Some(128),
        Some(i128::MAX),
        None,
    ]);
    let targets = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    for target in targets {
        let result = run(
            &large_type(true),
            &physical(target.clone(), true),
            EvaluatedArgument::Column(&source),
            Selection::all(7),
        );
        macro_rules! check {
            ($ty:ty) => {{
                let result = result.as_any().downcast_ref::<$ty>().unwrap();
                assert!(result.is_null(0));
                assert!(result.is_null(5));
                assert!(result.is_null(6));
                assert_eq!(i128::from(result.value(2)), -128);
                assert_eq!(i128::from(result.value(3)), 127);
                if target == DataType::Int8 {
                    assert!(result.is_null(1));
                    assert!(result.is_null(4));
                } else {
                    assert_eq!(i128::from(result.value(1)), -129);
                    assert_eq!(i128::from(result.value(4)), 128);
                }
            }};
        }
        match target {
            DataType::Int8 => check!(Int8Array),
            DataType::Int16 => check!(Int16Array),
            DataType::Int32 => check!(Int32Array),
            DataType::Int64 => check!(Int64Array),
            _ => unreachable!(),
        }
    }
    let source = large(&[
        Some(i128::MIN),
        Some(i128::MAX),
        Some((1i128 << 100) + (1i128 << 76) + 1),
        Some((1i128 << 53) + 1),
    ]);
    let f32_output = run(
        &large_type(false),
        &physical(DataType::Float32, false),
        EvaluatedArgument::Column(&source),
        Selection::all(4),
    );
    let f32_output = f32_output.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(f32_output.value(0).to_bits(), 0xff000000);
    assert_eq!(f32_output.value(1).to_bits(), 0x7f000000);
    assert_eq!(f32_output.value(2).to_bits(), 0x71800001); // A f64 intermediate incorrectly yields 0x71800000.
    let f64_output = run(
        &large_type(false),
        &physical(DataType::Float64, false),
        EvaluatedArgument::Column(&source),
        Selection::all(4),
    );
    let f64_output = f64_output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(f64_output.value(0).to_bits(), 0xc7e0000000000000);
    assert_eq!(f64_output.value(1).to_bits(), 0x47e0000000000000);
    assert_eq!(f64_output.value(3).to_bits(), 0x4340000000000000);
}

#[test]
fn json_changed_metadata_only_preserves_five_container_shapes_bytes_offsets_and_nulls() {
    let source_field = json_field("actual-item", true);
    let target_field = json_field("actual-item", false);
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some(" { \"a\": 1 } "), None]));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::clone(&source_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
            Arc::clone(&values),
            None,
        )
        .unwrap(),
    );
    let large_list: ArrayRef = Arc::new(
        LargeListArray::try_new(
            Arc::clone(&source_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 1, 2])),
            Arc::clone(&values),
            None,
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(Arc::clone(&source_field), 1, Arc::clone(&values), None)
            .unwrap(),
    );
    let structure: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![Arc::clone(&source_field)].into(),
            vec![Arc::clone(&values)],
            None,
        )
        .unwrap(),
    );
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
    let entries = StructArray::try_new(
        vec![Arc::clone(&key_field), Arc::clone(&source_field)].into(),
        vec![keys, Arc::clone(&values)],
        None,
    )
    .unwrap();
    let entry_field = Arc::new(
        Field::new("entries", entries.data_type().clone(), false).with_metadata(HashMap::from([(
            "provider.map".to_owned(),
            "retain".to_owned(),
        )])),
    );
    let map: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::clone(&entry_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 2])),
            entries,
            None,
            true,
        )
        .unwrap(),
    );
    let target_entries = entry_field
        .as_ref()
        .clone()
        .with_data_type(DataType::Struct(
            vec![key_field, Arc::clone(&target_field)].into(),
        ));
    let targets = [
        DataType::List(Arc::clone(&target_field)),
        DataType::LargeList(Arc::clone(&target_field)),
        DataType::FixedSizeList(Arc::clone(&target_field), 1),
        DataType::Struct(vec![Arc::clone(&target_field)].into()),
        DataType::Map(Arc::new(target_entries), true),
    ];
    for (source, target) in [list, large_list, fixed, structure, map]
        .into_iter()
        .zip(targets)
    {
        let target = physical(target, true);
        let output = run(
            &physical(source.data_type().clone(), true),
            &target,
            EvaluatedArgument::Column(&source),
            Selection::all(2),
        );
        assert!(novarocks_type_contract::arrow_data_types_exact(
            output.data_type(),
            target.data_type()
        ));
        let original = source.to_data();
        let output = output.to_data();
        assert_eq!(original.len(), output.len());
        assert_eq!(original.nulls(), output.nulls());
        assert_eq!(original.buffers(), output.buffers());
    }
    let source_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let output = run(
        &source_type,
        &physical(DataType::Utf8, true),
        EvaluatedArgument::Column(&values),
        Selection::all(2),
    );
    let output = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(output.value(0), " { \"a\": 1 } ");
    assert!(output.is_null(1));
}

#[test]
fn json_sparse_conversion_preserves_typed_uuid_and_explicitly_decoded_plain_sibling() {
    let dictionary_values: ArrayRef = Arc::new(StringArray::from(
        (0..128).map(|i| format!("v{i}")).collect::<Vec<_>>(),
    ));
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 127]),
        Arc::clone(&dictionary_values),
    )
    .unwrap();
    // Preserve the public authored dictionary ID/order oracle.
    #[allow(deprecated)]
    let dictionary_field = Arc::new(Field::new_dict(
        "actual-dict",
        dictionary.data_type().clone(),
        true,
        9,
        true,
    ));
    let uuid_field = Arc::new(
        Field::new("uuid", DataType::FixedSizeBinary(16), false).with_metadata(HashMap::from([(
            NR_LOGICAL_TYPE_KEY.to_owned(),
            "uuid".to_owned(),
        )])),
    );
    let uuid = large(&[Some(-1), Some(0)]);
    let source_field = json_field("json", true);
    let target_field = json_field("json", false);
    let array: ArrayRef = Arc::new(
        StructArray::try_new(
            vec![
                source_field,
                Arc::clone(&dictionary_field),
                Arc::clone(&uuid_field),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["unused", "selected"])),
                Arc::new(dictionary),
                uuid,
            ],
            None,
        )
        .unwrap(),
    );
    let target = DataType::Struct(vec![target_field, dictionary_field, uuid_field].into());
    let source = physical(array.data_type().clone(), false);
    let target = physical(target, false);
    let decoded = arrow_cast::cast(array.as_ref(), source.data_type()).unwrap();
    let array = crate::test_value_ingress::array_for_binding(&decoded, &source).unwrap();
    let rows = [1usize];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Column(&array),
        selection,
    );
    assert!(novarocks_type_contract::arrow_data_types_exact(
        output.data_type(),
        target.data_type()
    ));
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "selected"
    );
    let output_plain = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(output_plain.value(0), "v127");
    assert_eq!(
        target.value_type().child_type_at(2).unwrap().domain(),
        Some(ValueLogicalType::Uuid)
    );
    assert_eq!(
        output
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        0i128.to_be_bytes()
    );
}

#[test]
fn conversion_constant_nonzero_pool_ordinal_scalar_and_selected_column_keep_original_addresses() {
    let source_type = large_type(true);
    let target = physical(DataType::Int64, true);
    let source = large(&[Some(999), None, Some(-71)]);
    let field = Field::new("actual", DataType::FixedSizeBinary(16), true).with_metadata(
        HashMap::from([(NR_LOGICAL_TYPE_KEY.to_owned(), "largeint".to_owned())]),
    );
    let policy = ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 8,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1024,
        max_library_validation_work: 4096,
        max_library_validation_bytes: 8192,
    };
    let pool = ConstantPool::try_new(
        Arc::new(field),
        source_type.clone(),
        source.to_data(),
        policy,
        CompilePhase::FunctionSpecialization,
        &CompileControl::default(),
    )
    .unwrap();
    let value = pool.value(2).unwrap();
    let rows = [1usize, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let output = run(
        &source_type,
        &target,
        EvaluatedArgument::Constant(&value),
        selection,
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-71), Some(-71)]
    );
    let scalar = source.slice(2, 1);
    let output = run(
        &source_type,
        &target,
        EvaluatedArgument::Scalar(&scalar),
        selection,
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-71), Some(-71)]
    );
    let compact = large(&[Some(12), Some(-7)]);
    let selected = SelectedValues::try_new(
        selection,
        &DataType::FixedSizeBinary(16),
        compact,
        Box::default(),
    )
    .unwrap();
    let output = run(
        &source_type,
        &target,
        EvaluatedArgument::SelectedColumn(&selected),
        selection,
    );
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(12), Some(-7)]
    );
}

#[test]
fn conversion_actual_compile_metadata_quantum_and_runtime_rows_preserve_every_original_refusal_prefix()
 {
    let fields = (0..320)
        .map(|i| json_field(&format!("f{i}"), true))
        .collect::<Vec<_>>();
    let target_fields = (0..320)
        .map(|i| json_field(&format!("f{i}"), false))
        .collect::<Vec<_>>();
    let source_type = physical(DataType::Struct(fields.into()), true);
    let target = physical(DataType::Struct(target_fields.into()), true);
    let prepared = prepared(&source_type, &target);
    let baseline = CompileControl::default();
    ConversionRecipe::try_new(prepared.contract(), &baseline).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileControl {
                refusal: Some((at, cause)),
                ..Default::default()
            };
            let error = ConversionRecipe::try_new(prepared.contract(), &control).unwrap_err();
            assert_eq!(error, compile_failure(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let source_type = physical(DataType::Int64, false);
    let target = large_type(false);
    let source: ArrayRef = Arc::new(Int64Array::from(
        (0..320).map(i64::from).collect::<Vec<_>>(),
    ));
    let arguments = [EvaluatedArgument::Column(&source)];
    let selection = Selection::all(320);
    let baseline = Control::default();
    let mut instance = ScalarEvaluationInstance::instantiate(
        super::super::value_conversion_owner::prepared_for_test(&source_type, &target).unwrap(),
    )
    .unwrap();
    instance.evaluate(selection, &arguments, &baseline).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut instance = ScalarEvaluationInstance::instantiate(
                super::super::value_conversion_owner::prepared_for_test(&source_type, &target)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance
                    .evaluate(selection, &arguments, &control)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[derive(Debug)]
struct ForeignInt64(Int64Array);
// SAFETY: All Arrow buffer/layout methods delegate to the immutable canonical
// Int64Array; only Any identity differs to test the required class gate.
unsafe impl Array for ForeignInt64 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.into_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.0.slice(offset, length))
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}
#[test]
fn conversion_concrete_classes_and_required_error_journal_are_checked_before_successful_null() {
    let source = physical(DataType::Int64, true);
    let target = large_type(true);
    let foreign: ArrayRef = Arc::new(ForeignInt64(Int64Array::from(vec![None])));
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
    assert!(matches!(
        instance.evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Column(&foreign)],
            &Control::default()
        ),
        Err(KernelFailure::Internal(_))
    ));
    let child_values: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let child = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        child_values,
        vec![crate::RowDataError::new(0, "required source error")].into_boxed_slice(),
    )
    .unwrap();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
    assert!(matches!(
        instance.evaluate(
            Selection::all(1),
            &[EvaluatedArgument::SelectedColumn(&child)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn null_lift_has_real_empty_wrapper_target_and_rejects_nonempty_active_domain_without_fallback() {
    let source = physical(DataType::Null, true);
    let target =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let input: ArrayRef = Arc::new(arrow_array::NullArray::new(3));
    let rows = [];
    let empty = Selection::try_sparse(3, &rows).unwrap();
    let arguments = [EvaluatedArgument::Column(&input)];
    let mut instance = ScalarEvaluationInstance::instantiate(prepared(&source, &target)).unwrap();
    let output = instance
        .evaluate(empty, &arguments, &Control::default())
        .unwrap();
    assert_eq!(output.values().data_type(), &DataType::Utf8);
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert_eq!(instance.contract().result_type(), &target);
    assert!(matches!(
        instance.evaluate(Selection::all(3), &arguments, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let control = Control::default();
    assert_eq!(
        instance.evaluate(empty, &arguments, &control).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(control.trace.lock().unwrap().is_empty());
    // The public wrapper returns empty before body invocation; this is not a
    // claim that arbitrary empty foreign concrete implementations are checked.
}

#[test]
fn direct_format_extent_failure_is_primary_before_builder_and_finish_control() {
    assert!(extent(isize::MAX as usize / 16, 16).is_ok());
    assert_eq!(
        extent(isize::MAX as usize / 16 + 1, 16),
        Err(KernelFailure::ResourceExhausted)
    );
    for cause in failures() {
        let control = Control {
            refusal: Some((0, cause)),
            ..Default::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        work.step().unwrap();
        let failed = extent(usize::MAX, 16).unwrap_err();
        assert_eq!(
            finish_evaluation(Err(failed), work).unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert!(control.trace.lock().unwrap().is_empty());
    }
    let source_type = physical(DataType::Int64, true);
    let target = large_type(true);
    let input: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    let mut instance =
        ScalarEvaluationInstance::instantiate(prepared(&source_type, &target)).unwrap();
    assert_eq!(
        instance
            .evaluate(
                Selection::all(usize::MAX),
                &[EvaluatedArgument::Scalar(&input)],
                &Control::default()
            )
            .unwrap_err(),
        KernelFailure::ResourceExhausted
    );
}

#[path = "largeint_text_tests.rs"]
mod largeint_text_tests;

#[test]
fn opaque_domain_erasure_executes_existing_same_shape_bytes_and_preserves_original_control() {
    let bytes = crate::hll::encode_hll_single(0x0102_0304_0506_0708);
    let source_type =
        FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
            .unwrap();
    let target = physical(DataType::Binary, true);
    let source: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
        Some(bytes.as_slice()),
        None,
    ]));
    let output = run(
        &source_type,
        &target,
        EvaluatedArgument::Column(&source),
        Selection::all(2),
    );
    let actual = output
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap();
    assert_eq!(actual.value(0), bytes.as_slice());
    assert!(actual.is_null(1));
    let prepared = prepared(&source_type, &target);
    assert_eq!(
        prepared.contract().selected().overload.as_str(),
        super::super::value_conversion::OPAQUE_BINARY
    );
    let arguments = [EvaluatedArgument::Column(&source)];
    let baseline = Control::default();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
    instance
        .evaluate(Selection::all(2), &arguments, &baseline)
        .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            assert_eq!(
                instance
                    .evaluate(Selection::all(2), &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[path = "utf8_largeint_tests.rs"]
mod utf8_largeint_tests;

#[path = "largeint_datetime_tests.rs"]
mod largeint_datetime_tests;

#[path = "largeint_decimal_tests.rs"]
mod largeint_decimal_tests;
