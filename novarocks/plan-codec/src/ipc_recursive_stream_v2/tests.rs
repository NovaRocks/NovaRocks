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
use crate::ipc_recursive_batch_v2::{
    CheckedRecursiveBatch, RecursiveBatchProjectionLimits, preflight_recursive_record_batch,
};
use arrow::{
    array::{ArrayRef, Int32Array, ListArray, StringArray, StructArray},
    datatypes::Schema,
    ipc::{
        self,
        writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "observation after original primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 1 << 24,
        max_retained_buffer_bytes: 1 << 28,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 0,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 32,
        max_library_validation_bytes: 1 << 32,
    }
}
fn reader_limits() -> RecursiveReaderProjectionLimits {
    RecursiveReaderProjectionLimits {
        max_new_allocation_request_bytes: 1 << 50,
        max_coexisting_source_and_request_bytes: 1 << 50,
        max_cumulative_library_work: 1 << 55,
    }
}
fn raw_limits() -> RecursiveBatchProjectionLimits {
    RecursiveBatchProjectionLimits {
        flat: crate::ipc_flat_batch_v2::FlatBatchProjectionLimits {
            max_metadata_bytes: 1 << 20,
            max_body_bytes: 1 << 24,
            max_rows: 4096,
            max_buffer_descriptors: 16384,
            max_view_validation_bytes: 1 << 24,
        },
        max_field_nodes: 4096,
        max_total_rows: 1 << 24,
        max_geometry_request_bytes: 1 << 20,
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 65536,
        max_apparent_size: 1 << 26,
        ignore_missing_null_terminator: false,
    }
}
#[allow(deprecated)] // Actual locked public DictionaryTracker constructor.
fn encoded(array: ArrayRef, field: &Field) -> (Vec<u8>, Vec<u8>) {
    let batch =
        RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![array]).unwrap();
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encoded_batch(
            &batch,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .unwrap();
    assert!(dictionaries.is_empty());
    (encoded.ipc_message, encoded.arrow_data)
}
fn geometry<'f>(metadata: &[u8], body: &[u8], field: &'f Field) -> CheckedRecursiveBatch<'f> {
    preflight_recursive_record_batch(
        metadata,
        body,
        field,
        raw_limits(),
        &verifier(),
        &Control::default(),
    )
    .unwrap()
}
fn input<'a, 'b>(
    field: &'a Field,
    metadata: &'b [u8],
    body: &'b [u8],
    checked: &'a CheckedRecursiveBatch<'a>,
) -> ReaderInput<'a, 'b> {
    ReaderInput {
        field,
        batch: ipc::root_as_message(metadata)
            .unwrap()
            .header_as_record_batch()
            .unwrap(),
        body,
        nodes: &checked.nodes,
        geometry: checked.geometry,
        geometry_scratch_request_bytes: checked.scratch_request_bytes,
        geometry_scratch_request_count: checked.scratch_request_count,
    }
}
fn admitted(
    input: &ReaderInput<'_, '_>,
    source: usize,
    limits: RecursiveReaderProjectionLimits,
    control: &Control,
) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let ty = FunctionValueType::new(input.field.data_type().clone(), input.field.is_nullable());
    let result = preflight(input, &ty, source, policy(), limits, &mut work);
    if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn nested() -> ArrayRef {
    let child: ArrayRef = Arc::new(StructArray::new(
        vec![
            Arc::new(
                Field::new("text\n", DataType::Utf8, true).with_metadata(HashMap::from([(
                    "provider\0".into(),
                    "actual-child".into(),
                )])),
            ),
            Arc::new(Field::new("number", DataType::Int32, false)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("unused"),
                None,
                Some("selected"),
            ])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
        None,
    ));
    Arc::new(ListArray::new(
        Arc::new(Field::new("element", child.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3])),
        child,
        Some(NullBuffer::from(vec![false, true])),
    ))
}

#[test]
fn recursive_list_struct_requests_use_actual_projection_paths_and_complete_storage() {
    let array = nested();
    let field = Field::new("source", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    assert_eq!(checked.nodes.len(), 4);
    assert_eq!(
        checked
            .nodes
            .iter()
            .map(|n| n.list_map_ancestors)
            .collect::<Vec<_>>(),
        [0, 1, 1, 1]
    );
    assert_eq!(
        checked.nodes.iter().map(|n| n.rows).collect::<Vec<_>>(),
        [2, 3, 3, 3]
    );
    // The invoice is an explicit conservative fixture declaration for the
    // complete small raw/Field owners, not inferred from body length.
    let facts = admitted(&input, 1 << 20, reader_limits(), &Control::default()).unwrap();
    assert_eq!(facts.pool.array_nodes, 4);
    assert_eq!(facts.pool.rows, 2);
    assert!(facts.pool.logical_elements_upper_bound >= 11);
    assert!(facts.payload_request_bytes_upper_bound >= body.len());
    assert!(facts.structural_request_bytes_upper_bound >= checked.scratch_request_bytes);
    assert!(facts.diagnostic_request_bytes_upper_bound > field.to_string().len());
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        (1 << 20) + facts.new_allocation_request_bytes_upper_bound
    );
    assert!(
        facts.cumulative_library_work_upper_bound
            >= facts.pool.library_validation_work_upper_bound as usize
    );
}

#[test]
fn recursive_model_exact_limits_reject_before_any_reader_array_is_created() {
    let array = nested();
    let field = Field::new("source", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    let facts = admitted(&input, 1 << 20, reader_limits(), &Control::default()).unwrap();
    let exact = RecursiveReaderProjectionLimits {
        max_new_allocation_request_bytes: facts.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_library_work: facts.cumulative_library_work_upper_bound,
    };
    assert_eq!(
        admitted(&input, 1 << 20, exact, &Control::default()).unwrap(),
        facts
    );
    for kind in 0..3 {
        let mut smaller = exact;
        match kind {
            0 => smaller.max_new_allocation_request_bytes -= 1,
            1 => smaller.max_coexisting_source_and_request_bytes -= 1,
            _ => smaller.max_cumulative_library_work -= 1,
        }
        let good = Control::default();
        assert!(matches!(
            admitted(&input, 1 << 20, smaller, &good),
            Err(FlatPoolResourceError::Shape(_))
        ));
        let trace = good.trace();
        assert!(trace.len() >= 2);
        for cause in CAUSES {
            let refuse = Control::refusing(trace.len() - 1, cause.clone());
            assert!(matches!(admitted(&input, 1 << 20, smaller, &refuse),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause));
            assert_eq!(refuse.trace(), trace);
        }
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let wrong = FunctionValueType::new(DataType::Int32, true);
    assert!(matches!(
        preflight(&input, &wrong, 1 << 20, policy(), exact, &mut work),
        Err(FlatPoolResourceError::Constant(_))
    ));
    work.finish().unwrap();
}

#[test]
fn recursive_model_every_original_entry_quantum_and_tail_refusal_keeps_exact_primary() {
    let fields: Vec<_> = (0..320)
        .map(|i| {
            Arc::new(
                Field::new(format!("f{i}"), DataType::Int32, true)
                    .with_metadata(HashMap::from([("identity".into(), format!("source{i}"))])),
            )
        })
        .collect();
    let columns: Vec<ArrayRef> = (0..320)
        .map(|i| Arc::new(Int32Array::from(vec![i])) as ArrayRef)
        .collect();
    let array: ArrayRef = Arc::new(StructArray::new(fields.into(), columns, None));
    let field = Field::new("wide", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    let good = Control::default();
    admitted(&input, 1 << 20, reader_limits(), &good).unwrap();
    let trace = good.trace();
    assert_eq!(trace[0], (CompilePhase::Decode, 0));
    assert!(trace.iter().any(|(_, units)| *units == 256));
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(admitted(&input, 1 << 20, reader_limits(), &control),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause),
                "callback {at}, cause {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn recursive_diagnostic_formula_covers_real_escaped_container_strings_and_decimal_scale() {
    let array = nested();
    let field = Field::new("name\n\0", array.data_type().clone(), true)
        .with_metadata(HashMap::from([("key\n".into(), "value\0".into())]));
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let request = reader_diagnostics::preflight(&input, &mut work).unwrap();
    work.finish().unwrap();
    let original = format!(
        "{} child #{} invalid: {}",
        field.data_type(),
        usize::MAX,
        "Invalid argument error: invalid UTF-8 sequence of 4 bytes from index 18446744073709551615"
    );
    let converted = format!("Invalid argument error: {original}");
    assert!(request.request_bytes_upper_bound >= original.len() + converted.len());
    // The full i8 scale is a diagnostic input, not a new public carrier rule.
    let decimal = Field::new("coefficient", DataType::Decimal256(76, i8::MIN), true);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let decimal_request =
        crate::ipc_flat_stream_v2::reader_diagnostics::preflight_field(&decimal, &mut work)
            .unwrap();
    work.finish().unwrap();
    let coefficient = format!("{}{}", "-".to_owned() + &"9".repeat(77), "0".repeat(128));
    assert!(decimal_request.request_bytes_upper_bound >= 2 * coefficient.len());
}

#[test]
fn recursive_source_retention_and_checked_arithmetic_fail_before_hidden_work() {
    let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let field = Field::new("source", DataType::Int32, true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    let control = Control::default();
    assert!(matches!(
        admitted(&input, body.len() - 1, reader_limits(), &control),
        Err(FlatPoolResourceError::Shape(_))
    ));
    assert_eq!(control.trace().len(), 2); // entry + ordinary tail, no nested owner
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(capacity(usize::MAX).is_err());
    let control = Control::default();
    let work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert!(reader_work::metadata_comparison_work(usize::MAX, 1024).is_err());
    work.finish().unwrap();
}

fn metadata_struct(entries: usize, sparse: bool) -> (ArrayRef, Field) {
    let mut metadata = if sparse {
        let mut metadata = HashMap::with_capacity(8192);
        for index in 0..4096 {
            metadata.insert(format!("removed-{index}"), "removed".to_owned());
        }
        for index in 0..4096 {
            metadata.remove(&format!("removed-{index}"));
        }
        metadata
    } else {
        HashMap::new()
    };
    for index in 0..entries {
        // Legal maximum-length keys with a long common prefix force actual
        // exact-key chunks; neither the model nor Constant assumes this cap.
        metadata.insert(format!("{}{:04}", "k".repeat(1020), index), "value".into());
    }
    if sparse {
        assert!(metadata.capacity() > entries * 100);
    }
    let child = Arc::new(Field::new("child", DataType::Int32, true).with_metadata(metadata));
    if entries == 0 {
        assert!(child.metadata().as_arc().is_none());
    } else {
        let shared = child.metadata().clone();
        assert!(Arc::ptr_eq(
            child.metadata().as_arc().unwrap(),
            shared.as_arc().unwrap()
        ));
    }
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int32Array::from(vec![Some(7), None]))],
        None,
    ));
    let field =
        Field::new("root", array.data_type().clone(), true).with_metadata(HashMap::from([(
            "root-only".into(),
            "not-a-type-field".into(),
        )]));
    (array, field)
}

#[test]
fn recursive_metadata_bound_counts_three_type_passes_and_excludes_root_field_map() {
    // Hand-calculated candidate/table/byte upper bound for K=3, B=4096:
    // B*(1+K+2K+2) + (K+K²+K²+K+2).
    assert_eq!(
        reader_work::metadata_comparison_work(3, 4096).unwrap(),
        49_178
    );
    assert_eq!(reader_work::metadata_comparison_work(0, 4096).unwrap(), 0);
    assert!(reader_work::metadata_comparison_work(257, 4096).is_ok());
    assert!(reader_work::metadata_comparison_work(1, usize::MAX).is_err());
    let (array, field) = metadata_struct(3, false);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = reader_work::source_metadata_work(&checked.nodes, 4096, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(actual.one_comparison, 49_178);
    assert_eq!(actual.native_comparisons, 0);
    assert_eq!(
        actual.total,
        4096 * (6 * 2 + 2)
            + 4 * 2 * novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len()
            + 3 * 49_178
            + std::mem::size_of::<[usize; novarocks_type_contract::MAX_VALUE_TYPE_NODES]>()
            + 4 // Base calculation, metadata scan, index insertion, linear group item.
    );
    // The map on the root Field is still covered by the original source
    // walks/probes. It does not add a fourth metadata exact-comparison pass.
    assert_eq!(checked.nodes[0].field.metadata().len(), 1);
    let DataType::Struct(fields) = field.data_type() else {
        panic!("actual Struct fixture");
    };
    let child = Arc::clone(&fields[0]);
    let duplicate: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::clone(&child), child].into(),
        vec![
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(Int32Array::from(vec![8])),
        ],
        None,
    ));
    let repeated = Field::new("root", duplicate.data_type().clone(), true);
    let (metadata, body) = encoded(duplicate, &repeated);
    let checked = geometry(&metadata, &body, &repeated);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = reader_work::source_metadata_work(&checked.nodes, 4096, &mut work).unwrap();
    work.finish().unwrap();
    // One shared map is visited twice; independent per-name deduplication
    // would incorrectly charge it only once. Its group weight must be 24.
    assert_eq!(actual.one_comparison, 4096 * 24 + 2 * 26);
    assert_eq!(actual.native_comparisons, 0);
    let z = Arc::new(fields[0].as_ref().clone().with_name("z"));
    let aa = Arc::new(fields[0].as_ref().clone().with_name("aa"));
    let unordered: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::clone(&z), aa, z].into(),
        (0..3)
            .map(|index| Arc::new(Int32Array::from(vec![index])) as ArrayRef)
            .collect(),
        None,
    ));
    let unordered_field = Field::new("root", unordered.data_type().clone(), true);
    let (metadata, body) = encoded(unordered, &unordered_field);
    let checked = geometry(&metadata, &body, &unordered_field);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = reader_work::source_metadata_work(&checked.nodes, 4096, &mut work).unwrap();
    work.finish().unwrap();
    // Sort by actual bytes (aa before z); the two separated z occurrences
    // must form one weighted group, with all three candidate counts retained.
    assert_eq!(actual.one_comparison, 4096 * 24 + 3 * 26);
    assert_eq!(actual.native_comparisons, 0);
    let array = nested();
    let field = Field::new("root", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    assert_eq!(checked.nodes[2].depth, 3);
    assert_eq!(checked.nodes[2].list_map_ancestors, 1);
    assert_eq!(
        reader_work::native_metadata_frequency(&checked.nodes[2]).unwrap(),
        8
    ); // (4+1)*sum(1) + (2+1)*1, not a global T-squared multiplier.
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = reader_work::source_metadata_work(&checked.nodes, 4096, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(actual.one_comparison, 4096 * 6 + 6);
    assert_eq!(actual.native_comparisons, 8 * (4096 * 6 + 6));
    let child = Arc::new(Field::new("child", DataType::Int32, true));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int32Array::from(vec![7]))],
        None,
    ));
    let field = Field::new("root", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let actual = reader_work::source_metadata_work(&checked.nodes, 4096, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(actual.one_comparison, 0);
    assert_eq!(actual.native_comparisons, 0);
    assert_eq!(
        actual.total,
        4096 * 14 + 8 * novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len() + 2
    ); // Base and one length scan, without fixed-stack initialization work.
    assert_eq!(
        control.trace(),
        [(CompilePhase::Decode, 0), (CompilePhase::Decode, 2)]
    );
}

#[test]
fn recursive_converted_metadata_invoice_and_k_work_refuse_before_constant_preflight() {
    let (array, field) = metadata_struct(20, true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    // The staging HashMap has exited. These are two explicit source invoice
    // estimates for the same attached BTree/Arc/text and Field/FVT owners;
    // their difference exercises the conservative envelope, not spare buckets.
    let sparse_invoice = 1 << 20;
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let sparse =
        reader_work::source_metadata_work(&checked.nodes, sparse_invoice, &mut work).unwrap();
    let compact = reader_work::source_metadata_work(&checked.nodes, 1 << 16, &mut work).unwrap();
    work.finish().unwrap();
    assert!(sparse.one_comparison > compact.one_comparison);
    let facts = admitted(&input, sparse_invoice, reader_limits(), &Control::default()).unwrap();
    assert!(facts.cumulative_library_work_upper_bound > sparse.total);
    let mut limits = reader_limits();
    limits.max_cumulative_library_work = sparse.total - 1;
    let control = Control::default();
    assert!(matches!(
        admitted(&input, sparse_invoice, limits, &control),
        Err(FlatPoolResourceError::Shape(
            crate::physical_type_v2::TypeCodecError::InvalidShape(
                "recursive reader source metadata work envelope exceeded"
            )
        ))
    ));
    // Compare with the actual source-only owner trace, including its scratch
    // boundaries and tail. Numerical Constant was not entered after this gate.
    let source_only = Control::default();
    let mut work = CompileCheckpoints::try_new(&source_only, CompilePhase::Decode).unwrap();
    reader_work::source_metadata_work(&checked.nodes, sparse_invoice, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(control.trace(), source_only.trace());
    assert!(source_only.trace().last().unwrap().1 > 0);
    for at in 0..control.trace().len() {
        for cause in CAUSES {
            let refuse = Control::refusing(at, cause.clone());
            assert!(matches!(admitted(&input, sparse_invoice, limits, &refuse),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause));
            assert_eq!(refuse.trace(), control.trace()[..=at]);
        }
    }
}

#[test]
fn recursive_actual_long_source_names_preserve_every_original_control_prefix() {
    // Source maps are opaque bounded library work. The actual Rust walk
    // compares source names, so give its real sort long common-prefix names.
    let fields = (0..20)
        .map(|index| {
            Arc::new(
                Field::new(
                    format!("{}{:04}", "n".repeat(1020), index),
                    DataType::Int32,
                    true,
                )
                .with_metadata(HashMap::from([(
                    "source.attribute".into(),
                    "retained".into(),
                )])),
            )
        })
        .collect::<Vec<_>>();
    let arrays = (0..20)
        .map(|_| Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef)
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(StructArray::new(fields.into(), arrays, None));
    let field = Field::new("root", array.data_type().clone(), true);
    let (metadata, body) = encoded(array, &field);
    let checked = geometry(&metadata, &body, &field);
    let input = input(&field, &metadata, &body, &checked);
    let good = Control::default();
    admitted(&input, 1 << 20, reader_limits(), &good).unwrap();
    let trace = good.trace();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let refuse = Control::refusing(at, cause.clone());
            assert!(
                matches!(admitted(&input, 1 << 20, reader_limits(), &refuse),
                Err(FlatPoolResourceError::Control(actual)) if actual == cause),
                "callback {at}, cause {cause:?}"
            );
            assert_eq!(refuse.trace(), trace[..=at]);
        }
    }
}

#[test]
fn recursive_request_count_includes_body_repair_and_empty_offset_payload_requests() {
    // Empty List has an unused one-row Int32 child. The original reader still
    // constructs that child; its unaligned descriptor needs a repair. The
    // absent empty List offsets need the reader's width-four zero offset.
    let field = Field::new(
        "source",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    );
    let body = [0u8, 1, 2, 3, 4, 0, 0, 0];
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[ipc::FieldNode::new(0, 0), ipc::FieldNode::new(1, 0)]);
    let buffers = builder.create_vector(&[
        ipc::Buffer::new(0, 0),
        ipc::Buffer::new(0, 0),
        ipc::Buffer::new(0, 0),
        ipc::Buffer::new(1, 4),
    ]);
    let batch = ipc::RecordBatch::create(
        &mut builder,
        &ipc::RecordBatchArgs {
            length: 0,
            nodes: Some(nodes),
            buffers: Some(buffers),
            ..Default::default()
        },
    );
    let message = ipc::Message::create(
        &mut builder,
        &ipc::MessageArgs {
            version: ipc::MetadataVersion::V5,
            header_type: ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: 8,
            ..Default::default()
        },
    );
    builder.finish(message, None);
    let metadata = builder.finished_data();
    let checked = geometry(metadata, &body, &field);
    let input = input(&field, metadata, &body, &checked);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let payload = payload(&input, &mut work).unwrap();
    assert_eq!(payload.body_capacity, 64);
    assert_eq!(payload.repair_count, 1);
    assert_eq!(payload.repair_capacity, 64);
    assert_eq!(payload.empty_offsets_count, 1);
    assert_eq!(payload.empty_offsets_capacity, 4);
    let structures = reader_allocations::preflight(&input, &payload, &mut work).unwrap();
    let diagnostic = reader_diagnostics::preflight(&input, &mut work).unwrap();
    work.finish().unwrap();
    let facts = admitted(&input, 1 << 20, reader_limits(), &Control::default()).unwrap();
    assert_eq!(facts.payload_request_bytes_upper_bound, 64 + 64 + 4);
    assert_eq!(
        facts.allocation_request_count_upper_bound,
        structures.allocation_requests_upper_bound
            + diagnostic.allocation_requests_upper_bound
            + checked.scratch_request_count
            + 3
    );
}
