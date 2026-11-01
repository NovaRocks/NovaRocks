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
//! Actual source materialization and the unchanged Chunk schema consumer.
//! These tests do not claim a host grant or real allocator-tail observation.
use super::*;
use arrow::array::{ArrayRef, Int64Array, StructArray};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MaterializedFieldNamespace, MaterializedMetadataMap, OriginalFieldMaterialization,
    TypedSchemaMaterializations, materialize_value_field,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Control {
    calls: AtomicUsize,
    reject: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let ordinal = self.calls.fetch_add(1, Ordering::Relaxed);
        match self.reject.clone() {
            Some((at, cause)) if at == ordinal => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source() -> (SchemaMetadataMaterializations, Arc<Field>) {
    let mut metadata = MaterializedMetadataMap::with_capacity(17);
    metadata.insert("PARQUET:field_id".to_owned(), "41".to_owned());
    metadata.insert("original".to_owned(), "unchanged".to_owned());
    let child = metadata
        .into_field(Field::new("k", DataType::Int64, false))
        .into_shared();
    let child_ref = child.field().clone();
    let namespace = MaterializedFieldNamespace::from_original_loans(Arc::from([child.loan()]));
    // Provider metadata belongs to its actual source Field, independently of
    // the normalized execution value layout used by FunctionValueType.
    let root = MaterializedMetadataMap::new_for_original_reserve().into_field(Field::new(
        "payload",
        DataType::Struct(vec![child_ref.clone()].into()),
        false,
    ));
    (
        TypedSchemaMaterializations::new(vec![root], namespace).into_original_schema(),
        child_ref,
    )
}
fn layout(source: SchemaMetadataMaterializations, control: &Control) -> StaticLayout {
    StaticLayout::try_new_materialized_for_compile(source, Arc::from([SlotId::new(7)]), control)
        .unwrap()
}

#[test]
fn by_schema_origin_exact_pointer_and_foreign_equal_field_are_distinct() {
    let (source, child) = source();
    assert!(source.field_origin(&child).is_some());
    let foreign = Arc::new(child.as_ref().clone());
    assert_eq!(foreign, child);
    assert!(source.field_origin(&foreign).is_none());
    assert!(matches!(
        OriginalFieldMaterialization::clone_from_source(&foreign, Some(&source)),
        OriginalFieldMaterialization::Plain(_)
    ));
    let origin = source
        .field_origin(&child)
        .unwrap()
        .table_request()
        .unwrap();
    assert!(origin.buckets >= 17);
    assert!(origin.request_bytes_upper_bound > 0);
}

#[test]
fn by_schema_origin_chunk_projection_preserves_original_values_and_clears_root_metadata() {
    let (source, child) = source();
    let control = Control::default();
    let original = source.schema_owner().schema().clone();
    let bound = layout(source, &control);
    let chunk = ChunkSchema::from_compiled_layout(&bound).unwrap();
    assert_eq!(chunk.field(0), Some(original.field(0)));
    assert_eq!(chunk.arrow_schema.metadata(), original.metadata());
    assert!(
        chunk
            .metadata_materializations()
            .unwrap()
            .field_origin(&child)
            .is_some()
    );
    let projected = chunk.project_by_slots(&[SlotId::new(7)]).unwrap();
    assert_eq!(projected.field(0), chunk.field(0));
    assert!(projected.arrow_schema.metadata().is_empty());
    assert!(
        projected
            .metadata_materializations()
            .unwrap()
            .field_origin(&child)
            .is_some()
    );
}

#[test]
fn by_schema_origin_nullable_reconcile_uses_original_nested_author_and_escaped_schema() {
    let (source, child) = source();
    let bound = layout(source, &Control::default());
    let expected = ChunkSchema::from_compiled_layout(&bound).unwrap();
    let actual_child = Arc::new(Field::new("k", DataType::Int64, true));
    let values = Arc::new(Int64Array::from(vec![None, Some(9)])) as ArrayRef;
    let array = Arc::new(StructArray::from(vec![(actual_child.clone(), values)])) as ArrayRef;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![actual_child].into()),
            false,
        )])),
        vec![array],
    )
    .unwrap();
    let actual = align_chunk_schema_to_batch(&batch, &expected).unwrap();
    let escaped = actual.arrow_schema_ref();
    let DataType::Struct(fields) = escaped.field(0).data_type() else {
        panic!("original Struct");
    };
    assert!(fields[0].is_nullable());
    assert_eq!(fields[0].metadata(), child.metadata());
    assert!(
        actual
            .metadata_materializations()
            .unwrap()
            .field_origin(&fields[0])
            .is_some()
    );
    drop(actual);
    drop(expected);
    drop(bound);
    assert_eq!(escaped.field(0).name(), "payload");
    assert_eq!(
        fields[0]
            .metadata()
            .get("PARQUET:field_id")
            .map(String::as_str),
        Some("41")
    );
}

#[test]
fn by_schema_origin_multi_input_concat_retains_each_actual_child_source() {
    let (first, first_child) = source();
    let (second, second_child) = source();
    assert!(!Arc::ptr_eq(&first_child, &second_child));
    let first = ChunkSchema::from_compiled_layout(&layout(first, &Control::default())).unwrap();
    let second_layout = StaticLayout::try_new_materialized_for_compile(
        second,
        Arc::from([SlotId::new(8)]),
        &Control::default(),
    )
    .unwrap();
    let second = ChunkSchema::from_compiled_layout(&second_layout).unwrap();
    let result = ChunkSchema::concat(&[first, second]).unwrap();
    assert_eq!(result.slot_ids(), &[SlotId::new(7), SlotId::new(8)]);
    assert!(result.arrow_schema.metadata().is_empty());
    let origins = result.metadata_materializations().unwrap();
    assert!(origins.field_origin(&first_child).is_some());
    assert!(origins.field_origin(&second_child).is_some());
}

#[test]
fn by_schema_origin_unknown_deleted_table_never_becomes_fresh_from_capacity() {
    let mut map = HashMap::with_capacity(4096);
    map.insert("removed".to_owned(), "value".to_owned());
    map.remove("removed");
    assert!(map.is_empty());
    let field = Arc::new(Field::new("foreign", DataType::Int64, false).with_metadata(map));
    let (source, _) = source();
    assert!(source.field_origin(&field).is_none());
    let projected = source.clone_field_original(&field);
    assert!(matches!(projected, novarocks_type_contract::owned_resources::metadata_materialization::ProjectedMaterializedField::Foreign(_)));
}

#[test]
fn by_schema_origin_old_none_diagnostics_and_semantic_identity_are_unchanged() {
    let (source, _) = source();
    let schema = source.schema_owner().schema().clone();
    let slots: Arc<[SlotId]> = Arc::from([SlotId::new(7)]);
    let plain =
        StaticLayout::try_new_for_compile(schema.clone(), slots.clone(), &Control::default())
            .unwrap();
    let sourced =
        StaticLayout::try_new_materialized_for_compile(source, slots, &Control::default()).unwrap();
    assert_eq!(plain.identity().unwrap(), sourced.identity().unwrap());
    assert_eq!(format!("{plain:?}"), format!("{sourced:?}"));
    let plain = ChunkSchema::from_compiled_layout(&plain).unwrap();
    let sourced = ChunkSchema::from_compiled_layout(&sourced).unwrap();
    assert_eq!(plain, sourced);
    assert_eq!(format!("{plain:?}"), format!("{sourced:?}"));
    let old = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
        SlotId::new(7),
        schema.field(0).clone(),
        None,
        None,
    )])
    .unwrap();
    assert!(old.metadata_materializations().is_none());
}

#[test]
fn by_schema_origin_compilation_preserves_first_typed_control_without_footer() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let (source, _) = source();
        let control = Control {
            calls: AtomicUsize::new(0),
            reject: Some((0, cause.clone())),
        };
        let error = StaticLayout::try_new_materialized_for_compile(
            source,
            Arc::from([SlotId::new(7)]),
            &control,
        )
        .unwrap_err();
        assert!(
            matches!(error, novarocks_local_program::LayoutCompileError::Control(actual) if actual == cause)
        );
        assert_eq!(control.calls.load(Ordering::Relaxed), 1);
    }
}

#[derive(Debug, PartialEq)]
enum CloneProbeError {
    Metadata(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError),
    Refused,
}
impl From<novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError> for CloneProbeError {
    fn from(error: novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError) -> Self {
        Self::Metadata(error)
    }
}

#[test]
fn by_schema_origin_clone_invoice_shares_original_metadata_without_new_backing() {
    let mut metadata = MaterializedMetadataMap::with_capacity(64);
    let mut key = String::with_capacity(4096);
    key.push_str("key");
    let mut value = String::with_capacity(8192);
    value.push_str("value");
    let original_capacities = (key.capacity(), value.capacity());
    metadata.insert(key, value);
    let owner = metadata
        .into_field(Field::new("k", DataType::Int64, false))
        .into_shared();
    let loan = owner.loan();
    let mut calls = 0;
    let mut observe = || -> Result<(), CloneProbeError> {
        calls += 1;
        Ok(())
    };
    let request = loan
        .original_metadata_clone_request(owner.field(), &mut observe)
        .unwrap()
        .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(request.text_requests, 0);
    assert_eq!(request.text_bytes, 0);
    assert_eq!(request.table_backing, None);
    assert_eq!(request.request_bytes().unwrap(), 0);
    // Original staging facts remain provenance; they do not become a new
    // Metadata::clone request after Arrow consumes the temporary HashMap.
    let original = owner.metadata_origin().table_request().unwrap();
    assert!(original.layout.is_some());
    let copied = owner.clone_original();
    assert!(Arc::ptr_eq(
        owner.field().metadata().as_arc().unwrap(),
        copied.field().metadata().as_arc().unwrap(),
    ));
    let (key, value) = copied.field().metadata().iter().next().unwrap();
    assert_eq!((key.capacity(), value.capacity()), original_capacities);
    let foreign = Arc::new(owner.field().as_ref().clone());
    assert!(
        loan.original_metadata_clone_request(&foreign, &mut || -> Result<(), CloneProbeError> {
            panic!("foreign metadata must remain uninspected")
        })
        .is_none()
    );
}

#[test]
fn by_schema_origin_clone_invoice_has_no_empty_backing_and_keeps_first_borrow_refusal() {
    let empty = MaterializedMetadataMap::with_capacity(64).into_field(Field::new(
        "empty",
        DataType::Int64,
        false,
    ));
    let mut empty_calls = 0;
    let request = empty
        .original_metadata_clone_request(&mut || -> Result<(), CloneProbeError> {
            empty_calls += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(empty_calls, 1);
    assert!(empty.field().metadata().as_arc().is_none());
    assert_eq!(request.table_backing, None);
    assert_eq!(request.request_bytes().unwrap(), 0);
    assert_eq!(request.text_bytes, 0);
    assert_eq!(request.text_requests, 0);
    let mut metadata = MaterializedMetadataMap::with_capacity(2);
    metadata.insert("a".into(), "1".into());
    metadata.insert("b".into(), "2".into());
    let field = metadata.into_field(Field::new("k", DataType::Int64, false));
    let mut calls = 0;
    let result = field.original_metadata_clone_request(&mut || -> Result<(), CloneProbeError> {
        calls += 1;
        Err(CloneProbeError::Refused)
    });
    assert_eq!(result, Err(CloneProbeError::Refused));
    assert_eq!(calls, 1);
}

impl From<novarocks_type_contract::ValueTypeError> for CloneProbeError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Metadata(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError::ValueType(error))
    }
}

#[test]
fn by_schema_origin_field_clone_requests_owned_dictionary_boxes_but_shares_nested_field_arcs() {
    // The Dictionary under Struct is shared through Fields::clone. The outer
    // Dictionary recursively clones only its two owned Box<DataType> edges.
    let shared_child = Arc::new(Field::new(
        "nested",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    ));
    let dtype = DataType::Dictionary(
        Box::new(DataType::Int64),
        Box::new(DataType::Struct(vec![shared_child.clone()].into())),
    );
    let mut map = MaterializedMetadataMap::with_capacity(1);
    map.insert("original".into(), "retained".into());
    let field = map.into_field(Field::new("root", dtype, true));
    let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    let mut calls = 0;
    let request = field
        .original_field_clone_request(&mut scratch, &mut || -> Result<(), CloneProbeError> {
            calls += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(request.name_bytes, 4);
    assert_eq!(request.dictionary_box_requests, 2);
    assert_eq!(request.dictionary_box_bytes, 2 * size_of::<DataType>());
    assert_eq!(
        request.request_bytes,
        request.name_bytes
            + request.metadata.request_bytes().unwrap()
            + request.dictionary_box_bytes
    );
    assert!(calls > 1);
    let actual = field.clone_original();
    let DataType::Dictionary(_, value) = actual.field().data_type() else {
        panic!("original Dictionary clone");
    };
    let DataType::Struct(fields) = value.as_ref() else {
        panic!("original Struct clone");
    };
    assert!(Arc::ptr_eq(&fields[0], &shared_child));
    assert_eq!(actual.field(), field.field());
}

#[test]
fn by_schema_origin_publication_receipt_precedes_all_new_sidecar_work() {
    use novarocks_type_contract::owned_resources::metadata_materialization::{
        MetadataMaterializationError, MetadataSidecarRequest,
    };
    let namespace = MaterializedFieldNamespace::from_original_loans(Arc::from([]));
    let root =
        materialize_value_field(&FunctionValueType::new(DataType::Int64, false), "k").unwrap();
    let typed = TypedSchemaMaterializations::new(vec![root], namespace);
    let request = typed.original_publication_request().unwrap();
    assert_eq!(request.inherited_loans, 0);
    assert_eq!(request.original_roots, 1);
    assert_eq!(request.loans_upper_bound, 1);
    assert_eq!(
        request.coexistence_request_bytes,
        request.temporary_requests_bytes + request.result_request_bytes
    );
    let mut calls = 0;
    let result = typed.into_original_schema_in(
        &mut |actual: &MetadataSidecarRequest| -> Result<(), CloneProbeError> {
            calls += 1;
            assert_eq!(*actual, request);
            Err(CloneProbeError::Refused)
        },
    );
    assert!(matches!(result, Err(CloneProbeError::Refused)));
    assert_eq!(calls, 1);
    assert_eq!(
        MetadataSidecarRequest::original_publication(usize::MAX, 1),
        Err(MetadataMaterializationError::Arithmetic)
    );
}

#[test]
fn by_schema_origin_inherited_owned_nullable_keeps_exact_original_occurrence() {
    let (source, child) = source();
    let original = OriginalFieldMaterialization::clone_from_source(&child, Some(&source));
    let origin = original.metadata_origin().unwrap();
    let inherited = original
        .with_nullable_owned(true)
        .with_name_owned("published");
    assert_eq!(inherited.field().name(), "published");
    assert!(inherited.field().is_nullable());
    assert_eq!(inherited.metadata_origin(), Some(origin));
    assert_eq!(inherited.field().metadata(), child.metadata());
    let schema = TypedSchemaMaterializations::from_original_fields(
        vec![inherited],
        source.field_namespace(),
    )
    .into_original_schema();
    let actual = &schema.schema_owner().schema().fields()[0];
    assert_eq!(schema.field_origin(actual), Some(origin));
    assert!(schema.field_origin(&child).is_some());
    assert!(
        schema
            .field_origin(&Arc::new(actual.as_ref().clone()))
            .is_none()
    );
}

#[derive(Debug, PartialEq)]
enum KernelCloneProbeError {
    Source(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError),
    Kernel(novarocks_functions::KernelFailure),
    Resource(novarocks_type_contract::ControlResourceError),
}
impl From<novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError> for KernelCloneProbeError {
    fn from(error: novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError) -> Self { Self::Source(error) }
}
impl From<novarocks_type_contract::ValueTypeError> for KernelCloneProbeError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError::ValueType(error))
    }
}
#[test]
fn by_schema_origin_borrowed_namespace_preserves_seven_first_causes_without_footer() {
    use novarocks_functions::{KernelDiagnostic, KernelFailure};
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("source")),
        KernelFailure::Internal(KernelDiagnostic::new("source")),
        KernelFailure::Operational(KernelDiagnostic::new("source")),
        KernelFailure::InstanceFailed,
    ] {
        let (source, child) = source();
        let foreign = Arc::new(child.as_ref().clone());
        let mut calls = 0;
        let result =
            source.field_loan_observed(&foreign, &mut || -> Result<(), KernelCloneProbeError> {
                calls += 1;
                Err(KernelCloneProbeError::Kernel(cause.clone()))
            });
        assert!(
            matches!(result, Err(KernelCloneProbeError::Kernel(ref actual)) if actual == &cause)
        );
        assert_eq!(calls, 1);
        // The lookup never clones/elects an equal foreign metadata source.
        assert!(source.field_origin(&foreign).is_none());
    }
}

impl From<novarocks_type_contract::ControlResourceError> for KernelCloneProbeError {
    fn from(error: novarocks_type_contract::ControlResourceError) -> Self {
        Self::Resource(error)
    }
}

#[test]
fn by_schema_origin_semantic_clone_shares_immutable_storage_without_requests() {
    use novarocks_type_contract::{ValueKind, ValueTypeRef, ValueTypeStorageLifetime};
    let item =
        ValueTypeRef::try_new(ValueKind::String, true, ValueTypeStorageLifetime::pure()).unwrap();
    let tree = ValueTypeRef::try_new(
        ValueKind::List(item),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let value = ChunkFieldSchema::from_value_type(tree);
    let mut visits = 0;
    let facts = value
        .original_clone_request_observed::<CloneProbeError>(&mut || {
            visits += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(visits, 1);
    assert_eq!(facts.allocation_requests, 0);
    assert_eq!(facts.request_bytes, 0);
    assert!(
        value
            .value_type()
            .shares_storage(value.clone().value_type())
    );
    let mut calls = 0;
    let refused = value.original_clone_request_observed::<CloneProbeError>(&mut || {
        calls += 1;
        Err(CloneProbeError::Refused)
    });
    assert_eq!(refused, Err(CloneProbeError::Refused));
    assert_eq!(calls, 1);
}

#[test]
fn by_schema_origin_reconcile_invoice_uses_each_actual_root_nested_and_sidecar_source() {
    let (source, child) = source();
    let bound = layout(source, &Control::default());
    let contract = ChunkSchema::from_compiled_layout(&bound).unwrap();
    let mut traversal = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    let mut owned = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    let facts = contract
        .original_reconcile_metadata_request_observed::<KernelCloneProbeError>(
            &mut traversal,
            &mut owned,
            &mut || Ok(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(facts.nested_field_occurrences, 1);
    assert!(facts.field_clone_requests_bytes > child.name().len() * 3);
    assert!(facts.field_arc_requests_bytes > 0);
    assert!(facts.struct_publication_requests_bytes > 0);
    assert!(facts.sidecar_requests_bytes > 0);
    assert_eq!(
        facts.cumulative_requests_bytes,
        facts.field_clone_requests_bytes
            + facts.field_arc_requests_bytes
            + facts.semantic_clone_requests_bytes
            + facts.struct_publication_requests_bytes
            + facts.descriptor_requests_bytes
            + facts.sidecar_requests_bytes
    );
    let actual_child = Arc::new(child.as_ref().clone().with_nullable(true));
    let columns = vec![Arc::new(StructArray::new(
        vec![actual_child].into(),
        vec![Arc::new(Int64Array::from(vec![None, Some(2)]))],
        None,
    )) as ArrayRef];
    let reconciled = align_chunk_schema_to_columns(&columns, &contract).unwrap();
    assert!(
        reconciled
            .metadata_materializations()
            .unwrap()
            .field_origin(&child)
            .is_some()
    );
    assert!(
        reconciled.arrow_schema_ref().field(0).data_type()
            != contract.field(0).unwrap().data_type()
    );
    assert_eq!(columns[0].len(), 2);
    let foreign = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
        SlotId::new(1),
        Field::new("k", DataType::Int64, false),
        None,
        None,
    )])
    .unwrap();
    let mut called = false;
    assert!(
        foreign
            .original_reconcile_metadata_request_observed::<KernelCloneProbeError>(
                &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                &mut || {
                    called = true;
                    Ok(())
                },
            )
            .unwrap()
            .is_none()
    );
    assert!(!called);
}

#[test]
fn by_schema_origin_reconcile_invoice_every_callback_preserves_seven_causes() {
    use novarocks_functions::{KernelDiagnostic, KernelFailure};
    let (source, _) = source();
    let bound = layout(source, &Control::default());
    let contract = ChunkSchema::from_compiled_layout(&bound).unwrap();
    let mut count = 0;
    contract
        .original_reconcile_metadata_request_observed::<KernelCloneProbeError>(
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
            &mut || {
                count += 1;
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
    assert!(count > 1);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
        KernelFailure::InstanceFailed,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
    ] {
        for at in 0..count {
            let mut calls = 0;
            let result = contract
                .original_reconcile_metadata_request_observed::<KernelCloneProbeError>(
                    &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                    &mut [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES],
                    &mut || {
                        let ordinal = calls;
                        calls += 1;
                        if ordinal == at {
                            Err(KernelCloneProbeError::Kernel(cause.clone()))
                        } else {
                            Ok(())
                        }
                    },
                );
            assert_eq!(result, Err(KernelCloneProbeError::Kernel(cause.clone())));
            assert_eq!(calls, at + 1);
        }
    }
}

#[path = "metadata_allocation_tests.rs"]
mod metadata_allocation_tests;

#[test]
fn m07_dual_origin_projection_keeps_actual_field_arc_and_new_root_receipt() {
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnerLimits,
    };
    let control = Control::default();
    let field = ArrowMetadataOwner::try_new(
        vec![("m07.field".into(), "original".into())],
        MetadataOwnerLimits {
            entries: 1,
            construction_bytes: 4096,
        },
    )
    .unwrap()
    .into_field("payload".into(), DataType::Utf8, true);
    let actual = Arc::clone(field.field());
    let mut root = MaterializedMetadataMap::with_capacity(3);
    root.insert("uea.schema".into(), "original".into());
    let source = SchemaMetadataMaterializations::from_materialized_owners(
        root.into_schema(vec![Arc::clone(&actual)]).into_shared(),
        Arc::from([]),
    );
    let original_schema = Arc::clone(source.schema_owner().schema());
    let bound = layout(source, &control)
        .with_metadata_origins(FieldMetadataOrigins::try_new(vec![field], 1).unwrap(), None)
        .unwrap();
    let projected = bound
        .project_by_slots_for_compile(&[SlotId::new(7)], &control)
        .unwrap();
    assert!(Arc::ptr_eq(&projected.schema().fields()[0], &actual));
    assert!(!Arc::ptr_eq(projected.schema(), &original_schema));
    let materialized = projected.metadata_materializations().unwrap();
    assert!(materialized.schema_owner().lends(projected.schema()));
    assert_eq!(projected.schema().metadata(), original_schema.metadata());
    assert!(
        projected
            .field_metadata_origins()
            .unwrap()
            .metadata_bytes_for(&actual)
            .is_some()
    );
    assert!(projected.schema_metadata_origin().is_none());
    let chunk = ChunkSchema::from_compiled_layout(&projected).unwrap();
    assert!(Arc::ptr_eq(&chunk.arrow_schema_ref().fields()[0], &actual));
    assert_eq!(
        chunk.arrow_schema_ref().metadata(),
        projected.schema().metadata()
    );
    assert!(chunk.metadata_materializations().is_some());
    assert!(
        chunk
            .field_metadata_origins()
            .unwrap()
            .metadata_bytes_for(&actual)
            .is_some()
    );
}

#[test]
fn m07_shared_field_projection_retains_original_uea_namespace_without_cloning_field() {
    let (source, child) = source();
    let root = Arc::clone(&source.schema_owner().schema().fields()[0]);
    let projected = source.project_shared_fields_original(vec![Arc::clone(&root)]);
    assert!(Arc::ptr_eq(
        &projected.schema_owner().schema().fields()[0],
        &root
    ));
    assert!(projected.field_origin(&child).is_some());
    let foreign = Arc::new(child.as_ref().clone());
    assert!(projected.field_origin(&foreign).is_none());
    assert!(
        projected
            .schema_owner()
            .lends(projected.schema_owner().schema())
    );
    assert!(!Arc::ptr_eq(
        projected.schema_owner().schema(),
        source.schema_owner().schema()
    ));
}

#[test]
fn project_original_metadata_materialization_survives_actual_output_and_nullable_rebuild() {
    use crate::exec::chunk::{RootArrayStorageLimits, borrowed_root_chunk_storage};
    use crate::exec::operators::materialize_project_output;
    use arrow::buffer::NullBuffer;

    let (original, child) = source();
    let mut map = MaterializedMetadataMap::with_capacity(17);
    map.insert("project.original".into(), "kept".into());
    let source = SchemaMetadataMaterializations::from_materialized_owners(
        map.into_schema(original.schema_owner().schema().fields().clone())
            .into_shared(),
        Arc::from(original.fields()),
    );
    let declared = ChunkSchema::from_compiled_layout(&layout(source, &Control::default())).unwrap();
    let limits = RootArrayStorageLimits {
        bytes: 96 << 20,
        nodes: 8192,
        depth: 64,
    };
    for nulls in [None, Some(NullBuffer::from(vec![true, false]))] {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
        let column: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::clone(&child)].into(),
            vec![values],
            nulls,
        ));
        let output = materialize_project_output(vec![column], &declared, false).unwrap();
        let schema = output.chunk_schema();
        // Chunk alignment preserves the existing empty root metadata contract.
        assert!(schema.arrow_schema_ref().metadata().is_empty());
        assert_eq!(
            declared
                .arrow_schema_ref()
                .metadata()
                .get("project.original"),
            Some(&"kept".to_string())
        );
        let actual = schema.metadata_materializations().unwrap();
        assert!(actual.schema_owner().lends(&schema.arrow_schema_ref()));
        assert!(actual.field_origin(&child).is_some());
        assert!(
            actual
                .field_origin(&schema.arrow_schema_ref().fields()[0])
                .is_some()
        );
        assert_eq!(
            schema.field(0).unwrap().is_nullable(),
            output.record_batch().column(0).null_count() > 0
        );
        assert!(borrowed_root_chunk_storage(&output, limits).is_ok());
    }
}

#[test]
fn project_foreign_metadata_is_not_promoted_by_equal_original_field_values() {
    use crate::exec::chunk::{
        RootArrayStorageError, RootArrayStorageLimits, borrowed_root_chunk_storage,
    };
    use crate::exec::operators::materialize_project_output;

    let (source, child) = source();
    let original = ChunkSchema::from_compiled_layout(&layout(source, &Control::default())).unwrap();
    let field = Arc::new(original.field(0).unwrap().clone());
    let foreign = ChunkSchema::try_new_with_schema_metadata(
        vec![ChunkSlotSchema::try_new_with_field_ref(SlotId::new(7), field, None, None).unwrap()],
        original.arrow_schema_ref().metadata().clone(),
    )
    .unwrap();
    assert_eq!(foreign.arrow_schema_ref(), original.arrow_schema_ref());
    assert!(foreign.metadata_materializations().is_none());
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    let column: ArrayRef = Arc::new(StructArray::new(vec![child].into(), vec![values], None));
    let output = materialize_project_output(vec![column], &foreign, false).unwrap();
    assert!(output.chunk_schema().metadata_materializations().is_none());
    assert_eq!(
        borrowed_root_chunk_storage(
            &output,
            RootArrayStorageLimits {
                bytes: 96 << 20,
                nodes: 8192,
                depth: 64
            }
        ),
        Err(RootArrayStorageError::UnknownMetadataOwner),
    );
}
