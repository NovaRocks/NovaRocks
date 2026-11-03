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

//! Extract paths from the Connector-owned, normalized engine Variant carrier.
//! Parquet source structs belong to the Connector's sole conversion kernel.

use crate::exec::variant_failure::{self, SdkStage};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
use arrow::array::LargeBinaryBuilder;
use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, LargeBinaryArray, RecordBatch, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field};
use parquet::variant::{GetOptions, VariantArray, variant_get as kernel_variant_get};

use novarocks_types::SlotId;
#[cfg(test)]
use novarocks_types::value::variant::is_variant_struct_data_type;
use novarocks_types::value::variant::{
    VariantMetadata, VariantPathSegment, parse_variant_path, split_serialized,
};

/// Immutable extraction facts. A clone shares the finite spec creation owner;
/// it neither copies names/fields nor retains a decode context or controller.
#[derive(Clone, Debug)]
pub struct VariantPathSpec(Arc<VariantPathSpecBacking>);

#[derive(Debug)]
struct VariantPathSpecBacking {
    parts: VariantPathSpecParts,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

/// Consume-only input to the sole spec holder author. Backing strings and
/// fields have already been observed by their actual creator before publication.
#[derive(Debug)]
pub struct VariantPathSpecParts {
    pub source_slot_id: SlotId,
    pub source_read_slot_id: SlotId,
    pub output_slot_id: SlotId,
    pub source_field_id: Option<i32>,
    pub source_field: Field,
    pub output_field: Field,
    pub canonical_path: String,
    pub strict: bool,
    // None is restricted to the explicit legacy expansion author. Strong native
    // preparation supplies the complete, independently held original bindings.
    pub source_binding: Option<novarocks_type_contract::FunctionValueType>,
    pub output_binding: Option<novarocks_type_contract::FunctionValueType>,
}
impl VariantPathSpec {
    pub fn storage_allocation_layout() -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            VariantPathSpecBacking,
        >())
    }
    pub fn try_from_parts_observed<
        E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
    >(
        parts: VariantPathSpecParts,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        request: &mut impl FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        request(Self::storage_allocation_layout()?)?;
        Ok(Self::publish(parts, storage))
    }
    pub fn from_pure_parts(parts: VariantPathSpecParts) -> Self {
        Self::publish(
            parts,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
    }
    fn publish(
        parts: VariantPathSpecParts,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        Self(Arc::new(VariantPathSpecBacking {
            parts,
            _storage: storage,
        }))
    }
    pub fn clone_observed<E>(
        &self,
        work: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        work(1)?;
        Ok(self.clone())
    }
    pub fn source_slot_id(&self) -> SlotId {
        self.0.parts.source_slot_id
    }
    pub fn source_read_slot_id(&self) -> SlotId {
        self.0.parts.source_read_slot_id
    }
    pub fn output_slot_id(&self) -> SlotId {
        self.0.parts.output_slot_id
    }
    pub fn source_field_id(&self) -> Option<i32> {
        self.0.parts.source_field_id
    }
    pub fn source_name(&self) -> &str {
        self.0.parts.source_field.name()
    }
    pub fn output_name(&self) -> &str {
        self.0.parts.output_field.name()
    }
    pub fn source_field(&self) -> &Field {
        &self.0.parts.source_field
    }
    pub fn output_field(&self) -> &Field {
        &self.0.parts.output_field
    }
    pub fn canonical_path(&self) -> &str {
        &self.0.parts.canonical_path
    }
    pub fn requested_type(&self) -> &DataType {
        self.0.parts.output_field.data_type()
    }
    pub fn strict(&self) -> bool {
        self.0.parts.strict
    }
    pub fn source_binding(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.0.parts.source_binding.as_ref()
    }
    pub fn output_binding(&self) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.0.parts.output_binding.as_ref()
    }
}

enum KernelParsedPath {
    Pure(novarocks_types::value::variant::VariantPath),
    Observed(novarocks_types::value::variant::ObservedVariantPath),
}
impl KernelParsedPath {
    fn segments(&self) -> &[VariantPathSegment] {
        match self {
            Self::Pure(path) => &path.segments,
            Self::Observed(path) => path.segments(),
        }
    }
}
fn kernel_path<'a>(
    segments: &'a [VariantPathSegment],
    owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
) -> Result<parquet::variant::VariantPath<'a>, VariantMaterializationFailure> {
    if let Some(owner) = owner
        && !segments.is_empty()
    {
        owner.request(
            std::alloc::Layout::array::<parquet::variant::VariantPathElement<'a>>(segments.len())
                .map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?,
        )?;
    }
    let mut elems = Vec::with_capacity(segments.len());
    for seg in segments {
        if let Some(owner) = owner {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
        }
        elems.push(match seg {
            VariantPathSegment::ObjectKey(key) => {
                parquet::variant::VariantPathElement::field(key.as_str())
            }
            VariantPathSegment::ArrayIndex(index) => {
                parquet::variant::VariantPathElement::index(*index as usize)
            }
        });
    }
    Ok(parquet::variant::VariantPath::from(elems))
}

fn large_binary_to_variant_array(arr: &LargeBinaryArray) -> ExecutionResult<VariantArray> {
    let mut metadata_builder = novarocks_functions::aggregate_scalar::BinaryResultBuilder::new();
    let mut value_builder = novarocks_functions::aggregate_scalar::BinaryResultBuilder::new();
    let mut validity = BooleanBufferBuilder::new(arr.len());
    let empty_meta = VariantMetadata::empty();
    let empty_meta_raw = empty_meta.raw();

    for row in 0..arr.len() {
        if arr.is_null(row) {
            metadata_builder.append_value(empty_meta_raw)?;
            value_builder.append_value([])?;
            validity.append(false);
            continue;
        }
        let (metadata, value) = split_serialized(arr.value(row))
            .map_err(|source| variant_failure::input_row("variant path source", row, source))?;
        metadata_builder.append_value(metadata)?;
        value_builder.append_value(value)?;
        validity.append(true);
    }

    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
    ]);
    let struct_arr = StructArray::try_new(
        fields,
        vec![
            Arc::new(metadata_builder.finish()?) as ArrayRef,
            Arc::new(value_builder.finish()?) as ArrayRef,
        ],
        Some(NullBuffer::new(validity.finish())),
    )
    .map_err(|source| {
        variant_failure::sdk_at("variant path source", SdkStage::AssembleInput, source)
    })?;
    VariantArray::try_new(&struct_arr).map_err(|source| {
        variant_failure::sdk_at("variant path source", SdkStage::AssembleInput, source)
    })
}

fn source_to_engine_variant_array(
    source_array: &ArrayRef,
    column_name: &str,
) -> ExecutionResult<VariantArray> {
    let source = source_array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            materialization_contract(format_args!(
                "variant path source column `{column_name}` has unsupported type: {:?}",
                source_array.data_type()
            ))
        })?;
    large_binary_to_variant_array(source)
}

fn materialize_single_variant_path(
    source_array: &ArrayRef,
    spec: &VariantPathSpec,
    path_owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
) -> Result<ArrayRef, VariantMaterializationFailure> {
    let fn_name = if spec.strict() {
        "variant_get"
    } else {
        "try_variant_get"
    };
    let parsed = if let Some(owner) = path_owner {
        use novarocks_types::value::variant::{
            VariantPathConstructionStep, parse_variant_path_with_storage_observed,
        };
        KernelParsedPath::Observed(parse_variant_path_with_storage_observed::<
            VariantMaterializationFailure,
        >(
            spec.canonical_path(),
            owner.storage_lifetime(),
            |step| {
                match step {
                    VariantPathConstructionStep::Request(layout) => owner.request(layout)?,
                    VariantPathConstructionStep::Work(units) => owner.observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units,
                        },
                    )?,
                }
                Ok(())
            },
        )?)
    } else {
        KernelParsedPath::Pure(parse_variant_path(spec.canonical_path())?)
    };
    let variant_array = source_to_engine_variant_array(source_array, spec.source_name())?;
    let path = kernel_path(parsed.segments(), path_owner)?;
    if let Some(owner) = path_owner {
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<Field>(),
            )?,
        )?;
    }
    let opts = GetOptions::new_with_path(path)
        .with_cast_options(CastOptions {
            safe: !spec.strict(),
            format_options: Default::default(),
        })
        .with_as_type(Some(Arc::new(Field::new(
            "",
            spec.requested_type().clone(),
            true,
        ))));

    if let Some(owner) = path_owner {
        owner.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<StructArray>(),
            )?,
        )?;
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::OpaqueOperationFinished,
        )?;
    }
    let input_ref: ArrayRef = Arc::new(variant_array.into_inner());
    let result = kernel_variant_get(&input_ref, opts).map_err(|source| {
        VariantMaterializationFailure::Evaluation(variant_failure::sdk_at(
            fn_name,
            SdkStage::Extract,
            source,
        ))
    });
    if let Some(owner) = path_owner {
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::OpaqueOperationFinished,
        )?;
    }
    result
}

/// Materialize synthetic scan slots for pushed-down variant paths.
///
/// `batch` is the parquet read batch after read-column reordering. `read_slot_ids`
/// describes that read layout, including hidden source slots. `output_slot_ids`
/// is the reader-visible materialized layout: physical slots that remain visible
/// plus synthetic `VariantPathSpec::output_slot_id` slots. Hidden source read
/// slots are omitted unless the caller includes them explicitly in
/// `output_slot_ids`.
#[derive(Debug)]
pub enum VariantMaterializationFailure {
    Construction(crate::exec::pipeline::operator::ConstructionFailure),
    Evaluation(ExecutionFailure),
}
impl From<ExecutionFailure> for VariantMaterializationFailure {
    fn from(source: ExecutionFailure) -> Self {
        Self::Evaluation(source)
    }
}
impl From<novarocks_types::value::variant::VariantFailure> for VariantMaterializationFailure {
    fn from(source: novarocks_types::value::variant::VariantFailure) -> Self {
        Self::Evaluation(source.into())
    }
}
impl From<crate::exec::pipeline::operator::ConstructionFailure> for VariantMaterializationFailure {
    fn from(error: crate::exec::pipeline::operator::ConstructionFailure) -> Self {
        Self::Construction(error)
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for VariantMaterializationFailure
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Construction(error.into())
    }
}

#[cfg(test)]
fn materialize_variant_path_columns(
    batch: RecordBatch,
    read_slot_ids: &[SlotId],
    output_slot_ids: &[SlotId],
    specs: &[VariantPathSpec],
) -> ExecutionResult<RecordBatch> {
    materialize_variant_path_columns_core(
        &batch,
        read_slot_ids,
        output_slot_ids,
        specs,
        None,
        None,
        None,
    )
    .map_err(|error| match error {
        VariantMaterializationFailure::Evaluation(error) => error,
        VariantMaterializationFailure::Construction(error) => error.into(),
    })
}
/// Strong runtime borrows the frozen output field author. New temporary indices
/// and the actual column vector belong to this input construction scope;
/// variant kernel payload allocations retain their original independent policy.
pub fn materialize_variant_path_columns_owned(
    batch: &RecordBatch,
    read_slot_ids: &[SlotId],
    output_slot_ids: &[SlotId],
    specs: &[VariantPathSpec],
    final_schema: arrow::datatypes::SchemaRef,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    path_owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
) -> Result<RecordBatch, VariantMaterializationFailure> {
    materialize_variant_path_columns_core(
        batch,
        read_slot_ids,
        output_slot_ids,
        specs,
        Some(final_schema),
        Some(owner),
        Some(path_owner),
    )
}
fn materialize_variant_path_columns_core(
    batch: &RecordBatch,
    read_slot_ids: &[SlotId],
    output_slot_ids: &[SlotId],
    specs: &[VariantPathSpec],
    final_schema: Option<arrow::datatypes::SchemaRef>,
    owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
    path_owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
) -> Result<RecordBatch, VariantMaterializationFailure> {
    fn work(
        owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
        units: usize,
    ) -> Result<(), VariantMaterializationFailure> {
        if let Some(owner) = owner {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
            )?;
        }
        Ok(())
    }
    fn request_table<K, V>(
        count: usize,
        owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
    ) -> Result<(), VariantMaterializationFailure> {
        if let Some(owner) = owner {
            let facts=novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<K,V>(count).map_err(|error|match error{novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,_=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel})?;
            if let Some(layout) = facts.layout {
                owner.request(layout)?;
            }
        }
        Ok(())
    }
    if specs.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_columns() != read_slot_ids.len() {
        return Err(VariantMaterializationFailure::Evaluation(
            materialization_contract(format_args!(
                "variant path read slot mismatch: columns={} read_slot_ids={}",
                batch.num_columns(),
                read_slot_ids.len()
            )),
        ));
    }

    let batch_schema = batch.schema();
    request_table::<SlotId, (Option<Field>, ArrayRef)>(read_slot_ids.len(), owner)?;
    let mut existing = HashMap::with_capacity(read_slot_ids.len());
    for (idx, slot_id) in read_slot_ids.iter().copied().enumerate() {
        work(owner, 1)?;
        existing.insert(
            slot_id,
            (
                final_schema
                    .is_none()
                    .then(|| batch_schema.field(idx).as_ref().clone()),
                batch.column(idx).clone(),
            ),
        );
    }

    request_table::<SlotId, (Option<Field>, ArrayRef)>(specs.len(), owner)?;
    let mut synthetic = HashMap::with_capacity(specs.len());
    for spec in specs {
        work(owner, 1)?;
        let mut source_idx = None;
        for (index, slot_id) in read_slot_ids.iter().enumerate() {
            work(owner, 1)?;
            if *slot_id == spec.source_read_slot_id() {
                source_idx = Some(index);
                break;
            }
        }
        let source_idx=source_idx.ok_or_else(||materialization_contract(format_args!("variant path source_read_slot_id={} for output_slot_id={} is missing from read slots {:?}",spec.source_read_slot_id(),spec.output_slot_id(),read_slot_ids)))?;
        let array = materialize_single_variant_path(batch.column(source_idx), spec, path_owner)?;
        work(owner, 1)?;
        let field = if final_schema.is_none() {
            let mut field = spec.output_field().clone();
            if array.null_count() > 0 && !field.is_nullable() {
                field = field.with_nullable(true);
            }
            Some(field)
        } else {
            None
        };
        if synthetic
            .insert(spec.output_slot_id(), (field, array))
            .is_some()
        {
            return Err(VariantMaterializationFailure::Evaluation(
                materialization_contract(format_args!(
                    "duplicate variant path output_slot_id={}",
                    spec.output_slot_id()
                )),
            ));
        }
    }

    let mut fields = if final_schema.is_none() {
        Vec::with_capacity(output_slot_ids.len())
    } else {
        Vec::new()
    };
    if let Some(owner) = owner {
        owner.request(
            std::alloc::Layout::array::<ArrayRef>(output_slot_ids.len()).map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?,
        )?;
    }
    let mut columns = Vec::with_capacity(output_slot_ids.len());
    for slot_id in output_slot_ids {
        work(owner, 1)?;
        if let Some((field, array)) = synthetic.get(slot_id) {
            if let Some(field) = field {
                fields.push(field.clone());
            }
            columns.push(array.clone());
            continue;
        }
        if let Some((field, array)) = existing.get(slot_id) {
            if let Some(field) = field {
                fields.push(field.clone());
            }
            columns.push(array.clone());
            continue;
        }
        return Err(VariantMaterializationFailure::Evaluation(
            materialization_contract(format_args!(
                "variant path output slot_id={} is neither a read column nor a synthetic column",
                slot_id
            )),
        ));
    }

    work(owner, 1)?;
    let schema = final_schema.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::new(fields)));
    RecordBatch::try_new(schema, columns).map_err(|error| {
        VariantMaterializationFailure::Evaluation(
            crate::exec::chunk::ChunkConstructionFailure::record_batch(error).into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields, Schema};

    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    #[test]
    fn variant_struct_gate_accepts_unshredded() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_shredded_three_child() {
        let typed_value = struct_of(vec![Field::new(
            "a",
            struct_of(vec![
                Field::new("value", DataType::Binary, true),
                Field::new("typed_value", DataType::Int64, true),
            ]),
            false,
        )]);
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("typed_value", typed_value, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_metadata_plus_typed_value_only() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("typed_value", DataType::Int64, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_accepts_binary_view_children() {
        let dt = struct_of(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, true),
        ]);
        assert!(is_variant_struct_data_type(&dt));
    }

    #[test]
    fn variant_struct_gate_rejects_non_variant_shapes() {
        // missing metadata
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "value",
            DataType::Binary,
            true
        )])));
        // unknown extra field
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("extra", DataType::Int32, true),
        ])));
        // metadata only
        assert!(!is_variant_struct_data_type(&struct_of(vec![Field::new(
            "metadata",
            DataType::Binary,
            false
        )])));
        // metadata wrong type
        assert!(!is_variant_struct_data_type(&struct_of(vec![
            Field::new("metadata", DataType::Int32, false),
            Field::new("value", DataType::Binary, true),
        ])));
        // not a struct
        assert!(!is_variant_struct_data_type(&DataType::LargeBinary));
    }

    use arrow::array::{Float64Array, Int64Array};

    use super::VariantPathSpec;
    use novarocks_types::SlotId;

    fn engine_variant_from_json(rows: Vec<Option<&str>>) -> ArrayRef {
        let mut builder = LargeBinaryBuilder::new();
        for row in rows {
            match row {
                Some(json) => builder.append_value(
                    novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(json)
                        .unwrap(),
                ),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }
    fn test_engine_variant() -> ArrayRef {
        engine_variant_from_json(vec![
            Some(r#"{"a":1,"b":"x"}"#),
            Some(r#"{"a":99,"b":"y"}"#),
            Some(r#"{"b":"no-a"}"#),
            Some(r#"{"a":"not-an-int"}"#),
            None,
        ])
    }
    fn engine_variant_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::LargeBinary,
                true,
            )])),
            vec![test_engine_variant()],
        )
        .unwrap()
    }

    fn variant_path_spec_for(
        canonical_path: &str,
        requested_type: DataType,
        strict: bool,
    ) -> VariantPathSpec {
        VariantPathSpec::from_pure_parts(VariantPathSpecParts {
            source_slot_id: SlotId::new(1),
            source_read_slot_id: SlotId::new(90),
            output_slot_id: SlotId::new(2),
            source_field_id: None,
            source_field: Field::new("v", DataType::LargeBinary, true),
            output_field: Field::new("v_a", requested_type.clone(), true),
            canonical_path: canonical_path.to_string(),
            strict,
            source_binding: Some(
                novarocks_type_contract::FunctionValueType::try_with_logical_type(
                    DataType::LargeBinary,
                    true,
                    novarocks_type_contract::ValueLogicalType::Variant,
                )
                .unwrap(),
            ),
            output_binding: Some(novarocks_type_contract::FunctionValueType::new(
                requested_type.clone(),
                true,
            )),
        })
    }

    #[test]
    fn immutable_variant_spec_clone_shares_backing_and_preserves_first_creator_refusal() {
        let original = variant_path_spec_for("$.a", DataType::Int64, true);
        let clone = original.clone();
        assert!(Arc::ptr_eq(&original.0, &clone.0));
        assert_eq!(
            original.source_name().as_ptr(),
            clone.source_name().as_ptr()
        );
        let mut requests = 0;
        let parts = VariantPathSpecParts {
            source_slot_id: SlotId::new(1),
            source_read_slot_id: SlotId::new(90),
            output_slot_id: SlotId::new(2),
            source_field_id: None,
            source_field: Field::new("v", DataType::LargeBinary, true),
            output_field: Field::new("v_a", DataType::Int64, true),
            canonical_path: "$.a".to_string(),
            strict: true,
            source_binding: original.source_binding().cloned(),
            output_binding: original.output_binding().cloned(),
        };
        let refused = VariantPathSpec::try_from_parts_observed(
            parts,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut |layout| {
                requests += 1;
                assert_eq!(
                    layout,
                    VariantPathSpec::storage_allocation_layout().unwrap()
                );
                Err(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel)
            },
        );
        assert!(matches!(
            refused,
            Err(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel)
        ));
        assert_eq!(requests, 1);
    }

    fn variant_path_spec(strict: bool) -> VariantPathSpec {
        variant_path_spec_for("$.a", DataType::Int64, strict)
    }

    fn materialized_variant_path(strict: bool) -> ExecutionResult<RecordBatch> {
        materialize_variant_path_columns(
            engine_variant_batch(),
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec(strict)],
        )
    }

    fn assert_int64_array_eq(actual: &ArrayRef, expected: &ArrayRef) {
        let actual = actual.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = expected.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(actual.len(), expected.len());
        for row in 0..actual.len() {
            assert_eq!(actual.is_null(row), expected.is_null(row), "row {row}");
            if !actual.is_null(row) {
                assert_eq!(actual.value(row), expected.value(row), "row {row}");
            }
        }
    }

    // Historical test names remain stable in the P12/P13 retention map.
    // The value oracle is now independent literals; the actual stimulation
    // stays at this retained materialization owner's existing unit-test layer.
    #[test]
    fn variant_path_materialization_matches_variant_get_on_normalized_engine_source() {
        let source = engine_variant_from_json(vec![
            Some(r#"{"a": 1, "b": "x"}"#),
            Some(r#"{"a": 99, "b": "y"}"#),
            Some(r#"{"b": "no-a"}"#),
            None,
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                source.data_type().clone(),
                true,
            )])),
            vec![source.clone()],
        )
        .expect("batch");
        let batch = materialize_variant_path_columns(
            batch,
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec(true)],
        )
        .expect("materialize");
        // Independent JSON path semantics: $.a selects 1/99, a missing key
        // and the source SQL NULL both produce SQL NULL. No evaluator oracle.
        let expected: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(99), None, None]));
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "v_a");
        assert_int64_array_eq(batch.column(0), &expected);
    }

    #[test]
    fn variant_path_requested_float64_preserves_values_and_missing_nulls() {
        let source = engine_variant_from_json(vec![
            Some(r#"{"a": 1, "b": "x"}"#),
            Some(r#"{"a": 99, "b": "y"}"#),
            Some(r#"{"b": "no-a"}"#),
            None,
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                source.data_type().clone(),
                true,
            )])),
            vec![source],
        )
        .expect("batch");

        let batch = materialize_variant_path_columns(
            batch,
            &[SlotId::new(90)],
            &[SlotId::new(2)],
            &[variant_path_spec_for("$.a", DataType::Float64, false)],
        )
        .expect("materialize");

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 99.0);
        assert!(values.is_null(2));
        assert!(values.is_null(3));
    }

    #[test]
    fn variant_path_strict_errors_but_try_returns_null_on_cast_failure() {
        let error = materialized_variant_path(true).expect_err("strict cast must fail");
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Data);
        assert_eq!(fact.reason(), ExecutionReason::VariantKernelValue.reason());
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut retains_sdk_cause = false;
        while let Some(cause) = source {
            retains_sdk_cause |= cause.downcast_ref::<arrow::error::ArrowError>().is_some();
            source = cause.source();
        }
        assert!(
            retains_sdk_cause,
            "strict extraction retains its actual SDK cause"
        );
        assert!(
            error.to_string().contains("variant_get"),
            "error names strict function: {error}"
        );

        let batch = materialized_variant_path(false).expect("try materialize");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            values.is_null(3),
            "try_variant_get cast failure row should be NULL"
        );
    }

    #[test]
    fn variant_path_missing_path_returns_null() {
        let batch = materialized_variant_path(false).expect("materialize");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.is_null(2), "missing $.a should be NULL");
    }

    #[test]
    fn variant_path_hidden_source_read_slot_does_not_leak() {
        let batch = materialized_variant_path(false).expect("materialize");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "v_a");
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn variant_path_normalized_source_matches_variant_get() {
        let batch = materialized_variant_path(false).expect("materialize normalized engine source");
        // TRY extraction additionally maps the nonnumeric string in row 3
        // to SQL NULL; the source bytes and path are unchanged from the corpus.
        let expected: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(1), Some(99), None, None, None]));
        assert_int64_array_eq(batch.column(0), &expected);
    }
}

#[derive(Debug)]
struct MaterializationContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for MaterializationContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for MaterializationContract {}
impl novarocks_failure_contract::FailureProjection for MaterializationContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::VariantMaterializationContract.reason(),
        )
    }
}
fn materialization_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(MaterializationContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
