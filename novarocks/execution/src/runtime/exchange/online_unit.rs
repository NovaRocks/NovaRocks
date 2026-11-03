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
//! Preparation-bound exchange units. Task identity and delivery remain with
//! the original registry, stream owner and unit-ingress claim.

use super::*;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use novarocks_arrow_batch_codec::record_geometry::{GeometryError, GeometryProfile, GeometryWork};
use novarocks_arrow_batch_codec::{
    BatchRecipe, CodecError, DecodeKind, RecipeField, RepresentationPolicy, UnitObserver,
};
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, ValueSchemaCreation, ValueTypeStorageLifetime,
};
use std::alloc::Layout;

pub(crate) struct ExchangeRecipe {
    pub(super) value: BatchRecipe,
    storage: ValueTypeStorageLifetime,
    declarations: Vec<FunctionValueType>,
}

fn codec_error(
    error: CodecError<ExchangeFailure>,
    operation: failure::ArrowOperation,
) -> ExchangeFailure {
    match error {
        CodecError::Observer(error) => error,
        CodecError::Invalid(detail) => failure::payload(detail),
        CodecError::ValueType(error) => error.into(),
        CodecError::Arrow(error) => failure::arrow(operation, error),
        CodecError::Metadata(error) => failure::metadata("verify Arrow unit message", error),
        CodecError::Allocation(error) => failure::allocation(error),
        CodecError::Layout(error) => error.into(),
        CodecError::Carrier(error) => {
            ExchangeFailure::Execution(novarocks_functions::KernelFailure::from(error).into())
        }
        CodecError::Construction(novarocks_type_contract::ControlResourceError::Control(error)) => {
            error.into()
        }
        CodecError::Construction(novarocks_type_contract::ControlResourceError::SourceModel(
            detail,
        )) => ConstructionFailure::InvalidSource(detail.into()).into(),
    }
}

impl From<GeometryError> for ExchangeFailure {
    fn from(error: GeometryError) -> Self {
        match error {
            GeometryError::InvalidShape(detail) => failure::payload(detail),
            GeometryError::ResourceExhausted => {
                ConstructionFailure::Control(CompileControlError::ResourceExhausted).into()
            }
        }
    }
}
impl From<CompileControlError> for ExchangeFailure {
    fn from(error: CompileControlError) -> Self {
        ConstructionFailure::Control(error).into()
    }
}

fn construction_error(
    error: novarocks_functions::arrow_ipc_construction::IpcConstructionError<ConstructionFailure>,
) -> ExchangeFailure {
    use novarocks_functions::arrow_ipc_construction::{IpcConstructionError, IpcGeometryError};
    match error {
        IpcConstructionError::Creator(error) => error.into(),
        IpcConstructionError::Control(error) => ExchangeFailure::Execution(error.into()),
        IpcConstructionError::Shape(error) => error.into(),
        IpcConstructionError::Geometry(IpcGeometryError::Overflow)
        | IpcConstructionError::HashMap(
            novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_),
        ) => {
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic.into()
        }
        IpcConstructionError::Geometry(IpcGeometryError::SourceModel) => {
            ConstructionFailure::InvalidSource(
                "exchange Arrow unit construction source model differs".into(),
            )
            .into()
        }
        IpcConstructionError::HashMap(
            novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(
                detail,
            ),
        ) => ConstructionFailure::InvalidSource(detail.into()).into(),
    }
}

struct GeometryObserver<'a>(&'a dyn InputConstructionOwner);
impl GeometryWork for GeometryObserver<'_> {
    type Error = ExchangeFailure;
    fn has_admission(&self) -> bool {
        true
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        observe_work(self.0)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        observe_work(self.0)
    }
    fn geometry_request(&mut self, layout: Layout, construction: bool) -> Result<(), Self::Error> {
        if construction {
            self.0.request(layout)?;
        }
        Ok(())
    }
    fn source_work(&mut self) -> Result<(), Self::Error> {
        observe_work(self.0)
    }
    fn validate_root(&mut self, field: &arrow::datatypes::Field) -> Result<(), Self::Error> {
        novarocks_type_contract::validate_arrow_layout_structure_observed(
            field.data_type(),
            |_| observe_work(self.0),
        )?;
        Ok(())
    }
}
fn observe_work(owner: &dyn InputConstructionOwner) -> Result<(), ExchangeFailure> {
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units: 1 },
    )?;
    Ok(())
}

// Real immutable body custody follows every Arrow Buffer alias. The backing
// drops before the finite construction lifetime; neither keeps Task control.
struct DecodedBody {
    buffer: arrow::buffer::Buffer,
    _storage: ValueTypeStorageLifetime,
}
impl AsRef<[u8]> for DecodedBody {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
// Pinned bytes::Bytes::from_owner uses repr(C) Owned<T> { ref_cnt, owner }.
// This is its actual wrapper request, not a payload-size estimate or grant.
#[repr(C)]
struct DecodedBodyOwnerLayout {
    _references: std::sync::atomic::AtomicUsize,
    _owner: DecodedBody,
}

struct UnitConstruction<'a> {
    schema: &'a dyn InputConstructionOwner,
    constructing_schema: bool,
    body_storage: Option<ValueTypeStorageLifetime>,
    retained: &'a dyn InputConstructionOwner,
    temporary: &'a dyn InputConstructionOwner,
    control: Option<&'a dyn novarocks_functions::KernelEvaluationControl>,
}
impl UnitObserver for UnitConstruction<'_> {
    type Error = ExchangeFailure;
    fn checkpoint(&mut self) -> Result<(), Self::Error> {
        if let Some(control) = self.control {
            control
                .checkpoint(0)
                .map_err(|error| ExchangeFailure::Execution(error.into()))?;
        }
        observe_work(self.temporary)
    }
    fn before_allocation(&mut self, layout: Layout) -> Result<(), Self::Error> {
        if self.constructing_schema {
            self.schema.request(layout)?;
        } else {
            self.retained.request(layout)?;
        }
        Ok(())
    }
    fn before_metadata_verification(&mut self, metadata: &[u8]) -> Result<(), Self::Error> {
        self.constructing_schema = false;
        self.temporary.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                units: metadata.len(),
            },
        )?;
        self.checkpoint()
    }
    fn copy_body(&mut self, body: &[u8]) -> Result<arrow::buffer::Buffer, Self::Error> {
        let rounded = body.len().checked_add(63).ok_or(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )? & !63;
        if rounded != 0 {
            self.retained
                .request(Layout::from_size_align(rounded, 64).map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?)?;
        }
        self.retained.observe_opaque_upper_bound(
            novarocks_arrow_batch_codec::construction::original_reader_buffer_header_upper_bound()?,
        )?;
        let storage = self
            .body_storage
            .as_ref()
            .ok_or_else(|| {
                failure::contract("exchange body decoder has no original storage owner")
            })?
            .clone();
        self.retained
            .request(Layout::new::<DecodedBodyOwnerLayout>())?;
        self.retained.request(
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                bytes::Bytes,
            >())?,
        )?;
        self.retained.observe_opaque_upper_bound(
            novarocks_arrow_batch_codec::construction::original_reader_buffer_header_upper_bound()?,
        )?;
        let buffer = arrow::buffer::Buffer::from_slice_ref(body);
        let output = arrow::buffer::Buffer::from(bytes::Bytes::from_owner(DecodedBody {
            buffer,
            _storage: storage,
        }));
        self.checkpoint()?;
        Ok(output)
    }
    fn before_recipe(&mut self, fields: &[RecipeField<'_>]) -> Result<(), Self::Error> {
        self.constructing_schema = true;
        for _ in fields {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn before_unit_recipe(
        &mut self,
        _recipe: &BatchRecipe,
        tags: &[u8],
    ) -> Result<(), Self::Error> {
        self.constructing_schema = true;
        for _ in tags {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn before_encode_batch(
        &mut self,
        _recipe: &BatchRecipe,
        batch: &RecordBatch,
    ) -> Result<(), Self::Error> {
        self.constructing_schema = false;
        let mut request = |layout| self.retained.request(layout);
        let mut opaque = |bytes| self.retained.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(self.retained.storage_lifetime(), &mut request, &mut opaque);
        novarocks_functions::arrow_ipc_construction::observe_unit_batch_construction(
            batch,
            &mut creation,
            self.control.ok_or_else(|| {
                failure::contract("decode-only exchange constructor cannot encode an Arrow unit")
            })?,
        )
        .map_err(construction_error)?;
        Ok(())
    }
    fn before_decode_message(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &[u8],
    ) -> Result<(), Self::Error> {
        self.temporary.request(
            Layout::array::<&arrow::datatypes::Field>(schema.fields().len()).map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?,
        )?;
        let fields = schema
            .fields()
            .iter()
            .map(|field| field.as_ref())
            .collect::<Vec<_>>();
        let checked = novarocks_arrow_batch_codec::record_geometry::preflight_record_batch(
            batch,
            body,
            &fields,
            None,
            GeometryProfile::Online,
            &mut GeometryObserver(self.temporary),
        )?;
        drop(checked);
        Ok(())
    }
    fn before_arrow_decode(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &arrow::buffer::Buffer,
        dictionaries: &HashMap<i64, ArrayRef>,
        kind: DecodeKind,
    ) -> Result<(), Self::Error> {
        use novarocks_arrow_batch_codec::construction::{
            ReaderArrayHeaderClass, ReaderArrayStructureObserver,
        };
        if let DecodeKind::Dictionary { id } = kind {
            if !dictionaries.contains_key(&id) {
                let bytes = novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<i64, ArrayRef, ExchangeFailure>(
                    dictionaries.len(), dictionaries.capacity(), || observe_work(self.temporary),
                )?;
                if bytes != 0 {
                    self.temporary.observe_opaque_upper_bound(bytes)?;
                }
            }
        }
        let mut request = |layout, class| -> Result<(), ExchangeFailure> {
            match class {
                ReaderArrayHeaderClass::Retained => self.retained.request(layout)?,
                ReaderArrayHeaderClass::Temporary => self.temporary.request(layout)?,
            }
            Ok(())
        };
        let mut opaque = |bytes, class| -> Result<(), ExchangeFailure> {
            match class {
                ReaderArrayHeaderClass::Retained => {
                    self.retained.observe_opaque_upper_bound(bytes)?
                }
                ReaderArrayHeaderClass::Temporary => {
                    self.temporary.observe_opaque_upper_bound(bytes)?
                }
            }
            Ok(())
        };
        let mut work = || observe_work(self.temporary);
        let mut model = ReaderArrayStructureObserver {
            request: &mut request,
            opaque: &mut opaque,
            work: &mut work,
        };
        match kind {
            DecodeKind::Dictionary { id } => {
                model.dictionary_batch(schema, id, batch, body.as_slice())
            }
            DecodeKind::RecordBatch => model.record_batch(
                schema,
                batch,
                body.as_slice(),
                ReaderArrayHeaderClass::Retained,
                |id| {
                    observe_work(self.temporary)?;
                    Ok(dictionaries.contains_key(&id))
                },
            ),
        }
        .map_err(super::reader_structure::model_error)
    }
}

pub(crate) fn prepare_recipe(
    schema: &ChunkSchemaRef,
    output_columns: &[SlotId],
    factory: &dyn InputConstructionFactory,
) -> Result<Arc<ExchangeRecipe>, ExchangeFailure> {
    if !schema.is_typed() {
        return Err(failure::contract(
            "exchange recipe requires the complete frozen value declaration",
        ));
    }
    let owner = factory.begin_typed_construction(schema)?;
    let temporary = factory.begin_typed_construction(schema)?;
    let mut observer = UnitConstruction {
        schema: owner.as_ref(),
        constructing_schema: true,
        body_storage: None,
        retained: owner.as_ref(),
        temporary: temporary.as_ref(),
        control: None,
    };
    let count = if output_columns.is_empty() {
        schema.slots().len()
    } else {
        output_columns.len()
    };
    observer.before_allocation(Layout::array::<usize>(count).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?)?;
    let mut indices = Vec::with_capacity(count);
    if output_columns.is_empty() {
        indices.extend(0..count);
    } else {
        for id in output_columns {
            observer.checkpoint()?;
            let index = schema
                .slots()
                .iter()
                .position(|slot| slot.slot_id() == *id)
                .ok_or_else(|| {
                    failure::contract("exchange recipe output names an absent frozen slot")
                })?;
            indices.push(index);
        }
    }
    observer.before_allocation(Layout::array::<FunctionValueType>(count).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?)?;
    let mut values = Vec::with_capacity(count);
    for index in &indices {
        let binding = schema.slots()[*index]
            .typed_binding()
            .clone_observed(|| observer.checkpoint())?;
        values.push(FunctionValueType::try_from_typed_binding(binding)?);
    }
    let arrow_schema = schema.arrow_schema_ref();
    observer.before_allocation(Layout::array::<RecipeField<'_>>(count).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?)?;
    let mut fields = Vec::with_capacity(count);
    for (index, value_type) in indices.iter().zip(&values) {
        let field = &arrow_schema.fields()[*index];
        let representation = if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            RepresentationPolicy::CanonicalOrInt32Utf8Dictionary
        } else {
            RepresentationPolicy::Exact
        };
        fields.push(RecipeField {
            field,
            value_type,
            representation,
            nullability: novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
        });
    }
    let value = BatchRecipe::try_new(&fields, &mut observer)
        .map_err(|error| codec_error(error, failure::ArrowOperation::Construct))?;
    let storage = owner
        .storage_lifetime()
        .retaining_observed(&schema.storage_lifetime(), |layout| owner.request(layout))?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(
            Layout::new::<ExchangeRecipe>(),
        )?,
    )?;
    Ok(Arc::new(ExchangeRecipe {
        value,
        storage,
        declarations: values,
    }))
}

pub(crate) fn encode(
    recipe: &ExchangeRecipe,
    chunk: &Chunk,
    factory: &dyn InputConstructionFactory,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> Result<Vec<u8>, ExchangeFailure> {
    let retained = factory.begin_input_construction(chunk)?;
    let temporary = factory.begin_input_construction(chunk)?;
    let mut observer = UnitConstruction {
        schema: temporary.as_ref(),
        constructing_schema: false,
        body_storage: None,
        retained: retained.as_ref(),
        temporary: temporary.as_ref(),
        control: Some(control),
    };
    if recipe.declarations.len() != chunk.chunk_schema().slots().len() {
        return Err(failure::contract(
            "exchange input differs from its prepared output declaration",
        ));
    }
    for (expected, actual) in recipe.declarations.iter().zip(chunk.chunk_schema().slots()) {
        if !expected
            .typed_binding()
            .borrowed()
            .exactly_equals_observed(actual.typed_binding().borrowed(), || observer.checkpoint())?
        {
            return Err(failure::contract(
                "exchange input value declaration differs from its prepared recipe",
            ));
        }
    }
    novarocks_arrow_batch_codec::encode_unit(&recipe.value, chunk.record_batch(), &mut observer)
        .map_err(|error| codec_error(error, failure::ArrowOperation::Encode))
}

pub(crate) fn decode(
    recipe: &ExchangeRecipe,
    bytes: &[u8],
    expected: &ChunkSchemaRef,
    factory: &dyn InputConstructionFactory,
) -> Result<Vec<Chunk>, ExchangeFailure> {
    let schema = factory.begin_typed_construction(expected)?;
    let retained = factory.begin_typed_construction(expected)?;
    let output = factory.begin_typed_construction(expected)?;
    let temporary = factory.begin_typed_construction(expected)?;
    let body_storage = retained
        .storage_lifetime()
        .retaining_observed(&schema.storage_lifetime(), |layout| {
            retained.request(layout)
        })?
        .retaining_observed(&recipe.storage, |layout| retained.request(layout))?;
    let mut observer = UnitConstruction {
        schema: schema.as_ref(),
        constructing_schema: true,
        body_storage: Some(body_storage),
        retained: retained.as_ref(),
        temporary: temporary.as_ref(),
        control: None,
    };
    let batch = novarocks_arrow_batch_codec::decode_unit(&recipe.value, bytes, &mut observer)
        .map_err(|error| codec_error(error, failure::ArrowOperation::Decode))?;
    output.request(
        Layout::array::<SlotId>(expected.slots().len()).map_err(|_| {
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
        })?,
    )?;
    let wire = ExchangeWireMeta {
        slot_ids_by_index: expected.slots().iter().map(|slot| slot.slot_id()).collect(),
    };
    let source = schema
        .storage_lifetime()
        .retaining_observed(&recipe.storage, |layout| schema.request(layout))?;
    let source =
        source.retaining_observed(&output.storage_lifetime(), |layout| schema.request(layout))?;
    let chunk = materialize_chunk_for_wire_meta(
        expected,
        batch,
        &wire,
        false,
        DecodeCarrierContract::Exchange,
        &ExchangeHeaderCreation::RuntimeWithSource {
            owner: output.as_ref(),
            storage: source.clone(),
        },
    )?;
    let chunk = if expected.slots().is_empty() {
        // Count-only output carries rows without retaining a dead array group.
        chunk
    } else {
        chunk.retaining_array_type_storage_observed::<ExchangeFailure>(
            &retained.storage_lifetime(),
            |layout| Ok(output.request(layout)?),
        )?
    };
    output.request(Layout::array::<Chunk>(1).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?)?;
    Ok(vec![chunk])
}
