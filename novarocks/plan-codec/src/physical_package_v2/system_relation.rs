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

//! Independent snapshot data component. Its requests belong to the actual
//! creator; they never enter another structural namespace's fixed invoice.
use super::{
    type_sources::{PackageTypeChannel, PackageTypeOccurrence, PackageTypeOwner},
    type_views::{PackageTypeViews, TypeViewBudget},
};
use crate::host_projection_v2::ProjectionFailure;
use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions},
    buffer::Buffer,
    datatypes::{DataType, Field, FieldRef, Schema},
};
use novarocks_arrow_batch_codec::construction::{
    ReaderArrayHeaderClass, ReaderArrayStructureObserver, ReaderStructureFailure,
};
use novarocks_arrow_batch_codec::record_geometry::{
    GeometryError, GeometryProfile, GeometryWork, preflight_record_batch,
};
use novarocks_arrow_batch_codec::{
    BatchRecipe, CodecError, DecodeKind, RecipeField, RepresentationPolicy, UnitObserver,
    decode_unit, encode_unit,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use novarocks_type_contract::{
    CarrierNullabilityPolicy, CompileCheckpoints, CompileControlError, FunctionValueType,
    ValueSchemaCreation,
};
use std::{
    alloc::Layout,
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

#[derive(Debug)]
pub enum SystemRelationError {
    Deferred,
    Control(CompileControlError),
    Invalid(&'static str),
    Arrow(arrow::error::ArrowError),
    Geometry(GeometryError),
    Layout(LayoutResourceError),
    Constant(novarocks_constant_contract::ConstantError),
    Kernel(novarocks_functions::KernelFailure),
}
impl std::fmt::Display for SystemRelationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deferred => f.write_str("execution package contains a deferred system relation"),
            Self::Control(e) => e.fmt(f),
            Self::Invalid(e) => f.write_str(e),
            Self::Arrow(e) => e.fmt(f),
            Self::Geometry(e) => e.fmt(f),
            Self::Layout(e) => write!(f, "system relation allocation layout: {e:?}"),
            Self::Constant(e) => e.fmt(f),
            Self::Kernel(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for SystemRelationError {}
type Failure<H> = ProjectionFailure<SystemRelationError, H>;
fn invalid<H>(message: &'static str) -> Failure<H> {
    ProjectionFailure::Codec(SystemRelationError::Invalid(message))
}
pub(super) fn map_failure<C, H>(
    error: Failure<H>,
    map: impl FnOnce(SystemRelationError) -> C,
) -> ProjectionFailure<C, H> {
    match error {
        ProjectionFailure::Host(e) => ProjectionFailure::Host(e),
        ProjectionFailure::Codec(e) => ProjectionFailure::Codec(map(e)),
    }
}
impl<H> From<CompileControlError> for Failure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Codec(SystemRelationError::Control(e))
    }
}
impl<H> From<GeometryError> for Failure<H> {
    fn from(e: GeometryError) -> Self {
        Self::Codec(SystemRelationError::Geometry(e))
    }
}
impl<H> From<LayoutResourceError> for Failure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Codec(SystemRelationError::Layout(e))
    }
}
impl<H> From<novarocks_type_contract::CarrierParameterError> for Failure<H> {
    fn from(_: novarocks_type_contract::CarrierParameterError) -> Self {
        invalid("system relation carrier parameters are invalid")
    }
}
impl<H> From<novarocks_type_contract::ValueTypeError> for Failure<H> {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        invalid("system relation value type is invalid")
    }
}
impl<H> From<novarocks_type_contract::owned_resources::hashmap::HashMapResourceError>
    for Failure<H>
{
    fn from(_: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError) -> Self {
        invalid("system relation dictionary allocation geometry is invalid")
    }
}
impl<H> From<novarocks_arrow_batch_codec::construction::IpcGeometryError> for Failure<H> {
    fn from(_: novarocks_arrow_batch_codec::construction::IpcGeometryError) -> Self {
        invalid("system relation IPC allocation geometry is invalid")
    }
}
impl<H> From<novarocks_functions::KernelFailure> for Failure<H> {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        use novarocks_functions::KernelFailure;
        match error {
            KernelFailure::CompileControl(e) => e.into(),
            KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
            KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
            KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
            e => Self::Codec(SystemRelationError::Kernel(e)),
        }
    }
}
fn codec<H>(e: CodecError<Failure<H>>) -> Failure<H> {
    match e {
        CodecError::Observer(e) => e,
        CodecError::Arrow(e) => ProjectionFailure::Codec(SystemRelationError::Arrow(e)),
        CodecError::Invalid(e) => invalid(e),
        CodecError::Layout(e) => e.into(),
        CodecError::Allocation(_) => CompileControlError::ResourceExhausted.into(),
        _ => invalid("system relation Arrow unit violates its frozen recipe"),
    }
}
fn request<H>(creator: &mut ValueSchemaCreation<'_, H>, layout: Layout) -> Result<(), Failure<H>> {
    if layout.size() == 0 {
        return Ok(());
    }
    creator
        .request_layout(layout)
        .map_err(ProjectionFailure::Host)
}
fn array<T, H>(count: usize) -> Result<Layout, Failure<H>> {
    Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted.into())
}
fn reserve<T, H>(
    count: usize,
    creator: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Failure<H>> {
    work.flush()?;
    request(creator, array::<T, H>(count)?)?;
    let mut out = Vec::new();
    out.try_reserve_exact(count)
        .map_err(|_| Failure::from(CompileControlError::ResourceExhausted))?;
    Ok(out)
}

pub(super) fn encode_headers<'s>(
    package: &'s p::FragmentPackage,
    views: &PackageTypeViews<'s>,
    budget: &mut TypeViewBudget<'s, '_, '_>,
    admit: &mut dyn FnMut(Layout) -> Result<(), super::PackageEncodeError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SystemRelationUnits>, super::PackageEncodeError> {
    let count = package
        .fragment()
        .nodes()
        .values()
        .filter(|n| matches!(n.kind, p::NodeKind::SystemRelation { .. }))
        .count();
    if count == 0 {
        return Ok(Vec::new());
    }
    work.flush()?;
    admit(
        Layout::array::<wire::SystemRelationUnits>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?,
    )?;
    let mut out = Vec::new();
    out.try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for node in package.fragment().nodes().values() {
        if let p::NodeKind::SystemRelation { output_fields, .. } = &node.kind {
            work.flush()?;
            admit(
                Layout::array::<u32>(output_fields.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            )?;
            let mut ids = Vec::new();
            ids.try_reserve_exact(output_fields.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            for (ordinal, field) in output_fields.iter().enumerate() {
                ids.push(views.field_root_for_in(
                    PackageTypeOccurrence {
                        fragment: package.fragment().id(),
                        owner: PackageTypeOwner::SystemRelation(node.id),
                        channel: PackageTypeChannel::Field(ordinal),
                    },
                    field,
                    budget,
                    work,
                )?);
            }
            out.push(wire::SystemRelationUnits {
                node_id: Some(node.id.get()),
                units: Vec::new(),
                output_field_ids: ids,
            });
            work.step()?;
        }
    }
    Ok(out)
}

struct Observer<'a, 'b, 'c, 'd, H> {
    creation: RefCell<&'a mut ValueSchemaCreation<'b, H>>,
    work: RefCell<&'c mut CompileCheckpoints<'d>>,
}
impl<H> GeometryWork for Observer<'_, '_, '_, '_, H> {
    type Error = Failure<H>;
    fn has_admission(&self) -> bool {
        true
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.work.borrow_mut().step().map_err(Into::into)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.work.borrow_mut().flush().map_err(Into::into)
    }
    fn geometry_request(&mut self, layout: Layout, construction: bool) -> Result<(), Self::Error> {
        if construction {
            request(&mut self.creation.borrow_mut(), layout)?;
        }
        Ok(())
    }
    fn source_work(&mut self) -> Result<(), Self::Error> {
        GeometryWork::step(self)
    }
    fn validate_root(&mut self, field: &Field) -> Result<(), Self::Error> {
        novarocks_type_contract::validate_arrow_carrier_parameters_observed::<Failure<H>>(
            field.data_type(),
            || GeometryWork::step(self),
        )
    }
}
struct KernelControl<'a>(&'a dyn novarocks_type_contract::PureCompileControl);
impl novarocks_functions::KernelEvaluationControl for KernelControl<'_> {
    fn checkpoint(&self, work: u32) -> Result<(), novarocks_functions::KernelFailure> {
        self.0
            .checkpoint(novarocks_type_contract::CompilePhase::Encode, work)
            .map_err(novarocks_functions::KernelFailure::CompileControl)
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        Err(novarocks_functions::KernelFailure::ResourceExhausted)
    }
}
impl<H> UnitObserver for Observer<'_, '_, '_, '_, H> {
    type Error = Failure<H>;
    fn checkpoint(&mut self) -> Result<(), Self::Error> {
        GeometryWork::flush(self)
    }
    fn before_allocation(&mut self, layout: Layout) -> Result<(), Self::Error> {
        self.checkpoint()?;
        request(&mut self.creation.borrow_mut(), layout)
    }
    fn before_type_scratch(&mut self, _: usize) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    fn before_metadata_verification(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    fn copy_body(&mut self, body: &[u8]) -> Result<Buffer, Self::Error> {
        let rounded = body
            .len()
            .checked_add(63)
            .ok_or(CompileControlError::ResourceExhausted)?
            & !63;
        self.before_allocation(
            Layout::from_size_align(rounded, 64).map_err(|_| LayoutResourceError::Arithmetic)?,
        )?;
        self.creation.borrow_mut().observe_opaque_upper_bound(novarocks_arrow_batch_codec::construction::original_reader_buffer_header_upper_bound()?).map_err(ProjectionFailure::Host)?;
        Ok(Buffer::from_slice_ref(body))
    }
    fn before_recipe(&mut self, _: &[RecipeField<'_>]) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    fn before_unit_recipe(&mut self, _: &BatchRecipe, _: &[u8]) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    fn project_carrier(
        &mut self,
        _: usize,
        _: &ArrayRef,
        _: &DataType,
    ) -> Result<ArrayRef, Self::Error> {
        Err(invalid("system relation does not coerce a frozen carrier"))
    }
    fn before_encode_batch(
        &mut self,
        _: &BatchRecipe,
        batch: &RecordBatch,
    ) -> Result<(), Self::Error> {
        use novarocks_arrow_batch_codec::construction::IpcConstructionError;
        let work = self.work.borrow();
        let control = KernelControl(work.control());
        novarocks_functions::arrow_ipc_construction::observe_unit_batch_construction(
            batch,
            &mut self.creation.borrow_mut(),
            &control,
        )
        .map(|_| ())
        .map_err(|e| match e {
            IpcConstructionError::Creator(e) => ProjectionFailure::Host(e),
            IpcConstructionError::Control(novarocks_functions::KernelFailure::CompileControl(
                e,
            )) => e.into(),
            IpcConstructionError::Control(e) => {
                ProjectionFailure::Codec(SystemRelationError::Kernel(e))
            }
            _ => invalid("system relation IPC writer construction geometry is invalid"),
        })
    }
    fn before_decode_message(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &[u8],
    ) -> Result<(), Self::Error> {
        self.before_allocation(array::<&Field, H>(schema.fields().len())?)?;
        let fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<_>>();
        preflight_record_batch(batch, body, &fields, None, GeometryProfile::Online, self)?;
        Ok(())
    }
    fn before_arrow_decode(
        &mut self,
        schema: &Schema,
        batch: arrow::ipc::RecordBatch<'_>,
        body: &Buffer,
        dictionaries: &HashMap<i64, ArrayRef>,
        kind: DecodeKind,
    ) -> Result<(), Self::Error> {
        if let DecodeKind::Dictionary { id } = kind {
            if !dictionaries.contains_key(&id) {
                let n=novarocks_type_contract::owned_resources::hashmap::next_insert_allocation_upper_bound_observed::<i64,ArrayRef,Failure<H>>(dictionaries.len(),dictionaries.capacity(),||self.checkpoint()).map_err(|_|invalid("system relation dictionary allocation geometry is invalid"))?;
                self.creation
                    .borrow_mut()
                    .observe_opaque_upper_bound(n)
                    .map_err(ProjectionFailure::Host)?;
            }
        }
        let creation = &self.creation;
        let work = &self.work;
        let mut request = |layout, _| request(&mut creation.borrow_mut(), layout);
        let mut opaque = |bytes, _| {
            creation
                .borrow_mut()
                .observe_opaque_upper_bound(bytes)
                .map_err(ProjectionFailure::Host)
        };
        let mut checkpoint = || work.borrow_mut().flush().map_err(Failure::from);
        let mut reader = ReaderArrayStructureObserver {
            request: &mut request,
            opaque: &mut opaque,
            work: &mut checkpoint,
        };
        let result = match kind {
            DecodeKind::Dictionary { id } => {
                reader.dictionary_batch_with_repair_payload(schema, id, batch, body.as_slice())
            }
            DecodeKind::RecordBatch => reader.record_batch_with_repair_payload(
                schema,
                batch,
                body.as_slice(),
                ReaderArrayHeaderClass::Retained,
                |id| Ok(dictionaries.contains_key(&id)),
            ),
        };
        result.map_err(|e| match e {
            ReaderStructureFailure::Creator(e) => e,
            _ => invalid("system relation Arrow reader construction geometry is invalid"),
        })
    }
}
fn recipe<H>(
    fields: &[FieldRef],
    types: &[&FunctionValueType],
    creator: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BatchRecipe, Failure<H>> {
    let mut canonical = reserve(fields.len(), creator, work)?;
    for (field, ty) in fields.iter().zip(types) {
        request(creator, arc_layout(Layout::new::<Field>())?)?;
        canonical.push(Arc::new(Field::new(
            field.name(),
            ty.data_type().clone(),
            ty.nullable(),
        )));
    }
    let mut inputs = reserve(fields.len(), creator, work)?;
    for (field, ty) in canonical.iter().zip(types) {
        inputs.push(RecipeField {
            field,
            value_type: ty,
            representation: RepresentationPolicy::Exact,
            nullability: CarrierNullabilityPolicy::Exact,
        });
    }
    BatchRecipe::try_new(
        &inputs,
        &mut Observer {
            creation: RefCell::new(creator),
            work: RefCell::new(work),
        },
    )
    .map_err(codec)
}
fn validate_values<H>(
    batch: &RecordBatch,
    fields: &[FieldRef],
    creator: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    for (column, field) in batch.columns().iter().zip(fields) {
        // Arrow's decoder validates physical geometry first. This shared pass adds
        // decimal precision and selected nested requiredness without pool policy.
        let metadata =
            novarocks_functions::source_metadata_bytes_observed(column.as_ref(), &mut |before| {
                if before {
                    work.flush()?;
                } else {
                    work.step()?;
                }
                Ok::<_, Failure<H>>(())
            })?;
        work.flush()?;
        creator
            .observe_opaque_upper_bound(metadata)
            .map_err(ProjectionFailure::Host)?;
        let data = column.to_data();
        work.flush()?;
        novarocks_constant_contract::validate_array_values_with_creator(
            &data,
            field.is_nullable(),
            creator,
            work,
        )
        .map_err(|e| match e {
            novarocks_constant_contract::ConstantCreationFailure::Creator(e) => {
                ProjectionFailure::Host(e)
            }
            novarocks_constant_contract::ConstantCreationFailure::Constant(e) => {
                ProjectionFailure::Codec(SystemRelationError::Constant(e))
            }
            _ => invalid("system relation value validation construction failed"),
        })?;
    }
    Ok(())
}
pub(super) fn encode_units<H>(
    package: &p::FragmentPackage,
    output: &mut wire::FragmentPackage,
    creator: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    for header in &mut output.system_relation_units {
        let node = package
            .fragment()
            .nodes()
            .get(&p::NodeId::new(header.node_id.ok_or_else(|| {
                invalid("system relation node ID is absent")
            })?))
            .ok_or_else(|| invalid("system relation node is absent"))?;
        let p::NodeKind::SystemRelation {
            output_fields,
            data: p::SystemRelationData::Batches { batches, .. },
        } = &node.kind
        else {
            return Err(invalid("system relation data is deferred"));
        };
        let mut types = reserve(node.output.columns.len(), creator, work)?;
        for id in node.output.columns.iter() {
            types.push(
                &package
                    .fragment()
                    .values()
                    .get(id)
                    .ok_or_else(|| invalid("system relation output type is absent"))?
                    .ty,
            );
        }
        let recipe = recipe(output_fields, &types, creator, work)?;
        let mut units = reserve(batches.len(), creator, work)?;
        for batch in batches.iter() {
            validate_values(batch, output_fields, creator, work)?;
            units.push(
                encode_unit(
                    &recipe,
                    batch,
                    &mut Observer {
                        creation: RefCell::new(creator),
                        work: RefCell::new(work),
                    },
                )
                .map_err(codec)?,
            );
        }
        header.units = units;
    }
    Ok(())
}
pub(super) fn decode_units<H>(
    wire: &wire::FragmentPackage,
    nodes: &mut BTreeMap<p::NodeId, p::PhysicalNode>,
    values: &BTreeMap<p::ValueId, p::ValueDef>,
    types: &crate::physical_type_v2::DecodedTypeTable,
    creator: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    for node in nodes.values_mut() {
        if !matches!(node.kind, p::NodeKind::SystemRelation { .. }) {
            continue;
        }
        let mut matching = wire
            .system_relation_units
            .iter()
            .filter(|h| h.node_id == Some(node.id.get()));
        let header = matching
            .next()
            .ok_or_else(|| invalid("system relation units are absent"))?;
        if matching.next().is_some() {
            return Err(invalid("system relation units are duplicated"));
        }
        let mut fields = reserve(header.output_field_ids.len(), creator, work)?;
        for id in &header.output_field_ids {
            fields.push(Arc::clone(
                types
                    .field(*id)
                    .ok_or_else(|| invalid("system relation output field is absent"))?,
            ));
        }
        if fields.len() != node.output.columns.len() {
            return Err(invalid("system relation field count differs from output"));
        }
        let mut declarations = reserve(node.output.columns.len(), creator, work)?;
        for (id, field) in node.output.columns.iter().zip(&fields) {
            let ty = &values
                .get(id)
                .ok_or_else(|| invalid("system relation output value is absent"))?
                .ty;
            if ty.data_type() != field.data_type() || ty.nullable() != field.is_nullable() {
                return Err(invalid("system relation field differs from frozen type"));
            }
            declarations.push(ty);
        }
        let recipe = recipe(&fields, &declarations, creator, work)?;
        request(creator, arc_layout(Layout::new::<Schema>())?)?;
        request(creator, array::<FieldRef, H>(fields.len())?)?;
        request(creator, arc_layout(array::<FieldRef, H>(fields.len())?)?)?;
        let schema = Arc::new(Schema::new(fields.clone()));
        let mut batches = reserve(header.units.len(), creator, work)?;
        let mut rows = 0usize;
        for unit in &header.units {
            let batch = decode_unit(
                &recipe,
                unit,
                &mut Observer {
                    creation: RefCell::new(creator),
                    work: RefCell::new(work),
                },
            )
            .map_err(codec)?;
            if batch.num_rows() == 0 {
                return Err(invalid(
                    "empty system relation snapshot must contain zero batches",
                ));
            }
            validate_values(&batch, &fields, creator, work)?;
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or(CompileControlError::ResourceExhausted)?;
            request(creator, array::<ArrayRef, H>(batch.num_columns())?)?;
            batches.push(
                RecordBatch::try_new_with_options(
                    Arc::clone(&schema),
                    batch.columns().to_vec(),
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
                .map_err(|e| ProjectionFailure::Codec(SystemRelationError::Arrow(e)))?,
            );
        }
        let source = wire
            .fragment
            .as_ref()
            .and_then(|f| f.nodes.iter().find(|n| n.id == node.id.get()))
            .and_then(|n| n.kind.as_ref());
        let Some(wire::physical_node::Kind::SystemRelation(declared)) = source else {
            return Err(invalid("system relation declaration is absent"));
        };
        if declared.batches != batches.len() as u64 || declared.rows != rows as u64 {
            return Err(invalid("system relation batch or row declaration differs"));
        }
        request(creator, arc_layout(array::<FieldRef, H>(fields.len())?)?)?;
        request(
            creator,
            arc_layout(array::<RecordBatch, H>(batches.len())?)?,
        )?;
        node.kind = p::NodeKind::SystemRelation {
            output_fields: Arc::from(fields),
            data: p::SystemRelationData::Batches {
                batches: Arc::from(batches),
                storage: creator.storage_lifetime(),
            },
        };
    }
    for header in &wire.system_relation_units {
        if !header.node_id.is_some_and(|id| {
            nodes
                .get(&p::NodeId::new(id))
                .is_some_and(|n| matches!(n.kind, p::NodeKind::SystemRelation { .. }))
        }) {
            return Err(invalid("system relation units reference a foreign node"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
