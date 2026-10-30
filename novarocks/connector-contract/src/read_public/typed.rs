// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Expansion API: complete execution schema plus independent source facts.
use super::*;
use crate::{ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts};
use novarocks_type_contract::{CompilePhase, PureCompileControl};

#[derive(Clone, Debug)]
pub(super) struct TypedReadSchemas {
    pub(super) execution: ConnectorReadExecutionSchema,
    source: ConnectorSourceSchemaFacts,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl PartialEq for TypedReadSchemas {
    fn eq(&self, other: &Self) -> bool {
        self.execution == other.execution && self.source == other.source
    }
}
impl Eq for TypedReadSchemas {}
impl ConnectorReadPublicFacts {
    pub fn try_new_typed(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        execution: ConnectorReadExecutionSchema,
        source_schema: ConnectorSourceSchemaFacts,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_typed_core(
            source,
            metadata_kind,
            &execution,
            &source_schema,
            storage,
            &mut PlainCopy,
        )
    }
    /// All incoming schema/type/source objects already retain their actual
    /// creator holds. The unchanged source invoice covers private source and
    /// all retained backing; this constructor never replaces it with a floor.
    pub fn try_new_typed_observed(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        execution: &ConnectorReadExecutionSchema,
        source_schema: &ConnectorSourceSchemaFacts,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut context = ObservedCopy::new(source_retained_bytes, admit, work)?;
        Self::try_new_typed_core(
            source,
            metadata_kind,
            execution,
            source_schema,
            storage,
            &mut context,
        )
    }
    /// Construct the outward projection with the original numerical policy and
    /// the actual creator. The private label backing holds only that creator.
    pub fn try_new_typed_with_creator<H>(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        execution: &ConnectorReadExecutionSchema,
        source_schema: &ConnectorSourceSchemaFacts,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, TypedReadCreationFailure<H>> {
        let original = ObservedCopy::new(source_retained_bytes, admit, work)
            .map_err(TypedReadCreationFailure::Contract)?;
        let storage = creation.storage_lifetime();
        let mut copy = CreatedReadCopy { original, creation };
        Self::try_new_typed_core(
            source,
            metadata_kind,
            execution,
            source_schema,
            storage,
            &mut copy,
        )
    }

    /// FE pure preparation observes the original structural/control work. This
    /// adapter mints no memory invoice, allocation grant, or query owner. The
    /// borrowed immutable source objects retain their own original creators.
    pub fn try_new_typed_with_control(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        execution: &ConnectorReadExecutionSchema,
        source_schema: &ConnectorSourceSchemaFacts,
        control: &dyn PureCompileControl,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let mut copy = PureControlledCopy { work: &mut work };
        let result = Self::try_new_typed_core(
            source,
            metadata_kind,
            execution,
            source_schema,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut copy,
        );
        if matches!(result, Err(PureProviderCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    /// Legacy expansion values have no complete schema; callers must refuse
    /// such values at a typed production boundary instead of deriving one.
    pub fn execution_schema(&self) -> Option<&ConnectorReadExecutionSchema> {
        self.typed.as_ref().map(|typed| &typed.execution)
    }
    pub fn source_schema_facts(&self) -> Option<&ConnectorSourceSchemaFacts> {
        self.typed.as_ref().map(|typed| &typed.source)
    }
    fn try_new_typed_core<O: OwnedCopy>(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        execution: &ConnectorReadExecutionSchema,
        source_schema: &ConnectorSourceSchemaFacts,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        context: &mut O,
    ) -> Result<Self, O::Error> {
        let schema = execution.schema();
        let shape = source_schema.shape().schema();
        let count = schema.fields().len();
        completed_law(
            (|| {
                if count > MAX_CONNECTOR_RECIPE_COLUMNS
                    || count != shape.fields().len()
                    || count != execution.bindings().len()
                {
                    return Err(invalid(
                        "typed read execution/source occurrence counts differ",
                    ));
                }
                if metadata_kind.is_some() && source.coverage_evidence().is_empty() {
                    return Err(invalid("metadata read has no frozen coverage evidence"));
                }
                Ok(())
            })(),
            context,
        )?;
        // Existing minimum source facts, never an opaque source-work bound.
        context.source_floor(context.add(
            size_of::<ConnectorReadStaticFacts<ScanColumnId>>(),
            context.add(
                source.input_version().as_bytes().len(),
                source.coverage_evidence().len(),
            )?,
        )?)?;
        context.source_floor(context.mul(count, size_of::<FunctionValueType>())?)?;
        let distribution_keys = match source.properties().distribution() {
            ConnectorReadDistribution::Hash { keys, .. }
            | ConnectorReadDistribution::BucketShuffle { keys, .. } => keys.len(),
            _ => 0,
        };
        context.work(context.add(
            context.mul(
                context.add(distribution_keys, source.properties().ordering().len())?,
                128,
            )?,
            256,
        )?)?;
        let mut budget = SchemaBudget::default();
        completed_law(
            budget.add(
                size_of::<Self>()
                    + source.input_version().as_bytes().len()
                    + source.coverage_evidence().len()
                    + metadata_kind.as_ref().map_or(0, |kind| kind.as_str().len()),
            ),
            context,
        )?;
        // Schema/Field/type allocations are already retained by their explicit
        // creator objects. Only the existing real scratch walks are admitted.
        for input in [schema, shape] {
            completed_law(
                budget.add(size_of::<Schema>() + input.fields().len() * size_of::<Arc<Field>>()),
                context,
            )?;
            for field in input.fields() {
                let nodes = shape_nodes(field.data_type(), context)?;
                budget.field_observed(field, context)?;
                budget.data_type(field.data_type(), nodes, context)?;
            }
        }
        completed_law(
            budget.add(
                ConnectorSourceSchemaFacts::storage_allocation_layout()
                    .map_err(O::Error::from)?
                    .size(),
            ),
            context,
        )?;
        budget.attributes_with_bound_observed(
            source_schema.schema_attributes(),
            source_schema.attribute_work_upper_bound(),
            context,
        )?;
        for field in source_schema.field_facts() {
            context.step()?;
            completed_law(
                budget.add(
                    size_of::<crate::ConnectorSourceFieldFacts>()
                        + crate::ConnectorSourceFieldFacts::storage_allocation_layout()
                            .map_err(O::Error::from)?
                            .size()
                        + std::mem::size_of_val(field.field_path()),
                ),
                context,
            )?;
            context.source_floor(std::mem::size_of_val(field.field_path()))?;
            for _ in field.field_path() {
                context.step()?;
            }
            if let Some(default) = field.initial_default() {
                completed_law(
                    if default.as_json().len()
                        > novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES
                    {
                        Err(exhausted())
                    } else {
                        Ok(())
                    },
                    context,
                )?;
                // Provider has already validated/serialized this typed JSON.
                // Visit the real spelling under the original control meter.
                for _ in default.as_json().as_bytes().chunks(256) {
                    context.step()?;
                }
                completed_law(budget.add(default.as_json().len()), context)?;
                context.source_floor(default.as_json().len())?;
            }
            budget.attributes_with_bound_observed(
                field.attributes(),
                field.attribute_work_upper_bound(),
                context,
            )?;
        }
        let tail = (|| -> Result<(), ConnectorError> {
            let check = |column: &ScanColumnId| {
                if column.index() < count {
                    Ok(())
                } else {
                    Err(invalid(
                        "read property names an absent schema column ordinal",
                    ))
                }
            };
            match source.properties().distribution() {
                ConnectorReadDistribution::Hash { keys, .. }
                | ConnectorReadDistribution::BucketShuffle { keys, .. } => {
                    for key in keys.iter() {
                        check(key)?;
                    }
                    budget.add(keys.len() * size_of::<ScanColumnId>())?;
                }
                _ => {}
            }
            for key in source.properties().ordering() {
                check(key.column())?;
            }
            budget.add(std::mem::size_of_val(source.properties().ordering()))?;
            if let ConnectorReadArtifactCoverage::Exact {
                source_selection_digest,
                content_digest,
                evidence,
            } = source.artifact_coverage()
            {
                if *source_selection_digest == [0; 32]
                    || *content_digest == [0; 32]
                    || evidence.is_empty()
                    || evidence.len() > crate::MAX_READ_COVERAGE_EVIDENCE_BYTES
                {
                    return Err(invalid("read artifact coverage has invalid exact evidence"));
                }
                budget.add(evidence.len())?;
            }
            Ok(())
        })();
        completed_law(tail, context)?;
        // Root labels are a temporary outward compatibility projection only.
        // Full immutable bindings remain the sole execution semantic author.
        context.array::<ValueLogicalType>(count, 1)?;
        context.arc_slice::<ValueLogicalType>(count)?;
        context.work(context.mul(count, 2 * size_of::<ValueLogicalType>())?)?;
        context.flush()?;
        let mut logical = Vec::new();
        context.reserve_exit(logical.try_reserve_exact(count))?;
        for binding in execution.bindings() {
            logical.push(binding.logical_type());
            context.step()?;
        }
        let logical = logical.into_boxed_slice();
        context.step()?;
        context.flush()?;
        let logical_types = logical.into();
        context.step()?;
        context.flush()?;
        let schema = Arc::clone(execution.schema_ref());
        context.step()?;
        Ok(Self {
            source,
            metadata_kind,
            schema,
            logical_types,
            charged_bytes: budget.bytes,
            typed: Some(TypedReadSchemas {
                execution: execution.clone(),
                source: source_schema.clone(),
                _storage: storage,
            }),
            metadata_materializations: None,
        })
    }
}
fn shape_nodes<O: OwnedCopy>(root: &DataType, context: &mut O) -> Result<usize, O::Error> {
    use novarocks_type_contract::{
        MAX_VALUE_TYPE_NODES, ValueTypeVisit, validate_arrow_layout_structure_with_scratch_observed,
    };
    context.work(context.add(
        context.mul(
            MAX_VALUE_TYPE_NODES,
            size_of::<Option<(&DataType, usize)>>() + 128,
        )?,
        128,
    )?)?;
    context.flush()?;
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    context.step()?;
    context.flush()?;
    let mut nodes = 0usize;
    let result = validate_arrow_layout_structure_with_scratch_observed(
        root,
        &mut scratch,
        |event| -> Result<(), LogicalWalkError<O::Error>> {
            if let ValueTypeVisit::TypeNode(_) = event {
                nodes += 1;
            }
            context.step().map_err(LogicalWalkError::Observer)
        },
    );
    match result {
        Err(LogicalWalkError::Observer(cause)) => Err(cause),
        other => completed_law(
            other.map_err(|cause| match cause {
                LogicalWalkError::Law(cause) => value_type_error(cause),
                LogicalWalkError::Observer(_) => unreachable!(),
            }),
            context,
        )
        .map(|()| nodes),
    }
}

#[cfg(test)]
mod tests;

struct PureControlledCopy<'a, 'control> {
    work: &'a mut CompileCheckpoints<'control>,
}
impl OwnedCopy for PureControlledCopy<'_, '_> {
    type Error = PureProviderCompileError<ConnectorError>;
    fn work(&mut self, mut units: usize) -> Result<(), Self::Error> {
        self.work.flush()?;
        while units != 0 {
            let quantum = units.min(256) as u32;
            self.work
                .control()
                .checkpoint(CompilePhase::ProviderValidation, quantum)?;
            units -= quantum as usize;
        }
        Ok(())
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.work.step().map_err(Into::into)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.work.flush().map_err(Into::into)
    }
    fn arithmetic(&self) -> Self::Error {
        exhausted().into()
    }
    fn reserve_exit(
        &mut self,
        result: Result<(), std::collections::TryReserveError>,
    ) -> Result<(), Self::Error> {
        result.map_err(|_| exhausted().into())
    }
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error> {
        self.work(value.len())?;
        Ok(value.to_owned())
    }
    fn metadata_iteration(&mut self, entries: usize, traversals: usize) -> Result<(), Self::Error> {
        self.metadata_iteration_with_bound(entries, traversals, None)
    }
    fn metadata_iteration_with_bound(
        &mut self,
        entries: usize,
        traversals: usize,
        bound: Option<usize>,
    ) -> Result<(), Self::Error> {
        let units = if entries == 0 {
            64
        } else {
            bound.ok_or_else(|| {
                Self::Error::Provider(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "pure source metadata inspection has no declared work bound",
                ))
            })?
        };
        self.work(
            units
                .checked_mul(traversals)
                .ok_or_else(|| self.arithmetic())?,
        )?;
        self.flush()
    }
}

/// Keep the actual creator refusal separate from structural/control failures.
#[derive(Debug)]
pub enum TypedReadCreationFailure<H> {
    Creator(H),
    Contract(PureProviderCompileError<ConnectorError>),
}
impl<H> From<ConnectorError> for TypedReadCreationFailure<H> {
    fn from(error: ConnectorError) -> Self {
        Self::Contract(error.into())
    }
}
struct CreatedReadCopy<'a, 'creation, 'control, H, A> {
    original: ObservedCopy<'a, 'control, A>,
    creation: &'a mut novarocks_type_contract::ValueSchemaCreation<'creation, H>,
}
impl<H, A> OwnedCopy for CreatedReadCopy<'_, '_, '_, H, A>
where
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    type Error = TypedReadCreationFailure<H>;
    fn materializes(&self) -> bool {
        self.original.materializes()
    }
    fn source_invoice(&self) -> Option<usize> {
        self.original.source_invoice()
    }
    fn arithmetic(&self) -> Self::Error {
        Self::Error::Contract(self.original.arithmetic())
    }
    fn request(&mut self, layout: std::alloc::Layout, copies: usize) -> Result<(), Self::Error> {
        self.original
            .request(layout, copies)
            .map_err(Self::Error::Contract)?;
        for _ in 0..copies {
            self.creation
                .request_layout(layout)
                .map_err(Self::Error::Creator)?;
        }
        Ok(())
    }
    fn opaque_model_request(&mut self, bytes: usize, copies: usize) -> Result<(), Self::Error> {
        let layout = std::alloc::Layout::array::<u8>(bytes).map_err(|_| self.arithmetic())?;
        self.original
            .request(layout, copies)
            .map_err(Self::Error::Contract)?;
        if bytes != 0 {
            for _ in 0..copies {
                self.creation
                    .observe_opaque_upper_bound(bytes)
                    .map_err(Self::Error::Creator)?;
            }
        }
        Ok(())
    }
    fn work(&mut self, units: usize) -> Result<(), Self::Error> {
        self.original.work(units).map_err(Self::Error::Contract)
    }
    fn source_floor(&mut self, minimum: usize) -> Result<(), Self::Error> {
        self.original
            .source_floor(minimum)
            .map_err(Self::Error::Contract)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.original.step().map_err(Self::Error::Contract)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.original.flush().map_err(Self::Error::Contract)
    }
    fn begin_copy(&mut self) -> Result<(), Self::Error> {
        self.original.begin_copy().map_err(Self::Error::Contract)
    }
    fn reserve_exit(
        &mut self,
        result: Result<(), std::collections::TryReserveError>,
    ) -> Result<(), Self::Error> {
        self.original
            .reserve_exit(result)
            .map_err(Self::Error::Contract)
    }
    fn spelling(&mut self, value: &str) -> Result<String, Self::Error> {
        // The core observes spelling backing through request before this copy.
        self.original.spelling(value).map_err(Self::Error::Contract)
    }
}
