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

//! Actual schema entities and temporary construction on the sole emitter.
use super::*;
use crate::physical_type_v2::SourceFieldBorrow;
use novarocks_constant_contract::{ConstantCreationFailure, ConstantSourceOwner};
use novarocks_type_contract::{
    ValueSchemaCreationFactory, ValueSchemaCreationScope, ValueSchemaScopeBeginFailure,
};

type Failure<H> = ProjectionFailure<Error, H>;

pub(super) trait SchemaEmission<H> {
    fn output(&mut self, count: usize, work: &mut CompileCheckpoints<'_>)
    -> Result<(), Failure<H>>;
    fn schema(&mut self, count: usize, work: &mut CompileCheckpoints<'_>)
    -> Result<(), Failure<H>>;
    fn field(
        &mut self,
        original: SourceFieldBorrow<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>>;
    fn fields(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueTypeStorageLifetime, Failure<H>>;
    fn boxed(
        &mut self,
        values: &[(u32, MaterializedSchema)],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>>;
}
// The original pure/single-creator body has no factory or scope constructor.
// It preserves its existing observation sequence and source responsibility.
pub(super) struct UnscopedEmission(pub ValueTypeStorageLifetime);
impl SchemaEmission<std::convert::Infallible> for UnscopedEmission {
    fn output(
        &mut self,
        _: usize,
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<std::convert::Infallible>> {
        Ok(())
    }
    fn schema(
        &mut self,
        _: usize,
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<std::convert::Infallible>> {
        Ok(())
    }
    fn field(
        &mut self,
        _: SourceFieldBorrow<'_>,
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<std::convert::Infallible>> {
        Ok(())
    }
    fn fields(
        &mut self,
        _: usize,
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueTypeStorageLifetime, Failure<std::convert::Infallible>> {
        Ok(self.0.clone())
    }
    fn boxed(
        &mut self,
        _: &[(u32, MaterializedSchema)],
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<std::convert::Infallible>> {
        Ok(())
    }
}

fn extra<H>(
    model: &mut Model,
    layout: Layout,
    source: usize,
    fields: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut Admission<'_>,
) -> Result<(), Failure<H>> {
    model.layout_request(layout, 1).map_err(Error::from)?;
    admit_model(model, source, fields, limits, admit)?;
    Ok(())
}
fn begin<'a, H, F: ValueSchemaCreationFactory<H>>(
    factory: &'a F,
    model: &mut Model,
    source: usize,
    fields: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<F::Scope<'a>, Failure<H>> {
    work.flush()?;
    factory
        .begin_scope_observed(&mut |layout| extra(model, layout, source, fields, limits, admit))
        .map_err(|failure| match failure {
            ValueSchemaScopeBeginFailure::Observer(error) => error,
            ValueSchemaScopeBeginFailure::Creator(error) => ProjectionFailure::Host(error),
        })
}
fn request<H>(
    scope: &mut impl ValueSchemaCreationScope<H>,
    layout: Layout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure<H>> {
    if layout.size() != 0 {
        work.flush()?;
        scope
            .request_layout(layout)
            .map_err(ProjectionFailure::Host)?;
    }
    Ok(())
}
struct ScopedEmission<'a, 'm, 'admit, H, F: ValueSchemaCreationFactory<H> + 'a> {
    factory: &'a F,
    model: &'m mut Model,
    source: usize,
    field_count: usize,
    limits: Option<NodeProjectionLimits>,
    admit: &'m mut Admission<'admit>,
    temporary: F::Scope<'a>,
    container: F::Scope<'a>,
    current: Option<F::Scope<'a>>,
    // Only exact individual backing loans. This Vec is temporary; no result
    // stores its backing or an owning reference to the complete type table.
    originals: Vec<ValueTypeStorageLifetime>,
    schema_start: usize,
    marker: std::marker::PhantomData<fn() -> H>,
}
impl<H, F: ValueSchemaCreationFactory<H>> SchemaEmission<H> for ScopedEmission<'_, '_, '_, H, F> {
    fn output(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        request(
            &mut self.container,
            Layout::array::<(u32, MaterializedSchema)>(count)
                .map_err(|_| CompileControlError::ResourceExhausted)?,
            work,
        )
    }
    fn schema(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        self.schema_start = self.originals.len();
        self.current = Some(begin(
            self.factory,
            self.model,
            self.source,
            self.field_count,
            self.limits,
            self.admit,
            work,
        )?);
        request(
            &mut self.temporary,
            Layout::array::<Arc<Field>>(count)
                .map_err(|_| CompileControlError::ResourceExhausted)?,
            work,
        )
    }
    fn field(
        &mut self,
        original: SourceFieldBorrow<'_>,
        _: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        self.originals.push(original.storage_lifetime());
        Ok(())
    }
    fn fields(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ValueTypeStorageLifetime, Failure<H>> {
        let mut scope = self
            .current
            .take()
            .expect("schema scope started before its fields");
        request(&mut scope, fields_layout(count)?, work)?;
        let own = scope.storage_lifetime();
        let storage = own.retaining_flat_observed(
            self.originals[self.schema_start..].iter().map(Ok),
            |layout| {
                extra(
                    self.model,
                    layout,
                    self.source,
                    self.field_count,
                    self.limits,
                    self.admit,
                )?;
                scope
                    .request_layout(layout)
                    .map_err(ProjectionFailure::Host)
            },
            work,
        )?;
        Ok(storage)
    }
    fn boxed(
        &mut self,
        values: &[(u32, MaterializedSchema)],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Failure<H>> {
        // The original model already covers the complete possible trim. Its
        // unchanged operation upper is charged to this container alone.
        let bound = bytes::<(u32, MaterializedSchema)>(values.len())?;
        if bound != 0 {
            work.flush()?;
            self.container
                .observe_opaque_upper_bound(bound)
                .map_err(ProjectionFailure::Host)?;
        }
        Ok(())
    }
}
// Only nominal error projection; the genuine factory remains the sole scope
// constructor. Observer errors can carry the original schema/model failure.
struct ReceiptFactory<'a, F>(&'a F);
struct ReceiptScope<S>(S);
impl<H, S: ValueSchemaCreationScope<H>> ValueSchemaCreationScope<Failure<H>> for ReceiptScope<S> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), Failure<H>> {
        self.0
            .request_layout(layout)
            .map_err(ProjectionFailure::Host)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), Failure<H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(ProjectionFailure::Host)
    }
}
impl<H, F: ValueSchemaCreationFactory<H>> ValueSchemaCreationFactory<Failure<H>>
    for ReceiptFactory<'_, F>
{
    type Scope<'a>
        = ReceiptScope<F::Scope<'a>>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, Failure<H>> {
        self.0
            .begin_scope()
            .map(ReceiptScope)
            .map_err(ProjectionFailure::Host)
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, Failure<H>>> {
        self.0
            .begin_scope_observed(observe)
            .map(ReceiptScope)
            .map_err(|error| match error {
                ValueSchemaScopeBeginFailure::Observer(error) => {
                    ValueSchemaScopeBeginFailure::Observer(error)
                }
                ValueSchemaScopeBeginFailure::Creator(error) => {
                    ValueSchemaScopeBeginFailure::Creator(ProjectionFailure::Host(error))
                }
            })
    }
}
fn source_failure<H>(failure: ConstantCreationFailure<Failure<H>>) -> Failure<H> {
    match failure {
        ConstantCreationFailure::Creator(error) => error,
        ConstantCreationFailure::Resource(error) => {
            ProjectionFailure::Codec(Error::ConstructionResource(error))
        }
        ConstantCreationFailure::SourceMetadata(error) => {
            ProjectionFailure::Codec(Error::SourceMetadata(error))
        }
        ConstantCreationFailure::Constant(novarocks_constant_contract::ConstantError::Control(
            error,
        )) => error.into(),
        ConstantCreationFailure::Constant(error) => {
            ProjectionFailure::Codec(Error::SourceCustody(error))
        }
    }
}

/// Decode every schema with the original namespace preflight/index/emitter.
/// Each schema retains only its own Fields backing and exact reachable Field
/// loans. The index/emission scratch and final namespace container are separate.
pub fn decode_schemas_with_factory_in<'loan, 'control, H, F: ValueSchemaCreationFactory<H>>(
    definitions: &'loan [wire::SchemaDefinition],
    types: &'loan DecodedTypeTable,
    original_source_bytes: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    factory: &F,
    admit: &mut Admission<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<DecodedSchemas<'loan, 'control>, Failure<H>> {
    let limits = limits.into();
    let (mut model, _) = decode_preflight(
        definitions,
        types,
        original_source_bytes,
        limits,
        admit,
        work,
    )?;
    // Header/shape/unused validation above precedes even scope-header requests.
    let mut temporary = begin(
        factory,
        &mut model,
        original_source_bytes,
        types.field_count(),
        limits,
        admit,
        work,
    )?;
    request(
        &mut temporary,
        Layout::array::<usize>(definitions.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?,
        work,
    )?;
    let index = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, work)?;
    let index_storage = temporary.storage_lifetime();
    // Index backing remains live until namespace lookup ends. Emission scratch
    // has a distinct scope and is released when construction returns.
    let mut temporary = begin(
        factory,
        &mut model,
        original_source_bytes,
        types.field_count(),
        limits,
        admit,
        work,
    )?;
    let container = begin(
        factory,
        &mut model,
        original_source_bytes,
        types.field_count(),
        limits,
        admit,
        work,
    )?;
    let count = definitions
        .iter()
        .try_fold(0usize, |count, entry| add(count, entry.field_ids.len()))?;
    let loan_layout = Layout::array::<ValueTypeStorageLifetime>(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if loan_layout.size() != 0 {
        extra(
            &mut model,
            loan_layout,
            original_source_bytes,
            types.field_count(),
            limits,
            admit,
        )?;
        request(&mut temporary, loan_layout, work)?;
    }
    let originals = resources::reserve(count, work).map_err(Error::from)?;
    let mut emitter = ScopedEmission {
        factory,
        model: &mut model,
        source: original_source_bytes,
        field_count: types.field_count(),
        limits,
        admit,
        temporary,
        container,
        current: None,
        originals,
        schema_start: 0,
        marker: std::marker::PhantomData,
    };
    let wire_definitions = definitions;
    let (definitions, retained_bytes) = decode_emit(&mut emitter, definitions, types, work)?;
    let storage = emitter.container.storage_lifetime();
    // Receipt source roots are the same exact original Fields used by emission.
    // Their finite original backing loans are captured at its sole lookup.
    let metadata = if let Some(original) = types.metadata_namespace() {
        // The existing receipt author checks the actual sorted borrowed row
        // table once before lookup. This is real source inspection work even
        // for an empty projection, and is independent of allocation bytes.
        emitter.model.delegated_work = add(emitter.model.delegated_work, original.fields().len())?;
        admit_model(
            emitter.model,
            original_source_bytes,
            types.field_count(),
            limits,
            emitter.admit,
        )?;
        let prefix = *emitter.model;
        let mut observe = |facts: &novarocks_constant_contract::ConstantSourceCreationFacts| {
            emitter.model.requested = prefix
                .requested
                .checked_add(facts.temporary_request_bytes)
                .and_then(|bytes| bytes.checked_add(facts.retained_request_bytes))
                .ok_or(CompileControlError::ResourceExhausted)?;
            emitter.model.requests = prefix
                .requests
                .checked_add(facts.allocation_requests)
                .ok_or(CompileControlError::ResourceExhausted)?;
            admit_model(
                emitter.model,
                original_source_bytes,
                types.field_count(),
                limits,
                emitter.admit,
            )
            .map(|_| ())
            .map_err(|error| ConstantCreationFailure::Creator(ProjectionFailure::Codec(error)))
        };
        let roots = definitions
            .iter()
            .flat_map(|(_, schema)| schema.schema().fields().iter());
        let roots = roots
            .zip(emitter.originals.iter())
            .map(|(field, storage)| (field.as_ref(), storage));
        let (metadata, _) = ConstantSourceOwner::receipt_namespace_with_factory_in(
            roots,
            original,
            &ReceiptFactory(factory),
            &mut observe,
            work,
        )
        .map_err(source_failure)?;
        Some(metadata)
    } else {
        None
    };
    let facts = admit_model(
        emitter.model,
        original_source_bytes,
        types.field_count(),
        limits,
        emitter.admit,
    )?;
    drop(emitter);
    Ok(DecodedSchemas {
        wire_definitions,
        definitions,
        retained_bytes,
        types,
        control: work.control(),
        source_bytes: original_source_bytes,
        index,
        facts,
        storage,
        metadata,
        _index_storage: index_storage,
    })
}
