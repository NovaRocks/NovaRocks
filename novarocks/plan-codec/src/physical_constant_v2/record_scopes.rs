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

//! Actual independent source, preparation, result and pool-holder lifetimes.
//! Numerical contributions remain on the original record/namespace model.
use super::borrowed_reader::{
    PreparedReader, checked_record_source_header, prepare_reader_from_sources_profile_observed,
};
use super::{ConstantDecodeProjectionLimits, PhysicalConstantCodecError as Error};
use crate::host_projection_v2::ProjectionFailure;
use crate::ipc_flat_stream_v2::{FlatReaderError, progress::IpcReaderProgressFacts};
use arrow::datatypes::Field;
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{
    ConstantPolicy, ConstantPool, ConstantSourceCreationFacts, ConstantSourceOwner,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, sync::Arc};

type Failure<H> = ProjectionFailure<FlatReaderError, H>;

// A stack-only nominal adapter around the genuine factory. It creates no
// scope geometry, capacity, controller or independent completion channel.
struct MappedFactory<'a, F, H> {
    factory: &'a F,
    marker: std::marker::PhantomData<fn() -> H>,
}
struct MappedFactoryScope<S, H>(S, std::marker::PhantomData<fn() -> H>);
impl<S: ValueSchemaCreationScope<H>, H> ValueSchemaCreationScope<Failure<H>>
    for MappedFactoryScope<S, H>
{
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
impl<F: ValueSchemaCreationFactory<H>, H> ValueSchemaCreationFactory<Failure<H>>
    for MappedFactory<'_, F, H>
{
    type Scope<'a>
        = MappedFactoryScope<F::Scope<'a>, H>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, Failure<H>> {
        self.factory
            .begin_scope()
            .map(|scope| MappedFactoryScope(scope, std::marker::PhantomData))
            .map_err(ProjectionFailure::Host)
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, Failure<H>>> {
        self.factory
            .begin_scope_observed(observe)
            .map(|scope| MappedFactoryScope(scope, std::marker::PhantomData))
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

/// Completed conservative construction geometry. Only `retained_new_bytes`
/// belongs to the new pool source contribution; original source B is separate.
/// None of these numbers authorizes capacity or claims allocator precision.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantRecordConstructionFacts {
    pub new_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
    pub geometry_scratch_request_bytes: usize,
    pub geometry_scratch_request_count: usize,
    pub preparation_request_bytes_upper_bound: usize,
    pub retained_new_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}

#[derive(Clone, Copy, Default)]
struct Supplement {
    temporary: usize,
    retained: usize,
    requests: usize,
}
struct RecordModel {
    reader: IpcReaderProgressFacts,
    supplement: Supplement,
    limits: Option<(usize, usize, usize)>,
    preparation_supplement: usize,
    preparing: bool,
}
impl RecordModel {
    fn complete(&self) -> Result<ConstantRecordConstructionFacts, CompileControlError> {
        let add = |a: usize, b: usize| {
            a.checked_add(b)
                .ok_or(CompileControlError::ResourceExhausted)
        };
        Ok(ConstantRecordConstructionFacts {
            new_request_bytes_upper_bound: add(
                self.reader.new_allocation_request_bytes_upper_bound,
                add(self.supplement.temporary, self.supplement.retained)?,
            )?,
            allocation_requests_upper_bound: add(
                self.reader.allocation_request_count_upper_bound,
                self.supplement.requests,
            )?,
            geometry_scratch_request_bytes: self.reader.geometry_scratch_request_bytes,
            geometry_scratch_request_count: self.reader.geometry_scratch_request_count,
            preparation_request_bytes_upper_bound: add(
                self.reader.geometry_scratch_request_bytes,
                self.preparation_supplement,
            )?,
            retained_new_bytes_upper_bound: self.supplement.retained,
            cumulative_library_work_upper_bound: self.reader.cumulative_library_work_upper_bound,
        })
    }
    fn publish<H>(
        &self,
        admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
    ) -> Result<(), Failure<H>> {
        let facts = self.complete()?;
        if let Some((bytes, coexist, work)) = self.limits {
            let live = self
                .reader
                .source_retained_bytes
                .checked_add(facts.new_request_bytes_upper_bound)
                .ok_or(CompileControlError::ResourceExhausted)?;
            if facts.new_request_bytes_upper_bound > bytes
                || live > coexist
                || facts.cumulative_library_work_upper_bound > work
            {
                return Err(CompileControlError::ResourceExhausted.into());
            }
        }
        admit(&facts)
    }
    fn layout<H>(
        &mut self,
        layout: Layout,
        retained: bool,
        admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
    ) -> Result<(), Failure<H>> {
        let bytes = if retained {
            &mut self.supplement.retained
        } else {
            &mut self.supplement.temporary
        };
        *bytes = bytes
            .checked_add(layout.size())
            .ok_or(CompileControlError::ResourceExhausted)?;
        if self.preparing {
            self.preparation_supplement = self
                .preparation_supplement
                .checked_add(layout.size())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        self.supplement.requests = self
            .supplement
            .requests
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.publish(admit)
    }
}

fn begin<'f, H, F: ValueSchemaCreationFactory<H>>(
    factory: &'f F,
    model: &mut RecordModel,
    retained: bool,
    admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<F::Scope<'f>, Failure<H>> {
    work.flush()?;
    factory
        .begin_scope_observed(&mut |layout| model.layout(layout, retained, admit))
        .map_err(|error| match error {
            ValueSchemaScopeBeginFailure::Observer(error) => error,
            ValueSchemaScopeBeginFailure::Creator(error) => ProjectionFailure::Host(error),
        })
}

/// Preparation owns only its temporary geometry marker and precise original
/// source custody. Neither the factory nor its controller can escape.
pub struct ScopedPreparedConstantRecord<'raw, 'source, 'control> {
    reader: PreparedReader<'raw, 'source, 'control>,
    source: ConstantSourceOwner,
    model: RecordModel,
    preparation_storage: ValueTypeStorageLifetime,
}
pub(super) struct ConstantRecordScopes<S> {
    temporary: S,
    array: S,
    pool: S,
}
impl ScopedPreparedConstantRecord<'_, '_, '_> {
    pub(super) fn body_bytes(&self) -> usize {
        self.reader.body_bytes()
    }

    pub fn construction_facts(
        &self,
    ) -> Result<ConstantRecordConstructionFacts, CompileControlError> {
        self.model.complete()
    }
    pub fn materialize_with_factory_in<H, F: ValueSchemaCreationFactory<H>>(
        mut self,
        factory: &F,
        admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(ConstantPool, ConstantRecordConstructionFacts), ProjectionFailure<Error, H>> {
        let scopes = self.prepare_materialization_scopes(factory, admit, work)?;
        self.materialize_with_scopes_in(scopes, admit, work)
    }
    /// The namespace can admit every real scope header before its first Array.
    /// This is the same constructor sequence as the one-record entry.
    pub(super) fn prepare_materialization_scopes<'f, H, F: ValueSchemaCreationFactory<H>>(
        &mut self,
        factory: &'f F,
        admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantRecordScopes<F::Scope<'f>>, ProjectionFailure<Error, H>> {
        self.model.preparing = false;
        let temporary = begin(factory, &mut self.model, false, admit, work).map_err(map_failure)?;
        let array = begin(factory, &mut self.model, true, admit, work).map_err(map_failure)?;
        let pool = begin(factory, &mut self.model, true, admit, work).map_err(map_failure)?;
        Ok(ConstantRecordScopes {
            temporary,
            array,
            pool,
        })
    }
    pub(super) fn materialize_with_scopes_in<H, S: ValueSchemaCreationScope<H>>(
        mut self,
        scopes: ConstantRecordScopes<S>,
        admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(ConstantPool, ConstantRecordConstructionFacts), ProjectionFailure<Error, H>> {
        let ConstantRecordScopes {
            mut temporary,
            mut array,
            mut pool,
        } = scopes;
        let retained_reader = self
            .reader
            .retained_reader_requests()
            .map_err(Error::from)?;
        let value = self
            .reader
            .materialize_with_paired_creator_observed(
                self.source,
                &mut temporary,
                &mut array,
                &mut pool,
                &mut |progress| {
                    self.model.reader = *progress;
                    self.model.publish(admit)
                },
                work,
            )
            .map_err(map_failure)?;
        // The same actual reader author classified these final requests before
        // construction. PoolBacking's actual private layout is quoted by its
        // sole constructor and included once in the original complete bound.
        let mut facts = self.model.complete().map_err(Error::from)?;
        facts.retained_new_bytes_upper_bound = facts
            .retained_new_bytes_upper_bound
            .checked_add(retained_reader)
            .and_then(|bytes| bytes.checked_add(ConstantPool::backing_allocation_layout().size()))
            .ok_or(Error::Control(CompileControlError::ResourceExhausted))?;
        drop(self.preparation_storage);
        Ok((value, facts))
    }
}
fn map_failure<H>(error: Failure<H>) -> ProjectionFailure<Error, H> {
    match error {
        ProjectionFailure::Host(error) => ProjectionFailure::Host(error),
        ProjectionFailure::Codec(error) => ProjectionFailure::Codec(error.into()),
    }
}

pub fn prepare_constant_record_from_sources_input_derived_with_factory_in<
    'raw,
    'source,
    'control,
    H,
    F: ValueSchemaCreationFactory<H>,
>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    source_storage: ValueTypeStorageLifetime,
    source_metadata: &MaterializedFieldNamespace,
    original_source_bytes: usize,
    live_source_bytes: usize,
    factory: &F,
    admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<ScopedPreparedConstantRecord<'raw, 'source, 'control>, ProjectionFailure<Error, H>> {
    prepare_profile(
        record,
        value_type,
        field,
        source_storage,
        source_metadata,
        original_source_bytes,
        live_source_bytes,
        None,
        factory,
        admit,
        work,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The record profile keeps creator scope and caller-owned resource facts explicit."
)]
pub(super) fn prepare_profile<'raw, 'source, 'control, H, F: ValueSchemaCreationFactory<H>>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    source_storage: ValueTypeStorageLifetime,
    source_metadata: &MaterializedFieldNamespace,
    original_source_bytes: usize,
    live_source_bytes: usize,
    profile: Option<(
        ConstantPolicy,
        ConstantDecodeProjectionLimits,
        &VerifierOptions,
    )>,
    factory: &F,
    admit: &mut dyn FnMut(&ConstantRecordConstructionFacts) -> Result<(), Failure<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<ScopedPreparedConstantRecord<'raw, 'source, 'control>, ProjectionFailure<Error, H>> {
    checked_record_source_header(record, original_source_bytes, work)?;
    let limits = profile.map(|(_, limits, _)| {
        if super::recursive(field.data_type()) {
            (
                limits.recursive_reader.max_new_allocation_request_bytes,
                limits
                    .recursive_reader
                    .max_coexisting_source_and_request_bytes,
                limits.recursive_reader.max_cumulative_library_work,
            )
        } else {
            (
                limits.flat_reader.max_new_allocation_request_bytes,
                limits.flat_reader.max_coexisting_source_and_request_bytes,
                limits.flat_reader.max_cumulative_library_work,
            )
        }
    });
    let mut model = RecordModel {
        reader: IpcReaderProgressFacts {
            source_retained_bytes: live_source_bytes,
            ..Default::default()
        },
        supplement: Supplement::default(),
        limits,
        preparation_supplement: 0,
        preparing: true,
    };
    let mut temporary = begin(factory, &mut model, false, admit, work).map_err(map_failure)?;
    let reader = prepare_reader_from_sources_profile_observed(
        record,
        value_type,
        field,
        live_source_bytes,
        profile,
        &mut |progress| {
            model.reader = *progress;
            model.publish(admit)
        },
        Some(&mut |layout| {
            temporary
                .request_layout(layout)
                .map_err(ProjectionFailure::Host)
        }),
        work,
    )
    .map_err(map_failure)?;
    let (geometry_bytes, geometry_count) = reader.geometry_requests();
    let (bytes, count, library_work) = reader.reader_requests();
    model.reader = IpcReaderProgressFacts {
        source_retained_bytes: live_source_bytes,
        geometry_scratch_request_bytes: geometry_bytes,
        geometry_scratch_request_count: geometry_count,
        new_allocation_request_bytes_upper_bound: bytes,
        allocation_request_count_upper_bound: count,
        cumulative_library_work_upper_bound: library_work,
    };
    model.publish(admit).map_err(map_failure)?;
    let base = model.supplement;
    let preparation_base = model.preparation_supplement;
    let (source, source_facts) = ConstantSourceOwner::try_new_with_factory_in(
        field,
        source_metadata,
        original_source_bytes,
        source_storage,
        &MappedFactory {
            factory,
            marker: std::marker::PhantomData,
        },
        &mut |facts: &ConstantSourceCreationFacts| {
            model.supplement = Supplement {
                temporary: base
                    .temporary
                    .checked_add(facts.temporary_request_bytes)
                    .ok_or(CompileControlError::ResourceExhausted)?,
                retained: base
                    .retained
                    .checked_add(facts.retained_request_bytes)
                    .ok_or(CompileControlError::ResourceExhausted)?,
                requests: base
                    .requests
                    .checked_add(facts.allocation_requests)
                    .ok_or(CompileControlError::ResourceExhausted)?,
            };
            model.preparation_supplement = preparation_base
                .checked_add(facts.temporary_request_bytes)
                .and_then(|bytes| bytes.checked_add(facts.retained_request_bytes))
                .ok_or(CompileControlError::ResourceExhausted)?;
            model
                .publish(admit)
                .map_err(novarocks_constant_contract::ConstantCreationFailure::Creator)
        },
        work,
    )
    .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)
    .map_err(map_failure)?;
    // The observer already admitted this completed snapshot before each real
    // allocation; publishing a receipt does not create another request.
    debug_assert_eq!(
        model.supplement.retained,
        base.retained + source_facts.retained_request_bytes
    );
    Ok(ScopedPreparedConstantRecord {
        reader,
        source,
        model,
        preparation_storage: temporary.storage_lifetime(),
    })
}
