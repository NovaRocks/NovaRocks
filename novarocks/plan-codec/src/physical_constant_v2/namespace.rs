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

//! Accumulate the actual receiving records before any pool materialization.
//! Stream/schema/scratch preparation retains its preceding owner gates. These
//! request facts are neither allocator usable sizes nor formal MEM grants.

use super::{
    ConstantDecodeProjectionLimits, PhysicalConstantCodecError as Error, finish, record_sources,
    record_sources_captured_observed, recursive,
};
use crate::{
    ipc_flat_stream_v2::{preflight_flat_constant_stream, progress::IpcReaderProgressFacts},
    ipc_recursive_stream_v2::preflight_recursive_constant_stream,
    physical_type_v2::DecodedTypeTable,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPool};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReferenceError};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    ValueSchemaCreationFactory, ValueSchemaCreationScope, ValueSchemaScopeBeginFailure,
    ValueTypeStorageLifetime,
};
use std::{alloc::Layout, mem, sync::Arc};

/// The caller allocates a share of its whole-fragment envelope to this stage.
/// There is no independent default pool-count policy or implicit wallet.
#[derive(Clone, Copy, Debug)]
pub struct ConstantNamespaceProjectionLimits {
    pub max_records: usize,
    pub max_preparation_request_bytes: usize,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}

type SourceMetadata =
    novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace;

#[derive(Clone, Copy)]
struct BoundedNamespace<'a> {
    source: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &'a VerifierOptions,
}
impl<'a> BoundedNamespace<'a> {
    fn record(
        self,
    ) -> (
        ConstantPolicy,
        ConstantDecodeProjectionLimits,
        &'a VerifierOptions,
    ) {
        (self.policy, self.record_limits, self.verifier)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantNamespaceResourceFacts {
    pub record_count: usize,
    pub source_retained_bytes: usize,
    pub prepared_storage_request_bytes: usize,
    pub geometry_scratch_request_bytes: usize,
    pub preparation_request_bytes: usize,
    pub pool_table_request_bytes_upper_bound: usize,
    pub reader_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
    pub additional_preparation_request_bytes: usize,
    pub namespace_scope_request_bytes_upper_bound: usize,
    pub retained_new_allocation_request_bytes_upper_bound: usize,
}

trait NamespaceFailure:
    crate::ipc_flat_stream_v2::ReaderFailure
    + From<Error>
    + From<ConstantReferenceError>
    + From<novarocks_constant_contract::ConstantTableError>
{
}
impl<E> NamespaceFailure for E where
    E: crate::ipc_flat_stream_v2::ReaderFailure
        + From<Error>
        + From<ConstantReferenceError>
        + From<novarocks_constant_contract::ConstantTableError>
{
}
struct NamespaceCreation<'a,'port,E> {
    creation:&'a mut novarocks_type_contract::ValueSchemaCreation<'port,E>,
    metadata:Option<&'a novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
}
type HostFailure<H> = crate::host_projection_v2::ProjectionFailure<Error, H>;
struct MappedNamespaceFactory<'a, F, H> {
    factory: &'a F,
    marker: std::marker::PhantomData<fn() -> H>,
}
struct MappedNamespaceScope<S, H>(S, std::marker::PhantomData<fn() -> H>);
impl<S: ValueSchemaCreationScope<H>, H> ValueSchemaCreationScope<HostFailure<H>>
    for MappedNamespaceScope<S, H>
{
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), HostFailure<H>> {
        self.0.request_layout(layout).map_err(HostFailure::Host)
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), HostFailure<H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(HostFailure::Host)
    }
}
impl<F: ValueSchemaCreationFactory<H>, H> ValueSchemaCreationFactory<HostFailure<H>>
    for MappedNamespaceFactory<'_, F, H>
{
    type Scope<'a>
        = MappedNamespaceScope<F::Scope<'a>, H>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, HostFailure<H>> {
        self.factory
            .begin_scope()
            .map(|scope| MappedNamespaceScope(scope, std::marker::PhantomData))
            .map_err(HostFailure::Host)
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, HostFailure<H>>> {
        self.factory
            .begin_scope_observed(observe)
            .map(|scope| MappedNamespaceScope(scope, std::marker::PhantomData))
            .map_err(|error| match error {
                ValueSchemaScopeBeginFailure::Observer(error) => {
                    ValueSchemaScopeBeginFailure::Observer(error)
                }
                ValueSchemaScopeBeginFailure::Creator(error) => {
                    ValueSchemaScopeBeginFailure::Creator(HostFailure::Host(error))
                }
            })
    }
}
fn map_admission<H>(error: crate::host_projection_v2::AdmissionRefusal<H>) -> HostFailure<H> {
    match error {
        crate::host_projection_v2::AdmissionRefusal::Control(error) => error.into(),
        crate::host_projection_v2::AdmissionRefusal::Host(error) => HostFailure::Host(error),
    }
}
type Admit<'a, E = CompileControlError> =
    dyn FnMut(&ConstantNamespaceResourceFacts) -> Result<(), E> + 'a;
fn gate_parent<E: From<CompileControlError>>(
    facts: &ConstantNamespaceResourceFacts,
    limits: Option<ConstantNamespaceProjectionLimits>,
    admit: &mut Admit<'_, E>,
) -> Result<(), E> {
    if let Some(limits) = limits {
        for (actual, maximum) in [
            (facts.record_count, limits.max_records),
            (
                facts.preparation_request_bytes,
                limits.max_preparation_request_bytes,
            ),
            (
                facts.new_allocation_request_bytes_upper_bound,
                limits.max_new_allocation_request_bytes,
            ),
            (
                facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
            ),
            (
                facts.cumulative_library_work_upper_bound,
                limits.max_cumulative_library_work,
            ),
        ] {
            if actual > maximum {
                return Err(CompileControlError::ResourceExhausted.into());
            }
        }
    }
    admit(facts)
}
// One child snapshot replaces its previous contribution. Its complete reader
// requests already contain geometry; the preparation sublimit views it once.
fn with_child(
    base: ConstantNamespaceResourceFacts,
    child: &IpcReaderProgressFacts,
) -> Result<ConstantNamespaceResourceFacts, CompileControlError> {
    let sum = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(CompileControlError::ResourceExhausted)
    };
    let mut facts = base;
    facts.geometry_scratch_request_bytes = sum(
        base.geometry_scratch_request_bytes,
        child.geometry_scratch_request_bytes,
    )?;
    facts.preparation_request_bytes = sum(
        facts.prepared_storage_request_bytes,
        add_control(
            facts.geometry_scratch_request_bytes,
            base.additional_preparation_request_bytes,
        )?,
    )?;
    facts.reader_request_bytes_upper_bound = sum(
        base.reader_request_bytes_upper_bound,
        child.new_allocation_request_bytes_upper_bound,
    )?;
    facts.allocation_request_count_upper_bound = sum(
        base.allocation_request_count_upper_bound,
        child.allocation_request_count_upper_bound,
    )?;
    facts.new_allocation_request_bytes_upper_bound = sum(
        sum(
            facts.prepared_storage_request_bytes,
            facts.pool_table_request_bytes_upper_bound,
        )?,
        sum(
            facts.reader_request_bytes_upper_bound,
            facts.namespace_scope_request_bytes_upper_bound,
        )?,
    )?;
    facts.coexisting_source_and_request_bytes_upper_bound = sum(
        facts.source_retained_bytes,
        facts.new_allocation_request_bytes_upper_bound,
    )?;
    facts.cumulative_library_work_upper_bound = sum(
        base.cumulative_library_work_upper_bound,
        child.cumulative_library_work_upper_bound,
    )?;
    Ok(facts)
}

use super::borrowed_reader::{PreparedReader, prepare_reader_from_sources_profile_observed};

use super::record_scopes::{
    ConstantRecordConstructionFacts, ConstantRecordScopes, ScopedPreparedConstantRecord,
};
struct PreparedRecord<'raw, 'table, 'control> {
    id: ConstantPoolId,
    reader: PreparedRecordReader<'raw, 'table, 'control>,
}
enum PreparedRecordReader<'raw, 'table, 'control> {
    Original {
        reader: PreparedReader<'raw, 'table, 'control>,
        source: Option<novarocks_constant_contract::ConstantSourceOwner>,
    },
    Scoped {
        reader: ScopedPreparedConstantRecord<'raw, 'table, 'control>,
        prepared: ConstantRecordConstructionFacts,
    },
}
fn add_control(a: usize, b: usize) -> Result<usize, CompileControlError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted)
}
fn with_namespace_temporary_layout(
    mut facts: ConstantNamespaceResourceFacts,
    layout: Layout,
) -> Result<ConstantNamespaceResourceFacts, CompileControlError> {
    if layout.size() == 0 {
        return Ok(facts);
    }
    facts.namespace_scope_request_bytes_upper_bound = add_control(
        facts.namespace_scope_request_bytes_upper_bound,
        layout.size(),
    )?;
    facts.additional_preparation_request_bytes =
        add_control(facts.additional_preparation_request_bytes, layout.size())?;
    facts.preparation_request_bytes = add_control(facts.preparation_request_bytes, layout.size())?;
    facts.new_allocation_request_bytes_upper_bound = add_control(
        facts.new_allocation_request_bytes_upper_bound,
        layout.size(),
    )?;
    facts.coexisting_source_and_request_bytes_upper_bound = add_control(
        facts.coexisting_source_and_request_bytes_upper_bound,
        layout.size(),
    )?;
    facts.allocation_request_count_upper_bound =
        add_control(facts.allocation_request_count_upper_bound, 1)?;
    Ok(facts)
}
fn with_scoped_child(
    base: ConstantNamespaceResourceFacts,
    child: &ConstantRecordConstructionFacts,
) -> Result<ConstantNamespaceResourceFacts, CompileControlError> {
    let progress = IpcReaderProgressFacts {
        geometry_scratch_request_bytes: child.geometry_scratch_request_bytes,
        geometry_scratch_request_count: child.geometry_scratch_request_count,
        new_allocation_request_bytes_upper_bound: child.new_request_bytes_upper_bound,
        allocation_request_count_upper_bound: child.allocation_requests_upper_bound,
        cumulative_library_work_upper_bound: child.cumulative_library_work_upper_bound,
        ..Default::default()
    };
    let mut facts = with_child(base, &progress)?;
    let extra = child
        .preparation_request_bytes_upper_bound
        .checked_sub(child.geometry_scratch_request_bytes)
        .ok_or(CompileControlError::ResourceExhausted)?;
    facts.additional_preparation_request_bytes =
        add_control(base.additional_preparation_request_bytes, extra)?;
    facts.preparation_request_bytes = add_control(facts.preparation_request_bytes, extra)?;
    Ok(facts)
}
fn with_materialization_prefix(
    base: ConstantNamespaceResourceFacts,
    prepared: &ConstantRecordConstructionFacts,
    child: &ConstantRecordConstructionFacts,
) -> Result<ConstantNamespaceResourceFacts, CompileControlError> {
    let bytes = child
        .new_request_bytes_upper_bound
        .checked_sub(prepared.new_request_bytes_upper_bound)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let requests = child
        .allocation_requests_upper_bound
        .checked_sub(prepared.allocation_requests_upper_bound)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let mut facts = base;
    facts.reader_request_bytes_upper_bound =
        add_control(facts.reader_request_bytes_upper_bound, bytes)?;
    facts.new_allocation_request_bytes_upper_bound =
        add_control(facts.new_allocation_request_bytes_upper_bound, bytes)?;
    facts.coexisting_source_and_request_bytes_upper_bound =
        add_control(facts.coexisting_source_and_request_bytes_upper_bound, bytes)?;
    facts.allocation_request_count_upper_bound =
        add_control(facts.allocation_request_count_upper_bound, requests)?;
    Ok(facts)
}
fn flatten_scoped_failure<E: From<Error>>(
    error: crate::host_projection_v2::ProjectionFailure<Error, E>,
) -> E {
    match error {
        crate::host_projection_v2::ProjectionFailure::Host(error) => error,
        crate::host_projection_v2::ProjectionFailure::Codec(error) => error.into(),
    }
}

/// Private construction freezes the same checked stream, type, policy and
/// numeric model. The original control is retained through consumption.
pub struct PreparedConstantNamespace<'raw, 'table, 'control> {
    records: Vec<PreparedRecord<'raw, 'table, 'control>>,
    facts: ConstantNamespaceResourceFacts,
    control: &'control dyn PureCompileControl,
    preparation_storage: ValueTypeStorageLifetime,
    limits: Option<ConstantNamespaceProjectionLimits>,
}
impl PreparedConstantNamespace<'_, '_, '_> {
    /// Each body's extent comes from the accepted record geometry.
    pub fn checked_record_body_bytes(&self) -> impl Iterator<Item = (ConstantPoolId, usize)> + '_ {
        self.records.iter().map(|record| {
            let bytes = match &record.reader {
                PreparedRecordReader::Original { reader, .. } => reader.body_bytes(),
                PreparedRecordReader::Scoped { reader, .. } => reader.body_bytes(),
            };
            (record.id, bytes)
        })
    }

    pub const fn facts(&self) -> &ConstantNamespaceResourceFacts {
        &self.facts
    }

    /// Reuses the original ConstantPools duplicate author. No second pool map
    /// or byte-based backing deduplication exists. Package closure is later.
    pub fn materialize(self) -> Result<ConstantPools, Error> {
        let _storage_lifetime = &self.preparation_storage;
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = self
            .materialize_core::<Error, crate::value_type_table::resources::NoFactory>(
                None, None, None, None, &mut work,
            )
            .map(|(pools, _)| pools);
        finish(work, result)
    }
    pub(crate) fn materialize_in(
        self,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPools, Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(shape(
                "constant namespace caller work has a different original control",
            ));
        }
        admit(&self.facts)?;
        self.materialize_core::<Error, crate::value_type_table::resources::NoFactory>(
            Some(&mut |facts| admit(facts).map_err(Error::from)),
            None,
            None,
            None,
            work,
        )
        .map(|(pools, _)| pools)
    }
    pub fn materialize_with_creator_in<H>(
        self,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        admit: &mut dyn FnMut(
            &ConstantNamespaceResourceFacts,
        )
            -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPools, crate::host_projection_v2::ProjectionFailure<Error, H>> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(
                shape("constant namespace caller work has a different original control").into(),
            );
        }
        creation.with_error_mapping(
            crate::host_projection_v2::ProjectionFailure::Host,
            |mapped| {
                let mut capture = |facts: &ConstantNamespaceResourceFacts| {
                    admit(facts).map_err(|error| match error {
                        crate::host_projection_v2::AdmissionRefusal::Control(cause) => cause.into(),
                        crate::host_projection_v2::AdmissionRefusal::Host(error) => {
                            crate::host_projection_v2::ProjectionFailure::Host(error)
                        }
                    })
                };
                capture(&self.facts)?;
                self.materialize_core::<_, crate::value_type_table::resources::NoFactory>(
                    Some(&mut capture),
                    Some(mapped),
                    None,
                    None,
                    work,
                )
                .map(|(pools, _)| pools)
            },
        )
    }
    /// Materialize all previously checked records with the same actual factory.
    /// The table loan is independent of each selected pool/array loan.
    pub fn materialize_with_factory_in<H, F: ValueSchemaCreationFactory<H>>(
        self,
        factory: &F,
        admit: &mut dyn FnMut(
            &ConstantNamespaceResourceFacts,
        )
            -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(ConstantPools, ConstantNamespaceResourceFacts), HostFailure<H>> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(
                shape("constant namespace caller work has a different original control").into(),
            );
        }
        let limits = self.limits;
        let mut capture =
            |facts: &ConstantNamespaceResourceFacts| admit(facts).map_err(map_admission);
        capture(&self.facts)?;
        let mapped = MappedNamespaceFactory {
            factory,
            marker: std::marker::PhantomData,
        };
        self.materialize_core(Some(&mut capture), None, Some(&mapped), limits, work)
    }
    fn materialize_core<E: NamespaceFailure, F: ValueSchemaCreationFactory<E>>(
        mut self,
        mut admit: Option<&mut Admit<'_, E>>,
        mut creation: Option<&mut novarocks_type_contract::ValueSchemaCreation<'_, E>>,
        factory: Option<&F>,
        limits: Option<ConstantNamespaceProjectionLimits>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(ConstantPools, ConstantNamespaceResourceFacts), E> {
        (|| {
            if self.records.iter().any(|record| {
                matches!(
                    &record.reader,
                    PreparedRecordReader::Original {
                        source: Some(_),
                        ..
                    }
                )
            }) && creation.is_none()
            {
                return Err(shape(
                    "constant namespace materialization requires its supplied creator",
                )
                .into());
            }
            if let Some(creator) = creation.as_deref_mut() {
                creator
                    .observe_opaque_upper_bound(self.facts.pool_table_request_bytes_upper_bound)?;
            }
            let mut scope_queue = None;
            let mut queue_storage = ValueTypeStorageLifetime::pure();
            if let Some(factory) = factory.filter(|_| !self.records.is_empty()) {
                if self
                    .records
                    .iter()
                    .any(|record| !matches!(record.reader, PreparedRecordReader::Scoped { .. }))
                {
                    return Err(shape(
                        "factory namespace requires its prepared original source custody",
                    )
                    .into());
                }
                let parent = admit
                    .as_deref_mut()
                    .ok_or_else(|| E::from(shape("constant namespace factory parent is absent")))?;
                work.flush()?;
                let mut queue_scope = factory
                    .begin_scope_observed(&mut |layout| {
                        self.facts = with_namespace_temporary_layout(self.facts, layout)?;
                        gate_parent(&self.facts, limits, parent)
                    })
                    .map_err(|error| match error {
                        ValueSchemaScopeBeginFailure::Observer(error) => error,
                        ValueSchemaScopeBeginFailure::Creator(error) => error,
                    })?;
                let layout =
                    Layout::array::<ConstantRecordScopes<F::Scope<'_>>>(self.records.len())
                        .map_err(|_| CompileControlError::ResourceExhausted)?;
                self.facts = with_namespace_temporary_layout(self.facts, layout)?;
                gate_parent(&self.facts, limits, parent)?;
                work.flush()?;
                if layout.size() != 0 {
                    queue_scope.request_layout(layout)?;
                }
                let mut scopes = Vec::new();
                let reserved = scopes.try_reserve_exact(self.records.len());
                crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
                for record in &mut self.records {
                    let PreparedRecordReader::Scoped { reader, prepared } = &mut record.reader
                    else {
                        unreachable!("all factory records checked")
                    };
                    let base = self.facts;
                    let mut complete = base;
                    let record_scopes = reader
                        .prepare_materialization_scopes(
                            factory,
                            &mut |child| {
                                complete = with_materialization_prefix(base, prepared, child)?;
                                gate_parent(&complete, limits, parent)
                                    .map_err(crate::host_projection_v2::ProjectionFailure::Host)
                            },
                            work,
                        )
                        .map_err(flatten_scoped_failure)?;
                    self.facts = complete;
                    *prepared = reader.construction_facts()?;
                    scopes.push(record_scopes);
                    work.step()?;
                }
                queue_storage = queue_scope.storage_lifetime();
                scope_queue = Some(scopes.into_iter());
            }
            let mut table_scope = if let Some(factory) =
                factory.filter(|_| !self.records.is_empty())
            {
                work.flush()?;
                let scope = factory
                    .begin_scope_observed(&mut |layout| {
                        self.facts.namespace_scope_request_bytes_upper_bound = add_control(
                            self.facts.namespace_scope_request_bytes_upper_bound,
                            layout.size(),
                        )?;
                        self.facts.new_allocation_request_bytes_upper_bound = add_control(
                            self.facts.new_allocation_request_bytes_upper_bound,
                            layout.size(),
                        )?;
                        self.facts.coexisting_source_and_request_bytes_upper_bound = add_control(
                            self.facts.coexisting_source_and_request_bytes_upper_bound,
                            layout.size(),
                        )?;
                        self.facts.allocation_request_count_upper_bound =
                            add_control(self.facts.allocation_request_count_upper_bound, 1)?;
                        self.facts.retained_new_allocation_request_bytes_upper_bound = add_control(
                            self.facts.pool_table_request_bytes_upper_bound,
                            layout.size(),
                        )?;
                        gate_parent(
                            &self.facts,
                            limits,
                            admit.as_deref_mut().ok_or_else(|| {
                                E::from(shape("constant namespace factory parent is absent"))
                            })?,
                        )
                    })
                    .map_err(|error| match error {
                        ValueSchemaScopeBeginFailure::Observer(error) => error,
                        ValueSchemaScopeBeginFailure::Creator(error) => error,
                    })?;
                Some(scope)
            } else {
                None
            };
            if let Some(scope) = table_scope.as_mut() {
                work.flush()?;
                scope
                    .observe_opaque_upper_bound(self.facts.pool_table_request_bytes_upper_bound)?;
            }
            let mut pools = ConstantPools::empty();
            for record in self.records {
                work.step()?;
                work.flush()?;
                let pool = match record.reader {
                    PreparedRecordReader::Scoped { reader, prepared } => {
                        let scopes = scope_queue.as_mut().and_then(Iterator::next).ok_or_else(
                            || {
                                E::from(shape(
                                    "scoped constant namespace materialization scopes are absent",
                                ))
                            },
                        )?;
                        let parent = admit.as_deref_mut().ok_or_else(|| {
                            E::from(shape(
                                "scoped constant namespace requires its original parent",
                            ))
                        })?;
                        let base = self.facts;
                        let mut complete = base;
                        let (pool, final_facts) = reader
                            .materialize_with_scopes_in(
                                scopes,
                                &mut |child| {
                                    complete = with_materialization_prefix(base, &prepared, child)?;
                                    gate_parent(&complete, limits, parent)
                                        .map_err(crate::host_projection_v2::ProjectionFailure::Host)
                                },
                                work,
                            )
                            .map_err(flatten_scoped_failure)?;
                        self.facts = complete;
                        self.facts.retained_new_allocation_request_bytes_upper_bound = add_control(
                            self.facts.retained_new_allocation_request_bytes_upper_bound,
                            final_facts.retained_new_bytes_upper_bound,
                        )?;
                        pool
                    }
                    PreparedRecordReader::Original { reader, source } => {
                        if let Some(parent) = &mut admit {
                            let (geometry_bytes, geometry_count) = reader.geometry_requests();
                            let (reader_bytes, reader_count, reader_work) =
                                reader.reader_requests();
                            let mut capture = |progress: &IpcReaderProgressFacts| {
                                if progress.geometry_scratch_request_bytes > geometry_bytes
                                    || progress.geometry_scratch_request_count > geometry_count
                                    || progress.new_allocation_request_bytes_upper_bound
                                        > reader_bytes
                                    || progress.allocation_request_count_upper_bound > reader_count
                                    || progress.cumulative_library_work_upper_bound > reader_work
                                {
                                    return Err(E::from(CompileControlError::ResourceExhausted));
                                }
                                parent(&self.facts)
                            };
                            if let Some(creator) = creation.as_deref_mut() {
                                let source = source.ok_or_else(|| {
                                    shape("constant namespace creator record lacks source custody")
                                })?;
                                reader
                                    .materialize_with_creator_observed(
                                        source,
                                        creator,
                                        &mut |progress| {
                                            capture(progress).map_err(
                                                crate::host_projection_v2::ProjectionFailure::Host,
                                            )
                                        },
                                        work,
                                    )
                                    .map_err(|error| match error {
                                        crate::host_projection_v2::ProjectionFailure::Host(
                                            error,
                                        ) => error,
                                        crate::host_projection_v2::ProjectionFailure::Codec(
                                            error,
                                        ) => error.into(),
                                    })?
                            } else {
                                reader.materialize_pure_observed(&mut capture, work)?
                            }
                        } else {
                            reader.materialize(work.control())?
                        }
                    }
                };
                work.flush()?;
                let inserted = pools.insert(record.id, pool);
                work.step()?;
                inserted?;
            }
            if let Some(scope) = table_scope {
                pools = pools.with_storage_lifetime(scope.storage_lifetime());
            }
            drop(scope_queue);
            drop(queue_storage);
            Ok((pools, self.facts))
        })()
    }
}

impl From<ConstantReferenceError> for Error {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(error)
            | ConstantReferenceError::Constant(ConstantError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::Reference(error),
        }
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| shape("constant namespace resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| shape("constant namespace resource product overflow"))
}
fn cap(
    actual: usize,
    limit: usize,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    work.step()?;
    if actual > limit {
        return Err(shape(message));
    }
    Ok(())
}

/// The shared locked author uses these actual key/value member layouts.
fn table_node_layout() -> Result<Layout, Error> {
    crate::btree_resources_v2::node_layout::<ConstantPoolId, ConstantPool>().map_err(shape)
}

fn initial_facts(
    count: usize,
    source: usize,
    limits: ConstantNamespaceProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantNamespaceResourceFacts, Error> {
    cap(
        count,
        limits.max_records,
        "constant namespace count envelope exceeded",
        work,
    )?;
    let model_locked = LOCKED_FAMILY && LOCKED_TOOLCHAIN;
    work.step()?;
    if !model_locked {
        return Err(shape("constant namespace source model drift"));
    }
    let (storage, table, requests, bookkeeping) = initial_parts(count)?;
    cap(
        storage,
        limits.max_preparation_request_bytes,
        "constant namespace preparation envelope exceeded",
        work,
    )?;
    cap(
        requests,
        limits.max_new_allocation_request_bytes,
        "constant namespace request envelope exceeded",
        work,
    )?;
    cap(
        add(source, requests)?,
        limits.max_coexisting_source_and_request_bytes,
        "constant namespace coexistence envelope exceeded",
        work,
    )?;
    cap(
        bookkeeping,
        limits.max_cumulative_library_work,
        "constant namespace work envelope exceeded",
        work,
    )?;
    initial_header(count, source, storage, table, requests, bookkeeping)
}

// Pure extraction of the original numerical author. Neither the caller nor
// the old facade duplicates the table-layout or insertion-work model.
fn initial_parts(count: usize) -> Result<(usize, usize, usize, usize), Error> {
    let storage = Layout::array::<PreparedRecord<'_, '_, '_>>(count)
        .map_err(|_| shape("constant namespace prepared storage layout is unrepresentable"))?
        .size();
    // Only insertion occurs. Every retained node has at least one entry; no
    // node is removed or reallocated. Cumulative node requests <= input count.
    let table = mul(table_node_layout()?.size(), count)?;
    let requests = add(storage, table)?;
    let levels = if count == 0 {
        0
    } else {
        (usize::BITS - count.leading_zeros()) as usize + 1
    };
    let per_level = add(
        mul(
            11,
            add(
                mem::size_of::<ConstantPoolId>(),
                mem::size_of::<ConstantPool>(),
            )?,
        )?,
        12 * mem::size_of::<usize>() + 11,
    )?;
    let table_work = mul(count, mul(levels, per_level)?)?;
    let bookkeeping = add(requests, add(table_work, mul(count, 32)?)?)?;
    Ok((storage, table, requests, bookkeeping))
}

fn initial_header(
    count: usize,
    source: usize,
    storage: usize,
    table: usize,
    requests: usize,
    bookkeeping: usize,
) -> Result<ConstantNamespaceResourceFacts, Error> {
    Ok(ConstantNamespaceResourceFacts {
        record_count: count,
        source_retained_bytes: source,
        prepared_storage_request_bytes: storage,
        preparation_request_bytes: storage,
        pool_table_request_bytes_upper_bound: table,
        allocation_request_count_upper_bound: add(count, usize::from(count != 0))?,
        new_allocation_request_bytes_upper_bound: requests,
        coexisting_source_and_request_bytes_upper_bound: add(source, requests)?,
        cumulative_library_work_upper_bound: bookkeeping,
        ..Default::default()
    })
}

/// Raw DTO/type-table backing, source capacity and earlier schema/framing
/// stages remain the whole-package host's obligation. This model admits
/// prepared storage before reserve, aggregate live geometry before each
/// geometry allocation, and all receiving pools/table requests before the
/// first materialization. Each record's official metadata and reader model are
/// consumed once; inactive profiles never supply a fallback.
pub fn prepare_constant_namespace<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = prepare_core::<_, crate::value_type_table::resources::NoFactory>(
        records,
        types,
        Some(BoundedNamespace {
            source: source_retained_bytes,
            policy,
            record_limits,
            limits,
            verifier,
        }),
        None,
        None,
        None,
        None,
        &mut work,
    );
    finish(work, result)
}

#[cfg(test)]
pub(crate) fn prepare_constant_namespace_in<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    prepare_core::<_, crate::value_type_table::resources::NoFactory>(
        records,
        types,
        Some(BoundedNamespace {
            source: source_retained_bytes,
            policy,
            record_limits,
            limits,
            verifier,
        }),
        Some(&mut |facts| admit(facts).map_err(Error::from)),
        None,
        None,
        None,
        work,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen decode contract keeps source, policy, limits, creator, admission, and checkpoints explicit."
)]
pub fn prepare_constant_namespace_with_creator_in<'raw, 'table, 'control, H>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    source_metadata:Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    admit: &mut dyn FnMut(
        &ConstantNamespaceResourceFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<
    PreparedConstantNamespace<'raw, 'table, 'control>,
    crate::host_projection_v2::ProjectionFailure<Error, H>,
> {
    creation.with_error_mapping(
        crate::host_projection_v2::ProjectionFailure::Host,
        |mapped| {
            prepare_core::<_, crate::value_type_table::resources::NoFactory>(
                records,
                types,
                Some(BoundedNamespace {
                    source: source_retained_bytes,
                    policy,
                    record_limits,
                    limits,
                    verifier,
                }),
                Some(&mut |facts| {
                    admit(facts).map_err(|error| match error {
                        crate::host_projection_v2::AdmissionRefusal::Control(cause) => cause.into(),
                        crate::host_projection_v2::AdmissionRefusal::Host(error) => {
                            crate::host_projection_v2::ProjectionFailure::Host(error)
                        }
                    })
                }),
                Some(NamespaceCreation {
                    creation: mapped,
                    metadata: source_metadata,
                }),
                None,
                None,
                work,
            )
        },
    )
}

/// All-record preparation on the actual factory. The original reader model,
/// source law, receiving profiles and cumulative parent gate are unchanged.
#[expect(
    clippy::too_many_arguments,
    reason = "The frozen decode contract keeps source metadata, policy, limits, factory, admission, and checkpoints explicit."
)]
pub fn prepare_constant_namespace_with_factory_in<
    'raw,
    'table,
    'control,
    H,
    F: ValueSchemaCreationFactory<H>,
>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_retained_bytes: usize,
    source_metadata: Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    factory: &F,
    admit: &mut dyn FnMut(
        &ConstantNamespaceResourceFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, HostFailure<H>> {
    let mapped = MappedNamespaceFactory {
        factory,
        marker: std::marker::PhantomData,
    };
    prepare_core(
        records,
        types,
        Some(BoundedNamespace {
            source: source_retained_bytes,
            policy,
            record_limits,
            limits,
            verifier,
        }),
        Some(&mut |facts| admit(facts).map_err(map_admission)),
        None,
        Some(&mapped),
        source_metadata,
        work,
    )
}

/// Prepare borrowed records using their actual source storage and checked geometry.
pub fn prepare_constant_namespace_input_derived_in<'raw, 'table, 'control>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, Error> {
    prepare_core::<_, crate::value_type_table::resources::NoFactory>(
        records,
        types,
        None,
        Some(&mut |facts| admit(facts).map_err(Error::from)),
        None,
        None,
        None,
        work,
    )
}

pub fn prepare_constant_namespace_input_derived_with_creator_in<'raw, 'table, 'control, H>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_metadata: Option<&SourceMetadata>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    admit: &mut dyn FnMut(
        &ConstantNamespaceResourceFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, HostFailure<H>> {
    creation.with_error_mapping(
        crate::host_projection_v2::ProjectionFailure::Host,
        |mapped| {
            prepare_core::<_, crate::value_type_table::resources::NoFactory>(
                records,
                types,
                None,
                Some(&mut |facts| admit(facts).map_err(map_admission)),
                Some(NamespaceCreation {
                    creation: mapped,
                    metadata: source_metadata,
                }),
                None,
                None,
                work,
            )
        },
    )
}

pub fn prepare_constant_namespace_input_derived_with_factory_in<
    'raw,
    'table,
    'control,
    H,
    F: ValueSchemaCreationFactory<H>,
>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    source_metadata: Option<&SourceMetadata>,
    factory: &F,
    admit: &mut dyn FnMut(
        &ConstantNamespaceResourceFacts,
    ) -> Result<(), crate::host_projection_v2::AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, HostFailure<H>> {
    let mapped = MappedNamespaceFactory {
        factory,
        marker: std::marker::PhantomData,
    };
    prepare_core(
        records,
        types,
        None,
        Some(&mut |facts| admit(facts).map_err(map_admission)),
        None,
        Some(&mapped),
        source_metadata,
        work,
    )
}

fn visible_record_storage(
    records: &[wire::IpcConstantPool],
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, Error> {
    let mut extent = mem::size_of_val(records);
    for record in records {
        extent = add(extent, record.arrow_ipc.capacity())?;
        work.step()?;
    }
    Ok(extent)
}

#[expect(
    clippy::too_many_arguments,
    reason = "The internal composer keeps source, policy, limits, creator, admission, and progress owners distinct."
)]
fn prepare_core<'raw, 'table, 'control, E: NamespaceFailure, F: ValueSchemaCreationFactory<E>>(
    records: &'raw [wire::IpcConstantPool],
    types: &'table DecodedTypeTable,
    bounded: Option<BoundedNamespace<'_>>,
    mut admit: Option<&mut Admit<'_, E>>,
    mut creation: Option<NamespaceCreation<'_, '_, E>>,
    factory: Option<&F>,
    source_metadata: Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespace<'raw, 'table, 'control>, E> {
    (|| {
        let limits = bounded.map(|profile| profile.limits);
        let source_retained_bytes = match bounded {
            Some(profile) => profile.source,
            None => visible_record_storage(records, work)?,
        };
        let mut facts = if let Some(parent) = &mut admit {
            if !LOCKED_FAMILY || !LOCKED_TOOLCHAIN {
                return Err(shape("constant namespace source model drift").into());
            }
            let parts =
                initial_parts(records.len()).map_err(|_| CompileControlError::ResourceExhausted)?;
            let known = initial_header(
                records.len(),
                source_retained_bytes,
                parts.0,
                parts.1,
                parts.2,
                parts.3,
            )
            .map_err(|_| CompileControlError::ResourceExhausted)?;
            gate_parent(&known, limits, *parent)?;
            known
        } else {
            initial_facts(
                records.len(),
                source_retained_bytes,
                limits.expect("the legacy plain constructor supplies its explicit profile"),
                work,
            )?
        };
        if bounded.is_some() {
            cap(
                visible_record_storage(records, work)?,
                source_retained_bytes,
                "constant namespace source invoice excludes retained record storage",
                work,
            )?;
        }
        work.flush()?;
        if let Some(creator) = creation.as_mut() {
            let layout = Layout::array::<PreparedRecord<'_, '_, '_>>(records.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            if layout.size() != 0 {
                creator.creation.request_layout(layout)?;
            }
        }
        let mut preparation_scope = if let Some(factory) = factory.filter(|_| !records.is_empty()) {
            Some(
                factory
                    .begin_scope_observed(&mut |layout| {
                        facts = with_namespace_temporary_layout(facts, layout)?;
                        gate_parent(
                            &facts,
                            limits,
                            admit.as_deref_mut().ok_or_else(|| {
                                E::from(shape("constant namespace factory parent is absent"))
                            })?,
                        )
                    })
                    .map_err(|error| match error {
                        ValueSchemaScopeBeginFailure::Observer(error) => error,
                        ValueSchemaScopeBeginFailure::Creator(error) => error,
                    })?,
            )
        } else {
            None
        };
        if let Some(scope) = preparation_scope.as_mut() {
            let layout = Layout::array::<PreparedRecord<'_, '_, '_>>(records.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            if layout.size() != 0 {
                work.flush()?;
                scope.request_layout(layout)?;
            }
        }
        let mut prepared = Vec::new();
        let reserved = prepared.try_reserve_exact(records.len());
        crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
        for record in records {
            let record_source =
                bounded.map_or(record.arrow_ipc.capacity(), |profile| profile.source);
            if let Some(factory) = factory {
                let base = facts;
                let live_source = if bounded.is_some() {
                    add(record_source, facts.preparation_request_bytes)?
                } else {
                    record.arrow_ipc.capacity()
                };
                let parent = admit
                    .as_deref_mut()
                    .ok_or_else(|| E::from(shape("constant namespace factory parent is absent")))?;
                let mut completed = base;
                let mut frozen = None;
                record_sources_captured_observed(
                    record,
                    types,
                    bounded.map(|profile| profile.source),
                    &mut |value_type, field, work| {
                        let reader = super::record_scopes::prepare_profile(
                            record,
                            value_type,
                            field.field(),
                            field.storage_lifetime(),
                            source_metadata.ok_or_else(|| {
                                E::from(shape(
                                    "constant namespace factory source metadata is absent",
                                ))
                            })?,
                            record_source,
                            live_source,
                            bounded.map(BoundedNamespace::record),
                            factory,
                            &mut |child| {
                                completed = with_scoped_child(base, child)?;
                                gate_parent(&completed, limits, parent)
                                    .map_err(crate::host_projection_v2::ProjectionFailure::Host)
                            },
                            work,
                        )
                        .map_err(flatten_scoped_failure)?;
                        let prepared = reader.construction_facts()?;
                        frozen = Some((reader, prepared));
                        Ok::<(), E>(())
                    },
                    work,
                )?;
                let (reader, receipt) = frozen.ok_or_else(|| {
                    E::from(shape(
                        "constant namespace factory record source was not captured",
                    ))
                })?;
                facts = completed;
                prepared.push(PreparedRecord {
                    id: ConstantPoolId::new(record.id),
                    reader: PreparedRecordReader::Scoped {
                        reader,
                        prepared: receipt,
                    },
                });
                work.step()?;
                continue;
            }
            if let Some(parent) = &mut admit {
                let base = facts;
                let live_source = if bounded.is_some() {
                    add(record_source, facts.preparation_request_bytes)
                        .map_err(|_| CompileControlError::ResourceExhausted)?
                } else {
                    record.arrow_ipc.capacity()
                };
                let mut snapshot = IpcReaderProgressFacts::default();
                let (reader, source) = prepare_record_reader_observed(
                    record,
                    types,
                    bounded.map(|profile| profile.source),
                    live_source,
                    bounded.map(BoundedNamespace::record),
                    &mut |progress| {
                        snapshot.geometry_scratch_request_bytes = snapshot
                            .geometry_scratch_request_bytes
                            .max(progress.geometry_scratch_request_bytes);
                        snapshot.geometry_scratch_request_count = snapshot
                            .geometry_scratch_request_count
                            .max(progress.geometry_scratch_request_count);
                        snapshot.allocation_request_count_upper_bound = snapshot
                            .allocation_request_count_upper_bound
                            .max(progress.allocation_request_count_upper_bound);
                        snapshot.new_allocation_request_bytes_upper_bound = snapshot
                            .new_allocation_request_bytes_upper_bound
                            .max(progress.new_allocation_request_bytes_upper_bound);
                        snapshot.cumulative_library_work_upper_bound = snapshot
                            .cumulative_library_work_upper_bound
                            .max(progress.cumulative_library_work_upper_bound);
                        gate_parent(&with_child(base, &snapshot)?, limits, *parent)
                    },
                    creation.as_mut(),
                    work,
                )?;
                let (geometry_bytes, geometry_count) = reader.geometry_requests();
                let (reader_bytes, reader_count, reader_work) = reader.reader_requests();
                // The original complete author must cover every prepared prefix.
                if snapshot.geometry_scratch_request_bytes > geometry_bytes
                    || snapshot.geometry_scratch_request_count > geometry_count
                    || snapshot.new_allocation_request_bytes_upper_bound > reader_bytes
                    || snapshot.allocation_request_count_upper_bound > reader_count
                    || snapshot.cumulative_library_work_upper_bound > reader_work
                {
                    return Err(shape(
                        "constant reader complete projection omits an admitted preparation prefix",
                    )
                    .into());
                }
                snapshot.geometry_scratch_request_bytes = geometry_bytes;
                snapshot.geometry_scratch_request_count = geometry_count;
                snapshot.new_allocation_request_bytes_upper_bound = reader_bytes;
                snapshot.allocation_request_count_upper_bound = reader_count;
                snapshot.cumulative_library_work_upper_bound = reader_work;
                facts = with_child(base, &snapshot)?;
                gate_parent(&facts, limits, *parent)?;
                prepared.push(PreparedRecord {
                    id: ConstantPoolId::new(record.id),
                    reader: PreparedRecordReader::Original { reader, source },
                });
                work.step()?;
                continue;
            }
            let profile =
                bounded.expect("the legacy plain constructor supplies its explicit profile");
            let (policy, record_limits, verifier) = profile.record();
            let limits = profile.limits;
            let (value_type, field) = record_sources(record, types, source_retained_bytes, work)?;
            let live_source = add(source_retained_bytes, facts.preparation_request_bytes)?;
            work.flush()?;
            let reader = if recursive(field.data_type()) {
                let mut stream_limits = record_limits.recursive_stream;
                // Intersect the original explicit record envelope with the
                // caller's remaining namespace preparation share. The geometry
                // owner checks this before its actual Vec reserve.
                stream_limits.batch.max_geometry_request_bytes = stream_limits
                    .batch
                    .max_geometry_request_bytes
                    .min(limits.max_preparation_request_bytes - facts.preparation_request_bytes)
                    .min(
                        limits.max_new_allocation_request_bytes
                            - facts.new_allocation_request_bytes_upper_bound,
                    )
                    .min(
                        limits.max_coexisting_source_and_request_bytes
                            - facts.coexisting_source_and_request_bytes_upper_bound,
                    );
                let stream = preflight_recursive_constant_stream(
                    &record.arrow_ipc,
                    field,
                    stream_limits,
                    verifier,
                    work.control(),
                )?;
                work.flush()?;
                PreparedReader::Recursive(stream.prepare_pool_borrowed(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    record_limits.recursive_reader,
                    work.control(),
                )?)
            } else {
                let stream = preflight_flat_constant_stream(
                    &record.arrow_ipc,
                    field,
                    record_limits.flat_stream,
                    verifier,
                    work.control(),
                )?;
                work.flush()?;
                PreparedReader::Flat(stream.prepare_pool_borrowed(
                    Arc::clone(field),
                    value_type,
                    live_source,
                    policy,
                    record_limits.flat_reader,
                    work.control(),
                )?)
            };
            work.flush()?;
            let (geometry_bytes, _) = reader.geometry_requests();
            let (reader_bytes, reader_count, reader_work) = reader.reader_requests();
            facts.geometry_scratch_request_bytes =
                add(facts.geometry_scratch_request_bytes, geometry_bytes)?;
            facts.preparation_request_bytes = add(
                facts.prepared_storage_request_bytes,
                facts.geometry_scratch_request_bytes,
            )?;
            facts.reader_request_bytes_upper_bound =
                add(facts.reader_request_bytes_upper_bound, reader_bytes)?;
            facts.allocation_request_count_upper_bound =
                add(facts.allocation_request_count_upper_bound, reader_count)?;
            facts.new_allocation_request_bytes_upper_bound = add(
                add(
                    facts.prepared_storage_request_bytes,
                    facts.pool_table_request_bytes_upper_bound,
                )?,
                facts.reader_request_bytes_upper_bound,
            )?;
            facts.coexisting_source_and_request_bytes_upper_bound = add(
                source_retained_bytes,
                facts.new_allocation_request_bytes_upper_bound,
            )?;
            facts.cumulative_library_work_upper_bound =
                add(facts.cumulative_library_work_upper_bound, reader_work)?;
            cap(
                facts.preparation_request_bytes,
                limits.max_preparation_request_bytes,
                "constant namespace preparation envelope exceeded",
                work,
            )?;
            cap(
                facts.new_allocation_request_bytes_upper_bound,
                limits.max_new_allocation_request_bytes,
                "constant namespace request envelope exceeded",
                work,
            )?;
            cap(
                facts.coexisting_source_and_request_bytes_upper_bound,
                limits.max_coexisting_source_and_request_bytes,
                "constant namespace coexistence envelope exceeded",
                work,
            )?;
            cap(
                facts.cumulative_library_work_upper_bound,
                limits.max_cumulative_library_work,
                "constant namespace work envelope exceeded",
                work,
            )?;
            prepared.push(PreparedRecord {
                id: ConstantPoolId::new(record.id),
                reader: PreparedRecordReader::Original {
                    reader,
                    source: None,
                },
            });
            work.step()?;
        }
        Ok(PreparedConstantNamespace {
            records: prepared,
            facts,
            control: work.control(),
            preparation_storage: preparation_scope
                .map(|scope| scope.storage_lifetime())
                .unwrap_or_else(ValueTypeStorageLifetime::pure),
            limits,
        })
    })()
}

pub fn decode_constant_namespace(
    records: &[wire::IpcConstantPool],
    types: &DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    record_limits: ConstantDecodeProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<ConstantPools, Error> {
    prepare_constant_namespace(
        records,
        types,
        source_retained_bytes,
        policy,
        record_limits,
        limits,
        verifier,
        control,
    )?
    .materialize()
}

fn prepare_record_reader_observed<'raw, 'table, 'control, E: NamespaceFailure>(
    record: &'raw wire::IpcConstantPool,
    types: &'table DecodedTypeTable,
    original_source: Option<usize>,
    live_source: usize,
    profile: Option<(
        ConstantPolicy,
        ConstantDecodeProjectionLimits,
        &VerifierOptions,
    )>,
    admit: &mut dyn FnMut(&IpcReaderProgressFacts) -> Result<(), E>,
    mut creation: Option<&mut NamespaceCreation<'_, '_, E>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<
    (
        PreparedReader<'raw, 'table, 'control>,
        Option<novarocks_constant_contract::ConstantSourceOwner>,
    ),
    E,
> {
    let mut reader = None;
    record_sources_captured_observed(
        record,
        types,
        original_source,
        &mut |value_type, field, work| {
            let original_field = field.field();
            let prepared = if let Some(creator) = creation.as_mut() {
                let prepared = prepare_reader_from_sources_profile_observed(
                    record,
                    value_type,
                    original_field,
                    live_source,
                    profile,
                    admit,
                    Some(&mut |layout| creator.creation.request_layout(layout)),
                    work,
                )?;
                let source =
                    novarocks_constant_contract::ConstantSourceOwner::try_new_with_creator_in(
                        original_field,
                        creator.metadata.ok_or_else(||shape("constant source metadata is missing its original creator namespace"))?,
                        original_source.unwrap_or_else(|| record.arrow_ipc.capacity()),
                        field.storage_lifetime(),
                        creator.creation,
                        work,
                    )
                    .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?;
                (prepared, Some(source))
            } else {
                (
                    prepare_reader_from_sources_profile_observed(
                        record,
                        value_type,
                        original_field,
                        live_source,
                        profile,
                        admit,
                        None,
                        work,
                    )?,
                    None,
                )
            };
            reader = Some(prepared);
            Ok::<(), E>(())
        },
        work,
    )?;
    reader.ok_or_else(|| {
        shape("constant reader was not prepared from its captured original sources").into()
    })
}

impl From<novarocks_constant_contract::ConstantTableError> for Error {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
