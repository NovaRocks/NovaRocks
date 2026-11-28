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

//! Array-output materialization after complete immutable flat stream checking.
//! The request/work projection is an input to the original preparation host;
//! it does not implement Account/grant/allocation-origin/free authorization.

use super::{
    FlatConstantStream, FlatPoolResourceError, FlatPoolResourceProjection,
    progress::{Admission, IpcReaderProgressFacts},
    reader_allocations, reader_diagnostics, reader_work,
    resource_work::{CapturedWork, ResourceCountFacts},
};
use crate::physical_type_v2::TypeCodecError;
use arrow::{
    datatypes::{Field, Schema},
    ipc::reader::read_record_batch,
};
use arrow_buffer::Buffer;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

/// Explicit admitted host envelopes. They do not create a separate wallet.
#[derive(Clone, Copy, Debug)]
pub struct FlatReaderProjectionLimits {
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}

/// Conservative allocation requests (not allocator usable bytes/RSS) and
/// cumulative byte/element/header visits for this actual reader/pool path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatReaderResourceFacts {
    pub source_retained_bytes: usize,
    pub payload_request_bytes_upper_bound: usize,
    pub structural_request_bytes_upper_bound: usize,
    pub constant_owner_request_bytes_upper_bound: usize,
    pub retained_reader_request_bytes_upper_bound: usize,
    pub retained_pool_data_request_bytes_upper_bound: usize,
    pub retained_payload_request_bytes_upper_bound: usize,
    pub diagnostic_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
    pub pool: FlatPoolResourceProjection,
}

#[derive(Debug)]
pub enum FlatReaderError {
    Projection(FlatPoolResourceError),
    Arrow(arrow::error::ArrowError),
}
impl From<FlatPoolResourceError> for FlatReaderError {
    fn from(error: FlatPoolResourceError) -> Self {
        Self::Projection(error)
    }
}
impl From<novarocks_type_contract::CompileControlError> for FlatReaderError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Projection(FlatPoolResourceError::Control(error))
    }
}
impl std::fmt::Display for FlatReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Projection(e) => e.fmt(f),
            Self::Arrow(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for FlatReaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Projection(source) => Some(source),
            Self::Arrow(source) => Some(source),
        }
    }
}
impl From<TypeCodecError> for FlatReaderError {
    fn from(error: TypeCodecError) -> Self {
        Self::Projection(error.into())
    }
}
impl From<novarocks_constant_contract::ConstantError> for FlatReaderError {
    fn from(error: novarocks_constant_contract::ConstantError) -> Self {
        Self::Projection(error.into())
    }
}
impl From<novarocks_type_contract::ValueTypeError> for FlatReaderError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Projection(error.into())
    }
}
impl From<novarocks_type_contract::CarrierParameterError> for FlatReaderError {
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::Projection(error.into())
    }
}

impl<H> From<novarocks_type_contract::CompileControlError>
    for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>
{
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
impl<H> From<TypeCodecError> for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H> {
    fn from(error: TypeCodecError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
impl<H> From<FlatPoolResourceError>
    for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>
{
    fn from(error: FlatPoolResourceError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
impl<H> From<novarocks_constant_contract::ConstantError>
    for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>
{
    fn from(error: novarocks_constant_contract::ConstantError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
impl<H> From<novarocks_type_contract::ValueTypeError>
    for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>
{
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
impl<H> From<novarocks_type_contract::CarrierParameterError>
    for crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>
{
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::Codec(FlatReaderError::from(error))
    }
}
pub(crate) trait ReaderResourceFailure:
    super::progress::ReaderGeometryFailure
    + From<FlatPoolResourceError>
    + From<novarocks_constant_contract::ConstantError>
    + From<novarocks_type_contract::CarrierParameterError>
{
}
impl<E> ReaderResourceFailure for E where
    E: super::progress::ReaderGeometryFailure
        + From<FlatPoolResourceError>
        + From<novarocks_constant_contract::ConstantError>
        + From<novarocks_type_contract::CarrierParameterError>
{
}
pub(crate) trait ReaderFailure: ReaderResourceFailure + From<FlatReaderError> {}
impl<E> ReaderFailure for E where E: ReaderResourceFailure + From<FlatReaderError> {}
fn shape(message: &'static str) -> FlatPoolResourceError {
    FlatPoolResourceError::Shape(TypeCodecError::InvalidShape(message))
}
fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b)
        .ok_or_else(|| shape("flat reader resource sum overflow"))
}
fn cap(actual: usize, limit: usize, message: &'static str) -> Result<(), FlatPoolResourceError> {
    if actual > limit {
        Err(shape(message))
    } else {
        Ok(())
    }
}

struct PreparedParts<'v> {
    field: Arc<Field>,
    value_type: Cow<'v, FunctionValueType>,
    policy: ConstantPolicy,
    facts: FlatReaderResourceFacts,
    progress: Option<IpcReaderProgressFacts>,
}

/// Sealed preparation of the same checked stream. Geometry/source admission
/// precedes this owner; its facts do not authorize host memory or later work.
pub(crate) struct PreparedFlatReader<'a, 'f, 'v, 'c> {
    original_control: Option<&'c dyn PureCompileControl>,
    stream: FlatConstantStream<'a, 'f>,
    parts: PreparedParts<'v>,
}
impl PreparedFlatReader<'_, '_, '_, '_> {
    pub(crate) fn body_bytes(&self) -> usize {
        self.stream.batch_body().len()
    }

    pub(crate) fn facts(&self) -> &FlatReaderResourceFacts {
        &self.parts.facts
    }
    pub(crate) fn geometry_scratch_request_bytes(&self) -> usize {
        0
    }
    pub(crate) fn geometry_scratch_request_count(&self) -> usize {
        0
    }

    #[cfg(test)]
    pub(crate) fn materialize_in(
        self,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, FlatReaderError> {
        let Some(original) = self.original_control else {
            return Err(shape("flat reader requires caller-owned preparation").into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(shape("flat reader belongs to another original control").into());
        }
        let mut adapter =
            |facts: &IpcReaderProgressFacts| admit(facts).map_err(FlatReaderError::from);
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, &mut adapter);
        admission.seed(
            self.parts
                .progress
                .ok_or_else(|| shape("flat reader is missing caller-owned facts"))?,
        )?;
        self.stream
            .materialize_parts_core(self.parts, Some(&mut admission), None, None, work)
    }

    pub(crate) fn materialize_pure_observed<E: ReaderFailure>(
        self,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let Some(original) = self.original_control else {
            return Err(FlatReaderError::from(shape(
                "flat reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(FlatReaderError::from(shape(
                "flat reader belongs to another original control",
            ))
            .into());
        }
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(
            self.parts
                .progress
                .ok_or_else(|| shape("flat reader is missing caller-owned facts"))?,
        )?;
        self.stream
            .materialize_parts_core(self.parts, Some(&mut admission), None, None, work)
    }

    pub(crate) fn materialize_with_creator_observed<H>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<
            '_,
            crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>,
        >,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, crate::host_projection_v2::ProjectionFailure<FlatReaderError, H>>
    {
        creation.with_error_mapping(
            crate::host_projection_v2::ProjectionFailure::Host,
            |mapped| self.materialize_with_scope_observed(source, mapped, admit, work),
        )
    }
    pub(crate) fn materialize_with_scope_observed<E: ReaderFailure>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<E>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let Some(original) = self.original_control else {
            return Err(FlatReaderError::from(shape(
                "flat reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(FlatReaderError::from(shape(
                "flat reader belongs to another original control",
            ))
            .into());
        }
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(
            self.parts
                .progress
                .ok_or_else(|| shape("flat reader is missing caller-owned facts"))?,
        )?;
        self.stream.materialize_parts_core(
            self.parts,
            Some(&mut admission),
            Some(source),
            Some(crate::ipc_flat_stream_v2::reader_creation::ReaderCreation::Single(creation)),
            work,
        )
    }
    pub(crate) fn materialize_with_paired_creator_observed<H>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        pool: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<
            '_,
            crate::host_projection_v2::ProjectionFailure<
                crate::ipc_flat_stream_v2::FlatReaderError,
                H,
            >,
        >,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<
        ConstantPool,
        crate::host_projection_v2::ProjectionFailure<crate::ipc_flat_stream_v2::FlatReaderError, H>,
    > {
        let Some(original) = self.original_control else {
            return Err(FlatReaderError::from(shape(
                "flat reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(FlatReaderError::from(shape(
                "flat reader belongs to another original control",
            ))
            .into());
        }
        let mut temporary = crate::ipc_flat_stream_v2::reader_creation::MappedScope(temporary);
        let mut retained = crate::ipc_flat_stream_v2::reader_creation::MappedScope(retained);
        let mut pool = crate::ipc_flat_stream_v2::reader_creation::MappedScope(pool);
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(
            self.parts
                .progress
                .ok_or_else(|| shape("flat reader is missing caller-owned facts"))?,
        )?;
        self.stream.materialize_parts_core(
            self.parts,
            Some(&mut admission),
            Some(source),
            Some(
                crate::ipc_flat_stream_v2::reader_creation::ReaderCreation::Paired {
                    temporary: &mut temporary,
                    retained: &mut retained,
                    pool: &mut pool,
                },
            ),
            work,
        )
    }

    pub(crate) fn materialize(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, FlatReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = self.stream.materialize_parts(self.parts, &mut work);
        finish_reader(work, result)
    }
}
fn finish_reader<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, FlatReaderError>,
) -> Result<T, FlatReaderError> {
    if matches!(
        &result,
        Err(FlatReaderError::Projection(FlatPoolResourceError::Control(
            _
        )))
    ) {
        return result;
    }
    work.finish()?;
    result
}

impl<'a, 'f> FlatConstantStream<'a, 'f> {
    /// The caller supplies actual retained source backing, including any larger
    /// allocation kept by a slice and retained Field/type owners, including all
    /// HashMap bucket/control backing even after removals. Visible input
    /// length is only a lower bound. Raw framing/schema verification is the
    /// preceding stage, not authorized retroactively by this projection.
    pub fn preflight_reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<FlatReaderResourceFacts, FlatPoolResourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result =
            self.reader_resources(value_type, source_retained_bytes, policy, limits, &mut work);
        if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FlatReaderResourceFacts, FlatPoolResourceError> {
        self.reader_resources_core(
            value_type,
            source_retained_bytes,
            policy,
            limits,
            None,
            work,
        )
    }
    fn reader_resources_core<E: ReaderResourceFailure>(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FlatReaderResourceFacts, E> {
        self.reader_resources_profile_core(
            value_type,
            source_retained_bytes,
            Some(policy),
            Some(limits),
            admission,
            work,
        )
        .map(|(facts, _)| facts)
    }
    fn reader_resources_profile_core<E: ReaderResourceFailure>(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: Option<ConstantPolicy>,
        limits: Option<FlatReaderProjectionLimits>,
        mut admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(FlatReaderResourceFacts, ConstantPolicy), E> {
        cap(
            self.input().len(),
            source_retained_bytes,
            "flat reader source retention is below visible input",
        )?;
        if let (Some(a), Some(limits)) = (admission.as_deref_mut(), limits) {
            a.limits(
                limits.max_new_allocation_request_bytes,
                limits.max_coexisting_source_and_request_bytes,
                limits.max_cumulative_library_work,
            )?;
        }
        work.step()?;
        let source_work = if let Some(a) = admission.as_deref_mut() {
            let mut observed = CapturedWork {
                work,
                facts: ResourceCountFacts::default(),
                capture: |f: ResourceCountFacts| {
                    a.count_work(0, f.observed_work)?;
                    a.reader_work(f.work)
                },
            };
            reader_work::source_metadata_work(source_retained_bytes, &mut observed)?
        } else {
            reader_work::source_metadata_work(source_retained_bytes, work)?
        };
        if let Some(limits) = limits {
            cap(
                source_work,
                limits.max_cumulative_library_work,
                "flat reader source metadata work envelope exceeded",
            )?;
        }
        work.flush()?;
        let (pool, sealed) = if let Some(a) = admission.as_deref_mut() {
            self.pool_resources_profile_core(value_type, policy, Some(a), work)?
        } else {
            let policy = policy
                .ok_or_else(|| shape("input-derived reader requires original observation"))?;
            (
                self.preflight_pool_resources(value_type, policy, work.control())?,
                policy,
            )
        };
        let structures = if let Some(a) = admission.as_deref_mut() {
            let mut observed = CapturedWork {
                work,
                facts: ResourceCountFacts::default(),
                capture: |f: ResourceCountFacts| {
                    a.count_work(1, f.observed_work)?;
                    a.requests(2, f.bytes, f.count)
                },
            };
            reader_allocations::preflight(self, &pool, &mut observed)?
        } else {
            reader_allocations::preflight(self, &pool, work)?
        };
        let diagnostics = if let Some(a) = admission.as_deref_mut() {
            let mut observed = CapturedWork {
                work,
                facts: ResourceCountFacts::default(),
                capture: |f: ResourceCountFacts| {
                    a.count_work(2, f.observed_work)?;
                    a.requests(3, f.bytes, f.count)
                },
            };
            reader_diagnostics::preflight(self, &mut observed)?
        } else {
            reader_diagnostics::preflight(self, work)?
        };
        let payload = add(
            add(
                pool.owned_body_capacity_bytes,
                pool.alignment_repair_capacity_bytes_upper_bound,
            )?,
            pool.empty_offset_capacity_bytes,
        )?;
        let requested = add(
            add(payload, structures.structural_request_bytes_upper_bound)?,
            diagnostics.request_bytes_upper_bound,
        )?;
        let coexisting = add(source_retained_bytes, requested)?;
        let library_work = if let Some(a) = admission {
            let mut observed = CapturedWork {
                work,
                facts: ResourceCountFacts::default(),
                capture: |f: ResourceCountFacts| {
                    a.count_work(3, f.observed_work)?;
                    a.reader_work(f.work)
                },
            };
            let original = reader_work::preflight(
                self,
                source_retained_bytes,
                &pool,
                &structures,
                &diagnostics,
                &mut observed,
            )?;
            a.reader_work(original)?;
            original
        } else {
            reader_work::preflight(
                self,
                source_retained_bytes,
                &pool,
                &structures,
                &diagnostics,
                work,
            )?
        };
        if let Some(limits) = limits {
            cap(
                requested,
                limits.max_new_allocation_request_bytes,
                "flat reader allocation request envelope exceeded",
            )?;
            cap(
                coexisting,
                limits.max_coexisting_source_and_request_bytes,
                "flat reader source coexistence envelope exceeded",
            )?;
            cap(
                library_work,
                limits.max_cumulative_library_work,
                "flat reader cumulative library work envelope exceeded",
            )?;
        }
        work.step()?;
        let payload_requests = usize::from(pool.owned_body_capacity_bytes > 0)
            + usize::from(pool.alignment_repair_capacity_bytes_upper_bound > 0)
            + usize::from(pool.empty_offset_capacity_bytes > 0);
        Ok((
            FlatReaderResourceFacts {
                source_retained_bytes,
                payload_request_bytes_upper_bound: payload,
                retained_payload_request_bytes_upper_bound: if self.geometry().buffer_descriptors
                    != 0
                    || pool.empty_offset_capacity_bytes != 0
                {
                    payload
                } else {
                    0
                },
                structural_request_bytes_upper_bound: structures
                    .structural_request_bytes_upper_bound,
                retained_reader_request_bytes_upper_bound: structures
                    .retained_reader_request_bytes_upper_bound,
                retained_pool_data_request_bytes_upper_bound: structures
                    .retained_pool_data_request_bytes_upper_bound,
                constant_owner_request_bytes_upper_bound: structures
                    .constant_owner_request_bytes_upper_bound,
                diagnostic_request_bytes_upper_bound: diagnostics.request_bytes_upper_bound,
                allocation_request_count_upper_bound: add(
                    add(
                        structures.allocation_requests_upper_bound,
                        diagnostics.allocation_requests_upper_bound,
                    )?,
                    payload_requests,
                )?,
                new_allocation_request_bytes_upper_bound: requested,
                coexisting_source_and_request_bytes_upper_bound: coexisting,
                cumulative_library_work_upper_bound: library_work,
                pool,
            },
            sealed,
        ))
    }

    /// Invokes the actual public safe reader with the same original Field Arc.
    /// The complete array-output request/work gate precedes Schema/body creation.
    /// A typed refusal is primary; no partial pool is exposed on any error.
    pub fn materialize_pool(
        &self,
        field: Arc<Field>,
        value_type: FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, FlatReaderError> {
        self.materialize_pool_core(
            field,
            Cow::Owned(value_type),
            source_retained_bytes,
            policy,
            limits,
            control,
        )
    }

    /// Borrow the admitted type-table value until the original complete gate
    /// and reader succeed. The permitted carrier profile clones only Arc
    /// owners or inline parameters; no recursive Field or metadata is copied.
    pub fn materialize_pool_borrowed(
        &self,
        field: Arc<Field>,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, FlatReaderError> {
        self.materialize_pool_core(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            control,
        )
    }

    fn materialize_pool_core(
        &self,
        field: Arc<Field>,
        value_type: Cow<'_, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, FlatReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            let parts = self.prepare_parts(
                field,
                value_type,
                source_retained_bytes,
                policy,
                limits,
                &mut work,
            )?;
            self.materialize_parts(parts, &mut work)
        })();
        finish_reader(work, result)
    }

    /// Consume the original checked stream without cloning geometry or source.
    /// Namespace callers may aggregate these facts before consuming this owner.
    pub(crate) fn prepare_pool_borrowed<'v>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedFlatReader<'a, 'f, 'v, 'static>, FlatReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let parts = self.prepare_parts(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            &mut work,
        );
        let parts = finish_reader(work, parts)?;
        Ok(PreparedFlatReader {
            original_control: None,
            stream: self,
            parts,
        })
    }

    #[cfg(test)]
    pub(crate) fn prepare_pool_borrowed_in<'v, 'c>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedFlatReader<'a, 'f, 'v, 'c>, FlatReaderError> {
        self.prepare_pool_borrowed_observed(
            field,
            value_type,
            source_retained_bytes,
            policy,
            limits,
            &mut |facts| admit(facts).map_err(FlatReaderError::from),
            work,
        )
    }

    pub(crate) fn prepare_pool_borrowed_observed<'v, 'c, E: ReaderFailure>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedFlatReader<'a, 'f, 'v, 'c>, E> {
        let mut admission = Admission::new(source_retained_bytes, admit);
        admission.limits(
            limits.max_new_allocation_request_bytes,
            limits.max_coexisting_source_and_request_bytes,
            limits.max_cumulative_library_work,
        )?;
        if let Some(prefix) = self.progress {
            admission.seed_prefix(prefix)?;
        }
        let parts = self.prepare_parts_core(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            Some(&mut admission),
            work,
        )?;
        Ok(PreparedFlatReader {
            original_control: Some(work.control()),
            stream: self,
            parts,
        })
    }

    fn prepare_parts<'v>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedParts<'v>, FlatReaderError> {
        self.prepare_parts_core(
            field,
            value_type,
            source_retained_bytes,
            policy,
            limits,
            None,
            work,
        )
    }
    fn prepare_parts_core<'v, E: ReaderFailure>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: FlatReaderProjectionLimits,
        admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedParts<'v>, E> {
        self.prepare_parts_profile_core(
            field,
            value_type,
            source_retained_bytes,
            Some(policy),
            Some(limits),
            admission,
            work,
        )
    }
    pub(crate) fn prepare_pool_borrowed_input_derived_observed<'v, 'c, E: ReaderFailure>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source: usize,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedFlatReader<'a, 'f, 'v, 'c>, E> {
        let mut admission = Admission::new(source, admit);
        if let Some(prefix) = self.progress {
            admission.seed_prefix(prefix)?;
        }
        let parts = self.prepare_parts_profile_core(
            field,
            Cow::Borrowed(value_type),
            source,
            None,
            None,
            Some(&mut admission),
            work,
        )?;
        Ok(PreparedFlatReader {
            original_control: Some(work.control()),
            stream: self,
            parts,
        })
    }
    fn prepare_parts_profile_core<'v, E: ReaderFailure>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: Option<ConstantPolicy>,
        limits: Option<FlatReaderProjectionLimits>,
        mut admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedParts<'v>, E> {
        if !std::ptr::eq(self.field(), field.as_ref()) {
            return Err(shape("flat reader requires the original source Field Arc").into());
        }
        if let Some(a) = admission.as_deref_mut() {
            let header = reader_allocations::initial_header(&field)?;
            a.requests(
                2,
                header.structural_request_bytes_upper_bound,
                header.allocation_requests_upper_bound,
            )?;
            let body_len = self.batch_body().len();
            let rounded = a.numeric(super::pool_resources::rounded_capacity(body_len))?;
            a.requests(1, rounded, usize::from(rounded != 0))?;
            a.clone_work(crate::physical_type_v2::value_type_clone_preflight_work_upper_bound())?;
            crate::physical_type_v2::preflight_value_type_clone_admitted::<E>(
                value_type.as_ref(),
                &mut |f, _| {
                    a.requests(
                        5,
                        f.allocation_request_bytes_upper_bound(),
                        f.allocation_requests_upper_bound(),
                    )?;
                    a.clone_work(f.work_upper_bound())?;
                    Ok(())
                },
                work,
            )?;
        }
        work.step()?;
        let (mut facts, sealed) = self.reader_resources_profile_core(
            value_type.as_ref(),
            source_retained_bytes,
            policy,
            limits,
            admission.as_deref_mut(),
            work,
        )?;
        let progress = admission.as_deref().map(Admission::facts);
        if let Some(p) = progress {
            facts.new_allocation_request_bytes_upper_bound =
                p.new_allocation_request_bytes_upper_bound;
            facts.allocation_request_count_upper_bound = p.allocation_request_count_upper_bound;
            facts.coexisting_source_and_request_bytes_upper_bound = source_retained_bytes
                .checked_add(p.new_allocation_request_bytes_upper_bound)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            facts.cumulative_library_work_upper_bound = p.cumulative_library_work_upper_bound;
            facts.structural_request_bytes_upper_bound = p
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.payload_request_bytes_upper_bound)
                .and_then(|n| n.checked_sub(facts.diagnostic_request_bytes_upper_bound))
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        }
        Ok(PreparedParts {
            field,
            value_type,
            policy: sealed,
            facts,
            progress,
        })
    }

    fn materialize_parts(
        &self,
        parts: PreparedParts<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, FlatReaderError> {
        self.materialize_parts_core(parts, None, None, None, work)
    }
    fn materialize_parts_core<E: ReaderFailure>(
        &self,
        parts: PreparedParts<'_>,
        admission: Option<&mut Admission<'_, '_, E>>,
        source: Option<novarocks_constant_contract::ConstantSourceOwner>,
        mut creation: Option<crate::ipc_flat_stream_v2::reader_creation::ReaderCreation<'_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let PreparedParts {
            field,
            value_type,
            policy,
            facts,
            progress,
        } = parts;
        if let Some(creation) = creation.as_mut() {
            let operation = facts
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.constant_owner_request_bytes_upper_bound)
                .and_then(|bytes| {
                    bytes.checked_sub(progress.map_or(0, |p| p.geometry_scratch_request_bytes))
                })
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            creation.operation(
                operation,
                facts.retained_payload_request_bytes_upper_bound,
                facts.retained_reader_request_bytes_upper_bound,
                facts.retained_pool_data_request_bytes_upper_bound,
                work,
            )?;
        }
        work.flush()?;
        let schema = Arc::new(Schema::new([Arc::clone(&field)]));
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let body = Buffer::from_slice_ref(self.batch_body());
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let read = read_record_batch(
            &body,
            self.record_batch(),
            schema,
            &HashMap::new(),
            None,
            &self.metadata_version(),
        );
        // Formatting the ordinary Arrow error is part of the already checked
        // diagnostic envelope. Observe the same original control afterwards.
        let read = read.map_err(FlatReaderError::Arrow);
        work.flush()?;
        let decoded = read?;
        if admission.is_some() {
            work.step()?;
            work.flush()?;
        }
        let data = decoded.column(0).to_data();
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let value_type = if admission.is_some() {
            crate::physical_type_v2::clone_value_type_observed(value_type.as_ref(), work)
                .map_err(FlatPoolResourceError::from)?
        } else {
            value_type.into_owned()
        };
        let pool = if let Some(a) = admission {
            let mut capture = |f: &novarocks_constant_contract::ConstantOwnerResourceFacts| {
                a.constant(
                    f.allocation_request_bytes_upper_bound,
                    f.allocation_requests_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            };
            if let Some(creation) = creation {
                let source = source.ok_or_else(|| {
                    shape("flat creator reader is missing original source custody")
                })?;
                creation
                    .pool(
                        field,
                        value_type,
                        data,
                        policy,
                        source,
                        &mut |facts| {
                            capture(facts).map_err(
                                novarocks_constant_contract::ConstantCreationFailure::Creator,
                            )
                        },
                        work,
                    )
                    .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?
            } else {
                ConstantPool::try_new_pure_observed_in(
                    field,
                    value_type,
                    data,
                    policy,
                    &mut capture,
                    work,
                )
                .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?
            }
        } else {
            ConstantPool::try_new(
                field,
                value_type,
                data,
                policy,
                CompilePhase::Decode,
                work.control(),
            )
            .map_err(FlatPoolResourceError::from)?
        };
        Ok(pool)
    }
}
