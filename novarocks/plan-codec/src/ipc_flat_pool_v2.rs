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

//! Exact one-pool flat IPC encoding from the sole immutable constant owner.
//! Requests and retained source facts are host inputs, not Account/grant proof.
//! Source-preflight scratch is admitted before that phase; the full combined
//! gate precedes all schema/batch/output construction. No general writer fallback.

use crate::{
    physical_type_v2::TypeCodecError,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::alloc::Layout;
pub(crate) mod allocations;
pub(crate) mod body;
pub(crate) mod geometry;
pub(crate) mod header;
use geometry::{Geometry, add, aligned, mul};

#[derive(Clone, Copy, Debug)]
pub struct FlatPoolWriteLimits {
    pub max_rows: usize,
    pub max_buffer_descriptors: usize,
    pub max_body_bytes: usize,
    pub max_encoded_stream_bytes: usize,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatPoolWriteFacts {
    pub source_retained_bytes: usize,
    pub rows: usize,
    pub buffer_descriptors: usize,
    pub variadic_buffers: usize,
    pub body_bytes: usize,
    pub batch_backing_bytes_upper_bound: usize,
    pub encoded_stream_bytes_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}
#[derive(Clone, Copy)]
pub(crate) struct WriterAdmissionPolicy(pub bool);
impl WriterAdmissionPolicy {
    pub(crate) fn numeric<T>(self, result: Result<T, TypeCodecError>) -> Result<T, TypeCodecError> {
        if self.0 {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
    pub(crate) fn cap(
        self,
        actual: usize,
        maximum: usize,
        message: &'static str,
    ) -> Result<(), TypeCodecError> {
        if actual <= maximum {
            return Ok(());
        }
        if self.0 {
            Err(CompileControlError::ResourceExhausted.into())
        } else {
            Err(TypeCodecError::InvalidShape(message))
        }
    }
}

pub(crate) mod progress;

struct Prepared {
    geometry: Geometry,
    facts: Option<FlatPoolWriteFacts>,
    construction: WriterConstruction,
}
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn cap(actual: usize, maximum: usize, message: &'static str) -> Result<(), TypeCodecError> {
    if actual > maximum {
        Err(shape(message))
    } else {
        Ok(())
    }
}
pub(crate) fn metadata_capacity(bytes: usize) -> Result<usize, TypeCodecError> {
    let padded = aligned(bytes)?;
    if padded > i32::MAX as usize || bytes >= (1usize << 31) {
        return Err(shape(
            "flat pool writer metadata framing is not representable",
        ));
    }
    Layout::array::<u8>(bytes)
        .map_err(|_| shape("flat pool writer backing layout is not representable"))?;
    Ok(padded)
}
fn work_bound(
    geometry: &Geometry,
    requests: &allocations::Requests,
) -> Result<usize, TypeCodecError> {
    // Charge the actual batch builder and output backing, bitmap inspection,
    // and descriptor/row work. Field identity is already owned by the type table.
    let storage = add(mul(4, requests.bytes)?, requests.count)?;
    let elements = add(add(mul(3, geometry.rows)?, 1)?, mul(4, geometry.buffers)?)?;
    add(storage, elements)
}

fn prepare(
    pool: &ConstantPool,
    source: usize,
    limits: FlatPoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    prepare_core::<std::convert::Infallible>(pool, source, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn prepare_core<H>(
    pool: &ConstantPool,
    source: usize,
    limits: FlatPoolWriteLimits,
    mut admission: Option<&mut progress::Admission<'_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, HostError<H>> {
    let policy = WriterAdmissionPolicy(admission.is_some());
    let add = |a, b| policy.numeric(add(a, b));
    let cap = |a, b, c| policy.cap(a, b, c);
    if let Some(admission) = admission.as_deref_mut() {
        admission.gate()?;
    }
    if !(LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little"))
    {
        return Err((shape("flat pool writer source or endian model changed")).into());
    }
    let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
        .map_err(|_| shape("flat pool source retention is not representable"))?;
    self::cap(
        retained,
        source,
        "flat pool source retention is below original buffer backing",
    )?;
    work.step()?;
    let geometry = if let Some(admission) = admission.as_deref_mut() {
        geometry::inspect_with_host_in(
            pool,
            limits.max_rows,
            limits.max_buffer_descriptors,
            limits.max_body_bytes,
            &mut |g| admission.geometry(g),
            work,
        )?
    } else {
        geometry::inspect(
            pool,
            limits.max_rows,
            limits.max_buffer_descriptors,
            limits.max_body_bytes,
            work,
        )?
    };
    if geometry.buffers > u32::MAX as usize {
        return Err((shape("flat pool buffer vector is not representable")).into());
    }
    let construction = policy.numeric(construction_geometry(&geometry))?;
    let batch = construction.batch_backing;
    let capacity = construction.stream_backing;
    cap(
        capacity,
        limits.max_encoded_stream_bytes,
        "flat pool encoded stream envelope exceeded",
    )?;
    policy.numeric(
        Layout::array::<u8>(capacity)
            .map_err(|_| shape("flat pool output layout is not representable")),
    )?;
    if let Some(admission) = admission.as_deref_mut() {
        let requests = allocations::batch_and_stream_requests_core(batch, capacity, None)?;
        let bound = policy.numeric(work_bound(&geometry, &requests))?;
        admission.facts.batch_backing_bytes_upper_bound = batch;
        admission.facts.encoded_stream_bytes_upper_bound = capacity;
        admission.construction(requests, bound)?;
    }
    let requests = allocations::batch_and_stream_requests(batch, capacity, work)?;
    let coexisting = add(source, requests.bytes)?;
    let library_work = policy.numeric(work_bound(&geometry, &requests))?;
    cap(
        requests.bytes,
        limits.max_new_allocation_request_bytes,
        "flat pool allocation request envelope exceeded",
    )?;
    cap(
        coexisting,
        limits.max_coexisting_source_and_request_bytes,
        "flat pool source coexistence envelope exceeded",
    )?;
    cap(
        library_work,
        limits.max_cumulative_library_work,
        "flat pool cumulative library work envelope exceeded",
    )?;
    if admission.is_none() {
        work.step()?;
    }
    let facts = FlatPoolWriteFacts {
        source_retained_bytes: source,
        rows: geometry.rows,
        buffer_descriptors: geometry.buffers,
        variadic_buffers: geometry.variadic,
        body_bytes: geometry.body_bytes,
        batch_backing_bytes_upper_bound: batch,
        encoded_stream_bytes_upper_bound: capacity,
        new_allocation_request_bytes_upper_bound: requests.bytes,
        allocation_request_count_upper_bound: requests.count,
        coexisting_source_and_request_bytes_upper_bound: coexisting,
        cumulative_library_work_upper_bound: library_work,
    };
    let facts = if let Some(admission) = admission {
        let facts = admission.complete(facts)?;
        work.step()?;
        facts
    } else {
        facts
    };
    Ok(Prepared {
        geometry,
        facts: Some(facts),
        construction,
    })
}

/// Protocol construction extents, independent of any source/budget invoice.
pub(crate) struct WriterConstruction {
    pub(crate) batch_backing: usize,
    pub(crate) stream_backing: usize,
}
fn construction_geometry(geometry: &Geometry) -> Result<WriterConstruction, TypeCodecError> {
    construction_from_counts(
        1,
        geometry.buffers,
        usize::from(geometry.views),
        geometry.body_bytes,
    )
}
pub(crate) fn construction_from_counts(
    nodes: usize,
    buffers: usize,
    views: usize,
    body_bytes: usize,
) -> Result<WriterConstruction, TypeCodecError> {
    let batch_backing = header::backing_counts(nodes, buffers, views)?;
    let stream_backing = add(add(metadata_capacity(batch_backing)?, body_bytes)?, 8)?;
    Ok(WriterConstruction {
        batch_backing,
        stream_backing,
    })
}

/// FE owns an immutable checked pool and its original control. This type
/// exposes no receiving-resource facts or invented retained-source invoice.
pub(crate) struct PreparedPureFlatPoolWriter<'pool, 'control> {
    pool: &'pool ConstantPool,
    prepared: Prepared,
    control: &'control dyn PureCompileControl,
}
impl PreparedPureFlatPoolWriter<'_, '_> {
    pub(crate) fn emit_in(
        self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, TypeCodecError> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(shape("pure flat writer belongs to another control"));
        }
        emit_prepared(self.pool, self.prepared, work)
    }
}
pub(crate) fn prepare_flat_pool_write_pure_in<'pool, 'control>(
    pool: &'pool ConstantPool,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedPureFlatPoolWriter<'pool, 'control>, TypeCodecError> {
    validate_pure_pool_source(pool, work)?;
    let prepared = prepare_from_input(pool, work)?;
    Ok(PreparedPureFlatPoolWriter {
        pool,
        prepared,
        control: work.control(),
    })
}

pub(crate) fn validate_pure_pool_source(
    pool: &ConstantPool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    work.flush()?;
    if pool.source_owner().is_none() {
        novarocks_constant_contract::validate_pure_borrowed_source_in(pool.field(), work).map_err(
            |error| match error {
                novarocks_constant_contract::ConstantError::Control(cause) => {
                    TypeCodecError::Control(cause)
                }
                _ => TypeCodecError::ResourceSource(
                    "pure borrowed pool has no original metadata custody",
                ),
            },
        )?;
    }
    Ok(())
}

fn prepare_from_input(
    pool: &ConstantPool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    work.flush()?;
    if !(LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little"))
    {
        return Err(shape("flat pool writer source or endian model changed"));
    }
    let geometry = geometry::inspect_pure(pool, work)?;
    let construction = construction_geometry(&geometry)?;
    Layout::array::<u8>(construction.stream_backing)
        .map_err(|_| shape("flat pool output layout is not representable"))?;
    Ok(Prepared {
        geometry,
        facts: None,
        construction,
    })
}

pub(crate) fn encode_flat_pool_input_derived_in<H>(
    pool: &ConstantPool,
    request: &mut WriteRequest<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    let prepared = prepare_from_input(pool, work)?;
    emit_record(pool, prepared, Some(request), work)
}

/// Sealed geometry/model preparation bound to the original checked pool.
/// The preparation scratch and host authorization remain earlier obligations.
use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;

/// Requests made by the RecordBatch writer on the active construction scope.
#[derive(Clone, Copy, Debug)]
pub enum ConstantWriteRequest {
    /// The next reserve uses this exact layout.
    Layout(Layout),
    /// Aggregate internal FlatBuffer builder requests.
    OpaqueUpperBound(usize),
}
pub(crate) type WriteRequest<'a, H> =
    dyn FnMut(ConstantWriteRequest) -> Result<(), HostError<H>> + 'a;

pub(crate) struct PreparedFlatPoolWriter<'p, 'control> {
    pool: &'p ConstantPool,
    prepared: Prepared,
    control: Option<&'control dyn PureCompileControl>,
}
impl PreparedFlatPoolWriter<'_, '_> {
    pub(crate) fn facts(&self) -> &FlatPoolWriteFacts {
        self.prepared
            .facts
            .as_ref()
            .expect("receiving writer owns source facts")
    }
    #[cfg(test)]
    pub(crate) fn emit_in(
        self,
        admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, TypeCodecError> {
        self.emit_with_host_in(
            &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
            work,
        )
        .map_err(ProjectionFailure::without_host)
    }

    pub(crate) fn emit_with_host_in<H>(
        self,
        admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), HostError<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, HostError<H>> {
        if !self
            .control
            .is_some_and(|control| std::ptr::addr_eq(control, work.control()))
        {
            return Err((shape("flat pool writer belongs to another control")).into());
        }
        admit(
            self.prepared
                .facts
                .as_ref()
                .expect("receiving writer owns source facts"),
        )?;
        emit_record(self.pool, self.prepared, None, work)
    }
    pub(crate) fn emit(self, control: &dyn PureCompileControl) -> Result<Vec<u8>, TypeCodecError> {
        if self
            .control
            .is_some_and(|original| !std::ptr::addr_eq(original, control))
        {
            return Err(shape("flat pool writer belongs to another control"));
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = emit_prepared(self.pool, self.prepared, &mut work);
        finish_writer(work, result)
    }
}
fn finish_writer<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
/// Keep the same immutable snapshot through later namespace admission. No
/// output is allocated here and emission does not repeat numerical preparation.
pub(crate) fn prepare_flat_pool_write<'p>(
    pool: &'p ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<PreparedFlatPoolWriter<'p, 'static>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let prepared = prepare(pool, source_retained_bytes, limits, &mut work);
    let prepared = finish_writer(work, prepared)?;
    Ok(PreparedFlatPoolWriter {
        pool,
        prepared,
        control: None,
    })
}

#[cfg(test)]
pub(crate) fn prepare_flat_pool_write_in<'pool, 'control>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: FlatPoolWriteLimits,
    admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFlatPoolWriter<'pool, 'control>, TypeCodecError> {
    prepare_flat_pool_write_with_host_in(
        pool,
        source,
        limits,
        &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub(crate) fn prepare_flat_pool_write_with_host_in<'pool, 'control, H>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: FlatPoolWriteLimits,
    admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), HostError<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedFlatPoolWriter<'pool, 'control>, HostError<H>> {
    let facts = progress::Admission::<H>::initial(source, pool.data().len(), limits)?;
    let mut admission = progress::Admission {
        parent: admit,
        limits,
        facts,
    };
    let prepared = prepare_core(pool, source, limits, Some(&mut admission), work)?;
    Ok(PreparedFlatPoolWriter {
        pool,
        prepared,
        control: Some(work.control()),
    })
}

/// The mandatory trusted invoice includes all retained original pool, Field,
/// type, buffer and raw HashMap backing, including spare/deleted buckets.
/// This projection cannot establish that host fact or create a memory wallet.
pub fn preflight_flat_pool_write(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<FlatPoolWriteFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work)
        .map(|p| p.facts.expect("receiving writer owns source facts"));
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
pub(crate) fn reserve(
    capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    work.flush()?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| TypeCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(result)
}
pub(crate) fn initialize_to(
    output: &mut Vec<u8>,
    length: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if length > output.capacity() || length < output.len() {
        return Err(shape(
            "flat pool initialized output exceeds admitted capacity",
        ));
    }
    while output.len() < length {
        let end = add(output.len(), (length - output.len()).min(1024))?;
        output.resize(end, 0);
        work.step()?;
    }
    Ok(())
}
pub(crate) fn append(
    output: &mut Vec<u8>,
    bytes: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if add(output.len(), bytes.len())? > output.capacity() {
        return Err(shape("flat pool output exceeds admitted capacity"));
    }
    for chunk in bytes.chunks(1024) {
        output.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(())
}
pub(crate) fn frame(
    output: &mut Vec<u8>,
    metadata: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let padded = metadata_capacity(metadata.len())?;
    let length = i32::try_from(padded)
        .map_err(|_| shape("flat pool metadata continuation is not representable"))?;
    append(output, &[255; 4], work)?;
    append(output, &length.to_le_bytes(), work)?;
    append(output, metadata, work)?;
    initialize_to(output, add(output.len(), padded - metadata.len())?, work)
}
/// Encodes the whole original pool, preserving every ordinal. All output stays
/// unpublished until the final original control observation succeeds.
pub fn encode_flat_pool(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let prepared = prepare(pool, source_retained_bytes, limits, &mut work)?;
        emit_prepared(pool, prepared, &mut work)
    })();
    finish_writer(work, result)
}

fn emit_prepared(
    pool: &ConstantPool,
    prepared: Prepared,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    emit_record::<std::convert::Infallible>(pool, prepared, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn emit_record<H>(
    pool: &ConstantPool,
    prepared: Prepared,
    mut request: Option<&mut WriteRequest<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    work.flush()?;
    if let Some(request) = request.as_deref_mut() {
        request_record_backing_in(&prepared.construction, request, work)?;
    }
    let mut batch = reserve(prepared.construction.batch_backing, work)?;
    initialize_to(&mut batch, prepared.construction.batch_backing, work)?;
    work.flush()?;
    let mut builder = FlatBufferBuilder::from_vec(batch);
    work.flush()?;
    header::emit(pool, &prepared.geometry, &mut builder, work)?;
    // Witness the complete primary backing, not just the finished message.
    let backing = builder.mut_finished_buffer().0.len();
    if backing != prepared.construction.batch_backing {
        return Err((shape("flat pool batch builder grew beyond its admitted backing")).into());
    }
    if let Some(request) = request {
        request_writer_vec_in(prepared.construction.stream_backing, request, work)?;
    }
    let mut output = reserve(prepared.construction.stream_backing, work)?;
    frame(&mut output, builder.finished_data(), work)?;
    let body_start = output.len();
    let body_end = add(body_start, prepared.geometry.body_bytes)?;
    initialize_to(&mut output, body_end, work)?;
    body::emit(
        pool,
        &prepared.geometry,
        &mut output[body_start..body_end],
        work,
    )?;
    if output.len() > prepared.construction.stream_backing {
        return Err((shape("flat pool encoded result exceeds admitted capacity")).into());
    }
    Ok(output)
}

pub(crate) fn request_record_backing_in<H>(
    construction: &WriterConstruction,
    request: &mut WriteRequest<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    let requests = allocations::batch_and_stream_requests(
        construction.batch_backing,
        construction.stream_backing,
        work,
    )?;
    let primary = add(construction.batch_backing, construction.stream_backing)?;
    let auxiliary = requests
        .bytes
        .checked_sub(primary)
        .ok_or_else(|| shape("IPC record allocation model omits primary backing"))?;
    work.flush()?;
    request(ConstantWriteRequest::OpaqueUpperBound(auxiliary))?;
    request_writer_vec_in(construction.batch_backing, request, work)
}

pub(crate) fn request_writer_vec_in<H>(
    capacity: usize,
    request: &mut WriteRequest<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), HostError<H>> {
    let layout = Layout::array::<u8>(capacity)
        .map_err(|_| shape("IPC backing layout is not representable"))?;
    work.flush()?;
    request(ConstantWriteRequest::Layout(layout))
}

#[cfg(test)]
mod envelope_tests {
    use super::*;

    #[test]
    fn signed_continuation_limit_is_checked_after_padding_without_allocation() {
        let maximum = (i32::MAX as usize) & !7;
        assert_eq!(metadata_capacity(maximum).unwrap(), maximum);
        assert_eq!(metadata_capacity(maximum - 7).unwrap(), maximum);
        for length in [maximum + 1, i32::MAX as usize, 1usize << 31, usize::MAX] {
            assert!(metadata_capacity(length).is_err());
        }
    }

    #[test]
    fn descriptor_backing_arithmetic_refuses_before_builder_vector_multiplication() {
        let geometry = Geometry {
            rows: 0,
            buffers: usize::MAX,
            variadic: 0,
            views: false,
            body_bytes: 0,
            payload_bytes: 0,
            values_start: 0,
            values_bytes: 0,
            offset_base: 0,
        };
        assert!(header::backing(&geometry).is_err());
        assert!(aligned(usize::MAX).is_err());
        assert!(geometry::native_offset(&[0; 8], usize::MAX, 8).is_err());
    }
}

#[cfg(test)]
mod tests;
