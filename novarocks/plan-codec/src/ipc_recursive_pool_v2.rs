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

//! Checked-pool recursive IPC writer. Source retention is a caller invoice;
//! request/work bounds are projection admission, not a formal MEM grant.
//! No general writer, temporary ArrayData tree, or sliced data is constructed.

use crate::{
    ipc_flat_pool_v2::{
        self, FlatPoolWriteFacts, FlatPoolWriteLimits,
        allocations::batch_and_stream_requests,
        geometry::{add, mul},
    },
    physical_type_v2::TypeCodecError,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

mod body;
mod geometry;
mod header;

/// Explicit recursive admission; no application defaults are selected here.
#[derive(Clone, Copy, Debug)]
pub struct RecursivePoolWriteLimits {
    pub flat: FlatPoolWriteLimits,
    pub max_field_nodes: usize,
    /// Sum of the selected length of every actual FieldNode occurrence.
    pub max_total_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecursivePoolWriteFacts {
    pub flat: FlatPoolWriteFacts,
    pub field_nodes: usize,
    pub total_rows: usize,
    pub view_fields: usize,
}

struct Prepared {
    geometry: geometry::Geometry,
    facts: Option<RecursivePoolWriteFacts>,
    construction: ipc_flat_pool_v2::WriterConstruction,
}

/// One immutable checked pool paired with the admitted geometry and limits.
/// Consumption emits that snapshot without repeating resource preparation.
pub(crate) struct PreparedRecursivePoolWriter<'pool, 'control> {
    pool: &'pool ConstantPool,
    prepared: Prepared,
    control: Option<&'control dyn PureCompileControl>,
}
impl PreparedRecursivePoolWriter<'_, '_> {
    pub(crate) const fn facts(&self) -> &RecursivePoolWriteFacts {
        self.prepared
            .facts
            .as_ref()
            .expect("receiving recursive writer owns source facts")
    }

    #[cfg(test)]
    pub(crate) fn emit_in(
        self,
        admit: &mut dyn FnMut(&RecursivePoolWriteFacts) -> Result<(), CompileControlError>,
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
        admit: &mut dyn FnMut(&RecursivePoolWriteFacts) -> Result<(), HostError<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, HostError<H>> {
        if !self
            .control
            .is_some_and(|c| std::ptr::addr_eq(c, work.control()))
        {
            return Err((shape("recursive writer belongs to another control")).into());
        }
        admit(
            self.prepared
                .facts
                .as_ref()
                .expect("receiving recursive writer owns source facts"),
        )?;
        emit_record(self.pool, &self.prepared, None, work)
    }
    pub(crate) fn emit(self, control: &dyn PureCompileControl) -> Result<Vec<u8>, TypeCodecError> {
        if self
            .control
            .is_some_and(|original| !std::ptr::addr_eq(original, control))
        {
            return Err(shape("recursive writer belongs to another control"));
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = emit_prepared(self.pool, &self.prepared, &mut work);
        finish(work, result)
    }
}

/// Explicit pure owner: the immutable source and original control remain
/// borrowed, while receiving resource/admission facts are absent.
pub(crate) struct PreparedPureRecursivePoolWriter<'pool, 'control> {
    pool: &'pool ConstantPool,
    prepared: Prepared,
    control: &'control dyn PureCompileControl,
}
impl PreparedPureRecursivePoolWriter<'_, '_> {
    pub(crate) fn emit_in(
        self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, TypeCodecError> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(shape("pure recursive writer belongs to another control"));
        }
        emit_prepared(self.pool, &self.prepared, work)
    }
}
pub(crate) fn prepare_recursive_pool_write_pure_in<'pool, 'control>(
    pool: &'pool ConstantPool,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedPureRecursivePoolWriter<'pool, 'control>, TypeCodecError> {
    ipc_flat_pool_v2::validate_pure_pool_source(pool, work)?;
    let prepared = prepare_from_input(pool, work)?;
    Ok(PreparedPureRecursivePoolWriter {
        pool,
        prepared,
        control: work.control(),
    })
}

fn prepare_from_input(
    pool: &ConstantPool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    work.flush()?;
    validate_writer_environment()?;
    let geometry = geometry::inspect_pure(pool.data(), work)?;
    let construction = construction_geometry(&geometry)?;
    Ok(Prepared {
        geometry,
        facts: None,
        construction,
    })
}

pub(crate) fn encode_recursive_pool_input_derived_in<H>(
    pool: &ConstantPool,
    request: &mut ipc_flat_pool_v2::WriteRequest<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    let prepared = prepare_from_input(pool, work)?;
    emit_record(pool, &prepared, Some(request), work)
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}

fn gate_requests(
    bytes: usize,
    source: usize,
    work_bound: usize,
    limits: FlatPoolWriteLimits,
) -> Result<usize, TypeCodecError> {
    let coexist = add(source, bytes)?;
    if bytes > limits.max_new_allocation_request_bytes
        || coexist > limits.max_coexisting_source_and_request_bytes
        || work_bound > limits.max_cumulative_library_work
    {
        return Err(shape("recursive writer resource envelope exceeded"));
    }
    Ok(coexist)
}

fn prepare(
    pool: &ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    prepare_core::<std::convert::Infallible>(pool, source, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn own_work(
    geometry: &geometry::Geometry,
    request_bytes: usize,
    request_count: usize,
) -> Result<usize, TypeCodecError> {
    // There are at most five complete borrowed data walks: geometry, buffers,
    // nodes, optional variadic counts, and final body. Per walk a node performs
    // four node checks, one edge check, and at most two flat-leaf checks plus
    // one operation per descriptor; selected validity recounts visit <=N rows.
    let visits = mul(
        5,
        add(
            add(geometry.total_rows, mul(7, geometry.nodes)?)?,
            geometry.buffers,
        )?,
    )?;
    let profile = mul(2, geometry.nodes)?; // type node and child edge
    let summary = geometry.nodes;
    let header_pushes = add(add(geometry.nodes, geometry.buffers)?, geometry.view_fields)?;
    // Each node's body work includes validity/range checks and <=N offsets;
    // byte copies/initialization and opaque builder work use the same locked
    // four-storage-visits source cover as the existing flat writer.
    let body_checks = add(geometry.total_rows, mul(8, geometry.nodes)?)?;
    let storage = add(mul(4, request_bytes)?, request_count)?;
    let own_work = add(
        add(add(visits, profile)?, add(summary, header_pushes)?)?,
        add(body_checks, storage)?,
    )?;
    Ok(own_work)
}

use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
type HostPoolAdmit<'a, H> = dyn FnMut(&RecursivePoolWriteFacts) -> Result<(), HostError<H>> + 'a;

fn validate_writer_environment() -> Result<(), TypeCodecError> {
    if !(LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little"))
    {
        return Err(shape(
            "recursive writer source model does not match this target",
        ));
    }
    Ok(())
}

fn construction_geometry(
    geometry: &geometry::Geometry,
) -> Result<ipc_flat_pool_v2::WriterConstruction, TypeCodecError> {
    let construction = ipc_flat_pool_v2::construction_from_counts(
        geometry.nodes,
        geometry.buffers,
        geometry.view_fields,
        geometry.body_bytes,
    )?;
    std::alloc::Layout::array::<u8>(construction.stream_backing)
        .map_err(|_| shape("recursive writer IPC output is not representable"))?;
    Ok(construction)
}

fn write_facts(
    geometry: &geometry::Geometry,
    construction: &ipc_flat_pool_v2::WriterConstruction,
    requests: ipc_flat_pool_v2::allocations::Requests,
    source: usize,
) -> Result<RecursivePoolWriteFacts, TypeCodecError> {
    Ok(RecursivePoolWriteFacts {
        flat: FlatPoolWriteFacts {
            source_retained_bytes: source,
            rows: geometry.rows,
            buffer_descriptors: geometry.buffers,
            variadic_buffers: geometry.variadic,
            body_bytes: geometry.body_bytes,
            batch_backing_bytes_upper_bound: construction.batch_backing,
            encoded_stream_bytes_upper_bound: construction.stream_backing,
            new_allocation_request_bytes_upper_bound: requests.bytes,
            allocation_request_count_upper_bound: requests.count,
            coexisting_source_and_request_bytes_upper_bound: add(source, requests.bytes)?,
            cumulative_library_work_upper_bound: own_work(
                geometry,
                requests.bytes,
                requests.count,
            )?,
        },
        field_nodes: geometry.nodes,
        total_rows: geometry.total_rows,
        view_fields: geometry.view_fields,
    })
}

fn prepare_core<H>(
    pool: &ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    mut parent: Option<&mut HostPoolAdmit<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, HostError<H>> {
    validate_writer_environment()?;
    if source
        < usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
            .map_err(|_| shape("source retention is not representable"))?
    {
        return Err(shape("recursive writer source retention omits checked backing").into());
    }
    if parent.is_some() {
        let facts = RecursivePoolWriteFacts {
            flat: ipc_flat_pool_v2::progress::Admission::<H>::initial(
                source,
                pool.data().len(),
                limits.flat,
            )?,
            field_nodes: 1,
            total_rows: pool.data().len(),
            view_fields: 0,
        };
        publish(&mut parent, &facts, limits)?;
    }
    work.step()?;
    let geometry = if parent.is_some() {
        let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(true);
        geometry::inspect_with_host_in(
            pool.data(),
            limits,
            &mut |geometry| {
                let construction = policy.numeric(construction_geometry(geometry))?;
                let requests = ipc_flat_pool_v2::allocations::batch_and_stream_requests_core(
                    construction.batch_backing,
                    construction.stream_backing,
                    None,
                )?;
                let facts =
                    policy.numeric(write_facts(geometry, &construction, requests, source))?;
                publish(&mut parent, &facts, limits)
            },
            work,
        )?
    } else {
        geometry::inspect(pool.data(), limits, work)?
    };
    let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(parent.is_some());
    let construction = policy.numeric(construction_geometry(&geometry))?;
    if construction.stream_backing > limits.flat.max_encoded_stream_bytes {
        return Err(if parent.is_some() {
            CompileControlError::ResourceExhausted.into()
        } else {
            shape("recursive writer IPC output envelope exceeded").into()
        });
    }
    let requests = batch_and_stream_requests(
        construction.batch_backing,
        construction.stream_backing,
        work,
    )?;
    let facts = policy.numeric(write_facts(&geometry, &construction, requests, source))?;
    if parent.is_some() {
        publish(&mut parent, &facts, limits)?;
    } else {
        gate_requests(
            facts.flat.new_allocation_request_bytes_upper_bound,
            source,
            facts.flat.cumulative_library_work_upper_bound,
            limits.flat,
        )?;
    }
    work.step()?;
    Ok(Prepared {
        geometry,
        facts: Some(facts),
        construction,
    })
}

fn publish<H>(
    parent: &mut Option<&mut HostPoolAdmit<'_, H>>,
    facts: &RecursivePoolWriteFacts,
    limits: RecursivePoolWriteLimits,
) -> Result<(), HostError<H>> {
    let f = &facts.flat;
    let l = limits.flat;
    if facts.field_nodes > limits.max_field_nodes
        || facts.total_rows > limits.max_total_rows
        || f.rows > l.max_rows
        || f.buffer_descriptors > l.max_buffer_descriptors
        || f.body_bytes > l.max_body_bytes
        || f.encoded_stream_bytes_upper_bound > l.max_encoded_stream_bytes
        || f.new_allocation_request_bytes_upper_bound > l.max_new_allocation_request_bytes
        || f.coexisting_source_and_request_bytes_upper_bound
            > l.max_coexisting_source_and_request_bytes
        || f.cumulative_library_work_upper_bound > l.max_cumulative_library_work
    {
        return Err((CompileControlError::ResourceExhausted).into());
    }
    if let Some(parent) = parent.as_deref_mut() {
        parent(facts)?;
    }
    Ok(())
}
#[cfg(test)]
pub(crate) fn prepare_recursive_pool_write_in<'pool, 'control>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    admit: &mut dyn FnMut(&RecursivePoolWriteFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedRecursivePoolWriter<'pool, 'control>, TypeCodecError> {
    prepare_recursive_pool_write_with_host_in(
        pool,
        source,
        limits,
        &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub(crate) fn prepare_recursive_pool_write_with_host_in<'pool, 'control, H>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    admit: &mut dyn FnMut(&RecursivePoolWriteFacts) -> Result<(), HostError<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedRecursivePoolWriter<'pool, 'control>, HostError<H>> {
    let prepared = prepare_core(pool, source, limits, Some(admit), work)?;
    Ok(PreparedRecursivePoolWriter {
        pool,
        prepared,
        control: Some(work.control()),
    })
}

pub fn preflight_recursive_pool_write(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<RecursivePoolWriteFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work).map(|prepared| {
        prepared
            .facts
            .expect("receiving recursive writer owns source facts")
    });
    finish(work, result)
}

pub(crate) fn prepare_recursive_pool_write<'pool>(
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<PreparedRecursivePoolWriter<'pool, 'static>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work).map(|prepared| {
        PreparedRecursivePoolWriter {
            pool,
            prepared,
            control: None,
        }
    });
    finish(work, result)
}

/// Preserves the complete original pool and ordinals. Only the final original
/// success-tail observation permits publication of the assembled stream.
pub fn encode_recursive_pool(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work)
        .and_then(|prepared| emit_prepared(pool, &prepared, &mut work));
    finish(work, result)
}

fn emit_prepared(
    pool: &ConstantPool,
    prepared: &Prepared,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    emit_record::<std::convert::Infallible>(pool, prepared, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn emit_record<H>(
    pool: &ConstantPool,
    prepared: &Prepared,
    mut request: Option<&mut ipc_flat_pool_v2::WriteRequest<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    work.flush()?;
    if let Some(request) = request.as_deref_mut() {
        ipc_flat_pool_v2::request_record_backing_in(&prepared.construction, request, work)?;
    }
    let mut batch = ipc_flat_pool_v2::reserve(prepared.construction.batch_backing, work)?;
    ipc_flat_pool_v2::initialize_to(&mut batch, prepared.construction.batch_backing, work)?;
    work.flush()?;
    let mut builder = FlatBufferBuilder::from_vec(batch);
    work.flush()?;
    header::emit(pool.data(), &prepared.geometry, &mut builder, work)?;
    if builder.mut_finished_buffer().0.len() != prepared.construction.batch_backing {
        return Err((shape("recursive batch builder grew beyond admitted backing")).into());
    }
    if let Some(request) = request {
        ipc_flat_pool_v2::request_writer_vec_in(
            prepared.construction.stream_backing,
            request,
            work,
        )?;
    }
    let mut output = ipc_flat_pool_v2::reserve(prepared.construction.stream_backing, work)?;
    ipc_flat_pool_v2::frame(&mut output, builder.finished_data(), work)?;
    let body_start = output.len();
    let body_end = add(body_start, prepared.geometry.body_bytes)?;
    ipc_flat_pool_v2::initialize_to(&mut output, body_end, work)?;
    body::emit(
        pool.data(),
        &prepared.geometry,
        &mut output[body_start..body_end],
        work,
    )?;
    if output.len() > prepared.construction.stream_backing {
        return Err((shape("recursive writer result exceeds admitted stream capacity")).into());
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
