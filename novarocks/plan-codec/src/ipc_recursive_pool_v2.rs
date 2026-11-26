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
    ipc_schema_v2::{self, preflight_schema_writer_resources, schema_writer_prefix_resources},
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

struct Prepared<'pool> {
    geometry: geometry::Geometry,
    facts: Option<RecursivePoolWriteFacts>,
    construction: ipc_flat_pool_v2::WriterConstruction,
    schema: Option<ipc_schema_v2::PreparedSchemaWriter<'pool>>,
}

/// One immutable checked pool paired with the admitted geometry and limits.
/// Consumption emits that snapshot without repeating resource preparation.
pub(crate) struct PreparedRecursivePoolWriter<'pool, 'control> {
    pool: &'pool ConstantPool,
    limits: RecursivePoolWriteLimits,
    prepared: Prepared<'pool>,
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
        emit_prepared_core(
            self.pool,
            &self.prepared,
            Some(self.limits.flat.schema),
            Some(admit),
            work,
        )
    }
    pub(crate) fn emit(self, control: &dyn PureCompileControl) -> Result<Vec<u8>, TypeCodecError> {
        if self
            .control
            .is_some_and(|original| !std::ptr::addr_eq(original, control))
        {
            return Err(shape("recursive writer belongs to another control"));
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = emit_prepared(
            self.pool,
            &self.prepared,
            Some(self.limits.flat.schema),
            &mut work,
        );
        finish(work, result)
    }
}

/// Explicit pure owner: the immutable source and original control remain
/// borrowed, while receiving resource/admission facts are absent.
pub(crate) struct PreparedPureRecursivePoolWriter<'pool, 'control> {
    pool: &'pool ConstantPool,
    prepared: Prepared<'pool>,
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
        emit_prepared(self.pool, &self.prepared, None, work)
    }
}
pub(crate) fn prepare_recursive_pool_write_pure_in<'pool, 'control>(
    pool: &'pool ConstantPool,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedPureRecursivePoolWriter<'pool, 'control>, TypeCodecError> {
    let (prepared, _) = prepare_from_input(
        pool,
        |field, work| {
            ipc_schema_v2::preflight_writer_pure_in(field, work)
                .map_err(HostError::<std::convert::Infallible>::from)
        },
        work,
    )
    .map_err(ProjectionFailure::without_host)?;
    Ok(PreparedPureRecursivePoolWriter {
        pool,
        prepared,
        control: work.control(),
    })
}

fn prepare_from_input<'pool, H>(
    pool: &'pool ConstantPool,
    preflight_schema: impl FnOnce(
        &arrow::datatypes::Field,
        &mut CompileCheckpoints<'_>,
    ) -> Result<ipc_schema_v2::SchemaPreflight, HostError<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Prepared<'pool>, ipc_schema_v2::SchemaPreflight), HostError<H>> {
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
    if !(LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little"))
    {
        return Err(shape("recursive writer source model does not match this target").into());
    }
    let geometry = geometry::inspect_pure(pool.data(), work)?;
    let schema = preflight_schema(pool.field(), work)?;
    let construction = ipc_flat_pool_v2::construction_from_counts(
        &schema,
        geometry.nodes,
        geometry.buffers,
        geometry.view_fields,
        geometry.body_bytes,
    )?;
    std::alloc::Layout::array::<u8>(construction.stream_backing)
        .map_err(|_| shape("recursive writer stream request is not representable"))?;
    Ok((
        Prepared {
            geometry,
            facts: None,
            construction,
            schema: None,
        },
        schema,
    ))
}

pub(crate) fn encode_recursive_pool_input_derived_in<H>(
    pool: &ConstantPool,
    request: &mut ipc_schema_v2::WriteRequest<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    let (prepared, schema) = prepare_from_input(
        pool,
        |field, work| ipc_schema_v2::preflight_constant_schema_in(field, request, work),
        work,
    )?;
    let schema =
        ipc_schema_v2::emit_constant_schema_with_requests_in(pool.field(), schema, request, work)?;
    emit_record(pool, &prepared, schema, Some(request), work)
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

fn prepare<'pool>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared<'pool>, TypeCodecError> {
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

fn prepare_core<'pool, H>(
    pool: &'pool ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    mut parent: Option<&mut HostPoolAdmit<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared<'pool>, HostError<H>> {
    let policy = ipc_schema_v2::owner_admission::Policy(parent.is_some());
    let add = |a, b| policy.numeric(add(a, b));
    let mut growing = RecursivePoolWriteFacts {
        flat: if parent.is_some() {
            ipc_flat_pool_v2::progress::Admission::<H>::initial(
                source,
                pool.data().len(),
                limits.flat,
            )?
        } else {
            FlatPoolWriteFacts {
                source_retained_bytes: source,
                rows: pool.data().len(),
                buffer_descriptors: 0,
                variadic_buffers: 0,
                body_bytes: 0,
                schema_backing_bytes_upper_bound: 0,
                batch_backing_bytes_upper_bound: 0,
                encoded_stream_bytes_upper_bound: 0,
                new_allocation_request_bytes_upper_bound: 0,
                allocation_request_count_upper_bound: 0,
                coexisting_source_and_request_bytes_upper_bound: source,
                cumulative_library_work_upper_bound: 0,
            }
        },
        field_nodes: 0,
        total_rows: 0,
        view_fields: 0,
    };
    if parent.is_some() {
        growing.field_nodes = 1;
        growing.total_rows = pool.data().len();
        publish(&mut parent, &growing, limits)?;
        merge_schema(
            &mut growing,
            &ipc_schema_v2::initial_writer_request_facts(pool.field(), source, limits.flat.schema)?,
        )?;
        publish(&mut parent, &growing, limits)?;
    }
    if !LOCKED_FAMILY
        || !LOCKED_TOOLCHAIN
        || arrow::ARROW_VERSION != crate::resource_source_model::LOCKED_ARROW_VERSION
        || !cfg!(target_endian = "little")
    {
        return Err((shape("recursive writer source model does not match this target")).into());
    }
    if source
        < usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
            .map_err(|_| shape("source retention is not representable"))?
    {
        return Err((shape("recursive writer source retention omits checked backing")).into());
    }
    work.step()?;
    // This O(1) source-owner bound precedes its first type-walker allocation.
    let prefix = if parent.is_some() {
        ipc_schema_v2::schema_writer_prefix_resources_with_host_in(
            pool.field(),
            source,
            limits.flat.schema,
            limits.flat.max_cumulative_library_work,
            &mut |facts| {
                merge_schema(&mut growing, facts)?;
                publish(&mut parent, &growing, limits)
            },
            work,
        )?
    } else {
        schema_writer_prefix_resources(
            pool.field(),
            source,
            limits.flat.schema,
            limits.flat.max_cumulative_library_work,
            work,
        )?
    };
    gate_requests(
        prefix.request_bytes,
        source,
        prefix.work_upper_bound,
        limits.flat,
    )?;
    work.step()?;
    let schema_prefix = growing.flat;
    let geometry = if parent.is_some() {
        geometry::inspect_with_host_in(
            pool.data(),
            limits,
            &mut |geometry| {
                growing.field_nodes = geometry.nodes;
                growing.total_rows = geometry.total_rows;
                growing.view_fields = geometry.view_fields;
                growing.flat.buffer_descriptors = geometry.buffers;
                growing.flat.variadic_buffers = geometry.variadic;
                growing.flat.body_bytes = geometry.body_bytes;
                let batch = policy.numeric(ipc_flat_pool_v2::header::backing_counts(
                    geometry.nodes,
                    geometry.buffers,
                    geometry.view_fields,
                ))?;
                let capacity = add(
                    add(
                        policy.numeric(ipc_flat_pool_v2::metadata_capacity(batch))?,
                        geometry.body_bytes,
                    )?,
                    24,
                )?;
                let req = ipc_flat_pool_v2::allocations::batch_and_stream_requests_core(
                    batch, capacity, None,
                )?;
                growing.flat.new_allocation_request_bytes_upper_bound = growing
                    .flat
                    .new_allocation_request_bytes_upper_bound
                    .max(add(
                        schema_prefix.new_allocation_request_bytes_upper_bound,
                        req.bytes,
                    )?);
                growing.flat.allocation_request_count_upper_bound =
                    growing.flat.allocation_request_count_upper_bound.max(add(
                        schema_prefix.allocation_request_count_upper_bound,
                        req.count,
                    )?);
                growing.flat.coexisting_source_and_request_bytes_upper_bound = add(
                    source,
                    growing.flat.new_allocation_request_bytes_upper_bound,
                )?;
                growing.flat.encoded_stream_bytes_upper_bound =
                    growing.flat.encoded_stream_bytes_upper_bound.max(capacity);
                growing.flat.cumulative_library_work_upper_bound =
                    growing.flat.cumulative_library_work_upper_bound.max(add(
                        schema_prefix.cumulative_library_work_upper_bound,
                        policy.numeric(own_work(
                            geometry,
                            add(
                                schema_prefix.new_allocation_request_bytes_upper_bound,
                                req.bytes,
                            )?,
                            add(
                                schema_prefix.allocation_request_count_upper_bound,
                                req.count,
                            )?,
                        ))?,
                    )?);
                publish(&mut parent, &growing, limits)?;
                Ok(())
            },
            work,
        )?
    } else {
        geometry::inspect(pool.data(), limits, work)?
    };
    let schema_token = if parent.is_some() {
        Some(ipc_schema_v2::prepare_schema_writer_with_prefix_host_in(
            pool.field(),
            source,
            limits.flat.schema,
            limits.flat.max_cumulative_library_work,
            prefix,
            &mut |facts| {
                merge_schema(&mut growing, facts)?;
                publish(&mut parent, &growing, limits)
            },
            work,
        )?)
    } else {
        None
    };
    let legacy_resources = if schema_token.is_none() {
        Some(preflight_schema_writer_resources(
            pool.field(),
            source,
            limits.flat.schema,
            prefix,
            work,
        )?)
    } else {
        None
    };
    let (schema, schema_facts) = if let Some(token) = &schema_token {
        (token.schema(), token.facts())
    } else {
        let resources = legacy_resources.as_ref().expect("legacy schema resources");
        (
            resources.schema,
            ipc_schema_v2::SchemaWriterRequestFacts {
                request_bytes: resources.request_bytes,
                request_count: resources.request_count,
                work_upper_bound: resources.work_upper_bound,
            },
        )
    };
    let schema_resources = schema_facts;
    let construction = policy.numeric(ipc_flat_pool_v2::construction_from_counts(
        &schema,
        geometry.nodes,
        geometry.buffers,
        geometry.view_fields,
        geometry.body_bytes,
    ))?;
    let batch_backing = construction.batch_backing;
    let stream_capacity = construction.stream_backing;
    if stream_capacity > limits.flat.max_encoded_stream_bytes {
        return Err((if policy.0 {
            CompileControlError::ResourceExhausted.into()
        } else {
            shape("recursive writer stream envelope exceeded")
        })
        .into());
    }
    policy.numeric(
        std::alloc::Layout::array::<u8>(stream_capacity)
            .map_err(|_| shape("recursive writer stream request is not representable")),
    )?;
    if parent.is_some() {
        let req = ipc_flat_pool_v2::allocations::batch_and_stream_requests_core(
            batch_backing,
            stream_capacity,
            None,
        )?;
        growing.flat.schema_backing_bytes_upper_bound = schema.backing;
        growing.flat.batch_backing_bytes_upper_bound = batch_backing;
        growing.flat.encoded_stream_bytes_upper_bound = stream_capacity;
        growing.flat.new_allocation_request_bytes_upper_bound =
            add(schema_resources.request_bytes, req.bytes)?;
        growing.flat.allocation_request_count_upper_bound =
            add(schema_resources.request_count, req.count)?;
        growing.flat.coexisting_source_and_request_bytes_upper_bound = add(
            source,
            growing.flat.new_allocation_request_bytes_upper_bound,
        )?;
        // The exact same final own-work author below is evaluated purely before
        // its request counter's first completed callback.
        growing.flat.cumulative_library_work_upper_bound = add(
            schema_resources.work_upper_bound,
            policy.numeric(own_work(
                &geometry,
                growing.flat.new_allocation_request_bytes_upper_bound,
                growing.flat.allocation_request_count_upper_bound,
            ))?,
        )?;
        publish(&mut parent, &growing, limits)?;
    }
    work.step()?;
    let batch_requests = batch_and_stream_requests(batch_backing, stream_capacity, work)?;
    let request_bytes = add(schema_resources.request_bytes, batch_requests.bytes)?;
    let request_count = add(schema_resources.request_count, batch_requests.count)?;
    let own_work = policy.numeric(own_work(&geometry, request_bytes, request_count))?;
    let cumulative_work = add(schema_resources.work_upper_bound, own_work)?;
    let coexist = policy.numeric(gate_requests(
        request_bytes,
        source,
        cumulative_work,
        limits.flat,
    ))?;
    if parent.is_none() {
        work.step()?;
    }
    let facts = RecursivePoolWriteFacts {
        flat: FlatPoolWriteFacts {
            source_retained_bytes: source,
            rows: geometry.rows,
            buffer_descriptors: geometry.buffers,
            variadic_buffers: geometry.variadic,
            body_bytes: geometry.body_bytes,
            schema_backing_bytes_upper_bound: schema.backing,
            batch_backing_bytes_upper_bound: batch_backing,
            encoded_stream_bytes_upper_bound: stream_capacity,
            new_allocation_request_bytes_upper_bound: request_bytes,
            allocation_request_count_upper_bound: request_count,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_library_work_upper_bound: cumulative_work,
        },
        field_nodes: geometry.nodes,
        total_rows: geometry.total_rows,
        view_fields: geometry.view_fields,
    };
    let mut facts = facts;
    if parent.is_some() {
        facts.flat.new_allocation_request_bytes_upper_bound = facts
            .flat
            .new_allocation_request_bytes_upper_bound
            .max(growing.flat.new_allocation_request_bytes_upper_bound);
        facts.flat.allocation_request_count_upper_bound = facts
            .flat
            .allocation_request_count_upper_bound
            .max(growing.flat.allocation_request_count_upper_bound);
        facts.flat.cumulative_library_work_upper_bound = facts
            .flat
            .cumulative_library_work_upper_bound
            .max(growing.flat.cumulative_library_work_upper_bound);
        facts.flat.coexisting_source_and_request_bytes_upper_bound =
            add(source, facts.flat.new_allocation_request_bytes_upper_bound)?;
        publish(&mut parent, &facts, limits)?;
        work.step()?;
    }
    Ok(Prepared {
        geometry,
        facts: Some(facts),
        construction,
        schema: schema_token,
    })
}

fn merge_schema(
    facts: &mut RecursivePoolWriteFacts,
    schema: &ipc_schema_v2::SchemaWriterRequestFacts,
) -> Result<(), CompileControlError> {
    facts.flat.new_allocation_request_bytes_upper_bound = facts
        .flat
        .new_allocation_request_bytes_upper_bound
        .max(schema.request_bytes);
    facts.flat.allocation_request_count_upper_bound = facts
        .flat
        .allocation_request_count_upper_bound
        .max(schema.request_count);
    facts.flat.cumulative_library_work_upper_bound = facts
        .flat
        .cumulative_library_work_upper_bound
        .max(schema.work_upper_bound);
    facts.flat.coexisting_source_and_request_bytes_upper_bound = add(
        facts.flat.source_retained_bytes,
        facts.flat.new_allocation_request_bytes_upper_bound,
    )
    .map_err(|_| CompileControlError::ResourceExhausted)?;
    Ok(())
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
        limits,
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
            limits,
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
        .and_then(|prepared| emit_prepared(pool, &prepared, Some(limits.flat.schema), &mut work));
    finish(work, result)
}

fn emit_prepared(
    pool: &ConstantPool,
    prepared: &Prepared<'_>,
    schema_limits: Option<ipc_schema_v2::IpcSchemaProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    emit_prepared_core::<std::convert::Infallible>(pool, prepared, schema_limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn emit_prepared_core<H>(
    pool: &ConstantPool,
    prepared: &Prepared<'_>,
    schema_limits: Option<ipc_schema_v2::IpcSchemaProjectionLimits>,
    mut parent: Option<&mut HostPoolAdmit<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    work.flush()?;
    let schema = if let Some(schema) = &prepared.schema {
        schema.emit_with_host_in(
            &mut |_| {
                if let Some(parent) = parent.as_deref_mut() {
                    parent(
                        prepared
                            .facts
                            .as_ref()
                            .expect("receiving recursive writer owns source facts"),
                    )?;
                }
                Ok(())
            },
            work,
        )?
    } else {
        match schema_limits {
            Some(limits) => {
                ipc_schema_v2::encode_single_field_schema(pool.field(), limits, work.control())?
            }
            None => ipc_schema_v2::encode_single_field_schema_pure_in(pool.field(), work)?,
        }
    };
    emit_record(pool, prepared, schema, None, work)
}

fn emit_record<H>(
    pool: &ConstantPool,
    prepared: &Prepared<'_>,
    schema: Vec<u8>,
    mut request: Option<&mut ipc_schema_v2::WriteRequest<'_, H>>,
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
    ipc_flat_pool_v2::frame(&mut output, &schema, work)?;
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
    ipc_flat_pool_v2::append(&mut output, &[255, 255, 255, 255, 0, 0, 0, 0], work)?;
    if output.len() > prepared.construction.stream_backing {
        return Err((shape("recursive writer result exceeds admitted stream capacity")).into());
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
