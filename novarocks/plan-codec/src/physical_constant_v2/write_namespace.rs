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

use super::{
    ConstantNamespaceProjectionLimits, ConstantWriteProjectionLimits,
    PhysicalConstantCodecError as Error, PreparedConstantRecordWrite,
    binding_resources::{
        preflight_constant_binding_resources, preflight_constant_binding_resources_in,
    },
    finish, prepare_constant_record_write, prepare_constant_record_write_in, recursive,
};
use crate::{
    host_projection_v2::ProjectionFailure, ipc_flat_pool_v2::encode_flat_pool_input_derived_in,
    ipc_recursive_pool_v2::encode_recursive_pool_input_derived_in,
    ipc_schema_v2::ConstantWriteRequest, physical_type_v2::EncodedTypeTable,
};
use arrow::datatypes::Field;
use novarocks_physical_plan::{ConstantPoolId, ConstantPools};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    arrow_data_types_exact_borrowed_observed, arrow_fields_exact_borrowed_observed,
};
use std::{alloc::Layout, sync::Arc};

/// The sole type-table author binds one pair of IDs per pool in the original
/// ConstantPools canonical iteration order. These are separate namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantRecordTypeIds {
    pub pool: ConstantPoolId,
    pub value_type_id: u32,
    pub field_id: u32,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantNamespaceWriteFacts {
    pub record_count: usize,
    pub source_retained_bytes: usize,
    pub prepared_storage_request_bytes: usize,
    pub record_storage_request_bytes: usize,
    pub writer_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub binding_work_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}
type Admit<'a> = dyn FnMut(&ConstantNamespaceWriteFacts) -> Result<(), CompileControlError> + 'a;
fn gate_parent(
    facts: &ConstantNamespaceWriteFacts,
    limits: ConstantNamespaceProjectionLimits,
    admit: &mut Admit<'_>,
) -> Result<(), CompileControlError> {
    for (actual, maximum) in [
        (facts.record_count, limits.max_records),
        (
            facts.prepared_storage_request_bytes,
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
            return Err(CompileControlError::ResourceExhausted);
        }
    }
    admit(facts)
}

/// Retaining the immutable table loan prevents changing its DTO between the
/// exact source binding and emission. Source roots and pools also stay borrowed.
pub struct PreparedConstantNamespaceWrite<'pool, 'table, 'control> {
    records: Vec<PreparedConstantRecordWrite<'pool, 'control>>,
    facts: ConstantNamespaceWriteFacts,
    _types: &'table EncodedTypeTable<'table>,
    control: &'control dyn PureCompileControl,
}
impl PreparedConstantNamespaceWrite<'_, '_, '_> {
    pub const fn facts(&self) -> &ConstantNamespaceWriteFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<Vec<wire::IpcConstantPool>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self.emit_core(None, &mut work);
        finish(work, result)
    }
    #[cfg(test)]
    pub(crate) fn emit_in(
        self,
        admit: &mut Admit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<wire::IpcConstantPool>, Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(shape(
                "constant writer namespace caller work has a different original control",
            ));
        }
        admit(&self.facts)?;
        self.emit_core(Some(admit), work)
    }
    fn emit_core(
        self,
        mut admit: Option<&mut Admit<'_>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<wire::IpcConstantPool>, Error> {
        (|| {
            work.flush()?;
            let mut output = Vec::new();
            let reserved = output.try_reserve_exact(self.records.len());
            crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
            for record in self.records {
                work.step()?;
                work.flush()?;
                let encoded = if let Some(parent) = &mut admit {
                    let known = *record.facts();
                    record.emit_in(
                        &mut |progress| {
                            if progress.new_allocation_request_bytes_upper_bound
                                > known.new_allocation_request_bytes_upper_bound
                                || progress.allocation_request_count_upper_bound
                                    > known.allocation_request_count_upper_bound
                                || progress.cumulative_library_work_upper_bound
                                    > known.cumulative_library_work_upper_bound
                            {
                                return Err(CompileControlError::ResourceExhausted);
                            }
                            parent(&self.facts)
                        },
                        work,
                    )?
                } else {
                    record.emit()?
                };
                work.flush()?;
                output.push(encoded);
                work.step()?;
            }
            Ok(output)
        })()
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_add(right)
        .ok_or_else(|| shape("constant namespace writer resource sum overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_mul(right)
        .ok_or_else(|| shape("constant namespace writer resource product overflow"))
}
fn array_bytes<T>(count: usize) -> Result<usize, Error> {
    Layout::array::<T>(count)
        .map(|value| value.size())
        .map_err(|_| shape("constant namespace writer source layout is unrepresentable"))
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
fn check(
    facts: &ConstantNamespaceWriteFacts,
    limits: ConstantNamespaceProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(
        facts.prepared_storage_request_bytes,
        limits.max_preparation_request_bytes,
        "constant namespace writer preparation envelope exceeded",
        work,
    )?;
    cap(
        facts.new_allocation_request_bytes_upper_bound,
        limits.max_new_allocation_request_bytes,
        "constant namespace writer request envelope exceeded",
        work,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        "constant namespace writer coexistence envelope exceeded",
        work,
    )?;
    cap(
        facts.cumulative_library_work_upper_bound,
        limits.max_cumulative_library_work,
        "constant namespace writer work envelope exceeded",
        work,
    )
}

fn initial_header(
    count: usize,
    types: &EncodedTypeTable<'_>,
    source_retained_bytes: usize,
) -> Result<ConstantNamespaceWriteFacts, Error> {
    let storage = Layout::array::<PreparedConstantRecordWrite<'_, '_>>(count)
        .map_err(|_| shape("constant namespace writer preparation layout is unrepresentable"))?
        .size();
    let output = Layout::array::<wire::IpcConstantPool>(count)
        .map_err(|_| shape("constant namespace writer record layout is unrepresentable"))?
        .size();
    let (values, fields) = types.source_counts();
    let lookups = mul(count, add(values, fields)?)?;
    let own_work = add(add(storage, output)?, add(lookups, mul(count, 32)?)?)?;
    let requests = add(storage, output)?;
    let facts = ConstantNamespaceWriteFacts {
        record_count: count,
        source_retained_bytes,
        prepared_storage_request_bytes: storage,
        record_storage_request_bytes: output,
        allocation_request_count_upper_bound: if count == 0 { 0 } else { 2 },
        new_allocation_request_bytes_upper_bound: requests,
        coexisting_source_and_request_bytes_upper_bound: add(source_retained_bytes, requests)?,
        cumulative_library_work_upper_bound: own_work,
        ..Default::default()
    };
    Ok(facts)
}

/// All retained type DTO/root/pool/binding backing belongs in the mandatory
/// source invoice. Existing type/schema scratch stages retain their own prior
/// gates; these aggregate output request facts do not retroactively grant them.
pub fn prepare_constant_namespace_write<'pool, 'table, 'control>(
    pools: &'pool ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &'table EncodedTypeTable<'table>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedConstantNamespaceWrite<'pool, 'table, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare_core(
        pools,
        bindings,
        types,
        source_retained_bytes,
        record_limits,
        limits,
        None,
        &mut work,
    );
    finish(work, result)
}

#[cfg(test)]
pub(crate) fn prepare_constant_namespace_write_in<'pool, 'table, 'control>(
    pools: &'pool ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &'table EncodedTypeTable<'table>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespaceWrite<'pool, 'table, 'control>, Error> {
    prepare_core(
        pools,
        bindings,
        types,
        source_retained_bytes,
        record_limits,
        limits,
        Some(admit),
        work,
    )
}

/// Encode the original pools under the active creator's individual requests.
pub fn encode_constant_namespace_input_derived_in<H>(
    pools: &ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &EncodedTypeTable<'_>,
    request: &mut dyn FnMut(ConstantWriteRequest) -> Result<(), H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::IpcConstantPool>, ProjectionFailure<Error, H>> {
    let count = pools.entries().len();
    if bindings.len() != count {
        return Err(
            shape("constant namespace writer requires one binding per original pool").into(),
        );
    }
    let layout = Layout::array::<wire::IpcConstantPool>(count)
        .map_err(|_| shape("constant namespace writer record layout is unrepresentable"))?;
    let mut output = Vec::new();
    work.flush().map_err(Error::from)?;
    if layout.size() != 0 {
        request(ConstantWriteRequest::Layout(layout))
            .map_err(ProjectionFailure::<Error, H>::Host)?;
        let reserved = output.try_reserve_exact(count);
        crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
    }
    for ((id, pool), binding) in pools.entries().iter().zip(bindings) {
        work.step().map_err(Error::from)?;
        if *id != binding.pool {
            return Err(shape(
                "constant namespace writer binding order differs from original pools",
            )
            .into());
        }
        let ty = types
            .value_type_observed(binding.value_type_id, work)
            .map_err(Error::from)?
            .ok_or_else(|| {
                shape("constant namespace writer references an unknown source value type")
            })?;
        let field = types
            .source_field_observed(binding.field_id, work)
            .map_err(Error::from)?
            .ok_or_else(|| shape("constant namespace writer references an unknown source Field"))?;
        if ty.nullable() != pool.value_type().nullable()
            || ty.logical_type() != pool.value_type().logical_type()
            || !arrow_data_types_exact_borrowed_observed(
                pool.value_type().data_type(),
                ty.data_type(),
                || work.step().map_err(Error::from),
            )?
        {
            return Err(shape(
                "constant namespace writer source value type differs from original pool",
            )
            .into());
        }
        if !Arc::ptr_eq(pool.field_ref(), field)
            && !arrow_fields_exact_borrowed_observed(pool.field(), field, || {
                work.step().map_err(Error::from)
            })?
        {
            return Err(
                shape("constant namespace writer source Field differs from original pool").into(),
            );
        }
        let mut authorize = |operation| request(operation).map_err(ProjectionFailure::Host);
        let arrow_ipc = if recursive(pool.field().data_type()) {
            encode_recursive_pool_input_derived_in(pool, &mut authorize, work)
        } else {
            encode_flat_pool_input_derived_in(pool, &mut authorize, work)
        }
        .map_err(|error| match error {
            ProjectionFailure::Codec(cause) => ProjectionFailure::Codec(Error::from(cause)),
            ProjectionFailure::Host(cause) => ProjectionFailure::Host(cause),
        })?;
        output.push(wire::IpcConstantPool {
            id: id.get(),
            value_type_id: Some(binding.value_type_id),
            source_field_id: Some(binding.field_id),
            compression: wire::IpcCompression::Uncompressed as i32,
            arrow_ipc,
        });
    }
    work.flush().map_err(Error::from)?;
    Ok(output)
}

fn prepare_core<'pool, 'table, 'control>(
    pools: &'pool ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &'table EncodedTypeTable<'table>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    mut admit: Option<&mut Admit<'_>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantNamespaceWrite<'pool, 'table, 'control>, Error> {
    (|| {
        let count = pools.entries().len();
        if let Some(parent) = &mut admit {
            let known = initial_header(count, types, source_retained_bytes)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            gate_parent(&known, limits, *parent)?;
        }
        cap(
            count,
            limits.max_records,
            "constant namespace writer count envelope exceeded",
            work,
        )?;
        let lengths_match = bindings.len() == count;
        work.step()?;
        if !lengths_match {
            return Err(shape(
                "constant namespace writer requires one binding per original pool",
            ));
        }
        let mut facts = initial_header(count, types, source_retained_bytes)?;
        let storage = facts.prepared_storage_request_bytes;
        let requests = facts.new_allocation_request_bytes_upper_bound;
        let (values, fields) = types.source_counts();
        check(&facts, limits, work)?;
        // Known retained source lower bounds must use the ORIGINAL invoice.
        // Adding our new prepared storage would mask a missing input charge.
        // This top-level floor is not a complete invoice for nested strings,
        // source spare capacity or deleted metadata buckets; the host owns it.
        let table = types.as_wire();
        let mut source_floor = array_bytes::<ConstantRecordTypeIds>(count)?;
        source_floor = add(
            source_floor,
            array_bytes::<(u32, FunctionValueType)>(values)?,
        )?;
        source_floor = add(source_floor, array_bytes::<(u32, Arc<Field>)>(fields)?)?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
                table.carriers.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
                table.fields.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
                table.value_types.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            array_bytes::<novarocks_proto_models::value_type::LogicalTypeDefinition>(
                table.logical_types.capacity(),
            )?,
        )?;
        cap(
            source_floor,
            source_retained_bytes,
            "constant namespace writer source invoice excludes type and binding storage",
            work,
        )?;
        for pool in pools.entries().values() {
            let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
                .map_err(|_| {
                    shape("constant namespace writer original retained extent is unrepresentable")
                });
            work.step()?;
            cap(
                retained?,
                source_retained_bytes,
                "constant namespace writer source invoice excludes original pool backing",
                work,
            )?;
        }
        work.flush()?;
        let mut prepared = Vec::new();
        let reserved = prepared.try_reserve_exact(count);
        crate::allocation_exit_v2::reserve_exit::<Error>(reserved, work)?;
        for ((id, pool), binding) in pools.entries().iter().zip(bindings) {
            let id_matches = *id == binding.pool;
            work.step()?;
            if !id_matches {
                return Err(shape(
                    "constant namespace writer binding order differs from original pools",
                ));
            }
            let ty = types
                .value_type_observed(binding.value_type_id, work)?
                .ok_or_else(|| {
                    shape("constant namespace writer references an unknown source value type")
                })?;
            let mut captured_binding = None;
            let binding_base = facts;
            let captured_source = if admit.is_some() {
                Some(
                    add(source_retained_bytes, storage)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                )
            } else {
                None
            };
            let field = if let Some(parent) = &mut admit {
                types.source_field_captured(
                    binding.field_id,
                    &mut |field, work| {
                        // Invalid semantic flags still belong to the original author.
                        if ty.nullable() == pool.value_type().nullable()
                            && ty.logical_type() == pool.value_type().logical_type()
                        {
                            captured_binding = Some(preflight_constant_binding_resources_in(
                                pool,
                                field,
                                captured_source.ok_or_else(|| {
                                    shape("constant caller binding source is absent")
                                })?,
                                limits
                                    .max_cumulative_library_work
                                    .checked_sub(binding_base.cumulative_library_work_upper_bound)
                                    .ok_or(CompileControlError::ResourceExhausted)?,
                                &mut |prefix| {
                                    let mut current = binding_base;
                                    current.binding_work_upper_bound = binding_base
                                        .binding_work_upper_bound
                                        .checked_add(prefix)
                                        .ok_or(CompileControlError::ResourceExhausted)?;
                                    current.cumulative_library_work_upper_bound = binding_base
                                        .cumulative_library_work_upper_bound
                                        .checked_add(prefix)
                                        .ok_or(CompileControlError::ResourceExhausted)?;
                                    gate_parent(&current, limits, *parent)
                                },
                                work,
                            )?);
                        }
                        Ok::<_, Error>(())
                    },
                    work,
                )?
            } else {
                types.source_field_observed(binding.field_id, work)?
            }
            .ok_or_else(|| shape("constant namespace writer references an unknown source Field"))?;
            let flags_match = ty.nullable() == pool.value_type().nullable()
                && ty.logical_type() == pool.value_type().logical_type();
            work.step()?;
            if !flags_match {
                return Err(shape(
                    "constant namespace writer source value type differs from original pool",
                ));
            }
            let live_source = match captured_source {
                Some(source) => source,
                None => add(source_retained_bytes, storage)?,
            };
            let binding_facts = if let Some(captured) = captured_binding {
                captured
            } else {
                preflight_constant_binding_resources(
                    pool,
                    field,
                    live_source,
                    limits.max_cumulative_library_work - facts.cumulative_library_work_upper_bound,
                    work,
                )?
            };
            facts.binding_work_upper_bound = add(
                facts.binding_work_upper_bound,
                binding_facts.work_upper_bound(),
            )?;
            facts.cumulative_library_work_upper_bound = add(
                facts.cumulative_library_work_upper_bound,
                binding_facts.work_upper_bound(),
            )?;
            if let Some(parent) = &mut admit {
                gate_parent(&facts, limits, *parent)?;
            }
            check(&facts, limits, work)?;
            work.flush()?;
            let types_match = arrow_data_types_exact_borrowed_observed(
                pool.value_type().data_type(),
                ty.data_type(),
                || work.step().map_err(Error::from),
            )?;
            work.step()?;
            if !types_match {
                return Err(shape(
                    "constant namespace writer source value type differs from original pool",
                ));
            }
            let fields_match = if binding_facts.compare_full_field {
                arrow_fields_exact_borrowed_observed(pool.field(), field, || {
                    work.step().map_err(Error::from)
                })?
            } else {
                true
            };
            work.step()?;
            if !fields_match {
                return Err(shape(
                    "constant namespace writer source Field differs from original pool",
                ));
            }
            if admit.is_none() {
                work.flush()?;
            }
            let writer_base = facts;
            let writer = if let Some(parent) = &mut admit {
                prepare_constant_record_write_in(
                    *id,
                    binding.value_type_id,
                    binding.field_id,
                    pool,
                    live_source,
                    record_limits,
                    &mut |prefix| {
                        let current = with_writer(writer_base, prefix)?;
                        gate_parent(&current, limits, *parent)
                    },
                    work,
                )?
            } else {
                prepare_constant_record_write(
                    *id,
                    binding.value_type_id,
                    binding.field_id,
                    pool,
                    live_source,
                    record_limits,
                    work.control(),
                )?
            };
            work.flush()?;
            let writer_facts = writer.facts();
            facts.writer_request_bytes_upper_bound = add(
                facts.writer_request_bytes_upper_bound,
                writer_facts.new_allocation_request_bytes_upper_bound,
            )?;
            facts.allocation_request_count_upper_bound = add(
                facts.allocation_request_count_upper_bound,
                writer_facts.allocation_request_count_upper_bound,
            )?;
            facts.cumulative_library_work_upper_bound = add(
                facts.cumulative_library_work_upper_bound,
                writer_facts.cumulative_library_work_upper_bound,
            )?;
            facts.new_allocation_request_bytes_upper_bound =
                add(requests, facts.writer_request_bytes_upper_bound)?;
            facts.coexisting_source_and_request_bytes_upper_bound = add(
                source_retained_bytes,
                facts.new_allocation_request_bytes_upper_bound,
            )?;
            if let Some(parent) = &mut admit {
                gate_parent(&facts, limits, *parent)?;
            }
            check(&facts, limits, work)?;
            prepared.push(writer);
            work.step()?;
        }
        Ok(PreparedConstantNamespaceWrite {
            records: prepared,
            facts,
            _types: types,
            control: work.control(),
        })
    })()
}

pub fn encode_constant_namespace(
    pools: &ConstantPools,
    bindings: &[ConstantRecordTypeIds],
    types: &EncodedTypeTable<'_>,
    source_retained_bytes: usize,
    record_limits: ConstantWriteProjectionLimits,
    limits: ConstantNamespaceProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<wire::IpcConstantPool>, Error> {
    prepare_constant_namespace_write(
        pools,
        bindings,
        types,
        source_retained_bytes,
        record_limits,
        limits,
        control,
    )?
    .emit()
}

fn with_writer(
    base: ConstantNamespaceWriteFacts,
    writer: &crate::ipc_flat_pool_v2::FlatPoolWriteFacts,
) -> Result<ConstantNamespaceWriteFacts, CompileControlError> {
    let sum = |a: usize, b: usize| {
        a.checked_add(b)
            .ok_or(CompileControlError::ResourceExhausted)
    };
    let mut facts = base;
    facts.writer_request_bytes_upper_bound = sum(
        base.writer_request_bytes_upper_bound,
        writer.new_allocation_request_bytes_upper_bound,
    )?;
    facts.allocation_request_count_upper_bound = sum(
        base.allocation_request_count_upper_bound,
        writer.allocation_request_count_upper_bound,
    )?;
    facts.cumulative_library_work_upper_bound = sum(
        base.cumulative_library_work_upper_bound,
        writer.cumulative_library_work_upper_bound,
    )?;
    facts.new_allocation_request_bytes_upper_bound = sum(
        sum(
            facts.prepared_storage_request_bytes,
            facts.record_storage_request_bytes,
        )?,
        facts.writer_request_bytes_upper_bound,
    )?;
    facts.coexisting_source_and_request_bytes_upper_bound = sum(
        facts.source_retained_bytes,
        facts.new_allocation_request_bytes_upper_bound,
    )?;
    Ok(facts)
}
