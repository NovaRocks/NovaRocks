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

//! Pre-array numerical admission for verified recursive IPC geometry.
//! The caller proves descriptors, ranges, NULL counts and node/type pairing.
//! This owner accepts only Struct/List/LargeList/Map and the existing flat
//! carriers. It constructs no Arrow backing and grants no allocation authority.

use super::*;

/// Upper bounds supplied by a checked geometry and its reader allocation
/// recipe. Retained capacity includes body backing, repairs and synthesized
/// offsets. Visits include descriptors discarded by the reader; UTF8 fallback
/// and repeated view references remain distinct, even below a NULL parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecursiveConstantResourceInput {
    pub buffer_count_upper_bound: u64,
    pub buffer_visits_bytes_upper_bound: u64,
    pub retained_buffer_capacity_bytes_upper_bound: u64,
    pub view_validation_bytes_upper_bound: u64,
    pub utf8_fallback_validation_bytes_upper_bound: u64,
}

/// Reuses the complete Field/FVT validation and the actual constant owner's
/// logical and opaque-validation numerical authors before array construction.
/// `node_lengths` is the complete stored length of each array in declaration
/// DFS preorder, including unused child prefix/suffix. A lazy wire iterator is
/// accepted without constructing an intermediate tree or length vector.
///
/// This does not prove raw geometry, semantic values, reader allocations or
/// MEM admission. Those obligations remain with their original owners.
pub fn preflight_recursive_pool_resources(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<ConstantResourceFacts, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = preflight_recursive_pool_resources_core(
        field,
        value_type,
        node_lengths,
        input,
        policy,
        &mut PoolObserver::plain(),
        &mut work,
    );
    if matches!(&result, Err(ConstantError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Same original lazy-node pre-array author, on the caller's work and growing
/// owned-resource contribution. Entry/footer remain entirely with the caller.
pub fn preflight_recursive_pool_resources_in(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantResourceFacts, ConstantError> {
    preflight_recursive_pool_resources_core(
        field,
        value_type,
        node_lengths,
        input,
        policy,
        &mut PoolObserver::parent(&mut |facts| admit(facts).map_err(ConstantError::from)),
        work,
    )
}
/// The same recursive resource author with the original caller refusal intact.
pub fn preflight_recursive_pool_resources_observed<H>(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantResourceFacts, ConstantCreationFailure<H>> {
    preflight_recursive_pool_resources_core(
        field,
        value_type,
        node_lengths,
        input,
        policy,
        &mut PoolObserver::parent(&mut |facts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        }),
        work,
    )
}
/// The same lazy-node resource author without an external receiving quota.
/// Its output seals only this input's checked finite numerical envelope.
pub fn preflight_recursive_pool_resources_input_derived_observed<H>(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ConstantResourceFacts, ConstantPolicy), ConstantCreationFailure<H>> {
    preflight_recursive_pool_resources_profile_core(
        field,
        value_type,
        node_lengths,
        input,
        ConstantValidation::INPUT_DERIVED,
        &mut PoolObserver::parent(&mut |facts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        }),
        work,
    )
}
fn preflight_recursive_pool_resources_core<E: PoolFailure>(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantResourceFacts, E> {
    preflight_recursive_pool_resources_profile_core(
        field,
        value_type,
        node_lengths,
        input,
        policy.into(),
        observer,
        work,
    )
    .map(|(facts, _)| facts)
}
fn preflight_recursive_pool_resources_profile_core<E: PoolFailure>(
    field: &Field,
    value_type: &FunctionValueType,
    node_lengths: impl IntoIterator<Item = u64>,
    input: RecursiveConstantResourceInput,
    validation: ConstantValidation,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ConstantResourceFacts, ConstantPolicy), E> {
    let type_facts =
        validate_type_profile_owned(field, value_type, validation.shape, observer, work)?;
    let policy = validation.volume;
    let metadata_bytes = type_facts.metadata;
    let mut lengths = node_lengths.into_iter();
    let mut scanned = ScanFacts::default();
    let root = scan_nodes(
        field.data_type(),
        1,
        &mut lengths,
        validation,
        &mut scanned,
        observer,
        work,
    )?;
    let extra = lengths.next();
    observer.step(work)?;
    if extra.is_some() {
        return Err(
            (ConstantError::Invalid("constant resource projection has extra nodes")).into(),
        );
    }
    let logical = logical_elements_observed_owned(
        root.len,
        scanned.storage_elements,
        root.max_value,
        observer,
        work,
    )?;
    optional_limit(
        logical,
        policy.map(|p| p.max_logical_elements),
        "constant logical element limit exceeded",
    )?;
    optional_limit(
        input.retained_buffer_capacity_bytes_upper_bound,
        policy.map(|p| p.max_retained_buffer_bytes),
        "constant retained buffer limit exceeded",
    )?;
    scanned.buffer_count = input.buffer_count_upper_bound;
    scanned.buffer_visits = input.buffer_visits_bytes_upper_bound;
    scanned.retained = input.retained_buffer_capacity_bytes_upper_bound;
    scanned.view_validation_bytes = input.view_validation_bytes_upper_bound;
    scanned.utf8_fallback_validation_bytes = input.utf8_fallback_validation_bytes_upper_bound;
    observer.step(work)?;
    let envelope = validation_envelope_owned(
        ValidationCounts::from(&scanned),
        metadata_bytes,
        validation,
        observer,
        work,
    )?;
    let sealed = ConstantPolicy {
        max_rows: root.len,
        max_array_nodes: scanned.nodes,
        max_logical_elements: logical,
        max_retained_buffer_bytes: scanned.retained,
        max_type_depth: u32::try_from(scanned.depth)
            .map_err(|_| ConstantError::Limit("constant array depth is not representable"))?,
        max_type_nodes: type_facts.nodes,
        max_dictionary_depth: 0,
        max_metadata_bytes: metadata_bytes,
        max_library_validation_work: envelope.work,
        max_library_validation_bytes: envelope.bytes,
    };
    Ok((
        ConstantResourceFacts {
            rows: root.len,
            array_nodes: scanned.nodes,
            buffer_count: scanned.buffer_count,
            logical_elements_upper_bound: logical,
            retained_buffer_capacity_bytes: scanned.retained,
            metadata_bytes,
            library_validation_work_upper_bound: envelope.work,
            library_validation_temporary_bytes_upper_bound: envelope.temporary,
            library_validation_bytes_upper_bound: envelope.bytes,
        },
        sealed,
    ))
}

fn scan_nodes<E: PoolFailure>(
    ty: &DataType,
    depth: u32,
    lengths: &mut impl Iterator<Item = u64>,
    validation: ConstantValidation,
    scanned: &mut ScanFacts,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ElementMetric, E> {
    limit(
        u64::from(depth),
        u64::from(validation.shape.depth),
        "constant array depth limit exceeded",
    )?;
    limit(
        u64::from(depth),
        novarocks_type_contract::MAX_ARROW_TYPE_PATH_DEPTH as u64,
        "constant array exceeds intrinsic Arrow type depth",
    )?;
    let next = lengths.next();
    observer.step(work)?;
    let len = next.ok_or(ConstantError::Invalid(
        "constant resource projection lacks an expected node",
    ))?;
    if depth == 1 {
        optional_limit(
            len,
            validation.volume.map(|p| p.max_rows),
            "constant row limit exceeded",
        )?;
    }
    scanned.nodes = checked_add(scanned.nodes, 1)?;
    optional_limit(
        scanned.nodes,
        validation.volume.map(|p| p.max_array_nodes),
        "constant array node limit exceeded",
    )?;
    scanned.depth = scanned.depth.max(u64::from(depth));
    scanned.storage_elements = checked_add(scanned.storage_elements, len)?;
    optional_limit(
        scanned.storage_elements,
        validation.volume.map(|p| p.max_logical_elements),
        "constant stored element limit exceeded",
    )?;
    observer.step(work)?;
    let max_value = match ty {
        DataType::Struct(fields) => value_elements(
            ty,
            fields.iter().map(|field| {
                scan_nodes(
                    field.data_type(),
                    depth + 1,
                    lengths,
                    validation,
                    scanned,
                    observer,
                    work,
                )
            }),
        ),
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            value_elements(
                ty,
                std::iter::once_with(|| {
                    scan_nodes(
                        field.data_type(),
                        depth + 1,
                        lengths,
                        validation,
                        scanned,
                        observer,
                        work,
                    )
                }),
            )
        }
        _ if is_flat_resource_carrier(ty) => value_elements(ty, std::iter::empty()),
        _ => Err(ConstantError::Invalid(
            "constant resource projection has an unsupported recursive carrier",
        )
        .into()),
    };
    // A nested refusal is primary: do not observe again after it. Ordinary
    // metric errors still account for the completed variant/math work.
    if max_value.as_ref().is_err_and(|error| error.skip_tail()) {
        return max_value.map(|max_value| ElementMetric { len, max_value });
    }
    observer.step(work)?;
    Ok(ElementMetric {
        len,
        max_value: max_value?,
    })
}

#[cfg(test)]
#[path = "recursive_resource_tests.rs"]
mod tests;
