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

//! Controlled owned collection construction from actual caller source facts.
//! No Unpivot key policy, nominal retag or host allocation grant is authored here.

use super::{
    ConstantError, ConstantPolicy, ConstantPool, ConstantValue, ValidationCounts, checked_add,
    checked_mul, limit, validation_envelope_core,
};
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{alloc::Layout, sync::Arc};

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, ConstantError>,
) -> Result<T, ConstantError> {
    if matches!(
        &result,
        Err(ConstantError::Control(_) | ConstantError::Limit(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn reserve<T>(n: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, ConstantError> {
    Layout::array::<T>(n)
        .map_err(|_| ConstantError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(n)
        .map_err(|_| ConstantError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(output)
}
// Actual finite construction geometry, passed to the original policy author.
// This describes a recipe; it owns no separate budget or admission authority.
struct CollectionRecipe {
    nodes: u64,
    depth: u32,
    elements: u64,
    buffers: u64,
    backing: u64,
}
fn preflight(
    field: &Field,
    ty: &FunctionValueType,
    recipe: CollectionRecipe,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    preflight_observed(
        field,
        ty,
        recipe,
        policy,
        &mut super::PoolObserver::plain(),
        work,
    )
    .map(|_| ())
}
fn preflight_observed<E: super::PoolFailure>(
    field: &Field,
    ty: &FunctionValueType,
    recipe: CollectionRecipe,
    policy: ConstantPolicy,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, E> {
    let CollectionRecipe {
        nodes,
        depth,
        elements,
        buffers,
        backing,
    } = recipe;
    let metadata = super::validate_type_owned(field, ty, policy, observer, work)?;
    limit(1, policy.max_rows, "collection factory row limit exceeded")?;
    limit(
        nodes,
        policy.max_array_nodes,
        "collection factory array node limit exceeded",
    )?;
    limit(
        u64::from(depth),
        u64::from(policy.max_type_depth),
        "collection factory depth limit exceeded",
    )?;
    limit(
        elements,
        policy.max_logical_elements,
        "collection factory logical element limit exceeded",
    )?;
    // Buffer::from_vec retains the actual Vec allocation. Include fixed Arrow
    // representation headers conservatively, before the first Vec reserve.
    let retained = checked_add(
        backing,
        checked_add(
            checked_mul(nodes, std::mem::size_of::<ArrayData>() as u64)?,
            checked_mul(buffers, std::mem::size_of::<Buffer>() as u64)?,
        )?,
    )?;
    limit(
        retained,
        policy.max_retained_buffer_bytes,
        "collection factory backing limit exceeded",
    )?;
    let bounds = validation_envelope_core(
        ValidationCounts {
            nodes,
            storage_elements: elements,
            buffer_count: buffers,
            buffer_visits: backing,
            view_validation_bytes: 0,
            utf8_fallback_validation_bytes: 0,
            masks: 0,
            depth: u64::from(depth),
        },
        metadata,
        policy,
    )?;
    // Each safe builder can validate descendants; pool admission validates
    // the complete backing again. Reuse the sole numerical envelope for a
    // conservative count of these real finite opaque passes.
    limit(
        checked_mul(bounds.work, checked_add(nodes, 1)?)?,
        policy.max_library_validation_work,
        "collection factory library work limit exceeded",
    )?;
    limit(
        checked_mul(bounds.bytes, checked_add(nodes, 1)?)?,
        policy.max_library_validation_bytes,
        "collection factory library byte limit exceeded",
    )?;
    observer.step(work)?;
    Ok(retained)
}
fn array(
    builder: arrow_data::ArrayDataBuilder,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ArrayData, ConstantError> {
    work.flush()?;
    let result = builder.build().map_err(ConstantError::arrow_validation);
    work.flush()?;
    result
}
fn buffer<T: arrow_buffer::ArrowNativeType>(
    source: Vec<T>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Buffer, ConstantError> {
    work.flush()?;
    let buffer = Buffer::from_vec(source);
    work.flush()?;
    Ok(buffer)
}
fn copied_bytes(
    output: &mut Vec<u8>,
    source: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantError> {
    for chunk in source.chunks(256) {
        output.extend_from_slice(chunk);
        for _ in chunk {
            work.step()?;
        }
    }
    Ok(())
}

impl ConstantValue {
    /// Construct one actual non-NULL List<Int32> row using the caller's exact
    /// Field/FVT. Declared nullability and nested metadata are never replaced.
    pub fn from_int32_list(
        field: Arc<Field>,
        ty: FunctionValueType,
        values: &[i32],
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = (|| {
            let valid = matches!(ty.data_type(), DataType::List(child) if child.data_type() == &DataType::Int32);
            work.step()?;
            if !valid {
                return Err(ConstantError::Invalid(
                    "Int32 collection factory requires exact List<Int32> carrier",
                ));
            }
            let count = u64::try_from(values.len())
                .map_err(|_| ConstantError::Limit("collection source length overflow"))?;
            let end = i32::try_from(values.len())
                .map_err(|_| ConstantError::Limit("Int32 list offset exceeds carrier"))?;
            let backing = checked_add(8, checked_mul(count, 4)?)?;
            preflight(
                &field,
                &ty,
                CollectionRecipe {
                    nodes: 2,
                    depth: 2,
                    elements: checked_add(1, count)?,
                    buffers: 2,
                    backing,
                },
                policy,
                &mut work,
            )?;
            let mut copied = reserve(values.len(), &mut work)?;
            for &value in values {
                copied.push(value);
                work.step()?;
            }
            let mut offsets = reserve(2, &mut work)?;
            offsets.push(0_i32);
            offsets.push(end);
            work.step()?;
            let child_buffer = buffer(copied, &mut work)?;
            let child = array(
                ArrayData::builder(DataType::Int32)
                    .len(values.len())
                    .add_buffer(child_buffer),
                &mut work,
            )?;
            let offset_buffer = buffer(offsets, &mut work)?;
            let data = array(
                ArrayData::builder(field.data_type().clone())
                    .len(1)
                    .add_buffer(offset_buffer)
                    .add_child_data(child),
                &mut work,
            )?;
            work.flush()?;
            let pool = ConstantPool::try_new(field, ty, data, policy, phase, control)?;
            work.flush()?;
            pool.value(0)
        })();
        finish(work, result)
    }
    /// Construct one actual non-NULL Map<Utf8,Utf8> row without sorting or
    /// deduplicating its keys. The original consumer owns those semantics.
    pub fn from_utf8_map(
        field: Arc<Field>,
        ty: FunctionValueType,
        entries: &[(String, String)],
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = (|| {
            let entries_type = match field.data_type() {
                DataType::Map(entry, _) => match entry.data_type() {
                    DataType::Struct(fields)
                        if fields.len() == 2
                            && fields
                                .iter()
                                .all(|field| field.data_type() == &DataType::Utf8) =>
                    {
                        entry.data_type()
                    }
                    _ => {
                        return Err(ConstantError::Invalid(
                            "UTF8 map factory requires exact two UTF8 entry children",
                        ));
                    }
                },
                _ => {
                    return Err(ConstantError::Invalid(
                        "UTF8 map factory requires exact Map carrier",
                    ));
                }
            };
            let mut key_bytes = 0_usize;
            let mut value_bytes = 0_usize;
            for (key, value) in entries {
                key_bytes = key_bytes
                    .checked_add(key.len())
                    .ok_or(ConstantError::Limit("map key byte count overflow"))?;
                value_bytes = value_bytes
                    .checked_add(value.len())
                    .ok_or(ConstantError::Limit("map value byte count overflow"))?;
                work.step()?;
            }
            let end = i32::try_from(entries.len())
                .map_err(|_| ConstantError::Limit("map entry offset exceeds carrier"))?;
            i32::try_from(key_bytes)
                .map_err(|_| ConstantError::Limit("map key offsets exceed carrier"))?;
            i32::try_from(value_bytes)
                .map_err(|_| ConstantError::Limit("map value offsets exceed carrier"))?;
            let offsets_len = entries
                .len()
                .checked_add(1)
                .ok_or(ConstantError::Limit("map offset count overflow"))?;
            let count = u64::try_from(entries.len())
                .map_err(|_| ConstantError::Limit("map source length overflow"))?;
            let backing = checked_add(
                8,
                checked_add(
                    checked_mul(checked_add(count, 1)?, 8)?,
                    checked_add(key_bytes as u64, value_bytes as u64)?,
                )?,
            )?;
            preflight(
                &field,
                &ty,
                CollectionRecipe {
                    nodes: 4,
                    depth: 3,
                    elements: checked_add(1, checked_mul(count, 3)?)?,
                    buffers: 5,
                    backing,
                },
                policy,
                &mut work,
            )?;
            let mut keys = reserve(key_bytes, &mut work)?;
            let mut values = reserve(value_bytes, &mut work)?;
            let mut key_offsets = reserve(offsets_len, &mut work)?;
            let mut value_offsets = reserve(offsets_len, &mut work)?;
            key_offsets.push(0_i32);
            value_offsets.push(0_i32);
            work.step()?;
            for (key, value) in entries {
                copied_bytes(&mut keys, key.as_bytes(), &mut work)?;
                copied_bytes(&mut values, value.as_bytes(), &mut work)?;
                key_offsets.push(
                    i32::try_from(keys.len())
                        .map_err(|_| ConstantError::Limit("map key offset overflow"))?,
                );
                value_offsets.push(
                    i32::try_from(values.len())
                        .map_err(|_| ConstantError::Limit("map value offset overflow"))?,
                );
                work.step()?;
            }
            let key_offsets = buffer(key_offsets, &mut work)?;
            let keys = buffer(keys, &mut work)?;
            let value_offsets = buffer(value_offsets, &mut work)?;
            let values = buffer(values, &mut work)?;
            let keys = array(
                ArrayData::builder(DataType::Utf8)
                    .len(entries.len())
                    .add_buffer(key_offsets)
                    .add_buffer(keys),
                &mut work,
            )?;
            let values = array(
                ArrayData::builder(DataType::Utf8)
                    .len(entries.len())
                    .add_buffer(value_offsets)
                    .add_buffer(values),
                &mut work,
            )?;
            let data = array(
                ArrayData::builder(entries_type.clone())
                    .len(entries.len())
                    .add_child_data(keys)
                    .add_child_data(values),
                &mut work,
            )?;
            let mut offsets = reserve(2, &mut work)?;
            offsets.push(0_i32);
            offsets.push(end);
            work.step()?;
            let offsets = buffer(offsets, &mut work)?;
            let data = array(
                ArrayData::builder(field.data_type().clone())
                    .len(1)
                    .add_buffer(offsets)
                    .add_child_data(data),
                &mut work,
            )?;
            work.flush()?;
            let pool = ConstantPool::try_new(field, ty, data, policy, phase, control)?;
            work.flush()?;
            pool.value(0)
        })();
        finish(work, result)
    }
}

#[cfg(test)]
#[path = "collection_factory_tests.rs"]
mod tests;

/// The original finite pre-array recipe, including raw Map entries nodes.
/// No values are manufactured below the one real empty root.
fn empty_recipe<E: super::PoolFailure>(
    ty: &DataType,
    root: bool,
    depth: u32,
    policy: ConstantPolicy,
    recipe: &mut CollectionRecipe,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    observer.step(work)?;
    limit(
        u64::from(depth),
        u64::from(policy.max_type_depth),
        "empty collection depth limit exceeded",
    )?;
    limit(
        u64::from(depth),
        novarocks_type_contract::MAX_VALUE_TYPE_DEPTH as u64,
        "empty collection intrinsic depth exceeded",
    )?;
    recipe.nodes = checked_add(recipe.nodes, 1)?;
    limit(
        recipe.nodes,
        policy.max_array_nodes,
        "empty collection array node limit exceeded",
    )?;
    recipe.depth = recipe.depth.max(depth);
    let (buffers, inspected) = empty_buffer_shape(ty, root);
    recipe.buffers = checked_add(recipe.buffers, buffers as u64)?;
    recipe.backing = checked_add(recipe.backing, inspected as u64)?;
    for index in 0..empty_child_count(ty) {
        empty_recipe(
            empty_child_type(ty, index),
            false,
            depth + 1,
            policy,
            recipe,
            observer,
            work,
        )?;
    }
    Ok(())
}
fn empty_child_count(ty: &DataType) -> usize {
    match ty {
        DataType::Struct(fields) => fields.len(),
        DataType::Union(fields, _) => fields.len(),
        DataType::RunEndEncoded(_, _) => 2,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Dictionary(_, _) => 1,
        _ => 0,
    }
}
fn empty_child_type(ty: &DataType, index: usize) -> &DataType {
    match ty {
        DataType::Struct(fields) => fields[index].data_type(),
        DataType::Union(fields, _) => fields
            .iter()
            .nth(index)
            .expect("counted union child")
            .1
            .data_type(),
        DataType::RunEndEncoded(run, value) => {
            if index == 0 {
                run.data_type()
            } else {
                value.data_type()
            }
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => field.data_type(),
        DataType::Dictionary(_, value) => value,
        _ => unreachable!("only a counted physical child is borrowed"),
    }
}
// Number of actual Buffer handles and bytes a safe Arrow builder can inspect.
// All handles borrow the same aligned zero backing; no payload is read or cast.
fn empty_buffer_shape(ty: &DataType, root: bool) -> (usize, usize) {
    match ty {
        DataType::Null
        | DataType::Struct(_)
        | DataType::FixedSizeList(_, _)
        | DataType::RunEndEncoded(_, _) => (0, 0),
        DataType::Utf8 | DataType::Binary => (2, 4),
        DataType::LargeUtf8 | DataType::LargeBinary => (2, 8),
        DataType::List(_) | DataType::Map(_, _) => (1, if root { 8 } else { 4 }),
        DataType::LargeList(_) => (1, if root { 16 } else { 8 }),
        DataType::ListView(_) | DataType::LargeListView(_) => (2, 0),
        DataType::Union(_, arrow_schema::UnionMode::Dense) => (2, 0),
        _ => (1, 0),
    }
}
fn empty_reserve<T, E: super::PoolFailure>(
    count: usize,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, E> {
    let layout = Layout::array::<T>(count)
        .map_err(|cause| ConstantError::array_backing::<T>(count, cause))?;
    observer.request(layout, work)?;
    reserve(count, work).map_err(Into::into)
}
fn empty_type_clone<E: super::PoolFailure>(
    ty: &DataType,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DataType, E> {
    use novarocks_type_contract::{ValueConstructionFailure as F, ValueConstructionRequest as R};
    let mut visit = |request| match request {
        R::Exact(layout) => observer.request(layout, work),
        R::Work(units) => {
            for _ in 0..units {
                observer.step(work)?;
            }
            Ok(())
        }
        R::OpaqueUpper(bytes) => {
            observer.work(bytes)?;
            if let Some(creator) = &mut observer.creator {
                work.flush()?;
                creator.opaque(bytes, super::PoolStructureClass::Temporary)?;
            }
            Ok(())
        }
    };
    novarocks_type_contract::clone_data_type_with_construction(ty, Some(&mut visit)).map_err(
        |cause| match cause {
            F::Observer(cause) => cause,
            F::ValueType(cause) => ConstantError::Type(cause.into()).into(),
            F::Layout(cause) => ConstantError::from_domain(cause).into(),
            F::Vec(cause) => ConstantError::from_domain(cause).into(),
            F::HashMap(cause) => ConstantError::from_domain(cause).into(),
            F::BTree(cause) => ConstantError::from_domain(cause).into(),
            F::Rule(_) => {
                ConstantError::Invalid("validated empty collection type clone rejected a rule")
                    .into()
            }
        },
    )
}
fn empty_array<E: super::PoolFailure>(
    ty: &DataType,
    root: bool,
    zeros: &Buffer,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ArrayData, E> {
    observer.step(work)?;
    let (count, bytes) = empty_buffer_shape(ty, root);
    let mut buffers = empty_reserve(count, observer, work)?;
    for index in 0..count {
        observer.step(work)?;
        let len = if index == 0 { bytes } else { 0 };
        buffers.push(zeros.slice_with_length(0, len));
    }
    let mut children = empty_reserve(empty_child_count(ty), observer, work)?;
    for index in 0..empty_child_count(ty) {
        children.push(empty_array(
            empty_child_type(ty, index),
            false,
            zeros,
            observer,
            work,
        )?);
    }
    let data_type = empty_type_clone(ty, observer, work)?;
    let builder = ArrayData::builder(data_type)
        .len(usize::from(root))
        .buffers(buffers)
        .child_data(children);
    let data = array(builder, work)?;
    observer.step(work)?;
    Ok(data)
}
fn empty_collection_data<E: super::PoolFailure>(
    field: &Field,
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    observer: &mut super::PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ArrayData, E> {
    // These pre-array refusals precede even creator request observation.
    limit(1, policy.max_rows, "empty collection row limit exceeded")?;
    limit(
        1,
        policy.max_array_nodes,
        "empty collection array node limit exceeded",
    )?;
    observer.step(work)?;
    if !matches!(
        field.data_type(),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    ) {
        return Err(ConstantError::Invalid(
            "empty collection factory requires List, LargeList or Map root",
        )
        .into());
    }
    let mut recipe = CollectionRecipe {
        nodes: 0,
        depth: 0,
        elements: 1,
        buffers: 0,
        backing: 16,
    };
    empty_recipe(
        field.data_type(),
        true,
        1,
        policy,
        &mut recipe,
        observer,
        work,
    )?;
    // Reuse the existing construction/validation numerical owner. Its finite
    // header/backing envelope also covers the one shared Arrow Buffer handle.
    let opaque = preflight_observed(field, ty, recipe, policy, observer, work)?;
    observer.work(usize::try_from(opaque).map_err(|_| CompileControlError::ResourceExhausted)?)?;
    if let Some(creator) = &mut observer.creator {
        work.flush()?;
        creator.opaque(
            usize::try_from(opaque).map_err(|_| CompileControlError::ResourceExhausted)?,
            super::PoolStructureClass::Temporary,
        )?;
    }
    // u128 alignment suffices for every accepted zero-length primitive/view
    // child, including Decimal256. All offsets are literal zero bytes. Reuse
    // this same backing even for empty buffers, rather than allocating one
    // private Arrow Bytes owner for every empty child.
    let mut zero = empty_reserve::<u128, E>(1, observer, work)?;
    zero.push(0);
    let zeros = buffer(zero, work)?;
    empty_array(field.data_type(), true, &zeros, observer, work)
}

impl ConstantValue {
    /// Construct exactly one actual non-NULL empty List, LargeList or Map.
    /// The original Field/FVT and all nested nominal facts remain authored by
    /// the caller. There are no child values, casts or inferred annotations.
    pub fn from_empty_collection(
        field: Arc<Field>,
        ty: FunctionValueType,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let mut observer: super::PoolObserver<'_, ConstantError> = super::PoolObserver::plain();
        let result = (|| {
            let data = empty_collection_data(&field, &ty, policy, &mut observer, &mut work)?;
            let pool = ConstantPool::try_new_body(
                field,
                ty,
                data,
                policy,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                None,
                &mut observer,
                &mut work,
            )?;
            pool.value(0)
        })();
        finish(work, result)
    }
    /// The same finite builder under the original creator and cumulative
    /// constant-owner observation. The caller owns entry and finish callbacks.
    pub fn from_empty_collection_with_creator_in<H>(
        field: Arc<Field>,
        ty: FunctionValueType,
        policy: ConstantPolicy,
        source: super::ConstantSourceOwner,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut impl FnMut(
            &super::ConstantOwnerResourceFacts,
        ) -> Result<(), super::ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, super::ConstantCreationFailure<H>> {
        use super::ConstantCreationFailure as Failure;
        if !Arc::ptr_eq(&field, source.field_ref()) {
            return Err(ConstantError::Invalid(
                "empty collection creator requires its original source Field",
            )
            .into());
        }
        limit(1, policy.max_rows, "empty collection row limit exceeded")?;
        let storage = creation
            .storage_lifetime()
            .retaining_observed(&source.storage_lifetime(), |layout| {
                creation.request_layout(layout).map_err(Failure::Creator)
            })?;
        let mut creator = super::PoolCreator(creation);
        let mut observer = super::PoolObserver::parent(admit);
        observer.creator = Some(&mut creator);
        let data = empty_collection_data(&field, &ty, policy, &mut observer, work)?;
        let pool = ConstantPool::try_new_body(
            field,
            ty,
            data,
            policy,
            storage,
            Some(source),
            &mut observer,
            work,
        )?;
        pool.value(0).map_err(Into::into)
    }
}
