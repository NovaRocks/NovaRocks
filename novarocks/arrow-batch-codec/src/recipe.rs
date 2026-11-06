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

// Design: ADR-0214 (docs/adr/ADR-0214-schema-free-online-units-and-prepared-layout-recipes.md)
use std::{alloc::Layout, sync::Arc};

use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, UnionFields};
use novarocks_type_contract::{
    CarrierNullabilityPolicy, FunctionValueType, ValueTypeVisit,
    owned_resources::layout::arc_layout,
};
use sha2::{Digest, Sha256};

use crate::{CodecError, UnitObserver};

/// Exact frozen semantic declaration paired with its admitted wire carrier.
/// The caller establishes this pairing; this layer never infers a logical type
/// from display names, runtime values or Arrow representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepresentationPolicy {
    Exact,
    /// Existing exchange carrier support, top-level only. Nested dictionaries
    /// require an exact frozen carrier declaration and never use this policy.
    CanonicalOrInt32Utf8Dictionary,
}

pub struct RecipeField<'a> {
    pub field: &'a FieldRef,
    pub value_type: &'a FunctionValueType,
    pub representation: RepresentationPolicy,
    pub nullability: CarrierNullabilityPolicy,
}

#[derive(Clone, Debug)]
pub struct DictionarySlot {
    pub id: i64,
    /// Values-only field used by the shared batch geometry author. The original
    /// dictionary's logical declaration remains in the recipe identity.
    pub values: FieldRef,
    pub schema: SchemaRef,
}

#[derive(Debug)]
pub struct BatchRecipe {
    schema: SchemaRef,
    dictionaries: Vec<DictionarySlot>,
    digest: [u8; 32],
    policies: Vec<RepresentationPolicy>,
    nullability: Vec<CarrierNullabilityPolicy>,
    declarations: Vec<FunctionValueType>,
}

impl BatchRecipe {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    pub fn declarations(&self) -> &[FunctionValueType] {
        &self.declarations
    }
    pub fn dictionaries(&self) -> &[DictionarySlot] {
        &self.dictionaries
    }
    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Root identity is canonical ordinal; local slot IDs and root display names
    /// belong to each role. Nested field identities and semantic parameters are
    /// preserved by the exact value and carrier streams.
    pub fn try_new<O: UnitObserver>(
        fields: &[RecipeField<'_>],
        observer: &mut O,
    ) -> Result<Self, CodecError<O::Error>> {
        observer
            .before_recipe(fields)
            .map_err(CodecError::Observer)?;
        observer.checkpoint().map_err(CodecError::Observer)?;
        let mut hash = Sha256::new();
        hash.update(b"novarocks-arrow-unit-recipe-v1");
        hash.update((fields.len() as u64).to_le_bytes());
        let mut projected = Vec::new();
        reserve(&mut projected, fields.len(), observer)?;
        let mut dictionaries = Vec::new();
        let mut policies = Vec::new();
        reserve(&mut policies, fields.len(), observer)?;
        let mut nullability = Vec::new();
        reserve(&mut nullability, fields.len(), observer)?;
        let mut declarations = Vec::new();
        reserve(&mut declarations, fields.len(), observer)?;
        let mut count = 0i64;
        observer
            .before_type_scratch(
                novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound(
                ),
            )
            .map_err(CodecError::Observer)?;
        let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
        observer.checkpoint().map_err(CodecError::Observer)?;
        for input in fields {
            observer.checkpoint().map_err(CodecError::Observer)?;
            if !input.field.metadata().is_empty() {
                return Err(CodecError::Invalid(
                    "runtime recipe root must be metadata-free; exact semantic identity is supplied separately",
                ));
            }
            observer.checkpoint().map_err(CodecError::Observer)?;
            // Shared traversal enforces current depth/node/field grammar before
            // recursive Arrow constructors can run. It allocates no shadow tree.
            novarocks_type_contract::validate_arrow_layout_structure_with_profile_and_scratch_observed(
                input.field.data_type(),
                novarocks_type_contract::ArrowTypeProfile::Value,
                &mut scratch,
                |visit| {
                    observer.checkpoint().map_err(CodecError::Observer)?;
                    if let ValueTypeVisit::Field(field) = visit
                        && !field.metadata().is_empty()
                    {
                        return Err(CodecError::Invalid(
                            "runtime recipe fields must be metadata-free; exact semantic identity is supplied separately",
                        ));
                    }
                    if let ValueTypeVisit::TypeNode(ty) = visit {
                        novarocks_type_contract::validate_arrow_carrier_parameters_observed(
                            ty,
                            || observer.checkpoint().map_err(CodecError::Observer),
                        )?;
                    }
                    Ok::<_, CodecError<O::Error>>(())
                },
            )?;
            let root_nullability_matches = input.field.is_nullable() == input.value_type.nullable()
                || (input.nullability == CarrierNullabilityPolicy::Permissive
                    && input.field.is_nullable());
            if !root_nullability_matches
                || !novarocks_type_contract::value_layout_matches_carrier_with_nullability_observed(
                    input.value_type.typed_binding().layout(),
                    input.field.data_type(),
                    input.nullability,
                    |_| observer.checkpoint().map_err(CodecError::Observer),
                )?
            {
                return Err(CodecError::Invalid(
                    "prepared carrier differs from its complete value declaration",
                ));
            }
            hash.update([match input.nullability {
                CarrierNullabilityPolicy::Exact => 0,
                CarrierNullabilityPolicy::Permissive => 1,
            }]);
            novarocks_type_contract::write_value_type_identity_observed(
                input.value_type.value_type(),
                |bytes| {
                    observer.checkpoint().map_err(CodecError::Observer)?;
                    hash.update(bytes);
                    Ok::<_, CodecError<O::Error>>(())
                },
            )?;
            novarocks_type_contract::write_value_layout_identity_observed(
                input.value_type.typed_binding().layout(),
                |bytes| {
                    observer.checkpoint().map_err(CodecError::Observer)?;
                    hash.update(bytes);
                    Ok::<_, CodecError<O::Error>>(())
                },
            )?;
            if input.representation == RepresentationPolicy::CanonicalOrInt32Utf8Dictionary
                && !matches!(
                    input.field.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                )
            {
                return Err(CodecError::Invalid(
                    "dictionary representation policy requires a canonical UTF8 root",
                ));
            }
            hash.update([input.representation as u8]);
            policies.push(input.representation);
            nullability.push(input.nullability);
            declarations.push(input.value_type.clone());
            let permissive = input.nullability == CarrierNullabilityPolicy::Permissive;
            let field = if permissive || contains_dictionary(input.field.data_type(), observer)? {
                rebuild_field(
                    input.field,
                    None,
                    permissive,
                    false,
                    &mut count,
                    &mut dictionaries,
                    observer,
                )?
            } else {
                Arc::clone(input.field)
            };
            hash_field(&field, false, &mut hash, observer)?;
            projected.push(field);
        }
        let schema = make_schema(projected, observer)?;
        Ok(Self {
            schema,
            dictionaries,
            digest: hash.finalize().into(),
            policies,
            nullability,
            declarations,
        })
    }
}

pub(crate) fn reserve<T, O: UnitObserver>(
    value: &mut Vec<T>,
    additional: usize,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    let new = value
        .len()
        .checked_add(additional)
        .ok_or(CodecError::Invalid("unit capacity overflow"))?;
    if new > value.capacity() {
        let layout = Layout::array::<T>(new)
            .map_err(|_| CodecError::Invalid("unit allocation layout overflow"))?;
        observer
            .before_allocation(layout)
            .map_err(CodecError::Observer)?;
        value
            .try_reserve_exact(additional)
            .map_err(CodecError::Allocation)?;
    }
    Ok(())
}

fn rebuild_field<O: UnitObserver>(
    source: &Field,
    name: Option<&str>,
    permissive: bool,
    required: bool,
    count: &mut i64,
    dictionaries: &mut Vec<DictionarySlot>,
    observer: &mut O,
) -> Result<FieldRef, CodecError<O::Error>> {
    let ty = rebuild_type(
        source.data_type(),
        permissive,
        count,
        dictionaries,
        observer,
    )?;
    let nullable = !required && (permissive || source.is_nullable());
    finish_field(source, name, ty, nullable, count, dictionaries, observer)
}
fn finish_field<O: UnitObserver>(
    source: &Field,
    name: Option<&str>,
    ty: DataType,
    nullable: bool,
    count: &mut i64,
    dictionaries: &mut Vec<DictionarySlot>,
    observer: &mut O,
) -> Result<FieldRef, CodecError<O::Error>> {
    let field_name = copy_name(name.unwrap_or(source.name()), observer)?;
    observe_arc::<Field, O>(observer)?;
    let field = if let DataType::Dictionary(_, values) = &ty {
        let id = *count;
        *count = count
            .checked_add(1)
            .ok_or(CodecError::Invalid("dictionary ID overflow"))?;
        reserve(dictionaries, 1, observer)?;
        observe_arc::<Field, O>(observer)?;
        let values = Arc::new(Field::new("", values.as_ref().clone(), true));
        let mut fields = Vec::new();
        reserve(&mut fields, 1, observer)?;
        fields.push(Arc::clone(&values));
        let schema = make_schema(fields, observer)?;
        dictionaries.push(DictionarySlot { id, values, schema });
        dictionary_field(source, field_name, ty, nullable, id)
    } else {
        Field::new(field_name, ty, nullable)
    };
    Ok(Arc::new(field))
}

#[expect(
    deprecated,
    reason = "Arrow 60.1 readers still identify dictionaries by Field::dict_id; recipe reconstructs deterministic IDs without a Schema message"
)]
fn dictionary_field(source: &Field, name: String, ty: DataType, nullable: bool, id: i64) -> Field {
    Field::new_dict(
        name,
        ty,
        nullable,
        id,
        source.dict_is_ordered().unwrap_or(false),
    )
}

fn rebuild_type<O: UnitObserver>(
    source: &DataType,
    permissive: bool,
    count: &mut i64,
    dictionaries: &mut Vec<DictionarySlot>,
    observer: &mut O,
) -> Result<DataType, CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    let mut child = |f: &FieldRef| {
        rebuild_field(
            f,
            permissive.then_some("item"),
            permissive,
            false,
            count,
            dictionaries,
            observer,
        )
    };
    Ok(match source {
        DataType::List(f) => DataType::List(child(f)?),
        DataType::LargeList(f) => DataType::LargeList(child(f)?),
        DataType::ListView(f) => DataType::ListView(child(f)?),
        DataType::LargeListView(f) => DataType::LargeListView(child(f)?),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(child(f)?, *n),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CodecError::Invalid("recipe Map entries are not Struct"));
            };
            if fields.len() != 2 {
                return Err(CodecError::Invalid("recipe Map entries arity differs"));
            }
            let key = rebuild_field(
                &fields[0],
                permissive.then_some("key"),
                permissive,
                true,
                count,
                dictionaries,
                observer,
            )?;
            let value = rebuild_field(
                &fields[1],
                permissive.then_some("value"),
                permissive,
                false,
                count,
                dictionaries,
                observer,
            )?;
            let mut output = Vec::new();
            reserve(&mut output, 2, observer)?;
            output.push(key);
            output.push(value);
            observe_arc_slice::<FieldRef, O>(2, observer)?;
            let ty = DataType::Struct(output.into());
            DataType::Map(
                finish_field(
                    entries,
                    permissive.then_some("entries"),
                    ty,
                    false,
                    count,
                    dictionaries,
                    observer,
                )?,
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let mut output = Vec::new();
            reserve(&mut output, fields.len(), observer)?;
            for field in fields {
                output.push(rebuild_field(
                    field,
                    None,
                    permissive,
                    false,
                    count,
                    dictionaries,
                    observer,
                )?);
            }
            observe_arc_slice::<FieldRef, O>(output.len(), observer)?;
            DataType::Struct(output.into())
        }
        DataType::Union(fields, mode) => {
            let mut ids = Vec::new();
            let mut output = Vec::new();
            reserve(&mut ids, fields.len(), observer)?;
            reserve(&mut output, fields.len(), observer)?;
            for (id, field) in fields.iter() {
                ids.push(id);
                output.push(rebuild_field(
                    field,
                    None,
                    permissive,
                    false,
                    count,
                    dictionaries,
                    observer,
                )?);
            }
            {
                let shared = std::cell::RefCell::new(&mut *observer);
                let mut allocation = |layout, _| {
                    shared
                        .borrow_mut()
                        .before_allocation(layout)
                        .map_err(CodecError::Observer)
                };
                let mut loan:Option<novarocks_type_contract::owned_resources::metadata_materialization::MetadataAllocationLoan<'_,CodecError<O::Error>>>=Some(&mut allocation);
                novarocks_type_contract::owned_resources::vec::original_fresh_push_allocation_requests_observed::<(i8,FieldRef),CodecError<O::Error>>(fields.len(), &mut || shared.borrow_mut().checkpoint().map_err(CodecError::Observer), &mut loan)?;
            }
            observe_arc_slice::<(i8, FieldRef), O>(fields.len(), observer)?;
            DataType::Union(
                UnionFields::try_new(ids, output).map_err(CodecError::Arrow)?,
                *mode,
            )
        }
        DataType::RunEndEncoded(ends, values) => {
            let ends = rebuild_field(
                ends,
                permissive.then_some("run_ends"),
                permissive,
                true,
                count,
                dictionaries,
                observer,
            )?;
            let values = rebuild_field(
                values,
                permissive.then_some("values"),
                permissive,
                false,
                count,
                dictionaries,
                observer,
            )?;
            DataType::RunEndEncoded(ends, values)
        }
        DataType::Dictionary(key, values) => {
            if matches!(values.as_ref(), DataType::Dictionary(..)) {
                return Err(CodecError::Invalid(
                    "Arrow IPC cannot represent direct dictionary-of-dictionary",
                ));
            }
            observe_box::<DataType, O>(observer)?;
            observe_box::<DataType, O>(observer)?;
            DataType::Dictionary(
                key.clone(),
                Box::new(rebuild_type(
                    values,
                    permissive,
                    count,
                    dictionaries,
                    observer,
                )?),
            )
        }
        leaf => leaf.clone(),
    })
}

fn hash_field<O: UnitObserver>(
    field: &Field,
    include_name: bool,
    hash: &mut Sha256,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    if include_name {
        bytes(hash, field.name().as_bytes());
    }
    hash.update([u8::from(field.is_nullable())]);
    if !field.metadata().is_empty() {
        return Err(CodecError::Invalid(
            "runtime recipe field contains unexpected source metadata",
        ));
    }
    hash_type(field.data_type(), hash, observer)
}
fn bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_le_bytes());
    hash.update(value);
}
fn hash_type<O: UnitObserver>(
    ty: &DataType,
    hash: &mut Sha256,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    use DataType::*;
    match ty {
        List(f)
        | LargeList(f)
        | ListView(f)
        | LargeListView(f)
        | FixedSizeList(f, _)
        | Map(f, _) => {
            hash.update([match ty {
                List(_) => 1,
                LargeList(_) => 2,
                ListView(_) => 3,
                LargeListView(_) => 4,
                FixedSizeList(..) => 5,
                _ => 6,
            }]);
            if let FixedSizeList(_, n) = ty {
                hash.update(n.to_le_bytes());
            }
            if let Map(_, sorted) = ty {
                hash.update([u8::from(*sorted)]);
            }
            hash_field(f, true, hash, observer)?;
        }
        Struct(fields) => {
            hash.update([7]);
            hash.update((fields.len() as u64).to_le_bytes());
            for f in fields {
                hash_field(f, true, hash, observer)?;
            }
        }
        Dictionary(key, value) => {
            hash.update([8]);
            hash_type(key, hash, observer)?;
            hash_type(value, hash, observer)?;
        }
        Union(fields, mode) => {
            hash.update([
                9,
                match mode {
                    arrow::datatypes::UnionMode::Sparse => 0,
                    _ => 1,
                },
            ]);
            hash.update((fields.len() as u64).to_le_bytes());
            for (id, f) in fields.iter() {
                hash.update([id as u8]);
                hash_field(f, true, hash, observer)?;
            }
        }
        RunEndEncoded(ends, values) => {
            hash.update([10]);
            hash_field(ends, true, hash, observer)?;
            hash_field(values, true, hash, observer)?;
        }
        leaf => hash_scalar(leaf, hash)?,
    }
    Ok(())
}

/// One unit's finite representation projection. This holder must remain alive
/// through opaque decode; dictionary Arrays retain their actual Buffer owners.
pub(crate) struct UnitRecipe {
    pub schema: SchemaRef,
    pub dictionaries: Vec<DictionarySlot>,
}
impl BatchRecipe {
    pub fn representation_policy(&self, column: usize) -> Option<RepresentationPolicy> {
        self.policies.get(column).copied()
    }
    pub(crate) fn representation_tags<O: UnitObserver>(
        &self,
        batch: &arrow::array::RecordBatch,
        observer: &mut O,
    ) -> Result<Vec<u8>, CodecError<O::Error>> {
        if batch.num_columns() != self.policies.len() {
            return Err(CodecError::Invalid("unit columns differ from recipe"));
        }
        let mut tags = Vec::new();
        reserve(&mut tags, self.policies.len(), observer)?;
        observer
            .before_type_scratch(
                novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound(
                ),
            )
            .map_err(CodecError::Observer)?;
        let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
        observer.checkpoint().map_err(CodecError::Observer)?;
        for (ordinal, ((array, field), policy)) in batch
            .columns()
            .iter()
            .zip(self.schema.fields())
            .zip(&self.policies)
            .enumerate()
        {
            observer.checkpoint().map_err(CodecError::Observer)?;
            let normalized;
            let actual = if self.nullability[ordinal] == CarrierNullabilityPolicy::Permissive {
                novarocks_type_contract::validate_arrow_layout_structure_with_profile_and_scratch_observed(array.data_type(), novarocks_type_contract::ArrowTypeProfile::Value, &mut scratch, |_| observer.checkpoint().map_err(CodecError::Observer))?;
                let mut count = 0;
                let mut dictionaries = Vec::new();
                normalized = rebuild_type(
                    array.data_type(),
                    true,
                    &mut count,
                    &mut dictionaries,
                    observer,
                )?;
                &normalized
            } else {
                array.data_type()
            };
            let tag = if actual == field.data_type() {
                0
            } else if *policy == RepresentationPolicy::CanonicalOrInt32Utf8Dictionary
                && matches!(actual,DataType::Dictionary(key,value) if key.as_ref()==&DataType::Int32 && value.as_ref()==field.data_type())
            {
                1
            } else {
                return Err(CodecError::Invalid(
                    "unit carrier is not admitted by its frozen representation policy",
                ));
            };
            tags.push(tag);
        }
        Ok(tags)
    }
    pub(crate) fn unit_recipe<O: UnitObserver>(
        &self,
        tags: &[u8],
        observer: &mut O,
    ) -> Result<UnitRecipe, CodecError<O::Error>> {
        if tags.len() != self.policies.len() {
            return Err(CodecError::Invalid(
                "unit representation count differs from recipe",
            ));
        }
        for (tag, policy) in tags.iter().zip(&self.policies) {
            if *tag > 1
                || (*tag == 1 && *policy != RepresentationPolicy::CanonicalOrInt32Utf8Dictionary)
            {
                return Err(CodecError::Invalid(
                    "unit representation tag is not admitted by recipe",
                ));
            }
        }
        observer
            .before_unit_recipe(self, tags)
            .map_err(CodecError::Observer)?;
        let mut fields = Vec::new();
        reserve(&mut fields, tags.len(), observer)?;
        let mut dictionaries = Vec::new();
        let mut count = 0;
        for (tag, field) in tags.iter().zip(self.schema.fields()) {
            observer.checkpoint().map_err(CodecError::Observer)?;
            if *tag == 1 {
                observe_box::<DataType, O>(observer)?;
                observe_box::<DataType, O>(observer)?;
                let physical = DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(field.data_type().clone()),
                );
                fields.push(finish_field(
                    field,
                    None,
                    physical,
                    field.is_nullable(),
                    &mut count,
                    &mut dictionaries,
                    observer,
                )?);
            } else if contains_dictionary(field.data_type(), observer)? {
                fields.push(rebuild_field(
                    field,
                    None,
                    false,
                    false,
                    &mut count,
                    &mut dictionaries,
                    observer,
                )?);
            } else {
                fields.push(Arc::clone(field));
            }
        }
        Ok(UnitRecipe {
            schema: make_schema(fields, observer)?,
            dictionaries,
        })
    }
}
fn contains_dictionary<O: UnitObserver>(
    ty: &DataType,
    observer: &mut O,
) -> Result<bool, CodecError<O::Error>> {
    observer.checkpoint().map_err(CodecError::Observer)?;
    Ok(match ty {
        DataType::Dictionary(..) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => contains_dictionary(f.data_type(), observer)?,
        DataType::Struct(fields) => {
            let mut found = false;
            for f in fields {
                found |= contains_dictionary(f.data_type(), observer)?;
            }
            found
        }
        DataType::Union(fields, _) => {
            let mut found = false;
            for (_, f) in fields.iter() {
                found |= contains_dictionary(f.data_type(), observer)?;
            }
            found
        }
        DataType::RunEndEncoded(a, b) => {
            let left = contains_dictionary(a.data_type(), observer)?;
            let right = contains_dictionary(b.data_type(), observer)?;
            left || right
        }
        _ => false,
    })
}
fn observe_arc<T, O: UnitObserver>(observer: &mut O) -> Result<(), CodecError<O::Error>> {
    observer
        .before_allocation(arc_layout(Layout::new::<T>()).map_err(CodecError::Layout)?)
        .map_err(CodecError::Observer)
}
fn observe_arc_slice<T, O: UnitObserver>(
    len: usize,
    observer: &mut O,
) -> Result<(), CodecError<O::Error>> {
    let payload = Layout::array::<T>(len)
        .map_err(|_| CodecError::Invalid("unit Arc slice extent overflow"))?;
    observer
        .before_allocation(arc_layout(payload).map_err(CodecError::Layout)?)
        .map_err(CodecError::Observer)
}
fn observe_box<T, O: UnitObserver>(observer: &mut O) -> Result<(), CodecError<O::Error>> {
    observer
        .before_allocation(Layout::new::<T>())
        .map_err(CodecError::Observer)
}
fn make_schema<O: UnitObserver>(
    fields: Vec<FieldRef>,
    observer: &mut O,
) -> Result<SchemaRef, CodecError<O::Error>> {
    observe_arc_slice::<FieldRef, O>(fields.len(), observer)?;
    observe_arc::<Schema, O>(observer)?;
    Ok(Arc::new(Schema::new(fields)))
}
fn copy_name<O: UnitObserver>(
    name: &str,
    observer: &mut O,
) -> Result<String, CodecError<O::Error>> {
    if !name.is_empty() {
        observer
            .before_allocation(
                Layout::array::<u8>(name.len())
                    .map_err(|_| CodecError::Invalid("unit field name extent overflow"))?,
            )
            .map_err(CodecError::Observer)?;
    }
    Ok(name.to_owned())
}
fn hash_scalar<E>(ty: &DataType, hash: &mut Sha256) -> Result<(), CodecError<E>> {
    use DataType::*;
    let tag = match ty {
        Null => 0,
        Boolean => 1,
        Int8 => 2,
        Int16 => 3,
        Int32 => 4,
        Int64 => 5,
        UInt8 => 6,
        UInt16 => 7,
        UInt32 => 8,
        UInt64 => 9,
        Float16 => 10,
        Float32 => 11,
        Float64 => 12,
        Timestamp(..) => 13,
        Date32 => 14,
        Date64 => 15,
        Time32(_) => 16,
        Time64(_) => 17,
        Duration(_) => 18,
        Interval(_) => 19,
        Binary => 20,
        FixedSizeBinary(_) => 21,
        LargeBinary => 22,
        BinaryView => 23,
        Utf8 => 24,
        LargeUtf8 => 25,
        Utf8View => 26,
        Decimal32(..) => 27,
        Decimal64(..) => 28,
        Decimal128(..) => 29,
        Decimal256(..) => 30,
        _ => {
            return Err(CodecError::Invalid(
                "unexpected non-scalar unit representation",
            ));
        }
    };
    hash.update([0, tag]);
    fn time(unit: &arrow::datatypes::TimeUnit) -> u8 {
        match unit {
            arrow::datatypes::TimeUnit::Second => 0,
            arrow::datatypes::TimeUnit::Millisecond => 1,
            arrow::datatypes::TimeUnit::Microsecond => 2,
            arrow::datatypes::TimeUnit::Nanosecond => 3,
        }
    }
    match ty {
        FixedSizeBinary(width) => hash.update(width.to_le_bytes()),
        Timestamp(unit, zone) => {
            hash.update([time(unit), u8::from(zone.is_some())]);
            if let Some(zone) = zone {
                bytes(hash, zone.as_bytes());
            }
        }
        Time32(unit) | Time64(unit) | Duration(unit) => hash.update([time(unit)]),
        Interval(unit) => hash.update([match unit {
            arrow::datatypes::IntervalUnit::YearMonth => 0,
            arrow::datatypes::IntervalUnit::DayTime => 1,
            arrow::datatypes::IntervalUnit::MonthDayNano => 2,
        }]),
        Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s) | Decimal256(p, s) => {
            hash.update([*p, *s as u8])
        }
        _ => {}
    }
    Ok(())
}
