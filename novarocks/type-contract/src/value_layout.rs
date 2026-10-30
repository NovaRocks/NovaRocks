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

//! Exact value layout after removing format annotations and runtime encodings.

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Fields, UnionFields};

use crate::{
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, MAX_VALUE_TYPE_DEPTH,
    MAX_VALUE_TYPE_NODES,
    value_type::{ExecutionValueTypeError, ValueTypeStorageLifetime},
};

#[derive(Debug)]
struct LayoutStorage {
    data_type: DataType,
    comparison_work: usize,
    children: Option<Box<[ValueLayout]>>,
    arrow_backing: ValueTypeStorageLifetime,
    // Drop owned Arrow structure before its construction responsibility.
    _storage: ValueTypeStorageLifetime,
}

/// Immutable physical value layout, independent of per-batch encoding.
///
/// Anonymous List/Map roles have canonical names. Named Struct/Union fields,
/// nullability, offset widths and scalar parameters remain exact. Constructing
/// this contract does not authorize any execution carrier or encoding support.
#[derive(Clone, Debug)]
pub struct ValueLayout(Arc<LayoutStorage>);

impl ValueLayout {
    /// Reuse only a previously validated immutable layout owner. Identity
    /// alone never establishes semantics or authorizes a runtime encoding.
    pub fn shares_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Actual request for the private projection holder, without a mirror.
    /// Field/name/child storage is admitted separately by its author.
    pub fn storage_allocation_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<LayoutStorage>())
    }

    /// Normalize a borrowed layout under the actual owner's storage lifetime.
    ///
    /// The complete source shape is checked before allocating normalized fields.
    /// Metadata is discarded without interpreting it. Dictionary and run-end
    /// wrappers belong to the actual encoding contract and are removed here.
    pub fn try_new(
        data_type: &DataType,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_new_with_construction::<std::convert::Infallible>(data_type, storage, None)
            .map_err(crate::ValueConstructionFailure::into_pure)
    }

    /// The SAME normalizer, with an optional actual before-allocation event.
    /// A pure caller supplies None and does not open an admission scope.
    pub fn try_new_with_construction<E>(
        data_type: &DataType,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        preflight(data_type, 1, &mut 0)?;
        let mut construction = crate::value_construction::ValueConstruction::new(observer);
        let mut comparison_work = 0;
        let data_type = normalize(data_type, &mut comparison_work, &mut construction)?;
        construction.exact(Self::storage_allocation_layout)?;
        Ok(Self(Arc::new(LayoutStorage {
            data_type,
            comparison_work,
            children: None,
            arrow_backing: storage.clone(),
            _storage: storage,
        })))
    }

    /// Consume a projection already constructed by its actual owner. Borrowed
    /// validation precedes the sole holder allocation; no Field or child tree
    /// is cloned. Codec owners separately observe their actual construction.
    pub fn try_from_normalized_arrow(
        data_type: DataType,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_from_normalized_arrow_observed(data_type, storage, |_| Ok(()))
    }

    /// Validate and consume the actual projection under the original caller's
    /// work/control observer. Allocation admission belongs to that caller.
    pub fn try_from_normalized_arrow_observed<E: From<ExecutionValueTypeError>>(
        data_type: DataType,
        storage: ValueTypeStorageLifetime,
        mut observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let comparison_work = preflight_observed(&data_type, 1, &mut 0, true, &mut observe)?;
        Ok(Self(Arc::new(LayoutStorage {
            data_type,
            comparison_work,
            children: None,
            arrow_backing: storage.clone(),
            _storage: storage,
        })))
    }

    /// Consume children created independently by the same canonical layout
    /// author. Their order is the exact structural projection order (Map uses
    /// key/value, not its entries scaffolding). Allocation requests belong to
    /// the caller before Vec/Box/holder construction, never to this validator.
    pub fn try_from_normalized_arrow_with_children_observed<E: From<ExecutionValueTypeError>>(
        data_type: DataType,
        children: Box<[ValueLayout]>,
        arrow_backing: ValueTypeStorageLifetime,
        storage: ValueTypeStorageLifetime,
        mut observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let comparison_work = preflight_observed(&data_type, 1, &mut 0, true, &mut observe)?;
        for (index, child) in children.iter().enumerate() {
            observe(1)?;
            let actual = normalized_child_type_at(&data_type, index).ok_or_else(|| {
                E::from(ExecutionValueTypeError::Invalid(
                    "layout child owner count differs",
                ))
            })?;
            if !crate::schema::arrow_validated_data_types_exact_weighted_observed(
                actual,
                child.data_type(),
                |step| observe(step.work_units_upper()),
            )? {
                return Err(ExecutionValueTypeError::Invalid("layout child owner differs").into());
            }
        }
        observe(1)?;
        if normalized_child_type_at(&data_type, children.len()).is_some() {
            return Err(
                ExecutionValueTypeError::Invalid("layout child owner count differs").into(),
            );
        }
        Ok(Self(Arc::new(LayoutStorage {
            data_type,
            comparison_work,
            children: Some(children),
            arrow_backing,
            _storage: storage,
        })))
    }

    /// Borrow a child frozen by the actual materializer. No lookup constructs
    /// a projection, allocation or alternate source authority.
    pub fn materialized_child_at(&self, index: usize) -> Option<&Self> {
        self.0.children.as_ref()?.get(index)
    }

    /// Only reachable Arrow backing, independent of this holder and its child
    /// pointer array. A constructing parent borrows this actual shared loan.
    /// The marker observes responsibility; it grants no allocation capacity.
    pub fn arrow_backing_storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0.arrow_backing.clone()
    }

    /// Borrow an immediate child from this already validated normalized layout.
    /// An independent owned projection still needs its own creation lifetime.
    pub fn borrowed_child_type_at(&self, index: usize) -> Option<&DataType> {
        normalized_child_type_at(self.data_type(), index)
    }

    /// Retain only this layout's actual creation responsibility when an author
    /// reuses its Field/name/timezone backing in a new immutable projection.
    /// This marker grants no allocation and retains no source service.
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0._storage.clone()
    }

    /// Frozen work footprint from the actual canonical constructor walk.
    /// It does not include source attributes, identity or storage authority.
    pub fn comparison_work_upper_bound(&self) -> usize {
        self.0.comparison_work
    }

    /// Borrow the exact normalized Arrow projection while this holder lives.
    /// Independently retained projections must retain a `ValueLayout` clone.
    pub fn data_type(&self) -> &DataType {
        &self.0.data_type
    }

    /// Compare two validated immutable layouts under the caller's observer.
    /// Constructors own grammar validation; replay can therefore preserve the
    /// original control error without inventing another source-error domain.
    pub fn equals_observed<E>(
        &self,
        other: &Self,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        crate::schema::arrow_validated_data_types_exact_observed(
            self.data_type(),
            other.data_type(),
            observe,
        )
    }

    /// Bind a static, metadata-free projection to this validated exact layout.
    /// This is distinct from actual array encoding compatibility: a static
    /// projection admits neither Dictionary nor run-end wrappers.
    pub fn matches_projection_observed<E: From<ExecutionValueTypeError>>(
        &self,
        projection: &DataType,
        mut observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<bool, E> {
        preflight_observed(projection, 1, &mut 0, true, &mut observe)?;
        crate::schema::arrow_validated_data_types_exact_observed(
            self.data_type(),
            projection,
            || observe(1),
        )
    }

    /// Project an ordered structural child with its original creation lifetime.
    /// The new owner covers the new fixed holder; shared Field/name/timezone
    /// backing continues to hold the original responsibility independently.
    pub fn child_projection_at(
        &self,
        index: usize,
        storage: ValueTypeStorageLifetime,
    ) -> Option<Self> {
        self.child_projection_at_with_construction::<std::convert::Infallible>(index, storage, None)
            .expect("pure child projection")
    }
    pub fn child_projection_at_with_construction<E>(
        &self,
        index: usize,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Option<Self>, crate::ValueConstructionFailure<E>> {
        if let Some(children) = &self.0.children {
            return Ok(children.get(index).cloned());
        }
        let Some(data_type) = self.child_data_type(index) else {
            return Ok(None);
        };
        let Ok(comparison_work) = preflight_observed(data_type, 1, &mut 0, true, &mut |_| {
            Ok::<_, ExecutionValueTypeError>(())
        }) else {
            return Ok(None);
        };
        let mut creation = crate::value_construction::ValueConstruction::new(observer);
        let storage = storage.retaining_with_construction(&self.0._storage, creation.observer())?;
        creation.exact(Self::storage_allocation_layout)?;
        Ok(Some(Self(Arc::new(LayoutStorage {
            data_type: data_type.clone(),
            comparison_work,
            children: None,
            arrow_backing: self.0.arrow_backing.clone(),
            _storage: storage,
        }))))
    }

    /// Project a borrowed child through the creating owner's original request
    /// and control ports. Both the actual projection holder and a distinct
    /// source-lifetime holder are admitted before their respective allocation.
    pub fn child_projection_at_observed<
        E: From<crate::owned_resources::layout::LayoutResourceError>,
    >(
        &self,
        index: usize,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        work(1)?;
        if let Some(children) = &self.0.children {
            return Ok(children.get(index).cloned());
        }
        let Some(data_type) = self.child_data_type(index) else {
            return Ok(None);
        };
        // A new independent Arrow child holder has no per-child cache yet.
        // Its actual creator performs the same canonical walk once, before
        // publishing that holder; hot binding replay then reads its cache.
        let comparison_work = preflight_observed::<LayoutProfileFailure<E>>(
            data_type,
            1,
            &mut 0,
            true,
            &mut |units| work(units).map_err(LayoutProfileFailure::Caller),
        )
        .map_err(|error| match error {
            LayoutProfileFailure::Caller(error) => error,
            LayoutProfileFailure::Source => {
                E::from(crate::owned_resources::layout::LayoutResourceError::SourceModel)
            }
        })?;
        let storage = storage.retaining_observed(&self.0._storage, &mut request)?;
        request(Self::storage_allocation_layout()?)?;
        work(1)?;
        Ok(Some(Self(Arc::new(LayoutStorage {
            data_type: data_type.clone(),
            comparison_work,
            children: None,
            arrow_backing: storage.clone(),
            _storage: storage,
        }))))
    }

    fn child_data_type(&self, index: usize) -> Option<&DataType> {
        match self.data_type() {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
                if index == 0 =>
            {
                Some(item.data_type())
            }
            DataType::Struct(fields) => Some(fields.get(index)?.data_type()),
            DataType::Union(fields, _) => Some(fields.iter().nth(index)?.1.data_type()),
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return None;
                };
                Some(fields.get(index)?.data_type())
            }
            _ => None,
        }
    }
}

impl PartialEq for ValueLayout {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.data_type() == other.data_type()
    }
}
impl Eq for ValueLayout {}
impl Hash for ValueLayout {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data_type().hash(state);
    }
}

enum LayoutProfileFailure<E> {
    Caller(E),
    Source,
}
impl<E> From<ExecutionValueTypeError> for LayoutProfileFailure<E> {
    fn from(_: ExecutionValueTypeError) -> Self {
        Self::Source
    }
}

enum LayoutParameterFailure<E> {
    Caller(E),
    Source(crate::CarrierParameterError),
}
impl<E> From<crate::CarrierParameterError> for LayoutParameterFailure<E> {
    fn from(error: crate::CarrierParameterError) -> Self {
        Self::Source(error)
    }
}
fn preflight(
    data_type: &DataType,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), ExecutionValueTypeError> {
    preflight_observed(data_type, depth, nodes, false, &mut |_| Ok(())).map(|_| ())
}
fn preflight_field<E: From<ExecutionValueTypeError>>(
    field: &Field,
    role: Option<&str>,
    named: bool,
    depth: usize,
    nodes: &mut usize,
    normalized: bool,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<usize, E> {
    observe(1)?;
    if named && field.name().len() > MAX_ARROW_FIELD_NAME_BYTES {
        return Err(ExecutionValueTypeError::Invalid(
            "value layout field name exceeds the byte limit",
        )
        .into());
    }
    if normalized {
        if !field.metadata().is_empty() {
            return Err(ExecutionValueTypeError::Invalid(
                "value layout projection contains field annotations",
            )
            .into());
        }
        if let Some(role) = role {
            observe(1)?;
            if field.name().len() != role.len() {
                return Err(ExecutionValueTypeError::Invalid(
                    "value layout projection has a noncanonical role name",
                )
                .into());
            }
            observe(role.len())?;
            if field.name() != role {
                return Err(ExecutionValueTypeError::Invalid(
                    "value layout projection has a noncanonical role name",
                )
                .into());
            }
        }
    }
    let child_work = preflight_observed(field.data_type(), depth, nodes, normalized, observe)?;
    if normalized {
        Ok(crate::schema::comparison_work_add(
            crate::schema::field_comparison_work(field.name())?,
            child_work,
        )?)
    } else {
        // Anonymous source labels may be arbitrary: normalization replaces
        // them, so this source-only grammar pass must not charge/cache them.
        Ok(child_work)
    }
}
fn preflight_observed<E: From<ExecutionValueTypeError>>(
    data_type: &DataType,
    depth: usize,
    nodes: &mut usize,
    normalized: bool,
    observe: &mut impl FnMut(usize) -> Result<(), E>,
) -> Result<usize, E> {
    observe(1)?;
    if depth > MAX_VALUE_TYPE_DEPTH {
        return Err(ExecutionValueTypeError::Depth.into());
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| E::from(ExecutionValueTypeError::Nodes))?;
    if *nodes > MAX_VALUE_TYPE_NODES {
        return Err(ExecutionValueTypeError::Nodes.into());
    }
    crate::validate_arrow_carrier_parameters_observed::<LayoutParameterFailure<E>>(
        data_type,
        || observe(1).map_err(LayoutParameterFailure::Caller),
    )
    .map_err(|error| match error {
        LayoutParameterFailure::Caller(error) => error,
        LayoutParameterFailure::Source(crate::CarrierParameterError::Invalid(message)) => {
            E::from(ExecutionValueTypeError::Invalid(message))
        }
        LayoutParameterFailure::Source(crate::CarrierParameterError::Decimal(_)) => E::from(
            ExecutionValueTypeError::Invalid("invalid decimal value layout parameters"),
        ),
    })?;
    let child =
        |field: &Field, role: Option<&str>, named: bool, nodes: &mut usize, observe: &mut _| {
            preflight_field(field, role, named, depth + 1, nodes, normalized, observe)
        };
    let mut comparison_work = crate::schema::type_comparison_node_work(data_type)?;
    match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => {
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                child(item, Some("item"), false, nodes, observe)?,
            )?
        }
        DataType::Struct(fields) => {
            for field in fields {
                comparison_work = crate::schema::comparison_work_add(
                    comparison_work,
                    child(field, None, true, nodes, observe)?,
                )?;
            }
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(
                    ExecutionValueTypeError::Invalid("map entries layout is not a struct").into(),
                );
            };
            if fields[0].is_nullable() {
                return Err(ExecutionValueTypeError::Invalid("map key layout is nullable").into());
            }
            // The entries Struct is physical scaffolding, not another semantic
            // node. Its fields still use their distinct anonymous roles.
            observe(1)?;
            if normalized {
                observe(1)?;
                if entries.name().len() != "entries".len() || !entries.metadata().is_empty() {
                    return Err(ExecutionValueTypeError::Invalid(
                        "value layout Map entries wrapper is not canonical",
                    )
                    .into());
                }
                observe("entries".len())?;
                if entries.name() != "entries" {
                    return Err(ExecutionValueTypeError::Invalid(
                        "value layout Map entries wrapper is not canonical",
                    )
                    .into());
                }
            }
            observe(1)?;
            let entries_depth = depth + 1;
            if entries_depth > MAX_VALUE_TYPE_DEPTH {
                return Err(ExecutionValueTypeError::Depth.into());
            }
            *nodes = nodes
                .checked_add(1)
                .ok_or_else(|| E::from(ExecutionValueTypeError::Nodes))?;
            if *nodes > MAX_VALUE_TYPE_NODES {
                return Err(ExecutionValueTypeError::Nodes.into());
            }
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                crate::schema::comparison_work_add(
                    if normalized {
                        crate::schema::field_comparison_work(entries.name())?
                    } else {
                        0
                    },
                    crate::schema::type_comparison_node_work(entries.data_type())?,
                )?,
            )?;
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                preflight_field(
                    &fields[0],
                    Some("key"),
                    false,
                    depth + 2,
                    nodes,
                    normalized,
                    observe,
                )?,
            )?;
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                preflight_field(
                    &fields[1],
                    Some("value"),
                    false,
                    depth + 2,
                    nodes,
                    normalized,
                    observe,
                )?,
            )?;
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                comparison_work = crate::schema::comparison_work_add(
                    comparison_work,
                    child(field, None, true, nodes, observe)?,
                )?;
            }
        }
        DataType::Dictionary(keys, values) => {
            if normalized {
                return Err(ExecutionValueTypeError::Invalid(
                    "value layout projection contains runtime encoding",
                )
                .into());
            }
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                preflight_observed(keys, depth + 1, nodes, normalized, observe)?,
            )?;
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                preflight_observed(values, depth + 1, nodes, normalized, observe)?,
            )?;
        }
        DataType::RunEndEncoded(ends, values) => {
            if normalized {
                return Err(ExecutionValueTypeError::Invalid(
                    "value layout projection contains runtime encoding",
                )
                .into());
            }
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                child(ends, None, false, nodes, observe)?,
            )?;
            comparison_work = crate::schema::comparison_work_add(
                comparison_work,
                child(values, None, false, nodes, observe)?,
            )?;
        }
        DataType::Timestamp(_, Some(timezone)) => {
            observe(1)?;
            if timezone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                return Err(ExecutionValueTypeError::Invalid(
                    "value layout timezone exceeds the byte limit",
                )
                .into());
            }
        }
        _ => {}
    }
    Ok(comparison_work)
}

fn normalize<E>(
    data_type: &DataType,
    comparison_work: &mut usize,
    construction: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<DataType, crate::ValueConstructionFailure<E>> {
    if let DataType::Dictionary(_, values) = data_type {
        return normalize(values, comparison_work, construction);
    }
    if let DataType::RunEndEncoded(_, values) = data_type {
        return normalize(values.data_type(), comparison_work, construction);
    }
    *comparison_work = crate::schema::comparison_work_add(
        *comparison_work,
        crate::schema::type_comparison_node_work(data_type)?,
    )?;
    Ok(match data_type {
        DataType::List(item) => DataType::List(normalize_field(
            item,
            "item",
            comparison_work,
            construction,
        )?),
        DataType::LargeList(item) => DataType::LargeList(normalize_field(
            item,
            "item",
            comparison_work,
            construction,
        )?),
        DataType::ListView(item) => DataType::ListView(normalize_field(
            item,
            "item",
            comparison_work,
            construction,
        )?),
        DataType::LargeListView(item) => DataType::LargeListView(normalize_field(
            item,
            "item",
            comparison_work,
            construction,
        )?),
        DataType::FixedSizeList(item, length) => DataType::FixedSizeList(
            normalize_field(item, "item", comparison_work, construction)?,
            *length,
        ),
        DataType::Struct(fields) => {
            construction.collected_arc_slice::<Arc<Field>>(fields.len())?;
            DataType::Struct(
                fields
                    .iter()
                    .map(|source| {
                        normalize_field(source, source.name(), comparison_work, construction)
                    })
                    .collect::<Result<Fields, _>>()?,
            )
        }
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(
                    ExecutionValueTypeError::Invalid("map entries layout is not a struct").into(),
                );
            };
            *comparison_work = crate::schema::comparison_work_add(
                *comparison_work,
                crate::schema::comparison_work_add(
                    crate::schema::field_comparison_work("entries")?,
                    crate::schema::COMPARISON_HEADER_WORK,
                )?,
            )?;
            construction.array::<Arc<Field>>(2)?;
            let children = vec![
                normalize_field(&fields[0], "key", comparison_work, construction)?,
                normalize_field(&fields[1], "value", comparison_work, construction)?,
            ];
            construction.arc_slice::<Arc<Field>>(2)?;
            let children = DataType::Struct(children.into());
            construction.name("entries".len())?;
            let entry = Field::new("entries", children, false);
            construction.exact(|| {
                crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<Field>())
            })?;
            DataType::Map(Arc::new(entry), *ordered)
        }
        DataType::Union(fields, mode) => {
            construction.collected_union_fields(fields.len())?;
            let fields = fields
                .iter()
                .map(|(id, source)| {
                    Ok((
                        id,
                        normalize_field(source, source.name(), comparison_work, construction)?,
                    ))
                })
                .collect::<Result<UnionFields, crate::ValueConstructionFailure<E>>>()?;
            DataType::Union(fields, *mode)
        }
        DataType::Dictionary(_, values) => normalize(values, comparison_work, construction)?,
        DataType::RunEndEncoded(_, values) => {
            normalize(values.data_type(), comparison_work, construction)?
        }
        _ => data_type.clone(),
    })
}

fn normalize_field<E>(
    source: &Field,
    name: &str,
    comparison_work: &mut usize,
    construction: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<Arc<Field>, crate::ValueConstructionFailure<E>> {
    *comparison_work = crate::schema::comparison_work_add(
        *comparison_work,
        crate::schema::field_comparison_work(name)?,
    )?;
    let ty = normalize(source.data_type(), comparison_work, construction)?;
    construction.name(name.len())?;
    let field = Field::new(name, ty, source.is_nullable());
    construction
        .exact(|| crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<Field>()))?;
    Ok(Arc::new(field))
}

/// Only immutable validated bindings invoke this borrowed structural projection.
pub(crate) fn normalized_child_type_at(data_type: &DataType, index: usize) -> Option<&DataType> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
            if index == 0 =>
        {
            Some(field.data_type())
        }
        DataType::Struct(fields) => fields.get(index).map(|field| field.data_type()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields.get(index).map(|field| field.data_type()),
            _ => None,
        },
        DataType::Union(fields, _) => fields.iter().nth(index).map(|(_, field)| field.data_type()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use arrow_schema::{TimeUnit, UnionMode};

    use super::*;
    use crate::value_type::ValueTypeStorageHold;

    fn layout(data_type: &DataType) -> ValueLayout {
        ValueLayout::try_new(data_type, ValueTypeStorageLifetime::pure()).unwrap()
    }

    fn materialized(data_type: DataType, children: Vec<ValueLayout>) -> ValueLayout {
        ValueLayout::try_from_normalized_arrow_with_children_observed(
            data_type,
            children.into_boxed_slice(),
            ValueTypeStorageLifetime::pure(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok::<_, ExecutionValueTypeError>(()),
        )
        .unwrap()
    }

    #[test]
    fn materialized_child_cache_preserves_independent_weighted_timezone_and_named_layouts() {
        fn tree() -> ValueLayout {
            let child = materialized(
                DataType::Timestamp(TimeUnit::Microsecond, Some("z".repeat(1024).into())),
                vec![],
            );
            materialized(
                DataType::Struct(
                    vec![Arc::new(Field::new(
                        "named timezone",
                        child.data_type().clone(),
                        true,
                    ))]
                    .into(),
                ),
                vec![child],
            )
        }
        let first = tree();
        let second = tree();
        assert!(!first.shares_storage(&second));
        let mut actual = 0;
        assert!(
            crate::schema::arrow_validated_data_types_exact_weighted_observed(
                first.data_type(),
                second.data_type(),
                |step| {
                    actual += step.work_units_upper();
                    Ok::<_, std::convert::Infallible>(())
                },
            )
            .unwrap()
        );
        assert_eq!(actual, first.comparison_work_upper_bound());
        let mut requests = 0;
        let child = first
            .child_projection_at_observed(
                0,
                ValueTypeStorageLifetime::pure(),
                |_| {
                    requests += 1;
                    Ok::<_, crate::owned_resources::layout::LayoutResourceError>(())
                },
                |_| Ok(()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(requests, 0);
        assert!(child.shares_storage(first.materialized_child_at(0).unwrap()));
        assert_eq!(
            child.comparison_work_upper_bound(),
            second
                .materialized_child_at(0)
                .unwrap()
                .comparison_work_upper_bound()
        );
    }

    #[test]
    fn materialized_map_children_use_exact_key_value_roles_and_reject_mismatched_cache() {
        let key = materialized(DataType::Utf8, vec![]);
        let value = materialized(DataType::Int64, vec![]);
        let ty = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let map = materialized(ty.clone(), vec![key.clone(), value.clone()]);
        assert_eq!(
            map.materialized_child_at(0).unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            map.materialized_child_at(1).unwrap().data_type(),
            &DataType::Int64
        );
        let error = ValueLayout::try_from_normalized_arrow_with_children_observed(
            ty,
            vec![value, key].into_boxed_slice(),
            ValueTypeStorageLifetime::pure(),
            ValueTypeStorageLifetime::pure(),
            |_| Ok::<_, ExecutionValueTypeError>(()),
        )
        .unwrap_err();
        assert_eq!(
            error,
            ExecutionValueTypeError::Invalid("layout child owner differs")
        );
    }

    fn hash(value: &ValueLayout) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn annotated(name: &str, data_type: DataType, nullable: bool) -> Arc<Field> {
        Arc::new(
            Field::new(name, data_type, nullable).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([
                ("provider.field_id".into(), "123".into()),
                // A layout author must not parse or validate this annotation.
                (crate::NR_LOGICAL_TYPE_KEY.into(), "uninterpreted".into()),
            ])),
        )
    }

    fn map(entries: &str, key: &str, value: &str, ordered: bool) -> DataType {
        DataType::Map(
            annotated(
                entries,
                DataType::Struct(
                    vec![
                        annotated(key, DataType::Utf8, false),
                        annotated(value, DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            ),
            ordered,
        )
    }

    fn assert_metadata_free(data_type: &DataType) {
        let inspect = |field: &Field| {
            assert!(field.metadata().is_empty());
            assert_metadata_free(field.data_type());
        };
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => inspect(item),
            DataType::Struct(fields) => {
                for field in fields {
                    inspect(field);
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    inspect(field);
                }
            }
            DataType::Dictionary(..) | DataType::RunEndEncoded(..) => {
                panic!("encoding wrapper remains in a value layout");
            }
            _ => {}
        }
    }

    #[test]
    fn normalized_owned_projection_reuses_fields_and_refuses_repair() {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let original = DataType::List(Arc::clone(&item));
        let value =
            ValueLayout::try_from_normalized_arrow(original, ValueTypeStorageLifetime::pure())
                .unwrap();
        let DataType::List(retained) = value.data_type() else {
            panic!("List layout");
        };
        assert!(Arc::ptr_eq(retained, &item));
        for invalid in [
            DataType::List(Arc::new(Field::new("provider_item", DataType::Utf8, true))),
            DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "source".into(),
                        "value".into(),
                    )]),
                ),
            )),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ] {
            assert!(
                ValueLayout::try_from_normalized_arrow(invalid, ValueTypeStorageLifetime::pure())
                    .is_err()
            );
        }
    }

    #[test]
    fn anonymous_role_labels_and_annotations_do_not_change_layout_identity() {
        let item = |name| DataType::List(annotated(name, DataType::Utf8, true));
        let left = layout(&item("element"));
        let right = layout(&item("item"));
        assert_eq!(left, right);
        assert_eq!(hash(&left), hash(&right));
        let DataType::List(item) = left.data_type() else {
            panic!("expected List");
        };
        assert_eq!(item.name(), "item");
        assert_metadata_free(left.data_type());

        let left = layout(&map("pairs", "k", "v", true));
        let right = layout(&map("entries", "key", "value", true));
        assert_eq!(left, right);
        assert_eq!(hash(&left), hash(&right));
        let DataType::Map(entries, true) = left.data_type() else {
            panic!("expected ordered Map");
        };
        assert_eq!(entries.name(), "entries");
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries Struct");
        };
        assert_eq!(fields[0].name(), "key");
        assert_eq!(fields[1].name(), "value");
        assert_metadata_free(left.data_type());
    }

    #[test]
    fn offset_widths_and_value_parameters_remain_exact() {
        assert_ne!(layout(&DataType::Utf8), layout(&DataType::LargeUtf8));
        assert_ne!(layout(&DataType::Utf8), layout(&DataType::Utf8View));
        let item = || annotated("element", DataType::Utf8, true);
        let variants = [
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::ListView(item()),
            DataType::LargeListView(item()),
        ];
        for (index, left) in variants.iter().enumerate() {
            for right in variants.iter().skip(index + 1) {
                assert_ne!(layout(left), layout(right));
            }
            assert_metadata_free(layout(left).data_type());
        }
        assert_ne!(
            layout(&DataType::Decimal128(12, 2)),
            layout(&DataType::Decimal128(12, 3))
        );
        assert_ne!(
            layout(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            layout(&DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
        assert_ne!(
            layout(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            layout(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
        );
        assert_ne!(
            layout(&map("entries", "key", "value", true)),
            layout(&map("entries", "key", "value", false))
        );
    }

    #[test]
    fn named_fields_nullability_and_union_modes_and_ids_remain_exact() {
        let structure = |name, nullable| {
            DataType::Struct(vec![annotated(name, DataType::Int64, nullable)].into())
        };
        assert_ne!(layout(&structure("a", true)), layout(&structure("b", true)));
        assert_ne!(
            layout(&structure("a", true)),
            layout(&structure("a", false))
        );
        let union = |id, mode| {
            DataType::Union(
                UnionFields::try_new([id], [annotated("variant", structure("a", true), true)])
                    .unwrap(),
                mode,
            )
        };
        let dense = layout(&union(3, UnionMode::Dense));
        assert_ne!(dense, layout(&union(3, UnionMode::Sparse)));
        assert_ne!(dense, layout(&union(4, UnionMode::Dense)));
        assert_metadata_free(dense.data_type());
    }

    #[test]
    fn nested_dictionary_and_run_end_wrappers_are_encoding_not_value_layout() {
        let plain = DataType::Struct(
            vec![annotated(
                "payload",
                DataType::LargeList(annotated("element", DataType::LargeUtf8, true)),
                true,
            )]
            .into(),
        );
        let encoded = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(
                vec![annotated(
                    "payload",
                    DataType::LargeList(annotated(
                        "values",
                        DataType::RunEndEncoded(
                            annotated("ends", DataType::Int32, false),
                            annotated(
                                "run_values",
                                DataType::Dictionary(
                                    Box::new(DataType::Int16),
                                    Box::new(DataType::LargeUtf8),
                                ),
                                true,
                            ),
                        ),
                        true,
                    )),
                    true,
                )]
                .into(),
            )),
        );
        let normalized = layout(&encoded);
        assert_eq!(normalized, layout(&plain));
        assert_eq!(hash(&normalized), hash(&layout(&plain)));
        assert_metadata_free(normalized.data_type());
    }

    #[test]
    fn slice_coordinates_and_rows_are_not_part_of_the_type_contract() {
        // Layout construction takes only a type, never an array, ordinal or
        // row range. Independently authored equal type projections stay equal.
        let first = DataType::FixedSizeList(annotated("item", DataType::Utf8, false), 4);
        let independently_authored =
            DataType::FixedSizeList(annotated("element", DataType::Utf8, false), 4);
        assert_eq!(layout(&first), layout(&independently_authored));
        assert_ne!(
            layout(&first),
            layout(&DataType::FixedSizeList(
                annotated("item", DataType::Utf8, false),
                5
            ))
        );
    }

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn final_layout_clone_retains_its_creation_responsibility() {
        let exits = Arc::new(AtomicUsize::new(0));
        let source = DataType::List(annotated("element", DataType::Utf8, false));
        let first = ValueLayout::try_new(
            &source,
            ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone()))),
        )
        .unwrap();
        let final_holder = first.clone();
        drop(source);
        drop(first);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_metadata_free(final_holder.data_type());
        drop(final_holder);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_and_oversized_source_shapes_are_refused_before_normalization() {
        let invalid_map = DataType::Map(
            annotated(
                "entries",
                DataType::Struct(
                    vec![
                        annotated("key", DataType::Utf8, true),
                        annotated("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            ),
            false,
        );
        for data_type in [
            invalid_map,
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Decimal128(0, 0),
            DataType::Dictionary(Box::new(DataType::Float32), Box::new(DataType::Utf8)),
        ] {
            assert!(matches!(
                ValueLayout::try_new(&data_type, ValueTypeStorageLifetime::pure()),
                Err(ExecutionValueTypeError::Invalid(_))
            ));
        }
        let mut deep = DataType::Int64;
        for _ in 0..MAX_VALUE_TYPE_DEPTH {
            deep = DataType::List(Arc::new(Field::new("item", deep, true)));
        }
        assert!(matches!(
            ValueLayout::try_new(&deep, ValueTypeStorageLifetime::pure()),
            Err(ExecutionValueTypeError::Depth)
        ));
        let wide = DataType::Struct(
            (0..MAX_VALUE_TYPE_NODES)
                .map(|_| Arc::new(Field::new("v", DataType::Int64, true)))
                .collect(),
        );
        assert!(matches!(
            ValueLayout::try_new(&wide, ValueTypeStorageLifetime::pure()),
            Err(ExecutionValueTypeError::Nodes)
        ));
    }
}
