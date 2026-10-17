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

//! Recursive value semantics, independent of format annotations and encoding.

use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow_schema::{DataType, Field};

use crate::{MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, ValueLogicalType};

/// A caller's already-acquired storage lifetime, with no authorization API.
///
/// Implementations retain only the responsibility for this constructed storage,
/// never an entire query/context. The contract cannot acquire, renew or inspect
/// capacity. Each independently retained child carries its own storage hold.
pub trait ValueTypeStorageHold: Send + Sync {}

/// Lifetime-only responsibility supplied by the actual construction owner.
#[derive(Clone, Default)]
pub struct ValueTypeStorageLifetime(Option<Arc<dyn ValueTypeStorageHold>>);

impl ValueTypeStorageLifetime {
    pub fn held(hold: Arc<dyn ValueTypeStorageHold>) -> Self {
        Self(Some(hold))
    }

    /// Pure authors have no runtime grant. Runtime construction supplies `held`.
    pub const fn pure() -> Self {
        Self(None)
    }

    /// Whether this fact retains no storage lifetime. This grants no access,
    /// capacity or permission to construct or capture another source.
    pub const fn is_pure(&self) -> bool {
        self.0.is_none()
    }

    /// Actual Arc request when combining two distinct retained lifetimes.
    /// Reusing the same lifetime or a pure lifetime requires no allocation.
    pub fn retaining_allocation_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            SharedTypeStorageLifetime,
        >())
    }

    fn flat_retention_backing_layout(
        count: usize,
    ) -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        std::alloc::Layout::array::<Self>(count)
            .map_err(|_| crate::owned_resources::layout::LayoutResourceError::Arithmetic)
    }

    fn flat_retention_holder_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            FlatSharedTypeStorageLifetime,
        >())
    }

    /// Upper requests for this original flat holder: Vec, possible Box trim,
    /// and the actual private Arc payload. The caller supplies an input-derived
    /// loan-count bound; this visitor creates no holder or capacity grant.
    pub fn retaining_flat_allocation_layouts_upper<E>(
        count: usize,
        visit: &mut dyn FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<crate::owned_resources::layout::LayoutResourceError>,
    {
        if count == 0 {
            return Ok(());
        }
        let backing = Self::flat_retention_backing_layout(count)?;
        visit(backing)?;
        visit(backing)?;
        visit(Self::flat_retention_holder_layout()?)
    }

    /// Preserve responsibility for reused backing, admitting the actual extra
    /// holder before combining distinct lifetimes. This is a construction hook,
    /// not an allocation grant; the caller supplies its original request port.
    pub fn retaining_observed<E: From<crate::owned_resources::layout::LayoutResourceError>>(
        &self,
        original: &Self,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        match (&self.0, &original.0) {
            (_, None) => Ok(self.clone()),
            (None, _) => Ok(original.clone()),
            (Some(new), Some(old)) if Arc::ptr_eq(new, old) => Ok(self.clone()),
            _ => {
                request(Self::retaining_allocation_layout()?)?;
                Ok(Self::held(Arc::new(SharedTypeStorageLifetime {
                    _new: self.clone(),
                    _original: original.clone(),
                })))
            }
        }
    }

    /// Retain a bounded set of real shared backing loans in one flat holder.
    /// Width does not become recursive destructor depth. The caller's actual
    /// owning marker retains the new Vec/Box/Arc responsibility; pure loans
    /// allocate nothing. This callback requests no capacity by itself.
    pub fn retaining_flat_observed<'a, E, I>(
        &self,
        originals: I,
        request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: &mut crate::CompileCheckpoints<'_>,
    ) -> Result<Self, E>
    where
        E: From<crate::owned_resources::layout::LayoutResourceError>
            + From<crate::CompileControlError>,
        I: Iterator<Item = Result<&'a Self, E>> + Clone,
    {
        self.retaining_flat_with_source_facts_observed(originals, request, work)
            .map(|(storage, _)| storage)
    }

    /// Final backing of this newly constructed flat loan holder only. Borrowed
    /// originals are not added again; this numeric fact grants no capacity.
    pub fn retaining_flat_with_source_facts_observed<'a, E, I>(
        &self,
        originals: I,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        work: &mut crate::CompileCheckpoints<'_>,
    ) -> Result<(Self, usize), E>
    where
        E: From<crate::owned_resources::layout::LayoutResourceError>
            + From<crate::CompileControlError>,
        I: Iterator<Item = Result<&'a Self, E>> + Clone,
    {
        use crate::owned_resources::layout::LayoutResourceError;
        use crate::owned_resources::{copy, vec as owned_vec};
        let distinct = |original: &&Self| match (&self.0, &original.0) {
            (_, None) => false,
            (Some(own), Some(other)) => !Arc::ptr_eq(own, other),
            (None, Some(_)) => true,
        };
        let mut count = 0usize;
        let mut single = None;
        for original in originals.clone() {
            work.step().map_err(E::from)?;
            let original = original?;
            if !distinct(&original) {
                continue;
            }
            count = count
                .checked_add(1)
                .ok_or(LayoutResourceError::Arithmetic)
                .map_err(E::from)?;
            single = Some(original);
        }
        if count == 0 {
            return Ok((self.clone(), 0));
        }
        if self.0.is_none() {
            if count == 1 {
                return Ok((single.expect("one original loan").clone(), 0));
            }
            // A source lifetime is not authority for creating another holder.
            // Runtime callers must supply that holder's actual owning marker.
            return Err(E::from(LayoutResourceError::SourceModel));
        }
        let backing = Self::flat_retention_backing_layout(count).map_err(E::from)?;
        work.flush().map_err(E::from)?;
        request(backing)?;
        let mut loans = Vec::new();
        let reserved = loans.try_reserve_exact(count);
        if reserved.is_ok() {
            work.step().map_err(E::from)?;
        }
        copy::reserve_exit::<E>(reserved, work)?;
        for original in originals {
            work.step().map_err(E::from)?;
            let original = original?;
            if distinct(&original) {
                loans.push(original.clone());
            }
        }
        // The sole boxed-slice body admits its trim before its own opaque
        // boundary. Flush pending caller work first only if that real trim
        // requests backing; no-allocation branches acquire no new footer.
        if owned_vec::box_facts(&loans)
            .map_err(FlatLifetimeFailure::<E>::from)
            .map_err(|error| error.into_caller())?
            .requested_backing
            .is_some()
        {
            work.flush().map_err(E::from)?;
        }
        let loans = owned_vec::boxed_slice_in(
            loans,
            &mut |facts| {
                if let Some(backing) = facts.requested_backing {
                    request(backing).map_err(FlatLifetimeFailure::Caller)?;
                }
                Ok::<_, FlatLifetimeFailure<E>>(())
            },
            work,
        )
        .map_err(|error| error.into_caller())?;
        work.flush().map_err(E::from)?;
        let holder = Self::flat_retention_holder_layout().map_err(E::from)?;
        request(holder)?;
        let retained_bytes = backing
            .size()
            .checked_add(holder.size())
            .ok_or(LayoutResourceError::Arithmetic)
            .map_err(E::from)?;
        Ok((
            Self::held(Arc::new(FlatSharedTypeStorageLifetime {
                _loans: loans,
                _own: self.clone(),
            })),
            retained_bytes,
        ))
    }

    /// Keep the original responsibility for reused scalar parameter storage.
    /// This combines lifetimes only; it creates no allocation authorization.
    pub(crate) fn retaining(&self, original: &Self) -> Self {
        self.retaining_with_construction::<std::convert::Infallible>(original, None)
            .expect("pure lifetime combination")
    }
    pub(crate) fn retaining_with_construction<E>(
        &self,
        original: &Self,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        let mut creation = crate::value_construction::ValueConstruction::new(observer);
        match (&self.0, &original.0) {
            (_, None) => Ok(self.clone()),
            (None, _) => Ok(original.clone()),
            (Some(new), Some(old)) if Arc::ptr_eq(new, old) => Ok(self.clone()),
            _ => {
                creation.exact(Self::retaining_allocation_layout)?;
                Ok(Self::held(Arc::new(SharedTypeStorageLifetime {
                    _new: self.clone(),
                    _original: original.clone(),
                })))
            }
        }
    }
}

struct FlatSharedTypeStorageLifetime {
    _loans: Box<[ValueTypeStorageLifetime]>,
    // Drop pointer backing before its actual construction responsibility.
    _own: ValueTypeStorageLifetime,
}
impl ValueTypeStorageHold for FlatSharedTypeStorageLifetime {}

enum FlatLifetimeFailure<E> {
    Caller(E),
    Control(crate::CompileControlError),
    Model(crate::ControlResourceError),
}
impl<E> From<crate::CompileControlError> for FlatLifetimeFailure<E> {
    fn from(error: crate::CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<E> From<crate::ControlResourceError> for FlatLifetimeFailure<E> {
    fn from(error: crate::ControlResourceError) -> Self {
        Self::Model(error)
    }
}
impl<
    E: From<crate::CompileControlError> + From<crate::owned_resources::layout::LayoutResourceError>,
> FlatLifetimeFailure<E>
{
    fn into_caller(self) -> E {
        match self {
            Self::Caller(error) => error,
            Self::Control(error) | Self::Model(crate::ControlResourceError::Control(error)) => {
                E::from(error)
            }
            Self::Model(crate::ControlResourceError::SourceModel(_)) => {
                E::from(crate::owned_resources::layout::LayoutResourceError::SourceModel)
            }
        }
    }
}

struct SharedTypeStorageLifetime {
    _new: ValueTypeStorageLifetime,
    _original: ValueTypeStorageLifetime,
}
impl ValueTypeStorageHold for SharedTypeStorageLifetime {}

impl fmt::Debug for ValueTypeStorageLifetime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueTypeStorageLifetime")
            .field("held", &self.0.is_some())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionValueTypeError {
    Invalid(&'static str),
    Logical(crate::ValueTypeError),
    Depth,
    Nodes,
}

impl From<crate::ValueTypeError> for ExecutionValueTypeError {
    fn from(error: crate::ValueTypeError) -> Self {
        Self::Logical(error)
    }
}
impl fmt::Display for ExecutionValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::Logical(error) => error.fmt(f),
            Self::Depth => f.write_str("execution value type exceeds the nesting depth limit"),
            Self::Nodes => f.write_str("execution value type exceeds the node count limit"),
        }
    }
}
impl std::error::Error for ExecutionValueTypeError {}

/// A named semantic field. Anonymous containers use item/key/value roles.
#[derive(Clone, Debug)]
pub struct ValueField(Arc<ValueFieldStorage>);

#[derive(Debug)]
struct ValueFieldStorage {
    name: Box<str>,
    value_type: ValueTypeRef,
    // Fields and names are dropped before the creation responsibility.
    _storage: ValueTypeStorageLifetime,
}

impl ValueField {
    /// Actual Arc request for this module's private field payload.
    /// Name backing is a separate request admitted by the creating owner.
    pub fn storage_allocation_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<ValueFieldStorage>())
    }

    pub fn new(
        name: impl Into<Box<str>>,
        value_type: ValueTypeRef,
        storage: ValueTypeStorageLifetime,
    ) -> Self {
        Self::from_boxed_name_core::<std::convert::Infallible>(
            name.into(),
            value_type,
            storage,
            &mut crate::value_construction::ValueConstruction::new(None),
        )
        .expect("pure field holder has no allocation observer")
    }
    /// Observe a fresh name and the original private field holder before
    /// allocating either. Reused child semantics keep their own original hold.
    pub fn from_string_with_construction<E>(
        name: String,
        value_type: ValueTypeRef,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        let mut creation = crate::value_construction::ValueConstruction::new(observer);
        if name.capacity() != name.len() {
            creation.name(name.len())?;
        }
        Self::from_boxed_name_core(name.into_boxed_str(), value_type, storage, &mut creation)
    }
    pub fn from_borrowed_name_with_construction<E>(
        name: &str,
        value_type: ValueTypeRef,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        let mut construction = crate::value_construction::ValueConstruction::new(observer);
        Self::from_borrowed_name_core(name, value_type, storage, &mut construction)
    }
    pub(crate) fn from_borrowed_name_core<E>(
        name: &str,
        value_type: ValueTypeRef,
        storage: ValueTypeStorageLifetime,
        construction: &mut crate::value_construction::ValueConstruction<'_, E>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        construction.name(name.len())?;
        Self::from_boxed_name_core(name.into(), value_type, storage, construction)
    }
    fn from_boxed_name_core<E>(
        name: Box<str>,
        value_type: ValueTypeRef,
        storage: ValueTypeStorageLifetime,
        construction: &mut crate::value_construction::ValueConstruction<'_, E>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        construction.exact(Self::storage_allocation_layout)?;
        Ok(Self(Arc::new(ValueFieldStorage {
            name,
            value_type,
            _storage: storage,
        })))
    }
    pub fn name(&self) -> &str {
        &self.0.name
    }
    pub fn value_type(&self) -> &ValueTypeRef {
        &self.0.value_type
    }
}
impl PartialEq for ValueField {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
            || (self.name() == other.name() && self.value_type() == other.value_type())
    }
}
impl Eq for ValueField {}
impl Hash for ValueField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.value_type().hash(state);
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ValueUnionField {
    pub type_id: i8,
    pub field: ValueField,
}

/// Semantics after removing runtime encoding and physical offset widths.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ValueKind {
    /// A non-container Arrow scalar, with all value parameters preserved.
    Scalar(DataType),
    String,
    Binary,
    Nominal(ValueLogicalType),
    List(ValueTypeRef),
    FixedSizeList {
        item: ValueTypeRef,
        length: i32,
    },
    Struct(Box<[ValueField]>),
    Map {
        key: ValueTypeRef,
        value: ValueTypeRef,
    },
    Union(Box<[ValueUnionField]>),
}

#[derive(Debug)]
struct ValueType {
    nullable: bool,
    kind: ValueKind,
    depth: usize,
    nodes: usize,
    comparison_work: usize,
    _storage: ValueTypeStorageLifetime,
}

// Design: ADR-0176 (docs/adr/ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)
/// Strong immutable semantic reference; there is no raw Arc extraction API.
#[derive(Clone, Debug)]
pub struct ValueTypeRef(Arc<ValueType>);

impl PartialEq for ValueTypeRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
            || (self.nullable() == other.nullable() && self.kind() == other.kind())
    }
}
impl Eq for ValueTypeRef {}
impl Hash for ValueTypeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.nullable().hash(state);
        self.kind().hash(state);
    }
}

impl ValueTypeRef {
    /// Actual Arc request, without mirroring this private representation.
    /// Child collections and scalar parameter backing are separate requests.
    pub fn storage_allocation_layout()
    -> Result<std::alloc::Layout, crate::owned_resources::layout::LayoutResourceError> {
        crate::owned_resources::layout::arc_layout(std::alloc::Layout::new::<ValueType>())
    }

    pub fn try_new(
        kind: ValueKind,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_new_observed(kind, nullable, storage, |_| Ok(()))
    }

    /// Fold actual immutable child comparison bounds during the required
    /// constructor traversal. The original caller observes real numerical work.
    pub fn try_new_observed<E: From<ExecutionValueTypeError>>(
        kind: ValueKind,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        observe: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::try_new_core(kind, nullable, storage, observe, None).map_err(|error| match error {
            crate::ValueConstructionFailure::ValueType(error) => E::from(error),
            crate::ValueConstructionFailure::Observer(error) => error,
            crate::ValueConstructionFailure::Rule(_)
            | crate::ValueConstructionFailure::Layout(_)
            | crate::ValueConstructionFailure::Vec(_)
            | crate::ValueConstructionFailure::HashMap(_) => {
                unreachable!("pure semantic constructor has no allocation model observer")
            }
        })
    }

    /// Fold and create the SAME semantic node under an optional allocation
    /// event. Child/name backing is admitted by its actual producer earlier.
    pub fn try_new_with_construction<E>(
        kind: ValueKind,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        Self::try_new_core(kind, nullable, storage, |_| Ok(()), observer)
    }

    fn try_new_core<E>(
        kind: ValueKind,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        mut observe: impl FnMut(usize) -> Result<(), E>,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        observe(1).map_err(crate::ValueConstructionFailure::Observer)?;
        let mut comparison_work = crate::value_identity::semantic_comparison_node_work(&kind)?;
        let mut depth = 1;
        let mut nodes: usize = 1;
        let mut child =
            |value: &Self, field_work: usize| -> Result<(), crate::ValueConstructionFailure<E>> {
                observe(3).map_err(crate::ValueConstructionFailure::Observer)?;
                comparison_work = crate::schema::comparison_work_add(comparison_work, field_work)?;
                comparison_work =
                    crate::schema::comparison_work_add(comparison_work, value.0.comparison_work)?;
                depth = depth.max(value.0.depth + 1);
                nodes = nodes
                    .checked_add(value.0.nodes)
                    .ok_or(ExecutionValueTypeError::Nodes)?;
                Ok(())
            };
        match &kind {
            ValueKind::Scalar(ty) => {
                if ty.is_nested()
                    || matches!(
                        ty,
                        DataType::Dictionary(..)
                            | DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::BinaryView
                    )
                {
                    return Err(ExecutionValueTypeError::Invalid(
                        "value scalar contains a container, encoding or noncanonical byte type",
                    )
                    .into());
                }
                crate::validate_arrow_carrier_parameters_observed::<crate::CarrierParameterError>(
                    ty,
                    || Ok(()),
                )
                .map_err(|_| ExecutionValueTypeError::Invalid("invalid value scalar parameters"))?;
            }
            ValueKind::Nominal(ValueLogicalType::Physical) => {
                return Err(ExecutionValueTypeError::Invalid(
                    "a value semantic identity cannot be Physical",
                )
                .into());
            }
            ValueKind::List(item) => child(item, 0)?,
            ValueKind::FixedSizeList { item, length } => {
                if *length < 0 {
                    return Err(ExecutionValueTypeError::Invalid(
                        "negative fixed-size list length",
                    )
                    .into());
                }
                child(item, 0)?;
            }
            ValueKind::Struct(fields) => {
                for field in fields {
                    child(
                        field.value_type(),
                        crate::value_identity::semantic_named_comparison_work(field.name())?,
                    )?;
                }
            }
            ValueKind::Map { key, value } => {
                if key.nullable() {
                    return Err(ExecutionValueTypeError::Invalid("map key type is nullable").into());
                }
                child(key, 0)?;
                child(value, 0)?;
            }
            ValueKind::Union(fields) => {
                let mut ids = [false; 128];
                for field in fields {
                    if field.type_id < 0 || ids[field.type_id as usize] {
                        return Err(ExecutionValueTypeError::Invalid(
                            "invalid or duplicate union type id",
                        )
                        .into());
                    }
                    ids[field.type_id as usize] = true;
                    child(
                        field.field.value_type(),
                        crate::schema::comparison_work_add(
                            crate::schema::COMPARISON_HEADER_WORK,
                            crate::value_identity::semantic_named_comparison_work(
                                field.field.name(),
                            )?,
                        )?,
                    )?;
                }
            }
            _ => {}
        }
        if depth > MAX_VALUE_TYPE_DEPTH {
            return Err(ExecutionValueTypeError::Depth.into());
        }
        if nodes > MAX_VALUE_TYPE_NODES {
            return Err(ExecutionValueTypeError::Nodes.into());
        }
        crate::value_construction::ValueConstruction::new(observer)
            .exact(Self::storage_allocation_layout)?;
        Ok(Self(Arc::new(ValueType {
            nullable,
            kind,
            depth,
            nodes,
            comparison_work,
            _storage: storage,
        })))
    }

    pub fn nullable(&self) -> bool {
        self.0.nullable
    }
    pub fn kind(&self) -> &ValueKind {
        &self.0.kind
    }
    /// Named logical domain, when this node is nominal. A container or ordinary
    /// scalar has no extra domain that must be reconstructed from annotations.
    pub fn domain(&self) -> Option<ValueLogicalType> {
        match self.kind() {
            ValueKind::Nominal(domain) => Some(*domain),
            _ => None,
        }
    }
    pub fn child_count(&self) -> usize {
        match self.kind() {
            ValueKind::List(_) | ValueKind::FixedSizeList { .. } => 1,
            ValueKind::Map { .. } => 2,
            ValueKind::Struct(fields) => fields.len(),
            ValueKind::Union(fields) => fields.len(),
            _ => 0,
        }
    }
    /// Borrow an ordered structural child without building another tree or
    /// cloning a handle. Map positions are key then value; names belong only
    /// to Struct/Union fields, never to anonymous List/Map roles.
    pub fn child_type_at(&self, index: usize) -> Option<&Self> {
        match self.kind() {
            ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } if index == 0 => {
                Some(item)
            }
            ValueKind::Map { key, .. } if index == 0 => Some(key),
            ValueKind::Map { value, .. } if index == 1 => Some(value),
            ValueKind::Struct(fields) => fields.get(index).map(ValueField::value_type),
            ValueKind::Union(fields) => fields.get(index).map(|field| field.field.value_type()),
            _ => None,
        }
    }
    /// Checked immutable comparison footprint; not identity or capacity.
    pub fn comparison_work_upper_bound(&self) -> usize {
        self.0.comparison_work
    }

    pub fn depth(&self) -> usize {
        self.0.depth
    }
    pub fn expanded_nodes(&self) -> usize {
        self.0.nodes
    }
    pub fn shares_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn with_nullable(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        self.with_nullable_with_construction::<std::convert::Infallible>(nullable, storage, None)
            .map_err(crate::ValueConstructionFailure::into_pure)
    }
    pub fn with_nullable_with_construction<E>(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        let mut creation = crate::value_construction::ValueConstruction::new(observer);
        let storage = if matches!(
            self.kind(),
            ValueKind::Scalar(DataType::Timestamp(_, Some(_)))
        ) {
            storage.retaining_with_construction(&self.0._storage, creation.observer())?
        } else {
            storage
        };
        // These are the only owned child collections in ValueKind::clone.
        match self.kind() {
            ValueKind::Struct(fields) => creation.array::<ValueField>(fields.len())?,
            ValueKind::Union(fields) => creation.array::<ValueUnionField>(fields.len())?,
            _ => {}
        }
        Self::try_new_with_construction(self.kind().clone(), nullable, storage, creation.observer())
    }

    /// Author a root NULL contract from validated immutable semantics. Observe
    /// each actual strong reference copy and admit both the new node and any
    /// child-vector or lifetime-combination allocation before constructing it.
    pub fn with_nullable_observed<E: From<crate::owned_resources::layout::LayoutResourceError>>(
        &self,
        nullable: bool,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        work(1)?;
        if self.nullable() == nullable {
            return Ok(self.clone());
        }
        let storage = if matches!(
            self.kind(),
            ValueKind::Scalar(DataType::Timestamp(_, Some(_)))
        ) {
            storage.retaining_observed(&self.0._storage, &mut request)?
        } else {
            storage
        };
        let kind = match self.kind() {
            ValueKind::Struct(fields) => {
                if !fields.is_empty() {
                    request(
                        std::alloc::Layout::array::<ValueField>(fields.len()).map_err(|_| {
                            crate::owned_resources::layout::LayoutResourceError::Arithmetic
                        })?,
                    )?;
                }
                let mut copied = Vec::with_capacity(fields.len());
                for field in fields {
                    work(1)?;
                    copied.push(field.clone());
                }
                ValueKind::Struct(copied.into_boxed_slice())
            }
            ValueKind::Union(fields) => {
                if !fields.is_empty() {
                    request(
                        std::alloc::Layout::array::<ValueUnionField>(fields.len()).map_err(
                            |_| crate::owned_resources::layout::LayoutResourceError::Arithmetic,
                        )?,
                    )?;
                }
                let mut copied = Vec::with_capacity(fields.len());
                for field in fields {
                    work(1)?;
                    copied.push(field.clone());
                }
                ValueKind::Union(copied.into_boxed_slice())
            }
            kind => {
                work(self.child_count().max(1))?;
                kind.clone()
            }
        };
        request(Self::storage_allocation_layout()?)?;
        Ok(Self(Arc::new(ValueType {
            nullable,
            kind,
            depth: self.0.depth,
            nodes: self.0.nodes,
            comparison_work: self.0.comparison_work,
            _storage: storage,
        })))
    }

    /// Import a declaration at a real external format/legacy migration boundary.
    /// Execution callers must carry a bound type, never call this to repair one.
    pub fn try_from_external_field(
        field: &Field,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        let logical = match field.metadata().get(crate::NR_LOGICAL_TYPE_KEY) {
            None => ValueLogicalType::Physical,
            Some(tag) => ValueLogicalType::from_metadata_value(tag)?,
        };
        Self::try_from_declared_arrow(field.data_type(), field.is_nullable(), logical, storage)
    }

    pub fn try_from_declared_arrow(
        data_type: &DataType,
        nullable: bool,
        logical: ValueLogicalType,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ExecutionValueTypeError> {
        Self::try_from_declared_arrow_with_construction::<std::convert::Infallible>(
            data_type, nullable, logical, storage, None,
        )
        .map_err(crate::ValueConstructionFailure::into_pure)
    }

    /// The original declaration traversal with optional before-allocation
    /// events. This does not import any additional domain or format grammar.
    pub fn try_from_declared_arrow_with_construction<E>(
        data_type: &DataType,
        nullable: bool,
        logical: ValueLogicalType,
        storage: ValueTypeStorageLifetime,
        observer: Option<&mut dyn FnMut(crate::ValueConstructionRequest) -> Result<(), E>>,
    ) -> Result<Self, crate::ValueConstructionFailure<E>> {
        from_declared_arrow(
            data_type,
            nullable,
            logical,
            storage,
            1,
            &mut 0,
            &mut crate::value_construction::ValueConstruction::new(observer),
        )
    }
}

fn from_declared_arrow<E>(
    ty: &DataType,
    nullable: bool,
    logical: ValueLogicalType,
    storage: ValueTypeStorageLifetime,
    depth: usize,
    visited: &mut usize,
    construction: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<ValueTypeRef, crate::ValueConstructionFailure<E>> {
    *visited = visited
        .checked_add(1)
        .ok_or(ExecutionValueTypeError::Nodes)?;
    if *visited > MAX_VALUE_TYPE_NODES {
        return Err(ExecutionValueTypeError::Nodes.into());
    }
    crate::validate_arrow_carrier_parameters_observed::<crate::CarrierParameterError>(
        ty,
        || Ok(()),
    )
    .map_err(|_| ExecutionValueTypeError::Invalid("invalid declared value carrier parameters"))?;
    if depth > MAX_VALUE_TYPE_DEPTH {
        return Err(ExecutionValueTypeError::Depth.into());
    }
    if let DataType::Dictionary(_, values) = ty {
        return from_declared_arrow(
            values,
            nullable,
            logical,
            storage,
            depth,
            visited,
            construction,
        );
    }
    if let DataType::RunEndEncoded(_, values) = ty {
        let child_logical = external_logical_type(values)?;
        if logical != ValueLogicalType::Physical
            && child_logical != ValueLogicalType::Physical
            && logical != child_logical
        {
            return Err(ExecutionValueTypeError::Invalid(
                "run-end value domain conflicts with the root declaration",
            )
            .into());
        }
        let logical = if logical == ValueLogicalType::Physical {
            child_logical
        } else {
            logical
        };
        return from_declared_arrow(
            values.data_type(),
            nullable,
            logical,
            storage,
            depth,
            visited,
            construction,
        );
    }
    if logical != ValueLogicalType::Physical {
        logical.validate_carrier(ty)?;
        return ValueTypeRef::try_new_with_construction(
            ValueKind::Nominal(logical),
            nullable,
            storage,
            construction.observer(),
        );
    }
    let kind = match ty {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ValueKind::String,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => ValueKind::Binary,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => ValueKind::List(from_external_child(
            item,
            storage.clone(),
            depth + 1,
            visited,
            construction,
        )?),
        DataType::FixedSizeList(item, length) => ValueKind::FixedSizeList {
            item: from_external_child(item, storage.clone(), depth + 1, visited, construction)?,
            length: *length,
        },
        DataType::Struct(fields) => {
            construction.collected_box_slice::<ValueField>(fields.len())?;
            ValueKind::Struct(
                fields
                    .iter()
                    .map(|field| {
                        let child = from_external_child(
                            field,
                            storage.clone(),
                            depth + 1,
                            visited,
                            construction,
                        )?;
                        ValueField::from_borrowed_name_core(
                            field.name().as_str(),
                            child,
                            storage.clone(),
                            construction,
                        )
                    })
                    .collect::<Result<_, crate::ValueConstructionFailure<E>>>()?,
            )
        }
        DataType::Map(entries, _) => {
            if entries.is_nullable() {
                return Err(
                    ExecutionValueTypeError::Invalid("map entries type is nullable").into(),
                );
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(
                    ExecutionValueTypeError::Invalid("map entries are not a struct").into(),
                );
            };
            if fields.len() != 2 {
                return Err(ExecutionValueTypeError::Invalid(
                    "map entries do not contain key and value",
                )
                .into());
            }
            ValueKind::Map {
                key: from_external_child(
                    &fields[0],
                    storage.clone(),
                    depth + 1,
                    visited,
                    construction,
                )?,
                value: from_external_child(
                    &fields[1],
                    storage.clone(),
                    depth + 1,
                    visited,
                    construction,
                )?,
            }
        }
        DataType::Union(fields, _) => {
            construction.collected_box_slice::<ValueUnionField>(fields.len())?;
            ValueKind::Union(
                fields
                    .iter()
                    .map(|(type_id, field)| {
                        let child = from_external_child(
                            field,
                            storage.clone(),
                            depth + 1,
                            visited,
                            construction,
                        )?;
                        Ok(ValueUnionField {
                            type_id,
                            field: ValueField::from_borrowed_name_core(
                                field.name().as_str(),
                                child,
                                storage.clone(),
                                construction,
                            )?,
                        })
                    })
                    .collect::<Result<_, crate::ValueConstructionFailure<E>>>()?,
            )
        }
        _ => ValueKind::Scalar(ty.clone()),
    };
    ValueTypeRef::try_new_with_construction(kind, nullable, storage, construction.observer())
}

fn from_external_child<E>(
    field: &Field,
    storage: ValueTypeStorageLifetime,
    depth: usize,
    visited: &mut usize,
    construction: &mut crate::value_construction::ValueConstruction<'_, E>,
) -> Result<ValueTypeRef, crate::ValueConstructionFailure<E>> {
    from_declared_arrow(
        field.data_type(),
        field.is_nullable(),
        external_logical_type(field)?,
        storage,
        depth,
        visited,
        construction,
    )
}

fn external_logical_type(field: &Field) -> Result<ValueLogicalType, ExecutionValueTypeError> {
    Ok(match field.metadata().get(crate::NR_LOGICAL_TYPE_KEY) {
        None => ValueLogicalType::Physical,
        Some(tag) => ValueLogicalType::from_metadata_value(tag)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn format_roles_do_not_change_list_or_map_semantics() {
        let list = |name: &str| DataType::List(Arc::new(Field::new(name, DataType::Utf8, true)));
        let value = |ty: &DataType| {
            ValueTypeRef::try_from_declared_arrow(
                ty,
                true,
                ValueLogicalType::Physical,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        };
        assert_eq!(value(&list("element")), value(&list("item")));
        let map = |entries: &str, key: &str, val: &str| {
            DataType::Map(
                Arc::new(Field::new(
                    entries,
                    DataType::Struct(
                        vec![
                            Field::new(key, DataType::Int32, false),
                            Field::new(val, DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        assert_eq!(
            value(&map("entries", "key", "value")),
            value(&map("pairs", "k", "v"))
        );
    }

    #[test]
    fn named_structs_and_nominal_children_remain_distinct() {
        let nominal = Field::new("item", DataType::Utf8, true)
            .with_metadata([(crate::NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
        let list = |field| {
            ValueTypeRef::try_from_declared_arrow(
                &DataType::List(Arc::new(field)),
                true,
                ValueLogicalType::Physical,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        };
        assert_ne!(
            list(nominal),
            list(Field::new("item", DataType::Utf8, true))
        );
        let structure = |name: &str| {
            ValueTypeRef::try_from_declared_arrow(
                &DataType::Struct(vec![Field::new(name, DataType::Utf8, true)].into()),
                false,
                ValueLogicalType::Physical,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        };
        assert_ne!(structure("a"), structure("b"));
    }

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn an_independent_child_keeps_its_creation_responsibility() {
        let exits = Arc::new(AtomicUsize::new(0));
        let lifetime = ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone())));
        let parent = ValueTypeRef::try_from_declared_arrow(
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
            ValueLogicalType::Physical,
            lifetime,
        )
        .unwrap();
        let ValueKind::List(child) = parent.kind() else {
            panic!("list expected");
        };
        let child = child.clone();
        drop(parent);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_eq!(child.kind(), &ValueKind::String);
        drop(child);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn logical_strings_do_not_inherit_offset_width_or_encoding() {
        let value = |ty| {
            ValueTypeRef::try_from_declared_arrow(
                &ty,
                false,
                ValueLogicalType::Physical,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap()
        };
        assert_eq!(value(DataType::Utf8), value(DataType::LargeUtf8));
        assert_eq!(value(DataType::Utf8), value(DataType::Utf8View));
        assert_eq!(
            value(DataType::Utf8),
            value(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            ))
        );
    }

    #[test]
    fn nullable_timestamp_projection_keeps_the_shared_parameter_owner() {
        let original_exits = Arc::new(AtomicUsize::new(0));
        let projection_exits = Arc::new(AtomicUsize::new(0));
        let original = ValueTypeRef::try_new(
            ValueKind::Scalar(DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("Europe/Paris".into()),
            )),
            false,
            ValueTypeStorageLifetime::held(Arc::new(Hold(original_exits.clone()))),
        )
        .unwrap();
        let projection = original
            .with_nullable(
                true,
                ValueTypeStorageLifetime::held(Arc::new(Hold(projection_exits.clone()))),
            )
            .unwrap();
        drop(original);
        assert_eq!(original_exits.load(Ordering::SeqCst), 0);
        assert_eq!(projection_exits.load(Ordering::SeqCst), 0);
        assert!(projection.nullable());
        drop(projection);
        assert_eq!(original_exits.load(Ordering::SeqCst), 1);
        assert_eq!(projection_exits.load(Ordering::SeqCst), 1);
    }
}
