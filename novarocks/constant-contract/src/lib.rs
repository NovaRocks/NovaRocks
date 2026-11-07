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

//! Immutable typed constants shared by planning and exact function owners.
//!
//! This crate parses no IPC and owns no runtime capability or memory grant.
//! Inputs already own Arrow backing. Resource facts describe borrowed input,
//! including retained capacity, and are not allocation authorization.

#[cfg(test)]
mod borrowed_comparison_tests;
#[cfg(test)]
mod flat_resource_tests;
#[cfg(test)]
mod input_derived_resource_tests;
#[cfg(test)]
mod scalar_factory_tests;
#[cfg(test)]
mod tests;

mod source_owner;
pub use source_owner::{
    ConstantSourceCreationFacts, ConstantSourceOwner, validate_pure_borrowed_source_in,
};

mod recursive_resources;
pub use recursive_resources::{
    RecursiveConstantResourceInput, preflight_recursive_pool_resources,
    preflight_recursive_pool_resources_in,
    preflight_recursive_pool_resources_input_derived_observed,
    preflight_recursive_pool_resources_observed,
};

mod semantic_key;
pub use semantic_key::ConstantSemanticKey;
mod collection_factory;
mod diagnostic;
mod selected_collections;
mod selected_scalar;
mod selected_scalar_read;
pub use selected_collections::{SelectedInt32List, SelectedUtf8Map};

use arrow_array::{Array, ArrayRef, make_array};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, UnionMode};
use novarocks_type_contract::{
    CarrierParameterError, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionValueType, PureCompileControl, ValueLogicalType, ValueTypeError, ValueTypeVisit,
    arrow_data_types_exact_borrowed_observed, validate_arrow_carrier_parameters_observed,
};
use std::{collections::BTreeSet, fmt, sync::Arc};

/// Explicit admission policy. No application defaults are inferred here.
/// Library validation is opaque: its admitted work/temporary bytes are bounded
/// before the call, with checkpoints before and after, not inside Arrow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantPolicy {
    pub max_rows: u64,
    pub max_array_nodes: u64,
    pub max_logical_elements: u64,
    pub max_retained_buffer_bytes: u64,
    pub max_type_depth: u32,
    pub max_type_nodes: u64,
    pub max_dictionary_depth: u32,
    pub max_metadata_bytes: u64,
    pub max_library_validation_work: u64,
    pub max_library_validation_bytes: u64,
}

/// Derived input facts, not a MEM charge, allocation grant or process RSS.
/// Logical/opaque-call facts are conservative upper bounds. Buffer capacity
/// is deduplicated by Arrow's reported allocation base and capacity; hidden
/// external owner allocations and allocator overhead are not described.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantResourceFacts {
    pub rows: u64,
    pub array_nodes: u64,
    pub buffer_count: u64,
    pub logical_elements_upper_bound: u64,
    pub retained_buffer_capacity_bytes: u64,
    pub metadata_bytes: u64,
    pub library_validation_work_upper_bound: u64,
    pub library_validation_temporary_bytes_upper_bound: u64,
    /// Inspected bytes, including repeated references, plus temporary bytes.
    /// This is the exact derived input to the library-validation byte gate,
    /// distinct from deduplicated retained backing or allocation authorization.
    pub library_validation_bytes_upper_bound: u64,
}

/// Conservative numeric inputs from a checked flat carrier projection and its
/// real backing-allocation recipe. These are not ArrayData or allocation grants.
/// The caller must cover discarded descriptors, full retained backing and
/// repeated view inspection; byte length alone does not establish these bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatConstantResourceInput {
    pub rows: u64,
    pub buffer_count_upper_bound: u64,
    /// Includes additional successful UTF8 fallback scans of the values
    /// descriptor, as well as each visited buffer (including validity).
    pub buffer_visits_bytes_upper_bound: u64,
    pub retained_buffer_capacity_bytes_upper_bound: u64,
    pub view_validation_bytes_upper_bound: u64,
}

/// The same ConstantPolicy validation envelope before array materialization.
/// Reader/schema/container/error allocation admission remains the caller's duty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatConstantResourceBounds {
    pub metadata_bytes: u64,
    pub library_validation_work_upper_bound: u64,
    pub library_validation_temporary_bytes_upper_bound: u64,
    pub library_validation_bytes_upper_bound: u64,
}

/// Reuses the complete Field/FVT author and the post-array numerical envelope.
/// No schema, array, backing or value is constructed. All limits and control
/// originate with the caller; ordinary failures observe the completed tail.
pub fn preflight_flat_pool_resources(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<FlatConstantResourceBounds, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let checked = preflight_flat_pool_resources_core(
        field,
        value_type,
        input,
        policy,
        &mut PoolObserver::plain(),
        &mut work,
    );
    if matches!(&checked, Err(ConstantError::Control(_))) {
        return checked;
    }
    work.finish()?;
    checked
}

/// The same pre-array law on the caller's meter, with synchronous owned
/// request/work prefixes. The caller owns entry and all ordinary/success tails.
pub fn preflight_flat_pool_resources_in(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    policy: ConstantPolicy,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantResourceBounds, ConstantError> {
    preflight_flat_pool_resources_core(
        field,
        value_type,
        input,
        policy,
        &mut PoolObserver::parent(&mut |facts| admit(facts).map_err(ConstantError::from)),
        work,
    )
}
/// Same original resource author, retaining a caller's nominal observation refusal.
pub fn preflight_flat_pool_resources_observed<H>(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    policy: ConstantPolicy,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantResourceBounds, ConstantCreationFailure<H>> {
    preflight_flat_pool_resources_core(
        field,
        value_type,
        input,
        policy,
        &mut PoolObserver::parent(&mut |facts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        }),
        work,
    )
}
/// Derives a finite envelope from this checked flat input using the same
/// numerical author. No caller quota or allocation authority is introduced.
pub fn preflight_flat_pool_resources_input_derived_observed<H>(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FlatConstantResourceBounds, ConstantPolicy), ConstantCreationFailure<H>> {
    preflight_flat_pool_resources_profile_core(
        field,
        value_type,
        input,
        None,
        &mut PoolObserver::parent(&mut |facts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        }),
        work,
    )
}
fn preflight_flat_pool_resources_core<E: PoolFailure>(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantResourceBounds, E> {
    preflight_flat_pool_resources_profile_core(
        field,
        value_type,
        input,
        Some(policy),
        observer,
        work,
    )
    .map(|(bounds, _)| bounds)
}
fn preflight_flat_pool_resources_profile_core<E: PoolFailure>(
    field: &Field,
    value_type: &FunctionValueType,
    input: FlatConstantResourceInput,
    policy: Option<ConstantPolicy>,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FlatConstantResourceBounds, ConstantPolicy), E> {
    let type_facts = validate_type_profile_owned(field, value_type, policy, observer, work)?;
    let metadata_bytes = type_facts.metadata;
    observer.step(work)?;
    let flat = is_flat_resource_carrier(field.data_type());
    if !flat {
        return Err(ConstantError::Invalid("constant resource projection is not flat").into());
    }
    optional_limit(
        input.rows,
        policy.map(|p| p.max_rows),
        "constant row limit exceeded",
    )?;
    optional_limit(
        1,
        policy.map(|p| p.max_array_nodes),
        "constant array node limit exceeded",
    )?;
    optional_limit(
        1,
        policy.map(|p| u64::from(p.max_type_depth)),
        "constant array depth limit exceeded",
    )?;
    optional_limit(
        input.rows,
        policy.map(|p| p.max_logical_elements),
        "constant stored element limit exceeded",
    )?;
    optional_limit(
        input.retained_buffer_capacity_bytes_upper_bound,
        policy.map(|p| p.max_retained_buffer_bytes),
        "constant retained buffer limit exceeded",
    )?;
    logical_elements_observed_owned(input.rows, input.rows, 1, observer, work)?;
    let counts = ValidationCounts {
        nodes: 1,
        storage_elements: input.rows,
        buffer_count: input.buffer_count_upper_bound,
        buffer_visits: input.buffer_visits_bytes_upper_bound,
        view_validation_bytes: input.view_validation_bytes_upper_bound,
        utf8_fallback_validation_bytes: 0,
        masks: 0,
        depth: 1,
    };
    let envelope =
        validation_envelope_profile_owned(counts, metadata_bytes, policy, observer, work)?;
    let sealed = policy.unwrap_or(ConstantPolicy {
        max_rows: input.rows,
        max_array_nodes: 1,
        max_logical_elements: input.rows,
        max_retained_buffer_bytes: input.retained_buffer_capacity_bytes_upper_bound,
        max_type_depth: 1,
        max_type_nodes: type_facts.nodes,
        max_dictionary_depth: 0,
        max_metadata_bytes: metadata_bytes,
        max_library_validation_work: envelope.work,
        max_library_validation_bytes: envelope.bytes,
    });
    Ok((
        FlatConstantResourceBounds {
            metadata_bytes,
            library_validation_work_upper_bound: envelope.work,
            library_validation_temporary_bytes_upper_bound: envelope.temporary,
            library_validation_bytes_upper_bound: envelope.bytes,
        },
        sealed,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Invalid(&'static str),
    Source(novarocks_failure_contract::SharedFailureCause),
    Limit(&'static str),
}
impl From<ValueTypeError> for ConstantError {
    fn from(value: ValueTypeError) -> Self {
        Self::Type(value)
    }
}
impl From<CompileControlError> for ConstantError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<CarrierParameterError> for ConstantError {
    fn from(value: CarrierParameterError) -> Self {
        match value {
            CarrierParameterError::Invalid(message) => Self::Invalid(message),
            CarrierParameterError::Decimal(error) => Self::arrow_validation(error),
        }
    }
}
impl fmt::Display for ConstantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Invalid(e) | Self::Limit(e) => f.write_str(e),
            Self::Source(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ConstantError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Source(source) => Some(source),
            Self::Invalid(_) | Self::Limit(_) => None,
        }
    }
}
mod failure;

/// Cumulative requests and work of one constant-owner invocation. Source B,
/// retained input buffers, and IPC reader output requests are excluded. The
/// caller merges this same contribution with its previously admitted future
/// bound; these facts are not a second ledger or an allocation grant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConstantOwnerResourceFacts {
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// A constructor's ordinary constant error and its caller's original refusal
/// remain distinct. No host payload is formatted or recast as compile control.
#[derive(Debug)]
pub enum ConstantCreationFailure<H> {
    Constant(ConstantError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    SourceMetadata(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError),
}
impl<H> From<ConstantError> for ConstantCreationFailure<H> {
    fn from(error: ConstantError) -> Self {
        Self::Constant(error)
    }
}
impl<H> From<CompileControlError> for ConstantCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Constant(error.into())
    }
}
impl<H> From<ValueTypeError> for ConstantCreationFailure<H> {
    fn from(error: ValueTypeError) -> Self {
        Self::Constant(error.into())
    }
}
impl<H> From<CarrierParameterError> for ConstantCreationFailure<H> {
    fn from(error: CarrierParameterError) -> Self {
        Self::Constant(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ConstantCreationFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl<H> From<novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError> for ConstantCreationFailure<H> {
    fn from(error: novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError) -> Self { Self::SourceMetadata(error) }
}
impl<H: fmt::Display> fmt::Display for ConstantCreationFailure<H> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constant(error) => error.fmt(out),
            Self::Creator(error) => error.fmt(out),
            Self::Resource(error) => write!(out, "constant construction layout: {error:?}"),
            Self::SourceMetadata(error) => write!(out, "constant source metadata: {error:?}"),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for ConstantCreationFailure<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Constant(error) => Some(error),
            Self::Creator(error) => Some(error),
            Self::Resource(_) | Self::SourceMetadata(_) => None,
        }
    }
}
#[derive(Clone, Copy)]
enum PoolStructureClass {
    Temporary,
    Retained,
}
trait PoolCreationPort<E> {
    fn exact(&mut self, layout: std::alloc::Layout, class: PoolStructureClass) -> Result<(), E>;
    fn opaque(&mut self, bytes: usize, class: PoolStructureClass) -> Result<(), E>;
}
struct PoolCreator<'a, H>(&'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>);
impl<H> PoolCreationPort<ConstantCreationFailure<H>> for PoolCreator<'_, H> {
    fn exact(
        &mut self,
        layout: std::alloc::Layout,
        _: PoolStructureClass,
    ) -> Result<(), ConstantCreationFailure<H>> {
        self.0
            .request_layout(layout)
            .map_err(ConstantCreationFailure::Creator)
    }
    fn opaque(
        &mut self,
        bytes: usize,
        _: PoolStructureClass,
    ) -> Result<(), ConstantCreationFailure<H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(ConstantCreationFailure::Creator)
    }
}

struct PairedPoolCreator<'a, H> {
    temporary: &'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    retained: &'a mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
}
impl<H> PoolCreationPort<ConstantCreationFailure<H>> for PairedPoolCreator<'_, H> {
    fn exact(
        &mut self,
        layout: std::alloc::Layout,
        class: PoolStructureClass,
    ) -> Result<(), ConstantCreationFailure<H>> {
        let port = match class {
            PoolStructureClass::Temporary => &mut *self.temporary,
            PoolStructureClass::Retained => &mut *self.retained,
        };
        port.request_layout(layout)
            .map_err(ConstantCreationFailure::Creator)
    }
    fn opaque(
        &mut self,
        bytes: usize,
        class: PoolStructureClass,
    ) -> Result<(), ConstantCreationFailure<H>> {
        let port = match class {
            PoolStructureClass::Temporary => &mut *self.temporary,
            PoolStructureClass::Retained => &mut *self.retained,
        };
        port.observe_opaque_upper_bound(bytes)
            .map_err(ConstantCreationFailure::Creator)
    }
}

trait PoolFailure:
    From<ConstantError> + From<CompileControlError> + From<ValueTypeError> + From<CarrierParameterError>
{
    fn skip_tail(&self) -> bool;
}
impl PoolFailure for ConstantError {
    fn skip_tail(&self) -> bool {
        matches!(self, Self::Control(_))
    }
}
impl<H> PoolFailure for ConstantCreationFailure<H> {
    fn skip_tail(&self) -> bool {
        match self {
            Self::Constant(error) => error.skip_tail(),
            _ => true,
        }
    }
}

type ConstantOwnerAdmit<'a, E = ConstantError> =
    dyn FnMut(&ConstantOwnerResourceFacts) -> Result<(), E> + 'a;
struct PoolObserver<'a, E = ConstantError> {
    admit: Option<&'a mut ConstantOwnerAdmit<'a, E>>,
    creator: Option<&'a mut dyn PoolCreationPort<E>>,
    facts: ConstantOwnerResourceFacts,
    required_root_prefunded: bool,
    array_storage: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    tree: novarocks_type_contract::owned_resources::btree::InsertionOnlyFacts,
}
impl<'a, E: PoolFailure> PoolObserver<'a, E> {
    fn plain() -> Self {
        Self {
            admit: None,
            creator: None,
            facts: ConstantOwnerResourceFacts::default(),
            required_root_prefunded: false,
            array_storage: None,
            tree: novarocks_type_contract::owned_resources::btree::InsertionOnlyFacts {
                allocation_requests_upper_bound: 0,
                request_bytes_upper_bound: 0,
                cumulative_work_upper_bound: 0,
            },
        }
    }
    fn parent(admit: &'a mut ConstantOwnerAdmit<'a, E>) -> Self {
        Self {
            admit: Some(admit),
            ..Self::plain()
        }
    }
    fn is_parent(&self) -> bool {
        self.admit.is_some() || self.creator.is_some()
    }
    fn sum(a: usize, b: usize) -> Result<usize, E> {
        a.checked_add(b)
            .ok_or_else(|| CompileControlError::ResourceExhausted.into())
    }
    fn gate(&mut self) -> Result<(), E> {
        if let Some(admit) = &mut self.admit {
            admit(&self.facts)?;
        }
        Ok(())
    }
    fn work(&mut self, n: usize) -> Result<(), E> {
        if self.is_parent() {
            self.facts.cumulative_work_upper_bound =
                Self::sum(self.facts.cumulative_work_upper_bound, n)?;
            self.gate()?;
        }
        Ok(())
    }
    fn step(&mut self, work: &mut CompileCheckpoints<'_>) -> Result<(), E> {
        self.work(1)?;
        work.step()?;
        Ok(())
    }
    fn request(
        &mut self,
        layout: std::alloc::Layout,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        self.request_classified(layout, PoolStructureClass::Temporary, work)
    }
    fn request_classified(
        &mut self,
        layout: std::alloc::Layout,
        class: PoolStructureClass,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        if self.creator.is_some() && layout.size() != 0 {
            work.flush()?;
        }
        if self.is_parent() && layout.size() != 0 {
            if !novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN {
                return Err(
                    ConstantError::Invalid("constant owned request source model drift").into(),
                );
            }
            self.facts.allocation_requests_upper_bound =
                Self::sum(self.facts.allocation_requests_upper_bound, 1)?;
            self.facts.allocation_request_bytes_upper_bound = Self::sum(
                self.facts.allocation_request_bytes_upper_bound,
                layout.size(),
            )?;
            self.work(layout.size())?;
            if let Some(creator) = &mut self.creator {
                creator.exact(layout, class)?;
            }
        }
        Ok(())
    }
    fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        additional: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        if !self.is_parent() {
            return Ok(());
        }
        let needed = Self::sum(values.len(), additional)?;
        if needed <= values.capacity() || std::mem::size_of::<T>() == 0 {
            return Ok(());
        }
        // The original locked RawVec growth: actual capacity doubles and the
        // initial allocation uses its element-size-dependent minimum.
        let minimum = if std::mem::size_of::<T>() == 1 {
            8
        } else if std::mem::size_of::<T>() <= 1024 {
            4
        } else {
            1
        };
        let capacity = values
            .capacity()
            .checked_mul(2)
            .ok_or(CompileControlError::ResourceExhausted)?
            .max(needed)
            .max(minimum);
        let layout = std::alloc::Layout::array::<T>(capacity)
            .map_err(|source| ConstantError::array_backing::<T>(capacity, source))?;
        self.request(layout, work)?;
        work.flush()?;
        let result = values.try_reserve_exact(capacity - values.len());
        result.map_err(ConstantError::allocation)?;
        self.step(work)?;
        work.flush()?;
        Ok(())
    }
    fn tree_entries(&mut self, entries: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), E> {
        if self.creator.is_some() {
            work.flush()?;
        }
        if !self.is_parent() {
            return Ok(());
        }
        use novarocks_type_contract::owned_resources::btree::{BTreeResourceError, insertion_only};
        let next = insertion_only::<(usize, usize), ()>(entries).map_err(|error| match error {
            BTreeResourceError::SourceModel(message) => ConstantError::Invalid(message),
            BTreeResourceError::Arithmetic(_) => CompileControlError::ResourceExhausted.into(),
        })?;
        self.facts.allocation_requests_upper_bound = Self::sum(
            self.facts.allocation_requests_upper_bound,
            next.allocation_requests_upper_bound - self.tree.allocation_requests_upper_bound,
        )?;
        self.facts.allocation_request_bytes_upper_bound = Self::sum(
            self.facts.allocation_request_bytes_upper_bound,
            next.request_bytes_upper_bound - self.tree.request_bytes_upper_bound,
        )?;
        self.work(next.cumulative_work_upper_bound - self.tree.cumulative_work_upper_bound)?;
        if let Some(creator) = &mut self.creator {
            creator.opaque(
                next.request_bytes_upper_bound - self.tree.request_bytes_upper_bound,
                PoolStructureClass::Temporary,
            )?;
        }
        self.tree = next;
        Ok(())
    }
}

#[derive(Debug)]
struct PoolBacking {
    field: Arc<Field>,
    value_type: FunctionValueType,
    array: ArrayRef,
    data: ArrayData,
    facts: ConstantResourceFacts,
    source: Option<ConstantSourceOwner>,
    // An owning array projection carries its own result responsibility, not
    // the PoolBacking holder or validation operation.
    array_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    // Array/data/source fields exit before their finite creator responsibility.
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

/// One immutable typed pool. IDs and IPC parsing belong to the wire codec.
#[derive(Clone, Debug)]
pub struct ConstantPool(Arc<PoolBacking>);
/// An independently retained array carrier paired with its actual creation
/// and source responsibility. It retains no complete pool, query or control.
#[derive(Clone, Debug)]
pub struct ConstantArrayProjection {
    array: ArrayRef,
    _source: Option<ConstantSourceOwner>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl ConstantArrayProjection {
    pub fn array(&self) -> &dyn arrow_array::Array {
        self.array.as_ref()
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.storage.clone()
    }
}

/// One SQL value at a checked ordinal, retaining the same immutable backing.
/// Pool layout/other rows do not form part of this value's semantics.
#[derive(Clone, Debug)]
pub struct ConstantValue {
    pool: ConstantPool,
    ordinal: u32,
}

/// Opaque process-local retention identity while the backing is retained.
/// It is neither a wire address nor selected-value equality.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConstantBackingIdentity(usize);

impl ConstantPool {
    pub fn backing_identity(&self) -> ConstantBackingIdentity {
        ConstantBackingIdentity(Arc::as_ptr(&self.0) as usize)
    }

    /// Locked Rust Arc allocation request for this owner's actual backing.
    /// This is a layout input, not allocator usable size, RSS or a MEM grant.
    /// Audited Rust 1.98.1 ArcInner is repr(C, align(2)): two atomic usize
    /// counters followed by the payload; the common layout author owns the guard.
    pub fn backing_allocation_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<PoolBacking>())
            .expect("fixed ConstantPool Arc layout is representable")
            .0
            .pad_to_align()
    }
    /// A non-Null flat semantic validation uses one Range frame, with no
    /// child expansion. Its vec! allocation requests exactly this layout.
    /// This does not authorize a stack or a general nested validation walk.
    pub fn flat_value_validation_stack_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<RequiredFrame<'static>>()
    }
    /// Inputs are immutable ArrayData, with no caller Array implementation to
    /// execute. Inputs already own Arrow allocations; this is not first-allocation
    /// admission. Standard Arrow validation runs only after observed resource
    /// preflight. Its finite opaque call remains an explicit host obligation.
    /// Actual element-metric Vec entry used by the borrowed ArrayData scanner.
    /// This layout is not an allocation request, limit or memory grant.
    pub fn scan_child_metric_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<ElementMetric>()
    }

    /// Actual caller-owned fixed scratch of the sole borrowed type grammar.
    /// This stack layout is not a heap request or an allocation grant.
    pub fn type_validation_scratch_layout() -> std::alloc::Layout {
        novarocks_type_contract::owned_resources::type_validation::scratch_layout()
    }
    /// One opaque initialization writes the complete fixed scratch backing.
    /// Byte work, rather than slot count, covers the actual original layout;
    /// Parent callers admit it before the first source observation.
    pub fn type_validation_scratch_work_upper_bound() -> usize {
        novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound()
    }

    pub fn try_new(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let checked = validate_pool_source(
            &field,
            &value_type,
            &data,
            policy,
            &mut PoolObserver::plain(),
            &mut work,
        );
        if let Err(ConstantError::Control(error)) = &checked {
            return Err(ConstantError::Control(error.clone()));
        }
        work.finish()?;
        let facts = checked?;
        control.checkpoint(phase, 0)?;
        let validated = validate_arrow_data(&data);
        control.checkpoint(phase, 0)?;
        validated?;
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let validated = canonical_pool_data(
            data,
            field.is_nullable(),
            &mut PoolObserver::plain(),
            &mut work,
        );
        if let Err(ConstantError::Control(error)) = &validated {
            return Err(ConstantError::Control(error.clone()));
        }
        work.finish()?;
        let (canonical, data) = validated?;
        Ok(Self(Arc::new(PoolBacking {
            field,
            value_type,
            array: canonical,
            data,
            facts,
            source: None,
            array_storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        })))
    }
    /// Same constant owner on the caller's meter. The caller admits original
    /// Arrow opaque requests separately and merges this owned contribution
    /// with that future bound once; entry and all tails belong to the caller.
    pub fn try_new_in(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantError> {
        let mut adapter =
            |facts: &ConstantOwnerResourceFacts| admit(facts).map_err(ConstantError::from);
        let mut observer = PoolObserver::parent(&mut adapter);
        Self::try_new_body(
            field,
            value_type,
            data,
            policy,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
            &mut observer,
            work,
        )
    }

    /// Explicit pure expansion observer. It creates no runtime responsibility;
    /// BE authors use try_new_with_creator_in with original source custody.
    pub fn try_new_pure_observed_in<H>(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        let mut adapter = |facts: &ConstantOwnerResourceFacts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        };
        let mut observer = PoolObserver::parent(&mut adapter);
        Self::try_new_body(
            field,
            value_type,
            data,
            policy,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
            &mut observer,
            work,
        )
    }

    /// A new source author with caller-supplied finite construction custody.
    /// This same body creates no grant; source formats with opaque attributes
    /// still require the distinct original ConstantSourceOwner entry.
    pub fn try_new_owned_observed_in<H>(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        let mut adapter = |facts: &ConstantOwnerResourceFacts| {
            admit(facts).map_err(ConstantCreationFailure::Creator)
        };
        let mut observer = PoolObserver::parent(&mut adapter);
        Self::try_new_body(
            field,
            value_type,
            data,
            policy,
            storage,
            None,
            &mut observer,
            work,
        )
    }

    /// Construct on the same caller's meter with independent original source
    /// and new structure responsibility. The caller owns locked Arrow operation
    /// bounds; this observer bills only this actual constant owner's requests.
    pub fn try_new_with_creator_in<H>(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        source: ConstantSourceOwner,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        if !Arc::ptr_eq(&field, source.field_ref()) {
            return Err(ConstantError::Invalid(
                "constant creator requires its original source Field",
            )
            .into());
        }
        let storage = creation.storage_lifetime().retaining_observed(
            &source.storage_lifetime(),
            |layout| {
                creation
                    .request_layout(layout)
                    .map_err(ConstantCreationFailure::Creator)
            },
        )?;
        let mut creator = PoolCreator(creation);
        let mut observer = PoolObserver::parent(admit);
        observer.creator = Some(&mut creator);
        Self::try_new_body(
            field,
            value_type,
            data,
            policy,
            storage,
            Some(source),
            &mut observer,
            work,
        )
    }

    /// The same pool author with actual temporary-validation and final-backing
    /// ports. The enclosing reader owns its canonical Arrow-array requests.
    pub fn try_new_with_paired_creator_in<H>(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        source: ConstantSourceOwner,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        array_storage: novarocks_type_contract::ValueTypeStorageLifetime,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), ConstantCreationFailure<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantCreationFailure<H>> {
        if !Arc::ptr_eq(&field, source.field_ref()) {
            return Err(ConstantError::Invalid(
                "constant creator requires its original source Field",
            )
            .into());
        }
        let storage = retained.storage_lifetime();
        let mut creator = PairedPoolCreator {
            temporary,
            retained,
        };
        let mut observer = PoolObserver::parent(admit);
        observer.creator = Some(&mut creator);
        observer.array_storage = Some(array_storage);
        // PoolBacking independently stores its source and array loans. No
        // extra lifetime wrapper is needed to couple those actual fields.
        work.flush()?;
        Self::try_new_body(
            field,
            value_type,
            data,
            policy,
            storage,
            Some(source),
            &mut observer,
            work,
        )
    }

    fn try_new_body<E: PoolFailure>(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
        source: Option<ConstantSourceOwner>,
        observer: &mut PoolObserver<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, E> {
        observer.request_classified(
            Self::backing_allocation_layout(),
            PoolStructureClass::Retained,
            work,
        )?;
        if !matches!(data.data_type(), DataType::Null) {
            observer.request(Self::flat_value_validation_stack_layout(), work)?;
            observer.required_root_prefunded = true;
        }
        if !data.child_data().is_empty() {
            observer.request(
                std::alloc::Layout::array::<ElementMetric>(data.child_data().len()).map_err(
                    |source| {
                        ConstantError::array_backing::<ElementMetric>(
                            data.child_data().len(),
                            source,
                        )
                    },
                )?,
                work,
            )?;
        }
        let facts = validate_pool_source(&field, &value_type, &data, policy, observer, work)?;
        work.flush()?;
        let validated = validate_arrow_data(&data);
        validated?;
        observer.step(work)?;
        work.flush()?;
        let (canonical, data) = canonical_pool_data(data, field.is_nullable(), observer, work)?;
        observer.gate()?;
        work.flush()?;
        let pool = Self(Arc::new(PoolBacking {
            field,
            value_type,
            array: canonical,
            data,
            facts,
            source,
            array_storage: observer
                .array_storage
                .clone()
                .unwrap_or_else(|| storage.clone()),
            storage,
        }));
        observer.step(work)?;
        Ok(pool)
    }

    pub fn field(&self) -> &Field {
        &self.0.field
    }
    /// Borrows the original immutable source handle without cloning its Field
    /// metadata or allocating a replacement source representation.
    pub fn field_ref(&self) -> &Arc<Field> {
        &self.0.field
    }
    pub fn value_type(&self) -> &FunctionValueType {
        &self.0.value_type
    }
    pub fn array(&self) -> &ArrayRef {
        &self.0.array
    }
    /// Borrowed ArrayRef access remains an expansion adapter. Owned runtime
    /// users retain this projection instead of cloning that bare Arc.
    pub fn array_projection(&self) -> ConstantArrayProjection {
        ConstantArrayProjection {
            array: self.0.array.clone(),
            _source: self.0.source.clone(),
            storage: self.0.array_storage.clone(),
        }
    }
    /// Genuine original source invoice; legacy pure owners have no receipt.
    pub fn source_retained_bytes(&self) -> Option<usize> {
        self.0
            .source
            .as_ref()
            .map(ConstantSourceOwner::original_source_bytes)
    }
    pub fn source_metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.0
            .source
            .as_ref()
            .map(ConstantSourceOwner::metadata_namespace)
    }
    pub fn source_owner(&self) -> Option<&ConstantSourceOwner> {
        self.0.source.as_ref()
    }
    pub fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.0.storage.clone()
    }
    /// Borrows the canonical data already admitted by this sole backing owner.
    /// Writers can inspect layout before an allocating Array::to_data call.
    pub fn data(&self) -> &ArrayData {
        &self.0.data
    }
    pub fn resource_facts(&self) -> ConstantResourceFacts {
        self.0.facts
    }
    /// Protocol bytes from checked buffer facts, complete field/value types,
    /// and Arrow-reported carrier/descriptor structure. Arrow does not report
    /// every opaque container allocation; this is no heap/process upper bound
    /// or MEM grant. Shared inputs may be counted by each retained pool.
    pub fn exchange_resource_bytes_observed(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<usize, ConstantError> {
        let facts = self.resource_facts();
        control.checkpoint(CompilePhase::Validate, 0)?;
        // These finite metadata traversals consume already checked type
        // graphs. Their representation sizes also cover the repeated walk's
        // structural work before entering canonical Arrow metadata methods.
        let field_bytes = novarocks_type_contract::owned_resources::type_representation::arrow_field_retained_bytes(&self.0.field)
            .ok_or(ConstantError::Limit("constant field representation byte count overflow"))?;
        let type_bytes = novarocks_type_contract::owned_resources::type_representation::function_value_type_retained_bytes(&self.0.value_type)
            .ok_or(ConstantError::Limit("constant type representation byte count overflow"))?;
        let mut units = facts
            .array_nodes
            .checked_add(facts.buffer_count)
            .and_then(|nodes| nodes.checked_mul(4))
            .and_then(|units| units.checked_add(u64::try_from(field_bytes).ok()?))
            .and_then(|units| units.checked_add(u64::try_from(type_bytes).ok()?))
            .ok_or(ConstantError::Limit(
                "constant representation work count overflow",
            ))?;
        while units != 0 {
            let batch = units.min(u64::from(u32::MAX)) as u32;
            control.checkpoint(CompilePhase::Validate, batch)?;
            units -= u64::from(batch);
        }
        let array_structure = self
            .0
            .array
            .get_array_memory_size()
            .checked_sub(self.0.array.get_buffer_memory_size());
        let data_structure = self
            .0
            .data
            .get_array_memory_size()
            .checked_sub(self.0.data.get_buffer_memory_size());
        let retained = (|| {
            let mut bytes = Self::backing_allocation_layout().size();
            let array_layout = std::alloc::Layout::for_value(self.0.array.as_ref());
            let array_arc_header =
                novarocks_type_contract::owned_resources::layout::arc_layout(array_layout)
                    .ok()?
                    .size()
                    .checked_sub(array_layout.size())?;
            for value in [
                array_arc_header,
                array_structure?,
                data_structure?,
                field_bytes,
                type_bytes,
                usize::try_from(facts.retained_buffer_capacity_bytes).ok()?,
                self.source_retained_bytes().unwrap_or(0),
            ] {
                bytes = bytes.checked_add(value)?;
            }
            Some(bytes)
        })();
        let retained = retained.ok_or(ConstantError::Limit(
            "constant retained representation byte count overflow",
        ))?;
        control.checkpoint(CompilePhase::Validate, 0)?;
        Ok(retained)
    }
    pub fn value(&self, ordinal: u32) -> Result<ConstantValue, ConstantError> {
        if u64::from(ordinal) >= self.0.facts.rows {
            return Err(ConstantError::Invalid(
                "constant ordinal is outside its pool",
            ));
        }
        Ok(ConstantValue {
            pool: self.clone(),
            ordinal,
        })
    }
}
impl ConstantValue {
    /// Selected-value diagnostics only. Text is never semantic identity or SQL source.
    pub fn format_diagnostic_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<String, ConstantError> {
        diagnostic::format(self, phase, control)
    }

    /// Stream selected text without retaining an intermediate String.
    /// An inner fmt::Error is the caller writer's refusal; its journal remains
    /// authoritative and receives no completion callback after refusal.
    pub fn write_diagnostic_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
        output: &mut dyn std::fmt::Write,
    ) -> Result<std::fmt::Result, ConstantError> {
        diagnostic::write(self, phase, control, output)
    }

    pub fn pool(&self) -> &ConstantPool {
        &self.pool
    }
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
    pub fn field(&self) -> &Field {
        self.pool.field()
    }
    pub fn value_type(&self) -> &FunctionValueType {
        self.pool.value_type()
    }
    /// Exact value comparison: complete semantic type, value layout and one value.
    /// Floating values compare bits; SQL NULL ignores unselected payload bytes.
    /// Dictionary key numbering and unused dictionary entries are not values.
    pub fn equals_observed(
        &self,
        other: &Self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<bool, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let type_equal = self
            .value_type()
            .exactly_equals_observed(other.value_type(), || {
                work.step()?;
                Ok::<_, ConstantError>(())
            })?;
        if !type_equal {
            work.finish()?;
            return Ok(false);
        }
        let equal = equal_values(
            &self.pool.0.data,
            self.ordinal as usize,
            &other.pool.0.data,
            other.ordinal as usize,
            &mut work,
        )?;
        work.finish()?;
        Ok(equal)
    }
    /// Constant NULL still retains its complete semantic binding and value layout.
    pub fn is_null_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<bool, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let null = logical_null(&self.pool.0.data, self.ordinal as usize, &mut work)?;
        work.finish()?;
        Ok(null)
    }
    /// Exact payload bytes of this selected SQL value. NULL contributes zero;
    /// nested values count only their selected child payloads. Pool padding,
    /// offsets, headers, dictionary codes and unselected rows are excluded.
    /// This is a semantic size, distinct from retained backing/resource facts.
    pub fn selected_payload_bytes_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<u64, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let bytes = self.selected_payload_bytes_in(&mut work)?;
        work.finish()?;
        Ok(bytes)
    }
    /// Read the exact selected payload using the caller's original meter.
    /// The caller owns entry and finish. This semantic byte count does not
    /// admit the traversal's temporary stack or retained Arrow resources.
    pub fn selected_payload_bytes_in(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u64, ConstantError> {
        selected_payload_bytes(
            Row {
                data: &self.pool.0.data,
                index: self.ordinal as usize,
            },
            work,
        )
    }
    /// Factories use the same checked owner. The caller supplies all type/field
    /// facts and policy; no logical label, field name or environment is guessed.
    pub fn from_scalar_array(
        field: Arc<Field>,
        value_type: FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        control.checkpoint(phase, 0)?;
        if data.len() != 1 {
            return Err(ConstantError::Invalid(
                "scalar factory requires one Arrow row",
            ));
        }
        ConstantPool::try_new(field, value_type, data, policy, phase, control)?.value(0)
    }
}

fn checked_add(a: u64, b: u64) -> Result<u64, ConstantError> {
    a.checked_add(b).ok_or(ConstantError::Limit(
        "constant resource arithmetic overflow",
    ))
}
fn checked_mul(a: u64, b: u64) -> Result<u64, ConstantError> {
    a.checked_mul(b).ok_or(ConstantError::Limit(
        "constant resource arithmetic overflow",
    ))
}
fn limit(actual: u64, maximum: u64, message: &'static str) -> Result<(), ConstantError> {
    if actual > maximum {
        Err(ConstantError::Limit(message))
    } else {
        Ok(())
    }
}
fn optional_limit(
    actual: u64,
    maximum: Option<u64>,
    message: &'static str,
) -> Result<(), ConstantError> {
    if let Some(maximum) = maximum {
        limit(actual, maximum, message)?;
    }
    Ok(())
}
fn observe_bytes_owned<E: PoolFailure>(
    bytes: &[u8],
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    for _ in bytes.chunks(1024) {
        observer.step(work)?;
    }
    Ok(())
}

fn field_bytes_owned<E: PoolFailure>(
    field: &Field,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, E> {
    observer.step(work)?;
    observe_bytes_owned(field.name().as_bytes(), observer, work)?;
    let mut bytes = field.name().len() as u64;
    for (k, v) in field.metadata() {
        observer.step(work)?;
        observe_bytes_owned(k.as_bytes(), observer, work)?;
        observe_bytes_owned(v.as_bytes(), observer, work)?;
        bytes = checked_add(bytes, checked_add(k.len() as u64, v.len() as u64)?)?;
    }
    Ok(bytes)
}
fn validate_type(
    field: &Field,
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, ConstantError> {
    validate_type_owned(field, ty, policy, &mut PoolObserver::plain(), work)
}

struct TypeResourceFacts {
    metadata: u64,
    nodes: u64,
}
fn validate_type_owned<E: PoolFailure>(
    field: &Field,
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, E> {
    validate_type_profile_owned(field, ty, Some(policy), observer, work).map(|facts| facts.metadata)
}
fn validate_type_profile_owned<E: PoolFailure>(
    field: &Field,
    ty: &FunctionValueType,
    policy: Option<ConstantPolicy>,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<TypeResourceFacts, E> {
    if observer.is_parent() {
        observer.work(ConstantPool::type_validation_scratch_work_upper_bound())?;
    }
    observer.step(work)?;
    if field.is_nullable() != ty.nullable() {
        return Err(
            ConstantError::Invalid("constant field differs from exact value nullability").into(),
        );
    }
    if !novarocks_type_contract::value_layout_matches_carrier_observed::<E>(
        ty.typed_binding().layout(),
        field.data_type(),
        |units| {
            for _ in 0..units {
                observer.step(work)?;
            }
            Ok(())
        },
    )? {
        return Err(
            ConstantError::Invalid("constant field differs from exact value carrier").into(),
        );
    }
    let mut bytes = field_bytes_owned(field, observer, work)?;
    let mut nodes = 0u64;
    let parent = observer.is_parent();
    let mut scratch = if parent {
        work.flush()?;
        // Fixed stack initialization is opaque; its actual layout and full
        // work were admitted before the first source-validation callback.
        let scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
        observer.step(work)?;
        work.flush()?;
        Some(scratch)
    } else {
        None
    };
    let mut observe = |visit| -> Result<(), E> {
        observer.step(work)?;
        match visit {
            ValueTypeVisit::TypeNode(t) => {
                validate_arrow_carrier_parameters_observed::<E>(t, || observer.step(work))?;
                nodes = checked_add(nodes, 1)?;
                optional_limit(
                    nodes,
                    policy.map(|p| p.max_type_nodes),
                    "constant type node limit exceeded",
                )?;
                if let DataType::Timestamp(_, Some(zone)) = t {
                    observe_bytes_owned(zone.as_bytes(), observer, work)?;
                    bytes = checked_add(bytes, zone.len() as u64)?;
                }
            }
            ValueTypeVisit::Field(f) => {
                bytes = checked_add(bytes, field_bytes_owned(f, observer, work)?)?;
            }
            ValueTypeVisit::ChildEdge(_) => {}
        }
        optional_limit(
            bytes,
            policy.map(|p| p.max_metadata_bytes),
            "constant metadata byte limit exceeded",
        )?;
        Ok(())
    };
    if parent {
        novarocks_type_contract::validate_arrow_layout_structure_with_scratch_observed::<E>(
            field.data_type(),
            scratch.as_mut().expect("parent scratch is initialized"),
            &mut observe,
        )?;
    } else {
        novarocks_type_contract::validate_arrow_layout_structure_observed::<E>(
            field.data_type(),
            &mut observe,
        )?;
    }
    Ok(TypeResourceFacts {
        metadata: bytes,
        nodes,
    })
}

/// The flat IPC path retains at most body, alignment repair and empty offsets.
/// Keep those allocation identities inline; arbitrary ArrayData may have more
/// independent owners and spills to the same ordered set on the fourth one.
#[derive(Default)]
struct AllocationSet {
    inline: [Option<(usize, usize)>; 3],
    spill: BTreeSet<(usize, usize)>,
}
impl AllocationSet {
    fn prefund_owned<E: PoolFailure>(
        &self,
        allocation: (usize, usize),
        observer: &mut PoolObserver<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        if !observer.is_parent() {
            return Ok(());
        }
        if !self.spill.is_empty() {
            observer.tree_entries(PoolObserver::<E>::sum(self.spill.len(), 1)?, work)?;
        } else if !self.inline.contains(&Some(allocation))
            && self.inline.iter().all(Option::is_some)
        {
            observer.tree_entries(4, work)?;
        }
        Ok(())
    }
    fn insert_owned<E: PoolFailure>(
        &mut self,
        allocation: (usize, usize),
        observer: &mut PoolObserver<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, E> {
        if !observer.is_parent() {
            return Ok(self.insert(allocation));
        }
        self.prefund_owned(allocation, observer, work)?;
        // At most the first three inline entries migrate on this insertion.
        // This finite original std BTree operation is opaque, not a promise
        // of cooperative callbacks inside the library.
        work.flush()?;
        let inserted = self.insert(allocation);
        observer.step(work)?;
        work.flush()?;
        Ok(inserted)
    }
    fn insert(&mut self, allocation: (usize, usize)) -> bool {
        if !self.spill.is_empty() {
            return self.spill.insert(allocation);
        }
        if self.inline.contains(&Some(allocation)) {
            return false;
        }
        if let Some(slot) = self.inline.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(allocation);
            return true;
        }
        for old in self.inline.iter().flatten() {
            self.spill.insert(*old);
        }
        self.spill.insert(allocation)
    }
}

#[derive(Default)]
struct ScanFacts {
    nodes: u64,
    storage_elements: u64,
    buffer_visits: u64,
    buffer_count: u64,
    view_validation_bytes: u64,
    utf8_fallback_validation_bytes: u64,
    retained: u64,
    masks: u64,
    depth: u64,
    allocations: AllocationSet,
}

#[derive(Clone, Copy)]
struct ValidationCounts {
    nodes: u64,
    storage_elements: u64,
    buffer_count: u64,
    buffer_visits: u64,
    view_validation_bytes: u64,
    utf8_fallback_validation_bytes: u64,
    masks: u64,
    depth: u64,
}
impl From<&ScanFacts> for ValidationCounts {
    fn from(scanned: &ScanFacts) -> Self {
        Self {
            nodes: scanned.nodes,
            storage_elements: scanned.storage_elements,
            buffer_count: scanned.buffer_count,
            buffer_visits: scanned.buffer_visits,
            view_validation_bytes: scanned.view_validation_bytes,
            utf8_fallback_validation_bytes: scanned.utf8_fallback_validation_bytes,
            masks: scanned.masks,
            depth: scanned.depth,
        }
    }
}
struct ValidationEnvelope {
    work: u64,
    temporary: u64,
    bytes: u64,
}
fn validation_envelope(
    scanned: ValidationCounts,
    metadata: u64,
    policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValidationEnvelope, ConstantError> {
    let result = validation_envelope_core(scanned, metadata, policy);
    work.step()?;
    result
}
fn validation_envelope_owned<E: PoolFailure>(
    scanned: ValidationCounts,
    metadata: u64,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValidationEnvelope, E> {
    if !observer.is_parent() {
        return validation_envelope(scanned, metadata, policy, work).map_err(Into::into);
    }
    let envelope = validation_envelope_core_policy(scanned, metadata, policy, true)?;
    observer.work(
        usize::try_from(envelope.work).map_err(|_| CompileControlError::ResourceExhausted)?,
    )?;
    observer.step(work)?;
    Ok(envelope)
}

fn validation_envelope_profile_owned<E: PoolFailure>(
    scanned: ValidationCounts,
    metadata: u64,
    policy: Option<ConstantPolicy>,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ValidationEnvelope, E> {
    if let Some(policy) = policy {
        return validation_envelope_owned(scanned, metadata, policy, observer, work);
    }
    let envelope = validation_envelope_profile_core(scanned, metadata, None, observer.is_parent())?;
    if observer.is_parent() {
        observer.work(
            usize::try_from(envelope.work).map_err(|_| CompileControlError::ResourceExhausted)?,
        )?;
        observer.step(work)?;
    } else {
        work.step()?;
    }
    Ok(envelope)
}

fn validation_envelope_core(
    scanned: ValidationCounts,
    metadata: u64,
    policy: ConstantPolicy,
) -> Result<ValidationEnvelope, ConstantError> {
    validation_envelope_core_policy(scanned, metadata, policy, false)
}
fn validation_envelope_core_policy(
    scanned: ValidationCounts,
    metadata: u64,
    policy: ConstantPolicy,
    parent: bool,
) -> Result<ValidationEnvelope, ConstantError> {
    validation_envelope_profile_core(scanned, metadata, Some(policy), parent)
}
fn validation_envelope_profile_core(
    scanned: ValidationCounts,
    metadata: u64,
    policy: Option<ConstantPolicy>,
    parent: bool,
) -> Result<ValidationEnvelope, ConstantError> {
    let add = |a: u64, b: u64| {
        if parent {
            a.checked_add(b)
                .ok_or_else(|| ConstantError::from(CompileControlError::ResourceExhausted))
        } else {
            checked_add(a, b)
        }
    };
    let mul = |a: u64, b: u64| {
        if parent {
            a.checked_mul(b)
                .ok_or_else(|| ConstantError::from(CompileControlError::ResourceExhausted))
        } else {
            checked_mul(a, b)
        }
    };
    // This is the sole numerical author for both actual ArrayData facts and
    // raw pre-reader upper bounds. It does not model reader allocations.
    let structural = mul(mul(scanned.nodes, scanned.nodes)?, scanned.depth)?;
    let rows = mul(
        add(scanned.storage_elements, scanned.buffer_count)?,
        scanned.depth,
    )?;
    // Arrow first scans the whole UTF8 values descriptor. An invalid unused
    // suffix triggers a successful scan of all monotone referenced ranges,
    // totaling at most one more complete descriptor, including NULL rows.
    let inspected = add(
        add(scanned.buffer_visits, scanned.view_validation_bytes)?,
        scanned.utf8_fallback_validation_bytes,
    )?;
    let bytes = mul(add(inspected, metadata)?, scanned.depth)?;
    let library_work = add(add(structural, rows)?, add(bytes, scanned.masks)?)?;
    let headers = add(
        mul(scanned.nodes, std::mem::size_of::<ArrayData>() as u64)?,
        mul(
            scanned.buffer_count,
            std::mem::size_of::<arrow_buffer::Buffer>() as u64,
        )?,
    )?;
    let diagnostics = mul(
        mul(add(metadata, mul(scanned.nodes, 256)?)?, scanned.depth)?,
        16,
    )?;
    let temporary = add(scanned.masks, add(headers, diagnostics)?)?;
    let library_bytes = add(inspected, temporary)?;
    optional_limit(
        library_work,
        policy.map(|p| p.max_library_validation_work),
        "opaque Arrow validation work limit exceeded",
    )?;
    optional_limit(
        library_bytes,
        policy.map(|p| p.max_library_validation_bytes),
        "opaque Arrow validation byte limit exceeded",
    )?;
    Ok(ValidationEnvelope {
        work: library_work,
        temporary,
        bytes: library_bytes,
    })
}
fn validate_pool_source<E: PoolFailure>(
    field: &Field,
    value_type: &FunctionValueType,
    data: &ArrayData,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantResourceFacts, E> {
    let metadata_bytes = validate_type_owned(field, value_type, policy, observer, work)?;
    if !arrow_data_types_exact_borrowed_observed::<E>(field.data_type(), data.data_type(), || {
        observer.step(work)
    })? {
        return Err(
            ConstantError::Invalid("constant ArrayData differs from exact field carrier").into(),
        );
    }
    preflight_owned(data, metadata_bytes, policy, observer, work)
}
fn validate_arrow_data(data: &ArrayData) -> Result<(), ConstantError> {
    data.validate_full()
        .map_err(ConstantError::arrow_validation)
}
fn canonical_pool_data<E: PoolFailure>(
    data: ArrayData,
    nullable: bool,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(ArrayRef, ArrayData), E> {
    if observer.is_parent() {
        work.flush()?;
    }
    let canonical = make_array(data);
    if observer.is_parent() {
        observer.step(work)?;
        work.flush()?;
    }
    // Standard Arrow canonicalization applies parent offsets to Struct and
    // FixedSizeList children. All semantic reads use that same backing.
    let data = canonical.to_data();
    if observer.is_parent() {
        observer.step(work)?;
        work.flush()?;
    }
    validate_values_owned(&data, nullable, observer, work)?;
    Ok((canonical, data))
}

fn preflight_owned<E: PoolFailure>(
    data: &ArrayData,
    metadata: u64,
    policy: ConstantPolicy,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantResourceFacts, E> {
    limit(
        data.len() as u64,
        policy.max_rows,
        "constant row limit exceeded",
    )?;
    let mut scanned = ScanFacts::default();
    let max_value = scan_data(data, 1, 0, policy, &mut scanned, observer, work)?;
    let logical = logical_elements_observed_owned(
        data.len() as u64,
        scanned.storage_elements,
        max_value,
        observer,
        work,
    )?;
    limit(
        logical,
        policy.max_logical_elements,
        "constant logical element limit exceeded",
    )?;
    // validate_full calls validate_data at every node; validate itself revisits
    // descendants and exact child types. Include repeated ancestor/fanout work,
    // bytes inspected and FixedSizeList's expanded parent-null masks.
    let envelope = validation_envelope_owned(
        ValidationCounts::from(&scanned),
        metadata,
        policy,
        observer,
        work,
    )?;
    Ok(ConstantResourceFacts {
        rows: data.len() as u64,
        array_nodes: scanned.nodes,
        buffer_count: scanned.buffer_count,
        logical_elements_upper_bound: logical,
        retained_buffer_capacity_bytes: scanned.retained,
        metadata_bytes: metadata,
        library_validation_work_upper_bound: envelope.work,
        library_validation_temporary_bytes_upper_bound: envelope.temporary,
        library_validation_bytes_upper_bound: envelope.bytes,
    })
}
fn scan_data<E: PoolFailure>(
    data: &ArrayData,
    depth: u32,
    dict_depth: u32,
    policy: ConstantPolicy,
    facts: &mut ScanFacts,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, E> {
    observer.step(work)?;
    limit(
        u64::from(depth),
        u64::from(policy.max_type_depth),
        "constant array depth limit exceeded",
    )?;
    limit(
        u64::from(depth),
        novarocks_type_contract::MAX_VALUE_TYPE_DEPTH as u64,
        "constant array exceeds intrinsic Arrow type depth",
    )?;
    validate_arrow_carrier_parameters_observed::<E>(data.data_type(), || observer.step(work))?;
    let expected_children = match data.data_type() {
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
    };
    if data.child_data().len() != expected_children {
        return Err(
            ConstantError::Invalid("constant ArrayData child count differs from carrier").into(),
        );
    }
    let extent = checked_add(data.offset() as u64, data.len() as u64)?;
    match data.data_type() {
        DataType::Struct(_) => {
            for child in data.child_data() {
                observer.step(work)?;
                if extent > child.len() as u64 {
                    return Err(ConstantError::Invalid(
                        "constant Struct offset exceeds child extent",
                    )
                    .into());
                }
            }
        }
        DataType::FixedSizeList(_, width)
            if checked_mul(extent, *width as u64)? > data.child_data()[0].len() as u64 =>
        {
            return Err(ConstantError::Invalid(
                "constant FixedSizeList offset exceeds child extent",
            )
            .into());
        }
        _ => {}
    }
    if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        let views = data
            .buffers()
            .first()
            .ok_or(ConstantError::Invalid("constant view lacks its view table"))?
            .as_slice();
        let end = checked_mul(extent, 16)?;
        if end > views.len() as u64 {
            return Err(ConstantError::Invalid("constant view table is too short").into());
        }
        for index in data.offset()
            ..usize::try_from(extent)
                .map_err(|_| ConstantError::Invalid("constant view extent overflows usize"))?
        {
            observer.step(work)?;
            let start = index * 16;
            let length = u128::from_ne_bytes(
                views[start..start + 16]
                    .try_into()
                    .expect("checked view extent"),
            ) as u32;
            // String validation scans each reference, including repeated views
            // into one backing allocation. No unvalidated buffer index is read.
            facts.view_validation_bytes =
                checked_add(facts.view_validation_bytes, u64::from(length))?;
        }
    }
    if matches!(data.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        let values = data.buffers().get(1).ok_or(ConstantError::Invalid(
            "constant UTF8 lacks its values buffer",
        ))?;
        facts.utf8_fallback_validation_bytes =
            checked_add(facts.utf8_fallback_validation_bytes, values.len() as u64)?;
    }
    let dict_depth = dict_depth + u32::from(matches!(data.data_type(), DataType::Dictionary(_, _)));
    limit(
        u64::from(dict_depth),
        u64::from(policy.max_dictionary_depth),
        "constant dictionary depth limit exceeded",
    )?;
    facts.nodes = checked_add(facts.nodes, 1)?;
    facts.depth = facts.depth.max(u64::from(depth));
    limit(
        facts.nodes,
        policy.max_array_nodes,
        "constant array node limit exceeded",
    )?;
    facts.storage_elements = checked_add(facts.storage_elements, data.len() as u64)?;
    limit(
        facts.storage_elements,
        policy.max_logical_elements,
        "constant stored element limit exceeded",
    )?;
    for buffer in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|n| n.buffer()))
    {
        let capacity = buffer.capacity().max(buffer.len());
        facts.allocations.prefund_owned(
            (buffer.data_ptr().as_ptr() as usize, capacity),
            observer,
            work,
        )?;
        observer.step(work)?;
        facts.buffer_count = checked_add(facts.buffer_count, 1)?;
        if facts.allocations.insert_owned(
            (buffer.data_ptr().as_ptr() as usize, capacity),
            observer,
            work,
        )? {
            facts.retained = checked_add(facts.retained, capacity as u64)?;
            limit(
                facts.retained,
                policy.max_retained_buffer_bytes,
                "constant retained buffer limit exceeded",
            )?;
        }
        facts.buffer_visits = checked_add(facts.buffer_visits, buffer.len() as u64)?;
        observe_bytes_owned(buffer.as_slice(), observer, work)?;
    }
    let mut children = Vec::new();
    if observer.is_parent() && !data.child_data().is_empty() {
        work.flush()?;
        children
            .try_reserve_exact(data.child_data().len())
            .map_err(ConstantError::allocation)?;
        observer.step(work)?;
        work.flush()?;
    }
    for child in data.child_data() {
        observer.reserve(&mut children, 1, work)?;
        children.push(scan_child(
            child,
            depth + 1,
            dict_depth,
            policy,
            facts,
            observer,
            work,
        )?);
    }
    if let DataType::FixedSizeList(field, width) = data.data_type() {
        let width = u64::try_from(*width)
            .map_err(|_| ConstantError::Invalid("negative fixed-size list width"))?;
        if !field.is_nullable() && data.nulls().is_some() {
            let bits = checked_mul(data.len() as u64, width)?;
            facts.masks = checked_add(facts.masks, checked_add(bits, 7)? / 8 + 64)?;
        }
    }
    let elements = value_elements::<E>(data.data_type(), children.into_iter().map(Ok));
    observer.step(work)?;
    elements
}

fn scan_child<E: PoolFailure>(
    child: &ArrayData,
    depth: u32,
    dict_depth: u32,
    policy: ConstantPolicy,
    facts: &mut ScanFacts,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ElementMetric, E> {
    // The original loop has captured this child's complete header. Its metric
    // Vec request is known before the capture's completed observation; the
    // recursive node entry must not charge the same request a second time.
    if observer.is_parent() && !child.child_data().is_empty() {
        observer.request(
            std::alloc::Layout::array::<ElementMetric>(child.child_data().len()).map_err(
                |source| {
                    ConstantError::array_backing::<ElementMetric>(child.child_data().len(), source)
                },
            )?,
            work,
        )?;
    }
    observer.step(work)?;
    Ok(ElementMetric {
        len: child.len() as u64,
        max_value: scan_data(child, depth, dict_depth, policy, facts, observer, work)?,
    })
}

fn is_flat_resource_carrier(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Null
            | DataType::Boolean
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView
    ) || ty.primitive_width().is_some()
}

#[derive(Clone, Copy)]
struct ElementMetric {
    len: u64,
    max_value: u64,
}

// The child length is the complete stored extent, including unused list
// prefix/suffix and NULL payload. This is the original conservative metric.
fn value_elements<E: PoolFailure>(
    ty: &DataType,
    mut children: impl Iterator<Item = Result<ElementMetric, E>>,
) -> Result<u64, E> {
    let child =
        |children: &mut dyn Iterator<Item = Result<ElementMetric, E>>| -> Result<ElementMetric, E> {
            children.next().ok_or(ConstantError::Invalid(
                "constant array lacks an expected child",
            ))?
        };
    match ty {
        DataType::Struct(_) => {
            children.try_fold(1, |n, c| Ok::<_, E>(checked_add(n, c?.max_value)?))
        }
        DataType::FixedSizeList(_, width) => {
            let width = u64::try_from(*width)
                .map_err(|_| ConstantError::Invalid("negative fixed-size list width"))?;
            checked_add(1, checked_mul(width, child(&mut children)?.max_value)?).map_err(Into::into)
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => {
            let c = child(&mut children)?;
            checked_add(1, checked_mul(c.len, c.max_value)?).map_err(Into::into)
        }
        DataType::Dictionary(_, _) => {
            checked_add(1, child(&mut children)?.max_value).map_err(Into::into)
        }
        DataType::RunEndEncoded(_, _) => {
            child(&mut children)?;
            checked_add(1, child(&mut children)?.max_value).map_err(Into::into)
        }
        DataType::Union(_, _) => {
            let max_value = children.try_fold(0, |n, c| Ok::<_, E>(n.max(c?.max_value)))?;
            checked_add(1, max_value).map_err(Into::into)
        }
        _ => Ok(1),
    }
}

fn logical_elements_observed_owned<E: PoolFailure>(
    rows: u64,
    storage_elements: u64,
    max_value: u64,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, E> {
    let result = checked_mul(rows, max_value).map(|expanded| storage_elements.max(expanded));
    observer.step(work)?;
    Ok(result?)
}

// All row access below occurs after standard Arrow validation. It uses the
// existing Arrow type vocabulary, without another serialized type language.
#[derive(Clone, Copy)]
struct Row<'a> {
    data: &'a ArrayData,
    index: usize,
}
fn dictionary_index(data: &ArrayData, index: usize) -> Result<usize, ConstantError> {
    let DataType::Dictionary(key, _) = data.data_type() else {
        return Err(ConstantError::Invalid("expected dictionary"));
    };
    macro_rules! key {
        ($t:ty) => {
            usize::try_from(data.buffer::<$t>(0)[index])
                .map_err(|_| ConstantError::Invalid("invalid dictionary index"))
        };
    }
    match key.as_ref() {
        DataType::Int8 => key!(i8),
        DataType::Int16 => key!(i16),
        DataType::Int32 => key!(i32),
        DataType::Int64 => key!(i64),
        DataType::UInt8 => key!(u8),
        DataType::UInt16 => key!(u16),
        DataType::UInt32 => key!(u32),
        DataType::UInt64 => key!(u64),
        _ => Err(ConstantError::Invalid("invalid dictionary key type")),
    }
}
fn run_index(
    data: &ArrayData,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, ConstantError> {
    run_index_owned(data, index, &mut PoolObserver::plain(), work)
}

fn run_index_owned<E: PoolFailure>(
    data: &ArrayData,
    index: usize,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, E> {
    let ends = &data.child_data()[0];
    let logical = data.offset() + index;
    let mut low = 0usize;
    let mut high = ends.len();
    while low < high {
        observer.step(work)?;
        let mid = low + (high - low) / 2;
        let end = match ends.data_type() {
            DataType::Int16 => ends.buffer::<i16>(0)[mid] as i64,
            DataType::Int32 => ends.buffer::<i32>(0)[mid] as i64,
            DataType::Int64 => ends.buffer::<i64>(0)[mid],
            _ => return Err(ConstantError::Invalid("invalid run-end type").into()),
        };
        if u64::try_from(end).map_err(|_| ConstantError::Invalid("invalid run end"))?
            > logical as u64
        {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    if low >= data.child_data()[1].len() {
        return Err(ConstantError::Invalid("run does not cover constant ordinal").into());
    }
    Ok(low)
}
fn union_row<'a>(
    data: &'a ArrayData,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(i8, Row<'a>, bool), ConstantError> {
    union_row_owned(data, index, &mut PoolObserver::plain(), work)
}

fn union_row_owned<'a, E: PoolFailure>(
    data: &'a ArrayData,
    index: usize,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(i8, Row<'a>, bool), E> {
    let DataType::Union(fields, mode) = data.data_type() else {
        return Err(ConstantError::Invalid("expected union").into());
    };
    let id = data.buffer::<i8>(0)[index];
    let mut selected = None;
    for (index, (tag, field)) in fields.iter().enumerate() {
        observer.step(work)?;
        if tag == id {
            selected = Some((index, field.is_nullable()));
            break;
        }
    }
    let (child, nullable) = selected.ok_or(ConstantError::Invalid("unknown union tag"))?;
    let index = if *mode == UnionMode::Dense {
        usize::try_from(data.buffer::<i32>(1)[index])
            .map_err(|_| ConstantError::Invalid("negative union offset"))?
    } else {
        data.offset() + index
    };
    let data = &data.child_data()[child];
    if index >= data.len() {
        return Err(ConstantError::Invalid("union offset exceeds child").into());
    }
    Ok((id, Row { data, index }, nullable))
}
fn resolve_row<'a>(
    mut row: Row<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Row<'a>>, ConstantError> {
    loop {
        work.step()?;
        if row.index >= row.data.len() {
            return Err(ConstantError::Invalid(
                "constant child row is outside its array",
            ));
        }
        if matches!(row.data.data_type(), DataType::Null) || row.data.is_null(row.index) {
            return Ok(None);
        }
        match row.data.data_type() {
            DataType::Dictionary(_, _) => {
                row = Row {
                    data: &row.data.child_data()[0],
                    index: dictionary_index(row.data, row.index)?,
                }
            }
            DataType::RunEndEncoded(_, _) => {
                row = Row {
                    data: &row.data.child_data()[1],
                    index: run_index(row.data, row.index, work)?,
                }
            }
            _ => return Ok(Some(row)),
        }
    }
}
fn logical_null(
    data: &ArrayData,
    index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ConstantError> {
    let Some(row) = resolve_row(Row { data, index }, work)? else {
        return Ok(true);
    };
    if matches!(row.data.data_type(), DataType::Union(_, _)) {
        let (_, child, _) = union_row(row.data, row.index, work)?;
        logical_null(child.data, child.index, work)
    } else {
        Ok(false)
    }
}
fn list_range(row: Row<'_>) -> Result<(usize, usize), ConstantError> {
    let bounds = match row.data.data_type() {
        DataType::List(_) | DataType::Map(_, _) => {
            let b = row.data.buffer::<i32>(0);
            (i64::from(b[row.index]), i64::from(b[row.index + 1]))
        }
        DataType::LargeList(_) => {
            let b = row.data.buffer::<i64>(0);
            (b[row.index], b[row.index + 1])
        }
        DataType::ListView(_) => {
            let offset = row.data.buffer::<i32>(0)[row.index] as i64;
            let size = row.data.buffer::<i32>(1)[row.index] as i64;
            (offset, offset + size)
        }
        DataType::LargeListView(_) => {
            let offset = row.data.buffer::<i64>(0)[row.index];
            let size = row.data.buffer::<i64>(1)[row.index];
            (
                offset,
                offset
                    .checked_add(size)
                    .ok_or(ConstantError::Invalid("list view range overflow"))?,
            )
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width)
                .map_err(|_| ConstantError::Invalid("negative list width"))?;
            let start = (row.data.offset() + row.index)
                .checked_mul(width)
                .ok_or(ConstantError::Invalid("list range overflow"))?;
            let end = start
                .checked_add(width)
                .ok_or(ConstantError::Invalid("list range overflow"))?;
            return Ok((start, end));
        }
        _ => return Err(ConstantError::Invalid("expected list or map")),
    };
    let start =
        usize::try_from(bounds.0).map_err(|_| ConstantError::Invalid("negative list offset"))?;
    let end =
        usize::try_from(bounds.1).map_err(|_| ConstantError::Invalid("negative list offset"))?;
    if end < start || end > row.data.child_data()[0].len() {
        return Err(ConstantError::Invalid("invalid list range"));
    }
    Ok((start, end))
}

enum RequiredFrame<'a> {
    Row(Row<'a>, bool),
    Range(&'a ArrayData, usize, usize, bool),
}
fn validate_values_owned<E: PoolFailure>(
    data: &ArrayData,
    nullable: bool,
    observer: &mut PoolObserver<'_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    // Compact Null retains its logical size, with no per-row allocation/walk.
    if matches!(data.data_type(), DataType::Null) {
        return if nullable || data.is_empty() {
            Ok(())
        } else {
            Err(ConstantError::Invalid("non-null constant contains SQL NULL").into())
        };
    }
    let mut pending = if observer.is_parent() {
        let mut values = Vec::new();
        if !observer.required_root_prefunded {
            observer.request(ConstantPool::flat_value_validation_stack_layout(), work)?;
        }
        work.flush()?;
        values
            .try_reserve_exact(1)
            .map_err(ConstantError::allocation)?;
        observer.step(work)?;
        work.flush()?;
        values.push(RequiredFrame::Range(data, 0, data.len(), !nullable));
        values
    } else {
        vec![RequiredFrame::Range(data, 0, data.len(), !nullable)]
    };
    while let Some(frame) = pending.pop() {
        if observer.is_parent() {
            // A captured, validated row already fixes this original frame's
            // push fanout. Admit/reserve that actual Vec growth before the
            // pop's completed observation, rather than after a late callback.
            let (row, continuation) = match &frame {
                RequiredFrame::Range(data, start, end, _)
                    if start != end && !matches!(data.data_type(), DataType::Null) =>
                {
                    (
                        Some(Row {
                            data,
                            index: *start,
                        }),
                        1,
                    )
                }
                RequiredFrame::Row(row, _) => (Some(*row), 0),
                _ => (None, 0),
            };
            let children = row
                .filter(|row| {
                    !matches!(row.data.data_type(), DataType::Null) && !row.data.is_null(row.index)
                })
                .map_or(0, |row| match row.data.data_type() {
                    DataType::Struct(fields) => fields.len(),
                    DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Map(_, _)
                    | DataType::Union(_, _) => 1,
                    _ => 0,
                });
            observer.reserve(
                &mut pending,
                PoolObserver::<E>::sum(continuation, children)?,
                work,
            )?;
        }
        observer.step(work)?;
        let (row, required) = match frame {
            RequiredFrame::Range(data, start, end, required) => {
                if start == end {
                    continue;
                }
                if matches!(data.data_type(), DataType::Null) {
                    if required {
                        return Err(
                            ConstantError::Invalid("non-null child contains SQL NULL").into()
                        );
                    }
                    continue;
                }
                observer.reserve(&mut pending, 1, work)?;
                pending.push(RequiredFrame::Range(data, start + 1, end, required));
                (Row { data, index: start }, required)
            }
            RequiredFrame::Row(row, required) => (row, required),
        };
        if matches!(row.data.data_type(), DataType::Null) || row.data.is_null(row.index) {
            if required {
                return Err(ConstantError::Invalid("non-null constant contains SQL NULL").into());
            }
            continue;
        }
        // Encoded channels retain their own Field nullability obligations;
        // nullable outer values cannot erase a non-null selected child.
        match row.data.data_type() {
            DataType::Dictionary(_, _) => {
                observer.reserve(&mut pending, 1, work)?;
                pending.push(RequiredFrame::Row(
                    Row {
                        data: &row.data.child_data()[0],
                        index: dictionary_index(row.data, row.index)?,
                    },
                    required,
                ));
                continue;
            }
            DataType::RunEndEncoded(_, values) => {
                observer.reserve(&mut pending, 1, work)?;
                pending.push(RequiredFrame::Row(
                    Row {
                        data: &row.data.child_data()[1],
                        index: run_index_owned(row.data, row.index, observer, work)?,
                    },
                    required || !values.is_nullable(),
                ));
                continue;
            }
            _ => {}
        }
        validate_decimal_value(row)?;
        match row.data.data_type() {
            DataType::Struct(fields) => {
                observer.reserve(&mut pending, fields.len(), work)?;
                for (field, child) in fields.iter().zip(row.data.child_data()).rev() {
                    observer.step(work)?;
                    observer.reserve(&mut pending, 1, work)?;
                    pending.push(RequiredFrame::Row(
                        Row {
                            data: child,
                            index: row.index,
                        },
                        !field.is_nullable(),
                    ));
                }
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => {
                let (start, end) = list_range(row)?;
                observer.reserve(&mut pending, 1, work)?;
                pending.push(RequiredFrame::Range(
                    &row.data.child_data()[0],
                    start,
                    end,
                    !field.is_nullable(),
                ));
            }
            DataType::Union(_, _) => {
                let (_, child, nullable) = union_row_owned(row.data, row.index, observer, work)?;
                observer.reserve(&mut pending, 1, work)?;
                pending.push(RequiredFrame::Row(child, required || !nullable));
            }
            _ => {}
        }
    }
    Ok(())
}

enum PayloadFrame<'a> {
    Row(Row<'a>),
    Range(&'a ArrayData, usize, usize),
    Fields(&'a ArrayData, usize, usize),
}
fn selected_payload_bytes(
    row: Row<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, ConstantError> {
    let mut total = 0;
    let mut pending = vec![PayloadFrame::Row(row)];
    while let Some(frame) = pending.pop() {
        work.step()?;
        let row = match frame {
            PayloadFrame::Row(row) => row,
            PayloadFrame::Range(data, start, end) => {
                if start == end || matches!(data.data_type(), DataType::Null) {
                    continue;
                }
                pending.push(PayloadFrame::Range(data, start + 1, end));
                Row { data, index: start }
            }
            PayloadFrame::Fields(data, row, field) => {
                if field == data.child_data().len() {
                    continue;
                }
                pending.push(PayloadFrame::Fields(data, row, field + 1));
                Row {
                    data: &data.child_data()[field],
                    index: row,
                }
            }
        };
        let Some(row) = resolve_row(row, work)? else {
            continue;
        };
        if let Some(width) = row.data.data_type().primitive_width() {
            total = checked_add(total, width as u64)?;
            continue;
        }
        match row.data.data_type() {
            DataType::Boolean => total = checked_add(total, 1)?,
            DataType::FixedSizeBinary(width) => {
                total = checked_add(
                    total,
                    u64::try_from(*width)
                        .map_err(|_| ConstantError::Invalid("negative binary width"))?,
                )?;
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView => {
                total = checked_add(total, variable_bytes(row)?.len() as u64)?
            }
            DataType::Struct(_) => pending.push(PayloadFrame::Fields(row.data, row.index, 0)),
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _) => {
                let (start, end) = list_range(row)?;
                pending.push(PayloadFrame::Range(&row.data.child_data()[0], start, end));
            }
            DataType::Union(_, _) => {
                let (_, child, _) = union_row(row.data, row.index, work)?;
                pending.push(PayloadFrame::Row(child));
            }
            _ => {
                return Err(ConstantError::Invalid(
                    "unsupported selected constant payload carrier",
                ));
            }
        }
    }
    Ok(total)
}

enum EqualFrame<'a> {
    Row(Row<'a>, Row<'a>),
    Range(&'a ArrayData, usize, &'a ArrayData, usize, usize),
}
fn equal_values(
    left: &ArrayData,
    li: usize,
    right: &ArrayData,
    ri: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ConstantError> {
    let mut pending = vec![EqualFrame::Row(
        Row {
            data: left,
            index: li,
        },
        Row {
            data: right,
            index: ri,
        },
    )];
    while let Some(frame) = pending.pop() {
        work.step()?;
        let (left, right) = match frame {
            EqualFrame::Row(left, right) => (left, right),
            EqualFrame::Range(left, li, right, ri, count) => {
                if count == 0 {
                    continue;
                }
                if matches!(left.data_type(), DataType::Null)
                    && matches!(right.data_type(), DataType::Null)
                {
                    continue;
                }
                pending.push(EqualFrame::Range(left, li + 1, right, ri + 1, count - 1));
                (
                    Row {
                        data: left,
                        index: li,
                    },
                    Row {
                        data: right,
                        index: ri,
                    },
                )
            }
        };
        let ln = logical_null(left.data, left.index, work)?;
        let rn = logical_null(right.data, right.index, work)?;
        if ln || rn {
            if ln != rn {
                return Ok(false);
            }
            continue;
        }
        let left = resolve_row(left, work)?
            .ok_or(ConstantError::Invalid("inconsistent left constant NULL"))?;
        let right = resolve_row(right, work)?
            .ok_or(ConstantError::Invalid("inconsistent right constant NULL"))?;
        if let Some(width) = left.data.data_type().primitive_width() {
            if !bytes_equal(
                primitive_bytes(left, width)?,
                primitive_bytes(right, width)?,
                work,
            )? {
                return Ok(false);
            }
            continue;
        }
        match left.data.data_type() {
            DataType::Boolean => {
                let l = arrow_buffer::bit_util::get_bit(
                    left.data.buffers()[0].as_slice(),
                    left.data.offset() + left.index,
                );
                let r = arrow_buffer::bit_util::get_bit(
                    right.data.buffers()[0].as_slice(),
                    right.data.offset() + right.index,
                );
                if l != r {
                    return Ok(false);
                }
            }
            DataType::FixedSizeBinary(width) => {
                let width = usize::try_from(*width)
                    .map_err(|_| ConstantError::Invalid("negative binary width"))?;
                if !bytes_equal(
                    primitive_bytes(left, width)?,
                    primitive_bytes(right, width)?,
                    work,
                )? {
                    return Ok(false);
                }
            }
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView => {
                if !bytes_equal(variable_bytes(left)?, variable_bytes(right)?, work)? {
                    return Ok(false);
                }
            }
            DataType::Struct(fields) => {
                for i in (0..fields.len()).rev() {
                    work.step()?;
                    pending.push(EqualFrame::Row(
                        Row {
                            data: &left.data.child_data()[i],
                            index: left.index,
                        },
                        Row {
                            data: &right.data.child_data()[i],
                            index: right.index,
                        },
                    ));
                }
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _) => {
                let (ls, le) = list_range(left)?;
                let (rs, re) = list_range(right)?;
                if le - ls != re - rs {
                    return Ok(false);
                }
                pending.push(EqualFrame::Range(
                    &left.data.child_data()[0],
                    ls,
                    &right.data.child_data()[0],
                    rs,
                    le - ls,
                ));
            }
            DataType::Union(_, _) => {
                let (lt, lv, _) = union_row(left.data, left.index, work)?;
                let (rt, rv, _) = union_row(right.data, right.index, work)?;
                if lt != rt {
                    return Ok(false);
                }
                pending.push(EqualFrame::Row(lv, rv));
            }
            _ => {
                return Err(ConstantError::Invalid(
                    "unsupported constant equality carrier",
                ));
            }
        }
    }
    Ok(true)
}
fn primitive_bytes(row: Row<'_>, width: usize) -> Result<&[u8], ConstantError> {
    let start = (row.data.offset() + row.index)
        .checked_mul(width)
        .ok_or(ConstantError::Invalid("constant byte offset overflow"))?;
    row.data.buffers()[0]
        .as_slice()
        .get(start..start + width)
        .ok_or(ConstantError::Invalid("constant value exceeds its buffer"))
}
fn variable_bytes(row: Row<'_>) -> Result<&[u8], ConstantError> {
    let data = row.data;
    let (buffer, start, end) = match data.data_type() {
        DataType::Utf8 | DataType::Binary => {
            let offsets = data.buffer::<i32>(0);
            (
                1,
                offsets[row.index] as usize,
                offsets[row.index + 1] as usize,
            )
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = data.buffer::<i64>(0);
            (
                1,
                offsets[row.index] as usize,
                offsets[row.index + 1] as usize,
            )
        }
        DataType::Utf8View | DataType::BinaryView => {
            let view = data.buffer::<u128>(0)[row.index];
            let len = (view as u32) as usize;
            if len <= 12 {
                let start = (data.offset() + row.index) * 16 + 4;
                (0, start, start + len)
            } else {
                let buffer = ((view >> 64) as u32) as usize + 1;
                let start = ((view >> 96) as u32) as usize;
                (buffer, start, start + len)
            }
        }
        _ => return Err(ConstantError::Invalid("expected variable byte carrier")),
    };
    data.buffers()
        .get(buffer)
        .and_then(|b| b.as_slice().get(start..end))
        .ok_or(ConstantError::Invalid("constant bytes exceed buffer"))
}
fn bytes_equal(
    left: &[u8],
    right: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ConstantError> {
    if left.len() != right.len() {
        work.step()?;
        return Ok(false);
    }
    for (left, right) in left.chunks(1024).zip(right.chunks(1024)) {
        work.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

impl ConstantValue {
    /// Lossless extraction from signed integer carriers. Exact type is retained;
    /// this accessor never changes a binding's type or reads temporal integers.
    pub fn try_i64(&self) -> Result<Option<i64>, ConstantError> {
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! signed {
            ($t:ty) => {
                Ok(array
                    .as_any()
                    .downcast_ref::<$t>()
                    .ok_or(ConstantError::Invalid("integer array differs from carrier"))?
                    .is_valid(row)
                    .then(|| {
                        array
                            .as_any()
                            .downcast_ref::<$t>()
                            .expect("checked integer array")
                            .value(row) as i64
                    }))
            };
        }
        match array.data_type() {
            DataType::Int8 => signed!(arrow_array::Int8Array),
            DataType::Int16 => signed!(arrow_array::Int16Array),
            DataType::Int32 => signed!(arrow_array::Int32Array),
            DataType::Int64 => signed!(arrow_array::Int64Array),
            _ => Err(ConstantError::Invalid(
                "constant is not a signed integer carrier",
            )),
        }
    }
    /// Lossless borrowed extraction from unsigned integer carriers only.
    /// None means an actual SQL NULL; signed/temporal/encoded carriers are
    /// explicit type errors, never implicit conversions or nonconstants.
    pub fn try_u64(&self) -> Result<Option<u64>, ConstantError> {
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! unsigned {
            ($t:ty) => {{
                let array = array
                    .as_any()
                    .downcast_ref::<$t>()
                    .ok_or(ConstantError::Invalid(
                        "unsigned integer array differs from carrier",
                    ))?;
                Ok(array.is_valid(row).then(|| array.value(row) as u64))
            }};
        }
        match array.data_type() {
            DataType::UInt8 => unsigned!(arrow_array::UInt8Array),
            DataType::UInt16 => unsigned!(arrow_array::UInt16Array),
            DataType::UInt32 => unsigned!(arrow_array::UInt32Array),
            DataType::UInt64 => unsigned!(arrow_array::UInt64Array),
            _ => Err(ConstantError::Invalid(
                "constant is not an unsigned integer carrier",
            )),
        }
    }
    /// Borrow the selected text from an exact Physical UTF8 carrier.
    /// Only SQL NULL returns None; nominal/encoded/other carriers are errors.
    /// The immutable pool already owns checked type, ordinal and UTF8 facts.
    pub fn try_utf8_borrowed_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Option<&str>, ConstantError> {
        selected_scalar_read::utf8(self, phase, control)
    }

    pub fn try_utf8(&self) -> Result<Option<&str>, ConstantError> {
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! utf8 {
            ($t:ty) => {{
                let a = array
                    .as_any()
                    .downcast_ref::<$t>()
                    .ok_or(ConstantError::Invalid("UTF8 array differs from carrier"))?;
                Ok(a.is_valid(row).then(|| a.value(row)))
            }};
        }
        match array.data_type() {
            DataType::Utf8 => utf8!(arrow_array::StringArray),
            DataType::LargeUtf8 => utf8!(arrow_array::LargeStringArray),
            DataType::Utf8View => utf8!(arrow_array::StringViewArray),
            _ => Err(ConstantError::Invalid("constant is not a UTF8 carrier")),
        }
    }
    pub fn try_binary(&self) -> Result<Option<&[u8]>, ConstantError> {
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! binary {
            ($t:ty) => {{
                let a = array
                    .as_any()
                    .downcast_ref::<$t>()
                    .ok_or(ConstantError::Invalid("binary array differs from carrier"))?;
                Ok(a.is_valid(row).then(|| a.value(row)))
            }};
        }
        match array.data_type() {
            DataType::Binary => binary!(arrow_array::BinaryArray),
            DataType::LargeBinary => binary!(arrow_array::LargeBinaryArray),
            DataType::BinaryView => binary!(arrow_array::BinaryViewArray),
            _ => Err(ConstantError::Invalid("constant is not a binary carrier")),
        }
    }
    pub fn try_boolean(&self) -> Result<Option<bool>, ConstantError> {
        let a = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or(ConstantError::Invalid("constant is not a Boolean carrier"))?;
        let row = self.ordinal as usize;
        Ok(a.is_valid(row).then(|| a.value(row)))
    }
    pub fn try_f32_bits(&self) -> Result<Option<u32>, ConstantError> {
        let a = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .ok_or(ConstantError::Invalid("constant is not a Float32 carrier"))?;
        let row = self.ordinal as usize;
        Ok(a.is_valid(row).then(|| a.value(row).to_bits()))
    }
    pub fn try_f64_bits(&self) -> Result<Option<u64>, ConstantError> {
        let a = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .ok_or(ConstantError::Invalid("constant is not a Float64 carrier"))?;
        let row = self.ordinal as usize;
        Ok(a.is_valid(row).then(|| a.value(row).to_bits()))
    }
    pub fn try_decimal128(&self) -> Result<Option<i128>, ConstantError> {
        let a = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .ok_or(ConstantError::Invalid(
                "constant is not a Decimal128 carrier",
            ))?;
        let row = self.ordinal as usize;
        Ok(a.is_valid(row).then(|| a.value(row)))
    }
    pub fn try_decimal256(&self) -> Result<Option<arrow_buffer::i256>, ConstantError> {
        let a = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::Decimal256Array>()
            .ok_or(ConstantError::Invalid(
                "constant is not a Decimal256 carrier",
            ))?;
        let row = self.ordinal as usize;
        Ok(a.is_valid(row).then(|| a.value(row)))
    }
    /// Exact signed LARGEINT extraction. Fixed binary never confers this domain.
    pub fn try_largeint(&self) -> Result<Option<i128>, ConstantError> {
        if self.value_type().logical_type() != ValueLogicalType::LargeInt
            || *self.value_type().data_type() != DataType::FixedSizeBinary(16)
        {
            return Err(ConstantError::Invalid(
                "constant is not an exact LARGEINT value",
            ));
        }
        let array = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .ok_or(ConstantError::Invalid(
                "LARGEINT array differs from carrier",
            ))?;
        let row = self.ordinal as usize;
        if array.is_null(row) {
            return Ok(None);
        }
        let bytes = array
            .value(row)
            .try_into()
            .map_err(|_| ConstantError::Invalid("LARGEINT value requires sixteen bytes"))?;
        Ok(Some(i128::from_be_bytes(bytes)))
    }
    /// Temporal accessors return the authored unit's raw value, without conversion.
    pub fn try_date32(&self) -> Result<Option<i32>, ConstantError> {
        if self.value_type().logical_type() != ValueLogicalType::Physical {
            return Err(ConstantError::Invalid(
                "Date32 accessor requires Physical domain",
            ));
        }
        let array = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .ok_or(ConstantError::Invalid("constant is not a Date32 carrier"))?;
        let row = self.ordinal as usize;
        Ok(array.is_valid(row).then(|| array.value(row)))
    }
    pub fn try_time64(&self) -> Result<Option<i64>, ConstantError> {
        if self.value_type().logical_type() != ValueLogicalType::Physical {
            return Err(ConstantError::Invalid(
                "Time64 accessor requires Physical domain",
            ));
        }
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! read {
            ($array:ty) => {{
                let a = array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or(ConstantError::Invalid("Time64 array differs from carrier"))?;
                Ok(a.is_valid(row).then(|| a.value(row)))
            }};
        }
        match array.data_type() {
            DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                read!(arrow_array::Time64MicrosecondArray)
            }
            DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                read!(arrow_array::Time64NanosecondArray)
            }
            _ => Err(ConstantError::Invalid(
                "constant is not a supported Time64 carrier",
            )),
        }
    }
    pub fn try_timestamp(&self) -> Result<Option<i64>, ConstantError> {
        if self.value_type().logical_type() != ValueLogicalType::Physical {
            return Err(ConstantError::Invalid(
                "Timestamp accessor requires Physical domain",
            ));
        }
        let array = self.pool.array();
        let row = self.ordinal as usize;
        macro_rules! read {
            ($array:ty) => {{
                let a = array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or(ConstantError::Invalid(
                        "Timestamp array differs from carrier",
                    ))?;
                Ok(a.is_valid(row).then(|| a.value(row)))
            }};
        }
        match array.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                read!(arrow_array::TimestampSecondArray)
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                read!(arrow_array::TimestampMillisecondArray)
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                read!(arrow_array::TimestampMicrosecondArray)
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                read!(arrow_array::TimestampNanosecondArray)
            }
            _ => Err(ConstantError::Invalid(
                "constant is not a Timestamp carrier",
            )),
        }
    }
    /// The authored interval components have independent units and signs.
    /// No packed integer representation or calendar conversion is inferred.
    pub fn try_interval_month_day_nano(&self) -> Result<Option<(i32, i32, i64)>, ConstantError> {
        if self.value_type().logical_type() != ValueLogicalType::Physical
            || *self.value_type().data_type()
                != DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
        {
            return Err(ConstantError::Invalid(
                "constant is not an exact Physical MonthDayNano interval",
            ));
        }
        let array = self
            .pool
            .array()
            .as_any()
            .downcast_ref::<arrow_array::IntervalMonthDayNanoArray>()
            .ok_or(ConstantError::Invalid(
                "MonthDayNano interval array differs from carrier",
            ))?;
        let row = self.ordinal as usize;
        Ok(array.is_valid(row).then(|| {
            let value = array.value(row);
            (value.months, value.days, value.nanoseconds)
        }))
    }
    pub fn try_decimal256_be(&self) -> Result<Option<[u8; 32]>, ConstantError> {
        self.try_decimal256()
            .map(|value| value.map(arrow_buffer::i256::to_be_bytes))
    }
    pub fn from_u64(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: u64,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::Physical || *ty.data_type() != DataType::UInt64 {
            return Err(ConstantError::Invalid(
                "unsigned factory requires exact Physical UInt64 type",
            ));
        }
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::UInt64Array::from(vec![value]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_largeint(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i128,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::LargeInt
            || *ty.data_type() != DataType::FixedSizeBinary(16)
        {
            return Err(ConstantError::Invalid(
                "LARGEINT factory requires exact logical LARGEINT type",
            ));
        }
        let bytes = value.to_be_bytes();
        let array =
            arrow_array::FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter())
                .map_err(ConstantError::arrow_validation)?;
        Self::from_scalar_array(field, ty, array.to_data(), policy, phase, control)
    }
    pub fn from_date32(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i32,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::Physical || *ty.data_type() != DataType::Date32 {
            return Err(ConstantError::Invalid(
                "date factory requires exact Physical Date32 type",
            ));
        }
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::Date32Array::from(vec![value]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_time64(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i64,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::Physical {
            return Err(ConstantError::Invalid(
                "Time64 factory requires Physical domain",
            ));
        }
        let array = match ty.data_type() {
            DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                arrow_array::Time64MicrosecondArray::from(vec![value]).to_data()
            }
            DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                arrow_array::Time64NanosecondArray::from(vec![value]).to_data()
            }
            _ => {
                return Err(ConstantError::Invalid(
                    "time factory requires exact supported Time64 type",
                ));
            }
        };
        Self::from_scalar_array(field, ty, array, policy, phase, control)
    }
    pub fn from_timestamp(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i64,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::Physical {
            return Err(ConstantError::Invalid(
                "Timestamp factory requires Physical domain",
            ));
        }
        let array = match ty.data_type() {
            DataType::Timestamp(arrow_schema::TimeUnit::Second, zone) => {
                arrow_array::TimestampSecondArray::from(vec![value])
                    .with_timezone_opt(zone.clone())
                    .to_data()
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, zone) => {
                arrow_array::TimestampMillisecondArray::from(vec![value])
                    .with_timezone_opt(zone.clone())
                    .to_data()
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, zone) => {
                arrow_array::TimestampMicrosecondArray::from(vec![value])
                    .with_timezone_opt(zone.clone())
                    .to_data()
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, zone) => {
                arrow_array::TimestampNanosecondArray::from(vec![value])
                    .with_timezone_opt(zone.clone())
                    .to_data()
            }
            _ => {
                return Err(ConstantError::Invalid(
                    "timestamp factory requires exact Timestamp type",
                ));
            }
        };
        Self::from_scalar_array(field, ty, array, policy, phase, control)
    }
    pub fn from_interval_month_day_nano(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: (i32, i32, i64),
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        if ty.logical_type() != ValueLogicalType::Physical
            || *ty.data_type() != DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
        {
            return Err(ConstantError::Invalid(
                "interval factory requires exact Physical MonthDayNano type",
            ));
        }
        let value = arrow_buffer::IntervalMonthDayNano::new(value.0, value.1, value.2);
        let array = arrow_array::IntervalMonthDayNanoArray::from(vec![value]);
        Self::from_scalar_array(field, ty, array.to_data(), policy, phase, control)
    }
    /// Big-endian coefficient bytes are lossless; precision/scale remain authored type facts.
    pub fn from_decimal256_be(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: [u8; 32],
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        Self::from_decimal256(
            field,
            ty,
            arrow_buffer::i256::from_be_bytes(value),
            policy,
            phase,
            control,
        )
    }
    pub fn from_i64(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i64,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::Int64Array::from(vec![value]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_i32(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i32,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::Int32Array::from(vec![value]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_boolean(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: bool,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::BooleanArray::from(vec![value]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_f64_bits(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: u64,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        Self::from_scalar_array(
            field,
            ty,
            arrow_array::Float64Array::from(vec![f64::from_bits(value)]).to_data(),
            policy,
            phase,
            control,
        )
    }
    pub fn from_utf8(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: &str,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(
            &field,
            &ty,
            value.len() as u64,
            false,
            policy,
            phase,
            control,
        )?;
        let array = match ty.data_type() {
            DataType::Utf8 => arrow_array::StringArray::from(vec![value]).to_data(),
            DataType::LargeUtf8 => arrow_array::LargeStringArray::from(vec![value]).to_data(),
            DataType::Utf8View => arrow_array::StringViewArray::from(vec![value]).to_data(),
            _ => {
                return Err(ConstantError::Invalid(
                    "UTF8 factory requires exact UTF8 carrier",
                ));
            }
        };
        Self::from_scalar_array(field, ty, array, policy, phase, control)
    }
    pub fn from_binary(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: &[u8],
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(
            &field,
            &ty,
            value.len() as u64,
            false,
            policy,
            phase,
            control,
        )?;
        let array = match ty.data_type() {
            DataType::Binary => arrow_array::BinaryArray::from(vec![value]).to_data(),
            DataType::LargeBinary => arrow_array::LargeBinaryArray::from(vec![value]).to_data(),
            DataType::BinaryView => arrow_array::BinaryViewArray::from(vec![value]).to_data(),
            _ => {
                return Err(ConstantError::Invalid(
                    "binary factory requires exact binary carrier",
                ));
            }
        };
        Self::from_scalar_array(field, ty, array, policy, phase, control)
    }
    pub fn from_decimal128(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: i128,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        let DataType::Decimal128(precision, scale) = ty.data_type() else {
            return Err(ConstantError::Invalid(
                "decimal factory requires exact Decimal128 type",
            ));
        };
        let array = arrow_array::Decimal128Array::from(vec![value])
            .with_precision_and_scale(*precision, *scale)
            .map_err(ConstantError::arrow_validation)?;
        Self::from_scalar_array(field, ty, array.to_data(), policy, phase, control)
    }
    pub fn from_decimal256(
        field: Arc<Field>,
        ty: FunctionValueType,
        value: arrow_buffer::i256,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, false, policy, phase, control)?;
        let DataType::Decimal256(precision, scale) = ty.data_type() else {
            return Err(ConstantError::Invalid(
                "decimal factory requires exact Decimal256 type",
            ));
        };
        let array = arrow_array::Decimal256Array::from(vec![value])
            .with_precision_and_scale(*precision, *scale)
            .map_err(ConstantError::arrow_validation)?;
        Self::from_scalar_array(field, ty, array.to_data(), policy, phase, control)
    }
    pub fn null(
        field: Arc<Field>,
        ty: FunctionValueType,
        policy: ConstantPolicy,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConstantError> {
        factory_preflight(&field, &ty, 0, true, policy, phase, control)?;
        let array = arrow_array::new_null_array(field.data_type(), 1);
        Self::from_scalar_array(field, ty, array.to_data(), policy, phase, control)
    }
}

// Convenience factories actively allocate, unlike try_new's already-owned
// input. Check exact grammar, expanded child extents and payload before Arrow
// allocation. These are finite opaque construction bounds, not MEM grants.
/// Check one conventional scalar construction before allocating Arrow backing.
/// Non-NULL construction is limited to inline or variable primitive carriers;
/// nested and encoded values require their own expansion preflight. NULL
/// construction uses the same recursive bounds as the owner NULL factory.
/// This checks finite library work and bytes; it is not a host allocation grant.
pub fn preflight_scalar_construction(
    field: &Field,
    ty: &FunctionValueType,
    payload: u64,
    null: bool,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<(), ConstantError> {
    control.checkpoint(phase, 0)?;
    if !null
        && !matches!(
            ty.data_type(),
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::FixedSizeBinary(_)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
    {
        return Err(ConstantError::Invalid(
            "non-NULL scalar construction requires a primitive carrier",
        ));
    }
    factory_preflight(field, ty, payload, null, policy, phase, control)
}

fn factory_preflight(
    field: &Field,
    ty: &FunctionValueType,
    payload: u64,
    null: bool,
    policy: ConstantPolicy,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<(), ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let metadata = validate_type(field, ty, policy, &mut work)?;
    limit(1, policy.max_rows, "constant factory row limit exceeded")?;
    if null && !ty.nullable() {
        return Err(ConstantError::Invalid(
            "NULL factory requires nullable exact type",
        ));
    }
    let mut pending = vec![(field.data_type(), 1u64, 1u32)];
    let mut nodes = 0;
    let mut elements = 0;
    let mut bytes = payload;
    let mut max_depth = 1;
    while let Some((data_type, len, depth)) = pending.pop() {
        work.step()?;
        nodes = checked_add(nodes, 1)?;
        elements = checked_add(elements, len)?;
        max_depth = max_depth.max(u64::from(depth));
        limit(
            nodes,
            policy.max_array_nodes,
            "constant factory array node limit exceeded",
        )?;
        limit(
            elements,
            policy.max_logical_elements,
            "constant factory element limit exceeded",
        )?;
        limit(
            u64::from(depth),
            u64::from(policy.max_type_depth),
            "constant factory depth limit exceeded",
        )?;
        let layout = arrow_data::layout(data_type);
        for buffer in &layout.buffers {
            work.step()?;
            let size = match buffer {
                arrow_data::BufferSpec::FixedWidth { byte_width, .. } => {
                    // Offset arrays allocate len+1; using that for every fixed
                    // buffer also bounds primitive/union/view construction.
                    checked_mul(checked_add(len, 1)?, *byte_width as u64)?
                }
                arrow_data::BufferSpec::BitMap => checked_add(len, 7)? / 8,
                _ => 0,
            };
            bytes = checked_add(bytes, checked_add(size, 63)? / 64 * 64)?;
            bytes = checked_add(bytes, std::mem::size_of::<arrow_buffer::Buffer>() as u64)?;
        }
        if layout.can_contain_null_mask {
            bytes = checked_add(bytes, checked_add(checked_add(len, 7)? / 8, 63)? / 64 * 64)?;
        }
        bytes = checked_add(bytes, std::mem::size_of::<ArrayData>() as u64)?;
        limit(
            bytes,
            policy.max_retained_buffer_bytes,
            "constant factory backing limit exceeded",
        )?;
        if !null {
            continue;
        }
        macro_rules! push {
            ($child:expr, $child_len:expr) => {{
                work.step()?;
                pending.push(($child, $child_len, depth + 1));
            }};
        }
        match data_type {
            DataType::Struct(fields) => {
                for field in fields {
                    push!(field.data_type(), len);
                }
            }
            DataType::FixedSizeList(field, width) => {
                push!(field.data_type(), checked_mul(len, *width as u64)?)
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => push!(field.data_type(), 0),
            DataType::Dictionary(_, values) => push!(values.as_ref(), 0),
            DataType::RunEndEncoded(ends, values) => {
                let max = match ends.data_type() {
                    DataType::Int16 => i16::MAX as u64,
                    DataType::Int32 => i32::MAX as u64,
                    _ => i64::MAX as u64,
                };
                if len > max {
                    return Err(ConstantError::Invalid(
                        "NULL run extent exceeds run-end carrier",
                    ));
                }
                push!(ends.data_type(), u64::from(len > 0));
                push!(values.data_type(), u64::from(len > 0));
            }
            DataType::Union(fields, mode) => {
                if fields.is_empty() {
                    return Err(ConstantError::Invalid(
                        "NULL factory cannot select an empty Union",
                    ));
                }
                if *mode == UnionMode::Dense && len > i32::MAX as u64 {
                    return Err(ConstantError::Invalid(
                        "NULL dense Union extent exceeds offset carrier",
                    ));
                }
                for (index, (_, field)) in fields.iter().enumerate() {
                    push!(
                        field.data_type(),
                        if index == 0 || *mode == UnionMode::Sparse {
                            len
                        } else {
                            0
                        }
                    );
                }
            }
            _ => {}
        }
    }
    let opaque_work = checked_mul(
        checked_add(checked_add(bytes, metadata)?, checked_mul(nodes, nodes)?)?,
        max_depth,
    )?;
    limit(
        opaque_work,
        policy.max_library_validation_work,
        "constant factory opaque work limit exceeded",
    )?;
    limit(
        checked_add(bytes, metadata)?,
        policy.max_library_validation_bytes,
        "constant factory opaque byte limit exceeded",
    )?;
    work.finish()?;
    Ok(())
}

fn validate_decimal_value(row: Row<'_>) -> Result<(), ConstantError> {
    use arrow_array::types::*;
    let result = match row.data.data_type() {
        DataType::Decimal32(p, s) => {
            Decimal32Type::validate_decimal_precision(row.data.buffer::<i32>(0)[row.index], *p, *s)
        }
        DataType::Decimal64(p, s) => {
            Decimal64Type::validate_decimal_precision(row.data.buffer::<i64>(0)[row.index], *p, *s)
        }
        DataType::Decimal128(p, s) => Decimal128Type::validate_decimal_precision(
            row.data.buffer::<i128>(0)[row.index],
            *p,
            *s,
        ),
        DataType::Decimal256(p, s) => Decimal256Type::validate_decimal_precision(
            row.data.buffer::<arrow_buffer::i256>(0)[row.index],
            *p,
            *s,
        ),
        _ => return Ok(()),
    };
    result.map_err(ConstantError::arrow_decimal_value)
}

#[cfg(test)]
#[path = "failure_tail_tests.rs"]
mod failure_tail_tests;

#[cfg(test)]
#[path = "owner_ports_tests.rs"]
mod owner_ports_tests;
