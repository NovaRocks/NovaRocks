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

use super::{checkpoint_at, invariant, original, shared, vector};
use crate::{
    exec::{
        hash_table::{
            allocation::AggregateAllocator,
            partition_keys::{
                CanonicalTupleStorage, CanonicalTupleView, prepare_canonical_tuple,
                prepare_canonical_tuple_with_byte_layout_projection,
            },
        },
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::{array::ArrayRef, datatypes::DataType};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_value_arrays::{
    ByteLayout, ValueViolation,
    batch::{OwnedPreparedBytes, PreparedBytes},
    decode::ValueConversionContext,
    reachability::{ReachableRows, SelectionSet},
};
use std::sync::Arc;

/// SQL matching policy, independent of the signed nullable input declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NullPolicy {
    /// A logical NULL does not participate in this equality index.
    Unmatchable,
    /// NULL is one equality class (GROUP BY or null-safe equality).
    Equal,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexPurpose {
    KeyIds,
    PresenceOnly,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct KeyId(pub(super) usize);
impl KeyId {
    pub(crate) fn index(self) -> usize {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyStrategy {
    Fixed,
    Bytes,
    Composite,
    Canonical,
}

struct SpecData {
    types: Vec<FunctionValueType>,
    nulls: Vec<NullPolicy>,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the signed logical key strategy independently of actual Arrow carrier."
        )
    )]
    strategy: KeyStrategy,
    _storage: ValueTypeStorageLifetime,
}
#[derive(Clone)]
pub(crate) struct KeySpec(Arc<SpecData>);
impl KeySpec {
    pub(crate) fn new(
        types: &[FunctionValueType],
        nulls: &[NullPolicy],
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        if types.is_empty() || types.len() != nulls.len() {
            return Err(invariant(ValueViolation::Layout));
        }
        let mut copied_types = vector(owner, types.len())?;
        let mut copied_nulls = vector(owner, nulls.len())?;
        let mut canonical = false;
        for (i, (ty, policy)) in types.iter().zip(nulls).enumerate() {
            checkpoint_at(i, checkpoint)?;
            copied_types.push(ty.clone_observed(checkpoint)?);
            copied_nulls.push(*policy);
            canonical |=
                !is_fixed(ty.data_type()) && ByteLayout::from_data_type(ty.data_type()).is_none();
        }
        let strategy = if canonical {
            KeyStrategy::Canonical
        } else if types.len() != 1 {
            KeyStrategy::Composite
        } else if ByteLayout::from_data_type(types[0].data_type()).is_some() {
            KeyStrategy::Bytes
        } else {
            KeyStrategy::Fixed
        };
        Ok(Self(shared(
            owner,
            SpecData {
                types: copied_types,
                nulls: copied_nulls,
                strategy,
                _storage: owner.storage_lifetime(),
            },
        )?))
    }
    pub(crate) fn types(&self) -> &[FunctionValueType] {
        &self.0.types
    }
    pub(crate) fn nulls(&self) -> &[NullPolicy] {
        &self.0.nulls
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Exposes the frozen signed logical strategy fact used by K property tests."
        )
    )]
    pub(crate) fn strategy(&self) -> KeyStrategy {
        self.0.strategy
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
pub(super) fn is_fixed(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(..)
            | DataType::Time32(..)
            | DataType::Time64(..)
            | DataType::Duration(..)
            | DataType::FixedSizeBinary(_)
    )
}

struct InputData {
    arrays: Vec<ArrayRef>,
    bindings: Vec<FunctionValueType>,
    selected: Arc<SelectionSet>,
    rows: Vec<usize>,
    ordinals: Vec<usize>,
    tuples: CanonicalTupleStorage,
    preparation: Option<CapturedPreparation>,
    #[expect(
        dead_code,
        reason = "Retains the actual input directory/selection creator; preserve field order."
    )]
    storage: ValueTypeStorageLifetime,
}
/// One immutable, evaluated page. Saved hashes bind to this actual owner, not
/// to an array address, caller-supplied hash or reconstructed dictionary code.
#[derive(Clone)]
pub(crate) struct KeyInput(Arc<InputData>);
impl KeyInput {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(crate) fn new(
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        readable: &SelectionSet,
        occurrences: &[usize],
        conversions: &mut ValueConversionContext,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        Self::capture(
            None,
            arrays,
            bindings,
            readable,
            occurrences,
            conversions,
            owner,
            checkpoint,
        )
    }
    /// Freeze one page's exact key projection and byte preparation before the
    /// immutable input is shared with bulk tasks. No task borrows a temporary
    /// conversion context or repeats whole-page preparation at each advance.
    pub(crate) fn new_for_spec(
        spec: &KeySpec,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        readable: &SelectionSet,
        occurrences: &[usize],
        conversions: &mut ValueConversionContext,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        Self::capture(
            Some(spec),
            arrays,
            bindings,
            readable,
            occurrences,
            conversions,
            owner,
            checkpoint,
        )
    }
    fn capture(
        spec: Option<&KeySpec>,
        arrays: &[ArrayRef],
        bindings: &[FunctionValueType],
        readable: &SelectionSet,
        occurrences: &[usize],
        conversions: &mut ValueConversionContext,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        if arrays.is_empty()
            || arrays.len() != bindings.len()
            || arrays.iter().any(|a| a.len() != readable.len())
        {
            return Err(invariant(ValueViolation::Bounds));
        }
        let mut columns = vector(owner, arrays.len())?;
        let mut types = vector(owner, bindings.len())?;
        for (i, (array, binding)) in arrays.iter().zip(bindings).enumerate() {
            checkpoint_at(i, checkpoint)?;
            columns.push(Arc::clone(array));
            types.push(binding.clone_observed(checkpoint)?);
        }
        let mut rows = vector(owner, occurrences.len())?;
        let mut ordered = true;
        for (i, &row) in occurrences.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            if !readable.contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            ordered &= rows.last().is_none_or(|&previous| previous <= row);
            rows.push(row);
        }
        if !ordered {
            sort_rows(&mut rows, owner, checkpoint)?;
        }
        let mut unique = 0;
        for i in 0..rows.len() {
            checkpoint_at(i, checkpoint)?;
            if unique == 0 || rows[i] != rows[unique - 1] {
                rows[unique] = rows[i];
                unique += 1;
            }
        }
        rows.truncate(unique);
        let mut selected = SelectionSet::try_new_with_control_observed(
            readable.len(),
            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
            &mut |_| checkpoint(),
        )
        .map_err(original)?;
        for (i, &row) in rows.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            selected
                .insert(row)
                .map_err(ExecutionFailure::from_domain)?;
        }
        let selected = shared(owner, selected)?;
        let mut ordinals = vector(owner, occurrences.len())?;
        for (i, row) in occurrences.iter().enumerate() {
            checkpoint_at(i, checkpoint)?;
            ordinals.push(
                rows.binary_search(row)
                    .map_err(|_| invariant(ValueViolation::Bounds))?,
            );
        }
        checkpoint()?;
        let tuples = prepare_canonical_tuple(
            &columns,
            &types,
            &ReachableRows::Selection(&selected),
            conversions,
            owner,
            checkpoint,
        )?
        .into_storage();
        let mut data = InputData {
            arrays: columns,
            bindings: types,
            selected,
            rows,
            ordinals,
            tuples,
            preparation: None,
            storage: owner.storage_lifetime(),
        };
        if let Some(spec) = spec {
            data.preparation = Some(CapturedPreparation::new(spec, &data, owner, checkpoint)?);
        }
        Ok(Self(shared(owner, data)?))
    }
    pub(crate) fn len(&self) -> usize {
        self.0.ordinals.len()
    }
    pub(crate) fn row(&self, occurrence: usize) -> ExecutionResult<usize> {
        let ordinal = *self
            .0
            .ordinals
            .get(occurrence)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        Ok(self.0.rows[ordinal])
    }
    /// Observe root SQL NULL through the same already-prepared tuple author,
    /// including dictionary bottom NULL and logical NULL carriers. Occurrence
    /// coordinates retain their original source-row mapping and duplicates.
    pub(crate) fn row_has_null_observed(
        &self,
        occurrence: usize,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<bool> {
        checkpoint()?;
        let row = self.row(occurrence)?;
        let storage = self
            .0
            .preparation
            .as_ref()
            .and_then(|p| p.projected.as_ref())
            .map_or(&self.0.tuples, |(storage, _)| storage);
        storage
            .view(self.arrays())
            .row_has_null_observed(row, checkpoint)
    }
    pub(crate) fn arrays(&self) -> &[ArrayRef] {
        &self.0.arrays
    }
    pub(crate) fn bindings(&self) -> &[FunctionValueType] {
        &self.0.bindings
    }
    pub(crate) fn selected(&self) -> &SelectionSet {
        &self.0.selected
    }
    pub(crate) fn unique_rows(&self) -> &[usize] {
        &self.0.rows
    }
    pub(super) fn ordinal(&self, occurrence: usize) -> usize {
        self.0.ordinals[occurrence]
    }
    pub(super) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
/// Coordinate sorting is ordinary fast-path linear for ordered pages. The
/// uncommon reordered occurrence input uses controlled primitive merge passes;
/// no uninterruptible full-input sort or per-key Arrow array is introduced.
pub(super) fn sort_rows(
    rows: &mut [usize],
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    sort_by_key(rows, owner, checkpoint, |row| row)
}
/// The same controlled merge passes also group bounded collision metadata by
/// owned page. Keys are primitive coordinates, never fallible value equality.
pub(super) fn sort_by_key<T: Copy, K: Ord>(
    rows: &mut [T],
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
    key: impl Fn(T) -> K,
) -> ExecutionResult<()> {
    let mut scratch = vector(owner, rows.len())?;
    for (i, &row) in rows.iter().enumerate() {
        checkpoint_at(i, checkpoint)?;
        scratch.push(row);
    }
    let mut width = 1usize;
    while width < rows.len() {
        checkpoint()?;
        for start in (0..rows.len()).step_by(width.saturating_mul(2)) {
            let middle = start.saturating_add(width).min(rows.len());
            let end = middle.saturating_add(width).min(rows.len());
            let (mut left, mut right) = (start, middle);
            for (offset, slot) in scratch[start..end].iter_mut().enumerate() {
                checkpoint_at(offset, checkpoint)?;
                if left < middle && (right == end || key(rows[left]) <= key(rows[right])) {
                    *slot = rows[left];
                    left += 1;
                } else {
                    *slot = rows[right];
                    right += 1;
                }
            }
        }
        for (i, (to, &from)) in rows.iter_mut().zip(&scratch).enumerate() {
            checkpoint_at(i, checkpoint)?;
            *to = from;
        }
        width = width.saturating_mul(2);
    }
    Ok(())
}

pub(super) enum DomainFailure {
    Type(novarocks_type_contract::ValueTypeError),
    Caller(ExecutionFailure),
}
impl From<novarocks_type_contract::ValueTypeError> for DomainFailure {
    fn from(e: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(e)
    }
}
impl From<DomainFailure> for ExecutionFailure {
    fn from(e: DomainFailure) -> Self {
        match e {
            DomainFailure::Type(e) => novarocks_functions::KernelFailure::from(e).into(),
            DomainFailure::Caller(e) => e,
        }
    }
}

/// Owns one exact projection and its checked byte receipts. It is constructed
/// before InputData publication and never contains a reference back to KeyInput.
struct CapturedPreparation {
    projected: Option<(CanonicalTupleStorage, Vec<FunctionValueType>)>,
    bytes: Vec<Option<OwnedPreparedBytes>>,
    spec: KeySpec,
    _storage: ValueTypeStorageLifetime,
}
impl CapturedPreparation {
    fn new(
        spec: &KeySpec,
        input: &InputData,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        let projected = prepare_projection(spec, input, owner, checkpoint)?;
        let mut bytes = vector(owner, spec.types().len())?;
        for column in 0..spec.types().len() {
            checkpoint_at(column, checkpoint)?;
            bytes.push(
                input
                    .tuples
                    .decoded_component(column)
                    .map(|decoded| {
                        OwnedPreparedBytes::new_observed(
                            decoded.clone(),
                            Arc::clone(&input.selected),
                            owner.storage_lifetime(),
                            &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                            &mut |_| checkpoint(),
                        )
                        .map_err(original)
                    })
                    .transpose()?,
            );
        }
        checkpoint()?;
        Ok(Self {
            projected,
            bytes,
            spec: spec.clone(),
            _storage: owner.storage_lifetime(),
        })
    }
}

/// Both short-lived and captured preparation use this single signed-domain and
/// complex-layout projection author. Ordinary byte columns keep their actual
/// admitted carrier even beside a projected complex column.
fn prepare_projection(
    spec: &KeySpec,
    input: &InputData,
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<Option<(CanonicalTupleStorage, Vec<FunctionValueType>)>> {
    checkpoint()?;
    if spec.types().len() != input.bindings.len() {
        return Err(invariant(ValueViolation::Layout));
    }
    for (expected, actual) in spec.types().iter().zip(&input.bindings) {
        if !expected
            .same_value_domain_observed::<DomainFailure>(actual, || {
                checkpoint().map_err(DomainFailure::Caller)
            })
            .map_err(ExecutionFailure::from)?
        {
            return Err(invariant(ValueViolation::Layout));
        }
    }
    // The original codec distinguishes some physical layouts (for example
    // Binary and LargeBinary framing). Normalize complex columns once to
    // this index's exact projection, while keeping each admitted input's
    // own root NULL declaration. Distributed encoders remain unchanged.
    let mut needs_projection = false;
    for (expected, actual) in spec.types().iter().zip(&input.bindings) {
        if !is_fixed(expected.data_type())
            && ByteLayout::from_data_type(expected.data_type()).is_none()
            && !expected
                .typed_binding()
                .layout()
                .equals_observed(actual.typed_binding().layout(), checkpoint)?
        {
            needs_projection = true;
        }
    }
    let projected = if needs_projection {
        let mut bindings = vector(owner, spec.types().len())?;
        for (expected, actual) in spec.types().iter().zip(&input.bindings) {
            checkpoint()?;
            // A complex neighbor does not change an ordinary byte
            // carrier or invalidate its original decoded source.
            if is_fixed(expected.data_type())
                || ByteLayout::from_data_type(expected.data_type()).is_some()
            {
                bindings.push(actual.clone_observed(checkpoint)?);
                continue;
            }
            let value = actual.value_type().clone();
            checkpoint()?;
            let layout = expected.typed_binding().layout().clone();
            let binding = novarocks_type_contract::TypedValueBinding::try_new_observed::<
                super::store::BindingFailure,
            >(value, layout, |_| {
                checkpoint().map_err(super::store::BindingFailure::Caller)
            })
            .map_err(ExecutionFailure::from)?;
            bindings.push(
                FunctionValueType::try_from_typed_binding(binding).map_err(|e| {
                    ExecutionFailure::from(novarocks_functions::KernelFailure::from(e))
                })?,
            );
        }
        let storage = prepare_canonical_tuple_with_byte_layout_projection(
            input.arrays.as_slice(),
            &bindings,
            &ReachableRows::Selection(input.selected.as_ref()),
            &mut ValueConversionContext::default(),
            owner,
            checkpoint,
        )?
        .into_storage();
        Some((storage, bindings))
    } else {
        None
    };
    Ok(projected)
}

enum Preparation<'a> {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    Temporary {
        projected: Option<(CanonicalTupleStorage, Vec<FunctionValueType>)>,
        bytes: Vec<Option<PreparedBytes<'a>>>,
        _storage: ValueTypeStorageLifetime,
    },
    Captured(&'a CapturedPreparation),
}
impl Preparation<'_> {
    fn projected(&self) -> Option<&(CanonicalTupleStorage, Vec<FunctionValueType>)> {
        match self {
            Self::Temporary { projected, .. } => projected.as_ref(),
            Self::Captured(prepared) => prepared.projected.as_ref(),
        }
    }
    fn bytes(&self, column: usize) -> ExecutionResult<Option<PreparedBytes<'_>>> {
        match self {
            Self::Temporary { bytes, .. } => bytes.get(column).copied(),
            Self::Captured(prepared) => prepared
                .bytes
                .get(column)
                .map(|bytes| bytes.as_ref().map(OwnedPreparedBytes::borrow)),
        }
        .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
}

pub(crate) struct PreparedKeyBatch<'a> {
    pub(super) input: &'a KeyInput,
    pub(super) spec: KeySpec,
    preparation: Preparation<'a>,
    pub(super) allocator: AggregateAllocator,
}
impl<'a> PreparedKeyBatch<'a> {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Preserves the tested independent K preparation/task interface in production; current consumers use the shared captured graph path."
        )
    )]
    pub(crate) fn new(
        spec: &KeySpec,
        input: &'a KeyInput,
        allocator: AggregateAllocator,
        owner: &dyn InputConstructionOwner,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        let projected = prepare_projection(spec, &input.0, owner, checkpoint)?;
        let mut bytes = vector(owner, spec.types().len())?;
        for column in 0..spec.types().len() {
            bytes.push(
                input
                    .0
                    .tuples
                    .decoded_component(column)
                    .map(|decoded| {
                        PreparedBytes::new_observed(decoded, input.selected(), &mut |_| {
                            checkpoint()
                        })
                        .map_err(original)
                    })
                    .transpose()?,
            );
        }
        Ok(Self {
            input,
            spec: spec.clone(),
            preparation: Preparation::Temporary {
                projected,
                bytes,
                _storage: owner.storage_lifetime(),
            },
            allocator,
        })
    }
    /// Constant-time loan of this exact immutable capture. Missing or foreign
    /// preparation is rejected; it never falls back to scanning the whole page.
    pub(crate) fn from_captured(
        spec: &KeySpec,
        input: &'a KeyInput,
        allocator: AggregateAllocator,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<Self> {
        checkpoint()?;
        let captured = input
            .0
            .preparation
            .as_ref()
            .filter(|captured| captured.spec.same_instance(spec))
            .ok_or_else(|| invariant(ValueViolation::Layout))?;
        Ok(Self {
            input,
            spec: spec.clone(),
            preparation: Preparation::Captured(captured),
            allocator,
        })
    }
    pub(super) fn tuples(&self) -> CanonicalTupleView<'_> {
        self.preparation
            .projected()
            .map_or(&self.input.0.tuples, |(storage, _)| storage)
            .view(self.input.arrays())
    }
    pub(super) fn component_binding(&self, column: usize) -> ExecutionResult<&FunctionValueType> {
        let bindings = self
            .preparation
            .projected()
            .map_or(self.input.bindings(), |(_, bindings)| bindings.as_slice());
        bindings
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))
    }
    pub(crate) fn len(&self) -> usize {
        self.input.len()
    }
    pub(crate) fn bytes(&self, column: usize) -> ExecutionResult<Option<PreparedBytes<'_>>> {
        self.preparation.bytes(column)
    }

    /// Bind a materialization subset once, borrowing the same decoded source.
    /// The subset does not authorize rows outside this input's readable domain.
    pub(super) fn byte_subset<'b>(
        &'b self,
        column: usize,
        rows: &'b SelectionSet,
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<PreparedBytes<'b>> {
        let decoded = self
            .input
            .0
            .tuples
            .decoded_component(column)
            .ok_or_else(|| invariant(ValueViolation::Carrier))?;
        PreparedBytes::new_observed(decoded, rows, &mut |_| checkpoint()).map_err(original)
    }
}
