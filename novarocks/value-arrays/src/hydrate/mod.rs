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

//! Planned, binding-directed materialization. The caller owns capacity and
//! scheduling; plans retain the original source and publish only whole rows.
use crate::{
    ValueError, ValueFailure, ValueViolation,
    control::{Growth, Work},
    decode::ValueConversionContext,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use novarocks_failure_contract::RepresentationFacts;
use novarocks_type_contract::{BorrowedTypedValueBinding, TypedValueBinding};
use std::{alloc::Layout, sync::Arc};

mod execution;
mod tree;
mod validation;
use tree::Node;
pub use validation::{
    required_values_satisfied_observed, validate_required_observed,
    validate_required_with_options_observed,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HydratePathStep {
    ListItem,
    StructField(usize),
    UnionField(i8),
    MapKey,
    MapValue,
}

/// Structure observations precede actual wrapper creation. Payload allocation
/// is separately reported to Growth, and logical work to Work.
#[derive(Clone, Copy, Debug)]
pub enum HydrateEvent<'a> {
    InspectType(&'a DataType),
    InspectDictionaryRows {
        source: &'a ArrayRef,
        row_start: usize,
        row_count: usize,
    },
    ValueStructure {
        path: &'a [HydratePathStep],
        event: crate::control::ValueStructure,
    },
    InspectField(&'a Field),
    SliceArray {
        source: &'a ArrayRef,
        offset: usize,
        length: usize,
    },
    RebuildArray {
        path: &'a [HydratePathStep],
        source: &'a ArrayRef,
        target: &'a DataType,
    },
    RebuildType {
        path: &'a [HydratePathStep],
        source: &'a DataType,
    },
    RebuildField {
        path: &'a [HydratePathStep],
        source: &'a Field,
        target: &'a DataType,
    },
    MapEntries {
        path: &'a [HydratePathStep],
        source: &'a ArrayRef,
    },
    OpaqueOperationFinished,
}
pub type HydrateObserver<'a, E> = dyn for<'b> FnMut(HydrateEvent<'b>) -> Result<(), E> + 'a;

/// Already-held planning storage is informational and must not be requested a
/// second time. Output and execution scratch describe new storage separately.
#[derive(Debug, Default)]
pub struct HydrateRequirements {
    pub output: Vec<Layout>,
    pub execution_scratch: Vec<Layout>,
    /// Includes strong snapshots shared with the conversion cache; this is
    /// retained declared storage, not an additive physical-memory estimate.
    pub held_plan_scratch_bytes: usize,
    pub new_decode_storage_bytes: usize,
    pub shared_cached_storage_bytes: usize,
    pub logical_bytes: u128,
}

/// Borrowed output geometry from a frozen plan, before any output is built.
/// Buffer lengths are conservative copy extents, never owner capacities. A
/// newly materialized byte array may omit an all-valid bitmap at execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannedOutputStorage {
    Node { rows: usize, source_offset: usize },
    Buffer { bytes: usize, newly_owned: bool },
}

/// Caller-authored representation projection. Signed external boundaries may
/// freeze new ordinal field names and NULL declarations. An explicitly requested
/// byte-layout projection changes only plain STRING/BINARY representation;
/// scalar parameters and value domains remain exact.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HydrateProjection {
    #[default]
    Exact,
    FrozenNamesAndNullability,
    /// Project plain bytes to the caller's exact target layout within the same
    /// STRING or BINARY domain. Names and NULL declarations remain exact;
    /// dictionary values retain their original D(L)/explicit-override policy.
    ExactByteLayouts,
}

/// Primitive storage parameter for exercising a future dictionary-value
/// layout. Production callers use Default, which consumes canonical D(L).
#[derive(Clone, Copy, Debug, Default)]
pub struct HydrateOptions {
    pub dictionary_value_layout: Option<crate::ByteLayout>,
    pub projection: HydrateProjection,
}

struct AllocationClaim {
    layout: Layout,
    count: usize,
}

pub struct HydratePlan {
    source: ArrayRef,
    binding: TypedValueBinding,
    root: Node,
    input_rows: usize,
    rows: usize,
    blocking: Option<RepresentationFacts>,
    requirements: HydrateRequirements,
    claims: Vec<AllocationClaim>,
    claim_counter_layout: Option<Layout>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl HydratePlan {
    pub fn plan_observed<E>(
        source: &ArrayRef,
        binding: &TypedValueBinding,
        context: &mut ValueConversionContext,
        keep_encoded: &mut impl FnMut(&[HydratePathStep], &DataType) -> bool,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        Self::plan_with_options_observed(
            source,
            binding,
            HydrateOptions::default(),
            context,
            keep_encoded,
            growth,
            work,
            observe,
        )
    }
    pub fn plan_with_options_observed<E>(
        source: &ArrayRef,
        binding: &TypedValueBinding,
        options: HydrateOptions,
        context: &mut ValueConversionContext,
        keep_encoded: &mut impl FnMut(&[HydratePathStep], &DataType) -> bool,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        Self::plan_with_options_and_storage_observed(
            source,
            binding,
            options,
            context,
            &novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            keep_encoded,
            growth,
            work,
            observe,
        )
    }
    pub fn plan_with_options_and_storage_observed<E>(
        source: &ArrayRef,
        binding: &TypedValueBinding,
        options: HydrateOptions,
        context: &mut ValueConversionContext,
        storage: &novarocks_type_contract::ValueTypeStorageLifetime,
        keep_encoded: &mut impl FnMut(&[HydratePathStep], &DataType) -> bool,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        validation::validate_with_options(source, binding, options, work, observe)?;
        let mut path = [HydratePathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
        let mut root = Node::plan(
            source,
            binding.borrowed(),
            options,
            &mut path,
            0,
            keep_encoded,
            growth,
            work,
            observe,
        )?;
        let mut rows = 0;
        let mut blocking = None;
        for row in 0..source.len() {
            root.clear_row();
            root.accumulate_row(row, work)?;
            if let Some(facts) = root.row_boundary(rows)? {
                blocking = Some(facts);
                break;
            }
            root.commit_row();
            rows += 1;
        }
        if rows != source.len() {
            // Slice at a complete root row before constructing final physical
            // child domains. List/Map retain their real offset coordinate space.
            observe(HydrateEvent::SliceArray {
                source,
                offset: 0,
                length: rows,
            })
            .map_err(ValueFailure::Caller)?;
            let sliced = source.slice(0, rows);
            let mut decisions = Vec::new();
            root.keep_decisions(&mut decisions, growth)?;
            let mut ordinal = 0;
            root = Node::plan(
                &sliced,
                binding.borrowed(),
                options,
                &mut path,
                0,
                &mut |_, _| {
                    let keep = decisions[ordinal];
                    ordinal += 1;
                    keep
                },
                growth,
                work,
                observe,
            )?;
            for row in 0..rows {
                root.mark_row(row, work)?;
            }
        } else {
            for row in 0..rows {
                root.mark_row(row, work)?;
            }
        }
        let mut requirements = HydrateRequirements::default();
        root.requirements(&mut requirements, growth, work)?;
        root.prepare_decode(
            binding.borrowed(),
            context,
            storage,
            &mut path,
            0,
            growth,
            work,
            observe,
        )?;
        let (new_decode, shared_decode) = root.decode_storage_bytes();
        requirements.new_decode_storage_bytes = new_decode;
        requirements.shared_cached_storage_bytes = shared_decode;
        let allocation_count = requirements.output.len() + requirements.execution_scratch.len();
        let claim_counter_layout = if allocation_count == 0 {
            None
        } else {
            let layout = Layout::array::<usize>(allocation_count + 1).map_err(|_| invalid())?;
            let count = requirements.execution_scratch.len() + 1;
            reserve(&mut requirements.execution_scratch, count, growth)?;
            requirements.execution_scratch.push(layout);
            Some(layout)
        };
        let mut claims = Vec::new();
        reserve(
            &mut claims,
            allocation_count + usize::from(claim_counter_layout.is_some()),
            growth,
        )?;
        for layout in requirements
            .output
            .iter()
            .chain(&requirements.execution_scratch)
        {
            if layout.size() != 0 {
                claims.push(AllocationClaim {
                    layout: *layout,
                    count: 1,
                });
            }
        }
        claims.sort_unstable_by_key(|claim| (claim.layout.size(), claim.layout.align()));
        let mut unique = 0;
        for index in 0..claims.len() {
            if unique != 0 && claims[unique - 1].layout == claims[index].layout {
                claims[unique - 1].count += 1;
            } else {
                claims.swap(unique, index);
                unique += 1;
            }
        }
        claims.truncate(unique);
        requirements.held_plan_scratch_bytes = root.held_scratch_bytes()
            + requirements.output.capacity() * std::mem::size_of::<Layout>()
            + requirements.execution_scratch.capacity() * std::mem::size_of::<Layout>();
        requirements.held_plan_scratch_bytes +=
            claims.capacity() * std::mem::size_of::<AllocationClaim>();
        let binding = binding
            .clone_observed(|| work(1))
            .map_err(ValueFailure::Caller)?;
        Ok(Self {
            source: Arc::clone(source),
            binding,
            root,
            input_rows: source.len(),
            rows,
            blocking,
            requirements,
            claims,
            claim_counter_layout,
            _storage: storage.clone(),
        })
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn input_rows(&self) -> usize {
        self.input_rows
    }
    pub fn requirements(&self) -> &HydrateRequirements {
        &self.requirements
    }
    pub fn source(&self) -> &ArrayRef {
        &self.source
    }
    /// Visit the planned output directly. No ArrayData, duplicate key scan or
    /// secondary materialization policy is created by a boundary's preflight.
    pub fn visit_output_storage_observed<E>(
        &self,
        visit: &mut impl FnMut(PlannedOutputStorage) -> Result<(), E>,
        work: &mut Work<'_, E>,
    ) -> Result<(), ValueFailure<E>> {
        self.root
            .visit_output_storage(self.binding.borrowed(), visit, work)
    }
    /// Reject incomplete single-result output with the original value fact.
    pub fn require_complete(&self) -> Result<(), ValueError> {
        if let Some(facts) = self.blocking {
            return Err(ValueError::Representation(facts));
        }
        Ok(())
    }
    /// Admit every new output and execution-scratch allocation before creating
    /// the first output. Already-held planning storage is not charged again.
    /// An owner requiring the full input calls require_complete first.
    pub fn execute_observed<E>(
        &self,
        context: &mut ValueConversionContext,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
        observe: &mut HydrateObserver<'_, E>,
    ) -> Result<ArrayRef, ValueFailure<E>> {
        for layout in self
            .requirements
            .output
            .iter()
            .chain(&self.requirements.execution_scratch)
        {
            work(0).map_err(ValueFailure::Caller)?;
            if layout.size() != 0 {
                growth(*layout).map_err(ValueFailure::Caller)?;
            }
        }
        // The counter vector is itself a pre-admitted execution allocation.
        // Requests consume a frozen multiset, including duplicate layouts; an
        // unexpected growth is rejected in every build, not merely asserted.
        let mut consumed = Vec::new();
        if let Some(layout) = self.claim_counter_layout {
            let slots = layout.size() / std::mem::size_of::<usize>();
            consumed
                .try_reserve_exact(slots)
                .map_err(|source| ValueError::Allocation {
                    requested: layout.size(),
                    source,
                })?;
            consumed.resize(slots, 0usize);
            let index = self
                .claims
                .binary_search_by_key(&(layout.size(), layout.align()), |claim| {
                    (claim.layout.size(), claim.layout.align())
                })
                .map_err(|_| invalid())?;
            consumed[index] = 1;
        }
        let mut admitted = |layout: Layout| {
            if layout.size() == 0 {
                return Ok(());
            }
            let index = self
                .claims
                .binary_search_by_key(&(layout.size(), layout.align()), |claim| {
                    (claim.layout.size(), claim.layout.align())
                })
                .map_err(|_| {
                    ValueFailure::from(ValueError::Invariant(ValueViolation::RowTransaction))
                })?;
            if consumed[index] == self.claims[index].count {
                return Err(ValueError::Invariant(ValueViolation::RowTransaction).into());
            }
            consumed[index] += 1;
            Ok::<(), ValueFailure<E>>(())
        };
        let mut checked_work = |units| work(units).map_err(ValueFailure::Caller);
        let mut checked_observe =
            |event: HydrateEvent<'_>| observe(event).map_err(ValueFailure::Caller);
        let mut path = [HydratePathStep::ListItem; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
        self.root
            .execute(
                self.binding.borrowed(),
                context,
                &mut path,
                0,
                &mut admitted,
                &mut checked_work,
                &mut checked_observe,
            )
            .map_err(|error| match error {
                ValueFailure::Value(error) => ValueFailure::Value(error),
                ValueFailure::Caller(error) => error,
            })
    }
}
fn invalid() -> ValueError {
    ValueError::Invariant(ValueViolation::Structure)
}
fn child(
    binding: BorrowedTypedValueBinding<'_>,
    index: usize,
) -> Result<BorrowedTypedValueBinding<'_>, ValueError> {
    binding.child_at(index).ok_or_else(invalid)
}
fn reserve<T, E>(
    values: &mut Vec<T>,
    count: usize,
    growth: &mut Growth<'_, E>,
) -> Result<(), ValueFailure<E>> {
    if count <= values.capacity() {
        return Ok(());
    }
    let layout =
        Layout::array::<T>(count).map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
    growth(layout).map_err(ValueFailure::Caller)?;
    values
        .try_reserve_exact(count - values.len())
        .map_err(|source| ValueError::Allocation {
            requested: layout.size(),
            source,
        })?;
    Ok(())
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod byte_projection_tests;
