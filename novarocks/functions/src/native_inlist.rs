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
//! IN prepares its equality domain and assembles the ordered membership result.
//! Scheduling stays with the caller, which supplies materialized, error-free inputs.
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::json_canonical::{JsonCanonicalKey, key_allocator, normalize_json_key_in};
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::selected_copy::{CopyError, fixed_interleave_extent};
use crate::{
    AggregateStateAllocator, ComparisonOperator, EvaluatedArgument, KernelControlObservation,
    KernelEvaluationControl, KernelFailure, PreparedComparisonRecipe,
    UnaccountedAggregateStateAllocator,
};
use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, LargeBinaryArray};
use arrow_schema::DataType;
use novarocks_constant_contract::{ConstantBackingIdentity, ConstantValue};
use novarocks_type_contract::arrow_data_types_exact_observed;
use std::{convert::Infallible, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub enum InObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum InError<E> {
    Operation(KernelFailure),
    Host(E),
}
fn observe<E>(
    observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    event: InObservation,
) -> Result<(), InError<E>> {
    observer(event).map_err(InError::Host)
}
fn raw<T>(
    f: impl FnOnce(
        &mut dyn FnMut(InObservation) -> Result<(), Infallible>,
    ) -> Result<T, InError<Infallible>>,
) -> Result<T, KernelFailure> {
    match f(&mut |_| Ok(())) {
        Ok(value) => Ok(value),
        Err(InError::Operation(message)) => Err(message),
        Err(InError::Host(never)) => match never {},
    }
}
/// One original invocation's root NULL/match/null-candidate continuation.
pub struct InRows {
    len: usize,
    has_null: Vec<bool>,
    matched: Vec<bool>,
}
impl InRows {
    pub fn begin_required(input: &ArrayRef) -> Result<Self, KernelFailure> {
        raw(|observe| Self::begin_observed(input, observe))
    }
    pub fn begin_observed<E>(
        input: &ArrayRef,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<Self, InError<E>> {
        let len = input.len();
        if len == 0 {
            return Ok(Self {
                len,
                has_null: Vec::new(),
                matched: Vec::new(),
            });
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        // Original two independent std allocations; no new memory wallet.
        let mut has_null = Vec::new();
        has_null.try_reserve_exact(len).map_err(|source| {
            InError::Operation(KernelFailure::from_domain(
                crate::allocation_failure::AllocationRefusal::new("IN null state", source),
            ))
        })?;
        has_null.resize(len, false);
        let mut matched = Vec::new();
        matched.try_reserve_exact(len).map_err(|source| {
            InError::Operation(KernelFailure::from_domain(
                crate::allocation_failure::AllocationRefusal::new("IN match state", source),
            ))
        })?;
        matched.resize(len, false);
        observe(observer, InObservation::OpaqueBoundary)?;
        Ok(Self {
            len,
            has_null,
            matched,
        })
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn candidate_nulls_required(&mut self, candidate: &ArrayRef) -> Result<(), KernelFailure> {
        raw(|observe| self.candidate_nulls_observed(candidate, observe))
    }
    pub fn candidate_nulls_observed<E>(
        &mut self,
        candidate: &ArrayRef,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        self.candidate_nulls_projection_observed(candidate, self.len, |row| row, observer)
    }
    /// Parent ordinals were established by the host's checked selected domain.
    pub fn candidate_nulls_selected_observed<E>(
        &mut self,
        candidate: &ArrayRef,
        parents: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        self.candidate_nulls_projection_observed(
            candidate,
            parents.len(),
            |row| parents[row],
            observer,
        )
    }
    fn candidate_nulls_projection_observed<E>(
        &mut self,
        candidate: &ArrayRef,
        len: usize,
        parent: impl Fn(usize) -> usize,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        if candidate.len() != 1 && candidate.len() != len {
            return Err(InError::Operation(contract(format_args!(
                "IN predicate value length mismatch: input has {}, value has {}",
                len,
                candidate.len()
            ))));
        }
        for row in 0..len {
            observe(observer, InObservation::Step)?;
            if parent(row) >= self.len {
                return Err(InError::Operation(contract(
                    "IN parent ordinal is outside its state",
                )));
            }
            if candidate.is_null(row_index(row, candidate.len())) {
                self.has_null[parent(row)] = true;
            }
        }
        Ok(())
    }
    pub fn equalities_required(&mut self, equalities: &BooleanArray) -> Result<(), KernelFailure> {
        raw(|observe| self.equalities_observed(equalities, observe))
    }
    pub fn equalities_observed<E>(
        &mut self,
        equalities: &BooleanArray,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        self.equalities_projection_observed(equalities, self.len, |row| row, observer)
    }
    pub fn equalities_selected_observed<E>(
        &mut self,
        equalities: &BooleanArray,
        parents: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        self.equalities_projection_observed(equalities, parents.len(), |row| parents[row], observer)
    }
    fn equalities_projection_observed<E>(
        &mut self,
        equalities: &BooleanArray,
        len: usize,
        parent: impl Fn(usize) -> usize,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        if equalities.len() != len {
            return Err(InError::Operation(contract(
                "IN equality length differs from its selected rows",
            )));
        }
        for row in 0..len {
            observe(observer, InObservation::Step)?;
            let parent = parent(row);
            if parent >= self.len {
                return Err(InError::Operation(contract(
                    "IN equality parent is outside its state",
                )));
            }
            if equalities.is_null(row) {
                self.has_null[parent] = true;
            } else if equalities.value(row) {
                self.matched[parent] = true;
            }
        }
        Ok(())
    }
    pub fn finish_required(
        self,
        input: &ArrayRef,
        negated: bool,
    ) -> Result<BooleanArray, KernelFailure> {
        raw(|observe| self.finish_observed(input, negated, observe))
    }
    pub fn finish_observed<E>(
        self,
        input: &ArrayRef,
        negated: bool,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<BooleanArray, InError<E>> {
        self.finish_projection_observed(input, negated, false, &[], observer)
    }
    pub fn finish_selected_observed<E>(
        self,
        input: &ArrayRef,
        negated: bool,
        truth_only: bool,
        failed_rows: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<BooleanArray, InError<E>> {
        self.finish_projection_observed(input, negated, truth_only, failed_rows, observer)
    }
    fn finish_projection_observed<E>(
        self,
        input: &ArrayRef,
        negated: bool,
        truth_only: bool,
        failed_rows: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<BooleanArray, InError<E>> {
        if input.len() != self.len
            || failed_rows.iter().any(|row| *row >= self.len)
            || failed_rows.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(InError::Operation(contract(
                "IN finish input or failure ordinals differ from its state",
            )));
        }
        if self.len == 0 {
            return Ok(BooleanArray::from(Vec::<bool>::new()));
        }
        let mut failed = failed_rows.iter().peekable();
        observe(observer, InObservation::OpaqueBoundary)?;
        let mut builder = BooleanBuilder::with_capacity(self.len);
        observe(observer, InObservation::OpaqueBoundary)?;
        for (row, matched_row) in self.matched.iter().enumerate() {
            observe(observer, InObservation::Step)?;
            if failed.peek().is_some_and(|&&failed| failed == row) {
                failed.next();
                builder.append_null();
                continue;
            }
            if input.is_null(row) || matches!(input.data_type(), DataType::Null) {
                if truth_only {
                    builder.append_value(false);
                } else {
                    builder.append_null();
                }
                continue;
            }
            if *matched_row {
                builder.append_value(!negated);
                continue;
            }
            if self.has_null[row] {
                if truth_only {
                    builder.append_value(false);
                } else {
                    builder.append_null();
                }
                continue;
            }
            builder.append_value(negated);
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        let result = builder.finish();
        observe(observer, InObservation::OpaqueBoundary)?;
        Ok(result)
    }
}
fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

/// Comparison buffers are destroyed before their original reservation is returned.
/// Access borrows the values without publishing an independently owned Arrow array.
pub struct MembershipEqualities {
    array: BooleanArray,
    _reservation: Option<OpaqueReservation>,
}
impl MembershipEqualities {
    pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
        self.array.iter()
    }
    pub fn apply_to_selected_observed<E>(
        &self,
        state: &mut InRows,
        parents: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        state.equalities_selected_observed(&self.array, parents, observer)
    }
}

fn boolean_buffer_bytes(rows: usize, has_nulls: bool) -> Result<usize, KernelFailure> {
    fixed_interleave_extent(&DataType::Boolean, rows).map_err(CopyError::into_kernel)?;
    let one = rows
        .div_ceil(8)
        .checked_next_multiple_of(64)
        .ok_or(KernelFailure::ResourceExhausted)?;
    one.checked_mul(1 + usize::from(has_nulls))
        .ok_or(KernelFailure::ResourceExhausted)
}

fn append_equalities_observed<E>(
    builder: &mut BooleanBuilder,
    rows: usize,
    observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    mut equal: impl FnMut(usize) -> Result<Option<bool>, KernelFailure>,
) -> Result<(), InError<E>> {
    for row in 0..rows {
        observe(observer, InObservation::Step)?;
        builder.append_option(equal(row).map_err(InError::Operation)?);
    }
    Ok(())
}

/// Each selected probe is normalized once in its original activation frame.
pub struct JsonProbeKeys {
    values: ArrayRef,
    keys: allocator_api2::vec::Vec<Option<Option<JsonCanonicalKey>>, HostAggregateAllocator>,
}

impl JsonProbeKeys {
    pub fn prepare(
        values: &ArrayRef,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        json_array(values)?;
        let allocator = key_allocator(json_host(host))?;
        let mut keys = allocator_api2::vec::Vec::new_in(allocator.clone());
        keys.try_reserve_exact(values.len())
            .map_err(|_| allocator.take_failure())?;
        for _ in 0..values.len() {
            control.checkpoint(1)?;
            keys.push(None);
        }
        Ok(Self {
            values: Arc::clone(values),
            keys,
        })
    }

    fn prepare_at(
        &mut self,
        parent: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<&Option<JsonCanonicalKey>, KernelFailure> {
        let allocator = self.keys.allocator().clone();
        let key = self
            .keys
            .get_mut(parent)
            .ok_or_else(|| contract("IN JSON parent exceeds the original probe"))?;
        if key.is_none() {
            *key = Some(json_key_at(
                json_array(&self.values)?,
                parent,
                &allocator,
                control,
            )?);
        }
        Ok(key.as_ref().expect("prepared original probe"))
    }
}

type ConstantKeyId = (ConstantBackingIdentity, u32);
type ConstantKeys = hashbrown::HashMap<
    ConstantKeyId,
    Option<JsonCanonicalKey>,
    std::hash::RandomState,
    HostAggregateAllocator,
>;

/// Lazily admitted by the original expression root instance when it first
/// demands a candidate backed by an exact immutable constant.
#[derive(Default)]
pub struct JsonConstantKeyCache {
    keys: Option<ConstantKeys>,
}

impl JsonConstantKeyCache {
    fn get_or_prepare(
        &mut self,
        constant: &ConstantValue,
        values: &LargeBinaryArray,
        allocator: &HostAggregateAllocator,
        control: &dyn KernelEvaluationControl,
    ) -> Result<&Option<JsonCanonicalKey>, KernelFailure> {
        let keys = self.keys.get_or_insert_with(|| {
            ConstantKeys::with_hasher_in(std::hash::RandomState::new(), allocator.clone())
        });
        let id = (constant.pool().backing_identity(), constant.ordinal());
        control.checkpoint(1)?;
        if !keys.contains_key(&id) {
            let key = json_key_at(values, 0, allocator, control)?;
            keys.try_reserve(1)
                .map_err(|_| keys.allocator().take_failure())?;
            keys.insert(id, key);
        }
        Ok(keys.get(&id).expect("cached original constant"))
    }
}

fn json_host(host: Option<Arc<dyn AggregateStateAllocator>>) -> Arc<dyn AggregateStateAllocator> {
    host.unwrap_or_else(|| Arc::new(UnaccountedAggregateStateAllocator))
}

fn json_array(values: &ArrayRef) -> Result<&LargeBinaryArray, KernelFailure> {
    values
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| contract("IN JSON operand is not LargeBinary"))
}

fn json_key_at(
    values: &LargeBinaryArray,
    row: usize,
    allocator: &HostAggregateAllocator,
    control: &dyn KernelEvaluationControl,
) -> Result<Option<JsonCanonicalKey>, KernelFailure> {
    if values.is_null(row) {
        return Ok(None);
    }
    normalize_json_key_in(values.value(row), allocator.clone(), control).map(Some)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MembershipEquality {
    Comparison(PreparedComparisonRecipe),
    Json,
}

/// Common exact typed domain and membership equality for an IN expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNativeInListRecipe {
    negated: bool,
    source: novarocks_type_contract::FunctionValueType,
    candidates: novarocks_type_contract::FunctionParameterTypes,
    result: novarocks_type_contract::FunctionValueType,
    equality: MembershipEquality,
}
/// Original comparison refusal and actual construction cause stay distinct.
#[derive(Debug)]
pub enum NativeInListCreationFailure<H> {
    Control(novarocks_type_contract::CompileControlError),
    Creator(H),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Owner(crate::ComparisonPrepareError),
}
impl<H: std::fmt::Display> std::fmt::Display for NativeInListCreationFailure<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "IN recipe construction resource: {e:?}"),
            Self::Owner(e) => e.fmt(f),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for NativeInListCreationFailure<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Resource(_) => None,
            Self::Owner(e) => Some(e),
        }
    }
}
impl<H> From<crate::ComparisonPrepareError> for NativeInListCreationFailure<H> {
    fn from(e: crate::ComparisonPrepareError) -> Self {
        match e.control_error() {
            Some(cause) => Self::Control(cause),
            None => Self::Owner(e),
        }
    }
}
impl<H> From<novarocks_type_contract::CompileControlError> for NativeInListCreationFailure<H> {
    fn from(e: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for NativeInListCreationFailure<H>
{
    fn from(e: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}
fn inlist_request<H>(
    creation: &mut Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    layout: std::alloc::Layout,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), NativeInListCreationFailure<H>> {
    if let Some(creation) = creation.as_mut() {
        work.flush()?;
        creation
            .request_layout(layout)
            .map_err(NativeInListCreationFailure::Creator)?;
    }
    Ok(())
}
fn inlist_pure_failure(
    error: NativeInListCreationFailure<std::convert::Infallible>,
) -> crate::ComparisonPrepareError {
    match error {
        NativeInListCreationFailure::Control(error) => {
            crate::ComparisonPrepareError::Control(error)
        }
        NativeInListCreationFailure::Owner(error) => error,
        NativeInListCreationFailure::Resource(_) => crate::ComparisonPrepareError::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ),
        NativeInListCreationFailure::Creator(never) => match never {},
    }
}

impl PreparedNativeInListRecipe {
    pub fn try_new(
        negated: bool,
        source: &novarocks_type_contract::FunctionValueType,
        candidates: &[&novarocks_type_contract::FunctionValueType],
        result: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        Self::try_new_core::<true, std::convert::Infallible, _>(
            negated,
            source,
            candidates.iter().copied(),
            result,
            None,
            control,
        )
        .map(|recipe| recipe.expect("recipe publication requested"))
        .map_err(inlist_pure_failure)
    }
    pub fn try_new_with_creator_in<H>(
        negated: bool,
        source: &novarocks_type_contract::FunctionValueType,
        candidates: &[&novarocks_type_contract::FunctionValueType],
        result: &novarocks_type_contract::FunctionValueType,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, NativeInListCreationFailure<H>> {
        Self::try_new_core::<true, H, _>(
            negated,
            source,
            candidates.iter().copied(),
            result,
            Some(creation),
            control,
        )
        .map(|recipe| recipe.expect("recipe publication requested"))
    }

    /// Validate a static complete profile before any invocation exists.
    /// This borrows the original types and does not publish a candidate owner
    /// or invent an expression occurrence solely to obtain an effects value.
    pub fn validate_borrowed_profile<'a>(
        source: &novarocks_type_contract::FunctionValueType,
        candidates: impl ExactSizeIterator<Item = &'a novarocks_type_contract::FunctionValueType>,
        result: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), crate::ComparisonPrepareError> {
        Self::try_new_core::<false, std::convert::Infallible, _>(
            false, source, candidates, result, None, control,
        )
        .map(|_| ())
        .map_err(inlist_pure_failure)
    }

    /// Validate the same original complete source/candidate/result profile
    /// without publishing a recipe or its otherwise discarded candidate owner.
    /// Actual prepared instances still consume the original owning constructor.
    pub fn refine_borrowed_profile<'a>(
        source: &novarocks_type_contract::FunctionValueType,
        candidates: impl ExactSizeIterator<Item = &'a novarocks_type_contract::FunctionValueType>,
        result: &novarocks_type_contract::FunctionValueType,
        context: novarocks_type_contract::ExpressionEffectContext,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<crate::ScopedExpressionEffects, crate::ComparisonPrepareError> {
        Self::try_new_core::<false, std::convert::Infallible, _>(
            false, source, candidates, result, None, control,
        )
        .map(|_| inlist_own_effects(source, context))
        .map_err(inlist_pure_failure)
    }

    fn try_new_core<'a, const PUBLISH: bool, H, I>(
        negated: bool,
        source: &novarocks_type_contract::FunctionValueType,
        candidates: I,
        result: &novarocks_type_contract::FunctionValueType,
        mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<Self>, NativeInListCreationFailure<H>>
    where
        I: ExactSizeIterator<Item = &'a novarocks_type_contract::FunctionValueType>,
    {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome: Result<Option<Self>, NativeInListCreationFailure<H>> = (|| {
            crate::kernel_input::validate_type_observed(source, &mut work)
                .map_err(crate::ComparisonPrepareError::Kernel)?;
            crate::kernel_input::validate_type_observed(result, &mut work)
                .map_err(crate::ComparisonPrepareError::Kernel)?;
            let mut comparison_right = source;
            let mut nullable = source.nullable() || source.data_type() == &DataType::Null;
            if candidates.len() != 0 {
                let layout = std::alloc::Layout::array::<novarocks_type_contract::FunctionValueType>(
                    candidates.len(),
                ).map_err(|_| NativeInListCreationFailure::Resource(
                    novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?;
                if PUBLISH {
                    inlist_request(&mut creation, layout, &mut work)?;
                }
            }
            let mut types = if PUBLISH {
                Some(Vec::with_capacity(candidates.len()))
            } else {
                None
            };
            for candidate in candidates {
                crate::kernel_input::validate_type_observed(candidate, &mut work)
                    .map_err(crate::ComparisonPrepareError::Kernel)?;
                if candidate.logical_type() != source.logical_type()
                    || !arrow_data_types_exact_observed::<crate::ComparisonPrepareError>(
                        source.data_type(),
                        candidate.data_type(),
                        || work.step().map_err(Into::into),
                    )?
                {
                    return Err(crate::ComparisonPrepareError::TypeMismatch.into());
                }
                // All candidates have the same domain. Use an existing nullable
                // declaration on the right if any candidate can produce SQL NULL.
                if candidate.nullable() && !comparison_right.nullable() {
                    comparison_right = candidate;
                }
                nullable |= candidate.nullable();
                work.flush()?;
                if let Some(types) = types.as_mut() {
                    types.push(candidate.clone());
                }
                work.step()?;
            }
            if result.logical_type() != ValueLogicalType::Physical
                || result.data_type() != &DataType::Boolean
                || (nullable && !result.nullable())
            {
                return Err(crate::ComparisonPrepareError::TypeMismatch.into());
            }
            work.flush()?;
            let equality = if source.logical_type() == ValueLogicalType::Json
                && source.data_type() == &DataType::LargeBinary
            {
                MembershipEquality::Json
            } else {
                MembershipEquality::Comparison(PreparedComparisonRecipe::try_new(
                    ComparisonOperator::Eq,
                    source,
                    comparison_right,
                    control,
                )?)
            };
            let Some(types) = types else {
                return Ok(None);
            };
            let facts = novarocks_type_contract::owned_resources::vec::box_facts(&types)
                .map_err(|e| match e {
                    novarocks_type_contract::ControlResourceError::Control(e) =>
                        NativeInListCreationFailure::Control(e),
                    novarocks_type_contract::ControlResourceError::SourceModel(_) =>
                        NativeInListCreationFailure::Resource(
                            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
                })?;
            if let Some(layout) = facts.requested_backing {
                inlist_request(&mut creation, layout, &mut work)?;
            }
            let items = types.into_boxed_slice();
            let candidates = if let Some(creation) = creation.as_mut() {
                let storage = creation.storage_lifetime();
                novarocks_type_contract::FunctionParameterTypes::try_from_boxed_observed(
                    items,
                    storage,
                    |layout| {
                        work.flush()?;
                        creation
                            .request_layout(layout)
                            .map_err(NativeInListCreationFailure::Creator)
                    },
                )?
            } else {
                novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(items)
            };
            Ok(Some(Self {
                negated,
                source: source.clone(),
                candidates,
                result: result.clone(),
                equality,
            }))
        })();
        if matches!(
            outcome,
            Err(NativeInListCreationFailure::Control(_)
                | NativeInListCreationFailure::Creator(_)
                | NativeInListCreationFailure::Resource(_))
        ) {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    /// Compare one materialized operand pair over the same compact row domain.
    /// A candidate with one physical row may be broadcast across the input.
    pub fn membership_equalities_observed<E>(
        &self,
        input: &ArrayRef,
        candidate: &ArrayRef,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<MembershipEqualities, InError<E>> {
        if candidate.len() != 1 && candidate.len() != input.len() {
            return Err(InError::Operation(contract(
                "IN candidate length differs from input",
            )));
        }
        let control = KernelControlObservation::new(control);
        control.checkpoint(0).map_err(InError::Operation)?;
        for values in [input, candidate] {
            if !arrow_data_types_exact_observed(self.source.data_type(), values.data_type(), || {
                control.checkpoint(1)
            })
            .map_err(InError::Operation)?
            {
                return Err(InError::Operation(contract(
                    "IN comparison operand differs from the recipe carrier",
                )));
            }
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        let has_nulls = !input.is_empty()
            && (input.null_count() != 0
                || candidate.null_count() != 0
                || input.data_type() == &DataType::Null);
        let bytes = boolean_buffer_bytes(input.len(), has_nulls).map_err(InError::Operation)?;
        let reservation = host
            .as_ref()
            .filter(|_| bytes != 0)
            .map(|host| OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(bytes))
            .transpose()
            .map_err(InError::Operation)?;
        let mut builder = BooleanBuilder::with_capacity(input.len());
        match &self.equality {
            MembershipEquality::Comparison(recipe) => {
                let left = EvaluatedArgument::Column(input);
                let right = if candidate.len() == 1 {
                    EvaluatedArgument::Scalar(candidate)
                } else {
                    EvaluatedArgument::Column(candidate)
                };
                append_equalities_observed(&mut builder, input.len(), observer, |row| {
                    recipe.compare_rows(left, row, row, right, row, row, &control)
                })?;
            }
            MembershipEquality::Json => {
                let left = input
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        InError::Operation(contract("IN JSON source is not LargeBinary"))
                    })?;
                let right = candidate
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        InError::Operation(contract("IN JSON candidate is not LargeBinary"))
                    })?;
                // Direct evaluation outside a Task uses the process heap as
                // before. The Task caller supplies its actual Frame allocator.
                let host = host.unwrap_or_else(|| Arc::new(UnaccountedAggregateStateAllocator));
                let normalize = |values: &LargeBinaryArray, row| {
                    if values.is_null(row) {
                        Ok(None)
                    } else {
                        crate::json_canonical::normalize_json_key(
                            values.value(row),
                            Arc::clone(&host),
                            &control,
                        )
                        .map(Some)
                    }
                };
                append_equalities_observed(&mut builder, input.len(), observer, |row| {
                    // The demanded candidate is evaluated even when the probe is SQL NULL.
                    let left = normalize(left, row)?;
                    let right = normalize(right, row_index(row, candidate.len()))?;
                    Ok(match (left, right) {
                        (Some(left), Some(right)) => Some(left.as_bytes() == right.as_bytes()),
                        _ => None,
                    })
                })?;
            }
        }
        control.checkpoint(0).map_err(InError::Operation)?;
        observe(observer, InObservation::OpaqueBoundary)?;
        Ok(MembershipEqualities {
            array: builder.finish(),
            _reservation: reservation,
        })
    }

    pub fn uses_json_keys(&self) -> bool {
        matches!(self.equality, MembershipEquality::Json)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep the exact candidate, parent positions, optional constant proof and original control explicit"
    )]
    pub fn json_equalities_observed<E>(
        &self,
        probe: &mut JsonProbeKeys,
        candidate: &ArrayRef,
        parents: &[usize],
        proven_constant: Option<(&ConstantValue, &mut JsonConstantKeyCache)>,
        host: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<MembershipEqualities, InError<E>> {
        if !self.uses_json_keys() || (candidate.len() != parents.len() && candidate.len() != 1) {
            return Err(InError::Operation(contract(
                "IN JSON candidate differs from its exact membership recipe",
            )));
        }
        let candidate = json_array(candidate).map_err(InError::Operation)?;
        let control = KernelControlObservation::new(control);
        control.checkpoint(0).map_err(InError::Operation)?;
        let mut nullable = candidate.null_count() != 0;
        for &parent in parents {
            observe(observer, InObservation::Step)?;
            let key = probe
                .prepare_at(parent, &control)
                .map_err(InError::Operation)?;
            nullable |= key.is_none();
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        let bytes = boolean_buffer_bytes(parents.len(), nullable).map_err(InError::Operation)?;
        let reservation = host
            .as_ref()
            .filter(|_| bytes != 0)
            .map(|host| OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(bytes))
            .transpose()
            .map_err(InError::Operation)?;
        let mut builder = BooleanBuilder::with_capacity(parents.len());
        if !parents.is_empty() {
            let allocator = key_allocator(json_host(host)).map_err(InError::Operation)?;
            let direct;
            let known = if let Some((constant, cache)) = proven_constant {
                Some(
                    cache
                        .get_or_prepare(constant, candidate, &allocator, &control)
                        .map_err(InError::Operation)?,
                )
            } else if candidate.len() == 1 {
                direct =
                    json_key_at(candidate, 0, &allocator, &control).map_err(InError::Operation)?;
                Some(&direct)
            } else {
                None
            };
            append_equalities_observed(&mut builder, parents.len(), observer, |row| {
                // The demanded right operand is normalized even for SQL NULL
                // probes. A hit does not alter any later candidate's demand.
                let dynamic;
                let right = if let Some(key) = known {
                    key.as_ref()
                } else {
                    dynamic = json_key_at(candidate, row, &allocator, &control)?;
                    dynamic.as_ref()
                };
                let left = probe.keys[parents[row]].as_ref().and_then(Option::as_ref);
                match (left, right) {
                    (Some(left), Some(right)) => left.equals_observed(right, &control).map(Some),
                    _ => Ok(None),
                }
            })?;
        }
        control.checkpoint(0).map_err(InError::Operation)?;
        observe(observer, InObservation::OpaqueBoundary)?;
        Ok(MembershipEqualities {
            array: builder.finish(),
            _reservation: reservation,
        })
    }

    pub fn negated(&self) -> bool {
        self.negated
    }
    pub fn source_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.source
    }
    pub fn candidate_types(&self) -> &[novarocks_type_contract::FunctionValueType] {
        &self.candidates
    }
    pub fn result_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.result
    }
    pub fn own_effects(
        &self,
        context: novarocks_type_contract::ExpressionEffectContext,
    ) -> crate::ScopedExpressionEffects {
        inlist_own_effects(&self.source, context)
    }
}
fn inlist_own_effects(
    source: &novarocks_type_contract::FunctionValueType,
    context: novarocks_type_contract::ExpressionEffectContext,
) -> crate::ScopedExpressionEffects {
    // Keep the original guarded demand for JSON membership. Acquiring and
    // comparing keys can fail in the original host before a result exists.
    let json = source.logical_type() == novarocks_type_contract::ValueLogicalType::Json;
    crate::ScopedExpressionEffects::primitive(
        context,
        novarocks_type_contract::ExpressionEffects {
            may_raise_row_error: json,
            may_raise_invocation_data: json,
            ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
        },
    )
}

#[derive(Debug)]
struct InOperationFailure(novarocks_failure_contract::BoundedDetail);

impl std::fmt::Display for InOperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for InOperationFailure {}
impl novarocks_failure_contract::FailureProjection for InOperationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(381).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for InOperationFailure {}

fn contract(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(InOperationFailure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[cfg(test)]
mod tests;
