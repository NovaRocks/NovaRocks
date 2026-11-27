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
    AggregateStateAllocator, ComparisonAllocator, ComparisonOperator, CompositeComparisonResult,
    EvaluatedArgument, KernelControlObservation, KernelEvaluationControl, KernelFailure,
    PreparedComparisonRecipe, PreparedCompositeComparisonRecipe, RowDataError,
    UnaccountedAggregateStateAllocator,
};
use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, LargeBinaryArray};
use arrow_schema::DataType;
use novarocks_constant_contract::{ConstantBackingIdentity, ConstantValue};
use novarocks_type_contract::{ExpressionEffects, KeyRelation, arrow_data_types_exact_observed};
use std::sync::Arc;
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
/// One original invocation's root NULL/match/null-candidate continuation.
pub struct InRows {
    len: usize,
    has_null: Vec<bool>,
    matched: Vec<bool>,
    _host_hold: Option<OpaqueReservation>,
}
impl InRows {
    pub fn begin_in_observed<E>(
        len: usize,
        host: Option<&Arc<dyn AggregateStateAllocator>>,
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<Self, InError<E>> {
        if len == 0 {
            return Ok(Self {
                len,
                has_null: Vec::new(),
                matched: Vec::new(),
                _host_hold: None,
            });
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        let bytes = len
            .checked_mul(2 * size_of::<bool>())
            .ok_or(InError::Operation(KernelFailure::ResourceExhausted))?;
        let host_hold = host
            .map(|host| OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(bytes))
            .transpose()
            .map_err(InError::Operation)?;
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
            _host_hold: host_hold,
        })
    }
    pub fn needs_comparison(&self, parent: usize) -> Result<bool, KernelFailure> {
        self.matched
            .get(parent)
            .map(|matched| !matched)
            .ok_or_else(|| contract("IN comparison row is outside the original probe"))
    }

    pub fn record_equality(
        &mut self,
        parent: usize,
        result: Option<bool>,
    ) -> Result<(), KernelFailure> {
        let Some(matched) = self.matched.get_mut(parent) else {
            return Err(contract("IN equality row is outside the original probe"));
        };
        match result {
            Some(true) => *matched = true,
            Some(false) => {}
            None => self.has_null[parent] = true,
        }
        Ok(())
    }
    fn record_selected_equalities<E>(
        &mut self,
        equalities: &BooleanArray,
        parents: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        if equalities.len() != parents.len() {
            return Err(InError::Operation(contract(
                "IN equality length differs from its selected rows",
            )));
        }
        for (parent, result) in parents.iter().copied().zip(equalities.iter()) {
            observe(observer, InObservation::Step)?;
            self.record_equality(parent, result)
                .map_err(InError::Operation)?;
        }
        Ok(())
    }

    /// The grouped call records SQL-NULL probes before demanding candidates.
    /// Finishing uses that original row state and needs no second probe view.
    pub fn finish_group_observed<E>(
        self,
        truth_only: bool,
        failed_rows: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<BooleanArray, InError<E>> {
        if failed_rows.iter().any(|row| *row >= self.len)
            || failed_rows.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(InError::Operation(contract(
                "IN failure ordinals differ from the original probe",
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
            if *matched_row {
                builder.append_value(true);
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
            builder.append_value(false);
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

/// Comparison buffers are destroyed before their original reservations are returned.
/// Access borrows values and errors within the same compact candidate domain.
pub struct MembershipEqualities {
    array: BooleanArray,
    errors: Vec<RowDataError>,
    _reservation: Option<OpaqueReservation>,
    _errors_reservation: Option<OpaqueReservation>,
}
impl MembershipEqualities {
    pub fn iter(&self) -> impl Iterator<Item = Option<bool>> + '_ {
        self.array.iter()
    }

    /// Journaled errors use compact candidate ordinals. A row with an error
    /// has a NULL placeholder in the equality array.
    pub fn errors(&self) -> &[RowDataError] {
        &self.errors
    }

    pub fn apply_to_selected_observed<E>(
        &self,
        state: &mut InRows,
        parents: &[usize],
        observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
    ) -> Result<(), InError<E>> {
        state.record_selected_equalities(&self.array, parents, observer)
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
    Composite(PreparedCompositeComparisonRecipe),
    Json,
}

/// Flat comparisons selected for a real membership overload do not defer
/// equality to the fallible comparison phase. The caller still has to prove
/// the original argument occurrences independently.
pub fn selection_has_streaming_equality(selected: &crate::FunctionBindingSelection) -> bool {
    use novarocks_type_contract::{FunctionArgumentType, OperatorFunction};

    let [
        FunctionArgumentType::Value(probe),
        FunctionArgumentType::CandidateGroup { .. },
    ] = selected.argument_types.as_ref()
    else {
        return false;
    };
    selected.overload.as_str() == OperatorFunction::IN_LIST_OVERLOAD
        && !is_json(probe)
        && !PreparedCompositeComparisonRecipe::is_composite(probe)
}

fn is_json(source: &novarocks_type_contract::FunctionValueType) -> bool {
    source.logical_type() == novarocks_type_contract::ValueLogicalType::Json
        && source.data_type() == &DataType::LargeBinary
}

impl MembershipEquality {
    fn prepare_group(
        probe: &novarocks_type_contract::FunctionValueType,
        element: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        if probe.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
            && matches!(probe.data_type(), DataType::Float32 | DataType::Float64)
        {
            return PreparedComparisonRecipe::try_new_ieee_float_equality(probe, element, control)
                .map(Self::Comparison);
        }
        Self::prepare(probe, element, control)
    }

    fn prepare(
        source: &novarocks_type_contract::FunctionValueType,
        candidate: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        if is_json(source) {
            return Ok(Self::Json);
        }
        if PreparedCompositeComparisonRecipe::is_composite(source) {
            return PreparedCompositeComparisonRecipe::try_new(
                KeyRelation::Equal,
                source,
                candidate,
                control,
            )
            .map(Self::Composite);
        }
        PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, source, candidate, control)
            .map(Self::Comparison)
    }

    fn nullable_result(&self) -> bool {
        match self {
            Self::Composite(recipe) => recipe.nullable_result(),
            Self::Comparison(recipe) => recipe.nullable_result(),
            Self::Json => false,
        }
    }

    fn group_result_nullable(
        &self,
        probe: &novarocks_type_contract::FunctionValueType,
        element: &novarocks_type_contract::FunctionValueType,
    ) -> bool {
        probe.nullable() || element.nullable() || self.nullable_result()
    }

    fn effects(&self) -> ExpressionEffects {
        let mut effects = ExpressionEffects::PURE_VALUE;
        match self {
            Self::Comparison(_) => {}
            Self::Composite(recipe) => effects.may_raise_row_error = recipe.may_raise_row_error(),
            Self::Json => {
                // Key preparation and comparison follow the guarded fallible path.
                effects.may_raise_row_error = true;
                effects.may_raise_invocation_data = true;
            }
        }
        effects
    }
}

pub(crate) fn candidate_group_profile(
    probe: &novarocks_type_contract::FunctionValueType,
    element: &novarocks_type_contract::FunctionValueType,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(bool, ExpressionEffects), crate::ComparisonPrepareError> {
    let comparison = MembershipComparison::try_new(probe, element, control)?;
    Ok((comparison.result_nullable(element), comparison.effects()))
}

/// Equality for the common domain of a selected probe and candidate group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipComparison {
    source: novarocks_type_contract::FunctionValueType,
    equality: MembershipEquality,
}

/// Exact installed membership invocation. The original request and control
/// graph own each source occurrence. The kernel keeps only the common domain
/// and its equality preparation, never a second table of all candidates.
#[derive(Clone, Debug)]
pub struct PreparedMembership {
    call: Arc<crate::FunctionCallContract>,
    comparison: MembershipComparison,
}

impl PreparedMembership {
    pub(crate) fn from_call(
        call: Arc<crate::FunctionCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::FunctionSpecializationFailure> {
        use novarocks_type_contract::{
            ArgumentControl, FunctionArgumentType, OperatorFunction, ValueLogicalType,
        };
        let [
            FunctionArgumentType::Value(probe),
            FunctionArgumentType::CandidateGroup { element_type },
        ] = call.selected().argument_types.as_ref()
        else {
            return Err(contract("IN requires the selected probe and candidate group").into());
        };
        let crate::FunctionResultType::Scalar(result) = &call.selected().result_type else {
            return Err(contract("IN requires its exact Boolean result").into());
        };
        if call.function_id().as_str() != OperatorFunction::InList.function_id_text()
            || call.effects().argument_control != ArgumentControl::CandidateGroup
            || call.logical_argument_count() != 2
        {
            return Err(contract("IN preparation has a foreign call contract").into());
        }
        let comparison = MembershipComparison::try_new(probe, element_type, control).map_err(
            |error| match error.control_error() {
                Some(error) => crate::FunctionSpecializationFailure::Control(error),
                None => contract("IN has no selected equality implementation").into(),
            },
        )?;
        if result.logical_type() != ValueLogicalType::Physical
            || result.data_type() != &DataType::Boolean
            || result.nullable() != comparison.result_nullable(element_type)
        {
            return Err(contract("IN preparation has a foreign Boolean result").into());
        }
        Ok(Self { call, comparison })
    }

    pub fn call(&self) -> &crate::FunctionCallContract {
        &self.call
    }

    pub fn comparison(&self) -> &MembershipComparison {
        &self.comparison
    }

    pub fn comparison_effects(&self) -> ExpressionEffects {
        self.comparison.effects()
    }
}

impl MembershipComparison {
    pub fn try_new(
        probe: &novarocks_type_contract::FunctionValueType,
        element: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let same_domain = probe.same_physical_binding_ignoring_root_nullability_observed::<
            crate::ComparisonPrepareError,
        >(element, || work.step().map_err(Into::into))?;
        work.finish()?;
        if !same_domain {
            return Err(crate::ComparisonPrepareError::TypeMismatch);
        }
        let equality = MembershipEquality::prepare_group(probe, element, control)?;
        Ok(Self {
            source: probe.clone(),
            equality,
        })
    }

    fn result_nullable(&self, element: &novarocks_type_contract::FunctionValueType) -> bool {
        self.equality.group_result_nullable(&self.source, element)
    }

    fn effects(&self) -> ExpressionEffects {
        self.equality.effects()
    }

    /// These equality kernels borrow their actual values and allocate nothing.
    /// Composite comparison and JSON key preparation require all candidates to
    /// produce values for the same row before comparison starts.
    pub fn streaming_equality(&self) -> Option<&PreparedComparisonRecipe> {
        match &self.equality {
            MembershipEquality::Comparison(recipe) => Some(recipe),
            MembershipEquality::Composite(_) | MembershipEquality::Json => None,
        }
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
                    "IN comparison operand differs from its selected carrier",
                )));
            }
        }
        observe(observer, InObservation::OpaqueBoundary)?;
        let composite_nulls = matches!(
            &self.equality,
            MembershipEquality::Composite(recipe)
                if recipe.nullable_result() || recipe.may_raise_row_error()
        );
        let has_nulls = !input.is_empty()
            && (input.null_count() != 0
                || candidate.null_count() != 0
                || input.data_type() == &DataType::Null
                || composite_nulls);
        let bytes = boolean_buffer_bytes(input.len(), has_nulls).map_err(InError::Operation)?;
        let reservation = host
            .as_ref()
            .filter(|_| bytes != 0)
            .map(|host| OpaqueRetainedCharge::try_new(Arc::clone(host))?.reserve_operation(bytes))
            .transpose()
            .map_err(InError::Operation)?;
        let mut builder = BooleanBuilder::with_capacity(input.len());
        let mut errors_reservation = None;
        let mut errors = Vec::new();
        let left = EvaluatedArgument::Column(input);
        let right = if candidate.len() == 1 {
            EvaluatedArgument::Scalar(candidate)
        } else {
            EvaluatedArgument::Column(candidate)
        };
        match &self.equality {
            MembershipEquality::Comparison(recipe) => {
                append_equalities_observed(&mut builder, input.len(), observer, |row| {
                    recipe.compare_rows(left, row, row, right, row, row, &control)
                })?;
            }
            MembershipEquality::Composite(recipe) => {
                let allocator = host
                    .as_ref()
                    .filter(|_| recipe.requires_allocator() && !input.is_empty())
                    .map(|host| ComparisonAllocator::try_new(Arc::clone(host)))
                    .transpose()
                    .map_err(InError::Operation)?;
                let mut journal_slots = 0usize;
                for row in 0..input.len() {
                    observe(observer, InObservation::Step)?;
                    let outcome = recipe
                        .compare_rows(
                            left,
                            row,
                            row,
                            right,
                            row,
                            row,
                            allocator.as_ref(),
                            &control,
                        )
                        .map_err(InError::Operation)?;
                    match outcome {
                        CompositeComparisonResult::Value(value) => builder.append_option(value),
                        CompositeComparisonResult::Indeterminate => {
                            if errors.len() == journal_slots {
                                // Grow for actual failures, keeping the old charge
                                // through replacement allocation.
                                let slots = journal_slots
                                    .saturating_mul(2)
                                    .max(1)
                                    .min(errors.len() + (input.len() - row));
                                let bytes = slots
                                    .checked_mul(
                                        size_of::<RowDataError>()
                                            + crate::MAX_ROW_ERROR_MESSAGE_BYTES,
                                    )
                                    .ok_or(InError::Operation(KernelFailure::ResourceExhausted))?;
                                observe(observer, InObservation::OpaqueBoundary)?;
                                let new_reservation = host
                                    .as_ref()
                                    .map(|host| {
                                        OpaqueRetainedCharge::try_new(Arc::clone(host))?
                                            .reserve_operation(bytes)
                                    })
                                    .transpose()
                                    .map_err(InError::Operation)?;
                                errors
                                    .try_reserve_exact(slots - errors.len())
                                    .map_err(|_| {
                                        InError::Operation(KernelFailure::ResourceExhausted)
                                    })?;
                                errors_reservation = new_reservation;
                                journal_slots = slots;
                            }
                            let error = outcome.row_error(row).ok_or_else(|| {
                                InError::Operation(contract("IN lost an indeterminate comparison"))
                            })?;
                            errors.push(error);
                            builder.append_null();
                        }
                    }
                }
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
                        (Some(left), Some(right)) => Some(left.equals_observed(&right, &control)?),
                        _ => None,
                    })
                })?;
            }
        }
        control.checkpoint(0).map_err(InError::Operation)?;
        observe(observer, InObservation::OpaqueBoundary)?;
        Ok(MembershipEqualities {
            array: builder.finish(),
            errors,
            _reservation: reservation,
            _errors_reservation: errors_reservation,
        })
    }

    pub fn uses_json_keys(&self) -> bool {
        matches!(self.equality, MembershipEquality::Json)
    }

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
                "IN JSON candidate differs from its selected membership comparison",
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
            errors: Vec::new(),
            _errors_reservation: None,
            _reservation: reservation,
        })
    }

    pub fn requires_comparison_allocator(&self) -> bool {
        match &self.equality {
            MembershipEquality::Json => true,
            MembershipEquality::Composite(recipe) => recipe.requires_allocator(),
            MembershipEquality::Comparison(_) => false,
        }
    }

    pub fn source_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.source
    }
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
