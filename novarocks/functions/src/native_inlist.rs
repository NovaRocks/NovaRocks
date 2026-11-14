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
//! Original IN state assembly and signed comparison. Scheduling stays outside
//! this module; every input has already been evaluated by its original shell.
//! Observation grants no memory. Raw entrypoints preserve complete data errors.
use crate::KernelFailure;
use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use std::convert::Infallible;
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

fn is_signed_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn signed_integer_value(array: &ArrayRef, row: usize) -> Result<i64, KernelFailure> {
    match array.data_type() {
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| contract("failed to downcast signed IN value to Int8Array"))?
            .value(row) as i64),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| contract("failed to downcast signed IN value to Int16Array"))?
            .value(row) as i64),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| contract("failed to downcast signed IN value to Int32Array"))?
            .value(row) as i64),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| contract("failed to downcast signed IN value to Int64Array"))?
            .value(row)),
        other => Err(contract(format_args!(
            "unsupported signed IN value type: {other:?}"
        ))),
    }
}

/// A supported signed pair only; None preserves the original next dispatch.
pub fn signed_equality_required(
    input: &ArrayRef,
    candidate: &ArrayRef,
) -> Result<Option<BooleanArray>, KernelFailure> {
    raw(|observe| signed_equality_observed(input, candidate, observe))
}
pub fn signed_equality_observed<E>(
    array: &ArrayRef,
    candidate: &ArrayRef,
    observer: &mut dyn FnMut(InObservation) -> Result<(), E>,
) -> Result<Option<BooleanArray>, InError<E>> {
    if !is_signed_integer_type(array.data_type())
        || !is_signed_integer_type(candidate.data_type())
        || (candidate.len() != 1 && candidate.len() != array.len())
    {
        return Ok(None);
    }
    // Original same-type full-column dispatch precedes its scalar/cross-width loop.
    if candidate.len() == array.len() && array.data_type() == candidate.data_type() {
        observe(observer, InObservation::OpaqueBoundary)?;
        let result = arrow_ord::cmp::eq(
            &array.as_ref() as &dyn arrow_array::Datum,
            &candidate.as_ref() as &dyn arrow_array::Datum,
        )
        .map_err(|error| InError::Operation(arrow_failure(error)))?;
        observe(observer, InObservation::OpaqueBoundary)?;
        return Ok(Some(result));
    }
    observe(observer, InObservation::OpaqueBoundary)?;
    let mut builder = BooleanBuilder::with_capacity(array.len());
    observe(observer, InObservation::OpaqueBoundary)?;
    for row in 0..array.len() {
        observe(observer, InObservation::Step)?;
        let candidate_row = row_index(row, candidate.len());
        if array.is_null(row) || candidate.is_null(candidate_row) {
            builder.append_null();
            continue;
        }
        builder.append_value(
            signed_integer_value(array, row).map_err(InError::Operation)?
                == signed_integer_value(candidate, candidate_row).map_err(InError::Operation)?,
        );
    }
    observe(observer, InObservation::OpaqueBoundary)?;
    let result = builder.finish();
    observe(observer, InObservation::OpaqueBoundary)?;
    Ok(Some(result))
}

/// Exactly checked signed and Utf8 native IN domains. Other comparison branches
/// remain explicit unsupported preparation shapes pending their original author.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNativeInListRecipe {
    negated: bool,
    source: novarocks_type_contract::FunctionValueType,
    candidates: novarocks_type_contract::FunctionParameterTypes,
    result: novarocks_type_contract::FunctionValueType,
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
            let supported = match source.logical_type() {
                ValueLogicalType::Physical => {
                    is_signed_integer_type(source.data_type())
                        || source.data_type() == &DataType::Utf8
                }
                ValueLogicalType::Json => source.data_type() == &DataType::Utf8,
                _ => false,
            };
            if !supported {
                return Err(crate::ComparisonPrepareError::Unsupported.into());
            }
            let mut nullable = source.nullable();
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
                    || candidate.data_type() != source.data_type()
                {
                    return Err(crate::ComparisonPrepareError::TypeMismatch.into());
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
    // JSON normalization can reject malformed logical JSON as required Data.
    // Unlike buffered row errors, that outer failure cannot be reordered or
    // skipped by Boolean control. Physical Utf8 remains ordinary byte equality.
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
enum InOperationFailure {
    Contract(novarocks_failure_contract::BoundedDetail),
    Arrow(arrow_schema::ArrowError),
}
impl std::fmt::Display for InOperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Arrow(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for InOperationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for InOperationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 381),
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => (FailureClass::Resource, 382),
            Self::Arrow(_) => (FailureClass::Invariant, 383),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
pub fn contract(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(InOperationFailure::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
pub fn arrow_failure(source: arrow_schema::ArrowError) -> KernelFailure {
    KernelFailure::from_domain(InOperationFailure::Arrow(source))
}
impl novarocks_failure_contract::OuterFailureProjection for InOperationFailure {}

#[cfg(test)]
mod tests;
