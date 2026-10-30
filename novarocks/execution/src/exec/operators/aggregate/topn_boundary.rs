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

//! Aggregate-owned selection state for native TopN ordered boundaries.
//!
//! The state keeps only the best `N` projected order tuples. Complete aggregate
//! group identity is supplied by `KeyIndex`'s monotonically allocated group id,
//! so equal projections from different groups retain their multiplicity without
//! retaining every full group key. Service routing, logical versions, producer
//! sessions, and publication policy intentionally remain outside this module.

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
#[cfg(test)]
use std::sync::Arc;

use crate::exec::hash_table::allocation::AggregateVec;
use crate::runtime_filter::contribution::{
    ControlledOrderedTupleError, OrderedScalarRef, OrderedTupleError, RuntimeOrderContract,
};
use crate::runtime_filter::{OrderedTopNAllocationHost, OwnedOrderKey};
use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
#[cfg(test)]
use arrow::{array::DictionaryArray, datatypes::Int32Type};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};

#[cfg(test)]
use crate::runtime_filter::contribution::{OrderedScalar, OrderedTuple};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AggregateTopNCandidate<K> {
    tuple: K,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AggregateTopNBoundarySnapshot {
    NotReady,
    Ready { nth_best_inclusive: OrderedTuple },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateTopNBoundaryError {
    UnsupportedKeyArity {
        actual: usize,
    },
    NonOrderedContract,
    UnsupportedOrderKeyType {
        actual: novarocks_failure_contract::BoundedDetail,
    },
    GroupIdentityOutOfSequence {
        expected: usize,
        actual: usize,
    },
    CandidateContractMismatch(OrderedTupleError),
    MissingGroupKey {
        ordinal: usize,
        actual: usize,
    },
    CandidateRowOutOfBounds {
        row: usize,
        len: usize,
    },
    CandidateArrayTypeMismatch {
        expected: novarocks_failure_contract::BoundedDetail,
        actual: novarocks_failure_contract::BoundedDetail,
    },
    InvalidCandidateValue(String),
    CandidateCarrier(novarocks_functions::KernelFailure),
    BoundLoosened,
    Finished,
}

impl fmt::Display for AggregateTopNBoundaryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedKeyArity { actual } => {
                write!(
                    formatter,
                    "aggregate TopN boundary requires exactly one order key, got {actual}"
                )
            }
            Self::NonOrderedContract => {
                write!(
                    formatter,
                    "aggregate TopN boundary requires an ordered contract"
                )
            }
            Self::UnsupportedOrderKeyType { actual } => write!(
                formatter,
                "aggregate TopN boundary does not support order key type {actual}"
            ),
            Self::GroupIdentityOutOfSequence { expected, actual } => write!(
                formatter,
                "aggregate TopN group identity is out of sequence: expected={expected} actual={actual}"
            ),
            Self::CandidateContractMismatch(error) => write!(
                formatter,
                "aggregate TopN candidate does not match its order contract: {error:?}"
            ),
            Self::MissingGroupKey { ordinal, actual } => write!(
                formatter,
                "aggregate TopN group key ordinal is missing: ordinal={ordinal} key_count={actual}"
            ),
            Self::CandidateRowOutOfBounds { row, len } => write!(
                formatter,
                "aggregate TopN candidate row is out of bounds: row={row} len={len}"
            ),
            Self::CandidateArrayTypeMismatch { expected, actual } => write!(
                formatter,
                "aggregate TopN candidate type mismatch: expected={expected:?} actual={actual}"
            ),
            Self::CandidateCarrier(source) => source.fmt(formatter),
            Self::InvalidCandidateValue(detail) => {
                write!(
                    formatter,
                    "invalid aggregate TopN candidate value: {detail}"
                )
            }
            Self::BoundLoosened => {
                write!(
                    formatter,
                    "aggregate TopN cumulative boundary unexpectedly loosened"
                )
            }
            Self::Finished => write!(formatter, "aggregate TopN boundary state is finished"),
        }
    }
}

impl std::error::Error for AggregateTopNBoundaryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CandidateCarrier(source) => Some(source),
            _ => None,
        }
    }
}

impl From<OrderedTupleError> for AggregateTopNBoundaryError {
    fn from(error: OrderedTupleError) -> Self {
        Self::CandidateContractMismatch(error)
    }
}

fn topn_type_description(value: &DataType) -> novarocks_failure_contract::BoundedDetail {
    novarocks_failure_contract::BoundedDetail::truncating_display(format_args!("{value:?}"))
}
impl novarocks_failure_contract::FailureProjection for AggregateTopNBoundaryError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        let reason = match self {
            Self::UnsupportedKeyArity { .. } => ExecutionReason::TopNKeyArity,
            Self::NonOrderedContract => ExecutionReason::TopNNonOrdered,
            Self::UnsupportedOrderKeyType { .. } => ExecutionReason::TopNKeyType,
            Self::GroupIdentityOutOfSequence { .. } => ExecutionReason::TopNGroupSequence,
            Self::CandidateContractMismatch(_) => ExecutionReason::TopNCandidateContract,
            Self::MissingGroupKey { .. } => ExecutionReason::TopNMissingKey,
            Self::CandidateRowOutOfBounds { .. } => ExecutionReason::TopNRowBounds,
            Self::CandidateArrayTypeMismatch { .. } => ExecutionReason::TopNArrayType,
            Self::CandidateCarrier(source) => return source.failure_fact(),
            Self::InvalidCandidateValue(_) => ExecutionReason::TopNCandidateValue,
            Self::BoundLoosened => ExecutionReason::TopNBoundLoosened,
            Self::Finished => ExecutionReason::TopNAfterFinish,
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}

/// Storage owns real candidates; the ranking and publication state machine
/// below is shared by both adapters. Pure storage is intentionally not Clone:
/// whole key clones are cheap, while cloning a candidate vector is new work.
pub(crate) trait TopNBoundaryStorage: std::fmt::Debug {
    type Key: Clone + std::fmt::Debug + Eq;
    type Error: From<AggregateTopNBoundaryError>;
    fn candidates(&self) -> &[AggregateTopNCandidate<Self::Key>];
    fn push(
        &mut self,
        key: Self::Key,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), Self::Error>;
    fn replace(&mut self, index: usize, key: Self::Key);
    fn compare(
        contract: &RuntimeOrderContract,
        a: &Self::Key,
        b: &Self::Key,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Ordering, Self::Error>;
    fn checkpoint(control: &dyn KernelEvaluationControl, units: u32) -> Result<(), Self::Error>;
}

#[derive(Clone, Eq, PartialEq)]
#[cfg(test)]
pub(crate) struct LegacyTopNBoundaryStorage(Vec<AggregateTopNCandidate<OrderedTuple>>);
#[cfg(test)]
impl std::fmt::Debug for LegacyTopNBoundaryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[cfg(test)]
impl TopNBoundaryStorage for LegacyTopNBoundaryStorage {
    type Key = OrderedTuple;
    type Error = AggregateTopNBoundaryError;
    fn candidates(&self) -> &[AggregateTopNCandidate<OrderedTuple>] {
        &self.0
    }
    fn push(
        &mut self,
        tuple: OrderedTuple,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), Self::Error> {
        self.0.push(AggregateTopNCandidate { tuple });
        Ok(())
    }
    fn replace(&mut self, index: usize, tuple: OrderedTuple) {
        self.0[index] = AggregateTopNCandidate { tuple };
    }
    fn compare(
        contract: &RuntimeOrderContract,
        a: &OrderedTuple,
        b: &OrderedTuple,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Ordering, Self::Error> {
        contract.compare(a, b).map_err(Into::into)
    }
    fn checkpoint(_: &dyn KernelEvaluationControl, _: u32) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OwnedTopNBoundaryError {
    Boundary(AggregateTopNBoundaryError),
    Kernel(KernelFailure),
}
impl From<AggregateTopNBoundaryError> for OwnedTopNBoundaryError {
    fn from(cause: AggregateTopNBoundaryError) -> Self {
        Self::Boundary(cause)
    }
}
impl From<KernelFailure> for OwnedTopNBoundaryError {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<ControlledOrderedTupleError> for OwnedTopNBoundaryError {
    fn from(cause: ControlledOrderedTupleError) -> Self {
        match cause {
            ControlledOrderedTupleError::Tuple(cause) => Self::Boundary(cause.into()),
            ControlledOrderedTupleError::Kernel(cause) => Self::Kernel(cause),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OwnedTopNBoundaryStorage(AggregateVec<AggregateTopNCandidate<OwnedOrderKey>>);
impl TopNBoundaryStorage for OwnedTopNBoundaryStorage {
    type Key = OwnedOrderKey;
    type Error = OwnedTopNBoundaryError;
    fn candidates(&self) -> &[AggregateTopNCandidate<Self::Key>] {
        &self.0
    }
    fn push(
        &mut self,
        tuple: Self::Key,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), Self::Error> {
        control.checkpoint(0)?;
        self.0.try_reserve(1).map_err(|source| {
            self.0
                .allocator()
                .allocation_error("reserve ordered TopN candidates", source)
        })?;
        control.checkpoint(0)?;
        self.0.push(AggregateTopNCandidate { tuple });
        Ok(())
    }
    fn replace(&mut self, index: usize, tuple: Self::Key) {
        self.0[index] = AggregateTopNCandidate { tuple };
    }
    fn compare(
        contract: &RuntimeOrderContract,
        a: &Self::Key,
        b: &Self::Key,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Ordering, Self::Error> {
        contract
            .compare_borrowed_controlled(a.borrowed(), b.borrowed(), control)
            .map_err(Into::into)
    }
    fn checkpoint(control: &dyn KernelEvaluationControl, units: u32) -> Result<(), Self::Error> {
        control.checkpoint(units).map_err(Into::into)
    }
}

#[cfg(test)]
struct LegacyBoundaryControl;
#[cfg(test)]
impl KernelEvaluationControl for LegacyBoundaryControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        unreachable!("boundary selection never waits")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct AggregateTopNBoundaryCore<S: TopNBoundaryStorage> {
    limit: usize,
    contract: crate::runtime_filter::RuntimeOrderContractRef,
    best: S,
    observed_group_count: usize,
    last_emitted_bound: Option<S::Key>,
    finished: bool,
}
impl<S: TopNBoundaryStorage> std::fmt::Debug for AggregateTopNBoundaryCore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateTopNBoundaryState")
            .field("limit", &self.limit)
            .field("contract", &self.contract)
            .field("best", &self.best)
            .field("observed_group_count", &self.observed_group_count)
            .field("last_emitted_bound", &self.last_emitted_bound)
            .field("finished", &self.finished)
            .finish()
    }
}
#[cfg(test)]
pub(crate) type AggregateTopNBoundaryState = AggregateTopNBoundaryCore<LegacyTopNBoundaryStorage>;
pub(crate) type OwnedAggregateTopNBoundaryState =
    AggregateTopNBoundaryCore<OwnedTopNBoundaryStorage>;

impl<S: TopNBoundaryStorage> AggregateTopNBoundaryCore<S> {
    pub(crate) fn contract(&self) -> &crate::runtime_filter::RuntimeOrderContractRef {
        &self.contract
    }

    fn observe_controlled(
        &mut self,
        group_id: usize,
        tuple: S::Key,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), S::Error> {
        S::checkpoint(control, 0)?;
        if self.finished {
            return Err(AggregateTopNBoundaryError::Finished.into());
        }
        if group_id != self.observed_group_count {
            return Err(AggregateTopNBoundaryError::GroupIdentityOutOfSequence {
                expected: self.observed_group_count,
                actual: group_id,
            }
            .into());
        }
        S::compare(&self.contract, &tuple, &tuple, control)?;
        if self.best.candidates().len() < self.limit {
            self.best.push(tuple, control)?;
        } else {
            let worst = self.worst_candidate_index_controlled(control)?;
            if S::compare(
                &self.contract,
                &tuple,
                &self.best.candidates()[worst].tuple,
                control,
            )? == Ordering::Less
            {
                S::checkpoint(control, 0)?;
                self.best.replace(worst, tuple);
            }
        }
        self.observed_group_count += 1;
        Ok(())
    }

    fn snapshot_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<S::Key>, S::Error> {
        S::checkpoint(control, 0)?;
        if self.best.candidates().len() < self.limit {
            return Ok(None);
        }
        let worst = self.worst_candidate_index_controlled(control)?;
        Ok(Some(self.best.candidates()[worst].tuple.clone()))
    }

    pub(crate) fn take_pending_controlled(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<S::Key>, S::Error> {
        let Some(nth_best_inclusive) = self.snapshot_controlled(control)? else {
            return Ok(None);
        };
        match self.last_emitted_bound.as_ref() {
            None => {
                self.last_emitted_bound = Some(nth_best_inclusive.clone());
                Ok(Some(nth_best_inclusive))
            }
            Some(previous) => {
                match S::compare(&self.contract, &nth_best_inclusive, previous, control)? {
                    Ordering::Less => {
                        self.last_emitted_bound = Some(nth_best_inclusive.clone());
                        Ok(Some(nth_best_inclusive))
                    }
                    Ordering::Equal => Ok(None),
                    Ordering::Greater => Err(AggregateTopNBoundaryError::BoundLoosened.into()),
                }
            }
        }
    }

    pub(crate) fn finish_controlled(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<S::Key>, S::Error> {
        if self.finished {
            return Ok(None);
        }
        let pending = self.take_pending_controlled(control)?;
        self.finished = true;
        Ok(pending)
    }

    fn worst_candidate_index_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<usize, S::Error> {
        let mut worst = 0;
        for candidate in 1..self.best.candidates().len() {
            if S::compare(
                &self.contract,
                &self.best.candidates()[candidate].tuple,
                &self.best.candidates()[worst].tuple,
                control,
            )? == Ordering::Greater
            {
                worst = candidate;
            }
            S::checkpoint(control, 1)?;
        }
        Ok(worst)
    }
}

#[cfg(test)]
impl AggregateTopNBoundaryState {
    pub(crate) fn try_new(
        limit: NonZeroU32,
        contract: impl Into<crate::runtime_filter::RuntimeOrderContractRef>,
    ) -> Result<Self, AggregateTopNBoundaryError> {
        let contract = contract.into();
        validate_topn_boundary_contract(&contract)?;
        Ok(Self {
            limit: limit.get() as usize,
            contract,
            best: LegacyTopNBoundaryStorage(Vec::new()),
            observed_group_count: 0,
            last_emitted_bound: None,
            finished: false,
        })
    }
    pub(crate) fn observe_new_group(
        &mut self,
        group_id: usize,
        tuple: OrderedTuple,
    ) -> Result<(), AggregateTopNBoundaryError> {
        self.observe_controlled(group_id, tuple, &LegacyBoundaryControl)
    }
    pub(crate) fn snapshot(&self) -> AggregateTopNBoundarySnapshot {
        match self
            .snapshot_controlled(&LegacyBoundaryControl)
            .expect("validated aggregate TopN candidates must remain comparable")
        {
            None => AggregateTopNBoundarySnapshot::NotReady,
            Some(nth_best_inclusive) => AggregateTopNBoundarySnapshot::Ready { nth_best_inclusive },
        }
    }
    pub(crate) fn take_pending_tightening(
        &mut self,
    ) -> Result<Option<OrderedTuple>, AggregateTopNBoundaryError> {
        self.take_pending_controlled(&LegacyBoundaryControl)
    }
    pub(crate) fn finish(&mut self) -> Result<Option<OrderedTuple>, AggregateTopNBoundaryError> {
        self.finish_controlled(&LegacyBoundaryControl)
    }
}

impl OwnedAggregateTopNBoundaryState {
    pub(crate) fn try_new_owned(
        limit: NonZeroU32,
        contract: crate::runtime_filter::RuntimeOrderContractRef,
        host: &OrderedTopNAllocationHost,
    ) -> Result<Self, OwnedTopNBoundaryError> {
        validate_topn_boundary_contract(&contract)?;
        let data_type = contract.keys()[0].data_type();
        if !novarocks_local_program::is_aggregate_topn_key_carrier(data_type) {
            return Err(AggregateTopNBoundaryError::UnsupportedOrderKeyType {
                actual: topn_type_description(data_type),
            }
            .into());
        }
        Ok(Self {
            limit: limit.get() as usize,
            contract,
            best: OwnedTopNBoundaryStorage(AggregateVec::new_in(host.vector_allocator())),
            observed_group_count: 0,
            last_emitted_bound: None,
            finished: false,
        })
    }
    pub(crate) fn observe_owned_group(
        &mut self,
        group_id: usize,
        tuple: OwnedOrderKey,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), OwnedTopNBoundaryError> {
        self.observe_controlled(group_id, tuple, control)
    }
    #[cfg(test)]
    pub(crate) fn snapshot_owned(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Option<OwnedOrderKey>, OwnedTopNBoundaryError> {
        self.snapshot_controlled(control)
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AggregateTopNBoundaryBinding {
    group_key_ordinal: usize,
    state: AggregateTopNBoundaryState,
}

#[cfg(test)]
impl AggregateTopNBoundaryBinding {
    pub(crate) fn try_new(
        group_key_ordinal: usize,
        limit: NonZeroU32,
        contract: impl Into<crate::runtime_filter::RuntimeOrderContractRef>,
    ) -> Result<Self, AggregateTopNBoundaryError> {
        Ok(Self {
            group_key_ordinal,
            state: AggregateTopNBoundaryState::try_new(limit, contract)?,
        })
    }

    pub(crate) const fn state(&self) -> &AggregateTopNBoundaryState {
        &self.state
    }
}

fn validate_topn_boundary_contract(
    contract: &RuntimeOrderContract,
) -> Result<(), AggregateTopNBoundaryError> {
    if contract.keys().len() != 1 {
        return Err(AggregateTopNBoundaryError::UnsupportedKeyArity {
            actual: contract.keys().len(),
        });
    }
    let data_type = contract.keys()[0].data_type();
    if !matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    ) && !matches!(
        data_type,
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
    ) {
        return Err(AggregateTopNBoundaryError::UnsupportedOrderKeyType {
            actual: topn_type_description(data_type),
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn observe_scalar_projection(
    bindings: &mut [AggregateTopNBoundaryBinding],
    group_id: usize,
    group_arrays: &[ArrayRef],
    row: usize,
) -> Result<(), AggregateTopNBoundaryError> {
    for binding in bindings {
        let array = group_arrays.get(binding.group_key_ordinal).ok_or(
            AggregateTopNBoundaryError::MissingGroupKey {
                ordinal: binding.group_key_ordinal,
                actual: group_arrays.len(),
            },
        )?;
        let tuple = tuple_from_group_array(binding.state.contract(), array, row)?;
        binding.state.observe_new_group(group_id, tuple)?;
    }
    Ok(())
}

#[cfg(test)]
fn tuple_from_group_array(
    contract: &RuntimeOrderContract,
    array: &ArrayRef,
    row: usize,
) -> Result<OrderedTuple, AggregateTopNBoundaryError> {
    let value =
        borrowed_key_from_group_array(contract, array, row)?.map(OrderedScalarRef::into_owned);
    OrderedTuple::try_new(contract, [value]).map_err(Into::into)
}

/// Visit an actual K page loan without constructing an Arrow value per key.
/// Complete logical declarations are checked before physical page dispatch;
/// owned UTF-8 copies and boundary canonicalization keep their original authors.
pub(crate) fn visit_key_read_page<'a>(
    contract: &RuntimeOrderContract,
    values: &'a crate::exec::hash_table::key_index::KeyReadValues<'_>,
    rows: &[crate::exec::hash_table::key_index::KeyReadRow],
    control: &dyn KernelEvaluationControl,
    visitor: &mut impl FnMut(
        usize,
        Option<OrderedScalarRef<'a>>,
    ) -> crate::runtime::fragment::ExecutionResult<()>,
) -> crate::runtime::fragment::ExecutionResult<()> {
    use crate::exec::hash_table::key_index::KeyReadValues;
    control.checkpoint(0)?;
    if contract.keys().len() != 1 {
        return Err(AggregateTopNBoundaryError::UnsupportedKeyArity {
            actual: contract.keys().len(),
        }
        .into());
    }
    let binding = match values {
        KeyReadValues::Fixed { binding, .. } | KeyReadValues::Bytes { binding, .. } => *binding,
        KeyReadValues::NominalBytes {
            logical_binding, ..
        }
        | KeyReadValues::Canonical {
            logical_binding, ..
        } => *logical_binding,
    };
    let key = contract
        .keys()
        .first()
        .ok_or(AggregateTopNBoundaryError::UnsupportedKeyArity { actual: 0 })?;
    let expected_binding = key.value_type().ok_or_else(|| {
        AggregateTopNBoundaryError::InvalidCandidateValue(
            "ordered page contract has no complete value binding".into(),
        )
    })?;
    if !binding
        .same_value_domain_observed::<KernelFailure>(expected_binding, || control.checkpoint(0))?
    {
        return Err(AggregateTopNBoundaryError::CandidateContractMismatch(
            OrderedTupleError::TypeMismatch,
        )
        .into());
    }
    let (array, binding) = match values {
        KeyReadValues::Fixed { array, binding } => (*array, *binding),
        KeyReadValues::Bytes {
            array,
            binding,
            page,
        } => {
            if !page.is_utf8() {
                return Err(AggregateTopNBoundaryError::InvalidCandidateValue(
                    "ordered text page is not UTF-8".into(),
                )
                .into());
            }
            for selected in rows {
                control.checkpoint(1)?;
                if !page.defined_rows().contains(selected.row) {
                    return Err(AggregateTopNBoundaryError::CandidateRowOutOfBounds {
                        row: selected.row,
                        len: page.len(),
                    }
                    .into());
                }
            }
            (*array, *binding)
        }
        KeyReadValues::NominalBytes { .. } => {
            return Err(AggregateTopNBoundaryError::InvalidCandidateValue(
                "ordered scalar cannot borrow private nominal byte storage".into(),
            )
            .into());
        }
        KeyReadValues::Canonical { .. } => {
            return Err(AggregateTopNBoundaryError::InvalidCandidateValue(
                "ordered scalar cannot borrow an encoded complex key".into(),
            )
            .into());
        }
    };
    if binding.data_type() != array.data_type() {
        return Err(AggregateTopNBoundaryError::CandidateContractMismatch(
            OrderedTupleError::TypeMismatch,
        )
        .into());
    }
    let expected = key.data_type();
    macro_rules! visit {
        ($array:ty, $convert:expr) => {{
            let concrete = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
                    expected: topn_type_description(expected),
                    actual: topn_type_description(array.data_type()),
                }
            })?;
            for selected in rows {
                control.checkpoint(1)?;
                if selected.row >= concrete.len() {
                    return Err(AggregateTopNBoundaryError::CandidateRowOutOfBounds {
                        row: selected.row,
                        len: concrete.len(),
                    }
                    .into());
                }
                let value = if concrete.is_null(selected.row) {
                    None
                } else {
                    Some(($convert)(concrete.value(selected.row)))
                };
                visitor(selected.ordinal, value)?;
            }
        }};
    }
    if expected == &DataType::Utf8 {
        match array.data_type() {
            DataType::Utf8 => visit!(StringArray, OrderedScalarRef::Utf8),
            DataType::LargeUtf8 => visit!(LargeStringArray, OrderedScalarRef::Utf8),
            DataType::Utf8View => visit!(StringViewArray, OrderedScalarRef::Utf8),
            _ => {
                return Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
                    expected: topn_type_description(expected),
                    actual: topn_type_description(array.data_type()),
                }
                .into());
            }
        }
    } else {
        require_exact_array_type(expected, array)?;
        match expected {
            DataType::Int8 => visit!(Int8Array, OrderedScalarRef::Int8),
            DataType::Int16 => visit!(Int16Array, OrderedScalarRef::Int16),
            DataType::Int32 => visit!(Int32Array, OrderedScalarRef::Int32),
            DataType::Int64 => visit!(Int64Array, OrderedScalarRef::Int64),
            DataType::Date32 => visit!(Date32Array, OrderedScalarRef::Date32),
            DataType::Decimal128(_, _) => visit!(Decimal128Array, OrderedScalarRef::Decimal128),
            DataType::Timestamp(TimeUnit::Second, _) => {
                visit!(TimestampSecondArray, OrderedScalarRef::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                visit!(TimestampMillisecondArray, OrderedScalarRef::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                visit!(TimestampMicrosecondArray, OrderedScalarRef::Timestamp)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                visit!(TimestampNanosecondArray, OrderedScalarRef::Timestamp)
            }
            DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                let array = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("exact fixed binary carrier");
                for selected in rows {
                    control.checkpoint(1)?;
                    if selected.row >= array.len() {
                        return Err(AggregateTopNBoundaryError::CandidateRowOutOfBounds {
                            row: selected.row,
                            len: array.len(),
                        }
                        .into());
                    }
                    let value = if array.is_null(selected.row) {
                        None
                    } else {
                        Some(OrderedScalarRef::LargeInt(
                            novarocks_types::largeint::value_at(array, selected.row).map_err(
                                |source| {
                                    AggregateTopNBoundaryError::CandidateCarrier(source.into())
                                },
                            )?,
                        ))
                    };
                    visitor(selected.ordinal, value)?;
                }
            }
            _ => {
                return Err(AggregateTopNBoundaryError::UnsupportedOrderKeyType {
                    actual: topn_type_description(expected),
                }
                .into());
            }
        }
    }
    control.checkpoint(0)?;
    Ok(())
}

/// Original legacy-vector extraction used only by independent tests.
/// Preserve its bounds, dictionary NULL and physical-error order.
#[cfg(test)]
pub(crate) fn borrowed_key_from_group_array<'a>(
    contract: &RuntimeOrderContract,
    array: &'a ArrayRef,
    row: usize,
) -> Result<Option<OrderedScalarRef<'a>>, AggregateTopNBoundaryError> {
    if row >= array.len() {
        return Err(AggregateTopNBoundaryError::CandidateRowOutOfBounds {
            row,
            len: array.len(),
        });
    }
    let expected = contract
        .keys()
        .first()
        .expect("aggregate TopN state validates one order key")
        .data_type();
    let value = if expected == &DataType::Utf8 {
        utf8_value(array, row)?.map(OrderedScalarRef::Utf8)
    } else if array.is_null(row) {
        None
    } else {
        Some(scalar_from_group_array(expected, array, row)?)
    };
    Ok(value)
}

#[cfg(test)]
fn scalar_from_group_array(
    expected: &DataType,
    array: &ArrayRef,
    row: usize,
) -> Result<OrderedScalarRef<'static>, AggregateTopNBoundaryError> {
    macro_rules! primitive {
        ($array:ty, $variant:ident) => {{
            require_exact_array_type(expected, array)?;
            OrderedScalarRef::$variant(
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .expect("matching Arrow primitive type")
                    .value(row),
            )
        }};
    }

    Ok(match expected {
        DataType::Int8 => primitive!(Int8Array, Int8),
        DataType::Int16 => primitive!(Int16Array, Int16),
        DataType::Int32 => primitive!(Int32Array, Int32),
        DataType::Int64 => primitive!(Int64Array, Int64),
        DataType::Date32 => primitive!(Date32Array, Date32),
        DataType::Timestamp(TimeUnit::Second, _) => {
            primitive!(TimestampSecondArray, Timestamp)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondArray, Timestamp)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondArray, Timestamp)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondArray, Timestamp)
        }
        DataType::Decimal128(_, _) => primitive!(Decimal128Array, Decimal128),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            require_exact_array_type(expected, array)?;
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("matching Arrow FixedSizeBinary type");
            OrderedScalarRef::LargeInt(
                novarocks_types::largeint::value_at(array, row).map_err(|source| {
                    AggregateTopNBoundaryError::CandidateCarrier(source.into())
                })?,
            )
        }
        _ => {
            return Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
                expected: topn_type_description(expected),
                actual: topn_type_description(array.data_type()),
            });
        }
    })
}

fn require_exact_array_type(
    expected: &DataType,
    array: &ArrayRef,
) -> Result<(), AggregateTopNBoundaryError> {
    if array.data_type() == expected {
        Ok(())
    } else {
        Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
            expected: topn_type_description(expected),
            actual: topn_type_description(array.data_type()),
        })
    }
}

#[cfg(test)]
fn utf8_value(array: &ArrayRef, row: usize) -> Result<Option<&str>, AggregateTopNBoundaryError> {
    if array.data_type() == &DataType::Utf8 {
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("matching Arrow Utf8 type");
        return Ok((!strings.is_null(row)).then(|| strings.value(row)));
    }
    if matches!(
        array.data_type(),
        DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
    ) {
        let dictionary = array
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("matching Arrow Int32 dictionary type");
        if dictionary.is_null(row) {
            return Ok(None);
        }
        let key = usize::try_from(dictionary.keys().value(row)).map_err(|_| {
            AggregateTopNBoundaryError::InvalidCandidateValue(
                "negative UTF-8 dictionary key".to_string(),
            )
        })?;
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("matching Arrow UTF-8 dictionary values");
        if key >= values.len() {
            return Err(AggregateTopNBoundaryError::InvalidCandidateValue(
                "UTF-8 dictionary key is out of bounds".to_string(),
            ));
        }
        return Ok((!values.is_null(key)).then(|| values.value(key)));
    }
    Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
        expected: topn_type_description(&DataType::Utf8),
        actual: topn_type_description(array.data_type()),
    })
}

#[cfg(test)]
#[path = "topn_boundary_owned_tests.rs"]
mod owned_tests;

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Date32Array, Decimal128Array, DictionaryArray, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, Int32Type, TimeUnit};
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;

    use super::{
        AggregateTopNBoundaryBinding, AggregateTopNBoundaryError, AggregateTopNBoundarySnapshot,
        AggregateTopNBoundaryState, observe_scalar_projection,
    };
    use crate::runtime_filter::contribution::{
        OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
        RuntimeOrderNullOrder as NullOrder, RuntimeOrderSortDirection as SortDirection,
    };

    fn runtime_contract(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> Arc<RuntimeOrderContract> {
        Arc::new(RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                data_type, direction, null_order,
            )],
            [0; 32],
            [0; 32],
        ))
    }

    fn tuple(contract: &RuntimeOrderContract, value: Option<OrderedScalar>) -> OrderedTuple {
        OrderedTuple::try_new(contract, [value]).expect("valid ordered tuple")
    }

    fn int64_tuple(contract: &RuntimeOrderContract, value: Option<i64>) -> OrderedTuple {
        tuple(contract, value.map(OrderedScalar::Int64))
    }

    fn state(limit: u32, contract: Arc<RuntimeOrderContract>) -> AggregateTopNBoundaryState {
        AggregateTopNBoundaryState::try_new(
            NonZeroU32::new(limit).expect("nonzero limit"),
            contract,
        )
        .expect("valid aggregate TopN state")
    }

    fn oracle_nth(
        contract: &RuntimeOrderContract,
        candidates: &[OrderedTuple],
        limit: usize,
    ) -> Option<OrderedTuple> {
        if candidates.len() < limit {
            return None;
        }
        let mut ordered = candidates.to_vec();
        ordered.sort_by(|left, right| {
            contract
                .compare(left, right)
                .expect("oracle candidates match the contract")
        });
        ordered.get(limit - 1).cloned()
    }

    fn ready_bound(snapshot: AggregateTopNBoundarySnapshot) -> OrderedTuple {
        match snapshot {
            AggregateTopNBoundarySnapshot::NotReady => panic!("expected ready boundary"),
            AggregateTopNBoundarySnapshot::Ready { nth_best_inclusive } => nth_best_inclusive,
        }
    }

    #[test]
    fn topn_boundary_is_not_ready_until_n_complete_groups_exist() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(3, Arc::clone(&contract));

        state
            .observe_new_group(0, int64_tuple(&contract, Some(3)))
            .unwrap();
        state
            .observe_new_group(1, int64_tuple(&contract, Some(1)))
            .unwrap();

        assert_eq!(state.snapshot(), AggregateTopNBoundarySnapshot::NotReady);
        assert_eq!(state.take_pending_tightening().unwrap(), None);
    }

    #[test]
    fn topn_boundary_returns_the_nth_best_tuple_as_an_inclusive_bound() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(3, Arc::clone(&contract));
        for (group_id, value) in [8, 2, 5, 1].into_iter().enumerate() {
            state
                .observe_new_group(group_id, int64_tuple(&contract, Some(value)))
                .unwrap();
        }

        assert_eq!(
            ready_bound(state.snapshot()),
            int64_tuple(&contract, Some(5))
        );
        assert_eq!(
            state.take_pending_tightening().unwrap(),
            Some(int64_tuple(&contract, Some(5)))
        );
    }

    #[test]
    fn topn_boundary_preserves_projected_tuple_multiplicity_by_complete_group_identity() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(3, Arc::clone(&contract));
        for (group_id, value) in [7, 7, 9].into_iter().enumerate() {
            state
                .observe_new_group(group_id, int64_tuple(&contract, Some(value)))
                .unwrap();
        }

        assert_eq!(
            ready_bound(state.snapshot()),
            int64_tuple(&contract, Some(9))
        );

        state
            .observe_new_group(3, int64_tuple(&contract, Some(7)))
            .unwrap();
        assert_eq!(
            ready_bound(state.snapshot()),
            int64_tuple(&contract, Some(7))
        );
        assert_eq!(
            state.observe_new_group(3, int64_tuple(&contract, Some(1))),
            Err(AggregateTopNBoundaryError::GroupIdentityOutOfSequence {
                expected: 4,
                actual: 3,
            })
        );
    }

    #[test]
    fn topn_boundary_random_insertion_matches_independent_multiset_oracle_and_only_tightens() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let limit = 17_usize;
        let mut state = state(limit as u32, Arc::clone(&contract));
        let mut values = (-75_i64..125).collect::<Vec<_>>();
        values.shuffle(&mut StdRng::seed_from_u64(0x6c_b0_0d));
        let mut observed = Vec::new();
        let mut previous_bound: Option<OrderedTuple> = None;

        for (group_id, value) in values.into_iter().enumerate() {
            let candidate = int64_tuple(&contract, Some(value));
            observed.push(candidate.clone());
            state.observe_new_group(group_id, candidate).unwrap();

            let expected = oracle_nth(&contract, &observed, limit);
            match expected {
                None => assert_eq!(state.snapshot(), AggregateTopNBoundarySnapshot::NotReady),
                Some(expected) => {
                    let actual = ready_bound(state.snapshot());
                    assert_eq!(actual, expected);
                    if let Some(previous) = previous_bound.as_ref() {
                        assert_ne!(
                            contract.compare(&actual, previous).unwrap(),
                            Ordering::Greater,
                            "a cumulative nth-best bound must never loosen"
                        );
                    }
                    previous_bound = Some(actual);
                }
            }
        }
    }

    #[test]
    fn topn_boundary_honors_direction_and_null_order() {
        for direction in [SortDirection::Ascending, SortDirection::Descending] {
            for null_order in [NullOrder::First, NullOrder::Last] {
                let contract = runtime_contract(DataType::Int64, direction, null_order);
                let mut state = state(2, Arc::clone(&contract));
                let candidates = [None, Some(3), Some(1), Some(2)]
                    .into_iter()
                    .map(|value| int64_tuple(&contract, value))
                    .collect::<Vec<_>>();
                for (group_id, candidate) in candidates.iter().cloned().enumerate() {
                    state.observe_new_group(group_id, candidate).unwrap();
                }

                assert_eq!(
                    ready_bound(state.snapshot()),
                    oracle_nth(&contract, &candidates, 2).unwrap(),
                    "direction={direction:?} null_order={null_order:?}"
                );
            }
        }
    }

    #[test]
    fn topn_boundary_supports_all_frozen_single_key_scalar_types() {
        let cases = [
            (
                DataType::Int8,
                vec![OrderedScalar::Int8(4), OrderedScalar::Int8(-2)],
            ),
            (
                DataType::Int16,
                vec![OrderedScalar::Int16(4), OrderedScalar::Int16(-2)],
            ),
            (
                DataType::Int32,
                vec![OrderedScalar::Int32(4), OrderedScalar::Int32(-2)],
            ),
            (
                DataType::Int64,
                vec![OrderedScalar::Int64(4), OrderedScalar::Int64(-2)],
            ),
            (
                DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
                vec![OrderedScalar::LargeInt(4), OrderedScalar::LargeInt(-2)],
            ),
            (
                DataType::Utf8,
                vec![
                    OrderedScalar::Utf8("zeta".to_owned()),
                    OrderedScalar::Utf8("alpha".to_owned()),
                ],
            ),
            (
                DataType::Date32,
                vec![OrderedScalar::Date32(4), OrderedScalar::Date32(-2)],
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                vec![OrderedScalar::Timestamp(4), OrderedScalar::Timestamp(-2)],
            ),
            (
                DataType::Decimal128(18, 3),
                vec![OrderedScalar::Decimal128(4), OrderedScalar::Decimal128(-2)],
            ),
        ];

        for (data_type, values) in cases {
            let contract =
                runtime_contract(data_type.clone(), SortDirection::Ascending, NullOrder::Last);
            let mut state = state(2, Arc::clone(&contract));
            let candidates = values
                .into_iter()
                .map(|value| tuple(&contract, Some(value)))
                .collect::<Vec<_>>();
            for (group_id, candidate) in candidates.iter().cloned().enumerate() {
                state.observe_new_group(group_id, candidate).unwrap();
            }
            assert_eq!(
                ready_bound(state.snapshot()),
                oracle_nth(&contract, &candidates, 2).unwrap(),
                "data_type={data_type:?}"
            );
        }
    }

    #[test]
    fn topn_boundary_constructor_rejects_ordered_boolean_before_observation() {
        let contract =
            runtime_contract(DataType::Boolean, SortDirection::Ascending, NullOrder::Last);

        let error = AggregateTopNBoundaryState::try_new(NonZeroU32::new(1).unwrap(), contract)
            .expect_err("Boolean has no aggregate TopN boundary extractor");

        assert!(error.to_string().contains("Boolean"), "{error}");
    }

    #[test]
    fn topn_boundary_rejects_float_multi_key_and_exclusive_contracts() {
        assert_eq!(
            AggregateTopNBoundaryState::try_new(
                NonZeroU32::new(2).unwrap(),
                runtime_contract(DataType::Float64, SortDirection::Ascending, NullOrder::Last),
            ),
            Err(AggregateTopNBoundaryError::UnsupportedOrderKeyType {
                actual: super::topn_type_description(&DataType::Float64),
            })
        );

        let multi_key = Arc::new(RuntimeOrderContract::from_frozen(
            [
                RuntimeOrderKey::with_order(
                    DataType::Int64,
                    SortDirection::Ascending,
                    NullOrder::Last,
                ),
                RuntimeOrderKey::with_order(
                    DataType::Utf8,
                    SortDirection::Ascending,
                    NullOrder::Last,
                ),
            ],
            [0; 32],
            [0; 32],
        ));
        assert_eq!(
            AggregateTopNBoundaryState::try_new(NonZeroU32::new(2).unwrap(), multi_key),
            Err(AggregateTopNBoundaryError::UnsupportedKeyArity { actual: 2 })
        );
    }

    #[test]
    fn topn_boundary_equal_candidates_never_emit_a_falsely_strict_tightening() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(2, Arc::clone(&contract));
        state
            .observe_new_group(0, int64_tuple(&contract, Some(7)))
            .unwrap();
        state
            .observe_new_group(1, int64_tuple(&contract, Some(7)))
            .unwrap();
        assert_eq!(
            state.take_pending_tightening().unwrap(),
            Some(int64_tuple(&contract, Some(7)))
        );

        state
            .observe_new_group(2, int64_tuple(&contract, Some(7)))
            .unwrap();
        assert_eq!(state.take_pending_tightening().unwrap(), None);
        assert_eq!(
            ready_bound(state.snapshot()),
            int64_tuple(&contract, Some(7))
        );
    }

    #[test]
    fn topn_boundary_skipped_cadence_returns_a_sound_cumulative_snapshot() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(3, Arc::clone(&contract));
        let mut observed = Vec::new();

        for (group_id, value) in [90, 80, 70, 60, 50, 40].into_iter().enumerate() {
            let candidate = int64_tuple(&contract, Some(value));
            observed.push(candidate.clone());
            state.observe_new_group(group_id, candidate).unwrap();
        }

        let expected = oracle_nth(&contract, &observed, 3).unwrap();
        assert_eq!(state.take_pending_tightening().unwrap(), Some(expected));
    }

    #[test]
    fn topn_boundary_final_flush_returns_the_last_unpublished_tightening_once() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut state = state(3, Arc::clone(&contract));
        for (group_id, value) in [50, 40, 30].into_iter().enumerate() {
            state
                .observe_new_group(group_id, int64_tuple(&contract, Some(value)))
                .unwrap();
        }
        assert_eq!(
            state.take_pending_tightening().unwrap(),
            Some(int64_tuple(&contract, Some(50)))
        );

        state
            .observe_new_group(3, int64_tuple(&contract, Some(1)))
            .unwrap();
        assert_eq!(
            state.finish().unwrap(),
            Some(int64_tuple(&contract, Some(40)))
        );
        assert_eq!(state.finish().unwrap(), None);
        assert_eq!(
            state.observe_new_group(4, int64_tuple(&contract, Some(0))),
            Err(AggregateTopNBoundaryError::Finished)
        );
    }

    #[test]
    fn topn_boundary_dictionary_utf8_value_null_projects_logical_null() {
        let keys = Int32Array::from(vec![Some(0)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let dictionary: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());

        for null_order in [NullOrder::First, NullOrder::Last] {
            let contract = runtime_contract(DataType::Utf8, SortDirection::Ascending, null_order);
            let mut bindings = vec![
                AggregateTopNBoundaryBinding::try_new(
                    0,
                    NonZeroU32::new(1).unwrap(),
                    Arc::clone(&contract),
                )
                .unwrap(),
            ];

            observe_scalar_projection(&mut bindings, 0, &[Arc::clone(&dictionary)], 0).unwrap();

            assert_eq!(
                ready_bound(bindings[0].state().snapshot()),
                tuple(&contract, None),
                "null_order={null_order:?}"
            );
        }
    }

    #[test]
    fn topn_boundary_aggregate_hook_projects_every_supported_group_key_type() {
        let largeint = novarocks_types::largeint::array_from_i128(&[Some(-2)]).unwrap();
        let decimal = Decimal128Array::from(vec![-2])
            .with_precision_and_scale(18, 3)
            .unwrap();
        let cases: Vec<(DataType, ArrayRef, OrderedScalar)> = vec![
            (
                DataType::Int8,
                Arc::new(Int8Array::from(vec![-2])),
                OrderedScalar::Int8(-2),
            ),
            (
                DataType::Int16,
                Arc::new(Int16Array::from(vec![-2])),
                OrderedScalar::Int16(-2),
            ),
            (
                DataType::Int32,
                Arc::new(Int32Array::from(vec![-2])),
                OrderedScalar::Int32(-2),
            ),
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![-2])),
                OrderedScalar::Int64(-2),
            ),
            (
                DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
                largeint,
                OrderedScalar::LargeInt(-2),
            ),
            (
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["alpha"])),
                OrderedScalar::Utf8("alpha".to_owned()),
            ),
            (
                DataType::Utf8,
                Arc::new(
                    vec![Some("dictionary-alpha")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                OrderedScalar::Utf8("dictionary-alpha".to_owned()),
            ),
            (
                DataType::Date32,
                Arc::new(Date32Array::from(vec![-2])),
                OrderedScalar::Date32(-2),
            ),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                Arc::new(TimestampSecondArray::from(vec![-2])),
                OrderedScalar::Timestamp(-2),
            ),
            (
                DataType::Timestamp(TimeUnit::Millisecond, None),
                Arc::new(TimestampMillisecondArray::from(vec![-2])),
                OrderedScalar::Timestamp(-2),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![-2])),
                OrderedScalar::Timestamp(-2),
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Arc::new(TimestampNanosecondArray::from(vec![-2])),
                OrderedScalar::Timestamp(-2),
            ),
            (
                DataType::Decimal128(18, 3),
                Arc::new(decimal),
                OrderedScalar::Decimal128(-2),
            ),
        ];

        for (data_type, array, expected) in cases {
            let contract =
                runtime_contract(data_type.clone(), SortDirection::Ascending, NullOrder::Last);
            let mut bindings = vec![
                AggregateTopNBoundaryBinding::try_new(
                    0,
                    NonZeroU32::new(1).unwrap(),
                    Arc::clone(&contract),
                )
                .unwrap(),
            ];
            observe_scalar_projection(&mut bindings, 0, &[array], 0).unwrap();
            assert_eq!(
                ready_bound(bindings[0].state().snapshot()),
                tuple(&contract, Some(expected)),
                "data_type={data_type:?}"
            );
        }
    }

    #[test]
    fn topn_boundary_bindings_reconstruct_only_the_aggregate_candidate_contract() {
        let contract =
            runtime_contract(DataType::Int64, SortDirection::Descending, NullOrder::First);
        // The neutral boundary owns only candidate facts: no producer/session
        // wrapper is needed to declare the ordinal, limit and exact order.
        let bindings = [AggregateTopNBoundaryBinding::try_new(
            2,
            NonZeroU32::new(4).unwrap(),
            contract.clone(),
        )
        .unwrap()];
        assert_eq!(bindings[0].group_key_ordinal, 2);
        assert_eq!(bindings[0].state().limit, 4);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].state().contract().as_ref(), contract.as_ref());
    }
}
