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
//! group identity is supplied by `KeyTable`'s monotonically allocated group id,
//! so equal projections from different groups retain their multiplicity without
//! retaining every full group key. Service routing, logical versions, producer
//! sessions, and publication policy intentionally remain outside this module.

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};

use crate::exec::hash_table::key_table::KeyLookup;
use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;
use crate::runtime_filter::model::contract::ComparatorDigest;
use crate::runtime_filter::port::ordered_bound::{
    OrderContractDigest, OrderContractError, OrderedScalar, OrderedTuple, OrderedTupleError,
    RuntimeOrderContract,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct AggregateTopNCandidate {
    tuple: OrderedTuple,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AggregateTopNBoundarySnapshot {
    NotReady,
    Ready { nth_best_inclusive: OrderedTuple },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AggregateTopNBoundaryError {
    UnsupportedKeyArity {
        actual: usize,
    },
    InvalidOrderContract(OrderContractError),
    NonOrderedContract,
    UnsupportedOrderKeyType {
        actual: DataType,
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
        expected: DataType,
        actual: DataType,
    },
    InvalidCandidateValue(String),
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
            Self::InvalidOrderContract(error) => {
                write!(
                    formatter,
                    "aggregate TopN boundary has invalid order contract: {error:?}"
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
                "aggregate TopN boundary does not support order key type {actual:?}"
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
                "aggregate TopN candidate type mismatch: expected={expected:?} actual={actual:?}"
            ),
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

impl std::error::Error for AggregateTopNBoundaryError {}

impl From<OrderContractError> for AggregateTopNBoundaryError {
    fn from(error: OrderContractError) -> Self {
        Self::InvalidOrderContract(error)
    }
}

impl From<OrderedTupleError> for AggregateTopNBoundaryError {
    fn from(error: OrderedTupleError) -> Self {
        Self::CandidateContractMismatch(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AggregateTopNBoundaryState {
    limit: usize,
    contract: Arc<RuntimeOrderContract>,
    best: Vec<AggregateTopNCandidate>,
    observed_group_count: usize,
    last_emitted_bound: Option<OrderedTuple>,
    finished: bool,
}

impl AggregateTopNBoundaryState {
    pub(crate) fn try_new(
        limit: NonZeroU32,
        contract: Arc<RuntimeOrderContract>,
    ) -> Result<Self, AggregateTopNBoundaryError> {
        validate_topn_boundary_contract(&contract)?;
        Ok(Self {
            limit: limit.get() as usize,
            contract,
            best: Vec::new(),
            observed_group_count: 0,
            last_emitted_bound: None,
            finished: false,
        })
    }

    pub(crate) fn contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.contract
    }

    pub(crate) fn observe_new_group(
        &mut self,
        group_id: usize,
        tuple: OrderedTuple,
    ) -> Result<(), AggregateTopNBoundaryError> {
        if self.finished {
            return Err(AggregateTopNBoundaryError::Finished);
        }
        if group_id != self.observed_group_count {
            return Err(AggregateTopNBoundaryError::GroupIdentityOutOfSequence {
                expected: self.observed_group_count,
                actual: group_id,
            });
        }
        self.contract.compare(&tuple, &tuple)?;

        if self.best.len() < self.limit {
            self.best.push(AggregateTopNCandidate { tuple });
        } else {
            let worst = self.worst_candidate_index()?;
            if self.contract.compare(&tuple, &self.best[worst].tuple)? == Ordering::Less {
                self.best[worst] = AggregateTopNCandidate { tuple };
            }
        }
        self.observed_group_count += 1;
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> AggregateTopNBoundarySnapshot {
        if self.best.len() < self.limit {
            return AggregateTopNBoundarySnapshot::NotReady;
        }
        let worst = self
            .worst_candidate_index()
            .expect("validated aggregate TopN candidates must remain comparable");
        AggregateTopNBoundarySnapshot::Ready {
            nth_best_inclusive: self.best[worst].tuple.clone(),
        }
    }

    pub(crate) fn take_pending_tightening(
        &mut self,
    ) -> Result<Option<OrderedTuple>, AggregateTopNBoundaryError> {
        let AggregateTopNBoundarySnapshot::Ready { nth_best_inclusive } = self.snapshot() else {
            return Ok(None);
        };
        match self.last_emitted_bound.as_ref() {
            None => {
                self.last_emitted_bound = Some(nth_best_inclusive.clone());
                Ok(Some(nth_best_inclusive))
            }
            Some(previous) => match self.contract.compare(&nth_best_inclusive, previous)? {
                Ordering::Less => {
                    self.last_emitted_bound = Some(nth_best_inclusive.clone());
                    Ok(Some(nth_best_inclusive))
                }
                Ordering::Equal => Ok(None),
                Ordering::Greater => Err(AggregateTopNBoundaryError::BoundLoosened),
            },
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Option<OrderedTuple>, AggregateTopNBoundaryError> {
        if self.finished {
            return Ok(None);
        }
        let pending = self.take_pending_tightening()?;
        self.finished = true;
        Ok(pending)
    }

    fn worst_candidate_index(&self) -> Result<usize, AggregateTopNBoundaryError> {
        let mut worst = 0;
        for candidate in 1..self.best.len() {
            if self
                .contract
                .compare(&self.best[candidate].tuple, &self.best[worst].tuple)?
                == Ordering::Greater
            {
                worst = candidate;
            }
        }
        Ok(worst)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AggregateTopNBoundaryBinding {
    group_key_ordinal: usize,
    state: AggregateTopNBoundaryState,
}

impl AggregateTopNBoundaryBinding {
    pub(crate) fn try_new(
        group_key_ordinal: usize,
        limit: NonZeroU32,
        contract: Arc<RuntimeOrderContract>,
    ) -> Result<Self, AggregateTopNBoundaryError> {
        Ok(Self {
            group_key_ordinal,
            state: AggregateTopNBoundaryState::try_new(limit, contract)?,
        })
    }

    pub(crate) fn try_from_spec(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
    ) -> Result<Self, AggregateTopNBoundaryError> {
        let RuntimeFilterExecutionContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } = &spec.contract
        else {
            return Err(AggregateTopNBoundaryError::NonOrderedContract);
        };
        let contract = Arc::new(RuntimeOrderContract::from_codec(
            keys.to_vec(),
            ComparatorDigest::new(*comparator_digest),
            OrderContractDigest::from_bytes_for_codec(*order_contract_digest),
        )?);
        Self::try_new(spec.group_key_ordinal, spec.limit, contract)
    }

    pub(crate) const fn state(&self) -> &AggregateTopNBoundaryState {
        &self.state
    }

    pub(crate) fn state_mut(&mut self) -> &mut AggregateTopNBoundaryState {
        &mut self.state
    }
}

pub(crate) fn build_topn_boundary_bindings(
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
) -> Result<Vec<AggregateTopNBoundaryBinding>, AggregateTopNBoundaryError> {
    specs
        .iter()
        .map(AggregateTopNBoundaryBinding::try_from_spec)
        .collect()
}

pub(crate) fn validate_topn_boundary_specs(
    specs: &[AggregateTopNRuntimeFilterProducerBinding],
) -> Result<(), AggregateTopNBoundaryError> {
    for spec in specs {
        let RuntimeFilterExecutionContract::Ordered {
            keys,
            comparator_digest,
            order_contract_digest,
        } = &spec.contract
        else {
            return Err(AggregateTopNBoundaryError::NonOrderedContract);
        };
        let contract = RuntimeOrderContract::from_codec(
            keys.to_vec(),
            ComparatorDigest::new(*comparator_digest),
            OrderContractDigest::from_bytes_for_codec(*order_contract_digest),
        )?;
        validate_topn_boundary_contract(&contract)?;
    }
    Ok(())
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
            actual: data_type.clone(),
        });
    }
    Ok(())
}

pub(crate) fn observe_key_table_group(
    bindings: &mut [AggregateTopNBoundaryBinding],
    lookup: &KeyLookup,
    group_arrays: &[ArrayRef],
    row: usize,
) -> Result<(), AggregateTopNBoundaryError> {
    if !lookup.is_new {
        return Ok(());
    }
    for binding in bindings {
        let array = group_arrays.get(binding.group_key_ordinal).ok_or(
            AggregateTopNBoundaryError::MissingGroupKey {
                ordinal: binding.group_key_ordinal,
                actual: group_arrays.len(),
            },
        )?;
        let tuple = tuple_from_group_array(binding.state.contract(), array, row)?;
        binding.state.observe_new_group(lookup.group_id, tuple)?;
    }
    Ok(())
}

fn tuple_from_group_array(
    contract: &RuntimeOrderContract,
    array: &ArrayRef,
    row: usize,
) -> Result<OrderedTuple, AggregateTopNBoundaryError> {
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
        utf8_value(array, row)?.map(|value| OrderedScalar::Utf8(Arc::from(value)))
    } else if array.is_null(row) {
        None
    } else {
        Some(scalar_from_group_array(expected, array, row)?)
    };
    OrderedTuple::try_new(contract, [value]).map_err(Into::into)
}

fn scalar_from_group_array(
    expected: &DataType,
    array: &ArrayRef,
    row: usize,
) -> Result<OrderedScalar, AggregateTopNBoundaryError> {
    macro_rules! primitive {
        ($array:ty, $variant:ident) => {{
            require_exact_array_type(expected, array)?;
            OrderedScalar::$variant(
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
            OrderedScalar::LargeInt(
                novarocks_types::largeint::value_at(array, row)
                    .map_err(AggregateTopNBoundaryError::InvalidCandidateValue)?,
            )
        }
        _ => {
            return Err(AggregateTopNBoundaryError::CandidateArrayTypeMismatch {
                expected: expected.clone(),
                actual: array.data_type().clone(),
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
            expected: expected.clone(),
            actual: array.data_type().clone(),
        })
    }
}

fn utf8_value<'a>(
    array: &'a ArrayRef,
    row: usize,
) -> Result<Option<&'a str>, AggregateTopNBoundaryError> {
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
        expected: DataType::Utf8,
        actual: array.data_type().clone(),
    })
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::BTreeSet;
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
        AggregateTopNBoundaryState, build_topn_boundary_bindings, observe_key_table_group,
    };
    use crate::exec::hash_table::key_builder::build_group_key_views;
    use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
    use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
    use crate::exec::node::runtime_filter::{
        RuntimeFilterExecutionContract, RuntimeFilterExecutionReduction,
    };
    use crate::runtime_filter::model::contract::{
        ComparatorDigest, CompletionRequirement, ContributionKind, NullOrder, OrderContract,
        OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderContractError, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };

    fn order_plan(
        data_types: impl IntoIterator<Item = DataType>,
        direction: SortDirection,
        null_order: NullOrder,
        inclusive: bool,
    ) -> OrderContract {
        let keys = data_types
            .into_iter()
            .map(|data_type| OrderKeyContract {
                data_type,
                direction,
                null_order,
            })
            .collect::<Vec<_>>();
        let comparator_digest = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
        OrderContract {
            keys,
            inclusive,
            comparator_digest,
        }
    }

    fn runtime_contract(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> Arc<RuntimeOrderContract> {
        Arc::new(
            RuntimeOrderContract::try_from_plan(&order_plan(
                [data_type],
                direction,
                null_order,
                true,
            ))
            .expect("valid runtime order contract"),
        )
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
                    OrderedScalar::Utf8(Arc::from("zeta")),
                    OrderedScalar::Utf8(Arc::from("alpha")),
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
        let float_plan = OrderContract {
            keys: vec![OrderKeyContract {
                data_type: DataType::Float64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
            inclusive: true,
            comparator_digest: ComparatorDigest::new([0; 32]),
        };
        assert_eq!(
            RuntimeOrderContract::try_from_plan(&float_plan),
            Err(OrderContractError::UnsupportedSchema)
        );

        let multi_key = Arc::new(
            RuntimeOrderContract::try_from_plan(&order_plan(
                [DataType::Int64, DataType::Utf8],
                SortDirection::Ascending,
                NullOrder::Last,
                true,
            ))
            .unwrap(),
        );
        assert_eq!(
            AggregateTopNBoundaryState::try_new(NonZeroU32::new(2).unwrap(), multi_key),
            Err(AggregateTopNBoundaryError::UnsupportedKeyArity { actual: 2 })
        );

        let exclusive = order_plan(
            [DataType::Int64],
            SortDirection::Ascending,
            NullOrder::Last,
            false,
        );
        assert_eq!(
            RuntimeOrderContract::try_from_plan(&exclusive),
            Err(OrderContractError::ExclusiveBound)
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
    fn topn_boundary_aggregate_hook_uses_real_complete_key_table_group_identity() {
        let contract = runtime_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let mut bindings = vec![
            AggregateTopNBoundaryBinding::try_new(
                0,
                NonZeroU32::new(2).unwrap(),
                Arc::clone(&contract),
            )
            .unwrap(),
            AggregateTopNBoundaryBinding::try_new(
                0,
                NonZeroU32::new(3).unwrap(),
                Arc::clone(&contract),
            )
            .unwrap(),
        ];
        let group_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![7, 7, 7])),
            Arc::new(StringArray::from(vec!["group-a", "group-b", "group-a"])),
        ];
        let mut key_table = KeyTable::new(vec![DataType::Int64, DataType::Utf8], false).unwrap();
        let key_views = build_group_key_views(&group_arrays).unwrap();
        let rows = key_table.build_rows(&group_arrays).unwrap();
        let hashes = key_table
            .build_group_hashes(&key_views, group_arrays[0].len())
            .unwrap();
        let mut lookups = Vec::new();

        for (row, hash) in hashes.into_iter().enumerate() {
            let lookup = key_table
                .find_or_insert_from_row(&key_views, row, rows.row(row).data(), hash)
                .unwrap();
            lookups.push((lookup.group_id, lookup.is_new));
            observe_key_table_group(&mut bindings, &lookup, &group_arrays, row).unwrap();
        }

        assert_eq!(lookups, vec![(0, true), (1, true), (0, false)]);
        assert_eq!(
            ready_bound(bindings[0].state().snapshot()),
            int64_tuple(&contract, Some(7))
        );
        assert_eq!(
            bindings[0].state_mut().take_pending_tightening().unwrap(),
            Some(int64_tuple(&contract, Some(7)))
        );
        assert_eq!(
            bindings[1].state().snapshot(),
            AggregateTopNBoundarySnapshot::NotReady,
            "the duplicate complete key must not be counted as a third group"
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

            observe_key_table_group(
                &mut bindings,
                &KeyLookup {
                    group_id: 0,
                    is_new: true,
                },
                &[Arc::clone(&dictionary)],
                0,
            )
            .unwrap();

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
                OrderedScalar::Utf8(Arc::from("alpha")),
            ),
            (
                DataType::Utf8,
                Arc::new(
                    vec![Some("dictionary-alpha")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                OrderedScalar::Utf8(Arc::from("dictionary-alpha")),
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
            observe_key_table_group(
                &mut bindings,
                &KeyLookup {
                    group_id: 0,
                    is_new: true,
                },
                &[array],
                0,
            )
            .unwrap();
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
        let spec = AggregateTopNRuntimeFilterProducerBinding {
            binding_id: 11,
            channel_id: 12,
            group_key_expr_id: crate::exec::expr::ExprId(13),
            group_key_ordinal: 2,
            limit: NonZeroU32::new(4).unwrap(),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: contract.keys().to_vec().into(),
                comparator_digest: contract.plan_comparator_digest().get(),
                order_contract_digest: contract.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
            contribution_kinds: BTreeSet::from([
                ContributionKind::OrderedBoundUpdate,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
        };

        let bindings = build_topn_boundary_bindings(&[spec]).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].state().contract().as_ref(), contract.as_ref());
    }
}
