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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::datatypes::DataType;
use sha2::{Digest, Sha256};

use crate::runtime_filter::model::contract::{
    ComparatorDigest, NullOrder, OrderContract, OrderKeyContract, SortDirection,
};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;

use super::artifact::encode_schema;
use super::value_domain::ContributionSizeError;

const ORDER_CONTRACT_VERSION: u16 = 1;
pub(crate) const COMPARATOR_ALGORITHM_VERSION: u16 = 1;
const COMPARATOR_DOMAIN: &[u8] = b"novarocks.runtime-filter.comparator";
const ORDER_CONTRACT_DOMAIN: &[u8] = b"novarocks.runtime-filter.order-contract";
const REPLAY_DIGEST_DOMAIN: &[u8] = b"novarocks.runtime-filter.ordered-bound-replay";
const REPLAY_DIGEST_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrderContractDigest([u8; 32]);

impl OrderContractDigest {
    pub const fn from_bytes_for_codec(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderKey {
    data_type: DataType,
    direction: SortDirection,
    null_order: NullOrder,
}

impl RuntimeOrderKey {
    pub const fn new(data_type: DataType, direction: SortDirection, null_order: NullOrder) -> Self {
        Self {
            data_type,
            direction,
            null_order,
        }
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn direction(&self) -> SortDirection {
        self.direction
    }

    pub const fn null_order(&self) -> NullOrder {
        self.null_order
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeOrderContract {
    keys: Arc<[RuntimeOrderKey]>,
    plan_comparator_digest: ComparatorDigest,
    order_contract_digest: OrderContractDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedScalar {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Utf8(Arc<str>),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedTuple {
    values: Arc<[Option<OrderedScalar>]>,
}

impl OrderedTuple {
    pub(crate) fn try_new(
        contract: &RuntimeOrderContract,
        values: impl IntoIterator<Item = Option<OrderedScalar>>,
    ) -> Result<Self, OrderedTupleError> {
        let values = values.into_iter().collect::<Vec<_>>();
        if values.len() != contract.keys.len() {
            return Err(OrderedTupleError::ArityMismatch);
        }
        for (key, value) in contract.keys.iter().zip(&values) {
            if value
                .as_ref()
                .is_some_and(|value| !scalar_matches_type(value, &key.data_type))
            {
                return Err(OrderedTupleError::TypeMismatch);
            }
        }
        Ok(Self {
            values: values.into(),
        })
    }

    pub(crate) fn values(&self) -> &[Option<OrderedScalar>] {
        &self.values
    }

    pub(crate) fn try_from_codec(
        contract: &RuntimeOrderContract,
        values: Vec<Option<OrderedScalar>>,
    ) -> Result<Self, OrderedTupleError> {
        if values.len() != contract.keys.len() {
            return Err(OrderedTupleError::ArityMismatch);
        }
        if contract.keys.iter().zip(&values).any(|(key, value)| {
            value
                .as_ref()
                .is_some_and(|value| !scalar_matches_type(value, &key.data_type))
        }) {
            return Err(OrderedTupleError::TypeMismatch);
        }
        Ok(Self {
            values: values.into(),
        })
    }

    pub(crate) fn estimated_retained_bytes(&self) -> Option<usize> {
        self.values.iter().try_fold(0usize, |bytes, value| {
            let value_bytes = match value {
                None => 1,
                Some(OrderedScalar::Boolean(_)) | Some(OrderedScalar::Int8(_)) => 2,
                Some(OrderedScalar::Int16(_)) => 3,
                Some(OrderedScalar::Int32(_)) | Some(OrderedScalar::Date32(_)) => 5,
                Some(OrderedScalar::Int64(_)) | Some(OrderedScalar::Timestamp(_)) => 9,
                Some(OrderedScalar::LargeInt(_)) | Some(OrderedScalar::Decimal128(_)) => 17,
                Some(OrderedScalar::Utf8(value)) => 1usize.checked_add(value.len())?,
            };
            bytes.checked_add(value_bytes)
        })
    }

    pub(crate) fn visit_canonical(&self, mut visitor: impl FnMut(&[u8])) {
        visitor(
            &u64::try_from(self.values.len())
                .expect("ordered tuple arity must fit canonical u64")
                .to_be_bytes(),
        );
        for value in self.values() {
            match value {
                None => visitor(&[0]),
                Some(value) => {
                    visitor(&[1]);
                    visit_ordered_scalar(value, &mut visitor);
                }
            }
        }
    }

    pub(crate) fn canonical_codec_len(&self) -> Result<usize, ContributionSizeError> {
        self.validate_canonical_u64_lengths()?;
        let mut bytes = Some(0usize);
        self.visit_canonical(|part| {
            bytes = bytes.and_then(|bytes| bytes.checked_add(part.len()));
        });
        bytes.ok_or(ContributionSizeError::SizeOverflow)
    }

    pub(crate) fn encode_canonical_into(
        &self,
        output: &mut Vec<u8>,
    ) -> Result<(), ContributionSizeError> {
        let exact_len = self.canonical_codec_len()?;
        let start = output.len();
        self.visit_canonical(|part| output.extend_from_slice(part));
        debug_assert_eq!(output.len() - start, exact_len);
        Ok(())
    }

    fn validate_canonical_u64_lengths(&self) -> Result<(), ContributionSizeError> {
        u64::try_from(self.values.len())
            .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
        for value in self.values.iter().flatten() {
            if let OrderedScalar::Utf8(value) = value {
                u64::try_from(value.len())
                    .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedBoundUpdate {
    order_contract_digest: OrderContractDigest,
    bound: OrderedTuple,
    replay_digest: [u8; 32],
}

impl OrderedBoundUpdate {
    pub(crate) fn new(
        contract: &RuntimeOrderContract,
        bound: OrderedTuple,
    ) -> Result<Self, OrderedTupleError> {
        validate_tuple(contract, &bound)?;
        let replay_digest = canonical_replay_digest(contract, &bound);
        Ok(Self {
            order_contract_digest: contract.digest(),
            bound,
            replay_digest,
        })
    }

    pub(crate) const fn order_contract_digest(&self) -> OrderContractDigest {
        self.order_contract_digest
    }

    pub(crate) const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }

    pub(crate) const fn replay_digest(&self) -> [u8; 32] {
        self.replay_digest
    }

    pub(crate) fn canonical_contribution_bytes(&self) -> Option<usize> {
        let mut bytes = Some(
            REPLAY_DIGEST_DOMAIN
                .len()
                .checked_add(size_of::<u16>())?
                .checked_add(32)?,
        );
        self.bound.visit_canonical(|part| {
            bytes = bytes.and_then(|bytes| bytes.checked_add(part.len()));
        });
        bytes
    }

    pub(crate) fn canonical_contribution_len(&self) -> Result<usize, ContributionSizeError> {
        self.bound.canonical_codec_len()
    }

    pub(crate) fn encode_bound_canonical_into(
        &self,
        output: &mut Vec<u8>,
    ) -> Result<(), ContributionSizeError> {
        self.bound.encode_canonical_into(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderContractError {
    EmptyKeys,
    ExclusiveBound,
    UnsupportedSchema,
    ComparatorDigestMismatch,
    LengthOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrderedTupleError {
    ArityMismatch,
    TypeMismatch,
}

pub(crate) struct ComparatorDigestV1;

impl ComparatorDigestV1 {
    pub(crate) fn for_contract(
        keys: &[OrderKeyContract],
        algorithm_version: u16,
    ) -> Result<ComparatorDigest, OrderContractError> {
        let mut canonical = Vec::with_capacity(64);
        canonical.extend_from_slice(COMPARATOR_DOMAIN);
        canonical.extend_from_slice(&algorithm_version.to_be_bytes());
        encode_keys(keys, &mut canonical)?;
        Ok(ComparatorDigest::new(Sha256::digest(canonical).into()))
    }
}

pub fn comparator_digest_for_plan(
    keys: &[OrderKeyContract],
) -> Result<ComparatorDigest, OrderContractError> {
    ComparatorDigestV1::for_contract(keys, COMPARATOR_ALGORITHM_VERSION)
}

impl RuntimeOrderContract {
    pub(crate) fn from_codec(
        keys: Vec<RuntimeOrderKey>,
        plan_comparator_digest: ComparatorDigest,
        order_contract_digest: OrderContractDigest,
    ) -> Result<Self, OrderContractError> {
        if keys.is_empty() {
            return Err(OrderContractError::EmptyKeys);
        }
        for key in &keys {
            validate_supported_type(&key.data_type)?;
        }
        Ok(Self {
            keys: keys.into(),
            plan_comparator_digest,
            order_contract_digest,
        })
    }

    pub(crate) fn validate_codec_contract_digest(
        canonical_keys: &[u8],
        comparator_digest: [u8; 32],
    ) -> Result<OrderContractDigest, OrderContractError> {
        let mut comparator = Sha256::new();
        comparator.update(COMPARATOR_DOMAIN);
        comparator.update(COMPARATOR_ALGORITHM_VERSION.to_be_bytes());
        comparator.update(canonical_keys);
        if <[u8; 32]>::from(comparator.finalize()) != comparator_digest {
            return Err(OrderContractError::ComparatorDigestMismatch);
        }
        let mut order = Sha256::new();
        order.update(ORDER_CONTRACT_DOMAIN);
        order.update(ORDER_CONTRACT_VERSION.to_be_bytes());
        order.update(canonical_keys);
        order.update([1]);
        order.update(comparator_digest);
        order.update(COMPARATOR_ALGORITHM_VERSION.to_be_bytes());
        Ok(OrderContractDigest(order.finalize().into()))
    }

    pub fn try_from_plan(plan: &OrderContract) -> Result<Self, OrderContractError> {
        if plan.keys.is_empty() {
            return Err(OrderContractError::EmptyKeys);
        }
        if !plan.inclusive {
            return Err(OrderContractError::ExclusiveBound);
        }
        validate_key_schemas(&plan.keys)?;
        let comparator =
            ComparatorDigestV1::for_contract(&plan.keys, COMPARATOR_ALGORITHM_VERSION)?;
        if comparator != plan.comparator_digest {
            return Err(OrderContractError::ComparatorDigestMismatch);
        }
        let order_contract_digest =
            canonical_order_digest(plan, plan.inclusive, COMPARATOR_ALGORITHM_VERSION)?;
        Ok(Self {
            keys: plan
                .keys
                .iter()
                .map(|key| RuntimeOrderKey {
                    data_type: key.data_type.clone(),
                    direction: key.direction,
                    null_order: key.null_order,
                })
                .collect::<Vec<_>>()
                .into(),
            plan_comparator_digest: plan.comparator_digest,
            order_contract_digest,
        })
    }

    pub(crate) fn compare(
        &self,
        left: &OrderedTuple,
        right: &OrderedTuple,
    ) -> Result<Ordering, OrderedTupleError> {
        validate_tuple(self, left)?;
        validate_tuple(self, right)?;
        for ((key, left), right) in self.keys.iter().zip(left.values()).zip(right.values()) {
            let ordering = match (left, right) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => match key.null_order {
                    NullOrder::First => Ordering::Less,
                    NullOrder::Last => Ordering::Greater,
                },
                (Some(_), None) => match key.null_order {
                    NullOrder::First => Ordering::Greater,
                    NullOrder::Last => Ordering::Less,
                },
                (Some(left), Some(right)) => {
                    let ordering = compare_non_null(left, right)?;
                    match key.direction {
                        SortDirection::Ascending => ordering,
                        SortDirection::Descending => ordering.reverse(),
                    }
                }
            };
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }

    pub const fn digest(&self) -> OrderContractDigest {
        self.order_contract_digest
    }

    pub(crate) fn keys(&self) -> &[RuntimeOrderKey] {
        &self.keys
    }

    pub(crate) const fn plan_comparator_digest(&self) -> ComparatorDigest {
        self.plan_comparator_digest
    }
}

fn canonical_order_digest(
    plan: &OrderContract,
    inclusive: bool,
    algorithm_version: u16,
) -> Result<OrderContractDigest, OrderContractError> {
    let mut canonical = Vec::with_capacity(96);
    canonical.extend_from_slice(ORDER_CONTRACT_DOMAIN);
    canonical.extend_from_slice(&ORDER_CONTRACT_VERSION.to_be_bytes());
    encode_keys(&plan.keys, &mut canonical)?;
    canonical.push(u8::from(inclusive));
    canonical.extend_from_slice(&plan.comparator_digest.get());
    canonical.extend_from_slice(&algorithm_version.to_be_bytes());
    Ok(OrderContractDigest(Sha256::digest(canonical).into()))
}

fn encode_keys(keys: &[OrderKeyContract], output: &mut Vec<u8>) -> Result<(), OrderContractError> {
    let count = u32::try_from(keys.len()).map_err(|_| OrderContractError::LengthOverflow)?;
    output.extend_from_slice(&count.to_be_bytes());
    for key in keys {
        validate_supported_type(&key.data_type)?;
        encode_schema(&key.data_type, output).map_err(|_| OrderContractError::UnsupportedSchema)?;
        output.push(match key.direction {
            SortDirection::Ascending => 1,
            SortDirection::Descending => 2,
        });
        output.push(match key.null_order {
            NullOrder::First => 1,
            NullOrder::Last => 2,
        });
    }
    Ok(())
}

fn validate_key_schemas(keys: &[OrderKeyContract]) -> Result<(), OrderContractError> {
    for key in keys {
        validate_supported_type(&key.data_type)?;
        let mut encoded = Vec::new();
        encode_schema(&key.data_type, &mut encoded)
            .map_err(|_| OrderContractError::UnsupportedSchema)?;
    }
    Ok(())
}

fn validate_supported_type(data_type: &DataType) -> Result<(), OrderContractError> {
    if matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
    ) || matches!(data_type, DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH)
    {
        Ok(())
    } else {
        Err(OrderContractError::UnsupportedSchema)
    }
}

fn validate_tuple(
    contract: &RuntimeOrderContract,
    tuple: &OrderedTuple,
) -> Result<(), OrderedTupleError> {
    if tuple.values.len() != contract.keys.len() {
        return Err(OrderedTupleError::ArityMismatch);
    }
    if contract
        .keys
        .iter()
        .zip(tuple.values())
        .any(|(key, value)| {
            value
                .as_ref()
                .is_some_and(|value| !scalar_matches_type(value, &key.data_type))
        })
    {
        return Err(OrderedTupleError::TypeMismatch);
    }
    Ok(())
}

fn scalar_matches_type(value: &OrderedScalar, data_type: &DataType) -> bool {
    match (value, data_type) {
        (OrderedScalar::Boolean(_), DataType::Boolean)
        | (OrderedScalar::Int8(_), DataType::Int8)
        | (OrderedScalar::Int16(_), DataType::Int16)
        | (OrderedScalar::Int32(_), DataType::Int32)
        | (OrderedScalar::Int64(_), DataType::Int64)
        | (OrderedScalar::Utf8(_), DataType::Utf8)
        | (OrderedScalar::Date32(_), DataType::Date32)
        | (OrderedScalar::Timestamp(_), DataType::Timestamp(_, _)) => true,
        (OrderedScalar::Decimal128(value), DataType::Decimal128(precision, _)) => {
            decimal128_fits_precision(*value, *precision)
        }
        (OrderedScalar::LargeInt(_), DataType::FixedSizeBinary(width)) => {
            *width == LARGEINT_BYTE_WIDTH
        }
        _ => false,
    }
}

fn decimal128_fits_precision(value: i128, precision: u8) -> bool {
    let Some(limit) = 10_i128.checked_pow(u32::from(precision)) else {
        return false;
    };
    value > -limit && value < limit
}

fn canonical_replay_digest(contract: &RuntimeOrderContract, bound: &OrderedTuple) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(REPLAY_DIGEST_DOMAIN);
    canonical.update(REPLAY_DIGEST_VERSION.to_be_bytes());
    canonical.update(contract.digest().bytes());
    bound.visit_canonical(|part| canonical.update(part));
    canonical.finalize().into()
}

fn visit_ordered_scalar(value: &OrderedScalar, visitor: &mut impl FnMut(&[u8])) {
    match value {
        OrderedScalar::Boolean(value) => visitor(&[u8::from(*value)]),
        OrderedScalar::Int8(value) => visitor(&value.to_be_bytes()),
        OrderedScalar::Int16(value) => visitor(&value.to_be_bytes()),
        OrderedScalar::Int32(value) | OrderedScalar::Date32(value) => {
            visitor(&value.to_be_bytes());
        }
        OrderedScalar::Int64(value) | OrderedScalar::Timestamp(value) => {
            visitor(&value.to_be_bytes());
        }
        OrderedScalar::LargeInt(value) | OrderedScalar::Decimal128(value) => {
            visitor(&value.to_be_bytes());
        }
        OrderedScalar::Utf8(value) => {
            visitor(
                &u64::try_from(value.len())
                    .expect("ordered UTF-8 scalar length must fit canonical u64")
                    .to_be_bytes(),
            );
            visitor(value.as_bytes());
        }
    }
}

fn compare_non_null(
    left: &OrderedScalar,
    right: &OrderedScalar,
) -> Result<Ordering, OrderedTupleError> {
    Ok(match (left, right) {
        (OrderedScalar::Boolean(a), OrderedScalar::Boolean(b)) => a.cmp(b),
        (OrderedScalar::Int8(a), OrderedScalar::Int8(b)) => a.cmp(b),
        (OrderedScalar::Int16(a), OrderedScalar::Int16(b)) => a.cmp(b),
        (OrderedScalar::Int32(a), OrderedScalar::Int32(b)) => a.cmp(b),
        (OrderedScalar::Int64(a), OrderedScalar::Int64(b)) => a.cmp(b),
        (OrderedScalar::LargeInt(a), OrderedScalar::LargeInt(b)) => a.cmp(b),
        (OrderedScalar::Utf8(a), OrderedScalar::Utf8(b)) => a.as_bytes().cmp(b.as_bytes()),
        (OrderedScalar::Date32(a), OrderedScalar::Date32(b)) => a.cmp(b),
        (OrderedScalar::Timestamp(a), OrderedScalar::Timestamp(b)) => a.cmp(b),
        (OrderedScalar::Decimal128(a), OrderedScalar::Decimal128(b)) => a.cmp(b),
        _ => return Err(OrderedTupleError::TypeMismatch),
    })
}

#[cfg(test)]
pub(crate) fn comparator_digest_for_test(
    keys: &[OrderKeyContract],
    algorithm_version: u16,
) -> ComparatorDigest {
    ComparatorDigestV1::for_contract(keys, algorithm_version)
        .expect("test order keys must be supported")
}

#[cfg(test)]
fn canonical_order_digest_for_test(
    plan: &OrderContract,
    inclusive: bool,
    algorithm_version: u16,
) -> OrderContractDigest {
    canonical_order_digest(plan, inclusive, algorithm_version)
        .expect("test order keys must be supported")
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{
        ComparatorDigest, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };

    use super::*;

    fn plan_with_keys(keys: Vec<OrderKeyContract>, inclusive: bool) -> OrderContract {
        let comparator_digest = comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION);
        OrderContract {
            keys,
            inclusive,
            comparator_digest,
        }
    }

    #[test]
    fn plan_comparator_digest_matches_runtime_order_contract_and_rejects_unsupported_schema() {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        let digest = comparator_digest_for_plan(&keys).expect("Int64 supports ordered bounds");
        let contract = OrderContract {
            keys: keys.clone(),
            inclusive: true,
            comparator_digest: digest,
        };
        assert_eq!(
            RuntimeOrderContract::try_from_plan(&contract)
                .expect("plan digest must match runtime contract")
                .plan_comparator_digest(),
            digest
        );

        assert_eq!(
            comparator_digest_for_plan(&[OrderKeyContract {
                data_type: DataType::Float64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }]),
            Err(OrderContractError::UnsupportedSchema)
        );
    }

    fn test_order_contract(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> OrderContract {
        plan_with_keys(
            vec![OrderKeyContract {
                data_type,
                direction,
                null_order,
            }],
            true,
        )
    }

    fn two_key_contract() -> OrderContract {
        plan_with_keys(
            vec![
                OrderKeyContract {
                    data_type: DataType::Int64,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::Last,
                },
                OrderKeyContract {
                    data_type: DataType::Utf8,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::First,
                },
            ],
            true,
        )
    }

    fn contract_with_comparator_digest(bytes: [u8; 32]) -> OrderContract {
        let mut plan =
            test_order_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        plan.comparator_digest = ComparatorDigest::new(bytes);
        plan
    }

    fn wrong_digest_contract() -> OrderContract {
        contract_with_comparator_digest([99; 32])
    }

    fn float_contract() -> OrderContract {
        OrderContract {
            keys: vec![OrderKeyContract {
                data_type: DataType::Float64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
            inclusive: true,
            comparator_digest: ComparatorDigest::new([0; 32]),
        }
    }

    fn exclusive_contract() -> OrderContract {
        let mut plan =
            test_order_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        plan.inclusive = false;
        plan
    }

    fn mixed_key_contract() -> RuntimeOrderContract {
        RuntimeOrderContract::try_from_plan(&plan_with_keys(
            vec![
                OrderKeyContract {
                    data_type: DataType::Utf8,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::First,
                },
                OrderKeyContract {
                    data_type: DataType::Int64,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::Last,
                },
            ],
            true,
        ))
        .unwrap()
    }

    fn tuple<const N: usize>(
        contract: &RuntimeOrderContract,
        values: [Option<OrderedScalar>; N],
    ) -> OrderedTuple {
        OrderedTuple::try_new(contract, values).unwrap()
    }

    fn ordered_sample_tuples(contract: &RuntimeOrderContract) -> Vec<OrderedTuple> {
        vec![
            tuple(contract, [None, Some(OrderedScalar::Int64(9))]),
            tuple(
                contract,
                [
                    Some(OrderedScalar::Utf8("a".into())),
                    Some(OrderedScalar::Int64(10)),
                ],
            ),
            tuple(
                contract,
                [
                    Some(OrderedScalar::Utf8("a".into())),
                    Some(OrderedScalar::Int64(9)),
                ],
            ),
            tuple(
                contract,
                [
                    Some(OrderedScalar::Utf8("b".into())),
                    Some(OrderedScalar::Int64(99)),
                ],
            ),
        ]
    }

    #[test]
    fn comparator_digest_v1_changes_for_every_order_fact() {
        let base = test_order_contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let variants = [
            test_order_contract(DataType::Int32, SortDirection::Ascending, NullOrder::Last),
            test_order_contract(DataType::Int64, SortDirection::Descending, NullOrder::Last),
            test_order_contract(DataType::Int64, SortDirection::Ascending, NullOrder::First),
            two_key_contract(),
            contract_with_comparator_digest([9; 32]),
        ];
        let base_digest = canonical_order_digest_for_test(&base, true, 1);
        for variant in variants {
            assert_ne!(
                base_digest,
                canonical_order_digest_for_test(&variant, true, 1)
            );
        }
        assert_ne!(
            base_digest,
            canonical_order_digest_for_test(&base, false, 1)
        );
        assert_ne!(base_digest, canonical_order_digest_for_test(&base, true, 2));
    }

    #[test]
    fn install_rejects_opaque_wrong_comparator_float_and_exclusive_bound() {
        for plan in [
            wrong_digest_contract(),
            float_contract(),
            exclusive_contract(),
        ] {
            assert!(RuntimeOrderContract::try_from_plan(&plan).is_err());
        }
    }

    #[test]
    fn ordered_tuple_compare_is_lexicographic_direction_and_null_aware() {
        let contract = mixed_key_contract();
        let null_first = tuple(&contract, [None, Some(OrderedScalar::Int64(9))]);
        let value = tuple(
            &contract,
            [
                Some(OrderedScalar::Utf8("a".into())),
                Some(OrderedScalar::Int64(9)),
            ],
        );
        assert_eq!(
            contract.compare(&null_first, &value).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn ordered_tuple_compare_is_antisymmetric_and_transitive() {
        let contract = mixed_key_contract();
        let tuples = ordered_sample_tuples(&contract);
        for left in &tuples {
            for right in &tuples {
                assert_eq!(
                    contract.compare(left, right).unwrap(),
                    contract.compare(right, left).unwrap().reverse()
                );
            }
        }
        for triple in tuples.windows(3) {
            assert_ne!(
                contract.compare(&triple[0], &triple[1]).unwrap(),
                Ordering::Greater
            );
            assert_ne!(
                contract.compare(&triple[1], &triple[2]).unwrap(),
                Ordering::Greater
            );
            assert_ne!(
                contract.compare(&triple[0], &triple[2]).unwrap(),
                Ordering::Greater
            );
        }
    }

    #[test]
    fn decimal128_tuple_rejects_values_outside_declared_precision() {
        for (precision, valid_limit) in [
            (3, 10_i128.checked_pow(3).unwrap()),
            (38, 10_i128.checked_pow(38).unwrap()),
        ] {
            let contract = RuntimeOrderContract::try_from_plan(&test_order_contract(
                DataType::Decimal128(precision, 0),
                SortDirection::Ascending,
                NullOrder::Last,
            ))
            .unwrap();

            for value in [valid_limit - 1, -(valid_limit - 1)] {
                assert!(
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Decimal128(value))])
                        .is_ok()
                );
            }
            for value in [valid_limit, -valid_limit, i128::MIN] {
                assert_eq!(
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Decimal128(value))]),
                    Err(OrderedTupleError::TypeMismatch)
                );
            }
        }
    }

    #[test]
    fn ordered_bound_replay_digest_is_derived_from_contract_and_tuple() {
        let ascending = RuntimeOrderContract::try_from_plan(&test_order_contract(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        ))
        .unwrap();
        let descending = RuntimeOrderContract::try_from_plan(&test_order_contract(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
        ))
        .unwrap();
        let first = OrderedBoundUpdate::new(
            &ascending,
            tuple(&ascending, [Some(OrderedScalar::Int64(7))]),
        )
        .unwrap();
        let same = OrderedBoundUpdate::new(
            &ascending,
            tuple(&ascending, [Some(OrderedScalar::Int64(7))]),
        )
        .unwrap();
        let different_bound = OrderedBoundUpdate::new(
            &ascending,
            tuple(&ascending, [Some(OrderedScalar::Int64(8))]),
        )
        .unwrap();
        let different_contract = OrderedBoundUpdate::new(
            &descending,
            tuple(&descending, [Some(OrderedScalar::Int64(7))]),
        )
        .unwrap();

        assert_eq!(first.replay_digest(), same.replay_digest());
        assert_ne!(first.replay_digest(), different_bound.replay_digest());
        assert_ne!(first.replay_digest(), different_contract.replay_digest());
    }

    #[test]
    fn ordered_bound_codec_body_is_exact_canonical_tuple() {
        let contract = mixed_key_contract();
        let update = OrderedBoundUpdate::new(
            &contract,
            tuple(&contract, [Some(OrderedScalar::Utf8("codec".into())), None]),
        )
        .unwrap();
        let mut encoded = Vec::new();

        update.encode_bound_canonical_into(&mut encoded).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&2_u64.to_be_bytes());
        expected.push(1);
        expected.extend_from_slice(&5_u64.to_be_bytes());
        expected.extend_from_slice(b"codec");
        expected.push(0);
        assert_eq!(encoded, expected);
        assert_eq!(update.canonical_contribution_len(), Ok(encoded.len()));
        assert!(
            update.canonical_contribution_bytes().unwrap()
                > update.canonical_contribution_len().unwrap()
        );
    }

    #[test]
    fn ordered_tuple_codec_helpers_match_canonical_visitor() {
        let contract = mixed_key_contract();
        let tuple = tuple(&contract, [Some(OrderedScalar::Utf8("codec".into())), None]);
        let mut visited = Vec::new();
        tuple.visit_canonical(|part| visited.extend_from_slice(part));
        let mut encoded = Vec::new();

        assert_eq!(tuple.encode_canonical_into(&mut encoded), Ok(()));
        assert_eq!(tuple.canonical_codec_len(), Ok(visited.len()));
        assert_eq!(encoded, visited);
    }
}
