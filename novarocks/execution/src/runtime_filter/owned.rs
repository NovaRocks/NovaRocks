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

//! Actual nominal storage for ordered keys and canonical contribution bytes.
//! Vector Layouts and the immutable shared holder have separate authors. No
//! opaque capacity charge is added for an already-paid vector.

use std::{fmt, ops::Deref, sync::Arc};

use novarocks_functions::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, MAX_UNOBSERVED_KERNEL_WORK, SourceBackingOwner, source_custody_prefix_facts,
};

use super::contribution::{
    ControlledContributionCodecError, OrderedScalar, OrderedScalarRef, OrderedTupleRef,
    RuntimeOrderContract, RuntimeOrderContractValidationFailure, RuntimeOrderKey,
    RuntimeOrderNullOrder, RuntimeOrderSortDirection, borrowed_ordered_bound_encoded_len,
    visit_borrowed_ordered_bound,
};
use crate::exec::hash_table::allocation::{AggregateAllocator, AggregateVec};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime::tracked_state_allocator::expression_allocation_host;

struct ByteBacking {
    bytes: AggregateVec<u8>,
}

struct OrderKeyBacking {
    keys: AggregateVec<RuntimeOrderKey>,
}

/// Share the entire paid frozen key sequence; expose no detachable slots or
/// holder. Legacy contracts retain their original by-value Vec cloning.
#[derive(Clone)]
pub(crate) struct OwnedOrderKeys(SourceBackingOwner<OrderKeyBacking>);
impl OwnedOrderKeys {
    pub(crate) fn as_slice(&self) -> &[RuntimeOrderKey] {
        &self.0.borrowed().keys
    }
}

#[derive(Clone)]
enum ByteStorage {
    Legacy(Arc<[u8]>),
    Owned(SourceBackingOwner<ByteBacking>),
}

/// Borrow bytes or clone the complete carrier. The owned branch never exposes
/// a detachable vector/Arc that could outlive its original paying host.
#[derive(Clone)]
pub struct CanonicalContributionBytes(ByteStorage);

impl CanonicalContributionBytes {
    pub fn as_slice(&self) -> &[u8] {
        match &self.0 {
            ByteStorage::Legacy(bytes) => bytes,
            ByteStorage::Owned(owner) => &owner.borrowed().bytes,
        }
    }

    /// Preserve the original local-ingress copy for legacy payloads. Newly
    /// owned payloads instead share the same paid immutable backing.
    pub fn clone_for_local_ingress(&self) -> Self {
        match &self.0 {
            ByteStorage::Legacy(bytes) => Self::from(Arc::<[u8]>::from(bytes.as_ref())),
            ByteStorage::Owned(_) => self.clone(),
        }
    }
}
impl From<Arc<[u8]>> for CanonicalContributionBytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        Self(ByteStorage::Legacy(bytes))
    }
}
impl From<Vec<u8>> for CanonicalContributionBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl From<Box<[u8]>> for CanonicalContributionBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl From<&[u8]> for CanonicalContributionBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl<const N: usize> From<[u8; N]> for CanonicalContributionBytes {
    fn from(bytes: [u8; N]) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl<const N: usize> From<&[u8; N]> for CanonicalContributionBytes {
    fn from(bytes: &[u8; N]) -> Self {
        Self::from(bytes.as_slice())
    }
}
impl AsRef<[u8]> for CanonicalContributionBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl Deref for CanonicalContributionBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}
impl fmt::Debug for CanonicalContributionBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl PartialEq for CanonicalContributionBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for CanonicalContributionBytes {}

/// Inline values occupy the actual candidate container Layout; UTF8 retains
/// its complete original paid backing. No borrowed array pointer can escape.
#[derive(Clone)]
pub(crate) struct OwnedOrderKey(OrderKeyStorage);
#[derive(Clone)]
enum OrderKeyStorage {
    Null,
    Inline(OrderedScalar),
    Utf8(SourceBackingOwner<ByteBacking>),
}
impl fmt::Debug for OwnedOrderKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.borrowed().fmt(f)
    }
}
impl PartialEq for OwnedOrderKey {
    fn eq(&self, other: &Self) -> bool {
        match (self.borrowed(), other.borrowed()) {
            (OrderedTupleRef::Single(a), OrderedTupleRef::Single(b)) => a == b,
            _ => unreachable!("owned order keys always have exactly one borrowed scalar"),
        }
    }
}
impl Eq for OwnedOrderKey {}
impl OwnedOrderKey {
    pub(crate) fn borrowed(&self) -> OrderedTupleRef<'_> {
        OrderedTupleRef::Single(match &self.0 {
            OrderKeyStorage::Null => None,
            OrderKeyStorage::Inline(value) => Some(value.as_borrowed()),
            OrderKeyStorage::Utf8(owner) => {
                // SAFETY: only copy_utf8 creates this private immutable backing
                // from one complete valid str, retaining its whole paid owner.
                let text = unsafe { std::str::from_utf8_unchecked(&owner.borrowed().bytes) };
                Some(OrderedScalarRef::Utf8(text))
            }
        })
    }
}

/// Both real authors use the SAME supplied tracker. No default tracker or
/// foreign host can be paired with these vectors. The existing adapter Arc
/// retains its existing formal scaffold qualification, not a new payload grant.
#[derive(Clone)]
pub(crate) struct OrderedTopNAllocationHost {
    allocator: AggregateAllocator,
    host: Arc<dyn AggregateStateAllocator>,
}
impl OrderedTopNAllocationHost {
    pub(crate) fn new(tracker: Arc<MemTracker>) -> Self {
        Self {
            allocator: AggregateAllocator::new_nominal(Arc::clone(&tracker)),
            host: expression_allocation_host(tracker),
        }
    }

    pub(crate) fn vector_allocator(&self) -> AggregateAllocator {
        self.allocator.clone()
    }

    /// Borrow the same creating host for synchronous scalar/mask and filtered
    /// output admission. This clone creates neither a new payer nor a grant.
    pub(crate) fn scalar_allocator(&self) -> Arc<dyn AggregateStateAllocator> {
        Arc::clone(&self.host)
    }

    /// Retain a complete cold entity on the same host as its paid children.
    /// The prefix covers construction only; the returned owner retains the
    /// actual block until its final alias is destroyed.
    pub(crate) fn retain_shared<T: Send + Sync + 'static>(
        &self,
        value: T,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SourceBackingOwner<T>, KernelFailure> {
        let _prefix = self.reserve_prefix()?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let owner =
            SourceBackingOwner::try_from_owned_direct(value, Arc::clone(&self.host), &mut work)?;
        work.finish()?;
        Ok(owner)
    }

    /// Retain the complete validated order, not only its shared key inventory.
    pub(crate) fn shared_order_contract(
        &self,
        contract: &novarocks_local_program::StaticFilterContract,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<super::RuntimeOrderContractRef> {
        let order = self.order_contract(contract, control)?;
        let owner = self.retain_shared(order, control)?;
        Ok(super::RuntimeOrderContractRef::from_owned(owner))
    }

    /// Reconstruct the already frozen, first installed static order once on
    /// the actual creating host. No driver deep-copies these inner key slots.
    pub(crate) fn order_contract(
        &self,
        contract: &novarocks_local_program::StaticFilterContract,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<RuntimeOrderContract> {
        use novarocks_local_program::{FilterNullOrder, FilterSortDirection, StaticFilterContract};
        let StaticFilterContract::Ordered {
            keys,
            comparator_digest,
            contract_digest,
        } = contract
        else {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "pure Aggregate TopN runtime filter requires an ordered contract",
            ))
            .into());
        };
        let [key] = keys.as_ref() else {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "pure Aggregate TopN runtime filter requires one complete order key",
            ))
            .into());
        };
        if !novarocks_local_program::is_aggregate_topn_key_type(&key.value_type) {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "pure Aggregate TopN runtime filter order key type is not installed",
            ))
            .into());
        }
        let _prefix = self.reserve_prefix()?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let mut slots = AggregateVec::new_in(self.allocator.clone());
        slots.try_reserve_exact(1).map_err(|source| {
            self.allocator
                .allocation_error("reserve ordered key slots", source)
        })?;
        work.flush()?;
        let value_type = key.value_type.clone_observed(|| work.step())?;
        slots.push(RuntimeOrderKey::with_value_type(
            value_type,
            match key.direction {
                FilterSortDirection::Ascending => RuntimeOrderSortDirection::Ascending,
                FilterSortDirection::Descending => RuntimeOrderSortDirection::Descending,
            },
            match key.null_order {
                FilterNullOrder::First => RuntimeOrderNullOrder::First,
                FilterNullOrder::Last => RuntimeOrderNullOrder::Last,
            },
        ));
        // Reuse the original strict canonical author on these same paid
        // slots before a holder can escape or a producer can bind them.
        RuntimeOrderContract::validate_fragment_keys_observed(
            &slots,
            *comparator_digest,
            *contract_digest,
            |units| {
                if units == 0 {
                    return work.flush();
                }
                for _ in 0..units {
                    work.step()?;
                }
                Ok(())
            },
        )
        .map_err(|failure| match failure {
            RuntimeOrderContractValidationFailure::Contract(cause) => ExecutionFailure::from(cause),
            RuntimeOrderContractValidationFailure::Schema(cause) => ExecutionFailure::from(cause),
            RuntimeOrderContractValidationFailure::Work(cause) => ExecutionFailure::from(cause),
        })?;
        let owner = SourceBackingOwner::try_from_owned_direct(
            OrderKeyBacking { keys: slots },
            Arc::clone(&self.host),
            &mut work,
        )?;
        work.finish()?;
        Ok(RuntimeOrderContract::from_owned_keys(
            OwnedOrderKeys(owner),
            *comparator_digest,
            *contract_digest,
        ))
    }

    fn reserve_prefix(&self) -> Result<OpaqueReservation, KernelFailure> {
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?;
        authority.reserve_operation(source_custody_prefix_facts().requests().bytes())
    }

    fn retain_bytes<E: From<KernelFailure>>(
        &self,
        control: &dyn KernelEvaluationControl,
        build: impl FnOnce(
            &AggregateAllocator,
            &mut EvaluationCheckpoints<'_>,
        ) -> Result<AggregateVec<u8>, E>,
    ) -> Result<SourceBackingOwner<ByteBacking>, E> {
        // Fund the original cold-holder prefix before any control callback.
        let _prefix = self.reserve_prefix()?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let bytes = build(&self.allocator, &mut work)?;
        let owner = SourceBackingOwner::try_from_owned_direct(
            ByteBacking { bytes },
            Arc::clone(&self.host),
            &mut work,
        )?;
        work.finish()?;
        Ok(owner)
    }

    pub(crate) fn copy_utf8(
        &self,
        text: Option<&str>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<OwnedOrderKey, KernelFailure> {
        let Some(text) = text else {
            let _prefix = self.reserve_prefix()?;
            control.checkpoint(0)?;
            return Ok(OwnedOrderKey(OrderKeyStorage::Null));
        };
        let owner = self.retain_bytes(control, |allocator, work| {
            work.flush()?;
            let mut bytes = AggregateVec::new_in(allocator.clone());
            bytes.try_reserve_exact(text.len()).map_err(|source| {
                allocator.allocation_error("reserve ordered UTF-8 payload", source)
            })?;
            for chunk in text.as_bytes().chunks(MAX_UNOBSERVED_KERNEL_WORK as usize) {
                control.checkpoint(chunk.len() as u32)?;
                bytes.extend_from_slice(chunk);
            }
            work.flush()?;
            Ok::<_, KernelFailure>(bytes)
        })?;
        Ok(OwnedOrderKey(OrderKeyStorage::Utf8(owner)))
    }

    pub(crate) fn copy_scalar(
        &self,
        value: Option<OrderedScalarRef<'_>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<OwnedOrderKey, KernelFailure> {
        match value {
            None => self.copy_utf8(None, control),
            Some(OrderedScalarRef::Utf8(text)) => self.copy_utf8(Some(text), control),
            Some(value) => {
                control.checkpoint(0)?;
                Ok(OwnedOrderKey(OrderKeyStorage::Inline(value.into_owned())))
            }
        }
    }

    pub(crate) fn encode_ordered_bound(
        &self,
        tuple: OrderedTupleRef<'_>,
        contract: &RuntimeOrderContract,
        cap: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<CanonicalContributionBytes, ControlledContributionCodecError> {
        let owner = self.retain_bytes(control, |allocator, work| {
            // Counting itself observes the caller's control, so it belongs
            // inside this same paid prefix transaction, before vector creation.
            let len = borrowed_ordered_bound_encoded_len(tuple, contract, control)?;
            if len > cap {
                return Err(
                    super::contribution::ContributionCodecError::EncodedSizeExceeded.into(),
                );
            }
            work.flush()?;
            let mut bytes = AggregateVec::new_in(allocator.clone());
            bytes.try_reserve_exact(len).map_err(|source| {
                allocator.allocation_error("reserve ordered contribution payload", source)
            })?;
            // All output remains private until the complete visitor succeeds.
            // It supplies bounded slices and preserves the first sink/control cause.
            visit_borrowed_ordered_bound(tuple, contract, cap, control, |chunk| {
                if chunk.len() > bytes.capacity() - bytes.len() {
                    return Err(super::contribution::ContributionCodecError::LengthOverflow.into());
                }
                bytes.extend_from_slice(chunk);
                Ok(())
            })?;
            work.flush()?;
            Ok::<_, ControlledContributionCodecError>(bytes)
        })?;
        Ok(CanonicalContributionBytes(ByteStorage::Owned(owner)))
    }

    pub(crate) fn ordered_contribution(
        &self,
        tuple: OrderedTupleRef<'_>,
        contract: &RuntimeOrderContract,
        cap: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<super::RuntimeFilterContribution, ControlledContributionCodecError> {
        let bytes = self.encode_ordered_bound(tuple, contract, cap, control)?;
        Ok(super::RuntimeFilterContribution::from_shared(
            super::RuntimeFilterContributionKind::OrderedBound,
            contract.digest(),
            bytes,
        ))
    }
}

#[cfg(test)]
#[path = "owned_tests.rs"]
mod tests;
