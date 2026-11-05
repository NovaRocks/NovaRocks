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

//! Share the original order contract without detaching its actual creator.

use super::contribution::RuntimeOrderContract;
use novarocks_functions::SourceBackingOwner;
use std::{fmt, ops::Deref, sync::Arc};

#[derive(Clone)]
enum Storage {
    Legacy(Arc<RuntimeOrderContract>),
    Owned(SourceBackingOwner<RuntimeOrderContract>),
}

/// Ordinary contracts retain their original Arc sharing. A paid contract
/// retains its whole creating owner through session/request/artifact aliases.
/// Cloning either form never deep-copies the legacy key Vec.
#[derive(Clone)]
pub struct RuntimeOrderContractRef(Storage);

impl RuntimeOrderContractRef {
    pub fn from_legacy(contract: Arc<RuntimeOrderContract>) -> Self {
        Self(Storage::Legacy(contract))
    }

    pub(crate) fn from_owned(contract: SourceBackingOwner<RuntimeOrderContract>) -> Self {
        Self(Storage::Owned(contract))
    }
}
impl From<Arc<RuntimeOrderContract>> for RuntimeOrderContractRef {
    fn from(contract: Arc<RuntimeOrderContract>) -> Self {
        Self::from_legacy(contract)
    }
}
impl AsRef<RuntimeOrderContract> for RuntimeOrderContractRef {
    fn as_ref(&self) -> &RuntimeOrderContract {
        match &self.0 {
            Storage::Legacy(contract) => contract,
            Storage::Owned(contract) => contract.borrowed(),
        }
    }
}
impl Deref for RuntimeOrderContractRef {
    type Target = RuntimeOrderContract;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
impl fmt::Debug for RuntimeOrderContractRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(formatter)
    }
}
impl PartialEq for RuntimeOrderContractRef {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for RuntimeOrderContractRef {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        runtime::{mem_tracker::MemTracker, task::ExecutionFailureCause},
        runtime_filter::{
            contribution::{RuntimeOrderKey, RuntimeOrderNullOrder, RuntimeOrderSortDirection},
            owned::OrderedTopNAllocationHost,
        },
    };
    use arrow::datatypes::DataType;
    use novarocks_functions::{
        KernelEvaluationControl, KernelFailure, source_custody_prefix_facts,
    };
    use novarocks_local_program::{
        FilterNullOrder, FilterOrderKey, FilterOrderKeys, FilterSortDirection, StaticFilterContract,
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct Control {
        calls: AtomicUsize,
        refuse_at: Option<usize>,
        cause: KernelFailure,
    }
    impl Control {
        fn pass() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                refuse_at: None,
                cause: KernelFailure::Cancelled,
            }
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.refuse_at == Some(ordinal) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("order contract construction never waits");
        }
    }

    fn frozen_utf8_order() -> StaticFilterContract {
        // Reuse the ASC/NULLFirst literal in owned_tests::static_utf8_order.
        // The original strict constructor below independently checks it;
        // this fixture adds no digest or type encoder.
        let comparator_digest =
            hex::decode("77782fddf467caee0ce29343ed593a1acb8cf89a46ef1f6765730c4bdbabc51d")
                .unwrap()
                .try_into()
                .unwrap();
        let contract_digest =
            hex::decode("5137b9bc01ebb44e0f02e78917ef2e76a655d5320b360f7ec68ff2566970ca62")
                .unwrap()
                .try_into()
                .unwrap();
        StaticFilterContract::Ordered {
            keys: FilterOrderKeys::from_legacy(Arc::from([FilterOrderKey {
                value_type: novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
                direction: FilterSortDirection::Ascending,
                null_order: FilterNullOrder::First,
            }])),
            comparator_digest,
            contract_digest,
        }
    }

    fn legacy_order(frozen: &StaticFilterContract) -> RuntimeOrderContract {
        let StaticFilterContract::Ordered {
            comparator_digest,
            contract_digest,
            ..
        } = frozen
        else {
            unreachable!()
        };
        RuntimeOrderContract::from_fragment_contract(
            [RuntimeOrderKey::with_value_type(
                novarocks_functions::FunctionValueType::new(DataType::Utf8, true),
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::First,
            )],
            *comparator_digest,
            *contract_digest,
        )
        .unwrap()
    }

    // Owner-local evidence uses the actual original tracker and strict contract
    // author, not a private allocator trace, Native or GLOBAL qualification.
    #[test]
    fn ordered_contract_ref_preserves_legacy_arc_and_last_paid_whole_alias() {
        let frozen = frozen_utf8_order();
        let legacy_arc = Arc::new(legacy_order(&frozen));
        let original = Arc::as_ptr(&legacy_arc);
        let legacy = RuntimeOrderContractRef::from_legacy(legacy_arc.clone());
        let legacy_copy = legacy.clone();
        assert!(std::ptr::eq(original, legacy.as_ref()));
        assert!(std::ptr::eq(legacy.as_ref(), legacy_copy.as_ref()));
        assert_eq!(legacy.keys().as_ptr(), legacy_copy.keys().as_ptr());
        assert_eq!(format!("{legacy:?}"), format!("{legacy_arc:?}"));

        let tracker = MemTracker::new_root("ordered-contract-ref-last-alias");
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let inner = host.order_contract(&frozen, &Control::pass()).unwrap();
        let inner_stock = tracker.current();
        assert!(inner_stock > 0);
        drop(inner);
        assert_eq!(tracker.current(), 0);
        let owned = host
            .shared_order_contract(&frozen, &Control::pass())
            .unwrap();
        assert_eq!(owned, legacy);
        assert_eq!(format!("{owned:?}"), format!("{legacy:?}"));
        assert!(owned.keys()[0].value_type().unwrap().nullable());
        let paid = tracker.current();
        assert!(
            paid > inner_stock,
            "the complete outer owner has actual stock"
        );
        let last = owned.clone();
        assert!(std::ptr::eq(owned.as_ref(), last.as_ref()));
        assert_eq!(tracker.current(), paid);
        drop(frozen);
        drop(legacy_arc);
        drop(legacy);
        drop(legacy_copy);
        drop(host);
        drop(owned);
        assert_eq!(tracker.current(), paid);
        std::thread::spawn(move || {
            assert_eq!(last.keys()[0].data_type(), &DataType::Utf8);
            drop(last);
        })
        .join()
        .unwrap();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn ordered_contract_ref_refuses_real_prefix_slots_and_outer_stock() {
        let frozen = frozen_utf8_order();
        let prefix = source_custody_prefix_facts().requests().bytes();
        for limit in [1, prefix + std::mem::size_of::<RuntimeOrderKey>()] {
            let tracker = MemTracker::new_root("ordered-contract-ref-prefix-refusal");
            tracker.install_limit_once(limit as i64).unwrap();
            let host = OrderedTopNAllocationHost::new(tracker.clone());
            let control = Control::pass();
            let error = host.shared_order_contract(&frozen, &control).unwrap_err();
            crate::runtime_filter::assert_real_tracker_capacity(&error, limit as i64);
            assert_eq!(tracker.current(), 0);
            if limit == 1 {
                assert_eq!(control.calls.load(Ordering::SeqCst), 0);
            } else {
                assert!(control.calls.load(Ordering::SeqCst) > 0);
            }
        }

        let tracker = MemTracker::new_root("ordered-contract-ref-outer-refusal");
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let inner = host.order_contract(&frozen, &Control::pass()).unwrap();
        let inner_stock = tracker.current();
        tracker
            .install_limit_once(inner_stock + i64::try_from(prefix).unwrap())
            .unwrap();
        let control = Control::pass();
        let error = host
            .retain_shared(inner, &control)
            .err()
            .expect("the outer holder cannot fit beside its admitted prefix");
        crate::runtime_filter::assert_real_tracker_capacity(
            &error,
            inner_stock + i64::try_from(prefix).unwrap(),
        );
        assert!(control.calls.load(Ordering::SeqCst) > 0);
        assert_eq!(
            tracker.current(),
            0,
            "failed outer creation frees its moved inner"
        );
    }

    #[test]
    fn ordered_contract_ref_preserves_outer_control_first_causes() {
        let frozen = frozen_utf8_order();
        let tracker = MemTracker::new_root("ordered-contract-ref-control-refusal");
        let host = OrderedTopNAllocationHost::new(tracker.clone());
        let inner_control = Control::pass();
        drop(host.order_contract(&frozen, &inner_control).unwrap());
        let first_outer = inner_control.calls.load(Ordering::SeqCst);
        let full_control = Control::pass();
        drop(host.shared_order_contract(&frozen, &full_control).unwrap());
        let all_calls = full_control.calls.load(Ordering::SeqCst);
        assert!(all_calls > first_outer);
        assert_eq!(tracker.current(), 0);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for ordinal in [first_outer, all_calls - 1] {
                let control = Control {
                    refuse_at: Some(ordinal),
                    cause: cause.clone(),
                    ..Control::pass()
                };
                let error = host.shared_order_contract(&frozen, &control).unwrap_err();
                assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
                assert_eq!(control.calls.load(Ordering::SeqCst), ordinal + 1);
                assert_eq!(tracker.current(), 0);
            }
        }
    }
}
