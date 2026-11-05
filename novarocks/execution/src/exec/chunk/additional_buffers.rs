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

//! One immutable additional-buffer extent, never a whole-batch lease.

use std::fmt;
use std::sync::{Arc, OnceLock};

use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, ScalarResourceError, SourceBackingOwner, source_custody_prefix_facts,
};

use super::Chunk;
use super::source_authority::{ChunkSourceAuthority, ChunkSourceAuthorityRef};

struct AdditionalBufferStock {
    source: ChunkSourceAuthority,
    host: Arc<dyn AggregateStateAllocator>,
    stock: OnceLock<OpaqueRetainedCharge>,
}

/// Whole clones retain the exact cold holder and original source handles.
/// The creator initializes its actual delta once before attaching an output;
/// zero bytes is an explicit initialized extent, not an absent authority.
#[derive(Clone)]
pub(crate) struct AdditionalBufferLedger(SourceBackingOwner<AdditionalBufferStock>);

fn invalid(message: &'static str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}

impl AdditionalBufferLedger {
    /// Construct the cold ledger before the public filter creates any output.
    /// The surrounding transaction retains raw output and input ahead of this
    /// ledger, so its payload charge is released only after their actual free.
    pub(crate) fn prepare(
        input: &Chunk,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let source = input.source_authority().capture().ok_or_else(|| {
            invalid("additional-buffer output requires the original attached source authority")
        })?;
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix =
            authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let holder_host = Arc::clone(&host);
        let owner = SourceBackingOwner::try_from_owned_direct(
            AdditionalBufferStock {
                source,
                host,
                stock: OnceLock::new(),
            },
            holder_host,
            &mut work,
        )?;
        work.finish()?;
        Ok(Self(owner))
    }

    /// Only the unpublished filter creator calls this, using its observed
    /// buffer delta while the public work reservation is still alive.
    pub(crate) fn initialize(&self, bytes: usize) -> Result<(), KernelFailure> {
        let stock = self.0.borrowed();
        if stock.stock.get().is_some() {
            return Err(invalid("additional-buffer extent was already initialized"));
        }
        i64::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(&stock.host))?;
        let mut reservation = charge.reserve_operation(bytes)?;
        charge.reconcile_under_reservation(bytes, &mut reservation)?;
        drop(reservation);
        stock.stock.set(charge).map_err(|rejected| {
            drop(rejected);
            invalid("additional-buffer extent was already initialized")
        })
    }

    pub(super) fn initialized(&self) -> bool {
        self.0.borrowed().stock.get().is_some()
    }

    pub(super) fn source_authority(&self) -> ChunkSourceAuthorityRef<'_> {
        self.0.borrowed().source.borrowed()
    }

    pub(super) fn holder_bytes(&self) -> Result<usize, ScalarResourceError> {
        Ok(
            SourceBackingOwner::<AdditionalBufferStock>::owned_request_facts()?
                .typed()
                .bytes(),
        )
    }
}

impl fmt::Debug for AdditionalBufferLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdditionalBuffers")
            .field(
                "bytes",
                &self
                    .0
                    .borrowed()
                    .stock
                    .get()
                    .map(OpaqueRetainedCharge::bytes),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::tracked_state_allocator::expression_allocation_host;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use arrow::array::{Int64Array, RecordBatch};
    use novarocks_functions::FunctionValueType;
    use novarocks_types::SlotId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct Control {
        calls: AtomicUsize,
        cancel: bool,
    }
    impl Control {
        fn running() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                cancel: false,
            }
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.cancel {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ledger creation never waits")
        }
    }
    fn source() -> Chunk {
        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        super::super::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            )],
        )
    }

    #[test]
    fn additional_extent_zero_retains_original_authority_and_refuses_whole_leases() {
        let tracker = MemTracker::new_root("additional-extent-query");
        tracker.install_limit_once(64 * 1024 * 1024).unwrap();
        let mut input = source();
        input.try_transfer_to(&tracker).unwrap();
        let identity = input.source_authority().owner_identity();
        let original = tracker.current();
        let ledger = AdditionalBufferLedger::prepare(
            &input,
            expression_allocation_host(tracker.clone()),
            &Control::running(),
        )
        .unwrap();
        assert!(
            tracker.current() > original,
            "the real cold ledger has nonzero stock"
        );
        let mut output = Chunk::try_new_with_chunk_schema(
            input.record_batch().clone(),
            input.chunk_schema_ref(),
        )
        .unwrap();
        assert!(matches!(
            output.attach_additional_buffers(ledger.clone()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        ledger.initialize(0).unwrap();
        output.attach_additional_buffers(ledger.clone()).unwrap();
        assert_eq!(output.source_authority().owner_identity(), identity);
        assert!(output.memory_lease().is_none());
        assert!(output.take_memory_lease().is_none());
        assert_eq!(output.source_authority().owner_identity(), identity);
        let other = MemTracker::new_root("additional-extent-handoff");
        other.install_limit_once(1).unwrap();
        let paid = tracker.current();
        output.transfer_to(&other);
        output.try_transfer_to(&other).unwrap();
        assert_eq!(other.current(), 0);
        assert_eq!(tracker.current(), paid);

        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker.clone(), Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        let owner = factory
            .begin_typed_construction(output.chunk_schema())
            .unwrap();
        let view = output
            .slice_typed_observed::<crate::runtime::task::ExecutionFailure, _>(
                1,
                1,
                owner.storage_lifetime(),
                |step| Ok(owner.observe(0, &[], step)?),
            )
            .unwrap();
        assert_eq!(view.source_authority().owner_identity(), identity);
        assert!(view.memory_lease().is_none());
        drop((input, output, owner, factory, ledger));
        assert!(tracker.current() > 0);
        std::thread::spawn(move || drop(view)).join().unwrap();
        assert_eq!(tracker.current(), 0);
        // This proves whole-Chunk/view extent sharing only. The actual filter
        // creator separately retains its backing through the last Buffer alias.
    }

    #[test]
    fn additional_extent_requires_original_source_and_preserves_cold_control_failure() {
        let tracker = MemTracker::new_root("additional-extent-source-refusal");
        let mut input = source();
        let control = Control::running();
        assert!(matches!(
            AdditionalBufferLedger::prepare(
                &input,
                expression_allocation_host(tracker.clone()),
                &control
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(control.calls.load(Ordering::SeqCst), 0);
        input.try_transfer_to(&tracker).unwrap();
        let original = tracker.current();
        let cancelled = Control {
            cancel: true,
            ..Control::running()
        };
        assert!(matches!(
            AdditionalBufferLedger::prepare(
                &input,
                expression_allocation_host(tracker.clone()),
                &cancelled
            ),
            Err(KernelFailure::Cancelled)
        ));
        assert_eq!(tracker.current(), original);
        drop(input);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn additional_extent_payload_limit_refusal_keeps_its_unpublished_zero_stock() {
        let tracker = MemTracker::new_root("additional-extent-stock-refusal");
        let mut input = source();
        input.try_transfer_to(&tracker).unwrap();
        let ledger = AdditionalBufferLedger::prepare(
            &input,
            expression_allocation_host(tracker.clone()),
            &Control::running(),
        )
        .unwrap();
        let cold = tracker.current();
        tracker.install_limit_once(cold + 1).unwrap();
        let refusal = ledger
            .initialize(2)
            .expect_err("positive host limit refuses growth");
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&refusal).class(),
            novarocks_failure_contract::FailureClass::Resource,
            "{refusal:?}"
        );
        assert!(!ledger.initialized());
        assert_eq!(tracker.current(), cold);
        ledger.initialize(0).unwrap();
        assert!(ledger.initialized());
        assert!(matches!(
            ledger.initialize(0),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(tracker.current(), cold);
        drop((input, ledger));
        assert_eq!(tracker.current(), 0);
    }
}
