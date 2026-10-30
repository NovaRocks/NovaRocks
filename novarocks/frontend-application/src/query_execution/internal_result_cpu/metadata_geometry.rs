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

//! Known metadata of the same closed Internal CPU factory used in production.
//! Layouts do not pay for backing, certify a task, or bound opaque pointees.

use super::{
    CpuWorkControl, ExecutionFailureInput, InternalCpuActionSource, InternalCpuRetired,
    InternalCpuRetirement, InternalDomainFailure, InternalDomainSlot, InternalResultCpuJob,
    InternalResultOperation, InternalResultValue, OperationResult, QueryBlockingExecutor,
    QueryBlockingFailure, StatusIntakeWake, make_cpu_work, make_operation_forwarder, oneshot,
};
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use std::{
    alloc::Layout,
    sync::{Arc, Weak},
};

type Output = Result<InternalDomainSlot, InternalDomainFailure>;
type Completion = oneshot::Sender<Result<InternalResultValue<Output>, ExecutionFailureInput>>;

/// These are known requested or inline layouts only. Caller-owned payload,
/// closure/input pointees, two oneshot internals, Tokio task/JoinError pointees,
/// platform Mutex backing, activity/Notify pointees and opaque panic payloads
/// remain the original creators' responsibility. Reading this is not payment.
///
/// The per-job known subtotal conservatively sums the one packet, original live job,
/// actual forwarder frame, one normal retirement stack value, up to seven
/// opaque carriers (worker + physical cause + two notification panics and Provider close/marker/input retirement panics), and one Arc<JoinError>.
/// Handles already contained in those layouts must not be added again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InternalCpuMetadataGeometry {
    packet: Layout,
    job: Layout,
    forwarder_frame: Layout,
    worker_call: Layout,
    retired: Layout,
    opaque_carrier: Layout,
    forwarder_failure: Layout,
    // Domain/operation are explanatory sub-layouts, not additive amounts.
    domain_slot: Layout,
    operation: Layout,
    // Provider input is a distinct Box pointee. Its wrapper already embeds
    // the ordinary job; the subtotal uses max(job, wrapper), not their sum.
    provider_input: Layout,
    provider_job: Layout,
    action_detail_backing_bytes: usize,
}
impl InternalCpuMetadataGeometry {
    pub const fn packet_layout(self) -> Layout {
        self.packet
    }
    pub const fn job_inline_layout(self) -> Layout {
        self.job
    }
    pub const fn forwarder_frame_layout(self) -> Layout {
        self.forwarder_frame
    }
    pub const fn worker_call_layout(self) -> Layout {
        self.worker_call
    }
    pub const fn retired_inline_layout(self) -> Layout {
        self.retired
    }
    pub const fn opaque_carrier_layout(self) -> Layout {
        self.opaque_carrier
    }
    pub const fn maximum_opaque_carriers_per_job(self) -> usize {
        7
    }
    pub const fn forwarder_failure_layout(self) -> Layout {
        self.forwarder_failure
    }
    pub const fn domain_slot_inline_layout(self) -> Layout {
        self.domain_slot
    }
    pub const fn operation_inline_layout(self) -> Layout {
        self.operation
    }

    pub const fn provider_creator_input_layout(self) -> Layout {
        self.provider_input
    }
    pub const fn provider_creator_job_layout(self) -> Layout {
        self.provider_job
    }
    /// One original refusal marker's exact bounded heap request. The inline
    /// String is already in ExecutionFailureInput; this is only its backing.
    pub const fn action_detail_backing_bytes(self) -> usize {
        self.action_detail_backing_bytes
    }
    /// Known additive component, not a complete allocation/task/payload quote.
    /// The diagnostic domain_slot/operation sub-layouts are already represented
    /// in the real packet/factory frame and are not charged a second time.
    pub fn known_metadata_bytes_per_job(self) -> Result<usize, LayoutResourceError> {
        let failures = self
            .opaque_carrier
            .size()
            .checked_mul(7)
            .ok_or(LayoutResourceError::Arithmetic)?;
        [
            self.packet.size(),
            self.job.size().max(self.provider_job.size()),
            self.forwarder_frame.size(),
            self.retired.size(),
            failures,
            self.forwarder_failure.size(),
            self.provider_input.size(),
            self.action_detail_backing_bytes,
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or(LayoutResourceError::Arithmetic)
        })
    }

    /// Supply a bound from the actual original role/job inventory, including
    /// waiting forwarders and failed packets until their final real exit.
    /// Window positions, running workers and queue positions alone do not
    /// establish that bound. This function does not install or enforce one.
    pub fn known_metadata_bytes_for_jobs(
        self,
        live_jobs: usize,
    ) -> Result<usize, LayoutResourceError> {
        self.known_metadata_bytes_per_job()?
            .checked_mul(live_jobs)
            .ok_or(LayoutResourceError::Arithmetic)
    }
}

type OperationForwarderFactory<F> = fn(
    QueryBlockingExecutor,
    Option<super::InternalCpuFactoryBinding>,
    Option<InternalCpuActionSource>,
    CpuWorkControl,
    Arc<dyn StatusIntakeWake>,
    Arc<InternalCpuRetirement<Output>>,
    Completion,
) -> F;

// An output type derived from the actual same production function, without
// constructing any Work, packet, operation, closure, runtime or input graph.
fn forwarder_layout<F: std::future::Future<Output = ()>>(
    _: OperationForwarderFactory<F>,
) -> Layout {
    Layout::new::<F>()
}

// Derive the actual Box-call closure from the same factory prepare consumes.
// This neither constructs a dummy packet/input nor assumes a capture ABI.
pub(super) fn operation_call_layout() -> Layout {
    fn of<F: FnOnce() -> OperationResult + Send + 'static>(
        _: fn(Weak<InternalCpuRetirement<Output>>, CpuWorkControl) -> F,
    ) -> Layout {
        QueryBlockingExecutor::preserving_call_layout::<OperationResult, F>()
    }
    of(make_cpu_work)
}

/// Exact source quote for the original worker row and actual same boxed call.
/// Packet/header/forwarder aliases outlive this action and still require their
/// original fixed role envelope; this value does not certify that payment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InternalCpuActionQuote {
    pub(crate) metadata_bytes: std::num::NonZeroU64,
    pub(crate) physical_queue_bytes: std::num::NonZeroU64,
}
pub(super) fn operation_action_quote(
    executor: &QueryBlockingExecutor,
) -> Result<InternalCpuActionQuote, super::WorkError> {
    action_quote_from_geometry(
        executor
            .queue_geometry()
            .map_err(|_| super::WorkError::ArithmeticOverflow)?,
    )
}

pub(super) fn operation_action_quote_for_composition()
-> Result<InternalCpuActionQuote, super::WorkError> {
    action_quote_from_geometry(
        QueryBlockingExecutor::queue_geometry_for_composition()
            .map_err(|_| super::WorkError::ArithmeticOverflow)?,
    )
}

impl InternalCpuActionQuote {
    pub const fn metadata_bytes(self) -> std::num::NonZeroU64 {
        self.metadata_bytes
    }
    pub const fn physical_queue_bytes(self) -> std::num::NonZeroU64 {
        self.physical_queue_bytes
    }
}

fn action_quote_from_geometry(
    geometry: novarocks_query_application::cpu::QueryBlockingQueueGeometry,
) -> Result<InternalCpuActionQuote, super::WorkError> {
    let queue_bytes = geometry
        .queue_record_inline
        .size()
        .checked_add(operation_call_layout().size())
        .ok_or(super::WorkError::ArithmeticOverflow)?;
    let nonzero = |bytes| {
        u64::try_from(bytes)
            .ok()
            .and_then(std::num::NonZeroU64::new)
            .ok_or(super::WorkError::ArithmeticOverflow)
    };
    Ok(InternalCpuActionQuote {
        metadata_bytes: nonzero(geometry.queue_record_inline.size())?,
        physical_queue_bytes: nonzero(queue_bytes)?,
    })
}

pub(super) fn geometry() -> Result<InternalCpuMetadataGeometry, LayoutResourceError> {
    Ok(InternalCpuMetadataGeometry {
        packet: arc_layout(Layout::new::<InternalCpuRetirement<Output>>())?,
        job: Layout::new::<InternalResultCpuJob<Output>>(),
        forwarder_frame: forwarder_layout(make_operation_forwarder),
        worker_call: operation_call_layout(),
        retired: Layout::new::<InternalCpuRetired<Output>>(),
        opaque_carrier: QueryBlockingFailure::payload_holder_layout()?,
        forwarder_failure: arc_layout(Layout::new::<tokio::task::JoinError>())?,
        domain_slot: Layout::new::<InternalDomainSlot>(),
        operation: Layout::new::<InternalResultOperation>(),
        provider_input: Layout::new::<super::scoped_statistics::ClosedCreatorInput>(),
        provider_job: Layout::new::<super::ProviderReadCpuJob>(),
        action_detail_backing_bytes: super::CPU_ACTION_DETAIL_BYTES,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_factory_layout_covers_actual_domain_inline_and_checked_inventory() {
        let actual = geometry().unwrap();
        assert_eq!(
            actual.domain_slot_inline_layout(),
            Layout::new::<InternalDomainSlot>()
        );
        assert!(actual.packet_layout().size() >= std::mem::size_of::<InternalDomainSlot>());
        assert!(actual.packet_layout().align() >= std::mem::align_of::<InternalDomainSlot>());
        assert!(actual.packet_layout().size() >= std::mem::size_of::<InternalResultOperation>());
        assert_eq!(actual.worker_call_layout(), operation_call_layout());
        assert_eq!(
            actual.action_detail_backing_bytes(),
            super::super::CPU_ACTION_DETAIL_BYTES
        );
        let one = actual.known_metadata_bytes_per_job().unwrap();
        assert!(one > actual.packet_layout().size());
        assert_eq!(actual.known_metadata_bytes_for_jobs(0), Ok(0));
        assert_eq!(
            actual.known_metadata_bytes_for_jobs(2),
            Ok(one.checked_mul(2).unwrap())
        );
        assert_eq!(
            actual.known_metadata_bytes_for_jobs(usize::MAX),
            Err(LayoutResourceError::Arithmetic)
        );
    }

    #[test]
    fn failure_carriers_include_provider_retirement_without_copying_payload_graphs() {
        let actual = geometry().unwrap();
        assert_eq!(actual.maximum_opaque_carriers_per_job(), 7);
        assert_eq!(
            actual.opaque_carrier_layout(),
            QueryBlockingFailure::payload_holder_layout().unwrap()
        );
        assert!(
            actual.opaque_carrier_layout().size() >= std::mem::size_of::<QueryBlockingFailure>()
        );
        // Opaque Box payload extents are deliberately not inferred from the
        // small carrier, even though the carrier layout itself is known.
        assert_eq!(
            actual.forwarder_frame_layout(),
            forwarder_layout(make_operation_forwarder)
        );
    }
}
