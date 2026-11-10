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

use super::{CustodyError, TransportJoinRegistration, TransportRegistrationState};
use crate::{
    native::transport_capacity::{NativeTransportCapacity, TransportCapacityLane},
    task_execution::host::transport::EncodingWindow,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase, ExitObligation},
    obligation::{ReservationObligation, StepRegistration},
    owned_return::{OwnedReturnReader, OwnedReturnSender},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{
    completion::SlotState, identity::ConsumerId, scope::WorkKind,
};
use novarocks_types::BackendProcessId;

pub(super) struct EncodingChild {
    pub(super) custody: ExitCustody,
    returned: OwnedReturnReader<ReservationObligation>,
    observer: ConsumerId,
    observed: bool,
    partition_taken: bool,
}
impl EncodingChild {
    pub(super) fn take_partition(&mut self) -> Result<ReservationObligation, CustodyError> {
        if self.partition_taken {
            return Err(CustodyError::CompletionAlreadyIssued);
        }
        if !self.custody.actual_exit_ready()? {
            return Err(CustodyError::OriginalCapturesUnresolved);
        }
        if !self.observed {
            let report = self.returned.report_slot();
            match report.publication_state()? {
                SlotState::Filled => report.consume(self.observer)?,
                SlotState::RevokedUnused => (),
                SlotState::Reserved => return Err(CustodyError::OriginalCapturesUnresolved),
            }
            self.observed = true;
        }
        let partition = self
            .returned
            .try_take()?
            .ok_or(CustodyError::OriginalCapturesUnresolved)?;
        self.partition_taken = true;
        Ok(partition)
    }

    pub(super) fn retired(custody: &ExitCustody) -> Result<bool, CustodyError> {
        Ok(custody.phase() == ExitCustodyPhase::Retired || custody.try_retire()?)
    }
}

pub(crate) struct EncodingJob {
    pub(crate) exit: ExitObligation,
    pub(crate) window: EncodingWindow,
    pub(crate) returned: OwnedReturnSender<ReservationObligation>,
}
pub(crate) struct EncodingBinding {
    pub(crate) layout: NativeTransportCapacity,
    pub(crate) backend: BackendProcessId,
    pub(crate) lane: TransportCapacityLane,
    pub(crate) quoted_bytes: usize,
}
impl TransportJoinRegistration {
    /// Bind the same unbound child retained by the accepted Native row. The
    /// body runs inline inside the parent Join. Only the Source owns the
    /// child's report consumer and original return reader.
    pub(crate) fn bind_encoding(
        &self,
        guard: StepRegistration,
        binding: EncodingBinding,
    ) -> Result<EncodingJob, CustodyError> {
        let record = self.original.upgrade().expect("original send Join owner");
        assert!(guard.is_direct_child_of(&record.scope, record.parent));
        assert!(matches!(
            *record.state.lock().unwrap_or_else(|error| error.into_inner()),
            TransportRegistrationState::Attached(_, id) if id == guard.reservation_id()
        ));
        let kind = if binding.lane == TransportCapacityLane::Control {
            WorkKind::Settlement
        } else {
            WorkKind::Business
        };
        let (work, mut capacity) = guard.bind(kind)?;
        let window = EncodingWindow::split(
            &mut capacity,
            binding.layout,
            binding.backend,
            binding.lane,
            binding.quoted_bytes,
        )
        .expect("the original child reserves the exact encoding window");
        let observer = ConsumerId::new(1).expect("single original encoding observer");
        let report = ReportSlot::reserve_exit_observer_for_admitted(&mut capacity, observer)?;
        let original = ReportedWork::try_new(work, capacity, report)
            .expect("original encoding report and capture positions");
        let (returned, reader) = original.report().claim_owned_return()?;
        let (custody, exit) = ExitCustody::from_admitted(original);
        let mut encoding = record
            .encoding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(encoding.is_none(), "one original encoding child");
        *encoding = Some(EncodingChild {
            custody,
            returned: reader,
            observer,
            observed: false,
            partition_taken: false,
        });
        Ok(EncodingJob {
            exit,
            window,
            returned,
        })
    }
}

pub(crate) fn owned_layouts() -> Result<
    [std::alloc::Layout; 2],
    novarocks_type_contract::owned_resources::layout::LayoutResourceError,
> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    Ok([
        arc_layout(ExitCustody::backing_payload_layout())?,
        arc_layout(OwnedReturnSender::<ReservationObligation>::backing_payload_layout())?,
    ])
}
