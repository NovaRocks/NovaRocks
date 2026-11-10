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

//! Actual process/keyed capacity travels in original reservation partitions.
//! These wrappers contain no usage counter, limit, waiter cap or new Work.

use crate::native::transport_capacity::{
    NativeTransportCapacity, TOTAL_BYTES, TransportCapacityError, TransportCapacityLane,
};
use novarocks_control_runtime::{
    obligation::ReservationObligation, owned_return::OwnedReturnSender, transaction::RuntimeError,
};
use novarocks_control_runtime_core::{capacity::ReservationResources, identity::ReservationId};
use novarocks_types::BackendProcessId;

#[derive(Debug)]
pub(crate) enum CapacityFailure {
    Runtime(RuntimeError),
    Layout(TransportCapacityError),
    WrongOriginalResources,
}
impl From<RuntimeError> for CapacityFailure {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}
impl From<TransportCapacityError> for CapacityFailure {
    fn from(value: TransportCapacityError) -> Self {
        Self::Layout(value)
    }
}

/// The Source obtains this partition from its same bound Work before handing
/// metadata/header/report/capture storage to ExitCustody. No fresh admission
/// occurs here. A dropped active partition remains unresolved by the runtime.
struct OriginalQuota {
    parent: ReservationId,
    original: ReservationObligation,
}
impl OriginalQuota {
    fn split(
        original: &mut ReservationObligation,
        expected: &ReservationResources,
    ) -> Result<Self, CapacityFailure> {
        let current = original.resources()?;
        if current.quota != expected.quota || current.keyed != expected.keyed {
            return Err(CapacityFailure::WrongOriginalResources);
        }
        let quota = ReservationResources {
            quota: current.quota,
            keyed: current.keyed,
            owned_slots: Default::default(),
        };
        let parent = original.id();
        let original = original.split_next(&quota)?;
        Ok(Self { parent, original })
    }
    fn refund(&mut self, delta: ReservationResources) -> Result<(), CapacityFailure> {
        self.original
            .release_quota_with_keyed(&delta.quota, delta.keyed.as_ref())?;
        Ok(())
    }
}

pub(crate) struct QueueWindow {
    quota: OriginalQuota,
    layout: NativeTransportCapacity,
    backend: BackendProcessId,
    lane: TransportCapacityLane,
    queued_bytes: usize,
}
impl QueueWindow {
    pub(crate) fn split(
        original: &mut ReservationObligation,
        layout: NativeTransportCapacity,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        items: usize,
        queued_bytes: usize,
    ) -> Result<Self, CapacityFailure> {
        let expected = layout.queue_resources(backend, lane, items, queued_bytes)?;
        let quota = OriginalQuota::split(original, &expected)?;
        Ok(Self {
            quota,
            layout,
            backend,
            lane,
            queued_bytes,
        })
    }
    pub(crate) fn parent_reservation(&self) -> ReservationId {
        self.quota.parent
    }
    /// After actual encoding and immediately before RPC use. Only the original
    /// queue-window shares return; total item/byte and encoded shares remain.
    pub(crate) fn partition_obligation(&self) -> &ReservationObligation {
        &self.quota.original
    }

    pub(crate) fn mark_in_flight(&mut self) -> Result<(), CapacityFailure> {
        self.quota.refund(self.layout.in_flight_release(
            self.backend,
            self.lane,
            self.queued_bytes,
        )?)
    }
    /// Moving this object back to its Source grants no refund or exit fact.
    pub(crate) fn into_original(self) -> ReservationObligation {
        self.quota.original
    }
}

pub(crate) struct EncodingWindow {
    quota: OriginalQuota,
    layout: NativeTransportCapacity,
    backend: BackendProcessId,
    lane: TransportCapacityLane,
}
impl EncodingWindow {
    pub(crate) fn split(
        original: &mut ReservationObligation,
        layout: NativeTransportCapacity,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        quoted_bytes: usize,
    ) -> Result<Self, CapacityFailure> {
        let expected = layout.encoding_resources(backend, lane, quoted_bytes)?;
        let quota = OriginalQuota::split(original, &expected)?;
        Ok(Self {
            quota,
            layout,
            backend,
            lane,
        })
    }
    /// Derive the refund from the exact current original partition, never an
    /// independent shadow byte counter. P06 preserves the original family/key.
    pub(crate) fn shrink_to(&mut self, actual: usize) -> Result<(), CapacityFailure> {
        let reserved = usize::try_from(self.quota.original.resources()?.quota.get(TOTAL_BYTES))
            .map_err(|_| CapacityFailure::WrongOriginalResources)?;
        self.quota.refund(self.layout.encoding_shrink_release(
            self.backend,
            self.lane,
            reserved,
            actual,
        )?)
    }
    pub(crate) fn into_original(self) -> ReservationObligation {
        self.quota.original
    }
}

/// The Source returns these same actual financial objects to the Native row
/// after its unique transport Join. This is the encoded branch only; an unused
/// original guard stays owned by that row and is never represented by None.
/// Transferring this bundle performs no refund and certifies no exit.
#[must_use = "The original Source must retire or retain both financial legs"]
pub(crate) struct ReturnedQuota {
    pub(crate) queue: ReservationObligation,
    pub(crate) encoding: ReservationObligation,
}
/// Only Native's private actual-disposal continuation can construct this final
/// packet. Its Encoded/Unused funding distinction is data, not an exit grant.
/// Both variants return the same WC inputs without implicit Drop refunds;
/// Source must finish all actual financial legs before WC queue_exited_in.
pub(crate) type ReturnedPhysical = crate::native::apply_send_owner::SharedPhysicalReturn;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QuotaLeg {
    Queue,
}
struct ReturnQuota {
    leg: QuotaLeg,
    source: std::sync::Weak<dyn super::source_ports::TransportReports>,
}
struct UnprovenQuotaReturn {
    _original: ReservationObligation,
}
impl ReturnQuota {
    fn stage(&self, original: ReservationObligation) {
        let Some(source) = self.source.upgrade() else {
            std::panic::resume_unwind(Box::new(UnprovenQuotaReturn {
                _original: original,
            }));
        };
        if let Err(original) = source.stage_quota(self.leg, original) {
            std::panic::resume_unwind(Box::new(UnprovenQuotaReturn {
                _original: original,
            }));
        }
    }
}
/// GatedWork destroys this capture on accepted exit AND every unused path.
/// Only its financial object moves back to Source staging. No refund, finish,
/// body/capture exit or Join fact is inferred by this transfer. Source cannot
/// call it final physical return until the same actual Join/reaper cut wins.
pub(crate) struct QueueCustody {
    window: Option<QueueWindow>,
    returned: ReturnQuota,
}
impl QueueCustody {
    pub(crate) fn new(
        window: QueueWindow,
        source: std::sync::Weak<dyn super::source_ports::TransportReports>,
    ) -> Self {
        Self {
            window: Some(window),
            returned: ReturnQuota {
                leg: QuotaLeg::Queue,
                source,
            },
        }
    }
    pub(crate) fn parent_reservation(&self) -> ReservationId {
        self.window.as_ref().unwrap().parent_reservation()
    }
    pub(crate) fn partition(&self) -> ReservationId {
        self.window
            .as_ref()
            .expect("original queue custody")
            .quota
            .original
            .id()
    }
    pub(crate) fn mark_in_flight(&mut self) -> Result<(), CapacityFailure> {
        self.window.as_mut().unwrap().mark_in_flight()
    }
}
impl Drop for QueueCustody {
    fn drop(&mut self) {
        if let Some(window) = self.window.take() {
            self.returned.stage(window.into_original());
        }
    }
}
pub(crate) struct EncodingCustody {
    original: Option<(EncodingWindow, OwnedReturnSender<ReservationObligation>)>,
}
impl EncodingCustody {
    pub(crate) fn new(
        window: EncodingWindow,
        returned: OwnedReturnSender<ReservationObligation>,
    ) -> Self {
        Self {
            original: Some((window, returned)),
        }
    }
    pub(crate) fn shrink_to(&mut self, actual: usize) -> Result<(), CapacityFailure> {
        let (window, _) = self.original.as_mut().expect("live original encoding");
        window.shrink_to(actual)
    }
}
impl Drop for EncodingCustody {
    fn drop(&mut self) {
        if let Some((window, returned)) = self.original.take() {
            if let Err(refused) = returned.publish(window.into_original()) {
                std::panic::resume_unwind(Box::new(refused));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::transport_capacity::{
        ORDINARY_QUEUE_BYTES, TARGET_FAMILY, TARGET_ORDINARY_QUEUE_BYTES,
    };
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        identity::Generation,
        scope::{WorkAdmission, WorkKind},
    };
    fn finish(mut original: ReservationObligation) {
        let remaining = original.resources().unwrap();
        original
            .release_quota_with_keyed(&remaining.quota, remaining.keyed.as_ref())
            .unwrap();
        original.finish().unwrap();
    }
    #[test]
    fn queue_and_encoding_partitions_keep_the_original_work_key_and_real_usage_through_return() {
        let layout =
            NativeTransportCapacity::from_transport(novarocks_task_codec::TransportBudget::DEFAULT)
                .unwrap();
        let ledger =
            RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
                .unwrap();
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let backend = BackendProcessId::new_v7();
        let (mut parent, mut metadata) = scope
            .reserve_next(
                generation,
                layout
                    .queue_resources(backend, TransportCapacityLane::Ordinary, 1, 512)
                    .unwrap(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let mut queue = QueueWindow::split(
            &mut metadata,
            layout.clone(),
            backend,
            TransportCapacityLane::Ordinary,
            1,
            512,
        )
        .unwrap();
        assert!(metadata.resources().unwrap().quota.is_empty());
        assert!(
            metadata
                .resources()
                .unwrap()
                .keyed
                .as_ref()
                .is_none_or(|quota| quota.quota.is_empty())
        );
        let (mut child, mut child_metadata) = scope
            .reserve_child_next(
                generation,
                queue.parent_reservation(),
                layout
                    .encoding_resources(backend, TransportCapacityLane::Ordinary, 512)
                    .unwrap(),
                WorkKind::Business,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let mut encoding = EncodingWindow::split(
            &mut child_metadata,
            layout.clone(),
            backend,
            TransportCapacityLane::Ordinary,
            512,
        )
        .unwrap();
        assert_eq!(ledger.used(TOTAL_BYTES), 1024);
        encoding.shrink_to(128).unwrap();
        assert_eq!(ledger.used(TOTAL_BYTES), 640);
        queue.mark_in_flight().unwrap();
        assert_eq!(ledger.used(TOTAL_BYTES), 640);
        assert_eq!(ledger.used(ORDINARY_QUEUE_BYTES), 0);
        let keyed = ledger.try_keyed_usage_snapshot().unwrap();
        assert_eq!(keyed.len(), 1);
        assert_eq!(keyed[0].family, TARGET_FAMILY);
        assert_eq!(keyed[0].key, NativeTransportCapacity::backend_key(backend));
        assert_eq!(keyed[0].used.get(TARGET_ORDINARY_QUEUE_BYTES), 0);
        let original_queue = queue.into_original();
        let original_encoding = encoding.into_original();
        assert_eq!(
            ledger.used(TOTAL_BYTES),
            640,
            "returning original objects does not refund or invent exit"
        );
        parent.start().unwrap();
        child.start().unwrap();
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        assert!(
            !parent.actual_exit_ready().unwrap(),
            "the original child still blocks parent retirement"
        );
        child.body_exited().unwrap();
        child.captures_exited().unwrap();
        finish(original_encoding);
        finish(child_metadata);
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        assert!(parent.actual_exit_ready().unwrap());
        finish(original_queue);
        finish(metadata);
        ledger.reap_scope_ready(scope.id(), 8).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
