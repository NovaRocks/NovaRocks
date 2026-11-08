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

//! The original Native transport windows, expressed by the shared ledger.
//!
//! Let I/B be process items/bytes, TI/TB the ordinary target items/bytes,
//! CI the maximum batch items, MB the maximum encoded batch bytes, and
//! CB=2*MB. TB=max(configured target bytes, CB), exactly as the retired
//! supervisor. Process dimensions precede keyed dimensions on admission.
//!
//! | Dimension | Limit | Queue stage | Encoding stage | In-flight release |
//! |---|---|---|---|---|
//! | 1 total items | I | items | - | - |
//! | 2 total bytes | B | queued bytes | encoded bytes | - |
//! | 3 ordinary items | I-CI | ordinary items | - | - |
//! | 4 ordinary bytes | B-CB | ordinary queued bytes | ordinary encoded bytes | - |
//! | 5 ordinary queue bytes | B-CB-MB | ordinary queued bytes | - | queue bytes |
//! | 6 control queue bytes | CB-MB | control queued bytes | - | queue bytes |
//! | 7 target items | TI+CI | items | - | - |
//! | 8 target bytes | TB+CB | queued bytes | encoded bytes | - |
//! | 9 target ordinary items | TI | ordinary items | - | - |
//! | 10 target ordinary bytes | TB | ordinary queued bytes | ordinary encoded bytes | - |
//! | 11 target ordinary queue bytes | TB-MB | ordinary queued bytes | - | queue bytes |
//!
//! `process_limits` and `keyed_families` configure the original ledger once.
//! `queue_resources`/`encoding_resources` describe one original reservation;
//! `in_flight_release` and `encoding_shrink_release` describe partial refunds
//! from that same reservation. Queued bytes already include the frozen carrier
//! overhead (256 bytes); this layout never guesses payload size or double-adds it.
//! No counters, grants, waiter limits, or private capacity wallet live here.

use novarocks_control_runtime::transaction::{
    CapacityDenial, CapacityObservationError, RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, KeyedQuota, ReservationResources, ResourceVector},
    identity::{CapacityKey, DimensionId, FamilyId},
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::BackendProcessId;

pub(crate) const TOTAL_ITEMS: DimensionId = DimensionId::new(1).unwrap();
pub(crate) const TOTAL_BYTES: DimensionId = DimensionId::new(2).unwrap();
pub(crate) const ORDINARY_ITEMS: DimensionId = DimensionId::new(3).unwrap();
pub(crate) const ORDINARY_BYTES: DimensionId = DimensionId::new(4).unwrap();
pub(crate) const ORDINARY_QUEUE_BYTES: DimensionId = DimensionId::new(5).unwrap();
pub(crate) const CONTROL_QUEUE_BYTES: DimensionId = DimensionId::new(6).unwrap();
pub(crate) const TARGET_ITEMS: DimensionId = DimensionId::new(7).unwrap();
pub(crate) const TARGET_BYTES: DimensionId = DimensionId::new(8).unwrap();
pub(crate) const TARGET_ORDINARY_ITEMS: DimensionId = DimensionId::new(9).unwrap();
pub(crate) const TARGET_ORDINARY_BYTES: DimensionId = DimensionId::new(10).unwrap();
pub(crate) const TARGET_ORDINARY_QUEUE_BYTES: DimensionId = DimensionId::new(11).unwrap();
pub(crate) const TARGET_FAMILY: FamilyId = FamilyId::new(1).unwrap();
const PROCESS_DIMENSIONS: [DimensionId; 6] = [
    TOTAL_ITEMS,
    TOTAL_BYTES,
    ORDINARY_ITEMS,
    ORDINARY_BYTES,
    ORDINARY_QUEUE_BYTES,
    CONTROL_QUEUE_BYTES,
];
const TARGET_DIMENSIONS: [DimensionId; 5] = [
    TARGET_ITEMS,
    TARGET_BYTES,
    TARGET_ORDINARY_ITEMS,
    TARGET_ORDINARY_BYTES,
    TARGET_ORDINARY_QUEUE_BYTES,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportCapacityLane {
    Ordinary,
    Control,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportCapacityError {
    InvalidQueue,
    InvalidEncodingBytes,
    InvalidShrink,
    Arithmetic,
    ForeignDimension,
    InvalidBackendKey,
    Ledger(CapacityError),
}
impl From<CapacityError> for TransportCapacityError {
    fn from(error: CapacityError) -> Self {
        Self::Ledger(error)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportCapacityRefusal {
    ProcessFull,
    TargetFull(BackendProcessId),
}

/// Frozen configuration values from the same source that owns transport input
/// limits. These observations grant no capacity and define no registrant cap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransportCapacityFacts {
    pub(crate) process_items: usize,
    pub(crate) process_retained_bytes: usize,
    pub(crate) target_items: usize,
    pub(crate) target_retained_bytes: usize,
    pub(crate) control_reserved_items: usize,
    pub(crate) control_reserved_bytes: usize,
    pub(crate) maximum_batch_encoded_bytes: usize,
}
#[derive(Clone, Debug)]
pub(crate) struct NativeTransportCapacity {
    source: TransportBudget,
    facts: TransportCapacityFacts,
    process_limits: ResourceVector,
    target_limits: ResourceVector,
}
impl NativeTransportCapacity {
    pub(crate) fn from_transport(source: TransportBudget) -> Result<Self, String> {
        let items = source.max_backend_queued_operations();
        let bytes = source.max_backend_queued_bytes();
        let target_items = source.max_query_backend_queued_operations();
        let batch_items = source.max_batch_items();
        let batch_bytes = source.max_batch_encoded_bytes();
        let control_bytes = batch_bytes
            .checked_mul(2)
            .ok_or("native task transport retained-byte windows overflowed")?;
        let target_bytes = source.max_query_backend_queued_bytes().max(control_bytes);
        if batch_items
            .checked_mul(2)
            .ok_or("native task transport item windows overflowed")?
            > items
            || control_bytes
                .checked_mul(2)
                .ok_or("native task transport retained-byte windows overflowed")?
                > bytes
        {
            return Err("native task transport process bounds must fit one maximum ordinary batch and one maximum control batch".into());
        }
        let process = [
            items,
            bytes,
            items - batch_items,
            bytes - control_bytes,
            bytes - control_bytes - batch_bytes,
            control_bytes - batch_bytes,
        ];
        let target = [
            target_items.saturating_add(batch_items),
            target_bytes.saturating_add(control_bytes),
            target_items,
            target_bytes,
            target_bytes - batch_bytes,
        ];
        // TransportBudget rejects zero MB/CI and enforces 2*CB <= B and
        // 2*CI <= I. TB >= CB=2*MB. Hence every derived limit is positive;
        // ResourceVector must never erase a configured zero-limit dimension.
        assert!(process.into_iter().chain(target).all(|limit| limit > 0));
        let process_limits = limits(PROCESS_DIMENSIONS, process)?;
        let target_limits = limits(TARGET_DIMENSIONS, target)?;
        Ok(Self {
            source,
            facts: TransportCapacityFacts {
                process_items: items,
                process_retained_bytes: bytes,
                target_items,
                target_retained_bytes: target_bytes,
                control_reserved_items: batch_items,
                control_reserved_bytes: control_bytes,
                maximum_batch_encoded_bytes: batch_bytes,
            },
            process_limits,
            target_limits,
        })
    }
    pub(crate) fn accepts_transport_budget(&self, source: TransportBudget) -> bool {
        self.source == source
    }
    pub(crate) fn capacity_sources(&self) -> TransportCapacityFacts {
        self.facts
    }
    pub(crate) fn process_limits(&self) -> ResourceVector {
        self.process_limits.clone()
    }
    pub(crate) fn keyed_families(&self) -> [(FamilyId, ResourceVector); 1] {
        [(TARGET_FAMILY, self.target_limits.clone())]
    }
    pub(crate) fn backend_key(backend: BackendProcessId) -> CapacityKey {
        CapacityKey(u128::from_be_bytes(backend.to_bytes()))
    }
    fn keyed(backend: BackendProcessId, quota: ResourceVector) -> KeyedQuota {
        KeyedQuota {
            family: TARGET_FAMILY,
            key: Self::backend_key(backend),
            quota,
        }
    }
    pub(crate) fn queue_resources(
        &self,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        items: usize,
        queued_bytes: usize,
    ) -> Result<ReservationResources, TransportCapacityError> {
        if items == 0 || queued_bytes == 0 {
            return Err(TransportCapacityError::InvalidQueue);
        }
        let i = amount(items)?;
        let b = amount(queued_bytes)?;
        let ordinary = lane == TransportCapacityLane::Ordinary;
        let process = ResourceVector::new([
            (TOTAL_ITEMS, i),
            (TOTAL_BYTES, b),
            (ORDINARY_ITEMS, if ordinary { i } else { 0 }),
            (ORDINARY_BYTES, if ordinary { b } else { 0 }),
            (ORDINARY_QUEUE_BYTES, if ordinary { b } else { 0 }),
            (CONTROL_QUEUE_BYTES, if ordinary { 0 } else { b }),
        ])?;
        let target = ResourceVector::new([
            (TARGET_ITEMS, i),
            (TARGET_BYTES, b),
            (TARGET_ORDINARY_ITEMS, if ordinary { i } else { 0 }),
            (TARGET_ORDINARY_BYTES, if ordinary { b } else { 0 }),
            (TARGET_ORDINARY_QUEUE_BYTES, if ordinary { b } else { 0 }),
        ])?;
        Ok(ReservationResources::for_work(process).with_keyed(Self::keyed(backend, target)))
    }
    pub(crate) fn encoding_resources(
        &self,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        bytes: usize,
    ) -> Result<ReservationResources, TransportCapacityError> {
        if bytes == 0 || bytes > self.source.max_batch_encoded_bytes() {
            return Err(TransportCapacityError::InvalidEncodingBytes);
        }
        self.byte_resources(backend, lane, bytes, true)
    }
    fn byte_resources(
        &self,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        bytes: usize,
        work: bool,
    ) -> Result<ReservationResources, TransportCapacityError> {
        let bytes = amount(bytes)?;
        let ordinary = lane == TransportCapacityLane::Ordinary;
        let process = ResourceVector::new([
            (TOTAL_BYTES, bytes),
            (ORDINARY_BYTES, if ordinary { bytes } else { 0 }),
        ])?;
        let target = ResourceVector::new([
            (TARGET_BYTES, bytes),
            (TARGET_ORDINARY_BYTES, if ordinary { bytes } else { 0 }),
        ])?;
        Ok(ReservationResources::new(
            process,
            if work {
                novarocks_control_runtime_core::owned_storage::OwnedSlotNeed::work_only()
            } else {
                Default::default()
            },
        )
        .with_keyed(Self::keyed(backend, target)))
    }
    pub(crate) fn in_flight_release(
        &self,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        queued_bytes: usize,
    ) -> Result<ReservationResources, TransportCapacityError> {
        let bytes = amount(queued_bytes)?;
        let ordinary = lane == TransportCapacityLane::Ordinary;
        let process = ResourceVector::new([(
            if ordinary {
                ORDINARY_QUEUE_BYTES
            } else {
                CONTROL_QUEUE_BYTES
            },
            bytes,
        )])?;
        let target = ResourceVector::new([(
            TARGET_ORDINARY_QUEUE_BYTES,
            if ordinary { bytes } else { 0 },
        )])?;
        Ok(ReservationResources::new(process, Default::default())
            .with_keyed(Self::keyed(backend, target)))
    }
    pub(crate) fn encoding_shrink_release(
        &self,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        reserved: usize,
        actual: usize,
    ) -> Result<ReservationResources, TransportCapacityError> {
        if reserved == 0 || reserved > self.source.max_batch_encoded_bytes() {
            return Err(TransportCapacityError::InvalidEncodingBytes);
        }
        self.byte_resources(
            backend,
            lane,
            reserved
                .checked_sub(actual)
                .ok_or(TransportCapacityError::InvalidShrink)?,
            false,
        )
    }
    pub(crate) fn classify_denial(
        denial: CapacityDenial,
    ) -> Result<TransportCapacityRefusal, TransportCapacityError> {
        match denial.keyed {
            None if PROCESS_DIMENSIONS.contains(&denial.dimension) => {
                Ok(TransportCapacityRefusal::ProcessFull)
            }
            Some((family, key))
                if family == TARGET_FAMILY && TARGET_DIMENSIONS.contains(&denial.dimension) =>
            {
                BackendProcessId::try_from_bytes(key.0.to_be_bytes())
                    .map(TransportCapacityRefusal::TargetFull)
                    .map_err(|_| TransportCapacityError::InvalidBackendKey)
            }
            _ => Err(TransportCapacityError::ForeignDimension),
        }
    }
    /// Samples process and keys separately; this is diagnostic interval data,
    /// never an atomic exit or admission proof. No owner index is required.
    pub(crate) fn snapshot(
        &self,
        ledger: &RuntimeLedger,
    ) -> Result<Option<TransportCapacitySnapshot>, CapacityObservationError> {
        let Some(process) = ledger.try_capacity_dimensions(PROCESS_DIMENSIONS)? else {
            return Ok(None);
        };
        let Some(keyed) = ledger.try_keyed_usage_snapshot() else {
            return Ok(None);
        };
        let targets = keyed
            .into_iter()
            .filter(|entry| entry.family == TARGET_FAMILY)
            .map(|entry| {
                let backend = BackendProcessId::try_from_bytes(entry.key.0.to_be_bytes())
                    .expect("transport keys come from typed backend process identities");
                TransportTargetUsage {
                    backend,
                    used: TARGET_DIMENSIONS.map(|dimension| entry.used.get(dimension)),
                    limits: TARGET_DIMENSIONS.map(|dimension| entry.limits.get(dimension)),
                }
            })
            .collect();
        Ok(Some(TransportCapacitySnapshot {
            used: process.used,
            limits: process.limits,
            targets,
        }))
    }
}
fn amount(value: usize) -> Result<u64, TransportCapacityError> {
    u64::try_from(value).map_err(|_| TransportCapacityError::Arithmetic)
}
fn limits<const N: usize>(
    dimensions: [DimensionId; N],
    values: [usize; N],
) -> Result<ResourceVector, String> {
    ResourceVector::new(
        dimensions
            .into_iter()
            .zip(values)
            .map(|(dimension, value)| {
                Ok((
                    dimension,
                    u64::try_from(value).map_err(|_| "native task transport window exceeds u64")?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
    )
    .map_err(|_| "native task transport dimensions are duplicated".into())
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportCapacitySnapshot {
    pub(crate) used: [u64; 6],
    pub(crate) limits: [u64; 6],
    pub(crate) targets: Vec<TransportTargetUsage>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportTargetUsage {
    pub(crate) backend: BackendProcessId,
    pub(crate) used: [u64; 5],
    pub(crate) limits: [u64; 5],
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::{
        obligation::{ReservationObligation, WorkObligation},
        transaction::{RuntimeError, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        identity::Generation,
        scope::{WorkAdmission, WorkKind},
    };
    use std::time::Duration;
    fn generation() -> Generation {
        Generation::new(1).unwrap()
    }
    fn backend(id: u8) -> BackendProcessId {
        let mut bytes = [0; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = id;
        BackendProcessId::try_from_bytes(bytes).unwrap()
    }
    fn transport(
        items: usize,
        bytes: usize,
        target_items: usize,
        target_bytes: usize,
        batch_items: usize,
    ) -> TransportBudget {
        TransportBudget::new(
            batch_items,
            10,
            5,
            target_items,
            target_bytes,
            items,
            bytes,
            1,
            2,
            Duration::from_secs(1),
        )
        .unwrap()
    }
    fn layout() -> NativeTransportCapacity {
        NativeTransportCapacity::from_transport(transport(4, 100, 4, 100, 1)).unwrap()
    }
    fn ledger(layout: &NativeTransportCapacity) -> RuntimeLedger {
        RuntimeLedger::with_keyed_families(layout.process_limits(), layout.keyed_families())
            .unwrap()
    }
    struct Held {
        work: WorkObligation,
        quota: ReservationObligation,
    }
    fn queue(
        layout: &NativeTransportCapacity,
        scope: &ScopeHandle,
        target: BackendProcessId,
        lane: TransportCapacityLane,
        items: usize,
        bytes: usize,
    ) -> Result<Held, RuntimeError> {
        let (work, quota) = scope
            .reserve_next(
                generation(),
                layout.queue_resources(target, lane, items, bytes).unwrap(),
                WorkKind::Business,
                WorkAdmission::External,
            )?
            .bind(WorkKind::Business)?;
        Ok(Held { work, quota })
    }
    fn encoding(
        layout: &NativeTransportCapacity,
        scope: &ScopeHandle,
        parent: &Held,
        target: BackendProcessId,
        lane: TransportCapacityLane,
        bytes: usize,
    ) -> Result<Held, RuntimeError> {
        let (work, quota) = scope
            .reserve_child_next(
                generation(),
                parent.quota.id(),
                layout.encoding_resources(target, lane, bytes).unwrap(),
                WorkKind::Business,
            )?
            .bind(WorkKind::Business)?;
        Ok(Held { work, quota })
    }
    fn partial(held: &mut Held, release: ReservationResources) {
        held.quota
            .release_quota_with_keyed(&release.quota, release.keyed.as_ref())
            .unwrap();
    }
    fn release(ledger: &RuntimeLedger, mut held: Held) {
        held.work.body_exited().unwrap();
        held.work.captures_exited().unwrap();
        let resources = held.quota.resources().unwrap();
        partial(&mut held, resources);
        held.quota.finish().unwrap();
        ledger.reap_ready(16).unwrap();
    }
    fn refused(error: RuntimeError) -> TransportCapacityRefusal {
        let RuntimeError::CapacityDenied(denial) = error else {
            panic!("expected actual capacity denial: {error:?}")
        };
        NativeTransportCapacity::classify_denial(denial).unwrap()
    }
    fn sample(
        layout: &NativeTransportCapacity,
        ledger: &RuntimeLedger,
    ) -> TransportCapacitySnapshot {
        layout.snapshot(ledger).unwrap().unwrap()
    }
    const ORDINARY: TransportCapacityLane = TransportCapacityLane::Ordinary;
    const CONTROL: TransportCapacityLane = TransportCapacityLane::Control;

    #[test]
    fn dimension_limits_reproduce_original_configuration_and_saturating_target_bounds() {
        let layout = layout();
        assert_eq!(
            layout
                .process_limits()
                .iter()
                .map(|(_, limit)| limit)
                .collect::<Vec<_>>(),
            [4, 100, 3, 80, 70, 10]
        );
        assert_eq!(
            layout
                .target_limits
                .iter()
                .map(|(_, limit)| limit)
                .collect::<Vec<_>>(),
            [5, 120, 4, 100, 90]
        );
        assert!(layout.accepts_transport_budget(transport(4, 100, 4, 100, 1)));
        assert_eq!(layout.capacity_sources().control_reserved_bytes, 20);
        let huge = NativeTransportCapacity::from_transport(transport(
            usize::MAX,
            usize::MAX,
            usize::MAX,
            usize::MAX,
            1,
        ))
        .unwrap();
        assert_eq!(
            huge.target_limits.get(TARGET_ITEMS),
            u64::try_from(usize::MAX).unwrap()
        );
        assert_eq!(
            huge.target_limits.get(TARGET_BYTES),
            u64::try_from(usize::MAX).unwrap()
        );
    }
    #[test]
    fn ordinary_saturation_leaves_exact_control_item_and_byte_reserve() {
        let layout = layout();
        let ledger = ledger(&layout);
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let target = backend(1);
        let mut ordinary = queue(&layout, &scope, target, ORDINARY, 3, 70).unwrap();
        let ordinary_encoding = encoding(&layout, &scope, &ordinary, target, ORDINARY, 10).unwrap();
        partial(
            &mut ordinary,
            layout.in_flight_release(target, ORDINARY, 70).unwrap(),
        );
        assert_eq!(
            refused(
                queue(&layout, &scope, target, ORDINARY, 1, 1)
                    .err()
                    .unwrap()
            ),
            TransportCapacityRefusal::ProcessFull
        );
        let mut control = queue(&layout, &scope, target, CONTROL, 1, 10).unwrap();
        let control_encoding = encoding(&layout, &scope, &control, target, CONTROL, 10).unwrap();
        partial(
            &mut control,
            layout.in_flight_release(target, CONTROL, 10).unwrap(),
        );
        assert_eq!(sample(&layout, &ledger).used, [4, 100, 3, 80, 0, 0]);
        release(&ledger, control_encoding);
        release(&ledger, control);
        release(&ledger, ordinary_encoding);
        release(&ledger, ordinary);
        assert!(sample(&layout, &ledger).targets.is_empty());
    }
    #[test]
    fn process_shortfall_wins_even_when_target_is_also_full() {
        let layout = NativeTransportCapacity::from_transport(transport(4, 100, 1, 40, 1)).unwrap();
        let ledger = ledger(&layout);
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let slow = backend(1);
        let healthy = backend(2);
        let first = queue(&layout, &scope, slow, ORDINARY, 1, 1).unwrap();
        assert_eq!(
            refused(queue(&layout, &scope, slow, ORDINARY, 1, 1).err().unwrap()),
            TransportCapacityRefusal::TargetFull(slow)
        );
        let other = queue(&layout, &scope, healthy, ORDINARY, 1, 1).unwrap();
        let another = queue(&layout, &scope, backend(3), ORDINARY, 1, 1).unwrap();
        assert_eq!(
            refused(queue(&layout, &scope, slow, ORDINARY, 1, 1).err().unwrap()),
            TransportCapacityRefusal::ProcessFull
        );
        release(&ledger, another);
        release(&ledger, other);
        release(&ledger, first);
    }
    #[test]
    fn queue_window_keeps_one_encoded_batch_available_in_each_lane() {
        let layout = layout();
        let ledger = ledger(&layout);
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let target = backend(1);
        let mut queued = queue(&layout, &scope, target, ORDINARY, 1, 70).unwrap();
        assert_eq!(
            refused(
                queue(&layout, &scope, target, ORDINARY, 1, 1)
                    .err()
                    .unwrap()
            ),
            TransportCapacityRefusal::ProcessFull
        );
        let mut encoded = encoding(&layout, &scope, &queued, target, ORDINARY, 10).unwrap();
        let before = sample(&layout, &ledger);
        partial(
            &mut queued,
            layout.in_flight_release(target, ORDINARY, 70).unwrap(),
        );
        let after = sample(&layout, &ledger);
        assert_eq!(before.used[0..4], after.used[0..4]);
        assert_eq!(after.used[4], 0);
        assert_eq!(after.targets[0].used[4], 0);
        partial(
            &mut encoded,
            layout
                .encoding_shrink_release(target, ORDINARY, 10, 5)
                .unwrap(),
        );
        let extra = queue(&layout, &scope, target, ORDINARY, 1, 5).unwrap();
        release(&ledger, extra);
        release(&ledger, encoded);
        release(&ledger, queued);
        let control = queue(&layout, &scope, target, CONTROL, 1, 10).unwrap();
        assert_eq!(
            refused(queue(&layout, &scope, target, CONTROL, 1, 1).err().unwrap()),
            TransportCapacityRefusal::ProcessFull
        );
        let control_encoding = encoding(&layout, &scope, &control, target, CONTROL, 10).unwrap();
        release(&ledger, control_encoding);
        release(&ledger, control);
    }
    #[test]
    fn one_slow_target_across_attempts_preserves_healthy_and_control_capacity() {
        let layout = NativeTransportCapacity::from_transport(transport(8, 100, 4, 40, 2)).unwrap();
        let ledger = ledger(&layout);
        let first_scope = ledger.open_scope_next(None, generation()).unwrap();
        let second_scope = ledger.open_scope_next(None, generation()).unwrap();
        let slow = backend(1);
        let healthy = backend(2);
        let mut first = queue(&layout, &first_scope, slow, ORDINARY, 1, 20).unwrap();
        let first_encoding = encoding(&layout, &first_scope, &first, slow, ORDINARY, 10).unwrap();
        partial(
            &mut first,
            layout.in_flight_release(slow, ORDINARY, 20).unwrap(),
        );
        let second = queue(&layout, &second_scope, slow, ORDINARY, 1, 10).unwrap();
        assert_eq!(
            refused(
                queue(&layout, &second_scope, slow, ORDINARY, 1, 1)
                    .err()
                    .unwrap()
            ),
            TransportCapacityRefusal::TargetFull(slow)
        );
        let mut normal = queue(&layout, &second_scope, healthy, ORDINARY, 1, 20).unwrap();
        let normal_encoding =
            encoding(&layout, &second_scope, &normal, healthy, ORDINARY, 10).unwrap();
        partial(
            &mut normal,
            layout.in_flight_release(healthy, ORDINARY, 20).unwrap(),
        );
        let mut control = queue(&layout, &first_scope, slow, CONTROL, 1, 5).unwrap();
        let control_encoding = encoding(&layout, &first_scope, &control, slow, CONTROL, 5).unwrap();
        partial(
            &mut control,
            layout.in_flight_release(slow, CONTROL, 5).unwrap(),
        );
        assert_eq!(sample(&layout, &ledger).used[1], 80);
        release(&ledger, control_encoding);
        release(&ledger, control);
        release(&ledger, normal_encoding);
        release(&ledger, normal);
        release(&ledger, second);
        release(&ledger, first_encoding);
        release(&ledger, first);
        assert!(sample(&layout, &ledger).targets.is_empty());
    }
    #[test]
    fn encoding_checks_target_bytes_and_invalid_quotes_remain_permanent_errors() {
        let layout = NativeTransportCapacity::from_transport(transport(16, 200, 8, 40, 2)).unwrap();
        let ledger = ledger(&layout);
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let target = backend(1);
        let mut queued = queue(&layout, &scope, target, ORDINARY, 1, 30).unwrap();
        let encoded = encoding(&layout, &scope, &queued, target, ORDINARY, 10).unwrap();
        partial(
            &mut queued,
            layout.in_flight_release(target, ORDINARY, 30).unwrap(),
        );
        assert_eq!(
            refused(
                encoding(&layout, &scope, &queued, target, ORDINARY, 1)
                    .err()
                    .unwrap()
            ),
            TransportCapacityRefusal::TargetFull(target)
        );
        for invalid in [0, 11] {
            assert_eq!(
                layout.encoding_resources(target, ORDINARY, invalid),
                Err(TransportCapacityError::InvalidEncodingBytes)
            );
        }
        assert_eq!(
            layout.encoding_shrink_release(target, ORDINARY, 5, 6),
            Err(TransportCapacityError::InvalidShrink)
        );
        assert_eq!(
            layout.queue_resources(target, ORDINARY, 0, 1),
            Err(TransportCapacityError::InvalidQueue)
        );
        let control = queue(&layout, &scope, target, CONTROL, 1, 10).unwrap();
        let control_encoding = encoding(&layout, &scope, &control, target, CONTROL, 10).unwrap();
        assert_eq!(sample(&layout, &ledger).targets[0].used[1], 60);
        assert_eq!(
            refused(
                encoding(&layout, &scope, &control, target, CONTROL, 1)
                    .err()
                    .unwrap()
            ),
            TransportCapacityRefusal::TargetFull(target)
        );
        release(&ledger, control_encoding);
        release(&ledger, control);
        release(&ledger, encoded);
        release(&ledger, queued);
    }
    #[test]
    fn backend_keys_round_trip_and_foreign_denials_never_become_full() {
        let target = backend(3);
        assert_eq!(
            NativeTransportCapacity::backend_key(target).0.to_be_bytes(),
            target.to_bytes()
        );
        let denial = CapacityDenial {
            dimension: TARGET_BYTES,
            keyed: Some((TARGET_FAMILY, NativeTransportCapacity::backend_key(target))),
            requested: 1,
            committed: 1,
            limit: 1,
        };
        assert_eq!(
            NativeTransportCapacity::classify_denial(denial),
            Ok(TransportCapacityRefusal::TargetFull(target))
        );
        assert_eq!(
            NativeTransportCapacity::classify_denial(CapacityDenial {
                keyed: Some((FamilyId::new(2).unwrap(), CapacityKey(0))),
                ..denial
            }),
            Err(TransportCapacityError::ForeignDimension)
        );
        assert_eq!(
            NativeTransportCapacity::classify_denial(CapacityDenial {
                keyed: Some((TARGET_FAMILY, CapacityKey(0))),
                ..denial
            }),
            Err(TransportCapacityError::InvalidBackendKey)
        );
        assert_eq!(
            NativeTransportCapacity::classify_denial(CapacityDenial {
                dimension: DimensionId::new(99).unwrap(),
                keyed: None,
                ..denial
            }),
            Err(TransportCapacityError::ForeignDimension)
        );
    }
}
