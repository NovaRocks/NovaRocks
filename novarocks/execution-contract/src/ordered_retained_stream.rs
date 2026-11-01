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

//! Pure ordered retention semantics shared by carrier owners.
//!
//! This kernel retains only bounded item metadata. Payloads, allocation
//! leases, exact identities, delivery receipts and transport lifetimes remain
//! with the composing owner. Retiring an item is not physical reclamation.

use std::collections::VecDeque;
use std::num::NonZeroUsize;

pub mod exchange;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedStreamWindow {
    data_positions: NonZeroUsize,
    payload_bytes: NonZeroUsize,
}

impl RetainedStreamWindow {
    pub fn try_new(data_positions: usize, payload_bytes: usize) -> Result<Self, StreamError> {
        Ok(Self {
            data_positions: NonZeroUsize::new(data_positions).ok_or(StreamError::InvalidWindow)?,
            payload_bytes: NonZeroUsize::new(payload_bytes).ok_or(StreamError::InvalidWindow)?,
        })
    }
    pub const fn data_positions(self) -> usize {
        self.data_positions.get()
    }
    pub const fn payload_bytes(self) -> usize {
        self.payload_bytes.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetainedItemKind {
    Data {
        payload_bytes: usize,
        end_after_data: bool,
    },
    End,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedStreamItem {
    pub sequence: u64,
    pub kind: RetainedItemKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedStreamRange {
    pub data_sequence: Option<u64>,
    pub end_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferOutcome {
    Ready {
        item: RetainedStreamItem,
        end: Option<RetainedStreamItem>,
        replay: bool,
    },
    NotReady {
        wanted: u64,
        accepted_consumed: u64,
    },
    Retired {
        wanted: u64,
        accepted_consumed: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcknowledgementTransition {
    Applied,
    Idempotent,
    Older,
}

/// Logical metadata retirement only. The payload owner must retain its lease
/// until its last real payload/job/transport alias exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamRetirement {
    pub transition: AcknowledgementTransition,
    pub accepted_consumed: u64,
    pub retired_data_positions: usize,
    pub retired_payload_bytes: usize,
    pub retired_end: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedStreamSnapshot {
    pub produced_through: u64,
    pub offered_through: u64,
    pub consumed_through: u64,
    pub data_positions: usize,
    pub payload_bytes: usize,
    pub end_sequence: Option<u64>,
    pub sealed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamError {
    InvalidWindow,
    MetadataArithmetic,
    MetadataBacking(novarocks_failure_contract::SharedFailureCause),
    PayloadTooLarge,
    WindowFull,
    SequenceOverflow,
    ProducerEnded,
    Sealed,
    WantedZero,
    OfferGap,
    AcknowledgementBeyondOffered,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidWindow => "ordered stream window limits must be positive",
            Self::MetadataArithmetic => "ordered stream metadata position arithmetic overflowed",
            Self::MetadataBacking(cause) => return cause.fmt(f),
            Self::PayloadTooLarge => "ordered stream item exceeds the payload window",
            Self::WindowFull => "ordered stream data window is full",
            Self::SequenceOverflow => "ordered stream sequence would overflow",
            Self::ProducerEnded => "ordered stream producer already published End",
            Self::Sealed => "ordered stream reads and publication are sealed",
            Self::WantedZero => "ordered stream wanted sequence must be positive",
            Self::OfferGap => "ordered stream offer would cross an unoffered gap",
            Self::AcknowledgementBeyondOffered => {
                "ordered stream acknowledgement exceeds the offered prefix"
            }
        })
    }
}
impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MetadataBacking(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for StreamError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::MetadataBacking(cause) => return cause.failure_fact(),
            Self::InvalidWindow => (FailureClass::Invariant, 200),
            Self::MetadataArithmetic => (FailureClass::Resource, 201),
            Self::PayloadTooLarge => (FailureClass::Resource, 202),
            // Full is an owner control signal. Projection does not authorize
            // its promotion into a Task failure, cancellation or retry.
            Self::WindowFull => (FailureClass::Control, 203),
            Self::SequenceOverflow => (FailureClass::Representation, 204),
            Self::ProducerEnded => (FailureClass::Control, 205),
            Self::Sealed => (FailureClass::Control, 206),
            Self::WantedZero => (FailureClass::Data, 207),
            Self::OfferGap => (FailureClass::Data, 208),
            Self::AcknowledgementBeyondOffered => (FailureClass::Data, 209),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self, Self::SequenceOverflow) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::RESULT,
                    code: std::num::NonZeroU16::new(1002).unwrap(),
                },
                required: None,
                extent_limit: Some(u64::MAX),
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
#[derive(Debug)]
struct StreamMetadataBackingFailure {
    requested: usize,
    source: std::collections::TryReserveError,
}
impl std::fmt::Display for StreamMetadataBackingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ordered stream cannot reserve {} metadata entries: {}",
            self.requested, self.source
        )
    }
}
impl std::error::Error for StreamMetadataBackingFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for StreamMetadataBackingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(210).unwrap()),
        )
        .with_resource(ResourceFacts {
            dimension: ResourceDimension::Items,
            request_scope: Some(ResourceScope::Request),
            constraint_scope: None,
            requested: u64::try_from(self.requested).ok(),
            required_growth: None,
            limit: None,
            committed: None,
            live: None,
        })
    }
}

pub struct OrderedRetainedStream {
    window: RetainedStreamWindow,
    items: VecDeque<RetainedStreamItem>,
    next_sequence: u64,
    produced_through: u64,
    offered_through: u64,
    consumed_through: u64,
    data_positions: usize,
    payload_bytes: usize,
    end_sequence: Option<u64>,
    sealed: bool,
}

impl OrderedRetainedStream {
    /// Actual allocation retained by this pure metadata queue, including
    /// spare capacity. Payloads and composing owner objects are excluded.
    pub fn metadata_backing_bytes(&self) -> usize {
        self.items.capacity() * std::mem::size_of::<RetainedStreamItem>()
    }
    pub fn try_new(window: RetainedStreamWindow) -> Result<Self, StreamError> {
        let capacity = window
            .data_positions()
            .checked_add(1)
            .ok_or(StreamError::MetadataArithmetic)?;
        let mut items = VecDeque::new();
        items.try_reserve_exact(capacity).map_err(|source| {
            StreamError::MetadataBacking(novarocks_failure_contract::SharedFailureCause::new(
                StreamMetadataBackingFailure {
                    requested: capacity,
                    source,
                },
            ))
        })?;
        Ok(Self {
            window,
            items,
            next_sequence: 1,
            produced_through: 0,
            offered_through: 0,
            consumed_through: 0,
            data_positions: 0,
            payload_bytes: 0,
            end_sequence: None,
            sealed: false,
        })
    }

    /// Publishes immutable Data and optionally its adjacent End in one cut.
    /// An End flag can never be added to a previously published Data item.
    pub fn publish_data(
        &mut self,
        payload_bytes: usize,
        end_after_data: bool,
    ) -> Result<PublishedStreamRange, StreamError> {
        self.check_publication()?;
        if payload_bytes > self.window.payload_bytes() {
            return Err(StreamError::PayloadTooLarge);
        }
        let total = self
            .payload_bytes
            .checked_add(payload_bytes)
            .ok_or(StreamError::PayloadTooLarge)?;
        if self.data_positions == self.window.data_positions()
            || total > self.window.payload_bytes()
        {
            return Err(StreamError::WindowFull);
        }
        // Even nonterminal Data must leave a representable sequence for End.
        let following = self
            .next_sequence
            .checked_add(1)
            .ok_or(StreamError::SequenceOverflow)?;
        let sequence = self.next_sequence;
        self.items.push_back(RetainedStreamItem {
            sequence,
            kind: RetainedItemKind::Data {
                payload_bytes,
                end_after_data,
            },
        });
        self.data_positions += 1;
        self.payload_bytes = total;
        self.next_sequence = following;
        self.produced_through = sequence;
        let end_sequence = if end_after_data {
            self.items.push_back(RetainedStreamItem {
                sequence: following,
                kind: RetainedItemKind::End,
            });
            self.end_sequence = Some(following);
            self.produced_through = following;
            Some(following)
        } else {
            None
        };
        Ok(PublishedStreamRange {
            data_sequence: Some(sequence),
            end_sequence,
        })
    }

    /// The reserved terminal position remains available even with full Data.
    pub fn publish_end(&mut self) -> Result<PublishedStreamRange, StreamError> {
        self.check_publication()?;
        let sequence = self.next_sequence;
        self.items.push_back(RetainedStreamItem {
            sequence,
            kind: RetainedItemKind::End,
        });
        self.end_sequence = Some(sequence);
        self.produced_through = sequence;
        Ok(PublishedStreamRange {
            data_sequence: None,
            end_sequence: Some(sequence),
        })
    }

    pub fn offer(&mut self, wanted: u64) -> Result<OfferOutcome, StreamError> {
        if self.sealed {
            return Err(StreamError::Sealed);
        }
        if wanted == 0 {
            return Err(StreamError::WantedZero);
        }
        if wanted <= self.consumed_through {
            return Ok(OfferOutcome::Retired {
                wanted,
                accepted_consumed: self.consumed_through,
            });
        }
        if wanted > self.offered_through.saturating_add(1) {
            return Err(StreamError::OfferGap);
        }
        let Some(item) = self
            .items
            .iter()
            .find(|item| item.sequence == wanted)
            .copied()
        else {
            return Ok(OfferOutcome::NotReady {
                wanted,
                accepted_consumed: self.consumed_through,
            });
        };
        let replay = wanted <= self.offered_through;
        let end = if matches!(
            item.kind,
            RetainedItemKind::Data {
                end_after_data: true,
                ..
            }
        ) {
            Some(RetainedStreamItem {
                sequence: wanted.checked_add(1).ok_or(StreamError::SequenceOverflow)?,
                kind: RetainedItemKind::End,
            })
        } else {
            None
        };
        self.offered_through = self
            .offered_through
            .max(end.map_or(wanted, |end| end.sequence));
        Ok(OfferOutcome::Ready { item, end, replay })
    }

    /// Apply a legal cumulative ACK independently before looking up wanted.
    /// A later NotReady lookup must not roll this transition back.
    pub fn acknowledge(&mut self, consumed: u64) -> Result<StreamRetirement, StreamError> {
        if self.sealed {
            return Err(StreamError::Sealed);
        }
        if consumed > self.offered_through {
            return Err(StreamError::AcknowledgementBeyondOffered);
        }
        let mut retirement = StreamRetirement {
            transition: AcknowledgementTransition::Idempotent,
            accepted_consumed: self.consumed_through,
            retired_data_positions: 0,
            retired_payload_bytes: 0,
            retired_end: false,
        };
        if consumed < self.consumed_through {
            retirement.transition = AcknowledgementTransition::Older;
            return Ok(retirement);
        }
        if consumed == self.consumed_through {
            return Ok(retirement);
        }
        while self
            .items
            .front()
            .is_some_and(|item| item.sequence <= consumed)
        {
            let item = self.items.pop_front().expect("checked retained item");
            match item.kind {
                RetainedItemKind::Data { payload_bytes, .. } => {
                    self.data_positions -= 1;
                    self.payload_bytes -= payload_bytes;
                    retirement.retired_data_positions += 1;
                    retirement.retired_payload_bytes += payload_bytes;
                }
                RetainedItemKind::End => retirement.retired_end = true,
            }
        }
        self.consumed_through = consumed;
        retirement.transition = AcknowledgementTransition::Applied;
        retirement.accepted_consumed = consumed;
        Ok(retirement)
    }

    /// Seals visibility without forging ACKs or claiming any physical release.
    /// The composing owner separately closes payload/job/transport holders.
    pub fn seal(&mut self) {
        self.sealed = true;
        self.items.clear();
        self.data_positions = 0;
        self.payload_bytes = 0;
    }

    pub fn snapshot(&self) -> RetainedStreamSnapshot {
        RetainedStreamSnapshot {
            produced_through: self.produced_through,
            offered_through: self.offered_through,
            consumed_through: self.consumed_through,
            data_positions: self.data_positions,
            payload_bytes: self.payload_bytes,
            end_sequence: self.end_sequence,
            sealed: self.sealed,
        }
    }

    fn check_publication(&self) -> Result<(), StreamError> {
        if self.sealed {
            Err(StreamError::Sealed)
        } else if self.end_sequence.is_some() {
            Err(StreamError::ProducerEnded)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn stream() -> OrderedRetainedStream {
        OrderedRetainedStream::try_new(RetainedStreamWindow::try_new(2, 10).unwrap()).unwrap()
    }

    #[test]
    fn count_and_bytes_bound_data_but_leave_a_terminal_position() {
        let mut s = stream();
        s.publish_data(6, false).unwrap();
        assert_eq!(s.publish_data(5, false), Err(StreamError::WindowFull));
        s.publish_data(4, false).unwrap();
        assert_eq!(s.publish_data(0, false), Err(StreamError::WindowFull));
        assert_eq!(s.publish_end().unwrap().end_sequence, Some(3));
        assert_eq!(s.publish_end(), Err(StreamError::ProducerEnded));
        assert_eq!(s.snapshot().payload_bytes, 10);
    }

    #[test]
    fn data_and_end_offer_is_atomic_and_replay_is_immutable() {
        let mut s = stream();
        s.publish_data(7, true).unwrap();
        let first = s.offer(1).unwrap();
        assert!(matches!(
            first,
            OfferOutcome::Ready {
                end: Some(RetainedStreamItem { sequence: 2, .. }),
                replay: false,
                ..
            }
        ));
        assert_eq!(s.snapshot().offered_through, 2);
        assert!(matches!(
            s.offer(1).unwrap(),
            OfferOutcome::Ready { replay: true, .. }
        ));
        let retirement = s.acknowledge(2).unwrap();
        assert_eq!(retirement.retired_payload_bytes, 7);
        assert!(retirement.retired_end);
        assert_eq!(s.snapshot().payload_bytes, 0);
        assert!(matches!(
            s.offer(1).unwrap(),
            OfferOutcome::Retired {
                accepted_consumed: 2,
                ..
            }
        ));
    }

    #[test]
    fn ack_cannot_cross_unoffered_data_and_remains_applied_on_not_ready() {
        let mut s = stream();
        s.publish_data(5, false).unwrap();
        s.publish_data(5, false).unwrap();
        assert_eq!(s.offer(2), Err(StreamError::OfferGap));
        assert_eq!(
            s.acknowledge(1),
            Err(StreamError::AcknowledgementBeyondOffered)
        );
        s.offer(1).unwrap();
        assert_eq!(
            s.acknowledge(2),
            Err(StreamError::AcknowledgementBeyondOffered)
        );
        s.acknowledge(1).unwrap();
        s.offer(2).unwrap();
        s.acknowledge(2).unwrap();
        assert_eq!(
            s.offer(3).unwrap(),
            OfferOutcome::NotReady {
                wanted: 3,
                accepted_consumed: 2
            }
        );
        assert_eq!(
            s.acknowledge(1).unwrap().transition,
            AcknowledgementTransition::Older
        );
    }

    #[test]
    fn late_end_never_modifies_a_published_data_flag() {
        let mut s = stream();
        s.publish_data(1, false).unwrap();
        let first = s.offer(1).unwrap();
        s.publish_end().unwrap();
        let replay = s.offer(1).unwrap();
        assert!(matches!(first, OfferOutcome::Ready { end: None, .. }));
        assert!(matches!(replay, OfferOutcome::Ready { end: None, .. }));
        assert_eq!(s.snapshot().offered_through, 1);
        assert!(matches!(
            s.offer(2).unwrap(),
            OfferOutcome::Ready {
                item: RetainedStreamItem {
                    kind: RetainedItemKind::End,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn empty_terminal_and_seal_do_not_forge_consumption() {
        let mut s = stream();
        s.publish_end().unwrap();
        s.offer(1).unwrap();
        s.seal();
        assert_eq!(s.snapshot().consumed_through, 0);
        assert_eq!(s.offer(1), Err(StreamError::Sealed));
        assert_eq!(s.acknowledge(1), Err(StreamError::Sealed));
        assert_eq!(s.publish_data(1, false), Err(StreamError::Sealed));
    }

    #[test]
    fn zero_payload_still_uses_a_data_position_and_sequence_never_wraps() {
        let mut s = stream();
        s.publish_data(0, false).unwrap();
        assert_eq!(s.snapshot().data_positions, 1);
        assert_eq!(s.offer(0), Err(StreamError::WantedZero));
        let mut s = stream();
        s.next_sequence = u64::MAX;
        let before = s.snapshot();
        assert_eq!(s.publish_data(1, true), Err(StreamError::SequenceOverflow));
        assert_eq!(s.snapshot(), before);
        assert_eq!(s.publish_end().unwrap().end_sequence, Some(u64::MAX));
    }
}

#[cfg(test)]
mod stream_backing_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn actual_queue_backing_width_refusal_keeps_the_sdk_cause_and_is_not_an_arrow_extent() {
        let window = RetainedStreamWindow::try_new(usize::MAX - 1, 1).unwrap();
        let cause = OrderedRetainedStream::try_new(window)
            .err()
            .expect("metadata item layout exceeds the addressable request");
        assert!(matches!(cause, StreamError::MetadataBacking(_)));
        let fact = cause.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().code(), 210);
        assert_eq!(
            fact.resource().unwrap().requested,
            u64::try_from(usize::MAX).ok()
        );
        assert_eq!(fact.representation(), None);
        let mut source = cause.source();
        let mut saw_sdk = false;
        while let Some(error) = source {
            saw_sdk |= error
                .downcast_ref::<std::collections::TryReserveError>()
                .is_some();
            source = error.source();
        }
        assert!(saw_sdk);
        let window = RetainedStreamWindow::try_new(usize::MAX, 1).unwrap();
        let cause = OrderedRetainedStream::try_new(window)
            .err()
            .expect("metadata position addition overflows before reserve");
        assert_eq!(cause, StreamError::MetadataArithmetic);
        assert_eq!(cause.failure_fact().class(), FailureClass::Resource);
        assert_eq!(cause.failure_fact().reason().code(), 201);
        assert_eq!(cause.failure_fact().representation(), None);
    }
}
