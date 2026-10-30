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

//! The Frontend side of one ordered root stream, independent of transport.
//!
//! The frontier decides the next read, accepts each reply in sequence and
//! records delivery receipts. It keeps three frontiers apart:
//!
//! * `next_wanted` -- the next item this Frontend asks for;
//! * validated -- data accepted and handed to delivery, at most
//!   [`ROOT_RELAY_WINDOW`] items ahead of consumption;
//! * `consumed_through` -- the last item whose delivery receipt completed,
//!   which is the cumulative ACK sent on the next read.
//!
//! ClientRows bodies are validated whole before any byte is delivered: row
//! prefixes and lengths are checked against the frozen profile, a new prefix
//! must carry payload in the same body, and End must fall on a row boundary
//! with exactly the root's checked row count. Validation does not deliver and
//! delivery does not consume; consumption advances only through in-order
//! receipts. The local End consumption proof exists only when End is known and
//! every earlier item's receipt completed. It is not, by itself, success: the
//! owner still joins it with the root's terminal facts.

use std::num::NonZeroU64;

use novarocks_execution_contract::TaskIdentity;
use novarocks_execution_contract::root_result::{
    RootReadDeferral, RootReadOutcome, RootReadRefusal, RootReadResponse, RootResultEnd,
};
use novarocks_result_contract::{
    ClientBodyError, ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId,
};

/// Data items that may be validated and handed to delivery before their
/// receipts complete. Prefetching the next item while the previous one is
/// being written needs two; more would hold window bytes nothing consumes.
pub const ROOT_RELAY_WINDOW: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootRelayError {
    /// The reply names a different root, profile or output kind.
    Identity,
    /// The Backend acknowledged a consumption frontier this read did not send.
    ConsumedMismatch,
    /// A data or End item arrived out of sequence.
    Sequence,
    /// An item kind this read could not produce (for example ACK-only to a
    /// data request, or data for CountOnly).
    UnexpectedOutcome,
    /// The wanted item was retired before this Frontend read it.
    Retired,
    /// End does not match the rows or row boundary delivered before it.
    EndMismatch,
    /// A receipt arrived for an item that was not the next one delivered.
    ReceiptOrder,
    /// A ClientRows body failed validation; nothing of it was delivered.
    Body(ClientBodyError),
}
impl std::fmt::Display for RootRelayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identity => f.write_str("root reply identity differs from its read"),
            Self::ConsumedMismatch => {
                f.write_str("root reply acknowledged a frontier this read did not send")
            }
            Self::Sequence => f.write_str("root item arrived out of sequence"),
            Self::UnexpectedOutcome => f.write_str("root reply outcome does not answer its read"),
            Self::Retired => f.write_str("wanted root item was retired before it was read"),
            Self::EndMismatch => f.write_str("root End differs from the rows delivered before it"),
            Self::ReceiptOrder => f.write_str("root delivery receipt is out of order"),
            Self::Body(error) => write!(f, "malformed root client rows: {error}"),
        }
    }
}
impl std::error::Error for RootRelayError {}

impl RootRelayError {
    pub fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        // These diagnostics contain only this owner's fixed protocol wording.
        // Body validation retains its producer's safe detail without row bytes.
        let detail = match self {
            Self::Identity => "root reply identity differs from its read",
            Self::ConsumedMismatch => "root reply acknowledged a frontier this read did not send",
            Self::Sequence => "root item arrived out of sequence",
            Self::UnexpectedOutcome => "root reply outcome does not answer its read",
            Self::Retired => "wanted root item was retired before it was read",
            Self::EndMismatch => "root End differs from the rows delivered before it",
            Self::ReceiptOrder => "root delivery receipt is out of order",
            Self::Body(error) => error.safe_detail(),
        };
        novarocks_execution_contract::SafeDetail::truncating(detail)
    }
}

impl novarocks_execution_contract::failure::FailureProjection for RootRelayError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        match self {
            Self::Body(error) => error.failure_fact(),
            Self::Identity
            | Self::ConsumedMismatch
            | Self::Sequence
            | Self::UnexpectedOutcome
            | Self::Retired
            | Self::EndMismatch
            | Self::ReceiptOrder => {
                crate::execution_error::QueryFailureReason::ResultProtocol.failure_fact()
            }
        }
    }
}

/// What the next read asks for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RootRelayRead {
    /// `None` is ACK-only.
    pub wanted: Option<NonZeroU64>,
    pub consumed: u64,
}

/// What to do with an accepted reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootRelayStep {
    Deferred(RootReadDeferral),
    Refused(RootReadRefusal),
    /// Hand this item's validated body to delivery, in order. `rows` is the
    /// number of rows it completes.
    Deliver {
        sequence: NonZeroU64,
        rows: u64,
        end_after_data: Option<RootResultEnd>,
    },
    /// End is known; it is consumed once earlier receipts complete.
    EndKnown(RootResultEnd),
    /// The wanted item is not published yet; read again.
    NotReady,
    /// The root's terminal is decided by lifecycle control, not this stream.
    AwaitTerminalControl,
    /// An ACK-only read was acknowledged.
    Acknowledged,
}

/// One root stream's Frontend frontiers.
#[derive(Clone, Debug)]
pub struct RootRelayFrontier {
    root: TaskIdentity,
    profile: RootProfileId,
    kind: RootOutputKind,
    client_rows: Option<ClientRowProfile>,
    cursor: ClientRowStreamCursor,
    next_wanted: NonZeroU64,
    consumed_through: u64,
    validated_through: u64,
    end: Option<RootResultEnd>,
    acknowledged_end: bool,
    last_read: Option<RootRelayRead>,
}

impl RootRelayFrontier {
    /// `client_rows` is the frozen row profile for ClientRows roots and must
    /// be absent for every other kind.
    pub fn new(
        root: TaskIdentity,
        profile: RootProfileId,
        kind: RootOutputKind,
        client_rows: Option<ClientRowProfile>,
    ) -> Result<Self, RootRelayError> {
        if client_rows.is_some() != (kind == RootOutputKind::ClientRows) {
            return Err(RootRelayError::UnexpectedOutcome);
        }
        Ok(Self {
            root,
            profile,
            kind,
            client_rows,
            cursor: ClientRowStreamCursor::new(),
            next_wanted: NonZeroU64::MIN,
            consumed_through: 0,
            validated_through: 0,
            end: None,
            acknowledged_end: false,
            last_read: None,
        })
    }

    pub const fn root(&self) -> TaskIdentity {
        self.root
    }
    pub const fn profile(&self) -> RootProfileId {
        self.profile
    }
    pub const fn kind(&self) -> RootOutputKind {
        self.kind
    }
    /// The ClientRows profile and the row cursor the next body continues.
    pub fn client_rows(&self) -> Option<(ClientRowProfile, ClientRowStreamCursor)> {
        self.client_rows.map(|profile| (profile, self.cursor))
    }
    pub const fn consumed_through(&self) -> u64 {
        self.consumed_through
    }
    pub const fn end(&self) -> Option<RootResultEnd> {
        self.end
    }
    /// Validated items whose delivery receipt has not completed.
    pub fn in_flight(&self) -> usize {
        (self.validated_through - self.consumed_through) as usize
    }

    /// The next read, or `None` when the stream needs nothing more: either
    /// the window is full until a receipt completes, or End is consumed and
    /// acknowledged.
    pub fn next_read(&self) -> Option<RootRelayRead> {
        if self.end.is_some() {
            // After End, only an optional final ACK of the consumed prefix.
            if self.end_consumed().is_some() && !self.acknowledged_end {
                return Some(RootRelayRead {
                    wanted: None,
                    consumed: self.consumed_through,
                });
            }
            return None;
        }
        if self.in_flight() >= ROOT_RELAY_WINDOW {
            return None;
        }
        Some(RootRelayRead {
            wanted: Some(self.next_wanted),
            consumed: self.consumed_through,
        })
    }

    /// Record the read actually sent; its reply is checked against it.
    pub fn sent(&mut self, read: RootRelayRead) {
        self.last_read = Some(read);
    }

    /// Accept one reply to the last sent read. `body` is the reply's data
    /// body when it has one. Failure changes no frontier.
    pub fn accept(
        &mut self,
        response: &RootReadResponse,
        body: Option<&[u8]>,
    ) -> Result<RootRelayStep, RootRelayError> {
        let read = self.last_read.ok_or(RootRelayError::UnexpectedOutcome)?;
        if response.root_task() != self.root
            || response.profile() != self.profile
            || response.kind() != self.kind
        {
            return Err(RootRelayError::Identity);
        }
        let reply = match response {
            RootReadResponse::Deferred(deferred) => {
                self.last_read = None;
                return Ok(RootRelayStep::Deferred(deferred.reason()));
            }
            RootReadResponse::Refused(refused) => {
                self.last_read = None;
                return Ok(RootRelayStep::Refused(refused.as_ref().clone()));
            }
            RootReadResponse::Reply(reply) => reply,
        };
        if matches!(reply.outcome, RootReadOutcome::AwaitTerminalControl) {
            // Sealing may precede applying this request's cumulative ACK.
            // Only actual completed delivery receipts bound the real watermark.
            if reply.accepted_consumed > self.consumed_through {
                return Err(RootRelayError::ConsumedMismatch);
            }
            self.last_read = None;
            return Ok(RootRelayStep::AwaitTerminalControl);
        }
        if reply.accepted_consumed != read.consumed {
            return Err(RootRelayError::ConsumedMismatch);
        }
        let step = match (&reply.outcome, read.wanted) {
            (RootReadOutcome::AckOnly, None) => {
                if self.end_consumed().is_some() {
                    self.acknowledged_end = true;
                }
                RootRelayStep::Acknowledged
            }
            (RootReadOutcome::NotReady, Some(_)) => RootRelayStep::NotReady,
            (RootReadOutcome::AwaitTerminalControl, _) => RootRelayStep::AwaitTerminalControl,
            (RootReadOutcome::Retired, Some(_)) => return Err(RootRelayError::Retired),
            (RootReadOutcome::Data(data), Some(wanted)) => {
                if data.sequence() != wanted || self.kind == RootOutputKind::CountOnly {
                    return Err(RootRelayError::Sequence);
                }
                let body = body.ok_or(RootRelayError::UnexpectedOutcome)?;
                let (cursor, rows) = match self.client_rows {
                    Some(profile) => {
                        let validated = self
                            .cursor
                            .validate_body(profile, body)
                            .map_err(RootRelayError::Body)?;
                        let after = validated.after();
                        (after, after.completed_rows() - self.cursor.completed_rows())
                    }
                    None if self.kind
                        == RootOutputKind::InternalFacts(
                            novarocks_result_contract::InternalResultDomain::ScalarValueV1,
                        ) =>
                    {
                        // ScalarValueV1 publishes one complete record together
                        // with its sealed End. Its typed consumer validates the
                        // value/NoRows count before completing the receipt.
                        let end = data
                            .end_after_data()
                            .ok_or(RootRelayError::UnexpectedOutcome)?;
                        if self.validated_through != 0 || end.output_rows > 1 {
                            return Err(RootRelayError::EndMismatch);
                        }
                        (self.cursor, end.output_rows)
                    }
                    None => (self.cursor, 0),
                };
                if let Some(end) = data.end_after_data() {
                    self.check_end(end, cursor, wanted.get() + 1)?;
                }
                self.cursor = cursor;
                self.validated_through = wanted.get();
                self.next_wanted = wanted.checked_add(1).ok_or(RootRelayError::Sequence)?;
                self.end = data.end_after_data();
                RootRelayStep::Deliver {
                    sequence: wanted,
                    rows,
                    end_after_data: data.end_after_data(),
                }
            }
            (RootReadOutcome::End(end), Some(wanted)) => {
                if end.sequence != wanted {
                    return Err(RootRelayError::Sequence);
                }
                if self.kind
                    == RootOutputKind::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::ScalarValueV1,
                    )
                {
                    // NoRows is an explicit record, never an empty stream.
                    return Err(RootRelayError::UnexpectedOutcome);
                }
                self.check_end(*end, self.cursor, wanted.get())?;
                self.end = Some(*end);
                RootRelayStep::EndKnown(*end)
            }
            _ => return Err(RootRelayError::UnexpectedOutcome),
        };
        self.last_read = None;
        Ok(step)
    }

    fn check_end(
        &self,
        end: RootResultEnd,
        cursor: ClientRowStreamCursor,
        expected_sequence: u64,
    ) -> Result<(), RootRelayError> {
        if end.sequence.get() != expected_sequence {
            return Err(RootRelayError::Sequence);
        }
        if self.client_rows.is_some() {
            cursor.validate_end().map_err(RootRelayError::Body)?;
            if cursor.completed_rows() != end.output_rows {
                return Err(RootRelayError::EndMismatch);
            }
        }
        Ok(())
    }

    /// Delivery of `sequence` completed. Receipts complete strictly in the
    /// order items were delivered.
    pub fn receipt(&mut self, sequence: NonZeroU64) -> Result<(), RootRelayError> {
        if sequence.get() != self.consumed_through + 1 || sequence.get() > self.validated_through {
            return Err(RootRelayError::ReceiptOrder);
        }
        self.consumed_through = sequence.get();
        Ok(())
    }

    /// The local End consumption proof: End is known and every earlier
    /// item's delivery receipt completed.
    pub fn end_consumed(&self) -> Option<RootResultEnd> {
        self.end
            .filter(|end| self.consumed_through + 1 == end.sequence.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_execution_contract::root_result::{RootResultData, RootResultReply};
    use novarocks_result_contract::RootProfileId;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn root() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn seq(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }
    fn frontier(root: TaskIdentity) -> RootRelayFrontier {
        RootRelayFrontier::new(
            root,
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            Some(ClientRowProfile::try_new(16, 64).unwrap()),
        )
        .unwrap()
    }
    fn reply(root: TaskIdentity, consumed: u64, outcome: RootReadOutcome) -> RootResultReply {
        RootResultReply {
            root_task: root,
            profile: RootProfileId::V1,
            kind: RootOutputKind::ClientRows,
            accepted_consumed: consumed,
            outcome,
        }
    }
    fn data(sequence: u64, body: &'static [u8], end: Option<(u64, u64)>) -> RootReadOutcome {
        RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                seq(sequence),
                Bytes::from_static(body),
                end.map(|(sequence, rows)| RootResultEnd {
                    sequence: seq(sequence),
                    output_rows: rows,
                }),
            )
            .unwrap(),
        )
    }
    fn end(sequence: u64, rows: u64) -> RootReadOutcome {
        RootReadOutcome::End(RootResultEnd {
            sequence: seq(sequence),
            output_rows: rows,
        })
    }
    /// Send the frontier's next read and accept `outcome` for it.
    fn exchange(
        frontier: &mut RootRelayFrontier,
        outcome: RootReadOutcome,
    ) -> Result<RootRelayStep, RootRelayError> {
        let read = frontier.next_read().expect("a read is due");
        frontier.sent(read);
        let body = match &outcome {
            RootReadOutcome::Data(data) => Some(data.body().clone()),
            _ => None,
        };
        let reply = reply(frontier.root(), read.consumed, outcome);
        frontier.accept(&RootReadResponse::Reply(reply), body.as_deref())
    }

    #[test]
    fn prefetch_is_bounded_and_receipts_drive_the_cumulative_ack() {
        let mut frontier = frontier(root());
        assert_eq!(
            frontier.next_read(),
            Some(RootRelayRead {
                wanted: Some(seq(1)),
                consumed: 0
            })
        );
        assert!(matches!(
            exchange(&mut frontier, data(1, &[2, 0, 0, 0, b'a', b'b'], None)),
            Ok(RootRelayStep::Deliver { rows: 1, .. })
        ));
        // Seq 2 is requested while seq 1 is still being written.
        assert_eq!(
            frontier.next_read(),
            Some(RootRelayRead {
                wanted: Some(seq(2)),
                consumed: 0
            })
        );
        exchange(&mut frontier, data(2, &[1, 0, 0, 0, b'c'], None)).unwrap();
        // Two items in flight: no further read until a receipt completes.
        assert_eq!(frontier.next_read(), None);
        assert_eq!(frontier.receipt(seq(2)), Err(RootRelayError::ReceiptOrder));
        frontier.receipt(seq(1)).unwrap();
        assert_eq!(
            frontier.next_read(),
            Some(RootRelayRead {
                wanted: Some(seq(3)),
                consumed: 1
            })
        );
    }

    #[test]
    fn rows_may_cross_segments_but_end_must_close_them_with_the_exact_count() {
        let mut frontier = frontier(root());
        // A 6-byte row split across two bodies.
        exchange(&mut frontier, data(1, &[6, 0, 0, 0, b'a', b'b'], None)).unwrap();
        frontier.receipt(seq(1)).unwrap();
        // End inside the unfinished row is refused and changes nothing.
        assert_eq!(
            exchange(&mut frontier, end(2, 1)),
            Err(RootRelayError::Body(ClientBodyError::EndInsideRow))
        );
        assert_body_projection(
            RootRelayError::Body(ClientBodyError::EndInsideRow),
            ClientBodyError::EndInsideRow,
        );
        assert_eq!(frontier.consumed_through(), 1);
        assert_eq!(frontier.in_flight(), 0);
        assert!(matches!(
            exchange(&mut frontier, data(2, b"cdef", Some((3, 1)))),
            Ok(RootRelayStep::Deliver {
                rows: 1,
                end_after_data: Some(_),
                ..
            })
        ));
        assert_eq!(frontier.end_consumed(), None);
        frontier.receipt(seq(2)).unwrap();
        assert_eq!(frontier.end_consumed().map(|end| end.output_rows), Some(1));
        // One optional ACK-only read retires the consumed prefix.
        assert_eq!(
            frontier.next_read(),
            Some(RootRelayRead {
                wanted: None,
                consumed: 2
            })
        );
        assert_eq!(
            exchange(&mut frontier, RootReadOutcome::AckOnly),
            Ok(RootRelayStep::Acknowledged)
        );
        assert_eq!(frontier.next_read(), None);
    }

    #[test]
    fn end_row_count_and_sequence_must_match_what_was_delivered() {
        let mut frontier = frontier(root());
        exchange(&mut frontier, data(1, &[1, 0, 0, 0, b'a'], None)).unwrap();
        let error = exchange(&mut frontier, end(2, 2)).unwrap_err();
        assert_eq!(error, RootRelayError::EndMismatch);
        {
            use novarocks_execution_contract::failure::FailureProjection;
            assert_eq!(
                error.failure_fact(),
                crate::execution_error::QueryFailureReason::ResultProtocol.failure_fact(),
            );
            assert_eq!(
                error.safe_detail(),
                novarocks_execution_contract::SafeDetail::truncating(
                    "root End differs from the rows delivered before it",
                ),
            );
        }
        assert_eq!(
            exchange(&mut frontier, end(3, 1)),
            Err(RootRelayError::Sequence)
        );
        assert_eq!(
            exchange(&mut frontier, end(2, 1)),
            Ok(RootRelayStep::EndKnown(RootResultEnd {
                sequence: seq(2),
                output_rows: 1
            }))
        );
        // End is known but seq 1 is still being written.
        assert_eq!(frontier.end_consumed(), None);
        assert_eq!(frontier.next_read(), None);
        frontier.receipt(seq(1)).unwrap();
        assert!(frontier.end_consumed().is_some());
    }

    #[test]
    fn empty_stream_end_and_zero_rows_are_consumed_immediately() {
        let mut frontier = frontier(root());
        assert_eq!(
            exchange(&mut frontier, end(1, 0)),
            Ok(RootRelayStep::EndKnown(RootResultEnd {
                sequence: seq(1),
                output_rows: 0
            }))
        );
        assert_eq!(frontier.end_consumed().map(|end| end.output_rows), Some(0));
    }

    fn assert_body_projection(error: RootRelayError, source: ClientBodyError) {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureEvidence, FailureProjection},
        };
        assert_eq!(error.failure_fact(), source.failure_fact());
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(
            error.failure_fact().evidence(),
            FailureEvidence::Originating
        );
        assert_eq!(error.failure_fact().resource(), None);
        assert_eq!(error.failure_fact().representation(), None);
        assert_eq!(
            error.safe_detail(),
            SafeDetail::truncating(source.safe_detail())
        );
        assert_ne!(
            error.failure_fact().reason(),
            crate::execution_error::QueryFailureReason::BackendLost
                .failure_fact()
                .reason(),
        );
    }

    #[test]
    fn malformed_bodies_change_no_frontier() {
        let mut frontier = frontier(root());
        // A new prefix with no payload, a zero-length row and an oversize row.
        for (body, expected) in [
            (
                &[1, 0, 0, 0, b'a', 1, 0][..],
                ClientBodyError::PrefixWithoutPayload,
            ),
            (&[0, 0, 0, 0, b'a'][..], ClientBodyError::InvalidRowLength),
            (&[65, 0, 0, 0, b'a'][..], ClientBodyError::InvalidRowLength),
        ] {
            let read = frontier.next_read().unwrap();
            frontier.sent(read);
            let outcome = RootReadOutcome::Data(
                RootResultData::try_new(
                    RootOutputKind::ClientRows,
                    seq(1),
                    Bytes::copy_from_slice(body),
                    None,
                )
                .unwrap(),
            );
            let reply = reply(frontier.root(), 0, outcome);
            let error = frontier
                .accept(&RootReadResponse::Reply(reply), Some(body))
                .unwrap_err();
            assert_eq!(error, RootRelayError::Body(expected));
            assert_body_projection(error, expected);
            assert_eq!(frontier.in_flight(), 0);
            assert_eq!(
                frontier.next_read(),
                Some(RootRelayRead {
                    wanted: Some(seq(1)),
                    consumed: 0
                })
            );
        }
    }

    #[test]
    fn identity_ack_frontier_and_outcome_mismatches_are_refused() {
        let root = root();
        let mut frontier = frontier(root);
        let read = frontier.next_read().unwrap();
        frontier.sent(read);
        let mut foreign = reply(root, 0, RootReadOutcome::NotReady);
        foreign.kind = RootOutputKind::CountOnly;
        assert_eq!(
            frontier.accept(&RootReadResponse::Reply(foreign), None),
            Err(RootRelayError::Identity)
        );
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 3, RootReadOutcome::NotReady)),
                None
            ),
            Err(RootRelayError::ConsumedMismatch)
        );
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 0, RootReadOutcome::AckOnly)),
                None
            ),
            Err(RootRelayError::UnexpectedOutcome)
        );
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 0, RootReadOutcome::Retired)),
                None
            ),
            Err(RootRelayError::Retired)
        );
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 0, data(2, &[1, 0, 0, 0, b'a'], None))),
                Some(&[1, 0, 0, 0, b'a'])
            ),
            Err(RootRelayError::Sequence)
        );
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 0, RootReadOutcome::NotReady)),
                None
            ),
            Ok(RootRelayStep::NotReady)
        );
        // A reply with no matching sent read is refused.
        assert_eq!(
            frontier.accept(
                &RootReadResponse::Reply(reply(root, 0, RootReadOutcome::NotReady)),
                None
            ),
            Err(RootRelayError::UnexpectedOutcome)
        );
    }

    #[test]
    fn scalar_data_uses_its_sealed_end_count_and_requires_the_explicit_record() {
        use novarocks_result_contract::InternalResultDomain;
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1);
        for rows in [0, 1, 2] {
            let root = root();
            let mut frontier = RootRelayFrontier::new(root, RootProfileId::V1, kind, None).unwrap();
            let read = frontier.next_read().unwrap();
            frontier.sent(read);
            let outcome = RootReadOutcome::Data(
                RootResultData::try_new(
                    kind,
                    seq(1),
                    Bytes::from_static(b"domain consumer validates the record"),
                    Some(RootResultEnd {
                        sequence: seq(2),
                        output_rows: rows,
                    }),
                )
                .unwrap(),
            );
            let reply = RootResultReply {
                root_task: root,
                profile: RootProfileId::V1,
                kind,
                accepted_consumed: 0,
                outcome,
            };
            let accepted = frontier.accept(
                &RootReadResponse::Reply(reply),
                Some(b"domain consumer validates the record"),
            );
            if rows <= 1 {
                assert!(
                    matches!(accepted, Ok(RootRelayStep::Deliver { rows: actual, .. }) if actual == rows)
                );
                assert!(frontier.end_consumed().is_none());
                frontier.receipt(seq(1)).unwrap();
                assert_eq!(frontier.end_consumed().unwrap().output_rows, rows);
            } else {
                assert_eq!(accepted, Err(RootRelayError::EndMismatch));
            }
        }
        for outcome in [
            end(1, 0),
            RootReadOutcome::Data(
                RootResultData::try_new(kind, seq(1), Bytes::from_static(b"unsealed"), None)
                    .unwrap(),
            ),
        ] {
            let root = root();
            let mut frontier = RootRelayFrontier::new(root, RootProfileId::V1, kind, None).unwrap();
            let read = frontier.next_read().unwrap();
            frontier.sent(read);
            let body = if let RootReadOutcome::Data(data) = &outcome {
                Some(data.body().as_ref())
            } else {
                None
            };
            let reply = RootResultReply {
                root_task: root,
                profile: RootProfileId::V1,
                kind,
                accepted_consumed: 0,
                outcome: outcome.clone(),
            };
            assert_eq!(
                frontier.accept(&RootReadResponse::Reply(reply), body),
                Err(RootRelayError::UnexpectedOutcome)
            );
            assert_eq!(frontier.validated_through, 0);
        }
    }

    #[test]
    fn non_client_roots_need_no_row_profile_and_count_only_has_no_data() {
        assert!(
            RootRelayFrontier::new(
                root(),
                RootProfileId::V1,
                RootOutputKind::CountOnly,
                Some(ClientRowProfile::try_new(16, 64).unwrap())
            )
            .is_err()
        );
        let mut count =
            RootRelayFrontier::new(root(), RootProfileId::V1, RootOutputKind::CountOnly, None)
                .unwrap();
        let read = count.next_read().unwrap();
        count.sent(read);
        let reply = RootResultReply {
            root_task: count.root(),
            profile: RootProfileId::V1,
            kind: RootOutputKind::CountOnly,
            accepted_consumed: 0,
            outcome: end(1, 42),
        };
        assert_eq!(
            count.accept(&RootReadResponse::Reply(reply), None),
            Ok(RootRelayStep::EndKnown(RootResultEnd {
                sequence: seq(1),
                output_rows: 42
            }))
        );
        assert_eq!(count.end_consumed().map(|end| end.output_rows), Some(42));
    }
    #[test]
    fn sealed_reply_uses_actual_watermark_without_rewinding_delivery_or_ack() {
        let stream_root = root();
        let mut original = frontier(stream_root);
        exchange(&mut original, data(1, &[1, 0, 0, 0, b'a'], None)).unwrap();
        original.receipt(seq(1)).unwrap();
        let read = original.next_read().unwrap();
        assert_eq!(read.consumed, 1);
        original.sent(read);
        let sealed =
            RootReadResponse::Reply(reply(stream_root, 0, RootReadOutcome::AwaitTerminalControl));
        assert_eq!(
            original.accept(&sealed, None),
            Ok(RootRelayStep::AwaitTerminalControl)
        );
        assert_eq!(original.next_read(), Some(read));
        assert_eq!(original.end_consumed(), None);

        let mut original = frontier(stream_root);
        exchange(&mut original, data(1, &[1, 0, 0, 0, b'a'], None)).unwrap();
        // Validating bytes does not prove their socket delivery.
        let read = original.next_read().unwrap();
        original.sent(read);
        let unproven =
            RootReadResponse::Reply(reply(stream_root, 1, RootReadOutcome::AwaitTerminalControl));
        assert_eq!(
            original.accept(&unproven, None),
            Err(RootRelayError::ConsumedMismatch)
        );
    }
    #[test]
    fn deferred_response_keeps_the_exact_original_cumulative_read_replayable() {
        use novarocks_execution_contract::root_result::{RootReadDeferred, RootResultRead};
        let mut frontier = frontier(root());
        exchange(&mut frontier, data(1, &[1, 0, 0, 0, b'a'], None)).unwrap();
        frontier.receipt(seq(1)).unwrap();
        let read = frontier.next_read().unwrap();
        let request = RootResultRead::try_new(
            frontier.root(),
            frontier.profile(),
            frontier.kind(),
            read.wanted,
            read.consumed,
            std::time::Duration::from_millis(10),
        )
        .unwrap();
        frontier.sent(read);
        let deferred = RootReadResponse::Deferred(RootReadDeferred::for_request(
            &request,
            RootReadDeferral::Preparing,
        ));
        assert_eq!(
            frontier.accept(&deferred, None),
            Ok(RootRelayStep::Deferred(RootReadDeferral::Preparing))
        );
        assert_eq!(frontier.next_read(), Some(read));
        assert_eq!(frontier.end_consumed(), None);
        assert_eq!(
            frontier.accept(&deferred, None),
            Err(RootRelayError::UnexpectedOutcome)
        );
    }
}
