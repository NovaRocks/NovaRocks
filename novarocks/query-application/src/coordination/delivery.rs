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

#![allow(
    dead_code,
    reason = "delivery phase helpers remain private to the actor-owned result path"
)]

use novarocks_types::identity::QueryExecutionId;

use super::{ResultPacketVerdict, RootResultStream};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryPhase {
    BeforeVisibility,
    Visible,
    Ended,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryGateError {
    ForeignAttempt,
    NoEligibleAttempt,
    StaleEligibility,
    Packet(ResultPacketVerdict),
    DeliveryInFlight,
    NoDeliveryInFlight,
    AlreadyEnded,
    EligibilityExhausted,
    SchemaDeliveryInFlight,
    SchemaDeliveryFailed,
    SchemaNotEmitted,
    WrongExecutionPhase,
    WrongOutputMode,
}

/// Closed root-result shape. Every data packet conservatively closes recovery
/// before the writer can borrow its payload; only the payload-free EOS marker
/// preserves the before-visibility state.
#[derive(Debug, Eq, PartialEq)]
pub enum ResultPacket<P> {
    Data(P),
    EndOfStream,
}

impl<P> ResultPacket<P> {
    const fn contains_data(&self) -> bool {
        matches!(self, Self::Data(_))
    }

    const fn end_of_stream(&self) -> bool {
        matches!(self, Self::EndOfStream)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingPacket {
    sequence: u64,
    end_of_stream: bool,
    contains_data: bool,
}

/// Move-only ownership of one packet after the visibility linearization point.
/// The payload remains inseparable from the eligibility proof until the
/// protocol write completes or the statement fails.
#[derive(Debug)]
#[must_use = "a delivery permit must be completed or failed by its actor owner"]
pub struct DeliveryPermit<P> {
    execution: QueryExecutionId,
    eligibility_generation: u64,
    packet: PendingPacket,
    packet_owner: ResultPacket<P>,
}

impl<P> DeliveryPermit<P> {
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub const fn sequence(&self) -> u64 {
        self.packet.sequence
    }

    pub const fn end_of_stream(&self) -> bool {
        self.packet.end_of_stream
    }

    pub const fn contains_data(&self) -> bool {
        self.packet.contains_data
    }

    pub const fn payload(&self) -> Option<&P> {
        match &self.packet_owner {
            ResultPacket::Data(payload) => Some(payload),
            ResultPacket::EndOfStream => None,
        }
    }
}

#[derive(Debug)]
pub struct DeliveryRejection<P> {
    error: DeliveryGateError,
    packet: ResultPacket<P>,
}

impl<P> DeliveryRejection<P> {
    pub(crate) const fn new(error: DeliveryGateError, packet: ResultPacket<P>) -> Self {
        Self { error, packet }
    }

    pub const fn error(&self) -> DeliveryGateError {
        self.error
    }

    pub fn into_parts(self) -> (DeliveryGateError, ResultPacket<P>) {
        (self.error, self.packet)
    }
}

#[derive(Debug)]
pub struct DeliveryCompletionError<P> {
    error: DeliveryGateError,
    permit: DeliveryPermit<P>,
}

impl<P> DeliveryCompletionError<P> {
    pub(crate) const fn new(error: DeliveryGateError, permit: DeliveryPermit<P>) -> Self {
        Self { error, permit }
    }

    pub const fn error(&self) -> DeliveryGateError {
        self.error
    }

    pub fn into_parts(self) -> (DeliveryGateError, DeliveryPermit<P>) {
        (self.error, self.permit)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryCompletion {
    acknowledged_through: u64,
    end_of_stream: bool,
}

impl DeliveryCompletion {
    pub const fn acknowledged_through(self) -> u64 {
        self.acknowledged_through
    }

    pub const fn end_of_stream(self) -> bool {
        self.end_of_stream
    }
}

#[derive(Debug)]
pub struct CompletedDelivery<P> {
    completion: DeliveryCompletion,
    packet: ResultPacket<P>,
}

impl<P> CompletedDelivery<P> {
    pub const fn completion(&self) -> DeliveryCompletion {
        self.completion
    }

    pub fn into_packet(self) -> ResultPacket<P> {
        self.packet
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaDeliveryState {
    NotStarted,
    InFlight {
        execution: QueryExecutionId,
        eligibility_generation: u64,
    },
    Emitted,
    Failed,
}

#[derive(Debug)]
#[must_use = "a schema permit must be completed or failed by its actor owner"]
pub struct SchemaDeliveryPermit {
    execution: QueryExecutionId,
    eligibility_generation: u64,
}

impl SchemaDeliveryPermit {
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }
}

#[derive(Debug)]
pub enum BeginSchemaDelivery {
    AlreadyEmitted,
    Permit(SchemaDeliveryPermit),
}

#[derive(Debug)]
pub(crate) struct DeliveryGate {
    current: Option<QueryExecutionId>,
    eligibility_generation: u64,
    phase: DeliveryPhase,
    data_visible: bool,
    schema: SchemaDeliveryState,
    accepted_stream: RootResultStream,
    delivered_through: Option<u64>,
    in_flight: Option<PendingPacket>,
}

impl DeliveryGate {
    pub const fn new(current: QueryExecutionId) -> Self {
        Self {
            current: Some(current),
            eligibility_generation: 1,
            phase: DeliveryPhase::BeforeVisibility,
            data_visible: false,
            schema: SchemaDeliveryState::NotStarted,
            accepted_stream: RootResultStream::new(),
            delivered_through: None,
            in_flight: None,
        }
    }

    pub(crate) const fn eligibility_generation(&self) -> u64 {
        self.eligibility_generation
    }

    pub(crate) const fn has_eligible_attempt(&self) -> bool {
        self.current.is_some()
    }

    pub(crate) const fn phase(&self) -> DeliveryPhase {
        self.phase
    }

    pub(crate) const fn output_visible(&self) -> bool {
        self.data_visible
    }

    pub(crate) const fn schema_emitted(&self) -> bool {
        matches!(self.schema, SchemaDeliveryState::Emitted)
    }

    pub(crate) fn begin_schema_delivery(
        &mut self,
        execution: QueryExecutionId,
    ) -> Result<BeginSchemaDelivery, DeliveryGateError> {
        self.require_current(execution)?;
        match self.schema {
            SchemaDeliveryState::NotStarted => {
                self.schema = SchemaDeliveryState::InFlight {
                    execution,
                    eligibility_generation: self.eligibility_generation,
                };
                Ok(BeginSchemaDelivery::Permit(SchemaDeliveryPermit {
                    execution,
                    eligibility_generation: self.eligibility_generation,
                }))
            }
            SchemaDeliveryState::Emitted => Ok(BeginSchemaDelivery::AlreadyEmitted),
            SchemaDeliveryState::InFlight { .. } => Err(DeliveryGateError::SchemaDeliveryInFlight),
            SchemaDeliveryState::Failed => Err(DeliveryGateError::SchemaDeliveryFailed),
        }
    }

    pub(crate) fn complete_schema_delivery(
        &mut self,
        permit: SchemaDeliveryPermit,
    ) -> Result<(), DeliveryGateError> {
        self.require_schema_permit(&permit)?;
        self.schema = SchemaDeliveryState::Emitted;
        Ok(())
    }

    pub(crate) fn fail_schema_delivery(
        &mut self,
        permit: SchemaDeliveryPermit,
    ) -> Result<(), DeliveryGateError> {
        self.require_schema_permit(&permit)?;
        self.schema = SchemaDeliveryState::Failed;
        Ok(())
    }

    pub(crate) fn accepted_packets(&self) -> u64 {
        self.accepted_stream.packets_consumed()
    }

    pub(crate) const fn delivered_through(&self) -> Option<u64> {
        self.delivered_through
    }

    /// Atomically accepts the next packet and grants the only protocol write.
    /// At most one packet may be accepted but not acknowledged, so dropping a
    /// queued pre-permit token cannot create a permanent sequence hole.
    pub(crate) fn begin_delivery<P>(
        &mut self,
        execution: QueryExecutionId,
        sequence: u64,
        packet_owner: ResultPacket<P>,
    ) -> Result<DeliveryPermit<P>, DeliveryRejection<P>> {
        if let Err(error) = self.require_current(execution) {
            return Err(DeliveryRejection {
                error,
                packet: packet_owner,
            });
        }
        match self.schema {
            SchemaDeliveryState::Emitted => {}
            SchemaDeliveryState::InFlight { .. } => {
                return Err(DeliveryRejection {
                    error: DeliveryGateError::SchemaDeliveryInFlight,
                    packet: packet_owner,
                });
            }
            SchemaDeliveryState::Failed => {
                return Err(DeliveryRejection {
                    error: DeliveryGateError::SchemaDeliveryFailed,
                    packet: packet_owner,
                });
            }
            SchemaDeliveryState::NotStarted => {
                return Err(DeliveryRejection {
                    error: DeliveryGateError::SchemaNotEmitted,
                    packet: packet_owner,
                });
            }
        }
        if matches!(self.phase, DeliveryPhase::Ended) {
            return Err(DeliveryRejection {
                error: DeliveryGateError::AlreadyEnded,
                packet: packet_owner,
            });
        }
        if self.in_flight.is_some() {
            return Err(DeliveryRejection {
                error: DeliveryGateError::DeliveryInFlight,
                packet: packet_owner,
            });
        }
        let verdict = self.accepted_stream.classify(sequence);
        if !matches!(verdict, ResultPacketVerdict::Accept) {
            return Err(DeliveryRejection {
                error: DeliveryGateError::Packet(verdict),
                packet: packet_owner,
            });
        }
        let end_of_stream = packet_owner.end_of_stream();
        let packet = PendingPacket {
            sequence,
            end_of_stream,
            contains_data: packet_owner.contains_data(),
        };
        let consumed = self.accepted_stream.consume(sequence, end_of_stream);
        debug_assert!(matches!(consumed, ResultPacketVerdict::Accept));
        self.in_flight = Some(packet);
        if packet.contains_data {
            self.data_visible = true;
            self.phase = DeliveryPhase::Visible;
        }
        Ok(DeliveryPermit {
            execution,
            eligibility_generation: self.eligibility_generation,
            packet,
            packet_owner,
        })
    }

    pub(crate) fn complete_delivery<P>(
        &mut self,
        permit: DeliveryPermit<P>,
    ) -> Result<CompletedDelivery<P>, DeliveryCompletionError<P>> {
        let error = match self.require_current(permit.execution) {
            Ok(()) if permit.eligibility_generation != self.eligibility_generation => {
                Some(DeliveryGateError::StaleEligibility)
            }
            Ok(()) => match self.in_flight {
                Some(packet) if packet == permit.packet => None,
                Some(_) | None => Some(DeliveryGateError::NoDeliveryInFlight),
            },
            Err(error) => Some(error),
        };
        if let Some(error) = error {
            return Err(DeliveryCompletionError { error, permit });
        }

        self.in_flight = None;
        self.delivered_through = Some(permit.packet.sequence);
        if permit.packet.end_of_stream {
            debug_assert!(self.accepted_stream.end_of_stream_observed());
            self.phase = DeliveryPhase::Ended;
        }
        Ok(CompletedDelivery {
            completion: DeliveryCompletion {
                acknowledged_through: permit.packet.sequence,
                end_of_stream: permit.packet.end_of_stream,
            },
            packet: permit.packet_owner,
        })
    }

    pub(crate) fn fail_delivery<P>(
        &mut self,
        permit: DeliveryPermit<P>,
    ) -> Result<ResultPacket<P>, DeliveryCompletionError<P>> {
        let error = match self.require_current(permit.execution) {
            Ok(()) if permit.eligibility_generation != self.eligibility_generation => {
                Some(DeliveryGateError::StaleEligibility)
            }
            Ok(()) if self.in_flight == Some(permit.packet) => None,
            Ok(()) => Some(DeliveryGateError::NoDeliveryInFlight),
            Err(error) => Some(error),
        };
        if let Some(error) = error {
            return Err(DeliveryCompletionError { error, permit });
        }
        self.in_flight = None;
        self.phase = DeliveryPhase::Ended;
        Ok(permit.packet_owner)
    }

    pub(crate) fn revoke_for_replacement(
        &mut self,
        old: QueryExecutionId,
    ) -> Result<u64, DeliveryGateError> {
        self.require_current(old)?;
        if self.data_visible || self.in_flight.is_some() {
            return Err(DeliveryGateError::StaleEligibility);
        }
        match self.schema {
            SchemaDeliveryState::InFlight { .. } => {
                return Err(DeliveryGateError::SchemaDeliveryInFlight);
            }
            SchemaDeliveryState::Failed => return Err(DeliveryGateError::SchemaDeliveryFailed),
            SchemaDeliveryState::NotStarted | SchemaDeliveryState::Emitted => {}
        }
        self.bump_generation()?;
        self.current = None;
        self.accepted_stream = RootResultStream::new();
        self.delivered_through = None;
        Ok(self.eligibility_generation)
    }

    pub(crate) fn activate(&mut self, new: QueryExecutionId) -> Result<u64, DeliveryGateError> {
        if self.current.is_some() {
            return Err(DeliveryGateError::StaleEligibility);
        }
        if self.data_visible || matches!(self.phase, DeliveryPhase::Ended) {
            return Err(DeliveryGateError::AlreadyEnded);
        }
        self.current = Some(new);
        self.accepted_stream = RootResultStream::new();
        self.delivered_through = None;
        self.in_flight = None;
        Ok(self.eligibility_generation)
    }

    pub(crate) fn close_current(
        &mut self,
        current: QueryExecutionId,
    ) -> Result<(), DeliveryGateError> {
        self.require_current(current)?;
        self.bump_generation()?;
        self.current = None;
        self.in_flight = None;
        self.phase = DeliveryPhase::Ended;
        Ok(())
    }

    fn require_current(&self, execution: QueryExecutionId) -> Result<(), DeliveryGateError> {
        match self.current {
            Some(current) if current == execution => Ok(()),
            Some(_) => Err(DeliveryGateError::ForeignAttempt),
            None => Err(DeliveryGateError::NoEligibleAttempt),
        }
    }

    fn require_schema_permit(
        &self,
        permit: &SchemaDeliveryPermit,
    ) -> Result<(), DeliveryGateError> {
        match self.schema {
            SchemaDeliveryState::InFlight {
                execution,
                eligibility_generation,
            } if execution == permit.execution
                && eligibility_generation == permit.eligibility_generation =>
            {
                Ok(())
            }
            SchemaDeliveryState::InFlight { .. } => Err(DeliveryGateError::StaleEligibility),
            SchemaDeliveryState::Failed => Err(DeliveryGateError::SchemaDeliveryFailed),
            SchemaDeliveryState::NotStarted | SchemaDeliveryState::Emitted => {
                Err(DeliveryGateError::SchemaDeliveryInFlight)
            }
        }
    }

    fn bump_generation(&mut self) -> Result<(), DeliveryGateError> {
        self.eligibility_generation = self
            .eligibility_generation
            .checked_add(1)
            .ok_or(DeliveryGateError::EligibilityExhausted)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::identity::{AttemptId, QueryId};

    #[derive(Debug, Eq, PartialEq)]
    struct Payload;

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(1, 2),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("nonzero query")
    }

    fn emit_schema(gate: &mut DeliveryGate, execution: QueryExecutionId) {
        let BeginSchemaDelivery::Permit(permit) = gate.begin_schema_delivery(execution).unwrap()
        else {
            panic!("schema must not already be emitted");
        };
        gate.complete_schema_delivery(permit).unwrap();
    }

    #[test]
    fn delivery_winning_the_race_irreversibly_closes_replacement() {
        let mut gate = DeliveryGate::new(execution(1));
        emit_schema(&mut gate, execution(1));
        let permit = gate
            .begin_delivery(execution(1), 0, ResultPacket::Data(Payload))
            .unwrap();
        assert_eq!(permit.execution(), execution(1));
        assert!(gate.output_visible());
        assert_eq!(
            gate.revoke_for_replacement(execution(1)),
            Err(DeliveryGateError::StaleEligibility)
        );
    }

    #[test]
    fn replacement_winning_the_race_rejects_old_payload_without_losing_it() {
        let mut gate = DeliveryGate::new(execution(1));
        emit_schema(&mut gate, execution(1));
        gate.revoke_for_replacement(execution(1)).unwrap();
        gate.activate(execution(2)).unwrap();
        let rejection = gate
            .begin_delivery(execution(1), 0, ResultPacket::Data(Payload))
            .unwrap_err();
        assert_eq!(rejection.error(), DeliveryGateError::ForeignAttempt);
        assert_eq!(rejection.into_parts().1, ResultPacket::Data(Payload));
    }

    #[test]
    fn accepted_and_delivered_watermarks_are_distinct() {
        let mut gate = DeliveryGate::new(execution(1));
        emit_schema(&mut gate, execution(1));
        let first = gate
            .begin_delivery(execution(1), 0, ResultPacket::Data(Payload))
            .unwrap();
        assert_eq!(gate.accepted_packets(), 1);
        assert_eq!(gate.delivered_through(), None);
        assert_eq!(
            gate.begin_delivery(execution(1), 1, ResultPacket::Data(Payload))
                .unwrap_err()
                .error(),
            DeliveryGateError::DeliveryInFlight
        );
        let completed = gate.complete_delivery(first).unwrap();
        assert_eq!(completed.completion().acknowledged_through(), 0);
        assert_eq!(completed.into_packet(), ResultPacket::Data(Payload));
        assert_eq!(gate.delivered_through(), Some(0));
        let second = gate
            .begin_delivery(execution(1), 1, ResultPacket::Data(Payload))
            .unwrap();
        assert_eq!(
            gate.complete_delivery(second).unwrap().completion(),
            DeliveryCompletion {
                acknowledged_through: 1,
                end_of_stream: false,
            }
        );
    }

    #[test]
    fn schema_is_committed_once_across_attempts() {
        let mut gate = DeliveryGate::new(execution(1));
        let BeginSchemaDelivery::Permit(permit) = gate.begin_schema_delivery(execution(1)).unwrap()
        else {
            panic!("first schema write must receive a permit");
        };
        assert_eq!(
            gate.revoke_for_replacement(execution(1)),
            Err(DeliveryGateError::SchemaDeliveryInFlight)
        );
        gate.complete_schema_delivery(permit).unwrap();
        gate.revoke_for_replacement(execution(1)).unwrap();
        gate.activate(execution(2)).unwrap();
        assert!(matches!(
            gate.begin_schema_delivery(execution(2)).unwrap(),
            BeginSchemaDelivery::AlreadyEmitted
        ));
        let eos = gate
            .begin_delivery(execution(2), 0, ResultPacket::<Payload>::EndOfStream)
            .unwrap();
        assert!(!gate.output_visible());
        gate.complete_delivery(eos).unwrap();
        assert_eq!(gate.phase(), DeliveryPhase::Ended);
        assert!(!gate.output_visible());
    }

    #[test]
    fn failed_schema_write_closes_recovery() {
        let mut gate = DeliveryGate::new(execution(1));
        let BeginSchemaDelivery::Permit(permit) = gate.begin_schema_delivery(execution(1)).unwrap()
        else {
            panic!("first schema write must receive a permit");
        };
        gate.fail_schema_delivery(permit).unwrap();
        assert_eq!(
            gate.revoke_for_replacement(execution(1)),
            Err(DeliveryGateError::SchemaDeliveryFailed)
        );
    }
}
