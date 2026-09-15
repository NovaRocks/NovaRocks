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

//! Frontend-only Runtime Filter terminal projection.
//!
//! Participant admission and de-duplication belong to the caller.  This module
//! deliberately only reads the set it is handed: it preserves each
//! participant's validated wire facts and computes diagnostic query totals
//! without introducing another owner of participant identity.

use novarocks_proto_models::novarocks;
use novarocks_types::BackendProcessId;

/// A deterministic, query-scoped projection of Runtime Filter terminal facts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeFilterTerminalRollup {
    /// In the order the caller supplied the contributions.
    pub(crate) participants: Vec<RuntimeFilterParticipantTerminalTelemetry>,
    pub(crate) totals: RuntimeFilterTerminalTotalsTelemetry,
}

/// Participant identity prefixes every owner-local Runtime Filter detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RuntimeFilterTerminalParticipant {
    pub(crate) process_id: BackendProcessId,
}

/// The explicit P2 telemetry variant emitted by one terminal participant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalUnavailable {
    pub(crate) stage: String,
    pub(crate) code: String,
}

/// One participant's complete terminal telemetry.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeFilterParticipantTerminalTelemetry {
    pub(crate) participant: RuntimeFilterTerminalParticipant,
    pub(crate) telemetry: RuntimeFilterParticipantTerminalTelemetryValue,
}

/// Available details retain the validated generated leaves; unavailable
/// telemetry never pretends that an empty contribution was observed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RuntimeFilterParticipantTerminalTelemetryValue {
    Available(RuntimeFilterParticipantTerminalDetails),
    Unavailable(RuntimeFilterTerminalUnavailable),
}

/// All four participant-local Runtime Filter sections.
///
/// The enclosing participant identity is the required prefix for every leaf.
/// Keeping generated values here avoids creating a second Protocol DTO for
/// terminal details.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeFilterParticipantTerminalDetails {
    pub(crate) channels: Vec<novarocks::QueryTerminalRuntimeFilterChannelV1>,
    pub(crate) producer_streams: Vec<novarocks::QueryTerminalRuntimeFilterProducerStreamV1>,
    pub(crate) transport_routes: Vec<novarocks::QueryTerminalRuntimeFilterTransportRouteV1>,
    pub(crate) consumers: Vec<novarocks::QueryTerminalRuntimeFilterConsumerV1>,
}

/// Query totals are diagnostic-only: either the complete checked sum or an
/// explicit reason why no honest total can be represented.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "Diagnostic terminal telemetry retains the complete available totals without losing unavailable reasons."
)]
pub(crate) enum RuntimeFilterTerminalTotalsTelemetry {
    Available(RuntimeFilterTerminalTotals),
    Unavailable(RuntimeFilterTerminalTotalsUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterTerminalTotalsUnavailable {
    ParticipantTelemetryUnavailable,
    CounterOverflow,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalTotals {
    pub(crate) channels: RuntimeFilterTerminalChannelTotals,
    pub(crate) producer_streams: RuntimeFilterTerminalProducerStreamTotals,
    pub(crate) transport_routes: RuntimeFilterTerminalTransportRouteTotals,
    pub(crate) consumers: RuntimeFilterTerminalConsumerTotals,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalChannelTotals {
    pub(crate) count: u64,
    pub(crate) published_count: u64,
    pub(crate) completed_count: u64,
    pub(crate) unavailable_count: u64,
    pub(crate) cancelled_count: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalProducerStreamTotals {
    pub(crate) count: u64,
    pub(crate) accepted_count: u64,
    pub(crate) duplicate_count: u64,
    pub(crate) stale_count: u64,
    pub(crate) conflict_count: u64,
    pub(crate) resource_limit_count: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalTransportRouteTotals {
    pub(crate) count: u64,
    pub(crate) sent_count: u64,
    pub(crate) sent_bytes: u64,
    pub(crate) retried_count: u64,
    pub(crate) retried_bytes: u64,
    pub(crate) acked_count: u64,
    pub(crate) acked_bytes: u64,
    pub(crate) fail_open_count: u64,
    pub(crate) fail_open_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalConsumerTotals {
    pub(crate) count: u64,
    pub(crate) row_evaluations: u64,
    pub(crate) input_rows: u64,
    pub(crate) output_rows: u64,
    pub(crate) scan_evaluated: u64,
    pub(crate) scan_kept: u64,
    pub(crate) scan_pruned: u64,
    pub(crate) scan_not_evaluated: u64,
    pub(crate) scan_not_evaluated_reasons: RuntimeFilterTerminalScanNotEvaluatedTotals,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterTerminalScanNotEvaluatedTotals {
    pub(crate) unit_facts_missing: u64,
    pub(crate) column_facts_missing: u64,
    pub(crate) data_type_unsupported: u64,
    pub(crate) predicate_capability_unsupported: u64,
    pub(crate) resource_unavailable: u64,
    pub(crate) snapshot_unavailable: u64,
    pub(crate) snapshot_timed_out: u64,
    pub(crate) snapshot_not_published: u64,
}

/// Folds one participant's telemetry at a time into the query rollup.
///
/// Both carriers of these facts fold through this: the retired lifecycle's
/// terminal set and the task protocol's release acknowledgements. Two folds
/// would be two chances for the same counters to be summed differently, and
/// the rollup is what every structured assertion reads.
struct RuntimeFilterTerminalRollupBuilder {
    participants: Vec<RuntimeFilterParticipantTerminalTelemetry>,
    totals: RuntimeFilterTerminalTotals,
    totals_unavailable: Option<RuntimeFilterTerminalTotalsUnavailable>,
}

impl RuntimeFilterTerminalRollupBuilder {
    fn with_capacity(participants: usize) -> Self {
        Self {
            participants: Vec::with_capacity(participants),
            totals: RuntimeFilterTerminalTotals::default(),
            totals_unavailable: None,
        }
    }

    fn absorb(
        &mut self,
        process_id: BackendProcessId,
        telemetry: &novarocks_proto_codec::lifecycle::QueryTerminalProfileContributionTelemetry,
    ) {
        let participant = RuntimeFilterTerminalParticipant { process_id };
        let telemetry = if let Some(contribution) = telemetry.available() {
            if self.totals_unavailable.is_none()
                && add_contribution_totals(&mut self.totals, &contribution).is_err()
            {
                self.totals_unavailable =
                    Some(RuntimeFilterTerminalTotalsUnavailable::CounterOverflow);
            }
            RuntimeFilterParticipantTerminalTelemetryValue::Available(
                RuntimeFilterParticipantTerminalDetails {
                    channels: contribution.channels().to_vec(),
                    producer_streams: contribution.producer_streams().to_vec(),
                    transport_routes: contribution.transport_routes().to_vec(),
                    consumers: contribution.consumers().to_vec(),
                },
            )
        } else {
            let unavailable = telemetry
                .unavailable()
                .expect("validated telemetry is available or unavailable");
            self.totals_unavailable =
                Some(RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable);
            RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(
                RuntimeFilterTerminalUnavailable {
                    stage: unavailable.stage().to_owned(),
                    code: unavailable.code().to_owned(),
                },
            )
        };
        self.participants
            .push(RuntimeFilterParticipantTerminalTelemetry {
                participant,
                telemetry,
            });
    }

    fn finish(self) -> RuntimeFilterTerminalRollup {
        RuntimeFilterTerminalRollup {
            participants: self.participants,
            totals: match self.totals_unavailable {
                Some(reason) => RuntimeFilterTerminalTotalsTelemetry::Unavailable(reason),
                None => RuntimeFilterTerminalTotalsTelemetry::Available(self.totals),
            },
        }
    }
}

/// Folds the contributions the task protocol's release acknowledgements
/// carried, one per backend that released.
///
/// The caller owns completeness: this reports exactly the participants it was
/// given, in the order it was given them, and never substitutes an empty
/// contribution for a backend whose release did not answer.
pub(crate) fn rollup_from_release_contributions(
    contributions: &[(
        BackendProcessId,
        novarocks_proto_codec::lifecycle::QueryTerminalProfileContributionTelemetry,
    )],
) -> RuntimeFilterTerminalRollup {
    let mut builder = RuntimeFilterTerminalRollupBuilder::with_capacity(contributions.len());
    for (process_id, telemetry) in contributions {
        builder.absorb(*process_id, telemetry);
    }
    builder.finish()
}

fn add_contribution_totals(
    totals: &mut RuntimeFilterTerminalTotals,
    contribution: &novarocks_proto_codec::lifecycle::QueryTerminalProfileContributionV1,
) -> Result<(), ()> {
    for channel in contribution.channels() {
        checked_add(&mut totals.channels.count, 1)?;
        checked_add(
            &mut totals.channels.published_count,
            channel.published_count,
        )?;
        checked_add(
            &mut totals.channels.completed_count,
            channel.completed_count,
        )?;
        checked_add(
            &mut totals.channels.unavailable_count,
            channel.unavailable_count,
        )?;
        checked_add(
            &mut totals.channels.cancelled_count,
            channel.cancelled_count,
        )?;
    }
    for stream in contribution.producer_streams() {
        checked_add(&mut totals.producer_streams.count, 1)?;
        checked_add(
            &mut totals.producer_streams.accepted_count,
            stream.accepted_count,
        )?;
        checked_add(
            &mut totals.producer_streams.duplicate_count,
            stream.duplicate_count,
        )?;
        checked_add(&mut totals.producer_streams.stale_count, stream.stale_count)?;
        checked_add(
            &mut totals.producer_streams.conflict_count,
            stream.conflict_count,
        )?;
        checked_add(
            &mut totals.producer_streams.resource_limit_count,
            stream.resource_limit_count,
        )?;
    }
    for route in contribution.transport_routes() {
        checked_add(&mut totals.transport_routes.count, 1)?;
        checked_add(&mut totals.transport_routes.sent_count, route.sent_count)?;
        checked_add(&mut totals.transport_routes.sent_bytes, route.sent_bytes)?;
        checked_add(
            &mut totals.transport_routes.retried_count,
            route.retried_count,
        )?;
        checked_add(
            &mut totals.transport_routes.retried_bytes,
            route.retried_bytes,
        )?;
        checked_add(&mut totals.transport_routes.acked_count, route.acked_count)?;
        checked_add(&mut totals.transport_routes.acked_bytes, route.acked_bytes)?;
        checked_add(
            &mut totals.transport_routes.fail_open_count,
            route.fail_open_count,
        )?;
        checked_add(
            &mut totals.transport_routes.fail_open_bytes,
            route.fail_open_bytes,
        )?;
    }
    for consumer in contribution.consumers() {
        let reasons = consumer
            .scan_not_evaluated_reasons
            .as_ref()
            .expect("validated terminal consumer always has scan not-evaluated reasons");
        checked_add(&mut totals.consumers.count, 1)?;
        checked_add(
            &mut totals.consumers.row_evaluations,
            consumer.row_evaluations,
        )?;
        checked_add(&mut totals.consumers.input_rows, consumer.input_rows)?;
        checked_add(&mut totals.consumers.output_rows, consumer.output_rows)?;
        checked_add(
            &mut totals.consumers.scan_evaluated,
            consumer.scan_evaluated,
        )?;
        checked_add(&mut totals.consumers.scan_kept, consumer.scan_kept)?;
        checked_add(&mut totals.consumers.scan_pruned, consumer.scan_pruned)?;
        checked_add(
            &mut totals.consumers.scan_not_evaluated,
            consumer.scan_not_evaluated,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .unit_facts_missing,
            reasons.unit_facts_missing,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .column_facts_missing,
            reasons.column_facts_missing,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .data_type_unsupported,
            reasons.data_type_unsupported,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .predicate_capability_unsupported,
            reasons.predicate_capability_unsupported,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .resource_unavailable,
            reasons.resource_unavailable,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .snapshot_unavailable,
            reasons.snapshot_unavailable,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .snapshot_timed_out,
            reasons.snapshot_timed_out,
        )?;
        checked_add(
            &mut totals
                .consumers
                .scan_not_evaluated_reasons
                .snapshot_not_published,
            reasons.snapshot_not_published,
        )?;
    }
    Ok(())
}

fn checked_add(current: &mut u64, delta: u64) -> Result<(), ()> {
    *current = current.checked_add(delta).ok_or(())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        RuntimeFilterParticipantTerminalTelemetryValue, RuntimeFilterTerminalRollup,
        RuntimeFilterTerminalTotalsTelemetry, RuntimeFilterTerminalTotalsUnavailable,
    };
    use novarocks_proto_codec::lifecycle::QueryTerminalProfileContributionTelemetry;
    use novarocks_proto_models::{common, novarocks};

    /// Pairs one fixture contribution with the backend that released it, which
    /// is exactly what the live rollup consumes.
    fn release_contribution(
        participant_seed: u64,
        telemetry: QueryTerminalProfileContributionTelemetry,
    ) -> (
        novarocks_types::BackendProcessId,
        QueryTerminalProfileContributionTelemetry,
    ) {
        let bytes: [u8; 16] = test_backend_process_id(participant_seed)
            .value
            .try_into()
            .expect("sixteen bytes");
        (
            novarocks_types::BackendProcessId::try_from_bytes(bytes).expect("a legal UUIDv7"),
            telemetry,
        )
    }

    fn rollup_of(
        participants: impl IntoIterator<Item = (u64, QueryTerminalProfileContributionTelemetry)>,
    ) -> RuntimeFilterTerminalRollup {
        let contributions = participants
            .into_iter()
            .map(|(seed, telemetry)| release_contribution(seed, telemetry))
            .collect::<Vec<_>>();
        super::rollup_from_release_contributions(&contributions)
    }

    fn test_backend_process_id(participant_seed: u64) -> novarocks::BackendProcessId {
        let mut value = vec![
            0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1, 0x88, 0xad,
            0x1f, 0x00,
        ];
        value[15] = participant_seed as u8;
        novarocks::BackendProcessId { value }
    }

    fn available_contribution(
        participant_seed: u64,
        channel_id: u32,
        transport_sent_count: u64,
    ) -> QueryTerminalProfileContributionTelemetry {
        QueryTerminalProfileContributionTelemetry::parse(
            novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                        novarocks::QueryTerminalProfileContributionV1 {
                            version: 1,
                            channels: vec![novarocks::QueryTerminalRuntimeFilterChannelV1 {
                                channel_binding_id: 7,
                                channel_id,
                                install_state: novarocks::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed as i32,
                                terminal_state: novarocks::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed as i32,
                                latest_published_logical_version: Some(4),
                                published_count: 4,
                                completed_count: 1,
                                unavailable_count: 0,
                                cancelled_count: 0,
                            }],
                            producer_streams: vec![novarocks::QueryTerminalRuntimeFilterProducerStreamV1 {
                                channel_binding_id: 7,
                                channel_id,
                                producer_fragment_instance_id: Some(common::UniqueId { hi: participant_seed as i64, lo: 41 }),
                                partition_id: 3,
                                latest_accepted_sequence: Some(0),
                                accepted_count: 1,
                                duplicate_count: 2,
                                stale_count: 3,
                                conflict_count: 4,
                                resource_limit_count: 5,
                            }],
                            transport_routes: vec![novarocks::QueryTerminalRuntimeFilterTransportRouteV1 {
                                channel_binding_id: 7,
                                channel_id,
                                route_edge_id: 9,
                                sent_count: transport_sent_count,
                                sent_bytes: 10,
                                retried_count: 1,
                                retried_bytes: 11,
                                acked_count: 1,
                                acked_bytes: 10,
                                fail_open_count: 0,
                                fail_open_bytes: 0,
                            }],
                            consumers: vec![novarocks::QueryTerminalRuntimeFilterConsumerV1 {
                                channel_binding_id: 7,
                                channel_id,
                                consumer_binding_id: 5,
                                fragment_instance_id: Some(common::UniqueId { hi: participant_seed as i64, lo: 51 }),
                                latest_delivered_logical_version: Some(4),
                                latest_applied_logical_version: Some(4),
                                subscription_terminal: novarocks::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed as i32,
                                row_evaluations: 12,
                                input_rows: 100,
                                output_rows: 20,
                                scan_evaluated: 8,
                                scan_kept: 3,
                                scan_pruned: 5,
                                scan_not_evaluated: 7,
                                scan_not_evaluated_reasons: Some(novarocks::QueryTerminalRuntimeFilterScanNotEvaluatedV1 {
                                    unit_facts_missing: 1,
                                    column_facts_missing: 1,
                                    data_type_unsupported: 1,
                                    predicate_capability_unsupported: 1,
                                    resource_unavailable: 1,
                                    snapshot_unavailable: 1,
                                    snapshot_timed_out: 1,
                                    snapshot_not_published: 0,
                                }),
                            }],
                        },
                    ),
                ),
            },
        )
        .expect("terminal profile contribution")
    }

    fn unavailable_contribution(
        _participant_seed: u64,
    ) -> QueryTerminalProfileContributionTelemetry {
        QueryTerminalProfileContributionTelemetry::parse(
            novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                        novarocks::TerminalTelemetryUnavailable {
                            stage: "terminal_capture".to_string(),
                            code: "BUDGET_EXHAUSTED".to_string(),
                        },
                    ),
                ),
            },
        )
        .expect("terminal profile contribution")
    }

    #[test]
    fn runtime_filter_terminal_rollup_preserves_all_sections_and_checked_totals() {
        let rollup = rollup_of([
            (1, available_contribution(1, 101, 1)),
            (2, available_contribution(2, 101, 1)),
        ]);
        assert_eq!(rollup.participants.len(), 2);
        assert_eq!(
            rollup.participants[0].participant.process_id.to_bytes()[15],
            1
        );
        assert_eq!(
            rollup.participants[1].participant.process_id.to_bytes()[15],
            2
        );
        for participant in &rollup.participants {
            let RuntimeFilterParticipantTerminalTelemetryValue::Available(details) =
                &participant.telemetry
            else {
                panic!("fixture telemetry must be available");
            };
            assert_eq!(details.channels.len(), 1);
            assert_eq!(details.producer_streams.len(), 1);
            assert_eq!(details.transport_routes.len(), 1);
            assert_eq!(details.consumers.len(), 1);
        }
        let RuntimeFilterTerminalTotalsTelemetry::Available(totals) = rollup.totals else {
            panic!("all available participants must produce totals");
        };
        assert_eq!(totals.channels.count, 2);
        assert_eq!(totals.channels.published_count, 8);
        assert_eq!(totals.producer_streams.duplicate_count, 4);
        assert_eq!(totals.transport_routes.sent_count, 2);
        assert_eq!(totals.transport_routes.retried_count, 2);
        assert_eq!(totals.consumers.input_rows, 200);
        assert_eq!(totals.consumers.scan_pruned, 10);
        assert_eq!(
            totals
                .consumers
                .scan_not_evaluated_reasons
                .snapshot_timed_out,
            2
        );
    }

    #[test]
    fn runtime_filter_terminal_rollup_keeps_equal_local_ids_from_distinct_participants() {
        let rollup = rollup_of([
            (1, available_contribution(1, 101, 1)),
            (2, available_contribution(2, 101, 1)),
        ]);
        let mut route_prefixes = rollup
            .participants
            .iter()
            .map(|participant| {
                let RuntimeFilterParticipantTerminalTelemetryValue::Available(details) =
                    &participant.telemetry
                else {
                    panic!("fixture telemetry must be available");
                };
                assert_eq!(details.transport_routes[0].route_edge_id, 9);
                participant.participant.process_id.to_bytes()[15]
            })
            .collect::<Vec<_>>();
        route_prefixes.sort_unstable();
        assert_eq!(route_prefixes, vec![1, 2]);
    }

    #[test]
    fn runtime_filter_terminal_rollup_keeps_unavailable_participant_and_hides_partial_totals() {
        let rollup = rollup_of([
            (1, available_contribution(1, 101, 1)),
            (2, unavailable_contribution(2)),
        ]);
        let RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(unavailable) =
            &rollup.participants[1].telemetry
        else {
            panic!("second participant must retain unavailable telemetry");
        };
        assert_eq!(unavailable.stage, "terminal_capture");
        assert_eq!(unavailable.code, "BUDGET_EXHAUSTED");
        assert_eq!(
            rollup.totals,
            RuntimeFilterTerminalTotalsTelemetry::Unavailable(
                RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable
            )
        );
    }

    #[test]
    fn runtime_filter_terminal_rollup_marks_cross_participant_overflow_unavailable() {
        let mut first = available_contribution(1, 101, 1).as_proto().clone();
        let Some(novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
            contribution,
        )) = first.telemetry.as_mut()
        else {
            panic!("fixture profile contribution must be available");
        };
        contribution.channels[0].published_count = u64::MAX;
        let first = QueryTerminalProfileContributionTelemetry::parse(first)
            .expect("maximum valid terminal contribution");
        let rollup = rollup_of([(1, first), (2, available_contribution(2, 102, 1))]);
        assert_eq!(rollup.participants.len(), 2);
        assert_eq!(
            rollup.totals,
            RuntimeFilterTerminalTotalsTelemetry::Unavailable(
                RuntimeFilterTerminalTotalsUnavailable::CounterOverflow
            )
        );
    }

    #[test]
    fn runtime_filter_terminal_rollup_accepts_empty_runtime_filter_sections() {
        let empty = QueryTerminalProfileContributionTelemetry::parse(
            novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                        novarocks::QueryTerminalProfileContributionV1 {
                            version: 1,
                            ..Default::default()
                        },
                    ),
                ),
            },
        )
        .expect("empty terminal contribution");
        let rollup = rollup_of([(1, empty)]);
        let RuntimeFilterTerminalTotalsTelemetry::Available(totals) = rollup.totals else {
            panic!("empty available contribution must have zero totals");
        };
        assert_eq!(totals, Default::default());
    }
}
