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
//! The lifecycle ingress and [`super::terminal_set::QueryTerminalSet`] own
//! participant admission and de-duplication.  This module deliberately only
//! reads that complete set: it preserves each participant's validated wire
//! facts and computes diagnostic query totals without introducing another
//! lifecycle owner.

use novarocks_proto::novarocks;

use super::terminal_set::QueryTerminalSet;

/// A deterministic, query-scoped projection of Runtime Filter terminal facts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeFilterTerminalRollup {
    /// Ordered by `(backend_id, start_epoch)`, as provided by `QueryTerminalSet`.
    pub(crate) participants: Vec<RuntimeFilterParticipantTerminalTelemetry>,
    pub(crate) totals: RuntimeFilterTerminalTotalsTelemetry,
}

/// Participant identity prefixes every owner-local Runtime Filter detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RuntimeFilterTerminalParticipant {
    pub(crate) backend_id: u64,
    pub(crate) start_epoch: u64,
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

pub(crate) fn rollup(set: &QueryTerminalSet) -> RuntimeFilterTerminalRollup {
    let mut participants = Vec::with_capacity(set.snapshots().len());
    let mut totals = RuntimeFilterTerminalTotals::default();
    let mut totals_unavailable = None;

    for snapshot in set.snapshots() {
        let backend = snapshot.backend();
        let participant = RuntimeFilterTerminalParticipant {
            backend_id: backend.backend_id(),
            start_epoch: backend.start_epoch(),
        };
        let telemetry = snapshot.profile_contribution_telemetry();

        let telemetry = if let Some(contribution) = telemetry.available() {
            if totals_unavailable.is_none()
                && add_contribution_totals(&mut totals, &contribution).is_err()
            {
                totals_unavailable = Some(RuntimeFilterTerminalTotalsUnavailable::CounterOverflow);
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
            totals_unavailable =
                Some(RuntimeFilterTerminalTotalsUnavailable::ParticipantTelemetryUnavailable);
            RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(
                RuntimeFilterTerminalUnavailable {
                    stage: unavailable.stage().to_owned(),
                    code: unavailable.code().to_owned(),
                },
            )
        };

        participants.push(RuntimeFilterParticipantTerminalTelemetry {
            participant,
            telemetry,
        });
    }

    RuntimeFilterTerminalRollup {
        participants,
        totals: match totals_unavailable {
            Some(reason) => RuntimeFilterTerminalTotalsTelemetry::Unavailable(reason),
            None => RuntimeFilterTerminalTotalsTelemetry::Available(totals),
        },
    }
}

fn add_contribution_totals(
    totals: &mut RuntimeFilterTerminalTotals,
    contribution: &novarocks_proto::lifecycle::QueryTerminalProfileContributionV1,
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
        RuntimeFilterParticipantTerminalTelemetryValue, RuntimeFilterTerminalTotalsTelemetry,
        RuntimeFilterTerminalTotalsUnavailable,
    };
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::terminal_set::QueryTerminalSet;
    use novarocks_proto::lifecycle::{AttemptId, QueryExecutionId, QueryTerminalSnapshot};
    use novarocks_proto::{common, novarocks};

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(10, 20), AttemptId::new(1).expect("attempt id"))
            .expect("execution id")
    }

    fn available_snapshot(
        backend_id: u64,
        channel_id: u32,
        transport_sent_count: u64,
    ) -> QueryTerminalSnapshot {
        QueryTerminalSnapshot::seal(novarocks::QueryTerminalSnapshot {
            version: 1,
            execution_id: Some(execution_id().into()),
            backend: Some(novarocks::ParticipantBackendIdentity {
                backend_id,
                endpoint: Some(novarocks::QueryControlEndpoint {
                    host: "127.0.0.1".to_string(),
                    port: 19_000 + backend_id as u32,
                }),
                start_epoch: 1,
            }),
            init_digest: vec![backend_id as u8; 32],
            profile_contribution: Some(novarocks::QueryTerminalProfileContributionTelemetry {
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
                                producer_fragment_instance_id: Some(common::UniqueId { hi: backend_id as i64, lo: 41 }),
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
                                fragment_instance_id: Some(common::UniqueId { hi: backend_id as i64, lo: 51 }),
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
            }),
            ..Default::default()
        })
        .expect("terminal snapshot")
    }

    fn unavailable_snapshot(backend_id: u64) -> QueryTerminalSnapshot {
        QueryTerminalSnapshot::seal(novarocks::QueryTerminalSnapshot {
            version: 1,
            execution_id: Some(execution_id().into()),
            backend: Some(novarocks::ParticipantBackendIdentity {
                backend_id,
                endpoint: Some(novarocks::QueryControlEndpoint {
                    host: "127.0.0.1".to_string(),
                    port: 19_000 + backend_id as u32,
                }),
                start_epoch: 1,
            }),
            init_digest: vec![backend_id as u8; 32],
            profile_contribution: Some(novarocks::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                        novarocks::TerminalTelemetryUnavailable {
                            stage: "terminal_capture".to_string(),
                            code: "BUDGET_EXHAUSTED".to_string(),
                        },
                    ),
                ),
            }),
            ..Default::default()
        })
        .expect("terminal snapshot")
    }

    #[test]
    fn runtime_filter_terminal_rollup_preserves_all_sections_and_checked_totals() {
        let set = QueryTerminalSet::new(vec![
            available_snapshot(2, 101, 1),
            available_snapshot(1, 101, 1),
        ])
        .expect("terminal set");

        let rollup = set.runtime_filter_terminal_rollup();
        assert_eq!(rollup.participants.len(), 2);
        assert_eq!(rollup.participants[0].participant.backend_id, 1);
        assert_eq!(rollup.participants[1].participant.backend_id, 2);
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
        let set = QueryTerminalSet::new(vec![
            available_snapshot(1, 101, 1),
            available_snapshot(2, 101, 1),
        ])
        .expect("terminal set");

        let rollup = set.runtime_filter_terminal_rollup();
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
                participant.participant.backend_id
            })
            .collect::<Vec<_>>();
        route_prefixes.sort_unstable();
        assert_eq!(route_prefixes, vec![1, 2]);
    }

    #[test]
    fn runtime_filter_terminal_rollup_keeps_unavailable_participant_and_hides_partial_totals() {
        let set =
            QueryTerminalSet::new(vec![available_snapshot(1, 101, 1), unavailable_snapshot(2)])
                .expect("terminal set");

        let rollup = set.runtime_filter_terminal_rollup();
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
        let mut first = available_snapshot(1, 101, 1).as_proto().clone();
        let Some(novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
            contribution,
        )) = first
            .profile_contribution
            .as_mut()
            .and_then(|telemetry| telemetry.telemetry.as_mut())
        else {
            panic!("fixture profile contribution must be available");
        };
        contribution.channels[0].published_count = u64::MAX;
        let first = QueryTerminalSnapshot::seal(first).expect("maximum valid terminal snapshot");
        let set = QueryTerminalSet::new(vec![first, available_snapshot(2, 102, 1)])
            .expect("terminal set");

        let rollup = set.runtime_filter_terminal_rollup();
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
        let mut snapshot = available_snapshot(1, 101, 1).as_proto().clone();
        snapshot
            .profile_contribution
            .as_mut()
            .expect("profile contribution")
            .telemetry = Some(
            novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                novarocks::QueryTerminalProfileContributionV1 {
                    version: 1,
                    ..Default::default()
                },
            ),
        );
        let snapshot = QueryTerminalSnapshot::seal(snapshot).expect("empty terminal snapshot");
        let set = QueryTerminalSet::new(vec![snapshot]).expect("terminal set");

        let rollup = set.runtime_filter_terminal_rollup();
        let RuntimeFilterTerminalTotalsTelemetry::Available(totals) = rollup.totals else {
            panic!("empty available contribution must have zero totals");
        };
        assert_eq!(totals, Default::default());
    }

    #[test]
    fn runtime_filter_terminal_rollup_leaves_duplicate_participant_rejection_to_terminal_set() {
        let snapshot = available_snapshot(1, 101, 1);
        let error = QueryTerminalSet::new(vec![snapshot.clone(), snapshot])
            .expect_err("terminal set owns duplicate participant rejection");
        assert!(error.to_string().contains("duplicate participant identity"));
    }
}
