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

//! Native wire projection for a sealed runtime-filter observation.
//!
//! Worker owns the observation and its semantics. This adapter only encodes
//! that immutable snapshot as terminal telemetry for the Native carrier.

use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionV1;
use novarocks_worker::RuntimeFilterContractError;
use novarocks_worker::runtime_filter::observation::{
    RuntimeFilterChannelTerminal, RuntimeFilterConsumerOutcome, RuntimeFilterObservationSnapshot,
};
use tracing::warn;

/// The stage every unavailable runtime-filter contribution names.
pub const RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE: &str = "runtime_filter_terminal_capture";

fn terminal_profile_contribution(
    snapshot: RuntimeFilterObservationSnapshot,
) -> Result<QueryTerminalProfileContributionV1, RuntimeFilterContractError> {
    use novarocks_proto_models::{common, novarocks as wire};
    let channels = snapshot
        .channels()
        .iter()
        .map(|channel| {
            let terminal_state = match channel.terminal() {
                None => wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Open,
                Some(RuntimeFilterChannelTerminal::Completed(_)) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                }
                Some(RuntimeFilterChannelTerminal::Unavailable(_)) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Unavailable
                }
                Some(RuntimeFilterChannelTerminal::Cancelled) => {
                    wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Cancelled
                }
            };
            let identity = channel.identity();
            wire::QueryTerminalRuntimeFilterChannelV1 {
                channel_binding_id: identity.binding_id().get(),
                channel_id: identity.channel_id().get(),
                install_state: wire::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                    as i32,
                terminal_state: terminal_state as i32,
                latest_published_logical_version: channel
                    .latest_published_version()
                    .map(|value| value.get()),
                published_count: channel.published(),
                completed_count: channel.completed(),
                unavailable_count: channel.unavailable(),
                cancelled_count: channel.cancelled(),
            }
        })
        .collect();
    let producer_streams = snapshot
        .producer_streams()
        .iter()
        .map(|stream| {
            let identity = stream.identity();
            let channel = identity.channel();
            let fragment = identity.fragment_instance_id();
            wire::QueryTerminalRuntimeFilterProducerStreamV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                producer_fragment_instance_id: Some(common::UniqueId {
                    hi: fragment.high(),
                    lo: fragment.low(),
                }),
                partition_id: identity.partition_id().get(),
                latest_accepted_sequence: stream.latest_accepted_sequence(),
                accepted_count: stream.accepted(),
                duplicate_count: stream.duplicate(),
                stale_count: stream.stale(),
                conflict_count: stream.conflict(),
                resource_limit_count: stream.resource_limit(),
            }
        })
        .collect();
    let transport_routes = snapshot
        .transport_routes()
        .iter()
        .map(|route| {
            let identity = route.identity();
            let channel = identity.channel();
            wire::QueryTerminalRuntimeFilterTransportRouteV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                route_edge_id: identity.route_edge_id().get(),
                sent_count: route.sent(),
                sent_bytes: route.sent_bytes(),
                retried_count: route.retried(),
                retried_bytes: route.retried_bytes(),
                acked_count: route.acked(),
                acked_bytes: route.acked_bytes(),
                fail_open_count: route.failed_open(),
                fail_open_bytes: route.failed_open_bytes(),
            }
        })
        .collect();
    let consumers = snapshot
        .consumers()
        .iter()
        .map(|consumer| {
            let identity = consumer.identity();
            let subscription_terminal = match consumer.terminal() {
                Some(novarocks_execution::runtime_filter::LiveTerminal::Completed) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Completed
                }
                Some(
                    novarocks_execution::runtime_filter::LiveTerminal::CompletedWithoutArtifact,
                ) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::CompletedWithoutArtifact
                }
                Some(novarocks_execution::runtime_filter::LiveTerminal::Unavailable(_)) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable
                }
                Some(novarocks_execution::runtime_filter::LiveTerminal::Cancelled) => {
                    wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled
                }
                None => match consumer.outcome() {
                    None => wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Pending,
                    Some(RuntimeFilterConsumerOutcome::Acquired) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Acquired
                    }
                    Some(RuntimeFilterConsumerOutcome::TimedOut) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::TimedOut
                    }
                    Some(RuntimeFilterConsumerOutcome::Unavailable(_)) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unavailable
                    }
                    Some(RuntimeFilterConsumerOutcome::Unsupported(_)) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Unsupported
                    }
                    Some(RuntimeFilterConsumerOutcome::Cancelled) => {
                        wire::QueryTerminalRuntimeFilterSubscriptionTerminalV1::Cancelled
                    }
                },
            };
            let channel = identity.channel();
            let fragment = identity.fragment_instance_id();
            wire::QueryTerminalRuntimeFilterConsumerV1 {
                channel_binding_id: channel.binding_id().get(),
                channel_id: channel.channel_id().get(),
                consumer_binding_id: identity.consumer_binding_id().get(),
                fragment_instance_id: Some(common::UniqueId {
                    hi: fragment.high(),
                    lo: fragment.low(),
                }),
                latest_delivered_logical_version: consumer
                    .latest_delivered_version()
                    .map(|value| value.get()),
                latest_applied_logical_version: consumer
                    .latest_applied_version()
                    .map(|value| value.get()),
                subscription_terminal: subscription_terminal as i32,
                row_evaluations: consumer.row_evaluations(),
                input_rows: consumer.row_input(),
                output_rows: consumer.row_output(),
            }
        })
        .collect();
    QueryTerminalProfileContributionV1::seal(wire::QueryTerminalProfileContributionV1 {
        version:
            novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
        channels,
        producer_streams,
        transport_routes,
        consumers,
    })
    .map_err(protocol_contract_error)
}

/// Encodes the one terminal contribution permitted for a sealed Worker
/// observation. An invalid projection remains unavailable on the wire.
pub fn capture_runtime_filter_terminal_profile_contribution(
    snapshot: Option<RuntimeFilterObservationSnapshot>,
    runtime_filter_installed: bool,
) -> Result<
    novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry,
    RuntimeFilterContractError,
> {
    use novarocks_proto_models::novarocks as wire;
    use wire::query_terminal_profile_contribution_telemetry::Telemetry;
    let unavailable = |code: &str| wire::QueryTerminalProfileContributionTelemetry {
        telemetry: Some(Telemetry::Unavailable(wire::TerminalTelemetryUnavailable {
            stage: RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE.to_owned(),
            code: code.to_owned(),
        })),
    };
    let Some(snapshot) = snapshot else {
        if runtime_filter_installed {
            return Ok(unavailable("PARTICIPANT_RELEASED"));
        }
        return Ok(wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(Telemetry::Available(wire::QueryTerminalProfileContributionV1 {
                version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                ..Default::default()
            })),
        });
    };
    if let Some(error) = snapshot.correctness_error() {
        return Err(RuntimeFilterContractError::invalid_contract(format!(
            "runtime-filter observation correctness failure: {error}"
        )));
    }
    match terminal_profile_contribution(snapshot) {
        Ok(contribution) => Ok(wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(Telemetry::Available(contribution.as_proto().clone())),
        }),
        Err(error) => {
            warn!(
                target: "novarocks::runtime_filter",
                error = %error,
                "runtime-filter terminal profile contribution is unavailable"
            );
            Ok(unavailable("CONTRIBUTION_INVALID"))
        }
    }
}

fn protocol_contract_error(
    error: novarocks_proto_codec::ProtocolError,
) -> RuntimeFilterContractError {
    RuntimeFilterContractError::invalid_contract(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_uninstalled_participant_projects_an_empty_available_contribution() {
        let telemetry = capture_runtime_filter_terminal_profile_contribution(None, false)
            .expect("empty contribution projection");
        let Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(contribution)) = telemetry.telemetry else {
            panic!("uninstalled participant must have an available empty contribution");
        };
        assert_eq!(
            contribution.version,
            novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1
        );
        assert!(contribution.channels.is_empty());
    }

    #[test]
    fn released_installed_participant_projects_a_stable_unavailable_stage() {
        let telemetry = capture_runtime_filter_terminal_profile_contribution(None, true)
            .expect("unavailable contribution projection");
        let Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(unavailable)) = telemetry.telemetry else {
            panic!("released participant must have unavailable telemetry");
        };
        assert_eq!(unavailable.stage, RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE);
        assert_eq!(unavailable.code, "PARTICIPANT_RELEASED");
    }
}
