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

//! Passive diagnostic samples shared by frontend tests.
//! No runtime, round, transport, authorization or exit evidence is constructed.

/// One available runtime-filter contribution, shaped as a backend's release
/// acknowledgement carries it.
pub(crate) fn fixture_runtime_filter_contribution()
-> novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry {
    use novarocks_proto_models::novarocks as wire;
    novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry::parse(
        wire::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                wire::query_terminal_profile_contribution_telemetry::Telemetry::Available(
                    wire::QueryTerminalProfileContributionV1 {
                        version: novarocks_proto_codec::lifecycle::terminal::QUERY_TERMINAL_PROFILE_CONTRIBUTION_VERSION_V1,
                        channels: vec![wire::QueryTerminalRuntimeFilterChannelV1 {
                            channel_binding_id: 1,
                            channel_id: 7,
                            install_state:
                                wire::QueryTerminalRuntimeFilterChannelInstallStateV1::Installed
                                    as i32,
                            terminal_state:
                                wire::QueryTerminalRuntimeFilterChannelTerminalStateV1::Completed
                                    as i32,
                            latest_published_logical_version: Some(3),
                            published_count: 1,
                            completed_count: 1,
                            unavailable_count: 0,
                            cancelled_count: 0,
                        }],
                        ..Default::default()
                    },
                ),
            ),
        },
    )
    .expect("the fixture contribution satisfies the terminal contract")
}
