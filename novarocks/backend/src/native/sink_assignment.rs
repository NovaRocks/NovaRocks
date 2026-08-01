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

//! Backend-owned native sink-assignment DTO decoding.

use novarocks::common::types::UniqueId;
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks::runtime::fragment::instance::FragmentSinkAssignment;
use novarocks_protocol::{novarocks as proto, plan};

pub(crate) fn decode_fragment_sink_assignment(
    sink: &plan::DataSink,
    instance: &proto::InstanceParams,
) -> Result<FragmentSinkAssignment, ProtocolError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("kind"),
            "native PlanFragment sink requires kind",
        )
    })?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: decode_instance_destinations(&instance.destinations)?,
            sender_id: None,
        }),
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            let groups = grouped
                .destinations
                .iter()
                .enumerate()
                .map(|(index, group)| {
                    decode_stream_destination_list(
                        group,
                        path.clone()
                            .field("multi_cast_data_stream")
                            .field("destinations")
                            .index(index),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::ChangeStreamRouter(router) => {
            let groups = router
                .branches
                .iter()
                .enumerate()
                .map(|(index, branch)| {
                    let group_path = path
                        .clone()
                        .field("change_stream_router")
                        .field("branches")
                        .index(index)
                        .field("destinations");
                    let group = branch.destinations.as_ref().ok_or_else(|| {
                        missing(
                            group_path.clone(),
                            "native change-stream branch requires destinations",
                        )
                    })?;
                    decode_stream_destination_list(group, group_path)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::Result(_)
        | plan::data_sink::Kind::Noop(_)
        | plan::data_sink::Kind::ConnectorWrite(_) => {
            if instance.destinations.is_empty() {
                Ok(FragmentSinkAssignment::None)
            } else {
                Ok(FragmentSinkAssignment::StreamDestinations {
                    destinations: decode_instance_destinations(&instance.destinations)?,
                    sender_id: None,
                })
            }
        }
    }
}

fn decode_instance_destinations(
    src: &[proto::Destination],
) -> Result<Vec<FragmentDestination>, ProtocolError> {
    src.iter()
        .enumerate()
        .map(|(index, destination)| {
            let path = FieldPath::root("instance_params")
                .field("destinations")
                .index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("finst_id"),
                    "native Destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint)
                    .map_err(|error| invalid_value(path.field("endpoint"), error))?,
            ))
        })
        .collect()
}

fn decode_stream_destination_list(
    group: &plan::StreamDestinationList,
    path: FieldPath,
) -> Result<Vec<FragmentDestination>, ProtocolError> {
    group
        .destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let destination_path = path.clone().field("destinations").index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                missing(
                    destination_path.clone().field("finst_id"),
                    "native stream destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                UniqueId {
                    hi: finst_id.hi,
                    lo: finst_id.lo,
                },
                RuntimeEndpoint::parse(&destination.endpoint)
                    .map_err(|error| invalid_value(destination_path.field("endpoint"), error))?,
            ))
        })
        .collect()
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(
        ProtocolFamily::Native,
        path,
        ProtocolErrorKind::MissingField,
        detail.into(),
    )
}

fn invalid_value(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(
        ProtocolFamily::Native,
        path,
        ProtocolErrorKind::InvalidValue,
        detail.into(),
    )
}

#[cfg(test)]
mod tests {
    use super::decode_fragment_sink_assignment;
    use novarocks_protocol::{novarocks as proto, plan};

    #[test]
    fn stream_destination_missing_id_preserves_error_text() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(
                    plan::DataStreamSink::default(),
                )),
            },
            &proto::InstanceParams {
                destinations: vec![proto::Destination::default()],
                ..Default::default()
            },
        )
        .expect_err("destination id is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at instance_params.destinations[0].finst_id (missing field): native Destination requires finst_id"
        );
    }

    #[test]
    fn multicast_destination_missing_id_preserves_error_text() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                    plan::MultiCastDataStreamSink {
                        destinations: vec![plan::StreamDestinationList {
                            destinations: vec![plan::StreamDestination::default()],
                        }],
                        ..Default::default()
                    },
                )),
            },
            &proto::InstanceParams::default(),
        )
        .expect_err("stream destination id is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.sink.multi_cast_data_stream.destinations[0].destinations[0].finst_id (missing field): native stream destination requires finst_id"
        );
    }
}
