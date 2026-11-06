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

//! Independent Trino literals over the unchanged Native Root protocol.
//! The caller supplies an independently installed exact Root and retains the
//! actual held original response/actor under this same capture deadline.
use super::result_delivery::root_census;
use super::result_delivery_inputs::{RetentionShape, SEGMENT_BYTES};
use super::result_delivery_root_protocol::probe;
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use novarocks_execution_contract::{TaskIdentity, root_result::RootReadOutcome};
use novarocks_proto_models::{novarocks as proto, result as result_proto};
use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ProtocolMode {
    Retention,
    ZeroAck,
    FinalAckInterference,
    OneByte,
    MinimumNewRow,
}
impl ProtocolMode {
    pub(super) fn shape(self) -> RetentionShape {
        match self {
            Self::OneByte => RetentionShape::OneByte,
            Self::MinimumNewRow => RetentionShape::MinimumNewRow,
            _ => RetentionShape::Normal,
        }
    }
}

pub(super) fn installed_root_protocol(
    context: &mut ScenarioContext,
    backend: usize,
    root: TaskIdentity,
    mode: ProtocolMode,
    deadline: Instant,
    still_held: impl Fn() -> bool,
) -> Result<()> {
    if mode == ProtocolMode::Retention {
        return Ok(());
    }
    let shape = mode.shape();
    let expected = shape.independent_segments()?;
    let task = novarocks_task_codec::identity::encode_task_identity(root);
    let operations: &[&str] = match mode {
        ProtocolMode::ZeroAck => &["ack0", "ack0"],
        ProtocolMode::FinalAckInterference => &[
            "ack0", "ack0", "fetch1", "fetch2", "end3", "replay1", "ack3", "ack3", "retired1",
        ],
        _ => &["ack0", "ack0", "fetch1", "fetch2", "end3"],
    };
    let mut cursor = ClientRowStreamCursor::new();
    let profile = ClientRowProfile::try_new(SEGMENT_BYTES, RootProfileV1::ROW_PAYLOAD_BYTES)?;
    let mut delivered = 0u8;
    let mut proven = 0u64;
    let mut records = Vec::new();
    for (index, operation) in operations.iter().enumerate() {
        ensure!(
            Instant::now() < deadline && still_held(),
            "original held Root phase expired or its actor exited"
        );
        let (wanted, consumed) = match *operation {
            "ack0" => (None, 0),
            "fetch1" | "replay1" => (Some(1), 0),
            "fetch2" => (Some(2), 0),
            "end3" => (Some(3), 0),
            "ack3" => (None, proven),
            "retired1" => (Some(1), proven),
            _ => unreachable!("closed Root protocol operations"),
        };
        let request = root_request(&task, wanted, consumed, 100);
        let (reply, mut observation) = probe(context, backend, &request, proven, deadline)?;
        let matches = match (&reply.outcome, *operation) {
            (RootReadOutcome::AckOnly, "ack0") => reply.accepted_consumed == 0,
            (RootReadOutcome::AckOnly, "ack3") => proven == 3 && reply.accepted_consumed == 3,
            (RootReadOutcome::Retired, "retired1") => proven == 3 && reply.accepted_consumed == 3,
            (RootReadOutcome::Data(data), "fetch1" | "replay1") => {
                data.sequence().get() == 1
                    && data.end_after_data().is_none()
                    && data.body().as_ref() == expected[0]
                    && reply.accepted_consumed == 0
            }
            (RootReadOutcome::Data(data), "fetch2") => {
                data.sequence().get() == 2
                    && data.body().as_ref() == expected[1]
                    && reply.accepted_consumed == 0
                    && match data.end_after_data() {
                        None => true,
                        Some(end) => {
                            matches!(mode, ProtocolMode::OneByte | ProtocolMode::MinimumNewRow)
                                && end.sequence.get() == 3
                                && end.output_rows == shape.value_lengths().len() as u64
                        }
                    }
            }
            (RootReadOutcome::End(end), "end3") => {
                delivered == 3
                    && end.sequence.get() == 3
                    && end.output_rows == shape.value_lengths().len() as u64
                    && reply.accepted_consumed == 0
                    && cursor.validate_end().is_ok()
                    && cursor.completed_rows() == shape.value_lengths().len() as u64
            }
            _ => false,
        };
        ensure!(
            matches,
            "exact Root operation {operation} differs from its independent Trino row literal"
        );
        // Replaying Data1 compares bytes but never feeds or counts its row twice.
        if matches!(*operation, "fetch1" | "fetch2") {
            let RootReadOutcome::Data(data) = &reply.outcome else {
                unreachable!("checked Data");
            };
            let validated = cursor.validate_body(profile, data.body())?;
            let after = validated.after();
            let spans = validated.payload_spans().collect::<Vec<_>>();
            if mode == ProtocolMode::OneByte {
                let continuation = *operation == "fetch2";
                ensure!(
                    after.remaining() == u32::from(!continuation)
                        && after.completed_rows() == u64::from(continuation),
                    "one-byte cursor geometry differs"
                );
                if continuation {
                    ensure!(
                        spans.len() == 1
                            && spans[0].starts_row.is_none()
                            && spans[0].bytes == b"]"
                            && spans[0].completes_row,
                        "one-byte continuation must finish the original JSON row"
                    );
                }
            }
            if mode == ProtocolMode::MinimumNewRow {
                ensure!(
                    spans.len() == 1 && spans[0].completes_row && after.remaining() == 0,
                    "minimum String row did not start/finish independently"
                );
                let second = *operation == "fetch2";
                ensure!(
                    cursor.remaining() == 0
                        && cursor.completed_rows() == u64::from(second)
                        && after.completed_rows() == 1 + u64::from(second)
                        && spans[0].starts_row.map(|n| n.get() as usize)
                            == Some(if second { 4 } else { SEGMENT_BYTES - 4 }),
                    "new row boundary changed to a continuation"
                );
                if second {
                    ensure!(
                        spans[0].bytes == b"[\"\"]",
                        "minimum String row is not empty JSON text"
                    );
                }
            }
            observation["client_row_cursor"] = serde_json::json!({
                "before_remaining":cursor.remaining(), "after_remaining":after.remaining(),
                "completed_rows":after.completed_rows(), "spans":spans.len(),
            });
            cursor = after;
            delivered |= if *operation == "fetch1" { 1 } else { 2 };
        }
        if *operation == "end3" {
            proven = 3;
        }
        observation["operation"] = serde_json::json!(operation);
        observation["proven_delivered_consumed"] = serde_json::json!(proven);
        records.push(observation);
        std::fs::write(
            context.scenario_root().join("installed-root-protocol.json"),
            serde_json::to_vec_pretty(&records)?,
        )?;
        drop(reply);
        let timeout = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(1));
        ensure!(
            !timeout.is_zero(),
            "Root census original phase deadline expired"
        );
        let rows: serde_json::Value = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()?
            .get(format!(
                "http://127.0.0.1:{}/metrics?type=json",
                context.handle().runtime().be[backend].http
            ))
            .send()?
            .error_for_status()?
            .json()?;
        let census = root_census(rows.as_array().context("Root census is not an array")?)?
            .context("Root census is unavailable")?;
        let retired = matches!(*operation, "ack3" | "retired1");
        ensure!(
            census["channels"] == 1
                && census["terminal_task_records"] == 1
                && census["producers_exited"] == 1
                && census["producers_running"] == 0
                && census["ends_published"] == 1
                && census["sealed"] == 0
                && census["ends_acknowledged"] == u64::from(retired)
                && census["data_positions"] == if retired { 0 } else { 2 }
                && census["payload_bytes"]
                    == if retired {
                        0
                    } else {
                        shape.native_bytes() as u64
                    },
            "Root ownership changed outside its proven ACK frontier"
        );
        std::fs::write(
            context
                .scenario_root()
                .join(format!("installed-root-census-{index}.json")),
            serde_json::to_vec_pretty(&census)?,
        )?;
        ensure!(
            Instant::now() < deadline && still_held(),
            "original Root response/actor escaped the held phase"
        );
    }
    Ok(())
}

fn root_request(
    task: &proto::TaskIdentity,
    wanted_sequence: Option<u64>,
    consumed_sequence: u64,
    max_wait_millis: u64,
) -> proto::FetchRootResultRequest {
    proto::FetchRootResultRequest {
        root_task: Some(task.clone()),
        profile_id: 1,
        output_kind: Some(result_proto::RootOutputKind {
            kind: Some(result_proto::root_output_kind::Kind::ClientRows(true)),
        }),
        wanted_sequence,
        consumed_sequence,
        max_wait_millis,
    }
}
#[cfg(test)]
mod request_tests {
    #[test]
    fn retained_root_request_shapes_pass_the_production_decoder_and_zero_wait_is_refused() {
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let root = novarocks_execution_contract::TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let task = novarocks_task_codec::identity::encode_task_identity(root);
        for (wanted, consumed) in [
            (None, 0),
            (Some(1), 0),
            (Some(2), 0),
            (Some(3), 0),
            (None, 3),
            (Some(1), 3),
        ] {
            let request = super::root_request(&task, wanted, consumed, 100);
            assert!(
                novarocks_task_codec::root_result::decode_read(
                    &request,
                    novarocks_proto_codec::FieldPath::root("retained_probe")
                )
                .is_ok()
            );
        }
        assert!(
            novarocks_task_codec::root_result::decode_read(
                &super::root_request(&task, None, 0, 0),
                novarocks_proto_codec::FieldPath::root("invalid_zero_wait")
            )
            .is_err()
        );
    }
}
