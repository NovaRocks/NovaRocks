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

//! Independent fixed-layout component oracles; maximal options are not a native W2 history.

use super::super::original_freeze::{ClientBodyScalars, CurrentBodySource, OriginalFreezeScalars};
use super::*;
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};
use novarocks_query_application::client_connection::ClientConnectionToken;
use novarocks_query_application::{
    api::{ResidentSegmentScalars, RootDataScalars},
    session_control::{SessionToken, StatementToken},
};
use novarocks_result_contract::{RootOutputKind, RootProfileId};
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::{num::NonZeroU64, time::Duration};

fn literal(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn frontend() -> FrontendProcessId {
    FrontendProcessId::try_from_bytes(
        literal("01890f6e7a0071238123456789abcdef")
            .try_into()
            .unwrap(),
    )
    .unwrap()
}
fn facts() -> ControlFacts {
    ControlFacts {
        accepted_peers: 1,
        commands: 1,
        request_wire_bytes: 44,
        response_wire_bytes: 0,
        explicit_stop: false,
        last_request: PrefixSummary::empty(),
        last_response: PrefixSummary::empty(),
    }
}
fn hub() -> MysqlWriteHubSnapshot {
    MysqlWriteHubSnapshot {
        frontend: frontend(),
        used_arm: false,
        stopped: false,
        failure: None,
        original_writer_exited: false,
        gate: None,
        original_freeze: None,
    }
}
fn receipt(second: bool) -> FramingCursor {
    if second {
        FramingCursor {
            phase: WritePhase::Row,
            sequence: 8,
            logical_total: 41,
            logical_written: 19,
            packet_payload_length: 37,
            packet_payload_written: 11,
            header: [37, 0, 0, 8],
            header_written: 2,
            zero_terminal_pending: true,
            committed_wire_bytes: 103,
            rows_completed: 7,
        }
    } else {
        FramingCursor {
            phase: WritePhase::Boundary,
            sequence: 7,
            logical_total: 31,
            logical_written: 17,
            packet_payload_length: 29,
            packet_payload_written: 13,
            header: [29, 0, 0, 7],
            header_written: 4,
            zero_terminal_pending: false,
            committed_wire_bytes: 101,
            rows_completed: 6,
        }
    }
}
const SNAPSHOT_REPLY_LITERAL: &str = "2c00000002020001890f6e7a0071238123456789abcdef01012c00000000000000000000000000000000000000000000";
const MAX_FREEZE: &str = "e402000002020001890f6e7a0071238123456789abcdef01028d000000000000004d00000000000000000100000001040302010500000000000000010403020109000000000000000b00000000000000012222222222222222222222222222222222222222222222222222222222222222020002000000000000000200000000000000a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222030000000000000004000000000000000200000000000000010100071f000000110000001d0000000d0000001d0000070400650000000000000006000000000000000101082900000013000000250000000b000000250000080201670000000000000007000000000000000100010101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000010000000000000011000000000000000102000000000000000200000000000000010000000000000001000000000000000101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000020000000000000011000000000000000103000000000000000200000000000000020000000000000001000000000000000101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000010000000000000011000000000000000102000000000000000200000000000000010100000000000000010101082900000013000000250000000b00000025000008020167000000000000000700000000000000030000000000000001110000000000000016000000070000000000000005000000070000000000000001110000000000000016000000070000000000000005000000070000000000000001020b000000000000000d000000000000001800000000000000";
const SNAPSHOT_REQUEST: &str =
    "28000000020201100001890f6e7a0071238123456789abcdef02100000112233445566778899aabbccddeeff";

#[test]
fn strict_selected_version_never_autodetects_or_falls_back() {
    let mut frame = literal(SNAPSHOT_REQUEST);
    assert!(decode(&frame[4..], WireVersion::V2).is_ok());
    assert!(matches!(
        decode(&frame[4..], WireVersion::V1),
        Err(ControlClass::Fields)
    ));
    frame[4] = 1;
    assert!(decode(&frame[4..], WireVersion::V1).is_ok());
    assert!(matches!(
        decode(&frame[4..], WireVersion::V2),
        Err(ControlClass::Fields)
    ));
    for version in [0, 3, 255] {
        frame[4] = version;
        for selected in [WireVersion::V1, WireVersion::V2] {
            assert!(matches!(
                decode(&frame[4..], selected),
                Err(ControlClass::Fields)
            ));
        }
    }
}
#[test]
fn absent_observation_has_one_explicit_v2_byte_and_preserves_v1_shape() {
    let mut bytes = [0; FRAME_WIRE_CAP];
    let n = encode_reply(&mut bytes, SNAPSHOT, facts(), hub(), WireVersion::V2).unwrap();
    assert_eq!(n, 48);
    assert_eq!(&bytes[..n], literal(SNAPSHOT_REPLY_LITERAL));
    let v1_len = encode_reply(&mut bytes, SNAPSHOT, facts(), hub(), WireVersion::V1).unwrap();
    assert_eq!(v1_len, 47);
    let mut expected = literal(SNAPSHOT_REPLY_LITERAL);
    expected.pop();
    expected[4] = 1;
    expected[..4].copy_from_slice(&(43u32).to_le_bytes());
    assert_eq!(&bytes[..v1_len], expected);
}
#[test]
fn maximal_fixed_projection_matches_independent_literal_744_bytes() {
    let data = |sequence| RootDataScalars {
        root_task: TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(-7, 9), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(2).unwrap(),
            TaskId::new(3).unwrap(),
            BackendProcessId::try_from_bytes(
                literal("01890f6e7a0071238123456789abcdee")
                    .try_into()
                    .unwrap(),
            )
            .unwrap(),
        ),
        profile: RootProfileId::V1,
        kind: RootOutputKind::ClientRows,
        accepted_consumed: 0,
        native_sequence: NonZeroU64::new(sequence).unwrap(),
        body_bytes: 17,
        end_after_data: Some(RootResultEnd {
            sequence: NonZeroU64::new(sequence + 1).unwrap(),
            output_rows: 2,
        }),
    };
    let body = ClientBodyScalars {
        body_bytes: 17,
        before_remaining: 22,
        before_completed_rows: 7,
        after_remaining: 5,
        after_completed_rows: 7,
    };
    let mut snapshot = hub();
    snapshot.used_arm = true;
    snapshot.gate = Some(MysqlWriteGateSnapshot {
        connection: ClientConnectionToken::new(0x01020304, 5).unwrap(),
        statement: Some(StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    ((0x01020304 as u64) << 32) | 9_u64,
                )
                .expect("nonzero fixture session"),
            ),
            11,
        )),
        sql_sha256: Some([0x22; 32]),
        phase: GatePhase::Rows,
        failure: None,
        cut_bytes: 2,
        accepted_prefix_bytes: 2,
        accepted_prefix_sha256: literal(
            "a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222",
        )
        .try_into()
        .unwrap(),
        scalar_inner_polls: 3,
        vectored_inner_polls: 4,
        successful_inner_writes: 2,
        blocked_after_acceptance: true,
        baseline: Some(receipt(false)),
        cancel_receipt: Some(receipt(true)),
        writer_attached: true,
        writer_exited: false,
    });
    snapshot.original_freeze = Some(OriginalFreezeScalars {
        had_resident_window: true,
        slots: [
            Some(ResidentSegmentScalars {
                data: data(1),
                window_sequence: NonZeroU64::new(1).unwrap(),
                completed_rows_by_item: 1,
                has_validated_client_rows: true,
            }),
            Some(ResidentSegmentScalars {
                data: data(2),
                window_sequence: NonZeroU64::new(2).unwrap(),
                completed_rows_by_item: 1,
                has_validated_client_rows: true,
            }),
        ],
        fallback_delivery: Some(data(1)),
        fallback_delivery_rows: Some(1),
        current_source: CurrentBodySource::FrozenDelivering,
        framing: receipt(true),
        buffered_row_bytes: 3,
        current: Some(body),
        next: Some(body),
        tail_complete: true,
        tail_parts: 2,
        tail_part_bytes: [11, 13],
        tail_selected_bytes: 24,
    });
    let mut observed = facts();
    observed.commands = 2;
    observed.request_wire_bytes = 141;
    observed.response_wire_bytes = 77;
    let mut bytes = [0; FRAME_WIRE_CAP];
    let n = encode_reply(&mut bytes, SNAPSHOT, observed, snapshot, WireVersion::V2).unwrap();
    assert_eq!(n, 744);
    assert_eq!(&bytes[..n], literal(MAX_FREEZE));
    snapshot
        .original_freeze
        .as_mut()
        .unwrap()
        .fallback_delivery
        .as_mut()
        .unwrap()
        .kind = RootOutputKind::CountOnly;
    assert!(matches!(
        encode_reply(&mut bytes, SNAPSHOT, observed, snapshot, WireVersion::V2),
        Err(ControlClass::Fields)
    ));
}

// A single actual Unix owner and a directly polled peer. No listener/session/native result is invented.
struct PrivateParent {
    path: PathBuf,
    identity: FileIdentity,
    socket: Option<(PathBuf, FileIdentity)>,
}
impl PrivateParent {
    fn cleanup(&self) -> io::Result<()> {
        let parent = match std::fs::symlink_metadata(&self.path) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !self.identity.matches(&parent)
            || !parent.is_dir()
            || parent.permissions().mode() & 0o7777 != 0o700
        {
            return Err(io::Error::other("component private parent replaced"));
        }
        if let Some((path, identity)) = &self.socket {
            match std::fs::symlink_metadata(path) {
                Ok(meta) if identity.matches(&meta) && meta.file_type().is_socket() => {
                    std::fs::remove_file(path)?
                }
                Ok(_) => return Err(io::Error::other("component socket inode replaced")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        std::fs::remove_dir(&self.path)
    }
}
impl Drop for PrivateParent {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}
#[test]
fn actual_v2_owner_accepts_v2_stop_and_refuses_v1_on_same_original_socket() {
    use std::os::unix::fs::DirBuilderExt;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for wrong_version in [false, true] {
            let directory = std::env::temp_dir().join(format!(
                "nr-v2-{}-{}",
                std::process::id(),
                u8::from(wrong_version)
            ));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&directory)
                .unwrap();
            let mut parent = PrivateParent {
                identity: FileIdentity::of(&std::fs::symlink_metadata(&directory).unwrap()),
                path: directory.clone(),
                socket: None,
            };
            let path = directory.join("gate.sock");
            let nonce = literal("00112233445566778899aabbccddeeff")
                .try_into()
                .unwrap();
            let original_deadline = Instant::now() + Duration::from_secs(3);
            let (mut owner, _hub) =
                UnixMysqlWriteControl::bind_v2(path.clone(), frontend(), nonce, original_deadline)
                    .unwrap();
            parent.socket = Some((path.clone(), owner.path.socket.unwrap()));
            let client = async {
                tokio::time::timeout_at(tokio::time::Instant::from_std(original_deadline), async {
                    let mut peer = UnixStream::connect(&path).await?;
                    let mut request = literal(SNAPSHOT_REQUEST);
                    if wrong_version {
                        request[4] = 1;
                    } else {
                        request[5] = STOP;
                    }
                    peer.write_all(&request).await?;
                    let mut reply = [0u8; 49];
                    let mut length = 0;
                    loop {
                        let n = peer.read(&mut reply[length..]).await?;
                        if n == 0 {
                            break;
                        }
                        length += n;
                        if length == reply.len() {
                            return Err(io::Error::other("component reply exceeded fixed shape"));
                        }
                    }
                    Ok::<_, io::Error>((reply, length))
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "component original clock expired")
                })?
            };
            let (result, client_result) = tokio::join!(owner.run(), client);
            // Always close the original owner and exact empty private parent before propagating any test failure.
            let close = owner.close();
            let original_socket_removed = !path.exists();
            let final_hub = owner.hub_snapshot();
            drop(owner);
            let cleanup = parent.cleanup();
            close.unwrap();
            cleanup.unwrap();
            assert!(original_socket_removed);
            let (reply, length) = client_result.unwrap();
            if wrong_version {
                assert!(matches!(
                    result,
                    Err(ControlExitError {
                        primary: ControlFailure {
                            class: ControlClass::Fields,
                            ..
                        },
                        ..
                    })
                ));
                assert_eq!(length, 0);
                assert!(final_hub.failure.is_some());
            } else {
                let result = result.unwrap();
                assert_eq!(result.facts.accepted_peers, 1);
                assert_eq!(result.facts.commands, 1);
                assert!(result.facts.explicit_stop);
                assert!(result.hub.stopped);
                assert_eq!(length, 48);
                assert_eq!(reply[4..7], [2, STOP, 0]);
                assert_eq!(reply[length - 1], 0);
            }
        }
    });
}
