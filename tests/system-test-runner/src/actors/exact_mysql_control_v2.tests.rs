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

use super::*;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

// These independent wire literals were assembled from the frozen field table.
// No production/server encoder or DTO serialization generates test expectations.
const SNAPSHOT: &str = "2c00000002020001890f6e7a0071238123456789abcdef01012c00000000000000000000000000000000000000000000";
const FULL_GATE: &str = "0d01000002020001890f6e7a0071238123456789abcdef01028d000000000000004d00000000000000000100000001040302010500000000000000010403020109000000000000000b00000000000000012222222222222222222222222222222222222222222222222222222222222222020002000000000000000200000000000000a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222030000000000000004000000000000000200000000000000010100071f000000110000001d0000000d0000001d0000070400650000000000000006000000000000000101082900000013000000250000000b00000025000008020167000000000000000700000000000000010000";
const ARM_REQUEST: &str = "5d000000020101100001890f6e7a0071238123456789abcdef02100000112233445566778899aabbccddeeff0304000403020104200022222222222222222222222222222222222222222222222222222222222222220508000200000000000000";
const SNAPSHOT_REQUEST: &str =
    "28000000020201100001890f6e7a0071238123456789abcdef02100000112233445566778899aabbccddeeff";

fn literal(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let hex = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(hex, 16).unwrap()
        })
        .collect()
}
fn frontend() -> FrontendProcessId {
    FrontendProcessId::try_from_bytes([
        0x01, 0x89, 0x0f, 0x6e, 0x7a, 0x00, 0x71, 0x23, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef,
    ])
    .unwrap()
}
fn nonce() -> [u8; 16] {
    [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ]
}
fn decode(bytes: &[u8]) -> Result<ReplyFacts, DecodeError> {
    decode_reply(bytes, Opcode::Snapshot, frontend())
}
fn run(future: impl Future<Output = ()>) {
    tokio::runtime::Runtime::new().unwrap().block_on(future);
}
// Poll both borrowed futures on this caller; no task or JoinHandle is created.
async fn concurrently<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = pin!(a);
    let mut b = pin!(b);
    let mut a_done = None;
    let mut b_done = None;
    poll_fn(|cx| {
        if a_done.is_none()
            && let Poll::Ready(value) = a.as_mut().poll(cx)
        {
            a_done = Some(value);
        }
        if b_done.is_none()
            && let Poll::Ready(value) = b.as_mut().poll(cx)
        {
            b_done = Some(value);
        }
        if a_done.is_some() && b_done.is_some() {
            Poll::Ready((a_done.take().unwrap(), b_done.take().unwrap()))
        } else {
            Poll::Pending
        }
    })
    .await
}

#[test]
fn canonical_requests_match_independent_literals_and_fixed_tlv_cardinality() {
    let mut out = [0x7f; FRAME_WIRE_CAP];
    let n = encode_request(
        &mut out,
        frontend(),
        &nonce(),
        Command::Arm {
            connection_id: 0x01020304,
            exact_sql_sha256: [0x22; 32],
            cut_bytes: 2,
        },
    )
    .unwrap();
    assert_eq!(&out[..n], literal(ARM_REQUEST));
    assert_eq!(n, 97);
    assert!(out[n..].iter().all(|v| *v == 0));
    let mut at = 6;
    for (tag, width) in [(1, 16), (2, 16), (3, 4), (4, 32), (5, 8)] {
        assert_eq!(out[at], tag);
        assert_eq!(
            u16::from_le_bytes(out[at + 1..at + 3].try_into().unwrap()),
            width
        );
        at += 3 + width as usize;
    }
    assert_eq!(at, n);
    let n = encode_request(&mut out, frontend(), &nonce(), Command::Snapshot).unwrap();
    assert_eq!(&out[..n], literal(SNAPSHOT_REQUEST));
    assert_eq!(n, 44);
    let mut stop = literal(SNAPSHOT_REQUEST);
    stop[5] = 3;
    let n = encode_request(&mut out, frontend(), &nonce(), Command::Stop).unwrap();
    assert_eq!(&out[..n], stop);
}

#[test]
fn arm_bounds_refuse_invalid_requests_before_wire_copy() {
    let mut out = [0x7f; FRAME_WIRE_CAP];
    for (connection_id, cut_bytes) in [(0, 1), (1, 0), (1, MAX_CUT_BYTES + 1)] {
        assert_eq!(
            encode_request(
                &mut out,
                frontend(),
                &nonce(),
                Command::Arm {
                    connection_id,
                    exact_sql_sha256: [0x22; 32],
                    cut_bytes
                }
            ),
            Err(DecodeError::Fields)
        );
        assert!(out.iter().all(|v| *v == 0));
    }
    assert_eq!(
        encode_request(&mut out, frontend(), &[0; 16], Command::Snapshot),
        Err(DecodeError::Identity)
    );
    assert!(
        encode_request(
            &mut out,
            frontend(),
            &nonce(),
            Command::Arm {
                connection_id: 1,
                exact_sql_sha256: [0x22; 32],
                cut_bytes: MAX_CUT_BYTES
            }
        )
        .is_ok()
    );
}

#[test]
fn literal_replies_preserve_full_original_fixed_facts_without_minted_tokens() {
    let snapshot = decode(&literal(SNAPSHOT)).unwrap();
    assert_eq!(snapshot.frontend, frontend());
    assert_eq!(snapshot.commands, 1);
    assert_eq!(snapshot.request_wire_bytes, 44);
    assert_eq!(snapshot.response_wire_bytes_before_current_reply, 0);
    assert_eq!(snapshot.gate, None);
    let reply = decode(&literal(FULL_GATE)).unwrap();
    assert_eq!(reply.commands, 2);
    assert_eq!(reply.response_wire_bytes_before_current_reply, 77);
    let gate = reply.gate.unwrap();
    assert_eq!(
        gate.connection,
        ConnectionFacts {
            connection_id: 0x01020304,
            generation: 5
        }
    );
    assert_eq!(
        gate.statement,
        Some(StatementFacts {
            connection_id: 0x01020304,
            session_key: 9,
            generation: 11
        })
    );
    assert_eq!(gate.exact_sql_sha256, Some([0x22; 32]));
    assert_eq!(gate.phase, GatePhase::Rows);
    assert_eq!(gate.cut_bytes, 2);
    assert_eq!(gate.accepted_prefix_bytes, 2);
    assert_eq!(
        gate.accepted_prefix_sha256,
        literal("a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222").as_slice()
    );
    assert_eq!(
        (
            gate.scalar_inner_polls,
            gate.vectored_inner_polls,
            gate.successful_inner_writes
        ),
        (3, 4, 2)
    );
    assert_eq!(
        gate.baseline,
        Some(CursorFacts {
            phase: CursorPhase::Boundary,
            sequence: 7,
            logical_total: 31,
            logical_written: 17,
            packet_payload_length: 29,
            packet_payload_written: 13,
            header: [29, 0, 0, 7],
            header_written: 4,
            zero_terminal_pending: false,
            committed_wire_bytes: 101,
            rows_completed: 6
        })
    );
    assert_eq!(
        gate.cancel_receipt,
        Some(CursorFacts {
            phase: CursorPhase::Row,
            sequence: 8,
            logical_total: 41,
            logical_written: 19,
            packet_payload_length: 37,
            packet_payload_written: 11,
            header: [37, 0, 0, 8],
            header_written: 2,
            zero_terminal_pending: true,
            committed_wire_bytes: 103,
            rows_completed: 7
        })
    );
    assert!(gate.blocked_after_acceptance && gate.writer_attached && !gate.writer_exited);
}

#[test]
fn every_literal_byte_cut_and_truncated_option_is_refused() {
    for bytes in [literal(SNAPSHOT), literal(FULL_GATE)] {
        for cut in 0..bytes.len() {
            assert!(decode(&bytes[..cut]).is_err(), "cut={cut}");
        }
    }
    // Keep the declared envelope coherent: the inner reader must refuse every shortened body.
    let full = literal(FULL_GATE);
    for cut in 6..full.len() {
        let mut bytes = full[..cut].to_vec();
        bytes[..4].copy_from_slice(&((cut - 4) as u32).to_le_bytes());
        assert!(decode(&bytes).is_err(), "inner cut={cut}");
    }
}

#[test]
fn strict_version_opcode_status_and_complete_actual_fe_comparison() {
    for (offset, value, error) in [
        (4, 0, DecodeError::Version),
        (4, 1, DecodeError::Version),
        (5, 4, DecodeError::Opcode),
        (5, 1, DecodeError::Opcode),
        (6, 1, DecodeError::Status),
    ] {
        let mut bytes = literal(SNAPSHOT);
        bytes[offset] = value;
        assert_eq!(decode(&bytes), Err(error));
    }
    for change in 0..16 {
        let mut bytes = literal(SNAPSHOT);
        bytes[7 + change] ^= 1;
        assert_eq!(decode(&bytes), Err(DecodeError::Identity));
    }
    let mut version4 = literal(SNAPSHOT);
    version4[13] = 0x41;
    assert_eq!(decode(&version4), Err(DecodeError::Identity));
    let mut nil = literal(SNAPSHOT);
    nil[7..23].fill(0);
    assert_eq!(decode(&nil), Err(DecodeError::Identity));
}

#[test]
fn all_boolean_and_option_discriminants_are_canonical() {
    for offset in [
        41, 42, 43, 45, 46, 59, 80, 187, 188, 212, 229, 253, 270, 271,
    ] {
        for invalid in [2, 0x7f, 0xff] {
            let mut bytes = literal(FULL_GATE);
            bytes[offset] = invalid;
            assert_eq!(decode(&bytes), Err(DecodeError::Boolean), "offset={offset}");
        }
    }
    let mut bytes = literal(SNAPSHOT);
    bytes[46] = 1;
    assert_eq!(decode(&bytes), Err(DecodeError::Truncated));
    // Some -> None does not authorize ignoring the encoded value that follows it.
    let mut bytes = literal(FULL_GATE);
    bytes[46] = 0;
    assert!(decode(&bytes).is_err()); // Remaining bytes cannot become a fake freeze option.
}

#[test]
fn unknown_failure_gate_and_cursor_enums_are_refused() {
    for (offset, max) in [(44, 6), (113, 5), (114, 6), (189, 5), (230, 5)] {
        for valid in 0..=max {
            let mut bytes = literal(FULL_GATE);
            bytes[offset] = valid;
            assert!(decode(&bytes).is_ok(), "offset={offset} value={valid}");
        }
        for invalid in [max + 1, 0xff] {
            let mut bytes = literal(FULL_GATE);
            bytes[offset] = invalid;
            assert_eq!(decode(&bytes), Err(DecodeError::Enum), "offset={offset}");
        }
    }
}

#[test]
fn body_and_full_wire_caps_are_distinct_and_no_trailing_bytes_are_accepted() {
    for length in [0, 1, 4093, u32::MAX] {
        assert_eq!(body_length(length.to_le_bytes()), Err(DecodeError::Length));
    }
    assert_eq!(body_length(2u32.to_le_bytes()), Ok(2));
    assert_eq!(body_length(4092u32.to_le_bytes()), Ok(4092));
    let mut padded = literal(SNAPSHOT);
    padded.push(0);
    assert_eq!(decode(&padded), Err(DecodeError::Trailing));
    padded[..4].copy_from_slice(&44u32.to_le_bytes());
    assert_eq!(decode(&padded), Err(DecodeError::Trailing));
    let mut cap = literal(SNAPSHOT);
    cap.resize(4096, 0);
    cap[..4].copy_from_slice(&4092u32.to_le_bytes());
    assert_eq!(decode(&cap), Err(DecodeError::Length));
    cap.push(0);
    assert_eq!(decode(&cap), Err(DecodeError::Length));
}

#[test]
fn wire_hashes_and_secret_free_debug_have_independent_known_answers() {
    assert_eq!(
        PrefixSummary::empty().sha256,
        literal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").as_slice()
    );
    let gate = decode(&literal(FULL_GATE)).unwrap().gate.unwrap();
    let display = format!("{gate:?}");
    assert!(!display.contains("exact_sql_sha256"));
    assert!(!display.contains("34, 34, 34"));
    let failure = ClientFailure {
        class: ClientClass::Eof,
        stage: ClientStage::ReadBody,
        request: PrefixSummary::empty(),
        response: PrefixSummary::empty(),
        io_kind: None,
        raw_os_error: None,
        cause: None,
    };
    assert!(format!("{failure:?}").len() < 1024);
    assert_eq!(format!("{failure:?}"), format!("{failure}"));
}

#[test]
fn original_pair_stream_returns_complete_literal_reply_and_counts_before_current_reply() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            assert_eq!(request.as_slice(), literal(SNAPSHOT_REQUEST));
            // Split every byte across actual writes; client preserves one response prefix.
            for byte in literal(SNAPSHOT) {
                peer.write_all(&[byte]).await.unwrap();
            }
        };
        let (reply, ()) = concurrently(client.exchange(Command::Snapshot), peer_work).await;
        assert_eq!(reply.unwrap().response_wire_bytes_before_current_reply, 0);
        let (request, response) = client.last_prefixes();
        assert_eq!((request.observed_bytes, response.observed_bytes), (44, 48));
        assert_eq!(response.declared_wire_bytes, Some(48));
        assert_eq!(client.command_count(), 1);
        assert_eq!(
            response.sha256,
            literal("459809d172dff3a677e85af43b467f40e9185258dbfe20aa49e9f54ca6e6e01e").as_slice()
        );
    });
}

#[test]
fn actual_partial_reply_eof_keeps_observed_wire_prefix_length_and_hash() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            peer.write_all(&literal(SNAPSHOT)[..17]).await.unwrap();
            drop(peer);
        };
        let (failure, ()) = concurrently(client.exchange(Command::Snapshot), peer_work).await;
        let failure = failure.unwrap_err();
        assert_eq!(failure.class, ClientClass::Eof);
        assert_eq!(failure.stage, ClientStage::ReadBody);
        assert_eq!(failure.request.observed_bytes, 44);
        assert_eq!(failure.response.observed_bytes, 17);
        assert_eq!(failure.response.declared_wire_bytes, Some(48));
        assert_eq!(
            failure.response.sha256,
            literal("fc342349b81f5889f73e51cac16fdef7712afce8fec0d2f20f663e876d8f0d43").as_slice()
        );
        assert!(client.stream.is_none());
        assert!(
            client
                .request
                .iter()
                .chain(client.response.iter())
                .all(|v| *v == 0)
        );
    });
}

#[test]
fn oversize_header_is_refused_before_any_body_read_with_actual_header_digest() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            peer.write_all(&[0xfd, 0x0f, 0, 0, 0xaa, 0xbb, 0xcc])
                .await
                .unwrap();
        };
        let (failure, ()) = concurrently(client.exchange(Command::Snapshot), peer_work).await;
        let failure = failure.unwrap_err();
        assert_eq!(failure.class, ClientClass::Decode(DecodeError::Length));
        assert_eq!(failure.response.observed_bytes, 4);
        assert_eq!(failure.response.declared_wire_bytes, Some(4097));
        assert_eq!(
            failure.response.sha256,
            literal("fbbfb1f467b8e499ab2bb39ae4b7a83afc4561fa12b5f05112ee28f4d22dfc98").as_slice()
        );
    });
}

#[test]
fn deadline_keeps_actual_partial_header_and_never_refreshes_between_commands() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let original = Instant::now() + Duration::from_millis(150);
        let mut client =
            UnixControlClient::from_stream(stream, frontend(), nonce(), original).unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            peer.write_all(&literal(SNAPSHOT)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            peer.read_exact(&mut request).await.unwrap();
            peer.write_all(&[0x2c, 0]).await.unwrap();
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                original + Duration::from_millis(20),
            ))
            .await;
            let mut late_reply = literal(SNAPSHOT);
            late_reply[24] = 2;
            late_reply[25..33].copy_from_slice(&88u64.to_le_bytes());
            late_reply[33..41].copy_from_slice(&48u64.to_le_bytes());
            // The correct owner already closed; no write error is fabricated as a join result.
            let _late_write = peer.write_all(&late_reply[2..]).await;
        };
        let client_work = async {
            client.exchange(Command::Snapshot).await.unwrap();
            let failure = client.exchange(Command::Snapshot).await.unwrap_err();
            assert_eq!(failure.class, ClientClass::Deadline);
            assert_eq!(failure.stage, ClientStage::ReadHeader);
            assert_eq!(failure.response.observed_bytes, 2);
            assert_eq!(failure.response.declared_wire_bytes, None);
            assert_eq!(
                failure.response.sha256,
                literal("d8fa32dd40b181d8250a4ecde2f2f17bd7bfdf2793278d06ea5afc647f3d5762")
                    .as_slice()
            );
            assert_eq!(client.absolute, original);
            assert_eq!(client.command_count(), 2);
            assert!(client.stream.is_none());
        };
        concurrently(client_work, peer_work).await;
    });
}

#[test]
fn dropping_borrowed_exchange_preserves_owner_and_forbids_stream_reuse() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        {
            let mut exchange = pin!(client.exchange(Command::Snapshot));
            let mut peer_work = pin!(async {
                let mut request = [0; 44];
                peer.read_exact(&mut request).await.unwrap();
                peer.write_all(&[0x2c, 0]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            });
            poll_fn(|cx| {
                assert!(exchange.as_mut().poll(cx).is_pending());
                if peer_work.as_mut().poll(cx).is_ready() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
        assert!(client.stream.is_some());
        assert!(client.in_flight);
        assert_eq!(client.last_response.observed_bytes, 2);
        let failure = client.close_incomplete().unwrap();
        assert_eq!(failure.class, ClientClass::Cancelled);
        assert_eq!(
            failure.response.sha256,
            literal("d8fa32dd40b181d8250a4ecde2f2f17bd7bfdf2793278d06ea5afc647f3d5762").as_slice()
        );
        assert!(client.stream.is_none());
        assert_eq!(
            client.exchange(Command::Snapshot).await.unwrap_err().class,
            ClientClass::State
        );
    });
}

#[test]
fn at_most_sixteen_original_stream_exchanges_and_stop_cannot_restart() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            for count in 1..=16u8 {
                let mut request = [0; 44];
                peer.read_exact(&mut request).await.unwrap();
                let mut reply = literal(SNAPSHOT);
                reply[24] = count;
                reply[25..33].copy_from_slice(&(44 * count as u64).to_le_bytes());
                reply[33..41].copy_from_slice(&(48 * (count as u64 - 1)).to_le_bytes());
                peer.write_all(&reply).await.unwrap();
            }
        };
        let client_work = async {
            for _ in 0..16 {
                client.exchange(Command::Snapshot).await.unwrap();
            }
            assert_eq!(
                client.exchange(Command::Snapshot).await.unwrap_err().class,
                ClientClass::CommandLimit
            );
            assert!(client.stream.is_none());
        };
        concurrently(client_work, peer_work).await;
        // This pair is only a client bound test, not a server terminal/cap or native oracle.
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            assert_eq!(request[5], 3);
            let mut reply = literal(SNAPSHOT);
            reply[5] = 3;
            reply[41] = 1;
            reply[43] = 1;
            peer.write_all(&reply).await.unwrap();
        };
        let (reply, ()) = concurrently(client.exchange(Command::Stop), peer_work).await;
        assert!(reply.unwrap().explicit_stop);
        assert!(client.stream.is_none());
        assert_eq!(
            client.exchange(Command::Snapshot).await.unwrap_err().class,
            ClientClass::State
        );
    });
}

#[test]
fn reply_wrong_counters_cannot_be_used_as_a_successful_exchange() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            let mut reply = literal(SNAPSHOT);
            reply[33] = 47;
            peer.write_all(&reply).await.unwrap();
        };
        let (failure, ()) = concurrently(client.exchange(Command::Snapshot), peer_work).await;
        assert_eq!(failure.unwrap_err().class, ClientClass::Counter);
        assert!(client.stream.is_none());
    });
}

#[test]
fn a_cancelled_borrow_cannot_send_a_second_request_without_explicit_cleanup() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let mut first_request = [0; 44];
        {
            let mut exchange = pin!(client.exchange(Command::Snapshot));
            let mut read_original = pin!(peer.read_exact(&mut first_request));
            poll_fn(|cx| {
                assert!(exchange.as_mut().poll(cx).is_pending());
                match read_original.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        result.unwrap();
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await;
        }
        assert_eq!(first_request.as_slice(), literal(SNAPSHOT_REQUEST));
        assert!(client.stream.is_some());
        let failure = client.exchange(Command::Snapshot).await.unwrap_err();
        assert_eq!(failure.class, ClientClass::State);
        assert_eq!(failure.request.observed_bytes, 44);
        assert_eq!(client.command_count(), 1);
        let mut unexpected = [0; 44];
        assert_eq!(peer.read(&mut unexpected).await.unwrap(), 0);
    });
}

const MAX_FREEZE: &str = "e402000002020001890f6e7a0071238123456789abcdef01028d000000000000004d00000000000000000100000001040302010500000000000000010403020109000000000000000b00000000000000012222222222222222222222222222222222222222222222222222222222222222020002000000000000000200000000000000a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222030000000000000004000000000000000200000000000000010100071f000000110000001d0000000d0000001d0000070400650000000000000006000000000000000101082900000013000000250000000b000000250000080201670000000000000007000000000000000100010101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000010000000000000011000000000000000102000000000000000200000000000000010000000000000001000000000000000101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000020000000000000011000000000000000103000000000000000200000000000000020000000000000001000000000000000101f9ffffffffffffff09000000000000000100000000000000020000000300000001890f6e7a0071238123456789abcdee01000000010000000000000000010000000000000011000000000000000102000000000000000200000000000000010100000000000000010101082900000013000000250000000b00000025000008020167000000000000000700000000000000030000000000000001110000000000000016000000070000000000000005000000070000000000000001110000000000000016000000070000000000000005000000070000000000000001020b000000000000000d000000000000001800000000000000";
const SUFFIX_FLAGS: &[usize] = &[
    272, 273, 274, 352, 385, 386, 464, 497, 498, 576, 593, 603, 652, 685, 718,
];
const ROOT_STARTS: &[usize] = &[275, 387, 499];
const CURRENT_SOURCE_OFFSET: usize = 602;
const TAIL_PARTS_OFFSET: usize = 719;
const TAIL0_OFFSET: usize = 720;
const TAIL1_OFFSET: usize = 728;
const CURRENT_BODY_OFFSET: usize = 653;
const NEXT_BODY_OFFSET: usize = 686;
const FALLBACK_ROWS_OFFSET: usize = 594;
const FRAMING_OPTION_OFFSET: usize = 603;
const WINDOW0_OFFSET: usize = 369;
const WINDOW1_OFFSET: usize = 481;

#[test]
fn v2_maximum_literal_keeps_original_root_fields_and_prefix_hash() {
    let bytes = literal(MAX_FREEZE);
    let reply = decode(&bytes).unwrap();
    assert_eq!(bytes.len(), REPLY_WIRE_MAX);
    assert_eq!(
        reply.gate.unwrap().accepted_prefix_sha256,
        decode(&literal(FULL_GATE))
            .unwrap()
            .gate
            .unwrap()
            .accepted_prefix_sha256
    );
    let facts = reply.original_freeze.unwrap();
    assert_eq!(facts.slots[0].unwrap().data.root.query_high, -7);
    assert_eq!(facts.slots[0].unwrap().data.root.query_low, 9);
    assert_eq!(facts.slots[0].unwrap().data.native_sequence, 1);
    assert_eq!(facts.slots[1].unwrap().window_sequence, 2);
    assert_eq!(
        facts
            .fallback_delivery
            .unwrap()
            .end_after_data
            .unwrap()
            .sequence,
        2
    );
    assert_eq!(facts.fallback_delivery_rows, Some(1));
    assert_eq!(facts.current_source, CurrentSourceFacts::FrozenDelivering);
    assert_eq!(facts.buffered_row_bytes, 3);
    assert_eq!(facts.current.unwrap().before_completed_rows, 7);
    assert_eq!(facts.next.unwrap().after_remaining, 5);
    assert_eq!(facts.tail_part_bytes, [11, 13]);
    assert_eq!(facts.tail_selected_bytes, 24);
}
#[test]
fn v2_suffix_every_outer_and_inner_byte_cut_refuses() {
    let full = literal(MAX_FREEZE);
    for cut in 0..full.len() {
        assert!(decode(&full[..cut]).is_err(), "outer cut={cut}");
        if cut >= 6 {
            let mut short = full[..cut].to_vec();
            short[..4].copy_from_slice(&((cut - 4) as u32).to_le_bytes());
            assert!(decode(&short).is_err(), "inner cut={cut}");
        }
    }
}
#[test]
fn v2_suffix_options_reject_unknown_discriminants_and_absence_is_not_empty() {
    for offset in SUFFIX_FLAGS {
        for value in [2, 0x7f, 0xff] {
            let mut bytes = literal(MAX_FREEZE);
            bytes[*offset] = value;
            assert_eq!(decode(&bytes), Err(DecodeError::Boolean), "offset={offset}");
        }
    }
    let mut bytes = literal(MAX_FREEZE);
    bytes[272] = 0;
    assert_eq!(decode(&bytes), Err(DecodeError::Trailing));
    assert!(
        decode(&literal(FULL_GATE))
            .unwrap()
            .original_freeze
            .is_none()
    );
    let mut bytes = literal(MAX_FREEZE);
    bytes[FRAMING_OPTION_OFFSET] = 0;
    assert_eq!(decode(&bytes), Err(DecodeError::Fields));
}
#[test]
fn v2_root_kind_profile_identity_and_nonzero_sequence_are_strict() {
    for root in ROOT_STARTS {
        for offset in [*root + 48, *root + 52] {
            for value in [0, 2, 0xff] {
                let mut bytes = literal(MAX_FREEZE);
                bytes[offset] = value;
                assert_eq!(decode(&bytes), Err(DecodeError::Enum));
            }
        }
        for (offset, width) in [
            (*root + 16, 8),
            (*root + 24, 4),
            (*root + 28, 4),
            (*root + 32, 16),
        ] {
            let mut bytes = literal(MAX_FREEZE);
            bytes[offset..offset + width].fill(0);
            assert_eq!(decode(&bytes), Err(DecodeError::Identity));
        }
        let mut bytes = literal(MAX_FREEZE);
        bytes[*root + 38] = 0x41;
        assert_eq!(decode(&bytes), Err(DecodeError::Identity));
        for offset in [*root + 61, *root + 69] {
            let mut bytes = literal(MAX_FREEZE);
            bytes[offset..offset + 8].fill(0);
            assert_eq!(decode(&bytes), Err(DecodeError::Fields));
        }
        let mut bytes = literal(MAX_FREEZE);
        bytes[*root + 61..*root + 69].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(decode(&bytes), Err(DecodeError::Fields)); // End cannot wrap.
    }
    for offset in [WINDOW0_OFFSET, WINDOW1_OFFSET] {
        let mut bytes = literal(MAX_FREEZE);
        bytes[offset..offset + 8].fill(0);
        assert_eq!(decode(&bytes), Err(DecodeError::Fields));
    }
}
#[test]
fn v2_lengths_checked_tail_sum_and_trailing_bytes_refuse() {
    for offset in [CURRENT_SOURCE_OFFSET, TAIL_PARTS_OFFSET] {
        let mut bytes = literal(MAX_FREEZE);
        bytes[offset] = 0xff;
        assert!(decode(&bytes).is_err());
    }
    let mut bytes = literal(MAX_FREEZE);
    bytes[TAIL0_OFFSET..TAIL0_OFFSET + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    bytes[TAIL1_OFFSET..TAIL1_OFFSET + 8].copy_from_slice(&1u64.to_le_bytes());
    assert_eq!(decode(&bytes), Err(DecodeError::Fields));
    for offset in [CURRENT_BODY_OFFSET, NEXT_BODY_OFFSET] {
        for value in [0, SEGMENT_BYTES + 1] {
            let mut bytes = literal(MAX_FREEZE);
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
            assert_eq!(decode(&bytes), Err(DecodeError::Fields));
        }
    }
    let mut extra = literal(MAX_FREEZE);
    extra.push(0);
    assert_eq!(decode(&extra), Err(DecodeError::Length));
    let mut trailing = literal(FULL_GATE);
    trailing.push(0);
    let size = (trailing.len() - 4) as u32;
    trailing[..4].copy_from_slice(&size.to_le_bytes());
    assert_eq!(decode(&trailing), Err(DecodeError::Trailing));
}
#[test]
fn v2_preserves_zero_query_bits_and_some_zero_rows_without_repair() {
    let mut bytes = literal(MAX_FREEZE);
    bytes[ROOT_STARTS[0]..ROOT_STARTS[0] + 16].fill(0);
    let root = decode(&bytes).unwrap().original_freeze.unwrap().slots[0]
        .unwrap()
        .data
        .root;
    assert_eq!((root.query_high, root.query_low), (0, 0)); // QueryId permits these bits.
    let mut bytes = literal(MAX_FREEZE);
    bytes[FALLBACK_ROWS_OFFSET..FALLBACK_ROWS_OFFSET + 8].fill(0);
    assert_eq!(
        decode(&bytes)
            .unwrap()
            .original_freeze
            .unwrap()
            .fallback_delivery_rows,
        Some(0)
    );
}
#[test]
fn v2_client_refuses_over744_header_before_body_read_and_keeps_prefix_hash() {
    run(async {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            nonce(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let peer_work = async {
            let mut request = [0; 44];
            peer.read_exact(&mut request).await.unwrap();
            assert_eq!(request.as_slice(), literal(SNAPSHOT_REQUEST));
            peer.write_all(&741u32.to_le_bytes()).await.unwrap();
            // Peer stays open: rejection cannot depend on body EOF.
        };
        let (failure, ()) = concurrently(client.exchange(Command::Snapshot), peer_work).await;
        let failure = failure.unwrap_err();
        assert_eq!(failure.class, ClientClass::Decode(DecodeError::Length));
        assert_eq!(failure.response.observed_bytes, 4);
        assert_eq!(failure.response.declared_wire_bytes, Some(745));
        assert_eq!(
            failure.response.sha256,
            Sha256::digest(741u32.to_le_bytes()).as_slice()
        );
        assert!(client.stream.is_none());
    });
}

#[test]
fn v2_does_not_accept_v1_unknown_version_or_unknown_current_source() {
    for value in [0, 1, 3, 0xff] {
        let mut bytes = literal(SNAPSHOT);
        bytes[4] = value;
        assert_eq!(decode(&bytes), Err(DecodeError::Version));
    }
    let mut bytes = literal(MAX_FREEZE);
    bytes[CURRENT_SOURCE_OFFSET] = 4;
    assert_eq!(decode(&bytes), Err(DecodeError::Enum));
    let mut out = [0; FRAME_WIRE_CAP];
    let n = encode_request(&mut out, frontend(), &nonce(), Command::Snapshot).unwrap();
    assert_eq!(out[4], 2);
    assert_eq!(n, 44);
    assert_eq!(FRAME_WIRE_CAP, 4096);
    assert_eq!(COMMAND_CAP, 16);
}

#[test]
fn v2_independent_maximum_literal_has_frozen_wire_digest() {
    let bytes = literal(MAX_FREEZE);
    assert_eq!(bytes.len(), 744);
    assert_eq!(u32::from_le_bytes(bytes[..4].try_into().unwrap()), 740);
    assert_eq!(
        Sha256::digest(&bytes).as_slice(),
        literal("fa2bf60464f62472d009606c0f019b845ba7a713c5ca1876b98d0a066fa50dd7").as_slice()
    );
}

#[test]
fn actual_io_source_survives_owner_close_and_error_move_without_raw_presentation() {
    use std::error::Error;
    use std::sync::Arc;
    #[derive(Debug)]
    struct OriginalSource(Arc<()>);
    impl std::fmt::Display for OriginalSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("synthetic-raw-source-canary")
        }
    }
    impl Error for OriginalSource {}
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (stream, peer) = UnixStream::pair().unwrap();
        let mut client = UnixControlClient::from_stream(
            stream,
            frontend(),
            [3; 16],
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        let identity = Arc::new(());
        let cause = io::Error::new(io::ErrorKind::BrokenPipe, OriginalSource(identity.clone()));
        let failure = client.failure(ClientClass::Io, Some(cause));
        assert_eq!(failure.io_kind, Some(io::ErrorKind::BrokenPipe));
        assert!(!format!("{failure:?} {failure}").contains("synthetic-raw-source-canary"));
        client.close_incomplete();
        drop(peer);
        let error = anyhow::Error::new(failure);
        let retained = error
            .chain()
            .find_map(|source| source.downcast_ref::<io::Error>())
            .unwrap();
        let original = retained
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalSource>()
            .unwrap();
        assert!(Arc::ptr_eq(&original.0, &identity));
    });
}

#[test]
fn actual_unix_connect_failure_keeps_original_os_error_source() {
    use std::error::Error;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let failure = match UnixControlClient::connect(
            std::path::Path::new("/dev/null/novarocks-exact-source-test"),
            frontend(),
            [3; 16],
            Instant::now() + Duration::from_secs(1),
        )
        .await
        {
            Ok(_) => panic!("non-directory connect unexpectedly succeeded"),
            Err(failure) => failure,
        };
        assert_eq!(failure.class, ClientClass::Io);
        assert_eq!(failure.stage, ClientStage::Connect);
        let source = failure
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap();
        assert_eq!(source.kind(), failure.io_kind.unwrap());
        assert_eq!(source.raw_os_error(), failure.raw_os_error);
        assert!(source.raw_os_error().is_some());
        assert!(!format!("{failure:?} {failure}").contains("novarocks-exact-source-test"));
    });
}
