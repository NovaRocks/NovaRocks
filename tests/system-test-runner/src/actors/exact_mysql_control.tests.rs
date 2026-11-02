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
const SNAPSHOT: &str = "2b00000001020001890f6e7a0071238123456789abcdef01012c000000000000000000000000000000000000000000";
const FULL_GATE: &str = "0c01000001020001890f6e7a0071238123456789abcdef01028d000000000000004d00000000000000000100000001040302010500000000000000010403020109000000000000000b00000000000000012222222222222222222222222222222222222222222222222222222222222222020002000000000000000200000000000000a12871fee210fb8619291eaea194581cbd2531e4b23759d225f6806923f63222030000000000000004000000000000000200000000000000010100071f000000110000001d0000000d0000001d0000070400650000000000000006000000000000000101082900000013000000250000000b000000250000080201670000000000000007000000000000000100";
const ARM_REQUEST: &str = "5d000000010101100001890f6e7a0071238123456789abcdef02100000112233445566778899aabbccddeeff0304000403020104200022222222222222222222222222222222222222222222222222222222222222220508000200000000000000";
const SNAPSHOT_REQUEST: &str =
    "28000000010201100001890f6e7a0071238123456789abcdef02100000112233445566778899aabbccddeeff";

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
        (4, 2, DecodeError::Version),
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
    assert_eq!(decode(&bytes), Err(DecodeError::Trailing));
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
    assert_eq!(decode(&cap), Err(DecodeError::Trailing));
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
        assert_eq!((request.observed_bytes, response.observed_bytes), (44, 47));
        assert_eq!(response.declared_wire_bytes, Some(47));
        assert_eq!(client.command_count(), 1);
        assert_eq!(
            response.sha256,
            literal("3f0fec4814874af93775a9071657f1a3cc881f12d24088b19dcd30f4bbe2278a").as_slice()
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
        assert_eq!(failure.response.declared_wire_bytes, Some(47));
        assert_eq!(
            failure.response.sha256,
            literal("cd3c098b6cb53c04c8865500bdd257a38a66c662a438f651b538e65757e62c28").as_slice()
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
            peer.write_all(&[0x2b, 0]).await.unwrap();
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                original + Duration::from_millis(20),
            ))
            .await;
            let mut late_reply = literal(SNAPSHOT);
            late_reply[24] = 2;
            late_reply[25..33].copy_from_slice(&88u64.to_le_bytes());
            late_reply[33..41].copy_from_slice(&47u64.to_le_bytes());
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
                literal("206014a4266c2082b9433011fb2991059f972d570a8b3c976e5b2b8dfcfbfc8d")
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
                peer.write_all(&[0x2b, 0]).await.unwrap();
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
            literal("206014a4266c2082b9433011fb2991059f972d570a8b3c976e5b2b8dfcfbfc8d").as_slice()
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
                reply[33..41].copy_from_slice(&(47 * (count as u64 - 1)).to_le_bytes());
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
