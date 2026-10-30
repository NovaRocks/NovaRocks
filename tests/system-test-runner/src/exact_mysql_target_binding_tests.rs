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

// Pure byte/parser components only; no native FE, socket, process or owner-exit proof.
use super::*;
use std::io::{self, Cursor};
use std::time::Duration;

const FE: &[u8] = b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n";
const BOUND: &[u8] = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND connection_id=71 connection_generation=19 session_connection_id=71 session_key=23 statement_generation=29 sql_sha256=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";

fn frontend() -> FrontendProcessId {
    parse_frontend(&FE[..FE.len() - 1]).expect("valid independently fixed FE UUID")
}
fn digest() -> [u8; 32] {
    std::array::from_fn(|index| index as u8)
}
fn log() -> Vec<u8> {
    [FE, BOUND].concat()
}
fn decode(bytes: &[u8]) -> Result<OriginalTargetBinding> {
    scan(
        &mut Cursor::new(bytes),
        bytes.len() as u64,
        frontend(),
        71,
        digest(),
        Instant::now() + Duration::from_secs(1),
    )
}
fn replace(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let text = std::str::from_utf8(bytes).expect("ASCII fixture");
    assert!(text.contains(from), "negative case must change the fixture");
    text.replacen(from, to, 1).into_bytes()
}

#[test]
fn full_original_binding_preserves_distinct_domains_and_hash() {
    let value = decode(&log()).expect("exact original source accepts");
    assert_eq!(
        value,
        OriginalTargetBinding {
            frontend_process_id: frontend(),
            connection_id: 71,
            connection_generation: 19,
            session_connection_id: 71,
            session_key: 23,
            statement_generation: 29,
            sql_sha256: digest(),
        }
    );
    let mut reverse = BOUND.to_vec();
    reverse.extend_from_slice(FE);
    assert_eq!(
        decode(&reverse).unwrap(),
        value,
        "source owner checks do not infer byte chronology"
    );
    let equal = replace(
        &replace(&log(), "session_key=23", "session_key=19"),
        "statement_generation=29",
        "statement_generation=19",
    );
    let value = decode(&equal).unwrap();
    assert_eq!(
        (
            value.connection_generation,
            value.session_key,
            value.statement_generation
        ),
        (19, 19, 19),
        "independent domains need not have distinct numeric values"
    );
}

#[test]
fn maximum_numeric_widths_accept_without_narrowing_domains() {
    let max = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND connection_id=4294967295 connection_generation=18446744073709551615 session_connection_id=4294967295 session_key=18446744073709551615 statement_generation=18446744073709551615 sql_sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\n";
    let bytes = [FE, max.as_slice()].concat();
    let value = scan(
        &mut Cursor::new(&bytes),
        bytes.len() as u64,
        frontend(),
        u32::MAX,
        [255; 32],
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(value.connection_id, u32::MAX);
    assert_eq!(value.session_connection_id, u32::MAX);
    assert_eq!(value.connection_generation, u64::MAX);
    assert_eq!(value.session_key, u64::MAX);
    assert_eq!(value.statement_generation, u64::MAX);
}

#[test]
fn missing_either_marker_and_duplicate_even_equal_or_foreign_refuse() {
    for bytes in [
        FE.to_vec(),
        BOUND.to_vec(),
        b"unrelated\n".to_vec(),
        Vec::new(),
    ] {
        assert!(decode(&bytes).is_err());
    }
    for extra in [
        FE.to_vec(),
        BOUND.to_vec(),
        replace(BOUND, "connection_id=71", "connection_id=72"),
    ] {
        let mut bytes = log();
        bytes.extend_from_slice(&extra);
        assert!(decode(&bytes).is_err(), "no first/last marker wins policy");
    }
}

#[test]
fn unknown_missing_duplicate_reordered_fields_and_extra_spaces_refuse() {
    for (from, to) in [
        ("connection_id=", "cid="),
        ("connection_generation=19 ", ""),
        ("session_key=23", "session_key=23 session_key=23"),
        (
            "connection_id=71 connection_generation=19",
            "connection_generation=19 connection_id=71",
        ),
        ("connection_id=71 ", "connection_id=71  "),
        ("sql_sha256=", "other_sha256="),
        ("1e1f\n", "1e1f unknown=1\n"),
        ("1e1f\n", "1e1f \n"),
        (
            "NOVAROCKS_EXACT_MYSQL_TARGET_BOUND ",
            "NOVAROCKS_EXACT_MYSQL_TARGET_BOUND",
        ),
    ] {
        assert!(
            decode(&replace(&log(), from, to)).is_err(),
            "changed field contract must refuse"
        );
    }
    for bytes in [
        b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND\n".as_slice(),
        b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE\n".as_slice(),
    ] {
        let mut full = log();
        full.extend_from_slice(bytes);
        assert!(
            decode(&full).is_err(),
            "incomplete extra markers are not unrelated log lines"
        );
    }
}

#[test]
fn zero_signed_leading_zero_nondecimal_and_overflow_refuse() {
    for (field, original) in [
        ("connection_id", "71"),
        ("connection_generation", "19"),
        ("session_connection_id", "71"),
        ("session_key", "23"),
        ("statement_generation", "29"),
    ] {
        let from = format!("{field}={original}");
        for bad in [
            "0",
            "00",
            "01",
            "+1",
            "-1",
            "1a",
            "",
            "18446744073709551616",
            "999999999999999999999999999",
        ] {
            assert!(decode(&replace(&log(), &from, &format!("{field}={bad}"))).is_err());
        }
    }
    for field in ["connection_id", "session_connection_id"] {
        assert!(
            decode(&replace(
                &log(),
                &format!("{field}=71"),
                &format!("{field}=4294967296")
            ))
            .is_err()
        );
    }
}

#[test]
fn session_connection_id_is_separately_parsed_and_must_match() {
    assert!(
        decode(&replace(
            &log(),
            "session_connection_id=71",
            "session_connection_id=72"
        ))
        .is_err()
    );
}

#[test]
fn actual_fe_handshake_and_original_sql_hash_are_required_independent_matches() {
    let bytes = log();
    let mut other = frontend().to_bytes();
    other[15] ^= 1;
    let other = FrontendProcessId::try_from_bytes(other).unwrap();
    assert!(
        scan(
            &mut Cursor::new(&bytes),
            bytes.len() as u64,
            other,
            71,
            digest(),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
    for cid in [0, 72] {
        assert!(
            scan(
                &mut Cursor::new(&bytes),
                bytes.len() as u64,
                frontend(),
                cid,
                digest(),
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
    }
    let mut other_hash = digest();
    other_hash[31] ^= 1;
    assert!(
        scan(
            &mut Cursor::new(&bytes),
            bytes.len() as u64,
            frontend(),
            71,
            other_hash,
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
}

#[test]
fn noncanonical_digest_uuid_and_crlf_refuse() {
    for (from, to) in [
        ("1e1f\n", "1E1F\n"),
        ("1e1f\n", "1e1\n"),
        ("1e1f\n", "1e1ff\n"),
        ("1e1f\n", "1e1g\n"),
        ("1e1f\n", "1e1f\r\n"),
        ("frontend_process_id=", "frontend="),
        (
            "01890f6e-7a00-7123-8123-456789abcdef",
            "01890F6E-7a00-7123-8123-456789abcdef",
        ),
        (
            "01890f6e-7a00-7123-8123-456789abcdef",
            "01890f6e_7a00-7123-8123-456789abcdef",
        ),
        (
            "01890f6e-7a00-7123-8123-456789abcdef",
            "00000000-0000-0000-0000-000000000000",
        ),
        (
            "01890f6e-7a00-7123-8123-456789abcdef",
            "01890f6e-7a00-4123-8123-456789abcdef",
        ),
        ("456789abcdef\n", "456789abcdef unknown=1\n"),
        ("456789abcdef\n", "456789abcdef\r\n"),
    ] {
        assert!(decode(&replace(&log(), from, to)).is_err());
    }
    let mut non_ascii = log();
    let last = non_ascii.len() - 2;
    non_ascii[last] = 255;
    assert!(decode(&non_ascii).is_err());
}

#[test]
fn every_unterminated_or_partial_candidate_refuses_even_after_valid_markers() {
    for marker in [FE, BOUND] {
        for cut in 1..marker.len() {
            let mut bytes = log();
            bytes.extend_from_slice(&marker[..cut]);
            assert!(
                decode(&bytes).is_err(),
                "unfinished candidate cannot be ignored"
            );
            bytes.push(b'\n');
            assert!(
                decode(&bytes).is_err(),
                "newline cannot repair partial candidate"
            );
        }
    }
}

#[test]
fn candidate_line_overflow_refuses_but_unrelated_long_and_binary_lines_are_skipped() {
    for stem in [FE_STEM, BIND_STEM] {
        let mut bytes = log();
        bytes.extend_from_slice(stem);
        bytes.extend(std::iter::repeat_n(b'x', LINE_BYTES + 1));
        bytes.push(b'\n');
        assert!(decode(&bytes).is_err());
    }
    let mut bytes = vec![255; LINE_BYTES * 4];
    bytes.push(b'\n');
    bytes.extend_from_slice(&log());
    bytes.extend_from_slice(b"ordinary unterminated tail");
    assert_eq!(decode(&bytes).unwrap().statement_generation, 29);
}

struct PanicRead;
impl Read for PanicRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        panic!("pre-read bound and clock checks must refuse before any read")
    }
}
#[test]
fn snapshot_cap_and_expired_original_clock_refuse_before_read() {
    assert!(
        scan(
            &mut PanicRead,
            SCAN_BYTES + 1,
            frontend(),
            71,
            digest(),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(scan(&mut PanicRead, 1, frontend(), 71, digest(), Instant::now()).is_err());
    assert!(
        scan(
            &mut PanicRead,
            0,
            frontend(),
            0,
            digest(),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
    let original = log();
    let prefix_len = SCAN_BYTES as usize - original.len();
    let mut exact_cap = vec![b'x'; prefix_len];
    exact_cap[prefix_len - 1] = b'\n';
    exact_cap.extend_from_slice(&original);
    assert!(decode(&exact_cap).is_ok(), "the frozen cap is inclusive");
}

#[test]
fn premature_snapshot_eof_refuses_even_after_all_markers() {
    let bytes = log();
    assert!(
        scan(
            &mut Cursor::new(&bytes),
            bytes.len() as u64 + 1,
            frontend(),
            71,
            digest(),
            Instant::now() + Duration::from_secs(1)
        )
        .is_err()
    );
}

struct Chunked<'a> {
    source: Cursor<&'a [u8]>,
    reads: usize,
}
impl Read for Chunked<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(output.len() <= 512, "caller scratch never grows");
        self.reads += 1;
        let limit = output.len().min(7);
        self.source.read(&mut output[..limit])
    }
}
#[test]
fn partial_reads_across_numeric_uuid_and_hash_boundaries_preserve_facts() {
    let bytes = log();
    let mut reader = Chunked {
        source: Cursor::new(&bytes),
        reads: 0,
    };
    let value = scan(
        &mut reader,
        bytes.len() as u64,
        frontend(),
        71,
        digest(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    assert!(reader.reads > 10);
    assert_eq!(value.sql_sha256, digest());
    assert_eq!(value.statement_generation, 29);
}

struct Failing;
impl Read for Failing {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "component original read failure",
        ))
    }
}
#[test]
fn read_failure_preserves_original_io_error_and_is_not_retried() {
    let error = scan(
        &mut Failing,
        1,
        frontend(),
        71,
        digest(),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<io::Error>().unwrap().kind(),
        io::ErrorKind::Interrupted
    );
}

struct Expiring<'a> {
    source: Cursor<&'a [u8]>,
    deadline: Instant,
}
impl Read for Expiring<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        // A byte-only component seam: return the valid final chunk after its original clock.
        while Instant::now() < self.deadline {
            std::thread::yield_now();
        }
        self.source.read(output)
    }
}
#[test]
fn valid_final_chunk_after_original_deadline_cannot_pass() {
    let bytes = log();
    let deadline = Instant::now() + Duration::from_millis(1);
    let mut reader = Expiring {
        source: Cursor::new(&bytes),
        deadline,
    };
    assert!(
        scan(
            &mut reader,
            bytes.len() as u64,
            frontend(),
            71,
            digest(),
            deadline
        )
        .is_err()
    );
}

#[test]
fn embedded_full_reserved_marker_refuses_before_or_after_valid_pair() {
    for marker in [FE, BOUND] {
        let embedded = [b"noise ".as_slice(), marker].concat();
        for bytes in [
            [embedded.as_slice(), log().as_slice()].concat(),
            [log().as_slice(), embedded.as_slice()].concat(),
        ] {
            assert!(
                decode(&bytes).is_err(),
                "whole snapshot must reject embedded markers"
            );
        }
    }
}

#[test]
fn marker_after_fixed_line_storage_cannot_be_skipped_as_noise() {
    for stem in [FE_STEM, BIND_STEM] {
        let mut bytes = log();
        bytes.extend_from_slice(&[b'x'; LINE_BYTES + 19]);
        bytes.extend_from_slice(stem);
        bytes.push(b'\n');
        assert!(
            decode(&bytes).is_err(),
            "rolling recognition must outlive line storage"
        );
    }
}

#[test]
fn embedded_marker_across_read_chunks_and_at_eof_refuses() {
    for stem in [FE_STEM, BIND_STEM] {
        for newline in [false, true] {
            let mut bytes = log();
            bytes.extend_from_slice(&[b'x'; 509]);
            bytes.extend_from_slice(stem);
            if newline {
                bytes.push(b'\n');
            }
            let mut reader = Chunked {
                source: Cursor::new(bytes.as_slice()),
                reads: 0,
            };
            assert!(
                scan(
                    &mut reader,
                    bytes.len() as u64,
                    frontend(),
                    71,
                    digest(),
                    Instant::now() + Duration::from_secs(1)
                )
                .is_err()
            );
            assert!(
                reader.reads > 10,
                "fixture must cross actual bounded Read chunks"
            );
        }
    }
}

#[test]
fn reserved_recognizer_resets_at_line_boundary_without_rejecting_unrelated_noise() {
    let mut bytes = log();
    bytes.extend_from_slice(b"noise NOVAROCKS_EXACT_MYSQL_TARGET_\nBOUND\n");
    bytes.extend_from_slice(b"noise NOVAROCKS_MEM_1_M07_EXACT_\nMYSQL_FE\n");
    bytes.extend_from_slice(&[0xff; LINE_BYTES + 31]);
    assert!(
        decode(&bytes).is_ok(),
        "unrelated long binary tail remains bounded noise"
    );
}

#[test]
fn complete_reserved_stem_followed_by_noncanonical_bytes_refuses() {
    for stem in [FE_STEM, BIND_STEM] {
        let mut bytes = log();
        bytes.extend_from_slice(b"noise ");
        bytes.extend_from_slice(stem);
        bytes.extend_from_slice(b"_unknown=1\n");
        assert!(
            decode(&bytes).is_err(),
            "reserved ambiguity is never a new authority"
        );
    }
}
