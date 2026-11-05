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

//! Original FE stdout observation only; no minted identity or Unix Snapshot fallback.
use anyhow::{Result, bail, ensure};
use novarocks_types::FrontendProcessId;
use std::io::Read;
use std::time::Instant;

pub(crate) const SCAN_BYTES: u64 = 2 * 1024 * 1024;
const STEM: &[u8] =
    novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE.as_bytes();
#[cfg(test)]
const BASE: &[u8] = &const {
    let marker = novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE.as_bytes();
    let suffix = b" ";
    let mut bytes = [0; novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE
        .len()
        + b" ".len()];
    let mut index = 0;
    while index < marker.len() {
        bytes[index] = marker[index];
        index += 1;
    }
    let mut suffix_index = 0;
    while suffix_index < suffix.len() {
        bytes[index + suffix_index] = suffix[suffix_index];
        suffix_index += 1;
    }
    bytes
};
const PREFIX: &[u8] = &const {
    let marker = novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE.as_bytes();
    let suffix = b" frontend_process_id=";
    let mut bytes = [0; novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE
        .len()
        + b" frontend_process_id=".len()];
    let mut index = 0;
    while index < marker.len() {
        bytes[index] = marker[index];
        index += 1;
    }
    let mut suffix_index = 0;
    while suffix_index < suffix.len() {
        bytes[index + suffix_index] = suffix[suffix_index];
        suffix_index += 1;
    }
    bytes
};
const LINE_BYTES: usize = 384;
fn parse(line: &[u8]) -> Result<FrontendProcessId> {
    ensure!(
        line.len() == PREFIX.len() + 36 && line.starts_with(PREFIX),
        "root observation FE marker fields or length are invalid"
    );
    let uuid = &line[PREFIX.len()..];
    let mut bytes = [0; 16];
    let mut count = 0;
    let mut half = None;
    for (index, byte) in uuid.iter().copied().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            ensure!(
                byte == b'-',
                "root observation FE marker UUID punctuation is invalid"
            );
            continue;
        }
        let value = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => bail!("root observation FE marker UUID is not canonical lowercase ASCII"),
        };
        if let Some(high) = half.take() {
            bytes[count] = high * 16 + value;
            count += 1;
        } else {
            half = Some(value);
        }
    }
    ensure!(
        count == 16 && half.is_none(),
        "root observation FE marker UUID width is invalid"
    );
    FrontendProcessId::try_from_bytes(bytes).map_err(anyhow::Error::new)
}
/// Read the complete bounded original log snapshot, with fixed scratch and line storage.
/// Long unrelated lines are skipped; malformed/truncated candidate lines are never repaired.
pub(crate) fn scan(
    reader: &mut dyn Read,
    length: u64,
    original_deadline: Instant,
) -> Result<FrontendProcessId> {
    ensure!(
        length <= SCAN_BYTES,
        "root observation original FE log exceeds scan bound"
    );
    let mut scratch = [0; 512];
    let mut line = [0; LINE_BYTES];
    let mut used = 0usize;
    let mut overflow = false;
    let mut position = 0usize;
    let mut matched = 0usize;
    let mut candidate = false;
    let mut remaining = length;
    let mut found = None;
    while remaining != 0 {
        ensure!(
            Instant::now() < original_deadline,
            "root observation prelaunch clock expired during FE marker scan"
        );
        let limit = scratch.len().min(remaining as usize);
        let read = reader.read(&mut scratch[..limit])?;
        ensure!(
            read != 0,
            "original FE log snapshot was truncated while scanning"
        );
        remaining -= read as u64;
        for byte in &scratch[..read] {
            if *byte == b'\n' {
                ensure!(
                    !(used != 0 && STEM.starts_with(&line[..used])),
                    "neutral FE reserved marker prefix is truncated"
                );
                if candidate {
                    ensure!(
                        !overflow,
                        "root observation FE marker line exceeds fixed bound"
                    );
                    let value = parse(&line[..used])?;
                    ensure!(
                        found.is_none(),
                        "original FE log has multiple root observation FE markers"
                    );
                    found = Some(value);
                }
                used = 0;
                overflow = false;
                position = 0;
                matched = 0;
                candidate = false;
            } else {
                // STEM contains no internal 'NO' prefix, so this streaming matcher
                // requires only one fixed index; it scans past the line buffer.
                if *byte == STEM[matched] {
                    matched += 1;
                } else {
                    matched = usize::from(*byte == STEM[0]);
                }
                if matched == STEM.len() {
                    ensure!(
                        !candidate && position + 1 == STEM.len(),
                        "neutral FE reserved marker is embedded or repeated"
                    );
                    candidate = true;
                    matched = 0;
                }
                position += 1; // bounded by the 2 MiB complete source
                if used < line.len() {
                    line[used] = *byte;
                    used += 1;
                } else {
                    overflow = true;
                }
            }
        }
    }
    ensure!(
        Instant::now() < original_deadline,
        "root observation prelaunch clock expired after FE marker scan"
    );
    let last = &line[..used];
    ensure!(
        !candidate && (used == 0 || !STEM.starts_with(last)),
        "original FE identity marker has no full newline"
    );
    found.ok_or_else(|| anyhow::anyhow!("original FE identity marker is absent"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::time::Duration;
    const LINE: &[u8] = &const {
        let marker =
            novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE.as_bytes();
        let suffix = b" frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n";
        let mut bytes = [0; novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE
            .len()
            + b" frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n".len()];
        let mut index = 0;
        while index < marker.len() {
            bytes[index] = marker[index];
            index += 1;
        }
        let mut suffix_index = 0;
        while suffix_index < suffix.len() {
            bytes[index + suffix_index] = suffix[suffix_index];
            suffix_index += 1;
        }
        bytes
    };
    fn decode(bytes: &[u8]) -> Result<FrontendProcessId> {
        scan(
            &mut Cursor::new(bytes),
            bytes.len() as u64,
            Instant::now() + Duration::from_secs(1),
        )
    }
    #[test]
    fn root_unique_original_marker_accepts_and_ignores_unrelated_long_line() {
        let mut log = vec![b'x'; LINE_BYTES * 4];
        log.push(b'\n');
        log.extend_from_slice(LINE);
        assert_eq!(
            decode(&log).unwrap().to_bytes(),
            [
                1, 137, 15, 110, 122, 0, 113, 35, 129, 35, 69, 103, 137, 171, 205, 239
            ]
        );
    }
    #[test]
    fn missing_duplicate_truncated_wrong_fields_noncanonical_and_nil_refuse() {
        assert!(decode(b"unrelated\n").is_err());
        let mut duplicate = LINE.to_vec();
        duplicate.extend_from_slice(LINE);
        assert!(decode(&duplicate).is_err());
        assert!(decode(&LINE[..LINE.len() - 1]).is_err());
        for cut in 1..BASE.len() {
            assert!(decode(&LINE[..cut]).is_err());
        }
        let mut malformed = LINE.to_vec();
        malformed[PREFIX.len()] = b'A';
        assert!(decode(&malformed).is_err());
        let mut wrong = LINE.to_vec();
        wrong[BASE.len()] = b'x';
        assert!(decode(&wrong).is_err());
        let mut nil = LINE.to_vec();
        for index in PREFIX.len()..nil.len() - 1 {
            if nil[index] != b'-' {
                nil[index] = b'0';
            }
        }
        assert!(decode(&nil).is_err());
        let mut extra = LINE[..LINE.len() - 1].to_vec();
        extra.extend_from_slice(b" extra=1\n");
        assert!(decode(&extra).is_err());
    }
    #[test]
    fn every_candidate_byte_cut_and_unknown_uuid_version_refuse() {
        for cut in 0..LINE.len() {
            assert!(decode(&LINE[..cut]).is_err(), "cut={cut}");
        }
        let mut wrong = LINE.to_vec();
        wrong[PREFIX.len() + 14] = b'4';
        assert!(decode(&wrong).is_err());
        let mut crlf = LINE[..LINE.len() - 1].to_vec();
        crlf.extend_from_slice(b"\r\n");
        assert!(decode(&crlf).is_err());
    }
    #[test]
    fn whole_source_reserved_stem_is_never_ignored_after_a_valid_marker() {
        for offset in [1, 383, 384, 511, 512, 2048] {
            let mut log = LINE.to_vec();
            log.extend(std::iter::repeat_n(b'x', offset));
            log.extend_from_slice(LINE);
            assert!(decode(&log).is_err(), "embedded offset={offset}");
        }
        for suffix in [
            STEM,
            BASE,
            b"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FEbad=1".as_slice(),
        ] {
            let mut log = LINE.to_vec();
            log.extend_from_slice(suffix);
            log.push(b'\n');
            assert!(decode(&log).is_err());
        }
        for cut in 1..LINE.len() {
            let mut log = LINE.to_vec();
            log.extend_from_slice(&LINE[..cut]);
            assert!(decode(&log).is_err(), "appended candidate cut={cut}");
        }
    }
    #[test]
    fn neutral_source_does_not_accept_an_exact_gate_marker() {
        let old = &const {
            let marker =
                novarocks_failpoint::observation_marker::MEM_1_M07_EXACT_MYSQL_FE.as_bytes();
            let suffix = b" frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n";
            let mut bytes = [0; novarocks_failpoint::observation_marker::MEM_1_M07_EXACT_MYSQL_FE
                .len()
                + b" frontend_process_id=01890f6e-7a00-7123-8123-456789abcdef\n".len()];
            let mut index = 0;
            while index < marker.len() {
                bytes[index] = marker[index];
                index += 1;
            }
            let mut suffix_index = 0;
            while suffix_index < suffix.len() {
                bytes[index + suffix_index] = suffix[suffix_index];
                suffix_index += 1;
            }
            bytes
        };
        assert!(decode(old).is_err());
    }
    #[test]
    fn fixed_scan_bound_and_original_expired_clock_refuse_before_any_read() {
        struct PanicRead;
        impl Read for PanicRead {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                panic!("must not read");
            }
        }
        assert!(
            scan(
                &mut PanicRead,
                SCAN_BYTES + 1,
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
        assert!(scan(&mut PanicRead, 1, Instant::now()).is_err());
        assert!(
            scan(
                &mut Cursor::new(LINE),
                LINE.len() as u64 + 1,
                Instant::now() + Duration::from_secs(1)
            )
            .is_err()
        );
    }
}
