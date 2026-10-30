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

//! Bounded original-FE source observation, independent of adapter or control DTOs.
//! Only the original managed FE log owner may provide this scanner's reader in native use.

use anyhow::{Result, bail, ensure};
use novarocks_types::FrontendProcessId;
use std::io::Read;
use std::time::Instant;

pub(crate) const SCAN_BYTES: u64 = 2 * 1024 * 1024;
const LINE_BYTES: usize = 384;
const FE_STEM: &[u8] = b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE";
const FE_PREFIX: &[u8] = b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE frontend_process_id=";
const BIND_STEM: &[u8] = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND";
const BIND_PREFIX: &[u8] = b"NOVAROCKS_EXACT_MYSQL_TARGET_BOUND ";

/// Original source facts only: no write cursor, cancellation, ACK, or backing authority.
/// Generation fields remain distinct even when the source happens to give equal values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OriginalTargetBinding {
    pub(crate) frontend_process_id: FrontendProcessId,
    pub(crate) connection_id: u32,
    pub(crate) connection_generation: u64,
    pub(crate) session_connection_id: u32,
    pub(crate) session_key: u64,
    pub(crate) statement_generation: u64,
    pub(crate) sql_sha256: [u8; 32],
}

#[derive(Clone, Copy)]
struct RawBound {
    connection_id: u32,
    connection_generation: u64,
    session_connection_id: u32,
    session_key: u64,
    statement_generation: u64,
    sql_sha256: [u8; 32],
}

fn hex(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => bail!("original target marker hex is not canonical lowercase ASCII"),
    }
}

fn parse_frontend(line: &[u8]) -> Result<FrontendProcessId> {
    ensure!(
        line.len() == FE_PREFIX.len() + 36 && line.starts_with(FE_PREFIX),
        "original FE marker has invalid fields or length"
    );
    let uuid = &line[FE_PREFIX.len()..];
    let mut bytes = [0; 16];
    let mut count = 0;
    let mut high = None;
    for (index, byte) in uuid.iter().copied().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            ensure!(
                byte == b'-',
                "original FE marker UUID has invalid punctuation"
            );
        } else if let Some(value) = high.take() {
            bytes[count] = value * 16 + hex(byte)?;
            count += 1;
        } else {
            high = Some(hex(byte)?);
        }
    }
    ensure!(
        count == 16 && high.is_none(),
        "original FE marker UUID width is invalid"
    );
    FrontendProcessId::try_from_bytes(bytes).map_err(anyhow::Error::new)
}

fn number(rest: &mut &[u8], field: &[u8], max: u64) -> Result<u64> {
    let digits = rest
        .strip_prefix(field)
        .ok_or_else(|| anyhow::anyhow!("original target marker field order or name is invalid"))?;
    let end = digits
        .iter()
        .position(|byte| *byte == b' ')
        .ok_or_else(|| anyhow::anyhow!("original target marker numeric field is incomplete"))?;
    let digits_value = &digits[..end];
    ensure!(
        !digits_value.is_empty() && digits_value.len() <= 20,
        "original target marker numeric width is invalid"
    );
    ensure!(
        digits_value[0] != b'0',
        "original target marker numeric value is zero or noncanonical"
    );
    let mut value = 0u64;
    for byte in digits_value {
        ensure!(
            byte.is_ascii_digit(),
            "original target marker numeric value is not decimal ASCII"
        );
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or_else(|| anyhow::anyhow!("original target marker numeric value overflows"))?;
    }
    ensure!(
        value <= max,
        "original target marker numeric value exceeds its domain"
    );
    *rest = &digits[end + 1..];
    Ok(value)
}

fn parse_bound(line: &[u8]) -> Result<RawBound> {
    let mut rest = line
        .strip_prefix(BIND_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("original target marker prefix is incomplete"))?;
    let connection_id = number(&mut rest, b"connection_id=", u64::from(u32::MAX))? as u32;
    let connection_generation = number(&mut rest, b"connection_generation=", u64::MAX)?;
    let session_connection_id =
        number(&mut rest, b"session_connection_id=", u64::from(u32::MAX))? as u32;
    let session_key = number(&mut rest, b"session_key=", u64::MAX)?;
    let statement_generation = number(&mut rest, b"statement_generation=", u64::MAX)?;
    let digest = rest
        .strip_prefix(b"sql_sha256=")
        .ok_or_else(|| anyhow::anyhow!("original target marker digest field is invalid"))?;
    ensure!(
        digest.len() == 64,
        "original target marker digest width or extra fields are invalid"
    );
    let mut sql_sha256 = [0; 32];
    for (output, pair) in sql_sha256.iter_mut().zip(digest.as_chunks::<2>().0) {
        *output = hex(pair[0])? * 16 + hex(pair[1])?;
    }
    ensure!(
        session_connection_id == connection_id,
        "original target marker session and connection IDs differ"
    );
    Ok(RawBound {
        connection_id,
        connection_generation,
        session_connection_id,
        session_key,
        statement_generation,
        sql_sha256,
    })
}

fn candidate(line: &[u8], stem: &[u8]) -> bool {
    !line.is_empty() && (line.starts_with(stem) || stem.starts_with(line))
}

// Match full reserved stems anywhere in the line without retaining log history.
// Prefix fallback uses only the fixed marker literals, including across Read chunks.
fn advance_marker(stem: &[u8], matched: &mut usize, byte: u8) -> bool {
    while *matched != 0 && (*matched == stem.len() || stem[*matched] != byte) {
        let prior = *matched;
        *matched = (1..prior)
            .rev()
            .find(|width| stem[..*width] == stem[prior - *width..prior])
            .unwrap_or(0);
    }
    if stem[*matched] == byte {
        *matched += 1;
    }
    *matched == stem.len()
}

/// Scan exactly one complete snapshot supplied by the original FE owner.
/// Caller provenance: original live FE PID/launch identity, no replacement history,
/// and spawn-time durable-file identity must be checked before and after this visit.
/// Inputs are independently observed FE UUID, handshake CID and original command hash;
/// never derive them from this result or a Unix control/gate DTO. No deadline is minted.
pub(crate) fn scan(
    reader: &mut dyn Read,
    length: u64,
    actual_frontend: FrontendProcessId,
    actual_handshake_connection_id: u32,
    actual_sql_sha256: [u8; 32],
    original_deadline: Instant,
) -> Result<OriginalTargetBinding> {
    ensure!(
        length <= SCAN_BYTES,
        "original FE target-binding log exceeds scan bound"
    );
    ensure!(
        actual_handshake_connection_id != 0,
        "actual MySQL handshake connection ID is zero"
    );
    ensure!(
        Instant::now() < original_deadline,
        "original target-binding clock expired before scan"
    );
    let mut scratch = [0; 512];
    let mut line = [0; LINE_BYTES];
    let mut used = 0usize;
    let mut overflow = false;
    let mut fe_matched = 0usize;
    let mut bound_matched = 0usize;
    let mut reserved_seen = false;
    let mut remaining = length;
    let mut frontend = None;
    let mut bound = None;
    while remaining != 0 {
        ensure!(
            Instant::now() < original_deadline,
            "original target-binding clock expired during scan"
        );
        let limit = scratch.len().min(remaining as usize);
        let read = reader.read(&mut scratch[..limit])?;
        ensure!(
            read != 0,
            "original FE target-binding log snapshot was truncated"
        );
        remaining -= read as u64;
        for byte in &scratch[..read] {
            if *byte == b'\n' {
                let value = &line[..used];
                if reserved_seen {
                    ensure!(
                        !overflow,
                        "original FE reserved marker exceeds fixed line bound"
                    );
                    ensure!(
                        candidate(value, FE_STEM) || candidate(value, BIND_STEM),
                        "original FE reserved marker is embedded in an unrelated line"
                    );
                }
                if candidate(value, FE_STEM) {
                    ensure!(
                        !overflow,
                        "original FE identity marker exceeds fixed line bound"
                    );
                    let value = parse_frontend(value)?;
                    ensure!(
                        frontend.is_none(),
                        "original FE log has duplicate identity markers"
                    );
                    frontend = Some(value);
                } else if candidate(value, BIND_STEM) {
                    ensure!(
                        !overflow,
                        "original target-binding marker exceeds fixed line bound"
                    );
                    let value = parse_bound(value)?;
                    ensure!(
                        bound.is_none(),
                        "original FE log has duplicate target-binding markers"
                    );
                    bound = Some(value);
                }
                used = 0;
                overflow = false;
                fe_matched = 0;
                bound_matched = 0;
                reserved_seen = false;
            } else {
                reserved_seen |= advance_marker(FE_STEM, &mut fe_matched, *byte);
                reserved_seen |= advance_marker(BIND_STEM, &mut bound_matched, *byte);
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
        "original target-binding clock expired after scan"
    );
    let tail = &line[..used];
    ensure!(
        !reserved_seen && !candidate(tail, FE_STEM) && !candidate(tail, BIND_STEM),
        "original FE target-binding marker has no full newline"
    );
    let frontend =
        frontend.ok_or_else(|| anyhow::anyhow!("original FE identity marker is absent"))?;
    let bound = bound
        .ok_or_else(|| anyhow::anyhow!("original successful target-binding marker is absent"))?;
    ensure!(
        frontend == actual_frontend,
        "original target-binding frontend identity differs from actual FE"
    );
    ensure!(
        bound.connection_id == actual_handshake_connection_id,
        "original target-binding connection differs from actual handshake"
    );
    ensure!(
        bound.sql_sha256 == actual_sql_sha256,
        "original target-binding SQL hash differs from original command"
    );
    Ok(OriginalTargetBinding {
        frontend_process_id: frontend,
        connection_id: bound.connection_id,
        connection_generation: bound.connection_generation,
        session_connection_id: bound.session_connection_id,
        session_key: bound.session_key,
        statement_generation: bound.statement_generation,
        sql_sha256: bound.sql_sha256,
    })
}

#[cfg(test)]
#[path = "exact_mysql_target_binding_tests.rs"]
mod tests;
