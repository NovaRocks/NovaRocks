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

use crate::error_mapping::TrinoQueryError;
use bytes::Bytes;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Envelope<'a> {
    id: &'a str,
    info_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_uri: Option<String>,
    pub(crate) stats: Stats<'a>,
    warnings: [(); 0],
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a TrinoQueryError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_type: Option<&'a str>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Stats<'a> {
    state: &'a str,
    queued: bool,
    scheduled: bool,
    nodes: u64,
    total_splits: u64,
    queued_splits: u64,
    running_splits: u64,
    completed_splits: u64,
    cpu_time_millis: u64,
    wall_time_millis: u64,
    pub(crate) queued_time_millis: u64,
    pub(crate) elapsed_time_millis: u64,
    processed_rows: u64,
    processed_bytes: u64,
    physical_input_bytes: u64,
    physical_written_bytes: u64,
    peak_memory_bytes: u64,
    spilled_bytes: u64,
}
pub(crate) fn envelope<'a>(
    id: &'a str,
    base: &str,
    slug: &str,
    next: Option<u64>,
    state: &'a str,
    error: Option<&'a TrinoQueryError>,
    update_type: Option<&'a str>,
) -> Envelope<'a> {
    let next_uri = next.map(|token| format!("{base}/v1/statement/executing/{id}/{slug}/{token}"));
    Envelope {
        id,
        info_uri: format!("{base}/v1/query/{id}"),
        next_uri,
        stats: Stats {
            state,
            queued: state == "QUEUED",
            // The original statement owner does not expose stage-scheduling telemetry.
            scheduled: false,
            nodes: 0,
            total_splits: 0,
            queued_splits: 0,
            running_splits: 0,
            completed_splits: 0,
            cpu_time_millis: 0,
            wall_time_millis: 0,
            queued_time_millis: 0,
            elapsed_time_millis: 0,
            processed_rows: 0,
            processed_bytes: 0,
            physical_input_bytes: 0,
            physical_written_bytes: 0,
            peak_memory_bytes: 0,
            spilled_bytes: 0,
        },
        warnings: [],
        error,
        update_type,
    }
}
/// Borrow typed error text directly; never create a second JSON value tree.
/// Metadata and row bytes remain separate immutable chunks, so replay never reserializes them.
pub(crate) fn page_chunks(
    envelope: Envelope<'_>,
    columns: Option<&Bytes>,
    rows: Option<Bytes>,
) -> Vec<Bytes> {
    let mut prefix =
        crate::json_bytes::exact_json_bytes(&envelope, crate::paging::PAGE_ENVELOPE_RESERVE_BYTES)
            .expect("frozen envelope fits its admitted bound");
    let close = prefix.pop();
    assert_eq!(close, Some(b'}'));
    let mut chunks = vec![Bytes::from(prefix)];
    if let Some(columns) = columns {
        chunks.push(Bytes::from_static(b",\"columns\":"));
        chunks.push(columns.clone());
    }
    if let Some(rows) = rows {
        chunks.push(Bytes::from_static(b",\"data\":["));
        chunks.push(rows);
        chunks.push(Bytes::from_static(b"]}"));
    } else {
        chunks.push(Bytes::from_static(b"}"));
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maximal_frozen_envelope_fits_reserve_without_geometric_growth_or_message_tree() {
        let message = "x".repeat(16 * 1024 - 2); // JSON value quotes are part of the wire bound.
        let error = TrinoQueryError {
            message,
            error_code: i32::MAX,
            error_name: "GENERIC_INSUFFICIENT_RESOURCES",
            error_type: "INSUFFICIENT_RESOURCES",
            error_location: Some(crate::error_mapping::TrinoErrorLocation {
                line_number: u32::MAX,
                column_number: u32::MAX,
            }),
            sql_state: None,
            failure_info: None,
        };
        let mut value = envelope(
            "99991231_235959_99999_zzzzz",
            "http://localhost:65535",
            "ffffffffffffffffffffffffffffffff",
            Some(u64::MAX),
            "FINISHED",
            Some(&error),
            Some("ALTER MATERIALIZED VIEW REFRESH"),
        );
        value.stats.elapsed_time_millis = u64::MAX;
        value.stats.queued_time_millis = u64::MAX;
        let expected = serde_json::to_vec(&value).unwrap();
        assert!(
            expected.len() - serde_json::to_vec(&error.message).unwrap().len()
                <= crate::paging::PAGE_ENVELOPE_METADATA_BYTES
        );
        assert!(expected.len() <= crate::paging::PAGE_ENVELOPE_RESERVE_BYTES);
        let chunks = page_chunks(value, None, None);
        let actual = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        let decoded: serde_json::Value = serde_json::from_slice(&actual).unwrap();
        assert!(decoded.get("partialCancelUri").is_none());
    }
    #[test]
    fn unknown_envelope_growth_is_refused_before_payload_allocation() {
        let source = "x".repeat(crate::paging::PAGE_ENVELOPE_RESERVE_BYTES);
        assert!(
            crate::json_bytes::exact_json_bytes(
                &source,
                crate::paging::PAGE_ENVELOPE_RESERVE_BYTES
            )
            .is_err()
        );
    }
}
