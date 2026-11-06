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

//! Independent Trino row literals for the retained Native segment properties.
//! No production renderer, candidate output or historical receipt supplies an oracle.
use anyhow::{Result, ensure};
use std::time::Duration;

pub(super) const SEGMENT_BYTES: usize = 1_048_576;
pub(super) const QUERY: Duration = Duration::from_secs(20);
pub(super) const CAPTURE: Duration = Duration::from_secs(5);
pub(super) const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
pub(super) const MAX_SAMPLES: usize = 51;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RetentionShape {
    Normal,
    OneByte,
    MinimumNewRow,
}
impl RetentionShape {
    pub(super) fn sql(self) -> &'static str {
        match self {
            Self::Normal => "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)",
            Self::OneByte => "SELECT REPEAT('x', 1048569) AS payload FROM generate_series(1, 1)",
            Self::MinimumNewRow => {
                "SELECT payload FROM (SELECT 0 AS ordinal, REPEAT('x', 1048568) AS payload UNION ALL SELECT 1 AS ordinal, '' AS payload) ordered_rows ORDER BY ordinal ASC"
            }
        }
    }
    pub(super) fn value_lengths(self) -> &'static [usize] {
        match self {
            Self::Normal => &[1_048_576],
            Self::OneByte => &[1_048_569],
            Self::MinimumNewRow => &[1_048_568, 0],
        }
    }
    pub(super) fn native_bytes(self) -> usize {
        // Each ASCII scalar row is ["xxx"] (four JSON bytes) with the
        // carrier-neutral four-byte little-endian row length prefix.
        self.value_lengths().iter().map(|n| n + 8).sum()
    }
    pub(super) fn second_segment_bytes(self) -> usize {
        match self {
            Self::Normal => 8,
            Self::OneByte => 1,
            Self::MinimumNewRow => 8,
        }
    }
    pub(super) fn independent_segments(self) -> Result<[Vec<u8>; 2]> {
        let mut stream = Vec::with_capacity(self.native_bytes());
        for &length in self.value_lengths() {
            stream.extend(u32::try_from(length + 4)?.to_le_bytes());
            stream.extend_from_slice(b"[\"");
            stream.extend(std::iter::repeat_n(b'x', length));
            stream.extend_from_slice(b"\"]");
        }
        ensure!(
            stream.len() == self.native_bytes(),
            "independent Native row length differs"
        );
        let second = stream.split_off(SEGMENT_BYTES);
        ensure!(
            stream.len() == SEGMENT_BYTES && second.len() == self.second_segment_bytes(),
            "independent Native segment geometry differs"
        );
        Ok([stream, second])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};

    // These replace the old MySQL literal tests in the same System owner;
    // no protocol encoder is called to manufacture the expected bytes.
    #[test]
    fn trino_one_byte_is_the_original_json_row_final_byte() {
        let shape = RetentionShape::OneByte;
        assert_eq!(SEGMENT_BYTES, RootProfileV1::SEGMENT_BYTES);
        let [first, second] = shape.independent_segments().unwrap();
        assert_eq!(second, b"]");
        let profile =
            ClientRowProfile::try_new(SEGMENT_BYTES, RootProfileV1::ROW_PAYLOAD_BYTES).unwrap();
        let mut cursor = ClientRowStreamCursor::new();
        let validated = cursor.validate_body(profile, &first).unwrap();
        let next = validated.after();
        assert_eq!(next.remaining(), 1);
        assert_eq!(next.completed_rows(), 0);
        cursor = next;
        let validated = cursor.validate_body(profile, &second).unwrap();
        let spans = validated.payload_spans().collect::<Vec<_>>();
        assert_eq!(spans.len(), 1);
        assert!(spans[0].starts_row.is_none() && spans[0].completes_row);
        assert_eq!(spans[0].bytes, b"]");
        cursor = validated.after();
        assert_eq!(cursor.completed_rows(), 1);
        cursor.validate_end().unwrap();
    }

    #[test]
    fn trino_minimum_new_string_row_starts_after_the_completed_first_row() {
        let shape = RetentionShape::MinimumNewRow;
        let [first, second] = shape.independent_segments().unwrap();
        // Trino's empty string row is [""] rather than a MySQL length byte.
        assert_eq!(second.as_slice(), &[4, 0, 0, 0, b'[', b'"', b'"', b']']);
        let profile =
            ClientRowProfile::try_new(SEGMENT_BYTES, RootProfileV1::ROW_PAYLOAD_BYTES).unwrap();
        let mut cursor = ClientRowStreamCursor::new();
        for (index, data) in [&first, &second].into_iter().enumerate() {
            let validated = cursor.validate_body(profile, data).unwrap();
            let spans = validated.payload_spans().collect::<Vec<_>>();
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].starts_row.unwrap().get() as usize, data.len() - 4);
            assert!(spans[0].completes_row);
            if index == 1 {
                assert_eq!(spans[0].bytes, b"[\"\"]");
            }
            cursor = validated.after();
            assert_eq!(cursor.remaining(), 0);
            assert_eq!(cursor.completed_rows(), index as u64 + 1);
        }
        cursor.validate_end().unwrap();
    }

    #[test]
    fn normal_trino_literal_keeps_original_sql_and_two_native_segments() {
        let shape = RetentionShape::Normal;
        let [first, second] = shape.independent_segments().unwrap();
        assert_eq!(first.len(), SEGMENT_BYTES);
        assert_eq!(second, [b'x', b'x', b'x', b'x', b'x', b'x', b'"', b']']);
        assert_eq!(shape.native_bytes(), SEGMENT_BYTES + 8);
        assert_eq!(shape.value_lengths(), [SEGMENT_BYTES]);
    }
}
