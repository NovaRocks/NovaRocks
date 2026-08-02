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

use crate::runtime_filter::port::producer::ProducerFailureReason;

const MAGIC: &[u8; 4] = b"NRPU";
const VERSION: u8 = 1;
const ENCODED_LEN: usize = MAGIC.len() + 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProducerFailureCodecError {
    Malformed,
    UnknownVersion,
    UnknownReason,
}

impl std::fmt::Display for ProducerFailureCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid runtime filter producer failure: {self:?}"
        )
    }
}

impl std::error::Error for ProducerFailureCodecError {}

pub fn encode_producer_failure(reason: ProducerFailureReason) -> Vec<u8> {
    let tag = match reason {
        ProducerFailureReason::Cancelled => 1,
        ProducerFailureReason::ExecutionFailed => 2,
        ProducerFailureReason::UpstreamUnavailable => 3,
    };
    let mut encoded = Vec::with_capacity(ENCODED_LEN);
    encoded.extend_from_slice(MAGIC);
    encoded.push(VERSION);
    encoded.push(tag);
    encoded
}

pub fn decode_producer_failure(
    encoded: &[u8],
) -> Result<ProducerFailureReason, ProducerFailureCodecError> {
    if encoded.len() != ENCODED_LEN || &encoded[..MAGIC.len()] != MAGIC {
        return Err(ProducerFailureCodecError::Malformed);
    }
    if encoded[MAGIC.len()] != VERSION {
        return Err(ProducerFailureCodecError::UnknownVersion);
    }
    match encoded[MAGIC.len() + 1] {
        1 => Ok(ProducerFailureReason::Cancelled),
        2 => Ok(ProducerFailureReason::ExecutionFailed),
        3 => Ok(ProducerFailureReason::UpstreamUnavailable),
        _ => Err(ProducerFailureCodecError::UnknownReason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_failure_reason_round_trips_canonically() {
        for reason in [
            ProducerFailureReason::Cancelled,
            ProducerFailureReason::ExecutionFailed,
            ProducerFailureReason::UpstreamUnavailable,
        ] {
            let encoded = encode_producer_failure(reason);
            assert_eq!(decode_producer_failure(&encoded), Ok(reason));
            assert_eq!(
                encode_producer_failure(decode_producer_failure(&encoded).unwrap()),
                encoded
            );
        }
    }

    #[test]
    fn producer_failure_reason_rejects_malformed_payloads() {
        assert_eq!(
            decode_producer_failure(b""),
            Err(ProducerFailureCodecError::Malformed)
        );
        assert_eq!(
            decode_producer_failure(b"NRPU\x02\x01"),
            Err(ProducerFailureCodecError::UnknownVersion)
        );
        assert_eq!(
            decode_producer_failure(b"NRPU\x01\xff"),
            Err(ProducerFailureCodecError::UnknownReason)
        );
    }
}
