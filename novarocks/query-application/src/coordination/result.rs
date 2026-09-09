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

use std::fmt;

/// Result of delivering work to one frozen destination.
///
/// These facts come from exchange/result delivery rather than a Worker
/// operation receipt. A normal destination withdrawal is settled; a failed
/// or mismatched destination fails the attempt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DestinationDeliveryResult {
    Delivered,
    NormalDestinationCanceled,
    DestinationFailure,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DestinationDeliveryAction {
    Settled,
    FailAttempt,
}

pub const fn destination_delivery_action(
    result: DestinationDeliveryResult,
) -> DestinationDeliveryAction {
    match result {
        DestinationDeliveryResult::Delivered
        | DestinationDeliveryResult::NormalDestinationCanceled => {
            DestinationDeliveryAction::Settled
        }
        DestinationDeliveryResult::DestinationFailure => DestinationDeliveryAction::FailAttempt,
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResultPacketVerdict {
    Accept,
    Duplicate { expected: u64, observed: u64 },
    Gap { expected: u64, observed: u64 },
    AfterEndOfStream,
}

impl ResultPacketVerdict {
    pub const fn is_fatal(self) -> bool {
        matches!(
            self,
            Self::Duplicate { .. } | Self::Gap { .. } | Self::AfterEndOfStream
        )
    }
}

impl fmt::Display for ResultPacketVerdict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accept => formatter.write_str("in sequence"),
            Self::Duplicate { expected, observed } => write!(
                formatter,
                "root result packet {observed} was already consumed; {expected} was expected"
            ),
            Self::Gap { expected, observed } => write!(
                formatter,
                "root result packet {expected} was never delivered; {observed} arrived instead"
            ),
            Self::AfterEndOfStream => {
                formatter.write_str("a root result packet arrived after end of stream")
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct RootResultStream {
    next_expected: u64,
    end_of_stream_at: Option<u64>,
}

impl RootResultStream {
    pub const fn new() -> Self {
        Self {
            next_expected: 0,
            end_of_stream_at: None,
        }
    }
    pub const fn packets_consumed(self) -> u64 {
        self.next_expected
    }
    pub const fn end_of_stream_observed(self) -> bool {
        self.end_of_stream_at.is_some()
    }

    pub const fn classify(self, sequence: u64) -> ResultPacketVerdict {
        if self.end_of_stream_at.is_some() {
            return ResultPacketVerdict::AfterEndOfStream;
        }
        if sequence < self.next_expected {
            return ResultPacketVerdict::Duplicate {
                expected: self.next_expected,
                observed: sequence,
            };
        }
        if sequence > self.next_expected {
            return ResultPacketVerdict::Gap {
                expected: self.next_expected,
                observed: sequence,
            };
        }
        ResultPacketVerdict::Accept
    }

    pub fn consume(&mut self, sequence: u64, end_of_stream: bool) -> ResultPacketVerdict {
        let verdict = self.classify(sequence);
        if matches!(verdict, ResultPacketVerdict::Accept) {
            self.next_expected = sequence.saturating_add(1);
            if end_of_stream {
                self.end_of_stream_at = Some(sequence);
            }
        }
        verdict
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_packets_are_contiguous_and_eof_is_final() {
        let mut stream = RootResultStream::new();
        assert_eq!(stream.consume(0, false), ResultPacketVerdict::Accept);
        assert!(stream.consume(2, false).is_fatal());
        assert_eq!(stream.consume(1, true), ResultPacketVerdict::Accept);
        assert_eq!(
            stream.consume(2, false),
            ResultPacketVerdict::AfterEndOfStream
        );
    }

    #[test]
    fn destination_delivery_has_its_own_settlement_policy() {
        assert_eq!(
            destination_delivery_action(DestinationDeliveryResult::NormalDestinationCanceled),
            DestinationDeliveryAction::Settled
        );
        assert_eq!(
            destination_delivery_action(DestinationDeliveryResult::DestinationFailure),
            DestinationDeliveryAction::FailAttempt
        );
    }
}
