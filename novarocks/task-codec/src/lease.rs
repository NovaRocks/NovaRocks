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

//! Query execution lease codec.

use std::time::Duration;

use novarocks_execution::task_execution::lease::{LeaseReceipt, LeaseSequence, LeaseValidFor};
use novarocks_proto_models::novarocks;

use novarocks_proto_codec::{FieldPath, ProtocolError};

use crate::{invalid, out_of_range};

/// One grant of the query execution lease.
///
/// The sequence and the requested duration travel together so a same-sequence
/// replay can be recognised as idempotent and a same-sequence change as a
/// conflict.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct LeaseGrant {
    sequence: LeaseSequence,
    valid_for: LeaseValidFor,
}

impl LeaseGrant {
    pub const fn new(sequence: LeaseSequence, valid_for: LeaseValidFor) -> Self {
        Self {
            sequence,
            valid_for,
        }
    }

    pub const fn sequence(self) -> LeaseSequence {
        self.sequence
    }

    pub const fn valid_for(self) -> LeaseValidFor {
        self.valid_for
    }
}

/// Decodes a lease grant.
pub fn decode_lease_grant(
    src: &novarocks::QueryExecutionLeaseGrant,
    path: FieldPath,
) -> Result<LeaseGrant, ProtocolError> {
    let valid_for = decode_duration_millis(
        src.valid_for_millis,
        path.field("valid_for_millis"),
        LeaseValidFor::MAX_REPRESENTABLE,
    )?;
    let valid_for = LeaseValidFor::new(valid_for)
        .map_err(|error| invalid(path.field("valid_for_millis"), error.to_string()))?;
    Ok(LeaseGrant::new(LeaseSequence::new(src.sequence), valid_for))
}

pub fn encode_lease_grant(value: LeaseGrant) -> novarocks::QueryExecutionLeaseGrant {
    novarocks::QueryExecutionLeaseGrant {
        sequence: value.sequence().get(),
        valid_for_millis: value.valid_for().get().as_millis() as u64,
    }
}

/// Decodes a lease receipt.
///
/// The effective duration is the only one a frontend may schedule from, so it
/// is validated as strictly as the requested one.
pub fn decode_lease_receipt(
    src: &novarocks::QueryExecutionLeaseReceipt,
    path: FieldPath,
) -> Result<LeaseReceipt, ProtocolError> {
    let requested = decode_duration_millis(
        src.requested_valid_for_millis,
        path.clone().field("requested_valid_for_millis"),
        LeaseValidFor::MAX_REPRESENTABLE,
    )?;
    let requested = LeaseValidFor::new(requested).map_err(|error| {
        invalid(
            path.clone().field("requested_valid_for_millis"),
            error.to_string(),
        )
    })?;
    let effective = decode_duration_millis(
        src.effective_valid_for_millis,
        path.clone().field("effective_valid_for_millis"),
        LeaseValidFor::MAX_REPRESENTABLE,
    )?;
    if effective.is_zero() {
        return Err(invalid(
            path.field("effective_valid_for_millis"),
            "effective lease duration must be greater than zero",
        ));
    }
    Ok(LeaseReceipt::new(
        LeaseSequence::new(src.sequence),
        requested,
        effective,
    ))
}

pub fn encode_lease_receipt(value: LeaseReceipt) -> novarocks::QueryExecutionLeaseReceipt {
    novarocks::QueryExecutionLeaseReceipt {
        sequence: value.sequence().get(),
        requested_valid_for_millis: value.requested_valid_for().get().as_millis() as u64,
        effective_valid_for_millis: value.effective_valid_for().as_millis() as u64,
    }
}

/// Decodes a millisecond duration, rejecting zero and anything past `limit`.
///
/// Durations, never absolute deadlines, cross this boundary: the receiver
/// times every one of them against its own monotonic clock.
pub(crate) fn decode_duration_millis(
    millis: u64,
    path: FieldPath,
    limit: Duration,
) -> Result<Duration, ProtocolError> {
    if millis == 0 {
        return Err(invalid(path, "duration must be greater than zero"));
    }
    let value = Duration::from_millis(millis);
    if value > limit {
        return Err(out_of_range(
            path,
            "duration exceeds the representable range",
        ));
    }
    Ok(value)
}
