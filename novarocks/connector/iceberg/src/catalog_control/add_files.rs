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

//! Provider-owned ADD FILES source admission.

use std::fmt;

use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, FailureReason};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

#[derive(Debug)]
struct ProcedureFailure {
    fact: FailureFact,
    message: String,
}

impl fmt::Display for ProcedureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(&self.message)
    }
}

impl std::error::Error for ProcedureFailure {}

impl FailureProjection for ProcedureFailure {
    fn failure_fact(&self) -> FailureFact {
        self.fact
    }
}

pub(super) fn procedure_error(
    kind: ConnectorErrorKind,
    class: FailureClass,
    reason: FailureReason,
    message: impl Into<String>,
) -> ConnectorError {
    ConnectorError::from_domain(
        kind,
        ProcedureFailure {
            fact: FailureFact::new(class, reason),
            message: message.into(),
        },
    )
    .with_public_failure_detail()
}

#[expect(
    dead_code,
    reason = "source listing in P07 consumes the location check"
)]
pub(super) fn validate_registration_location(location: &str) -> Result<(), ConnectorError> {
    crate::write_codec::validate_secret_free_text("ADD FILES source location", location)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

#[cfg(test)]
pub(crate) mod tests;
