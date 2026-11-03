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

//! The Worker-owned refusal vocabulary for runtime-filter contracts.
//!
//! Native decode and transport adapters may construct this error, but task
//! lifecycle ownership decides how the bounded contract refusal affects a
//! context or task.  Keeping the vocabulary here prevents an adapter-local
//! error type from becoming a second runtime-filter authority.

/// Why a runtime-filter contract path refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterContractErrorCode {
    /// The contribution, install, session binding, or terminal projection is
    /// structurally illegal or disagrees with the attempt it names.
    InvalidContract,
    /// The participant that would have to answer is not the one this attempt
    /// installed, so there is nothing left to bind.
    ParticipantClosed,
}

/// One runtime-filter contract refusal, with the detail its producer wrote.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterContractError {
    code: RuntimeFilterContractErrorCode,
    detail: novarocks_execution_contract::failure::BoundedDetail,
}

impl RuntimeFilterContractError {
    pub fn new(code: RuntimeFilterContractErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: novarocks_execution_contract::failure::BoundedDetail::truncating(
                &detail.into(),
            ),
        }
    }

    pub fn invalid_contract(detail: impl Into<String>) -> Self {
        Self::new(RuntimeFilterContractErrorCode::InvalidContract, detail)
    }

    pub const fn code(&self) -> RuntimeFilterContractErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

impl std::fmt::Display for RuntimeFilterContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for RuntimeFilterContractError {}

#[cfg(test)]
mod tests {
    use super::{RuntimeFilterContractError, RuntimeFilterContractErrorCode};

    #[test]
    fn refusal_retains_its_exact_code_and_detail() {
        let error = RuntimeFilterContractError::new(
            RuntimeFilterContractErrorCode::ParticipantClosed,
            "participant already released",
        );

        assert_eq!(
            error.code(),
            RuntimeFilterContractErrorCode::ParticipantClosed
        );
        assert_eq!(error.detail(), "participant already released");
        assert_eq!(
            error.to_string(),
            "ParticipantClosed: participant already released"
        );
    }
}

impl novarocks_execution_contract::failure::FailureProjection for RuntimeFilterContractError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        let reason = match self.code {
            RuntimeFilterContractErrorCode::InvalidContract => R::RuntimeFilterInvalidContract,
            // This code identifies an attempt/participant binding mismatch,
            // not accepted client cancellation or dependency unavailability.
            RuntimeFilterContractErrorCode::ParticipantClosed => {
                R::RuntimeFilterParticipantMismatch
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection, domain};
    #[test]
    fn real_rf_codes_survive_scan_resolution_and_host_rejection_without_text_classification() {
        use std::error::Error;
        for (code, reason) in [
            (RuntimeFilterContractErrorCode::InvalidContract, 12),
            (RuntimeFilterContractErrorCode::ParticipantClosed, 13),
        ] {
            let original = RuntimeFilterContractError::new(
                code,
                "cancelled resource exhausted numeric overflow 雪".repeat(100),
            );
            let expected = original.failure_fact();
            assert_eq!(expected.class(), FailureClass::Invariant);
            assert_eq!(expected.reason().domain(), domain::WORKER.get());
            assert_eq!(expected.reason().code(), reason);
            assert!(original.detail().len() <= 512);
            let resolver: crate::RuntimeFilterSessionResolver =
                std::sync::Arc::new(move || Err(original.clone()));
            let original = resolver().err().unwrap();
            let rejection = crate::HostRejection::from_domain(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                original,
            );
            let execution = rejection
                .source()
                .unwrap()
                .downcast_ref::<novarocks_execution::runtime::task::ExecutionFailure>()
                .unwrap();
            assert_eq!(execution.failure_fact(), expected);
            let mut source = execution.source();
            let mut retained = false;
            while let Some(error) = source {
                if let Some(actual) = error.downcast_ref::<RuntimeFilterContractError>() {
                    assert_eq!(actual.code(), code);
                    assert_eq!(actual.failure_fact(), expected);
                    retained = true;
                }
                source = error.source();
            }
            assert!(retained);
        }
    }
}
