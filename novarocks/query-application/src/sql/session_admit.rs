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

//! Query-application capability errors for typed SQL session statements.

use novarocks_parser::Span;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const ADMIT_SESSION_GLOBAL_SCOPE_UNSUPPORTED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.session_global_scope_unsupported"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_SESSION_TRANSACTION_UNSUPPORTED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.session_transaction_unsupported"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const ADMIT_KILL_CONNECTION_UNSUPPORTED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.kill_connection_unsupported"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Deprecated,
};
const ADMIT_KILL_DENIED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.kill_denied"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const ADMIT_PERSISTED_DEFINITION_SEMANTICS_UNSUPPORTED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.persisted_definition_semantics_unsupported"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

/// Session capability descriptors exported for manifest and wire-mapping checks.
pub const SESSION_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    ADMIT_SESSION_GLOBAL_SCOPE_UNSUPPORTED,
    ADMIT_SESSION_TRANSACTION_UNSUPPORTED,
    ADMIT_KILL_CONNECTION_UNSUPPORTED,
    ADMIT_KILL_DENIED,
    ADMIT_PERSISTED_DEFINITION_SEMANTICS_UNSUPPORTED,
];

/// Capability failures owned by Query Application SQL admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionAdmitError {
    GlobalScopeUnsupported,
    TransactionUnsupported,
    KillDenied,
    PersistedDefinitionSemanticsUnsupported,
}

impl SessionAdmitError {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::GlobalScopeUnsupported => ADMIT_SESSION_GLOBAL_SCOPE_UNSUPPORTED,
            Self::TransactionUnsupported => ADMIT_SESSION_TRANSACTION_UNSUPPORTED,
            Self::KillDenied => ADMIT_KILL_DENIED,
            Self::PersistedDefinitionSemanticsUnsupported => {
                ADMIT_PERSISTED_DEFINITION_SEMANTICS_UNSUPPORTED
            }
        }
    }

    pub fn to_user_error(self, source: &str, span: Span, message: impl Into<String>) -> UserError {
        UserError::from_descriptor(
            self.descriptor(),
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn session_admit_descriptor_registry_is_unique_and_preserves_tombstones() {
        let codes = SESSION_ERROR_CODE_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.code)
            .collect::<HashSet<_>>();

        assert_eq!(codes.len(), SESSION_ERROR_CODE_DESCRIPTORS.len());
        assert!(
            SESSION_ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.phase == ErrorPhase::Admit)
        );
        assert_eq!(
            SESSION_ERROR_CODE_DESCRIPTORS
                .iter()
                .find(|descriptor| descriptor.code.as_str()
                    == "sql.admit.kill_connection_unsupported")
                .expect("published tombstone")
                .status,
            ErrorCodeStatus::Deprecated
        );
        assert_eq!(
            SESSION_ERROR_CODE_DESCRIPTORS
                .iter()
                .find(|descriptor| descriptor.code.as_str() == "sql.admit.kill_denied")
                .expect("active KILL denial")
                .status,
            ErrorCodeStatus::Active
        );
    }

    #[test]
    fn session_admit_errors_preserve_code_phase_and_location() {
        for (kind, code) in [
            (
                SessionAdmitError::GlobalScopeUnsupported,
                "sql.admit.session_global_scope_unsupported",
            ),
            (
                SessionAdmitError::TransactionUnsupported,
                "sql.admit.session_transaction_unsupported",
            ),
            (SessionAdmitError::KillDenied, "sql.admit.kill_denied"),
            (
                SessionAdmitError::PersistedDefinitionSemanticsUnsupported,
                "sql.admit.persisted_definition_semantics_unsupported",
            ),
        ] {
            let error = kind.to_user_error(
                "SET GLOBAL query_timeout = 1",
                Span::new(4, 10),
                "session capability is not supported",
            );

            assert_eq!(error.code().as_str(), code);
            assert_eq!(error.phase(), ErrorPhase::Admit);
            assert_eq!(error.location().map(|location| location.column()), Some(5));
        }
    }
}
