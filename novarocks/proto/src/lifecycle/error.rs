//! Typed failures raised while validating role-neutral lifecycle values.
//!
//! These errors intentionally stop at the contract boundary. Transport,
//! liveness, registry-state, and application failures belong to their role
//! owners and must not be folded into this vocabulary.

use std::fmt;

/// Stable categories for invalid lifecycle values and value comparisons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractErrorCode {
    /// A required field, scalar bound, enum value, or cross-field invariant is invalid.
    InvalidValue,
    /// Two otherwise valid contract values disagree.
    Conflict,
    /// A value exceeds a protocol-owned bounded limit.
    Capacity,
    /// A recognized value uses an unsupported contract version.
    VersionMismatch,
    /// A supplied digest does not match the canonical value.
    DigestMismatch,
}

/// A typed, deterministic contract-validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractError {
    code: ContractErrorCode,
    detail: String,
}

impl ContractError {
    pub fn new(code: ContractErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid_value(detail: impl Into<String>) -> Self {
        Self::new(ContractErrorCode::InvalidValue, detail)
    }

    pub(crate) fn conflict(detail: impl Into<String>) -> Self {
        Self::new(ContractErrorCode::Conflict, detail)
    }

    pub(crate) fn capacity(detail: impl Into<String>) -> Self {
        Self::new(ContractErrorCode::Capacity, detail)
    }

    pub(crate) fn version_mismatch(detail: impl Into<String>) -> Self {
        Self::new(ContractErrorCode::VersionMismatch, detail)
    }

    pub(crate) fn digest_mismatch(detail: impl Into<String>) -> Self {
        Self::new(ContractErrorCode::DigestMismatch, detail)
    }

    pub const fn code(&self) -> ContractErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for ContractError {}

#[cfg(test)]
mod tests {
    use super::{ContractError, ContractErrorCode};

    #[test]
    fn preserves_the_contract_error_code_mapping() {
        let cases = [
            (
                ContractError::invalid_value("missing manifest"),
                ContractErrorCode::InvalidValue,
            ),
            (
                ContractError::conflict("digest conflict"),
                ContractErrorCode::Conflict,
            ),
            (
                ContractError::capacity("too many fragments"),
                ContractErrorCode::Capacity,
            ),
            (
                ContractError::version_mismatch("version 2"),
                ContractErrorCode::VersionMismatch,
            ),
            (
                ContractError::digest_mismatch("manifest digest"),
                ContractErrorCode::DigestMismatch,
            ),
        ];

        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert!(!error.detail().is_empty());
        }
    }

    #[test]
    fn display_retains_the_typed_category_and_detail() {
        let error = ContractError::new(ContractErrorCode::Conflict, "different payload");

        assert_eq!(error.to_string(), "Conflict: different payload");
    }
}
