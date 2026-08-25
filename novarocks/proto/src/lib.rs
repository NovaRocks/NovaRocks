//! NovaRocks-native protobuf schema artifacts.
//!
//! This crate owns generated DTOs, schema-ledger metadata, and the neutral
//! lifecycle contract derived from the canonical repository-level IDL.
//! Transport, role-local state machines, and FE/BE execution conversion remain
//! outside this package.
// Design: ADR-0105 (docs/adr/ADR-0105-wire-authority-and-domain-carrier-separation.md)

pub const SCHEMA_LEDGER_VERSION: u32 = 1;

// Design: ADR-0098 (docs/adr/ADR-0098-native-protocol-error-contract.md)
pub mod error;
pub use error::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};

/// File descriptor set generated from the canonical repository-level IDL.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/novarocks_descriptor.bin"));

/// Canonical descriptor-driven projection and digest utilities.
pub mod canonical;

/// Validated connector execution-binding declaration and result values.
pub mod provider;

/// Validated neutral values used by the native query lifecycle.
pub mod lifecycle;

#[allow(clippy::len_without_is_empty)]
pub mod common {
    include!(concat!(env!("OUT_DIR"), "/novarocks.common.rs"));
}

#[allow(clippy::module_inception)]
pub mod expr {
    include!(concat!(env!("OUT_DIR"), "/novarocks.expr.rs"));
}

pub mod filter {
    include!(concat!(env!("OUT_DIR"), "/novarocks.filter.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod plan {
    include!(concat!(env!("OUT_DIR"), "/novarocks.plan.rs"));
}

#[allow(clippy::large_enum_variant)]
pub mod novarocks {
    use super::{common, filter, plan};

    include!(concat!(env!("OUT_DIR"), "/novarocks.rs"));
}
