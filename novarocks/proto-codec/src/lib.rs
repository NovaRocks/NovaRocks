//! NovaRocks-native protobuf wire codecs.
//!
//! Generated DTOs, schema-ledger metadata, and the descriptor set belong to
//! `novarocks-proto-models`. This crate owns the codec and validation layer
//! derived from those artifacts. Transport, role-local state machines, and
//! FE/BE execution conversion remain outside this package.
// Design: ADR-0106 (docs/adr/ADR-0106-native-wire-layering-and-terminal-content-identity.md)
pub mod error;
pub use error::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};

/// Validated catalog identity, materialization, and reachability carriers.
pub mod catalog;

/// Structural validation and canonical encoding for the typed connector read wire.
pub mod connector_read;

/// Structural validation and canonical encoding for the connector write
/// carriers.
pub mod connector_write;

/// Validated neutral values used by the native query lifecycle.
pub mod lifecycle;

/// Validated membership and backend process wire values.
pub mod membership;

/// Central codec for the native task protocol.
pub mod task_execution;
