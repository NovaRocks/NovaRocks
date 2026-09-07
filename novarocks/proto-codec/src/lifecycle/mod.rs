//! Validated, role-neutral native query values.
//!
//! What is left here after the fragment query lifecycle was retired is the
//! vocabulary the task protocol still shares with both roles: query-attempt
//! identity, the endpoint a backend is addressed by, query options, scan
//! ranges, credential leases, the runtime-filter contribution installed with a
//! query context, and the terminal runtime-filter observation released from
//! it.

pub mod credential_lease;
pub mod identity;
pub mod manifest;
pub mod query_options;
pub mod scan_range;
pub mod terminal;

/// Why one backend-local query participant stood down.
///
/// No wire message carries this enum: the RPCs that did belonged to the
/// retired fragment lifecycle. It survives as the vocabulary the backend's own
/// runtime-filter participant close path is driven by from the task protocol's
/// query-context host.
pub use novarocks_proto_models::novarocks::QueryTerminationReason;

pub use credential_lease::{
    CredentialLeaseSecretEnvelope, decode_credential_lease_descriptor,
    decode_credential_lease_secret_envelope, encode_credential_lease_descriptor,
    encode_credential_lease_secret_envelope, validate_credential_lease_descriptors,
    validate_initial_credential_lease_envelopes,
};
pub use identity::{
    AttemptId, QueryExecutionId, decode_query_execution_id, encode_query_execution_id,
};
pub use manifest::{QueryControlEndpoint, RuntimeFilterContribution};
pub use query_options::QueryOptions;
pub use scan_range::{FileScanRange, ScanRange, ScanRangeParams};
pub use terminal::{
    QueryTerminalProfileContributionTelemetry, QueryTerminalProfileContributionV1,
    TerminalTelemetryUnavailable,
};
