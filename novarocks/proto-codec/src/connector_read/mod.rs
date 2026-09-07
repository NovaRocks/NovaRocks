//! Structural validation for the typed connector read wire.
//!
//! This module owns bounds, required presence, known enums, uniqueness,
//! cross-field consistency, and closed-carrier bounds. It does
//! not own connector semantics: a provider variant is carried and validated
//! structurally, and only the provider that produced it interprets it.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

mod handle;
mod predicate;
mod runtime_codec;
mod scan;
mod split;
mod task_update;
mod value;

pub use handle::{
    CatalogTableHandle, ConnectorRelation, ConnectorRelationKind, TableExecuteProcedure,
    ValidatedConnectorChangeWindowHandle, ValidatedConnectorMergeTableHandle,
    ValidatedConnectorSystemTableReference, ValidatedConnectorTableExecuteHandle,
    ValidatedConnectorTableFunctionHandle, ValidatedConnectorTableHandle,
    ValidatedTransactionHandle,
};
pub use predicate::{
    ValidatedColumnHandle, decode_connector_expression, decode_tuple_domain,
    encode_connector_expression, encode_tuple_domain,
};
pub use runtime_codec::{
    ConnectorReadCodecError, ConnectorReadDecoder, ConnectorReadEncoder, DecodedConnectorReadScan,
    DecodedScheduledReadSplit, ReceivedScheduledSplit,
};
pub use scan::{ConnectorTableScanSource, DynamicFilterBinding, ScanAssignment, ScanWorkSource};
pub use split::{SplitCategory, ValidatedConnectorSplit};
pub use task_update::{
    ScheduledSplit, SplitAssignment, parse_task_update_assignments,
};
pub use value::{decode_value, decode_value_type, encode_value, encode_value_type};

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

// Hard bounds. Every one of these is a wire-visible budget: exceeding it is a
// typed rejection before any connector I/O or side effect.
pub const MAX_SPLIT_ENCODED_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ASSIGNMENT_ENCODED_BYTES: usize = 48 * 1024 * 1024;
pub const MAX_ASSIGNMENT_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_SPLITS_PER_ASSIGNMENT: usize = 4096;
pub const MAX_ASSIGNMENTS_PER_TASK_UPDATE: usize = 64;
pub const MAX_PATH_BYTES: usize = 16 * 1024;
pub const MAX_AFFINITY_KEY_BYTES: usize = 4 * 1024;
pub const MAX_PARTITION_VALUES: usize = 4096;
pub const MAX_DELETES_PER_SPLIT: usize = 1024;
pub const MAX_EQUALITY_FIELD_IDS: usize = 4096;
pub const MAX_TUPLE_DOMAIN_COLUMNS: usize = 4096;
pub const MAX_VALUE_SET_RANGES: usize = 4096;
pub const MAX_EXPRESSION_NODES: usize = 16_384;
pub const MAX_EXPRESSION_DEPTH: usize = 64;
pub const MAX_SCALAR_BYTES: usize = 64 * 1024;
pub const MAX_SPLIT_SCALAR_TOTAL_BYTES: usize = 1024 * 1024;
pub const MAX_ENCRYPTION_MATERIAL_BYTES: usize = 64 * 1024;
pub const MAX_SCAN_ASSIGNMENTS: usize = 4096;
pub const MAX_SPLIT_ADDRESSES: usize = 256;
pub const MAX_SCHEMA_TABLE_NAME_BYTES: usize = 1024;
pub const MAX_NAME_BYTES: usize = 256;
pub const MAX_JSON_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SAFE_DETAIL_BYTES: usize = 512;
pub const MAX_SAFE_FIELD_PATH_BYTES: usize = 256;

pub(crate) fn missing(path: FieldPath, detail: &'static str) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

pub(crate) fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

pub(crate) fn out_of_range(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::OutOfRange, detail)
}

pub(crate) fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InconsistentFields, detail)
}

pub(crate) fn invalid_enum(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidEnum, detail)
}

pub(crate) fn unsupported(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::Unsupported, detail)
}

pub(crate) fn bounded_text(
    value: &str,
    max_bytes: usize,
    path: FieldPath,
    allow_empty: bool,
) -> Result<(), ProtocolError> {
    if !allow_empty && value.is_empty() {
        return Err(invalid(path, "value must not be empty"));
    }
    if value.len() > max_bytes {
        return Err(out_of_range(
            path,
            format!("value exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn bounded_bytes(
    value: &[u8],
    max_bytes: usize,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if value.len() > max_bytes {
        return Err(out_of_range(
            path,
            format!("value exceeds {max_bytes} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn exact_bytes(
    value: &[u8],
    expected: usize,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if value.len() != expected {
        return Err(invalid(
            path,
            format!("value must contain exactly {expected} bytes"),
        ));
    }
    Ok(())
}

pub(crate) fn nonnegative_i64(
    value: i64,
    path: FieldPath,
    label: &'static str,
) -> Result<i64, ProtocolError> {
    if value < 0 {
        return Err(out_of_range(path, format!("{label} must be nonnegative")));
    }
    Ok(value)
}

/// Re-root a nested error under this message's path without rebuilding it.
pub(crate) fn nest(path: FieldPath, error: ProtocolError) -> ProtocolError {
    let kind = error.kind();
    let detail = error.detail().to_owned();
    let nested = path.append_segments(error.path().segments().iter().skip(1).cloned());
    ProtocolError::new(nested, kind, detail)
}
