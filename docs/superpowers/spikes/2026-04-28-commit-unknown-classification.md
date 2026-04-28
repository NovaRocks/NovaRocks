# Spike — commit-unknown classification

**Date:** 2026-04-28
**Spec ref:** §5.4 / §7.2

## iceberg::ErrorKind variants (0.9)

iceberg-rust 0.9 defines nine `ErrorKind` variants. The enum is `#[non_exhaustive]`,
so new variants may appear in future minor releases. The accessor is `err.kind()` which
returns `ErrorKind` (a `Copy` enum). `Error::retryable()` returns a separate bool that
the built-in `Transaction::commit` retry loop uses; we do not rely on it here — we
classify by `kind()` alone.

All `TableRequirement::check` failures (OCC mismatch, snapshot-id mismatch, schema-id
mismatch, etc.) are emitted as `CatalogCommitConflicts` with `retryable = true`. There
is no separate `ConflictRequest` variant in 0.9 — the plan-template used a hypothetical
name. The real variant is `CatalogCommitConflicts`.

IO failures from metadata writes (`std::io::Error`, `reqwest::Error`, etc.) are all
coerced to `Unexpected` via `define_from_err!` macros in `error.rs`. There is no
separate network-timeout variant.

| ErrorKind | Where constructed / semantics | Classification |
|---|---|---|
| `CatalogCommitConflicts` | Every `TableRequirement::check` failure: `NotExist`, `UuidMatch`, `RefSnapshotIdMatch` (OCC snapshot mismatch), `CurrentSchemaIdMatch`, `DefaultSortOrderIdMatch`, `DefaultSpecIdMatch`, `LastAssignedPartitionIdMatch`, `LastAssignedFieldIdMatch`; also `TableNotFound` branch of `HadoopFileSystemCatalog` on missing table during update | **DEFINITE FAIL** → clean up staged files. The catalog confirmed the current table state does not match the requirements we submitted, so our commit was definitively rejected. |
| `DataInvalid` | Malformed metadata payload, bad JSON, invalid schema, corrupt data file; raised by `update.apply()` and metadata serialization before the network call | **DEFINITE FAIL** → clean up. The error occurs before the catalog write reaches the remote; no snapshot was committed. |
| `FeatureUnsupported` | Catalog or format version does not support the requested operation | **DEFINITE FAIL** → clean up. Structural rejection; cannot have committed. |
| `TableNotFound` | Table missing when loading it as part of requirements check | **DEFINITE FAIL** → clean up. No commit possible against a non-existent table. |
| `TableAlreadyExists` | Race on `TableRequirement::NotExist` check | **DEFINITE FAIL** → clean up. |
| `NamespaceNotFound` | Namespace missing at requirement check | **DEFINITE FAIL** → clean up. |
| `NamespaceAlreadyExists` | Namespace already exists (not relevant to commit path) | **DEFINITE FAIL** → clean up. |
| `PreconditionFailed` | Used in transaction-action pre-checks (e.g. `UpdatePropertiesAction`, delete-vector pre-condition); raised before catalog network call | **DEFINITE FAIL** → clean up. |
| `Unexpected` | Catch-all: `std::io::Error` (metadata write failure), `reqwest::Error` (HTTP network error), `parquet::errors::ParquetError`, `arrow_schema::ArrowError`, `futures::channel::mpsc::SendError`, and any other conversions via `define_from_err!`; also explicit internal errors ("Failed to create snapshot summary", etc.) | **COMMIT-UNKNOWN** → do NOT clean up. An IO or network error during the metadata write means the catalog may have persisted the new snapshot before the connection dropped. Files must be left for human review. |

### Key finding: `CatalogCommitConflicts` vs `Unexpected` is the critical split

- `CatalogCommitConflicts` — catalog definitively checked requirements and rejected the commit. The catalog *never* applied the snapshot. Safe to clean up.
- `Unexpected` — covers every transport/IO error, meaning the metadata write may have completed at the catalog before we lost connectivity. **Do not clean up.**

All other variants (`DataInvalid`, `FeatureUnsupported`, `TableNotFound`, `TableAlreadyExists`,
`NamespaceNotFound`, `NamespaceAlreadyExists`, `PreconditionFailed`) are raised during
pre-flight validation before the actual catalog write, so they are safe-to-clean-up.

### Note on `retryable` flag

The `retryable` field in `iceberg::Error` is orthogonal to our classification. The
built-in `Transaction::commit` retry loop uses `retryable = true` on `CatalogCommitConflicts`
to retry with refreshed metadata. Our code calls `Catalog::update_table` directly (not
through `Transaction`) and does not retry — we classify and surface the error to the
caller. We ignore `err.retryable()` in `is_commit_unknown`.

### Note on `#[non_exhaustive]`

Because `ErrorKind` is `#[non_exhaustive]`, a `match` with explicit arms must include
a wildcard. The safe-by-default wildcard should return `true` (commit-unknown, leave
files) because that is the fail-safe direction.

## Classification function for run.rs (Task 11)

```rust
/// Returns `true` if the error from `Catalog::update_table` is ambiguous —
/// meaning the snapshot may or may not have been committed at the catalog.
/// When `true`, staged data files and uncommitted manifests must NOT be deleted
/// (leave them for human review, per spec §5.4 / §7.2).
///
/// When `false`, the catalog definitively rejected the commit before persisting
/// any snapshot; it is safe to clean up all staged files.
///
/// # Classification rationale
///
/// iceberg-rust 0.9 uses `ErrorKind::Unexpected` as the catch-all for all IO
/// and transport errors (std::io::Error, reqwest::Error, etc.).  An IO error
/// during the metadata-write phase means the catalog write may have completed
/// before the connection dropped, so we treat `Unexpected` as commit-unknown.
///
/// Every other variant is raised before or during pre-flight requirement checks,
/// before the catalog writes the new snapshot — they are definitively rejected.
///
/// Because `ErrorKind` is `#[non_exhaustive]`, unknown future variants default
/// to `true` (commit-unknown) for safety.
pub(crate) fn is_commit_unknown(err: &iceberg::Error) -> bool {
    use iceberg::ErrorKind::*;
    match err.kind() {
        // Definite failures: catalog checked requirements and rejected the commit
        // before writing any new snapshot, or validation failed before the network
        // call. Safe to clean up staged files.
        CatalogCommitConflicts
        | DataInvalid
        | FeatureUnsupported
        | TableNotFound
        | TableAlreadyExists
        | NamespaceNotFound
        | NamespaceAlreadyExists
        | PreconditionFailed => false,

        // Catch-all: Unexpected covers IO errors (std::io::Error, reqwest::Error,
        // parquet errors, etc.) — the catalog may have committed before we lost
        // connectivity. Also catches any future non_exhaustive variants.
        // Do NOT clean up.
        _ => true,
    }
}
```

## Test cases for unit tests in Task 11

The following assertions should be added to a `#[cfg(test)]` module in `run.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::is_commit_unknown;
    use iceberg::{Error, ErrorKind};

    fn make(kind: ErrorKind) -> iceberg::Error {
        Error::new(kind, "test error")
    }

    #[test]
    fn test_catalog_commit_conflicts_is_definite_fail() {
        assert!(!is_commit_unknown(&make(ErrorKind::CatalogCommitConflicts)));
    }

    #[test]
    fn test_data_invalid_is_definite_fail() {
        assert!(!is_commit_unknown(&make(ErrorKind::DataInvalid)));
    }

    #[test]
    fn test_feature_unsupported_is_definite_fail() {
        assert!(!is_commit_unknown(&make(ErrorKind::FeatureUnsupported)));
    }

    #[test]
    fn test_table_not_found_is_definite_fail() {
        assert!(!is_commit_unknown(&make(ErrorKind::TableNotFound)));
    }

    #[test]
    fn test_precondition_failed_is_definite_fail() {
        assert!(!is_commit_unknown(&make(ErrorKind::PreconditionFailed)));
    }

    #[test]
    fn test_unexpected_is_commit_unknown() {
        // IO and transport errors — catalog may have committed.
        assert!(is_commit_unknown(&make(ErrorKind::Unexpected)));
    }

    #[test]
    fn test_unexpected_with_io_source_is_commit_unknown() {
        let err = Error::new(ErrorKind::Unexpected, "IO Operation failed")
            .with_source(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            ));
        assert!(is_commit_unknown(&err));
    }

    #[test]
    fn test_catalog_commit_conflicts_occ_is_definite_fail() {
        // Simulates RefSnapshotIdMatch mismatch (OCC conflict).
        let err = Error::new(
            ErrorKind::CatalogCommitConflicts,
            "Requirement failed: Branch or tag `main`'s snapshot has changed",
        )
        .with_retryable(true);
        assert!(!is_commit_unknown(&err));
    }
}
```

## HadoopFileSystemCatalog — specific error analysis

The in-tree `HadoopFileSystemCatalog` (`src/connector/iceberg/catalog/hadoop_catalog.rs`)
manually applies requirements via `requirement.check(Some(current_metadata))?`. All OCC
failures from `check()` are `CatalogCommitConflicts` (definite fail). Metadata write
failures are `Unexpected` (IO-wrapped). The in-memory table registry update happens after
the file write, so if the file write succeeds but the registry update panics (impossible
here), the classification still holds.

## Conclusion

The `is_commit_unknown` function above is correct and complete for iceberg-rust 0.9 +
`HadoopFileSystemCatalog`. No string-matching fallback is needed. The classification is
driven entirely by `err.kind()`. The only ambiguity category is `Unexpected`, which maps
directly to the commit-unknown safety bucket.
