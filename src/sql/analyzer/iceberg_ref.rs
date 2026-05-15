//! Resolve Iceberg time-travel clauses + DML branch suffixes into a single
//! `IcebergRefBinding` that the read and commit paths consume.

#![allow(dead_code)]

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergRefKind {
    Branch,
    Tag,
}

// ---------------------------------------------------------------------------
// DML branch/tag suffix helpers
// ---------------------------------------------------------------------------

/// The trailing suffix of a qualified table name that identifies a branch or tag.
///
/// `INSERT INTO t.branch_dev` → `Branch("dev")`.
/// `INSERT INTO t.tag_v1`     → `Tag("v1")`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergRefSuffix {
    Branch(String),
    Tag(String),
}

/// Inspect the trailing segment of a qualified table name.
///
/// If the last part matches `^branch_(.+)$`, strip that part and return
/// `(stripped_parts, Some(IcebergRefSuffix::Branch(name)))`.
/// If the last part matches `^tag_(.+)$`, return
/// `(stripped_parts, Some(IcebergRefSuffix::Tag(name)))`.
/// Otherwise return `(original_parts, None)` unchanged.
pub fn split_ref_suffix(parts: &[String]) -> (Vec<String>, Option<IcebergRefSuffix>) {
    if let Some(last) = parts.last() {
        if let Some(name) = last.strip_prefix("branch_")
            && !name.is_empty() {
                return (
                    parts[..parts.len() - 1].to_vec(),
                    Some(IcebergRefSuffix::Branch(name.to_string())),
                );
            }
        if let Some(name) = last.strip_prefix("tag_")
            && !name.is_empty() {
                return (
                    parts[..parts.len() - 1].to_vec(),
                    Some(IcebergRefSuffix::Tag(name.to_string())),
                );
            }
    }
    (parts.to_vec(), None)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergRefBinding {
    pub snapshot_id: i64,
    pub ref_name: Option<String>,
    pub ref_kind: Option<IcebergRefKind>,
}

impl IcebergRefBinding {
    pub fn ref_repr(&self) -> String {
        match (&self.ref_name, &self.ref_kind) {
            (Some(name), Some(IcebergRefKind::Branch)) => format!("branch '{name}'"),
            (Some(name), Some(IcebergRefKind::Tag)) => format!("tag '{name}'"),
            (Some(name), None) => format!("ref '{name}'"),
            (None, _) => format!("snapshot {}", self.snapshot_id),
        }
    }
}

impl fmt::Display for IcebergRefBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.ref_repr())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDmlTarget {
    pub read_binding: IcebergRefBinding,
    pub write_ref: String,
}

/// Resolve a SQL `FOR VERSION/TIMESTAMP AS OF` clause into an `IcebergRefBinding`
/// against the given table metadata.
///
/// Resolution rules (Iceberg spec §4.2):
/// - `VERSION AS OF <integer>` → snapshot id; must exist in metadata.
/// - `VERSION AS OF '<string>'` → named ref (branch or tag); must exist in `metadata.refs()`.
/// - `TIMESTAMP AS OF <integer>` or `FOR SYSTEM_TIME AS OF <integer>` → epoch-ms; finds the
///   snapshot with the largest `timestamp_ms` ≤ requested_ms from `metadata.history()`.
/// - `TIMESTAMP AS OF '<rfc3339-string>'` or `'<YYYY-MM-DD HH:MM:SS>'` → parsed to ms and
///   treated the same as an integer epoch-ms timestamp.
/// - Any other expression (function call, identifier, cast, …) → fail-fast error.
/// - `Function(_)` (BigQuery AT syntax) → rejected in phase 1.
///
/// Phase-1 limitation: timestamp expressions must be literals (integer or quoted string).
/// Expression-level timestamps (e.g. `CURRENT_TIMESTAMP() - INTERVAL 1 HOUR`) are rejected.
pub fn resolve_read_binding(
    version: &sqlparser::ast::TableVersion,
    metadata: &iceberg::spec::TableMetadata,
    fully_qualified_name: &str,
) -> Result<IcebergRefBinding, String> {
    use sqlparser::ast::{Expr, TableVersion, Value};

    match version {
        TableVersion::VersionAsOf(expr) => match expr {
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => {
                    let snapshot_id: i64 = n.parse().map_err(|_| {
                        format!("iceberg time travel: invalid snapshot id '{n}' for {fully_qualified_name}")
                    })?;
                    if metadata.snapshot_by_id(snapshot_id).is_none() {
                        return Err(format!(
                            "iceberg time travel: snapshot {snapshot_id} not found in {fully_qualified_name}"
                        ));
                    }
                    Ok(IcebergRefBinding {
                        snapshot_id,
                        ref_name: None,
                        ref_kind: None,
                    })
                }
                Value::SingleQuotedString(s) => {
                    let refs = metadata.refs();
                    let entry = refs.get(s.as_str()).ok_or_else(|| {
                        format!(
                            "iceberg time travel: ref '{s}' not found in {fully_qualified_name}"
                        )
                    })?;
                    let ref_kind = match &entry.retention {
                        iceberg::spec::SnapshotRetention::Branch { .. } => IcebergRefKind::Branch,
                        iceberg::spec::SnapshotRetention::Tag { .. } => IcebergRefKind::Tag,
                    };
                    Ok(IcebergRefBinding {
                        snapshot_id: entry.snapshot_id,
                        ref_name: Some(s.clone()),
                        ref_kind: Some(ref_kind),
                    })
                }
                other => Err(format!(
                    "iceberg time travel: phase 1 only accepts literal snapshot id or ref name for VERSION AS OF; got value: {other}"
                )),
            },
            other => Err(format!(
                "iceberg time travel: phase 1 only accepts literal snapshot id or ref name for VERSION AS OF; got expression: {other}"
            )),
        },

        TableVersion::TimestampAsOf(expr) | TableVersion::ForSystemTimeAsOf(expr) => {
            // Check for the `__nr_ref:` magic prefix produced by `normalize_for_raw_parse`
            // when rewriting `FOR VERSION AS OF '<string_ref>'` to a form that sqlparser
            // can parse.  Branch/tag names are routed here because sqlparser 0.61 only
            // accepts numeric literals for `VERSION AS OF`, so the normalizer encodes
            // `VERSION AS OF 'branch'` as `SYSTEM_TIME AS OF '__nr_ref:branch'`.
            if let sqlparser::ast::Expr::Value(v) = expr
                && let sqlparser::ast::Value::SingleQuotedString(s) = &v.value
                    && let Some(ref_name) = s.strip_prefix("__nr_ref:") {
                        let refs = metadata.refs();
                        let entry = refs.get(ref_name).ok_or_else(|| {
                            format!(
                                "iceberg time travel: ref '{ref_name}' not found in {fully_qualified_name}"
                            )
                        })?;
                        let ref_kind = match &entry.retention {
                            iceberg::spec::SnapshotRetention::Branch { .. } => {
                                IcebergRefKind::Branch
                            }
                            iceberg::spec::SnapshotRetention::Tag { .. } => IcebergRefKind::Tag,
                        };
                        return Ok(IcebergRefBinding {
                            snapshot_id: entry.snapshot_id,
                            ref_name: Some(ref_name.to_string()),
                            ref_kind: Some(ref_kind),
                        });
                    }
            let ts_ms = resolve_timestamp_expr(expr, fully_qualified_name)?;
            find_snapshot_at_or_before(metadata, ts_ms, fully_qualified_name)
        }

        TableVersion::Function(_) => Err(format!(
            "iceberg time travel: BigQuery AT(...) syntax is not supported for {fully_qualified_name}; use VERSION AS OF or TIMESTAMP AS OF"
        )),
    }
}

/// Parse a timestamp literal expression into epoch milliseconds.
/// Phase 1: only accepts integer literals (epoch ms) or single-quoted strings
/// parseable as RFC 3339 or `%Y-%m-%d %H:%M:%S`.
fn resolve_timestamp_expr(
    expr: &sqlparser::ast::Expr,
    fully_qualified_name: &str,
) -> Result<i64, String> {
    use sqlparser::ast::{Expr, Value};

    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse::<i64>().map_err(|_| {
                format!(
                    "iceberg time travel: invalid epoch-ms value '{n}' for {fully_qualified_name}"
                )
            }),
            Value::SingleQuotedString(s) => parse_timestamp_string(s, fully_qualified_name),
            other => Err(format!(
                "iceberg time travel: phase 1 only accepts literal timestamp; got value: {other}"
            )),
        },
        other => Err(format!(
            "iceberg time travel: phase 1 only accepts literal timestamp; got expression: {other}"
        )),
    }
}

/// Parse a timestamp string as RFC 3339 or `%Y-%m-%d %H:%M:%S` (UTC assumed).
fn parse_timestamp_string(s: &str, fully_qualified_name: &str) -> Result<i64, String> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    // Try RFC 3339 / ISO 8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).timestamp_millis());
    }
    // Fallback: `YYYY-MM-DD HH:MM:SS`
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(ndt.and_utc().timestamp_millis());
    }
    Err(format!(
        "iceberg time travel: cannot parse timestamp '{s}' for {fully_qualified_name}; expected RFC 3339 or 'YYYY-MM-DD HH:MM:SS'"
    ))
}

/// Find the latest snapshot whose `timestamp_ms` ≤ `ts_ms` in the snapshot log.
fn find_snapshot_at_or_before(
    metadata: &iceberg::spec::TableMetadata,
    ts_ms: i64,
    fully_qualified_name: &str,
) -> Result<IcebergRefBinding, String> {
    let history = metadata.history();
    // history is ordered chronologically; find last entry with timestamp_ms <= ts_ms
    let best = history
        .iter()
        .filter(|log| log.timestamp_ms <= ts_ms)
        .max_by_key(|log| log.timestamp_ms);
    match best {
        Some(log) => Ok(IcebergRefBinding {
            snapshot_id: log.snapshot_id,
            ref_name: None,
            ref_kind: None,
        }),
        None => Err(format!(
            "iceberg time travel: no snapshot at or before timestamp {ts_ms} in {fully_qualified_name}"
        )),
    }
}

#[cfg(test)]
mod split_ref_tests {
    use super::*;

    #[test]
    fn branch_suffix_is_stripped() {
        let parts = vec!["db".to_string(), "t".to_string(), "branch_dev".to_string()];
        let (stripped, suffix) = split_ref_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(suffix, Some(IcebergRefSuffix::Branch("dev".to_string())));
    }

    #[test]
    fn tag_suffix_is_stripped() {
        let parts = vec!["db".to_string(), "t".to_string(), "tag_v1".to_string()];
        let (stripped, suffix) = split_ref_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(suffix, Some(IcebergRefSuffix::Tag("v1".to_string())));
    }

    #[test]
    fn no_suffix_returns_original() {
        let parts = vec!["db".to_string(), "t".to_string()];
        let (stripped, suffix) = split_ref_suffix(&parts);
        assert_eq!(stripped, parts);
        assert_eq!(suffix, None);
    }

    #[test]
    fn bare_branch_prefix_without_name_is_ignored() {
        // "branch_" with no name after it should not be treated as a suffix
        let parts = vec!["db".to_string(), "branch_".to_string()];
        let (stripped, suffix) = split_ref_suffix(&parts);
        assert_eq!(stripped, parts);
        assert_eq!(suffix, None);
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use std::collections::HashMap;

    pub(crate) fn base_builder() -> TableMetadataBuilder {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
    }

    /// Build a minimal V2 TableMetadata with no snapshots.
    pub(crate) fn metadata_empty() -> iceberg::spec::TableMetadata {
        base_builder().build().unwrap().metadata
    }

    /// Build a TableMetadata with two snapshots; `snapshot_log` will contain both entries.
    ///
    /// Uses two separate builder phases (one per snapshot commit) so the iceberg builder
    /// does not classify the first snapshot as "intermediate" and strip it from the log.
    pub(crate) fn metadata_with_two_snapshots() -> iceberg::spec::TableMetadata {
        let snap1 = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(1_700_000_000_000)
            .with_sequence_number(1)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list("memory://test/table/metadata/snap-1.avro".to_string())
            .with_schema_id(0)
            .build();

        // Phase 1: commit snap1 as current.
        let meta1 = base_builder()
            .add_snapshot(snap1)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(
                    1,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        // Phase 2: continue from meta1, adding snap2 as the new current.
        let snap2 = Snapshot::builder()
            .with_snapshot_id(2)
            .with_timestamp_ms(1_700_000_001_000)
            .with_sequence_number(2)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list("memory://test/table/metadata/snap-2.avro".to_string())
            .with_schema_id(0)
            .build();

        meta1
            .into_builder(None)
            .add_snapshot(snap2)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(
                    2,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    /// Build a TableMetadata with one snapshot and a named branch.
    pub(crate) fn metadata_with_branch(branch_name: &str) -> iceberg::spec::TableMetadata {
        let snapshot_id = 1_i64;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000)
            .with_sequence_number(1)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list("memory://test/table/metadata/snap-1.avro".to_string())
            .with_schema_id(0)
            .build();

        let branch_ref = SnapshotReference::new(
            snapshot_id,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        );

        base_builder()
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .set_ref(branch_name, branch_ref)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    /// Build a TableMetadata with one snapshot and a named tag.
    pub(crate) fn metadata_with_tag(tag_name: &str) -> iceberg::spec::TableMetadata {
        let snapshot_id = 1_i64;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000)
            .with_sequence_number(1)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list("memory://test/table/metadata/snap-1.avro".to_string())
            .with_schema_id(0)
            .build();

        let tag_ref = SnapshotReference::new(
            snapshot_id,
            SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        );

        base_builder()
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .set_ref(tag_name, tag_ref)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val_num(n: &str) -> sqlparser::ast::Expr {
        sqlparser::ast::Expr::Value(
            sqlparser::ast::Value::Number(n.to_string(), false).with_empty_span(),
        )
    }

    fn val_str(s: &str) -> sqlparser::ast::Expr {
        sqlparser::ast::Expr::Value(
            sqlparser::ast::Value::SingleQuotedString(s.to_string()).with_empty_span(),
        )
    }

    #[test]
    fn ref_repr_branch() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("dev".into()),
            ref_kind: Some(IcebergRefKind::Branch),
        };
        assert_eq!(b.ref_repr(), "branch 'dev'");
    }

    #[test]
    fn ref_repr_tag() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("v1".into()),
            ref_kind: Some(IcebergRefKind::Tag),
        };
        assert_eq!(b.ref_repr(), "tag 'v1'");
    }

    #[test]
    fn ref_repr_snapshot_only() {
        let b = IcebergRefBinding {
            snapshot_id: 42,
            ref_name: None,
            ref_kind: None,
        };
        assert_eq!(b.ref_repr(), "snapshot 42");
    }

    #[test]
    fn display_matches_ref_repr() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("dev".into()),
            ref_kind: Some(IcebergRefKind::Branch),
        };
        assert_eq!(format!("{b}"), b.ref_repr());
    }

    // ---------------------------------------------------------------------------
    // resolve_read_binding tests
    // ---------------------------------------------------------------------------

    #[test]
    fn version_as_of_int_resolves_snapshot() {
        let metadata = test_utils::metadata_with_two_snapshots();
        let version = sqlparser::ast::TableVersion::VersionAsOf(val_num("2"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 2);
        assert!(binding.ref_name.is_none());
        assert!(binding.ref_kind.is_none());
    }

    #[test]
    fn version_as_of_string_resolves_branch() {
        let metadata = test_utils::metadata_with_branch("dev");
        let version = sqlparser::ast::TableVersion::VersionAsOf(val_str("dev"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
        assert_eq!(binding.ref_name.as_deref(), Some("dev"));
        assert_eq!(binding.ref_kind, Some(IcebergRefKind::Branch));
    }

    #[test]
    fn version_as_of_string_resolves_tag() {
        let metadata = test_utils::metadata_with_tag("v1.0");
        let version = sqlparser::ast::TableVersion::VersionAsOf(val_str("v1.0"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
        assert_eq!(binding.ref_name.as_deref(), Some("v1.0"));
        assert_eq!(binding.ref_kind, Some(IcebergRefKind::Tag));
    }

    #[test]
    fn unknown_ref_errors() {
        let metadata = test_utils::metadata_with_branch("dev");
        let version = sqlparser::ast::TableVersion::VersionAsOf(val_str("nope"));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.contains("ref 'nope' not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn version_as_of_unknown_snapshot_id_errors() {
        let metadata = test_utils::metadata_with_two_snapshots();
        let version = sqlparser::ast::TableVersion::VersionAsOf(val_num("99999"));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.contains("snapshot 99999 not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timestamp_as_of_epoch_ms_resolves() {
        let metadata = test_utils::metadata_with_two_snapshots();
        // snapshot 1 is at 1_700_000_000_000 ms, snapshot 2 at 1_700_000_001_000 ms
        // requesting at 1_700_000_000_500 should give snapshot 1
        let version = sqlparser::ast::TableVersion::TimestampAsOf(val_num("1700000000500"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
    }

    #[test]
    fn timestamp_as_of_too_early_errors() {
        let metadata = test_utils::metadata_with_two_snapshots();
        // before any snapshot
        let version = sqlparser::ast::TableVersion::TimestampAsOf(val_num("1000000000000"));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.contains("no snapshot at or before"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timestamp_as_of_rfc3339_string_resolves() {
        let metadata = test_utils::metadata_with_two_snapshots();
        // 2023-11-14T22:13:20Z = 1700000000 seconds = 1_700_000_000_000 ms (exactly snap1)
        let version = sqlparser::ast::TableVersion::TimestampAsOf(val_str("2023-11-14T22:13:20Z"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
    }

    #[test]
    fn expression_timestamp_rejected() {
        let metadata = test_utils::metadata_with_two_snapshots();
        // Use an identifier expression (not a literal) to trigger the fail-fast path
        let version = sqlparser::ast::TableVersion::TimestampAsOf(
            sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("some_var")),
        );
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.contains("phase 1 only accepts literal timestamp"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bigquery_function_syntax_rejected() {
        let metadata = test_utils::metadata_with_two_snapshots();
        // Use a nested value expression to represent the unsupported Function-style AT(...)
        let version = sqlparser::ast::TableVersion::Function(sqlparser::ast::Expr::Identifier(
            sqlparser::ast::Ident::new("AT"),
        ));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.contains("BigQuery AT(...) syntax is not supported"),
            "unexpected error: {err}"
        );
    }
}
