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

//! Resolve Iceberg time-travel clauses + DML branch suffixes into a single
//! `IcebergRefBinding` that the read and commit paths consume.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_parser::ast::{Expr, LiteralKind, TableVersion, TableVersionKind};

use crate::analyze_error::AnalyzeError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergRefKind {
    Branch,
    Tag,
}

/// Immutable ref and snapshot facts projected by the application from a
/// provider table metadata object.  Time-travel analysis needs neither a
/// catalog handle nor Iceberg's metadata representation, so those remain on
/// the application side of the compiler boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SqlIcebergRefMetadata {
    snapshot_ids: BTreeSet<i64>,
    history: Vec<SqlIcebergSnapshotLog>,
    refs: BTreeMap<String, SqlIcebergNamedRef>,
    current_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlIcebergSnapshotLog {
    pub(crate) snapshot_id: i64,
    pub(crate) timestamp_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlIcebergNamedRef {
    pub(crate) snapshot_id: i64,
    pub(crate) kind: IcebergRefKind,
}

impl SqlIcebergRefMetadata {
    pub(crate) fn new(
        snapshot_ids: impl IntoIterator<Item = i64>,
        history: Vec<SqlIcebergSnapshotLog>,
        refs: BTreeMap<String, SqlIcebergNamedRef>,
        current_snapshot_id: Option<i64>,
    ) -> Self {
        Self {
            snapshot_ids: snapshot_ids.into_iter().collect(),
            history,
            refs,
            current_snapshot_id,
        }
    }

    pub(crate) fn has_snapshot(&self, snapshot_id: i64) -> bool {
        self.snapshot_ids.contains(&snapshot_id)
    }

    pub(crate) fn named_ref(&self, name: &str) -> Option<&SqlIcebergNamedRef> {
        self.refs.get(name)
    }

    pub(crate) fn snapshot_at_or_before(&self, timestamp_ms: i64) -> Option<i64> {
        self.history
            .iter()
            .filter(|entry| entry.timestamp_ms <= timestamp_ms)
            .max_by_key(|entry| entry.timestamp_ms)
            .map(|entry| entry.snapshot_id)
    }

    pub(crate) fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
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
            && !name.is_empty()
        {
            return (
                parts[..parts.len() - 1].to_vec(),
                Some(IcebergRefSuffix::Branch(name.to_string())),
            );
        }
        if let Some(name) = last.strip_prefix("tag_")
            && !name.is_empty()
        {
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

/// Resolve a SQL `FOR VERSION/SYSTEM_TIME AS OF` clause into an `IcebergRefBinding`
/// against the given table metadata.
///
/// Resolution rules (Iceberg spec §4.2):
/// - `VERSION AS OF <integer>` → snapshot id; must exist in metadata.
/// - `VERSION AS OF '<string>'` → named ref (branch or tag); must exist in `metadata.refs()`.
/// - `FOR SYSTEM_TIME AS OF <integer>` → epoch-ms; finds the
///   snapshot with the largest `timestamp_ms` ≤ requested_ms from `metadata.history()`.
/// - `SYSTEM_TIME AS OF '<rfc3339-string>'` or `'<YYYY-MM-DD HH:MM:SS>'` → parsed to ms and
///   treated the same as an integer epoch-ms timestamp.
/// - Any other expression (function call, identifier, cast, …) → fail-fast error.
///
/// Phase-1 limitation: timestamp expressions must be literals (integer or quoted string).
/// Expression-level timestamps (e.g. `CURRENT_TIMESTAMP() - INTERVAL 1 HOUR`) are rejected.
/// An exact parsed request, before catalog admission freezes its snapshot.
/// Syntax locations stay on the original TableVersion, not in this memo key.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SqlTimeTravelSelection {
    SnapshotId(i64),
    NamedReference(String),
    TimestampMillis(i64),
}

pub fn parse_time_travel_selection(
    version: &TableVersion,
    fully_qualified_name: &str,
) -> Result<SqlTimeTravelSelection, AnalyzeError> {
    match version.kind {
        TableVersionKind::ForVersionAsOf => match &version.value {
            Expr::Literal(literal) => match &literal.kind {
                LiteralKind::Number(n) => {
                    let snapshot_id: i64 = n.parse().map_err(|_| {
                        AnalyzeError::invalid_literal(
                            format!("iceberg time travel: invalid snapshot id '{n}' for {fully_qualified_name}"),
                            literal.span,
                        )
                    })?;
                    Ok(SqlTimeTravelSelection::SnapshotId(snapshot_id))
                }
                LiteralKind::String(s) => Ok(SqlTimeTravelSelection::NamedReference(s.clone())),
                other => Err(AnalyzeError::invalid_literal(
                    format!(
                        "iceberg time travel: phase 1 only accepts literal snapshot id or ref name for VERSION AS OF; got value: {other:?}"
                    ),
                    literal.span,
                )),
            },
            other => Err(AnalyzeError::invalid_argument(
                format!(
                    "iceberg time travel: phase 1 only accepts literal snapshot id or ref name for VERSION AS OF; got expression: {other:?}"
                ),
                other.span(),
            )),
        },
        TableVersionKind::ForSystemTimeAsOf => {
            resolve_timestamp_expr(&version.value, fully_qualified_name)
                .map(SqlTimeTravelSelection::TimestampMillis)
        }
    }
}

pub fn resolve_read_binding(
    version: &TableVersion,
    metadata: &SqlIcebergRefMetadata,
    fully_qualified_name: &str,
) -> Result<IcebergRefBinding, AnalyzeError> {
    match parse_time_travel_selection(version, fully_qualified_name)? {
        SqlTimeTravelSelection::SnapshotId(snapshot_id) => {
            if !metadata.has_snapshot(snapshot_id) {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "iceberg time travel: snapshot {snapshot_id} not found in {fully_qualified_name}"
                    ),
                    version.value.span(),
                ));
            }
            Ok(IcebergRefBinding {
                snapshot_id,
                ref_name: None,
                ref_kind: None,
            })
        }
        SqlTimeTravelSelection::NamedReference(name) => {
            let entry = metadata.named_ref(&name).ok_or_else(|| {
                AnalyzeError::invalid_argument(
                    format!(
                        "iceberg time travel: ref '{name}' not found in {fully_qualified_name}"
                    ),
                    version.value.span(),
                )
            })?;
            Ok(IcebergRefBinding {
                snapshot_id: entry.snapshot_id,
                ref_name: Some(name),
                ref_kind: Some(entry.kind.clone()),
            })
        }
        SqlTimeTravelSelection::TimestampMillis(ts_ms) => {
            find_snapshot_at_or_before(metadata, ts_ms, fully_qualified_name, version.value.span())
        }
    }
}

/// Parse a timestamp literal expression into epoch milliseconds.
/// Phase 1: only accepts integer literals (epoch ms) or single-quoted strings
/// parseable as RFC 3339 or `%Y-%m-%d %H:%M:%S`.
fn resolve_timestamp_expr(expr: &Expr, fully_qualified_name: &str) -> Result<i64, AnalyzeError> {
    match expr {
        Expr::Literal(literal) => match &literal.kind {
            LiteralKind::Number(n) => n.parse::<i64>().map_err(|_| {
                AnalyzeError::invalid_literal(
                    format!(
                        "iceberg time travel: invalid epoch-ms value '{n}' for {fully_qualified_name}"
                    ),
                    literal.span,
                )
            }),
            LiteralKind::String(s) => parse_timestamp_string(s, fully_qualified_name, literal.span),
            other => Err(AnalyzeError::invalid_literal(
                format!(
                    "iceberg time travel: phase 1 only accepts literal timestamp; got value: {other:?}"
                ),
                literal.span,
            )),
        },
        other => Err(AnalyzeError::invalid_argument(
            format!(
                "iceberg time travel: phase 1 only accepts literal timestamp; got expression: {other:?}"
            ),
            other.span(),
        )),
    }
}

/// Parse a timestamp string as RFC 3339 or `%Y-%m-%d %H:%M:%S` (UTC assumed).
fn parse_timestamp_string(
    s: &str,
    fully_qualified_name: &str,
    span: novarocks_parser::Span,
) -> Result<i64, AnalyzeError> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    // Try RFC 3339 / ISO 8601 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc).timestamp_millis());
    }
    // Fallback: `YYYY-MM-DD HH:MM:SS`
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(ndt.and_utc().timestamp_millis());
    }
    Err(AnalyzeError::invalid_literal(
        format!(
            "iceberg time travel: cannot parse timestamp '{s}' for {fully_qualified_name}; expected RFC 3339 or 'YYYY-MM-DD HH:MM:SS'"
        ),
        span,
    ))
}

/// Find the latest snapshot whose `timestamp_ms` ≤ `ts_ms` in the snapshot log.
fn find_snapshot_at_or_before(
    metadata: &SqlIcebergRefMetadata,
    ts_ms: i64,
    fully_qualified_name: &str,
    span: novarocks_parser::Span,
) -> Result<IcebergRefBinding, AnalyzeError> {
    match metadata.snapshot_at_or_before(ts_ms) {
        Some(snapshot_id) => Ok(IcebergRefBinding {
            snapshot_id,
            ref_name: None,
            ref_kind: None,
        }),
        None => Err(AnalyzeError::invalid_argument(
            format!(
                "iceberg time travel: no snapshot at or before timestamp {ts_ms} in {fully_qualified_name}"
            ),
            span,
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
mod tests {
    use super::*;
    use novarocks_parser::{
        Span,
        ast::{Ident, Literal},
    };

    fn metadata_empty() -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::default()
    }

    fn metadata_with_two_snapshots() -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::new(
            [1, 2],
            vec![
                SqlIcebergSnapshotLog {
                    snapshot_id: 1,
                    timestamp_ms: 1_700_000_000_000,
                },
                SqlIcebergSnapshotLog {
                    snapshot_id: 2,
                    timestamp_ms: 1_700_000_001_000,
                },
            ],
            BTreeMap::new(),
            Some(2),
        )
    }

    fn metadata_with_ref(name: &str, kind: IcebergRefKind) -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::new(
            [1],
            vec![SqlIcebergSnapshotLog {
                snapshot_id: 1,
                timestamp_ms: 1_700_000_000_000,
            }],
            BTreeMap::from([(
                name.to_string(),
                SqlIcebergNamedRef {
                    snapshot_id: 1,
                    kind,
                },
            )]),
            Some(1),
        )
    }

    fn span() -> Span {
        Span::new(0, 0)
    }

    fn val_num(n: &str) -> Expr {
        Expr::Literal(Literal {
            kind: LiteralKind::Number(n.to_string()),
            span: span(),
        })
    }

    fn val_str(s: &str) -> Expr {
        Expr::Literal(Literal {
            kind: LiteralKind::String(s.to_string()),
            span: span(),
        })
    }

    fn version(kind: TableVersionKind, value: Expr) -> TableVersion {
        TableVersion {
            kind,
            value,
            span: span(),
        }
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
        let metadata = metadata_with_two_snapshots();
        let version = version(TableVersionKind::ForVersionAsOf, val_num("2"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 2);
        assert!(binding.ref_name.is_none());
        assert!(binding.ref_kind.is_none());
    }

    #[test]
    fn sqlx2_resolution_iceberg_ref_input_is_provider_neutral() {
        let metadata = metadata_with_ref("dev", IcebergRefKind::Branch);
        let version = version(TableVersionKind::ForVersionAsOf, val_str("dev"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t")
            .expect("resolve frozen SQL ref facts");
        assert_eq!(binding.snapshot_id, 1);
        assert_eq!(binding.ref_kind, Some(IcebergRefKind::Branch));
    }

    #[test]
    fn version_as_of_string_resolves_branch() {
        let metadata = metadata_with_ref("dev", IcebergRefKind::Branch);
        let version = version(TableVersionKind::ForVersionAsOf, val_str("dev"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
        assert_eq!(binding.ref_name.as_deref(), Some("dev"));
        assert_eq!(binding.ref_kind, Some(IcebergRefKind::Branch));
    }

    #[test]
    fn version_as_of_string_resolves_tag() {
        let metadata = metadata_with_ref("v1.0", IcebergRefKind::Tag);
        let version = version(TableVersionKind::ForVersionAsOf, val_str("v1.0"));
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
        assert_eq!(binding.ref_name.as_deref(), Some("v1.0"));
        assert_eq!(binding.ref_kind, Some(IcebergRefKind::Tag));
    }

    #[test]
    fn unknown_ref_errors() {
        let metadata = metadata_with_ref("dev", IcebergRefKind::Branch);
        let version = version(TableVersionKind::ForVersionAsOf, val_str("nope"));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.message().contains("ref 'nope' not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn version_as_of_unknown_snapshot_id_errors() {
        let metadata = metadata_with_two_snapshots();
        let version = version(TableVersionKind::ForVersionAsOf, val_num("99999"));
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.message().contains("snapshot 99999 not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timestamp_as_of_epoch_ms_resolves() {
        let metadata = metadata_with_two_snapshots();
        // snapshot 1 is at 1_700_000_000_000 ms, snapshot 2 at 1_700_000_001_000 ms
        // requesting at 1_700_000_000_500 should give snapshot 1
        let version = version(
            TableVersionKind::ForSystemTimeAsOf,
            val_num("1700000000500"),
        );
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
    }

    #[test]
    fn timestamp_as_of_too_early_errors() {
        let metadata = metadata_with_two_snapshots();
        // before any snapshot
        let version = version(
            TableVersionKind::ForSystemTimeAsOf,
            val_num("1000000000000"),
        );
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.message().contains("no snapshot at or before"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn timestamp_as_of_rfc3339_string_resolves() {
        let metadata = metadata_with_two_snapshots();
        // 2023-11-14T22:13:20Z = 1700000000 seconds = 1_700_000_000_000 ms (exactly snap1)
        let version = version(
            TableVersionKind::ForSystemTimeAsOf,
            val_str("2023-11-14T22:13:20Z"),
        );
        let binding = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap();
        assert_eq!(binding.snapshot_id, 1);
    }

    #[test]
    fn expression_timestamp_rejected() {
        let metadata = metadata_with_two_snapshots();
        // Use an identifier expression (not a literal) to trigger the fail-fast path
        let version = version(
            TableVersionKind::ForSystemTimeAsOf,
            Expr::Identifier(Ident {
                value: "some_var".to_string(),
                quoted: false,
                quote_style: None,
                span: span(),
            }),
        );
        let err = resolve_read_binding(&version, &metadata, "cat.ns.t").unwrap_err();
        assert!(
            err.message()
                .contains("phase 1 only accepts literal timestamp"),
            "unexpected error: {err}"
        );
    }
}
