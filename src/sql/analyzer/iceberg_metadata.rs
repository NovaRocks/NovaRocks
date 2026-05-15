//! Resolve the trailing `__nr_meta_<type>__` suffix that the parser-level
//! pre-parse rewrites `<tbl>$<metatype>` into.
//!
//! Mirrors `iceberg_ref::split_ref_suffix` for branch/tag.

use arrow::datatypes::DataType;

use crate::connector::iceberg::IcebergMetadataTableType;

/// Inspect the trailing identifier part of a qualified name and, if it
/// matches `__nr_meta_<type>__`, return the parts with the suffix stripped
/// plus the parsed metadata-table type.
pub fn split_metadata_suffix(parts: &[String]) -> (Vec<String>, Option<IcebergMetadataTableType>) {
    if let Some(last) = parts.last()
        && let Some(inner) = last
            .strip_prefix("__nr_meta_")
            .and_then(|s| s.strip_suffix("__"))
            && let Ok(ty) = IcebergMetadataTableType::parse(inner) {
                return (parts[..parts.len() - 1].to_vec(), Some(ty));
            }
    (parts.to_vec(), None)
}

#[derive(Clone, Debug)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl MetadataColumn {
    fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }
}

/// Fixed analyzer-level column schema for each Iceberg metadata table.
///
/// Wire types match `IcebergMetadataScanOp::build_*_array` in
/// `src/connector/iceberg/metadata.rs`. Logical formatting (e.g. converting
/// the `committed_at` Int64-of-micros into a TIMESTAMPTZ display) is FE's
/// responsibility; the analyzer surfaces the underlying Arrow type.
pub fn metadata_table_schema(ty: IcebergMetadataTableType) -> Vec<MetadataColumn> {
    use IcebergMetadataTableType as T;
    match ty {
        T::Snapshots => vec![
            MetadataColumn::new("committed_at", DataType::Int64, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("parent_id", DataType::Int64, true),
            MetadataColumn::new("operation", DataType::Utf8, true),
            MetadataColumn::new("manifest_list", DataType::Utf8, false),
            // TODO: BE actually emits this column as Map<Utf8,Utf8>; the analyzer
            // surfaces it as Utf8 for now so users can SELECT it without the
            // analyzer rejecting unknown Map operations. Refining this requires
            // teaching the scope/expr layer about Map-typed metadata columns
            // and is tracked alongside the broader metadata-table follow-ups.
            MetadataColumn::new("summary", DataType::Utf8, false),
        ],
        T::History => vec![
            MetadataColumn::new("made_current_at", DataType::Int64, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("parent_id", DataType::Int64, true),
            MetadataColumn::new("is_current_ancestor", DataType::Boolean, false),
        ],
        T::Refs => vec![
            MetadataColumn::new("name", DataType::Utf8, false),
            MetadataColumn::new("type", DataType::Utf8, false),
            MetadataColumn::new("snapshot_id", DataType::Int64, false),
            MetadataColumn::new("max_reference_age_in_ms", DataType::Int64, true),
            MetadataColumn::new("min_snapshots_to_keep", DataType::Int32, true),
            MetadataColumn::new("max_snapshot_age_in_ms", DataType::Int64, true),
        ],
        T::Partitions => vec![
            // The dynamic `partition` struct column is appended at lowering
            // time when the table's partition spec is available (Task A4).
            // Analyzer surfaces only the count columns.
            MetadataColumn::new("record_count", DataType::Int64, false),
            MetadataColumn::new("file_count", DataType::Int64, false),
            MetadataColumn::new("position_delete_file_count", DataType::Int64, true),
            MetadataColumn::new("equality_delete_file_count", DataType::Int64, true),
        ],
        T::Files | T::Manifests | T::LogicalIcebergMetadata => {
            // Out of scope for this PR (D6). Returning an empty vec causes
            // resolve_from to surface a "no such column" error if the user
            // tries to use these (defense in depth — the parser-level
            // whitelist already rejects them).
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_suffix_is_stripped() {
        let parts = vec![
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_snapshots__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(ty, Some(IcebergMetadataTableType::Snapshots));
    }

    #[test]
    fn three_part_qualified_name_works() {
        let parts = vec![
            "ice".to_string(),
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_history__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(
            stripped,
            vec!["ice".to_string(), "db".to_string(), "t".to_string()]
        );
        assert_eq!(ty, Some(IcebergMetadataTableType::History));
    }

    #[test]
    fn refs_and_partitions_round_trip() {
        for (suffix, expected) in [
            ("__nr_meta_refs__", IcebergMetadataTableType::Refs),
            (
                "__nr_meta_partitions__",
                IcebergMetadataTableType::Partitions,
            ),
        ] {
            let parts = vec!["t".to_string(), suffix.to_string()];
            let (_, ty) = split_metadata_suffix(&parts);
            assert_eq!(ty, Some(expected));
        }
    }

    #[test]
    fn unrecognised_metatype_returns_none() {
        let parts = vec!["t".to_string(), "__nr_meta_xyz__".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }

    #[test]
    fn no_suffix_passthrough() {
        let parts = vec!["db".to_string(), "t".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }

    #[test]
    fn metadata_table_schema_snapshots_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Snapshots);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_history_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::History);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "made_current_at",
                "snapshot_id",
                "parent_id",
                "is_current_ancestor",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_refs_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Refs);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "name",
                "type",
                "snapshot_id",
                "max_reference_age_in_ms",
                "min_snapshots_to_keep",
                "max_snapshot_age_in_ms",
            ]
        );
    }

    #[test]
    fn metadata_table_schema_partitions_has_expected_columns() {
        let cols = metadata_table_schema(IcebergMetadataTableType::Partitions);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"record_count"));
        assert!(names.contains(&"file_count"));
        assert!(names.contains(&"position_delete_file_count"));
        assert!(names.contains(&"equality_delete_file_count"));
    }

    #[test]
    fn out_of_scope_metatypes_produce_empty_schema() {
        for ty in [
            IcebergMetadataTableType::Files,
            IcebergMetadataTableType::Manifests,
            IcebergMetadataTableType::LogicalIcebergMetadata,
        ] {
            assert!(
                metadata_table_schema(ty.clone()).is_empty(),
                "{ty:?} schema must be empty"
            );
        }
    }
}
