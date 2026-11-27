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

//! Frozen Trino-aligned schemas for the Iceberg metadata relations.
//!
//! Every column name, ordinal, and type below is a user-visible contract. It
//! mirrors Trino's `FilesTable` / `EntriesTable` / `SnapshotsTable` /
//! `HistoryTable` / `RefsTable` / `ManifestsTable` / `PartitionsView` exactly
//! so a query written against Trino keeps its meaning here. The closed worker
//! set is the same one the wire carries (`IcebergSystemTableType`): FILES,
//! ENTRIES, SNAPSHOTS, HISTORY, REFS, MANIFESTS. `$partitions` is not a worker
//! type; it is a view over the same pinned `$files` snapshot.
//!
//! There is no alias, no `ALL_*` variant, and no unknown variant: an
//! unrecognized `$suffix` is refused by
//! [`SqlMetadataTableKind::parse`](crate::planner::table::SqlMetadataTableKind).

use arrow::datatypes::{DataType, Field, Fields};
use novarocks_type_contract::{TemporalPrecision, TemporalType};
use novarocks_types::schema::SqlType;
use std::sync::Arc;

use crate::planner::table::SqlMetadataTableKind;

#[derive(Clone, Debug)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Complete SQL declaration when the Arrow carrier alone is ambiguous.
    pub logical_type: Option<SqlType>,
}

impl MetadataColumn {
    fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
            logical_type: None,
        }
    }

    fn timestamp_with_time_zone(name: &str) -> Self {
        let declaration = TemporalType::TimestampWithTimeZone(TemporalPrecision::MICROSECONDS);
        Self {
            name: name.to_string(),
            data_type: declaration.carrier(),
            nullable: false,
            logical_type: Some(SqlType::Temporal(declaration)),
        }
    }

    /// Trino `JSON` retains its nominal declaration over native JSON bytes.
    fn json(name: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: DataType::LargeBinary,
            nullable: true,
            logical_type: Some(SqlType::Json),
        }
    }
}

// ---------------------------------------------------------------------------
// Trino type spellings
// ---------------------------------------------------------------------------

/// Trino `INTEGER`.
fn integer() -> DataType {
    DataType::Int32
}

/// Trino `BIGINT`.
fn bigint() -> DataType {
    DataType::Int64
}

/// Trino `VARCHAR`.
fn varchar() -> DataType {
    DataType::Utf8
}

/// Trino `VARBINARY`.
fn varbinary() -> DataType {
    DataType::Binary
}

/// Trino `BOOLEAN`.
fn boolean() -> DataType {
    DataType::Boolean
}

/// Trino `MAP(key, value)`.
///
/// Field names (`entries` / `key` / `value`) MUST mirror the Iceberg metadata
/// reader's `MapFieldNames`: the scan-op builders produce their `Map` columns
/// through that naming, and `RecordBatch::try_new` compares `Field` names
/// structurally, so any divergence here fails the column-type check at
/// execution time rather than at compile time.
fn map_of(key: DataType, value: DataType) -> DataType {
    let entries = DataType::Struct(
        vec![
            Arc::new(Field::new("key", key, false)),
            Arc::new(Field::new("value", value, true)),
        ]
        .into(),
    );
    DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
}

/// Trino `ARRAY(item)`. `item` is Arrow's default list-child field name.
fn array_of(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, true)))
}

/// Trino `ROW(...)`.
fn row_of(fields: Vec<Field>) -> DataType {
    DataType::Struct(fields.into_iter().map(Arc::new).collect::<Fields>())
}

fn field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

// ---------------------------------------------------------------------------
// Schema-derived types
// ---------------------------------------------------------------------------

/// The metadata-relation types that the frozen contract cannot know on its
/// own because they come from the Iceberg table's schema and partition specs.
///
/// Each one is genuinely optional in Trino: a table with no partition field
/// has no `partition` column at all, and a table with no boundable column has
/// no `lower_bounds`/`upper_bounds`. Absent means *the column is omitted*, not
/// "substitute a placeholder type" — a guessed type would be a silent
/// downgrade of a user-visible contract.
#[derive(Clone, Debug, Default)]
pub struct IcebergMetadataDerivedTypes {
    partition: Option<DataType>,
    bounds: Option<DataType>,
}

impl IcebergMetadataDerivedTypes {
    /// The derivation for a table with no partition field, no boundable
    /// column, and no metric column. Every optional column is omitted.
    pub fn none() -> Self {
        Self::try_new(None, None).expect("an empty derivation is always valid")
    }

    /// Build the derivation from the caller's already-resolved Iceberg facts.
    ///
    /// * `partition` — ROW of the unified partition struct across every
    ///   partition spec the table has ever had. Drives `$files.partition`,
    ///   `$entries.data_file.partition`, and `$partitions.partition`.
    /// * `bounds` — ROW whose field *names* are Iceberg field ids and whose
    ///   field types are the target column types. Drives `$files.lower_bounds`
    ///   and `$files.upper_bounds`. `$entries` deliberately does not use it:
    ///   its nested bounds stay `MAP(INTEGER, VARCHAR)`, matching Trino.
    ///
    /// Every supplied type must be a ROW; anything else is a caller bug and is
    /// rejected rather than coerced.
    pub fn try_new(partition: Option<DataType>, bounds: Option<DataType>) -> Result<Self, String> {
        for (label, candidate) in [("partition", &partition), ("bounds", &bounds)] {
            if let Some(data_type) = candidate
                && !matches!(data_type, DataType::Struct(_))
            {
                return Err(format!(
                    "iceberg metadata derived type `{label}` must be a ROW, got {data_type:?}"
                ));
            }
        }
        Ok(Self { partition, bounds })
    }

    pub fn partition(&self) -> Option<&DataType> {
        self.partition.as_ref()
    }

    pub fn bounds(&self) -> Option<&DataType> {
        self.bounds.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Frozen relation schemas
// ---------------------------------------------------------------------------

/// Frozen column schema for one Iceberg metadata relation, for a table that
/// contributes no schema-derived column.
///
/// Use [`metadata_table_schema_with`] whenever the caller has already resolved
/// the table's partition struct or bounds ROW: this entry point cannot know
/// them and therefore omits `partition`, `lower_bounds`, and `upper_bounds`.
pub fn metadata_table_schema(kind: SqlMetadataTableKind) -> Vec<MetadataColumn> {
    metadata_table_schema_with(kind, &IcebergMetadataDerivedTypes::none())
}

/// Frozen column schema for one Iceberg metadata relation, including the
/// schema-derived columns the caller was able to resolve.
///
/// The match is exhaustive on purpose: adding a metadata relation must be a
/// compile error here, never a silently missing schema.
pub fn metadata_table_schema_with(
    kind: SqlMetadataTableKind,
    derived: &IcebergMetadataDerivedTypes,
) -> Vec<MetadataColumn> {
    match kind {
        SqlMetadataTableKind::Files => files_columns(derived),
        SqlMetadataTableKind::Entries => entries_columns(derived),
        SqlMetadataTableKind::Snapshots => snapshots_columns(),
        SqlMetadataTableKind::History => history_columns(),
        SqlMetadataTableKind::Refs => refs_columns(),
        SqlMetadataTableKind::Manifests => manifests_columns(),
        SqlMetadataTableKind::Partitions => partitions_columns(derived),
    }
}

/// Trino `$files`: 27 ordered columns.
///
/// Nullability follows Iceberg's own manifest schema: only the fields Iceberg
/// declares required on `data_file` are non-nullable. `manifest_location` is
/// non-nullable because a `$files` row only exists while some manifest is
/// being scanned, so its location is always known.
fn files_columns(derived: &IcebergMetadataDerivedTypes) -> Vec<MetadataColumn> {
    let mut columns = vec![
        MetadataColumn::new("content", integer(), false),
        MetadataColumn::new("file_path", varchar(), false),
        MetadataColumn::new("file_format", varchar(), false),
        MetadataColumn::new("spec_id", integer(), false),
    ];
    if let Some(partition) = derived.partition() {
        columns.push(MetadataColumn::new("partition", partition.clone(), true));
    }
    columns.extend([
        MetadataColumn::new("record_count", bigint(), false),
        MetadataColumn::new("file_size_in_bytes", bigint(), false),
        MetadataColumn::new("column_sizes", map_of(integer(), bigint()), true),
        MetadataColumn::new("value_counts", map_of(integer(), bigint()), true),
        MetadataColumn::new("null_value_counts", map_of(integer(), bigint()), true),
        MetadataColumn::new("nan_value_counts", map_of(integer(), bigint()), true),
    ]);
    // `lower_bounds`/`upper_bounds` decode to a typed ROW keyed by Iceberg
    // field id. They must never fall back to a binary or UTF-8 map: a user
    // comparing `lower_bounds."3" > DATE '2026-01-01'` needs the target type,
    // and a byte-array or string carrier would compare in the wrong domain
    // while still returning rows.
    if let Some(bounds) = derived.bounds() {
        columns.push(MetadataColumn::new("lower_bounds", bounds.clone(), true));
        columns.push(MetadataColumn::new("upper_bounds", bounds.clone(), true));
    }
    columns.extend([
        MetadataColumn::new("key_metadata", varbinary(), true),
        MetadataColumn::new("split_offsets", array_of(bigint()), true),
        MetadataColumn::new("equality_ids", array_of(integer()), true),
        MetadataColumn::new("sort_order_id", integer(), true),
        MetadataColumn::json("readable_metrics"),
        MetadataColumn::new("added_snapshot_id", bigint(), true),
        MetadataColumn::new("file_sequence_number", bigint(), true),
        MetadataColumn::new("data_sequence_number", bigint(), true),
        MetadataColumn::new("referenced_data_file", varchar(), true),
        MetadataColumn::new("pos", bigint(), true),
        MetadataColumn::new("manifest_location", varchar(), false),
        MetadataColumn::new("first_row_id", bigint(), true),
        MetadataColumn::new("content_offset", bigint(), true),
        MetadataColumn::new("content_size_in_bytes", bigint(), true),
    ]);
    columns
}

/// Trino `$entries`: four manifest-entry columns, the nested `data_file` ROW,
/// and `readable_metrics`.
///
/// The legacy NovaRocks shape flattened `data_file` into the top level (a
/// `$files` superset). That is forbidden now: `$entries` and `$files` are
/// different relations in Trino, and flattening makes
/// `SELECT data_file.file_path FROM t$entries` — the query every Trino user
/// writes — fail with an unknown column.
fn entries_columns(derived: &IcebergMetadataDerivedTypes) -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::new("status", integer(), false),
        MetadataColumn::new("snapshot_id", bigint(), true),
        MetadataColumn::new("sequence_number", bigint(), true),
        MetadataColumn::new("file_sequence_number", bigint(), true),
        MetadataColumn::new("data_file", entries_data_file_row(derived), false),
        MetadataColumn::json("readable_metrics"),
    ]
}

/// The `$entries.data_file` ROW.
///
/// Its `lower_bounds`/`upper_bounds` are `MAP(INTEGER, VARCHAR)`, not the
/// typed ROW `$files` uses. That asymmetry is Trino's, and it is deliberate:
/// `$entries` reproduces the raw manifest entry, where bounds are still
/// undecoded per-field byte buffers rendered as text.
fn entries_data_file_row(derived: &IcebergMetadataDerivedTypes) -> DataType {
    let mut fields = vec![
        field("content", integer(), false),
        field("file_path", varchar(), false),
        field("file_format", varchar(), false),
        field("spec_id", integer(), false),
    ];
    if let Some(partition) = derived.partition() {
        fields.push(field("partition", partition.clone(), true));
    }
    fields.extend([
        field("record_count", bigint(), false),
        field("file_size_in_bytes", bigint(), false),
        field("column_sizes", map_of(integer(), bigint()), true),
        field("value_counts", map_of(integer(), bigint()), true),
        field("null_value_counts", map_of(integer(), bigint()), true),
        field("nan_value_counts", map_of(integer(), bigint()), true),
        field("lower_bounds", map_of(integer(), varchar()), true),
        field("upper_bounds", map_of(integer(), varchar()), true),
        field("key_metadata", varbinary(), true),
        field("split_offsets", array_of(bigint()), true),
        field("equality_ids", array_of(integer()), true),
        field("sort_order_id", integer(), true),
    ]);
    row_of(fields)
}

/// Trino `$snapshots`.
fn snapshots_columns() -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::timestamp_with_time_zone("committed_at"),
        MetadataColumn::new("snapshot_id", bigint(), false),
        MetadataColumn::new("parent_id", bigint(), true),
        MetadataColumn::new("operation", varchar(), true),
        MetadataColumn::new("manifest_list", varchar(), false),
        // A real MAP, not a rendered string. The whole point of this column is
        // `summary['total-records']`; a VARCHAR forces users to string-parse
        // the map back out. Nullable because the Iceberg spec only requires a
        // summary from v2 onward.
        MetadataColumn::new("summary", map_of(varchar(), varchar()), true),
    ]
}

/// Trino `$history`.
fn history_columns() -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::timestamp_with_time_zone("made_current_at"),
        MetadataColumn::new("snapshot_id", bigint(), false),
        MetadataColumn::new("parent_id", bigint(), true),
        MetadataColumn::new("is_current_ancestor", boolean(), false),
    ]
}

/// Trino `$refs`.
fn refs_columns() -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::new("name", varchar(), false),
        MetadataColumn::new("type", varchar(), false),
        MetadataColumn::new("snapshot_id", bigint(), false),
        MetadataColumn::new("max_reference_age_in_ms", bigint(), true),
        MetadataColumn::new("min_snapshots_to_keep", integer(), true),
        MetadataColumn::new("max_snapshot_age_in_ms", bigint(), true),
    ]
}

/// Trino `$manifests`.
///
/// The count columns interleave file counts with row counts
/// (`added_data_files_count`, `added_rows_count`, `existing_...`) rather than
/// grouping all file counts before all row counts. That is Trino's order and
/// therefore the order a ported `SELECT *` expects.
fn manifests_columns() -> Vec<MetadataColumn> {
    vec![
        MetadataColumn::new("content", integer(), false),
        MetadataColumn::new("path", varchar(), false),
        MetadataColumn::new("length", bigint(), false),
        MetadataColumn::new("partition_spec_id", integer(), false),
        MetadataColumn::new("added_snapshot_id", bigint(), true),
        MetadataColumn::new("added_data_files_count", integer(), true),
        MetadataColumn::new("added_rows_count", bigint(), true),
        MetadataColumn::new("existing_data_files_count", integer(), true),
        MetadataColumn::new("existing_rows_count", bigint(), true),
        MetadataColumn::new("deleted_data_files_count", integer(), true),
        MetadataColumn::new("deleted_rows_count", bigint(), true),
        MetadataColumn::new(
            "partition_summaries",
            array_of(row_of(vec![
                field("contains_null", boolean(), true),
                field("contains_nan", boolean(), true),
                field("lower_bound", varchar(), true),
                field("upper_bound", varchar(), true),
            ])),
            true,
        ),
    ]
}

/// SQL partition aggregation over data-file rows; per-column metrics are not exposed.
///
/// SQL COMPATIBILITY CHANGE: the previous NovaRocks shape carried
/// `position_delete_file_count` and `equality_delete_file_count`. Both are
/// removed. They were a NovaRocks invention with no Trino counterpart, and
/// keeping them would have made `$partitions` the one relation where a ported
/// Trino query silently sees a different row shape. The information is not
/// lost: delete-file content is a per-file fact, so it belongs to `$files`,
/// where `content` distinguishes data (0), position deletes (1), and equality
/// deletes (2). The replacement for
/// `SELECT position_delete_file_count FROM t$partitions` is
/// `SELECT count(*) FROM t$files WHERE content = 1` grouped by `partition`.
fn partitions_columns(derived: &IcebergMetadataDerivedTypes) -> Vec<MetadataColumn> {
    let mut columns = Vec::with_capacity(5);
    if let Some(partition) = derived.partition() {
        columns.push(MetadataColumn::new("partition", partition.clone(), true));
    }
    columns.extend([
        MetadataColumn::new("record_count", bigint(), true),
        MetadataColumn::new("file_count", bigint(), false),
        MetadataColumn::new("total_size", bigint(), true),
    ]);
    columns
}

// ---------------------------------------------------------------------------
// Refusals
#[cfg(test)]
mod tests {
    use super::*;

    /// SQL name of a relation, for assertion messages.
    fn metadata_relation_suffix(kind: SqlMetadataTableKind) -> &'static str {
        match kind {
            SqlMetadataTableKind::Files => "$files",
            SqlMetadataTableKind::Entries => "$entries",
            SqlMetadataTableKind::Snapshots => "$snapshots",
            SqlMetadataTableKind::History => "$history",
            SqlMetadataTableKind::Refs => "$refs",
            SqlMetadataTableKind::Manifests => "$manifests",
            SqlMetadataTableKind::Partitions => "$partitions",
        }
    }

    fn names(columns: &[MetadataColumn]) -> Vec<&str> {
        columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn column<'a>(columns: &'a [MetadataColumn], name: &str) -> &'a MetadataColumn {
        columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("missing column {name}"))
    }

    /// A partition ROW as a real caller would derive it from a partition spec.
    fn partition_row() -> DataType {
        row_of(vec![
            field("bucket_id", integer(), true),
            field("event_day", DataType::Date32, true),
        ])
    }

    /// A bounds ROW keyed by Iceberg field id, values typed as the target
    /// columns. Field *names* are the ids.
    fn bounds_row() -> DataType {
        row_of(vec![
            field("1", bigint(), true),
            field("2", varchar(), true),
            field("3", DataType::Date32, true),
        ])
    }

    fn full_derivation() -> IcebergMetadataDerivedTypes {
        IcebergMetadataDerivedTypes::try_new(Some(partition_row()), Some(bounds_row()))
            .expect("ROW derivations")
    }

    const ALL_RELATIONS: [SqlMetadataTableKind; 7] = [
        SqlMetadataTableKind::Files,
        SqlMetadataTableKind::Entries,
        SqlMetadataTableKind::Snapshots,
        SqlMetadataTableKind::History,
        SqlMetadataTableKind::Refs,
        SqlMetadataTableKind::Manifests,
        SqlMetadataTableKind::Partitions,
    ];

    // -----------------------------------------------------------------
    // $files
    // -----------------------------------------------------------------

    #[test]
    fn files_schema_freezes_the_twenty_seven_trino_columns_in_order() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Files, &full_derivation());
        assert_eq!(
            names(&columns),
            vec![
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "partition",
                "record_count",
                "file_size_in_bytes",
                "column_sizes",
                "value_counts",
                "null_value_counts",
                "nan_value_counts",
                "lower_bounds",
                "upper_bounds",
                "key_metadata",
                "split_offsets",
                "equality_ids",
                "sort_order_id",
                "readable_metrics",
                "added_snapshot_id",
                "file_sequence_number",
                "data_sequence_number",
                "referenced_data_file",
                "pos",
                "manifest_location",
                "first_row_id",
                "content_offset",
                "content_size_in_bytes",
            ]
        );
        assert_eq!(columns.len(), 27);
    }

    #[test]
    fn files_schema_freezes_column_types() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Files, &full_derivation());
        let expected: Vec<(&str, DataType)> = vec![
            ("content", integer()),
            ("file_path", varchar()),
            ("file_format", varchar()),
            ("spec_id", integer()),
            ("partition", partition_row()),
            ("record_count", bigint()),
            ("file_size_in_bytes", bigint()),
            ("column_sizes", map_of(integer(), bigint())),
            ("value_counts", map_of(integer(), bigint())),
            ("null_value_counts", map_of(integer(), bigint())),
            ("nan_value_counts", map_of(integer(), bigint())),
            ("lower_bounds", bounds_row()),
            ("upper_bounds", bounds_row()),
            ("key_metadata", varbinary()),
            ("split_offsets", array_of(bigint())),
            ("equality_ids", array_of(integer())),
            ("sort_order_id", integer()),
            ("readable_metrics", DataType::LargeBinary),
            ("added_snapshot_id", bigint()),
            ("file_sequence_number", bigint()),
            ("data_sequence_number", bigint()),
            ("referenced_data_file", varchar()),
            ("pos", bigint()),
            ("manifest_location", varchar()),
            ("first_row_id", bigint()),
            ("content_offset", bigint()),
            ("content_size_in_bytes", bigint()),
        ];
        for (name, data_type) in expected {
            assert_eq!(column(&columns, name).data_type, data_type, "{name}");
        }
    }

    #[test]
    fn files_schema_freezes_nullability() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Files, &full_derivation());
        let required = [
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "record_count",
            "file_size_in_bytes",
            "manifest_location",
        ];
        for candidate in &columns {
            let expected_nullable = !required.contains(&candidate.name.as_str());
            assert_eq!(
                candidate.nullable, expected_nullable,
                "{} nullability",
                candidate.name
            );
        }
    }

    #[test]
    fn files_bounds_are_typed_rows_not_binary_or_utf8_maps() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Files, &full_derivation());
        for name in ["lower_bounds", "upper_bounds"] {
            let bounds = &column(&columns, name).data_type;
            let DataType::Struct(fields) = bounds else {
                panic!("{name} must be a ROW, got {bounds:?}");
            };
            // Field names are Iceberg field ids; values are the target types.
            assert_eq!(
                fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
                vec!["1", "2", "3"]
            );
            assert_eq!(fields[0].data_type(), &bigint());
            assert_eq!(fields[1].data_type(), &varchar());
            assert_eq!(fields[2].data_type(), &DataType::Date32);
            assert_ne!(bounds, &map_of(integer(), varbinary()));
            assert_ne!(bounds, &map_of(integer(), varchar()));
        }
    }

    #[test]
    fn readable_metrics_has_the_exact_nominal_json_carrier() {
        for kind in [SqlMetadataTableKind::Files, SqlMetadataTableKind::Entries] {
            let columns = metadata_table_schema_with(kind, &full_derivation());
            let metrics = column(&columns, "readable_metrics");
            assert_eq!(metrics.logical_type, Some(SqlType::Json));
            assert_eq!(metrics.data_type, DataType::LargeBinary);
            assert!(metrics.nullable);
        }
    }

    #[test]
    fn files_omits_schema_derived_columns_when_the_table_has_none() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Files);
        let present = names(&columns);
        for omitted in ["partition", "lower_bounds", "upper_bounds"] {
            assert!(!present.contains(&omitted), "{omitted} must be omitted");
        }
        assert_eq!(columns.len(), 24);
    }

    // -----------------------------------------------------------------
    // $entries
    // -----------------------------------------------------------------

    #[test]
    fn entries_schema_freezes_top_level_columns() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Entries, &full_derivation());
        assert_eq!(
            names(&columns),
            vec![
                "status",
                "snapshot_id",
                "sequence_number",
                "file_sequence_number",
                "data_file",
                "readable_metrics",
            ]
        );
        assert_eq!(column(&columns, "status").data_type, integer());
        assert_eq!(column(&columns, "snapshot_id").data_type, bigint());
        assert_eq!(column(&columns, "sequence_number").data_type, bigint());
        assert_eq!(column(&columns, "file_sequence_number").data_type, bigint());
        assert_eq!(
            column(&columns, "readable_metrics").logical_type,
            Some(SqlType::Json)
        );
        assert!(!column(&columns, "status").nullable);
        assert!(column(&columns, "snapshot_id").nullable);
        assert!(!column(&columns, "data_file").nullable);
    }

    #[test]
    fn entries_carries_a_nested_data_file_row() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Entries, &full_derivation());
        let DataType::Struct(fields) = &column(&columns, "data_file").data_type else {
            panic!("data_file must be a ROW");
        };
        assert_eq!(
            fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            vec![
                "content",
                "file_path",
                "file_format",
                "spec_id",
                "partition",
                "record_count",
                "file_size_in_bytes",
                "column_sizes",
                "value_counts",
                "null_value_counts",
                "nan_value_counts",
                "lower_bounds",
                "upper_bounds",
                "key_metadata",
                "split_offsets",
                "equality_ids",
                "sort_order_id",
            ]
        );
    }

    #[test]
    fn entries_nested_bounds_are_integer_to_varchar_maps() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Entries, &full_derivation());
        let DataType::Struct(fields) = &column(&columns, "data_file").data_type else {
            panic!("data_file must be a ROW");
        };
        for name in ["lower_bounds", "upper_bounds"] {
            let bounds = fields
                .iter()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!(bounds.data_type(), &map_of(integer(), varchar()));
        }
    }

    #[test]
    fn entries_rejects_the_legacy_flattened_shape() {
        let columns = metadata_table_schema_with(SqlMetadataTableKind::Entries, &full_derivation());
        let present = names(&columns);
        // The legacy alias exposed every `$files` column at the top level.
        for flattened in [
            "content",
            "file_path",
            "file_format",
            "spec_id",
            "partition",
            "record_count",
            "file_size_in_bytes",
            "column_sizes",
            "lower_bounds",
            "upper_bounds",
            "split_offsets",
            "equality_ids",
            "sort_order_id",
            "first_row_id",
        ] {
            assert!(
                !present.contains(&flattened),
                "`{flattened}` must live inside data_file, not at the top level"
            );
        }
    }

    // -----------------------------------------------------------------
    // $snapshots / $history / $refs
    // -----------------------------------------------------------------

    #[test]
    fn snapshots_schema_freezes_columns_and_types() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Snapshots);
        assert_eq!(
            names(&columns),
            vec![
                "committed_at",
                "snapshot_id",
                "parent_id",
                "operation",
                "manifest_list",
                "summary",
            ]
        );
        let instant = TemporalType::TimestampWithTimeZone(TemporalPrecision::MICROSECONDS);
        assert_eq!(
            column(&columns, "committed_at").data_type,
            instant.carrier()
        );
        assert_eq!(
            column(&columns, "committed_at").logical_type,
            Some(SqlType::Temporal(instant))
        );
        assert_eq!(column(&columns, "snapshot_id").data_type, bigint());
        assert_eq!(column(&columns, "parent_id").data_type, bigint());
        assert_eq!(column(&columns, "operation").data_type, varchar());
        assert_eq!(column(&columns, "manifest_list").data_type, varchar());
    }

    #[test]
    fn snapshots_summary_is_a_map_not_a_string() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Snapshots);
        let summary = column(&columns, "summary");
        assert_eq!(summary.data_type, map_of(varchar(), varchar()));
        assert!(matches!(summary.data_type, DataType::Map(_, _)));
        assert_ne!(summary.data_type, varchar());
        assert!(summary.nullable);
    }

    #[test]
    fn history_schema_freezes_columns_and_types() {
        let columns = metadata_table_schema(SqlMetadataTableKind::History);
        assert_eq!(
            names(&columns),
            vec![
                "made_current_at",
                "snapshot_id",
                "parent_id",
                "is_current_ancestor",
            ]
        );
        let instant = TemporalType::TimestampWithTimeZone(TemporalPrecision::MICROSECONDS);
        assert_eq!(
            column(&columns, "made_current_at").data_type,
            instant.carrier()
        );
        assert_eq!(
            column(&columns, "made_current_at").logical_type,
            Some(SqlType::Temporal(instant))
        );
        assert_eq!(column(&columns, "snapshot_id").data_type, bigint());
        assert_eq!(column(&columns, "parent_id").data_type, bigint());
        assert_eq!(column(&columns, "is_current_ancestor").data_type, boolean());
        assert!(!column(&columns, "is_current_ancestor").nullable);
    }

    #[test]
    fn refs_schema_freezes_columns_and_types() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Refs);
        assert_eq!(
            names(&columns),
            vec![
                "name",
                "type",
                "snapshot_id",
                "max_reference_age_in_ms",
                "min_snapshots_to_keep",
                "max_snapshot_age_in_ms",
            ]
        );
        assert_eq!(column(&columns, "name").data_type, varchar());
        assert_eq!(column(&columns, "type").data_type, varchar());
        assert_eq!(column(&columns, "snapshot_id").data_type, bigint());
        assert_eq!(
            column(&columns, "max_reference_age_in_ms").data_type,
            bigint()
        );
        assert_eq!(
            column(&columns, "min_snapshots_to_keep").data_type,
            integer()
        );
        assert_eq!(
            column(&columns, "max_snapshot_age_in_ms").data_type,
            bigint()
        );
    }

    // -----------------------------------------------------------------
    // $manifests
    // -----------------------------------------------------------------

    #[test]
    fn manifests_schema_freezes_trino_column_order() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Manifests);
        assert_eq!(
            names(&columns),
            vec![
                "content",
                "path",
                "length",
                "partition_spec_id",
                "added_snapshot_id",
                "added_data_files_count",
                "added_rows_count",
                "existing_data_files_count",
                "existing_rows_count",
                "deleted_data_files_count",
                "deleted_rows_count",
                "partition_summaries",
            ]
        );
        assert_eq!(column(&columns, "content").data_type, integer());
        assert_eq!(column(&columns, "path").data_type, varchar());
        assert_eq!(column(&columns, "length").data_type, bigint());
        assert_eq!(column(&columns, "partition_spec_id").data_type, integer());
        assert_eq!(column(&columns, "added_snapshot_id").data_type, bigint());
        assert_eq!(
            column(&columns, "added_data_files_count").data_type,
            integer()
        );
        assert_eq!(column(&columns, "added_rows_count").data_type, bigint());
        assert!(columns[5..11].iter().all(|count| count.nullable));
    }

    #[test]
    fn manifests_partition_summaries_is_an_array_of_row() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Manifests);
        let summaries = &column(&columns, "partition_summaries").data_type;
        let DataType::List(item) = summaries else {
            panic!("partition_summaries must be an ARRAY, got {summaries:?}");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("partition_summaries item must be a ROW");
        };
        assert_eq!(
            fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            vec![
                "contains_null",
                "contains_nan",
                "lower_bound",
                "upper_bound"
            ]
        );
        assert_eq!(fields[0].data_type(), &boolean());
        assert_eq!(fields[1].data_type(), &boolean());
        assert_eq!(fields[2].data_type(), &varchar());
        assert_eq!(fields[3].data_type(), &varchar());
    }

    // -----------------------------------------------------------------
    // $partitions
    // -----------------------------------------------------------------

    #[test]
    fn partitions_schema_is_the_trino_partitions_view() {
        let columns =
            metadata_table_schema_with(SqlMetadataTableKind::Partitions, &full_derivation());
        assert_eq!(
            names(&columns),
            vec!["partition", "record_count", "file_count", "total_size"]
        );
        assert_eq!(column(&columns, "partition").data_type, partition_row());
        assert_eq!(column(&columns, "record_count").data_type, bigint());
        assert_eq!(column(&columns, "file_count").data_type, bigint());
        assert_eq!(column(&columns, "total_size").data_type, bigint());
        assert!(column(&columns, "record_count").nullable);
        assert!(!column(&columns, "file_count").nullable);
        assert!(column(&columns, "total_size").nullable);
        assert!(!names(&columns).contains(&"data"));
    }

    #[test]
    fn partitions_schema_drops_the_novarocks_delete_count_columns() {
        for derived in [IcebergMetadataDerivedTypes::none(), full_derivation()] {
            let columns = metadata_table_schema_with(SqlMetadataTableKind::Partitions, &derived);
            let present = names(&columns);
            for removed in ["position_delete_file_count", "equality_delete_file_count"] {
                assert!(
                    !present.contains(&removed),
                    "`{removed}` is a removed NovaRocks column; delete content lives on $files.content"
                );
            }
        }
    }

    #[test]
    fn partitions_schema_omits_optional_rows_for_an_unpartitioned_table() {
        let columns = metadata_table_schema(SqlMetadataTableKind::Partitions);
        assert_eq!(
            names(&columns),
            vec!["record_count", "file_count", "total_size"]
        );
    }

    // -----------------------------------------------------------------
    // Cross-relation invariants
    // -----------------------------------------------------------------

    #[test]
    fn every_relation_has_unique_non_empty_column_names_so_select_star_expands() {
        for kind in ALL_RELATIONS {
            for derived in [IcebergMetadataDerivedTypes::none(), full_derivation()] {
                let columns = metadata_table_schema_with(kind, &derived);
                assert!(
                    !columns.is_empty(),
                    "{} has no columns",
                    metadata_relation_suffix(kind)
                );
                let mut seen = std::collections::BTreeSet::new();
                for candidate in &columns {
                    assert!(
                        !candidate.name.is_empty(),
                        "{} has an unnamed column",
                        metadata_relation_suffix(kind)
                    );
                    assert!(
                        seen.insert(candidate.name.clone()),
                        "{} repeats column `{}`; a duplicate name makes an invalid RecordBatch",
                        metadata_relation_suffix(kind),
                        candidate.name
                    );
                }
            }
        }
    }

    #[test]
    fn select_star_expands_to_exactly_the_frozen_column_list() {
        // `SELECT *` expands the relation schema in declaration order, so the
        // frozen list *is* the star expansion. Pin all seven at once.
        let derived = full_derivation();
        let expansion: Vec<(&str, Vec<String>)> = ALL_RELATIONS
            .iter()
            .map(|kind| {
                (
                    metadata_relation_suffix(*kind),
                    metadata_table_schema_with(*kind, &derived)
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                )
            })
            .collect();
        let expected: Vec<(&str, Vec<&str>)> = vec![
            (
                "$files",
                vec![
                    "content",
                    "file_path",
                    "file_format",
                    "spec_id",
                    "partition",
                    "record_count",
                    "file_size_in_bytes",
                    "column_sizes",
                    "value_counts",
                    "null_value_counts",
                    "nan_value_counts",
                    "lower_bounds",
                    "upper_bounds",
                    "key_metadata",
                    "split_offsets",
                    "equality_ids",
                    "sort_order_id",
                    "readable_metrics",
                    "added_snapshot_id",
                    "file_sequence_number",
                    "data_sequence_number",
                    "referenced_data_file",
                    "pos",
                    "manifest_location",
                    "first_row_id",
                    "content_offset",
                    "content_size_in_bytes",
                ],
            ),
            (
                "$entries",
                vec![
                    "status",
                    "snapshot_id",
                    "sequence_number",
                    "file_sequence_number",
                    "data_file",
                    "readable_metrics",
                ],
            ),
            (
                "$snapshots",
                vec![
                    "committed_at",
                    "snapshot_id",
                    "parent_id",
                    "operation",
                    "manifest_list",
                    "summary",
                ],
            ),
            (
                "$history",
                vec![
                    "made_current_at",
                    "snapshot_id",
                    "parent_id",
                    "is_current_ancestor",
                ],
            ),
            (
                "$refs",
                vec![
                    "name",
                    "type",
                    "snapshot_id",
                    "max_reference_age_in_ms",
                    "min_snapshots_to_keep",
                    "max_snapshot_age_in_ms",
                ],
            ),
            (
                "$manifests",
                vec![
                    "content",
                    "path",
                    "length",
                    "partition_spec_id",
                    "added_snapshot_id",
                    "added_data_files_count",
                    "added_rows_count",
                    "existing_data_files_count",
                    "existing_rows_count",
                    "deleted_data_files_count",
                    "deleted_rows_count",
                    "partition_summaries",
                ],
            ),
            (
                "$partitions",
                vec!["partition", "record_count", "file_count", "total_size"],
            ),
        ];
        let expected: Vec<(&str, Vec<String>)> = expected
            .into_iter()
            .map(|(relation, columns)| {
                (
                    relation,
                    columns.into_iter().map(str::to_string).collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(expansion, expected);
    }

    #[test]
    fn no_relation_downgrades_a_complex_column_to_int64_or_utf8() {
        // Guards the exact failure mode section 6.8 forbids: shipping a MAP,
        // ROW, ARRAY, or JSON column as a scalar so an expression type-checks.
        let derived = full_derivation();
        let complex = [
            (SqlMetadataTableKind::Files, "column_sizes"),
            (SqlMetadataTableKind::Files, "lower_bounds"),
            (SqlMetadataTableKind::Files, "upper_bounds"),
            (SqlMetadataTableKind::Files, "split_offsets"),
            (SqlMetadataTableKind::Files, "partition"),
            (SqlMetadataTableKind::Entries, "data_file"),
            (SqlMetadataTableKind::Snapshots, "summary"),
            (SqlMetadataTableKind::Manifests, "partition_summaries"),
            (SqlMetadataTableKind::Partitions, "partition"),
        ];
        for (kind, name) in complex {
            let columns = metadata_table_schema_with(kind, &derived);
            let data_type = &column(&columns, name).data_type;
            assert!(
                matches!(
                    data_type,
                    DataType::Map(_, _) | DataType::Struct(_) | DataType::List(_)
                ),
                "{}.{name} degraded to {data_type:?}",
                metadata_relation_suffix(kind)
            );
        }
    }

    // -----------------------------------------------------------------
    // Derived-type validation and refusals
    // -----------------------------------------------------------------

    #[test]
    fn derived_types_reject_a_non_row_derivation() {
        let error = IcebergMetadataDerivedTypes::try_new(Some(varchar()), None)
            .expect_err("a VARCHAR partition is a caller bug");
        assert!(error.contains("partition"), "{error}");
        assert!(error.contains("must be a ROW"), "{error}");

        let error = IcebergMetadataDerivedTypes::try_new(None, Some(map_of(integer(), varchar())))
            .expect_err("a MAP bounds derivation is a caller bug");
        assert!(error.contains("bounds"), "{error}");
    }
}
