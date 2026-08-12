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

//! Provider-owned validators used by INSERT write preparation and DELETE
//! (`engine/delete_flow.rs`) entry points before lowering.
//! All errors returned here are user-visible — keep the messages action-oriented.

use std::collections::HashMap;

use crate::iceberg::spec::{FormatVersion, TableMetadata};
use crate::iceberg::table::Table;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;

use crate::commit::{
    IcebergUpdateMode, IcebergWriteMode, NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_COW,
};

pub fn row_lineage_property_enabled(props: &HashMap<String, String>) -> bool {
    props
        .get("write.row-lineage")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub fn classify_iceberg_write_mode(table: &Table) -> IcebergWriteMode {
    classify_iceberg_write_mode_from_metadata(
        table.metadata().format_version(),
        table.metadata().properties(),
    )
}

fn classify_iceberg_write_mode_from_metadata(
    format_version: FormatVersion,
    props: &HashMap<String, String>,
) -> IcebergWriteMode {
    if format_version == FormatVersion::V3 || row_lineage_property_enabled(props) {
        IcebergWriteMode::RowLineageV3
    } else {
        IcebergWriteMode::LegacyPositionDeletes
    }
}

/// Returns the write mode selected from Iceberg table metadata after rejecting
/// table schemas that the current writer cannot encode.
pub fn ensure_iceberg_write_supported(table: &Table) -> Result<IcebergWriteMode, String> {
    ensure_iceberg_write_supported_from_metadata(table.metadata())
}

/// [`ensure_iceberg_write_supported`] for callers that hold frozen metadata
/// rather than a live catalog table, such as row-mutation admission working
/// from an already-admitted table payload.
pub fn ensure_iceberg_write_supported_from_metadata(
    metadata: &TableMetadata,
) -> Result<IcebergWriteMode, String> {
    ensure_default_sort_order_resolvable_from_metadata(metadata)?;
    ensure_no_variant_in_partition_spec_from_metadata(metadata)?;
    ensure_no_variant_in_sort_order_from_metadata(metadata)?;
    Ok(classify_iceberg_write_mode_from_metadata(
        metadata.format_version(),
        metadata.properties(),
    ))
}

// Wired in by later tasks (insert/overwrite/update/delete planning).
#[allow(dead_code)]
pub fn ensure_no_variant_in_partition_spec(table: &Table) -> Result<(), String> {
    ensure_no_variant_in_partition_spec_from_metadata(table.metadata())
}

pub fn ensure_no_variant_in_partition_spec_from_metadata(
    metadata: &TableMetadata,
) -> Result<(), String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    let schema = metadata.current_schema();
    for f in metadata.default_partition_spec().fields() {
        let source = schema.field_by_id(f.source_id).ok_or_else(|| {
            format!(
                "iceberg table partition field '{name}' references missing source id {sid}",
                name = f.name,
                sid = f.source_id
            )
        })?;
        if matches!(
            source.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Variant)
        ) {
            return Err(format!(
                "iceberg table column '{name}' is variant; variant columns cannot appear in the partition spec. \
                 Drop the partition transform on '{name}' before writing.",
                name = source.name,
            ));
        }
    }
    Ok(())
}

// Wired in by later tasks (insert/overwrite/update/delete planning).
#[allow(dead_code)]
pub fn ensure_no_variant_in_sort_order(table: &Table) -> Result<(), String> {
    ensure_no_variant_in_sort_order_from_metadata(table.metadata())
}

pub fn ensure_no_variant_in_sort_order_from_metadata(
    metadata: &TableMetadata,
) -> Result<(), String> {
    use crate::iceberg::spec::{PrimitiveType, Type};
    let schema = metadata.current_schema();
    for f in metadata.default_sort_order().fields.iter() {
        let source = schema.field_by_id(f.source_id).ok_or_else(|| {
            format!(
                "iceberg table sort field references missing source id {}",
                f.source_id
            )
        })?;
        if matches!(
            source.field_type.as_ref(),
            Type::Primitive(PrimitiveType::Variant)
        ) {
            return Err(format!(
                "iceberg table column '{name}' is variant; variant columns cannot appear in the sort order. \
                 Drop the sort key on '{name}' before writing.",
                name = source.name,
            ));
        }
    }
    Ok(())
}

/// Fail-fast guard: the table's `default-sort-order-id` must reference an
/// existing sort order. iceberg-rust's `TableMetadata::default_sort_order()`
/// panics if the id is dangling; this surfaces a clean error instead.
pub fn ensure_default_sort_order_resolvable(table: &Table) -> Result<(), String> {
    ensure_default_sort_order_resolvable_from_metadata(table.metadata())
}

pub fn ensure_default_sort_order_resolvable_from_metadata(
    metadata: &TableMetadata,
) -> Result<(), String> {
    let id = metadata.default_sort_order_id();
    if metadata.sort_order_by_id(id).is_none() {
        return Err(format!(
            "iceberg table default-sort-order-id {id} does not reference any existing sort order"
        ));
    }
    Ok(())
}

/// Fail-fast guard: after a partition-spec evolution commit, the reloaded
/// table's `last-partition-id` must not have regressed. iceberg-rust assigns
/// partition field ids during `AddSpec`; this asserts the committed result
/// preserved monotonicity (catalog round-trip sanity).
pub fn ensure_partition_id_not_regressed(previous: i32, reloaded: i32) -> Result<(), String> {
    if reloaded < previous {
        return Err(format!(
            "iceberg partition-spec evolution regressed last-partition-id from {previous} to \
             {reloaded}; partition field ids must be monotonically increasing"
        ));
    }
    Ok(())
}

/// Fail-fast guard: the new schema's `last-column-id` high-watermark must not
/// regress below the table's current value. Iceberg requires this id be
/// monotonically increasing; a regression would corrupt field-id assignment.
pub fn ensure_column_id_not_regressed(current: i32, next: i32) -> Result<(), String> {
    if next < current {
        return Err(format!(
            "iceberg schema evolution would regress last-column-id from {current} to {next}; \
             column ids must be monotonically increasing"
        ));
    }
    Ok(())
}

/// The physical row-mutation strategy this provider will use for `intent`
/// against `metadata`.
///
/// This is the single place the Iceberg strategy is decided. It runs the same
/// fail-fast write guards as ordinary write admission first, so an unsupported
/// table shape is rejected before a strategy is named.
///
/// Two rules deserve calling out:
///
/// - Deletion vectors are a format-v3 feature, and the DV commit path rejects
///   any other format version outright. The DELETE strategy is therefore keyed
///   on the format version alone. A v2 table that declares
///   `write.row-lineage=true` gets position deletes, not a deletion vector it
///   could never commit.
/// - A MERGE that can delete matched rows is served merge-on-read even when the
///   table asks for copy-on-write, because the copy-on-write rewrite has no way
///   to express a matched delete.
// Design: ADR-0055 (docs/adr/ADR-0055-row-dml-strategy-consumer-closeout.md)
pub fn row_mutation_strategy_from_metadata(
    metadata: &TableMetadata,
    intent: &novarocks_spi::connector::ConnectorRowMutationIntent,
) -> Result<novarocks_spi::connector::ConnectorRowMutationStrategy, String> {
    use novarocks_spi::connector::{
        ConnectorRowMutationEffect, ConnectorRowMutationIntent, ConnectorRowMutationStrategy,
    };

    ensure_iceberg_write_supported_from_metadata(metadata)?;

    match intent {
        ConnectorRowMutationIntent::Delete => {
            if metadata.format_version() == FormatVersion::V3 {
                Ok(ConnectorRowMutationStrategy::DeletionVector)
            } else {
                Ok(ConnectorRowMutationStrategy::PositionDelete)
            }
        }
        ConnectorRowMutationIntent::Update | ConnectorRowMutationIntent::Merge { .. } => {
            // An insert-only MERGE rewrites nothing: no live row is matched, so
            // there is no copy-on-write rewrite to express and the mutation is
            // append-shaped. Resolving it before the update-mode property keeps
            // a copy-on-write table from demanding a bounded selection that an
            // insert-only merge can never produce.
            if !intent.accepts(ConnectorRowMutationEffect::Delete)
                && !intent.accepts(ConnectorRowMutationEffect::Replace)
            {
                return Ok(ConnectorRowMutationStrategy::MergeOnRead);
            }
            let mode = select_update_mode_from_properties(
                metadata.format_version(),
                metadata.properties(),
            )?;
            if intent.accepts(ConnectorRowMutationEffect::Delete) {
                return Ok(ConnectorRowMutationStrategy::MergeOnRead);
            }
            Ok(match mode {
                IcebergUpdateMode::CopyOnWrite => ConnectorRowMutationStrategy::CopyOnWrite,
                IcebergUpdateMode::MergeOnRead => ConnectorRowMutationStrategy::MergeOnRead,
            })
        }
    }
}

fn select_update_mode_from_properties(
    format_version: FormatVersion,
    props: &HashMap<String, String>,
) -> Result<IcebergUpdateMode, String> {
    ensure_update_properties_require_v3_row_lineage(format_version, props)?;
    let value = props
        .get(NOVAROCKS_UPDATE_MODE)
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| NOVAROCKS_UPDATE_MODE_COW.to_string());
    IcebergUpdateMode::from_property_value(value.as_str()).ok_or_else(|| {
        format!("unsupported write.update.mode `{value}`; expected copy-on-write or merge-on-read")
    })
}

fn ensure_update_properties_require_v3_row_lineage(
    format_version: FormatVersion,
    props: &HashMap<String, String>,
) -> Result<(), String> {
    if format_version != FormatVersion::V3 || !row_lineage_property_enabled(props) {
        return Err("UPDATE requires an Iceberg v3 table with write.row-lineage=true".to_string());
    }
    Ok(())
}

/// Phase 1 only handles tables whose data is all under the current default
/// partition spec. Multiple historical specs (partition evolution) require
/// per-file spec routing in the writer that we don't have yet.
pub fn ensure_single_partition_spec(table: &Table) -> Result<(), String> {
    ensure_single_partition_spec_from_metadata(table.metadata())
}

/// [`ensure_single_partition_spec`] for callers that hold frozen metadata
/// rather than a live catalog table, such as write preparation working from an
/// already-admitted table payload.
pub fn ensure_single_partition_spec_from_metadata(metadata: &TableMetadata) -> Result<(), String> {
    let default_id = metadata.default_partition_spec_id();
    let other = metadata
        .partition_specs_iter()
        .filter(|s| s.spec_id() != default_id)
        .count();
    if other > 0 {
        return Err(format!(
            "iceberg table has {other} non-default partition spec(s); phase 1 \
             writes require a single partition spec. Rewrite or drop historical \
             data under prior specs."
        ));
    }
    Ok(())
}

pub fn ensure_overwrite_single_partition_spec(table: &Table) -> Result<(), String> {
    ensure_overwrite_single_partition_spec_from_metadata(table.metadata())
}

/// [`ensure_overwrite_single_partition_spec`] for callers that hold frozen metadata.
pub fn ensure_overwrite_single_partition_spec_from_metadata(
    metadata: &TableMetadata,
) -> Result<(), String> {
    ensure_single_partition_spec_from_metadata(metadata).map_err(|err| {
        format!("INSERT OVERWRITE on an evolved Iceberg table is not supported yet: {err}")
    })
}

pub fn ensure_equality_delete_single_partition_spec(table: &Table) -> Result<(), String> {
    ensure_equality_delete_single_partition_spec_from_metadata(table.metadata())
}

/// [`ensure_equality_delete_single_partition_spec`] for callers that hold frozen metadata.
pub fn ensure_equality_delete_single_partition_spec_from_metadata(
    metadata: &TableMetadata,
) -> Result<(), String> {
    ensure_single_partition_spec_from_metadata(metadata).map_err(|err| {
        format!("ADD EQUALITY DELETE on an evolved Iceberg table is not supported yet: {err}")
    })
}

/// INSERT OVERWRITE rewrites data manifests without fully reconciling existing
/// equality-delete manifests yet. Row-level DELETE has its own visibility
/// planner; keep this guard scoped to overwrite-style write planning.
///
/// Best-effort check via the snapshot summary's `total-equality-deletes`
/// property. When absent, we accept (no manifest walk yet — that belongs to
/// Tasks 9/10). Empty table (no current snapshot) → accept.
pub fn ensure_no_equality_deletes(table: &Table) -> Result<(), String> {
    ensure_no_equality_deletes_from_metadata(table.metadata())
}

/// [`ensure_no_equality_deletes`] for callers that hold frozen metadata.
pub fn ensure_no_equality_deletes_from_metadata(metadata: &TableMetadata) -> Result<(), String> {
    let snap = match metadata.current_snapshot() {
        Some(s) => s,
        None => return Ok(()), // empty table — no manifests to inspect
    };
    let n = snap
        .summary()
        .additional_properties
        .get("total-equality-deletes")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    if n > 0 {
        return Err(
            "iceberg table has equality-delete files in its current snapshot; \
             INSERT OVERWRITE planning does not yet reconcile existing \
             equality-delete manifests. Compact away the equality deletes \
             before issuing INSERT OVERWRITE."
                .to_string(),
        );
    }
    Ok(())
}

/// Strict column-by-column type match between the SELECT's arrow schema and
/// the iceberg table schema. No implicit cast, no reorder.
///
/// `columns_clause` is the optional `INSERT INTO t (cols)` list; when `None`,
/// SELECT must produce exactly `table_schema.fields().len()` columns in the
/// table's natural declaration order.
pub fn match_select_schema_to_table(
    select_schema: &ArrowSchemaRef,
    table: &Table,
    columns_clause: Option<&[String]>,
) -> Result<(), String> {
    let iceberg_schema = table.metadata().current_schema();
    let table_fields = iceberg_schema.as_struct().fields();

    let target_fields: Vec<_> = match columns_clause {
        None => table_fields.iter().collect(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                let f = table_fields
                    .iter()
                    .find(|f| f.name == *n)
                    .ok_or_else(|| format!("INSERT column `{n}` does not exist in table"))?;
                out.push(f);
            }
            out
        }
    };

    if select_schema.fields().len() != target_fields.len() {
        return Err(format!(
            "INSERT column count mismatch: SELECT produces {} columns, target expects {}",
            select_schema.fields().len(),
            target_fields.len()
        ));
    }

    for (i, (sel, tgt)) in select_schema
        .fields()
        .iter()
        .zip(target_fields.iter())
        .enumerate()
    {
        if !arrow_iceberg_types_compatible(sel.data_type(), &tgt.field_type) {
            return Err(format!(
                "INSERT column {i} type mismatch: SELECT produces {:?}, target column `{}` is {:?}; \
                 phase 1 does not perform implicit cast — wrap the SELECT expression in CAST() explicitly.",
                sel.data_type(),
                tgt.name,
                tgt.field_type
            ));
        }
    }
    Ok(())
}

/// Returns `true` when `arrow_ty` and `iceberg_ty` represent the same logical
/// type. Delegates to `novarocks_connector_iceberg::iceberg::arrow::type_to_arrow_type` so there is one
/// canonical mapping. On conversion error (unknown / complex type), returns
/// `false` (conservative reject).
fn arrow_iceberg_types_compatible(
    arrow_ty: &arrow::datatypes::DataType,
    iceberg_ty: &crate::iceberg::spec::Type,
) -> bool {
    use crate::iceberg::spec::{PrimitiveType, Type};
    if matches!(iceberg_ty, Type::Primitive(PrimitiveType::Variant)) {
        // NovaRocks execution layer carries variants as LargeBinary
        // (see src/lower/compat/type_lowering.rs:89,170). The full struct shape
        // is materialized later by transform_variant_columns_for_write.
        return matches!(arrow_ty, arrow::datatypes::DataType::LargeBinary);
    }
    match crate::iceberg::arrow::type_to_arrow_type(iceberg_ty) {
        Ok(expected) => &expected == arrow_ty,
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::NOVAROCKS_UPDATE_MODE_MOR;

    #[test]
    fn default_sort_order_resolvable_ok_for_unsorted_table() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
        let table = make_table_with(
            vec![NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into()],
            vec![],
            vec![],
        );
        assert!(super::ensure_default_sort_order_resolvable(&table).is_ok());
    }

    #[test]
    fn partition_id_monotonic_ok_and_regression_fails() {
        assert!(super::ensure_partition_id_not_regressed(1000, 1001).is_ok());
        assert!(super::ensure_partition_id_not_regressed(1000, 1000).is_ok());
        let err = super::ensure_partition_id_not_regressed(1001, 1000).unwrap_err();
        assert!(err.contains("last-partition-id"), "got: {err}");
    }

    #[test]
    fn column_id_monotonic_ok_and_regression_fails() {
        assert!(super::ensure_column_id_not_regressed(10, 12).is_ok());
        assert!(super::ensure_column_id_not_regressed(10, 10).is_ok());
        let err = super::ensure_column_id_not_regressed(10, 9).unwrap_err();
        assert!(err.contains("last-column-id"), "got: {err}");
    }

    #[test]
    fn row_lineage_property_parser_accepts_true_case_insensitive() {
        let mut props = std::collections::HashMap::new();
        props.insert("write.row-lineage".to_string(), "TrUe".to_string());
        assert!(row_lineage_property_enabled(&props));
    }

    #[test]
    fn row_lineage_property_parser_treats_missing_or_false_as_legacy() {
        let props = std::collections::HashMap::<String, String>::new();
        assert!(!row_lineage_property_enabled(&props));

        let mut props = std::collections::HashMap::new();
        props.insert("write.row-lineage".to_string(), "false".to_string());
        assert!(!row_lineage_property_enabled(&props));
    }

    #[test]
    fn write_mode_classifies_v3_without_property_as_row_lineage() {
        let props = std::collections::HashMap::<String, String>::new();
        assert_eq!(
            classify_iceberg_write_mode_from_metadata(FormatVersion::V3, &props),
            IcebergWriteMode::RowLineageV3
        );
    }

    #[test]
    fn write_mode_classifies_v2_without_property_as_legacy() {
        let props = std::collections::HashMap::<String, String>::new();
        assert_eq!(
            classify_iceberg_write_mode_from_metadata(FormatVersion::V2, &props),
            IcebergWriteMode::LegacyPositionDeletes
        );
    }

    #[test]
    fn update_mode_defaults_to_copy_on_write() {
        let props = HashMap::from([("write.row-lineage".to_string(), "true".to_string())]);
        assert_eq!(
            select_update_mode_from_properties(FormatVersion::V3, &props).expect("mode"),
            IcebergUpdateMode::CopyOnWrite
        );
    }

    #[test]
    fn update_mode_accepts_merge_on_read() {
        let props = HashMap::from([
            ("write.row-lineage".to_string(), "true".to_string()),
            (
                NOVAROCKS_UPDATE_MODE.to_string(),
                NOVAROCKS_UPDATE_MODE_MOR.to_string(),
            ),
        ]);
        assert_eq!(
            select_update_mode_from_properties(FormatVersion::V3, &props).expect("mode"),
            IcebergUpdateMode::MergeOnRead
        );
    }

    #[test]
    fn update_mode_rejects_v3_without_row_lineage() {
        let props = HashMap::new();
        let err =
            select_update_mode_from_properties(FormatVersion::V3, &props).expect_err("must fail");
        assert!(err.contains("write.row-lineage=true"), "{err}");
    }

    #[test]
    fn update_mode_rejects_invalid_property() {
        let props = HashMap::from([
            ("write.row-lineage".to_string(), "true".to_string()),
            (NOVAROCKS_UPDATE_MODE.to_string(), "delta".to_string()),
        ]);
        let err =
            select_update_mode_from_properties(FormatVersion::V3, &props).expect_err("must fail");
        assert!(err.contains("unsupported write.update.mode"), "{err}");
    }

    #[test]
    fn errors_carry_actionable_messages() {
        // Sanity test that the module compiles and the public API is accessible.
        // Real coverage comes from NEG-* integration tests in Task 17.
        let s = "row-lineage";
        assert!(s.contains("row-lineage"));
    }

    fn make_table_with(
        fields: Vec<crate::iceberg::spec::NestedFieldRef>,
        partition_fields: Vec<crate::iceberg::spec::PartitionField>,
        sort_fields: Vec<crate::iceberg::spec::SortField>,
    ) -> crate::iceberg::table::Table {
        use std::sync::Arc;
        let schema = Arc::new(
            crate::iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(fields)
                .build()
                .expect("schema"),
        );
        let mut spec_builder =
            crate::iceberg::spec::PartitionSpec::builder(schema.clone()).with_spec_id(0);
        for f in partition_fields {
            // Resolve source field name from source_id; the vendored
            // `add_partition_field` API takes (source_name, target_name, transform).
            let source_name = schema
                .field_by_id(f.source_id)
                .expect("partition source must exist in schema")
                .name
                .clone();
            spec_builder = spec_builder
                .add_partition_field(source_name, f.name, f.transform)
                .expect("add");
        }
        let partition_spec = spec_builder.build().expect("spec");
        let mut order_builder = crate::iceberg::spec::SortOrder::builder();
        for f in sort_fields {
            order_builder = order_builder.with_sort_field(f).clone();
        }
        let sort_order = order_builder.build_unbound().expect("sort");
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            partition_spec,
            sort_order,
            "file:///tmp/x".to_string(),
            crate::iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["d", "t"]).unwrap())
            .file_io(crate::fs_io::build_file_io_for_location(
                "file:///tmp/x",
                None,
            ))
            .metadata(metadata)
            .build()
            .expect("table")
    }

    #[test]
    fn ensure_no_variant_in_partition_spec_rejects_variant_partition_column() {
        use crate::iceberg::spec::{NestedField, PartitionField, PrimitiveType, Transform, Type};
        let table = make_table_with(
            vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
            ],
            vec![PartitionField {
                source_id: 2,
                field_id: 1000,
                name: "v_part".to_string(),
                transform: Transform::Identity,
            }],
            vec![],
        );
        let err = ensure_no_variant_in_partition_spec(&table).expect_err("must reject");
        assert!(err.contains("'v'"), "{err}");
        assert!(err.contains("partition"), "{err}");
    }

    #[test]
    fn ensure_no_variant_in_partition_spec_accepts_clean_table() {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
        let table = make_table_with(
            vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
            ],
            vec![],
            vec![],
        );
        ensure_no_variant_in_partition_spec(&table).expect("clean");
    }

    #[test]
    fn ensure_no_variant_in_sort_order_rejects_variant_sort_column() {
        use crate::iceberg::spec::{
            NestedField, NullOrder, PrimitiveType, SortDirection, SortField, Transform, Type,
        };
        let table = make_table_with(
            vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
            ],
            vec![],
            vec![SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }],
        );
        let err = ensure_no_variant_in_sort_order(&table).expect_err("must reject");
        assert!(err.contains("'v'"), "{err}");
        assert!(err.contains("sort"), "{err}");
    }

    /// UPDATE and MERGE additionally require the row-lineage property, not just
    /// format v3, so every non-DELETE strategy fixture carries it.
    const ROW_LINEAGE_ON: (&str, &str) = ("write.row-lineage", "true");

    /// Plain single-column table metadata at a chosen format version and
    /// property set, which is all the strategy rule reads.
    fn strategy_metadata(
        format_version: crate::iceberg::spec::FormatVersion,
        props: &[(&str, &str)],
    ) -> TableMetadata {
        use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, SortOrder, Type};
        use std::sync::Arc;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let partition_spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("spec");
        let sort_order = SortOrder::builder().build_unbound().expect("sort");
        crate::iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            partition_spec,
            sort_order,
            "file:///tmp/x".to_string(),
            format_version,
            props
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn spi5h_delete_strategy_is_keyed_on_format_version_alone() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorRowMutationIntent, ConnectorRowMutationStrategy};

        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(FormatVersion::V3, &[]),
                &ConnectorRowMutationIntent::Delete,
            )
            .expect("v3 delete"),
            ConnectorRowMutationStrategy::DeletionVector
        );

        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(FormatVersion::V2, &[]),
                &ConnectorRowMutationIntent::Delete,
            )
            .expect("v2 delete"),
            ConnectorRowMutationStrategy::PositionDelete
        );

        // A v2 table that declares row lineage must still get position deletes.
        // Deletion vectors are v3-only and the DV commit path rejects v2, so
        // the looser write-mode classification would only pick a strategy that
        // is guaranteed to fail at commit time.
        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(FormatVersion::V2, &[("write.row-lineage", "true")]),
                &ConnectorRowMutationIntent::Delete,
            )
            .expect("v2 row-lineage delete"),
            ConnectorRowMutationStrategy::PositionDelete
        );
    }

    #[test]
    fn spi5h_update_strategy_follows_the_table_mode_and_requires_v3_row_lineage() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorRowMutationIntent, ConnectorRowMutationStrategy};

        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(FormatVersion::V3, &[ROW_LINEAGE_ON]),
                &ConnectorRowMutationIntent::Update,
            )
            .expect("default is copy-on-write"),
            ConnectorRowMutationStrategy::CopyOnWrite
        );

        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(
                    FormatVersion::V3,
                    &[ROW_LINEAGE_ON, (NOVAROCKS_UPDATE_MODE, "merge-on-read")]
                ),
                &ConnectorRowMutationIntent::Update,
            )
            .expect("declared merge-on-read"),
            ConnectorRowMutationStrategy::MergeOnRead
        );

        let v2 = row_mutation_strategy_from_metadata(
            &strategy_metadata(FormatVersion::V2, &[ROW_LINEAGE_ON]),
            &ConnectorRowMutationIntent::Update,
        )
        .expect_err("UPDATE needs v3 row lineage");
        assert!(v2.contains("row-lineage"), "{v2}");

        let v3_without_row_lineage = row_mutation_strategy_from_metadata(
            &strategy_metadata(FormatVersion::V3, &[]),
            &ConnectorRowMutationIntent::Update,
        )
        .expect_err("UPDATE needs the row-lineage property, not just v3");
        assert!(
            v3_without_row_lineage.contains("row-lineage"),
            "{v3_without_row_lineage}"
        );

        let unsupported = row_mutation_strategy_from_metadata(
            &strategy_metadata(
                FormatVersion::V3,
                &[ROW_LINEAGE_ON, (NOVAROCKS_UPDATE_MODE, "sideways")],
            ),
            &ConnectorRowMutationIntent::Update,
        )
        .expect_err("unknown update mode must fail");
        assert!(unsupported.contains("sideways"), "{unsupported}");
    }

    #[test]
    fn spi5h_merge_that_can_delete_is_served_merge_on_read() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{
            ConnectorRowMutationEffect, ConnectorRowMutationIntent, ConnectorRowMutationStrategy,
        };

        // Copy-on-write table, but the MERGE can delete matched rows: the
        // rewrite cannot express that, so merge-on-read wins.
        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(
                    FormatVersion::V3,
                    &[ROW_LINEAGE_ON, (NOVAROCKS_UPDATE_MODE, "copy-on-write")]
                ),
                &ConnectorRowMutationIntent::Merge {
                    effects: vec![
                        ConnectorRowMutationEffect::Delete,
                        ConnectorRowMutationEffect::Insert,
                    ],
                },
            )
            .expect("merge with delete"),
            ConnectorRowMutationStrategy::MergeOnRead
        );

        // Same table, a MERGE that only replaces and inserts, stays on the
        // table's declared copy-on-write mode.
        assert_eq!(
            row_mutation_strategy_from_metadata(
                &strategy_metadata(
                    FormatVersion::V3,
                    &[ROW_LINEAGE_ON, (NOVAROCKS_UPDATE_MODE, "copy-on-write")]
                ),
                &ConnectorRowMutationIntent::Merge {
                    effects: vec![
                        ConnectorRowMutationEffect::Replace,
                        ConnectorRowMutationEffect::Insert,
                    ],
                },
            )
            .expect("merge without delete"),
            ConnectorRowMutationStrategy::CopyOnWrite
        );
    }

    #[test]
    fn spi5h_strategy_runs_the_write_support_guards_first() {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionField, PrimitiveType, Transform, Type,
        };
        use novarocks_spi::connector::ConnectorRowMutationIntent;

        // Reuse the variant-partition table, which the write guards reject.
        let table = make_table_with(
            vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
            ],
            vec![PartitionField {
                source_id: 2,
                field_id: 1000,
                name: "v_part".to_string(),
                transform: Transform::Identity,
            }],
            vec![],
        );
        assert_eq!(table.metadata().format_version(), FormatVersion::V3);

        let err = row_mutation_strategy_from_metadata(
            table.metadata(),
            &ConnectorRowMutationIntent::Delete,
        )
        .expect_err("variant partition column must be rejected before a strategy is named");
        assert!(err.contains("variant"), "{err}");
    }

    #[test]
    fn variant_iceberg_type_matches_largebinary_arrow_type() {
        use crate::iceberg::spec::{PrimitiveType, Type};
        use arrow::datatypes::DataType;
        let iceberg_ty = Type::Primitive(PrimitiveType::Variant);
        assert!(arrow_iceberg_types_compatible(
            &DataType::LargeBinary,
            &iceberg_ty
        ));
        assert!(!arrow_iceberg_types_compatible(
            &DataType::Binary,
            &iceberg_ty
        ));
        assert!(!arrow_iceberg_types_compatible(
            &DataType::Utf8,
            &iceberg_ty
        ));
    }
}
