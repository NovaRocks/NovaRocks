use iceberg::spec::{
    PartitionSpecRef, PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};

use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::ast::IcebergPartitionFieldExpr;

/// First partition field id by Iceberg spec convention.
/// V2 metadata starts partition field ids at 1000; iceberg-rust matches via
/// `UNPARTITIONED_LAST_ASSIGNED_ID = 999`. We assign explicitly because the
/// REST CreateTableRequest schema requires `field-id` to be a non-null integer.
const INITIAL_PARTITION_FIELD_ID: i32 = 1000;

pub(crate) fn build_initial_partition_spec(
    schema: &Schema,
    fields: &[IcebergPartitionFieldExpr],
) -> Result<Option<UnboundPartitionSpec>, String> {
    if fields.is_empty() {
        return Ok(None);
    }

    // The REST CreateTableRequest schema requires non-null `spec-id` and
    // `field-id` integers; iceberg-rust's UnboundPartitionSpec defaults both to
    // None, which serialize as JSON null and the REST server rejects with a
    // Jackson deserialization error. Assign them explicitly. Hadoop catalog
    // binds these via `unwrap_or(DEFAULT_PARTITION_SPEC_ID)` and field-id
    // reassignment, so this is a no-op there.
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for (index, field) in fields.iter().enumerate() {
        let source_id = source_field_id(schema, field)?;
        validate_transform(schema, source_id, field)?;
        let unbound = UnboundPartitionField {
            source_id,
            field_id: Some(INITIAL_PARTITION_FIELD_ID + index as i32),
            name: stable_field_name(field),
            transform: to_transform(field),
        };
        builder = builder
            .add_partition_fields([unbound])
            .map_err(|e| format!("build iceberg partition spec failed: {e}"))?;
    }
    Ok(Some(builder.build()))
}

pub(crate) fn build_evolved_partition_spec(
    schema: &Schema,
    current: &PartitionSpecRef,
    change: PartitionSpecChange<'_>,
) -> Result<UnboundPartitionSpec, String> {
    let mut fields: Vec<UnboundPartitionField> =
        current.fields().iter().cloned().map(Into::into).collect();

    match change {
        PartitionSpecChange::Add(expr) => {
            let source_id = source_field_id(schema, expr)?;
            validate_transform(schema, source_id, expr)?;
            let transform = to_transform(expr);
            if fields
                .iter()
                .any(|field| field.source_id == source_id && field.transform == transform)
            {
                return Err(format!(
                    "partition field `{}` already exists in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
            fields.push(UnboundPartitionField {
                source_id,
                field_id: None,
                name: stable_field_name(expr),
                transform,
            });
        }
        PartitionSpecChange::Drop(expr) => {
            let source_id = source_field_id(schema, expr)?;
            let transform = to_transform(expr);
            let before = fields.len();
            fields.retain(|field| !(field.source_id == source_id && field.transform == transform));
            if fields.len() == before {
                return Err(format!(
                    "partition field `{}` is not present in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
        }
    }

    let mut builder = UnboundPartitionSpecBuilder::new();
    for field in fields {
        builder = builder
            .add_partition_fields([field])
            .map_err(|e| format!("build evolved iceberg partition spec failed: {e}"))?;
    }
    Ok(builder.build())
}

pub(crate) fn build_replacement_partition_spec(
    schema: &Schema,
    fields: &[IcebergPartitionFieldExpr],
) -> Result<UnboundPartitionSpec, String> {
    if fields.is_empty() {
        return Err("REPARTITION BY requires at least one partition field".to_string());
    }
    let mut builder = UnboundPartitionSpecBuilder::new();
    for field in fields {
        let source_id = source_field_id(schema, field)?;
        validate_transform(schema, source_id, field)?;
        builder = builder
            .add_partition_fields([UnboundPartitionField {
                source_id,
                field_id: None,
                name: stable_field_name(field),
                transform: to_transform(field),
            }])
            .map_err(|e| format!("build replacement iceberg partition spec failed: {e}"))?;
    }
    Ok(builder.build())
}

pub(crate) enum PartitionSpecChange<'a> {
    Add(&'a IcebergPartitionFieldExpr),
    Drop(&'a IcebergPartitionFieldExpr),
}

#[allow(dead_code)]
pub(crate) fn spec_count(table: &iceberg::table::Table) -> usize {
    table.metadata().partition_specs_iter().count()
}

#[allow(dead_code)]
pub(crate) fn partition_spec_by_id(
    table: &iceberg::table::Table,
    spec_id: i32,
) -> Result<PartitionSpecRef, String> {
    table
        .metadata()
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| format!("iceberg table metadata missing partition spec id {spec_id}"))
}

fn source_field_id(schema: &Schema, expr: &IcebergPartitionFieldExpr) -> Result<i32, String> {
    let column = normalize_identifier(source_column(expr))?;
    schema
        .field_by_name_case_insensitive(&column)
        .map(|field| field.id)
        .ok_or_else(|| format!("partition source column `{column}` does not exist"))
}

fn source_column(expr: &IcebergPartitionFieldExpr) -> &str {
    match expr {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column,
    }
}

fn to_transform(expr: &IcebergPartitionFieldExpr) -> Transform {
    match expr {
        IcebergPartitionFieldExpr::Identity { .. } => Transform::Identity,
        IcebergPartitionFieldExpr::Year { .. } => Transform::Year,
        IcebergPartitionFieldExpr::Month { .. } => Transform::Month,
        IcebergPartitionFieldExpr::Day { .. } => Transform::Day,
        IcebergPartitionFieldExpr::Hour { .. } => Transform::Hour,
        IcebergPartitionFieldExpr::Bucket { num_buckets, .. } => Transform::Bucket(*num_buckets),
        IcebergPartitionFieldExpr::Truncate { width, .. } => Transform::Truncate(*width),
        IcebergPartitionFieldExpr::Void { .. } => Transform::Void,
    }
}

fn stable_field_name(expr: &IcebergPartitionFieldExpr) -> String {
    let normalized = normalize_identifier(source_column(expr))
        .unwrap_or_else(|_| source_column(expr).to_string());
    match expr {
        IcebergPartitionFieldExpr::Identity { .. } => normalized,
        IcebergPartitionFieldExpr::Year { .. } => format!("{normalized}_year"),
        IcebergPartitionFieldExpr::Month { .. } => format!("{normalized}_month"),
        IcebergPartitionFieldExpr::Day { .. } => format!("{normalized}_day"),
        IcebergPartitionFieldExpr::Hour { .. } => format!("{normalized}_hour"),
        IcebergPartitionFieldExpr::Bucket { num_buckets, .. } => {
            format!("{normalized}_bucket_{num_buckets}")
        }
        IcebergPartitionFieldExpr::Truncate { width, .. } => {
            format!("{normalized}_truncate_{width}")
        }
        IcebergPartitionFieldExpr::Void { .. } => format!("{normalized}_void"),
    }
}

fn validate_transform(
    schema: &Schema,
    source_id: i32,
    expr: &IcebergPartitionFieldExpr,
) -> Result<(), String> {
    let field = schema
        .field_by_id(source_id)
        .ok_or_else(|| format!("partition source field id {source_id} is missing"))?;
    let source_type = field.field_type.as_ref();
    let col = field.name.as_str();
    if matches!(source_type, Type::Primitive(PrimitiveType::Variant)) {
        return Err(format!(
            "iceberg table column `{col}` is variant; variant columns cannot appear in the partition spec. Use a non-variant source column for partition transforms."
        ));
    }
    match expr {
        IcebergPartitionFieldExpr::Year { .. }
        | IcebergPartitionFieldExpr::Month { .. }
        | IcebergPartitionFieldExpr::Day { .. } => {
            // Fail-fast for nanosecond timestamps: partition derivation assumes
            // microsecond epoch ticks; nanosecond columns would silently produce
            // wrong partition values (IV3-7.1 tracks full ns transform support).
            if matches!(
                source_type,
                Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs)
            ) {
                let transform = to_transform(expr);
                return Err(format!(
                    "partition transform `{transform}` on nanosecond timestamp column `{col}` is not supported yet (IV3-7.1); use a microsecond timestamp column or omit the time-based partition transform"
                ));
            }
            if !matches!(
                source_type,
                Type::Primitive(
                    PrimitiveType::Date | PrimitiveType::Timestamp | PrimitiveType::Timestamptz
                )
            ) {
                return Err(format!(
                    "temporal partition transform requires date/timestamp source, got {source_type}"
                ));
            }
        }
        IcebergPartitionFieldExpr::Hour { .. } => {
            // Fail-fast for nanosecond timestamps (same reason as year/month/day).
            if matches!(
                source_type,
                Type::Primitive(PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs)
            ) {
                let transform = to_transform(expr);
                return Err(format!(
                    "partition transform `{transform}` on nanosecond timestamp column `{col}` is not supported yet (IV3-7.1); use a microsecond timestamp column or omit the time-based partition transform"
                ));
            }
            if !matches!(
                source_type,
                Type::Primitive(PrimitiveType::Timestamp | PrimitiveType::Timestamptz)
            ) {
                return Err(format!(
                    "temporal partition transform requires timestamp source, got {source_type}"
                ));
            }
        }
        IcebergPartitionFieldExpr::Bucket { .. }
        | IcebergPartitionFieldExpr::Truncate { .. }
        | IcebergPartitionFieldExpr::Identity { .. }
        | IcebergPartitionFieldExpr::Void { .. } => {
            to_transform(expr)
                .result_type(source_type)
                .map_err(|e| format!("invalid iceberg partition transform: {e}"))?;
        }
    }
    Ok(())
}

/// Result of testing whether a base file's partition falls into the set
/// of partitions touched by a new write under `INSERT OVERWRITE PARTITIONS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionMatch {
    /// The base file's partition is one of the touched partitions
    /// (same partition spec, same partition values).
    InSet,
    /// The base file's partition is NOT in the touched set
    /// (same partition spec, different partition values).
    NotInSet,
    /// The base file was written under a different partition spec than
    /// the current write. Cross-spec OVERWRITE PARTITIONS is not yet
    /// supported — the caller must turn this into a user-facing reject:
    /// "OVERWRITE PARTITIONS: base file under historical partition spec X
    /// cannot be matched against current spec Y; run OPTIMIZE TABLE to
    /// consolidate first".
    DifferentSpec,
}

/// Decide how a base file's `(partition_struct, spec_id)` relates to the
/// set of partitions touched by the new files. `current_spec_id` is the
/// partition spec id used by every entry in `touched`.
///
/// Returns `InSet` if any touched partition is exactly equal to `base`,
/// `NotInSet` if none match, `DifferentSpec` if `base_spec_id !=
/// current_spec_id`.
pub(crate) fn partition_match_in_touched(
    base: &Struct,
    base_spec_id: i32,
    current_spec_id: i32,
    touched: &[Struct],
) -> PartitionMatch {
    if base_spec_id != current_spec_id {
        return PartitionMatch::DifferentSpec;
    }
    if touched.iter().any(|t| t == base) {
        return PartitionMatch::InSet;
    }
    PartitionMatch::NotInSet
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Transform, Type};

    use super::*;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn initial_spec_builds_expected_transforms() {
        let spec = build_initial_partition_spec(
            &schema(),
            &[
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string(),
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 16,
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "name".to_string(),
                    width: 8,
                },
            ],
        )
        .unwrap()
        .unwrap()
        .bind(Arc::new(schema()))
        .unwrap();

        assert_eq!(spec.fields().len(), 3);
        assert_eq!(spec.fields()[0].name, "ts_month");
        assert_eq!(spec.fields()[0].transform, Transform::Month);
        assert_eq!(spec.fields()[1].name, "id_bucket_16");
        assert_eq!(spec.fields()[1].transform, Transform::Bucket(16));
        assert_eq!(spec.fields()[2].name, "name_truncate_8");
        assert_eq!(spec.fields()[2].transform, Transform::Truncate(8));
    }

    #[test]
    fn temporal_transform_rejects_non_temporal_source() {
        let err = build_initial_partition_spec(
            &schema(),
            &[IcebergPartitionFieldExpr::Month {
                column: "name".to_string(),
            }],
        )
        .unwrap_err();
        assert!(err.contains("date/timestamp"), "{err}");
    }

    fn schema_with_ns_timestamp() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "ts_ns",
                    Type::Primitive(PrimitiveType::TimestampNs),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "tstz_ns",
                    Type::Primitive(PrimitiveType::TimestamptzNs),
                )),
            ])
            .build()
            .unwrap()
    }

    fn schema_with_variant() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "v",
                    Type::Primitive(PrimitiveType::Variant),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn partition_transform_rejects_variant_source_column() {
        let err = build_initial_partition_spec(
            &schema_with_variant(),
            &[IcebergPartitionFieldExpr::Identity {
                column: "v".to_string(),
            }],
        )
        .expect_err("variant partition source should be rejected at DDL time");
        assert!(err.contains("`v`"), "{err}");
        assert!(
            err.contains("variant columns cannot appear in the partition spec"),
            "{err}"
        );
    }

    #[test]
    fn evolved_partition_spec_rejects_variant_source_column() {
        let schema = schema_with_variant();
        let current = Arc::new(PartitionSpec::unpartition_spec());
        let expr = IcebergPartitionFieldExpr::Identity {
            column: "v".to_string(),
        };
        let err = build_evolved_partition_spec(&schema, &current, PartitionSpecChange::Add(&expr))
            .expect_err("variant partition source should be rejected during ALTER");
        assert!(err.contains("`v`"), "{err}");
        assert!(
            err.contains("variant columns cannot appear in the partition spec"),
            "{err}"
        );
    }

    #[test]
    fn time_transform_on_ns_timestamp_is_rejected() {
        let schema = schema_with_ns_timestamp();
        // All four time transforms must fail for TimestampNs.
        for expr in [
            IcebergPartitionFieldExpr::Year {
                column: "ts_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Month {
                column: "ts_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Day {
                column: "ts_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Hour {
                column: "ts_ns".to_string(),
            },
        ] {
            let err = build_initial_partition_spec(&schema, &[expr]).unwrap_err();
            assert!(
                err.contains("nanosecond"),
                "expected nanosecond in error, got: {err}"
            );
            assert!(
                err.contains("IV3-7.1"),
                "expected IV3-7.1 reference in error, got: {err}"
            );
        }
    }

    #[test]
    fn time_transform_on_ns_timestamptz_is_rejected() {
        let schema = schema_with_ns_timestamp();
        for expr in [
            IcebergPartitionFieldExpr::Year {
                column: "tstz_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Month {
                column: "tstz_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Day {
                column: "tstz_ns".to_string(),
            },
            IcebergPartitionFieldExpr::Hour {
                column: "tstz_ns".to_string(),
            },
        ] {
            let err = build_initial_partition_spec(&schema, &[expr]).unwrap_err();
            assert!(
                err.contains("nanosecond"),
                "expected nanosecond in error, got: {err}"
            );
        }
    }

    #[test]
    fn non_time_transforms_on_ns_timestamp_are_allowed() {
        let schema = schema_with_ns_timestamp();
        // Bucket and truncate do not depend on the tick base; they should be
        // permitted even for nanosecond columns.
        let result = build_initial_partition_spec(
            &schema,
            &[
                IcebergPartitionFieldExpr::Identity {
                    column: "ts_ns".to_string(),
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 4,
                },
            ],
        );
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
    }
}

#[cfg(test)]
mod overwrite_partitions_match_tests {
    use iceberg::spec::{Literal, PrimitiveLiteral, Struct};

    use super::*;

    fn region(value: &str) -> Struct {
        Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            value.to_string(),
        )))])
    }

    #[test]
    fn same_spec_equal_partition_returns_in_set() {
        let base = region("us");
        let touched = vec![region("us"), region("eu")];
        assert_eq!(
            partition_match_in_touched(&base, /*base_spec=*/ 0, /*current=*/ 0, &touched),
            PartitionMatch::InSet,
        );
    }

    #[test]
    fn same_spec_unequal_partition_returns_not_in_set() {
        let base = region("ap");
        let touched = vec![region("us"), region("eu")];
        assert_eq!(
            partition_match_in_touched(&base, 0, 0, &touched),
            PartitionMatch::NotInSet,
        );
    }

    #[test]
    fn different_spec_returns_different_spec() {
        let base = region("us");
        let touched = vec![region("us")];
        assert_eq!(
            partition_match_in_touched(&base, /*base=*/ 1, /*current=*/ 0, &touched),
            PartitionMatch::DifferentSpec,
        );
    }

    #[test]
    fn empty_touched_returns_not_in_set() {
        let base = region("us");
        let touched: Vec<Struct> = vec![];
        assert_eq!(
            partition_match_in_touched(&base, 0, 0, &touched),
            PartitionMatch::NotInSet,
        );
    }
}
