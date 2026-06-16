use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::{PrimitiveType, Transform, Type};

use crate::cloud_configuration::TCloudConfiguration;
use crate::data_sinks;
use crate::descriptors;
use crate::sql::catalog::{ColumnDef, IcebergTableInfo, TableDef};
use crate::types;

use super::type_infer::arrow_type_to_type_desc;

#[derive(Clone, Debug)]
pub(crate) struct IcebergWriteSinkSpec {
    pub mode: IcebergWriteSinkMode,
    pub target_table_id: i64,
    pub target_table: TableDef,
    pub iceberg: IcebergTableInfo,
    pub target_columns: Vec<ColumnDef>,
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub cloud_configuration: Option<TCloudConfiguration>,
    pub file_format: String,
    pub compression: types::TCompressionType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    #[allow(dead_code)]
    EqualityDeletes,
}

impl IcebergWriteSinkMode {
    fn data_sink_type(self) -> data_sinks::TDataSinkType {
        match self {
            Self::Data | Self::RowLineageData => data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            Self::PositionDeletes => data_sinks::TDataSinkType::ICEBERG_DELETE_SINK,
            Self::DeletionVectors => data_sinks::TDataSinkType::ICEBERG_DV_SINK,
            Self::EqualityDeletes => data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK,
        }
    }
}

pub(crate) fn synthetic_iceberg_write_table_id() -> i64 {
    -9_000_000_001
}

impl IcebergWriteSinkSpec {
    pub(crate) fn set_planned_snapshot_id(
        &mut self,
        planned_snapshot_id: Option<i64>,
    ) -> Result<(), String> {
        self.iceberg.current_snapshot_id = planned_snapshot_id;
        match &mut self.target_table.source {
            crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. } => {
                table.current_snapshot_id = planned_snapshot_id;
                Ok(())
            }
            other => Err(format!(
                "iceberg write sink expected IcebergDataFiles target source, got {other:?}"
            )),
        }
    }

    pub(crate) fn build_sink(&self, tuple_id: i32) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            self.mode.data_sink_type(),
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            Some(data_sinks::TIcebergTableSink::new(
                Some(self.table_location.clone()),
                Some(self.file_format.clone()),
                Some(self.target_table_id),
                Some(self.compression),
                Some(false),
                self.cloud_configuration.clone(),
                None::<i64>,
                Some(tuple_id),
                Some(self.data_location.clone()),
                Some(self.target_partition_spec_id),
            )),
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        )
    }
}

pub(crate) fn transform_to_thrift_string(transform: &iceberg::spec::Transform) -> String {
    transform.to_string()
}

pub(crate) fn partition_info_from_metadata(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<descriptors::TIcebergPartitionInfo>, String> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    spec.fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "iceberg write sink partition source field id {} not found",
                    field.source_id
                )
            })?;
            Ok(descriptors::TIcebergPartitionInfo::new(
                Some(source.name.clone()),
                Some(field.name.clone()),
                Some(transform_to_thrift_string(&field.transform)),
                Some(partition_expr_from_transform(source, &field.transform)?),
            ))
        })
        .collect()
}

pub(crate) fn partition_info_from_serialized_metadata(
    iceberg: &IcebergTableInfo,
) -> Result<Vec<descriptors::TIcebergPartitionInfo>, String> {
    let Some(serialized) = iceberg.serialized_metadata.as_ref() else {
        return Err(format!(
            "iceberg write sink requires serialized table metadata for {}.{}",
            iceberg.namespace, iceberg.table
        ));
    };
    let metadata =
        serde_json::from_str::<iceberg::spec::TableMetadata>(serialized).map_err(|e| {
            format!(
                "parse iceberg write sink serialized metadata for {}.{} failed: {e}",
                iceberg.namespace, iceberg.table
            )
        })?;
    partition_info_from_metadata(&metadata)
}

fn partition_expr_from_transform(
    source: &iceberg::spec::NestedField,
    transform: &Transform,
) -> Result<crate::exprs::TExpr, String> {
    let source_type = iceberg_type_to_arrow_type(source.field_type.as_ref())?;
    let source_node = source_column_slot_ref_placeholder_node(&source_type)?;
    let expr = match transform {
        Transform::Identity => crate::exprs::TExpr::new(vec![source_node]),
        Transform::Void => transform_call_expr(
            "__iceberg_transform_void",
            vec![source_node],
            &[source_type],
            DataType::Null,
        )?,
        Transform::Year => {
            time_transform_expr("__iceberg_transform_year", source_node, source_type)?
        }
        Transform::Month => {
            time_transform_expr("__iceberg_transform_month", source_node, source_type)?
        }
        Transform::Day => time_transform_expr("__iceberg_transform_day", source_node, source_type)?,
        Transform::Hour => {
            time_transform_expr("__iceberg_transform_hour", source_node, source_type)?
        }
        Transform::Bucket(width) => transform_call_expr(
            "__iceberg_transform_bucket",
            vec![
                source_node,
                super::expr_compiler::int_literal_node(i64::from(*width)),
            ],
            &[source_type, DataType::Int64],
            DataType::Int32,
        )?,
        Transform::Truncate(width) => transform_call_expr(
            "__iceberg_transform_truncate",
            vec![
                source_node,
                super::expr_compiler::int_literal_node(i64::from(*width)),
            ],
            &[source_type.clone(), DataType::Int64],
            source_type,
        )?,
        other => {
            return Err(format!(
                "unsupported iceberg partition transform for write sink: {other:?}"
            ));
        }
    };
    Ok(expr)
}

fn time_transform_expr(
    name: &str,
    source_node: crate::exprs::TExprNode,
    source_type: DataType,
) -> Result<crate::exprs::TExpr, String> {
    transform_call_expr(name, vec![source_node], &[source_type], DataType::Int64)
}

fn transform_call_expr(
    name: &str,
    children: Vec<crate::exprs::TExprNode>,
    arg_types: &[DataType],
    return_type: DataType,
) -> Result<crate::exprs::TExpr, String> {
    let ret_type = arrow_type_to_type_desc(&return_type)?;
    let fn_arg_types = arg_types
        .iter()
        .map(arrow_type_to_type_desc)
        .collect::<Result<Vec<_>, _>>()?;
    let mut nodes = Vec::with_capacity(children.len() + 1);
    nodes.push(crate::exprs::TExprNode {
        node_type: crate::exprs::TExprNodeType::FUNCTION_CALL,
        type_: ret_type.clone(),
        num_children: children.len() as i32,
        fn_: Some(types::TFunction {
            name: types::TFunctionName {
                db_name: None,
                function_name: name.to_string(),
            },
            binary_type: types::TFunctionBinaryType::BUILTIN,
            arg_types: fn_arg_types,
            ret_type,
            has_var_args: false,
            comment: None,
            signature: None,
            hdfs_location: None,
            scalar_fn: None,
            aggregate_fn: None,
            id: None,
            checksum: None,
            agg_state_desc: None,
            fid: None,
            table_fn: None,
            could_apply_dict_optimize: None,
            ignore_nulls: None,
            isolated: None,
            input_type: None,
            content: None,
        }),
        ..super::expr_compiler::default_expr_node()
    });
    nodes.extend(children);
    Ok(crate::exprs::TExpr::new(nodes))
}

fn source_column_slot_ref_placeholder_node(
    source_type: &DataType,
) -> Result<crate::exprs::TExprNode, String> {
    super::expr_compiler::build_slot_ref_texpr(0, 0, arrow_type_to_type_desc(source_type)?)
        .nodes
        .into_iter()
        .next()
        .ok_or_else(|| "iceberg partition placeholder slot ref is empty".to_string())
}

fn iceberg_type_to_arrow_type(ty: &Type) -> Result<DataType, String> {
    match ty {
        Type::Primitive(primitive) => Ok(match primitive {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision)
                    .map_err(|_| format!("iceberg decimal precision out of range: {precision}"))?,
                i8::try_from(*scale)
                    .map_err(|_| format!("iceberg decimal scale out of range: {scale}"))?,
            ),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
                i32::try_from(*width)
                    .map_err(|_| format!("iceberg fixed width out of range: {width}"))?,
            ),
            PrimitiveType::Binary | PrimitiveType::Variant => DataType::Binary,
        }),
        other => Err(format!(
            "iceberg partition transform source type must be primitive, got {other:?}"
        )),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::catalog::{IcebergSchemaDef, IcebergSchemaFieldDef, ScanSource};

    pub(crate) fn simple_sink_spec() -> IcebergWriteSinkSpec {
        let iceberg = IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000002".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: "file:///warehouse/target_orders".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: Vec::new(),
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let target_table = TableDef {
            name: "target_orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg.clone(),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };

        IcebergWriteSinkSpec {
            mode: IcebergWriteSinkMode::Data,
            target_table_id: 99,
            target_table,
            iceberg,
            target_columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            table_location: "file:///warehouse/target_orders".to_string(),
            data_location: "file:///warehouse/target_orders/data".to_string(),
            target_partition_spec_id: 0,
            cloud_configuration: None,
            file_format: "parquet".to_string(),
            compression: types::TCompressionType::SNAPPY,
        }
    }

    pub(crate) fn single_bucket_partition_metadata_json() -> String {
        use std::sync::Arc;

        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id_bucket", iceberg::spec::Transform::Bucket(16))
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/target_orders".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        serde_json::to_string(&metadata.metadata).expect("serialize metadata")
    }

    pub(crate) fn unpartitioned_metadata_json() -> String {
        use std::sync::Arc;

        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .build()
            .expect("partition spec");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/target_orders".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        serde_json::to_string(&metadata.metadata).expect("serialize metadata")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn metadata_with_single_partition(
        transform: iceberg::spec::Transform,
    ) -> iceberg::spec::TableMetadata {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "id_bucket", transform)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/orders".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        metadata.metadata
    }

    fn metadata_with_timestamp_partition(
        transform: iceberg::spec::Transform,
    ) -> iceberg::spec::TableMetadata {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "ts",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Timestamp),
            ))])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(schema.clone())
            .add_partition_field("ts", "ts_month", transform)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/orders".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        metadata.metadata
    }

    #[test]
    fn transform_to_thrift_string_matches_sink_parser_contract() {
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Identity),
            "identity"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Bucket(16)),
            "bucket[16]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Truncate(8)),
            "truncate[8]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Day),
            "day"
        );
    }

    #[test]
    fn build_sink_uses_delete_sink_type_for_position_deletes() {
        let mut spec = test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::PositionDeletes;

        let sink = spec.build_sink(3);

        assert_eq!(sink.type_, data_sinks::TDataSinkType::ICEBERG_DELETE_SINK);
        assert!(sink.iceberg_table_sink.is_some());
    }

    #[test]
    fn dv_mode_maps_to_iceberg_dv_sink() {
        let mut spec = test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::DeletionVectors;
        let sink = spec.build_sink(0);
        assert_eq!(sink.type_, data_sinks::TDataSinkType::ICEBERG_DV_SINK);
    }

    #[test]
    fn equality_delete_mode_maps_to_iceberg_equality_delete_sink() {
        assert_eq!(
            IcebergWriteSinkMode::EqualityDeletes.data_sink_type(),
            data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK
        );
    }

    #[test]
    fn planned_snapshot_id_updates_sink_spec_and_target_source() {
        let mut spec = test_support::simple_sink_spec();

        spec.set_planned_snapshot_id(Some(42))
            .expect("set planned snapshot");

        assert_eq!(spec.iceberg.current_snapshot_id, Some(42));
        let crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. } =
            &spec.target_table.source
        else {
            panic!("expected IcebergDataFiles source");
        };
        assert_eq!(table.current_snapshot_id, Some(42));

        spec.set_planned_snapshot_id(None)
            .expect("clear planned snapshot");
        assert_eq!(spec.iceberg.current_snapshot_id, None);
        let crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. } =
            &spec.target_table.source
        else {
            panic!("expected IcebergDataFiles source");
        };
        assert_eq!(table.current_snapshot_id, None);
    }

    #[test]
    fn partition_info_from_metadata_includes_slot_ref_placeholder_expr() {
        let metadata = metadata_with_single_partition(iceberg::spec::Transform::Identity);

        let partition_info = partition_info_from_metadata(&metadata).expect("partition info");

        assert_eq!(partition_info.len(), 1);
        let expr = partition_info[0]
            .partition_expr
            .as_ref()
            .expect("partition expr");
        assert_eq!(expr.nodes.len(), 1);
        assert_eq!(
            expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::SLOT_REF
        );
        assert!(expr.nodes[0].slot_ref.is_some());
    }

    #[test]
    fn partition_info_from_serialized_metadata_preserves_bucket_transform() {
        let metadata = metadata_with_single_partition(iceberg::spec::Transform::Bucket(16));
        let mut spec = test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata =
            Some(serde_json::to_string(&metadata).expect("serialize metadata"));

        let partition_info =
            partition_info_from_serialized_metadata(&spec.iceberg).expect("partition info");

        assert_eq!(partition_info.len(), 1);
        assert_eq!(partition_info[0].source_column_name.as_deref(), Some("id"));
        assert_eq!(
            partition_info[0].partition_column_name.as_deref(),
            Some("id_bucket")
        );
        assert_eq!(
            partition_info[0].transform_expr.as_deref(),
            Some("bucket[16]")
        );
        let expr = partition_info[0]
            .partition_expr
            .as_ref()
            .expect("partition expr");
        assert_eq!(expr.nodes.len(), 3);
        assert_eq!(
            expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::FUNCTION_CALL
        );
        assert_eq!(
            expr.nodes[0]
                .fn_
                .as_ref()
                .map(|func| func.name.function_name.as_str()),
            Some("__iceberg_transform_bucket")
        );
        assert_eq!(
            expr.nodes[1].node_type,
            crate::exprs::TExprNodeType::SLOT_REF
        );
        assert_eq!(
            expr.nodes[2].int_literal.as_ref().map(|lit| lit.value),
            Some(16)
        );
    }

    #[test]
    fn partition_info_from_metadata_builds_month_transform_expr() {
        let metadata = metadata_with_timestamp_partition(iceberg::spec::Transform::Month);

        let partition_info = partition_info_from_metadata(&metadata).expect("partition info");

        let expr = partition_info[0]
            .partition_expr
            .as_ref()
            .expect("partition expr");
        assert_eq!(expr.nodes.len(), 2);
        assert_eq!(
            expr.nodes[0].node_type,
            crate::exprs::TExprNodeType::FUNCTION_CALL
        );
        assert_eq!(
            expr.nodes[0]
                .fn_
                .as_ref()
                .map(|func| func.name.function_name.as_str()),
            Some("__iceberg_transform_month")
        );
        assert_eq!(
            expr.nodes[1].node_type,
            crate::exprs::TExprNodeType::SLOT_REF
        );
    }

    #[test]
    fn partition_info_from_serialized_metadata_requires_metadata() {
        let spec = test_support::simple_sink_spec();

        let err = partition_info_from_serialized_metadata(&spec.iceberg)
            .expect_err("missing metadata must fail");

        assert!(err.contains("iceberg write sink requires serialized table metadata"));
        assert!(err.contains("test_db.target_orders"));
    }

    #[test]
    fn partition_info_from_serialized_metadata_rejects_invalid_json() {
        let mut spec = test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata = Some("{not valid json".to_string());

        let err = partition_info_from_serialized_metadata(&spec.iceberg)
            .expect_err("invalid metadata json must fail");

        assert!(err.contains("parse iceberg write sink serialized metadata"));
    }
}
