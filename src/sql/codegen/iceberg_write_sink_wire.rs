use std::collections::BTreeMap;

use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::{PrimitiveType, Transform, Type};

use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteOutputField, PositionDeletePartitionSourceField,
};
use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::planner::write_sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
    transform_to_sink_string,
};
use crate::thrift::cloud_configuration;
use crate::thrift::data_sinks;
use crate::thrift::descriptors;
use crate::thrift::types;

use super::type_infer::arrow_type_to_type_desc;

pub(crate) fn build_iceberg_write_sink_thrift(
    spec: &IcebergWriteSinkSpec,
    tuple_id: i32,
) -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sink_type(spec.mode),
        None::<data_sinks::TDataStreamSink>,
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        Some(data_sinks::TIcebergTableSink::new(
            Some(spec.table_location.clone()),
            Some(spec.file_format.clone()),
            Some(spec.target_table_id),
            Some(compression_to_thrift(spec.compression)),
            Some(false),
            cloud_configuration_from_properties(&spec.cloud_properties),
            None::<i64>,
            Some(tuple_id),
            Some(spec.data_location.clone()),
            Some(spec.target_partition_spec_id),
            spec.position_delete_output_descriptor
                .as_ref()
                .map(position_delete_descriptor_to_thrift),
        )),
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
        None::<data_sinks::TIcebergChangeStreamRouterSink>,
    )
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

pub(in crate::sql::codegen) fn add_iceberg_sink_target_table_to_desc_builder(
    desc_builder: &mut DescriptorTableBuilder,
    current_database: &str,
    sink_spec: &IcebergWriteSinkSpec,
) -> Result<(), String> {
    let partition_info = partition_info_from_serialized_metadata(&sink_spec.iceberg)?;
    let equality_delete_schema = equality_delete_schema_for_sink(sink_spec)?;
    desc_builder.add_iceberg_target_table(
        sink_spec.target_table_id,
        current_database,
        &sink_spec.target_table,
        &sink_spec.iceberg,
        partition_info,
        equality_delete_schema.as_ref(),
    );
    Ok(())
}

fn equality_delete_schema_for_sink(
    sink_spec: &IcebergWriteSinkSpec,
) -> Result<Option<IcebergSchemaDef>, String> {
    if sink_spec.mode != IcebergWriteSinkMode::EqualityDeletes {
        return Ok(None);
    }
    if sink_spec.target_columns.is_empty() {
        return Err(
            "iceberg equality-delete sink requires at least one equality column".to_string(),
        );
    }

    let mut fields = Vec::with_capacity(sink_spec.target_columns.len());
    for column in &sink_spec.target_columns {
        let field = sink_spec
            .iceberg
            .schema
            .fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(&column.name))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "iceberg equality-delete sink column `{}` missing from iceberg schema",
                    column.name
                )
            })?;
        fields.push(field);
    }

    Ok(Some(IcebergSchemaDef { fields }))
}

fn data_sink_type(mode: IcebergWriteSinkMode) -> data_sinks::TDataSinkType {
    match mode {
        IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData => {
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK
        }
        IcebergWriteSinkMode::PositionDeletes => data_sinks::TDataSinkType::ICEBERG_DELETE_SINK,
        IcebergWriteSinkMode::DeletionVectors => data_sinks::TDataSinkType::ICEBERG_DV_SINK,
        IcebergWriteSinkMode::EqualityDeletes => {
            data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK
        }
    }
}

fn compression_to_thrift(compression: IcebergWriteFileCompression) -> types::TCompressionType {
    match compression {
        IcebergWriteFileCompression::Snappy => types::TCompressionType::SNAPPY,
    }
}

fn cloud_configuration_from_properties(
    cloud_properties: &BTreeMap<String, String>,
) -> Option<cloud_configuration::TCloudConfiguration> {
    if cloud_properties.is_empty() {
        return None;
    }
    Some(cloud_configuration::TCloudConfiguration::new(
        None::<cloud_configuration::TCloudType>,
        None::<Vec<cloud_configuration::TCloudProperty>>,
        Some(cloud_properties.clone()),
        None::<bool>,
    ))
}

fn position_delete_descriptor_to_thrift(
    desc: &PositionDeleteDescriptorInput,
) -> data_sinks::TIcebergPositionDeleteOutputDescriptor {
    data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
        Some(position_delete_output_field_to_thrift(&desc.file_path)),
        Some(position_delete_output_field_to_thrift(&desc.pos)),
        Some(
            desc.partition_source_fields
                .iter()
                .map(position_delete_partition_source_field_to_thrift)
                .collect(),
        ),
        Some(desc.target_partition_spec_id),
    )
}

fn position_delete_output_field_to_thrift(
    field: &PositionDeleteOutputField,
) -> data_sinks::TIcebergPositionDeleteOutputField {
    data_sinks::TIcebergPositionDeleteOutputField::new(
        Some(
            i32::try_from(field.output_expr_index)
                .expect("position-delete output field index must fit i32"),
        ),
        Some(field.name.clone()),
        Some(
            arrow_type_to_type_desc(&field.data_type)
                .expect("position-delete output field type must be thrift-compatible"),
        ),
        Some(field.field_id),
    )
}

fn position_delete_partition_source_field_to_thrift(
    field: &PositionDeletePartitionSourceField,
) -> data_sinks::TIcebergPositionDeletePartitionSourceField {
    data_sinks::TIcebergPositionDeletePartitionSourceField::new(
        Some(
            i32::try_from(field.output_expr_index)
                .expect("position-delete partition source index must fit i32"),
        ),
        Some(field.source_column_name.clone()),
        Some(field.partition_field_name.clone()),
        Some(field.transform_expr.clone()),
        Some(field.source_field_id),
    )
}

fn partition_info_from_metadata(
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
                Some(transform_to_sink_string(&field.transform)),
                Some(partition_expr_from_transform(source, &field.transform)?),
            ))
        })
        .collect()
}

fn partition_expr_from_transform(
    source: &iceberg::spec::NestedField,
    transform: &Transform,
) -> Result<crate::thrift::exprs::TExpr, String> {
    let source_type = iceberg_type_to_arrow_type(source.field_type.as_ref())?;
    let source_node = source_column_slot_ref_placeholder_node(&source_type)?;
    let expr = match transform {
        Transform::Identity => crate::thrift::exprs::TExpr::new(vec![source_node]),
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
    source_node: crate::thrift::exprs::TExprNode,
    source_type: DataType,
) -> Result<crate::thrift::exprs::TExpr, String> {
    transform_call_expr(name, vec![source_node], &[source_type], DataType::Int64)
}

fn transform_call_expr(
    name: &str,
    children: Vec<crate::thrift::exprs::TExprNode>,
    arg_types: &[DataType],
    return_type: DataType,
) -> Result<crate::thrift::exprs::TExpr, String> {
    let ret_type = arrow_type_to_type_desc(&return_type)?;
    let fn_arg_types = arg_types
        .iter()
        .map(arrow_type_to_type_desc)
        .collect::<Result<Vec<_>, _>>()?;
    let mut nodes = Vec::with_capacity(children.len() + 1);
    nodes.push(crate::thrift::exprs::TExprNode {
        node_type: crate::thrift::exprs::TExprNodeType::FUNCTION_CALL,
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
    Ok(crate::thrift::exprs::TExpr::new(nodes))
}

fn source_column_slot_ref_placeholder_node(
    source_type: &DataType,
) -> Result<crate::thrift::exprs::TExprNode, String> {
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
mod tests {
    use super::*;
    use crate::connector::iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        PositionDeleteDescriptorInput, PositionDeleteOutputField,
        PositionDeletePartitionSourceField,
    };
    use crate::sql::planner::write_sink::{
        IcebergWriteSinkMode, test_support, transform_to_sink_string,
    };

    fn assert_position_delete_output_field(
        field: Option<&crate::thrift::data_sinks::TIcebergPositionDeleteOutputField>,
        output_expr_index: i32,
        name: &str,
        primitive: crate::thrift::types::TPrimitiveType,
        field_id: i32,
    ) {
        let field = field.expect("position delete output field");
        assert_eq!(field.output_expr_index, Some(output_expr_index));
        assert_eq!(field.name.as_deref(), Some(name));
        assert_eq!(field.field_id, Some(field_id));
        assert_eq!(
            field
                .type_desc
                .as_ref()
                .and_then(crate::types::arrow_thrift::thrift_desc_to_primitive),
            Some(primitive)
        );
    }

    fn test_position_delete_descriptor() -> PositionDeleteDescriptorInput {
        PositionDeleteDescriptorInput {
            file_path: PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            },
            pos: PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            },
            partition_source_fields: vec![PositionDeletePartitionSourceField {
                output_expr_index: 2,
                source_column_name: "id".to_string(),
                partition_field_name: "id_bucket".to_string(),
                transform_expr: transform_to_sink_string(&iceberg::spec::Transform::Bucket(8)),
                source_field_id: 42,
                data_type: arrow::datatypes::DataType::Int32,
            }],
            target_partition_spec_id: 7,
        }
    }

    #[test]
    fn build_iceberg_write_sink_thrift_uses_delete_sink_type_for_position_deletes() {
        let mut spec = test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::PositionDeletes;

        let sink = build_iceberg_write_sink_thrift(&spec, 3);

        assert_eq!(
            sink.type_,
            crate::thrift::data_sinks::TDataSinkType::ICEBERG_DELETE_SINK
        );
        assert!(sink.iceberg_table_sink.is_some());
    }

    #[test]
    fn build_iceberg_write_sink_thrift_maps_dv_and_equality_modes() {
        let mut spec = test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::DeletionVectors;
        let sink = build_iceberg_write_sink_thrift(&spec, 0);
        assert_eq!(
            sink.type_,
            crate::thrift::data_sinks::TDataSinkType::ICEBERG_DV_SINK
        );

        spec.mode = IcebergWriteSinkMode::EqualityDeletes;
        let sink = build_iceberg_write_sink_thrift(&spec, 0);
        assert_eq!(
            sink.type_,
            crate::thrift::data_sinks::TDataSinkType::ICEBERG_EQUALITY_DELETE_SINK
        );
    }

    #[test]
    fn build_iceberg_write_sink_thrift_maps_internal_position_delete_descriptor() {
        let mut spec = test_support::simple_sink_spec();
        spec.mode = IcebergWriteSinkMode::PositionDeletes;
        spec.position_delete_output_descriptor = Some(test_position_delete_descriptor());

        let sink = build_iceberg_write_sink_thrift(&spec, 17);
        let desc = sink
            .iceberg_table_sink
            .as_ref()
            .and_then(|sink| sink.position_delete_output_descriptor.as_ref())
            .expect("sink descriptor");

        assert_eq!(desc.target_partition_spec_id, Some(7));
        assert_position_delete_output_field(
            desc.file_path.as_ref(),
            0,
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
            crate::thrift::types::TPrimitiveType::VARCHAR,
            ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        );
        assert_position_delete_output_field(
            desc.pos.as_ref(),
            1,
            ICEBERG_POSITION_DELETE_POS_COLUMN,
            crate::thrift::types::TPrimitiveType::BIGINT,
            ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        );
        let partition_field = desc
            .partition_source_fields
            .as_ref()
            .and_then(|fields| fields.first())
            .expect("partition source field");
        assert_eq!(partition_field.output_expr_index, Some(2));
        assert_eq!(partition_field.source_column_name.as_deref(), Some("id"));
        assert_eq!(
            partition_field.partition_field_name.as_deref(),
            Some("id_bucket")
        );
        assert_eq!(partition_field.transform_expr.as_deref(), Some("bucket[8]"));
        assert_eq!(partition_field.source_field_id, Some(42));
    }

    #[test]
    fn partition_info_from_serialized_metadata_preserves_bucket_transform() {
        let mut spec = test_support::simple_sink_spec();
        spec.iceberg.serialized_metadata =
            Some(test_support::single_bucket_partition_metadata_json());

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
            crate::thrift::exprs::TExprNodeType::FUNCTION_CALL
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
            crate::thrift::exprs::TExprNodeType::SLOT_REF
        );
        assert_eq!(
            expr.nodes[2].int_literal.as_ref().map(|lit| lit.value),
            Some(16)
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
}
