use std::collections::BTreeMap;

use crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput;
use crate::sql::catalog::{ColumnDef, IcebergTableInfo, TableDef};

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
    pub cloud_properties: BTreeMap<String, String>,
    pub file_format: String,
    pub compression: IcebergWriteFileCompression,
    pub position_delete_output_descriptor: Option<PositionDeleteDescriptorInput>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergWriteFileCompression {
    Snappy,
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
}

pub(crate) fn transform_to_sink_string(transform: &iceberg::spec::Transform) -> String {
    transform.to_string()
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
            cloud_properties: BTreeMap::new(),
            file_format: "parquet".to_string(),
            compression: IcebergWriteFileCompression::Snappy,
            position_delete_output_descriptor: None,
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

    #[test]
    fn transform_to_sink_string_matches_sink_parser_contract() {
        assert_eq!(
            transform_to_sink_string(&iceberg::spec::Transform::Identity),
            "identity"
        );
        assert_eq!(
            transform_to_sink_string(&iceberg::spec::Transform::Bucket(16)),
            "bucket[16]"
        );
        assert_eq!(
            transform_to_sink_string(&iceberg::spec::Transform::Truncate(8)),
            "truncate[8]"
        );
        assert_eq!(
            transform_to_sink_string(&iceberg::spec::Transform::Day),
            "day"
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
    fn iceberg_write_sink_domain_module_has_no_thrift_wire_types() {
        let source = include_str!("iceberg_write_sink.rs");
        for pattern in [
            concat!("crate::", "thrift"),
            concat!("TType", "Desc"),
            concat!("TPrimitive", "Type"),
            concat!("TCompression", "Type"),
            concat!("TIce", "berg"),
            concat!("thrift_type_desc_from", "_primitive"),
        ] {
            assert!(
                !source.contains(pattern),
                "domain iceberg write sink module must not contain wire type pattern `{pattern}`"
            );
        }
    }
}
