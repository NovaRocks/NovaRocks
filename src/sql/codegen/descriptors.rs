use arrow::datatypes::DataType;
use std::collections::BTreeSet;

use crate::descriptors;
use crate::sql::catalog::{IcebergSchemaDef, IcebergSchemaFieldDef, TableDef};
use crate::types;

use super::type_infer::arrow_type_to_type_desc;

pub(crate) struct DescriptorTableBuilder {
    slots: Vec<descriptors::TSlotDescriptor>,
    tuples: Vec<descriptors::TTupleDescriptor>,
    tables: Vec<descriptors::TTableDescriptor>,
    table_ids: BTreeSet<types::TTableId>,
}

impl DescriptorTableBuilder {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            tuples: Vec::new(),
            tables: Vec::new(),
            table_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn from_existing(desc_tbl: descriptors::TDescriptorTable) -> Self {
        let table_ids = desc_tbl
            .table_descriptors
            .as_ref()
            .map(|tables| tables.iter().map(|table| table.id).collect())
            .unwrap_or_default();
        Self {
            slots: desc_tbl.slot_descriptors.unwrap_or_default(),
            tuples: desc_tbl.tuple_descriptors,
            tables: desc_tbl.table_descriptors.unwrap_or_default(),
            table_ids,
        }
    }

    pub fn add_slot(
        &mut self,
        slot_id: types::TSlotId,
        tuple_id: types::TTupleId,
        name: &str,
        data_type: &DataType,
        nullable: bool,
        col_pos: i32,
    ) {
        let slot_type = match arrow_type_to_type_desc(data_type) {
            Ok(t) => t,
            Err(_) => return, // skip unsupported types
        };
        self.add_slot_with_type_desc(slot_id, tuple_id, name, slot_type, nullable, col_pos);
    }

    pub fn add_slot_with_type_desc(
        &mut self,
        slot_id: types::TSlotId,
        tuple_id: types::TTupleId,
        name: &str,
        slot_type: types::TTypeDesc,
        nullable: bool,
        col_pos: i32,
    ) {
        self.slots.push(descriptors::TSlotDescriptor::new(
            Some(slot_id),
            Some(tuple_id),
            Some(slot_type),
            Some(col_pos),
            Some(0), // byte_offset
            Some(0), // null_indicator_byte
            Some(0), // null_indicator_bit
            Some(name.to_string()),
            Some(col_pos),
            Some(true), // is_materialized
            Some(true), // is_output_column
            Some(nullable),
            None::<i32>,
            None::<String>,
            None::<bool>,
        ));
    }

    pub fn add_tuple(&mut self, tuple_id: types::TTupleId, table_id: Option<types::TTableId>) {
        self.tuples.push(descriptors::TTupleDescriptor::new(
            Some(tuple_id),
            Some(0), // byte_size
            Some(0), // num_null_bytes
            table_id,
            Some(0), // num_null_slots
        ));
    }

    pub fn add_table(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table_name: &str,
        num_cols: i32,
    ) {
        if !self.table_ids.insert(table_id) {
            return;
        }
        self.tables.push(descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::OLAP_TABLE,
            num_cols,
            0,
            table_name.to_string(),
            db_name.to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            None::<descriptors::TIcebergTable>,
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        ));
    }

    pub fn add_table_for_scan(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
    ) {
        if let Some(iceberg) = iceberg_table_info(&table.source) {
            self.add_iceberg_table(table_id, db_name, table, iceberg);
        } else {
            self.add_table(table_id, db_name, &table.name, table.columns.len() as i32);
        }
    }

    fn add_iceberg_table(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
        iceberg: &crate::sql::catalog::IcebergTableInfo,
    ) {
        self.add_iceberg_table_with_partition_info(
            table_id,
            db_name,
            table,
            iceberg,
            Vec::new(),
            false,
        );
    }

    pub(crate) fn add_iceberg_target_table(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
        iceberg: &crate::sql::catalog::IcebergTableInfo,
        partition_info: Vec<descriptors::TIcebergPartitionInfo>,
    ) {
        self.add_iceberg_table_with_partition_info(
            table_id,
            db_name,
            table,
            iceberg,
            partition_info,
            true,
        );
    }

    fn add_iceberg_table_with_partition_info(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table: &TableDef,
        iceberg: &crate::sql::catalog::IcebergTableInfo,
        partition_info: Vec<descriptors::TIcebergPartitionInfo>,
        include_serialized_metadata: bool,
    ) {
        if !self.table_ids.insert(table_id) {
            return;
        }
        let columns = table
            .columns
            .iter()
            .map(|column| {
                let type_desc = arrow_type_to_type_desc(&column.data_type).ok();
                descriptors::TColumn::new(
                    column.name.clone(),
                    None::<types::TColumnType>,
                    None::<types::TAggregationType>,
                    None::<bool>,
                    Some(column.nullable),
                    None::<String>,
                    None::<bool>,
                    None::<crate::exprs::TExpr>,
                    None::<bool>,
                    None::<i32>,
                    None::<bool>,
                    None::<types::TAggStateDesc>,
                    None::<i32>,
                    type_desc,
                    None::<crate::exprs::TExpr>,
                )
            })
            .collect::<Vec<_>>();
        let iceberg_table = descriptors::TIcebergTable::new(
            Some(iceberg.location.clone()),
            Some(columns),
            Some(to_thrift_iceberg_schema(&iceberg.schema)),
            None::<Vec<String>>,
            None::<descriptors::TCompressedPartitionMap>,
            None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
            None::<descriptors::TIcebergSchema>,
            (!partition_info.is_empty()).then_some(partition_info),
            None::<descriptors::TSortOrder>,
            include_serialized_metadata
                .then(|| iceberg.serialized_metadata.clone())
                .flatten(),
        );
        self.tables.push(descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::ICEBERG_TABLE,
            table.columns.len() as i32,
            0,
            table.name.clone(),
            db_name.to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg_table),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        ));
    }

    /// Mark all slots belonging to the given tuple as nullable.
    /// Used for outer/anti join nullable side columns.
    pub fn widen_tuple_nullable(&mut self, tuple_id: types::TTupleId) {
        for slot in &mut self.slots {
            if slot.parent == Some(tuple_id) {
                slot.is_nullable = Some(true);
            }
        }
    }

    pub fn build(self) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(self.slots),
            self.tuples,
            if self.tables.is_empty() {
                None::<Vec<descriptors::TTableDescriptor>>
            } else {
                Some(self.tables)
            },
            None::<bool>,
        )
    }
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState { .. } => None,
    }
}

fn to_thrift_iceberg_schema(schema: &IcebergSchemaDef) -> descriptors::TIcebergSchema {
    descriptors::TIcebergSchema::new(Some(
        schema
            .fields
            .iter()
            .map(to_thrift_iceberg_schema_field)
            .collect::<Vec<_>>(),
    ))
}

fn to_thrift_iceberg_schema_field(
    field: &IcebergSchemaFieldDef,
) -> descriptors::TIcebergSchemaField {
    // Prefer the spec-compliant JSON precomputed at iceberg-field construction
    // time (where the iceberg `Type` is available — required for decimal
    // because `Literal::Int128` carries no scale). Fall back to the
    // type-blind serializer for non-decimal cases that still arrive without
    // a precomputed JSON.
    let initial_default_json = match field.initial_default_json.clone() {
        Some(json) => Some(json),
        None => field
            .initial_default
            .as_ref()
            .map(serialize_iceberg_literal_json)
            .transpose()
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "drop initial_default_json: {err}");
                None
            }),
    };
    descriptors::TIcebergSchemaField::new(
        Some(field.field_id),
        Some(field.name.clone()),
        initial_default_json,
        (!field.children.is_empty()).then(|| {
            field
                .children
                .iter()
                .map(to_thrift_iceberg_schema_field)
                .map(Box::new)
                .collect::<Vec<_>>()
        }),
    )
}

fn serialize_iceberg_literal_json(literal: &iceberg::spec::Literal) -> Result<String, String> {
    match literal {
        iceberg::spec::Literal::Primitive(prim) => match prim {
            iceberg::spec::PrimitiveLiteral::Boolean(b) => Ok(b.to_string()),
            iceberg::spec::PrimitiveLiteral::Int(v) => Ok(v.to_string()),
            iceberg::spec::PrimitiveLiteral::Long(v) => Ok(v.to_string()),
            iceberg::spec::PrimitiveLiteral::Float(v) => Ok(v.0.to_string()),
            iceberg::spec::PrimitiveLiteral::Double(v) => Ok(v.0.to_string()),
            iceberg::spec::PrimitiveLiteral::Int128(v) => Ok(v.to_string()),
            iceberg::spec::PrimitiveLiteral::String(s) => {
                serde_json::to_string(s).map_err(|e| format!("serialize String default: {e}"))
            }
            iceberg::spec::PrimitiveLiteral::Binary(b) => {
                let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
                serde_json::to_string(&hex).map_err(|e| format!("serialize Binary default: {e}"))
            }
            other => Err(format!(
                "unsupported primitive literal for thrift emission: {other:?}"
            )),
        },
        other => Err(format!(
            "unsupported literal kind for thrift emission: {other:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, IcebergTableInfo, ScanSource};

    fn test_iceberg_table_info(schema: IcebergSchemaDef) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///warehouse/orders".to_string(),
            schema,
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    #[test]
    fn descriptor_builder_emits_iceberg_schema_field_ids() {
        let iceberg = test_iceberg_table_info(IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 7,
                name: "order_id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![IcebergSchemaFieldDef {
                    field_id: 8,
                    name: "nested".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                }],
            }],
        });
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let mut builder = DescriptorTableBuilder::new();

        builder.add_table_for_scan(42, "db1", &table);
        let desc = builder.build();

        let table_desc = desc
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|desc| desc.id == 42)
            .expect("iceberg table descriptor");
        assert_eq!(table_desc.table_type, types::TTableType::ICEBERG_TABLE);
        let iceberg = table_desc.iceberg_table.as_ref().expect("iceberg table");
        assert_eq!(
            iceberg.location.as_deref(),
            Some("file:///warehouse/orders")
        );
        let schema_field = &iceberg
            .iceberg_schema
            .as_ref()
            .expect("iceberg schema")
            .fields
            .as_ref()
            .expect("schema fields")[0];
        assert_eq!(schema_field.field_id, Some(7));
        assert_eq!(
            schema_field.children.as_ref().expect("children")[0].field_id,
            Some(8)
        );
        assert!(
            schema_field.children.as_ref().expect("children")[0]
                .children
                .is_none()
        );
    }

    #[test]
    fn descriptor_builder_emits_iceberg_initial_default_json() {
        use crate::sql::catalog::IcebergSchemaFieldDef;
        let field = IcebergSchemaFieldDef {
            field_id: 1,
            name: "c".to_string(),
            initial_default: Some(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::Int(5),
            )),
            write_default: None,
            initial_default_json: None,
            children: vec![],
        };
        let thrift = to_thrift_iceberg_schema_field(&field);
        assert_eq!(thrift.initial_default_json.as_deref(), Some("5"));
    }

    #[test]
    fn descriptor_builder_escapes_control_chars_in_string_default() {
        use crate::sql::catalog::IcebergSchemaFieldDef;
        let field = IcebergSchemaFieldDef {
            field_id: 1,
            name: "c".to_string(),
            initial_default: Some(iceberg::spec::Literal::Primitive(
                iceberg::spec::PrimitiveLiteral::String("a\nb\\c\"d".into()),
            )),
            write_default: None,
            initial_default_json: None,
            children: vec![],
        };
        let thrift = to_thrift_iceberg_schema_field(&field);
        // serde_json escapes \n as \n, \\ as \\\\, " as \"
        let json = thrift.initial_default_json.expect("present");
        let decoded: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        assert_eq!(decoded.as_str(), Some("a\nb\\c\"d"));
    }

    #[test]
    fn target_iceberg_descriptor_preserves_partition_info() {
        let mut builder = DescriptorTableBuilder::new();
        let iceberg = test_iceberg_table_info(IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            }],
        });
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg.clone(),
                files: vec![],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let partition_info = vec![descriptors::TIcebergPartitionInfo::new(
            Some("id".to_string()),
            Some("id".to_string()),
            Some("identity".to_string()),
            None::<crate::exprs::TExpr>,
        )];

        builder.add_iceberg_target_table(99, "db", &table, &iceberg, partition_info);
        let desc = builder.build();
        let tables = desc.table_descriptors.expect("table descriptors");
        let iceberg_table = tables[0].iceberg_table.as_ref().expect("iceberg table");
        let partitions = iceberg_table
            .partition_info
            .as_ref()
            .expect("partition info");
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].source_column_name.as_deref(), Some("id"));
        assert_eq!(partitions[0].transform_expr.as_deref(), Some("identity"));
    }
}
