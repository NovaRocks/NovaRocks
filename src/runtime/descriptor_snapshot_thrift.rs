use std::collections::HashMap;

use arrow::datatypes::Field;

use crate::common::ids::SlotId;
use crate::exec::row_position::RowPositionType;
use crate::formats::{FileFormatConfig, parquet::ParquetScanConfig};
use crate::runtime::descriptor_snapshot::{
    DescriptorIcebergSchema, DescriptorIcebergSchemaField, DescriptorLogicalType, DescriptorSlot,
    DescriptorSnapshot, DescriptorTable, DescriptorTableKind,
};
use crate::thrift::{descriptors, types};

pub(crate) type LookupNodeInfo = descriptors::TNodeInfo;
pub(crate) type LookupNodesInfo = descriptors::TNodesInfo;

pub(crate) fn descriptor_snapshot_from_thrift(
    desc: &descriptors::TDescriptorTable,
) -> Result<DescriptorSnapshot, String> {
    let mut tuple_to_table = HashMap::new();
    for tuple in &desc.tuple_descriptors {
        if let (Some(tuple_id), Some(table_id)) = (tuple.id, tuple.table_id) {
            tuple_to_table.insert(tuple_id, table_id);
        }
    }

    let tables = desc
        .table_descriptors
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(descriptor_table_from_thrift)
        .collect::<Result<Vec<_>, _>>()?;

    let Some(slot_descs) = desc.slot_descriptors.as_ref() else {
        return DescriptorSnapshot::new_with_tables(Vec::new(), tuple_to_table, tables);
    };
    let mut slots = Vec::with_capacity(slot_descs.len());
    for slot in slot_descs {
        let (Some(tuple_id), Some(raw_slot_id), Some(type_desc)) =
            (slot.parent, slot.id, slot.slot_type.as_ref())
        else {
            continue;
        };
        let slot_id = SlotId::try_from(raw_slot_id)?;
        let data_type =
            crate::lower::type_lowering::arrow_type_from_desc(type_desc).ok_or_else(|| {
                format!(
                    "unsupported descriptor slot type for tuple_id={} slot_id={}",
                    tuple_id, raw_slot_id
                )
            })?;
        let name = descriptor_slot_display_name(slot);
        let nullable = slot.is_nullable.unwrap_or(true);
        let logical = logical_type_from_desc(type_desc).unwrap_or(DescriptorLogicalType::Unknown);
        slots.push(DescriptorSlot {
            tuple_id,
            slot_id,
            name: name.clone(),
            field: Field::new(name, data_type, nullable),
            logical,
            unique_id: slot.col_unique_id.filter(|v| *v > 0),
        });
    }

    DescriptorSnapshot::new_with_tables(slots, tuple_to_table, tables)
}

pub(crate) fn is_iceberg_v3_row_position(row_position_type: RowPositionType) -> bool {
    row_position_type == RowPositionType::Iceberg
}

pub(crate) fn is_lake_row_position(row_position_type: RowPositionType) -> bool {
    row_position_type == RowPositionType::Lake
}

pub(crate) fn lookup_file_format_config(
    file_format: descriptors::THdfsFileFormat,
    parquet_cfg: ParquetScanConfig,
) -> Result<FileFormatConfig, String> {
    match file_format {
        descriptors::THdfsFileFormat::PARQUET => Ok(FileFormatConfig::Parquet(parquet_cfg)),
        other => Err(format!("lookup only supports PARQUET, got {:?}", other)),
    }
}

#[cfg(all(test, feature = "compat"))]
pub(crate) fn test_lookup_nodes_info(
    backend_id: i64,
    host: &str,
    async_internal_port: i32,
) -> LookupNodesInfo {
    descriptors::TNodesInfo::new(
        1,
        vec![descriptors::TNodeInfo::new(
            backend_id,
            0,
            host.to_string(),
            async_internal_port,
        )],
    )
}

fn descriptor_slot_display_name(desc: &descriptors::TSlotDescriptor) -> String {
    if let Some(name) = desc.col_name.as_ref().filter(|v| !v.is_empty()) {
        return name.clone();
    }
    if let Some(name) = desc.col_physical_name.as_ref().filter(|v| !v.is_empty()) {
        return name.clone();
    }
    match (desc.parent, desc.id) {
        (Some(parent), Some(id)) => format!("col_{parent}_{id}"),
        (_, Some(id)) => format!("col_{id}"),
        _ => "col_unknown".to_string(),
    }
}

fn descriptor_table_from_thrift(
    desc: &descriptors::TTableDescriptor,
) -> Result<DescriptorTable, String> {
    let kind = if desc.iceberg_table.is_some() {
        DescriptorTableKind::Iceberg
    } else if desc.paimon_table.is_some() {
        DescriptorTableKind::Paimon
    } else {
        DescriptorTableKind::Other
    };
    let iceberg_schema = desc
        .iceberg_table
        .as_ref()
        .and_then(|table| table.iceberg_schema.as_ref())
        .map(descriptor_iceberg_schema_from_thrift);
    Ok(DescriptorTable {
        id: desc.id,
        kind,
        iceberg_schema,
    })
}

fn descriptor_iceberg_schema_from_thrift(
    schema: &descriptors::TIcebergSchema,
) -> DescriptorIcebergSchema {
    DescriptorIcebergSchema {
        fields: schema.fields.as_ref().map(|fields| {
            fields
                .iter()
                .map(descriptor_iceberg_schema_field_from_thrift)
                .collect()
        }),
    }
}

fn descriptor_iceberg_schema_field_from_thrift(
    field: &descriptors::TIcebergSchemaField,
) -> DescriptorIcebergSchemaField {
    DescriptorIcebergSchemaField {
        field_id: field.field_id,
        name: field.name.clone(),
        initial_default_json: field.initial_default_json.clone(),
        children: field.children.as_ref().map(|children| {
            children
                .iter()
                .map(|child| descriptor_iceberg_schema_field_from_thrift(child.as_ref()))
                .collect()
        }),
    }
}

fn logical_type_from_desc(desc: &types::TTypeDesc) -> Option<DescriptorLogicalType> {
    let nodes = desc.types.as_ref()?;
    let scalar = nodes.first()?.scalar_type.as_ref()?;
    Some(match scalar.type_ {
        t if t == types::TPrimitiveType::NULL_TYPE => DescriptorLogicalType::Null,
        t if t == types::TPrimitiveType::BOOLEAN => DescriptorLogicalType::Boolean,
        t if t == types::TPrimitiveType::TINYINT => DescriptorLogicalType::Int8,
        t if t == types::TPrimitiveType::SMALLINT => DescriptorLogicalType::Int16,
        t if t == types::TPrimitiveType::INT => DescriptorLogicalType::Int32,
        t if t == types::TPrimitiveType::BIGINT => DescriptorLogicalType::Int64,
        t if t == types::TPrimitiveType::LARGEINT => DescriptorLogicalType::LargeInt,
        t if t == types::TPrimitiveType::FLOAT => DescriptorLogicalType::Float32,
        t if t == types::TPrimitiveType::DOUBLE => DescriptorLogicalType::Float64,
        t if t == types::TPrimitiveType::DATE => DescriptorLogicalType::Date,
        t if t == types::TPrimitiveType::DATETIME => DescriptorLogicalType::Timestamp,
        t if t == types::TPrimitiveType::TIME => DescriptorLogicalType::Time,
        t if t == types::TPrimitiveType::DECIMAL256 => DescriptorLogicalType::Decimal256 {
            precision: scalar
                .precision
                .and_then(|v| u8::try_from(v).ok())
                .unwrap_or(76),
            scale: scalar.scale.and_then(|v| i8::try_from(v).ok()).unwrap_or(0),
        },
        t if t == types::TPrimitiveType::DECIMAL
            || t == types::TPrimitiveType::DECIMAL32
            || t == types::TPrimitiveType::DECIMAL64
            || t == types::TPrimitiveType::DECIMAL128
            || t == types::TPrimitiveType::DECIMALV2 =>
        {
            let precision = scalar
                .precision
                .and_then(|v| u8::try_from(v).ok())
                .unwrap_or(38);
            let scale = scalar.scale.and_then(|v| i8::try_from(v).ok()).unwrap_or(0);
            if precision > 38 {
                DescriptorLogicalType::Decimal256 { precision, scale }
            } else {
                DescriptorLogicalType::Decimal128 { precision, scale }
            }
        }
        t if t == types::TPrimitiveType::CHAR || t == types::TPrimitiveType::VARCHAR => {
            DescriptorLogicalType::Utf8
        }
        t if t == types::TPrimitiveType::BINARY || t == types::TPrimitiveType::VARBINARY => {
            DescriptorLogicalType::Binary
        }
        t if t == types::TPrimitiveType::JSON => DescriptorLogicalType::Json,
        t if t == types::TPrimitiveType::VARIANT => DescriptorLogicalType::Variant,
        t if t == types::TPrimitiveType::HLL => DescriptorLogicalType::Hll,
        t if t == types::TPrimitiveType::OBJECT => DescriptorLogicalType::Object,
        t if t == types::TPrimitiveType::PERCENTILE => DescriptorLogicalType::Percentile,
        t if t == types::TPrimitiveType::FUNCTION => DescriptorLogicalType::Function,
        _ => DescriptorLogicalType::Unknown,
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::runtime::descriptor_snapshot::DescriptorLogicalType;
    use crate::thrift::descriptors;
    use crate::thrift::types::{
        TPrimitiveType, TScalarType, TStructField, TTypeDesc, TTypeNode, TTypeNodeType,
    };

    fn scalar(primitive: TPrimitiveType) -> TTypeDesc {
        crate::lower::type_lowering::scalar_type_desc(primitive)
    }

    fn decimal256(precision: i32, scale: i32) -> TTypeDesc {
        TTypeDesc::new(vec![TTypeNode::new(
            TTypeNodeType::SCALAR,
            Some(TScalarType::new(
                TPrimitiveType::DECIMAL256,
                None,
                Some(precision),
                Some(scale),
                None,
            )),
            None::<Vec<TStructField>>,
            None,
        )])
    }

    fn nested_type() -> TTypeDesc {
        TTypeDesc::new(vec![
            TTypeNode::new(
                TTypeNodeType::STRUCT,
                None::<TScalarType>,
                Some(vec![
                    TStructField::new(Some("items".to_string()), None, None, None),
                    TStructField::new(Some("attrs".to_string()), None, None, None),
                ]),
                None,
            ),
            TTypeNode::new(
                TTypeNodeType::ARRAY,
                None::<TScalarType>,
                None::<Vec<TStructField>>,
                None,
            ),
            TTypeNode::new(
                TTypeNodeType::SCALAR,
                Some(TScalarType::new(
                    TPrimitiveType::INT,
                    None,
                    None,
                    None,
                    None,
                )),
                None::<Vec<TStructField>>,
                None,
            ),
            TTypeNode::new(
                TTypeNodeType::MAP,
                None::<TScalarType>,
                None::<Vec<TStructField>>,
                None,
            ),
            TTypeNode::new(
                TTypeNodeType::SCALAR,
                Some(TScalarType::new(
                    TPrimitiveType::VARCHAR,
                    None,
                    None,
                    None,
                    None,
                )),
                None::<Vec<TStructField>>,
                None,
            ),
            TTypeNode::new(
                TTypeNodeType::SCALAR,
                Some(TScalarType::new(
                    TPrimitiveType::BIGINT,
                    None,
                    None,
                    None,
                    None,
                )),
                None::<Vec<TStructField>>,
                None,
            ),
        ])
    }

    fn slot(
        tuple_id: i32,
        slot_id: i32,
        col_name: Option<&str>,
        physical_name: Option<&str>,
        ty: TTypeDesc,
    ) -> descriptors::TSlotDescriptor {
        descriptors::TSlotDescriptor {
            id: Some(slot_id),
            parent: Some(tuple_id),
            slot_type: Some(ty),
            column_pos: None,
            byte_offset: None,
            null_indicator_byte: None,
            null_indicator_bit: None,
            col_name: col_name.map(ToString::to_string),
            slot_idx: None,
            is_materialized: Some(true),
            is_output_column: Some(true),
            is_nullable: Some(true),
            col_unique_id: Some(slot_id + 1000),
            col_physical_name: physical_name.map(ToString::to_string),
            is_virtual_column: None,
        }
    }

    fn desc(slots: Vec<descriptors::TSlotDescriptor>) -> descriptors::TDescriptorTable {
        desc_with_tables(slots, None)
    }

    fn desc_with_tables(
        slots: Vec<descriptors::TSlotDescriptor>,
        tables: Option<Vec<descriptors::TTableDescriptor>>,
    ) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(slots),
            vec![descriptors::TTupleDescriptor::new(
                Some(1),
                None,
                None,
                Some(10),
                None,
            )],
            tables,
            None,
        )
    }

    fn iceberg_table_descriptor(
        schema: Option<descriptors::TIcebergSchema>,
    ) -> descriptors::TTableDescriptor {
        let mut iceberg = descriptors::TIcebergTable::default();
        iceberg.iceberg_schema = schema;
        descriptors::TTableDescriptor::new(
            10,
            types::TTableType::ICEBERG_TABLE,
            1,
            0,
            "t".to_string(),
            "db".to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            Some(iceberg),
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        )
    }

    #[test]
    fn adapter_converts_named_logical_slots() {
        let snapshot = descriptor_snapshot_from_thrift(&desc(vec![
            slot(1, 1, Some("json_col"), None, scalar(TPrimitiveType::JSON)),
            slot(1, 2, Some("hll_col"), None, scalar(TPrimitiveType::HLL)),
            slot(1, 3, Some("obj_col"), None, scalar(TPrimitiveType::OBJECT)),
            slot(
                1,
                4,
                Some("pct_col"),
                None,
                scalar(TPrimitiveType::PERCENTILE),
            ),
            slot(1, 5, Some("fn_col"), None, scalar(TPrimitiveType::FUNCTION)),
            slot(
                1,
                6,
                Some("variant_col"),
                None,
                scalar(TPrimitiveType::VARIANT),
            ),
            slot(1, 7, Some("wide_decimal"), None, decimal256(76, 10)),
        ]))
        .expect("snapshot");

        assert_eq!(snapshot.table_id_for_tuple(1), Some(10));
        assert_eq!(
            snapshot.slot(1, SlotId::new(1)).expect("json").logical,
            DescriptorLogicalType::Json
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(2)).expect("hll").logical,
            DescriptorLogicalType::Hll
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(3)).expect("object").logical,
            DescriptorLogicalType::Object
        );
        assert_eq!(
            snapshot
                .slot(1, SlotId::new(4))
                .expect("percentile")
                .logical,
            DescriptorLogicalType::Percentile
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(5)).expect("function").logical,
            DescriptorLogicalType::Function
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(6)).expect("variant").logical,
            DescriptorLogicalType::Variant
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(7)).expect("decimal").logical,
            DescriptorLogicalType::Decimal256 {
                precision: 76,
                scale: 10
            }
        );
        assert_eq!(
            snapshot
                .slot(1, SlotId::new(7))
                .expect("decimal")
                .field
                .data_type(),
            &DataType::Decimal256(76, 10)
        );
    }

    #[test]
    fn adapter_projects_iceberg_table_schema_into_snapshot() {
        let mut schema_field = descriptors::TIcebergSchemaField::default();
        schema_field.field_id = Some(12);
        schema_field.name = Some("id".to_string());
        schema_field.initial_default_json = Some("7".to_string());
        let mut schema = descriptors::TIcebergSchema::default();
        schema.fields = Some(vec![schema_field]);

        let snapshot = descriptor_snapshot_from_thrift(&desc_with_tables(
            vec![slot(1, 1, Some("id"), None, scalar(TPrimitiveType::INT))],
            Some(vec![iceberg_table_descriptor(Some(schema))]),
        ))
        .expect("snapshot");

        assert!(snapshot.is_iceberg_table_for_tuple(1));
        let fields = snapshot
            .iceberg_schema_for_tuple(1)
            .and_then(|schema| schema.fields.as_ref())
            .expect("iceberg fields");
        assert_eq!(fields[0].field_id, Some(12));
        assert_eq!(fields[0].name.as_deref(), Some("id"));
        assert_eq!(fields[0].initial_default_json.as_deref(), Some("7"));
    }

    #[test]
    fn adapter_preserves_nested_arrow_type() {
        let snapshot = descriptor_snapshot_from_thrift(&desc(vec![slot(
            1,
            8,
            Some("nested"),
            None,
            nested_type(),
        )]))
        .expect("snapshot");
        let field = &snapshot.slot(1, SlotId::new(8)).expect("nested").field;

        let DataType::Struct(fields) = field.data_type() else {
            panic!("expected struct, got {:?}", field.data_type());
        };
        assert_eq!(fields[0].name(), "items");
        assert!(matches!(fields[0].data_type(), DataType::List(_)));
        assert_eq!(fields[1].name(), "attrs");
        assert!(matches!(fields[1].data_type(), DataType::Map(_, false)));
    }

    #[test]
    fn adapter_uses_display_name_fallbacks() {
        let snapshot = descriptor_snapshot_from_thrift(&desc(vec![
            slot(
                1,
                9,
                Some("logical_name"),
                Some("physical_name"),
                scalar(TPrimitiveType::INT),
            ),
            slot(
                1,
                10,
                Some(""),
                Some("physical_name"),
                scalar(TPrimitiveType::INT),
            ),
            slot(1, 11, None, None, scalar(TPrimitiveType::INT)),
        ]))
        .expect("snapshot");

        assert_eq!(
            snapshot.slot(1, SlotId::new(9)).expect("logical").name,
            "logical_name"
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(10)).expect("physical").name,
            "physical_name"
        );
        assert_eq!(
            snapshot.slot(1, SlotId::new(11)).expect("synthetic").name,
            "col_1_11"
        );
        assert_eq!(
            descriptor_slot_display_name(&descriptors::TSlotDescriptor {
                id: Some(12),
                parent: None,
                slot_type: Some(scalar(TPrimitiveType::INT)),
                column_pos: None,
                byte_offset: None,
                null_indicator_byte: None,
                null_indicator_bit: None,
                col_name: None,
                slot_idx: None,
                is_materialized: Some(true),
                is_output_column: Some(true),
                is_nullable: Some(true),
                col_unique_id: None,
                col_physical_name: None,
                is_virtual_column: None,
            }),
            "col_12"
        );
        assert_eq!(
            descriptor_slot_display_name(&descriptors::TSlotDescriptor {
                id: None,
                parent: None,
                slot_type: Some(scalar(TPrimitiveType::INT)),
                column_pos: None,
                byte_offset: None,
                null_indicator_byte: None,
                null_indicator_bit: None,
                col_name: None,
                slot_idx: None,
                is_materialized: Some(true),
                is_output_column: Some(true),
                is_nullable: Some(true),
                col_unique_id: None,
                col_physical_name: None,
                is_virtual_column: None,
            }),
            "col_unknown"
        );
    }

    #[test]
    fn adapter_rejects_duplicate_slots() {
        let err = descriptor_snapshot_from_thrift(&desc(vec![
            slot(1, 13, Some("a"), None, scalar(TPrimitiveType::INT)),
            slot(1, 13, Some("b"), None, scalar(TPrimitiveType::BIGINT)),
        ]))
        .expect_err("duplicate slot should fail");

        assert!(
            err.contains("duplicate descriptor slot tuple_id=1 slot_id=13"),
            "got: {err}"
        );
    }
}
