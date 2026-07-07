use arrow::datatypes::{DataType, Field};

use crate::lower::compat::type_lowering::{scalar_type_desc, thrift_primitive_from_native};
use crate::thrift::types;
use crate::types::arrow_primitive::{arrow_type_to_primitive, field_logical_primitive};

/// Convert Arrow DataType to Thrift TTypeDesc.
pub(crate) fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_arrow_type_nodes(data_type, None, &mut nodes)?;
    Ok(types::TTypeDesc::new(nodes))
}

/// Convert an Arrow Field to Thrift TTypeDesc at a protocol/codegen boundary.
#[allow(dead_code)] // Staged M2 boundary helper; callers migrate in later slices.
pub(crate) fn arrow_field_to_type_desc(field: &Field) -> Result<types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_arrow_type_nodes(field.data_type(), Some(field), &mut nodes)?;
    Ok(types::TTypeDesc::new(nodes))
}

fn append_arrow_type_nodes(
    data_type: &DataType,
    parent_field: Option<&Field>,
    nodes: &mut Vec<types::TTypeNode>,
) -> Result<(), String> {
    // If the enclosing `Field` carries a logical-type tag, override the
    // inferred primitive so the child reports e.g. JSON instead of VARCHAR.
    if let Some(primitive) = parent_field.and_then(field_logical_primitive) {
        nodes.extend(
            scalar_type_desc(thrift_primitive_from_native(primitive))
                .types
                .unwrap_or_default(),
        );
        return Ok(());
    }
    match data_type {
        DataType::List(field) => {
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::ARRAY,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_arrow_type_nodes(field.data_type(), Some(field.as_ref()), nodes)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(format!(
                    "MAP logical entries field must be Struct, got {:?}",
                    entries.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err(format!(
                    "MAP logical entries field must have exactly 2 children, got {}",
                    fields.len()
                ));
            }
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::MAP,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_arrow_type_nodes(fields[0].data_type(), Some(fields[0].as_ref()), nodes)?;
            append_arrow_type_nodes(fields[1].data_type(), Some(fields[1].as_ref()), nodes)
        }
        DataType::Struct(fields) => {
            nodes.push(types::TTypeNode {
                type_: types::TTypeNodeType::STRUCT,
                scalar_type: None,
                is_named: None,
                struct_fields: Some(
                    fields
                        .iter()
                        .map(|field| {
                            types::TStructField::new(
                                Some(field.name().to_string()),
                                None::<String>,
                                None::<i32>,
                                None::<String>,
                            )
                        })
                        .collect(),
                ),
            });
            for field in fields {
                append_arrow_type_nodes(field.data_type(), Some(field.as_ref()), nodes)?;
            }
            Ok(())
        }
        DataType::Decimal128(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL128,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
                None,
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
        DataType::Decimal256(p, s) => {
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DECIMAL256,
                None::<i32>,
                Some(i32::from(*p)),
                Some(i32::from(*s)),
                None,
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
        DataType::Timestamp(unit, _tz) => {
            // Carry the time unit so the unitless thrift DATETIME descriptor does
            // not collapse nanosecond to microsecond. tz is intentionally not
            // carried (DATETIME descriptors are tz-less); the nanosecond value is
            // preserved regardless. Microsecond keeps time_unit absent so
            // FE-compat descriptors stay byte-identical.
            let time_unit = crate::lower::compat::type_lowering::thrift_time_unit_for_arrow(*unit)?;
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DATETIME,
                None::<i32>,
                None::<i32>,
                None::<i32>,
                time_unit,
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
        _ => {
            let primitive = thrift_primitive_from_native(arrow_type_to_primitive(data_type)?);
            nodes.extend(scalar_type_desc(primitive).types.unwrap_or_default());
            Ok(())
        }
    }
}

pub(crate) use crate::types::{arithmetic_result_type_with_op, wider_type};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PrimitiveType;
    use crate::types::arrow_primitive::arrow_field_to_primitive;
    use crate::types::logical::LogicalType;

    fn primitive_from_desc(desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
        crate::lower::compat::type_lowering::primitive_type_from_desc(desc)
    }

    fn logical_field(name: &str, data_type: DataType, logical_type: LogicalType) -> Field {
        crate::types::logical::field_with_logical_type(
            Field::new(name, data_type, true),
            logical_type,
        )
    }

    #[test]
    fn arrow_field_to_type_desc_honors_top_level_json_metadata() {
        let field = logical_field("payload", DataType::Utf8, LogicalType::Json);

        let desc = arrow_field_to_type_desc(&field).unwrap();

        assert_eq!(
            primitive_from_desc(&desc),
            Some(types::TPrimitiveType::JSON)
        );
        assert_eq!(arrow_field_to_primitive(&field), Some(PrimitiveType::Json));
    }

    #[test]
    fn arrow_field_to_primitive_honors_object_family_metadata() {
        let cases = [
            (
                logical_field("hll", DataType::Binary, LogicalType::Hll),
                types::TPrimitiveType::HLL,
                PrimitiveType::Hll,
            ),
            (
                logical_field("bitmap", DataType::Binary, LogicalType::Bitmap),
                types::TPrimitiveType::OBJECT,
                PrimitiveType::Object,
            ),
            (
                logical_field("object", DataType::LargeBinary, LogicalType::Object),
                types::TPrimitiveType::OBJECT,
                PrimitiveType::Object,
            ),
            (
                logical_field("percentile", DataType::Binary, LogicalType::Percentile),
                types::TPrimitiveType::PERCENTILE,
                PrimitiveType::Percentile,
            ),
        ];

        for (field, thrift_expected, native_expected) in cases {
            let desc = arrow_field_to_type_desc(&field).unwrap();

            assert_eq!(primitive_from_desc(&desc), Some(thrift_expected));
            assert_eq!(arrow_field_to_primitive(&field), Some(native_expected));
        }
    }

    #[test]
    fn arrow_field_to_primitive_falls_back_to_arrow_type_without_metadata() {
        let field = Field::new("plain", DataType::Utf8, true);

        assert_eq!(
            arrow_field_to_primitive(&field),
            Some(PrimitiveType::Varchar)
        );
    }

    #[test]
    fn nested_json_metadata_survives_type_desc_conversion() {
        use std::sync::Arc;

        let array_json = DataType::List(Arc::new(logical_field(
            "item",
            DataType::Utf8,
            LogicalType::Json,
        )));
        let array_desc = arrow_type_to_type_desc(&array_json).unwrap();
        let array_nodes = array_desc.types.as_ref().unwrap();
        assert_eq!(array_nodes[0].type_, types::TTypeNodeType::ARRAY);
        assert_eq!(
            array_nodes[1]
                .scalar_type
                .as_ref()
                .map(|scalar| scalar.type_),
            Some(types::TPrimitiveType::JSON)
        );

        let map_json = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(logical_field("value", DataType::Utf8, LogicalType::Json)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let map_desc = arrow_type_to_type_desc(&map_json).unwrap();
        let map_nodes = map_desc.types.as_ref().unwrap();
        assert_eq!(map_nodes[0].type_, types::TTypeNodeType::MAP);
        assert_eq!(
            map_nodes[2].scalar_type.as_ref().map(|scalar| scalar.type_),
            Some(types::TPrimitiveType::JSON)
        );
    }

    #[test]
    fn timestamp_unit_roundtrips_through_thrift_desc() {
        use crate::lower::compat::type_lowering::arrow_type_from_desc;
        use arrow::datatypes::{DataType, TimeUnit};

        // microsecond stays microsecond (FE-compat default)
        let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
        let desc = arrow_type_to_type_desc(&micro).unwrap();
        assert_eq!(arrow_type_from_desc(&desc), Some(micro));

        // nanosecond must survive the round-trip (the bug this task fixes)
        let nano = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let desc = arrow_type_to_type_desc(&nano).unwrap();
        assert_eq!(
            arrow_type_from_desc(&desc),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
    }

    #[test]
    fn unsupported_timestamp_unit_is_rejected() {
        use arrow::datatypes::{DataType, TimeUnit};
        let sec = DataType::Timestamp(TimeUnit::Second, None);
        assert!(arrow_type_to_type_desc(&sec).is_err());
    }
}
