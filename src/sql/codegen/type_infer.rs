use arrow::datatypes::{DataType, Field};

use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::types;

/// Metadata key on a `Field` that overrides the inferred StarRocks primitive.
/// Mirrors `crate::sql::analyzer::helpers::NR_LOGICAL_TYPE_KEY` (kept duplicated
/// to avoid pulling the analyzer module into codegen). Today only "json" is
/// emitted; the analyzer attaches it when a CAST target is `json` nested
/// inside `map<…>`/`array<…>`/`struct<…>` so the JSON-ness survives the
/// JSON-to-`Utf8` collapse in `sql_type_to_arrow`.
const NR_LOGICAL_TYPE_KEY: &str = "nr_logical_type";

/// Convert Arrow DataType to Thrift TTypeDesc.
pub(crate) fn arrow_type_to_type_desc(data_type: &DataType) -> Result<types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_arrow_type_nodes(data_type, None, &mut nodes)?;
    Ok(types::TTypeDesc::new(nodes))
}

fn append_arrow_type_nodes(
    data_type: &DataType,
    parent_field: Option<&Field>,
    nodes: &mut Vec<types::TTypeNode>,
) -> Result<(), String> {
    // If the enclosing `Field` carries a logical-type tag, override the
    // inferred primitive so the child reports e.g. JSON instead of VARCHAR.
    if let Some(primitive) = logical_type_override(parent_field) {
        nodes.extend(scalar_type_desc(primitive).types.unwrap_or_default());
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
            let primitive = arrow_type_to_primitive(data_type)?;
            nodes.extend(scalar_type_desc(primitive).types.unwrap_or_default());
            Ok(())
        }
    }
}

fn logical_type_override(field: Option<&Field>) -> Option<types::TPrimitiveType> {
    let logical = field?.metadata().get(NR_LOGICAL_TYPE_KEY)?;
    match logical.as_str() {
        "json" => Some(types::TPrimitiveType::JSON),
        _ => None,
    }
}

pub(crate) fn arrow_type_to_primitive(
    data_type: &DataType,
) -> Result<types::TPrimitiveType, String> {
    match data_type {
        DataType::Boolean => Ok(types::TPrimitiveType::BOOLEAN),
        DataType::Int8 => Ok(types::TPrimitiveType::TINYINT),
        DataType::Int16 => Ok(types::TPrimitiveType::SMALLINT),
        DataType::Int32 => Ok(types::TPrimitiveType::INT),
        DataType::Int64 => Ok(types::TPrimitiveType::BIGINT),
        DataType::Float32 => Ok(types::TPrimitiveType::FLOAT),
        DataType::Float64 => Ok(types::TPrimitiveType::DOUBLE),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(types::TPrimitiveType::VARCHAR),
        DataType::Binary => Ok(types::TPrimitiveType::VARBINARY),
        // NovaRocks reserves arrow `LargeBinary` for the v3 variant payload
        // (see src/lower/type_lowering.rs:170). Plain BINARY uses `Binary`.
        DataType::LargeBinary => Ok(types::TPrimitiveType::VARIANT),
        DataType::Date32 => Ok(types::TPrimitiveType::DATE),
        DataType::Timestamp(_, _) => Ok(types::TPrimitiveType::DATETIME),
        DataType::Decimal128(_, _) => Ok(types::TPrimitiveType::DECIMAL128),
        DataType::Decimal256(_, _) => Ok(types::TPrimitiveType::DECIMAL256),
        DataType::FixedSizeBinary(16) => Ok(types::TPrimitiveType::LARGEINT),
        DataType::Time64(_) => Ok(types::TPrimitiveType::TIME),
        DataType::Null => Ok(types::TPrimitiveType::NULL_TYPE),
        other => Err(format!(
            "ThriftPlanBuilder does not support data type {:?}",
            other
        )),
    }
}

pub(crate) use crate::sql::types::{arithmetic_result_type_with_op, wider_type};
