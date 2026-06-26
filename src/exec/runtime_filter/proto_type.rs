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
use arrow::datatypes::{DataType, TimeUnit};

use crate::service::grpc_client::proto::starrocks::{PScalarType, PTypeDesc, PTypeNode};

const TYPE_NODE_SCALAR: i32 = 0;

pub(crate) fn arrow_type_to_proto_type_desc(data_type: &DataType) -> Option<PTypeDesc> {
    let (primitive, len, precision, scale) = match data_type {
        DataType::Boolean => (
            crate::thrift::types::TPrimitiveType::BOOLEAN.0,
            None,
            None,
            None,
        ),
        DataType::Int8 => (
            crate::thrift::types::TPrimitiveType::TINYINT.0,
            None,
            None,
            None,
        ),
        DataType::Int16 => (
            crate::thrift::types::TPrimitiveType::SMALLINT.0,
            None,
            None,
            None,
        ),
        DataType::Int32 => (
            crate::thrift::types::TPrimitiveType::INT.0,
            None,
            None,
            None,
        ),
        DataType::Int64 => (
            crate::thrift::types::TPrimitiveType::BIGINT.0,
            None,
            None,
            None,
        ),
        DataType::Float32 => (
            crate::thrift::types::TPrimitiveType::FLOAT.0,
            None,
            None,
            None,
        ),
        DataType::Float64 => (
            crate::thrift::types::TPrimitiveType::DOUBLE.0,
            None,
            None,
            None,
        ),
        DataType::Date32 => (
            crate::thrift::types::TPrimitiveType::DATE.0,
            None,
            None,
            None,
        ),
        DataType::Timestamp(_, _) => (
            crate::thrift::types::TPrimitiveType::DATETIME.0,
            None,
            None,
            None,
        ),
        DataType::Utf8 => (
            crate::thrift::types::TPrimitiveType::VARCHAR.0,
            None,
            None,
            None,
        ),
        DataType::Decimal128(precision, scale) => {
            if !is_valid_decimal128(*precision, *scale) {
                return None;
            }
            (
                crate::thrift::types::TPrimitiveType::DECIMAL128.0,
                None,
                Some(i32::from(*precision)),
                Some(i32::from(*scale)),
            )
        }
        _ => return None,
    };

    Some(PTypeDesc {
        types: vec![PTypeNode {
            r#type: TYPE_NODE_SCALAR,
            scalar_type: Some(PScalarType {
                r#type: primitive,
                len,
                precision,
                scale,
            }),
            struct_fields: Vec::new(),
        }],
    })
}

pub(crate) fn arrow_type_from_proto_type_desc(desc: &PTypeDesc) -> Option<DataType> {
    if desc.types.len() != 1 {
        return None;
    }
    let node = desc.types.first()?;
    if node.r#type != TYPE_NODE_SCALAR {
        return None;
    }
    let scalar = node.scalar_type.as_ref()?;
    let primitive = crate::thrift::types::TPrimitiveType(scalar.r#type);
    if primitive == crate::thrift::types::TPrimitiveType::BOOLEAN {
        Some(DataType::Boolean)
    } else if primitive == crate::thrift::types::TPrimitiveType::TINYINT {
        Some(DataType::Int8)
    } else if primitive == crate::thrift::types::TPrimitiveType::SMALLINT {
        Some(DataType::Int16)
    } else if primitive == crate::thrift::types::TPrimitiveType::INT {
        Some(DataType::Int32)
    } else if primitive == crate::thrift::types::TPrimitiveType::BIGINT {
        Some(DataType::Int64)
    } else if primitive == crate::thrift::types::TPrimitiveType::FLOAT {
        Some(DataType::Float32)
    } else if primitive == crate::thrift::types::TPrimitiveType::DOUBLE {
        Some(DataType::Float64)
    } else if primitive == crate::thrift::types::TPrimitiveType::DATE {
        Some(DataType::Date32)
    } else if primitive == crate::thrift::types::TPrimitiveType::DATETIME {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else if primitive == crate::thrift::types::TPrimitiveType::VARCHAR
        || primitive == crate::thrift::types::TPrimitiveType::CHAR
    {
        Some(DataType::Utf8)
    } else if primitive == crate::thrift::types::TPrimitiveType::DECIMAL128 {
        let precision = scalar.precision?;
        let scale = scalar.scale?;
        if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
            return None;
        }
        let precision = u8::try_from(precision).ok()?;
        let scale = i8::try_from(scale).ok()?;
        Some(DataType::Decimal128(precision, scale))
    } else {
        None
    }
}

fn is_valid_decimal128(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision) && scale >= 0 && i32::from(scale) <= i32::from(precision)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{TYPE_NODE_SCALAR, arrow_type_from_proto_type_desc, arrow_type_to_proto_type_desc};
    use crate::service::grpc_client::proto::starrocks::{PScalarType, PTypeDesc, PTypeNode};
    use crate::thrift::types::TPrimitiveType;

    #[test]
    fn proto_type_desc_round_trips_supported_runtime_filter_types() {
        let cases = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Utf8,
            DataType::Decimal128(18, 2),
            DataType::Decimal128(38, 9),
        ];

        for data_type in cases {
            let desc = arrow_type_to_proto_type_desc(&data_type)
                .unwrap_or_else(|| panic!("missing proto type desc for {data_type:?}"));
            let decoded = arrow_type_from_proto_type_desc(&desc)
                .unwrap_or_else(|| panic!("missing Arrow type for {data_type:?}"));
            assert_eq!(decoded, data_type);
        }
    }

    #[test]
    fn unsupported_runtime_filter_type_has_no_proto_type_desc() {
        assert!(arrow_type_to_proto_type_desc(&DataType::Binary).is_none());
    }

    #[test]
    fn proto_type_desc_with_trailing_nodes_is_rejected() {
        let mut desc = arrow_type_to_proto_type_desc(&DataType::Int32).expect("int proto type");
        desc.types.push(PTypeNode {
            r#type: TYPE_NODE_SCALAR,
            scalar_type: Some(PScalarType {
                r#type: TPrimitiveType::TINYINT.0,
                len: None,
                precision: None,
                scale: None,
            }),
            struct_fields: Vec::new(),
        });

        assert!(arrow_type_from_proto_type_desc(&desc).is_none());
    }

    #[test]
    fn invalid_decimal_arrow_type_has_no_proto_type_desc() {
        let cases = [
            DataType::Decimal128(0, 0),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(18, -1),
            DataType::Decimal128(18, 19),
        ];

        for data_type in cases {
            assert!(
                arrow_type_to_proto_type_desc(&data_type).is_none(),
                "invalid decimal should not encode: {data_type:?}"
            );
        }
    }

    #[test]
    fn invalid_decimal_proto_type_desc_is_rejected() {
        let cases = [
            (None, Some(0)),
            (Some(18), None),
            (Some(0), Some(0)),
            (Some(39), Some(0)),
            (Some(18), Some(-1)),
            (Some(18), Some(19)),
        ];

        for (precision, scale) in cases {
            let desc = PTypeDesc {
                types: vec![PTypeNode {
                    r#type: TYPE_NODE_SCALAR,
                    scalar_type: Some(PScalarType {
                        r#type: TPrimitiveType::DECIMAL128.0,
                        len: None,
                        precision,
                        scale,
                    }),
                    struct_fields: Vec::new(),
                }],
            };

            assert!(
                arrow_type_from_proto_type_desc(&desc).is_none(),
                "invalid decimal should not decode: precision={precision:?} scale={scale:?}"
            );
        }
    }
}
