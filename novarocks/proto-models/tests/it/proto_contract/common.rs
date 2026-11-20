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

use prost::Message;

use novarocks_proto_models::{arrow_type as a, common, plan, value_type as v};

#[derive(Clone, Debug, PartialEq)]
enum InternalType {
    Scalar {
        prim: i32,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    List(Box<InternalType>),
    Map(Box<InternalType>, Box<InternalType>),
    Struct(Vec<(String, InternalType)>),
}

fn type_to_proto(t: &InternalType, table: &mut plan::ExecutionTypeTable) -> common::TypeDesc {
    fn field(name: &str, id: u32, layout: a::ArrowPhysicalType) -> a::ArrowPhysicalField {
        a::ArrowPhysicalField {
            name: name.into(),
            nullable: false,
            logical_type_id: Some(id),
            r#type: Some(Box::new(layout)),
        }
    }
    fn node(t: &InternalType, table: &mut plan::ExecutionTypeTable) -> (u32, a::ArrowPhysicalType) {
        use a::arrow_physical_type::Kind as P;
        use v::logical_type_definition::Kind as L;
        let (logical, layout) = match t {
            InternalType::Scalar {
                prim,
                precision,
                scale,
            } => {
                let (kind, physical) = match common::PrimitiveType::try_from(*prim).unwrap() {
                    common::PrimitiveType::Boolean => (
                        v::ScalarKind::Boolean,
                        P::Primitive(a::ArrowPrimitiveType::Boolean as i32),
                    ),
                    common::PrimitiveType::Bigint => (
                        v::ScalarKind::Int64,
                        P::Primitive(a::ArrowPrimitiveType::Int64 as i32),
                    ),
                    common::PrimitiveType::Double => (
                        v::ScalarKind::Float64,
                        P::Primitive(a::ArrowPrimitiveType::Float64 as i32),
                    ),
                    common::PrimitiveType::Varchar => (
                        v::ScalarKind::String,
                        P::Primitive(a::ArrowPrimitiveType::Utf8 as i32),
                    ),
                    common::PrimitiveType::Decimal128 => (
                        v::ScalarKind::Decimal128,
                        P::Decimal128(a::ArrowDecimalType {
                            precision: precision.unwrap().try_into().unwrap(),
                            scale: scale.unwrap(),
                        }),
                    ),
                    other => panic!("undeclared fixture scalar {other:?}"),
                };
                (
                    L::Scalar(v::ScalarValue {
                        kind: kind as i32,
                        precision: precision.map(|p| p.try_into().unwrap()),
                        scale: *scale,
                        ..Default::default()
                    }),
                    physical,
                )
            }
            InternalType::List(item) => {
                let (id, layout) = node(item, table);
                (
                    L::ListItemTypeId(id),
                    P::List(Box::new(field("item", id, layout))),
                )
            }
            InternalType::Struct(fields) => {
                let children: Vec<_> = fields
                    .iter()
                    .map(|(name, ty)| {
                        let (id, layout) = node(ty, table);
                        (name, id, layout)
                    })
                    .collect();
                (
                    L::StructValue(v::StructValue {
                        fields: children
                            .iter()
                            .map(|(name, id, _)| v::NamedValueField {
                                name: (*name).clone(),
                                value_type_id: Some(*id),
                            })
                            .collect(),
                    }),
                    P::StructType(a::ArrowStructType {
                        fields: children
                            .into_iter()
                            .map(|(name, id, layout)| field(name, id, layout))
                            .collect(),
                    }),
                )
            }
            InternalType::Map(key, value) => {
                let (key_id, key_layout) = node(key, table);
                let (value_id, value_layout) = node(value, table);
                let entries = a::ArrowPhysicalField {
                    name: "entries".into(),
                    nullable: false,
                    logical_type_id: None,
                    r#type: Some(Box::new(a::ArrowPhysicalType {
                        kind: Some(P::StructType(a::ArrowStructType {
                            fields: vec![
                                field("key", key_id, key_layout),
                                field("value", value_id, value_layout),
                            ],
                        })),
                    })),
                };
                (
                    L::MapValue(v::MapValue {
                        key_type_id: Some(key_id),
                        value_type_id: Some(value_id),
                    }),
                    P::Map(Box::new(a::ArrowMapType {
                        entries: Some(Box::new(entries)),
                        ordered: false,
                    })),
                )
            }
        };
        let id = table.logical_types.len().try_into().unwrap();
        table.logical_types.push(v::LogicalTypeDefinition {
            id: Some(id),
            nullable: false,
            kind: Some(logical),
        });
        (id, a::ArrowPhysicalType { kind: Some(layout) })
    }
    let (logical_type_id, value_layout) = node(t, table);
    let id = table.value_bindings.len().try_into().unwrap();
    table.value_bindings.push(plan::ExecutionValueBinding {
        id: Some(id),
        logical_type_id: Some(logical_type_id),
        value_layout: Some(value_layout),
    });
    common::TypeDesc {
        value_binding_id: Some(id),
    }
}

fn type_from_proto(
    p: &common::TypeDesc,
    table: &plan::ExecutionTypeTable,
) -> Result<InternalType, String> {
    fn node(id: u32, table: &plan::ExecutionTypeTable) -> Result<InternalType, String> {
        use v::logical_type_definition::Kind as L;
        let definition = table
            .logical_types
            .iter()
            .find(|n| n.id == Some(id))
            .ok_or("logical type missing")?;
        Ok(
            match definition.kind.as_ref().ok_or("logical kind missing")? {
                L::Scalar(s) => InternalType::Scalar {
                    prim: match v::ScalarKind::try_from(s.kind).map_err(|_| "unknown scalar")? {
                        v::ScalarKind::Boolean => common::PrimitiveType::Boolean,
                        v::ScalarKind::Int64 => common::PrimitiveType::Bigint,
                        v::ScalarKind::Float64 => common::PrimitiveType::Double,
                        v::ScalarKind::String => common::PrimitiveType::Varchar,
                        v::ScalarKind::Decimal128 => common::PrimitiveType::Decimal128,
                        _ => return Err("undeclared fixture scalar".into()),
                    } as i32,
                    precision: s.precision.map(|n| n.try_into().unwrap()),
                    scale: s.scale,
                },
                L::ListItemTypeId(id) => InternalType::List(Box::new(node(*id, table)?)),
                L::MapValue(m) => InternalType::Map(
                    Box::new(node(m.key_type_id.ok_or("map key missing")?, table)?),
                    Box::new(node(m.value_type_id.ok_or("map value missing")?, table)?),
                ),
                L::StructValue(s) => InternalType::Struct(
                    s.fields
                        .iter()
                        .map(|f| {
                            Ok((
                                f.name.clone(),
                                node(f.value_type_id.ok_or("struct field missing")?, table)?,
                            ))
                        })
                        .collect::<Result<_, String>>()?,
                ),
                _ => return Err("undeclared fixture kind".into()),
            },
        )
    }
    let binding = binding_from_ref(p, table)?;
    node(
        binding
            .logical_type_id
            .ok_or("binding logical type missing")?,
        table,
    )
}

pub(super) fn binding_from_ref<'a>(
    p: &common::TypeDesc,
    table: &'a plan::ExecutionTypeTable,
) -> Result<&'a plan::ExecutionValueBinding, String> {
    let id = p
        .value_binding_id
        .ok_or("TypeDesc.value_binding_id missing")?;
    table
        .value_bindings
        .iter()
        .find(|b| b.id == Some(id))
        .ok_or_else(|| "TypeDesc.value_binding_id unknown".into())
}

// This is one explicitly declared fixture namespace, shared by its owning fragment
// and every expression/output reference. IDs are labels, never SQL type tags.
pub(super) fn scalar_ref(prim: common::PrimitiveType, nullable: bool) -> common::TypeDesc {
    let ordinal = match prim {
        common::PrimitiveType::Boolean => 0,
        common::PrimitiveType::Bigint => 1,
        common::PrimitiveType::Double => 2,
        common::PrimitiveType::Varchar => 3,
        _ => panic!("undeclared scalar fixture"),
    };
    common::TypeDesc {
        value_binding_id: Some(ordinal * 2 + u32::from(nullable)),
    }
}
pub(super) fn scalar_table() -> plan::ExecutionTypeTable {
    let mut table = plan::ExecutionTypeTable::default();
    for prim in [
        common::PrimitiveType::Boolean,
        common::PrimitiveType::Bigint,
        common::PrimitiveType::Double,
        common::PrimitiveType::Varchar,
    ] {
        for nullable in [false, true] {
            let desc = type_to_proto(
                &InternalType::Scalar {
                    prim: prim as i32,
                    precision: None,
                    scale: None,
                },
                &mut table,
            );
            assert_eq!(desc, scalar_ref(prim, nullable));
            table.logical_types.last_mut().unwrap().nullable = nullable;
        }
    }
    table
}
fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn sample_recursive_type() -> InternalType {
    use common::PrimitiveType;

    // Map<VARCHAR, List<Struct<a: DECIMAL128(10,2), b: List<BIGINT>>>>
    InternalType::Map(
        Box::new(InternalType::Scalar {
            prim: PrimitiveType::Varchar as i32,
            precision: None,
            scale: None,
        }),
        Box::new(InternalType::List(Box::new(InternalType::Struct(vec![
            (
                "a".to_string(),
                InternalType::Scalar {
                    prim: PrimitiveType::Decimal128 as i32,
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
            (
                "b".to_string(),
                InternalType::List(Box::new(InternalType::Scalar {
                    prim: PrimitiveType::Bigint as i32,
                    precision: None,
                    scale: None,
                })),
            ),
        ])))),
    )
}

fn literal_boundary_accepts(value: &common::LiteralValue) -> Result<(), String> {
    use common::literal_value::Value;

    match value.value.as_ref() {
        Some(Value::NullValue(false)) => {
            Err("LiteralValue.null_value false is invalid".to_string())
        }
        _ => Ok(()),
    }
}

#[test]
fn recursive_type_desc_survives_proto_roundtrip() {
    let original = sample_recursive_type();
    let mut table = plan::ExecutionTypeTable::default();
    let proto = type_to_proto(&original, &mut table);
    let decoded_table = roundtrip_message(&table);
    assert_eq!(table, decoded_table);

    let decoded: common::TypeDesc = roundtrip_message(&proto);
    assert_eq!(proto, decoded);

    let back = type_from_proto(&decoded, &decoded_table).expect("convert TypeDesc back");
    assert_eq!(original, back);
}

#[test]
fn missing_type_desc_kind_reports_boundary_error() {
    let table = scalar_table();
    let err = binding_from_ref(
        &common::TypeDesc {
            value_binding_id: None,
        },
        &table,
    )
    .expect_err("missing binding");
    assert_eq!(err, "TypeDesc.value_binding_id missing");
    assert_eq!(
        binding_from_ref(
            &common::TypeDesc {
                value_binding_id: Some(u32::MAX)
            },
            &table
        )
        .unwrap_err(),
        "TypeDesc.value_binding_id unknown"
    );
    let zero = common::TypeDesc {
        value_binding_id: Some(0),
    };
    assert_ne!(
        zero.encode_to_vec(),
        common::TypeDesc::default().encode_to_vec()
    );
    assert_eq!(binding_from_ref(&zero, &table).unwrap().id, Some(0));
}

#[test]
fn literal_value_oneof_arms_survive_proto_roundtrip() {
    use common::literal_value::Value;

    let values = vec![
        common::LiteralValue {
            value: Some(Value::NullValue(true)),
        },
        common::LiteralValue {
            value: Some(Value::BoolValue(false)),
        },
        common::LiteralValue {
            value: Some(Value::IntValue(-42)),
        },
        common::LiteralValue {
            value: Some(Value::LargeintValue((-1i128).to_be_bytes().to_vec())),
        },
        common::LiteralValue {
            value: Some(Value::FloatValue(-12.5)),
        },
        common::LiteralValue {
            value: Some(Value::StringValue("hello".to_string())),
        },
        common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xff, 0x2a])),
        },
        common::LiteralValue {
            value: Some(Value::Date32Value(19_000)),
        },
        common::LiteralValue {
            value: Some(Value::DecimalValue(common::DecimalLiteral {
                value: (-12345i128).to_be_bytes().to_vec(),
                precision: 10,
                scale: 2,
            })),
        },
    ];

    for original in values {
        let decoded: common::LiteralValue = roundtrip_message(&original);
        assert_eq!(original, decoded);
        literal_boundary_accepts(&decoded).expect("valid literal boundary");
    }
}

#[test]
fn null_value_false_is_wire_representable_but_boundary_invalid() {
    use common::literal_value::Value;

    let original = common::LiteralValue {
        value: Some(Value::NullValue(false)),
    };

    let decoded: common::LiteralValue = roundtrip_message(&original);
    assert_eq!(original, decoded);

    let err = literal_boundary_accepts(&decoded).expect_err("null false must be rejected");
    assert_eq!(err, "LiteralValue.null_value false is invalid");
}

#[test]
fn status_unique_id_and_output_column_survive_proto_roundtrip() {
    let status = common::Status {
        code: 7,
        message: "backend rejected fragment".to_string(),
    };
    assert_eq!(status, roundtrip_message(&status));

    let unique_id = common::UniqueId {
        hi: 0x1122_3344_5566_7788,
        lo: -0x0102_0304_0506_0708,
    };
    assert_eq!(unique_id, roundtrip_message(&unique_id));

    let mut table = plan::ExecutionTypeTable::default();
    let output_column = common::OutputColumn {
        column_id: 42,
        name: "sum_revenue".to_string(),
        r#type: Some(type_to_proto(
            &InternalType::Scalar {
                prim: common::PrimitiveType::Decimal128 as i32,
                precision: Some(18),
                scale: Some(2),
            },
            &mut table,
        )),
        nullable: false,
        is_internal: true,
    };
    assert_eq!(output_column, roundtrip_message(&output_column));
    assert!(binding_from_ref(output_column.r#type.as_ref().unwrap(), &table).is_ok());
}
