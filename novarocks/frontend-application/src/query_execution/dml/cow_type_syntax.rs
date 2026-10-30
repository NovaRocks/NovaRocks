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

//! Borrowed spelling for the actual signed COW TypeName builder.
//! This owns no semantic type, resource allowance or heap invoice.
use arrow::datatypes::{DataType, Fields, TimeUnit};
use novarocks_type_contract::{
    FunctionValueType, ValueField, ValueKind, ValueLogicalType, ValueTypeRef,
};

#[derive(Debug)]
pub(super) enum Failure {
    InvalidSource(&'static str),
    OriginalSemantic(&'static str),
}
type Result<T> = std::result::Result<T, Failure>;
#[derive(Clone, Copy)]
pub(in crate::query_execution::dml) struct Node<'a> {
    pub value: &'a ValueTypeRef,
    pub layout: &'a DataType,
}
// This is transient AST grammar geometry, not a second semantic vocabulary.
pub(in crate::query_execution::dml) enum Spelling<'a> {
    Leaf(&'static str),
    Decimal { precision: u8, scale: u8 },
    Array(Node<'a>),
    Map(Node<'a>, Node<'a>),
    Struct(&'a [ValueField], &'a Fields),
}
impl<'a> Node<'a> {
    pub(in crate::query_execution::dml) fn root(value: &'a FunctionValueType) -> Self {
        Self {
            value: value.value_type(),
            layout: value.data_type(),
        }
    }
    pub(in crate::query_execution::dml) fn spelling(self) -> Result<Spelling<'a>> {
        use DataType as D;
        use ValueKind as V;
        let leaf = match self.value.kind() {
            V::String => Some("STRING"),
            V::Binary => Some("VARBINARY"),
            V::Nominal(ValueLogicalType::Json) => Some("JSON"),
            V::Nominal(ValueLogicalType::Variant) => Some("VARIANT"),
            V::Nominal(ValueLogicalType::LargeInt) => Some("LARGEINT"),
            V::Nominal(ValueLogicalType::Hll) => Some("HLL"),
            V::Nominal(ValueLogicalType::Bitmap) => Some("BITMAP"),
            // The current SQL CAST resolver has no UUID spelling. Do not
            // reinterpret its fixed binary carrier or introduce SQL grammar.
            V::Nominal(_) => {
                return Err(Failure::OriginalSemantic(
                    "COW nominal domain has no supported CAST spelling",
                ));
            }
            V::Scalar(_) => match self.layout {
                D::Boolean => Some("BOOLEAN"),
                D::Int8 => Some("TINYINT"),
                D::Int16 => Some("SMALLINT"),
                D::Int32 => Some("INT"),
                D::Int64 => Some("BIGINT"),
                D::Float32 => Some("FLOAT"),
                D::Float64 => Some("DOUBLE"),
                D::Date32 => Some("DATE"),
                D::Timestamp(TimeUnit::Microsecond, _) => Some("DATETIME"),
                D::Timestamp(TimeUnit::Nanosecond, _) => Some("DATETIME_NS"),
                D::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => Some("TIME"),
                // The SQL Binary declaration accepts a source carrier width,
                // but VARBINARY CAST constructs Binary and cannot preserve a
                // Scalar(FixedSizeBinary(width)) target. No FIXED CAST grammar
                // exists; a nominal LARGEINT remains its separate branch above.
                D::FixedSizeBinary(_) => {
                    return Err(Failure::OriginalSemantic(
                        "fixed binary has no exact supported COW CAST spelling",
                    ));
                }
                D::Decimal128(precision, scale) if *scale >= 0 => {
                    return Ok(Spelling::Decimal {
                        precision: *precision,
                        scale: *scale as u8,
                    });
                }
                D::Decimal128(_, _) => {
                    return Err(Failure::OriginalSemantic("negative DECIMAL type parameter"));
                }
                _ => {
                    return Err(Failure::OriginalSemantic(
                        "unsupported value layout for COW CAST",
                    ));
                }
            },
            V::List(item) => {
                let D::List(field) = self.layout else {
                    return Err(Failure::OriginalSemantic(
                        "unsupported value layout for COW ARRAY CAST",
                    ));
                };
                return Ok(Spelling::Array(Node {
                    value: item,
                    layout: field.data_type(),
                }));
            }
            V::Map { key, value } => {
                let D::Map(entries, _) = self.layout else {
                    return Err(Failure::InvalidSource("COW MAP layout differs"));
                };
                let D::Struct(fields) = entries.data_type() else {
                    return Err(Failure::InvalidSource("COW MAP entries differ"));
                };
                if fields.len() != 2 {
                    return Err(Failure::InvalidSource("COW MAP entries width differs"));
                }
                return Ok(Spelling::Map(
                    Node {
                        value: key,
                        layout: fields[0].data_type(),
                    },
                    Node {
                        value,
                        layout: fields[1].data_type(),
                    },
                ));
            }
            V::Struct(fields) => {
                let D::Struct(layout) = self.layout else {
                    return Err(Failure::InvalidSource("COW STRUCT layout differs"));
                };
                if fields.is_empty() {
                    return Err(Failure::OriginalSemantic("empty STRUCT type parameter"));
                }
                if fields.len() != layout.len() {
                    return Err(Failure::InvalidSource("COW STRUCT layout width differs"));
                }
                return Ok(Spelling::Struct(fields, layout));
            }
            _ => {
                return Err(Failure::OriginalSemantic(
                    "unsupported semantic type for COW CAST",
                ));
            }
        };
        Ok(Spelling::Leaf(leaf.expect("leaf branch")))
    }
}
