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

//! Storage vocabulary projected from the original semantic declaration.
//! The normal schema/formal-map authors perform publication and exact replay
//! validation. This projection neither authors a type nor infers a domain from
//! Arrow layout metadata; unrepresentable declarations are explicit refusals.

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorDataType as D, ConnectorError, ConnectorErrorKind,
    ConnectorStructField, ConnectorViewDefinition, read_stack::ConnectorReadSchemaCreation,
};
use novarocks_type_contract::{ValueKind, ValueLogicalType, ValueTypeRef};

type Failure = PureProviderCompileError<ConnectorError>;

pub(super) fn columns(
    definition: &ConnectorViewDefinition,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorColumnDefinition>, Failure> {
    definition
        .columns
        .iter()
        .map(|column| {
            creation.work(1)?;
            Ok(ConnectorColumnDefinition {
                name: column.name.clone(),
                data_type: data_type(column.value_type.value_type(), creation)?,
                nullable: column.value_type.nullable(),
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

fn data_type(
    value: &ValueTypeRef,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<D, Failure> {
    creation.work(1)?;
    Ok(match value.kind() {
        ValueKind::String => D::String,
        ValueKind::Binary => D::Binary,
        ValueKind::Nominal(logical) => match logical {
            ValueLogicalType::Hll => D::Hll,
            ValueLogicalType::Bitmap => D::Bitmap,
            ValueLogicalType::LargeInt => D::LargeInt,
            ValueLogicalType::Variant => D::Variant,
            ValueLogicalType::Json => D::Json,
            _ => return unsupported(),
        },
        ValueKind::Scalar(data_type) => match data_type {
            DataType::Boolean => D::Boolean,
            DataType::Int8 => D::TinyInt,
            DataType::Int16 => D::SmallInt,
            DataType::Int32 => D::Int,
            DataType::Int64 => D::BigInt,
            DataType::Float32 => D::Float,
            DataType::Float64 => D::Double,
            DataType::Decimal128(precision, scale) => D::Decimal {
                precision: *precision,
                scale: *scale,
            },
            DataType::Utf8 => D::String,
            DataType::Binary => D::Binary,
            DataType::Date32 => D::Date,
            DataType::Time64(TimeUnit::Microsecond) => D::Time,
            DataType::Timestamp(TimeUnit::Microsecond, None) => D::DateTime,
            DataType::Timestamp(TimeUnit::Nanosecond, None) => D::DateTimeNs,
            _ => return unsupported(),
        },
        ValueKind::List(child) => D::Array(Box::new(data_type(child, creation)?)),
        ValueKind::Map { key, value } => D::Map(
            Box::new(data_type(key, creation)?),
            Box::new(data_type(value, creation)?),
        ),
        ValueKind::Struct(fields) => D::Struct(
            fields
                .iter()
                .map(|field| {
                    creation.work(1)?;
                    Ok(ConnectorStructField {
                        name: field.name().into(),
                        data_type: data_type(field.value_type(), creation)?,
                        nullable: field.value_type().nullable(),
                    })
                })
                .collect::<Result<_, Failure>>()?,
        ),
        _ => return unsupported(),
    })
}
fn unsupported<T>() -> Result<T, Failure> {
    Err(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "Iceberg view storage vocabulary cannot represent the declared semantic type",
    )
    .into())
}
