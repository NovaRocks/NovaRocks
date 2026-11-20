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

//! Registration preview uses the same field identities and physical binding as the scan.

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_fs::ParquetMetadataInspection;
use novarocks_spi::connector::ConnectorError;

use crate::file_reader::field_identity::{
    FieldIdentityError, ResolvedParquetFields, resolve_parquet_fields,
};
use crate::iceberg::spec::{NameMapping, Schema};
use crate::schema_mapping::field_id_for_arrow_field;

use super::super::column_handle::corrupt;
use super::{AdaptationError, FieldAdaptation, PhysicalIndex, annotated_read_schema};

#[derive(Debug)]
pub(crate) enum RegistrationPreviewError {
    Identity(FieldIdentityError),
    Corrupt(ConnectorError),
    TypeMismatch {
        field: String,
        source: DataType,
        target: DataType,
    },
    UnsupportedCarrier {
        field: String,
        source: DataType,
    },
    MissingRequiredChild(String),
}

pub(crate) fn preview_registration(
    footer: &ParquetMetadataInspection,
    table_schema: &Schema,
    mapping: Option<&NameMapping>,
) -> Result<ResolvedParquetFields, RegistrationPreviewError> {
    use RegistrationPreviewError as Error;

    let fields = resolve_parquet_fields(footer, mapping).map_err(Error::Identity)?;
    let physical = PhysicalIndex::build(&fields.schema).map_err(Error::Corrupt)?;
    let target_schema = annotated_read_schema(table_schema).map_err(Error::Corrupt)?;
    for target in target_schema.fields() {
        let id = field_id_for_arrow_field(target)
            .map_err(corrupt)
            .map_err(Error::Corrupt)?
            .ok_or_else(|| Error::Corrupt(corrupt("Iceberg table field has no ID")))?;
        let Some(slot) = physical.index_of(id) else {
            // The per-file metrics check decides whether an absent root is
            // legal. The nested binding can validate only present roots.
            continue;
        };
        let adaptation =
            FieldAdaptation::bind(Some(fields.schema.field(slot)), target).map_err(|error| {
                match error {
                    AdaptationError::Invalid(error) => Error::Corrupt(error),
                    AdaptationError::TypeMismatch {
                        field,
                        source,
                        target,
                        ..
                    } => {
                        if unsupported_carrier(&source) {
                            Error::UnsupportedCarrier { field, source }
                        } else {
                            Error::TypeMismatch {
                                field,
                                source,
                                target,
                            }
                        }
                    }
                }
            })?;
        if let Some(field) = adaptation.missing_required() {
            return Err(Error::MissingRequiredChild(field.to_owned()));
        }
    }
    Ok(fields)
}

fn unsupported_carrier(source: &DataType) -> bool {
    matches!(
        source,
        DataType::Int8
            | DataType::Int16
            | DataType::UInt8
            | DataType::UInt16
            | DataType::Time32(_)
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Nanosecond, _)
    )
}
