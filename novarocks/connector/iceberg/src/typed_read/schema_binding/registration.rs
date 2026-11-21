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
use novarocks_fs::{ParquetMetadataInspection, ParquetPhysicalType};
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
    Caller(ConnectorError),
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
    bound_leaf_ids: &mut Vec<i32>,
    mut observe: impl FnMut(&[&str], Option<i32>) -> Result<(), ConnectorError>,
) -> Result<ResolvedParquetFields, RegistrationPreviewError> {
    use RegistrationPreviewError as Error;

    let mut observation_failure = None;
    let fields = resolve_parquet_fields(footer, mapping, |path, id| {
        if observation_failure.is_none() {
            observation_failure = observe(path, id).err();
        }
    });
    if let Some(error) = observation_failure {
        return Err(Error::Caller(error));
    }
    let mut fields = fields.map_err(Error::Identity)?;
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
            FieldAdaptation::bind(Some(fields.schema.field(slot)), target, table_schema).map_err(
                |error| match error {
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
                },
            )?;
        if let Some(field) = adaptation.missing_required() {
            return Err(Error::MissingRequiredChild(field.to_owned()));
        }
        adaptation
            .visit_bound_leaf_ids(&mut |id| bound_leaf_ids.push(id))
            .map_err(Error::Corrupt)?;
    }
    bound_leaf_ids.sort_unstable();
    for (leaf, physical) in fields.leaves.iter_mut().zip(footer.physical_columns()) {
        let Some(id) = leaf.field_id else { continue };
        let Some(field) = table_schema.field_by_id(id) else {
            continue;
        };
        if bound_leaf_ids.binary_search(&id).is_err() {
            leaf.field_id = None;
            continue;
        }
        if physical.physical_type() == ParquetPhysicalType::Int96 {
            return Err(Error::UnsupportedCarrier {
                field: field.name.clone(),
                source: DataType::Timestamp(TimeUnit::Nanosecond, None),
            });
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
