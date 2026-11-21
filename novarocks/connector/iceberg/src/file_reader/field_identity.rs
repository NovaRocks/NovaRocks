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

//! Iceberg identity projected onto the immutable footer's ordered leaves.

use std::collections::HashSet;

use arrow::datatypes::{DataType, Field, SchemaRef};
use novarocks_fs::{ParquetInterpretationPolicy, ParquetMetadataInspection};
use novarocks_spi::connector::ConnectorError;

use crate::iceberg::spec::NameMapping;
use crate::typed_read::FileFieldIdCoverage;
use crate::typed_read::column_handle::corrupt;

use crate::schema_mapping::{
    apply_name_mapping, field_id_for_arrow_field, is_variant_struct_data_type,
    schema_field_id_coverage, unidentified_fields_are_only_opaque_variants,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedParquetLeaf {
    pub(crate) field_id: Option<i32>,
    pub(crate) repeated: bool,
}

pub(crate) type ResolvedParquetLeaves = Vec<ResolvedParquetLeaf>;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedParquetFields {
    pub(crate) schema: SchemaRef,
    pub(crate) coverage: FileFieldIdCoverage,
    pub(crate) leaves: ResolvedParquetLeaves,
}

#[derive(Debug)]
pub(crate) enum FieldIdentityError {
    MixedFieldIds,
    Corrupt(ConnectorError),
}

impl FieldIdentityError {
    pub(crate) fn into_read_error(self) -> ConnectorError {
        match self {
            Self::MixedFieldIds => {
                corrupt("iceberg data file mixes fields with and without field ids")
            }
            Self::Corrupt(error) => error,
        }
    }
}

impl From<ConnectorError> for FieldIdentityError {
    fn from(error: ConnectorError) -> Self {
        Self::Corrupt(error)
    }
}

/// Classify the unmodified Parquet schema before consulting any mapping.
/// The children of opaque VARIANTs are encoding details.
pub(crate) fn original_field_id_coverage(
    schema: &SchemaRef,
) -> Result<FileFieldIdCoverage, FieldIdentityError> {
    let schema_error = |error: String| corrupt(format!("iceberg data file schema: {error}"));
    let (identified, total) = schema_field_id_coverage(schema).map_err(schema_error)?;
    if identified == total {
        return Ok(FileFieldIdCoverage::Complete);
    }
    if identified == 0 {
        return Ok(FileFieldIdCoverage::None);
    }
    if unidentified_fields_are_only_opaque_variants(schema).map_err(schema_error)? {
        return Ok(FileFieldIdCoverage::Complete);
    }
    Err(FieldIdentityError::MixedFieldIds)
}

pub(crate) fn resolve_parquet_fields(
    footer: &ParquetMetadataInspection,
    mapping: Option<&NameMapping>,
    observe: impl FnMut(&[&str], Option<i32>),
) -> Result<ResolvedParquetFields, FieldIdentityError> {
    if footer.interpretation_policy() != ParquetInterpretationPolicy::SkipArrowHint {
        return Err(corrupt("Iceberg field identity requires the original Parquet schema").into());
    }
    let coverage = original_field_id_coverage(footer.schema())?;
    let schema = resolve_arrow_fields(footer.schema(), coverage, mapping, observe)?;
    let mut leaves = Vec::with_capacity(footer.physical_columns().len());
    let mut ids = HashSet::new();
    for field in schema.fields() {
        collect_leaves(field, false, false, &mut ids, &mut leaves)?;
    }
    if leaves.len() != footer.physical_columns().len() {
        return Err(corrupt("Iceberg logical leaves disagree with the raw Parquet footer").into());
    }
    if coverage == FileFieldIdCoverage::Complete
        && leaves
            .iter()
            .zip(footer.physical_columns())
            .any(|(leaf, physical)| {
                leaf.field_id
                    .is_some_and(|id| Some(id) != physical.field_id())
            })
    {
        return Err(corrupt("Iceberg field IDs disagree with the raw Parquet leaf order").into());
    }
    Ok(ResolvedParquetFields {
        schema,
        coverage,
        leaves,
    })
}

pub(crate) fn resolve_arrow_fields(
    schema: &SchemaRef,
    coverage: FileFieldIdCoverage,
    mapping: Option<&NameMapping>,
    observe: impl FnMut(&[&str], Option<i32>),
) -> Result<SchemaRef, ConnectorError> {
    match (coverage, mapping) {
        (FileFieldIdCoverage::None, Some(mapping)) => apply_name_mapping(schema, mapping, observe)
            .map_err(|error| corrupt(format!("iceberg name mapping: {error}"))),
        _ => Ok(schema.clone()),
    }
}

fn collect_leaves(
    field: &Field,
    repeated: bool,
    opaque: bool,
    ids: &mut HashSet<i32>,
    leaves: &mut ResolvedParquetLeaves,
) -> Result<(), ConnectorError> {
    let id = if opaque {
        None
    } else {
        field_id_for_arrow_field(field).map_err(corrupt)?
    };
    if let Some(id) = id
        && !ids.insert(id)
    {
        return Err(corrupt(format!(
            "iceberg data file schema declares field id {id} twice"
        )));
    }
    let data_type = field.data_type();
    let opaque = opaque || is_variant_struct_data_type(data_type);
    match data_type {
        DataType::Struct(fields) => {
            for child in fields {
                collect_leaves(child, repeated, opaque, ids, leaves)?;
            }
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            collect_leaves(child, true, opaque, ids, leaves)?;
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(corrupt("Iceberg physical map does not have struct entries"));
            };
            for child in fields {
                collect_leaves(child, true, opaque, ids, leaves)?;
            }
        }
        _ => leaves.push(ResolvedParquetLeaf {
            field_id: id,
            repeated,
        }),
    }
    Ok(())
}
