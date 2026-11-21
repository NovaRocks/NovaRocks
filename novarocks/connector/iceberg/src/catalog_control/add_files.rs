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

//! Provider-owned ADD FILES source admission.

use std::fmt;
use std::sync::Arc;

use novarocks_fs::{FileCancellation, FileIdentity, ParquetInterpretationPolicy};
use novarocks_spi::connector::{
    ConnectorAddFilesRecursion, ConnectorOperationControl, ConnectorPayloadRetentionGuard,
    ConnectorRequestContext,
};

use crate::access_binding::IcebergReadBinding;
use crate::catalog::NovaRocksCatalog;
use crate::commit::manifest_sources::SnapshotSources;
use crate::commit::{RegisteredFile, RegisteredNewFiles};
use crate::file_reader::field_identity::{
    FieldIdentityError, ResolvedParquetLeaf, original_field_id_coverage,
};
use crate::iceberg::spec::{NameMapping, TableProperties};
use crate::iceberg::table::Table;
use crate::schema_mapping::{encode_name_mapping, name_mapping_from_schema, parse_name_mapping};
use crate::typed_read::FileFieldIdCoverage;
use crate::typed_read::schema_binding::registration::{
    RegistrationPreviewError, preview_registration,
};
use crate::typed_read::split::validate_data_file_path;
use crate::typed_read::table_handle::validate_name_mapping_carrier;

mod listing;
mod mapping_uses;
mod registered_file;

use mapping_uses::MappingUses;

use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, connector_reason,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

#[derive(Debug)]
struct ProcedureFailure {
    fact: FailureFact,
    message: String,
}

impl fmt::Display for ProcedureFailure {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(&self.message)
    }
}

impl std::error::Error for ProcedureFailure {}

impl FailureProjection for ProcedureFailure {
    fn failure_fact(&self) -> FailureFact {
        self.fact
    }
}

pub(super) fn procedure_error(
    kind: ConnectorErrorKind,
    class: FailureClass,
    reason: FailureReason,
    message: impl Into<String>,
) -> ConnectorError {
    ConnectorError::from_domain(
        kind,
        ProcedureFailure {
            fact: FailureFact::new(class, reason),
            message: message.into(),
        },
    )
    .with_public_failure_detail()
}

pub(super) fn validate_registration_location(location: &str) -> Result<(), ConnectorError> {
    crate::write_codec::validate_secret_free_text("ADD FILES source location", location)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

pub(super) struct RegistrationPreparation<'a> {
    pub(super) table: &'a Table,
    pub(super) location: &'a str,
    pub(super) recursion: ConnectorAddFilesRecursion,
    pub(super) catalog: &'a dyn NovaRocksCatalog,
    pub(super) binding: &'a IcebergReadBinding,
    pub(super) context: &'a ConnectorRequestContext,
    pub(super) byte_author: &'a ConnectorPayloadRetentionGuard,
}

pub(super) struct MappingDependency {
    pub(super) originally_present: bool,
    pub(super) used: MappingUses,
}

impl RegistrationPreparation<'_> {
    pub(super) async fn collect(
        self,
    ) -> Result<(RegisteredNewFiles, Option<MappingDependency>), ConnectorError> {
        let source = self.table.metadata_ref();
        let schema = source.current_schema();
        if !source.default_partition_spec().is_unpartitioned() {
            return Err(procedure_error(
                ConnectorErrorKind::InvalidRequest,
                FailureClass::Data,
                connector_reason::PROCEDURE_ARGUMENT_INVALID,
                "The procedure does not support partitioned tables",
            ));
        }
        let admission = self.catalog.listing_admission();
        let listed = listing::resolve_and_list(
            self.location,
            self.recursion,
            self.binding,
            &admission,
            self.byte_author,
        )
        .await?;
        let (access, candidates) = listed.into_parts();
        let mut observation = SourceObservation {
            candidates: candidates.len(),
            hidden_files: candidates.skipped_hidden_count(),
            footer_read_bytes: 0,
        };
        let entries_retention =
            self.byte_author
                .reserve_owned_bytes(vector_bytes::<RegisteredFile>(candidates.len())?)?;
        let mut entries = Vec::with_capacity(candidates.len());
        let mut missing_required = None;
        let mut first_duplicate = None;
        let mut effective_mapping: Option<(NameMapping, MappingDependency)> = None;
        let read = self.binding.file_read_context(
            FileCancellation::from_connector_request(self.context),
            self.context.deadline(),
        )?;
        for candidate in candidates {
            self.context.check_active()?;
            candidate.validate()?;
            let path = candidate.path();
            let file = access
                .bind_location(path, FileIdentity::new(path, candidate.size(), None))
                .map_err(|error| footer_error(path, error))?;
            let footer = novarocks_fs::inspect_parquet_metadata_async(
                file,
                None,
                read.clone(),
                ParquetInterpretationPolicy::SkipArrowHint,
            )
            .await
            .map_err(|error| footer_error(path, error))?;
            observation.footer_read_bytes = observation
                .footer_read_bytes
                .saturating_add(footer.storage_bytes_read());
            validate_data_file_path(path)
                .map_err(|error| unsupported_scan_carrier(error.message()))?;

            let coverage = original_field_id_coverage(footer.schema()).map_err(identity_error)?;
            let mapping = if coverage == FileFieldIdCoverage::None {
                if effective_mapping.is_none() {
                    effective_mapping = Some(self.prepare_mapping()?);
                }
                effective_mapping.as_mut()
            } else {
                None
            };
            let columns = footer.physical_columns().len();
            let _leaf_retention = self
                .byte_author
                .reserve_owned_bytes(vector_bytes::<ResolvedParquetLeaf>(columns)?)?;
            let _bound_retention = self
                .byte_author
                .reserve_owned_bytes(vector_bytes::<i32>(columns)?)?;
            let mut bound_leaf_ids = Vec::with_capacity(columns);
            let (name_mapping, mut dependency) = match mapping {
                Some((mapping, dependency)) => (Some(&*mapping), Some(dependency)),
                None => (None, None),
            };
            let resolved = preview_registration(
                &footer,
                schema,
                name_mapping,
                &mut bound_leaf_ids,
                |path, id| {
                    if let Some(dependency) = dependency.as_deref_mut() {
                        dependency.used.record(path, id, self.byte_author)?;
                    }
                    Ok(())
                },
            )
            .map_err(preview_error)?;

            if first_duplicate.is_none()
                && entries
                    .last()
                    .is_some_and(|entry: &RegisteredFile| entry.path() == path)
            {
                first_duplicate = Some(entries.len());
            }
            let (entry, required) = registered_file::build_registered_file(
                candidate,
                &footer,
                &resolved,
                schema,
                source.default_partition_spec_id(),
                self.byte_author,
            )?;
            missing_required = missing_required.or(required);
            entries.push(entry);
        }
        if let Some(id) = missing_required {
            let field = schema
                .field_by_id(id)
                .expect("required field came from the captured schema");
            return Err(not_null_violation(&field.name));
        }
        if let Some(index) = first_duplicate {
            return Err(duplicate_path(entries[index].path()));
        }
        let files = RegisteredNewFiles::try_new(
            Arc::clone(&source),
            entries,
            entries_retention,
            self.byte_author.try_clone().map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
            })?,
        )
        .map_err(registration_error)?;
        if !files.is_empty()
            && let Some(snapshot) = source.snapshot_for_ref("main")
        {
            let io =
                crate::fs_io::build_file_io_for_location(source.location(), self.binding.clone());
            let active = || {
                self.context.check_active().map_err(|error| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::Unexpected,
                        error.message(),
                    )
                    .with_source(error)
                })
            };
            let manifest_sources = SnapshotSources::load(&source, snapshot, &io, active)
                .await
                .map_err(registration_error)?;
            if let Some(index) = manifest_sources
                .registered_path_match(|path| files.candidate_index(path), &io, active)
                .await
                .map_err(registration_error)?
            {
                return Err(duplicate_path(
                    files
                        .candidate_path(index)
                        .expect("matched candidate index"),
                ));
            }
        }
        self.context.check_active()?;
        Ok((files, effective_mapping.map(|(_, dependency)| dependency)))
    }

    fn prepare_mapping(&self) -> Result<(NameMapping, MappingDependency), ConnectorError> {
        let location = self
            .table
            .metadata_location()
            .ok_or_else(|| commit_error("Iceberg registration target has no metadata location"))?;
        if !self
            .catalog
            .supports_atomic_registration_mapping(location, self.context)?
        {
            return Err(unsupported_scan_carrier(
                "ADD FILES without Parquet field IDs requires an Iceberg catalog with atomic metadata version comparison and installation",
            ));
        }
        let source = self.table.metadata();
        let original = source
            .properties()
            .get(TableProperties::PROPERTY_DEFAULT_NAME_MAPPING);
        let mapping = match original {
            Some(raw) => {
                validate_name_mapping_carrier(raw)
                    .map_err(|error| unsupported_scan_carrier(error.message()))?;
                parse_name_mapping(raw).map_err(commit_error)?
            }
            None => {
                let mapping = name_mapping_from_schema(source.current_schema());
                let raw = encode_name_mapping(&mapping).map_err(commit_error)?;
                validate_name_mapping_carrier(&raw)
                    .map_err(|error| unsupported_scan_carrier(error.message()))?;
                mapping
            }
        };
        Ok((
            mapping,
            MappingDependency {
                originally_present: original.is_some(),
                used: MappingUses::default(),
            },
        ))
    }
}

struct SourceObservation {
    candidates: usize,
    hidden_files: usize,
    footer_read_bytes: u64,
}

impl Drop for SourceObservation {
    fn drop(&mut self) {
        tracing::debug!(
            candidates = self.candidates,
            hidden_files = self.hidden_files,
            footer_read_bytes = self.footer_read_bytes,
            "Iceberg ADD FILES source inspection exited"
        );
    }
}

pub(super) fn vector_bytes<T>(len: usize) -> Result<usize, ConnectorError> {
    std::alloc::Layout::array::<T>(len)
        .map(|layout| layout.size())
        .map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg registration vector geometry overflow",
            )
        })
}

fn footer_error(path: &str, error: novarocks_fs::FileError) -> ConnectorError {
    use novarocks_fs::FileErrorKind as Kind;
    match error.kind() {
        Kind::ResourceExhausted | Kind::Cancelled | Kind::DeadlineExceeded => {
            crate::file_reader::map_file_error(error)
        }
        _ => commit_error(format!("Failed to read file footer: {path}")),
    }
}

pub(super) fn registration_error(error: crate::iceberg::Error) -> ConnectorError {
    use std::error::Error as _;
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<ConnectorError>() {
            return error.clone();
        }
        source = cause.source();
    }
    commit_error(error.message())
}

fn identity_error(error: FieldIdentityError) -> ConnectorError {
    match error {
        FieldIdentityError::MixedFieldIds => {
            unsupported_scan_carrier(error.into_read_error().message())
        }
        FieldIdentityError::Corrupt(error) => commit_error(error.message()),
    }
}

fn preview_error(error: RegistrationPreviewError) -> ConnectorError {
    match error {
        RegistrationPreviewError::Identity(error) => identity_error(error),
        RegistrationPreviewError::Caller(error) => error,
        RegistrationPreviewError::Corrupt(error) => commit_error(error.message()),
        RegistrationPreviewError::UnsupportedCarrier { field, source } => unsupported_scan_carrier(
            format!("Iceberg scan does not support Parquet type {source} for column {field}"),
        ),
        RegistrationPreviewError::TypeMismatch {
            field,
            source,
            target,
        } => procedure_error(
            ConnectorErrorKind::InvalidRequest,
            FailureClass::Data,
            connector_reason::SOURCE_TYPE_MISMATCH,
            format!(
                "Target '{field}' column is '{}' type, but got source '{}' type",
                type_display(&target),
                type_display(&source)
            ),
        ),
        RegistrationPreviewError::MissingRequiredChild(field) => not_null_violation(&field),
    }
}

fn type_display(data_type: &arrow::datatypes::DataType) -> String {
    crate::iceberg::arrow::arrow_type_to_type(data_type).map_or_else(
        |_| data_type.to_string(),
        |iceberg_type| iceberg_type.to_string(),
    )
}

pub(super) fn unsupported_scan_carrier(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message).with_public_failure_detail()
}

pub(super) fn duplicate_path(path: &str) -> ConnectorError {
    commit_error(format!("File already exists: {path}"))
}

pub(super) fn commit_error(message: impl AsRef<str>) -> ConnectorError {
    procedure_error(
        ConnectorErrorKind::CorruptData,
        FailureClass::Dependency,
        connector_reason::ICEBERG_COMMIT_ERROR,
        format!("Failed to add files: {}", message.as_ref()),
    )
}

fn not_null_violation(field: &str) -> ConnectorError {
    procedure_error(
        ConnectorErrorKind::InvalidRequest,
        FailureClass::Data,
        connector_reason::NOT_NULL_VIOLATION,
        format!("NULL value not allowed for NOT NULL column: {field}"),
    )
}

#[cfg(test)]
pub(crate) mod tests;
