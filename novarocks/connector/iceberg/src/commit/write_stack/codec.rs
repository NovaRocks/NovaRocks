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

//! Iceberg's four directional connector-write codec facets.
//!
//! This is the only Iceberg module that turns the provider-private write wire
//! payload into an Iceberg write domain value, or the reverse. Each facet holds
//! the [`IcebergWriteAdapter`] of one exact catalog generation and delegates
//! value projection to the same capability-free [`IcebergWriteValueCodec`], so:
//!
//! * an **encoder** can only start from a neutral value its own generation
//!   minted — the adapter refuses every other one — and it has no method that
//!   accepts untrusted carrier data at all; and
//! * a **decoder** rewraps the value it builds with its own generation's
//!   binding, so a decoded value is usable only by that generation.
//!
//! Two layers of validation meet here and neither substitutes for the other.
//! [`crate::wire::write`] proves a payload is canonical, in bounds, and
//! structurally an Iceberg write value. Iceberg's deeper cross-field rules live
//! in the domain constructors, so every decode below routes through `try_new*`
//! rather than assembling a struct field by field. Building one by hand would
//! let a wire value exist that the provider's own rules would have rejected —
//! for example a Puffin writer carrying a Parquet row-group size, or a merge
//! target frozen against another snapshot — and nothing downstream would ever
//! catch it.
//!
//! Where a domain fact has no faithful carrier the answer is an error carrying
//! the real field path, never a default and never a silent narrowing.

use crate::commit::model::EntryIdentity;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_proto_codec::connector_write::ConnectorWriteCodecError;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_spi::connector::write_stack::{
    ConnectorCommitFragment, ConnectorWriterHandle, MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES,
    MAX_CONNECTOR_WRITER_HANDLE_BYTES,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorError, ConnectorErrorKind, ConnectorFieldPath,
    ConnectorPrivateDecoder, ConnectorPrivateEncoder, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder, ConnectorWriteHandleWireDecoder,
    ConnectorWriteHandleWireEncoder,
};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use prost::Message;

use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitArtifact, IcebergCommitFragment,
    IcebergContentRange, IcebergDataBranchRecipe, IcebergDataFileArtifact,
    IcebergDeletionVectorArtifact, IcebergEqualityDeleteColumnFacts,
    IcebergEqualityDeleteFileArtifact, IcebergEqualityDeleteRecipe,
    IcebergPositionDeleteFileArtifact, IcebergWriteBranch, IcebergWriteTableFacts,
    IcebergWriterHandle, IcebergWriterOutput,
};
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::delete_file::{IcebergFileContent, IcebergFileFormat};
use crate::scan_model::IcebergSchemaDef;
use crate::wire::dto;
use crate::write_descriptor::{IcebergPartitionDescriptor, IcebergPartitionValueDescriptor};

fn encode_merged_reference(identity: &EntryIdentity) -> dto::IcebergMergedDeleteReference {
    let entry = match identity {
        EntryIdentity::DeleteFile { path } => {
            dto::iceberg_merged_delete_reference::Entry::DeleteFilePath(path.clone())
        }
        EntryIdentity::DeletionVector {
            path,
            offset,
            length,
            referenced_data_file,
        } => dto::iceberg_merged_delete_reference::Entry::DeletionVector(
            dto::IcebergDeletionVectorReference {
                path: path.clone(),
                content_offset: *offset,
                content_size_in_bytes: *length,
                referenced_data_file: referenced_data_file.clone(),
            },
        ),
        EntryIdentity::DataFile { .. } => {
            unreachable!("validated merged delete references exclude data entries")
        }
    };
    dto::IcebergMergedDeleteReference { entry: Some(entry) }
}

/// Generation-bound envelope and capability checks shared by the four facets.
/// The value projector owns no adapter and cannot wrap a runtime capability.
#[derive(Clone)]
struct IcebergWriteCodec {
    adapter: IcebergWriteAdapter,
    values: IcebergWriteValueCodec,
}

impl IcebergWriteCodec {
    fn new(adapter: IcebergWriteAdapter) -> Self {
        let values = IcebergWriteValueCodec::new(Arc::<str>::from(
            adapter.binding().descriptor().instance_id.as_str(),
        ));
        Self { adapter, values }
    }

    fn validate_private_header(
        &self,
        context: &ConnectorDecodeContext<'_>,
        category: ConnectorCodecCategory,
    ) -> Result<(), ConnectorCodecError> {
        let binding = self.adapter.binding();
        context.expected_header().validate_expected(
            &binding.descriptor().provider_id,
            binding.catalog_handle(),
            category,
            ConnectorCodecRevision::try_new(crate::wire::write::WRITE_CODEC_REVISION)
                .expect("Iceberg write codec revision is non-zero"),
        )
    }

    fn envelope(
        &self,
        category: ConnectorCodecCategory,
        payload: Bytes,
    ) -> ConnectorEncodedPayload {
        let binding = self.adapter.binding();
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(crate::wire::write::WRITE_CODEC_REVISION)
                    .expect("Iceberg write codec revision is non-zero"),
            ),
            payload,
        )
    }
}

/// The single provider-private DTO/domain projector, independent of catalog
/// runtime bindings. The owner identifies diagnostics only; callers retain
/// responsibility for envelope admission and raw-wire structural validation.
#[derive(Clone)]
pub(crate) struct IcebergWriteValueCodec {
    owner: Arc<str>,
}

impl IcebergWriteValueCodec {
    pub(crate) fn decode_limits(max_bytes: usize) -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(max_bytes, max_bytes, max_bytes, 1_000_000, 64)
            .expect("Iceberg write decode limits are finite and non-zero")
    }

    pub(crate) fn new(owner: impl Into<Arc<str>>) -> Self {
        Self {
            owner: owner.into(),
        }
    }

    fn invalid(&self, path: FieldPath, detail: impl Into<String>) -> ConnectorWriteCodecError {
        ConnectorWriteCodecError::invalid(&self.owner, path, detail)
    }

    /// Surface a provider constructor's own refusal verbatim, at the field it
    /// belongs to.
    ///
    /// `CorruptData` means two facts that each look legal alone contradict each
    /// other, which is exactly a conflict; anything else is an invalid value.
    /// Either way the domain's sentence is kept, because it names the rule that
    /// was broken more precisely than the codec ever could.
    fn rejected(&self, path: FieldPath, error: &ConnectorError) -> ConnectorWriteCodecError {
        match error.kind() {
            ConnectorErrorKind::CorruptData => {
                ConnectorWriteCodecError::conflict(&self.owner, path, error.message())
            }
            _ => ConnectorWriteCodecError::invalid(&self.owner, path, error.message()),
        }
    }

    /// A required message the carrier did not carry.
    ///
    /// `ValidatedWriterHandle` / `ValidatedCommitFragment` already prove every
    /// one of these is present, so reaching here means the two layers disagree.
    /// That is still an error rather than an `expect`: a decoder must not be
    /// the thing that panics on a carrier.
    fn missing(&self, path: FieldPath, detail: &'static str) -> ConnectorWriteCodecError {
        ConnectorWriteCodecError::new(
            &self.owner,
            ProtocolError::new(path, ProtocolErrorKind::MissingField, detail),
        )
    }

    // -- enums ------------------------------------------------------------

    fn encode_file_format(
        &self,
        format: IcebergFileFormat,
        path: FieldPath,
    ) -> Result<i32, ConnectorWriteCodecError> {
        match format {
            IcebergFileFormat::Parquet => Ok(dto::IcebergWriteFileFormat::Parquet as i32),
            IcebergFileFormat::Puffin => Ok(dto::IcebergWriteFileFormat::Puffin as i32),
            IcebergFileFormat::Unknown => Err(self.invalid(
                path,
                "an Iceberg write carrier requires an exact file format",
            )),
        }
    }

    fn decode_file_format(
        &self,
        value: i32,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergFileFormat, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = match dto::IcebergWriteFileFormat::try_from(value) {
            Ok(dto::IcebergWriteFileFormat::Parquet) => Ok(IcebergFileFormat::Parquet),
            Ok(dto::IcebergWriteFileFormat::Puffin) => Ok(IcebergFileFormat::Puffin),
            Ok(dto::IcebergWriteFileFormat::Unspecified) | Err(_) => {
                Err(spi_codec_error(self.invalid(
                    path,
                    "an Iceberg write carrier requires a named file format",
                )))
            }
        };
        finish_value_decode(result, context)
    }

    fn encode_file_content(&self, content: IcebergFileContent) -> i32 {
        match content {
            IcebergFileContent::Data => dto::IcebergFileContent::Data as i32,
            IcebergFileContent::PositionDeletes => dto::IcebergFileContent::PositionDeletes as i32,
            IcebergFileContent::EqualityDeletes => dto::IcebergFileContent::EqualityDeletes as i32,
        }
    }

    fn decode_file_content(
        &self,
        value: i32,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergFileContent, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = match dto::IcebergFileContent::try_from(value) {
            Ok(dto::IcebergFileContent::Data) => Ok(IcebergFileContent::Data),
            Ok(dto::IcebergFileContent::PositionDeletes) => Ok(IcebergFileContent::PositionDeletes),
            Ok(dto::IcebergFileContent::EqualityDeletes) => Ok(IcebergFileContent::EqualityDeletes),
            Ok(dto::IcebergFileContent::Unspecified) | Err(_) => {
                Err(spi_codec_error(self.invalid(
                    path,
                    "an Iceberg write carrier requires a named file content kind",
                )))
            }
        };
        finish_value_decode(result, context)
    }

    fn encode_branch(&self, branch: IcebergWriteBranch) -> i32 {
        match branch {
            IcebergWriteBranch::Data => dto::IcebergWriteBranch::Data as i32,
            IcebergWriteBranch::PositionDelete => dto::IcebergWriteBranch::PositionDelete as i32,
            IcebergWriteBranch::DeletionVector => dto::IcebergWriteBranch::DeletionVector as i32,
            IcebergWriteBranch::EqualityDelete => dto::IcebergWriteBranch::EqualityDelete as i32,
        }
    }

    fn decode_branch(
        &self,
        value: i32,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergWriteBranch, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = match dto::IcebergWriteBranch::try_from(value) {
            Ok(dto::IcebergWriteBranch::Data) => Ok(IcebergWriteBranch::Data),
            Ok(dto::IcebergWriteBranch::PositionDelete) => Ok(IcebergWriteBranch::PositionDelete),
            Ok(dto::IcebergWriteBranch::DeletionVector) => Ok(IcebergWriteBranch::DeletionVector),
            Ok(dto::IcebergWriteBranch::EqualityDelete) => Ok(IcebergWriteBranch::EqualityDelete),
            Ok(dto::IcebergWriteBranch::Unspecified) | Err(_) => {
                Err(spi_codec_error(self.invalid(
                    path,
                    "an Iceberg writer handle requires a named write branch",
                )))
            }
        };
        finish_value_decode(result, context)
    }

    /// The carrier names a codec, not a codec *and* a level.
    ///
    /// A non-default level therefore has nowhere to go, and silently writing it
    /// at the default level would produce files the frontend did not ask for.
    /// Rejecting is the only honest answer; the production path freezes SNAPPY.
    fn encode_compression(
        &self,
        compression: Compression,
        path: FieldPath,
    ) -> Result<i32, ConnectorWriteCodecError> {
        match compression {
            Compression::UNCOMPRESSED => Ok(dto::IcebergCompression::None as i32),
            Compression::SNAPPY => Ok(dto::IcebergCompression::Snappy as i32),
            Compression::LZ4 => Ok(dto::IcebergCompression::Lz4 as i32),
            Compression::GZIP(level) if level == GzipLevel::default() => {
                Ok(dto::IcebergCompression::Gzip as i32)
            }
            Compression::ZSTD(level) if level == ZstdLevel::default() => {
                Ok(dto::IcebergCompression::Zstd as i32)
            }
            other => Err(self.invalid(
                path,
                format!("the Iceberg write carrier cannot express compression {other:?}"),
            )),
        }
    }

    fn decode_compression(
        &self,
        value: i32,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<Compression, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = match dto::IcebergCompression::try_from(value) {
            Ok(dto::IcebergCompression::None) => Ok(Compression::UNCOMPRESSED),
            Ok(dto::IcebergCompression::Snappy) => Ok(Compression::SNAPPY),
            Ok(dto::IcebergCompression::Gzip) => Ok(Compression::GZIP(GzipLevel::default())),
            Ok(dto::IcebergCompression::Lz4) => Ok(Compression::LZ4),
            Ok(dto::IcebergCompression::Zstd) => Ok(Compression::ZSTD(ZstdLevel::default())),
            Ok(dto::IcebergCompression::Unspecified) | Err(_) => {
                Err(spi_codec_error(self.invalid(
                    path,
                    "an Iceberg writer output requires a named compression codec",
                )))
            }
        };
        finish_value_decode(result, context)
    }

    // -- shared value shapes ----------------------------------------------

    fn encode_content_range(&self, range: IcebergContentRange) -> dto::IcebergContentRange {
        dto::IcebergContentRange {
            offset: range.offset(),
            size_in_bytes: range.size_in_bytes(),
        }
    }

    fn decode_content_range(
        &self,
        range: &dto::IcebergContentRange,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergContentRange, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            {
                let arguments = (range.offset, range.size_in_bytes);
                observe_value_opaque(context, || {
                    IcebergContentRange::try_new(arguments.0, arguments.1)
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_partition(
        &self,
        partition: &IcebergArtifactPartition,
    ) -> dto::IcebergArtifactPartition {
        dto::IcebergArtifactPartition {
            partition_path: partition.partition_path().to_string(),
            null_fingerprint: partition.null_fingerprint().to_string(),
            partition_spec_id: partition.partition_spec_id(),
            descriptor: Some(dto::IcebergPartitionDescriptor {
                values: partition
                    .descriptor()
                    .values
                    .iter()
                    .map(|value| dto::IcebergPartitionValueDescriptor {
                        is_null: value.is_null,
                        datum_bytes: value.datum_bytes.clone(),
                    })
                    .collect(),
            }),
        }
    }

    fn decode_partition(
        &self,
        partition: Option<&dto::IcebergArtifactPartition>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergArtifactPartition, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let partition = partition.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg write carrier requires its partition",
                ))
            })?;
            let descriptor = partition.descriptor.as_ref().ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.field("descriptor"),
                    "an Iceberg artifact partition requires its descriptor",
                ))
            })?;
            let mut values = Vec::with_capacity(descriptor.values.len());
            for value in &descriptor.values {
                values.push(IcebergPartitionValueDescriptor {
                    is_null: value.is_null,
                    datum_bytes: value
                        .datum_bytes
                        .as_deref()
                        .map(|bytes| copy_value_slice(bytes, context))
                        .transpose()?,
                });
                context.observe_compile_step()?;
            }
            // `IcebergArtifactPartition::try_new` owns the null/datum agreement:
            // repairing it here would move a row into a different partition.
            {
                let arguments = (
                    copy_value_string(&partition.partition_path, context)?,
                    copy_value_string(&partition.null_fingerprint, context)?,
                    partition.partition_spec_id,
                    IcebergPartitionDescriptor { values },
                );
                observe_value_opaque(context, || {
                    IcebergArtifactPartition::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_metrics(&self, metrics: &IcebergArtifactMetrics) -> dto::IcebergArtifactMetrics {
        dto::IcebergArtifactMetrics {
            record_count: metrics.record_count(),
            file_size_in_bytes: metrics.file_size_in_bytes(),
            split_offsets: metrics.split_offsets().to_vec(),
            column_stats: metrics.column_stats().map(|stats| dto::IcebergColumnStats {
                column_sizes: stats.column_sizes.clone(),
                value_counts: stats.value_counts.clone(),
                null_value_counts: stats.null_value_counts.clone(),
                nan_value_counts: stats.nan_value_counts.clone(),
                lower_bounds: stats.lower_bounds.clone(),
                upper_bounds: stats.upper_bounds.clone(),
            }),
        }
    }

    fn decode_metrics(
        &self,
        metrics: Option<&dto::IcebergArtifactMetrics>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergArtifactMetrics, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let metrics = metrics.ok_or_else(|| {
                spi_codec_error(
                    self.missing(path.clone(), "an Iceberg artifact requires its metrics"),
                )
            })?;
            let column_stats = metrics
                .column_stats
                .as_ref()
                .map(|stats| {
                    Ok::<_, ConnectorCodecError>(IcebergColumnStats {
                        column_sizes: copy_value_map(&stats.column_sizes, context)?,
                        value_counts: copy_value_map(&stats.value_counts, context)?,
                        null_value_counts: copy_value_map(&stats.null_value_counts, context)?,
                        nan_value_counts: copy_value_map(&stats.nan_value_counts, context)?,
                        lower_bounds: copy_value_bytes_map(&stats.lower_bounds, context)?,
                        upper_bounds: copy_value_bytes_map(&stats.upper_bounds, context)?,
                    })
                })
                .transpose()?;
            {
                let arguments = (
                    metrics.record_count,
                    metrics.file_size_in_bytes,
                    copy_value_slice(&metrics.split_offsets, context)?,
                    column_stats,
                );
                observe_value_opaque(context, || {
                    IcebergArtifactMetrics::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    /// The carrier holds one route field, and a route's prefix is the only part
    /// of it that decides which objects the route covers.
    ///
    /// Every production route is derived from the artifact's own location, so
    /// deriving it back from the prefix is exact. A hand-assembled route whose
    /// scheme or authority does not follow from its own prefix has no faithful
    /// carrier, and is refused here rather than quietly rewritten on the far
    /// side into a route naming storage the artifact does not live in.
    fn encode_storage_route(
        &self,
        route: &IcebergStorageRoute,
        path: FieldPath,
    ) -> Result<dto::IcebergStorageRoute, ConnectorWriteCodecError> {
        let access_binding = route.prefix().to_string();
        let derived = IcebergStorageRoute::try_for_location(&access_binding)
            .map_err(|error| self.rejected(path.clone(), &error))?;
        if &derived != route {
            return Err(self.invalid(
                path,
                "Iceberg storage route scheme or authority does not follow from its own prefix",
            ));
        }
        Ok(dto::IcebergStorageRoute { access_binding })
    }

    fn decode_storage_route(
        &self,
        route: Option<&dto::IcebergStorageRoute>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergStorageRoute, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let route = route.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg old delete reference requires its storage route",
                ))
            })?;
            {
                let arguments = (&route.access_binding,);
                observe_value_opaque(context, || {
                    IcebergStorageRoute::try_for_location(arguments.0)
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path.field("access_binding"), &error)))
        })();
        finish_value_decode(result, context)
    }

    // -- writer handle -----------------------------------------------------

    fn encode_table(&self, table: &IcebergWriteTableFacts) -> dto::IcebergWriteTableFacts {
        dto::IcebergWriteTableFacts {
            table_uuid: table.table_uuid().to_string(),
            namespace: table.namespace().to_string(),
            table_name: table.table_name().to_string(),
            table_location: table.table_location().to_string(),
            data_location: table.data_location().to_string(),
            target_ref: table.target_ref().to_string(),
            base_snapshot_id: table.base_snapshot_id(),
            base_sequence_number: table.base_sequence_number(),
            schema_id: table.schema_id(),
            default_partition_spec_id: table.default_partition_spec_id(),
            format_version: u32::from(table.format_version()),
        }
    }

    fn decode_table(
        &self,
        table: Option<&dto::IcebergWriteTableFacts>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergWriteTableFacts, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let table = table.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg writer handle requires its table facts",
                ))
            })?;
            let format_version = u8::try_from(table.format_version).map_err(|_| {
                spi_codec_error(self.invalid(
                    path.field("format_version"),
                    format!(
                        "Iceberg table format version {} is not a supported version",
                        table.format_version
                    ),
                ))
            })?;
            {
                let arguments = (
                    copy_value_string(&table.table_uuid, context)?,
                    copy_value_string(&table.namespace, context)?,
                    copy_value_string(&table.table_name, context)?,
                    copy_value_string(&table.table_location, context)?,
                    copy_value_string(&table.data_location, context)?,
                    copy_value_string(&table.target_ref, context)?,
                    table.base_snapshot_id,
                    table.base_sequence_number,
                    table.schema_id,
                    table.default_partition_spec_id,
                    format_version,
                );
                observe_value_opaque(context, || {
                    IcebergWriteTableFacts::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                        arguments.4,
                        arguments.5,
                        arguments.6,
                        arguments.7,
                        arguments.8,
                        arguments.9,
                        arguments.10,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_output(
        &self,
        output: &IcebergWriterOutput,
        path: FieldPath,
    ) -> Result<dto::IcebergWriterOutput, ConnectorWriteCodecError> {
        Ok(dto::IcebergWriterOutput {
            file_format: self
                .encode_file_format(output.file_format(), path.field("file_format"))?,
            compression: self
                .encode_compression(output.compression(), path.field("compression"))?,
            parquet_row_group_size_bytes: output.parquet_row_group_size_bytes(),
        })
    }

    fn decode_output(
        &self,
        output: Option<&dto::IcebergWriterOutput>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergWriterOutput, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let output = output.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg writer handle requires its output settings",
                ))
            })?;
            let file_format =
                self.decode_file_format(output.file_format, path.field("file_format"), context)?;
            let compression =
                self.decode_compression(output.compression, path.field("compression"), context)?;
            // `try_new` owns the rule that a Parquet row-group size belongs only to
            // a Parquet writer: the carrier can state both, and only Iceberg knows
            // the pairing is a contradiction.
            {
                let arguments = (
                    file_format,
                    compression,
                    output.parquet_row_group_size_bytes,
                );
                observe_value_opaque(context, || {
                    IcebergWriterOutput::try_new(arguments.0, arguments.1, arguments.2)
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_recipe(
        &self,
        recipe: &IcebergDataBranchRecipe,
        path: FieldPath,
    ) -> Result<dto::IcebergDataBranchRecipe, ConnectorWriteCodecError> {
        // The schema's own definition makes JSON its serialized form: the two
        // `Literal` convenience fields are `#[serde(skip)]` and their durable
        // spellings travel as `initial_default_json` / `write_default_json`.
        let input_schema_json = recipe
            .input_schema()
            .map(|schema| {
                serde_json::to_string(schema).map_err(|error| {
                    self.invalid(
                        path.field("input_schema_json"),
                        format!("encode Iceberg data writer input schema failed: {error}"),
                    )
                })
            })
            .transpose()?;
        Ok(dto::IcebergDataBranchRecipe {
            input_schema_json,
            partition_source_column_names: recipe.partition_source_column_names().to_vec(),
            partition_column_names: recipe.partition_column_names().to_vec(),
            transform_exprs: recipe.transform_exprs().to_vec(),
            row_lineage: recipe.row_lineage(),
        })
    }

    fn decode_recipe(
        &self,
        recipe: Option<&dto::IcebergDataBranchRecipe>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergDataBranchRecipe, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let recipe = recipe.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg data branch requires its data recipe",
                ))
            })?;
            let input_schema = recipe
                .input_schema_json
                .as_deref()
                .map(|json| {
                    {
                        let arguments = (json,);
                        observe_value_opaque(context, || {
                            serde_json::from_str::<IcebergSchemaDef>(arguments.0)
                        })?
                    }
                    .map_err(|error| {
                        spi_codec_error(self.invalid(
                            path.field("input_schema_json"),
                            format!("decode Iceberg data writer input schema failed: {error}"),
                        ))
                    })
                })
                .transpose()?;
            {
                let arguments = (
                    input_schema,
                    copy_value_strings(&recipe.partition_source_column_names, context)?,
                    copy_value_strings(&recipe.partition_column_names, context)?,
                    copy_value_strings(&recipe.transform_exprs, context)?,
                    recipe.row_lineage,
                );
                observe_value_opaque(context, || {
                    IcebergDataBranchRecipe::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                        arguments.4,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_reference(
        &self,
        reference: &IcebergOldDeleteArtifactRef,
        path: FieldPath,
    ) -> Result<dto::IcebergOldDeleteArtifactRef, ConnectorWriteCodecError> {
        Ok(dto::IcebergOldDeleteArtifactRef {
            path: reference.path().to_string(),
            content: self.encode_file_content(reference.content()),
            file_format: self
                .encode_file_format(reference.file_format(), path.field("file_format"))?,
            file_size_in_bytes: reference.file_size_in_bytes(),
            // Presence is semantic. A missing manifest count stays unknown;
            // zero, when a future source can prove it, remains a known zero.
            record_count: reference.record_count(),
            content_range: reference
                .content_range()
                .map(|range| self.encode_content_range(range)),
            referenced_data_file: reference.referenced_data_file().map(str::to_string),
            data_sequence_number: reference.data_sequence_number(),
            added_snapshot_id: reference.added_snapshot_id(),
            partition_spec_id: reference.partition_spec_id(),
            storage_route: Some(
                self.encode_storage_route(reference.storage_route(), path.field("storage_route"))?,
            ),
        })
    }

    fn decode_reference(
        &self,
        reference: &dto::IcebergOldDeleteArtifactRef,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergOldDeleteArtifactRef, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let content_range = reference
                .content_range
                .as_ref()
                .map(|range| self.decode_content_range(range, path.field("content_range"), context))
                .transpose()?;
            {
                let arguments = (
                    copy_value_string(&reference.path, context)?,
                    self.decode_file_content(reference.content, path.field("content"), context)?,
                    self.decode_file_format(
                        reference.file_format,
                        path.field("file_format"),
                        context,
                    )?,
                    reference.file_size_in_bytes,
                    reference.record_count,
                    content_range,
                    reference
                        .referenced_data_file
                        .as_deref()
                        .map(|value| copy_value_string(value, context))
                        .transpose()?,
                    reference.data_sequence_number,
                    reference.added_snapshot_id,
                    reference.partition_spec_id,
                    self.decode_storage_route(
                        reference.storage_route.as_ref(),
                        path.field("storage_route"),
                        context,
                    )?,
                );
                observe_value_opaque(context, || {
                    IcebergOldDeleteArtifactRef::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                        arguments.4,
                        arguments.5,
                        arguments.6,
                        arguments.7,
                        arguments.8,
                        arguments.9,
                        arguments.10,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_merge_target(
        &self,
        target: &IcebergOldDeleteMergeTarget,
        path: FieldPath,
    ) -> Result<dto::IcebergOldDeleteMergeTarget, ConnectorWriteCodecError> {
        let mut references = Vec::with_capacity(target.references().len());
        for (index, reference) in target.references().iter().enumerate() {
            references
                .push(self.encode_reference(reference, path.field("references").index(index))?);
        }
        Ok(dto::IcebergOldDeleteMergeTarget {
            data_file_path: target.data_file_path().to_string(),
            data_file_record_count: target.data_file_record_count(),
            data_file_sequence_number: target.data_file_sequence_number(),
            partition: Some(self.encode_partition(target.partition())),
            base_snapshot_id: target.base_snapshot_id(),
            references,
        })
    }

    fn decode_merge_target(
        &self,
        target: &dto::IcebergOldDeleteMergeTarget,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergOldDeleteMergeTarget, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let mut references = Vec::with_capacity(target.references.len());
            for (index, reference) in target.references.iter().enumerate() {
                references.push(self.decode_reference(
                    reference,
                    path.field("references").index(index),
                    context,
                )?);
                context.observe_compile_step()?;
            }
            // `try_new` owns the target-wide rules — a reference that belongs to
            // another data file, a repeated artifact, a partition spec that
            // disagrees with the data file's — none of which the carrier's shape
            // can express.
            {
                let arguments = (
                    copy_value_string(&target.data_file_path, context)?,
                    target.data_file_record_count,
                    target.data_file_sequence_number,
                    self.decode_partition(
                        target.partition.as_ref(),
                        path.field("partition"),
                        context,
                    )?,
                    target.base_snapshot_id,
                    references,
                );
                observe_value_opaque(context, || {
                    IcebergOldDeleteMergeTarget::try_new(
                        arguments.0,
                        arguments.1,
                        arguments.2,
                        arguments.3,
                        arguments.4,
                        arguments.5,
                    )
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    fn encode_equality_recipe(
        &self,
        recipe: &IcebergEqualityDeleteRecipe,
    ) -> dto::IcebergEqualityDeleteRecipe {
        dto::IcebergEqualityDeleteRecipe {
            columns: recipe
                .columns()
                .iter()
                .map(|column| dto::IcebergEqualityDeleteColumn {
                    name: column.name().to_string(),
                    field_id: column.field_id(),
                    data_type: column.data_type().to_string(),
                    nullable: column.nullable(),
                })
                .collect(),
        }
    }

    fn decode_equality_recipe(
        &self,
        recipe: Option<&dto::IcebergEqualityDeleteRecipe>,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergEqualityDeleteRecipe, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let recipe = recipe.ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg equality-delete branch requires its equality recipe",
                ))
            })?;
            let mut columns = Vec::with_capacity(recipe.columns.len());
            for (index, column) in recipe.columns.iter().enumerate() {
                columns.push(
                    {
                        let arguments = (
                            copy_value_string(&column.name, context)?,
                            column.field_id,
                            copy_value_string(&column.data_type, context)?,
                            column.nullable,
                        );
                        observe_value_opaque(context, || {
                            IcebergEqualityDeleteColumnFacts::try_new(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                            )
                        })?
                    }
                    .map_err(|error| {
                        spi_codec_error(
                            self.rejected(path.clone().field("columns").index(index), &error),
                        )
                    })?,
                );
                context.observe_compile_step()?;
            }
            {
                let arguments = (columns,);
                observe_value_opaque(context, || {
                    IcebergEqualityDeleteRecipe::try_new(arguments.0)
                })?
            }
            .map_err(|error| spi_codec_error(self.rejected(path, &error)))
        })();
        finish_value_decode(result, context)
    }

    pub(crate) fn encode_writer_handle_value(
        &self,
        handle: &IcebergWriterHandle,
    ) -> Result<dto::IcebergWriterHandle, ConnectorWriteCodecError> {
        let path = FieldPath::root("writer_handle").field("iceberg");
        let mut old_deletes = std::collections::BTreeMap::new();
        for (key, target) in handle.old_deletes() {
            old_deletes.insert(
                key.clone(),
                self.encode_merge_target(target, path.field("old_deletes").map_key(key.clone()))?,
            );
        }
        let data = handle
            .data()
            .map(|recipe| self.encode_recipe(recipe, path.field("data")))
            .transpose()?;
        Ok(dto::IcebergWriterHandle {
            branch: self.encode_branch(handle.branch()),
            table: Some(self.encode_table(handle.table())),
            output: Some(self.encode_output(handle.output(), path.field("output"))?),
            data,
            old_deletes,
            equality: handle
                .equality()
                .map(|recipe| self.encode_equality_recipe(recipe)),
        })
    }

    /// Project a DTO already admitted by the private writer wire validator.
    /// This performs the existing domain constructor checks, not envelope or
    /// raw-wire admission.
    pub(crate) fn decode_writer_handle_value(
        &self,
        iceberg: &dto::IcebergWriterHandle,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergWriterHandle, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let path = FieldPath::root("writer_handle").field("iceberg");
            let branch = self.decode_branch(iceberg.branch, path.field("branch"), context)?;
            let table = self.decode_table(iceberg.table.as_ref(), path.field("table"), context)?;
            let output =
                self.decode_output(iceberg.output.as_ref(), path.field("output"), context)?;
            match branch {
                IcebergWriteBranch::Data => {
                    let recipe =
                        self.decode_recipe(iceberg.data.as_ref(), path.field("data"), context)?;
                    // `try_new_data` owns "a data writer produces Parquet".
                    {
                        let arguments = (table, output, recipe);
                        observe_value_opaque(context, || {
                            IcebergWriterHandle::try_new_data(arguments.0, arguments.1, arguments.2)
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))
                }
                IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => {
                    let mut targets = Vec::with_capacity(iceberg.old_deletes.len());
                    for (key, target) in &iceberg.old_deletes {
                        targets.push(
                            self.decode_merge_target(
                                target,
                                path.field("old_deletes")
                                    .map_key(copy_value_string(key, context)?),
                                context,
                            )?,
                        );
                        context.observe_compile_step()?;
                    }
                    // `try_new_delete` owns the branch/format pairing, the frozen
                    // base snapshot every target must agree with, and the exclusive
                    // ownership of each referenced data file.
                    {
                        let arguments = (branch, table, output, targets);
                        observe_value_opaque(context, || {
                            IcebergWriterHandle::try_new_delete(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))
                }
                IcebergWriteBranch::EqualityDelete => {
                    let recipe = self.decode_equality_recipe(
                        iceberg.equality.as_ref(),
                        path.field("equality"),
                        context,
                    )?;
                    // `try_new_equality_delete` owns "an equality delete writes
                    // Parquet and freezes no old-delete reference".
                    {
                        let arguments = (table, output, recipe);
                        observe_value_opaque(context, || {
                            IcebergWriterHandle::try_new_equality_delete(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))
                }
            }
        })();
        finish_value_decode(result, context)
    }

    // -- commit fragment ---------------------------------------------------

    pub(crate) fn encode_commit_fragment_value(
        &self,
        fragment: &IcebergCommitFragment,
    ) -> Result<dto::IcebergCommitFragment, ConnectorWriteCodecError> {
        let path = FieldPath::root("commit_fragment").field("iceberg");
        let artifact = match fragment.artifact() {
            IcebergCommitArtifact::DataFile(file) => {
                let path = path.field("data_file");
                dto::iceberg_commit_fragment::Artifact::DataFile(dto::IcebergDataFileArtifact {
                    path: file.path().to_string(),
                    file_format: self
                        .encode_file_format(file.file_format(), path.field("file_format"))?,
                    partition: Some(self.encode_partition(file.partition())),
                    metrics: Some(self.encode_metrics(file.metrics())),
                    first_row_id: file.first_row_id(),
                })
            }
            IcebergCommitArtifact::PositionDeleteFile(file) => {
                dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(
                    dto::IcebergPositionDeleteFileArtifact {
                        path: file.path().to_string(),
                        partition: Some(self.encode_partition(file.partition())),
                        metrics: Some(self.encode_metrics(file.metrics())),
                        referenced_data_file: file.referenced_data_file().to_string(),
                        merged_old_references: file
                            .merged_old_references()
                            .iter()
                            .map(encode_merged_reference)
                            .collect(),
                    },
                )
            }
            IcebergCommitArtifact::EqualityDeleteFile(file) => {
                dto::iceberg_commit_fragment::Artifact::EqualityDeleteFile(
                    dto::IcebergEqualityDeleteFileArtifact {
                        path: file.path().to_string(),
                        partition: Some(self.encode_partition(file.partition())),
                        metrics: Some(self.encode_metrics(file.metrics())),
                        equality_field_ids: file.equality_field_ids().to_vec(),
                    },
                )
            }
            IcebergCommitArtifact::DeletionVector(file) => {
                dto::iceberg_commit_fragment::Artifact::DeletionVector(
                    dto::IcebergDeletionVectorArtifact {
                        path: file.path().to_string(),
                        partition: Some(self.encode_partition(file.partition())),
                        metrics: Some(self.encode_metrics(file.metrics())),
                        referenced_data_file: file.referenced_data_file().to_string(),
                        content_range: Some(self.encode_content_range(file.content_range())),
                        cardinality: file.cardinality(),
                        merged_old_references: file
                            .merged_old_references()
                            .iter()
                            .map(encode_merged_reference)
                            .collect(),
                    },
                )
            }
        };
        Ok(dto::IcebergCommitFragment {
            artifact: Some(artifact),
        })
    }

    fn decode_merged_reference(
        &self,
        value: &dto::IcebergMergedDeleteReference,
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<EntryIdentity, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let entry = value.entry.as_ref().ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "a merged delete reference requires its logical entry",
                ))
            })?;
            let identity = match entry {
                dto::iceberg_merged_delete_reference::Entry::DeleteFilePath(value) => {
                    EntryIdentity::DeleteFile {
                        path: copy_value_string(value, context)?,
                    }
                }
                dto::iceberg_merged_delete_reference::Entry::DeletionVector(value) => {
                    EntryIdentity::DeletionVector {
                        path: copy_value_string(&value.path, context)?,
                        offset: value.content_offset,
                        length: value.content_size_in_bytes,
                        referenced_data_file: copy_value_string(
                            &value.referenced_data_file,
                            context,
                        )?,
                    }
                }
            };
            observe_value_opaque(context, || identity.validate())?
                .map_err(|error| spi_codec_error(self.invalid(path, error.to_string())))?;
            Ok(identity)
        })();
        finish_value_decode(result, context)
    }

    fn decode_merged_references(
        &self,
        values: &[dto::IcebergMergedDeleteReference],
        path: FieldPath,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<Vec<EntryIdentity>, ConnectorCodecError> {
        context.flush_compile_control()?;
        let mut references = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            references.push(self.decode_merged_reference(value, path.index(index), context)?);
            context.observe_compile_step()?;
        }
        context.flush_compile_control()?;
        Ok(references)
    }

    /// Project a DTO already admitted by the private fragment wire validator.
    /// Envelope and raw-wire admission remain the caller's responsibility.
    pub(crate) fn decode_commit_fragment_value(
        &self,
        fragment: &dto::IcebergCommitFragment,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergCommitFragment, ConnectorCodecError> {
        context.flush_compile_control()?;
        let result = (|| {
            let path = FieldPath::root("commit_fragment").field("iceberg");
            let artifact = fragment.artifact.as_ref().ok_or_else(|| {
                spi_codec_error(self.missing(
                    path.clone(),
                    "an Iceberg commit fragment describes exactly one artifact",
                ))
            })?;
            match artifact {
                dto::iceberg_commit_fragment::Artifact::DataFile(file) => {
                    let path = path.field("data_file");
                    let artifact = {
                        let arguments = (
                            copy_value_string(&file.path, context)?,
                            self.decode_file_format(
                                file.file_format,
                                path.field("file_format"),
                                context,
                            )?,
                            self.decode_partition(
                                file.partition.as_ref(),
                                path.field("partition"),
                                context,
                            )?,
                            self.decode_metrics(
                                file.metrics.as_ref(),
                                path.field("metrics"),
                                context,
                            )?,
                            file.first_row_id,
                        );
                        observe_value_opaque(context, || {
                            IcebergDataFileArtifact::try_new(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                                arguments.4,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))?;
                    Ok(IcebergCommitFragment::data_file(artifact))
                }
                dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(file) => {
                    let path = path.field("position_delete_file");
                    let artifact = {
                        let arguments = (
                            copy_value_string(&file.path, context)?,
                            self.decode_partition(
                                file.partition.as_ref(),
                                path.field("partition"),
                                context,
                            )?,
                            self.decode_metrics(
                                file.metrics.as_ref(),
                                path.field("metrics"),
                                context,
                            )?,
                            copy_value_string(&file.referenced_data_file, context)?,
                            self.decode_merged_references(
                                &file.merged_old_references,
                                path.field("merged_old_references"),
                                context,
                            )?,
                        );
                        observe_value_opaque(context, || {
                            IcebergPositionDeleteFileArtifact::try_new(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                                arguments.4,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))?;
                    Ok(IcebergCommitFragment::position_delete_file(artifact))
                }
                dto::iceberg_commit_fragment::Artifact::EqualityDeleteFile(file) => {
                    let path = path.field("equality_delete_file");
                    let artifact = {
                        let arguments = (
                            copy_value_string(&file.path, context)?,
                            self.decode_partition(
                                file.partition.as_ref(),
                                path.field("partition"),
                                context,
                            )?,
                            self.decode_metrics(
                                file.metrics.as_ref(),
                                path.field("metrics"),
                                context,
                            )?,
                            copy_value_slice(&file.equality_field_ids, context)?,
                        );
                        observe_value_opaque(context, || {
                            IcebergEqualityDeleteFileArtifact::try_new(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))?;
                    Ok(IcebergCommitFragment::equality_delete_file(artifact))
                }
                dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => {
                    let path = path.field("deletion_vector");
                    let content_range = file.content_range.as_ref().ok_or_else(|| {
                        spi_codec_error(self.missing(
                            path.field("content_range"),
                            "an Iceberg deletion vector requires its blob range",
                        ))
                    })?;
                    // `try_new` owns the facts only Iceberg can check: the blob must
                    // fit inside its own Puffin file, and the cardinality must be
                    // the record count it claims.
                    let artifact = {
                        let arguments = (
                            copy_value_string(&file.path, context)?,
                            self.decode_partition(
                                file.partition.as_ref(),
                                path.field("partition"),
                                context,
                            )?,
                            self.decode_metrics(
                                file.metrics.as_ref(),
                                path.field("metrics"),
                                context,
                            )?,
                            copy_value_string(&file.referenced_data_file, context)?,
                            self.decode_content_range(
                                content_range,
                                path.field("content_range"),
                                context,
                            )?,
                            file.cardinality,
                            self.decode_merged_references(
                                &file.merged_old_references,
                                path.field("merged_old_references"),
                                context,
                            )?,
                        );
                        observe_value_opaque(context, || {
                            IcebergDeletionVectorArtifact::try_new(
                                arguments.0,
                                arguments.1,
                                arguments.2,
                                arguments.3,
                                arguments.4,
                                arguments.5,
                                arguments.6,
                            )
                        })?
                    }
                    .map_err(|error| spi_codec_error(self.rejected(path, &error)))?;
                    Ok(IcebergCommitFragment::deletion_vector(artifact))
                }
            }
        })();
        finish_value_decode(result, context)
    }
}

// Own copies and loops observe completed bounded work. Standard constructors,
// serde, map comparisons and allocations remain finite opaque library work.
fn copy_value_string(
    value: &str,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<String, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(value.to_owned());
    }
    let mut copied = String::with_capacity(value.len());
    for character in value.chars() {
        copied.push(character);
        context.observe_compile_step()?;
    }
    Ok(copied)
}
fn copy_value_slice<T: Copy>(
    value: &[T],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<T>, ConnectorCodecError> {
    if !context.is_compile_observed() {
        return Ok(value.to_vec());
    }
    let mut copied = Vec::with_capacity(value.len());
    for item in value {
        copied.push(*item);
        context.observe_compile_step()?;
    }
    Ok(copied)
}
fn copy_value_strings(
    value: &[String],
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<Vec<String>, ConnectorCodecError> {
    let mut copied = Vec::with_capacity(value.len());
    for item in value {
        copied.push(copy_value_string(item, context)?);
        context.observe_compile_step()?;
    }
    Ok(copied)
}
fn copy_value_map<T: Copy>(
    value: &std::collections::BTreeMap<i32, T>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<std::collections::BTreeMap<i32, T>, ConnectorCodecError> {
    let mut copied = std::collections::BTreeMap::new();
    for (key, item) in value {
        copied.insert(*key, *item);
        context.observe_compile_step()?;
    }
    Ok(copied)
}
fn copy_value_bytes_map(
    value: &std::collections::BTreeMap<i32, Vec<u8>>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<std::collections::BTreeMap<i32, Vec<u8>>, ConnectorCodecError> {
    let mut copied = std::collections::BTreeMap::new();
    for (key, item) in value {
        copied.insert(*key, copy_value_slice(item, context)?);
        context.observe_compile_step()?;
    }
    Ok(copied)
}
fn observe_value_opaque<T>(
    context: &mut ConnectorDecodeContext<'_>,
    operation: impl FnOnce() -> T,
) -> Result<T, ConnectorCodecError> {
    context.flush_compile_control()?;
    let result = operation();
    context.observe_compile_step()?;
    context.flush_compile_control()?;
    Ok(result)
}
fn finish_value_decode<T>(
    result: Result<T, ConnectorCodecError>,
    context: &mut ConnectorDecodeContext<'_>,
) -> Result<T, ConnectorCodecError> {
    if result
        .as_ref()
        .is_err_and(|error| error.compile_control_error().is_some())
    {
        return result;
    }
    context.flush_compile_control()?;
    result
}

/// FE half: one Iceberg write recipe becomes its carrier.
pub(crate) struct IcebergWriteHandleEncoder(IcebergWriteCodec);

impl IcebergWriteHandleEncoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteHandleWireEncoder for IcebergWriteHandleEncoder {
    fn owner(&self) -> &str {
        &self.0.values.owner
    }

    fn encode_writer_handle_payload(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        // The adapter is the only door to the domain value, and it is bound to
        // this exact generation: a handle another generation minted cannot be
        // encoded here, so a frontend cannot launder a foreign recipe onto the
        // wire under this catalog's name.
        let handle = self.0.adapter.writer_handle(handle).map_err(|error| {
            spi_codec_error(
                self.0
                    .values
                    .rejected(FieldPath::root("writer_handle"), &error),
            )
        })?;
        let private = self.encode_private(handle)?;
        Ok(self
            .0
            .envelope(ConnectorCodecCategory::WriteHandle, private))
    }
}

impl ConnectorPrivateEncoder<IcebergWriterHandle> for IcebergWriteHandleEncoder {
    fn encode_private(&self, handle: &IcebergWriterHandle) -> Result<Bytes, ConnectorCodecError> {
        let bytes = self
            .0
            .values
            .encode_writer_handle_value(handle)
            .map(|value| Bytes::from(value.encode_to_vec()))
            .map_err(spi_codec_error)?;
        if bytes.len() > MAX_CONNECTOR_WRITER_HANDLE_BYTES {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("writer_handle").field("iceberg"),
                ConnectorCodecErrorKind::Capacity,
                "Iceberg writer handle exceeds its hard byte limit",
            ));
        }
        Ok(bytes)
    }
}

/// BE half: a validated carrier becomes an Iceberg write recipe again.
pub(crate) struct IcebergWriteHandleDecoder(IcebergWriteCodec);

impl IcebergWriteHandleDecoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteHandleWireDecoder for IcebergWriteHandleDecoder {
    fn owner(&self) -> &str {
        &self.0.values.owner
    }

    fn decode_writer_handle_payload(
        &self,
        envelope: &ConnectorEncodedPayload,
    ) -> Result<ConnectorWriterHandle, ConnectorCodecError> {
        let mut ledger = ConnectorDecodeLedger::new(IcebergWriteValueCodec::decode_limits(
            MAX_CONNECTOR_WRITER_HANDLE_BYTES,
        ));
        let mut context = ConnectorDecodeContext::new(envelope.header(), &mut ledger);
        let value = self.decode_private(envelope.payload(), &mut context)?;
        // The result is rewrapped with this decoder's own binding, so only this
        // generation's writer factory can open a writer for it.
        Ok(self.0.adapter.wrap_writer_handle(value))
    }
}

impl ConnectorPrivateDecoder<IcebergWriterHandle> for IcebergWriteHandleDecoder {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergWriterHandle, ConnectorCodecError> {
        self.0
            .validate_private_header(context, ConnectorCodecCategory::WriteHandle)?;
        let value = crate::wire::write::decode_writer_handle(payload, context)?;
        self.0.values.decode_writer_handle_value(&value, context)
    }
}

/// BE half: one staged Iceberg artifact becomes its carrier.
pub(crate) struct IcebergWriteFragmentEncoder(IcebergWriteCodec);

impl IcebergWriteFragmentEncoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteFragmentWireEncoder for IcebergWriteFragmentEncoder {
    fn owner(&self) -> &str {
        &self.0.values.owner
    }

    fn encode_commit_fragment_payload(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        let fragment = self.0.adapter.commit_fragment(fragment).map_err(|error| {
            spi_codec_error(
                self.0
                    .values
                    .rejected(FieldPath::root("commit_fragment"), &error),
            )
        })?;
        let private = self.encode_private(fragment)?;
        Ok(self
            .0
            .envelope(ConnectorCodecCategory::CommitFragment, private))
    }
}

impl ConnectorPrivateEncoder<IcebergCommitFragment> for IcebergWriteFragmentEncoder {
    fn encode_private(
        &self,
        fragment: &IcebergCommitFragment,
    ) -> Result<Bytes, ConnectorCodecError> {
        let bytes = self
            .0
            .values
            .encode_commit_fragment_value(fragment)
            .map(|value| Bytes::from(value.encode_to_vec()))
            .map_err(spi_codec_error)?;
        if bytes.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("commit_fragment").field("iceberg"),
                ConnectorCodecErrorKind::Capacity,
                "Iceberg commit fragment exceeds its hard byte limit",
            ));
        }
        Ok(bytes)
    }
}

/// FE half: a validated carrier becomes an Iceberg artifact again.
pub(crate) struct IcebergWriteFragmentDecoder(IcebergWriteCodec);

impl IcebergWriteFragmentDecoder {
    pub(crate) fn new(adapter: IcebergWriteAdapter) -> Self {
        Self(IcebergWriteCodec::new(adapter))
    }
}

impl ConnectorWriteFragmentWireDecoder for IcebergWriteFragmentDecoder {
    fn owner(&self) -> &str {
        &self.0.values.owner
    }

    fn decode_commit_fragment_payload(
        &self,
        envelope: &ConnectorEncodedPayload,
    ) -> Result<ConnectorCommitFragment, ConnectorCodecError> {
        let mut ledger = ConnectorDecodeLedger::new(IcebergWriteValueCodec::decode_limits(
            MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES,
        ));
        let mut context = ConnectorDecodeContext::new(envelope.header(), &mut ledger);
        let value = self.decode_private(envelope.payload(), &mut context)?;
        Ok(self.0.adapter.wrap_commit_fragment(value))
    }
}

impl ConnectorPrivateDecoder<IcebergCommitFragment> for IcebergWriteFragmentDecoder {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergCommitFragment, ConnectorCodecError> {
        self.0
            .validate_private_header(context, ConnectorCodecCategory::CommitFragment)?;
        let value = crate::wire::write::decode_commit_fragment(payload, context)?;
        self.0.values.decode_commit_fragment_value(&value, context)
    }
}

pub(crate) fn spi_codec_error(error: ConnectorWriteCodecError) -> ConnectorCodecError {
    let protocol = error.protocol();
    let mut segments = protocol.path().segments().iter();
    let first = match segments.next() {
        Some(novarocks_proto_codec::FieldPathSegment::Field(value)) => *value,
        _ => "connector_payload",
    };
    let mut path = ConnectorFieldPath::root(first);
    for segment in segments {
        path = match segment {
            novarocks_proto_codec::FieldPathSegment::Field(value) => path.field(*value),
            novarocks_proto_codec::FieldPathSegment::Index(value) => path.index(*value),
            novarocks_proto_codec::FieldPathSegment::MapKey(value) => path.map_key(value),
        };
    }
    let kind = match protocol.kind() {
        ProtocolErrorKind::MissingField => ConnectorCodecErrorKind::MissingField,
        ProtocolErrorKind::InvalidEnum => ConnectorCodecErrorKind::InvalidEnum,
        ProtocolErrorKind::DuplicateField => ConnectorCodecErrorKind::DuplicateField,
        ProtocolErrorKind::InconsistentFields | ProtocolErrorKind::Conflict => {
            ConnectorCodecErrorKind::InconsistentFields
        }
        ProtocolErrorKind::Unsupported => ConnectorCodecErrorKind::Unsupported,
        ProtocolErrorKind::Capacity | ProtocolErrorKind::OutOfRange => {
            ConnectorCodecErrorKind::Capacity
        }
        ProtocolErrorKind::VersionMismatch => ConnectorCodecErrorKind::VersionMismatch,
        ProtocolErrorKind::InvalidValue => ConnectorCodecErrorKind::InvalidValue,
        ProtocolErrorKind::CompileControl(cause) => ConnectorCodecErrorKind::CompileControl(cause),
    };
    ConnectorCodecError::new(path, kind, protocol.detail())
}

#[cfg(test)]
mod tests {

    #[test]
    fn write_codec_bridge_keeps_pure_compile_control_cause() {
        use novarocks_type_contract::CompileControlError;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let protocol = novarocks_proto_codec::ProtocolError::new(
                novarocks_proto_codec::FieldPath::root("provider_payload").index(7),
                novarocks_proto_codec::ProtocolErrorKind::CompileControl(cause),
                cause.to_string(),
            );
            let error = super::spi_codec_error(
                novarocks_proto_codec::connector_write::ConnectorWriteCodecError::new(
                    "iceberg", protocol,
                ),
            );
            assert_eq!(error.compile_control_error(), Some(cause));
            assert_eq!(error.path().to_string(), "provider_payload[7]");
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }
    use super::*;
    use novarocks_proto_codec::ProtocolErrorKind;
    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_proto_codec::connector_write::{
        ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder, ConnectorWriteHandleDecoder,
        ConnectorWriteHandleEncoder, ValidatedCommitFragment, ValidatedWriterHandle,
    };
    use novarocks_proto_models::connector_write as public_dto;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorDecodeContext, ConnectorDecodeLedger,
        ConnectorDecodeLimits, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId,
    };

    use crate::commit::write_stack::runtime::build_write_adapter;
    use crate::commit::write_stack::test_support::{
        equality_delete_recipe, merge_target, parquet_ref, puffin_ref, sample_metrics,
        sample_partition, table_facts,
    };
    use crate::scan_model::IcebergSchemaFieldDef;

    fn adapter(catalog: &str, version: u8) -> IcebergWriteAdapter {
        let instance_id = ConnectorInstanceId::parse(catalog).expect("instance id");
        build_write_adapter(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID).expect("provider id"),
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([version; 32])),
        )
    }

    struct Facets {
        handle_encoder: IcebergWriteHandleEncoder,
        handle_decoder: IcebergWriteHandleDecoder,
        fragment_encoder: IcebergWriteFragmentEncoder,
        fragment_decoder: IcebergWriteFragmentDecoder,
        adapter: IcebergWriteAdapter,
    }

    fn facets(catalog: &str, version: u8) -> Facets {
        let adapter = adapter(catalog, version);
        Facets {
            handle_encoder: IcebergWriteHandleEncoder::new(adapter.clone()),
            handle_decoder: IcebergWriteHandleDecoder::new(adapter.clone()),
            fragment_encoder: IcebergWriteFragmentEncoder::new(adapter.clone()),
            fragment_decoder: IcebergWriteFragmentDecoder::new(adapter.clone()),
            adapter,
        }
    }

    fn private_header(
        catalog: &str,
        version: u8,
        category: ConnectorCodecCategory,
    ) -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse(crate::PROVIDER_ID).expect("provider id"),
            CatalogHandle::new(
                ConnectorInstanceId::parse(catalog).expect("catalog id"),
                CatalogVersion::from_bytes([version; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(crate::wire::write::WRITE_CODEC_REVISION)
                .expect("revision"),
        )
    }

    fn private_limits() -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(
            MAX_CONNECTOR_WRITER_HANDLE_BYTES,
            MAX_CONNECTOR_WRITER_HANDLE_BYTES,
            MAX_CONNECTOR_WRITER_HANDLE_BYTES,
            1_000_000,
            64,
        )
        .expect("private codec limits")
    }

    fn decode_private_handle(
        facets: &Facets,
        payload: &[u8],
        limits: ConnectorDecodeLimits,
    ) -> Result<IcebergWriterHandle, ConnectorCodecError> {
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        let mut ledger = ConnectorDecodeLedger::new(limits);
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        facets.handle_decoder.decode_private(payload, &mut context)
    }

    fn decode_private_fragment(
        facets: &Facets,
        payload: &[u8],
    ) -> Result<IcebergCommitFragment, ConnectorCodecError> {
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::CommitFragment);
        let mut ledger = ConnectorDecodeLedger::new(private_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        facets
            .fragment_decoder
            .decode_private(payload, &mut context)
    }

    fn push_varint(bytes: &mut Vec<u8>, mut value: u64) {
        loop {
            let next = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                bytes.push(next);
                return;
            }
            bytes.push(next | 0x80);
        }
    }

    fn push_length_delimited(bytes: &mut Vec<u8>, field: u32, value: &[u8]) {
        push_varint(bytes, u64::from((field << 3) | 2));
        push_varint(bytes, value.len() as u64);
        bytes.extend_from_slice(value);
    }

    fn writer_with_raw_table(handle: &IcebergWriterHandle, table: Vec<u8>) -> Vec<u8> {
        let codec = IcebergWriteValueCodec::new("catalog.iceberg");
        let mut private = codec
            .encode_writer_handle_value(handle)
            .expect("private handle");
        private.table = None;
        let mut bytes = private.encode_to_vec();
        push_length_delimited(&mut bytes, 2, &table);
        bytes
    }

    fn generation() -> Facets {
        facets("catalog.iceberg", 1)
    }

    fn output(format: IcebergFileFormat) -> IcebergWriterOutput {
        IcebergWriterOutput::try_new(format, Compression::SNAPPY, None).expect("output")
    }

    fn schema() -> IcebergSchemaDef {
        IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "k1".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: Some("7".to_string()),
                write_default_json: None,
                children: Vec::new(),
            }],
        }
    }

    fn data_handle() -> IcebergWriterHandle {
        IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::SNAPPY,
                Some(4096),
            )
            .expect("output"),
            IcebergDataBranchRecipe::try_new(
                Some(schema()),
                vec!["d".to_string()],
                vec!["d_day".to_string()],
                vec!["day(d)".to_string()],
                true,
            )
            .expect("recipe"),
        )
        .expect("data handle")
    }

    /// Two merge targets, each carrying two frozen references, so the map and
    /// the per-target reference vectors both have to survive the round trip.
    fn delete_handle(branch: IcebergWriteBranch) -> IcebergWriterHandle {
        let format = match branch {
            IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
            _ => IcebergFileFormat::Parquet,
        };
        let first = merge_target(
            "s3://b/wh/db/t/data/a.parquet",
            100,
            vec![
                puffin_ref(
                    "s3://b/wh/db/t/data/a-dv-1.puffin",
                    Some("s3://b/wh/db/t/data/a.parquet"),
                    4096,
                    3,
                    0,
                    64,
                )
                .expect("reference"),
                parquet_ref("s3://b/wh/db/t/data/shared-1.parquet", None, 4096, 9)
                    .expect("reference"),
            ],
        );
        let second = merge_target(
            "s3://b/wh/db/t/data/b.parquet",
            200,
            vec![
                puffin_ref(
                    "s3://b/wh/db/t/data/b-dv-1.puffin",
                    Some("s3://b/wh/db/t/data/b.parquet"),
                    8192,
                    5,
                    16,
                    128,
                )
                .expect("reference"),
                parquet_ref("s3://b/wh/db/t/data/shared-2.parquet", None, 4096, 0)
                    .expect("reference"),
            ],
        );
        IcebergWriterHandle::try_new_delete(
            branch,
            table_facts(),
            output(format),
            vec![first, second],
        )
        .expect("delete handle")
    }

    fn data_file_fragment() -> IcebergCommitFragment {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 128);
        stats.value_counts.insert(1, 10);
        stats.null_value_counts.insert(1, 0);
        stats.lower_bounds.insert(1, vec![0_u8]);
        stats.upper_bounds.insert(1, vec![9_u8]);
        IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                "s3://b/wh/db/t/data/new.parquet".to_string(),
                IcebergFileFormat::Parquet,
                sample_partition(),
                IcebergArtifactMetrics::try_new(10, 4096, vec![0, 2048], Some(stats))
                    .expect("metrics"),
                Some(100),
            )
            .expect("data file"),
        )
    }

    fn position_delete_fragment() -> IcebergCommitFragment {
        IcebergCommitFragment::position_delete_file(
            IcebergPositionDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/new-pos.parquet".to_string(),
                sample_partition(),
                sample_metrics(4, 2048),
                "s3://b/wh/db/t/data/a.parquet".to_string(),
                vec![
                    EntryIdentity::DeleteFile {
                        path: "s3://b/wh/db/t/data/old-1.parquet".to_string(),
                    },
                    EntryIdentity::DeleteFile {
                        path: "s3://b/wh/db/t/data/old-2.parquet".to_string(),
                    },
                ],
            )
            .expect("position delete file"),
        )
    }

    fn deletion_vector_fragment() -> IcebergCommitFragment {
        IcebergCommitFragment::deletion_vector(
            IcebergDeletionVectorArtifact::try_new(
                "s3://b/wh/db/t/data/new.puffin".to_string(),
                sample_partition(),
                sample_metrics(3, 4096),
                "s3://b/wh/db/t/data/a.parquet".to_string(),
                IcebergContentRange::try_new(4, 64).expect("range"),
                3,
                vec![EntryIdentity::DeletionVector {
                    path: "s3://b/wh/db/t/data/a-dv-1.puffin".to_string(),
                    offset: 4,
                    length: 64,
                    referenced_data_file: "s3://b/wh/db/t/data/a.parquet".to_string(),
                }],
            )
            .expect("deletion vector"),
        )
    }

    fn parse_handle(raw: public_dto::ConnectorWriterHandle) -> ValidatedWriterHandle {
        ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle"))
            .expect("the encoder produces a structurally valid carrier")
    }

    fn parse_fragment(raw: public_dto::ConnectorCommitFragment) -> ValidatedCommitFragment {
        ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
            .expect("the encoder produces a structurally valid carrier")
    }

    fn mutate_private_handle(
        raw: public_dto::ConnectorWriterHandle,
        mutate: impl FnOnce(&mut dto::IcebergWriterHandle),
    ) -> public_dto::ConnectorWriterHandle {
        let envelope = parse_handle(raw).into_provider_payload();
        let (header, payload) = envelope.into_parts();
        let mut private = dto::IcebergWriterHandle::decode(payload).expect("private handle");
        mutate(&mut private);
        public_dto::ConnectorWriterHandle {
            provider_payload: Some(encode_connector_payload_message(
                &ConnectorEncodedPayload::new(header, Bytes::from(private.encode_to_vec())),
            )),
        }
    }

    fn mutate_private_fragment(
        raw: public_dto::ConnectorCommitFragment,
        mutate: impl FnOnce(&mut dto::IcebergCommitFragment),
    ) -> public_dto::ConnectorCommitFragment {
        let envelope = parse_fragment(raw).into_provider_payload();
        let (header, payload) = envelope.into_parts();
        let mut private = dto::IcebergCommitFragment::decode(payload).expect("private fragment");
        mutate(&mut private);
        public_dto::ConnectorCommitFragment {
            provider_payload: Some(encode_connector_payload_message(
                &ConnectorEncodedPayload::new(header, Bytes::from(private.encode_to_vec())),
            )),
        }
    }

    fn assert_same_handle(left: &IcebergWriterHandle, right: &IcebergWriterHandle) {
        assert_eq!(left.branch(), right.branch());
        assert_eq!(left.table(), right.table());
        assert_eq!(left.output().file_format(), right.output().file_format());
        assert_eq!(left.output().compression(), right.output().compression());
        assert_eq!(
            left.output().parquet_row_group_size_bytes(),
            right.output().parquet_row_group_size_bytes()
        );
        match (left.data(), right.data()) {
            (None, None) => {}
            (Some(left), Some(right)) => {
                assert_eq!(left.input_schema(), right.input_schema());
                assert_eq!(
                    left.partition_source_column_names(),
                    right.partition_source_column_names()
                );
                assert_eq!(
                    left.partition_column_names(),
                    right.partition_column_names()
                );
                assert_eq!(left.transform_exprs(), right.transform_exprs());
                assert_eq!(left.row_lineage(), right.row_lineage());
            }
            _ => panic!("one handle has a data recipe and the other does not"),
        }
        assert_eq!(left.old_deletes(), right.old_deletes());
    }

    fn assert_same_fragment(left: &IcebergCommitFragment, right: &IcebergCommitFragment) {
        assert_eq!(left.path(), right.path());
        assert_eq!(left.partition(), right.partition());
        assert_eq!(left.metrics(), right.metrics());
        assert_eq!(left.referenced_data_file(), right.referenced_data_file());
        assert_eq!(left.merged_old_references(), right.merged_old_references());
        match (left.artifact(), right.artifact()) {
            (IcebergCommitArtifact::DataFile(left), IcebergCommitArtifact::DataFile(right)) => {
                assert_eq!(left.file_format(), right.file_format());
                assert_eq!(left.first_row_id(), right.first_row_id());
            }
            (
                IcebergCommitArtifact::PositionDeleteFile(_),
                IcebergCommitArtifact::PositionDeleteFile(_),
            ) => {}
            (
                IcebergCommitArtifact::DeletionVector(left),
                IcebergCommitArtifact::DeletionVector(right),
            ) => {
                assert_eq!(left.content_range(), right.content_range());
                assert_eq!(left.cardinality(), right.cardinality());
            }
            (
                IcebergCommitArtifact::EqualityDeleteFile(left),
                IcebergCommitArtifact::EqualityDeleteFile(right),
            ) => {
                assert_eq!(left.equality_field_ids(), right.equality_field_ids());
            }
            _ => panic!("the recovered fragment describes another artifact kind"),
        }
    }

    fn assert_public_provider_error_path(error: &ConnectorWriteCodecError, private_path: &str) {
        assert_eq!(error.protocol().path().to_string(), "provider_payload");
        assert!(
            error.protocol().detail().contains(private_path),
            "public provider_payload error must retain private path `{private_path}` in its detail: {}",
            error.protocol().detail()
        );
    }

    /// Encode, validate, decode, and prove the recovered value is the original.
    ///
    /// The re-encoding check is the backstop: the field-by-field comparison can
    /// only miss a field the encoder still carries, and comparing the carriers
    /// catches exactly that.
    fn round_trip_handle(facets: &Facets, handle: &IcebergWriterHandle) -> IcebergWriterHandle {
        let neutral = facets.adapter.wrap_writer_handle(handle.clone());
        let raw = facets
            .handle_encoder
            .encode_writer_handle(&neutral)
            .expect("encode writer handle");
        let decoded = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw.clone()))
            .expect("decode writer handle");
        let recovered = facets
            .adapter
            .writer_handle(&decoded)
            .expect("the decoded handle belongs to this generation")
            .clone();
        assert_same_handle(handle, &recovered);
        assert_eq!(
            facets
                .handle_encoder
                .encode_writer_handle(&decoded)
                .expect("re-encode"),
            raw
        );
        recovered
    }

    fn round_trip_fragment(
        facets: &Facets,
        fragment: &IcebergCommitFragment,
    ) -> IcebergCommitFragment {
        let neutral = facets.adapter.wrap_commit_fragment(fragment.clone());
        let raw = facets
            .fragment_encoder
            .encode_commit_fragment(&neutral)
            .expect("encode commit fragment");
        let decoded = facets
            .fragment_decoder
            .decode_commit_fragment(&parse_fragment(raw.clone()))
            .expect("decode commit fragment");
        let recovered = facets
            .adapter
            .commit_fragment(&decoded)
            .expect("the decoded fragment belongs to this generation")
            .clone();
        assert_same_fragment(fragment, &recovered);
        assert_eq!(
            facets
                .fragment_encoder
                .encode_commit_fragment(&decoded)
                .expect("re-encode"),
            raw
        );
        recovered
    }

    #[test]
    fn stateless_value_codec_projects_writer_and_fragment_without_runtime_binding() {
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        let mut ledger = ConnectorDecodeLedger::new(private_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        let codec = IcebergWriteValueCodec::new("pure-writer-diagnostics");
        let equality = IcebergWriterHandle::try_new_equality_delete(
            table_facts(),
            output(IcebergFileFormat::Parquet),
            equality_delete_recipe(),
        )
        .expect("equality handle");
        for handle in [
            data_handle(),
            delete_handle(IcebergWriteBranch::PositionDelete),
            delete_handle(IcebergWriteBranch::DeletionVector),
            equality,
        ] {
            let private = codec.encode_writer_handle_value(&handle).unwrap();
            let recovered = codec
                .decode_writer_handle_value(&private, &mut context)
                .unwrap();
            assert_same_handle(&handle, &recovered);
            assert_eq!(handle.equality(), recovered.equality());
        }
        for fragment in [
            data_file_fragment(),
            position_delete_fragment(),
            deletion_vector_fragment(),
        ] {
            let private = codec.encode_commit_fragment_value(&fragment).unwrap();
            let recovered = codec
                .decode_commit_fragment_value(&private, &mut context)
                .unwrap();
            assert_same_fragment(&fragment, &recovered);
        }
    }

    #[test]
    fn stateless_value_codec_keeps_domain_refusal_path_and_category() {
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        let mut ledger = ConnectorDecodeLedger::new(private_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        let codec = IcebergWriteValueCodec::new("pure-writer-diagnostics");
        let mut private = codec
            .encode_writer_handle_value(&delete_handle(IcebergWriteBranch::DeletionVector))
            .unwrap();
        private
            .output
            .as_mut()
            .unwrap()
            .parquet_row_group_size_bytes = Some(4096);
        let error = codec
            .decode_writer_handle_value(&private, &mut context)
            .expect_err("Puffin cannot carry a Parquet row group size");
        assert_eq!(error.kind(), ConnectorCodecErrorKind::InvalidValue);
        assert_eq!(error.path().to_string(), "writer_handle.iceberg.output");
        assert!(error.detail().contains("Iceberg Parquet row group size"));

        let mut private = codec
            .encode_writer_handle_value(&delete_handle(IcebergWriteBranch::PositionDelete))
            .unwrap();
        private
            .old_deletes
            .values_mut()
            .next()
            .unwrap()
            .base_snapshot_id += 1;
        let error = codec
            .decode_writer_handle_value(&private, &mut context)
            .expect_err("merge target must retain the session's base snapshot");
        assert_eq!(error.kind(), ConnectorCodecErrorKind::InvalidValue);
        assert_eq!(error.path().to_string(), "writer_handle.iceberg");
        assert!(error.detail().contains("frozen base snapshot"));
    }

    struct ProjectionControl {
        failure: novarocks_type_contract::CompileControlError,
        at_units: u32,
        calls: std::sync::Mutex<Vec<u32>>,
        refused: std::sync::atomic::AtomicBool,
    }
    impl ProjectionControl {
        fn new(failure: novarocks_type_contract::CompileControlError, at_units: u32) -> Self {
            Self {
                failure,
                at_units,
                calls: std::sync::Mutex::new(Vec::new()),
                refused: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }
    impl novarocks_type_contract::PureCompileControl for ProjectionControl {
        fn checkpoint(
            &self,
            phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            assert_eq!(
                phase,
                novarocks_type_contract::CompilePhase::ProviderValidation
            );
            assert!(
                !self.refused.load(std::sync::atomic::Ordering::SeqCst),
                "no callback after primary refusal"
            );
            self.calls.lock().unwrap().push(units);
            if units == self.at_units {
                self.refused
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(self.failure);
            }
            Ok(())
        }
    }
    fn projection_control_causes() -> [novarocks_type_contract::CompileControlError; 3] {
        use novarocks_type_contract::CompileControlError;
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }

    #[test]
    fn value_projection_control_entry_keeps_all_three_typed_causes() {
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        for cause in projection_control_causes() {
            let control = ProjectionControl::new(cause, 0);
            let mut ledger = ConnectorDecodeLedger::new(private_limits());
            let error =
                match ConnectorDecodeContext::try_new_for_compile(&header, &mut ledger, &control) {
                    Ok(_) => panic!("entry refusal must not establish a decode scope"),
                    Err(error) => error,
                };
            assert_eq!(error.compile_control_error(), Some(cause));
            assert_eq!(*control.calls.lock().unwrap(), [0]);
        }
    }

    #[test]
    fn merged_reference_value_copies_keep_compile_refusal_before_returning_owned_facts() {
        let codec = IcebergWriteValueCodec::new("pure-writer-diagnostics");
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::CommitFragment);
        for cause in projection_control_causes() {
            for vector in [false, true] {
                let path = format!("s3://b/{}", "p".repeat(768));
                let entry = if vector {
                    dto::iceberg_merged_delete_reference::Entry::DeletionVector(
                        dto::IcebergDeletionVectorReference {
                            path: "s3://b/shared.puffin".into(),
                            content_offset: 4,
                            content_size_in_bytes: 64,
                            referenced_data_file: path,
                        },
                    )
                } else {
                    dto::iceberg_merged_delete_reference::Entry::DeleteFilePath(path)
                };
                let references = [dto::IcebergMergedDeleteReference { entry: Some(entry) }];
                let control = ProjectionControl::new(cause, 256);
                let mut ledger = ConnectorDecodeLedger::new(private_limits());
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&header, &mut ledger, &control)
                        .unwrap();
                let error = codec
                    .decode_merged_references(
                        &references,
                        FieldPath::root("merged_old_references"),
                        &mut context,
                    )
                    .unwrap_err();
                assert_eq!(error.compile_control_error(), Some(cause));
                assert_eq!(control.calls.lock().unwrap().last(), Some(&256));
                let count = control.calls.lock().unwrap().len();
                assert_eq!(
                    context
                        .flush_compile_control()
                        .unwrap_err()
                        .compile_control_error(),
                    Some(cause)
                );
                assert_eq!(control.calls.lock().unwrap().len(), count);
            }
        }
    }

    #[test]
    fn value_projection_writer_and_fragment_copies_stop_at_256_without_publication() {
        let codec = IcebergWriteValueCodec::new("pure-writer-diagnostics");
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        let mut writer = codec.encode_writer_handle_value(&data_handle()).unwrap();
        writer.table.as_mut().unwrap().table_name = "w".repeat(768);
        let mut fragment = codec
            .encode_commit_fragment_value(&data_file_fragment())
            .unwrap();
        match fragment.artifact.as_mut().unwrap() {
            dto::iceberg_commit_fragment::Artifact::DataFile(file) => {
                file.path = format!("s3://b/{}", "f".repeat(768))
            }
            _ => panic!("data artifact fixture"),
        }
        for cause in projection_control_causes() {
            for writer_side in [true, false] {
                let control = ProjectionControl::new(cause, 256);
                let mut ledger = ConnectorDecodeLedger::new(private_limits());
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&header, &mut ledger, &control)
                        .unwrap();
                let error = if writer_side {
                    codec
                        .decode_writer_handle_value(&writer, &mut context)
                        .expect_err("writer copy refusal")
                } else {
                    codec
                        .decode_commit_fragment_value(&fragment, &mut context)
                        .expect_err("fragment copy refusal")
                };
                assert_eq!(error.compile_control_error(), Some(cause));
                let calls = control.calls.lock().unwrap();
                assert_eq!(calls.last(), Some(&256));
                assert!(calls.iter().all(|units| *units <= 256));
            }
        }
    }

    #[test]
    fn value_projection_opaque_tail_refuses_success_and_preserves_control_over_domain_error() {
        let codec = IcebergWriteValueCodec::new("pure-writer-diagnostics");
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        for cause in projection_control_causes() {
            for invalid in [false, true] {
                let output = dto::IcebergWriterOutput {
                    file_format: if invalid {
                        dto::IcebergWriteFileFormat::Puffin as i32
                    } else {
                        dto::IcebergWriteFileFormat::Parquet as i32
                    },
                    compression: dto::IcebergCompression::Snappy as i32,
                    parquet_row_group_size_bytes: Some(4096),
                };
                let control = ProjectionControl::new(cause, 1);
                let mut ledger = ConnectorDecodeLedger::new(private_limits());
                let mut context =
                    ConnectorDecodeContext::try_new_for_compile(&header, &mut ledger, &control)
                        .unwrap();
                let error = codec
                    .decode_output(
                        Some(&output),
                        FieldPath::root("writer_handle")
                            .field("iceberg")
                            .field("output"),
                        &mut context,
                    )
                    .expect_err(
                        "the completed constructor tail must refuse publication or ordinary error",
                    );
                assert_eq!(error.compile_control_error(), Some(cause));
                assert_eq!(control.calls.lock().unwrap().last(), Some(&1));
            }
        }
    }

    #[test]
    fn every_facet_names_its_own_generation_as_the_owner() {
        let facets = generation();
        assert_eq!(
            ConnectorWriteHandleWireEncoder::owner(&facets.handle_encoder),
            "catalog.iceberg"
        );
        assert_eq!(
            ConnectorWriteHandleWireDecoder::owner(&facets.handle_decoder),
            "catalog.iceberg"
        );
        assert_eq!(
            ConnectorWriteFragmentWireEncoder::owner(&facets.fragment_encoder),
            "catalog.iceberg"
        );
        assert_eq!(
            ConnectorWriteFragmentWireDecoder::owner(&facets.fragment_decoder),
            "catalog.iceberg"
        );
    }

    #[test]
    fn provider_private_four_direction_facets_cover_every_recipe_and_artifact() {
        let facets = generation();
        for handle in [
            data_handle(),
            delete_handle(IcebergWriteBranch::PositionDelete),
            delete_handle(IcebergWriteBranch::DeletionVector),
            IcebergWriterHandle::try_new_equality_delete(
                table_facts(),
                IcebergWriterOutput::try_new(IcebergFileFormat::Parquet, Compression::SNAPPY, None)
                    .expect("output"),
                equality_delete_recipe(),
            )
            .expect("equality-delete handle"),
        ] {
            let bytes = facets
                .handle_encoder
                .encode_private(&handle)
                .expect("private FE -> BE encoding");
            let recovered = decode_private_handle(&facets, &bytes, private_limits())
                .expect("private BE decode");
            assert_same_handle(&handle, &recovered);
        }

        let equality = IcebergCommitFragment::equality_delete_file(
            IcebergEqualityDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/_staging/eq-private.parquet".to_string(),
                unpartitioned(),
                sample_metrics(3, 128),
                vec![1, 4],
            )
            .expect("equality artifact"),
        );
        for fragment in [
            data_file_fragment(),
            position_delete_fragment(),
            deletion_vector_fragment(),
            equality,
        ] {
            let bytes = facets
                .fragment_encoder
                .encode_private(&fragment)
                .expect("private BE -> FE encoding");
            let recovered = decode_private_fragment(&facets, &bytes).expect("private FE decode");
            assert_same_fragment(&fragment, &recovered);
        }
    }

    #[test]
    fn private_decoder_rejects_wrong_binding_category_and_revision_before_payload_walk() {
        let facets = generation();
        let bytes = facets
            .handle_encoder
            .encode_private(&data_handle())
            .expect("private handle");
        for header in [
            private_header("catalog.other", 1, ConnectorCodecCategory::WriteHandle),
            private_header("catalog.iceberg", 1, ConnectorCodecCategory::CommitFragment),
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse(crate::PROVIDER_ID).expect("provider"),
                CatalogHandle::new(
                    ConnectorInstanceId::parse("catalog.iceberg").expect("catalog"),
                    CatalogVersion::from_bytes([1; 32]),
                ),
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(
                    crate::contract_revision::ICEBERG_CONTRACT_REVISION + 1,
                )
                .expect("revision"),
            ),
        ] {
            let mut ledger = ConnectorDecodeLedger::new(private_limits());
            let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
            assert!(
                facets
                    .handle_decoder
                    .decode_private(&bytes, &mut context)
                    .is_err()
            );
            assert_eq!(ledger.raw_bytes(), 0);
        }
    }

    #[test]
    fn private_raw_scan_rejects_root_and_nested_shape_loss_before_prost_decode() {
        let facets = generation();
        let valid = facets
            .handle_encoder
            .encode_private(&data_handle())
            .expect("private handle");

        let mut unknown_root = valid.to_vec();
        push_varint(&mut unknown_root, 7 << 3);
        push_varint(&mut unknown_root, 0);
        assert_eq!(
            decode_private_handle(&facets, &unknown_root, private_limits())
                .expect_err("unknown root field")
                .kind(),
            ConnectorCodecErrorKind::UnknownField
        );

        let codec = IcebergWriteValueCodec::new("catalog.iceberg");
        let table = codec.encode_table(data_handle().table());

        let mut duplicate_nested = table.encode_to_vec();
        push_length_delimited(&mut duplicate_nested, 1, b"duplicate-uuid");
        assert_eq!(
            decode_private_handle(
                &facets,
                &writer_with_raw_table(&data_handle(), duplicate_nested),
                private_limits(),
            )
            .expect_err("duplicate nested singular")
            .kind(),
            ConnectorCodecErrorKind::DuplicateField
        );

        let mut unknown_nested = table.encode_to_vec();
        push_varint(&mut unknown_nested, 12 << 3);
        push_varint(&mut unknown_nested, 0);
        assert_eq!(
            decode_private_handle(
                &facets,
                &writer_with_raw_table(&data_handle(), unknown_nested),
                private_limits(),
            )
            .expect_err("unknown nested field")
            .kind(),
            ConnectorCodecErrorKind::UnknownField
        );

        let mut wrong_wire = table.encode_to_vec();
        push_varint(&mut wrong_wire, 1 << 3);
        push_varint(&mut wrong_wire, 7);
        assert_eq!(
            decode_private_handle(
                &facets,
                &writer_with_raw_table(&data_handle(), wrong_wire),
                private_limits(),
            )
            .expect_err("wrong nested wire type")
            .kind(),
            ConnectorCodecErrorKind::InvalidValue
        );
    }

    #[test]
    fn private_decode_budgets_are_independent() {
        let facets = generation();
        let bytes = facets
            .handle_encoder
            .encode_private(&data_handle())
            .expect("private handle");
        let cases = [
            ConnectorDecodeLimits::try_new(bytes.len() - 1, usize::MAX, usize::MAX, usize::MAX, 64)
                .expect("raw"),
            ConnectorDecodeLimits::try_new(bytes.len(), 1, usize::MAX, usize::MAX, 64)
                .expect("retained"),
            ConnectorDecodeLimits::try_new(bytes.len(), usize::MAX, 1, usize::MAX, 64)
                .expect("scalar"),
            ConnectorDecodeLimits::try_new(bytes.len(), usize::MAX, usize::MAX, 1, 64)
                .expect("items"),
            ConnectorDecodeLimits::try_new(bytes.len(), usize::MAX, usize::MAX, usize::MAX, 1)
                .expect("depth"),
        ];
        for limits in cases {
            assert_eq!(
                decode_private_handle(&facets, &bytes, limits)
                    .expect_err("one independent budget must reject")
                    .kind(),
                ConnectorCodecErrorKind::Capacity
            );
        }
    }

    #[test]
    fn optional_old_delete_record_count_preserves_presence_before_domain_validation() {
        let facets = generation();
        let codec = IcebergWriteValueCodec::new("catalog.iceberg");
        let mut private = codec
            .encode_writer_handle_value(&delete_handle(IcebergWriteBranch::PositionDelete))
            .expect("private handle");
        private
            .old_deletes
            .values_mut()
            .next()
            .expect("target")
            .references
            .first_mut()
            .expect("reference")
            .record_count = None;
        let none_bytes = private.encode_to_vec();
        let header = private_header("catalog.iceberg", 1, ConnectorCodecCategory::WriteHandle);
        let mut ledger = ConnectorDecodeLedger::new(private_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        let parsed = crate::wire::write::decode_writer_handle(&none_bytes, &mut context)
            .expect("unknown count remains legal");
        assert_eq!(
            parsed.old_deletes.values().next().unwrap().references[0].record_count,
            None
        );

        private
            .old_deletes
            .values_mut()
            .next()
            .expect("target")
            .references
            .first_mut()
            .expect("reference")
            .record_count = Some(0);
        let zero_bytes = private.encode_to_vec();
        let mut ledger = ConnectorDecodeLedger::new(private_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        let parsed = crate::wire::write::decode_writer_handle(&zero_bytes, &mut context)
            .expect("protobuf presence preserves a claimed zero");
        assert_eq!(
            parsed.old_deletes.values().next().unwrap().references[0].record_count,
            Some(0)
        );
        let error = decode_private_handle(&facets, &zero_bytes, private_limits())
            .expect_err("the existing domain invariant rejects a claimed zero");
        assert_eq!(error.kind(), ConnectorCodecErrorKind::InvalidValue);
        assert!(error.detail().contains("must be positive"));
    }

    #[test]
    fn private_descriptor_contains_write_types_under_the_iceberg_namespace() {
        for name in [
            b"IcebergWriterHandle".as_slice(),
            b"IcebergCommitFragment".as_slice(),
        ] {
            assert!(
                crate::wire::FILE_DESCRIPTOR_SET
                    .windows(name.len())
                    .any(|window| window == name),
                "private descriptor is missing {}",
                String::from_utf8_lossy(name)
            );
        }
        assert!(
            !crate::wire::FILE_DESCRIPTOR_SET
                .windows(b"novarocks.connector_write".len())
                .any(|window| window == b"novarocks.connector_write")
        );
    }

    #[test]
    fn a_data_branch_handle_round_trips_through_its_carrier() {
        let facets = generation();
        let recovered = round_trip_handle(&facets, &data_handle());
        let recipe = recovered.data().expect("a data branch keeps its recipe");
        assert_eq!(recipe.input_schema(), Some(&schema()));
        assert!(recipe.row_lineage());
        assert_eq!(
            recovered.output().parquet_row_group_size_bytes(),
            Some(4096)
        );
    }

    #[test]
    fn both_delete_branches_round_trip_with_every_frozen_reference() {
        let facets = generation();
        for branch in [
            IcebergWriteBranch::PositionDelete,
            IcebergWriteBranch::DeletionVector,
        ] {
            let handle = delete_handle(branch);
            let recovered = round_trip_handle(&facets, &handle);
            assert_eq!(recovered.old_deletes().len(), 2);
            for target in recovered.old_deletes().values() {
                assert_eq!(target.references().len(), 2);
            }
            // A reference whose frozen manifest projection carried no record
            // count must come back as unknown, not as a claimed count of zero.
            let unknown = recovered
                .old_deletes()
                .get("s3://b/wh/db/t/data/b.parquet")
                .expect("second target")
                .references()
                .iter()
                .find(|reference| reference.path().ends_with("shared-2.parquet"))
                .expect("shared reference");
            assert_eq!(unknown.record_count(), None);
        }
    }

    #[test]
    fn old_delete_recipe_round_trips_two_vectors_in_one_puffin() {
        let facets = generation();
        let data = "s3://b/wh/db/t/data/a.parquet";
        let refs = [80, 4]
            .map(|offset| {
                puffin_ref(
                    "s3://b/wh/db/t/data/shared.puffin",
                    Some(data),
                    4096,
                    3,
                    offset,
                    64,
                )
                .unwrap()
            })
            .to_vec();
        let target = merge_target(data, 100, refs);
        let expected = target
            .references()
            .iter()
            .map(|entry| entry.entry_identity())
            .collect::<Vec<_>>();
        let handle = IcebergWriterHandle::try_new_delete(
            IcebergWriteBranch::DeletionVector,
            table_facts(),
            output(IcebergFileFormat::Puffin),
            vec![target],
        )
        .unwrap();
        let recovered = round_trip_handle(&facets, &handle);
        assert_eq!(
            recovered.old_deletes()[data]
                .references()
                .iter()
                .map(|entry| entry.entry_identity())
                .collect::<Vec<_>>(),
            expected
        );
        let raw = facets.handle_encoder.encode_private(&handle).unwrap();
        let mut private = dto::IcebergWriterHandle::decode(raw.as_ref()).unwrap();
        let references = &mut private.old_deletes.get_mut(data).unwrap().references;
        references.swap(0, 1);
        assert_eq!(
            decode_private_handle(&facets, &private.encode_to_vec(), private_limits())
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::InconsistentFields
        );
    }

    #[test]
    fn merged_vectors_in_one_puffin_round_trip_with_distinct_ranges() {
        let facets = generation();
        let references = [4, 80]
            .map(|offset| EntryIdentity::DeletionVector {
                path: "s3://b/wh/db/t/data/shared.puffin".to_string(),
                offset,
                length: 64,
                referenced_data_file: "s3://b/wh/db/t/data/a.parquet".to_string(),
            })
            .to_vec();
        let fragment = IcebergCommitFragment::deletion_vector(
            IcebergDeletionVectorArtifact::try_new(
                "s3://b/wh/db/t/data/new.puffin".to_string(),
                sample_partition(),
                sample_metrics(3, 4096),
                "s3://b/wh/db/t/data/a.parquet".to_string(),
                IcebergContentRange::try_new(4, 64).unwrap(),
                3,
                references.clone(),
            )
            .unwrap(),
        );
        let recovered = round_trip_fragment(&facets, &fragment);
        assert_eq!(recovered.merged_old_references(), references.as_slice());
        assert_ne!(references[0], references[1]);
        let raw = facets.fragment_encoder.encode_private(&fragment).unwrap();
        for invalid in 0..3 {
            let mut private = dto::IcebergCommitFragment::decode(raw.as_ref()).unwrap();
            let file = match private.artifact.as_mut().unwrap() {
                dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => file,
                _ => panic!("vector fixture"),
            };
            match invalid {
                0 => file.merged_old_references.swap(0, 1),
                1 => file.merged_old_references[1] = file.merged_old_references[0].clone(),
                _ => match file.merged_old_references[0].entry.as_mut().unwrap() {
                    dto::iceberg_merged_delete_reference::Entry::DeletionVector(reference) => {
                        reference.content_offset = i64::MAX;
                    }
                    _ => panic!("vector reference fixture"),
                },
            }
            assert_eq!(
                decode_private_fragment(&facets, &private.encode_to_vec())
                    .unwrap_err()
                    .kind(),
                ConnectorCodecErrorKind::InconsistentFields
            );
        }
    }

    #[test]
    fn every_artifact_kind_round_trips_through_its_carrier() {
        let facets = generation();
        for fragment in [
            data_file_fragment(),
            position_delete_fragment(),
            deletion_vector_fragment(),
        ] {
            round_trip_fragment(&facets, &fragment);
        }
    }

    #[test]
    fn a_value_from_another_generation_cannot_be_encoded() {
        let mine = generation();
        let theirs = facets("catalog.iceberg", 2);

        let handle = theirs.adapter.wrap_writer_handle(data_handle());
        let error = mine
            .handle_encoder
            .encode_writer_handle(&handle)
            .expect_err("a foreign generation's handle");
        assert_eq!(error.owner(), "catalog.iceberg");
        assert_public_provider_error_path(&error, "writer_handle");
        assert!(
            error
                .protocol()
                .detail()
                .contains("does not belong to this exact provider generation")
        );

        let fragment = theirs.adapter.wrap_commit_fragment(data_file_fragment());
        let error = mine
            .fragment_encoder
            .encode_commit_fragment(&fragment)
            .expect_err("a foreign generation's fragment");
        assert_public_provider_error_path(&error, "commit_fragment");
    }

    #[test]
    fn a_carrier_can_only_be_decoded_by_the_generation_that_encoded_it() {
        let mine = generation();
        let theirs = facets("catalog.iceberg", 2);
        let raw = mine
            .handle_encoder
            .encode_writer_handle(&mine.adapter.wrap_writer_handle(data_handle()))
            .expect("encode");
        let error = theirs
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect_err("another generation must reject the envelope");
        assert_eq!(
            error.protocol().kind(),
            ProtocolErrorKind::InconsistentFields
        );
        assert_public_provider_error_path(&error, "header.catalog");
        assert!(error.protocol().detail().contains("catalog"));
    }

    #[test]
    fn a_structurally_valid_carrier_still_faces_the_domain_constructors() {
        let facets = generation();

        // A Puffin writer carrying a Parquet row-group size: the carrier can
        // state both, and only `IcebergWriterOutput::try_new` knows the pairing
        // is a contradiction.
        let raw = facets
            .handle_encoder
            .encode_writer_handle(
                &facets
                    .adapter
                    .wrap_writer_handle(delete_handle(IcebergWriteBranch::DeletionVector)),
            )
            .expect("encode");
        let raw = mutate_private_handle(raw, |iceberg| {
            iceberg
                .output
                .as_mut()
                .expect("output")
                .parquet_row_group_size_bytes = Some(4096);
        });
        let error = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect_err("a Puffin writer with a Parquet row group size");
        assert_eq!(error.protocol().kind(), ProtocolErrorKind::InvalidValue);
        assert_public_provider_error_path(&error, "writer_handle.iceberg.output");
        assert!(
            error
                .protocol()
                .detail()
                .contains("Iceberg Parquet row group size")
        );

        // A merge target frozen against a snapshot the session is not based on.
        let raw = facets
            .handle_encoder
            .encode_writer_handle(
                &facets
                    .adapter
                    .wrap_writer_handle(delete_handle(IcebergWriteBranch::PositionDelete)),
            )
            .expect("encode");
        let raw = mutate_private_handle(raw, |iceberg| {
            iceberg
                .old_deletes
                .get_mut("s3://b/wh/db/t/data/a.parquet")
                .expect("target")
                .base_snapshot_id = 78;
        });
        let error = facets
            .handle_decoder
            .decode_writer_handle(&parse_handle(raw))
            .expect_err("a target frozen against another snapshot");
        assert_public_provider_error_path(&error, "writer_handle.iceberg");
        assert!(
            error
                .protocol()
                .detail()
                .contains("does not name the session's frozen base snapshot")
        );

        // A deletion vector whose cardinality disagrees with its record count.
        let raw = facets
            .fragment_encoder
            .encode_commit_fragment(
                &facets
                    .adapter
                    .wrap_commit_fragment(deletion_vector_fragment()),
            )
            .expect("encode");
        let raw = mutate_private_fragment(raw, |iceberg| {
            let Some(dto::iceberg_commit_fragment::Artifact::DeletionVector(vector)) =
                iceberg.artifact.as_mut()
            else {
                unreachable!("deletion vector fixture")
            };
            vector.cardinality = 2;
        });
        let error = facets
            .fragment_decoder
            .decode_commit_fragment(&parse_fragment(raw))
            .expect_err("a deletion vector that disagrees with itself");
        assert_eq!(
            error.protocol().kind(),
            ProtocolErrorKind::InconsistentFields
        );
        assert_public_provider_error_path(&error, "commit_fragment.iceberg.deletion_vector");
        assert!(
            error
                .protocol()
                .detail()
                .contains("cardinality differs from its record count")
        );
    }

    #[test]
    fn a_compression_the_carrier_cannot_express_is_refused_rather_than_narrowed() {
        let facets = generation();
        let handle = IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::GZIP(GzipLevel::try_new(9).expect("level")),
                None,
            )
            .expect("output"),
            IcebergDataBranchRecipe::try_new(None, Vec::new(), Vec::new(), Vec::new(), false)
                .expect("recipe"),
        )
        .expect("handle");
        let error = facets
            .handle_encoder
            .encode_writer_handle(&facets.adapter.wrap_writer_handle(handle))
            .expect_err("a non-default gzip level");
        assert_public_provider_error_path(&error, "writer_handle.iceberg.output.compression");
        assert!(error.protocol().detail().contains("cannot express"));
    }

    #[test]
    fn an_oversized_handle_or_fragment_is_refused_by_the_codec_layer() {
        let facets = generation();

        // One path past the 16 MiB writer-handle bound. A bare path keeps the
        // fixture cheap: it is a location the domain accepts and nothing else.
        let huge = format!("/wh/db/t/data/{}.parquet", "x".repeat(17 * 1024 * 1024));
        let handle = IcebergWriterHandle::try_new_delete(
            IcebergWriteBranch::PositionDelete,
            table_facts(),
            output(IcebergFileFormat::Parquet),
            vec![merge_target(&huge, 10, Vec::new())],
        )
        .expect("handle");
        let neutral = facets.adapter.wrap_writer_handle(handle);
        let error = facets
            .handle_encoder
            .encode_writer_handle(&neutral)
            .expect_err("an oversized writer handle");
        assert_eq!(error.protocol().kind(), ProtocolErrorKind::Capacity);
        assert_public_provider_error_path(&error, "writer_handle.iceberg");

        // One path past the 1 MiB commit-fragment bound.
        let huge = format!("/wh/db/t/data/{}.parquet", "x".repeat(1024 * 1024 + 16));
        let fragment = IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                huge,
                IcebergFileFormat::Parquet,
                sample_partition(),
                sample_metrics(1, 4096),
                None,
            )
            .expect("data file"),
        );
        let neutral = facets.adapter.wrap_commit_fragment(fragment);
        let error = facets
            .fragment_encoder
            .encode_commit_fragment(&neutral)
            .expect_err("an oversized commit fragment");
        assert_eq!(error.protocol().kind(), ProtocolErrorKind::Capacity);
        assert_public_provider_error_path(&error, "commit_fragment.iceberg");
    }
    fn unpartitioned() -> IcebergArtifactPartition {
        IcebergArtifactPartition::try_new(
            String::new(),
            String::new(),
            0,
            crate::write_descriptor::IcebergPartitionDescriptor { values: Vec::new() },
        )
        .expect("unpartitioned artifact partition")
    }

    /// The equality-delete recipe survives the FE -> BE carrier.
    ///
    /// The backend cannot invent a match key: the field ids only exist because
    /// the frontend resolved them against the frozen schema, so a recipe that
    /// did not round-trip would leave the writer with nothing to match on.
    #[test]
    fn an_equality_delete_writer_handle_round_trips_through_its_carrier() {
        let facets = generation();
        let handle = IcebergWriterHandle::try_new_equality_delete(
            table_facts(),
            IcebergWriterOutput::try_new(IcebergFileFormat::Parquet, Compression::SNAPPY, None)
                .expect("output"),
            equality_delete_recipe(),
        )
        .expect("equality delete handle");

        let encoded = facets
            .handle_encoder
            .encode_writer_handle(&facets.adapter.wrap_writer_handle(handle.clone()))
            .expect("encode");
        let validated = ValidatedWriterHandle::parse(encoded, FieldPath::root("writer_handle"))
            .expect("the carrier is structurally valid");
        let recovered = facets
            .handle_decoder
            .decode_writer_handle(&validated)
            .expect("decode");
        let recovered = facets
            .adapter
            .writer_handle(&recovered)
            .expect("provider handle");
        assert_eq!(recovered.branch(), IcebergWriteBranch::EqualityDelete);
        assert_eq!(recovered.equality(), handle.equality());
        // It carries no old-delete merge and no data recipe: an equality delete
        // supersedes nothing and writes no data file.
        assert!(recovered.old_deletes().is_empty());
        assert!(recovered.data().is_none());
    }

    /// The staged artifact survives the BE -> FE carrier, including the match
    /// key Iceberg records on the manifest.
    #[test]
    fn an_equality_delete_artifact_round_trips_through_its_carrier() {
        let facets = generation();
        let artifact =
            crate::commit::write_stack::domain::IcebergEqualityDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/_staging/eq-0.parquet".to_string(),
                unpartitioned(),
                sample_metrics(3, 128),
                vec![1, 4],
            )
            .expect("equality delete artifact");

        let encoded = facets
            .fragment_encoder
            .encode_commit_fragment(
                &facets
                    .adapter
                    .wrap_commit_fragment(IcebergCommitFragment::equality_delete_file(artifact)),
            )
            .expect("encode");
        let validated = ValidatedCommitFragment::parse(encoded, FieldPath::root("commit_fragment"))
            .expect("the carrier is structurally valid");
        let recovered = facets
            .fragment_decoder
            .decode_commit_fragment(&validated)
            .expect("decode");
        let recovered = facets
            .adapter
            .commit_fragment(&recovered)
            .expect("provider value");
        assert_eq!(recovered.branch(), IcebergWriteBranch::EqualityDelete);
        // It names no data file and merges nothing -- that is exactly what
        // separates it from a position delete.
        assert_eq!(recovered.referenced_data_file(), None);
        assert!(recovered.merged_old_references().is_empty());
        let IcebergCommitArtifact::EqualityDeleteFile(file) = recovered.artifact() else {
            panic!("expected an equality delete artifact");
        };
        assert_eq!(file.equality_field_ids(), &[1, 4]);
    }

    /// A match key must be a key. An empty one would delete every row the file
    /// is applied to, and a repeated field id would say the same column twice.
    #[test]
    fn an_equality_delete_artifact_requires_a_sorted_unique_match_key() {
        assert!(
            crate::commit::write_stack::domain::IcebergEqualityDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/eq.parquet".to_string(),
                unpartitioned(),
                sample_metrics(3, 128),
                Vec::new(),
            )
            .is_err()
        );
        assert!(
            crate::commit::write_stack::domain::IcebergEqualityDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/eq.parquet".to_string(),
                unpartitioned(),
                sample_metrics(3, 128),
                vec![4, 1],
            )
            .is_err()
        );
        assert!(
            crate::commit::write_stack::domain::IcebergEqualityDeleteFileArtifact::try_new(
                "s3://b/wh/db/t/data/eq.parquet".to_string(),
                unpartitioned(),
                sample_metrics(3, 128),
                vec![1, 1],
            )
            .is_err()
        );
    }
    // Insert INSIDE commit::write_stack::codec::tests; reuse original facets/table_facts/output.
    #[test]
    fn cow_shared_data_recipe_uses_the_original_writer_handle_wire_and_decodes_owned() {
        use crate::commit::write_stack::domain::IcebergCowSchemaBacking;
        use novarocks_spi::connector::ConnectorPayloadRetentionGuard;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Witness(Arc<AtomicUsize>);
        impl Drop for Witness {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = ConnectorPayloadRetentionGuard::new(Witness(dropped.clone()));
        // This known finite existing codec fixture does not claim a FE authorization.
        // SchemaOnlyPlan whole-before-growth is tested separately on V1/V2/V3.
        let backing = IcebergCowSchemaBacking::from_checked(schema(), guard);
        let old_recipe = IcebergDataBranchRecipe::try_new(
            Some(schema()),
            vec!["d".into()],
            vec!["d_day".into()],
            vec!["day(d)".into()],
            true,
        )
        .unwrap();
        let cow_recipe = IcebergDataBranchRecipe::try_new_cow(
            backing,
            vec!["d".into()],
            vec!["d_day".into()],
            vec!["day(d)".into()],
            true,
        )
        .unwrap();
        let old = IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::SNAPPY,
                Some(4096),
            )
            .unwrap(),
            old_recipe,
        )
        .unwrap();
        let cow = IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                Compression::SNAPPY,
                Some(4096),
            )
            .unwrap(),
            cow_recipe,
        )
        .unwrap();
        let facets = generation();
        let old_wire = facets.handle_encoder.encode_private(&old).unwrap();
        let cow_wire = facets.handle_encoder.encode_private(&cow).unwrap();
        assert_eq!(cow_wire, old_wire);
        let decoded = decode_private_handle(&facets, &cow_wire, private_limits()).unwrap();
        assert_eq!(
            decoded.data().unwrap().input_schema(),
            old.data().unwrap().input_schema()
        );
        assert_eq!(
            decoded.data().unwrap().partition_source_column_names(),
            &["d".to_string()]
        );
        assert_eq!(
            decoded.data().unwrap().partition_column_names(),
            &["d_day".to_string()]
        );
        assert_eq!(
            decoded.data().unwrap().transform_exprs(),
            &["day(d)".to_string()]
        );
        assert!(decoded.data().unwrap().row_lineage());
        let decoded_clone = decoded.clone();
        // Real codec decode uses ordinary Owned; no extra variant/tag/guard on wire.
        assert!(!std::ptr::eq(
            decoded.data().unwrap().input_schema().unwrap(),
            decoded_clone.data().unwrap().input_schema().unwrap()
        ));
        let cow_last = cow.clone();
        assert!(std::ptr::eq(
            cow.data().unwrap().input_schema().unwrap(),
            cow_last.data().unwrap().input_schema().unwrap()
        ));
        drop(cow);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(cow_last);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        drop(decoded_clone);
        drop(decoded);
        drop(old);
    }
}
