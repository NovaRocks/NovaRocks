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
//! the [`IcebergWriteAdapter`] of one exact catalog generation and nothing else,
//! so:
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

/// The shared half of all four facets: one exact generation's adapter and the
/// owner name every rejection is attributed to.
///
/// It is deliberately not public and has no accessor for its adapter. A facet
/// is installed as an `Arc<dyn ...>` trait object, so a role host holds a codec
/// it can call and can never reach the adapter, an erased payload, or a
/// downcast behind it.
#[derive(Clone)]
struct IcebergWriteCodec {
    adapter: IcebergWriteAdapter,
    owner: Arc<str>,
}

impl IcebergWriteCodec {
    fn new(adapter: IcebergWriteAdapter) -> Self {
        Self {
            owner: Arc::from(adapter.binding().descriptor().instance_id.as_str()),
            adapter,
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

    fn decode_limits(max_bytes: usize) -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(max_bytes, max_bytes, max_bytes, 1_000_000, 64)
            .expect("Iceberg write decode limits are finite and non-zero")
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
    ) -> Result<IcebergFileFormat, ConnectorWriteCodecError> {
        match dto::IcebergWriteFileFormat::try_from(value) {
            Ok(dto::IcebergWriteFileFormat::Parquet) => Ok(IcebergFileFormat::Parquet),
            Ok(dto::IcebergWriteFileFormat::Puffin) => Ok(IcebergFileFormat::Puffin),
            Ok(dto::IcebergWriteFileFormat::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg write carrier requires a named file format",
            )),
        }
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
    ) -> Result<IcebergFileContent, ConnectorWriteCodecError> {
        match dto::IcebergFileContent::try_from(value) {
            Ok(dto::IcebergFileContent::Data) => Ok(IcebergFileContent::Data),
            Ok(dto::IcebergFileContent::PositionDeletes) => Ok(IcebergFileContent::PositionDeletes),
            Ok(dto::IcebergFileContent::EqualityDeletes) => Ok(IcebergFileContent::EqualityDeletes),
            Ok(dto::IcebergFileContent::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg write carrier requires a named file content kind",
            )),
        }
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
    ) -> Result<IcebergWriteBranch, ConnectorWriteCodecError> {
        match dto::IcebergWriteBranch::try_from(value) {
            Ok(dto::IcebergWriteBranch::Data) => Ok(IcebergWriteBranch::Data),
            Ok(dto::IcebergWriteBranch::PositionDelete) => Ok(IcebergWriteBranch::PositionDelete),
            Ok(dto::IcebergWriteBranch::DeletionVector) => Ok(IcebergWriteBranch::DeletionVector),
            Ok(dto::IcebergWriteBranch::EqualityDelete) => Ok(IcebergWriteBranch::EqualityDelete),
            Ok(dto::IcebergWriteBranch::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg writer handle requires a named write branch",
            )),
        }
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
    ) -> Result<Compression, ConnectorWriteCodecError> {
        match dto::IcebergCompression::try_from(value) {
            Ok(dto::IcebergCompression::None) => Ok(Compression::UNCOMPRESSED),
            Ok(dto::IcebergCompression::Snappy) => Ok(Compression::SNAPPY),
            Ok(dto::IcebergCompression::Gzip) => Ok(Compression::GZIP(GzipLevel::default())),
            Ok(dto::IcebergCompression::Lz4) => Ok(Compression::LZ4),
            Ok(dto::IcebergCompression::Zstd) => Ok(Compression::ZSTD(ZstdLevel::default())),
            Ok(dto::IcebergCompression::Unspecified) | Err(_) => Err(self.invalid(
                path,
                "an Iceberg writer output requires a named compression codec",
            )),
        }
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
    ) -> Result<IcebergContentRange, ConnectorWriteCodecError> {
        IcebergContentRange::try_new(range.offset, range.size_in_bytes)
            .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergArtifactPartition, ConnectorWriteCodecError> {
        let partition = partition.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg write carrier requires its partition",
            )
        })?;
        let descriptor = partition.descriptor.as_ref().ok_or_else(|| {
            self.missing(
                path.field("descriptor"),
                "an Iceberg artifact partition requires its descriptor",
            )
        })?;
        let values = descriptor
            .values
            .iter()
            .map(|value| IcebergPartitionValueDescriptor {
                is_null: value.is_null,
                datum_bytes: value.datum_bytes.clone(),
            })
            .collect();
        // `IcebergArtifactPartition::try_new` owns the null/datum agreement:
        // repairing it here would move a row into a different partition.
        IcebergArtifactPartition::try_new(
            partition.partition_path.clone(),
            partition.null_fingerprint.clone(),
            partition.partition_spec_id,
            IcebergPartitionDescriptor { values },
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergArtifactMetrics, ConnectorWriteCodecError> {
        let metrics = metrics.ok_or_else(|| {
            self.missing(path.clone(), "an Iceberg artifact requires its metrics")
        })?;
        let column_stats = metrics
            .column_stats
            .as_ref()
            .map(|stats| IcebergColumnStats {
                column_sizes: stats.column_sizes.clone(),
                value_counts: stats.value_counts.clone(),
                null_value_counts: stats.null_value_counts.clone(),
                nan_value_counts: stats.nan_value_counts.clone(),
                lower_bounds: stats.lower_bounds.clone(),
                upper_bounds: stats.upper_bounds.clone(),
            });
        IcebergArtifactMetrics::try_new(
            metrics.record_count,
            metrics.file_size_in_bytes,
            metrics.split_offsets.clone(),
            column_stats,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergStorageRoute, ConnectorWriteCodecError> {
        let route = route.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg old delete reference requires its storage route",
            )
        })?;
        IcebergStorageRoute::try_for_location(&route.access_binding)
            .map_err(|error| self.rejected(path.field("access_binding"), &error))
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
    ) -> Result<IcebergWriteTableFacts, ConnectorWriteCodecError> {
        let table = table.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg writer handle requires its table facts",
            )
        })?;
        let format_version = u8::try_from(table.format_version).map_err(|_| {
            self.invalid(
                path.field("format_version"),
                format!(
                    "Iceberg table format version {} is not a supported version",
                    table.format_version
                ),
            )
        })?;
        IcebergWriteTableFacts::try_new(
            table.table_uuid.clone(),
            table.namespace.clone(),
            table.table_name.clone(),
            table.table_location.clone(),
            table.data_location.clone(),
            table.target_ref.clone(),
            table.base_snapshot_id,
            table.base_sequence_number,
            table.schema_id,
            table.default_partition_spec_id,
            format_version,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergWriterOutput, ConnectorWriteCodecError> {
        let output = output.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg writer handle requires its output settings",
            )
        })?;
        let file_format = self.decode_file_format(output.file_format, path.field("file_format"))?;
        let compression = self.decode_compression(output.compression, path.field("compression"))?;
        // `try_new` owns the rule that a Parquet row-group size belongs only to
        // a Parquet writer: the carrier can state both, and only Iceberg knows
        // the pairing is a contradiction.
        IcebergWriterOutput::try_new(
            file_format,
            compression,
            output.parquet_row_group_size_bytes,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergDataBranchRecipe, ConnectorWriteCodecError> {
        let recipe = recipe.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg data branch requires its data recipe",
            )
        })?;
        let input_schema = recipe
            .input_schema_json
            .as_deref()
            .map(|json| {
                serde_json::from_str::<IcebergSchemaDef>(json).map_err(|error| {
                    self.invalid(
                        path.field("input_schema_json"),
                        format!("decode Iceberg data writer input schema failed: {error}"),
                    )
                })
            })
            .transpose()?;
        IcebergDataBranchRecipe::try_new(
            input_schema,
            recipe.partition_source_column_names.clone(),
            recipe.partition_column_names.clone(),
            recipe.transform_exprs.clone(),
            recipe.row_lineage,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergOldDeleteArtifactRef, ConnectorWriteCodecError> {
        let content_range = reference
            .content_range
            .as_ref()
            .map(|range| self.decode_content_range(range, path.field("content_range")))
            .transpose()?;
        IcebergOldDeleteArtifactRef::try_new(
            reference.path.clone(),
            self.decode_file_content(reference.content, path.field("content"))?,
            self.decode_file_format(reference.file_format, path.field("file_format"))?,
            reference.file_size_in_bytes,
            reference.record_count,
            content_range,
            reference.referenced_data_file.clone(),
            reference.data_sequence_number,
            reference.added_snapshot_id,
            reference.partition_spec_id,
            self.decode_storage_route(
                reference.storage_route.as_ref(),
                path.field("storage_route"),
            )?,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergOldDeleteMergeTarget, ConnectorWriteCodecError> {
        let mut references = Vec::with_capacity(target.references.len());
        for (index, reference) in target.references.iter().enumerate() {
            references
                .push(self.decode_reference(reference, path.field("references").index(index))?);
        }
        // `try_new` owns the target-wide rules — a reference that belongs to
        // another data file, a repeated artifact, a partition spec that
        // disagrees with the data file's — none of which the carrier's shape
        // can express.
        IcebergOldDeleteMergeTarget::try_new(
            target.data_file_path.clone(),
            target.data_file_record_count,
            target.data_file_sequence_number,
            self.decode_partition(target.partition.as_ref(), path.field("partition"))?,
            target.base_snapshot_id,
            references,
        )
        .map_err(|error| self.rejected(path, &error))
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
    ) -> Result<IcebergEqualityDeleteRecipe, ConnectorWriteCodecError> {
        let recipe = recipe.ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg equality-delete branch requires its equality recipe",
            )
        })?;
        let mut columns = Vec::with_capacity(recipe.columns.len());
        for (index, column) in recipe.columns.iter().enumerate() {
            columns.push(
                IcebergEqualityDeleteColumnFacts::try_new(
                    column.name.clone(),
                    column.field_id,
                    column.data_type.clone(),
                    column.nullable,
                )
                .map_err(|error| {
                    self.rejected(path.clone().field("columns").index(index), &error)
                })?,
            );
        }
        IcebergEqualityDeleteRecipe::try_new(columns).map_err(|error| self.rejected(path, &error))
    }

    fn encode_writer_handle_value(
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

    fn decode_writer_handle_value(
        &self,
        iceberg: &dto::IcebergWriterHandle,
    ) -> Result<IcebergWriterHandle, ConnectorWriteCodecError> {
        let path = FieldPath::root("writer_handle").field("iceberg");
        let branch = self.decode_branch(iceberg.branch, path.field("branch"))?;
        let table = self.decode_table(iceberg.table.as_ref(), path.field("table"))?;
        let output = self.decode_output(iceberg.output.as_ref(), path.field("output"))?;
        match branch {
            IcebergWriteBranch::Data => {
                let recipe = self.decode_recipe(iceberg.data.as_ref(), path.field("data"))?;
                // `try_new_data` owns "a data writer produces Parquet".
                IcebergWriterHandle::try_new_data(table, output, recipe)
                    .map_err(|error| self.rejected(path, &error))
            }
            IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => {
                let mut targets = Vec::with_capacity(iceberg.old_deletes.len());
                for (key, target) in &iceberg.old_deletes {
                    targets.push(self.decode_merge_target(
                        target,
                        path.field("old_deletes").map_key(key.clone()),
                    )?);
                }
                // `try_new_delete` owns the branch/format pairing, the frozen
                // base snapshot every target must agree with, and the exclusive
                // ownership of each referenced data file.
                IcebergWriterHandle::try_new_delete(branch, table, output, targets)
                    .map_err(|error| self.rejected(path, &error))
            }
            IcebergWriteBranch::EqualityDelete => {
                let recipe =
                    self.decode_equality_recipe(iceberg.equality.as_ref(), path.field("equality"))?;
                // `try_new_equality_delete` owns "an equality delete writes
                // Parquet and freezes no old-delete reference".
                IcebergWriterHandle::try_new_equality_delete(table, output, recipe)
                    .map_err(|error| self.rejected(path, &error))
            }
        }
    }

    // -- commit fragment ---------------------------------------------------

    fn encode_commit_fragment_value(
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
                        merged_old_references: file.merged_old_references().to_vec(),
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
                        merged_old_references: file.merged_old_references().to_vec(),
                    },
                )
            }
        };
        Ok(dto::IcebergCommitFragment {
            artifact: Some(artifact),
        })
    }

    fn decode_commit_fragment_value(
        &self,
        fragment: &dto::IcebergCommitFragment,
    ) -> Result<IcebergCommitFragment, ConnectorWriteCodecError> {
        let path = FieldPath::root("commit_fragment").field("iceberg");
        let artifact = fragment.artifact.as_ref().ok_or_else(|| {
            self.missing(
                path.clone(),
                "an Iceberg commit fragment describes exactly one artifact",
            )
        })?;
        match artifact {
            dto::iceberg_commit_fragment::Artifact::DataFile(file) => {
                let path = path.field("data_file");
                let artifact = IcebergDataFileArtifact::try_new(
                    file.path.clone(),
                    self.decode_file_format(file.file_format, path.field("file_format"))?,
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.first_row_id,
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::data_file(artifact))
            }
            dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(file) => {
                let path = path.field("position_delete_file");
                let artifact = IcebergPositionDeleteFileArtifact::try_new(
                    file.path.clone(),
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.referenced_data_file.clone(),
                    file.merged_old_references.clone(),
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::position_delete_file(artifact))
            }
            dto::iceberg_commit_fragment::Artifact::EqualityDeleteFile(file) => {
                let path = path.field("equality_delete_file");
                let artifact = IcebergEqualityDeleteFileArtifact::try_new(
                    file.path.clone(),
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.equality_field_ids.clone(),
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::equality_delete_file(artifact))
            }
            dto::iceberg_commit_fragment::Artifact::DeletionVector(file) => {
                let path = path.field("deletion_vector");
                let content_range = file.content_range.as_ref().ok_or_else(|| {
                    self.missing(
                        path.field("content_range"),
                        "an Iceberg deletion vector requires its blob range",
                    )
                })?;
                // `try_new` owns the facts only Iceberg can check: the blob must
                // fit inside its own Puffin file, and the cardinality must be
                // the record count it claims.
                let artifact = IcebergDeletionVectorArtifact::try_new(
                    file.path.clone(),
                    self.decode_partition(file.partition.as_ref(), path.field("partition"))?,
                    self.decode_metrics(file.metrics.as_ref(), path.field("metrics"))?,
                    file.referenced_data_file.clone(),
                    self.decode_content_range(content_range, path.field("content_range"))?,
                    file.cardinality,
                    file.merged_old_references.clone(),
                )
                .map_err(|error| self.rejected(path, &error))?;
                Ok(IcebergCommitFragment::deletion_vector(artifact))
            }
        }
    }
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
        &self.0.owner
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
            spi_codec_error(self.0.rejected(FieldPath::root("writer_handle"), &error))
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
        &self.0.owner
    }

    fn decode_writer_handle_payload(
        &self,
        envelope: &ConnectorEncodedPayload,
    ) -> Result<ConnectorWriterHandle, ConnectorCodecError> {
        let mut ledger = ConnectorDecodeLedger::new(IcebergWriteCodec::decode_limits(
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
        self.0
            .decode_writer_handle_value(&value)
            .map_err(spi_codec_error)
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
        &self.0.owner
    }

    fn encode_commit_fragment_payload(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        let fragment = self.0.adapter.commit_fragment(fragment).map_err(|error| {
            spi_codec_error(self.0.rejected(FieldPath::root("commit_fragment"), &error))
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
        &self.0.owner
    }

    fn decode_commit_fragment_payload(
        &self,
        envelope: &ConnectorEncodedPayload,
    ) -> Result<ConnectorCommitFragment, ConnectorCodecError> {
        let mut ledger = ConnectorDecodeLedger::new(IcebergWriteCodec::decode_limits(
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
        self.0
            .decode_commit_fragment_value(&value)
            .map_err(spi_codec_error)
    }
}

fn spi_codec_error(error: ConnectorWriteCodecError) -> ConnectorCodecError {
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
    };
    ConnectorCodecError::new(path, kind, protocol.detail())
}

#[cfg(test)]
mod tests {
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
        let codec = IcebergWriteCodec::new(adapter("catalog.iceberg", 1));
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
                    "s3://b/wh/db/t/data/old-1.parquet".to_string(),
                    "s3://b/wh/db/t/data/old-2.parquet".to_string(),
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
                vec!["s3://b/wh/db/t/data/a-dv-1.puffin".to_string()],
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
                ConnectorCodecRevision::try_new(2).expect("revision"),
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

        let codec = IcebergWriteCodec::new(adapter("catalog.iceberg", 1));
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
        let codec = IcebergWriteCodec::new(adapter("catalog.iceberg", 1));
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
}
