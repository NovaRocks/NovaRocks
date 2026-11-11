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

//! Backend-local Iceberg writers for the provider-neutral write stack.
//!
//! One pipeline driver owns one writer for its whole lifetime. A writer stages
//! artifacts and describes each one as an independent commit fragment; it has
//! no catalog client and no commit capability at all.
//!
//! Mutation writers materialize against their fixed read endpoint. A maintenance
//! writer repacks positions from the already selected input scan.

mod cleanup;
mod materialization;
use cleanup::UnsealedWriterCleanup;

use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriteExecutionFactory, ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorRequestContext,
};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{
    StagedDataFile, StagedPathJournal, StagedWriteContext, staged_data_file_to_writer_report,
    write_record_batches_journaled,
};
use crate::commit::frozen_write::{FrozenDataWriteFacts, staged_write_context_with_file_io};
use crate::commit::report::{IcebergPartitionReport, partition_path_from_struct};
use crate::commit::write_io::{build_staged_file_io, build_staged_file_io_with_creation};
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitFragment,
    IcebergDataFileArtifact, IcebergEqualityDeleteFileArtifact, IcebergWriteBranch,
    IcebergWriterHandle,
};
use crate::commit::write_stack::equality_schema::equality_delete_target_from_recipe;
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::delete_file::IcebergFileFormat;
use crate::write_descriptor::encode_partition_descriptor;

fn error(kind: ConnectorErrorKind, message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(kind, message.into())
}

/// The backend write capability of one exact catalog generation.
pub struct IcebergWriteStackExecution {
    catalog_handle: CatalogHandle,
    adapter: IcebergWriteAdapter,
    binding: IcebergReadBinding,
}

impl IcebergWriteStackExecution {
    pub(crate) fn new(
        catalog_handle: CatalogHandle,
        adapter: IcebergWriteAdapter,
        binding: IcebergReadBinding,
    ) -> Self {
        Self {
            catalog_handle,
            adapter,
            binding,
        }
    }
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for IcebergWriteStackExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        // The adapter is the only door back to the provider domain, and it is
        // bound to this exact generation, so a handle minted by any other
        // generation cannot open a writer here.
        let handle = self.adapter.writer_handle(&request.handle)?.clone();
        if handle.table().default_partition_spec_id() < 0 {
            return Err(error(
                ConnectorErrorKind::CorruptData,
                "Iceberg writer handle carries a negative partition spec id",
            ));
        }
        match handle.branch() {
            IcebergWriteBranch::Data => Ok(Box::new(IcebergDataStackWriter::open(
                self, handle, request,
            )?)),
            IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => {
                Ok(Box::new(materialization::MaterializedDeleteWriter::open(
                    self, handle, request,
                )?))
            }
            IcebergWriteBranch::EqualityDelete => Ok(Box::new(
                IcebergEqualityDeleteStackWriter::open(self, handle, request)?,
            )),
        }
    }
}

/// Startup-sealed factory for the catalog-keyed backend write capability.
///
/// The adapter is minted here, from the exact catalog generation the properties
/// name, so a backend can never open a writer for a handle produced by another
/// generation.
#[derive(Clone)]
pub struct IcebergWriteStackExecutionFactory {
    descriptor: ConnectorInstanceDescriptor,
    binding: IcebergReadBinding,
}

impl IcebergWriteStackExecutionFactory {
    pub fn new(descriptor: ConnectorInstanceDescriptor, binding: IcebergReadBinding) -> Self {
        Self {
            descriptor,
            binding,
        }
    }
}

impl ConnectorWriteExecutionFactory for IcebergWriteStackExecutionFactory {
    fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn ConnectorWriteExecution>, ConnectorError> {
        let catalog_handle = properties.handle().clone();
        if self.descriptor.instance_id != *catalog_handle.catalog_name() {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg write execution factory does not own this catalog",
            ));
        }
        let binding = self.binding.bind_catalog(properties)?;
        let adapter = crate::commit::write_stack::runtime::build_write_adapter(
            self.descriptor.clone(),
            catalog_handle.clone(),
        );
        Ok(Arc::new(IcebergWriteStackExecution::new(
            catalog_handle,
            adapter,
            binding,
        )))
    }
}

/// Attempt-local output naming.
///
/// The physical context is used for naming, logging, and failure localization
/// only. It never enters a commit fragment's identity, so replaying an attempt
/// under a new physical context cannot change what was or was not committed.
fn staged_path(
    handle: &IcebergWriterHandle,
    physical: &ConnectorWriterPhysicalContext,
    partition_path: &str,
    sequence: u64,
    extension: &str,
) -> String {
    let base = format!(
        "{}/_staging/{}",
        handle.table().data_location().trim_end_matches('/'),
        uuid::Uuid::from_bytes(physical.execution_query_id())
    );
    let name = format!(
        "{}-{}-{:08x}-{:08x}-{:016x}.{extension}",
        handle.branch().as_str(),
        uuid::Uuid::from_bytes(physical.task_key()),
        physical.driver_id(),
        physical.writer_ordinal(),
        sequence
    );
    let partition = partition_path.trim_matches('/');
    if partition.is_empty() {
        format!("{base}/{name}")
    } else {
        format!("{base}/{partition}/{name}")
    }
}

/// Local deletion authority ends at successful sealing, before any fragment
/// escapes. A failed or interrupted finish remains owned and abortable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriterLifecycle {
    Live,
    Finishing,
    Sealed,
    Aborting,
    Aborted,
}
impl WriterLifecycle {
    fn can_abort(self) -> bool {
        matches!(self, Self::Live | Self::Finishing | Self::Aborting)
    }
}

fn ensure_live(
    lifecycle: WriterLifecycle,
    context: &ConnectorRequestContext,
    what: &str,
) -> Result<(), ConnectorError> {
    if lifecycle != WriterLifecycle::Live {
        return Err(error(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg {what} writer is already terminal"),
        ));
    }
    if context.is_cancelled() {
        return Err(error(
            ConnectorErrorKind::Cancelled,
            format!("Iceberg {what} writer was cancelled"),
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(error(
            ConnectorErrorKind::DeadlineExceeded,
            format!("Iceberg {what} writer deadline elapsed"),
        ));
    }
    Ok(())
}

fn managed_writer_io(
    binding: &IcebergReadBinding,
    handle: &IcebergWriterHandle,
    physical: ConnectorWriterPhysicalContext,
) -> Result<(crate::iceberg::io::FileIO, StagedPathJournal), ConnectorError> {
    let location = handle.table().data_location();
    if handle.creation_scope().is_some() {
        let journal = StagedPathJournal::managed();
        let file_io =
            build_staged_file_io_with_creation(binding, location, handle, physical, &journal)?;
        Ok((file_io, journal))
    } else {
        let file_io = build_staged_file_io(binding, location)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok((file_io, StagedPathJournal::default()))
    }
}

fn with_creation_receipt(
    fragment: IcebergCommitFragment,
    journal: &StagedPathJournal,
) -> Result<IcebergCommitFragment, ConnectorError> {
    let Some(path) = fragment.new_object_path() else {
        return Ok(fragment);
    };
    match journal
        .receipt(path)
        .map_err(|message| error(ConnectorErrorKind::CorruptData, message))?
    {
        Some(receipt) => fragment.try_with_creation(receipt),
        None => Ok(fragment),
    }
}

fn stage_position_error(cause: anyhow::Error) -> ConnectorError {
    if let Some(original) = cause
        .chain()
        .find_map(|error| error.downcast_ref::<ConnectorError>())
    {
        return original.clone();
    }
    error(
        ConnectorErrorKind::Internal,
        format!("Stage Iceberg position artifact: {cause:#}"),
    )
}

/// The per-driver data-file writer.
struct IcebergDataStackWriter {
    adapter: IcebergWriteAdapter,
    context: StagedWriteContext,
    request_context: ConnectorRequestContext,
    fragments: Vec<ConnectorCommitFragment>,
    staged_paths: StagedPathJournal,
    cleanup: UnsealedWriterCleanup,
    lifecycle: WriterLifecycle,
}

impl IcebergDataStackWriter {
    fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        let recipe = handle.data().ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "Iceberg data writer handle carries no data branch recipe",
            )
        })?;
        let target_schema_json =
            recipe
                .target_schema_json()
                .map(str::to_owned)
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::InvalidRequest,
                        "Iceberg data writer handle carries no frozen input schema",
                    )
                })?;
        let binding = execution.binding.for_request(request.context.clone());
        let facts = FrozenDataWriteFacts {
            format_version: handle.table().format_version(),
            table_location: handle.table().table_location().to_string(),
            data_location: handle.table().data_location().to_string(),
            target_partition_spec_id: handle.table().default_partition_spec_id(),
            partition_source_column_names: recipe.partition_source_column_names().to_vec(),
            partition_column_names: recipe.partition_column_names().to_vec(),
            transform_exprs: recipe.transform_exprs().to_vec(),
            target_schema_json,
            parquet_row_group_size_bytes: handle.output().parquet_row_group_size_bytes(),
        };
        let (file_io, staged_paths) = managed_writer_io(&binding, &handle, request.physical)?;
        let context = staged_write_context_with_file_io(&request.expected_schema, facts, file_io)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let cleanup = UnsealedWriterCleanup::new(
            binding,
            request.context.clone(),
            handle.table().data_location(),
        );
        Ok(Self {
            adapter: execution.adapter.clone(),
            context,
            request_context: request.context,
            fragments: Vec::new(),
            staged_paths,
            cleanup,
            lifecycle: WriterLifecycle::Live,
        })
    }

    fn record(&mut self, staged: Vec<StagedDataFile>) -> Result<(), ConnectorError> {
        for file in staged {
            let metadata = file.metadata.clone();
            let partition_spec = metadata
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg staged data file references unknown partition spec {}",
                            file.partition_spec_id
                        ),
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(file.data_file.partition(), partition_spec)
                    .map_err(|message| error(ConnectorErrorKind::CorruptData, message))?;
            let descriptor = encode_partition_descriptor(
                file.data_file.partition(),
                file.partition_spec_id,
                metadata.as_ref(),
            )
            .map_err(|descriptor_error| {
                error(
                    ConnectorErrorKind::CorruptData,
                    descriptor_error.detail_message(),
                )
            })?;
            let partition = IcebergArtifactPartition::try_new(
                partition_path.clone(),
                null_fingerprint.clone(),
                file.partition_spec_id,
                descriptor,
            )?;
            let report = staged_data_file_to_writer_report(
                &file,
                IcebergPartitionReport {
                    partition_path,
                    null_fingerprint,
                    partition_spec_id: file.partition_spec_id,
                    partition_values: file.data_file.partition().clone(),
                },
                "parquet".to_string(),
                crate::delete_file::IcebergFileContent::Data,
            )
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            let metrics = IcebergArtifactMetrics::try_new(
                u64::try_from(report.file.record_count).map_err(|_| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg staged data file record count is negative",
                    )
                })?,
                u64::try_from(report.file.file_size_in_bytes).map_err(|_| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg staged data file size is negative",
                    )
                })?,
                report.file.split_offsets.clone().unwrap_or_default(),
                report.file.column_stats.clone(),
            )?;
            let artifact = IcebergDataFileArtifact::try_new(
                report.file.path.clone(),
                IcebergFileFormat::Parquet,
                partition,
                metrics,
                report.file.first_row_id,
            )?;
            let fragment = with_creation_receipt(
                IcebergCommitFragment::data_file(artifact),
                &self.staged_paths,
            )?;
            self.fragments
                .push(self.adapter.wrap_commit_fragment(fragment));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for IcebergDataStackWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        ensure_live(self.lifecycle, &self.request_context, "data")?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let staged = write_record_batches_journaled(&self.context, [batch], &self.staged_paths)
            .await
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        self.record(staged)
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        ensure_live(self.lifecycle, &self.request_context, "data")?;
        self.lifecycle = WriterLifecycle::Sealed;
        // A writer that staged nothing returns an empty vector; that is a
        // legal outcome and is not a failure.
        Ok(std::mem::take(&mut self.fragments))
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        if !self.lifecycle.can_abort() {
            return Ok(());
        }
        self.lifecycle = WriterLifecycle::Aborting;
        // The same unsealed journal activates the Provider's existing cleanup
        // phase once; Task stop still forbids append/finish work.
        self.cleanup.run(&self.staged_paths).await?;
        self.fragments.clear();
        self.lifecycle = WriterLifecycle::Aborted;
        Ok(())
    }
}

/// The per-driver equality-delete writer.
///
/// It is a separate writer from the position-delete one because the two share
/// nothing but the word "delete": this one reads no old artifact, owns no data
/// file, and stages a Parquet file whose rows *are* the match key. Its Arrow
/// types come from the fragment's own frozen input schema; the handle carries
/// only the field ids and the type rendering the frontend signed, and a
/// disagreement between them is a refusal rather than a coercion.
struct IcebergEqualityDeleteStackWriter {
    adapter: IcebergWriteAdapter,
    handle: IcebergWriterHandle,
    request_context: ConnectorRequestContext,
    file_io: crate::iceberg::io::FileIO,
    staging_dir: String,
    target: super::equality_schema::EqualityDeleteFileTarget,
    equality_field_ids: Vec<i32>,
    partition: IcebergArtifactPartition,
    fragments: Vec<ConnectorCommitFragment>,
    staged_paths: StagedPathJournal,
    cleanup: UnsealedWriterCleanup,
    lifecycle: WriterLifecycle,
}

impl IcebergEqualityDeleteStackWriter {
    fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        let recipe = handle.equality().ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "Iceberg equality-delete writer handle carries no equality recipe",
            )
        })?;
        let target = equality_delete_target_from_recipe(recipe, &request.expected_schema)?;
        let equality_field_ids = recipe.field_ids();
        let binding = execution.binding.for_request(request.context.clone());
        let (file_io, staged_paths) = managed_writer_io(&binding, &handle, request.physical)?;
        // One directory per query attempt, the same shape every other writer
        // stages into, so an abort has one prefix to clean.
        let staging_dir = format!(
            "{}/_staging/{}",
            handle.table().data_location().trim_end_matches('/'),
            uuid::Uuid::from_bytes(request.physical.execution_query_id())
        );
        // An equality delete is admitted only on an unpartitioned table, so its
        // artifact carries the empty partition of the table's default spec. The
        // frontend still decodes that descriptor against the real metadata at
        // commit, which is what makes the emptiness checked rather than assumed.
        let partition = IcebergArtifactPartition::try_new(
            String::new(),
            String::new(),
            handle.table().default_partition_spec_id(),
            crate::write_descriptor::IcebergPartitionDescriptor { values: Vec::new() },
        )?;
        let cleanup = UnsealedWriterCleanup::new(
            binding,
            request.context.clone(),
            handle.table().data_location(),
        );
        Ok(Self {
            adapter: execution.adapter.clone(),
            handle,
            request_context: request.context,
            file_io,
            staging_dir,
            target,
            equality_field_ids,
            partition,
            fragments: Vec::new(),
            staged_paths,
            cleanup,
            lifecycle: WriterLifecycle::Live,
        })
    }

    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        self.cleanup.run(&self.staged_paths).await
    }
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for IcebergEqualityDeleteStackWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        ensure_live(self.lifecycle, &self.request_context, "equality-delete")?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let written = crate::commit::equality_delete_writer::write_equality_delete_file_journaled(
            &self.file_io,
            &self.staging_dir,
            self.handle.table().default_partition_spec_id(),
            self.target.clone(),
            batch,
            &self.staged_paths,
        )
        .await
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        let Some(written) = written else {
            return Ok(());
        };
        let metrics = IcebergArtifactMetrics::try_new(
            written.record_count,
            written.file_size_in_bytes,
            Vec::new(),
            None,
        )?;
        let artifact = IcebergEqualityDeleteFileArtifact::try_new(
            written.path,
            self.partition.clone(),
            metrics,
            self.equality_field_ids.clone(),
        )?;
        let fragment = with_creation_receipt(
            IcebergCommitFragment::equality_delete_file(artifact),
            &self.staged_paths,
        )?;
        self.fragments
            .push(self.adapter.wrap_commit_fragment(fragment));
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        ensure_live(self.lifecycle, &self.request_context, "equality-delete")?;
        self.lifecycle = WriterLifecycle::Sealed;
        Ok(std::mem::take(&mut self.fragments))
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        if !self.lifecycle.can_abort() {
            return Ok(());
        }
        self.lifecycle = WriterLifecycle::Aborting;
        self.cleanup().await?;
        self.fragments.clear();
        self.lifecycle = WriterLifecycle::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod seal_tests;
