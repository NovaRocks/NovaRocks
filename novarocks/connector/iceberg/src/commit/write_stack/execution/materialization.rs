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

//! Original file positions become final effects as each authenticated author
//! finishes the contiguous shards of that file.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorPositionShardSource, ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionResources, ConnectorOperationControl,
    ConnectorPayloadRetentionGuard, ConnectorRequestContext, ConnectorResourceClass,
    ConnectorResourceReservation,
};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::StagedPathJournal;
use crate::commit::model::EntryIdentity;
use crate::commit::position_delete_writer::{
    PositionDeleteGroup, write_position_delete_files_journaled,
};
use crate::commit::puffin_dv::{DeletionVector, StagedPuffinBlob, StreamingPuffinDv};
use crate::commit::report::partition_path_from_struct;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitFragment, IcebergContentRange,
    IcebergDeleteTargetLocator, IcebergDeletionVectorArtifact, IcebergFinalDeleteEffect,
    IcebergFinalDeleteKind, IcebergPositionDeleteFileArtifact, IcebergWriteBranch,
    IcebergWriterHandle,
};
use crate::commit::write_stack::old_delete::ScopedOldPositions;
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::delete_semantics::TypedPartition;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{FormatVersion, Struct, TableMetadata};
use crate::manifest::partition_types::resolved_partition_values;
use crate::manifest::target_query::ManifestReadScope;
use crate::typed_read::schema_binding::MutationMetadataSchema;
use crate::write_descriptor::encode_partition_descriptor_with_type;

use super::{
    IcebergWriteStackExecution, UnsealedWriterCleanup, WriterLifecycle, ensure_live, error,
    managed_writer_io, stage_position_error, staged_path, with_creation_receipt,
};

type Author = ([u8; 16], u32);

const MAX_PORTABLE_SHARD_BYTES: usize = 8208;

struct OpenPositions {
    file: String,
    partition: TypedPartition,
    next: u64,
    count: u64,
    last_window: Option<(u32, u16)>,
    positions: DeletionVector,
    charge: ConnectorResourceReservation,
}

struct V2File {
    partition: TypedPartition,
    values: Struct,
    positions: RoaringTreemap,
    charge: ConnectorResourceReservation,
}

struct PuffinArtifact {
    data_file: String,
    partition: IcebergArtifactPartition,
    mutation: Option<(IcebergDeleteTargetLocator, u64)>,
    exclusive: Vec<EntryIdentity>,
}

struct PendingPuffinEffect {
    artifact: PuffinArtifact,
    blob: StagedPuffinBlob,
    charge: ConnectorResourceReservation,
}

struct RetainedEffect {
    fragment: IcebergCommitFragment,
    charge: ConnectorResourceReservation,
}

struct PuffinOutput {
    path: String,
    writer: StreamingPuffinDv,
    effects: Vec<PendingPuffinEffect>,
}

struct ReadEndpoint {
    scope: ManifestReadScope,
    partition_schema: MutationMetadataSchema,
}

pub(super) struct MaterializedDeleteWriter {
    adapter: IcebergWriteAdapter,
    handle: IcebergWriterHandle,
    physical: ConnectorWriterPhysicalContext,
    context: ConnectorRequestContext,
    input_schema: SchemaRef,
    resources: ConnectorExecutionResources,
    binding: IcebergReadBinding,
    read_io: FileIO,
    file_io: FileIO,
    endpoint: Option<ReadEndpoint>,
    old_positions: ScopedOldPositions,
    journal: StagedPathJournal,
    cleanup: UnsealedWriterCleanup,
    authors: BTreeMap<Author, OpenPositions>,
    files: BTreeSet<String>,
    file_id_charge: Option<ConnectorResourceReservation>,
    v2: BTreeMap<String, V2File>,
    rewrite: Option<(DeletionVector, ConnectorResourceReservation)>,
    puffin: Option<PuffinOutput>,
    next_puffin: u64,
    effects: Vec<RetainedEffect>,
    lifecycle: WriterLifecycle,
}

impl MaterializedDeleteWriter {
    pub(super) fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        if handle.mutation_read().is_some() == handle.position_rewrite().is_some() {
            return Err(invalid(
                "Iceberg position writer requires exactly one position input recipe",
            ));
        }
        let resources = request.resources.ok_or_else(|| {
            error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg position writer has no original task resource ledger",
            )
        })?;
        if handle.creation_scope().is_none() {
            return Err(error(
                ConnectorErrorKind::CorruptData,
                "Iceberg position writer has no original creation scope",
            ));
        }
        let binding = execution.binding.for_request(request.context.clone());
        let (file_io, journal) = managed_writer_io(&binding, &handle, request.physical)?;
        let cleanup = UnsealedWriterCleanup::new(
            binding.clone(),
            request.context.clone(),
            handle.table().data_location(),
        );
        let read_io = crate::fs_io::build_file_io_for_location(
            handle.table().table_location(),
            binding.clone(),
        );
        Ok(Self {
            adapter: execution.adapter.clone(),
            handle,
            physical: request.physical,
            context: request.context,
            input_schema: request.expected_schema,
            old_positions: ScopedOldPositions::new(resources.clone()),
            resources,
            binding,
            read_io,
            file_io,
            endpoint: None,
            journal,
            cleanup,
            authors: BTreeMap::new(),
            files: BTreeSet::new(),
            file_id_charge: None,
            v2: BTreeMap::new(),
            rewrite: None,
            puffin: None,
            next_puffin: 0,
            effects: Vec::new(),
            lifecycle: WriterLifecycle::Live,
        })
    }

    fn live(&self) -> Result<(), ConnectorError> {
        ensure_live(self.lifecycle, &self.context, self.handle.branch().as_str())
    }

    fn check_schema(&self, batch: &RecordBatch, fields: usize) -> Result<(), ConnectorError> {
        if batch.num_columns() != fields || batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err(invalid(
                "Iceberg position input differs from its sealed schema",
            ));
        }
        Ok(())
    }

    async fn load_endpoint(&mut self) -> Result<(), ConnectorError> {
        if self.endpoint.is_some() {
            return Ok(());
        }
        let recipe = self
            .handle
            .mutation_read()
            .ok_or_else(|| corrupt("Iceberg position writer omitted its fixed read recipe"))?;
        let snapshot_id = self.handle.table().base_snapshot_id().ok_or_else(|| {
            corrupt("Iceberg position writer received rows for an empty fixed endpoint")
        })?;
        let bytes = self
            .read_io
            .new_input(recipe.metadata_location())
            .map_err(read_error)?
            .read()
            .await
            .map_err(read_error)?;
        self.context.check_active()?;
        let metadata = parse_metadata_file(&bytes)?;
        recipe.validate_loaded(self.handle.table(), &metadata)?;
        let expected_format = match self.handle.branch() {
            IcebergWriteBranch::PositionDelete => FormatVersion::V2,
            IcebergWriteBranch::DeletionVector => FormatVersion::V3,
            _ => {
                return Err(corrupt(
                    "Fixed position writer opened for a non-position branch",
                ));
            }
        };
        if metadata.format_version() != expected_format {
            return Err(corrupt(
                "Fixed Iceberg position recipe has the wrong format version",
            ));
        }
        let scope = ManifestReadScope::new(Arc::new(metadata), snapshot_id)
            .map_err(read_error)?
            .with_writer_resources(self.resources.clone(), self.binding.clone());
        let projection = crate::commit::write_shared::position_mutation_read_schema(
            scope.metadata(),
            self.handle.table().target_ref(),
            Some(snapshot_id),
        )?;
        let partition_schema = MutationMetadataSchema::try_new(scope.metadata(), &projection)?;
        self.endpoint = Some(ReadEndpoint {
            scope,
            partition_schema,
        });
        Ok(())
    }

    fn partition(
        &self,
        spec_id: i32,
        column: &StructArray,
        row: usize,
    ) -> Result<(TypedPartition, Struct), ConnectorError> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| corrupt("Fixed endpoint is unavailable"))?;
        let spec = endpoint
            .scope
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| corrupt(format!("Unknown Iceberg mutation partition spec {spec_id}")))?;
        let historical_type = endpoint
            .scope
            .domain()
            .endpoint()
            .partition_type(spec_id)
            .map_err(|cause| corrupt(cause.to_string()))?;
        let values =
            endpoint
                .partition_schema
                .decode_partition(spec, &historical_type, column, row)?;
        let typed = TypedPartition::bind_type(spec, &historical_type, &values)
            .map_err(|cause| corrupt(cause.to_string()))?;
        Ok((typed, values))
    }

    fn reserve(&mut self, bytes: u64) -> Result<ConnectorResourceReservation, ConnectorError> {
        match self
            .resources
            .try_reserve(ConnectorResourceClass::WriterState, bytes)
        {
            Err(cause) if cause.kind() == ConnectorErrorKind::ResourceExhausted => {
                self.evict_caches();
                self.resources
                    .try_reserve(ConnectorResourceClass::WriterState, bytes)
            }
            result => result,
        }
    }

    fn grow(
        &mut self,
        charge: &mut ConnectorResourceReservation,
        bytes: u64,
    ) -> Result<(), ConnectorError> {
        match charge.try_grow(bytes) {
            Err(cause) if cause.kind() == ConnectorErrorKind::ResourceExhausted => {
                self.evict_caches();
                charge.try_grow(bytes)
            }
            result => result,
        }
    }

    fn evict_caches(&mut self) {
        self.old_positions.evict();
        if let Some(endpoint) = &mut self.endpoint {
            endpoint.scope.evict();
        }
    }

    fn accept_shard(
        &mut self,
        source: ConnectorPositionShardSource,
        columns: &ShardColumns<'_>,
        row: usize,
    ) -> Result<Option<OpenPositions>, ConnectorError> {
        let file = columns.file.value(row);
        let (partition, _) = self.partition(columns.spec.value(row), columns.partition, row)?;
        let driver = u32::try_from(columns.driver.value(row))
            .ok()
            .filter(|driver| *driver < source.driver_count().get())
            .ok_or_else(|| {
                invalid("Iceberg position shard driver is outside its authenticated roster")
            })?;
        let high = u32::try_from(columns.high.value(row))
            .map_err(|_| invalid("Iceberg position shard high word is negative"))?;
        let container = u16::try_from(columns.container.value(row))
            .map_err(|_| invalid("Iceberg position shard container key is out of range"))?;
        let window = (high, container);
        let index = u64::try_from(columns.index.value(row))
            .map_err(|_| invalid("Iceberg position shard index is negative"))?;
        let count = u64::try_from(columns.count.value(row))
            .ok()
            .filter(|count| index < *count)
            .ok_or_else(|| invalid("Iceberg position shard count is invalid"))?;
        let author = (source.task_key(), driver);
        let bytes = columns.bitmap.value(row);
        if bytes.len() > MAX_PORTABLE_SHARD_BYTES {
            return Err(invalid(
                "Iceberg position shard exceeds one Roaring container",
            ));
        }
        let _decode =
            self.reserve(bytes.len() as u64 + std::mem::size_of::<RoaringBitmap>() as u64)?;
        let bitmap = decode_container(window, bytes)?;
        let backing = bitmap_backing_bytes(&bitmap);
        let mut open = match self.authors.remove(&author) {
            Some(open) => open,
            None => {
                if index != 0 || self.files.contains(file) {
                    return Err(invalid(
                        "Iceberg position shard repeats a file or omits its first window",
                    ));
                }
                let identity_bytes = (std::mem::size_of::<String>() + file.len()) as u64;
                self.file_id_charge = Some(match self.file_id_charge.take() {
                    Some(mut receipt) => {
                        self.grow(&mut receipt, identity_bytes)?;
                        receipt
                    }
                    None => self.reserve(identity_bytes)?,
                });
                self.files.insert(file.to_owned());
                OpenPositions {
                    file: file.to_owned(),
                    partition: partition.clone(),
                    next: 0,
                    count,
                    last_window: None,
                    positions: DeletionVector::new(),
                    charge: self.reserve(
                        (std::mem::size_of::<OpenPositions>()
                            + file.len()
                            + partition.to_json_string().len()) as u64,
                    )?,
                }
            }
        };
        if open.file != file
            || open.partition != partition
            || open.next != index
            || open.count != count
            || open.last_window.is_some_and(|last| last >= window)
        {
            return Err(invalid(
                "Iceberg position shards disagree on metadata or contiguous window order",
            ));
        }
        self.grow(&mut open.charge, backing)?;
        open.positions
            .merge_owned_bitmap(high, bitmap)
            .map_err(|cause| corrupt(cause.to_string()))?;
        open.next += 1;
        open.last_window = Some(window);
        if open.next == count {
            Ok(Some(open))
        } else {
            self.authors.insert(author, open);
            Ok(None)
        }
    }

    async fn materialize(&mut self, mut file: OpenPositions) -> Result<(), ConnectorError> {
        let endpoint = self
            .endpoint
            .as_mut()
            .ok_or_else(|| corrupt("Fixed endpoint is unavailable"))?;
        let context = &self.context;
        let observation = endpoint
            .scope
            .observe(
                BTreeMap::from([(file.file.clone(), file.partition)]),
                &self.read_io,
                || {
                    context.check_active().map_err(|cause| {
                        crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::Unexpected,
                            "Iceberg writer stopped",
                        )
                        .with_source(cause)
                    })
                },
            )
            .await
            .map_err(|cause| {
                context
                    .check_active()
                    .err()
                    .unwrap_or_else(|| read_error(cause))
            })?;
        let target = observation.files.get(&file.file).ok_or_else(|| {
            corrupt("Completed Iceberg target observation omitted its declared file")
        })?;
        let locator = target
            .data_locator(&observation.completion)
            .map_err(read_error)?;
        let record_count = locator.record_count();
        if file
            .positions
            .maximum_position()
            .is_none_or(|position| position >= record_count)
        {
            return Err(corrupt(
                "New Iceberg positions fall outside the fixed data file",
            ));
        }
        let new_rows = file.positions.cardinality();
        let old = self
            .old_positions
            .read(target, &self.binding, &self.context)
            .await?;
        let exclusive = old.merged_references().to_vec();
        let old = old.into_positions();
        let old_bytes = old
            .bitmaps()
            .map(|(_, bitmap)| bitmap_backing_bytes(bitmap))
            .fold(0, u64::saturating_add);
        let _old_and_union = self.reserve(
            old_bytes
                .saturating_mul(2)
                .saturating_add(std::mem::size_of::<RoaringTreemap>() as u64),
        )?;
        file.positions
            .merge_disjoint_positions(&old)
            .map_err(|cause| corrupt(cause.to_string()))?;
        drop(old);
        let frozen = target.data.frozen.facts();
        let partition = self.artifact_partition(frozen.partition_spec_id, &frozen.partition)?;
        let effect_bytes = effect_backing_bytes(&locator, &partition, &exclusive);
        if file.positions.cardinality() == record_count {
            let charge = self.reserve(effect_bytes)?;
            let effect = IcebergFinalDeleteEffect::try_new(
                locator,
                new_rows,
                IcebergFinalDeleteKind::RemoveDataFile {
                    partition,
                    old_exclusive_deletes: exclusive.into_boxed_slice(),
                },
            )?;
            self.effects.push(RetainedEffect {
                fragment: IcebergCommitFragment::final_delete(effect),
                charge,
            });
            return Ok(());
        }
        self.append_puffin(
            &file.positions,
            PuffinArtifact {
                data_file: file.file,
                partition,
                mutation: Some((locator, new_rows)),
                exclusive,
            },
            effect_bytes
                .saturating_add(std::mem::size_of::<IcebergDeletionVectorArtifact>() as u64),
        )
        .await
    }

    async fn append_puffin(
        &mut self,
        positions: &DeletionVector,
        artifact: PuffinArtifact,
        effect_bytes: u64,
    ) -> Result<(), ConnectorError> {
        let _payload = self.reserve(
            positions
                .payload_size()
                .map_err(|cause| corrupt(cause.to_string()))?,
        )?;
        if let Some(puffin) = &self.puffin
            && !puffin
                .writer
                .fits(&artifact.data_file, positions)
                .map_err(stage_position_error)?
        {
            self.close_puffin().await?;
        }
        if self.puffin.is_none() {
            // A container may carry different target partitions. Each
            // manifest entry retains its own partition and byte range.
            let sequence = self.next_puffin;
            self.next_puffin = sequence
                .checked_add(1)
                .ok_or_else(|| corrupt("Puffin sequence overflow"))?;
            let path = staged_path(&self.handle, &self.physical, "", sequence, "puffin");
            self.journal.record(&path);
            let writer = StreamingPuffinDv::open(&self.file_io, &path)
                .await
                .map_err(stage_position_error)?;
            self.puffin = Some(PuffinOutput {
                path,
                writer,
                effects: Vec::new(),
            });
        }
        let path_bytes = self
            .puffin
            .as_ref()
            .map_or(0, |puffin| puffin.path.len() as u64);
        let charge = self.reserve(effect_bytes.saturating_add(path_bytes))?;
        let puffin = self
            .puffin
            .as_mut()
            .ok_or_else(|| corrupt("Open Puffin is unavailable"))?;
        let blob = puffin
            .writer
            .append(&artifact.data_file, positions)
            .await
            .map_err(stage_position_error)?;
        puffin.effects.push(PendingPuffinEffect {
            artifact,
            blob,
            charge,
        });
        Ok(())
    }

    fn artifact_partition(
        &self,
        spec_id: i32,
        values: &Struct,
    ) -> Result<IcebergArtifactPartition, ConnectorError> {
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| corrupt("Fixed endpoint is unavailable"))?;
        let spec = endpoint
            .scope
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| corrupt("Historical Iceberg partition spec is unavailable"))?;
        let historical_type = endpoint
            .scope
            .domain()
            .endpoint()
            .partition_type(spec_id)
            .map_err(|cause| corrupt(cause.to_string()))?;
        let values = resolved_partition_values(values, &historical_type);
        let descriptor = encode_partition_descriptor_with_type(&values, &historical_type)
            .map_err(|cause| corrupt(cause.to_string()))?;
        let (path, nulls) = partition_path_from_struct(&values, spec).map_err(corrupt)?;
        IcebergArtifactPartition::try_new(path, nulls, spec_id, descriptor)
    }

    async fn close_puffin(&mut self) -> Result<(), ConnectorError> {
        let Some(puffin) = self.puffin.take() else {
            return Ok(());
        };
        let size = puffin.writer.finish().await.map_err(stage_position_error)?;
        for pending in puffin.effects {
            self.context.check_active()?;
            let metrics =
                IcebergArtifactMetrics::try_new(pending.blob.cardinality, size, Vec::new(), None)?;
            let artifact = IcebergDeletionVectorArtifact::try_new(
                puffin.path.clone(),
                pending.artifact.partition,
                metrics,
                pending.artifact.data_file,
                IcebergContentRange::try_new(pending.blob.offset, pending.blob.length)?,
                pending.blob.cardinality,
                pending.artifact.exclusive,
            )?;
            let fragment = match pending.artifact.mutation {
                Some((target, new_rows)) => {
                    IcebergCommitFragment::final_delete(IcebergFinalDeleteEffect::try_new(
                        target,
                        new_rows,
                        IcebergFinalDeleteKind::ReplacePositions(Box::new(artifact)),
                    )?)
                }
                None => IcebergCommitFragment::deletion_vector(artifact),
            };
            let fragment = with_creation_receipt(fragment, &self.journal)?;
            self.effects.push(RetainedEffect {
                fragment,
                charge: pending.charge,
            });
        }
        Ok(())
    }

    fn append_rewrite(&mut self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        self.check_schema(batch, 2)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.rewrite.is_none() {
            self.rewrite = Some((
                DeletionVector::new(),
                self.reserve(std::mem::size_of::<DeletionVector>() as u64)?,
            ));
        }
        let target = self
            .handle
            .position_rewrite()
            .ok_or_else(|| corrupt("Iceberg position rewrite selection is unavailable"))?;
        let (positions, charge) = self
            .rewrite
            .as_mut()
            .ok_or_else(|| corrupt("Iceberg position rewrite state is unavailable"))?;
        let files: &StringArray = required(batch, 0)?;
        let rows: &Int64Array = required(batch, 1)?;
        for row in 0..batch.num_rows() {
            self.context.check_active()?;
            if files.value(row) != target.path() {
                return Err(invalid(
                    "Iceberg position rewrite row names an unselected data file",
                ));
            }
            let position = u64::try_from(rows.value(row))
                .ok()
                .filter(|position| *position < target.record_count())
                .ok_or_else(|| {
                    invalid("Iceberg position rewrite row is outside the selected data file")
                })?;
            if !positions.contains(position) {
                charge.try_grow(std::mem::size_of::<u64>() as u64)?;
                positions.insert(position).map_err(stage_position_error)?;
            }
        }
        Ok(())
    }

    async fn finish_rewrite(&mut self) -> Result<(), ConnectorError> {
        let Some((positions, _charge)) = self.rewrite.take() else {
            return Ok(());
        };
        let target = self
            .handle
            .position_rewrite()
            .ok_or_else(|| corrupt("Iceberg position rewrite selection is unavailable"))?;
        let artifact = PuffinArtifact {
            data_file: target.path().to_owned(),
            partition: target.partition().clone(),
            mutation: None,
            exclusive: Vec::new(),
        };
        let bytes = artifact_partition_bytes(&artifact.partition)
            .saturating_add(artifact.data_file.len() as u64)
            .saturating_add(std::mem::size_of::<PendingPuffinEffect>() as u64);
        self.append_puffin(&positions, artifact, bytes).await
    }

    async fn finish_v2(&mut self) -> Result<(), ConnectorError> {
        for (ordinal, (file, state)) in std::mem::take(&mut self.v2).into_iter().enumerate() {
            self.context.check_active()?;
            let spec_id = state.partition.spec_id();
            let partition = self.artifact_partition(spec_id, &state.values)?;
            let path = staged_path(
                &self.handle,
                &self.physical,
                partition.partition_path(),
                u64::try_from(ordinal).map_err(|_| corrupt("Iceberg file ordinal overflow"))?,
                "d",
            );
            let directory = path
                .rsplit_once('/')
                .map_or(path.as_str(), |(parent, _)| parent);
            let positions = state
                .positions
                .into_iter()
                .map(|position| {
                    i64::try_from(position)
                        .map_err(|_| corrupt("Position-delete position exceeds i64"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let files = write_position_delete_files_journaled(
                &self.file_io,
                directory,
                vec![PositionDeleteGroup {
                    referenced_data_file: file.clone(),
                    partition_spec_id: spec_id,
                    partition_values: state.values,
                    positions,
                }],
                &self.journal,
            )
            .await
            .map_err(stage_position_error)?;
            let [written] = files.as_slice() else {
                return Err(corrupt(
                    "Iceberg position writer staged an unexpected file count",
                ));
            };
            let metrics = IcebergArtifactMetrics::try_new(
                written.record_count,
                written.file_size_in_bytes,
                Vec::new(),
                None,
            )?;
            let file_bytes = file.len() as u64;
            let artifact = IcebergPositionDeleteFileArtifact::try_new(
                written.path.clone(),
                partition,
                metrics,
                file,
                Vec::new(),
            )?;
            let bytes = (std::mem::size_of::<IcebergCommitFragment>() as u64)
                .saturating_add(written.path.len() as u64)
                .saturating_add(file_bytes)
                .saturating_add(artifact_partition_bytes(artifact.partition()));
            let charge = self.reserve(bytes)?;
            let fragment = with_creation_receipt(
                IcebergCommitFragment::position_delete_file(artifact),
                &self.journal,
            )?;
            self.effects.push(RetainedEffect { fragment, charge });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for MaterializedDeleteWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.live()?;
        if self.handle.position_rewrite().is_some() {
            return self.append_rewrite(&batch);
        }
        if self.handle.branch() != IcebergWriteBranch::PositionDelete {
            return Err(invalid(
                "Iceberg DV writer requires authenticated position shards",
            ));
        }
        self.check_schema(&batch, 4)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.load_endpoint().await?;
        let files: &StringArray = required(&batch, 0)?;
        let positions: &Int64Array = required(&batch, 1)?;
        let specs: &Int32Array = required(&batch, 2)?;
        let partitions: &StructArray = required(&batch, 3)?;
        for row in 0..batch.num_rows() {
            self.context.check_active()?;
            let file = files.value(row);
            let position = u64::try_from(positions.value(row))
                .map_err(|_| invalid("Iceberg delete position must be nonnegative"))?;
            let (partition, values) = self.partition(specs.value(row), partitions, row)?;
            if !self.v2.contains_key(file) {
                let charge = self.reserve(
                    (std::mem::size_of::<V2File>() + file.len() + partition.to_json_string().len())
                        as u64,
                )?;
                self.v2.insert(
                    file.to_owned(),
                    V2File {
                        partition: partition.clone(),
                        values,
                        positions: RoaringTreemap::new(),
                        charge,
                    },
                );
            }
            let pending = self
                .v2
                .get_mut(file)
                .ok_or_else(|| corrupt("Iceberg v2 file state is unavailable"))?;
            if pending.partition != partition {
                return Err(invalid(
                    "Iceberg v2 rows disagree on a file's historical partition",
                ));
            }
            if !pending.positions.contains(position) {
                pending.charge.try_grow(std::mem::size_of::<u64>() as u64)?;
                pending.positions.insert(position);
            }
        }
        Ok(())
    }

    async fn append_position_shards(
        &mut self,
        source: ConnectorPositionShardSource,
        batch: RecordBatch,
    ) -> Result<(), ConnectorError> {
        self.live()?;
        if self.handle.branch() != IcebergWriteBranch::DeletionVector
            || self.handle.position_group() != Some(source.group())
        {
            return Err(invalid(
                "Iceberg position shard source differs from its sealed group",
            ));
        }
        self.check_schema(&batch, 9)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.load_endpoint().await?;
        let columns = ShardColumns::read(&batch)?;
        for row in 0..batch.num_rows() {
            self.context.check_active()?;
            if let Some(file) = self.accept_shard(source, &columns, row)? {
                self.materialize(file).await?;
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        self.live()?;
        self.lifecycle = WriterLifecycle::Finishing;
        if !self.authors.is_empty() {
            return Err(invalid(
                "Iceberg position input ended with an incomplete shard sequence",
            ));
        }
        self.finish_v2().await?;
        self.finish_rewrite().await?;
        self.close_puffin().await?;
        self.context.check_active()?;
        self.lifecycle = WriterLifecycle::Sealed;
        Ok(std::mem::take(&mut self.effects)
            .into_iter()
            .map(|effect| {
                self.adapter.wrap_retained_commit_fragment(
                    effect.fragment,
                    ConnectorPayloadRetentionGuard::new(Mutex::new(effect.charge)),
                )
            })
            .collect())
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        if !self.lifecycle.can_abort() {
            return Ok(());
        }
        self.lifecycle = WriterLifecycle::Aborting;
        self.puffin.take();
        self.rewrite.take();
        self.authors.clear();
        self.v2.clear();
        self.files.clear();
        self.file_id_charge.take();
        self.effects.clear();
        self.evict_caches();
        self.cleanup.run(&self.journal).await?;
        self.lifecycle = WriterLifecycle::Aborted;
        Ok(())
    }
}

struct ShardColumns<'a> {
    file: &'a StringArray,
    spec: &'a Int32Array,
    partition: &'a StructArray,
    driver: &'a Int64Array,
    high: &'a Int32Array,
    container: &'a Int32Array,
    index: &'a Int64Array,
    count: &'a Int64Array,
    bitmap: &'a BinaryArray,
}

impl<'a> ShardColumns<'a> {
    fn read(batch: &'a RecordBatch) -> Result<Self, ConnectorError> {
        Ok(Self {
            file: required(batch, 0)?,
            spec: required(batch, 1)?,
            partition: required(batch, 2)?,
            driver: required(batch, 3)?,
            high: required(batch, 4)?,
            container: required(batch, 5)?,
            index: required(batch, 6)?,
            count: required(batch, 7)?,
            bitmap: required(batch, 8)?,
        })
    }
}

fn required<A: Array + 'static>(batch: &RecordBatch, ordinal: usize) -> Result<&A, ConnectorError> {
    batch
        .column(ordinal)
        .as_any()
        .downcast_ref::<A>()
        .filter(|column| column.null_count() == 0)
        .ok_or_else(|| {
            invalid(format!(
                "Iceberg position column {ordinal} has an invalid type or NULL"
            ))
        })
}

fn decode_container(window: (u32, u16), bytes: &[u8]) -> Result<RoaringBitmap, ConnectorError> {
    // Standard portable serialization for one container has at most 8 KiB of
    // bitmap data and 16 bytes of header, keys and offsets.
    if bytes.len() > MAX_PORTABLE_SHARD_BYTES {
        return Err(invalid(
            "Iceberg position shard exceeds one Roaring container",
        ));
    }
    // Check the portable count before the decoder allocates its container
    // table. The two cookies encode a plain count or the run count minus one.
    const NO_RUN_COOKIE: u32 = 12346;
    const ONE_CONTAINER_RUN_COOKIE: u32 = 12347;
    let cookie = bytes
        .get(..4)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_le_bytes);
    let one_container = match cookie {
        Some(NO_RUN_COOKIE) => bytes.get(4..8) == Some(&1u32.to_le_bytes()),
        Some(ONE_CONTAINER_RUN_COOKIE) => true,
        _ => false,
    };
    if !one_container {
        return Err(invalid(
            "Iceberg position shard must encode exactly one Roaring container",
        ));
    }
    let mut cursor = Cursor::new(bytes);
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
        .map_err(|cause| invalid(format!("Invalid Iceberg position shard bitmap: {cause}")))?;
    let container = u32::from(window.1);
    if cursor.position() != bytes.len() as u64
        || bitmap
            .min()
            .is_none_or(|position| position >> 16 != container)
        || bitmap
            .max()
            .is_none_or(|position| position >> 16 != container)
    {
        return Err(invalid(
            "Iceberg position shard bitmap differs from its declared window",
        ));
    }
    Ok(bitmap)
}

fn effect_backing_bytes(
    target: &IcebergDeleteTargetLocator,
    partition: &IcebergArtifactPartition,
    exclusive: &[EntryIdentity],
) -> u64 {
    let bytes = (std::mem::size_of::<RetainedEffect>() as u64)
        .saturating_add(target.manifest_list().len() as u64)
        .saturating_add(target.manifest_path().len() as u64)
        .saturating_add(target.data_file_path().len() as u64)
        .saturating_add(artifact_partition_bytes(partition));
    exclusive.iter().fold(bytes, |size, entry| {
        let data_file = match entry {
            EntryIdentity::DeletionVector {
                referenced_data_file,
                ..
            } => referenced_data_file.len(),
            _ => 0,
        };
        size.saturating_add(std::mem::size_of::<EntryIdentity>() as u64)
            .saturating_add(entry.path().len() as u64)
            .saturating_add(data_file as u64)
    })
}

fn artifact_partition_bytes(partition: &IcebergArtifactPartition) -> u64 {
    let values = &partition.descriptor().values;
    let fixed = values.capacity() as u64
        * std::mem::size_of::<crate::write_descriptor::IcebergPartitionValueDescriptor>() as u64;
    values.iter().fold(
        fixed
            .saturating_add(partition.partition_path().len() as u64)
            .saturating_add(partition.null_fingerprint().len() as u64),
        |size, value| {
            size.saturating_add(value.datum_bytes.as_ref().map_or(0, Vec::capacity) as u64)
        },
    )
}

fn bitmap_backing_bytes(bitmap: &RoaringBitmap) -> u64 {
    let stats = bitmap.statistics();
    stats
        .n_bytes_array_containers
        .saturating_add(stats.n_bytes_run_containers)
        .saturating_add(stats.n_bytes_bitset_containers)
        .saturating_add(std::mem::size_of::<(u32, RoaringBitmap)>() as u64)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    error(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    error(ConnectorErrorKind::CorruptData, message)
}

fn parse_metadata_file(bytes: &[u8]) -> Result<TableMetadata, ConnectorError> {
    let mut decompressed = Vec::new();
    let document = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut reader = flate2::read::GzDecoder::new(bytes);
        std::io::Read::read_to_end(&mut reader, &mut decompressed)
            .map_err(|cause| corrupt(format!("Invalid gzip Iceberg mutation metadata: {cause}")))?;
        decompressed.as_slice()
    } else {
        bytes
    };
    crate::metadata_json::from_slice_document(
        document,
        crate::metadata_json::Document::TableMetadata,
    )
    .map_err(|cause| {
        error(
            cause.connector_kind(ConnectorErrorKind::CorruptData),
            format!("Invalid fixed Iceberg mutation metadata: {cause}"),
        )
    })
}

fn read_error(cause: crate::iceberg::Error) -> ConnectorError {
    let mut source = std::error::Error::source(&cause);
    while let Some(original) = source {
        if let Some(connector) = original.downcast_ref::<ConnectorError>() {
            return connector.clone();
        }
        source = original.source();
    }
    crate::catalog::error::map_read_error(&cause)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialized(positions: &[u32]) -> Vec<u8> {
        let bitmap: RoaringBitmap = positions.iter().copied().collect();
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn decode_windows_keeps_real_low_bits_and_refuses_an_extra_container_or_tail() {
        let mut vector = DeletionVector::new();
        let bytes = serialized(&[0x1_0000, 0x1_4321, 0x1_ffff]);
        let bitmap = decode_container((1, 1), &bytes).unwrap();
        vector.merge_owned_bitmap(1, bitmap).unwrap();
        let bytes = serialized(&[0xffff_fffe]);
        let bitmap = decode_container((0, u16::MAX), &bytes).unwrap();
        vector.merge_owned_bitmap(0, bitmap).unwrap();
        assert_eq!(
            vector.to_roaring_treemap().into_iter().collect::<Vec<_>>(),
            [0xffff_fffe, 0x1_0001_0000, 0x1_0001_4321, 0x1_0001_ffff],
        );
        assert!(decode_container((1, 1), &serialized(&[0xffff, 0x1_0000])).is_err());
        let mut bytes = serialized(&[0x1_4321]);
        bytes.push(0);
        assert!(decode_container((1, 1), &bytes).is_err());
        assert!(decode_container((1, 1), &serialized(&[])).is_err());
    }

    #[test]
    fn final_union_rejects_an_already_deleted_visible_row() {
        let mut vector = DeletionVector::new();
        vector
            .merge_owned_bitmap(0, RoaringBitmap::from([2, 6]))
            .unwrap();
        assert!(
            vector
                .merge_disjoint_positions(&RoaringTreemap::from([2, 5]))
                .is_err()
        );
        assert_eq!(vector.cardinality(), 2);
        vector
            .merge_disjoint_positions(&RoaringTreemap::from([1, 5]))
            .unwrap();
        assert_eq!(
            vector.to_roaring_treemap().into_iter().collect::<Vec<_>>(),
            [1, 2, 5, 6]
        );
    }
}
