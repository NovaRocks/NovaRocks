// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Design: ADR-0235 (docs/adr/ADR-0235-iceberg-write-layout-and-data-plane-capacity.md)

//! One driver's real partition writers, retained across input pages and close waits.
use super::{
    IcebergDataFileWriterBuilder, StagedDataFile, StagedPathJournal, StagedWriteContext,
    StagedWriteProgress, adapt_batch, retag_data_file_partition_spec_id,
    row_group_write_batch_rows,
};
use crate::iceberg::{
    arrow::RecordBatchPartitionSplitter,
    spec::{DataFile, PartitionKey, Struct},
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};
use anyhow::{Context, Result, ensure};
use arrow::record_batch::RecordBatch;
use novarocks_failure_contract::connector_reason::ICEBERG_TOO_MANY_OPEN_PARTITIONS;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts, ResourceScope,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::{collections::HashMap, sync::Arc};

// Trino's default idle-writer check interval and minimum size, measured here
// against the bytes this driver has passed to FileIO.
const IDLE_CHECK_INTERVAL_BYTES: u64 = 256 << 20;
const IDLE_MIN_FILE_BYTES: u64 = 16 << 20;

type DataWriter = <IcebergDataFileWriterBuilder as IcebergWriterBuilder>::R;
type CloseFuture = futures::future::BoxFuture<'static, crate::iceberg::Result<Vec<DataFile>>>;
type Partition = Option<Struct>;

struct CurrentFileProgress {
    path: String,
    progress: StagedWriteProgress,
}

struct PartitionWriter {
    writer: Option<DataWriter>,
    close: Option<CloseFuture>,
    returned: Option<crate::iceberg::Result<Vec<DataFile>>>,
    current_file: Option<CurrentFileProgress>,
    buffered_bytes: u64,
    touched_since_check: bool,
}

impl PartitionWriter {
    fn observe_append(&mut self, journal: &StagedPathJournal) -> Result<()> {
        let writer = self
            .writer
            .as_ref()
            .context("streaming Iceberg partition writer is closed")?;
        // The SDK's current-status methods require an open physical file.
        // This is called only after a successful nonempty SDK write.
        let path = writer.current_file_path();
        if self
            .current_file
            .as_ref()
            .is_none_or(|file| file.path != path)
        {
            let progress = journal
                .write_progress(&path)
                .context("streaming Iceberg file has no staged path write count")?;
            self.current_file = Some(CurrentFileProgress { path, progress });
        }
        let current_file = self.current_file.as_ref().unwrap();
        self.buffered_bytes = (writer.current_written_size() as u64)
            .saturating_sub(current_file.progress.accepted_bytes());
        self.touched_since_check = true;
        Ok(())
    }

    fn accepted_file_bytes(&self) -> u64 {
        self.current_file
            .as_ref()
            .map_or(0, |file| file.progress.accepted_bytes())
    }
}

#[derive(Debug)]
struct TooManyOpenPartitions {
    limit: u32,
    opening: u64,
}

impl std::fmt::Display for TooManyOpenPartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Exceeded limit of {} open writers for partitions: {}",
            self.limit, self.opening
        )
    }
}

impl std::error::Error for TooManyOpenPartitions {}

impl FailureProjection for TooManyOpenPartitions {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Resource, ICEBERG_TOO_MANY_OPEN_PARTITIONS).with_resource(
            ResourceFacts {
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Operator),
                dimension: ResourceDimension::Items,
                requested: Some(self.opening),
                required_growth: Some(1),
                limit: Some(u64::from(self.limit)),
                live: Some(self.opening - 1),
                committed: None,
            },
        )
    }
}

/// The whole source remains in the original BE writer while append/finish is
/// borrowed. Input pages are consumed immediately, never accumulated here.
pub(crate) struct StreamingStagedDataFileWriter {
    context: StagedWriteContext,
    builder: IcebergDataFileWriterBuilder,
    splitter: Option<RecordBatchPartitionSplitter>,
    writers: HashMap<Partition, PartitionWriter>,
    completed: Vec<StagedDataFile>,
    journal: StagedPathJournal,
    write_batch_rows: usize,
    written_since_idle_check: u64,
    pending_idle_closes: Vec<Partition>,
    finishing: bool,
}
impl StreamingStagedDataFileWriter {
    pub(crate) fn new(context: StagedWriteContext, journal: &StagedPathJournal) -> Result<Self> {
        let builder = context
            .data_file_writer_builder(Some(journal))
            .map_err(anyhow::Error::msg)?;
        let splitter = if context.partition_spec().fields().is_empty() {
            None
        } else {
            Some(
                RecordBatchPartitionSplitter::try_new_with_computed_values(
                    context.schema().clone(),
                    context.partition_spec().clone(),
                )
                .context("build streaming Iceberg partition splitter")?,
            )
        };
        let write_batch_rows =
            row_group_write_batch_rows(context.max_row_group_bytes().map_err(anyhow::Error::msg)?);
        Ok(Self {
            context,
            builder,
            splitter,
            writers: HashMap::new(),
            completed: Vec::new(),
            journal: journal.clone(),
            write_batch_rows,
            written_since_idle_check: 0,
            pending_idle_closes: Vec::new(),
            finishing: false,
        })
    }
    pub(crate) async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        ensure!(
            !self.finishing,
            "streaming Iceberg writer is already finishing"
        );
        self.finish_idle_closes().await?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let before = self.journal.total_accepted_bytes();
        let annotated = adapt_batch(&batch, &self.context.annotated_schema)
            .map_err(super::WriteAdaptationError::into_connector_error)?;
        if let Some(splitter) = &self.splitter {
            let partitioned = splitter
                .split(&annotated)
                .context("split streaming Iceberg batch")?;
            for (partition, batch) in partitioned {
                self.write_partition(Some(partition), batch).await?;
            }
        } else {
            self.write_partition(None, annotated).await?;
        }
        let written = self.journal.total_accepted_bytes().wrapping_sub(before);
        self.consider_idle_close(written, IDLE_CHECK_INTERVAL_BYTES, IDLE_MIN_FILE_BYTES);
        self.finish_idle_closes().await
    }

    fn consider_idle_close(&mut self, written: u64, interval: u64, min_file_bytes: u64) {
        self.written_since_idle_check = self.written_since_idle_check.saturating_add(written);
        if self.written_since_idle_check <= interval {
            return;
        }
        for (key, slot) in &mut self.writers {
            if !slot.touched_since_check && slot.accepted_file_bytes() > min_file_bytes {
                self.pending_idle_closes.push(key.clone());
            }
            slot.touched_since_check = false;
        }
        self.written_since_idle_check = 0;
    }

    async fn finish_idle_closes(&mut self) -> Result<()> {
        while let Some(key) = self.pending_idle_closes.last().cloned() {
            self.close_partition(&key).await?;
            self.pending_idle_closes.pop();
        }
        Ok(())
    }

    async fn write_partition(
        &mut self,
        partition: Option<PartitionKey>,
        batch: RecordBatch,
    ) -> Result<()> {
        // This context has exactly one frozen partition spec. Use its actual
        // typed tuple, including NULLs, rather than a lossy path spelling.
        let key = partition.as_ref().map(|partition| partition.data().clone());
        let open = self.writers.len() as u64;
        let limit = self.context.max_partitions_per_writer;
        let slot = match self.writers.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if open >= u64::from(limit) {
                    return Err(ConnectorError::from_domain(
                        ConnectorErrorKind::ResourceExhausted,
                        TooManyOpenPartitions {
                            limit,
                            opening: open + 1,
                        },
                    )
                    .with_public_failure_detail()
                    .into());
                }
                entry.insert(PartitionWriter {
                    writer: Some(
                        self.builder
                            .build(partition)
                            .await
                            .context("open streaming Iceberg partition writer")?,
                    ),
                    close: None,
                    returned: None,
                    current_file: None,
                    buffered_bytes: 0,
                    touched_since_check: false,
                })
            }
        };
        let writer = slot
            .writer
            .as_mut()
            .context("streaming Iceberg partition writer is closed")?;
        for offset in (0..batch.num_rows()).step_by(self.write_batch_rows.min(batch.num_rows())) {
            let length = (batch.num_rows() - offset).min(self.write_batch_rows);
            writer
                .write(batch.slice(offset, length))
                .await
                .context("write streaming Iceberg partition page")?;
        }
        slot.observe_append(&self.journal)
    }

    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.writers
            .values()
            .fold(0, |bytes, slot| bytes.saturating_add(slot.buffered_bytes))
    }

    async fn close_partition(&mut self, key: &Partition) -> Result<()> {
        let slot = self
            .writers
            .get_mut(key)
            .context("missing Iceberg partition writer")?;
        if slot.returned.is_none() {
            if slot.close.is_none() {
                let mut writer = slot
                    .writer
                    .take()
                    .context("streaming Iceberg partition close lost its original writer")?;
                slot.close = Some(Box::pin(async move { writer.close().await }));
            }
            // A cancelled borrower leaves this exact close future installed
            // in the same slot. Receive its files before dropping the future.
            slot.returned = Some(slot.close.as_mut().unwrap().as_mut().await);
            drop(slot.close.take());
        }
        slot.buffered_bytes = 0;
        if slot.returned.as_ref().unwrap().is_err() {
            return Err(slot.returned.take().unwrap().unwrap_err())
                .context("close streaming Iceberg partition writer");
        }
        let files = slot.returned.as_mut().unwrap().as_mut().unwrap();
        while let Some(file) = files.pop() {
            self.completed.push(StagedDataFile {
                data_file: retag_data_file_partition_spec_id(
                    file,
                    self.context.partition_spec_id(),
                )
                .map_err(anyhow::Error::msg)?,
                metadata: Arc::clone(&self.context.metadata),
                partition_spec_id: self.context.partition_spec_id(),
            });
        }
        self.writers.remove(key);
        Ok(())
    }

    pub(crate) async fn finish(&mut self) -> Result<Vec<StagedDataFile>> {
        self.finishing = true;
        self.finish_idle_closes().await?;
        while !self.writers.is_empty() {
            let key = self.writers.keys().next().unwrap().clone();
            self.close_partition(&key).await?;
        }
        Ok(std::mem::take(&mut self.completed))
    }

    /// Destroy live SDK handles first, so managed stream Drop can deposit its
    /// original abort F in the same journal before the actor polls cleanup.
    pub(crate) fn close_for_abort(&mut self) {
        self.finishing = true;
        for slot in self.writers.values_mut() {
            drop((slot.writer.take(), slot.close.take()));
            slot.buffered_bytes = 0;
        }
    }
    /// Only the original cleanup's positive close/deletion permits retirement
    /// of completed file facts. A failed cleanup keeps this same inventory.
    pub(crate) fn complete_abort(&mut self) {
        self.writers.clear();
        self.completed.clear();
        self.pending_idle_closes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::data_writer::{
        PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY,
        tests::{build_local_fs_test_table, local_test_binding, test_batch},
    };

    fn tracked_context(
        table: &crate::iceberg::table::Table,
        journal: &StagedPathJournal,
        row_group_bytes: Option<u64>,
    ) -> StagedWriteContext {
        let mut context = StagedWriteContext::from_table(table).unwrap();
        context.file_io =
            crate::fs_io::build_tracked_staged_file_io(local_test_binding(), journal.clone());
        if let Some(row_group_bytes) = row_group_bytes {
            let properties = HashMap::from([(
                PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_owned(),
                row_group_bytes.to_string(),
            )]);
            context.metadata = Arc::new(
                context
                    .metadata()
                    .clone()
                    .into_builder(None)
                    .set_properties(properties)
                    .unwrap()
                    .build()
                    .unwrap()
                    .metadata,
            );
        }
        context
    }

    #[tokio::test]
    async fn rolling_at_one_byte_writes_one_file_per_page_and_counts_accepted_bytes() {
        let table = build_local_fs_test_table("stream_target_size", false).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, None)
            .with_frozen_file_shaping(1, 100)
            .unwrap();
        let io = context.file_io().clone();
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        for value in [4, 7, 8] {
            writer.append(test_batch(&[value])).await.unwrap();
        }
        let files = writer.finish().await.unwrap();
        assert_eq!(files.len(), 3);
        for file in files {
            assert_eq!(file.data_file.record_count(), 1);
            let path = file.data_file.file_path();
            let stored = io.new_input(path).unwrap().metadata().await.unwrap().size;
            assert_eq!(file.data_file.file_size_in_bytes(), stored);
            assert_eq!(
                journal.write_progress(path).unwrap().accepted_bytes(),
                stored
            );
        }
    }

    #[tokio::test]
    async fn idle_check_closes_only_an_untouched_file_above_minimum_and_can_reopen_it() {
        let table = build_local_fs_test_table("stream_idle", true).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, Some(1));
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        // A flushed row group can still be in Parquet's buffered output.
        // Each burst contains enough one-row groups to reach FileIO.
        const ROWS_PER_BURST: usize = 1024;
        let mut initial = vec![1; ROWS_PER_BURST];
        initial.resize(2 * ROWS_PER_BURST, 2);
        let active = test_batch(&[2; ROWS_PER_BURST]);
        writer.append(test_batch(&initial)).await.unwrap();
        let written = writer.written_since_idle_check;
        assert!(written > 0);

        // Drive the same idle selection with a small interval and minimum.
        // An equal interval does not trigger the first check.
        writer.consider_idle_close(0, written, 0);
        assert!(writer.writers.values().all(|slot| slot.touched_since_check));
        writer.consider_idle_close(0, written - 1, 0);
        assert!(
            writer
                .writers
                .values()
                .all(|slot| !slot.touched_since_check)
        );
        assert!(writer.pending_idle_closes.is_empty());

        writer.append(active.clone()).await.unwrap();
        let idle = writer
            .writers
            .values()
            .find(|slot| !slot.touched_since_check)
            .unwrap();
        let idle_bytes = idle.accepted_file_bytes();
        assert!(idle_bytes > 0);
        writer.consider_idle_close(0, 0, idle_bytes);
        assert!(writer.pending_idle_closes.is_empty());

        writer.append(active).await.unwrap();
        writer.consider_idle_close(0, 0, idle_bytes - 1);
        assert_eq!(writer.pending_idle_closes.len(), 1);
        writer.finish_idle_closes().await.unwrap();
        assert_eq!(writer.writers.len(), 1);
        assert_eq!(writer.completed.len(), 1);
        let idle_file = &writer.completed[0].data_file;
        assert_eq!(idle_file.record_count(), ROWS_PER_BURST as u64);
        assert_eq!(
            journal
                .write_progress(idle_file.file_path())
                .unwrap()
                .accepted_bytes(),
            idle_file.file_size_in_bytes()
        );

        writer.append(test_batch(&[1])).await.unwrap();
        assert_eq!(writer.writers.len(), 2);
        let mut rows = writer
            .finish()
            .await
            .unwrap()
            .into_iter()
            .map(|file| file.data_file.record_count())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(rows, [1, ROWS_PER_BURST as u64, 3 * ROWS_PER_BURST as u64]);
        assert_eq!(writer.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn row_group_flush_releases_buffered_bytes_and_idle_check_counts_actual_writes() {
        let table = build_local_fs_test_table("stream_buffered_size", false).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, Some(512));
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        writer.append(test_batch(&[0])).await.unwrap();
        assert_eq!(writer.written_since_idle_check, 0);
        assert_eq!(journal.total_accepted_bytes(), 0);
        assert!(writer.buffered_bytes() > 0);
        let mut buffered = writer.buffered_bytes();
        // Span multiple row groups so a flush also drains the SDK's
        // buffered output into the original FileIO writer.
        for value in 1..4096 {
            writer.append(test_batch(&[value])).await.unwrap();
            let slot = writer.writers.values().next().unwrap();
            let written = slot.accepted_file_bytes();
            let sdk_bytes = slot.writer.as_ref().unwrap().current_written_size() as u64;
            assert_eq!(writer.buffered_bytes(), sdk_bytes - written);
            if written > 0 {
                assert!(writer.buffered_bytes() < buffered);
                assert_eq!(writer.written_since_idle_check, written);
                break;
            }
            buffered = writer.buffered_bytes();
        }
        assert!(
            journal.total_accepted_bytes() > 0,
            "a row group has flushed"
        );
        let files = writer.finish().await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(
            files[0]
                .data_file
                .split_offsets()
                .is_some_and(|groups| groups.len() > 1)
        );
        assert_eq!(writer.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn three_input_pages_use_one_real_unpartitioned_file() {
        let table = build_local_fs_test_table("stream_three_pages", false).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, None);
        let io = context.file_io().clone();
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        for ids in [&[1][..], &[2][..], &[3][..]] {
            writer.append(test_batch(ids)).await.unwrap();
        }
        assert_eq!(
            journal.snapshot().len(),
            1,
            "all pages retain the same original object path"
        );
        let files = writer.finish().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].data_file.record_count(), 3);
        assert!(io.exists(files[0].data_file.file_path()).await.unwrap());
    }
    #[tokio::test]
    async fn interleaved_pages_keep_one_real_writer_per_partition() {
        let table = build_local_fs_test_table("stream_interleaved_partitions", true).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, None);
        let io = context.file_io().clone();
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        for ids in [&[0, 1][..], &[0][..], &[1, 0][..]] {
            writer.append(test_batch(ids)).await.unwrap();
        }
        let files = writer.finish().await.unwrap();
        assert_eq!(files.len(), 2);
        let mut counts = files
            .iter()
            .map(|file| file.data_file.record_count())
            .collect::<Vec<_>>();
        counts.sort_unstable();
        assert_eq!(counts, vec![2, 3]);
        for file in &files {
            assert!(io.exists(file.data_file.file_path()).await.unwrap());
        }
    }
    #[tokio::test]
    async fn dropping_a_close_waiter_keeps_the_original_sdk_close_until_resumed() {
        use futures::FutureExt;
        let table = build_local_fs_test_table("stream_close_custody", false).await;
        let journal = StagedPathJournal::default();
        let context = tracked_context(&table, &journal, None);
        let mut writer = StreamingStagedDataFileWriter::new(context, &journal).unwrap();
        writer.append(test_batch(&[1, 2, 3])).await.unwrap();
        let gate = Arc::new(tokio::sync::Notify::new());
        let slot = writer.writers.values_mut().next().unwrap();
        let mut original = slot.writer.take().unwrap();
        let waited = gate.clone();
        slot.close = Some(Box::pin(async move {
            waited.notified().await;
            original.close().await
        }));
        writer.pending_idle_closes.push(None);
        assert!(writer.append(test_batch(&[])).now_or_never().is_none());
        assert!(writer.writers.values().next().unwrap().close.is_some());
        assert_eq!(writer.pending_idle_closes, [None]);
        assert!(writer.writers.values().next().unwrap().returned.is_none());
        assert_eq!(journal.snapshot().len(), 1);
        gate.notify_one();
        let files = writer.finish().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].data_file.record_count(), 3);
    }
    #[tokio::test]
    async fn empty_input_opens_no_object_and_cannot_be_appended_after_finish() {
        let table = build_local_fs_test_table("stream_empty", false).await;
        let journal = StagedPathJournal::default();
        let mut writer =
            StreamingStagedDataFileWriter::new(tracked_context(&table, &journal, None), &journal)
                .unwrap();
        writer.append(test_batch(&[])).await.unwrap();
        assert!(writer.finish().await.unwrap().is_empty());
        assert!(journal.snapshot().is_empty());
        assert!(writer.append(test_batch(&[1])).await.is_err());
    }
}
