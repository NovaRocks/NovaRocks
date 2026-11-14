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

//! One driver's real partition writers, retained across input pages and close waits.
use super::{
    IcebergDataFileWriterBuilder, StagedDataFile, StagedPathJournal, StagedWriteContext,
    adapt_batch, parquet_row_group_size_bytes, retag_data_file_partition_spec_id,
    row_group_write_batch_rows,
};
use crate::iceberg::{
    arrow::RecordBatchPartitionSplitter,
    spec::{DataFile, PartitionKey, Struct},
    writer::{IcebergWriter, IcebergWriterBuilder},
};
use anyhow::{Context, Result, ensure};
use arrow::record_batch::RecordBatch;
use std::{collections::HashMap, sync::Arc};

type DataWriter = <IcebergDataFileWriterBuilder as IcebergWriterBuilder>::R;
type CloseFuture = futures::future::BoxFuture<'static, crate::iceberg::Result<Vec<DataFile>>>;
struct PartitionWriter {
    writer: Option<DataWriter>,
    close: Option<CloseFuture>,
    returned: Option<crate::iceberg::Result<Vec<DataFile>>>,
}

/// The whole source remains in the original BE writer while append/finish is
/// borrowed. Input pages are consumed immediately, never accumulated here.
pub(crate) struct StreamingStagedDataFileWriter {
    context: StagedWriteContext,
    builder: IcebergDataFileWriterBuilder,
    splitter: Option<RecordBatchPartitionSplitter>,
    writers: HashMap<Option<Struct>, PartitionWriter>,
    completed: Vec<StagedDataFile>,
    write_batch_rows: usize,
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
        let write_batch_rows = parquet_row_group_size_bytes(context.metadata.properties())
            .map_err(anyhow::Error::msg)?
            .map(row_group_write_batch_rows)
            .unwrap_or(usize::MAX);
        Ok(Self {
            context,
            builder,
            splitter,
            writers: HashMap::new(),
            completed: Vec::new(),
            write_batch_rows,
            finishing: false,
        })
    }
    pub(crate) async fn append(&mut self, batch: RecordBatch) -> Result<()> {
        ensure!(
            !self.finishing,
            "streaming Iceberg writer is already finishing"
        );
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let annotated =
            adapt_batch(&batch, &self.context.annotated_schema).map_err(anyhow::Error::msg)?;
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
        let slot = match self.writers.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(PartitionWriter {
                writer: Some(
                    self.builder
                        .build(partition)
                        .await
                        .context("open streaming Iceberg partition writer")?,
                ),
                close: None,
                returned: None,
            }),
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
        Ok(())
    }
    pub(crate) async fn finish(&mut self) -> Result<Vec<StagedDataFile>> {
        self.finishing = true;
        for slot in self.writers.values_mut() {
            if slot.returned.is_none() {
                if slot.close.is_none() {
                    let mut writer = slot
                        .writer
                        .take()
                        .context("streaming Iceberg partition close lost its original writer")?;
                    slot.close = Some(Box::pin(async move { writer.close().await }));
                }
                // A cancelled borrower leaves this exact close F installed in
                // the same slot. Completed file facts are received before F Drop.
                slot.returned = Some(slot.close.as_mut().unwrap().as_mut().await);
                drop(slot.close.take());
            }
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
        }
        Ok(std::mem::take(&mut self.completed))
    }
    /// Destroy live SDK handles first, so managed stream Drop can deposit its
    /// original abort F in the same journal before the actor polls cleanup.
    pub(crate) fn close_for_abort(&mut self) {
        self.finishing = true;
        for slot in self.writers.values_mut() {
            drop((slot.writer.take(), slot.close.take()));
        }
    }
    /// Only the original cleanup's positive close/deletion permits retirement
    /// of completed file facts. A failed cleanup keeps this same inventory.
    pub(crate) fn complete_abort(&mut self) {
        self.writers.clear();
        self.completed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::data_writer::tests::{build_local_fs_test_table, test_batch};

    #[tokio::test]
    async fn three_input_pages_use_one_real_unpartitioned_file() {
        let table = build_local_fs_test_table("stream_three_pages", false).await;
        let context = StagedWriteContext::from_table(&table).unwrap();
        let io = context.file_io().clone();
        let journal = StagedPathJournal::default();
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
        let context = StagedWriteContext::from_table(&table).unwrap();
        let io = context.file_io().clone();
        let journal = StagedPathJournal::default();
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
        let context = StagedWriteContext::from_table(&table).unwrap();
        let journal = StagedPathJournal::default();
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
        assert!(writer.finish().now_or_never().is_none());
        assert!(writer.writers.values().next().unwrap().close.is_some());
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
        let mut writer = StreamingStagedDataFileWriter::new(
            StagedWriteContext::from_table(&table).unwrap(),
            &journal,
        )
        .unwrap();
        writer.append(test_batch(&[])).await.unwrap();
        assert!(writer.finish().await.unwrap().is_empty());
        assert!(journal.snapshot().is_empty());
        assert!(writer.append(test_batch(&[1])).await.is_err());
    }
}
