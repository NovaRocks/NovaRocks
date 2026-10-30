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

//! Original SDK paths and real local close, with only the close reply failed.

use super::*;
use crate::iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use crate::iceberg::{Error, ErrorKind, Result};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct Probe {
    original: FileIO,
    journal: StagedPathJournal,
    close_count: AtomicUsize,
    fail_close: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CloseReplyStorage {
    #[serde(skip)]
    probe: Option<Arc<Probe>>,
}
impl CloseReplyStorage {
    fn probe(&self) -> &Probe {
        self.probe.as_deref().unwrap()
    }
}
#[async_trait::async_trait]
#[typetag::serde]
impl Storage for CloseReplyStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.probe().original.exists(path).await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.probe().original.new_input(path)?.metadata().await
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.probe().original.new_input(path)?.read().await
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.probe().original.new_input(path)?.reader().await
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.probe().original.new_output(path)?.write(bytes).await
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let inner = self.probe().original.new_output(path)?.writer().await?;
        Ok(Box::new(CloseReplyWriter {
            inner,
            probe: self.probe.as_ref().unwrap().clone(),
        }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.probe().original.delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.probe().original.delete_prefix(path).await
    }
    async fn delete_stream(
        &self,
        mut paths: futures::stream::BoxStream<'static, String>,
    ) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.probe().original.new_input(path)
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        assert!(
            self.probe()
                .journal
                .snapshot()
                .iter()
                .any(|original| original == path),
            "exact original path must be stable before SDK output construction"
        );
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
    }
}
#[derive(Debug, Serialize, Deserialize)]
struct CloseReplyFactory {
    #[serde(skip)]
    probe: Option<Arc<Probe>>,
}
#[typetag::serde]
impl StorageFactory for CloseReplyFactory {
    fn build(&self, _: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(CloseReplyStorage {
            probe: self.probe.clone(),
        }))
    }
}
struct CloseReplyWriter {
    inner: Box<dyn FileWrite>,
    probe: Arc<Probe>,
}
#[async_trait::async_trait]
impl FileWrite for CloseReplyWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        self.inner.write(bytes).await
    }
    async fn close(&mut self) -> Result<()> {
        self.inner.close().await?;
        // The same physical file is fully closed first. Only its acknowledgement
        // is now failed; a successful-object/unknown-result frontier is real.
        let count = self.probe.close_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count == self.probe.fail_close {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected close reply loss after actual close",
            ));
        }
        Ok(())
    }
}
fn file_io(
    original: FileIO,
    journal: &StagedPathJournal,
    fail_close: usize,
) -> (FileIO, Arc<Probe>) {
    let probe = Arc::new(Probe {
        original,
        journal: journal.clone(),
        close_count: AtomicUsize::new(0),
        fail_close,
    });
    let io = FileIOBuilder::new(Arc::new(CloseReplyFactory {
        probe: Some(probe.clone()),
    }))
    .build();
    (io, probe)
}
async fn cleanup(io: &FileIO, journal: &StagedPathJournal) {
    while let Some(path) = journal.last() {
        assert!(
            io.exists(&path).await.unwrap(),
            "closed object must still exist before exact cleanup"
        );
        io.delete(&path).await.unwrap();
        journal.confirm_deleted(&path).unwrap();
        assert!(!io.exists(&path).await.unwrap());
    }
}
#[tokio::test]
async fn first_partition_and_unknown_second_close_remain_in_the_original_journal() {
    let table = super::tests::build_local_fs_test_table("journal_partition_close", true).await;
    let mut ctx = StagedWriteContext::from_table(&table).unwrap();
    let journal = StagedPathJournal::default();
    let (io, probe) = file_io(ctx.file_io.clone(), &journal, 2);
    ctx.file_io = io;
    let failure =
        write_record_batches_journaled(&ctx, [super::tests::test_batch(&[0, 1])], &journal).await;
    assert!(failure.is_err());
    assert_eq!(
        probe.close_count.load(Ordering::SeqCst),
        2,
        "first partition actually closed before the second close acknowledgement failed"
    );
    let paths = journal.snapshot();
    assert_eq!(paths.len(), 2);
    assert_ne!(paths[0], paths[1]);
    for path in &paths {
        assert!(
            !ctx.file_io
                .new_input(path)
                .unwrap()
                .read()
                .await
                .unwrap()
                .is_empty()
        );
    }
    cleanup(&ctx.file_io, &journal).await;
    assert!(journal.snapshot().is_empty());
}
#[tokio::test]
async fn equality_delete_unknown_close_keeps_its_actual_generated_path() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let dir = tempfile::tempdir().unwrap();
    let root = format!("file://{}", dir.path().display());
    let journal = StagedPathJournal::default();
    let (io, probe) = file_io(FileIO::new_with_fs(), &journal, 1);
    let schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let target = crate::commit::write_stack::equality_schema::equality_delete_target_from_recipe(
        &crate::commit::write_stack::test_support::equality_delete_recipe(),
        &schema,
    )
    .unwrap();
    let failure = crate::commit::equality_delete_writer::write_equality_delete_file_journaled(
        &io, &root, 0, target, batch, &journal,
    )
    .await;
    assert!(failure.is_err());
    assert_eq!(probe.close_count.load(Ordering::SeqCst), 1);
    assert_eq!(journal.snapshot().len(), 1);
    cleanup(&io, &journal).await;
    assert!(journal.snapshot().is_empty());
}
