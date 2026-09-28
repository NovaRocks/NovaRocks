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

//! One supervised range read with separate result and actual-exit receipts.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crate::{
    BoundFile, FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult, FileTask,
    FileTaskSpawner,
};

/// A completed result does not release the spawned operation's ownership.
/// The caller must also await `drained` or `stop_and_drain` before releasing a
/// request slot or a buffer reservation associated with that operation.
pub struct FileRangeOperation {
    cancellation: FileCancellation,
    result: Option<oneshot::Receiver<FileResult<Bytes>>>,
    task: FileTask,
}

impl FileRangeOperation {
    pub fn start(
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        spawner: &Arc<dyn FileTaskSpawner>,
    ) -> FileResult<Self> {
        cancellation.check()?;
        let cancellation = cancellation.child();
        let (sender, result) = oneshot::channel();
        let operation_cancellation = cancellation.clone();
        let task = spawner.spawn(Box::pin(async move {
            let outcome = file.read(range, &operation_cancellation).await;
            let _ = sender.send(outcome);
        }))?;
        Ok(Self {
            cancellation,
            result: Some(result),
            task,
        })
    }

    /// Fill a caller-sized final backing without allocating a second complete
    /// range result. A failed or short stream never publishes the target.
    pub fn start_into(
        file: BoundFile,
        range: FileReadRange,
        target: BytesMut,
        cancellation: FileCancellation,
        spawner: &Arc<dyn FileTaskSpawner>,
    ) -> FileResult<Self> {
        Self::start_segmented_into(file, range, target, usize::MAX, cancellation, spawner)
    }

    /// Fill one final backing through serial bounded segments. Every segment
    /// owns a distinct slice, so no completed whole-range buffer is copied.
    pub fn start_segmented_into(
        file: BoundFile,
        range: FileReadRange,
        mut target: BytesMut,
        max_segment_bytes: usize,
        cancellation: FileCancellation,
        spawner: &Arc<dyn FileTaskSpawner>,
    ) -> FileResult<Self> {
        cancellation.check()?;
        if max_segment_bytes == 0 {
            return Err(FileError::invalid("range segment limit must be nonzero"));
        }
        let (offset, expected) = match range {
            FileReadRange::WholeFile => (0, file.identity().file_size()),
            FileReadRange::Bounded { offset, length } => (offset, length),
        };
        let end = offset
            .checked_add(expected)
            .ok_or_else(|| FileError::invalid("range end overflows"))?;
        if end > file.identity().file_size() {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                "range exceeds bound file length",
            ));
        }
        let target_len = u64::try_from(target.len())
            .map_err(|_| FileError::invalid("range target length overflows"))?;
        if expected != target_len {
            return Err(FileError::invalid(
                "range target length differs from the requested range",
            ));
        }
        let cancellation = cancellation.child();
        let (sender, result) = oneshot::channel();
        let operation_cancellation = cancellation.clone();
        let task = spawner.spawn(Box::pin(async move {
            let outcome = async {
                let mut filled = 0usize;
                while filled < target.len() {
                    let remaining = target.len() - filled;
                    let segment_len = remaining.min(max_segment_bytes);
                    let next = filled + segment_len;
                    let filled_u64 = u64::try_from(filled)
                        .map_err(|_| FileError::invalid("range segment offset overflows"))?;
                    let segment_len_u64 = u64::try_from(segment_len)
                        .map_err(|_| FileError::invalid("range segment length overflows"))?;
                    let segment_offset = offset
                        .checked_add(filled_u64)
                        .ok_or_else(|| FileError::invalid("range segment offset overflows"))?;
                    file.read_into(
                        FileReadRange::Bounded {
                            offset: segment_offset,
                            length: segment_len_u64,
                        },
                        &mut target[filled..next],
                        &operation_cancellation,
                    )
                    .await?;
                    filled = next;
                }
                Ok(target.freeze())
            }
            .await;
            let _ = sender.send(outcome);
        }))?;
        Ok(Self {
            cancellation,
            result: Some(result),
            task,
        })
    }

    /// Return the read outcome once. A missing sender is an abnormal exit,
    /// never a successful empty range. The receiver is awaited in place, so a
    /// dropped wait loses nothing and a later one receives the same outcome.
    pub async fn result_ready(&mut self) -> FileResult<Bytes> {
        let receiver = self.result.as_mut().ok_or_else(|| {
            FileError::new(
                FileErrorKind::Invalid,
                "file range result was already consumed",
            )
        })?;
        let outcome = receiver.await;
        self.result = None;
        outcome.map_err(|_| {
            FileError::new(
                FileErrorKind::Internal,
                "file range operation exited without a result",
            )
        })?
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
    }

    /// Wait for the spawned operation's destructor path and task exit.
    pub async fn drained(&mut self) -> FileResult<()> {
        self.task.drain().await
    }

    pub async fn stop_and_drain(&mut self) -> FileResult<()> {
        self.request_stop();
        self.drained().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileIdentity, FileTaskFuture, FsAccessResolver, TokioFileTaskSpawner};
    use novarocks_spi::connector::StorageAccessDomainId;

    fn local_file() -> (tempfile::TempDir, BoundFile) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("range.parquet");
        std::fs::write(&path, b"range-data").expect("write fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([9; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("local access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 10, None))
            .expect("bound file");
        (directory, file)
    }

    #[tokio::test]
    async fn successful_result_still_requires_an_exit_receipt() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current()));
        let mut operation = FileRangeOperation::start(
            file,
            FileReadRange::Bounded {
                offset: 1,
                length: 5,
            },
            FileCancellation::new(),
            &spawner,
        )
        .expect("range operation");

        assert_eq!(
            operation.result_ready().await.expect("result").as_ref(),
            b"ange-"
        );
        operation.drained().await.expect("actual task exit");
    }

    #[tokio::test]
    async fn fixed_target_range_is_published_only_after_exact_fill() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current()));
        let mut operation = FileRangeOperation::start_into(
            file,
            FileReadRange::Bounded {
                offset: 1,
                length: 5,
            },
            BytesMut::zeroed(5),
            FileCancellation::new(),
            &spawner,
        )
        .expect("fixed target range");
        assert_eq!(
            operation.result_ready().await.expect("exact fill").as_ref(),
            b"ange-"
        );
        operation.drained().await.expect("actual task exit");
    }

    #[tokio::test]
    async fn serial_segments_fill_one_final_backing() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current()));
        let mut operation = FileRangeOperation::start_segmented_into(
            file,
            FileReadRange::WholeFile,
            BytesMut::zeroed(10),
            3,
            FileCancellation::new(),
            &spawner,
        )
        .expect("segmented range");
        assert_eq!(
            operation
                .result_ready()
                .await
                .expect("complete backing")
                .as_ref(),
            b"range-data"
        );
        operation.drained().await.expect("segment task exit");
    }

    struct PanickingSpawner;

    impl FileTaskSpawner for PanickingSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            Ok(FileTask::new(tokio::spawn(async move {
                drop(task);
                panic!("injected range task failure");
            })))
        }

        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!("range test has no credential refresh")
        }
    }

    #[tokio::test]
    async fn abnormal_task_exit_has_separate_result_and_drain_failures() {
        let (_directory, file) = local_file();
        let spawner: Arc<dyn FileTaskSpawner> = Arc::new(PanickingSpawner);
        let source = FileCancellation::new();
        let mut operation =
            FileRangeOperation::start(file, FileReadRange::WholeFile, source.clone(), &spawner)
                .expect("range operation started");

        assert_eq!(
            operation
                .result_ready()
                .await
                .expect_err("no result was sent")
                .kind(),
            FileErrorKind::Internal
        );
        assert_eq!(
            operation.drained().await.expect_err("task panicked").kind(),
            FileErrorKind::Internal
        );
        assert!(!source.is_cancelled());
    }
}
