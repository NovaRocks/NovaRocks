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

//! File-task spawners the read-stack tests hold or count reads with.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_fs::{
    FileIoFuture, FileIoJob, FileIoSpawner, FileRangeScope, FileRangeService, FileReadContext,
    FileResult,
};
use novarocks_spi::connector::read_stack::ConnectorSourceOperations;

/// Holds every spawned file task until the test adds a permit for it.
pub(crate) struct GatedFileIoSpawner {
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) gate: Arc<tokio::sync::Semaphore>,
    pub(crate) started: AtomicUsize,
}

impl GatedFileIoSpawner {
    /// A spawner that holds every task until [`Self::release`] lets it run.
    pub(crate) fn closed(handle: tokio::runtime::Handle) -> Arc<Self> {
        Arc::new(Self {
            handle,
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            started: AtomicUsize::new(0),
        })
    }

    pub(crate) fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    pub(crate) fn release(&self, tasks: usize) {
        self.gate.add_permits(tasks);
    }
}

impl FileIoSpawner for GatedFileIoSpawner {
    fn spawn(&self, task: FileIoFuture) -> FileResult<FileIoJob> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let gate = Arc::clone(&self.gate);
        Ok(FileIoJob::new(self.handle.spawn(async move {
            gate.acquire_owned().await.expect("gate").forget();
            task.await;
        })))
    }
}

/// Runs every spawned file task at once, counting them.
pub(crate) struct CountingFileIoSpawner {
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) spawned: AtomicUsize,
}

impl FileIoSpawner for CountingFileIoSpawner {
    fn spawn(&self, task: FileIoFuture) -> FileResult<FileIoJob> {
        self.spawned.fetch_add(1, Ordering::SeqCst);
        Ok(FileIoJob::new(self.handle.spawn(task)))
    }
}

/// Routes `context`'s reads through a range service whose every file task is
/// spawned by `spawner`, as the reads of one task source; returns that
/// source's operations.
pub(crate) fn route_reads_through(
    context: &mut FileReadContext,
    spawner: Arc<dyn FileIoSpawner>,
    handle: tokio::runtime::Handle,
) -> ConnectorSourceOperations {
    let operations = ConnectorSourceOperations::for_test();
    context.io_spawner = Arc::clone(&spawner);
    context.range = Some(
        FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            spawner,
            handle,
        )
        .bind(
            FileRangeScope::for_test(1, 0, 1, 2, 0, 3).unwrap(),
            operations.clone(),
        ),
    );
    operations
}
