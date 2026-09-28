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

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use futures::stream;
use paimon::io::{FileIO, FileStatus, FileStatusStream, ReadControl, ReadOnlyFileIO};

#[derive(Debug)]
struct Control {
    cancelled: AtomicBool,
    checkpoints: AtomicU64,
}

impl ReadControl for Control {
    fn check_active(&self) -> paimon::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(paimon::Error::UnexpectedError {
                message: "cancelled".to_string(),
                source: None,
            })
        } else {
            Ok(())
        }
    }
    fn checkpoint(&self) -> paimon::Result<()> {
        self.checkpoints.fetch_add(1, Ordering::AcqRel);
        self.check_active()
    }
}

#[derive(Debug)]
struct Backend {
    bytes: Bytes,
    reads: AtomicU64,
    writes: AtomicU64,
}

#[async_trait::async_trait]
impl ReadOnlyFileIO for Backend {
    async fn stat(&self, path: &str) -> paimon::Result<FileStatus> {
        Ok(FileStatus {
            size: self.bytes.len() as u64,
            is_dir: false,
            path: path.to_string(),
            last_modified: None,
        })
    }
    async fn exists(&self, _path: &str) -> paimon::Result<bool> {
        Ok(true)
    }
    async fn read(
        &self,
        _path: &str,
        range: Range<u64>,
        _known_size: Option<u64>,
    ) -> paimon::Result<Bytes> {
        self.reads.fetch_add(1, Ordering::AcqRel);
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }
    async fn list(&self, path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
        let path = path.to_owned();
        let entries = (0..3).map(move |i| {
            Ok(FileStatus {
                size: 1,
                is_dir: false,
                path: format!("{path}/file-{i}"),
                last_modified: None,
            })
        });
        Ok(Box::pin(stream::iter(entries)))
    }
}

fn fixture() -> (FileIO, Arc<Control>, Arc<Backend>) {
    let control = Arc::new(Control {
        cancelled: AtomicBool::new(false),
        checkpoints: AtomicU64::new(0),
    });
    let backend = Arc::new(Backend {
        bytes: Bytes::from_static(b"authorized-data"),
        reads: AtomicU64::new(0),
        writes: AtomicU64::new(0),
    });
    (
        FileIO::from_read_only(backend.clone(), control.clone()),
        control,
        backend,
    )
}

#[tokio::test]
async fn authorized_read_uses_host_backend_and_checks_liveness() {
    let (io, control, backend) = fixture();
    let bytes = io
        .new_input("s3://bucket/warehouse/file")
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(bytes, Bytes::from_static(b"authorized-data"));
    assert_eq!(backend.reads.load(Ordering::Acquire), 1);
    assert!(control.checkpoints.load(Ordering::Acquire) > 0);
}

#[tokio::test]
async fn listing_is_plain_owned() {
    let (io, _control, backend) = fixture();
    assert_eq!(
        io.list_status("s3://bucket/warehouse").await.unwrap().len(),
        3
    );
    assert_eq!(backend.reads.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn cancellation_prevents_backend_read() {
    let (io, control, backend) = fixture();
    control.cancelled.store(true, Ordering::Release);
    assert!(io.new_input("s3://bucket/warehouse/file").is_err());
    assert_eq!(backend.reads.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn every_mutating_entry_is_unsupported_before_backend_side_effect() {
    let (io, _control, backend) = fixture();
    assert!(io.new_output("s3://bucket/warehouse/out").is_err());
    assert!(io.mkdirs("s3://bucket/warehouse/dir").await.is_err());
    assert!(io.delete_file("s3://bucket/warehouse/file").await.is_err());
    assert!(io.delete_dir("s3://bucket/warehouse/dir").await.is_err());
    assert!(
        io.copy_file("s3://bucket/warehouse/a", "s3://bucket/warehouse/b")
            .await
            .is_err()
    );
    assert!(
        io.rename("s3://bucket/warehouse/a", "s3://bucket/warehouse/b")
            .await
            .is_err()
    );
    assert_eq!(backend.writes.load(Ordering::Acquire), 0);
}
