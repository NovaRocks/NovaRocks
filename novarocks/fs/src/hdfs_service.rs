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

//! HDFS projection through registry hdfs-native's public client API.

use std::{fmt, sync::Arc};

use hdfs_native::{Client, ClientBuilder, HdfsError, WriteOptions};
use opendal::raw::{self, Timestamp, oio};
use opendal::{
    Buffer, Capability, EntryMode, Error, ErrorKind, Metadata, OperationContext, Result,
};
use serde::{Deserialize, Serialize};

/// This private configuration feeds only the original FS constructor.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct HdfsServiceConfig {
    endpoint: Option<String>,
    user: Option<String>,
}

impl opendal::Configurator for HdfsServiceConfig {
    type Builder = HdfsServiceBuilder;

    fn into_builder(self) -> Self::Builder {
        HdfsServiceBuilder {
            config: self,
            runtime: None,
        }
    }
}

#[derive(Default)]
pub(crate) struct HdfsServiceBuilder {
    config: HdfsServiceConfig,
    runtime: Option<tokio::runtime::Handle>,
}

impl HdfsServiceBuilder {
    pub(crate) fn new(endpoint: &str, user: Option<&str>, runtime: tokio::runtime::Handle) -> Self {
        Self {
            config: HdfsServiceConfig {
                endpoint: Some(endpoint.to_owned()),
                user: user.map(str::to_owned),
            },
            runtime: Some(runtime),
        }
    }

    fn client_builder(&self) -> Result<ClientBuilder> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "HDFS service requires an explicitly composed role runtime",
            )
        })?;
        let endpoint = self
            .config
            .endpoint
            .as_deref()
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "HDFS endpoint is missing"))?;
        // Preserve the original logical authority and SDK Hadoop configuration.
        // Effective user is independent of the NameNode RPC address.
        let mut builder = ClientBuilder::new()
            .with_url(endpoint)
            .with_io_runtime(runtime.clone());
        if let Some(user) = &self.config.user {
            builder = builder.with_user(user);
        }
        Ok(builder)
    }
}

impl opendal::Builder for HdfsServiceBuilder {
    type Config = HdfsServiceConfig;

    fn build(self) -> Result<impl raw::Service> {
        let client = self.client_builder()?.build().map_err(map_error)?;
        Ok(HdfsService {
            client: Arc::new(client),
        })
    }
}

struct HdfsService {
    client: Arc<Client>,
}

impl fmt::Debug for HdfsService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HdfsService").finish_non_exhaustive()
    }
}

impl raw::Service for HdfsService {
    type Reader = oio::PositionReader<HdfsRead>;
    type Writer = HdfsWrite;
    type Lister = HdfsList;
    type Deleter = oio::OneShotDeleter<HdfsDelete>;
    type Copier = ();

    fn info(&self) -> raw::ServiceInfo {
        raw::ServiceInfo::new("hdfs-native", "/", "")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            write: true,
            create_dir: true,
            delete: true,
            list: true,
            rename: true,
            shared: true,
            // The original FS constructor did not enable append.
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        path: &str,
        _: raw::OpCreateDir,
    ) -> Result<raw::RpCreateDir> {
        self.client
            .mkdirs(&absolute(path), 0o777, true)
            .await
            .map_err(map_error)?;
        Ok(raw::RpCreateDir::default())
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: raw::OpStat) -> Result<raw::RpStat> {
        let status = self
            .client
            .get_file_info(&absolute(path))
            .await
            .map_err(map_error)?;
        Ok(raw::RpStat::new(metadata(&status)?))
    }

    fn read(&self, _: &OperationContext, path: &str, _: raw::OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(HdfsRead {
            client: Arc::clone(&self.client),
            path: absolute(path),
        }))
    }

    fn write(&self, _: &OperationContext, path: &str, args: raw::OpWrite) -> Result<Self::Writer> {
        if args.append() {
            return Err(unsupported("HDFS append is not enabled"));
        }
        Ok(HdfsWrite {
            client: Arc::clone(&self.client),
            path: absolute(path),
            file: None,
            written: 0,
        })
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(HdfsDelete {
            client: Arc::clone(&self.client),
        }))
    }

    fn list(&self, _: &OperationContext, path: &str, _: raw::OpList) -> Result<Self::Lister> {
        Ok(HdfsList {
            client: Arc::clone(&self.client),
            path: absolute(path),
            entries: None,
            finished: false,
        })
    }

    fn copy(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: raw::OpCopy,
        _: raw::OpCopier,
    ) -> Result<Self::Copier> {
        Err(unsupported("HDFS copy is not supported"))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        from: &str,
        to: &str,
        _: raw::OpRename,
    ) -> Result<raw::RpRename> {
        let from = absolute(from);
        let to = absolute(to);
        // Preserve file-destination and parent-creation behavior using public
        // operations, without the old delete-then-create destination stub.
        match self.client.get_file_info(&to).await {
            Ok(status) if status.isdir => {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "HDFS destination is a directory",
                ));
            }
            Ok(_) => {}
            Err(HdfsError::FileNotFound(_)) => {
                let parent = to.rsplit_once('/').map_or("/", |(parent, _)| parent);
                self.client
                    .mkdirs(if parent.is_empty() { "/" } else { parent }, 0o777, true)
                    .await
                    .map_err(map_error)?;
            }
            Err(error) => return Err(map_error(error)),
        }
        self.client
            .rename(&from, &to, true)
            .await
            .map_err(map_error)?;
        Ok(raw::RpRename::default())
    }

    async fn presign(
        &self,
        _: &OperationContext,
        _: &str,
        _: raw::OpPresign,
    ) -> Result<raw::RpPresign> {
        Err(unsupported("HDFS presign is not supported"))
    }
}

fn absolute(path: &str) -> String {
    format!("/{}", path.trim_start_matches('/'))
}

fn unsupported(message: &'static str) -> Error {
    Error::new(ErrorKind::Unsupported, message)
}

fn metadata(status: &hdfs_native::client::FileStatus) -> Result<Metadata> {
    let mode = if status.isdir {
        EntryMode::DIR
    } else {
        EntryMode::FILE
    };
    let mut result = Metadata::new(mode);
    result.set_content_length(
        u64::try_from(status.length)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS file length is out of range"))?,
    );
    let millis = i64::try_from(status.modification_time).map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "HDFS modification time is out of range",
        )
    })?;
    result.set_last_modified(Timestamp::from_millisecond(millis)?);
    Ok(result)
}

fn map_error(error: HdfsError) -> Error {
    // Retain the original typed source and established HDFS classifications.
    let kind = match &error {
        HdfsError::FileNotFound(_) | HdfsError::BlocksNotFound(_) => ErrorKind::NotFound,
        HdfsError::AlreadyExists(_) => ErrorKind::AlreadyExists,
        HdfsError::IsADirectoryError(_) => ErrorKind::IsADirectory,
        HdfsError::RPCError(class, _) if class.contains("java.io.FileNotFoundException") => {
            ErrorKind::NotFound
        }
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, error.to_string()).set_source(error)
}

struct HdfsRead {
    client: Arc<Client>,
    path: String,
}

impl oio::PositionRead for HdfsRead {
    type Handle = hdfs_native::file::FileReader;

    async fn open(&self) -> Result<Self::Handle> {
        self.client.read(&self.path).await.map_err(map_error)
    }

    async fn read_at(file: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        let offset = usize::try_from(offset).map_err(|_| {
            Error::new(
                ErrorKind::RangeNotSatisfied,
                "HDFS read offset is out of range",
            )
        })?;
        if size == 0 || offset >= file.file_length() {
            return Ok(Buffer::new());
        }
        let size = size.min(file.file_length() - offset);
        file.read_range(offset, size)
            .await
            .map(Buffer::from)
            .map_err(map_error)
    }
}

struct HdfsWrite {
    client: Arc<Client>,
    path: String,
    file: Option<hdfs_native::file::FileWriter>,
    written: u64,
}

impl HdfsWrite {
    async fn file(&mut self) -> Result<&mut hdfs_native::file::FileWriter> {
        if self.file.is_none() {
            // Ordinary write is an overwrite operation. Do not turn a prior
            // existence probe into a conditional-create race.
            self.file = Some(
                self.client
                    .create(&self.path, WriteOptions::default().overwrite(true))
                    .await
                    .map_err(map_error)?,
            );
        }
        Ok(self.file.as_mut().expect("HDFS writer was initialized"))
    }
}

impl oio::Write for HdfsWrite {
    async fn write(&mut self, mut buffer: Buffer) -> Result<()> {
        let length = u64::try_from(buffer.len())
            .map_err(|_| Error::new(ErrorKind::Unexpected, "HDFS write length is out of range"))?;
        let written = self
            .written
            .checked_add(length)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "HDFS written length overflow"))?;
        let file = self.file().await?;
        for chunk in buffer.by_ref() {
            let expected = chunk.len();
            if file.write_bytes(chunk).await.map_err(map_error)? != expected {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "HDFS write accepted a partial buffer",
                ));
            }
        }
        self.written = written;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.file().await?.close().await.map_err(map_error)?;
        Ok(Metadata::new(EntryMode::FILE).with_content_length(self.written))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(unsupported("HDFS writer abort is not supported"))
    }
}

struct HdfsDelete {
    client: Arc<Client>,
}

impl oio::OneShotDelete for HdfsDelete {
    async fn delete_once(&self, path: String, _: raw::OpDelete) -> Result<()> {
        self.client
            .delete(&absolute(&path), true)
            .await
            .map_err(map_error)?;
        Ok(())
    }
}

struct HdfsList {
    client: Arc<Client>,
    path: String,
    entries: Option<hdfs_native::client::ListStatusIterator>,
    finished: bool,
}

impl oio::List for HdfsList {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if self.finished {
            return Ok(None);
        }
        if self.entries.is_none() {
            let status = match self.client.get_file_info(&self.path).await {
                Ok(status) => status,
                Err(HdfsError::FileNotFound(_)) => {
                    self.finished = true;
                    return Ok(None);
                }
                Err(error) => return Err(map_error(error)),
            };
            if !status.isdir {
                self.finished = true;
                return entry(status).map(Some);
            }
            self.entries = Some(self.client.list_status_iter(&self.path, false));
        }
        match self
            .entries
            .as_ref()
            .expect("HDFS directory iterator was initialized")
            .next()
            .await
        {
            Some(Ok(status)) => entry(status).map(Some),
            Some(Err(error)) => Err(map_error(error)),
            None => {
                self.finished = true;
                Ok(None)
            }
        }
    }
}

fn entry(status: hdfs_native::client::FileStatus) -> Result<oio::Entry> {
    let relative = status.path.strip_prefix('/').ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "HDFS listing returned a non-absolute path",
        )
    })?;
    let path = if status.isdir {
        format!("{}/", relative.trim_end_matches('/'))
    } else {
        relative.to_owned()
    };
    Ok(oio::Entry::new(&path, metadata(&status)?))
}

#[cfg(test)]
mod tests;
