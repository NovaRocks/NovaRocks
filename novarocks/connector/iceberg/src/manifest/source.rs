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

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{Manifest, ManifestFile, ManifestMetadata};
use crate::iceberg::{Error, ErrorKind, Result};

use super::binding::BoundEntries;

// Design: ADR-0225 (docs/adr/ADR-0225-iceberg-manifest-source-binding-and-incremental-commits.md)
/// The list observation and its descriptor are both part of the source.
/// Pending outputs belong only to the step that wrote them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BindingOrigin<'a> {
    Committed {
        snapshot_id: i64,
        list_path: &'a str,
    },
    Pending {
        snapshot_id: i64,
    },
}

impl BindingOrigin<'_> {
    pub(crate) const fn is_pending(self) -> bool {
        matches!(self, Self::Pending { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ManifestSource<'a> {
    pub(crate) descriptor: &'a ManifestFile,
    pub(crate) origin: BindingOrigin<'a>,
}

impl<'a> ManifestSource<'a> {
    pub(crate) fn committed(
        snapshot_id: i64,
        list_path: &'a str,
        descriptor: &'a ManifestFile,
    ) -> Self {
        Self {
            descriptor,
            origin: BindingOrigin::Committed {
                snapshot_id,
                list_path,
            },
        }
    }

    pub(crate) fn pending(snapshot_id: i64, descriptor: &'a ManifestFile) -> Self {
        Self {
            descriptor,
            origin: BindingOrigin::Pending { snapshot_id },
        }
    }

    pub(crate) async fn load(self, io: &FileIO) -> Result<Manifest> {
        let bytes = io.new_input(&self.descriptor.manifest_path)?.read().await?;
        super::parse_manifest_checked(&bytes)
    }

    pub(crate) fn bind(self, raw: &'a Manifest) -> Result<BoundEntries<'a>> {
        self.check_header(raw.metadata())?;
        BoundEntries::new(self, raw.entries())
    }

    pub(crate) fn check_header(self, header: &ManifestMetadata) -> Result<()> {
        if header.content != self.descriptor.content
            || header.partition_spec.spec_id() != self.descriptor.partition_spec_id
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Manifest header disagrees with list descriptor: {}",
                    self.descriptor.manifest_path
                ),
            ));
        }
        if let BindingOrigin::Pending { snapshot_id } = self.origin
            && self.descriptor.added_snapshot_id != snapshot_id
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Pending manifest belongs to a different snapshot: {}",
                    self.descriptor.manifest_path
                ),
            ));
        }
        Ok(())
    }
}
