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

//! Provider-owned commit facts and staged-file metadata.

use std::collections::HashMap;

use crate::iceberg::spec::{DataContentType, DataFileFormat, Datum, Struct};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
    RowDeltaDvFromFiles,
    SelectedRewrite,
    CowUpdate,
    Truncate,
    OverwritePartitions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergWriteMode {
    LegacyPositionDeletes,
    RowLineageV3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergUpdateMode {
    CopyOnWrite,
    MergeOnRead,
}

impl IcebergUpdateMode {
    pub fn as_property_value(self) -> &'static str {
        match self {
            Self::CopyOnWrite => NOVAROCKS_UPDATE_MODE_COW,
            Self::MergeOnRead => NOVAROCKS_UPDATE_MODE_MOR,
        }
    }

    pub fn from_property_value(value: &str) -> Option<Self> {
        match value {
            NOVAROCKS_UPDATE_MODE_COW => Some(Self::CopyOnWrite),
            NOVAROCKS_UPDATE_MODE_MOR => Some(Self::MergeOnRead),
            _ => None,
        }
    }
}

pub const NOVAROCKS_UPDATE_MODE: &str = "novarocks.update.mode";
pub const NOVAROCKS_UPDATE_MODE_COW: &str = "copy-on-write";
pub const NOVAROCKS_UPDATE_MODE_MOR: &str = "merge-on-read";

/// Physical metadata for one staged Iceberg artifact. This stays provider
/// private; Core transports terminal SPI receipts rather than this value.
#[derive(Clone, Debug, PartialEq)]
pub struct WrittenFile {
    pub path: String,
    pub format: DataFileFormat,
    pub content: DataContentType,
    pub partition_values: Struct,
    pub partition_spec_id: i32,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub split_offsets: Vec<i64>,
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub nan_value_counts: HashMap<i32, u64>,
    pub lower_bounds: HashMap<i32, Datum>,
    pub upper_bounds: HashMap<i32, Datum>,
    pub key_metadata: Option<Vec<u8>>,
    pub referenced_data_file: Option<String>,
    pub equality_ids: Option<Vec<i32>>,
    pub first_row_id: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub cardinality: Option<u64>,
}

impl WrittenFile {
    pub(crate) fn entry_identity(
        &self,
    ) -> crate::iceberg::Result<crate::commit::model::EntryIdentity> {
        use crate::commit::model::EntryIdentity;
        let identity = match (self.content, self.format) {
            (DataContentType::Data, _) => EntryIdentity::DataFile {
                path: self.path.clone(),
            },
            (DataContentType::PositionDeletes, DataFileFormat::Puffin) => {
                EntryIdentity::DeletionVector {
                    path: self.path.clone(),
                    offset: self.content_offset.ok_or_else(|| {
                        crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::DataInvalid,
                            "Deletion vector has no content offset",
                        )
                    })?,
                    length: self.content_size_in_bytes.ok_or_else(|| {
                        crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::DataInvalid,
                            "Deletion vector has no content length",
                        )
                    })?,
                    referenced_data_file: self.referenced_data_file.clone().ok_or_else(|| {
                        crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::DataInvalid,
                            "Deletion vector has no referenced data file",
                        )
                    })?,
                }
            }
            _ => EntryIdentity::DeleteFile {
                path: self.path.clone(),
            },
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_mode_properties_round_trip() {
        assert_eq!(
            IcebergUpdateMode::from_property_value(
                IcebergUpdateMode::CopyOnWrite.as_property_value()
            ),
            Some(IcebergUpdateMode::CopyOnWrite)
        );
    }

    #[test]
    fn commit_kinds_are_distinct() {
        assert_ne!(CommitOpKind::FastAppend, CommitOpKind::CowUpdate);
    }
}
