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

//! Logical entries are distinct from the physical objects that contain them.

use std::fmt;

use novarocks_spi::connector::{ConnectorMutationOperationId, ConnectorWriteOperationId};
use uuid::Uuid;

use crate::iceberg::Result;
use crate::iceberg::spec::{DataContentType, DataFile, DataFileFormat};

use super::invalid;

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EntryIdentity {
    DataFile {
        path: String,
    },
    DeleteFile {
        path: String,
    },
    DeletionVector {
        path: String,
        offset: i64,
        length: i64,
        referenced_data_file: String,
    },
}

impl EntryIdentity {
    pub fn path(&self) -> &str {
        match self {
            Self::DataFile { path }
            | Self::DeleteFile { path }
            | Self::DeletionVector { path, .. } => path,
        }
    }

    pub fn object(&self) -> ObjectIdentity {
        ObjectIdentity {
            path: self.path().to_owned(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.path().is_empty() {
            return Err(invalid("Iceberg entry path must not be empty"));
        }
        if let Self::DeletionVector {
            offset,
            length,
            referenced_data_file,
            ..
        } = self
        {
            if *offset < 0 || *length <= 0 || referenced_data_file.is_empty() {
                return Err(invalid(
                    "Deletion vector identity requires a nonnegative offset, positive length and referenced data file",
                ));
            }
            offset
                .checked_add(*length)
                .ok_or_else(|| invalid("Deletion vector content range overflows"))?;
        }
        Ok(())
    }
}

impl TryFrom<&DataFile> for EntryIdentity {
    type Error = crate::iceberg::Error;

    fn try_from(file: &DataFile) -> Result<Self> {
        let path = file.file_path().to_owned();
        let identity = match file.content_type() {
            DataContentType::Data => Self::DataFile { path },
            DataContentType::PositionDeletes if file.file_format() == DataFileFormat::Puffin => {
                Self::DeletionVector {
                    path,
                    offset: file
                        .content_offset()
                        .ok_or_else(|| invalid("Deletion vector is missing its content offset"))?,
                    length: file
                        .content_size_in_bytes()
                        .ok_or_else(|| invalid("Deletion vector is missing its content length"))?,
                    referenced_data_file: file.referenced_data_file().ok_or_else(|| {
                        invalid("Deletion vector is missing its referenced data file")
                    })?,
                }
            }
            DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                Self::DeleteFile { path }
            }
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectIdentity {
    path: String,
}

impl ObjectIdentity {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if path.is_empty() {
            return Err(invalid("Iceberg object path must not be empty"));
        }
        Ok(Self { path })
    }
    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OperationAuthority {
    Write,
    Mutation,
    WriteSession,
}

impl OperationAuthority {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::Mutation => "mutation",
            Self::WriteSession => "write-session",
        }
    }
}

/// Preserves the identity minted by the caller; it never mints a replacement operation ID.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationToken {
    authority: OperationAuthority,
    bytes: [u8; 16],
}

impl OperationToken {
    pub fn from_write(id: ConnectorWriteOperationId) -> Self {
        Self {
            authority: OperationAuthority::Write,
            bytes: id.to_bytes(),
        }
    }
    pub fn from_mutation(id: ConnectorMutationOperationId) -> Self {
        Self {
            authority: OperationAuthority::Mutation,
            bytes: id.to_bytes(),
        }
    }
    pub fn from_write_session(
        id: crate::commit::write_stack::domain::IcebergWriteSessionId,
    ) -> Self {
        Self {
            authority: OperationAuthority::WriteSession,
            bytes: id.to_bytes(),
        }
    }
    pub const fn authority(self) -> OperationAuthority {
        self.authority
    }
    pub const fn to_bytes(self) -> [u8; 16] {
        self.bytes
    }
    pub fn path_component(self) -> String {
        format!(
            "{}-{}",
            self.authority.label(),
            Uuid::from_bytes(self.bytes)
        )
    }
}

impl fmt::Display for OperationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path_component())
    }
}

/// A fresh attempt identity is nested under the unchanged logical operation identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttemptToken {
    operation: OperationToken,
    ordinal: u32,
    nonce: Uuid,
}

impl AttemptToken {
    pub fn new(operation: OperationToken, ordinal: u32) -> Self {
        Self {
            operation,
            ordinal,
            nonce: Uuid::now_v7(),
        }
    }
    pub const fn operation(self) -> OperationToken {
        self.operation
    }
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
    pub const fn nonce_bytes(self) -> [u8; 16] {
        *self.nonce.as_bytes()
    }
    pub fn path_component(self) -> String {
        format!("{}-attempt-{}-{}", self.operation, self.ordinal, self.nonce)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn deletion_vector_identity_includes_the_complete_entry() {
        let identity = EntryIdentity::DeletionVector {
            path: "s3://bucket/shared.puffin".into(),
            offset: 16,
            length: 32,
            referenced_data_file: "s3://bucket/a.parquet".into(),
        };
        let mut variants = vec![identity.clone()];
        if let EntryIdentity::DeletionVector { offset, .. } = &mut variants[0] {
            *offset += 1;
        }
        let mut different_length = identity.clone();
        if let EntryIdentity::DeletionVector { length, .. } = &mut different_length {
            *length += 1;
        }
        let mut different_reference = identity.clone();
        if let EntryIdentity::DeletionVector {
            referenced_data_file,
            ..
        } = &mut different_reference
        {
            *referenced_data_file = "s3://bucket/b.parquet".into();
        }
        variants.extend([identity.clone(), different_length, different_reference]);
        assert_eq!(variants.iter().cloned().collect::<HashSet<_>>().len(), 4);
        assert_eq!(
            variants
                .iter()
                .map(EntryIdentity::object)
                .collect::<HashSet<_>>()
                .len(),
            1
        );
        assert_eq!(identity, identity.clone());
    }

    #[test]
    fn invalid_deletion_vector_ranges_are_rejected() {
        for (offset, length) in [(-1, 1), (0, 0), (i64::MAX, 1)] {
            assert!(
                EntryIdentity::DeletionVector {
                    path: "dv.puffin".into(),
                    offset,
                    length,
                    referenced_data_file: "data.parquet".into()
                }
                .validate()
                .is_err()
            );
        }
    }

    #[test]
    fn operation_authorities_preserve_bytes_and_attempts_are_unique() {
        let bytes = [7; 16];
        let write = OperationToken::from_write(ConnectorWriteOperationId::from_bytes(bytes));
        let mutation =
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes(bytes));
        assert_eq!(write.to_bytes(), bytes);
        assert_ne!(write, mutation);
        let a = AttemptToken::new(write, 0);
        let b = AttemptToken::new(write, 0);
        assert_eq!(a.operation(), b.operation());
        assert_ne!(a.path_component(), b.path_component());
    }
    #[test]
    fn format_decode_requires_complete_dv_metadata_but_not_for_position_delete_files() {
        let puffin = |offset, length, referenced| {
            crate::iceberg::spec::DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path("s3://bucket/shared.puffin".into())
                .file_format(DataFileFormat::Puffin)
                .record_count(1)
                .file_size_in_bytes(100)
                .content_offset(offset)
                .content_size_in_bytes(length)
                .referenced_data_file(referenced)
                .build()
                .unwrap()
        };
        assert!(
            EntryIdentity::try_from(&puffin(None, Some(10), Some("data.parquet".into()))).is_err()
        );
        assert!(
            EntryIdentity::try_from(&puffin(Some(0), None, Some("data.parquet".into()))).is_err()
        );
        assert!(EntryIdentity::try_from(&puffin(Some(0), Some(10), None)).is_err());
        assert!(matches!(
            EntryIdentity::try_from(&puffin(Some(0), Some(10), Some("data.parquet".into())))
                .unwrap(),
            EntryIdentity::DeletionVector { .. }
        ));
        let position = crate::iceberg::spec::DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("s3://bucket/delete.parquet".into())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap();
        assert!(matches!(
            EntryIdentity::try_from(&position).unwrap(),
            EntryIdentity::DeleteFile { .. }
        ));
    }
    #[test]
    fn logical_identity_json_preserves_complete_dv_identity_and_rejects_unknown_fields() {
        let identity = EntryIdentity::DeletionVector {
            path: "s3://bucket/shared.puffin".into(),
            offset: 16,
            length: 32,
            referenced_data_file: "s3://bucket/data.parquet".into(),
        };
        let json = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"kind":"deletion_vector","path":"s3://bucket/shared.puffin","offset":16,"length":32,"referenced_data_file":"s3://bucket/data.parquet"})
        );
        let decoded: EntryIdentity = serde_json::from_value(json.clone()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded, identity);
        let mut unknown = json;
        unknown["legacy_path"] = serde_json::json!("s3://bucket/legacy.puffin");
        assert!(serde_json::from_value::<EntryIdentity>(unknown).is_err());
        assert!(
            serde_json::from_value::<EntryIdentity>(serde_json::json!("s3://bucket/shared.puffin"))
                .is_err()
        );
        assert!(
            serde_json::from_value::<EntryIdentity>(
                serde_json::json!({"kind":"deletion_vector","path":"puffin","offset":0,"length":10})
            )
            .is_err()
        );
        for identity in [
            EntryIdentity::DataFile {
                path: "data.parquet".into(),
            },
            EntryIdentity::DeleteFile {
                path: "delete.parquet".into(),
            },
        ] {
            assert_eq!(
                serde_json::from_value::<EntryIdentity>(serde_json::to_value(&identity).unwrap())
                    .unwrap(),
                identity
            );
        }
    }
}
