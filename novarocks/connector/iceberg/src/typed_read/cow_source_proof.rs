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

//! Immutable facts adopted from ONE original COW signed source.
//! This contains no read/access capability. The provider-owned pending source
//! remains FE-local; its exact file and access recipe never cross this wire.

use super::schema_binding::{IcebergMetadataColumn, metadata_target_field};
use super::{IcebergColumnHandle, IcebergTableHandle};
use crate::wire::dto;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergCowSourceProof {
    source_digest: [u8; 32],
    signed_base: [u8; 32],
    data_file_path: String,
    metadata_columns: Vec<IcebergColumnHandle>,
}

impl IcebergCowSourceProof {
    pub(crate) fn try_new(
        source_digest: [u8; 32],
        signed_base: [u8; 32],
        data_file_path: String,
        metadata_columns: Vec<IcebergColumnHandle>,
    ) -> Result<Self, ConnectorError> {
        if data_file_path.is_empty()
            || data_file_path.len() > novarocks_proto_codec::connector_read::MAX_PATH_BYTES
            || metadata_columns.len() > 4
        {
            return Err(invalid(
                "Iceberg COW source proof exceeds its original source shape",
            ));
        }
        let mut seen = BTreeSet::new();
        for column in &metadata_columns {
            let metadata = IcebergMetadataColumn::from_field_id(column.base_field_id())
                .ok_or_else(|| invalid("Iceberg COW proof contains a non-metadata field"))?;
            if metadata == IcebergMetadataColumn::IsDeleted
                || !seen.insert(column.base_field_id())
                || column.nullable() != metadata.nullable()
            {
                return Err(invalid(
                    "Iceberg COW proof metadata fields differ from the signed source",
                ));
            }
            metadata_target_field(column, metadata)?;
        }
        if !seen.contains(&IcebergMetadataColumn::Path.field_id())
            || !seen.contains(&IcebergMetadataColumn::RowPosition.field_id())
            || seen.contains(&IcebergMetadataColumn::RowId.field_id())
                != seen.contains(&IcebergMetadataColumn::LastUpdatedSequenceNumber.field_id())
        {
            return Err(invalid(
                "Iceberg COW proof metadata membership is incomplete",
            ));
        }
        Ok(Self {
            source_digest,
            signed_base,
            data_file_path,
            metadata_columns,
        })
    }

    pub(crate) fn check_table(&self, table: &IcebergTableHandle) -> Result<(), ConnectorError> {
        let pinned = table
            .pinned_data_files()
            .ok_or_else(|| invalid("Iceberg COW source requires its exact pinned file"))?;
        if table.read_domain().is_none()
            || table.snapshot_id().is_none()
            || pinned.len() != 1
            || !pinned.contains(&self.data_file_path)
        {
            return Err(invalid(
                "Iceberg COW proof differs from its frozen table source",
            ));
        }
        Ok(())
    }

    pub fn source_digest(&self) -> &[u8; 32] {
        &self.source_digest
    }
    pub fn signed_base(&self) -> &[u8; 32] {
        &self.signed_base
    }
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }
    pub fn metadata_columns(&self) -> &[IcebergColumnHandle] {
        &self.metadata_columns
    }

    pub(crate) fn to_proto(&self) -> dto::IcebergCowSourceProof {
        dto::IcebergCowSourceProof {
            source_digest: self.source_digest.to_vec(),
            signed_base: self.signed_base.to_vec(),
            data_file_path: self.data_file_path.clone(),
            metadata_columns: self
                .metadata_columns
                .iter()
                .map(IcebergColumnHandle::to_proto)
                .collect(),
        }
    }
    pub(crate) fn from_proto(raw: &dto::IcebergCowSourceProof) -> Result<Self, ConnectorError> {
        Self::try_new(
            raw.source_digest
                .as_slice()
                .try_into()
                .map_err(|_| invalid("Iceberg COW source digest must be 32 bytes"))?,
            raw.signed_base
                .as_slice()
                .try_into()
                .map_err(|_| invalid("Iceberg COW signed base must be 32 bytes"))?,
            raw.data_file_path.clone(),
            raw.metadata_columns
                .iter()
                .map(IcebergColumnHandle::from_proto)
                .collect::<Result<_, _>>()?,
        )
    }
}
fn invalid(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
