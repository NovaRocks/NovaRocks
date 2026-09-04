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

//! Puffin materialization for already-computed generic statistics artifacts.
//!
//! Aggregate execution owns artifact bodies. This module only binds them to
//! the transaction-local snapshot/sequence and writes the attempt-local Puffin
//! object that the same catalog commit registers.

use std::collections::HashMap;

use crate::iceberg::io::FileIO;
use crate::iceberg::puffin::{Blob, PuffinWriter};
use crate::iceberg::spec::{BlobMetadata, StatisticsFile, TableMetadata};
use novarocks_spi::connector::StatisticsArtifactDraft;

/// Per-table switch for collect-on-write statistics.
pub const COLLECT_ON_WRITE_PROPERTY: &str = "novarocks.statistics.collect-on-write";

/// Write already-computed long-form artifact drafts to one Puffin file.
/// Snapshot and sequence provenance are bound here, after execution, and are
/// never carried in the generic Root relation.
pub async fn write_puffin_artifacts(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    artifacts: &[StatisticsArtifactDraft],
) -> Result<Option<StatisticsFile>, String> {
    if artifacts.is_empty() {
        return Ok(None);
    }
    let output_file = file_io
        .new_output(puffin_path)
        .map_err(|error| format!("open output Puffin {puffin_path}: {error}"))?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
        .await
        .map_err(|error| format!("create Puffin writer: {error}"))?;
    let mut metadata = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let (identity, body, properties) = artifact.clone().into_parts();
        let blob = Blob::builder()
            .r#type(identity.blob_type().to_string())
            .fields(identity.input_fields().to_vec())
            .snapshot_id(snapshot_id)
            .sequence_number(sequence_number)
            .data(body.to_vec())
            .properties(properties.clone().into_iter().collect())
            .build();
        writer
            .add(blob, crate::iceberg::puffin::CompressionCodec::None)
            .await
            .map_err(|error| format!("write Puffin artifact: {error}"))?;
        metadata.push(BlobMetadata {
            r#type: identity.blob_type().to_string(),
            snapshot_id,
            sequence_number,
            fields: identity.input_fields().to_vec(),
            properties: properties.into_iter().collect(),
        });
    }
    writer
        .close()
        .await
        .map_err(|error| format!("close Puffin writer: {error}"))?;
    let input_file = file_io
        .new_input(puffin_path)
        .map_err(|error| format!("open Puffin for sizing {puffin_path}: {error}"))?;
    let file_size = input_file
        .metadata()
        .await
        .map_err(|error| format!("stat Puffin {puffin_path}: {error}"))?
        .size;
    let footer_size = read_footer_size(&input_file, file_size).await?;
    Ok(Some(StatisticsFile {
        snapshot_id,
        statistics_path: puffin_path.to_string(),
        file_size_in_bytes: file_size as i64,
        file_footer_size_in_bytes: footer_size as i64,
        key_metadata: None,
        blob_metadata: metadata,
    }))
}

async fn read_footer_size(
    input_file: &crate::iceberg::io::InputFile,
    file_size: u64,
) -> Result<u64, String> {
    const FOOTER_STRUCT_LENGTH: u64 = 12;
    const MAGIC_LENGTH: u64 = 4;
    if file_size < FOOTER_STRUCT_LENGTH + MAGIC_LENGTH {
        return Err(format!(
            "Puffin file too small to contain footer: {file_size} bytes"
        ));
    }
    let reader = input_file
        .reader()
        .await
        .map_err(|error| format!("open Puffin reader: {error}"))?;
    let start = file_size - FOOTER_STRUCT_LENGTH;
    let bytes = reader
        .read(start..start + 4)
        .await
        .map_err(|error| format!("read Puffin footer payload length: {error}"))?;
    let mut encoded = [0u8; 4];
    encoded.copy_from_slice(&bytes);
    Ok(MAGIC_LENGTH + u32::from_le_bytes(encoded) as u64 + FOOTER_STRUCT_LENGTH)
}

/// Operation-specific Puffin location. The supplied identity must identify a
/// single attempt, so conflict retries never overwrite an earlier attempt.
pub fn puffin_path_for_statistics_operation(
    table_metadata: &TableMetadata,
    snapshot_id: i64,
    operation_id: [u8; 16],
) -> String {
    let location = table_metadata.location().trim_end_matches('/');
    let operation_id = uuid::Uuid::from_bytes(operation_id);
    format!("{location}/metadata/snap-{snapshot_id}-statistics-{operation_id}.puffin")
}
