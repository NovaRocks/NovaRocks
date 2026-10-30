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

/// Formation invoice for the actual session's drafts. Original guarded body
/// bytes are borrowed; only the largest sequential Puffin body copy coexists.
/// Metadata accounts for normalization, identity sets, the Blob/StatisticsFile
/// copies, frozen updates and JSON escaping. This changes no input limit.
pub(crate) fn statistics_publication_owned_bytes(
    drafts: &[StatisticsArtifactDraft],
    table: &crate::iceberg::table::Table,
    expectations: &[novarocks_spi::connector::StatisticsArtifactIdentity],
) -> Result<std::num::NonZeroU64, novarocks_spi::connector::ConnectorError> {
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    };
    use std::mem::size_of;
    let overflow = || {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Statistics publication formation invoice overflow",
        )
    };
    if drafts.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Statistics artifact count exceeds its original contract",
        ));
    }
    let mut metadata = drafts
        .len()
        .checked_mul(
            size_of::<StatisticsArtifactDraft>() + size_of::<Blob>() + size_of::<BlobMetadata>(),
        )
        .ok_or_else(overflow)?;
    let mut body = 0;
    for draft in drafts {
        body = body.max(draft.body().len());
        metadata = metadata
            .checked_add(
                draft
                    .identity()
                    .input_fields()
                    .len()
                    .checked_mul(size_of::<i32>())
                    .ok_or_else(overflow)?,
            )
            .and_then(|n| n.checked_add(draft.identity().blob_type().len()))
            .ok_or_else(overflow)?;
        // The only normalized property is the NDV value. Include its missing
        // value's f64 display buffer even when the actual input has no entry.
        metadata = metadata
            .checked_add(
                size_of::<(String, String)>()
                    + crate::stats_loader::NDV_PROPERTY.len()
                    + f64::MAX_10_EXP as usize
                    + f64::MANTISSA_DIGITS as usize
                    + 4,
            )
            .ok_or_else(overflow)?;
        for (key, value) in draft.properties() {
            metadata = metadata
                .checked_add(size_of::<(String, String)>())
                .and_then(|n| n.checked_add(key.len()))
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(overflow)?;
        }
    }
    for expected in expectations {
        metadata = metadata
            .checked_add(size_of::<
                novarocks_spi::connector::StatisticsArtifactIdentity,
            >())
            .and_then(|n| {
                n.checked_add(
                    expected
                        .input_fields()
                        .len()
                        .checked_mul(size_of::<i32>())?,
                )
            })
            .and_then(|n| n.checked_add(expected.blob_type().len()))
            .ok_or_else(overflow)?;
    }
    metadata = metadata
        .checked_add(table.metadata().location().len())
        .and_then(|n| n.checked_add(table.identifier().name.len()))
        .ok_or_else(overflow)?;
    for part in table.identifier().namespace.iter() {
        metadata = metadata
            .checked_add(size_of::<String>())
            .and_then(|n| n.checked_add(part.len()))
            .ok_or_else(overflow)?;
    }
    // Eight live typed metadata views (normalization/sets, Blob/metadata,
    // frozen request/projection) with at most six JSON bytes per input byte.
    let peak = metadata
        .checked_mul(8)
        .and_then(|n| n.checked_mul(6))
        .and_then(|n| n.checked_add(body))
        .and_then(|n| {
            n.checked_add(2 * novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES)
        })
        .ok_or_else(overflow)?;
    std::num::NonZeroU64::new(u64::try_from(peak).map_err(|_| overflow())?).ok_or_else(overflow)
}

/// Per-table switch for collect-on-write statistics.
pub const COLLECT_ON_WRITE_PROPERTY: &str = "novarocks.statistics.collect-on-write";

// Scoped CPU failures retain their exact Connector fact and cause through
// the shared writer. Legacy callers still receive their original String API.
#[derive(Debug)]
enum PuffinWriteFailure {
    Legacy(String),
    Scoped(novarocks_spi::connector::ConnectorError),
}
impl From<String> for PuffinWriteFailure {
    fn from(message: String) -> Self {
        Self::Legacy(message)
    }
}
impl std::fmt::Display for PuffinWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(message) => f.write_str(message),
            Self::Scoped(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl PuffinWriteFailure {
    fn into_preparation_error(self) -> crate::iceberg::Error {
        let error = match self {
            Self::Legacy(message) => novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                message,
            ),
            Self::Scoped(error) => error,
        };
        crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, error.message())
            .with_source(error)
    }
}

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
    write_puffin_artifacts_original(
        file_io,
        puffin_path,
        snapshot_id,
        sequence_number,
        artifacts,
        None,
    )
    .await
    .map_err(|error| error.to_string())
}
type PuffinArtifactCpu<'a> = (
    &'a std::sync::Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    &'a novarocks_spi::connector::ConnectorInstanceDescriptor,
    novarocks_spi::connector::ProviderBindingEpoch,
    &'a std::sync::Arc<[StatisticsArtifactDraft]>,
);

async fn write_puffin_artifacts_original(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    artifacts: &[StatisticsArtifactDraft],
    cpu: Option<PuffinArtifactCpu<'_>>,
) -> Result<Option<StatisticsFile>, PuffinWriteFailure> {
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
    for (ordinal, artifact) in artifacts.iter().enumerate() {
        // Keep the original carrier borrowed through every async writer exit.
        // Copies made for Puffin coexist under its admission holder.
        let (blob, blob_metadata) = if let Some((cpu, descriptor, incarnation, drafts)) = cpu {
            let source =
                crate::catalog_control::statistics::scoped_cpu::StatisticsCpuOriginal::blob(
                    descriptor.clone(),
                    incarnation,
                    std::sync::Arc::clone(drafts),
                    ordinal,
                    snapshot_id,
                    sequence_number,
                );
            crate::catalog_control::statistics::scoped_cpu::run_artifacts(cpu, &source)
                .await
                .map_err(PuffinWriteFailure::Scoped)?;
            source.take_blob()
        } else {
            // Explicit legacy caller, not a fallback from scoped refusal.
            prepare_puffin_blob(artifact, snapshot_id, sequence_number)
        };
        writer
            .add(blob, crate::iceberg::puffin::CompressionCodec::None)
            .await
            .map_err(|error| format!("write Puffin artifact: {error}"))?;
        metadata.push(blob_metadata);
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

/// Allocate the statistics object with the attempt that binds its provenance.
/// An empty draft set allocates no object. Storage admission and actual writer
/// exit are owned by the supplied operation capability.
pub(crate) async fn write_puffin_artifacts_allocated(
    writer: &dyn crate::commit::model::ArtifactWriter,
    snapshot_id: i64,
    sequence_number: i64,
    artifacts: &[StatisticsArtifactDraft],
) -> Result<Option<StatisticsFile>, String> {
    writer.check_active().map_err(|error| error.to_string())?;
    if artifacts.is_empty() {
        return Ok(None);
    }
    let object = writer
        .allocate(
            crate::commit::model::ArtifactClass::Attempt,
            crate::commit::model::ArtifactKind::Statistics,
        )
        .map_err(|error| error.to_string())?;
    let statistics = write_puffin_artifacts(
        writer.file_io(),
        object.path(),
        snapshot_id,
        sequence_number,
        artifacts,
    )
    .await?;
    writer.check_active().map_err(|error| error.to_string())?;
    Ok(statistics)
}

/// Exact closed first-party materialization, with the guarded source retained
/// by the original Statistics CPU input until actual worker exit.
pub(crate) fn prepare_puffin_blob(
    artifact: &StatisticsArtifactDraft,
    snapshot_id: i64,
    sequence_number: i64,
) -> (Blob, BlobMetadata) {
    let identity = artifact.identity();
    let body = artifact.body();
    let properties = artifact.properties();
    let blob = Blob::builder()
        .r#type(identity.blob_type().to_string())
        .fields(identity.input_fields().to_vec())
        .snapshot_id(snapshot_id)
        .sequence_number(sequence_number)
        .data(body.to_vec())
        .properties(properties.clone().into_iter().collect())
        .build();
    let metadata = BlobMetadata {
        r#type: identity.blob_type().to_string(),
        snapshot_id,
        sequence_number,
        fields: identity.input_fields().to_vec(),
        properties: properties.clone().into_iter().collect(),
    };
    (blob, metadata)
}

pub(crate) async fn write_puffin_artifacts_allocated_scoped(
    writer: &dyn crate::commit::model::ArtifactWriter,
    snapshot_id: i64,
    sequence_number: i64,
    drafts: std::sync::Arc<[StatisticsArtifactDraft]>,
    cpu: std::sync::Arc<dyn novarocks_spi::connector::StatisticsCpuPort>,
    descriptor: &novarocks_spi::connector::ConnectorInstanceDescriptor,
    incarnation: novarocks_spi::connector::ProviderBindingEpoch,
) -> crate::iceberg::Result<Option<StatisticsFile>> {
    writer.check_active()?;
    if drafts.is_empty() {
        return Ok(None);
    }
    let object = writer.allocate(
        crate::commit::model::ArtifactClass::Attempt,
        crate::commit::model::ArtifactKind::Statistics,
    )?;
    let result = write_puffin_artifacts_original(
        writer.file_io(),
        object.path(),
        snapshot_id,
        sequence_number,
        &drafts,
        Some((&cpu, descriptor, incarnation, &drafts)),
    )
    .await
    .map_err(PuffinWriteFailure::into_preparation_error)?;
    writer.check_active()?;
    Ok(result)
}

#[cfg(test)]
mod scoped_refusal_tests {
    use super::*;
    use novarocks_failure_contract::{FailureFact, FailureProjection};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorMutationFailureKind,
    };

    #[derive(Debug)]
    struct OriginalRefusal(ConnectorErrorKind);
    impl std::fmt::Display for OriginalRefusal {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original bounded CPU marker")
        }
    }
    impl std::error::Error for OriginalRefusal {}
    impl FailureProjection for OriginalRefusal {
        fn failure_fact(&self) -> FailureFact {
            self.0.failure_fact()
        }
    }

    #[test]
    fn scoped_puffin_refusal_reaches_original_publication_fact_and_cause() {
        for (kind, expected) in [
            (
                ConnectorErrorKind::ResourceExhausted,
                ConnectorMutationFailureKind::ResourceExhausted,
            ),
            (
                ConnectorErrorKind::Interrupted,
                ConnectorMutationFailureKind::Interrupted,
            ),
            (
                ConnectorErrorKind::Internal,
                ConnectorMutationFailureKind::Internal,
            ),
        ] {
            let original =
                ConnectorError::from_domain(kind, OriginalRefusal(kind)).map_message(|_| {
                    "internal CPU action Begin refused: original bounded detail".into()
                });
            let error = PuffinWriteFailure::Scoped(original.clone()).into_preparation_error();
            let failure = crate::commit::attempt::before_dispatch_failure(&error);
            assert_eq!(failure.kind(), expected);
            assert_eq!(failure.failure_fact(), original.failure_fact());
            assert_eq!(failure.connector_source(), Some(&original));
            assert!(failure.message().contains("original bounded detail"));
        }
    }

    #[test]
    fn legacy_puffin_io_failure_keeps_original_unavailable_projection() {
        let error = PuffinWriteFailure::Legacy("original Puffin storage error".into())
            .into_preparation_error();
        let failure = crate::commit::attempt::before_dispatch_failure(&error);
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unavailable);
        assert_eq!(
            failure.connector_source().unwrap().message(),
            "original Puffin storage error"
        );
    }
}
