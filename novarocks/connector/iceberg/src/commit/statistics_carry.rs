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

//! Byte-for-byte statistics carry during the original Replace attempt.

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use super::model::{ArtifactWriter, OperationIntent, ReusableStatisticsFile, StatisticsReuseBasis};
use super::operation::IcebergCommitOperation;
use super::staging::{PreparedChange, Preparer, StagedView};
use crate::iceberg::spec::{Operation, StatisticsFile};
use crate::iceberg::{Error, ErrorKind, Result, TableUpdate};

pub(crate) struct CarryForwardStatisticsPreparer<'a> {
    operation: &'a IcebergCommitOperation,
    replace_stage: usize,
    author: &'a ConnectorPayloadRetentionGuard,
}

impl<'a> CarryForwardStatisticsPreparer<'a> {
    pub(crate) fn new(
        operation: &'a IcebergCommitOperation,
        replace_stage: usize,
        author: &'a ConnectorPayloadRetentionGuard,
    ) -> Self {
        Self {
            operation,
            replace_stage,
            author,
        }
    }
}

#[async_trait]
impl Preparer for CarryForwardStatisticsPreparer<'_> {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let artifacts = view.artifacts();
        if intent.token() != self.operation.token()
            || artifacts.attempt_token().operation() != self.operation.token()
            || view.stage_key().checked_sub(1) != Some(self.replace_stage)
        {
            return Err(failure(
                ConnectorErrorKind::Internal,
                "Iceberg statistics lost its original Replace attempt",
            ));
        }
        let snapshot_id = self
            .operation
            .prepared_statistics_target(self.replace_stage)?;
        let target = view
            .metadata()
            .snapshot_for_ref(view.target_ref())
            .filter(|target| {
                target.snapshot_id() == snapshot_id
                    && target.summary().operation == Operation::Replace
            })
            .ok_or_else(|| {
                failure(
                    ConnectorErrorKind::Internal,
                    "Iceberg statistics has no staged Replace on the target ref",
                )
            })?;
        let Some(source) = crate::statistics_ancestry::first_registered_statistics(
            view.metadata(),
            target.parent_snapshot_id(),
            || artifacts.check_active(),
        )?
        else {
            return Ok(PreparedChange::default());
        };

        // The original Root owns already retained statement results. Reserve
        // these added allocations against that same byte author, never against
        // the previous round's high-water mark.
        let footprint = crate::stats_assembler::statistics_carry_backing(
            source,
            self.operation.statistics_artifact_path_bound()?,
        )?;
        let registration = self.reserve(footprint.registration)?;
        let copy_backing = self.reserve(footprint.reuse)?;
        let basis = StatisticsReuseBasis {
            stage: self.replace_stage,
            source: source.into(),
            stage_snapshot_id: snapshot_id,
        };
        let copy = match artifacts.find_reusable_statistics(&basis)? {
            Some(copy) => copy,
            None => {
                let copy = self.copy(artifacts, source, snapshot_id).await?;
                self.operation
                    .remember_paid_statistics_copy(basis, copy.clone(), copy_backing)?;
                copy
            }
        };
        artifacts.check_active()?;

        let statistics = crate::stats_assembler::carried_statistics_registration(source, &copy);
        self.operation
            .retain_statistics_registration(artifacts.attempt_token(), registration)?;
        Ok(PreparedChange {
            requirements: Vec::new(),
            updates: vec![TableUpdate::SetStatistics { statistics }],
        })
    }
}

impl CarryForwardStatisticsPreparer<'_> {
    fn reserve(&self, bytes: usize) -> Result<ConnectorPayloadRetentionGuard> {
        self.author.reserve_owned_bytes(bytes).map_err(|error| {
            Error::new(
                ErrorKind::Unexpected,
                "Iceberg statistics backing admission refused",
            )
            .with_source(error)
        })
    }

    async fn copy(
        &self,
        artifacts: &dyn ArtifactWriter,
        source: &StatisticsFile,
        snapshot_id: i64,
    ) -> Result<ReusableStatisticsFile> {
        let geometry = crate::stats_assembler::PuffinCopyGeometry::from_registration(source)?;
        let input = artifacts.file_io().new_input(&source.statistics_path)?;
        if input.metadata().await?.size != geometry.file_bytes() {
            return Err(failure(
                ConnectorErrorKind::CorruptData,
                "Iceberg statistics file size differs from its registration",
            ));
        }
        let length = usize::try_from(geometry.file_bytes()).map_err(|_| backing_overflow())?;
        let scratch = length.checked_mul(2).ok_or_else(backing_overflow)?;
        let backing = self.reserve(scratch)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| backing_overflow())?;
        bytes.resize(length, 0);
        self.operation
            .read_statistics_into(&source.statistics_path, geometry.file_bytes(), &mut bytes)
            .await?;

        // This owner follows the actual buffer into the writer, including its
        // last alias during original stream settlement or failure cleanup.
        let bytes = Bytes::from_owner(ChargedPuffin {
            bytes,
            _backing: backing,
        });
        geometry.verify(&bytes, source.key_metadata.as_deref())?;
        artifacts.check_active()?;

        // The exact range I/O and its permit exited before opening this writer.
        let object = artifacts.allocate_reusable_statistics()?;
        let mut writer = artifacts
            .file_io()
            .new_output(object.path())?
            .writer()
            .await?;
        writer.write(bytes).await?;
        let closed = writer.close().await?;
        if closed.size != geometry.file_bytes() {
            return Err(failure(
                ConnectorErrorKind::CorruptData,
                "Iceberg statistics output size differs from the copied bytes",
            ));
        }
        artifacts.check_active()?;
        Ok(ReusableStatisticsFile {
            object,
            snapshot_id,
            file_size_in_bytes: source.file_size_in_bytes,
            file_footer_size_in_bytes: source.file_footer_size_in_bytes,
        })
    }
}

struct ChargedPuffin {
    bytes: Vec<u8>,
    _backing: ConnectorPayloadRetentionGuard,
}

impl AsRef<[u8]> for ChargedPuffin {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn failure(kind: ConnectorErrorKind, message: &'static str) -> Error {
    Error::new(ErrorKind::Unexpected, message).with_source(ConnectorError::new(kind, message))
}

fn backing_overflow() -> Error {
    failure(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg statistics copy backing is not representable",
    )
}
