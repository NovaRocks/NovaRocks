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

//! Single-dispatch commit mechanisms shared by the concrete catalogs.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::{Catalog, TableCommit, TableIdent};

use super::transaction::{CatalogCommitDispatch, CommitProof};

/// Publishes to an existing table with exactly one `update_table`.
///
/// This is the plain path all three catalogs share. It deliberately does not
/// go through the vendored `Transaction::commit`, which would retry on
/// `Error::retryable()` and therefore resend a request whose outcome is
/// unknown.
///
/// Adjudication looks for the publication's own snapshot-summary marker. It is
/// keyed on the exact marker rather than on a snapshot id, because a snapshot
/// id says only "something committed" while the marker says "*this* attempt
/// committed". Absence is never treated as proof.
#[derive(Debug)]
// No production caller yet: this is the update-commit half of the lifecycle,
// wired when `commit/**` migrates off `vendored_client`.
#[allow(dead_code)]
pub(super) struct UpdateTableDispatch {
    client: Arc<dyn Catalog>,
    ident: TableIdent,
    /// Property key and value that identify this exact publication, when the
    /// operation stamps one. Without it, adjudication cannot answer and must
    /// keep saying "unknown" rather than guess.
    marker: Option<(Arc<str>, Arc<str>)>,
}

#[allow(dead_code)]
impl UpdateTableDispatch {
    pub(super) fn new(
        client: Arc<dyn Catalog>,
        ident: TableIdent,
        marker: Option<(Arc<str>, Arc<str>)>,
    ) -> Self {
        Self {
            client,
            ident,
            marker,
        }
    }
}

#[async_trait]
impl CatalogCommitDispatch for UpdateTableDispatch {
    async fn dispatch_once(
        &self,
        staged: Option<TableCommit>,
    ) -> Result<CommitProof, crate::iceberg::Error> {
        let Some(staged) = staged else {
            return Ok(CommitProof::no_op());
        };
        if staged.is_empty() {
            // Nothing to publish, and nothing was sent. This is a proven no-op
            // rather than a commit, and it must not reach the catalog.
            return Ok(CommitProof::no_op());
        }
        if staged.identifier() != &self.ident {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "staged Iceberg commit target does not match the admitted publication target",
            ));
        }
        let table = self.client.update_table(staged).await?;
        let snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        Ok(CommitProof::applied(snapshot_id).with_table_uuid(table.metadata().uuid().to_string()))
    }

    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
        let Some((key, value)) = &self.marker else {
            // No marker was stamped, so this publication left nothing that
            // distinguishes it from any other. Refusing to answer is the only
            // honest result; claiming absence here would authorize cleanup of
            // data that may be live.
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "this Iceberg publication stamped no operation marker, so its outcome cannot be \
                 adjudicated after the fact",
            ));
        };
        let table = self
            .client
            .load_table(&self.ident)
            .await
            .map_err(|error| super::error::map_read_error(&error))?;
        let table_uuid: Arc<str> = Arc::from(table.metadata().uuid().to_string());
        let mut matched = None;
        for snapshot in table.metadata().snapshots() {
            if snapshot.summary().additional_properties.get(key.as_ref())
                == Some(&value.to_string())
            {
                if matched.is_some() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "more than one Iceberg snapshot carries the same publication marker",
                    ));
                }
                matched = Some(snapshot.snapshot_id());
            }
        }
        Ok(matched.map(|snapshot_id| {
            CommitProof::applied(Some(snapshot_id)).with_table_uuid(Arc::clone(&table_uuid))
        }))
    }
}

/// Creates a table with exactly one `create_table`.
///
/// Used by catalogs whose create is already a single atomic catalog request.
/// The Hadoop implementation does not use this: its linearization point is a
/// conditional metadata write in storage (ADR-0077), not a catalog call.
#[derive(Debug)]
pub(super) struct CreateTableDispatch {
    client: Arc<dyn Catalog>,
    namespace: crate::iceberg::NamespaceIdent,
    creation: std::sync::Mutex<Option<crate::iceberg::TableCreation>>,
    ident: TableIdent,
}

impl CreateTableDispatch {
    pub(super) fn new(
        client: Arc<dyn Catalog>,
        namespace: crate::iceberg::NamespaceIdent,
        creation: crate::iceberg::TableCreation,
        ident: TableIdent,
    ) -> Self {
        Self {
            client,
            namespace,
            creation: std::sync::Mutex::new(Some(creation)),
            ident,
        }
    }
}

#[async_trait]
impl CatalogCommitDispatch for CreateTableDispatch {
    async fn dispatch_once(
        &self,
        _staged: Option<TableCommit>,
    ) -> Result<CommitProof, crate::iceberg::Error> {
        let creation = self
            .creation
            .lock()
            .map_err(|_| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "Iceberg create-table dispatch state was poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                // The transaction already refuses a second commit; this is the
                // belt-and-braces guard at the mechanism itself.
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "Iceberg create-table dispatch was already consumed",
                )
            })?;
        let table = self.client.create_table(&self.namespace, creation).await?;
        Ok(CommitProof::applied(
            table
                .metadata()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
        )
        .with_table_uuid(table.metadata().uuid().to_string()))
    }

    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
        // A create is adjudicated by presence of the target itself. Any table
        // at the identity proves *a* create landed; whether it was this attempt
        // is settled by the caller comparing the expected UUID, which is why
        // the UUID travels in the proof.
        match self.client.load_table(&self.ident).await {
            Ok(table) => Ok(Some(
                CommitProof::applied(
                    table
                        .metadata()
                        .current_snapshot()
                        .map(|snapshot| snapshot.snapshot_id()),
                )
                .with_table_uuid(table.metadata().uuid().to_string()),
            )),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::iceberg::ErrorKind::TableNotFound
                        | crate::iceberg::ErrorKind::NamespaceNotFound
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(super::error::map_read_error(&error)),
        }
    }
}

/// Publishes a prepared conditional create with exactly one storage request.
///
/// The attempt is consumed on first dispatch, so a second commit cannot resend
/// it even if the transaction's own guard were bypassed.
#[derive(Debug)]
pub(super) struct ConditionalCreateDispatch {
    /// The filesystem client that owns the conditional-create primitive. It
    /// stays inside this module, which is the whole point of the boundary.
    client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
    attempt: std::sync::Mutex<Option<crate::hadoop_catalog::HadoopCreateAttempt>>,
    evidence: super::ConditionalCreateEvidence,
}

impl ConditionalCreateDispatch {
    pub(super) fn new(
        client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
        attempt: crate::hadoop_catalog::HadoopCreateAttempt,
        evidence: super::ConditionalCreateEvidence,
    ) -> Self {
        Self {
            client,
            attempt: std::sync::Mutex::new(Some(attempt)),
            evidence,
        }
    }
}

#[async_trait]
impl CatalogCommitDispatch for ConditionalCreateDispatch {
    async fn dispatch_once(
        &self,
        _staged: Option<TableCommit>,
    ) -> Result<CommitProof, crate::iceberg::Error> {
        let attempt = self
            .attempt
            .lock()
            .map_err(|_| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "conditional-create dispatch state was poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "conditional-create dispatch was already consumed",
                )
            })?;
        match self.client.publish_create_attempt(attempt).await {
            // The digest comes from the publish result, which re-read the
            // metadata after writing it. That is what makes it authoritative
            // rather than an echo of what was sent.
            Ok(result) => Ok(CommitProof::new(match result.disposition {
                crate::hadoop_catalog::HadoopCreateDisposition::Created => {
                    novarocks_spi::connector::ExternalMutationEffect::Applied
                }
                crate::hadoop_catalog::HadoopCreateDisposition::Existing => {
                    novarocks_spi::connector::ExternalMutationEffect::NoOp
                }
            })
            .with_table_uuid(result.authoritative_table_uuid)
            .with_metadata(
                result
                    .table
                    .metadata_location()
                    .unwrap_or(self.evidence.metadata_location.as_ref())
                    .to_string(),
                result.authoritative_metadata_digest,
            )),
            // Map onto kinds `proves_uncommitted` recognises so the
            // transaction's own classification keeps each verdict: only
            // `Unknown` may leave the outcome unknown.
            Err(failure) => Err(match failure.kind {
                crate::hadoop_catalog::HadoopCreateFailureKind::Unsupported => {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::FeatureUnsupported,
                        failure.message,
                    )
                }
                crate::hadoop_catalog::HadoopCreateFailureKind::Invalid => {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        failure.message,
                    )
                }
                crate::hadoop_catalog::HadoopCreateFailureKind::Uncommitted => {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::PreconditionFailed,
                        failure.message,
                    )
                }
                crate::hadoop_catalog::HadoopCreateFailureKind::Unknown => {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::Unexpected,
                        failure.message,
                    )
                }
            }),
        }
    }

    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
        let verdict = self
            .client
            .reconcile_create_attempt(
                &self.evidence.namespace,
                &self.evidence.table,
                &self.evidence.expected_table_uuid,
                &self.evidence.metadata_location,
                &self.evidence.metadata_digest,
            )
            .await
            .map_err(|error| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    error,
                )
            })?;
        match verdict {
            crate::hadoop_catalog::HadoopCreateReconciliation::Committed { .. } => Ok(Some(
                CommitProof::new(novarocks_spi::connector::ExternalMutationEffect::Applied)
                    .with_table_uuid(Arc::clone(&self.evidence.expected_table_uuid))
                    .with_metadata(
                        Arc::clone(&self.evidence.metadata_location),
                        Arc::clone(&self.evidence.metadata_digest),
                    ),
            )),
            // Absent proves nothing, and a foreign target proves this attempt is
            // not what is there -- neither upgrades the verdict.
            crate::hadoop_catalog::HadoopCreateReconciliation::Absent
            | crate::hadoop_catalog::HadoopCreateReconciliation::Foreign => Ok(None),
        }
    }
}
