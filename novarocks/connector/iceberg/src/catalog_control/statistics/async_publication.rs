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

//! Borrowed statistics publication. The original session owns every field
//! across all polls, cancellation, panic and result/cleanup handoffs.

use super::*;
use crate::commit::attempt::observation::{PublicationJournal, RetainedPublicationProjection};
use crate::commit::attempt::prepared::CatalogPublisher;
use crate::commit::attempt::{self, Publisher, RetryPolicy};
use crate::commit::model::OperationIntent;
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::staging::{PreparedChange, Preparer};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorMetadataMaintenanceIoGeometry, ConnectorRequestContext,
    ConnectorStatisticsLease, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
use publication::{Projection, StatisticsDraftPreparer};
use std::num::{NonZeroU64, NonZeroUsize};

pub(super) enum PreparedStatisticsPublication {
    NoOp(ExternalMutationOutcome<StatisticsReceipt>),
    Active(StatisticsPublicationSource),
}

pub(super) struct StatisticsPublicationSource {
    operation: IcebergCommitOperation,
    intent: OperationIntent,
    prefix: PreparedChange,
    preparer: Arc<dyn Preparer>,
    publisher: Arc<dyn Publisher>,
    journal: PublicationJournal<StatisticsReceipt>,
    policy: RetryPolicy,
    progress: Box<StatisticsPublicationProgress>,
}

struct StatisticsPublicationProgress {
    started: bool,
    ready: Option<StatisticsPublicationReady>,
    close: Option<StatisticsPublicationClose>,
}

pub(super) fn original_io(
    context: &ConnectorRequestContext,
) -> Result<Arc<dyn ConnectorIoAdmissionPort>, ConnectorError> {
    context
        .io_admission()
        .and_then(std::sync::Weak::upgrade)
        .ok_or_else(|| {
            invalid("Async statistics collection requires its original supervised I/O source")
        })
}

pub(super) fn same_original_io(
    original: &ConnectorRequestContext,
    current: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    original_io(original)?;
    original_io(current)?;
    if !original
        .io_admission()
        .zip(current.io_admission())
        .is_some_and(|(a, b)| std::sync::Weak::ptr_eq(a, b))
    {
        return Err(invalid(
            "Statistics settlement has a foreign original I/O source",
        ));
    }
    Ok(())
}

impl StatisticsPublicationSource {
    pub(super) fn new(
        session: &StatisticsSessionBody,
        drafts: Vec<StatisticsArtifactDraft>,
        revision: StatisticsEvidenceRevision,
        cpu: std::sync::Weak<dyn novarocks_spi::connector::StatisticsCpuPort>,
    ) -> Result<Self, ConnectorError> {
        original_io(&session.context)?;
        let snapshot_id = session
            .snapshot_id
            .ok_or_else(|| corrupt("Statistics session has no measured snapshot"))?;
        let sequence_number = session
            .sequence_number
            .ok_or_else(|| corrupt("Statistics session has no measured sequence"))?;
        let projection = Projection {
            descriptor: session.provider.descriptor().clone(),
            incarnation: session.provider.incarnation(),
            operation_id: session.operation_id,
            target: session.physical_table.identifier().clone(),
            uuid: session.physical_table.metadata().uuid(),
            snapshot_id,
            sequence_number,
            data_version: session.data_version.clone(),
            revision,
        };
        let intent = projection.intent().map_err(|e| corrupt(e.to_string()))?;
        let policy = RetryPolicy::from_properties(session.physical_table.metadata().properties())
            .map_err(|e| invalid(e.to_string()))?;
        let operation = IcebergCommitOperation::new(
            intent.token(),
            session.physical_table.metadata().location(),
            session
                .provider
                .runtime()
                .resources()
                .planning_binding()
                .clone(),
            session.context.clone(),
            session
                .provider
                .runtime()
                .resources()
                .catalog_runtime()
                .clone(),
            OperationLimits::default(),
        )
        .map_err(|e| unavailable(e.to_string()))?;
        let journal = PublicationJournal::for_runtime(
            operation.token(),
            session.provider.runtime().resources().catalog_runtime(),
        );
        let original_publisher = Arc::new(CatalogPublisher {
            catalog_operation: crate::catalog::admission::CatalogOperation::Statistics,
            catalog: Arc::clone(session.provider.runtime().novarocks_catalog()),
            ident: projection.target.clone(),
            target_ref: "main".into(),
            operation: operation.token(),
            recovery_preflight: projection.preflight_callback(&journal),
        });
        let publisher = journal.observe_prepared(Arc::new(ScopedStatisticsPublisher {
            inner: original_publisher,
            projection,
            journal: journal.clone(),
            cpu: cpu.clone(),
            pending: std::sync::Mutex::new(None),
        }));
        Ok(Self::from_original(
            operation,
            intent,
            Arc::new(StatisticsDraftPreparer {
                snapshot_id,
                sequence_number,
                drafts: drafts.into(),
                cpu: Some(cpu),
                cpu_owner: Some((
                    session.provider.descriptor().clone(),
                    session.provider.incarnation(),
                )),
            }),
            Arc::new(publisher),
            journal,
            policy,
        ))
    }

    pub(super) fn from_original(
        operation: IcebergCommitOperation,
        intent: OperationIntent,
        preparer: Arc<dyn Preparer>,
        publisher: Arc<dyn Publisher>,
        journal: PublicationJournal<StatisticsReceipt>,
        policy: RetryPolicy,
    ) -> Self {
        Self {
            operation,
            intent,
            prefix: PreparedChange::default(),
            preparer,
            publisher,
            journal,
            policy,
            progress: Box::new(StatisticsPublicationProgress {
                started: false,
                ready: None,
                close: None,
            }),
        }
    }

    pub(super) async fn finish(&mut self) -> Result<StatisticsPublicationReady, ConnectorError> {
        if let Some(ready) = &self.progress.ready {
            return Ok(ready.clone());
        }
        if self.progress.started {
            return Err(invalid(
                "Statistics publication already started; observe/close its original journal",
            ));
        }
        // This original source, not the local future, records the single start.
        // Dropping a borrowing future cannot permit another publication run.
        self.progress.started = true;
        let report = attempt::run_to_committed_frontier(
            &self.operation,
            &self.intent,
            &self.prefix,
            &[self.preparer.as_ref()],
            self.publisher.as_ref(),
            self.policy,
        )
        .await;
        let ready = match self.journal.project_retained_committed_frontier(report) {
            RetainedPublicationProjection::Committed {
                effect,
                receipt,
                finalization,
            } => StatisticsPublicationReady::Committed {
                effect,
                receipt,
                finalization,
            },
            RetainedPublicationProjection::Settled(outcome) => {
                StatisticsPublicationReady::Settled(outcome)
            }
        };
        self.progress.ready = Some(ready.clone());
        Ok(ready)
    }

    pub(super) async fn close(
        &mut self,
        current: &ConnectorRequestContext,
    ) -> Result<StatisticsPublicationClose, ConnectorError> {
        same_original_io(self.operation.request_context(), current)?;
        if let Some(StatisticsPublicationClose::RetainedUnknown { .. }) = &self.progress.close {
            return Ok(self
                .progress
                .close
                .as_ref()
                .expect("observed close")
                .clone());
        }
        if self.journal.cleanup_inventory_is_disposed()
            && let Some(close) = &self.progress.close
        {
            return Ok(close.clone());
        }
        let mut uncommitted_disposed = false;
        let outcome = match &self.progress.ready {
            Some(StatisticsPublicationReady::Committed { .. })
            | Some(StatisticsPublicationReady::Settled(
                ExternalMutationOutcome::KnownCommitted { .. },
            )) => {
                self.journal
                    .settle_committed_cleanup_in(&self.operation, current)
                    .await?;
                self.journal.catalog_entry_outcome(
                    self.journal
                        .current_entry()
                        .expect("committed publication owns its entry"),
                )
            }
            Some(StatisticsPublicationReady::Settled(ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence,
            })) => ExternalMutationOutcome::CommitUnknown {
                failure: failure.clone(),
                evidence: evidence.clone(),
            },
            Some(StatisticsPublicationReady::Settled(
                ExternalMutationOutcome::KnownUncommitted {
                    failure,
                    cleanup: previous,
                },
            )) => {
                // The original runner's typed fact, not cancellation or an
                // empty receipt, proves ownership of the uncommitted objects.
                let cleanup = self
                    .operation
                    .cleanup_in(crate::commit::model::CleanupScope::EntireOperation, current)
                    .await;
                uncommitted_disposed = matches!(
                    &cleanup,
                    crate::commit::model::IcebergCleanupReport::Complete { .. }
                );
                let finalization = match previous {
                    ExternalMutationFinalization::Complete => cleanup.finalization(),
                    failed => failed.clone(),
                };
                if let Some(entry) = self.journal.current_entry() {
                    self.journal.record_entry_cleanup_report(entry, cleanup);
                }
                ExternalMutationOutcome::KnownUncommitted {
                    failure: failure.clone(),
                    cleanup: finalization,
                }
            }
            None => {
                // The same local borrowing future has exited before this &mut
                // borrow is possible. The original journal decides whether it
                // never dispatched, committed, or retained Unknown. This is
                // recovery observation only, never another publication run.
                self.progress.started = true;
                self.journal
                    .recover_bridge_in(
                        &self.operation,
                        current,
                        "Statistics publication closed before its result handoff".into(),
                    )
                    .await
            }
        };
        let close = match outcome {
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                StatisticsPublicationClose::RetainedUnknown { failure, evidence }
            }
            outcome => {
                let owned_objects_disposed = self.journal.cleanup_inventory_is_disposed()
                    || uncommitted_disposed
                    || matches!(
                        &outcome,
                        ExternalMutationOutcome::KnownUncommitted {
                            cleanup: ExternalMutationFinalization::Complete,
                            ..
                        }
                    );
                StatisticsPublicationClose::Observed {
                    outcome,
                    owned_objects_disposed,
                }
            }
        };
        self.progress.close = Some(close.clone());
        // Closing an unlaunched/interrupted source also closes the start gate.
        if self.progress.ready.is_none() {
            self.progress.ready = Some(match &close {
                StatisticsPublicationClose::Observed { outcome, .. } => {
                    StatisticsPublicationReady::Settled(outcome.clone())
                }
                StatisticsPublicationClose::RetainedUnknown { failure, evidence } => {
                    StatisticsPublicationReady::Settled(ExternalMutationOutcome::CommitUnknown {
                        failure: failure.clone(),
                        evidence: evidence.clone(),
                    })
                }
            });
        }
        Ok(close)
    }
}

/// This quotes the actual stable Session/Source and owned Arc allocations.
/// The two evidence buffers are the bounded encoding and retained projection;
/// artifact-dependent graphs are admitted separately from actual drafts.
pub(super) fn geometry(
    future_bytes: usize,
) -> Result<ConnectorMetadataMaintenanceIoGeometry, ConnectorError> {
    let arc = std::mem::size_of::<[usize; 2]>();
    let io = OperationLimits::default().io_requests as usize;
    let headers = std::mem::size_of::<IcebergStatisticsCollectionSession>()
        .checked_add(std::mem::size_of::<StatisticsPublicationProgress>())
        .and_then(|n| n.checked_add(ConnectorStatisticsLease::session_backing_bytes()))
        .and_then(|n| n.checked_add(std::mem::size_of::<StatisticsDraftPreparer>() + arc))
        .and_then(|n| n.checked_add(std::mem::size_of::<CatalogPublisher>() + arc))
        .and_then(|n| {
            n.checked_add(
                PublicationJournal::<StatisticsReceipt>::prepared_publisher_backing_bytes(),
            )
        })
        .and_then(|n| {
            n.checked_add(
                std::mem::size_of::<Projection>()
                    + std::mem::size_of::<PublicationJournal<StatisticsReceipt>>()
                    + arc,
            )
        })
        .and_then(|n| n.checked_add(future_bytes))
        // This is an independent Box pointee held by the outer prepare F.
        .and_then(|n| n.checked_add(publication::preparation_future_bytes()))
        .and_then(|n| {
            fn bytes<A, B, C, F: std::future::Future>(_: fn(A, B, C) -> F) -> usize {
                std::mem::size_of::<F>()
            }
            n.checked_add(bytes(ScopedStatisticsPublisher::preflight_original))
        })
        .and_then(|n| {
            n.checked_add(
                PublicationJournal::<StatisticsReceipt>::prepared_preflight_future_bytes(),
            )
        })
        .and_then(|n| n.checked_add(super::scoped_cpu::known_source_bytes().ok()?))
        .and_then(|n| n.checked_add(std::mem::size_of::<ScopedStatisticsPublisher>() + arc))
        .and_then(|n| n.checked_add(if cfg!(target_os = "macos") { 64 } else { 0 }))
        .and_then(|n| n.checked_add(IcebergCommitOperation::fixed_backing_bytes().ok()?))
        .and_then(|n| n.checked_add(PublicationJournal::<StatisticsReceipt>::fixed_backing_bytes()))
        .and_then(|n| {
            n.checked_add(
                PublicationJournal::<StatisticsReceipt>::retained_evidence_backing_bytes(),
            )
        })
        .and_then(|n| {
            n.checked_add(
                crate::fs_io::governed_writer_backing_bytes()
                    .ok()?
                    .checked_mul(io)?,
            )
        })
        .ok_or_else(|| internal("statistics source geometry overflow"))?;
    let initial = MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
        .checked_mul(2)
        .ok_or_else(|| internal("statistics evidence geometry overflow"))?;
    Ok(ConnectorMetadataMaintenanceIoGeometry::new(
        NonZeroUsize::new(io).ok_or_else(|| internal("statistics I/O inventory is empty"))?,
        NonZeroU64::new(
            u64::try_from(headers).map_err(|_| internal("statistics header geometry overflow"))?,
        )
        .ok_or_else(|| internal("statistics header geometry is empty"))?,
        NonZeroU64::new(
            u64::try_from(initial)
                .map_err(|_| internal("statistics evidence geometry overflow"))?,
        )
        .ok_or_else(|| internal("statistics evidence geometry is empty"))?,
    ))
}

/// Same original prepared publisher, with only its Statistics recovery phase
/// delegated to the scoped CPU. The fixed slot owns each FrozenRequest across
/// borrowed wait cancellation and worker unwind, before any dispatch callback.
struct ScopedStatisticsPublisher {
    inner: Arc<dyn crate::commit::attempt::prepared::PublicationPreparer>,
    projection: Projection,
    journal: PublicationJournal<StatisticsReceipt>,
    cpu: std::sync::Weak<dyn novarocks_spi::connector::StatisticsCpuPort>,
    pending: std::sync::Mutex<Option<Arc<super::scoped_cpu::StatisticsCpuOriginal>>>,
}
#[async_trait::async_trait]
impl crate::commit::attempt::prepared::PublicationPreparer for ScopedStatisticsPublisher {
    async fn load_target(
        &self,
        attempt: &crate::commit::operation::IcebergCommitAttempt,
    ) -> crate::iceberg::Result<crate::commit::staging::StagingBase> {
        self.inner.load_target(attempt).await
    }
    async fn load_target_with_snapshots(
        &self,
        attempt: &crate::commit::operation::IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> crate::iceberg::Result<crate::commit::staging::StagingBase> {
        self.inner.load_target_with_snapshots(attempt, mode).await
    }
    fn preflight_recovery(
        &self,
        _: &crate::commit::model::FrozenRequest,
        _: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::DataInvalid,
            "Statistics recovery requires its original scoped CPU observation",
        ))
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        crate::commit::attempt::RecoveryPreflightFuture::pending(Box::pin(
            self.preflight_original(request, operation),
        ))
    }
    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: crate::commit::model::FrozenRequest,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        Arc::clone(&self.inner)
            .prepare_dispatch(request, descriptor, generation, preparation)
            .await
    }
}

impl ScopedStatisticsPublisher {
    async fn preflight_original(
        &self,
        request: &mut Option<crate::commit::model::FrozenRequest>,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        let cpu = self.cpu.upgrade().ok_or_else(|| {
            publication::preparation_error(invalid("Statistics recovery CPU source is gone"))
        })?;
        let occupied = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|s| !s.evidence_request_returned());
        if occupied || request.is_none() {
            return Err(publication::preparation_error(invalid(
                "Statistics recovery retains its previous original request",
            )));
        }
        let source = super::scoped_cpu::StatisticsCpuOriginal::evidence(
            self.projection.clone(),
            request.take().expect("checked original request"),
            operation.clone(),
            self.journal.clone(),
        );
        let old = self
            .pending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replace(Arc::clone(&source));
        // The new actual request is stable before any old Source Drop or wake.
        drop(old);
        super::scoped_cpu::run_evidence(&cpu, &source)
            .await
            .map_err(publication::preparation_error)?;
        source.install_evidence_and_return_request(request)
    }
}
