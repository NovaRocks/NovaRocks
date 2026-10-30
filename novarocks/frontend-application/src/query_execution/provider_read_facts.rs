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

//! Negotiating and freezing one query's provider reads.
//!
//! This is the fourth kind of question the compiler asks, and the only one
//! that is not a lookup. The other three ask an owner what is already true;
//! this one asks a provider what it will take on, and then commits it. What it
//! commits is what the plan is built against, so it happens exactly once per
//! scan and cannot be re-asked later with a different answer.
//!
//! Negotiation offers the whole list at once - projection, then filter, then
//! limit - because order is part of the question: a limit means one thing above
//! a filter and another below it. The provider answers each offer in turn, and
//! only an answer that says `Exact` relieves the engine of that work.
//!
//! Freezing yields two things of different kinds, and they leave here by
//! different doors. The static facts become the read's contract and travel with
//! the plan to every worker. The capability to perform the read belongs to this
//! attempt alone: it is deposited the moment it is taken and never appears in a
//! fact, a plan, or anything encoded.

mod control_binding;
mod error;
use control_binding::ProviderCreatorControlBinding;
pub(crate) use error::{ProviderReadCpuFailure, ProviderReadError};

use novarocks_query_application::api::RuntimeSplitPlacement;

#[cfg(test)]
use super::internal_result_cpu::InternalResultCpu;
use super::internal_result_cpu::{ProviderCreatorCpuSource, ProviderReadCpuJob};
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoAdmission, ConnectorBlockingIoResponsibility,
};
use crate::task_execution::status_intake::StatusIntakeWake;
use futures::task::AtomicWaker;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_physical_plan::{
    ExactInputVersion, NullOrdering, PredicateGuaranteeKind, ProviderColumnReference,
    ProviderReadReference, SortDirection,
};
use novarocks_query_application::preparation::{
    FrozenReadAccess, ProviderReadFactPort, ReadAccessDeposit, ReadAccessSink,
};
use novarocks_spi::connector::{
    CatalogProperties, ConnectorControlPlanningLease, ConnectorControlReadBinding, ConnectorError,
    ConnectorPlanningContext, ConnectorReadAttemptAccess, ConnectorReadWireEncoder,
    ConnectorRequestContext,
    read_stack::{
        ConnectorExpression, ConnectorReadColumnBinding, ConnectorReadColumnHandle,
        ConnectorReadConstraint, ConnectorReadDistribution, ConnectorReadMetadata,
        ConnectorReadMetadataKind, ConnectorReadMetadataRequest, ConnectorReadMetadataVersion,
        ConnectorReadNullOrdering, ConnectorReadProperties, ConnectorReadPublicSchema,
        ConnectorReadRelationVersion, ConnectorReadRequestControl, ConnectorReadSortDirection,
        ConnectorReadStaticFacts, ConnectorReadTableHandle, ConnectorSession, Constraint,
        SchemaTableName, TupleDomain,
        negotiation::{
            ReadFreezeRequest, ReadNegotiation, ReadPushdownDisposition, ReadPushdownOp,
            ReadPushdownOutcome,
        },
        runtime::ConnectorReadAssignment,
    },
};
use novarocks_sql::compiler::{
    ProviderReadColumnFact, ProviderReadColumnNeed, ProviderReadDistribution, ProviderReadFact,
    ProviderReadLimitFact, ProviderReadNeed, ProviderReadOrderingKey, ProviderReadPredicateFact,
    ProviderReadProperties, ProviderReadRelationNeed, ProviderReadRequestBinding,
    ProviderReadStaticContract, ProviderReadVersionNeed,
};
use novarocks_sql::planning::catalog::MetadataTableKind;
use novarocks_types::naming::TableIdentity;

use crate::catalog_application::query_bindings::{QueryFrozenReadInput, QueryTableBindingStore};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;

/// Variable names offered to a provider for one read's projection.
///
/// They are positional on purpose: the provider is being asked about the
/// request's own dense ordinals, not about anything the query calls a column.
const READ_VARIABLE_PREFIX: &str = "r";

/// What this role keeps for one frozen read.
///
/// A freeze leaves two things behind that the plan itself must not carry, and
/// they are kept together because they are accounted for together: each belongs
/// to exactly one scan occurrence, each is useless without the plan, and the
/// pairing that proves every scan has a capability proves every scan has its
/// encoding too.
///
/// They are spent differently. The capability is released when the attempt
/// ends. The encoding is read once, when the plan is put on the wire, and dies
/// with the plan version.
pub(crate) struct FrozenProviderRead {
    pub(crate) access: ConnectorReadAttemptAccess,
    /// The generation this read was frozen against, held open for as long as
    /// the plan may run. Releasing it earlier would let the provider retire a
    /// generation a frozen read still names.
    pub(crate) generation: ConnectorControlPlanningLease,
    pub(crate) catalog: CatalogProperties,
    pub(crate) encoding: FrozenReadEncoding,
}

/// The provider-side half of one scan's wire form.
///
/// These values exist only while a read is being negotiated: the assignments
/// name provider columns, and the two domains are what the provider said it
/// would and would not enforce. Nothing downstream can recover them from the
/// plan, so they are kept from the moment the read stops being negotiable.
#[derive(Clone)]
pub(crate) struct FrozenReadEncoding {
    /// Each provider column this read projects, paired with how the request
    /// named it. The plan addresses these columns by provider reference; what
    /// a column is *called* is SQL-local and survives only here, so the pairing
    /// is kept rather than the two halves separately - matching them again by
    /// position later is how a rename becomes a mislabelled column.
    pub(crate) columns: Box<[(ProviderColumnReference, ProviderReadColumnNeed)]>,
    pub(crate) assignments: Vec<ConnectorReadAssignment>,
    /// The constraint the provider was offered.
    ///
    /// Splits are enumerated against what was offered, not against what the
    /// provider took on: a provider that declined still prunes by whatever it
    /// can, and handing it less than it was asked about would enumerate more
    /// work than the query needs.
    pub(crate) offered_constraint: ConnectorReadConstraint,
    /// What the provider guarantees. The engine may stop evaluating it.
    pub(crate) enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    /// What the reader must still apply itself.
    pub(crate) unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    pub(crate) remaining_expression: Option<ConnectorExpression>,
    pub(crate) placement: RuntimeSplitPlacement,
    /// The provider's own static facts, exactly as it froze them.
    ///
    /// The physical contract and compiled program retain the provider's own
    /// distribution. Work-unit hints remain separate frontend scheduling facts.
    pub(crate) static_facts: ConnectorReadStaticFacts<ConnectorReadColumnHandle>,
    /// The public fields of the assignment columns, one per assignment in
    /// assignment order, as the provider that owns them published them; or
    /// why it would not.
    ///
    /// Only a compiled program carries these fields, and a provider may
    /// decline to publish them for a relation family it does not program.
    /// The refusal is kept rather than raised here so the compiled-package
    /// owner reports the provider's own reason.
    pub(crate) public_schema: Result<ConnectorReadPublicSchema, ConnectorError>,
}

/// Freezes each scan of one statement with its provider.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ProviderCreatorMode {
    BindingOnly,
    GovernedReadIo,
}
pub(crate) struct FrontendProviderReadFacts {
    creator_mode: ProviderCreatorMode,
    control: Arc<ConnectorControlHost>,
    bindings: Arc<QueryTableBindingStore>,
    session: ConnectorSession,
    context: ConnectorRequestContext,
    blocking: ConnectorBlockingIoSupervisor,
    capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    source: tokio::sync::Mutex<ProviderCreatorSource>,
    closing: AtomicBool,
    wake: Arc<ProviderCreatorWake>,
    #[cfg(test)]
    fixture_cpu: Option<InternalResultCpu>,
}

#[derive(Default)]
struct ProviderCreatorSource {
    creator_request: Option<novarocks_spi::connector::ConnectorIoRequest>,
    creator_permit: Option<novarocks_spi::connector::ConnectorIoPermit>,
    control_binding: ProviderCreatorControlBinding,
    admission: Option<ConnectorBlockingIoAdmission>,
    responsibility: Option<ConnectorBlockingIoResponsibility>,
    prepared: Option<PreparedProviderRead>,
    job: Option<ProviderReadCpuJob>,
    result: Option<Result<ProviderReadFact, ProviderReadError>>,
}
#[derive(Default)]
struct ProviderCreatorWake(AtomicWaker);
impl std::fmt::Debug for ProviderCreatorWake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderCreatorWake")
    }
}
impl StatusIntakeWake for ProviderCreatorWake {
    fn wake(&self) {
        self.0.wake();
    }
}

impl FrontendProviderReadFacts {
    pub(crate) fn binding_only(
        control: Arc<ConnectorControlHost>,
        bindings: Arc<QueryTableBindingStore>,
        session: ConnectorSession,
        context: ConnectorRequestContext,
        blocking: ConnectorBlockingIoSupervisor,
        capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Self {
        Self {
            creator_mode: ProviderCreatorMode::BindingOnly,
            control,
            bindings,
            session,
            context,
            blocking,
            capacity,
            source: tokio::sync::Mutex::new(ProviderCreatorSource::default()),
            closing: AtomicBool::new(false),
            wake: Arc::new(ProviderCreatorWake::default()),
            #[cfg(test)]
            fixture_cpu: None,
        }
    }
    pub(super) fn with_creator_mode(mut self, mode: ProviderCreatorMode) -> Self {
        self.creator_mode = mode;
        self
    }
    /// Explicit unqualified Counted fixture. Production never takes this path.
    #[cfg(test)]
    pub(crate) fn for_cpu_fixture(mut self, cpu: InternalResultCpu) -> Self {
        self.fixture_cpu = Some(cpu);
        self
    }
    pub(crate) fn begin_local_close(&self) {
        self.closing.store(true, Ordering::Release);
        self.wake.wake();
    }
    /// This borrows the same source/job. Cancelling this wait never detaches or
    /// replaces its original worker receipt or forwarder JoinHandle.
    pub(crate) async fn join_local_closed(
        &self,
    ) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        self.begin_local_close();
        let mut source = self.source.lock().await;
        if let Some(job) = source.job.as_mut() {
            job.close().await?;
        }
        source.job.take();
        source.prepared.take();
        source.admission.take();
        source.responsibility.take();
        drop(source.creator_request.take());
        drop(source.creator_permit.take());
        source.control_binding.after_source_exit();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) async fn fixture_worker_failure_is<T: std::any::Any>(&self) -> bool {
        self.source
            .lock()
            .await
            .job
            .as_ref()
            .is_some_and(ProviderReadCpuJob::fixture_worker_failure_is::<T>)
    }
    /// Return the SAME joined job to the fixture for explicit disposition.
    /// Refusal preserves the original field; no foreign Drop runs in this lock.
    #[cfg(test)]
    pub(crate) async fn take_joined_for_fixture_disposition(
        &self,
        observed: &novarocks_query_application::api::StatementNativeHealthFailure,
    ) -> Result<super::internal_result_cpu::ProviderCreatorFixtureDisposition, &'static str> {
        let mut source = self.source.lock().await;
        source
            .job
            .as_mut()
            .ok_or("fixture has no original creator job")?
            .take_joined_for_fixture_disposition(observed)
    }
}

impl FrontendProviderReadFacts {
    /// What a freeze leaves this role: the ability to reacquire exactly the
    /// handle that was frozen, for one attempt, and the provider-side half of
    /// that read's wire form. Neither holds a secret and neither offers a way
    /// to choose a different read.
    pub(crate) async fn resolve_provider_reads_typed(
        &self,
        needs: &[ProviderReadNeed],
        taken: &ReadAccessSink<FrozenProviderRead>,
    ) -> Result<Vec<ProviderReadFact>, ProviderReadError> {
        let mut facts = Vec::with_capacity(needs.len());
        // One original Source serializes its pure creators. This private body
        // lock is never acquired by the worker; no WC/Core/registry lock spans
        // Provider calls, publication, notification, actual Drop or an await.
        let mut source = self.source.lock().await;
        for need in needs {
            if self.closing.load(Ordering::Acquire) {
                return Err("provider read source is closing".into());
            }
            // An interrupted previous borrow is not another request. Its same
            // job/output/cause must be observed/closed by this original owner.
            if source.job.is_some() || source.prepared.is_some() {
                if let Some(cause) = source
                    .job
                    .as_ref()
                    .and_then(ProviderReadCpuJob::health_failure)
                {
                    return Err(ProviderReadError::ActualExit(
                        novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                            "completion Provider creator actual exit is unproven", cause,
                        )
                    ));
                }
                return Err("provider read source has an unresolved creator".into());
            }
            let cpu = {
                #[cfg(test)]
                if let Some(cpu) = &self.fixture_cpu {
                    cpu.clone()
                } else {
                    self.blocking
                        .provider_creator_cpu()
                        .map_err(ProviderReadError::from)?
                }
                #[cfg(not(test))]
                self.blocking
                    .provider_creator_cpu()
                    .map_err(ProviderReadError::from)?
            };
            let fixture = {
                #[cfg(test)]
                {
                    self.fixture_cpu.is_some()
                }
                #[cfg(not(test))]
                {
                    false
                }
            };
            if fixture {
                source.responsibility = Some(
                    ConnectorBlockingIoResponsibility::admit(
                        self.capacity.scope(),
                        &self.capacity.window_alias(),
                    )
                    .map_err(ProviderReadError::from)?,
                );
            } else {
                // Catalog resolution already retained this exact admitted
                // generation. Read only its pure route facts; do not construct
                // a request-bound control or perform Provider I/O yet.
                let admitted = self.bindings.binding(need.binding())?;
                let lease = admitted.admission.exact_planning_lease()?;
                let read = self
                    .control
                    .typed_read_for_planning_lease(&lease)
                    .map_err(ProviderReadError::from)?;
                let factory = read.request_factory().ok_or_else(|| {
                    ProviderReadError::Contract({
                        "provider creator has no exact request control factory".to_owned()
                    })
                })?;
                if self.creator_mode == ProviderCreatorMode::GovernedReadIo {
                    let io = self
                        .context
                        .io_admission()
                        .and_then(|p| p.upgrade())
                        .ok_or_else(|| {
                            ProviderReadError::Contract({
                                "read Provider source has no original I/O owner".to_owned()
                            })
                        })?;
                    let mut dependencies = Vec::new();
                    factory
                        .visit_planning_dependencies(&mut |fact| {
                            if !dependencies.contains(fact) {
                                dependencies.push(fact.clone());
                            }
                            Ok(())
                        })
                        .map_err(ProviderReadError::from)?;
                    let profile =
                    novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::from_geometry(
                        factory.planning_io_geometry().map_err(ProviderReadError::from)?,
                        dependencies,
                    )
                    .map_err(ProviderReadError::from)?;
                    io.configure_read_source(&profile)
                        .map_err(ProviderReadError::from)?;
                    std::future::poll_fn(|cx| io.poll_read_source_ready(cx))
                        .await
                        .map_err(ProviderReadError::from)?;
                    // Exact factory service facts are joined into the SAME Root
                    // payload/action D before the child can retain this creator.
                    for dependency in profile.initial_dependencies() {
                        source.creator_request = Some(
                            io.request(dependency.clone())
                                .map_err(ProviderReadError::from)?,
                        );
                        std::future::poll_fn(|cx| {
                            let ProviderCreatorSource {
                                creator_request,
                                creator_permit,
                                ..
                            } = &mut *source;
                            creator_request
                                .as_ref()
                                .expect("same creator request")
                                .poll_admitted_in(creator_permit, cx)
                        })
                        .await
                        .map_err(ProviderReadError::from)?;
                        drop(source.creator_request.take());
                        drop(source.creator_permit.take());
                    }
                }
                std::future::poll_fn(|cx| {
                    source
                        .control_binding
                        .poll(self.capacity.scope(), factory.as_ref(), cx)
                })
                .await?;
                if source.admission.is_none() {
                    source.admission = Some(
                        ConnectorBlockingIoResponsibility::request(
                            self.capacity.scope(),
                            &self.capacity.window_alias(),
                        )
                        .map_err(ProviderReadError::from)?,
                    );
                }
                source.responsibility = Some(
                    source
                        .admission
                        .as_mut()
                        .expect("same child request")
                        .ready()
                        .await
                        .map_err(ProviderReadError::from)?,
                );
                source.admission.take();
            }
            // Original admission precedes need/context/deposit capture. The
            // original opening remains in this source across async prepare.
            source.prepared = Some(begin_one_read(
                need.clone(),
                self.control.as_ref(),
                self.bindings.as_ref(),
                &self.context,
            )?);
            prepare_observation(
                source.prepared.as_mut().expect("stable opening"),
                &self.session,
            )
            .await?;
            let responsibility = source.responsibility.as_ref().expect("same original child");
            let scope = responsibility.scope().map_err(ProviderReadError::from)?;
            #[cfg(test)]
            let action = if fixture {
                ProviderCreatorCpuSource::UnqualifiedFixture
            } else {
                ProviderCreatorCpuSource::Bound(
                    cpu.provider_creator_source(&scope)
                        .map_err(ProviderReadError::from)?,
                )
            };
            #[cfg(not(test))]
            let action = ProviderCreatorCpuSource::Bound(
                cpu.provider_creator_source(&scope)
                    .map_err(ProviderReadError::from)?,
            );
            let window = responsibility.window().map_err(ProviderReadError::from)?;
            let backing = responsibility.retain_backing();
            let cancellation =
                novarocks_query_application::cancellation::QueryCancellationView::governed(
                    scope.cancellation().map_err(ProviderReadError::from)?,
                    None,
                );
            let input = ProviderReadCreatorInput::new(
                source.prepared.take().expect("stable prepared input"),
                self.session.clone(),
                self.context.clone(),
                taken.deposits(),
            );
            source.job = Some(cpu.submit_provider_creator(
                input,
                window,
                backing,
                action,
                cancellation,
                &tokio::runtime::Handle::current(),
                self.wake.clone(),
            ));
            let ProviderCreatorSource { job, result, .. } = &mut *source;
            std::future::poll_fn(|cx| {
                self.wake.0.register(cx.waker());
                job.as_mut()
                    .expect("stable original CPU job")
                    .poll_finish_in(cx, result)
            })
            .await
            .map_err(|cause| ProviderReadError::ActualExit(
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "completion Provider creator actual exit is unproven", cause,
                )
            ))?;
            // Result is already stable before input/worker retirement callbacks.
            // Only a genuinely joined, healthy original job is removed here.
            source.job.take();
            source.responsibility.take();
            drop(source.creator_request.take());
            drop(source.creator_permit.take());
            // Only after the original worker/input/cause and child actually
            // exit may the SAME Source prepare its next need's binding.
            source.control_binding.after_source_exit();
            facts.push(
                source
                    .result
                    .take()
                    .expect("same original result receiver")?,
            );
        }
        Ok(facts)
    }
}

/// What this request admitted for the relation one read names.
///
/// The two are not interchangeable. A relation is admitted as a
/// materialization -- a table, a schema and the generation it was resolved
/// through -- and the version the read names picks which one. A cohort is
/// admitted as itself: the files or the group *are* the read, and the only
/// thing a name and a version would select is a different read.
enum AdmittedRead {
    Relation(crate::catalog_application::query_bindings::QueryScanMaterialization),
    PreparedSource(Arc<crate::catalog_application::query_bindings::QueryPreparedSourceRead>),
    MvTarget {
        materialization: crate::catalog_application::query_bindings::QueryScanMaterialization,
        object_id: novarocks_spi::connector::ConnectorTableObjectId,
    },
    Cohort(crate::catalog_application::query_bindings::QueryFrozenCohortRead),
}

impl AdmittedRead {
    /// The generation this read was admitted through, and the only one it may
    /// be frozen through.
    fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        match self {
            Self::Relation(materialization)
            | Self::MvTarget {
                materialization, ..
            } => &materialization.planning_lease,
            Self::Cohort(cohort) => cohort.planning_lease(),
            Self::PreparedSource(source) => &source.planning,
        }
    }

    /// The relation to ask the provider about.
    ///
    /// For a relation read that is the name the plan carries, put back
    /// together with the metadata suffix when the read names a metadata
    /// relation. For a cohort it is the provider's own name for the relation
    /// the cohort was frozen from: the name in the plan is query-local and
    /// synthetic, so it names nothing a provider could resolve.
    fn provider_relation(
        &self,
        relation: &ProviderReadRelationNeed,
    ) -> Result<SchemaTableName, ProviderReadError> {
        use crate::catalog_application::query_bindings::QueryFrozenCohortRead;

        let (namespace, table) = match self {
            Self::PreparedSource(source) => return Ok(source.receipt.request().name().clone()),
            Self::Relation(_) | Self::MvTarget { .. } => {
                let identity = relation_identity(relation);
                (
                    identity.namespace.clone(),
                    provider_relation_name(relation)?,
                )
            }
            Self::Cohort(QueryFrozenCohortRead::PinnedFileSet(read)) => (
                read.pinned.namespace().to_string(),
                read.pinned.table().to_string(),
            ),
            Self::Cohort(QueryFrozenCohortRead::TableExecute(read)) => (
                read.group.schema_name().to_string(),
                read.group.table_name().to_string(),
            ),
        };
        SchemaTableName::try_new(&namespace, &table).map_err(ProviderReadError::from)
    }
}

/// The original admitted materialization, exact planning generation, request
/// control and Provider name retained together across physical observation.
struct ReadOpening {
    table: SchemaTableName,
    request_control: ConnectorReadRequestControl,
    read: ConnectorControlReadBinding,
    planning_lease: ConnectorControlPlanningLease,
    // Keep the original admitted materialization/generation through the
    // request control and Provider facts' actual destruction.
    admitted: AdmittedRead,
    name: String,
}

fn prepare_read_opening(
    need: &ProviderReadNeed,
    control: &ConnectorControlHost,
    bindings: &QueryTableBindingStore,
    context: &ConnectorRequestContext,
) -> Result<ReadOpening, ProviderReadError> {
    let relation = need.relation();
    let identity = relation_identity(relation);
    let name = format!("{}.{}", identity.namespace, identity.table);
    // What the request admitted for this relation. A version of a table is
    // admitted as a materialization; a cohort a provider froze for this
    // request is admitted as its own carrier, because the cohort is the read
    // and nothing could resolve it from a name and a version.
    let admitted = if let ProviderReadRelationNeed::MvTarget {
        target_table_uuid,
        target_snapshot_id,
        use_affected_partitions,
        ..
    } = relation
    {
        let binding = bindings.binding(need.binding())?;
        let target = binding.mv_target_read.as_ref().ok_or_else(|| {
            ProviderReadError::Contract({
                format!("provider read of {name} has no admitted MV target materialization")
            })
        })?;
        if target.target_table_uuid != *target_table_uuid
            || target.frozen_snapshot_id != *target_snapshot_id
        {
            return Err(format!(
                "provider read of {name} names a different MV target UUID or snapshot than its frozen binding"
            ).into());
        }
        AdmittedRead::MvTarget {
            materialization: if *use_affected_partitions {
                target.affected_partitions.clone()
            } else {
                target.full.clone()
            },
            object_id: target.target_object_id.clone(),
        }
    } else {
        let input = frozen_input(relation)?;
        let binding = bindings.binding(need.binding())?;
        let prepared = match relation {
            ProviderReadRelationNeed::Data { .. }
            | ProviderReadRelationNeed::FrozenInputSet { .. } => {
                binding.prepared_source_read(input)?
            }
            ProviderReadRelationNeed::Delta {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => binding.prepared_change_window(*from_snapshot_id, *to_snapshot_id)?,
            _ => None,
        };
        if let Some(prepared) = prepared {
            AdmittedRead::PreparedSource(prepared)
        } else {
            match input {
                QueryFrozenReadInput::Current | QueryFrozenReadInput::Snapshot(_) => {
                    AdmittedRead::Relation(
                        bindings
                            .frozen_read_input(need.binding(), input)
                            .map_err(|error| {
                                ProviderReadError::from(error)
                                    .context(format!("provider read of {name}"))
                            })?,
                    )
                }
                QueryFrozenReadInput::PinnedFileSet | QueryFrozenReadInput::TableExecute => {
                    AdmittedRead::Cohort(
                        bindings
                            .frozen_cohort_read(need.binding(), input)
                            .map_err(|error| {
                                ProviderReadError::from(error)
                                    .context(format!("provider read of {name}"))
                            })?,
                    )
                }
            }
        }
    };
    let planning_lease = admitted.planning_lease().clone();
    let read = control
        .typed_read_for_planning_lease(&planning_lease)
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} has no typed read binding"))
        })?;
    // The control this read is negotiated and frozen through is the same one
    // its attempt access is sealed against. A provider may pin what it
    // resolved while answering -- the sealed access is then a view of that pin
    // -- and asking through a second control would seal against a request that
    // was never asked anything.
    let request_control = request_control_for(&read, context)
        .map_err(|error| error.context(format!("provider read of {name}")))?;
    // A provider spells one of its metadata relations as the base relation
    // plus the kind's suffix; the plan states the two separately, because
    // which metadata relation this is belongs to the read and not to the
    // name. Put them back together where the provider is asked.
    let table = admitted
        .provider_relation(relation)
        .map_err(|error| error.context(format!("provider read of {name}")))?;

    Ok(ReadOpening {
        name,
        admitted,
        planning_lease,
        read,
        request_control,
        table,
    })
}

enum ReadObservation {
    Physical(novarocks_spi::connector::read_stack::ConnectorReadRelationPreparation),
    OriginalFrozenSource,
    OriginalPreparedSource,
}

/// Completed physical facts and exact original request/generation. SDK facts
/// exit before opening; a frozen original source is adopted only in the pure job.
pub(crate) struct PreparedProviderRead {
    observation: Option<ReadObservation>,
    opening: ReadOpening,
    need: ProviderReadNeed,
}

fn begin_one_read(
    need: ProviderReadNeed,
    control: &ConnectorControlHost,
    bindings: &QueryTableBindingStore,
    context: &ConnectorRequestContext,
) -> Result<PreparedProviderRead, ProviderReadError> {
    let opening = prepare_read_opening(&need, control, bindings, context)?;
    Ok(PreparedProviderRead {
        observation: None,
        opening,
        need,
    })
}
async fn prepare_observation(
    prepared: &mut PreparedProviderRead,
    session: &ConnectorSession,
) -> Result<(), ProviderReadError> {
    use crate::catalog_application::query_bindings::QueryFrozenCohortRead;
    use novarocks_spi::connector::read_stack::{
        ConnectorReadRelationPreparationKind as K,
        ConnectorReadRelationPreparationRequest as Request,
    };
    let PreparedProviderRead {
        need,
        opening,
        observation,
    } = prepared;
    if observation.is_some() {
        return Ok(());
    }
    if matches!(&opening.admitted, AdmittedRead::PreparedSource(_)) {
        // The original preparation already completed. Projection is pure and
        // remains in the existing paid creator, never an async reopening.
        *observation = Some(ReadObservation::OriginalPreparedSource);
        return Ok(());
    }
    let kind = if let AdmittedRead::Cohort(cohort) = &opening.admitted {
        match (need.relation(),cohort) {
            (ProviderReadRelationNeed::PinnedFileSet { .. },QueryFrozenCohortRead::PinnedFileSet(read)) => {
                if read.frozen_source.is_some() {
                    *observation=Some(ReadObservation::OriginalFrozenSource); return Ok(());
                }
                K::PinnedFileSet { pinned:read.pinned.clone() }
            },
            (ProviderReadRelationNeed::TableExecute { .. },QueryFrozenCohortRead::TableExecute(read)) => {
                K::TableExecute { procedure:novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(
                    novarocks_spi::connector::read_stack::ConnectorReadFrozenRewriteGroup::from_cohort(&read.group,read.group_digest)) }
            },
            (relation,_)=>return Err(unsupported_family(relation).into()),
        }
    } else {
        match need.relation() {
            ProviderReadRelationNeed::Data { version, .. }
            | ProviderReadRelationNeed::FrozenInputSet { version, .. } => K::Table {
                version: relation_version(*version),
                reference: None,
            },
            ProviderReadRelationNeed::Metadata { kind, version, .. } => K::Metadata {
                request: Some(
                    ConnectorReadMetadataRequest::try_new(
                        metadata_kind(*kind)?,
                        metadata_version(*version),
                    )
                    .map_err(|error| {
                        ProviderReadError::from(error)
                            .context(format!("provider read of {}", opening.name))
                    })?,
                ),
            },
            ProviderReadRelationNeed::Delta {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => K::Delta {
                window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(
                    *from_snapshot_id,
                    *to_snapshot_id,
                ),
            },
            ProviderReadRelationNeed::MvTarget {
                target_snapshot_id, ..
            } => {
                let AdmittedRead::MvTarget {
                    materialization,
                    object_id,
                } = &opening.admitted
                else {
                    return Err(format!(
                        "provider read of {} lacks its exact MV target admission",
                        opening.name
                    )
                    .into());
                };
                match &materialization.mv_partition_selection {
                    Some(selection) => {
                        if selection.object_id() != object_id {
                            return Err(format!(
                                "provider read of {} has a different MV target selection object",
                                opening.name
                            )
                            .into());
                        }
                        K::MvTargetPartition {
                            selection: selection.clone(),
                        }
                    }
                    None => K::MvTarget {
                        object_id: object_id.clone(),
                        snapshot_id: *target_snapshot_id,
                    },
                }
            }
            relation => return Err(unsupported_family(relation).into()),
        }
    };
    let request = Request::new(opening.table.clone(), kind);
    let metadata = opening.request_control.metadata();
    let prepared = metadata
        .prepare_relation(session, &request)
        .await
        .map_err(|error| relation_open_error(need.relation(), opening, error))?
        .ok_or_else(|| relation_open_missing(need.relation(), opening))?;
    *observation = Some(ReadObservation::Physical(prepared));
    Ok(())
}

fn relation_open_error(
    relation: &ProviderReadRelationNeed,
    opening: &ReadOpening,
    error: ConnectorError,
) -> ProviderReadError {
    let name = &opening.name;
    let context = match relation {
        ProviderReadRelationNeed::Data { .. } | ProviderReadRelationNeed::FrozenInputSet { .. } => {
            format!("provider read of {name} cannot be opened")
        }
        ProviderReadRelationNeed::Metadata { .. } => {
            format!("provider read of {name} cannot be opened as metadata")
        }
        ProviderReadRelationNeed::Delta { .. } => {
            format!("provider read of {name} cannot open change window")
        }
        ProviderReadRelationNeed::MvTarget { .. } => {
            format!("provider read of {name} cannot open frozen MV target")
        }
        ProviderReadRelationNeed::PinnedFileSet { .. } => {
            let AdmittedRead::Cohort(
                crate::catalog_application::query_bindings::QueryFrozenCohortRead::PinnedFileSet(
                    read,
                ),
            ) = &opening.admitted
            else {
                return ProviderReadError::Contract(unsupported_family(relation));
            };
            format!(
                "provider read of {name} cannot be opened restricted to the {} data files pinned at version {}",
                read.pinned.files().len(),
                read.pinned.version_ordinal()
            )
        }
        ProviderReadRelationNeed::TableExecute { .. } => {
            format!("provider read of {name} cannot be opened as table-execute")
        }
    };
    ProviderReadError::from(error).context(context)
}

fn relation_open_missing(
    relation: &ProviderReadRelationNeed,
    opening: &ReadOpening,
) -> ProviderReadError {
    let name = &opening.name;
    ProviderReadError::Contract(match relation {
        ProviderReadRelationNeed::Data { .. } | ProviderReadRelationNeed::FrozenInputSet { .. } => {
            format!("provider read of {name} is no longer resolvable after admission pinned it")
        }
        ProviderReadRelationNeed::Metadata { .. } => {
            format!("provider read of {name} is not a metadata relation of this provider")
        }
        ProviderReadRelationNeed::Delta { .. } => {
            format!("provider read of {name} exposes no change window")
        }
        ProviderReadRelationNeed::MvTarget { .. } => {
            format!("provider read of {name} exposes no frozen MV target")
        }
        ProviderReadRelationNeed::PinnedFileSet { .. } => {
            format!("provider read of {name} exposes no pinned file set read")
        }
        ProviderReadRelationNeed::TableExecute { .. } => {
            format!("provider read of {name} exposes no table-execute relation")
        }
    })
}

/// One closed Provider creator's original stable input/output. The fixed CPU
/// factory only borrows this value; its request/generation/context and rejected
/// access are not moved into, or destroyed at the tail of, that worker F.
/// There is no generic closure or caller-selected finite qualification here.
pub(crate) struct ProviderReadCreatorInput {
    prepared: PreparedProviderRead,
    session: ConnectorSession,
    context: ConnectorRequestContext,
    deposits: ReadAccessDeposit<FrozenProviderRead>,
    rejected_access: Option<FrozenReadAccess<FrozenProviderRead>>,
    result: Option<Result<ProviderReadFact, ProviderReadError>>,
    started: bool,
}
impl ProviderReadCreatorInput {
    pub(crate) fn new(
        prepared: PreparedProviderRead,
        session: ConnectorSession,
        context: ConnectorRequestContext,
        deposits: ReadAccessDeposit<FrozenProviderRead>,
    ) -> Self {
        Self {
            prepared,
            session,
            context,
            deposits,
            rejected_access: None,
            result: None,
            started: false,
        }
    }
    /// The same finite factory invokes this once while its original packet
    /// retains self. A panic poisons this attempt before any foreign callback.
    pub(crate) fn run_once(&mut self) {
        assert!(!self.started, "one original Provider creator invocation");
        self.started = true;
        let result = finish_prepared_read_borrowed(
            &mut self.prepared,
            &self.session,
            &self.context,
            &self.deposits,
            Some(&mut self.rejected_access),
        );
        self.result = Some(result);
    }
    /// Delivery does not retire this input or its original rejected capability.
    pub(crate) fn take_result_in(
        &mut self,
        target: &mut Option<Result<ProviderReadFact, ProviderReadError>>,
    ) -> Result<(), &'static str> {
        if target.is_some() {
            return Err("Provider creator target is occupied");
        }
        if self.result.is_none() {
            return Err("Provider creator has no completed result");
        }
        *target = self.result.take();
        Ok(())
    }
}

fn finish_prepared_read_borrowed(
    prepared: &mut PreparedProviderRead,
    session: &ConnectorSession,
    context: &ConnectorRequestContext,
    deposits: &ReadAccessDeposit<FrozenProviderRead>,
    rejected_access: Option<&mut Option<FrozenReadAccess<FrozenProviderRead>>>,
) -> Result<ProviderReadFact, ProviderReadError> {
    let PreparedProviderRead {
        need,
        observation,
        opening,
    } = prepared;
    let observation = observation.take().ok_or_else(|| {
        ProviderReadError::Contract("Provider creator observation was already consumed".to_owned())
    })?;
    let metadata = opening.request_control.metadata();
    let opened = match observation {
        ReadObservation::OriginalFrozenSource | ReadObservation::OriginalPreparedSource => {
            open_relation(
                metadata.as_ref(),
                session,
                &opening.table,
                need.relation(),
                &opening.admitted,
                &opening.name,
            )?
        }
        ReadObservation::Physical(prepared) => {
            let relation = metadata
                .create_prepared_relation(prepared)
                .map_err(|error| relation_open_error(need.relation(), opening, error))?
                .ok_or_else(|| relation_open_missing(need.relation(), opening))?;
            let (handle, hint) = relation.into_parts();
            let (placement, kind) =
                if let ProviderReadRelationNeed::Metadata { kind, .. } = need.relation() {
                    let hint = hint.ok_or_else(|| {
                        ProviderReadError::Contract({
                            format!(
                                "provider read of {} has no metadata unit hint",
                                opening.name
                            )
                        })
                    })?;
                    let placement = match hint {
                        novarocks_spi::connector::read_stack::SystemTableUnitHint::ManyUnits => {
                            RuntimeSplitPlacement::Spread
                        }
                        novarocks_spi::connector::read_stack::SystemTableUnitHint::SingleUnit => {
                            RuntimeSplitPlacement::SingleUnit
                        }
                    };
                    (Some(placement), Some(metadata_kind(*kind)?))
                } else {
                    (None, None)
                };
            (handle, placement, kind)
        }
    };
    finish_read_from_opened(
        need,
        session,
        context,
        deposits,
        opening,
        opened,
        rejected_access,
    )
}

fn finish_read_from_opened(
    need: &ProviderReadNeed,
    session: &ConnectorSession,
    context: &ConnectorRequestContext,
    deposits: &ReadAccessDeposit<FrozenProviderRead>,
    opening: &ReadOpening,
    opened: (
        ConnectorReadTableHandle,
        Option<RuntimeSplitPlacement>,
        Option<ConnectorReadMetadataKind>,
    ),
    rejected_access: Option<&mut Option<FrozenReadAccess<FrozenProviderRead>>>,
) -> Result<ProviderReadFact, ProviderReadError> {
    let relation = need.relation();
    let name = &opening.name;
    let planning_lease = &opening.planning_lease;
    let read = &opening.read;
    let request_control = &opening.request_control;
    let metadata = request_control.metadata();
    let (handle, metadata_placement, frozen_metadata_kind) = opened;
    let placement = placement_of(relation, metadata_placement);

    // 2. Bind the requested projection to the provider's own columns. The
    //    request's ordinals are the output authority; the provider is asked for
    //    each column by its own schema spelling.
    let column_bindings = metadata
        .get_column_bindings(session, &handle)
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} cannot read its columns"))
        })?;
    let assignments = assignments_for(need.columns(), &column_bindings, name)?;
    let columns_by_ordinal = assignments
        .iter()
        .enumerate()
        .map(|(ordinal, assignment)| (assignment.column().clone(), ordinal_of(ordinal)))
        .collect::<BTreeMap<_, _>>();

    // 3. Offer projection, filter and limit as one ordered list. Order is part
    //    of the question, and asking as a list is what lets the provider answer
    //    about the read it will actually perform rather than about three
    //    unrelated fragments of it.
    let offer = offered_ops(need, &assignments, &column_ordinals(&assignments))?;
    let negotiation = ReadNegotiation {
        handle,
        ops: offer.ops.clone(),
    };
    let offered = negotiation.ops.len();
    let negotiated = metadata.negotiate(session, &negotiation).map_err(|error| {
        ProviderReadError::from(error)
            .context(format!("provider read of {name} could not be negotiated"))
    })?;
    negotiated.verify_shape(offered).map_err(|error| {
        ProviderReadError::from(error)
            .context(format!("provider read of {name} answered the wrong shape"))
    })?;

    // 4. Commit it. Everything after this point describes a read that is no
    //    longer negotiable.
    let relation_kind = relation.relation_kind();
    let freeze_request = ReadFreezeRequest {
        handle: negotiated.handle.clone(),
        relation_kind,
        expected_input_version: None,
    };
    let frozen = metadata
        .freeze(session, &freeze_request)
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} could not be frozen"))
        })?
        .into_verified(&freeze_request)
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} froze the wrong read"))
        })?;
    // The provider publishes the public fields of exactly the columns this
    // read assigns, repeats included, from the handle it froze. Asking later
    // would need a live provider; asking with SQL names would guess.
    let assigned_columns = assignments
        .iter()
        .map(|assignment| assignment.column().clone())
        .collect::<Vec<_>>();
    let public_schema =
        crate::connector::schema_creation::with_schema_creation(context, |creation| {
            metadata.read_public_schema(session, &negotiated.handle, &assigned_columns, creation)
        })
        .map_err(crate::connector::schema_creation::connector_schema_error);
    if let Err(error) = &public_schema
        && error.kind() != novarocks_spi::connector::ConnectorErrorKind::Unsupported
    {
        return Err(ProviderReadError::from(error.clone()).context(format!(
            "provider read of {name} could not publish its schema"
        )));
    }

    // 5. Assemble everything the freeze leaves behind, so that taking the
    //    capability and accounting for it are adjacent: nothing may happen
    //    between sealing one and depositing it.
    let encoder = read.encoder();
    let provider_relation = metadata
        .relation(relation_kind, negotiated.handle.clone())
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} has no frozen relation"))
        })?;
    let (enforced_predicate, unenforced_predicate, remaining_expression) =
        filter_responsibility(&offer, &negotiated.outcomes);
    let (schema, named_columns) =
        column_facts(need.columns(), &assignments, encoder.as_ref(), name)?;
    let catalog_properties = planning_lease
        .binding()
        .catalog_properties()
        .map_err(|error| {
            ProviderReadError::from(error)
                .context(format!("provider read of {name} has no catalog properties"))
        })?
        .clone();

    // 6. Take the capability and account for it in the same breath.
    let access = read
        .seal_attempt_access(request_control, &negotiated.handle)
        .map_err(|error| {
            ProviderReadError::from(error).context(format!(
                "provider read of {name} cannot seal its per-attempt access"
            ))
        })?;
    deposits
        .deposit(
            need.occurrence(),
            FrozenReadAccess {
                binding: need.binding(),
                access: FrozenProviderRead {
                    access,
                    generation: planning_lease.clone(),
                    catalog: catalog_properties,
                    encoding: FrozenReadEncoding {
                        columns: named_columns,
                        assignments: assignments.clone(),
                        offered_constraint: offered_constraint(&offer),
                        enforced_predicate,
                        unenforced_predicate,
                        remaining_expression,
                        placement,
                        static_facts: frozen.clone(),
                        public_schema,
                    },
                },
            },
        )
        .map_err(|access| {
            // Original fixed receiver first; no callback or foreign Drop may
            // destroy the returned capability on the finite CPU worker.
            if let Some(target) = rejected_access {
                assert!(target.is_none(), "one original rejected read capability");
                *target = Some(access);
            } else {
                // Legacy synchronous families retain their old contract until
                // their own stable source/async cutover is completed.
                drop(access);
            }
            format!("provider read of {name} completed after its plan owner closed")
        })?;

    // 7. Project the frozen read into the contract the plan is built against.
    let relation_payload = encoder
        .encode_relation_payload(&provider_relation)
        .map_err(|error| {
            ProviderReadError::from(error).context(format!(
                "provider read of {name} cannot encode its relation"
            ))
        })?;
    let input_version = ExactInputVersion::try_new(frozen.input_version().as_bytes().to_vec())
        .map_err(|error| {
            ProviderReadError::from(error).context(format!(
                "provider read of {name} has no exact input version"
            ))
        })?;
    let contract = ProviderReadStaticContract {
        sql_binding: need.binding(),
        request: ProviderReadRequestBinding::from_need(need),
        read: ProviderReadReference {
            binding: negotiated.handle.binding().clone(),
            input_version,
            relation: relation_payload,
        },
        metadata_kind: frozen_metadata_kind,
        selection_digest: frozen.selection_digest(),
        schema,
        predicates: predicate_facts(need, &offer, &negotiated.outcomes),
        limit: limit_fact(need, &offer, &negotiated.outcomes),
        provided_properties: read_properties(frozen.properties(), &columns_by_ordinal, name)?,
        coverage_evidence: frozen.coverage_evidence().into(),
    };
    ProviderReadFact::negotiated(need, contract).map_err(|error| {
        ProviderReadError::Contract(format!(
            "provider read of {name} answered its own request wrongly: {error}"
        ))
    })
}

/// Which admission-frozen input this read names.
fn frozen_input(
    relation: &ProviderReadRelationNeed,
) -> Result<QueryFrozenReadInput, ProviderReadError> {
    let version = match relation {
        ProviderReadRelationNeed::Data { version, .. }
        | ProviderReadRelationNeed::FrozenInputSet { version, .. }
        | ProviderReadRelationNeed::Metadata { version, .. } => *version,
        // A cohort names no version: what it reads is the set itself.
        ProviderReadRelationNeed::PinnedFileSet { .. } => {
            return Ok(QueryFrozenReadInput::PinnedFileSet);
        }
        ProviderReadRelationNeed::TableExecute { .. } => {
            return Ok(QueryFrozenReadInput::TableExecute);
        }
        // The binding retains the exact admitted generation. The endpoints
        // are stated by the Delta need and select the window at open time.
        ProviderReadRelationNeed::Delta { .. } => return Ok(QueryFrozenReadInput::Current),
        ProviderReadRelationNeed::MvTarget { .. } => {
            return Err("MV target reads require their target admission"
                .to_string()
                .into());
        }
    };
    Ok(match version {
        ProviderReadVersionNeed::Current => QueryFrozenReadInput::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            QueryFrozenReadInput::Snapshot(snapshot_id)
        }
    })
}

/// A relation family whose admitted carrier arrives with its own product.
///
/// Each of these is frozen from something the read need does not carry - a
/// pinned file cohort, a change window's endpoints, a rewrite group - and each
/// belongs to a statement family (DML, incremental view maintenance, table
/// maintenance) whose own cutover owns that carrier. Refusing is how a read of
/// the wrong relation stays impossible in the meantime: a change window
/// answered with a table handle would silently read the whole relation.
fn unsupported_family(relation: &ProviderReadRelationNeed) -> String {
    let (family, owner) = match relation {
        ProviderReadRelationNeed::Delta { .. } => ("change window", "incremental view maintenance"),
        ProviderReadRelationNeed::MvTarget { .. } => ("MV target", "incremental view maintenance"),
        ProviderReadRelationNeed::PinnedFileSet { .. } => ("pinned file set", "row mutation"),
        ProviderReadRelationNeed::TableExecute { .. } => ("table execute", "table maintenance"),
        ProviderReadRelationNeed::Data { .. }
        | ProviderReadRelationNeed::FrozenInputSet { .. }
        | ProviderReadRelationNeed::Metadata { .. } => ("relation", "query"),
    };
    format!(
        "provider read of a {family} relation is frozen from a carrier this read request does not \
         name; the {owner} product owns it"
    )
}

fn relation_identity(relation: &ProviderReadRelationNeed) -> &TableIdentity {
    match relation {
        ProviderReadRelationNeed::Data { relation, .. }
        | ProviderReadRelationNeed::FrozenInputSet { relation, .. }
        | ProviderReadRelationNeed::Metadata { relation, .. }
        | ProviderReadRelationNeed::Delta { relation, .. }
        | ProviderReadRelationNeed::MvTarget { relation, .. }
        | ProviderReadRelationNeed::PinnedFileSet { relation }
        | ProviderReadRelationNeed::TableExecute { relation } => relation,
    }
}

/// Provider work-unit hints affect placement only. Every read uses splits.
const fn placement_of(
    relation: &ProviderReadRelationNeed,
    metadata: Option<RuntimeSplitPlacement>,
) -> RuntimeSplitPlacement {
    match (relation, metadata) {
        (ProviderReadRelationNeed::Metadata { .. }, Some(placement)) => placement,
        _ => RuntimeSplitPlacement::Spread,
    }
}

/// Open the relation family this read names.
fn open_relation(
    metadata: &dyn ConnectorReadMetadata,
    session: &ConnectorSession,
    table: &SchemaTableName,
    relation: &ProviderReadRelationNeed,
    admitted: &AdmittedRead,
    name: &str,
) -> Result<
    (
        ConnectorReadTableHandle,
        Option<RuntimeSplitPlacement>,
        Option<ConnectorReadMetadataKind>,
    ),
    ProviderReadError,
> {
    use crate::catalog_application::query_bindings::QueryFrozenCohortRead;

    if let AdmittedRead::PreparedSource(source) = admitted {
        return metadata
            .project_prepared_source_read(session, &source.receipt, source.projection)
            .map(|handle| (handle, None, None))
            .map_err(|error| {
                ProviderReadError::from(error).context(format!(
                    "provider read of {name} cannot project its original prepared source"
                ))
            });
    }

    // A cohort is frozen as a freeze of its own rather than as a table read
    // with the cohort pushed down: a pushdown may be declined, and a declined
    // file set silently widens the read to the whole relation -- which the
    // cohort's own commit then contradicts.
    if let AdmittedRead::Cohort(cohort) = admitted {
        return match (relation, cohort) {
            (
                ProviderReadRelationNeed::PinnedFileSet { .. },
                QueryFrozenCohortRead::PinnedFileSet(read),
            ) => {
                if let Some(source) = &read.frozen_source {
                    // The actual read generation adopts the provider's source;
                    // FE neither interprets it nor substitutes generic reopening.
                    return metadata
                        .adopt_frozen_source(session, source)
                        .map(|handle| (handle, None, None))
                        .map_err(|error| ProviderReadError::from(error).context(format!("provider read of {name} cannot adopt its original frozen source")));
                }
                metadata
                .get_pinned_file_set_handle(session, table, &read.pinned)
                .map_err(|error| ProviderReadError::from(error).context(format!(
                        "provider read of {name} cannot be opened restricted to the {} data files pinned at version {}",
                        read.pinned.files().len(),
                        read.pinned.version_ordinal()
                    )))?
                .ok_or_else(|| ProviderReadError::Contract({
                    format!("provider read of {name} exposes no pinned file set read")
                }))
                .map(|handle| (handle, None, None))
            }
            (
                ProviderReadRelationNeed::TableExecute { .. },
                QueryFrozenCohortRead::TableExecute(read),
            ) => metadata
                .get_table_execute_plan(
                    session,
                    table,
                    novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure::RewritePositionDeleteFiles(
                        novarocks_spi::connector::read_stack::ConnectorReadFrozenRewriteGroup::from_cohort(&read.group, read.group_digest),
                    ),
                )
                .map_err(|error| ProviderReadError::from(error).context(format!("provider read of {name} cannot be opened as table-execute")))?
                .ok_or_else(|| ProviderReadError::Contract({
                    format!("provider read of {name} exposes no table-execute relation")
                }))
                .map(|handle| (handle, None, None)),
            _ => Err(unsupported_family(relation).into()),
        };
    }
    match relation {
        ProviderReadRelationNeed::Data { version, .. }
        | ProviderReadRelationNeed::FrozenInputSet { version, .. } => metadata
            .get_table_handle(session, table, relation_version(*version), None)
            .map_err(|error| {
                ProviderReadError::from(error)
                    .context(format!("provider read of {name} cannot be opened"))
            })?
            .ok_or_else(|| {
                ProviderReadError::Contract({
                    format!(
                        "provider read of {name} is no longer resolvable after admission pinned it"
                    )
                })
            })
            .map(|handle| (handle, None, None)),
        ProviderReadRelationNeed::Metadata { kind, version, .. } => {
            let request = ConnectorReadMetadataRequest::try_new(
                metadata_kind(*kind)?,
                metadata_version(*version),
            )
            .map_err(|error| {
                ProviderReadError::from(error).context(format!("provider read of {name}"))
            })?;
            let plan = metadata
                .get_system_table_plan_for_request(session, table, &request)
                .map_err(|error| {
                    ProviderReadError::from(error).context(format!(
                        "provider read of {name} cannot be opened as metadata"
                    ))
                })?
                .ok_or_else(|| {
                    ProviderReadError::Contract({
                        format!(
                            "provider read of {name} is not a metadata relation of this provider"
                        )
                    })
                })?;
            let placement = match plan.unit_hint() {
                novarocks_spi::connector::read_stack::SystemTableUnitHint::ManyUnits => {
                    RuntimeSplitPlacement::Spread
                }
                novarocks_spi::connector::read_stack::SystemTableUnitHint::SingleUnit => {
                    RuntimeSplitPlacement::SingleUnit
                }
            };
            Ok((
                plan.into_handle(),
                Some(placement),
                Some(request.kind().clone()),
            ))
        }
        ProviderReadRelationNeed::Delta {
            from_snapshot_id,
            to_snapshot_id,
            ..
        } => metadata
            .get_change_window_plan(
                session,
                table,
                novarocks_spi::connector::read_stack::ConnectorReadChangeWindow::new(
                    *from_snapshot_id,
                    *to_snapshot_id,
                ),
            )
            .map_err(|error| {
                ProviderReadError::from(error)
                    .context(format!("provider read of {name} cannot open change window"))
            })?
            .ok_or_else(|| {
                ProviderReadError::Contract(format!(
                    "provider read of {name} exposes no change window"
                ))
            })
            .map(|handle| (handle, None, None)),
        ProviderReadRelationNeed::MvTarget {
            target_snapshot_id, ..
        } => {
            let AdmittedRead::MvTarget {
                materialization,
                object_id,
            } = admitted
            else {
                return Err(
                    format!("provider read of {name} lacks its exact MV target admission").into(),
                );
            };
            let selection = materialization.mv_partition_selection.as_ref();
            let opened = match selection {
                Some(selection) => {
                    if selection.object_id() != object_id {
                        return Err(format!(
                            "provider read of {name} has a different MV target selection object"
                        )
                        .into());
                    }
                    metadata.get_mv_target_partition_handle(session, table, selection)
                }
                None => {
                    metadata.get_mv_target_handle(session, table, object_id, *target_snapshot_id)
                }
            };
            opened
                .map_err(|error| {
                    ProviderReadError::from(error).context(format!(
                        "provider read of {name} cannot open frozen MV target"
                    ))
                })?
                .ok_or_else(|| {
                    ProviderReadError::Contract(format!(
                        "provider read of {name} exposes no frozen MV target"
                    ))
                })
                .map(|handle| (handle, None, None))
        }
        _ => Err(unsupported_family(relation).into()),
    }
}

const fn relation_version(version: ProviderReadVersionNeed) -> ConnectorReadRelationVersion {
    match version {
        ProviderReadVersionNeed::Current => ConnectorReadRelationVersion::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            ConnectorReadRelationVersion::SnapshotId(snapshot_id)
        }
    }
}

const fn metadata_version(version: ProviderReadVersionNeed) -> ConnectorReadMetadataVersion {
    match version {
        ProviderReadVersionNeed::Current => ConnectorReadMetadataVersion::Current,
        ProviderReadVersionNeed::Snapshot(snapshot_id) => {
            ConnectorReadMetadataVersion::SnapshotId(snapshot_id)
        }
    }
}

/// The name the provider knows this relation by.
///
/// A metadata relation is one of the relations a table carries, and the
/// provider names it by the table's name with the kind's suffix -- the same
/// spelling the statement wrote.
///
/// A time-travel read names a query-local overlay of its table, which exists
/// only as an analyzer key: the provider has the table, and which snapshot of
/// it this read wants is the read's own version. So the overlay is unwrapped
/// here, and the snapshot its name carries has to be the one the read asks
/// for -- two different snapshots would be two different reads.
fn provider_relation_name(
    relation: &ProviderReadRelationNeed,
) -> Result<String, ProviderReadError> {
    let identity = relation_identity(relation);
    if let Some((base_table, overlaid)) =
        crate::catalog_application::query_bindings::parse_time_travel_overlay_identity(
            &identity.table,
        )
    {
        let version = match relation {
            ProviderReadRelationNeed::Data { version, .. }
            | ProviderReadRelationNeed::FrozenInputSet { version, .. }
            | ProviderReadRelationNeed::Metadata { version, .. } => Some(*version),
            _ => None,
        };
        if version != Some(ProviderReadVersionNeed::Snapshot(overlaid)) {
            return Err(format!(
                "provider read of {} names snapshot {overlaid} but asks for {version:?}",
                identity.table
            )
            .into());
        }
        return Ok(base_table.to_string());
    }
    match relation {
        ProviderReadRelationNeed::Metadata { kind, .. } => Ok(format!(
            "{}{}",
            identity.table,
            metadata_kind(*kind)?.as_str()
        )),
        _ => Ok(identity.table.clone()),
    }
}

/// The provider's own spelling of one metadata relation.
fn metadata_kind(kind: MetadataTableKind) -> Result<ConnectorReadMetadataKind, ProviderReadError> {
    let suffix = match kind {
        MetadataTableKind::Snapshots => "$snapshots",
        MetadataTableKind::History => "$history",
        MetadataTableKind::Refs => "$refs",
        MetadataTableKind::Files => "$files",
        MetadataTableKind::Manifests => "$manifests",
        MetadataTableKind::Partitions => {
            return Err("invariant: $partitions is a SQL view over $files".into());
        }
        MetadataTableKind::Entries => "$entries",
    };
    ConnectorReadMetadataKind::try_new(suffix).map_err(|error| {
        ProviderReadError::from(error).context(format!("metadata relation kind {suffix}"))
    })
}

/// Bind each requested ordinal to the provider column it names.
fn assignments_for(
    columns: &[ProviderReadColumnNeed],
    bindings: &[ConnectorReadColumnBinding],
    name: &str,
) -> Result<Vec<ConnectorReadAssignment>, ProviderReadError> {
    let mut assignments = Vec::with_capacity(columns.len());
    for column in columns {
        let binding = unique_binding(bindings, column.name(), name)?;
        let assignment = ConnectorReadAssignment::try_new(
            format!("{READ_VARIABLE_PREFIX}{}", column.ordinal()),
            binding.column().clone(),
            column.connector_type(),
        )
        .map_err(|error| {
            ProviderReadError::from(error).context(format!(
                "provider read of {name} cannot bind output column '{}'",
                column.name()
            ))
        })?;
        assignments.push(assignment);
    }
    Ok(assignments)
}

/// A read may name one provider column more than once; the reverse - one name
/// matching two provider columns - would make every predicate about it
/// ambiguous, and picking either would push down a predicate about the other.
fn unique_binding<'a>(
    bindings: &'a [ConnectorReadColumnBinding],
    column: &str,
    name: &str,
) -> Result<&'a ConnectorReadColumnBinding, ProviderReadError> {
    let mut matched = bindings.iter().filter(|binding| binding.name() == column);
    let first = matched.next().ok_or_else(|| {
        ProviderReadError::Contract({
            format!("provider read of {name} has no provider column named '{column}'")
        })
    })?;
    if matched.next().is_some() {
        return Err(format!(
            "provider read of {name} matches more than one provider column named '{column}'"
        )
        .into());
    }
    Ok(first)
}

fn column_ordinals(
    assignments: &[ConnectorReadAssignment],
) -> BTreeMap<u32, ConnectorReadColumnHandle> {
    assignments
        .iter()
        .enumerate()
        .map(|(ordinal, assignment)| (ordinal_of(ordinal), assignment.column().clone()))
        .collect()
}

fn ordinal_of(ordinal: usize) -> u32 {
    u32::try_from(ordinal).unwrap_or(u32::MAX)
}

/// What was offered, and where each answer will be found.
struct OfferedOps {
    ops: Vec<ReadPushdownOp>,
    filter: Option<usize>,
    limit: Option<usize>,
}

/// Offer the projection, then the filter, then the limit.
fn offered_ops(
    need: &ProviderReadNeed,
    assignments: &[ConnectorReadAssignment],
    columns: &BTreeMap<u32, ConnectorReadColumnHandle>,
) -> Result<OfferedOps, ProviderReadError> {
    let mut ops = vec![ReadPushdownOp::Projection {
        assignments: assignments.to_vec(),
    }];
    let filter = if need.predicates().is_empty() {
        None
    } else {
        ops.push(ReadPushdownOp::Filter {
            constraint: provider_constraint(need.filter(), columns)?,
        });
        Some(ops.len() - 1)
    };
    let limit = need.limit().map(|rows| {
        ops.push(ReadPushdownOp::Limit { rows });
        ops.len() - 1
    });
    Ok(OfferedOps { ops, filter, limit })
}

/// Restate the request's constraint in the provider's own column vocabulary.
///
/// A requested ordinal with no provider column is refused rather than dropped:
/// dropping it would widen what the provider is told to prune by, which reads
/// as a weaker hint but is really a different question than the one asked.
fn provider_constraint(
    filter: &Constraint<u32>,
    columns: &BTreeMap<u32, ConnectorReadColumnHandle>,
) -> Result<ConnectorReadConstraint, ProviderReadError> {
    let mut missing = None;
    let summary = filter.summary().transform_keys(|ordinal| {
        let column = columns.get(ordinal).cloned();
        if column.is_none() {
            missing = Some(*ordinal);
        }
        column
    });
    let mut assignments = BTreeMap::new();
    for (variable, ordinal) in filter.assignments() {
        let column = columns
            .get(ordinal)
            .cloned()
            .ok_or_else(|| ProviderReadError::Contract(format!("read constraint names projection ordinal {ordinal}, which the read does not project")))?;
        assignments.insert(Arc::clone(variable), column);
    }
    if let Some(ordinal) = missing {
        return Err(format!(
            "read constraint names projection ordinal {ordinal}, which the read does not project"
        )
        .into());
    }
    ConnectorReadConstraint::try_new(summary, filter.expression().clone(), assignments).map_err(
        |error| {
            ProviderReadError::from(error).context("read constraint cannot be offered".to_string())
        },
    )
}

/// What the provider guaranteed, per predicate occurrence.
///
/// Only `Exact` relieves the engine. A provider that merely prunes has
/// answered about its own work, not about which rows the query returns, so
/// every occurrence stays the engine's to evaluate.
fn predicate_facts(
    need: &ProviderReadNeed,
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> Box<[ProviderReadPredicateFact]> {
    let guarantee = offer
        .filter
        .and_then(|index| outcomes.get(index))
        .filter(|outcome| outcome.disposition == ReadPushdownDisposition::Exact)
        .map_or(PredicateGuaranteeKind::PruningOnly, |_| {
            PredicateGuaranteeKind::Exact
        });
    need.predicates()
        .iter()
        .map(|predicate| ProviderReadPredicateFact::new(predicate.occurrence(), guarantee))
        .collect()
}

/// Whether the provider took the limit on, or only used it.
fn limit_fact(
    need: &ProviderReadNeed,
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> ProviderReadLimitFact {
    let Some(rows) = need.limit() else {
        return ProviderReadLimitFact::NotRequested;
    };
    let exact = offer
        .limit
        .and_then(|index| outcomes.get(index))
        .is_some_and(|outcome| outcome.disposition == ReadPushdownDisposition::Exact);
    if exact {
        ProviderReadLimitFact::Exact(rows)
    } else {
        ProviderReadLimitFact::Residual(rows)
    }
}

/// What the provider was offered as a filter, if anything.
fn offered_constraint(offer: &OfferedOps) -> ConnectorReadConstraint {
    offer
        .filter
        .and_then(|index| match &offer.ops[index] {
            ReadPushdownOp::Filter { constraint } => Some(constraint.clone()),
            _ => None,
        })
        .unwrap_or_else(|| ConnectorReadConstraint::of_summary(TupleDomain::all()))
}

/// Who evaluates what, after the provider answered the filter.
///
/// Only `Exact` relieves the engine, and that is decided elsewhere - here it
/// decides what the *reader* is told to do, which is a different question with
/// the same answer at only one of the three dispositions.
///
/// - `Exact`: the provider guarantees the filter, so the engine stops
///   evaluating it and the reader applies only what came back as residual.
/// - `PruningOnly`: the provider will use the filter but guarantees nothing, so
///   the engine keeps evaluating it. The reader is still handed the domain,
///   because applying it removes only rows the engine would remove anyway -
///   evaluating a predicate twice costs time, and skipping the pruning costs
///   the entire point of a pruning answer.
/// - `Unsupported`, or no filter offered: the reader is told nothing. An
///   unenforced domain is the reader's own work by contract. A provider that
///   declines the filter may not apply it, so it must remain in the engine.
fn filter_responsibility(
    offer: &OfferedOps,
    outcomes: &[ReadPushdownOutcome],
) -> (
    TupleDomain<ConnectorReadColumnHandle>,
    TupleDomain<ConnectorReadColumnHandle>,
    Option<ConnectorExpression>,
) {
    let nothing_for_the_reader = (TupleDomain::all(), TupleDomain::all(), None);
    let Some(index) = offer.filter else {
        return nothing_for_the_reader;
    };
    let ReadPushdownOp::Filter {
        constraint: offered,
    } = &offer.ops[index]
    else {
        return nothing_for_the_reader;
    };
    let Some(answer) = outcomes.get(index) else {
        return nothing_for_the_reader;
    };
    match answer.disposition {
        ReadPushdownDisposition::Unsupported => nothing_for_the_reader,
        ReadPushdownDisposition::PruningOnly => (
            // No guarantee is claimed for any column: the engine evaluates the
            // whole predicate regardless of what the provider skipped.
            TupleDomain::all(),
            offered.summary().clone(),
            None,
        ),
        ReadPushdownDisposition::Exact => {
            let Some(residual) = answer.residual.as_ref() else {
                // A provider that guarantees a filter says what remains, even
                // when nothing does. Without that we cannot tell what it kept,
                // and guessing "nothing remains" would drop rows.
                return nothing_for_the_reader;
            };
            let unenforced = residual.summary().clone();
            // Enforcement is claimed only for a column the provider kept
            // whole. A column handed back partially is covered by its own
            // guarantee for the complement, and by the reader for the rest.
            let enforced = offered
                .summary()
                .filter_columns(|column| unenforced.domain_for(column).is_none());
            let remaining = Some(residual.expression().clone()).filter(|e| !e.is_constant_true());
            (enforced, unenforced, remaining)
        }
    }
}

/// The provider's column identity for each requested ordinal, and the name the
/// request used for it.
type ProviderColumnFacts = (
    Box<[ProviderReadColumnFact]>,
    Box<[(ProviderColumnReference, ProviderReadColumnNeed)]>,
);

fn column_facts(
    columns: &[ProviderReadColumnNeed],
    assignments: &[ConnectorReadAssignment],
    encoder: &dyn ConnectorReadWireEncoder,
    name: &str,
) -> Result<ProviderColumnFacts, ProviderReadError> {
    let mut facts = Vec::with_capacity(columns.len());
    let mut named = Vec::with_capacity(columns.len());
    for (column, assignment) in columns.iter().zip(assignments.iter()) {
        let payload = encoder
            .encode_column_payload(assignment.column())
            .map_err(|error| {
                ProviderReadError::from(error).context(format!(
                    "provider read of {name} cannot encode column '{}'",
                    column.name()
                ))
            })?;
        let reference = ProviderColumnReference {
            column_payload: payload,
        };
        facts.push(ProviderReadColumnFact::new(
            column.ordinal(),
            reference.clone(),
            column.engine_type().clone(),
        ));
        named.push((reference, column.clone()));
    }
    Ok((facts.into_boxed_slice(), named.into_boxed_slice()))
}

/// The physical facts the provider guarantees, in the request's own ordinals.
fn read_properties(
    properties: &ConnectorReadProperties<ConnectorReadColumnHandle>,
    ordinals: &BTreeMap<ConnectorReadColumnHandle, u32>,
    name: &str,
) -> Result<ProviderReadProperties, ProviderReadError> {
    let distribution = match properties.distribution() {
        ConnectorReadDistribution::Unconstrained => ProviderReadDistribution::Unconstrained,
        ConnectorReadDistribution::Singleton => ProviderReadDistribution::Singleton,
        ConnectorReadDistribution::RoundRobin => ProviderReadDistribution::RoundRobin,
        // A partitioned read is only usable as a plan property with a
        // proof the provider contract does not carry: a hash read needs
        // its exact partition-count token, a bucket read its
        // ordinal-domain evidence. Claiming the property without them
        // would let the plan skip an exchange on a guarantee nobody made.
        ConnectorReadDistribution::Hash { .. }
        | ConnectorReadDistribution::BucketShuffle { .. } => {
            return Err(format!(
                "provider read of {name} declares a partitioned distribution without the \
                     partition proof a plan property requires"
            )
            .into());
        }
    };
    let ordering = properties
        .ordering()
        .iter()
        .map(|key| {
            let request_ordinal = ordinals.get(key.column()).copied().ok_or_else(|| {
                ProviderReadError::Contract({
                    format!("provider read of {name} orders by a column it does not project")
                })
            })?;
            Ok(ProviderReadOrderingKey {
                request_ordinal,
                direction: match key.direction() {
                    ConnectorReadSortDirection::Ascending => SortDirection::Ascending,
                    ConnectorReadSortDirection::Descending => SortDirection::Descending,
                },
                null_ordering: match key.null_ordering() {
                    ConnectorReadNullOrdering::First => NullOrdering::First,
                    ConnectorReadNullOrdering::Last => NullOrdering::Last,
                },
            })
        })
        .collect::<Result<Vec<_>, ProviderReadError>>()?;
    Ok(ProviderReadProperties {
        distribution,
        ordering: ordering.into_boxed_slice(),
    })
}

/// The request-scoped control one read's attempt access is sealed against.
fn request_control_for(
    read: &ConnectorControlReadBinding,
    context: &ConnectorRequestContext,
) -> Result<ConnectorReadRequestControl, ProviderReadError> {
    let Some(factory) = read.request_factory() else {
        return Ok(ConnectorReadRequestControl::unsupported_attempt_access(
            read.metadata(),
            read.splits(),
        ));
    };
    let planning =
        ConnectorPlanningContext::try_from_request(context.clone().without_attempt_capabilities())
            .map_err(|error| {
                ProviderReadError::from(error).context("read planning context".to_string())
            })?;
    factory
        .for_planning(&planning)
        .map_err(|error| ProviderReadError::from(error).context("read request control".to_string()))
}

#[async_trait]
impl ProviderReadFactPort for FrontendProviderReadFacts {
    type Access = FrozenProviderRead;
    async fn resolve_provider_reads(
        &self,
        needs: &[ProviderReadNeed],
        taken: &ReadAccessSink<Self::Access>,
    ) -> Result<Vec<ProviderReadFact>, String> {
        self.resolve_provider_reads_typed(needs, taken)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use novarocks_physical_plan::{ProviderReadOccurrenceId, ValueType};
    use novarocks_spi::connector::read_stack::{ConnectorValueType, TupleDomain};
    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::compiler::fixtures;
    use novarocks_types::naming::TableIdentity;

    use super::*;

    fn identity() -> TableIdentity {
        TableIdentity::new("iceberg", "db", "orders")
    }

    fn a_binding() -> novarocks_sql::binding::SqlTableBindingId {
        SqlTableBindingAllocator::try_new_for_test(std::num::NonZeroU64::new(1).unwrap())
            .expect("allocator")
            .allocate()
            .expect("binding")
    }

    #[test]
    fn partitions_cannot_be_frozen_as_a_provider_metadata_relation() {
        assert!(
            metadata_kind(MetadataTableKind::Partitions)
                .unwrap_err()
                .to_string()
                .contains("invariant: $partitions is a SQL view over $files")
        );
        assert_eq!(
            metadata_kind(MetadataTableKind::Files).unwrap().as_str(),
            "$files"
        );
    }

    #[test]
    fn single_metadata_unit_preserves_unconstrained_static_distribution() {
        let relation = ProviderReadRelationNeed::Metadata {
            relation: identity(),
            kind: MetadataTableKind::Snapshots,
            version: ProviderReadVersionNeed::Current,
        };
        let properties =
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, Vec::new())
                .unwrap();
        assert_eq!(
            placement_of(&relation, Some(RuntimeSplitPlacement::SingleUnit)),
            RuntimeSplitPlacement::SingleUnit
        );
        let frozen = read_properties(&properties, &BTreeMap::new(), "orders$snapshots").unwrap();
        assert!(matches!(
            frozen.distribution,
            ProviderReadDistribution::Unconstrained
        ));
        assert!(frozen.ordering.is_empty());
        assert_eq!(
            placement_of(&relation, Some(RuntimeSplitPlacement::Spread)),
            RuntimeSplitPlacement::Spread
        );
        let spread = read_properties(&properties, &BTreeMap::new(), "orders$snapshots").unwrap();
        assert!(matches!(
            spread.distribution,
            ProviderReadDistribution::Unconstrained
        ));
    }

    /// One read of one column, with one predicate and a limit, so both
    /// responsibilities below have something to be decided about.
    fn a_need(limit: Option<u64>) -> ProviderReadNeed {
        let column = fixtures::provider_read_column_need(
            0,
            "order_key",
            ValueType::new(arrow::datatypes::DataType::Int64, false),
            ConnectorValueType::BigInt,
        )
        .expect("column need");
        let predicate = fixtures::provider_read_predicate_need(
            fixtures::provider_predicate_occurrence(0),
            Constraint::of_summary(TupleDomain::all()),
        );
        fixtures::provider_read_need(
            1,
            ProviderReadOccurrenceId::new(1),
            a_binding(),
            ProviderReadRelationNeed::Data {
                relation: identity(),
                version: ProviderReadVersionNeed::Current,
            },
            vec![column],
            vec![predicate],
            limit,
        )
        .expect("provider read need")
    }

    /// The offer layout production builds for this need: projection, then
    /// filter, then limit. Only the positions matter to what is read back, so
    /// the operations themselves stand in for ones carrying provider columns.
    fn offer_of(need: &ProviderReadNeed) -> OfferedOps {
        let mut ops = vec![ReadPushdownOp::Projection {
            assignments: Vec::new(),
        }];
        let filter = (!need.predicates().is_empty()).then(|| {
            ops.push(ReadPushdownOp::Filter {
                constraint: Constraint::of_summary(TupleDomain::all()),
            });
            ops.len() - 1
        });
        let limit = need.limit().map(|rows| {
            ops.push(ReadPushdownOp::Limit { rows });
            ops.len() - 1
        });
        OfferedOps { ops, filter, limit }
    }

    /// One answer per offered operation, in the offered order.
    fn all_answered(
        offer: &OfferedOps,
        filter: ReadPushdownDisposition,
        limit: ReadPushdownDisposition,
    ) -> Vec<ReadPushdownOutcome> {
        let mut outcomes = vec![answered(ReadPushdownDisposition::Exact); offer.ops.len()];
        if let Some(index) = offer.filter {
            outcomes[index] = answered(filter);
        }
        if let Some(index) = offer.limit {
            outcomes[index] = answered(limit);
        }
        outcomes
    }

    fn answered(disposition: ReadPushdownDisposition) -> ReadPushdownOutcome {
        ReadPushdownOutcome {
            disposition,
            residual: None,
        }
    }

    /// The version a read names is the admitted input it reads, and it selects
    /// that input by value alone.
    #[test]
    fn a_read_names_the_input_it_was_admitted_against() {
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::Data {
                relation: identity(),
                version: ProviderReadVersionNeed::Current,
            }),
            Ok(QueryFrozenReadInput::Current)
        ));
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::FrozenInputSet {
                relation: identity(),
                version: ProviderReadVersionNeed::Snapshot(7),
            }),
            Ok(QueryFrozenReadInput::Snapshot(7))
        ));
    }

    /// A change window uses the already admitted generation, while its exact
    /// endpoints select the provider's window rather than the whole table.
    #[test]
    fn change_window_uses_the_admitted_generation() {
        let relation = ProviderReadRelationNeed::Delta {
            relation: identity(),
            from_snapshot_id: 1,
            to_snapshot_id: 2,
        };
        assert!(matches!(
            frozen_input(&relation),
            Ok(QueryFrozenReadInput::Current)
        ));
    }

    /// A cohort names no version because the cohort is the read. Each names
    /// its own admitted input, and the carrier admitted under it is what the
    /// freeze opens.
    #[test]
    fn a_cohort_names_the_admitted_input_it_was_frozen_as() {
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::PinnedFileSet {
                relation: identity(),
            }),
            Ok(QueryFrozenReadInput::PinnedFileSet)
        ));
        assert!(matches!(
            frozen_input(&ProviderReadRelationNeed::TableExecute {
                relation: identity(),
            }),
            Ok(QueryFrozenReadInput::TableExecute)
        ));
    }

    /// Only `Exact` relieves the engine. A provider that prunes has answered
    /// about its own work, not about which rows the query returns, so the
    /// engine keeps evaluating every predicate.
    #[test]
    fn only_an_exact_filter_answer_guarantees_a_predicate() {
        let need = a_need(None);
        let offer = offer_of(&need);
        for (disposition, expected) in [
            (
                ReadPushdownDisposition::Exact,
                PredicateGuaranteeKind::Exact,
            ),
            (
                ReadPushdownDisposition::PruningOnly,
                PredicateGuaranteeKind::PruningOnly,
            ),
            (
                ReadPushdownDisposition::Unsupported,
                PredicateGuaranteeKind::PruningOnly,
            ),
        ] {
            let outcomes = all_answered(&offer, disposition, ReadPushdownDisposition::Exact);
            let facts = predicate_facts(&need, &offer, &outcomes);
            assert_eq!(facts.len(), 1);
            assert_eq!(facts[0].guarantee(), expected, "{disposition:?}");
        }
    }

    /// A filter the provider declined leaves the reader nothing to apply. The
    /// engine evaluates the whole predicate regardless of unit placement.
    #[test]
    fn a_declined_filter_leaves_the_reader_nothing() {
        let need = a_need(None);
        let offer = offer_of(&need);
        for disposition in [
            ReadPushdownDisposition::Unsupported,
            // `Exact` without a residual is the same case: a provider that
            // guarantees a filter says what remains, and without that we
            // cannot tell what it kept.
            ReadPushdownDisposition::Exact,
        ] {
            let outcomes = all_answered(&offer, disposition, ReadPushdownDisposition::Exact);
            let (enforced, unenforced, remaining) = filter_responsibility(&offer, &outcomes);
            assert_eq!(enforced, TupleDomain::all(), "{disposition:?}");
            assert_eq!(unenforced, TupleDomain::all(), "{disposition:?}");
            assert!(remaining.is_none(), "{disposition:?}");
        }
    }

    /// A limit the provider only used stays the engine's to enforce; repeating
    /// a limit is free, and not repeating one the provider never guaranteed
    /// returns rows the query excluded.
    #[test]
    fn a_limit_is_residual_unless_the_provider_took_it_on() {
        let unasked = a_need(None);
        assert!(matches!(
            limit_fact(&unasked, &offer_of(&unasked), &[]),
            ProviderReadLimitFact::NotRequested
        ));
        let need = a_need(Some(10));
        let offer = offer_of(&need);
        let took_it_on = all_answered(
            &offer,
            ReadPushdownDisposition::Exact,
            ReadPushdownDisposition::Exact,
        );
        assert!(matches!(
            limit_fact(&need, &offer, &took_it_on),
            ProviderReadLimitFact::Exact(10)
        ));
        let only_used_it = all_answered(
            &offer,
            ReadPushdownDisposition::Exact,
            ReadPushdownDisposition::PruningOnly,
        );
        assert!(matches!(
            limit_fact(&need, &offer, &only_used_it),
            ProviderReadLimitFact::Residual(10)
        ));
    }
}

#[cfg(test)]
mod late_deposit_tests;
