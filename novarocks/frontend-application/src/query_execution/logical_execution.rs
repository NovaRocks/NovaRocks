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

//! Fixed Frontend launch boundary for client and internal logical execution.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

use super::internal_consumer::attempt_handoff::AttemptHandoffOfferPort;
use crate::query_execution::{
    artifact::PreparedDistributedAttemptTemplate,
    attempt_initialization::RoundCredentialLeaseSource,
    completion::QueryAttemptReservation,
    contract::{DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions},
    lifecycle_plan::{AttemptCredentialStorage, AttemptTerminalCredentialResolver},
};
use novarocks_coordination_contract::RecoveryMode;
use novarocks_query_application::api::{
    InternalComputationConsumer, QueryExecutionError, QueryExecutionFuture,
    ReservedExecutionIdentity,
};
use novarocks_query_application::preparation::{
    ExecutionFinishBoundary, FrozenExecutionDescription,
};
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::WorkOwner;
use std::sync::{Arc, Mutex};

use crate::query_execution::completion::PreparedLogicalRead;

/// Production composition injects one implementation backed by the process
/// Query Application runtime. The SQL session transfers the complete prepared
/// carrier and the unique governed owner without learning Native adapter parts.
pub(crate) trait LogicalExecutionLauncher: Send + Sync + 'static {
    fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError>;

    /// Resolve evidence only through the process's already authorized journal.
    /// A neutral nomination cannot install an observer or keep one alive.
    #[cfg(novarocks_dev_controls)]
    fn resolve_statement_observation(
        &self,
        nomination: &novarocks_failpoint::target_binding::StatementWorkNomination,
    ) -> Option<crate::preparation_diagnostics::StatementObservationHandle>;

    fn start_read_in(
        &self,
        read: &mut Option<PreparedLogicalRead>,
        owner: &mut Option<WorkOwner>,
        capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        io: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
        original: &mut OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>;

    fn start_computation_in(
        &self,
        computation: &mut Option<PreparedLogicalComputation>,
        owner: &mut Option<WorkOwner>,
        capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        io: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        original: &mut OriginalComputationStart,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<novarocks_query_application::api::LogicalExecutionStartAttempt, QueryExecutionError>;
}

/// Fixed fields inside the statement's existing stable action. These are
/// original QA inputs/results, not another execution identity or task owner.
#[derive(Default)]
pub(crate) struct OriginalComputationStart {
    pub(crate) reservation: Option<ReservedExecutionIdentity>,
    pub(crate) request: Option<novarocks_query_application::api::QueryExecutionRequest>,
    pub(crate) input: Option<novarocks_query_application::api::OriginalLogicalStartInput>,
    pub(crate) ready: Option<QueryExecutionFuture>,
    pub(crate) exit: Option<novarocks_query_application::api::LogicalExecutionExitObserver>,
}

/// Original first-attempt ownership established before domain preparation.
/// Credentials remain FA-owned; the QA token is moved unchanged into start.
pub(crate) struct InitialComputationReservation {
    reserved: Option<ReservedExecutionIdentity>,
    credentials: Arc<InitialComputationCredentials>,
}

impl InitialComputationReservation {
    pub(crate) fn new(reserved: ReservedExecutionIdentity) -> Result<Self, DistributedQueryError> {
        let reservation = QueryAttemptReservation::from_reserved(&reserved)?;
        let terminal = reservation.terminal_storage_resolver();
        Ok(Self {
            credentials: Arc::new(InitialComputationCredentials {
                execution: reserved.execution(),
                reservation: Mutex::new(Some(reservation)),
                terminal,
                write_production_session: Mutex::new(None),
            }),
            reserved: Some(reserved),
        })
    }

    pub(crate) fn credentials(&self) -> Arc<InitialComputationCredentials> {
        Arc::clone(&self.credentials)
    }

    fn into_start_parts(
        mut self,
    ) -> (
        ReservedExecutionIdentity,
        Arc<InitialComputationCredentials>,
    ) {
        let reserved = self.reserved.take().expect("one initial QA reservation");
        (reserved, Arc::clone(&self.credentials))
    }

    pub(crate) fn connector_request_context(
        &self,
        context: ConnectorRequestContext,
        #[cfg(novarocks_dev_controls)] nomination: Option<
            &Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
        >,
    ) -> Result<ConnectorRequestContext, DistributedQueryError> {
        let reservation = self
            .credentials
            .reservation
            .lock()
            .expect("initial credential reservation");
        reservation
            .as_ref()
            .ok_or_else(|| {
                initial_credential_error(
                    "initial computation credentials no longer authorize preparation",
                )
            })?
            .connector_request_context(
                context,
                #[cfg(novarocks_dev_controls)]
                nomination,
            )
            .map_err(initial_credential_error)
    }
}

impl Drop for InitialComputationReservation {
    fn drop(&mut self) {
        if self.reserved.is_some() {
            // An attachment takes the token first and disarms this fallback.
            // The statement's stable owner still retains terminal material.
            let _ = self.credentials.seal_for_terminal();
        }
    }
}

/// One exact reservation plus the same strong terminal resolver. Holding this
/// Arc grants neither a second start nor the right to create another collector.
pub(crate) struct InitialComputationCredentials {
    execution: QueryExecutionId,
    reservation: Mutex<Option<QueryAttemptReservation>>,
    terminal: Arc<AttemptTerminalCredentialResolver>,
    write_production_session:
        Mutex<Option<std::sync::Weak<super::write_session::ConnectorWriteSession>>>,
}

impl InitialComputationCredentials {
    pub(crate) fn bind_write_production_session(
        &self,
        session: &Arc<super::write_session::ConnectorWriteSession>,
    ) -> Result<(), &'static str> {
        let incoming = Arc::downgrade(session);
        let mut route = self
            .write_production_session
            .lock()
            .map_err(|_| "write production route is poisoned")?;
        match route.as_ref() {
            Some(original) if !std::sync::Weak::ptr_eq(original, &incoming) => {
                Err("initial write production session changed")
            }
            Some(_) => Ok(()),
            None => {
                *route = Some(incoming);
                Ok(())
            }
        }
    }
    /// Only clone the same Weak under the route lock; upgrade and all work are
    /// outside it. This does not extend WeakReceivingPort/session lifetime.
    pub(crate) fn write_production_session(
        &self,
    ) -> Option<std::sync::Weak<super::write_session::ConnectorWriteSession>> {
        self.write_production_session.lock().ok()?.clone()
    }
    pub(crate) const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub(crate) fn terminal_storage_resolver(&self) -> Arc<AttemptTerminalCredentialResolver> {
        Arc::clone(&self.terminal)
    }

    pub(crate) fn verify_native_storage(
        &self,
        storage: &Arc<AttemptCredentialStorage>,
    ) -> Result<(), DistributedQueryError> {
        self.terminal.verify_native_storage(self.execution, storage)
    }

    /// The stable statement owner calls this on failure/cancellation, including
    /// a start rejected before Native consumes the original reservation.
    pub(crate) fn seal_for_terminal(&self) -> Result<(), DistributedQueryError> {
        let abandoned = self
            .reservation
            .lock()
            .expect("initial credential reservation")
            .take();
        // Close the collection authority even when initialization already took
        // its reservation. Never drop that reservation under the slot mutex.
        let result = self.terminal.seal_for_terminal().map(|_| ());
        drop(abandoned);
        result
    }

    pub(super) fn take_source(
        &self,
        execution: QueryExecutionId,
    ) -> Result<RoundCredentialLeaseSource, DistributedQueryError> {
        if execution != self.execution {
            return Err(initial_credential_error(
                "initial computation credentials belong to another execution",
            ));
        }
        let reservation = self
            .reservation
            .lock()
            .expect("initial credential reservation")
            .take()
            .ok_or_else(|| {
                initial_credential_error(
                    "initial computation credential reservation was consumed or retired",
                )
            })?;
        Ok(RoundCredentialLeaseSource::Reservation {
            reservation: Some(reservation),
            observed_collected: None,
        })
    }
}

fn initial_credential_error(detail: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, detail)
}

/// The diagnostic contract comes from the closed FA consumer constructor,
/// never from Native's interpretation of a wire kind or public output mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComputationDiagnostics {
    None,
    Profile,
}

/// Authenticate the original pair without consuming either input. A reserved
/// identity describes first-attempt custody, independently of external effects.
/// QA still checks the move-only token's issuing process when start takes it.
pub(super) fn validate_initial_computation_start(
    description: &FrozenExecutionDescription,
    diagnostics: ComputationDiagnostics,
    credentials: Option<&Arc<InitialComputationCredentials>>,
    reservation: Option<&ReservedExecutionIdentity>,
) -> Result<(), &'static str> {
    match (credentials, reservation) {
        (None, None)
            if description.effect() == novarocks_coordination_contract::ExecutionEffect::None =>
        {
            Ok(())
        }
        (Some(credentials), Some(reservation))
            if description.recovery() == RecoveryMode::NoRecovery
                && diagnostics == ComputationDiagnostics::None
                && credentials.execution() == reservation.execution() =>
        {
            Ok(())
        }
        _ => Err(
            "initial computation requires its matching original reserved NoRecovery execution without Profile diagnostics",
        ),
    }
}

/// One frozen computation and its registered application's exact handoff pair.
/// Domain payload/proof construction stays in the fixed FA consumer.
pub(crate) struct PreparedLogicalComputation {
    pub(super) description: FrozenExecutionDescription,
    pub(super) template: PreparedDistributedAttemptTemplate,
    pub(super) options: Arc<ResolvedQueryOptions>,
    pub(super) consumer: Arc<dyn InternalComputationConsumer>,
    pub(super) handoffs: Arc<AttemptHandoffOfferPort>,
    pub(super) diagnostics: ComputationDiagnostics,
    pub(super) initial_credentials: Option<Arc<InitialComputationCredentials>>,
}
fn validate_prepared_computation(
    description: &FrozenExecutionDescription,
    plan: novarocks_query_application::api::PlanSeal,
) -> Result<(), String> {
    if description.finish_boundary() != ExecutionFinishBoundary::ComputationReady
        || !description.matches_plan_seal(plan)
    {
        return Err(
            "internal computation does not match its frozen finish boundary or Native plan".into(),
        );
    }
    Ok(())
}

/// Checked neutral pieces; the original template remains outside this value
/// until its Source's actual CPU exit and receiving transfer.
pub(crate) struct PreparedLogicalComputationParts {
    description: FrozenExecutionDescription,
    options: Arc<ResolvedQueryOptions>,
    consumer: Arc<dyn InternalComputationConsumer>,
    handoffs: Arc<AttemptHandoffOfferPort>,
    diagnostics: ComputationDiagnostics,
}
impl PreparedLogicalComputationParts {
    pub(crate) fn try_new(
        description: FrozenExecutionDescription,
        plan: novarocks_query_application::api::PlanSeal,
        options: Arc<ResolvedQueryOptions>,
        consumer: Arc<dyn InternalComputationConsumer>,
        handoffs: AttemptHandoffOfferPort,
        diagnostics: ComputationDiagnostics,
    ) -> Result<Self, String> {
        validate_prepared_computation(&description, plan)?;
        Ok(Self {
            description,
            options,
            consumer,
            handoffs: Arc::new(handoffs),
            diagnostics,
        })
    }
    pub(crate) fn move_in(
        parts: &mut Option<Self>,
        template: &mut Option<PreparedDistributedAttemptTemplate>,
        destination: &mut Option<PreparedLogicalComputation>,
    ) -> Result<(), String> {
        if destination.is_some() {
            return Err("computation receiver is occupied".into());
        }
        let original = parts
            .as_ref()
            .ok_or("checked computation parts are absent")?;
        let original_template = template
            .as_ref()
            .ok_or("original computation template is absent")?;
        validate_prepared_computation(
            &original.description,
            original_template.native_manifest_template().plan(),
        )?;
        let original = parts.take().unwrap();
        let template = template.take().unwrap();
        *destination = Some(PreparedLogicalComputation {
            description: original.description,
            template,
            options: original.options,
            consumer: original.consumer,
            handoffs: original.handoffs,
            diagnostics: original.diagnostics,
            initial_credentials: None,
        });
        Ok(())
    }
}

impl PreparedLogicalComputation {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        description: FrozenExecutionDescription,
        template: PreparedDistributedAttemptTemplate,
        options: Arc<ResolvedQueryOptions>,
        consumer: Arc<dyn InternalComputationConsumer>,
        handoffs: AttemptHandoffOfferPort,
        diagnostics: ComputationDiagnostics,
    ) -> Result<Self, String> {
        validate_prepared_computation(&description, template.native_manifest_template().plan())?;
        Ok(Self {
            description,
            template,
            options,
            consumer,
            handoffs: Arc::new(handoffs),
            diagnostics,
            initial_credentials: None,
        })
    }

    pub(crate) fn with_initial_reservation(
        mut self,
        initial: InitialComputationReservation,
    ) -> Result<(Self, ReservedExecutionIdentity), DistributedQueryError> {
        if self.description.recovery() != RecoveryMode::NoRecovery
            || self.initial_credentials.is_some()
        {
            return Err(initial_credential_error(
                "initial computation credentials require one unbound NoRecovery computation",
            ));
        }
        let (reserved, credentials) = initial.into_start_parts();
        self.initial_credentials = Some(credentials);
        Ok((self, reserved))
    }
}

#[cfg(test)]
mod initial_reservation_tests {
    use super::*;
    use novarocks_query_application::{
        api::{LogicalExecutionNativePort, LogicalNativeOpenRequest},
        coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
    };
    use novarocks_spi::connector::{
        ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use novarocks_types::identity::{AttemptId, FrontendProcessId, QueryProcessNamespace};
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        time::{Duration, Instant},
    };

    #[derive(Debug)]
    struct NoNativeStart;
    impl LogicalExecutionNativePort for NoNativeStart {
        fn bind_in(
            &self,
            _source: &mut Option<LogicalNativeOpenRequest>,
            _target: &mut Option<novarocks_query_application::api::LogicalNativeSession>,
        ) -> Result<(), novarocks_query_application::api::LogicalNativeOpenError> {
            panic!("reserving an identity cannot bind Native work")
        }
    }

    fn process() -> (
        crate::query_execution::logical_execution_test_profile::IdentityTestOwner,
        novarocks_query_application::api::QueryExecutionClient,
    ) {
        crate::query_execution::logical_execution_test_profile::process(
            tokio::runtime::Handle::current(),
            Arc::new(NoNativeStart),
            QueryProcessNamespace::new(0x9b1),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::MIN,
                NonZeroUsize::MIN,
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::MIN,
                    NonZeroU32::MIN,
                    Duration::from_secs(1),
                ),
            ),
        )
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn original_qa_reservation_transfers_its_same_request_scope_once() {
        let (mut owner, client) = process();
        let original = client.reserve_identity().unwrap();
        let execution = original.execution();
        let initial = InitialComputationReservation::new(original).unwrap();
        let stable = initial.credentials();
        let prepared_context = initial
            .connector_request_context(
                context(),
                #[cfg(novarocks_dev_controls)]
                None,
            )
            .unwrap();
        let marker =
            prepared_context.request_scope_extension_or_insert_with(|| String::from("original"));
        let (original, transferred) = initial.into_start_parts();
        assert_eq!(original.execution(), execution);
        assert!(Arc::ptr_eq(&stable, &transferred));
        let foreign =
            QueryExecutionId::new(execution.query_id(), AttemptId::new(2).unwrap()).unwrap();
        assert!(stable.take_source(foreign).is_err());
        let source = stable.take_source(execution).unwrap();
        assert!(stable.take_source(execution).is_err());
        let native_context = source.connector_request_context(context()).unwrap();
        let observed =
            native_context.request_scope_extension_or_insert_with(|| String::from("foreign"));
        assert!(Arc::ptr_eq(&marker, &observed));
        assert!(Arc::ptr_eq(
            prepared_context.vended_credential_lease_sink().unwrap(),
            native_context.vended_credential_lease_sink().unwrap(),
        ));
        // Dropping the actual unstarted source closes production rights even
        // though the stable responsibility owner still retains the material.
        drop(source);
        let terminal = stable.terminal_storage_resolver();
        let original_table = terminal.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &original_table,
            &terminal.seal_for_terminal().unwrap()
        ));
        assert!(stable.verify_native_storage(&original_table).is_err());
        assert!(stable.take_source(execution).is_err());
        drop((original, transferred, stable, terminal, original_table));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abandoned_original_qa_reservation_retires_before_terminal_custody_drops() {
        let (mut owner, client) = process();
        let initial =
            InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
        let stable = initial.credentials();
        let execution = stable.execution();
        drop(initial);
        assert!(stable.take_source(execution).is_err());
        let terminal = stable.terminal_storage_resolver();
        let original_table = terminal.seal_for_terminal().unwrap();
        stable.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &original_table,
            &terminal.seal_for_terminal().unwrap()
        ));
        assert!(stable.verify_native_storage(&original_table).is_err());
        drop((stable, terminal, original_table));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[cfg(novarocks_dev_controls)]
    #[tokio::test]
    async fn reserved_statistics_start_retains_the_original_collector_through_native_drain() {
        use crate::query_execution::internal_consumer::test_fixture::{
            internal_statistics_description, offer_original_credentials,
        };
        let (mut owner, client) = process();
        let (description, _) = internal_statistics_description();
        assert_eq!(
            description.effect(),
            novarocks_coordination_contract::ExecutionEffect::None
        );
        assert_eq!(description.recovery(), RecoveryMode::NoRecovery);
        let initial =
            InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
        // The domain's stable state keeps this original Arc after the initial
        // assembly transfers its token and the Native seed retains only Weak.
        let stable = initial.credentials();
        let prepared = initial.connector_request_context(context(), None).unwrap();
        let marker =
            prepared.request_scope_extension_or_insert_with(|| String::from("statistics-original"));
        offer_original_credentials(prepared.vended_credential_lease_sink().unwrap().as_ref());
        let (reservation, transferred) = initial.into_start_parts();
        let original = OriginalComputationStart {
            reservation: Some(reservation),
            ..Default::default()
        };
        validate_initial_computation_start(
            &description,
            ComputationDiagnostics::None,
            Some(&transferred),
            original.reservation.as_ref(),
        )
        .unwrap();
        let factory_view = Arc::downgrade(&transferred);
        drop(transferred);
        assert!(Arc::ptr_eq(&factory_view.upgrade().unwrap(), &stable));
        let source = factory_view
            .upgrade()
            .unwrap()
            .take_source(stable.execution())
            .unwrap();
        let native = source.connector_request_context(context()).unwrap();
        assert!(Arc::ptr_eq(
            &marker,
            &native.request_scope_extension_or_insert_with(|| String::from("foreign")),
        ));
        assert!(Arc::ptr_eq(
            prepared.vended_credential_lease_sink().unwrap(),
            native.vended_credential_lease_sink().unwrap(),
        ));
        let leases = source.test_into_credential_leases().unwrap();
        assert!(!leases.leases().is_empty(), "the original vended table");
        let storage = leases.into_storage();
        stable.verify_native_storage(&storage).unwrap();
        stable.seal_for_terminal().unwrap();
        assert!(Arc::ptr_eq(
            &storage,
            &stable
                .terminal_storage_resolver()
                .seal_for_terminal()
                .unwrap()
        ));
        stable.verify_native_storage(&storage).unwrap();
        assert!(stable.take_source(stable.execution()).is_err());
        drop((native, prepared, original));
        assert!(
            factory_view.upgrade().is_some(),
            "Native's weak view cannot retire domain custody"
        );
        drop(stable);
        assert!(factory_view.upgrade().is_none());
        drop((storage, marker, factory_view));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[cfg(novarocks_dev_controls)]
    #[tokio::test]
    async fn reserved_start_rejection_preserves_both_inputs_and_original_request_scope() {
        use crate::query_execution::internal_consumer::test_fixture::{
            internal_statistics_description, internal_write_description,
        };
        let (mut owner, client) = process();
        let (statistics, _) = internal_statistics_description();
        let (write, _) = internal_write_description();
        let initial =
            InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
        let prepared = initial.connector_request_context(context(), None).unwrap();
        let marker = prepared.request_scope_extension_or_insert_with(|| String::from("original"));
        let (reservation, credentials) = initial.into_start_parts();
        let execution = reservation.execution();
        let original = OriginalComputationStart {
            reservation: Some(reservation),
            ..Default::default()
        };
        let foreign = client.reserve_identity().unwrap();
        for description in [&statistics, &write] {
            assert!(
                validate_initial_computation_start(
                    description,
                    ComputationDiagnostics::None,
                    Some(&credentials),
                    None
                )
                .is_err()
            );
            assert!(
                validate_initial_computation_start(
                    description,
                    ComputationDiagnostics::None,
                    None,
                    original.reservation.as_ref()
                )
                .is_err()
            );
            assert!(
                validate_initial_computation_start(
                    description,
                    ComputationDiagnostics::None,
                    Some(&credentials),
                    Some(&foreign)
                )
                .is_err()
            );
            assert!(
                validate_initial_computation_start(
                    description,
                    ComputationDiagnostics::Profile,
                    Some(&credentials),
                    original.reservation.as_ref()
                )
                .is_err()
            );
            // The actual launch guard borrowed both original slots. No failure
            // consumed either identity or sealed/changed the collector.
            assert_eq!(
                original.reservation.as_ref().unwrap().execution(),
                execution
            );
            assert_eq!(credentials.execution(), execution);
            validate_initial_computation_start(
                description,
                ComputationDiagnostics::None,
                Some(&credentials),
                original.reservation.as_ref(),
            )
            .unwrap();
        }
        assert!(
            validate_initial_computation_start(&write, ComputationDiagnostics::None, None, None)
                .is_err()
        );
        validate_initial_computation_start(&statistics, ComputationDiagnostics::None, None, None)
            .unwrap();
        let source = credentials
            .take_source(execution)
            .expect("all rejected starts preserve the one original source");
        let native = source.connector_request_context(context()).unwrap();
        assert!(Arc::ptr_eq(
            &marker,
            &native.request_scope_extension_or_insert_with(|| String::from("foreign"))
        ));
        assert!(Arc::ptr_eq(
            prepared.vended_credential_lease_sink().unwrap(),
            native.vended_credential_lease_sink().unwrap()
        ));
        assert!(credentials.take_source(execution).is_err());
        drop(source);
        credentials.seal_for_terminal().unwrap();
        drop((native, prepared, original, foreign, credentials, marker));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[cfg(novarocks_dev_controls)]
    #[tokio::test]
    async fn original_profile_recovery_stays_unreserved_and_refuses_reserved_inputs() {
        use crate::query_execution::internal_consumer::test_fixture::internal_profile_recovery_description;
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let (description, _) = internal_profile_recovery_description(&root.owner.scope()).await;
        assert_eq!(
            description.recovery(),
            RecoveryMode::RestartAttemptBeforeVisibility
        );
        validate_initial_computation_start(
            &description,
            ComputationDiagnostics::Profile,
            None,
            None,
        )
        .unwrap();
        let (mut owner, client) = process();
        let initial =
            InitialComputationReservation::new(client.reserve_identity().unwrap()).unwrap();
        let (reservation, credentials) = initial.into_start_parts();
        let execution = reservation.execution();
        // Reject recovery even if diagnostics are None; Profile is a separate
        // refusal from the reserved NoRecovery constraint.
        for diagnostics in [
            ComputationDiagnostics::None,
            ComputationDiagnostics::Profile,
        ] {
            assert!(
                validate_initial_computation_start(
                    &description,
                    diagnostics,
                    Some(&credentials),
                    Some(&reservation)
                )
                .is_err()
            );
            assert_eq!(reservation.execution(), execution);
        }
        let source = credentials.take_source(execution).unwrap();
        drop(source);
        credentials.seal_for_terminal().unwrap();
        drop((reservation, credentials, root, control));
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
