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

//! Rewrite's typed facade over the SAME original serial creator slot.
use super::*;
use novarocks_spi::connector::{ConnectorDistributedRewriteLease, RewriteCpuInput, RewriteCpuPort};

type RewriteCpuPorts = (
    Arc<dyn RewriteCpuPort>,
    Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
);

fn trace_rewrite_cpu_work_refusal(site: &'static str, error: &WorkError) {
    // WorkError has a closed first-party projection; never format the cause.
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(error);
    tracing::debug!(
        target: "novarocks_frontend_application::maintenance_error",
        site,
        failure_domain = fact.reason().domain(),
        failure_reason = fact.reason().code(),
        "Original Rewrite work admission refused"
    );
}

impl InternalResultCpu {
    /// Two typed facades, one original serial slot. Metadata accepts only the
    /// selected first-party binding input; Rewrite accepts only its selected
    /// Provider source or the closed first-party FE cohort/seal input.
    pub(crate) fn rewrite_cpu_ports(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorDistributedRewriteLease,
        write: &novarocks_catalog_application::ConnectorWriteStackLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<RewriteCpuPorts, WorkError> {
        if !window.is_for_scope(scope) || window.class() != ResultWindowClass::Internal {
            trace_rewrite_cpu_work_refusal("tm.cpu_ports.window", &WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        scope.check().inspect_err(|error| {
            trace_rewrite_cpu_work_refusal("tm.cpu_ports.scope", error);
        })?;
        let _ = self.provider_creator_source(scope).inspect_err(|error| {
            trace_rewrite_cpu_work_refusal("tm.cpu_ports.creator_source", error);
        })?;
        let provider = lease
            .scoped_cpu_source_type_id()
            .map_err(|_| WorkError::InvalidConfig("selected Rewrite CPU source is unsupported"))?;
        let source_type = lease.metadata_cpu_source_type_id().map_err(|_| {
            WorkError::InvalidConfig("selected metadata binding CPU author is unsupported")
        })?;
        let (write_key, write_begin) =
            write
                .session()
                .rewrite_begin_cpu_author()
                .ok_or(WorkError::InvalidConfig(
                    "selected WriteControl has no governed Rewrite begin author",
                ))?;
        if write_key.instance_id != lease.descriptor().instance_id
            || write_key.incarnation != lease.incarnation()
        {
            trace_rewrite_cpu_work_refusal("tm.cpu_ports.write_author", &WorkError::Conflict);
            return Err(WorkError::Conflict);
        }
        let source = Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode: CreatorCpuMode::RewriteBound {
                provider,
                write_begin,
            },
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        });
        Ok((source.clone(), source))
    }

    /// Authenticate the actual metadata author from this retained planning
    /// generation. Management and pre-computation MV phases retain payload on
    /// their original Root without claiming a computation position.
    pub(crate) fn planning_metadata_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>, WorkError> {
        if !window.is_for_scope(scope) || !window.is_root_backed_internal() {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        let _ = self.provider_creator_source(scope)?;
        let binding = planning.binding();
        let (key, source_type) =
            binding
                .metadata()
                .provider_metadata_cpu_author()
                .ok_or(WorkError::InvalidConfig(
                    "selected metadata CPU author is unsupported",
                ))?;
        if key.instance_id != binding.descriptor().instance_id
            || key.incarnation != binding.incarnation()
        {
            return Err(WorkError::Conflict);
        }
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: binding.descriptor().clone(),
            incarnation: binding.incarnation(),
            source_type,
            mode: CreatorCpuMode::ProviderMetadata,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }

    /// Only the journal's already-started obligation may use this independent
    /// finite control. Ordinary planning never upgrades itself on cancellation.
    pub(crate) fn planning_metadata_settlement_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        planning: &novarocks_spi::connector::ConnectorControlPlanningLease,
        io: Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        obligation: novarocks_spi::connector::ConnectorRequestContext,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>, WorkError> {
        if !window.is_for_scope(scope) || !window.is_root_backed_internal() {
            return Err(WorkError::Conflict);
        }
        // Capture from the real positive payload before constructing a port.
        io.check_existing_metadata_obligation_context(scope, &obligation)?;
        let _ = self.metadata_cpu_continuation_source(&io, scope)?;
        let binding = planning.binding();
        let (key, source_type) =
            binding
                .metadata()
                .provider_metadata_cpu_author()
                .ok_or(WorkError::InvalidConfig(
                    "selected metadata CPU author is unsupported",
                ))?;
        if key.instance_id != binding.descriptor().instance_id
            || key.incarnation != binding.incarnation()
        {
            return Err(WorkError::Conflict);
        }
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: binding.descriptor().clone(),
            incarnation: binding.incarnation(),
            source_type,
            mode: CreatorCpuMode::ProviderMetadataSettlement(io),
            control: CpuWorkControl::existing_metadata_obligation(&obligation),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }

    pub(crate) fn cleanup_metadata_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>, WorkError> {
        if !window.is_for_scope(scope) || !window.is_root_backed_internal() {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        let _ = self.provider_creator_source(scope)?;
        let source_type = lease.metadata_cpu_source_type_id().map_err(|_| {
            WorkError::InvalidConfig("selected Cleanup metadata CPU author is unsupported")
        })?;
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode: CreatorCpuMode::ProviderMetadata,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }

    pub(crate) fn provider_metadata_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorDistributedRewriteLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>, WorkError> {
        if !window.is_for_scope(scope)
            || !matches!(
                window.class(),
                ResultWindowClass::Local | ResultWindowClass::Client
            )
        {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        let _ = self.provider_creator_source(scope)?;
        let source_type = lease.metadata_cpu_source_type_id().map_err(|_| {
            WorkError::InvalidConfig("selected metadata binding CPU author is unsupported")
        })?;
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode: CreatorCpuMode::ProviderMetadata,
            control: cancellation.into(),
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }
}
impl RewriteCpuPort for ScopedProviderCreatorCpu {
    fn submit_in(&self, input: &mut Option<RewriteCpuInput>) -> Result<(), ConnectorError> {
        let original = input.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU input is empty",
            )
        })?;
        let actual = original.original_source_type_id();
        let fe = std::any::TypeId::of::<
            crate::query_execution::maintenance::rewrite_preparation::RewriteCohortCpuOriginal,
        >();
        let selected = match &self.mode {
            CreatorCpuMode::RewriteBound {
                provider,
                write_begin,
            } => actual == *provider || actual == fe || actual == *write_begin,
            _ => false,
        };
        if !selected
            || original.descriptor() != &self.descriptor
            || original.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU input belongs to a foreign original Provider generation",
            ));
        }
        let mut loan = CreatorPortLoan::take(self)?;
        if loan.job.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU source retains its previous original job",
            ));
        }
        let source = self.cpu.provider_creator_source(&self.scope).map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        let window = self.window.try_clone().map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        // Every refusal above precedes the move from the actual input slot.
        loan.job = Some(self.cpu.submit_creator_input_with_control(
            ClosedCreatorInput::RewriteProvider(input.take().expect("checked Rewrite input")),
            window,
            None,
            Some(source),
            self.control.clone(),
            &self.runtime,
            Arc::clone(&self.wake),
        ));
        Ok(())
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, false)
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, true)
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .job
            .as_ref()
            .and_then(ProviderReadCpuJob::health_failure)
    }
}
