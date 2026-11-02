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

//! The original MV write retains analysis, Provider access and encoding until
//! every concrete creator actually exits. It creates no Root or query permit.

use super::{
    activation_cpu::{
        MvWriteActivationOriginal, MvWriteActivationOutput, MvWriteActivationRequest,
        MvWriteAnalysis, MvWriteFinishInput, MvWriteProviderNeeds,
    },
    activation_preparation::MvWriteAnalysisPreparationInput,
    refresh_preparation::{
        async_original::{MvRefreshPreparationOriginal, MvRefreshPreparationOutput},
        driver::{MvRefreshTurnCustody, MvTurnFailure},
        io_gate::{MvRefreshPreparationIo, contract_error},
    },
};
use crate::{
    catalog_application::query_bindings::QueryTableBindingStore,
    mv::domain::application::MvApplicationError,
    query_execution::{
        completion_facts::{
            CompletionFactOwners, FrontendCompletionCustody, StatementFactScope,
            install_frontend_fact_source,
        },
        kernels::QueryPreparationKernel,
        mv_native_write::PreparedMvNativeWriteAssembly,
        planning::statistics::{QueryStatisticsContext, StatisticsSnapshotFailure},
        provider_read_facts::{FrozenProviderRead, ProviderReadError},
        write_session::ConnectorWriteSession,
    },
};
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::StatementNativeHealthFailure,
    preparation::{FinalPlanRuntimeAccess, FrozenReadAccess, ReadAccessSink},
};
use novarocks_spi::connector::ConnectorRequestContext;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) enum MvWritePreparationFailure {
    Domain(MvApplicationError),
    Turn(MvTurnFailure),
    Provider(ProviderReadError),
    Statistics(StatisticsSnapshotFailure),
    Health(StatementNativeHealthFailure),
}

pub struct MvWritePreparationCustody {
    turn: MvRefreshTurnCustody,
    io: Arc<MvRefreshPreparationIo>,
    provider: Arc<FrontendCompletionCustody>,
    owners: CompletionFactOwners,
    capacity: QueryResultCapacityBinding,
    requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    kernel: QueryPreparationKernel,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
    session: Arc<ConnectorWriteSession>,
    bindings: Option<Arc<QueryTableBindingStore>>,
    prepared_response: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    phase_response: Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    analyzed: Option<MvWriteAnalysis>,
    finish_failure: Option<MvApplicationError>,
    provider_needs: Option<MvWriteProviderNeeds>,
    statistics: Option<QueryStatisticsContext>,
    sink: Option<ReadAccessSink<FrozenProviderRead>>,
    facts: Option<Vec<novarocks_sql::compiler::ProviderReadFact>>,
    access: Option<FinalPlanRuntimeAccess<FrozenProviderRead>>,
    returned_access: Option<Vec<FrozenReadAccess<FrozenProviderRead>>>,
    assembly: Option<PreparedMvNativeWriteAssembly>,
    failure: Option<MvWritePreparationFailure>,
    started: bool,
    assembly_ready: bool,
}
impl MvWritePreparationCustody {
    /// The exact session was already received by WriteBeginReceiving. The
    /// target facts are preparation facts, never a fresh Current attestation.
    pub(crate) fn new(
        input: MvWriteAnalysisPreparationInput,
        io: Arc<MvRefreshPreparationIo>,
        mut turn: MvRefreshTurnCustody,
        owners: CompletionFactOwners,
        capacity: QueryResultCapacityBinding,
        requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
    ) -> Self {
        let kernel = input.kernel.clone();
        let execution = input.execution.clone();
        let context = input.context.clone();
        let session = input.session.clone();
        let original = MvRefreshPreparationOriginal::for_write_analysis(input, io.clone());
        assert!(
            turn.install_preparation(original).is_ok(),
            "original MV write receives one empty preparation turn"
        );
        Self {
            turn,
            io,
            provider: FrontendCompletionCustody::new(),
            owners,
            capacity,
            requests,
            kernel,
            execution,
            context,
            session,
            bindings: None,
            prepared_response: None,
            phase_response: None,
            analyzed: None,
            finish_failure: None,
            provider_needs: None,
            statistics: None,
            sink: None,
            facts: None,
            access: None,
            returned_access: None,
            assembly: None,
            failure: None,
            started: false,
            assembly_ready: false,
        }
    }
    /// This future only borrows the original custody stored in its Action.
    /// Failure is received here before its Product observer can be notified.
    pub(crate) async fn prepare(&mut self) -> Result<(), MvWritePreparationFailure> {
        assert!(
            !self.started,
            "one original MV write preparation invocation"
        );
        self.started = true;
        let result = self.prepare_original().await;
        if let Err(failure) = &result {
            self.failure = Some(failure.clone());
        }
        result
    }
    async fn drive(&mut self) -> Result<(), MvWritePreparationFailure> {
        std::future::poll_fn(|cx| self.turn.poll_ready(cx))
            .await
            .map_err(MvWritePreparationFailure::Turn)
    }
    fn retire_transferred(&mut self) -> Result<(), MvWritePreparationFailure> {
        self.turn
            .retire_transferred()
            .map_err(|error| MvWritePreparationFailure::Domain(contract_error(error)))
    }
    fn install_phase(&mut self, request: MvWriteActivationRequest) {
        let original = MvWriteActivationOriginal::new(request);
        assert!(
            self.turn.install_activation(original).is_ok(),
            "preceding original MV phase transferred before its successor"
        );
    }
    async fn receive_phase(&mut self) -> Result<(), MvWritePreparationFailure> {
        self.drive().await?;
        self.turn
            .activation_original()
            .take_output_in(&mut self.phase_response)
            .map_err(|error| MvWritePreparationFailure::Domain(contract_error(error)))?;
        Ok(())
    }
    async fn prepare_original(&mut self) -> Result<(), MvWritePreparationFailure> {
        use MvWritePreparationFailure as F;
        self.drive().await?;
        assert!(
            self.turn
                .preparation_original()
                .take_result_in(&mut self.prepared_response)
        );
        let prepared = match self
            .prepared_response
            .take()
            .expect("same original MV reader result")
        {
            Ok(MvRefreshPreparationOutput::WriteAnalysis(prepared)) => *prepared,
            Err(error) => return Err(F::Domain(error)),
            _ => {
                return Err(F::Domain(contract_error(
                    "MV analysis received another reader family",
                )));
            }
        };
        self.bindings = Some(prepared.bindings);
        self.retire_transferred()?;
        self.install_phase(MvWriteActivationRequest::Analyze(Some(prepared.analysis)));
        self.receive_phase().await?;
        self.analyzed = Some(
            match self
                .phase_response
                .take()
                .expect("same original analysis receiver")
            {
                Ok(MvWriteActivationOutput::Analyzed(analyzed)) => analyzed,
                Err(error) => return Err(F::Domain(error)),
                _ => {
                    return Err(F::Domain(contract_error(
                        "MV analysis returned a foreign phase",
                    )));
                }
            },
        );
        let control =
            crate::query_execution::planning::sql_compile_control_from_execution(&self.execution);
        self.retire_transferred()?;
        // The original analyzed graph stays in its Action's stable field
        // while the async observer reads the exact captured binding set.
        self.statistics = Some(
            QueryStatisticsContext::from_statistics_resolver_with_bindings_async_typed(
                &self.kernel,
                self.bindings.as_ref().unwrap().clone(),
                &self.context,
            )
            .await
            .map_err(F::Statistics)?,
        );
        let analysis = self.analyzed.take();
        self.install_phase(MvWriteActivationRequest::Optimize {
            analysis,
            statistics: self.statistics.as_ref().unwrap().snapshot().clone(),
            control: control.clone(),
        });
        self.receive_phase().await?;
        self.provider_needs = Some(
            match self
                .phase_response
                .take()
                .expect("same original optimizer receiver")
            {
                Ok(MvWriteActivationOutput::ProviderNeeds(needs)) => needs,
                Err(error) => return Err(F::Domain(error)),
                _ => {
                    return Err(F::Domain(contract_error(
                        "MV optimizer returned a foreign phase",
                    )));
                }
            },
        );
        self.retire_transferred()?;
        let scope = StatementFactScope::governed_read_io_in(
            self.bindings.as_ref().unwrap().clone(),
            self.context.clone(),
            None,
            self.capacity.scope(),
            self.capacity.clone(),
            Some(self.requests.clone()),
        )
        .map_err(|error| F::Turn(MvTurnFailure::Admission(error)))?;
        install_frontend_fact_source(
            &self.provider,
            self.owners.clone(),
            scope,
            crate::query_execution::compiler::typed_connector_session()
                .map_err(|error| F::Domain(contract_error(error)))?,
        )
        .map_err(|error| F::Domain(contract_error(error)))?;
        self.sink = Some(ReadAccessSink::new());
        self.facts = Some(
            self.provider
                .resolve_provider_reads_typed(
                    &self.provider_needs.as_ref().unwrap().needs,
                    self.sink.as_ref().unwrap(),
                )
                .await
                .map_err(F::Provider)?,
        );
        self.provider.close().await.map_err(F::Health)?;
        let sink = self.sink.take().expect("same original read access sink");
        self.access = Some(match sink.try_into_access() {
            Ok(access) => access,
            Err((error, returned)) => {
                self.returned_access = Some(returned);
                return Err(F::Domain(contract_error(error.to_string())));
            }
        });
        let needs = self
            .provider_needs
            .take()
            .expect("same original MV continuation");
        let facts = self.facts.take();
        let access = self.access.take();
        self.install_phase(MvWriteActivationRequest::Finish(MvWriteFinishInput {
            continuation: Some(needs.continuation),
            facts,
            access,
            session: self.session.clone(),
            carrier: self.kernel.static_plan_carrier().clone(),
            constant_policy: self.kernel.constant_policy(),
            options: crate::query_execution::contract::synthetic_statement_query_options(
                &self.execution,
            ),
            control,
        }));
        self.drive().await?;
        if self
            .turn
            .activation_original()
            .take_failure_in(&mut self.finish_failure)
            .map_err(|error| F::Domain(contract_error(error)))?
        {
            return Err(F::Domain(self.finish_failure.as_ref().unwrap().clone()));
        }
        self.turn
            .activation_original()
            .take_assembly_in(&mut self.assembly)
            .map_err(|error| F::Domain(contract_error(error)))?;
        self.retire_transferred()?;
        self.assembly_ready = true;
        Ok(())
    }
    /// Failure retains the same job, rejected access, session and raw health.
    /// It does not authorize abort/publication; those belong to the MV Action.
    pub(crate) async fn close(&mut self) -> Result<(), StatementNativeHealthFailure> {
        std::future::poll_fn(|cx| self.turn.poll_closed(cx)).await?;
        self.provider.close().await?;
        std::future::poll_fn(|cx| self.io.poll_closed(cx)).await?;
        if let Some(sink) = self.sink.take() {
            assert!(self.returned_access.is_none());
            self.returned_access = Some(sink.into_taken());
        }
        Ok(())
    }
    pub(crate) fn take_assembly_in(
        &mut self,
        destination: &mut Option<PreparedMvNativeWriteAssembly>,
    ) -> Result<(), &'static str> {
        if destination.is_some()
            || !self.assembly_ready
            || self.failure.is_some()
            || self.assembly.is_none()
        {
            return Err("MV write preparation has no closed transferable assembly");
        }
        *destination = self.assembly.take();
        Ok(())
    }
}
