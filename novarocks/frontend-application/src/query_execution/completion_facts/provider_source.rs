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

//! The original statement retains its Provider outside every borrowed compiler
//! and close future. This is one fixed source slot, not an execution authority.

use super::*;
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    preparation::{CompletedPlanWithAccess, FinalPlanCompletionDriver, FinalPlanCompletionFailure},
};
use std::sync::Mutex;

pub(super) type CompletionResult = Result<
    CompletedPlanWithAccess<FrozenProviderRead>,
    FinalPlanCompletionFailure<FrozenProviderRead>,
>;
type Facts = QueryCompletionFactSource<FrozenProviderRead>;

#[derive(Default)]
struct ProviderCompletionState {
    facts: Option<Arc<Facts>>,
    provider: Option<Arc<FrontendProviderReadFacts>>,
    outcome: Option<CompletionResult>,
    health: Option<StatementNativeHealthFailure>,
    joined: bool,
}

struct CompletionAccessBody {
    taken: Option<novarocks_query_application::preparation::ReadAccessSink<FrozenProviderRead>>,
}
struct CompletionAccessOriginal {
    body: Mutex<Option<CompletionAccessBody>>,
}
struct CompletionAccessLoan<'a> {
    original: &'a CompletionAccessOriginal,
    body: Option<CompletionAccessBody>,
}
impl Drop for CompletionAccessLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "one original completion access loan");
        *target = self.body.take();
    }
}
pub(crate) struct FrontendCompletionCustody {
    state: Mutex<ProviderCompletionState>,
    access: CompletionAccessOriginal,
}
impl FrontendCompletionCustody {
    pub(crate) fn new() -> Arc<Self> {
        let owner = Arc::new(Self {
            state: Mutex::new(ProviderCompletionState::default()),
            access: CompletionAccessOriginal {
                body: Mutex::new(Some(CompletionAccessBody { taken: None })),
            },
        });
        // Materialize the same platform mutex at the source constructor.
        drop(owner.state.lock().unwrap_or_else(|p| p.into_inner()));
        owner
    }
    pub(super) fn install(
        &self,
        facts: Arc<Facts>,
        provider: Arc<FrontendProviderReadFacts>,
    ) -> Result<(), (Arc<Facts>, Arc<FrontendProviderReadFacts>)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.facts.is_some() || state.provider.is_some() || state.joined {
            return Err((facts, provider));
        }
        state.facts = Some(facts);
        state.provider = Some(provider);
        Ok(())
    }
    /// Borrow the already installed catalog port; no second fact source is created.
    pub(crate) async fn resolve_catalog_relations(
        &self,
        needs: &[novarocks_sql::compiler::CatalogRelationNeed],
    ) -> Result<Vec<novarocks_sql::compiler::CatalogRelationFact>, String> {
        use novarocks_query_application::preparation::SqlCompletionFactSource;
        let facts = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined || state.health.is_some() {
                return Err("completion fact source is closed or unhealthy".into());
            }
            state
                .facts
                .clone()
                .ok_or_else(|| "completion fact source is not installed".to_owned())?
        };
        let batch = novarocks_sql::compiler::SqlNeedBatch::CatalogRelations(
            needs.to_vec().into_boxed_slice(),
        );
        let taken = novarocks_query_application::preparation::ReadAccessSink::new();
        match facts.resolve(&batch, &taken).await? {
            novarocks_sql::compiler::SqlFactBatch::CatalogRelations(answer) => {
                Ok(answer.into_vec())
            }
            _ => Err("completion catalog port returned another fact family".into()),
        }
    }

    /// The original statement retains this Provider across the borrowed wait.
    /// A failed or cancelled compiler borrow must still be closed by that owner.
    pub(crate) async fn resolve_provider_reads(
        &self,
        needs: &[novarocks_sql::compiler::ProviderReadNeed],
        taken: &novarocks_query_application::preparation::ReadAccessSink<FrozenProviderRead>,
    ) -> Result<Vec<novarocks_sql::compiler::ProviderReadFact>, String> {
        self.resolve_provider_reads_typed(needs, taken)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn resolve_provider_reads_typed(
        &self,
        needs: &[novarocks_sql::compiler::ProviderReadNeed],
        taken: &novarocks_query_application::preparation::ReadAccessSink<FrozenProviderRead>,
    ) -> Result<
        Vec<novarocks_sql::compiler::ProviderReadFact>,
        crate::query_execution::provider_read_facts::ProviderReadError,
    > {
        let provider = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(health) = &state.health {
                return Err(
                    crate::query_execution::provider_read_facts::ProviderReadError::ActualExit(
                        health.clone(),
                    ),
                );
            }
            if state.joined {
                return Err("completion Provider source is closed or unhealthy".into());
            }
            state.provider.clone().ok_or_else(|| {
                crate::query_execution::provider_read_facts::ProviderReadError::Contract(
                    "completion Provider source is not installed".to_owned(),
                )
            })?
        };
        provider.resolve_provider_reads_typed(needs, taken).await
    }

    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .health
            .clone()
    }
    /// Complete the same original driver, then join the same Provider source.
    /// No caller cancellation can detach the borrowed close from its holder.
    pub(crate) async fn complete_in(
        &self,
        request: novarocks_sql::compiler::SqlFinalPlanCompileRequest,
        scope: &novarocks_workload_control::WorkScope,
        cpu: &dyn novarocks_query_application::preparation::SqlCompletionCpuPort,
    ) -> Result<(), StatementNativeHealthFailure> {
        let facts = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            Arc::clone(state.facts.as_ref().expect("installed original facts"))
        };
        let body = self
            .access
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let mut loan = CompletionAccessLoan {
            original: &self.access,
            body,
        };
        let body = loan.body.as_mut().expect("original completion access body");
        assert!(body.taken.is_none(), "one original completion invocation");
        body.taken = Some(novarocks_query_application::preparation::ReadAccessSink::new());
        let driver = FinalPlanCompletionDriver::new(facts);
        let candidate = match driver
            .complete(request, scope, body.taken.as_ref().unwrap(), cpu)
            .await
        {
            Ok(candidate) => candidate,
            Err(health) => {
                let selected = {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    if state.health.is_none() {
                        state.health = Some(health.clone());
                    }
                    state.health.as_ref().unwrap().clone()
                };
                return Err(selected);
            }
        };
        let outcome =
            FinalPlanCompletionDriver::pair_completed(candidate, body.taken.take().unwrap());
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.outcome.is_none() && !state.joined);
            state.outcome = Some(outcome);
        }
        // The original outcome and all capabilities are stable before cleanup.
        drop(loan);
        drop(driver);
        self.close().await
    }
    /// Observe the original verdict without moving its unpaired capabilities.
    pub(crate) fn completed_error(
        &self,
    ) -> Option<novarocks_query_application::preparation::FinalPlanCompletionError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state
            .outcome
            .as_ref()
            .expect("same completed original outcome")
        {
            Ok(_) => None,
            Err(failure) => Some(failure.error().clone()),
        }
    }
    pub(crate) fn handoff_completed_in(
        &self,
        destination: &super::planning_cpu::FrontendPlanningCpuSource,
        input: &mut Option<super::planning_cpu::CompletedReadCpuInput>,
    ) -> Result<(), &'static str> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.joined || state.health.is_some() {
            return Err("completion Provider exit is unproven");
        }
        destination.install_completed_in(&mut state.outcome, input)
    }
    pub(crate) async fn close(&self) -> Result<(), StatementNativeHealthFailure> {
        let provider = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(failure) = &state.health {
                return Err(failure.clone());
            }
            if state.joined {
                return Ok(());
            }
            state.provider.clone()
        };
        // No Provider can issue a job before install. Logical-only EXPLAIN
        // does not construct this family; this is its exact unused branch.
        let Some(provider) = provider else {
            return Ok(());
        };
        if let Err(cause) = provider.join_local_closed().await {
            let failure = StatementNativeHealthFailure::actual_exit_unproven(
                "completion Provider creator actual exit is unproven",
                cause,
            );
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.health.is_none());
            state.health = Some(failure.clone());
            return Err(failure);
        }
        self.state.lock().unwrap_or_else(|p| p.into_inner()).joined = true;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn install_provider_for_test(&self, provider: Arc<FrontendProviderReadFacts>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.provider.is_none() && !state.joined);
        state.provider = Some(provider);
    }
}
