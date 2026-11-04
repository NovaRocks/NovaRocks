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

//! The original statement retains its Provider and owned catalog CPU outside
//! every borrowed compiler and close future. These are fixed source slots,
//! not execution authorities.

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
type RejectedCompletionSources = (
    Arc<Facts>,
    Arc<FrontendProviderReadFacts>,
    Option<Arc<FrontendPlanningCpuSource>>,
);

#[derive(Default)]
struct ProviderCompletionState {
    facts: Option<Arc<Facts>>,
    provider: Option<Arc<FrontendProviderReadFacts>>,
    owned_catalog_cpu: Option<Arc<FrontendPlanningCpuSource>>,
    provider_close_outcome: Option<Result<(), StatementNativeHealthFailure>>,
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
        owned_catalog_cpu: Option<Arc<FrontendPlanningCpuSource>>,
    ) -> Result<(), RejectedCompletionSources> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.facts.is_some()
            || state.provider.is_some()
            || state.owned_catalog_cpu.is_some()
            || state.joined
        {
            return Err((facts, provider, owned_catalog_cpu));
        }
        state.facts = Some(facts);
        state.provider = Some(provider);
        state.owned_catalog_cpu = owned_catalog_cpu;
        Ok(())
    }
    /// Borrow the already installed catalog port; no second fact source is created.
    pub(crate) async fn resolve_catalog_relations(
        &self,
        needs: &[novarocks_sql::compiler::CatalogRelationNeed],
    ) -> Result<
        Vec<novarocks_sql::compiler::CatalogRelationFact>,
        novarocks_sql::compiler::SqlCompileError,
    > {
        use novarocks_query_application::preparation::SqlCompletionFactSource;
        let facts = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined || state.health.is_some() {
                return Err(novarocks_sql::compiler::SqlCompileError::Compilation(
                    "completion fact source is closed or unhealthy".into(),
                ));
            }
            state.facts.clone().ok_or_else(|| {
                novarocks_sql::compiler::SqlCompileError::Compilation(
                    "completion fact source is not installed".to_owned(),
                )
            })?
        };
        let batch = novarocks_sql::compiler::SqlNeedBatch::CatalogRelations(
            needs.to_vec().into_boxed_slice(),
        );
        let taken = novarocks_query_application::preparation::ReadAccessSink::new();
        match facts.resolve(&batch, &taken).await? {
            novarocks_sql::compiler::SqlFactBatch::CatalogRelations(answer) => {
                Ok(answer.into_vec())
            }
            _ => Err(novarocks_sql::compiler::SqlCompileError::Compilation(
                "completion catalog port returned another fact family".into(),
            )),
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
        let owned_catalog_cpu = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(health) = &state.health {
                return Some(health.clone());
            }
            state.owned_catalog_cpu.clone()
        };
        owned_catalog_cpu.and_then(|cpu| cpu.local_health_failure())
    }

    fn retain_health(&self, failure: StatementNativeHealthFailure) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.health.is_none() {
            state.health = Some(failure);
        }
    }

    /// Complete the same original driver, then close its original sources.
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
        let (provider, owned_catalog_cpu) = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined {
                return Ok(());
            }
            (state.provider.clone(), state.owned_catalog_cpu.clone())
        };
        // Keep an already observed CPU cause before driving the other owner.
        // Health selects the original failure; it never skips real cleanup.
        if let Some(health) = owned_catalog_cpu
            .as_ref()
            .and_then(|cpu| cpu.local_health_failure())
        {
            self.retain_health(health);
        }
        if let Some(provider) = &provider {
            let received = self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .provider_close_outcome
                .clone();
            let outcome = match received {
                Some(outcome) => outcome,
                None => {
                    let outcome = provider.join_local_closed().await.map_err(|cause| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "completion Provider creator actual exit is unproven",
                            cause,
                        )
                    });
                    // Receive this owner's verdict before borrowing the CPU close.
                    // An unhealthy result remains unhealthy after disposition.
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.provider_close_outcome.get_or_insert(outcome).clone()
                }
            };
            if let Err(health) = outcome {
                self.retain_health(health);
            }
        }
        // Only CPU sources created for this custody enter this slot. A Read
        // or EXPLAIN caller's borrowed CPU must remain available for its later
        // candidate/encoding turns and is never closed here.
        if let Some(cpu) = &owned_catalog_cpu
            && let Err(health) = cpu.close().await
        {
            self.retain_health(health);
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(health) = &state.health {
            return Err(health.clone());
        }
        if provider.is_some() || owned_catalog_cpu.is_some() {
            state.joined = true;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn install_provider_for_test(&self, provider: Arc<FrontendProviderReadFacts>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.provider.is_none() && !state.joined);
        state.provider = Some(provider);
    }
}

#[cfg(test)]
mod owned_catalog_cpu_tests {
    use super::*;
    use novarocks_query_application::{
        system_catalog::{
            SystemCatalog, SystemCatalogInputRequirement, SystemCatalogInputs,
            SystemCatalogRelations, SystemCatalogService, SystemTableData,
        },
        system_catalog_rewrite::{SystemCatalogFacts, SystemCatalogFactsPort},
    };
    use novarocks_sql::compiler::{
        CatalogRelationNeed, SqlCapturedCatalogPreparation, SqlCompileControl, SqlSessionContext,
    };
    use std::sync::{
        Condvar, Weak,
        atomic::{AtomicBool, Ordering},
    };

    #[derive(Default)]
    struct CatalogGate {
        entered: tokio::sync::Notify,
        released: Mutex<bool>,
        changed: Condvar,
        returned: AtomicBool,
        source_field: Mutex<Option<Weak<arrow::datatypes::Field>>>,
    }
    impl CatalogGate {
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }
    struct ReleaseOnDrop(Arc<CatalogGate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    struct BlockingCatalog {
        gate: Arc<CatalogGate>,
        original: SystemCatalogService,
    }
    impl SystemCatalog for BlockingCatalog {
        fn required_inputs(&self, db: &str, table: &str) -> Option<SystemCatalogInputRequirement> {
            self.original.required_inputs(db, table)
        }
        fn resolve(
            &self,
            db: &str,
            table: &str,
            inputs: &SystemCatalogInputs<'_>,
        ) -> Result<Option<SystemTableData>, String> {
            self.gate.entered.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
            drop(released);
            let data = self.original.resolve(db, table, inputs)?;
            let schema = data.as_ref().expect("registered system table").batches[0].schema();
            *self.gate.source_field.lock().unwrap() = Some(Arc::downgrade(&schema.fields()[0]));
            self.gate.returned.store(true, Ordering::Release);
            Ok(data)
        }
    }
    struct UnusedFacts;
    impl SystemCatalogFactsPort for UnusedFacts {
        fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
            panic!("CPU materialization cannot perform catalog I/O")
        }
        fn external_system_catalog_facts(
            &self,
            _: &ConnectorRequestContext,
            _: &str,
            _: bool,
        ) -> Result<Option<SystemCatalogFacts>, String> {
            panic!("CPU materialization cannot perform catalog I/O")
        }
    }
    fn query(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query fixture")
        };
        query.clone()
    }
    fn definition_need() -> CatalogRelationNeed {
        let preparation = SqlCapturedCatalogPreparation::try_new_definition(
            query("SELECT * FROM information_schema.schemata"),
            "ice",
            "db",
            None,
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let [need] = preparation.needs() else {
            panic!("one exact definition need")
        };
        assert!(need.is_definition_context());
        need.clone()
    }

    #[tokio::test]
    async fn dropped_catalog_cpu_borrow_remains_owned_until_real_close_without_a_provider() {
        assert_owned_catalog_cpu_close(None).await;
    }

    #[tokio::test]
    async fn retained_health_cannot_skip_the_owned_catalog_cpu_actual_close() {
        let health = StatementNativeHealthFailure::actual_exit_unproven(
            "fixture retains an earlier independent owner failure",
            Arc::new(std::io::Error::other("original fixture health cause")),
        );
        assert_owned_catalog_cpu_close(Some(health)).await;
    }

    async fn assert_owned_catalog_cpu_close(health: Option<StatementNativeHealthFailure>) {
        let (mut owners, supervisor) =
            crate::task_execution::blocking_io::tests::original_executors(
                tokio::runtime::Handle::current(),
            );
        let cpu = FrontendPlanningCpuSource::new(supervisor.clone());
        let custody = FrontendCompletionCustody::new();
        custody.state.lock().unwrap().owned_catalog_cpu = Some(Arc::clone(&cpu));
        let gate = Arc::new(CatalogGate::default());
        let _release_on_drop = ReleaseOnDrop(Arc::clone(&gate));
        let catalog = Arc::new(BlockingCatalog {
            gate: Arc::clone(&gate),
            original: SystemCatalogService::with_defaults(),
        });
        let original_catalog = Arc::downgrade(&catalog);
        let source = SystemCatalogRelations::new(Arc::new(UnusedFacts), catalog);
        let mut borrowed = Box::pin(cpu.system_relation(
            source,
            definition_need(),
            SystemCatalogFacts {
                catalog_name: "ice".into(),
                schema_names: vec!["actual_schema".into()],
                table_names: Vec::new(),
            },
            crate::application::test_constant_policy(),
            SqlCompileControl::unbounded(),
        ));
        assert!(futures::poll!(borrowed.as_mut()).is_pending());
        gate.entered.notified().await;
        drop(borrowed);
        assert!(
            original_catalog.upgrade().is_some(),
            "original CPU input is still retained"
        );
        assert!(!gate.returned.load(Ordering::Acquire));
        if let Some(health) = &health {
            custody.retain_health(health.clone());
        }
        let mut close = Box::pin(custody.close());
        assert!(
            futures::poll!(close.as_mut()).is_pending(),
            "borrow cancellation cannot substitute for CPU exit"
        );
        {
            let state = custody.state.lock().unwrap();
            assert!(!state.joined);
            assert!(state.provider.is_none());
            assert!(Arc::ptr_eq(state.owned_catalog_cpu.as_ref().unwrap(), &cpu));
        }
        assert!(original_catalog.upgrade().is_some());
        // Cancelling this borrow must retain both the original close verdict
        // and the actual CPU work for the next close borrow.
        drop(close);
        let mut close = Box::pin(custody.close());
        assert!(futures::poll!(close.as_mut()).is_pending());
        gate.release();
        let outcome = close.await;
        if let Some(health) = &health {
            assert!(
                matches!(outcome, Err(ref failure) if std::ptr::addr_eq(failure.cause(), health.cause()))
            );
            let repeated = custody.close().await;
            assert!(
                matches!(repeated, Err(ref failure) if std::ptr::addr_eq(failure.cause(), health.cause()))
            );
        } else {
            outcome.expect("actual original catalog CPU close");
        }
        assert!(gate.returned.load(Ordering::Acquire));
        {
            let state = custody.state.lock().unwrap();
            assert_eq!(state.joined, health.is_none());
            assert_eq!(state.health.is_some(), health.is_some());
            assert!(Arc::ptr_eq(state.owned_catalog_cpu.as_ref().unwrap(), &cpu));
        }
        // The unreceived Inline result remains in the same CPU source body,
        // including its genuine original Field, after actual worker exit.
        let original_field = gate.source_field.lock().unwrap().clone().unwrap();
        assert!(original_field.upgrade().is_some());
        assert!(
            custody.resolve_catalog_relations(&[]).await.is_err(),
            "a closed custody cannot rearm its CPU through catalog resolution"
        );
        drop(custody);
        assert!(
            original_field.upgrade().is_some(),
            "the last original CPU alias still owns its output"
        );
        drop(cpu);
        drop(supervisor);
        owners.shutdown().await;
        assert!(
            original_field.upgrade().is_none(),
            "actual source release retires the original output"
        );
    }

    #[tokio::test]
    async fn empty_completion_custody_does_not_close_a_borrowed_planning_cpu() {
        let (mut owners, supervisor) =
            crate::task_execution::blocking_io::tests::original_executors(
                tokio::runtime::Handle::current(),
            );
        let cpu = FrontendPlanningCpuSource::new(supervisor.clone());
        let custody = FrontendCompletionCustody::new();
        custody.close().await.expect("unused custody close");
        assert!(custody.state.lock().unwrap().owned_catalog_cpu.is_none());
        // The actual borrowed Read/Explain CPU can still accept its next turn.
        let logical = cpu
            .logical_catalog(
                query("SELECT 1 AS retained_value"),
                SqlSessionContext {
                    current_catalog: None,
                    current_database: "db".into(),
                    optimizer_settings: Default::default(),
                    sql_semantics: Default::default(),
                },
                SqlCompileControl::unbounded(),
            )
            .await
            .expect("same borrowed CPU actual exit")
            .expect("logical query");
        assert!(logical.needs().is_empty());
        drop(logical);
        cpu.close().await.expect("borrower's original CPU close");
        drop(custody);
        drop(cpu);
        drop(supervisor);
        owners.shutdown().await;
    }
}
