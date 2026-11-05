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

//! Role-owned composition of explicitly authorized test capabilities.

use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner};
use novarocks_failpoint::scope::{ProcessBinding, SessionScope};
use novarocks_failpoint::target_binding::{StatementInputNomination, StatementWorkNomination};
use novarocks_types::FrontendProcessId;
use std::sync::Arc;

pub(crate) mod result_handoff;

pub type StatementInputHook =
    Arc<dyn Fn(&StatementInputNomination) -> Result<(), String> + Send + Sync>;
pub type StatementNominationHook = Arc<
    dyn Fn(&StatementInputNomination, &StatementWorkNomination) -> Result<(), String> + Send + Sync,
>;
pub type ProviderCloseHook = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;
type FrontendProviderBindHook =
    Arc<dyn Fn(&SessionScope) -> Result<FrontendProviderTestBindings, String> + Send + Sync>;

/// These callbacks are fixed Server composition, never declaration-supplied code.
#[derive(Default)]
pub struct FrontendProviderTestBindings {
    pub pin_writer_input: Option<StatementInputHook>,
    pub nominate_writer: Option<StatementNominationHook>,
    pub close: Option<ProviderCloseHook>,
}

#[derive(Clone)]
pub struct FrontendTestSessionConfig {
    prepared: PreparedRoleSession,
    provider_bind: Option<FrontendProviderBindHook>,
}
impl FrontendTestSessionConfig {
    pub fn new(prepared: PreparedRoleSession) -> Self {
        Self {
            prepared,
            provider_bind: None,
        }
    }
    pub fn with_provider_bind(mut self, bind: FrontendProviderBindHook) -> Self {
        self.provider_bind = Some(bind);
        self
    }
    pub(crate) fn bind(&self, process: FrontendProcessId) -> Result<FrontendTestSession, String> {
        let role = self
            .prepared
            .bind(ProcessBinding::Frontend(process))
            .map_err(|error| error.to_string())?;
        let scope = role.scope();
        let allowed = [
            "statement-observation",
            "result-handoff",
            "mv",
            "native",
            "native-observation",
            "iceberg-observation",
            "root-observation",
            "table-maintenance",
        ];
        if scope.domain_names().any(|name| !allowed.contains(&name)) {
            return Err("unsupported FE test-session domain".into());
        }
        let mv_grants = scope.grants_for("mv");
        let mv = if mv_grants.is_empty() {
            None
        } else {
            Some(Arc::new(
                crate::mv::test_controls::MvTestControls::from_grants(mv_grants)?,
            ))
        };
        let maintenance =
            crate::table_maintenance::test_controls::MaintenanceTestControls::from_grants(
                scope.grants_for("table-maintenance"),
            )?;
        let native = Arc::new(
            novarocks_native_adapter::test_controls::NativeTestControls::from_grants(
                scope.grants_for("native"),
            )?,
        );
        let observation_grants = scope.grants_for("native-observation");
        let observation = if observation_grants.is_empty() {
            None
        } else {
            Some(Arc::new(novarocks_native_adapter::test_observation_controls::NativeObservationControls::from_grants(observation_grants)?))
        };
        let statement = crate::preparation_diagnostics::StatementObservationOwner::from_grants(
            scope.grants_for("statement-observation"),
        )?;
        let result_handoff =
            result_handoff::ResultHandoffControl::from_grants(scope.grants_for("result-handoff"))?;
        if result_handoff.is_some() && statement.is_none() {
            return Err("result handoff control requires exact statement nomination".into());
        }

        let provider = match &self.provider_bind {
            Some(bind) => bind(scope)?,
            None => {
                if !scope.grants_for("iceberg-observation").is_empty() {
                    return Err("provider test capabilities require the Server binding".into());
                }
                FrontendProviderTestBindings::default()
            }
        };

        let mut session = FrontendTestSession {
            role,
            mv,
            maintenance,
            native,
            observation,
            statement,
            result_handoff,

            provider,

            cleanup: None,
            failures: Vec::new(),
        };
        if let Some(owner) = &session.statement {
            let mv = session.mv.clone();
            let native = session.native.clone();
            let observation = session.observation.clone();
            let writer = session.provider.nominate_writer.clone();
            let maintenance = session.maintenance.clone();
            let input_mv = mv.clone();
            let input_maintenance = maintenance.clone();
            let input_native = native.clone();
            let input_observation = observation.clone();
            let input_writer = session.provider.pin_writer_input.clone();
            let installed = owner
                .install_input_freezer(Arc::new(move |input| {
                    if let Some(mv) = &input_mv {
                        mv.pin_statement_input(input)?;
                    }
                    if let Some(maintenance) = &input_maintenance {
                        maintenance.pin_statement_input(input)?;
                    }
                    input_native.pin_statement_input(input)?;
                    if let Some(observation) = &input_observation {
                        observation.pin_statement_input(input)?;
                    }
                    if let Some(writer) = &input_writer {
                        writer(input)?;
                    }
                    Ok(())
                }))
                .and_then(|_| {
                    owner.install_nomination_freezer(Arc::new(move |input, nomination| {
                        if let Some(mv) = &mv {
                            mv.nominate_statement(input, nomination)?;
                        }
                        if let Some(maintenance) = &maintenance {
                            maintenance.nominate_statement(input, nomination)?;
                        }
                        native.nominate_statement(input, nomination)?;
                        if let Some(observation) = &observation {
                            observation.nominate_statement(input, nomination)?;
                        }
                        if let Some(writer) = &writer {
                            writer(input, nomination)?;
                        }
                        Ok(())
                    }))
                });
            if let Err(primary) = installed {
                return Err(match session.close_and_join() {
                    Ok(()) => primary,
                    Err(cleanup) => format!("{primary}; test-session rollback: {cleanup}"),
                });
            }
        }
        Ok(session)
    }
}

pub(crate) struct FrontendTestSession {
    role: RoleSessionOwner,
    pub(crate) mv: Option<Arc<crate::mv::test_controls::MvTestControls>>,
    pub(crate) maintenance:
        Option<Arc<crate::table_maintenance::test_controls::MaintenanceTestControls>>,
    pub(crate) native: Arc<novarocks_native_adapter::test_controls::NativeTestControls>,
    pub(crate) observation:
        Option<Arc<novarocks_native_adapter::test_observation_controls::NativeObservationControls>>,
    pub(crate) statement: Option<Arc<crate::preparation_diagnostics::StatementObservationOwner>>,
    pub(crate) result_handoff: Option<Arc<result_handoff::ResultHandoffControl>>,

    provider: FrontendProviderTestBindings,

    cleanup: Option<std::thread::JoinHandle<Result<(), String>>>,
    failures: Vec<String>,
}
impl FrontendTestSession {
    pub(crate) fn scope(&self) -> &SessionScope {
        self.role.scope()
    }
    pub(crate) fn start_cleanup(
        &mut self,
        diagnostics: Arc<
            crate::query_execution::lifecycle_diagnostics::FrontendLifecycleDiagnostics,
        >,
        projection: Arc<crate::catalog_application::CatalogRuntimeProjection>,
    ) -> Result<(), String> {
        let scope = self.role.scope().clone();
        let statement = self.statement.clone();
        let close = self.provider.close.clone();
        let maintenance = self.maintenance.clone();
        self.cleanup = Some(
            std::thread::Builder::new()
                .name("frontend-test-provider-cleanup".into())
                .spawn(move || {
                    scope.wait_closed();
                    diagnostics.close_observation();

                    let _ = projection;
                    if let Some(statement) = statement {
                        statement.close_observation();
                    }
                    let maintenance_result =
                        maintenance.as_ref().map_or(Ok(()), |owner| owner.close());
                    let provider_result = close.as_ref().map_or(Ok(()), |close| close());
                    match (maintenance_result, provider_result) {
                        (Ok(()), Ok(())) => (),
                        (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
                        (Err(maintenance), Err(provider)) => {
                            return Err(format!(
                                "maintenance close: {maintenance}; provider close: {provider}"
                            ));
                        }
                    }
                    Ok(())
                })
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    pub(crate) fn close_and_join(&mut self) -> Result<(), String> {
        if let Err(error) = self.role.close_and_join() {
            self.failures.push(error.to_string());
        }
        let cleanup = match self.cleanup.take() {
            Some(worker) => worker
                .join()
                .map_err(|_| "frontend test provider cleanup panicked".to_owned())
                .and_then(|result| result),
            None => {
                if let Some(statement) = &self.statement {
                    statement.close_observation();
                }
                let maintenance = self
                    .maintenance
                    .as_ref()
                    .map_or(Ok(()), |owner| owner.close());
                let provider = self.provider.close.as_ref().map_or(Ok(()), |close| close());
                match (maintenance, provider) {
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                    (Err(maintenance), Err(provider)) => Err(format!(
                        "maintenance close: {maintenance}; provider close: {provider}"
                    )),
                }
            }
        };
        if let Err(error) = cleanup {
            self.failures.push(error);
        }
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(self.failures.join("; "))
        }
    }
}
impl Drop for FrontendTestSession {
    fn drop(&mut self) {
        if let Err(error) = self.close_and_join() {
            tracing::error!(%error, "frontend test-session cleanup failed");
        }
    }
}
