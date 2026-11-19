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

//! One governed LIKE source read; column projection and target mutation are
use super::maintenance::io_admission::MetadataIoAdmission;
use crate::{
    connector::{context_for_planning_pin_typed, mutation::command_error_from_connector},
    query::product_command_service_error,
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    api::{CatalogTableReadBinding, CommandContext, CommandError, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::QueryServiceError,
    session_outcome::{cancellation_error, governed_cancellation_error},
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorMetadataMaintenanceIoProfile, ConnectorTableRequest,
    ConnectorTableResolution,
};
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

type Call = Pin<Box<dyn Future<Output = Result<bool, ConnectorError>> + Send>>;

/// Observes one strict base-table existence request on the caller's exact
/// retained generation and original Root.
pub(crate) struct TableExistsSource {
    binding: CatalogTableReadBinding,
    command: CommandContext,
    cancellation: QueryCancellationView,
    profile: Option<ConnectorMetadataMaintenanceIoProfile>,
    io: Option<Arc<MetadataIoAdmission>>,
    completed_io: Option<Arc<MetadataIoAdmission>>,
    bootstrapped: bool,
    call: Option<Call>,
    outcome: Option<Result<bool, QueryServiceError>>,
    progress_failure: Option<WorkError>,
    delivered: bool,
    in_foreign: bool,
    health: Option<StatementNativeHealthFailure>,
}

impl TableExistsSource {
    pub(crate) fn try_new(
        binding: CatalogTableReadBinding,
        command: CommandContext,
        cancellation: QueryCancellationView,
    ) -> Result<Self, QueryServiceError> {
        if !command
            .result_window_alias()
            .is_some_and(|window| window.is_for_scope(command.scope()))
            || command.connector_context().io_admission().is_some()
            || command.connector_context().metadata_cpu().is_some()
        {
            return Err(work(WorkError::Conflict));
        }
        Ok(Self {
            binding,
            command,
            cancellation,
            profile: None,
            io: None,
            completed_io: None,
            bootstrapped: false,
            call: None,
            outcome: None,
            progress_failure: None,
            delivered: false,
            in_foreign: false,
            health: None,
        })
    }

    pub(crate) fn with_io_profile(
        mut self,
        profile: ConnectorMetadataMaintenanceIoProfile,
    ) -> Self {
        self.profile = Some(profile);
        self
    }

    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                self.in_foreign.then(|| {
                    unproven(
                        "table existence source exit is unproven",
                        WorkError::Conflict,
                    )
                })
            })
    }

    fn observed_cancellation(
        &self,
        owner: &GovernedActionContext<'_>,
    ) -> Option<QueryServiceError> {
        self.cancellation
            .reason()
            .map(cancellation_error)
            .or_else(|| {
                owner
                    .cancellation()
                    .reason()
                    .map(governed_cancellation_error)
            })
    }

    fn setup(&mut self, owner: &mut GovernedActionContext<'_>) -> Result<(), QueryServiceError> {
        if let Some(error) = self.observed_cancellation(owner) {
            return Err(error);
        }
        let original = owner
            .execution_owner_slot()
            .as_ref()
            .filter(|original| original.is_for_scope(self.command.scope()))
            .ok_or_else(|| work(WorkError::Conflict))?;
        let dependencies = self
            .binding
            .planning()
            .binding()
            .metadata()
            .read_planning_initial_dependencies()
            .map_err(connector)?;
        self.io = Some(
            match self.profile.take() {
                Some(profile) => {
                    MetadataIoAdmission::for_profiled_probe(original, profile, dependencies)
                }
                None => MetadataIoAdmission::for_original_source(original, dependencies),
            }
            .map_err(work)?,
        );
        Ok(())
    }

    fn prepare_call(&self) -> Result<Call, QueryServiceError> {
        let request = ConnectorTableRequest {
            table: self.binding.table().clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context_for_planning_pin_typed(
                self.binding.planning(),
                self.command.connector_context().clone(),
            )
            .map_err(connector)?
            .with_io_admission(
                self.io
                    .as_ref()
                    .expect("bootstrapped table existence I/O")
                    .clone(),
            ),
        };
        let pin = self.binding.planning().clone();
        Ok(Box::pin(async move {
            pin.binding().metadata().table_exists_async(request).await
        }))
    }

    pub(crate) fn poll_result(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<bool, QueryServiceError>> {
        assert!(!self.delivered, "one original table existence result");
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if self.io.is_none()
            && self.outcome.is_none()
            && let Err(error) = self.setup(owner)
        {
            self.outcome = Some(Err(error));
        }
        if self.outcome.is_none() {
            let io = self.io.as_ref().expect("original table existence I/O");
            self.in_foreign = true;
            let progress = if self.bootstrapped {
                io.poll_actual_progress_continuing(cx)
            } else {
                owner
                    .execution_owner_slot()
                    .as_ref()
                    .ok_or(WorkError::Conflict)
                    .and_then(|original| io.poll_progress(original, cx))
            };
            self.in_foreign = false;
            match progress {
                Ok(ready) => self.bootstrapped |= ready,
                // A provider future already started still owns its I/O. Drive
                // it and retain its actual result before observing closure.
                Err(error) if self.call.is_none() => self.outcome = Some(Err(work(error))),
                Err(error) => {
                    self.progress_failure.get_or_insert(error);
                }
            }
            if self.outcome.is_none() && self.call.is_none() {
                if let Some(error) = self.observed_cancellation(owner) {
                    self.outcome = Some(Err(error));
                } else if self.bootstrapped {
                    match self.prepare_call() {
                        Ok(call) => self.call = Some(call),
                        Err(error) => self.outcome = Some(Err(error)),
                    }
                }
            }
            if let Some(call) = self.call.as_mut() {
                self.in_foreign = true;
                if let Poll::Ready(result) = call.as_mut().poll(cx) {
                    self.outcome = Some(result.map_err(connector));
                    drop(self.call.take());
                }
                self.in_foreign = false;
            }
        }
        if self.local_health_failure().is_some() || self.outcome.is_none() {
            return Poll::Pending;
        }
        if let Some(io) = self.io.as_ref() {
            self.in_foreign = true;
            let withdrawn = if self.bootstrapped {
                Ok(None)
            } else {
                io.withdraw_unstarted_in()
            };
            self.in_foreign = false;
            let io_failure = match withdrawn {
                Err(error) => {
                    self.health = Some(unproven(
                        "table existence I/O withdrawal is unproven",
                        error,
                    ));
                    return Poll::Pending;
                }
                Ok(Some(receipt)) => receipt.into_ordinary_failure(),
                Ok(None) => {
                    self.in_foreign = true;
                    let closed = io.poll_actual_quiescent_continuing(cx);
                    self.in_foreign = false;
                    match ready!(closed) {
                        Ok(receipt) => receipt.into_ordinary_failure(),
                        Err(health) => {
                            self.health = Some(health);
                            return Poll::Pending;
                        }
                    }
                }
            };
            if let Some(error) = io_failure
                && self.outcome.as_ref().is_some_and(Result::is_ok)
            {
                self.outcome = Some(Err(work(error)));
            }
        }
        if let Some(error) = self.progress_failure.take()
            && self.outcome.as_ref().is_some_and(Result::is_ok)
        {
            self.outcome = Some(Err(work(error)));
        }
        let outcome = self
            .outcome
            .take()
            .expect("original table existence outcome");
        if outcome.is_ok() {
            self.completed_io = Some(
                self.io
                    .take()
                    .expect("same positive table existence source"),
            );
        }
        self.delivered = true;
        Poll::Ready(outcome)
    }

    /// Transfer the positive Root source once after every probe request exits.
    pub(crate) fn take_completed_io(&mut self) -> Option<Arc<MetadataIoAdmission>> {
        self.completed_io.take()
    }
}

fn work(error: WorkError) -> QueryServiceError {
    product_command_service_error(CommandError::from_failure_cause(SharedFailureCause::new(
        error,
    )))
}
fn connector(error: ConnectorError) -> QueryServiceError {
    product_command_service_error(command_error_from_connector(error))
}
fn unproven(message: &'static str, error: WorkError) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(message, Arc::new(error))
}
