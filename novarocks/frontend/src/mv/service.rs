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

use std::sync::Arc;

use novarocks::mv::application::{
    MvApplicationError, MvApplicationService, MvApplicationStatement, MvEngine, MvRequestContext,
    MvStatementResult,
};
use novarocks::mv::repository::MvRepository;
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::sql::mv_refresh::PreparedMvRefresh;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};

use super::{create, refresh};

/// Frontend-owned application service for materialized-view statements.
///
/// MVX-1 owns only Iceberg CREATE sequencing. Other MV statement classes
/// deliberately return `None` so their existing core routes remain active.
pub struct FrontendMvService {
    repository: Arc<dyn MvRepository>,
    refresh: Option<refresh::FrontendMvRefreshDependencies>,
}

impl FrontendMvService {
    pub fn new(repository: Arc<dyn MvRepository>) -> Self {
        Self {
            repository,
            refresh: None,
        }
    }

    pub(crate) fn with_refresh_dependencies(
        repository: Arc<dyn MvRepository>,
        query_execution: QueryExecutionService,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        first_refresh_activator: Arc<refresh::FrontendMvFirstRefreshWriteActivatorPort>,
    ) -> Self {
        Self {
            repository,
            refresh: Some(refresh::FrontendMvRefreshDependencies {
                query_execution,
                connector_control,
                first_refresh_activator,
            }),
        }
    }
}

impl MvApplicationService for FrontendMvService {
    fn try_handle_statement(
        &self,
        engine: &dyn MvEngine,
        statement: &MvApplicationStatement,
        context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError> {
        match statement {
            MvApplicationStatement::Create(statement) => {
                create::handle_create(self.repository.as_ref(), engine, statement, context)
                    .map(Some)
            }
            // REFRESH needs the immutable admitted execution context, which
            // only the typed refresh entrypoint accepts.  Returning `None`
            // here would let a caller silently fall through to the retired
            // core lifecycle.
            MvApplicationStatement::Refresh(_) => Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "REFRESH MATERIALIZED VIEW requires the frontend refresh entrypoint",
            )),
            MvApplicationStatement::Unhandled => Ok(None),
        }
    }

    fn execute_prepared_refresh(
        &self,
        refresh_plan: PreparedMvRefresh,
        connector_context: ConnectorRequestContext,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let dependencies = self.refresh.as_ref().ok_or_else(|| {
            MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::Unavailable,
                "frontend MV refresh dependencies are not installed",
            )
        })?;
        refresh::execute(
            self.repository.as_ref(),
            dependencies,
            refresh_plan,
            connector_context,
            execution,
        )
    }

    fn prepare_and_execute_refresh(
        &self,
        preparation: &dyn novarocks::sql::mv_refresh::MvRefreshPreparationService,
        statement: MvApplicationStatement,
        target: novarocks::mv::repository::MvTarget,
        connector_context: ConnectorRequestContext,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let MvApplicationStatement::Refresh(statement) = statement else {
            return Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "frontend refresh entrypoint requires REFRESH MATERIALIZED VIEW",
            ));
        };
        let attempt = self.reserve_refresh_attempt()?;
        let prepared = preparation
            .prepare_step(novarocks::sql::mv_refresh::MvRefreshPreparationRequest {
                statement,
                target,
                attempt: attempt.clone(),
            })
            .map_err(|error| {
                MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                    error,
                )
            })?;
        if prepared.attempt != attempt {
            return Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "MV refresh preparation changed the frontend-reserved attempt identity",
            ));
        }
        self.execute_prepared_refresh(prepared, connector_context, execution)
    }
}

impl FrontendMvService {
    fn reserve_refresh_attempt(
        &self,
    ) -> Result<novarocks::sql::mv_refresh::MvRefreshAttemptIdentity, MvApplicationError> {
        let refresh_id = self
            .repository
            .reserve_frontend_refresh_id()
            .map_err(|error| {
                MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::Repository,
                    error.to_string(),
                )
            })?;
        let request_id = *uuid::Uuid::now_v7().as_bytes();
        Ok(novarocks::sql::mv_refresh::MvRefreshAttemptIdentity {
            refresh_id,
            request_id,
            staging_branch: format!("__novarocks_mv_refresh_{refresh_id}"),
            marker_token: uuid::Uuid::now_v7().to_string(),
            staging_create_operation_id: *uuid::Uuid::now_v7().as_bytes(),
            write_operation_id: novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(
                *uuid::Uuid::now_v7().as_bytes(),
            ),
            publication_operation_id: *uuid::Uuid::now_v7().as_bytes(),
            staging_drop_operation_id: *uuid::Uuid::now_v7().as_bytes(),
        })
    }
}
