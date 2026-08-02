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
}
