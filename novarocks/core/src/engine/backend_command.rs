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

//! Closed Frontend backend-membership command capability.

use crate::common::app_config::ClusterRole;
use crate::engine::domain::BackendManagementKernel;
use crate::engine::{StatementResult, require_backend_management_role};

#[derive(Clone)]
pub struct BackendCommandExecutor {
    kernel: BackendManagementKernel,
}

impl BackendCommandExecutor {
    pub(crate) fn new(kernel: BackendManagementKernel) -> Self {
        Self { kernel }
    }

    pub fn try_execute(
        &self,
        sql: &str,
        role: ClusterRole,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let mut statements = match crate::sql::parser::parse_sql(&normalized) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        if statements.len() != 1 {
            return Err("backend command accepts exactly one statement".to_string());
        }
        use crate::sql::parser::ast::Statement;
        match statements.pop().expect("one checked statement") {
            Statement::AddBackend(statement) => {
                require_backend_management_role("ADD BACKEND", role)?;
                let endpoint = statement.addr.parse().map_err(|error| {
                    format!("invalid backend address '{}': {error}", statement.addr)
                })?;
                self.kernel.topology().add_backend(endpoint)?;
                Ok(Some(StatementResult::Ok))
            }
            Statement::DropBackend(statement) => {
                require_backend_management_role("DROP BACKEND", role)?;
                let endpoint = statement.addr.parse().map_err(|error| {
                    format!("invalid backend address '{}': {error}", statement.addr)
                })?;
                self.kernel
                    .topology()
                    .drop_backend(endpoint, statement.force)?;
                Ok(Some(StatementResult::Ok))
            }
            Statement::ShowBackends(_) => {
                if role == ClusterRole::Be {
                    return Err("SHOW BACKENDS is not available in role=be".to_string());
                }
                self.kernel
                    .topology()
                    .show_backends()
                    .map(StatementResult::Query)
                    .map(Some)
            }
            _ => Ok(None),
        }
    }
}
