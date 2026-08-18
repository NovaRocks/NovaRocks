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

use crate::query_execution::kernels::BackendManagementKernel;
use crate::runtime::statement_result::StatementResult;
use novarocks_types::ClusterRole;

#[derive(Clone)]
pub struct BackendCommandExecutor {
    kernel: BackendManagementKernel,
}

fn require_backend_management_role(statement: &str, role: ClusterRole) -> Result<(), String> {
    match role {
        ClusterRole::Fe => Ok(()),
        ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
        ClusterRole::AllInOne => Err(format!("{statement} requires role=fe")),
    }
}

impl BackendCommandExecutor {
    pub fn new(kernel: BackendManagementKernel) -> Self {
        Self { kernel }
    }

    pub fn try_execute(
        &self,
        sql: &str,
        role: ClusterRole,
    ) -> Result<Option<StatementResult>, String> {
        match novarocks_sql::syntax::parse_backend_management_command(sql)? {
            Some(novarocks_sql::syntax::BackendManagementCommand::Add { address }) => {
                require_backend_management_role("ADD BACKEND", role)?;
                let endpoint = address
                    .parse()
                    .map_err(|error| format!("invalid backend address '{address}': {error}"))?;
                self.kernel.topology().add_backend(endpoint)?;
                Ok(Some(StatementResult::Ok))
            }
            Some(novarocks_sql::syntax::BackendManagementCommand::Drop { address, force }) => {
                require_backend_management_role("DROP BACKEND", role)?;
                let endpoint = address
                    .parse()
                    .map_err(|error| format!("invalid backend address '{address}': {error}"))?;
                self.kernel.topology().drop_backend(endpoint, force)?;
                Ok(Some(StatementResult::Ok))
            }
            Some(novarocks_sql::syntax::BackendManagementCommand::Show) => {
                if role == ClusterRole::Be {
                    return Err("SHOW BACKENDS is not available in role=be".to_string());
                }
                self.kernel
                    .topology()
                    .show_backends()
                    .map(StatementResult::Query)
                    .map(Some)
            }
            None => Ok(None),
        }
    }
}
