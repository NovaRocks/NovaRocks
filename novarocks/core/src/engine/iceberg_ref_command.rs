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

//! Closed capability for `ALTER ICEBERG REF`.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlRegistry;

use crate::engine::StatementResult;
use crate::mv::storage_observation::MvStorageObservationPort;

#[derive(Clone)]
pub struct IcebergRefCommandExecutor {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl IcebergRefCommandExecutor {
    pub(crate) fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            connector_control,
            storage_observation,
        }
    }

    pub fn try_execute(
        &self,
        sql: &str,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let mut statements = match crate::sql::parser::parse_sql(&normalized) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        if statements.len() != 1 {
            return Err("Iceberg ref command accepts exactly one statement".to_string());
        }
        let crate::sql::parser::ast::Statement::AlterIcebergRef(statement) =
            statements.pop().expect("one checked statement")
        else {
            return Ok(None);
        };
        crate::engine::iceberg_ref_flow::execute_with_ports(
            self.connector_control.as_ref(),
            self.storage_observation.as_ref(),
            current_database,
            &statement,
            connector_context,
        )
        .map(Some)
    }
}
