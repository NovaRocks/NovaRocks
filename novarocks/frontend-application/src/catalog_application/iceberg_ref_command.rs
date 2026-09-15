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

//! Closed capability for semantic Iceberg branch and tag mutations.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlRegistry;

use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;

#[derive(Clone)]
pub struct IcebergRefCommandExecutor {
    connector_control: Arc<dyn ConnectorControlRegistry>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl IcebergRefCommandExecutor {
    pub fn new(
        connector_control: Arc<dyn ConnectorControlRegistry>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            connector_control,
            storage_observation,
        }
    }

    pub fn execute_command(
        &self,
        statement: &novarocks_sql::semantic::command::AlterIcebergTableSqlCommand,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        crate::mv::domain::ref_flow::execute_with_ports(
            self.connector_control.as_ref(),
            self.storage_observation.as_ref(),
            current_database,
            statement,
            connector_context,
        )
    }
}
