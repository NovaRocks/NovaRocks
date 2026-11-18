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

//! Session-derived facts frozen at the query application request boundary.

use crate::sql::session::registry::SessionPropertyInputs;
use novarocks_sql::compiler::SessionOptimizerSettings;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RequestSessionContext {
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    presentation: Option<novarocks_result_contract::ClientPresentationFamily>,
    property_inputs: Arc<SessionPropertyInputs>,
}

impl RequestSessionContext {
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        optimizer_settings: SessionOptimizerSettings,
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
        presentation: Option<novarocks_result_contract::ClientPresentationFamily>,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            optimizer_settings,
            sql_semantics,
            presentation,
            property_inputs: Arc::default(),
        }
    }

    pub(crate) fn with_property_inputs(mut self, inputs: SessionPropertyInputs) -> Self {
        self.property_inputs = Arc::new(inputs);
        self
    }

    pub fn property_inputs(&self) -> &Arc<SessionPropertyInputs> {
        &self.property_inputs
    }

    pub fn presentation(&self) -> Option<&novarocks_result_contract::ClientPresentationFamily> {
        self.presentation.as_ref()
    }

    pub fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    pub fn sql_semantics(&self) -> &novarocks_sql::sql_mode::SqlSemanticSettings {
        &self.sql_semantics
    }

    pub fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}
