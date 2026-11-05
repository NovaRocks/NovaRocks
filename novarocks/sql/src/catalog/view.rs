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

//! Immutable declaration read from one exact stored-view version.

use novarocks_type_contract::FunctionValueType;
use novarocks_types::naming::TableIdentity;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlViewColumn {
    pub name: Arc<str>,
    pub value_type: FunctionValueType,
}

/// Pure facts only: this value carries no catalog lease or I/O capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlViewDefinition {
    pub identity: TableIdentity,
    pub version: Arc<str>,
    pub raw_sql: Arc<str>,
    pub default_catalog: Arc<str>,
    pub default_database: Arc<str>,
    pub columns: Arc<[SqlViewColumn]>,
}

impl SqlViewDefinition {
    pub fn validate(&self) -> Result<(), String> {
        if self.version.is_empty()
            || self.raw_sql.is_empty()
            || self.default_catalog.is_empty()
            || self.default_database.is_empty()
            || self.identity.catalog.is_empty()
            || self.identity.namespace.is_empty()
            || self.identity.table.is_empty()
            || self.columns.is_empty()
        {
            return Err(
                "stored view facts have an empty identity, source, context, or declaration".into(),
            );
        }
        let mut names = std::collections::HashSet::new();
        for column in self.columns.iter() {
            if column.name.is_empty() || !names.insert(column.name.to_lowercase()) {
                return Err(
                    "stored view facts have an empty or duplicate declared column name".into(),
                );
            }
        }
        Ok(())
    }
}
