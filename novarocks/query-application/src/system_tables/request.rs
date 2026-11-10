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

use super::SystemTableError;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::system_table::{SystemTableDeclaration, SystemTableKeyConstraint};
use std::collections::BTreeSet;
use std::sync::Arc;

/// A projection and optional conservative pruning facts for one declaration.
/// The context borrows the original statement's stop, deadline and I/O source.
/// `batch_target_rows` is a processing target, never a snapshot size limit.
pub struct SystemTableRequest<'a> {
    pub declaration: Arc<SystemTableDeclaration>,
    pub required_columns: Vec<usize>,
    pub key_constraint: SystemTableKeyConstraint,
    pub context: &'a ConnectorRequestContext,
    pub batch_target_rows: usize,
}

impl<'a> SystemTableRequest<'a> {
    pub fn try_new(
        declaration: Arc<SystemTableDeclaration>,
        required_columns: Vec<usize>,
        key_constraint: SystemTableKeyConstraint,
        context: &'a ConnectorRequestContext,
        batch_target_rows: usize,
    ) -> Result<Self, SystemTableError> {
        if batch_target_rows == 0 {
            return Err(SystemTableError::InvalidRequest(
                "system table batch target must be positive".into(),
            ));
        }
        let mut seen = BTreeSet::new();
        for &ordinal in &required_columns {
            if ordinal >= declaration.columns.len() || !seen.insert(ordinal) {
                return Err(SystemTableError::InvalidRequest(
                    "system table projection contains an invalid or duplicate ordinal".into(),
                ));
            }
        }
        if key_constraint
            .columns()
            .keys()
            .any(|ordinal| !declaration.key_columns.contains(ordinal))
        {
            return Err(SystemTableError::InvalidRequest(
                "system table constraint names a non-key column".into(),
            ));
        }
        Ok(Self {
            declaration,
            required_columns,
            key_constraint,
            context,
            batch_target_rows,
        })
    }

    pub fn requires(&self, ordinal: usize) -> bool {
        self.required_columns.contains(&ordinal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_tables::{SystemTableLookup, SystemTableRegistry};
    use novarocks_spi::connector::{
        ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use novarocks_sql::system_table::{SystemTableBinding, SystemTableKeyValues};
    use std::time::{Duration, Instant};

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap()
    }
    fn declaration() -> Arc<SystemTableDeclaration> {
        let registry = SystemTableRegistry::try_new([]).unwrap();
        let SystemTableLookup::Declared(declaration) = registry.declare(
            &SystemTableBinding::Catalog("ice".into()),
            "information_schema",
            "tables",
        ) else {
            panic!("tables declaration");
        };
        declaration
    }
    #[test]
    fn empty_projection_and_requested_order_are_preserved() {
        let context = context();
        for columns in [vec![], vec![3, 0]] {
            let request = SystemTableRequest::try_new(
                declaration(),
                columns.clone(),
                SystemTableKeyConstraint::default(),
                &context,
                4096,
            )
            .unwrap();
            assert_eq!(request.required_columns, columns);
            assert!(std::ptr::eq(request.context, &context));
        }
    }
    #[test]
    fn invalid_projection_and_nonkey_constraints_are_rejected() {
        let context = context();
        for columns in [vec![4], vec![1, 1]] {
            assert!(matches!(
                SystemTableRequest::try_new(
                    declaration(),
                    columns,
                    SystemTableKeyConstraint::default(),
                    &context,
                    4096
                ),
                Err(SystemTableError::InvalidRequest(_))
            ));
        }
        let mut keys = SystemTableKeyConstraint::default();
        keys.set_values(0, SystemTableKeyValues::Any);
        assert!(matches!(
            SystemTableRequest::try_new(declaration(), vec![], keys, &context, 4096),
            Err(SystemTableError::InvalidRequest(_))
        ));
    }
}
