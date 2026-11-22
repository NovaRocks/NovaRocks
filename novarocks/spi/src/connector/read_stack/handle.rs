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

//! Trino-named marker interfaces for connector-owned handles.
//!
//! Each provider defines its own concrete handle types and implements these
//! markers. The SPI never names a provider, never carries opaque bytes, and
//! never downcasts: code that must interpret a concrete handle lives in the
//! provider that produced it, or in a role adapter over the closed wire
//! carrier.

use std::fmt::Debug;
use std::sync::Arc;

use crate::connector::{ConnectorError, ConnectorErrorKind};

/// The maximum length of one schema or table name component.
pub const MAX_SCHEMA_TABLE_NAME_BYTES: usize = 1024;

/// A schema-qualified relation name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaTableName {
    schema_name: Arc<str>,
    table_name: Arc<str>,
}

impl SchemaTableName {
    pub fn try_new(
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            schema_name: bounded_name(schema_name.as_ref(), "schema")?,
            table_name: bounded_name(table_name.as_ref(), "table")?,
        })
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

fn bounded_name(value: &str, what: &'static str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty() || value.len() > MAX_SCHEMA_TABLE_NAME_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            match what {
                "schema" => "connector schema name must be non-empty and bounded",
                _ => "connector table name must be non-empty and bounded",
            },
        ));
    }
    Ok(Arc::from(value))
}

/// A connector-owned handle for one planned table read.
pub trait ConnectorTableHandle: Debug + Send + Sync + 'static {
    /// The relation this handle reads.
    fn schema_table_name(&self) -> &SchemaTableName;
}

/// A connector-owned handle for one table-function invocation.
pub trait ConnectorTableFunctionHandle: Debug + Send + Sync + 'static {}

/// A connector-owned handle for the read side of a row-level merge.
pub trait ConnectorMergeTableHandle: Debug + Send + Sync + 'static {
    fn schema_table_name(&self) -> &SchemaTableName;
}

/// A connector-owned transaction marker.
///
/// The frontend transaction manager is the only transaction owner; a worker
/// never creates, looks up, or extends a transaction from this marker.
pub trait ConnectorTransactionHandle: Debug + Send + Sync + 'static {}

/// A connector-owned column reference.
///
/// The bounds make a column handle usable as a `TupleDomain` key and as a
/// deterministic map key in canonical orderings.
pub trait ColumnHandle: Clone + Debug + Eq + Ord + Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_table_names_are_bounded_and_non_empty() {
        assert!(SchemaTableName::try_new("", "t").is_err());
        assert!(SchemaTableName::try_new("s", "").is_err());
        assert!(
            SchemaTableName::try_new("s", "x".repeat(MAX_SCHEMA_TABLE_NAME_BYTES + 1)).is_err()
        );
        let name = SchemaTableName::try_new("s", "t").expect("valid");
        assert_eq!(name.schema_name(), "s");
        assert_eq!(name.table_name(), "t");
    }
}
