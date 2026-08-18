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

//! System-catalog injection port. `information_schema` virtual tables are a
//! frontend-owned domain capability (FEH-3); core's query-rewrite path resolves
//! them through this trait so it never names the frontend registry/provider.

use arrow::record_batch::RecordBatch;
use novarocks_catalog::schema::ColumnDef;

/// Inputs the core rewriter gathers from the live engine state and hands to the
/// system catalog. Kept minimal to what the only current provider (`schemata`)
/// needs; extend as providers are added.
pub struct SystemCatalogInputs<'a> {
    /// `catalog_name` column value: `"default_catalog"` for the local catalog,
    /// or the external catalog name for `<cat>.information_schema.schemata`.
    pub catalog_name: &'a str,
    /// One entry per schema/namespace row: sorted+deduped local database names,
    /// or the external catalog's namespaces.
    pub schema_names: &'a [String],
}

/// Columns + materialized rows for a resolved system table.
pub struct SystemTableData {
    pub columns: Vec<ColumnDef>,
    pub batches: Vec<RecordBatch>,
}

/// Resolves `information_schema` virtual-table references to their columns and
/// rows. Implemented by the frontend `SystemCatalogService` and injected into
/// the retired Core application facade; core holds `Arc<dyn SystemCatalog>` and does not depend
/// on the frontend crate.
pub trait SystemCatalog: Send + Sync {
    /// `Ok(None)` = `(db, tbl)` is not a registered system table; the rewriter
    /// leaves the reference untouched for downstream resolution.
    fn resolve(
        &self,
        db: &str,
        tbl: &str,
        inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String>;
}

/// No-op catalog used when nothing is injected (legacy configless open path and
/// tests that do not exercise system tables).
pub struct EmptySystemCatalog;

impl SystemCatalog for EmptySystemCatalog {
    fn resolve(
        &self,
        _db: &str,
        _tbl: &str,
        _inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_system_catalog_returns_none() {
        let names = vec!["a".to_string()];
        let inputs = SystemCatalogInputs {
            catalog_name: "default_catalog",
            schema_names: &names,
        };
        assert!(
            EmptySystemCatalog
                .resolve("information_schema", "schemata", &inputs)
                .unwrap()
                .is_none()
        );
    }
}
