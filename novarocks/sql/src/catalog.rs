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

use crate::planner::table::TableDef;
use crate::planning::catalog::MetadataTableKind;
use novarocks_types::naming::TableIdentity;
use novarocks_types::schema::CatalogTable;

mod inline;
pub(crate) mod memory;
mod view;
pub use inline::{SqlInlineColumn, SqlInlineRelation};
pub use view::{SqlViewColumn, SqlViewDefinition};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableLookupMode {
    SchemaOnly,
    ExplainStats,
}

#[derive(Clone, Debug)]
pub struct ResolvedAnalyzerTable {
    pub(crate) catalog: CatalogTable,
    pub(crate) planner: TableDef,
}

impl ResolvedAnalyzerTable {
    pub(crate) fn from_planner(catalog: Option<&str>, database: &str, planner: TableDef) -> Self {
        let identity = TableIdentity::new(
            catalog.unwrap_or("default_catalog"),
            database,
            &planner.name,
        );
        let table = CatalogTable {
            identity,
            columns: planner.columns.clone(),
            hidden_columns: planner.iceberg_row_lineage_metadata_columns.clone(),
        };
        Self {
            catalog: table,
            planner,
        }
    }
}

/// Planner-facing table materialization extension.
///
/// This is the only ordinary analyzer/planner lookup seam: implementations
/// must return the neutral schema and planner binding from one authoritative
/// resolution.
pub trait PlannerTableProvider {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String>;

    /// An explicit historical request never falls back to a Current lookup.
    fn resolve_table_version_for_analysis(
        &self,
        _catalog: Option<&str>,
        _database: &str,
        _table: &str,
        version: &novarocks_parser::ast::TableVersion,
    ) -> Result<ResolvedAnalyzerTable, crate::analyze_error::AnalyzeError> {
        Err(crate::analyze_error::AnalyzeError::unsupported_query_shape(
            "catalog does not supply historical table facts",
            version.span,
        ))
    }

    /// A fact-only stored definition, selected by the same relation lookup.
    fn resolve_view_for_analysis(
        &self,
        _catalog: Option<&str>,
        _database: &str,
        _view: &str,
    ) -> Result<Option<&SqlViewDefinition>, crate::analyze_error::AnalyzeError> {
        Ok(None)
    }

    /// Definition contexts may distinguish a virtual relation from a current
    /// statement lookup. Pure catalogs without that distinction keep the same
    /// exact fully qualified facts.
    fn resolve_definition_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis(catalog, database, table)
    }
    fn resolve_definition_table_version_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        version: &novarocks_parser::ast::TableVersion,
    ) -> Result<ResolvedAnalyzerTable, crate::analyze_error::AnalyzeError> {
        self.resolve_table_version_for_analysis(catalog, database, table, version)
    }
    fn resolve_definition_view_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        view: &str,
    ) -> Result<Option<&SqlViewDefinition>, crate::analyze_error::AnalyzeError> {
        self.resolve_view_for_analysis(catalog, database, view)
    }
    fn resolve_inline_relation_for_analysis(
        &self,
        _catalog: Option<&str>,
        _database: &str,
        _table: &str,
    ) -> Result<Option<&SqlInlineRelation>, crate::analyze_error::AnalyzeError> {
        Ok(None)
    }
    fn resolve_definition_inline_relation_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<Option<&SqlInlineRelation>, crate::analyze_error::AnalyzeError> {
        self.resolve_inline_relation_for_analysis(catalog, database, table)
    }

    /// A replacement definition's target; absent relations still resolve Missing.
    fn view_definition_target(&self) -> Option<&TableIdentity> {
        None
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        None
    }
}

pub trait IcebergMetadataTableProvider {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: MetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String>;
}
