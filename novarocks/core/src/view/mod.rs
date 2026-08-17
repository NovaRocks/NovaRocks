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

//! View application ports and core engine adapter.
//!
//! The public traits and DTOs are the dependency-inversion boundary used by
//! `novarocks-frontend`: core exposes only the engine capabilities required by
//! view DDL and rewrite, without leaking the retired Core application facade, connector
//! backends, or parser-internal column definitions.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::catalog_application::query_catalog::CatalogServiceSource;
use crate::query_execution::kernels::ViewExecutionKernel;
use crate::runtime::query_result::QueryResult;
pub mod view_command;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorViewDefinition, ConnectorViewDialect, ConnectorViewIdentity,
    ConnectorViewRequest, CreateOrReplacePolicy, DropPolicy,
};
/// Shared StarRocks SQL syntax contract for view DDL, storage, and rewrite.
pub use novarocks_sql::syntax::StarRocksDialect as ViewSqlDialect;

#[derive(Clone, Copy)]
pub struct ViewRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
    /// Query-owned context used by connector reads and external view mutations.
    pub connector_context: Option<&'a ConnectorRequestContext>,
}

#[derive(Clone, Debug)]
pub enum ViewStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ViewTarget {
    pub catalog: String,
    pub database: String,
    pub view: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ViewColumnDefinition {
    pub name: String,
    pub data_type: sqlparser::ast::DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateExternalViewRequest {
    pub target: ViewTarget,
    pub columns: Vec<ViewColumnDefinition>,
    pub sql: String,
    pub comment: Option<String>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedExternalView {
    pub sql: String,
    pub dialect: String,
    pub default_database: String,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExternalViewResolution {
    Table,
    View(ResolvedExternalView),
    Missing,
}

pub trait ViewService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn ViewEngine,
        sql: &str,
        context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String>;

    fn rewrite_query(
        &self,
        engine: &dyn ViewEngine,
        query: &mut sqlparser::ast::Query,
        context: ViewRequestContext<'_>,
    ) -> Result<(), String>;

    fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String>;
}

pub trait ViewEngine: Send + Sync {
    /// Resolve a table-or-view name through exactly one connector control
    /// generation.  Missing view metadata is not equivalent to an undeclared
    /// view capability; the latter remains a typed Unsupported error.
    fn resolve_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String>;
    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        context: &ConnectorRequestContext,
    ) -> Result<(), String>;
    fn drop_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
        policy: DropPolicy,
    ) -> Result<(), String>;
    fn load_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String>;
    fn list_external_views(
        &self,
        catalog: &str,
        database: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, String>;
    fn analyze_external_view(
        &self,
        catalog: &str,
        database: &str,
        query: &sqlparser::ast::Query,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyViewService;

impl ViewService for EmptyViewService {
    fn try_handle_statement(
        &self,
        _engine: &dyn ViewEngine,
        sql: &str,
        _context: ViewRequestContext<'_>,
    ) -> Result<Option<ViewStatementResult>, String> {
        let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
        if normalized.starts_with("create view ")
            || normalized.starts_with("create or replace view ")
            || normalized.starts_with("drop view ")
            || normalized.starts_with("show create view ")
            || normalized == "show views"
            || normalized.starts_with("show views ")
        {
            return Err("view service is not injected".to_string());
        }
        Ok(None)
    }

    fn rewrite_query(
        &self,
        _engine: &dyn ViewEngine,
        _query: &mut sqlparser::ast::Query,
        _context: ViewRequestContext<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    fn drop_database(&self, _catalog: &str, _database: &str) -> Result<(), String> {
        Ok(())
    }
}

/// The exact leaf dependencies needed by external view metadata operations.
/// This deliberately stays narrower than either a session or an application
/// aggregate, so frontend composition can pass its typed view kernel directly.
trait ViewExecutionContext: CatalogServiceSource + Send + Sync {
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlRegistry;
    fn catalog_application(
        &self,
    ) -> Option<&dyn crate::catalog_application::CatalogApplicationPort>;
}

impl ViewExecutionContext for ViewExecutionKernel {
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlRegistry {
        self.connector_control().as_ref()
    }

    fn catalog_application(
        &self,
    ) -> Option<&dyn crate::catalog_application::CatalogApplicationPort> {
        self.catalog_application().map(Arc::as_ref)
    }
}

impl<T> ViewEngine for T
where
    T: ViewExecutionContext,
{
    fn resolve_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<ExternalViewResolution, String> {
        let lease = crate::connector::acquire_metadata_planning_lease(
            self.connector_control(),
            &target.catalog,
        )?;
        if crate::connector::metadata_table_exists_with_planning_lease(
            lease.clone(),
            context.clone(),
            &target.database,
            &target.view,
        )? {
            return Ok(ExternalViewResolution::Table);
        }
        let binding = lease.binding();
        let Some(capability) = binding.view_metadata() else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            )
            .to_string());
        };
        let instance_id = binding.descriptor().instance_id.clone();
        match capability.load_view(ConnectorViewRequest {
            view: ConnectorViewIdentity {
                instance_id,
                namespace: Arc::from(target.database.as_str()),
                view: Arc::from(target.view.as_str()),
            },
            context: context.clone(),
        }) {
            Ok(view) => Ok(ExternalViewResolution::View(resolved_external_view(view))),
            // A catalog that cannot host views has none, so resolution treats
            // that exactly like an absent view and the name falls through to
            // the ordinary unknown-relation error. Creating a view still fails
            // loudly with the provider's capability error.
            Err(error)
                if matches!(
                    error.kind(),
                    ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
                ) =>
            {
                Ok(ExternalViewResolution::Missing)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn create_external_view(
        &self,
        request: CreateExternalViewRequest,
        context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        let columns = request
            .columns
            .into_iter()
            .map(|column| {
                let column = novarocks_sql::syntax::TableColumnDef {
                    name: column.name,
                    data_type: novarocks_sql::syntax::convert_sql_type(column.data_type)?,
                    nullable: column.nullable,
                    aggregation: None,
                    default: None,
                };
                crate::catalog_application::statement::connector_column(&column)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let instance_id = ConnectorInstanceId::parse(&request.target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::mutation::execute_catalog_mutation(
            self.connector_control(),
            &instance_id,
            ConnectorCatalogMutationOperation::CreateView {
                view: ConnectorViewIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(request.target.database),
                    view: Arc::from(request.target.view),
                },
                columns,
                definition: ConnectorViewDefinition {
                    dialect: ConnectorViewDialect::StarRocks,
                    sql: Arc::from(request.sql),
                },
                comment: request.comment.map(Arc::from),
                properties: request
                    .properties
                    .into_iter()
                    .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                    .collect(),
                policy: if request.or_replace {
                    CreateOrReplacePolicy::ReplaceIfExists
                } else if request.if_not_exists {
                    CreateOrReplacePolicy::NoOpIfExists
                } else {
                    CreateOrReplacePolicy::FailIfExists
                },
            },
            context.clone(),
        )
        .map(|_| ())
    }

    fn drop_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
        policy: DropPolicy,
    ) -> Result<(), String> {
        let instance_id =
            ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
        crate::connector::mutation::execute_catalog_mutation(
            self.connector_control(),
            &instance_id,
            ConnectorCatalogMutationOperation::DropView {
                view: ConnectorViewIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.database.as_str()),
                    view: Arc::from(target.view.as_str()),
                },
                policy,
            },
            context.clone(),
        )
        .map(|_| ())
    }

    fn load_external_view(
        &self,
        target: &ViewTarget,
        context: &ConnectorRequestContext,
    ) -> Result<Option<ResolvedExternalView>, String> {
        let lease = crate::connector::acquire_metadata_planning_lease(
            self.connector_control(),
            &target.catalog,
        )?;
        let binding = lease.binding();
        let Some(capability) = binding.view_metadata() else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            )
            .to_string());
        };
        let instance_id = binding.descriptor().instance_id.clone();
        match capability.load_view(ConnectorViewRequest {
            view: ConnectorViewIdentity {
                instance_id,
                namespace: Arc::from(target.database.as_str()),
                view: Arc::from(target.view.as_str()),
            },
            context: context.clone(),
        }) {
            Ok(view) => Ok(Some(resolved_external_view(view))),
            // See `resolve_external_view`: a catalog without view support has
            // no view to find.
            Err(error)
                if matches!(
                    error.kind(),
                    ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn list_external_views(
        &self,
        catalog: &str,
        database: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, String> {
        let lease =
            crate::connector::acquire_metadata_planning_lease(self.connector_control(), catalog)?;
        let binding = lease.binding();
        let Some(capability) = binding.view_metadata() else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            )
            .to_string());
        };
        let instance_id = binding.descriptor().instance_id.clone();
        capability
            .list_views(novarocks_spi::connector::ConnectorListViewsRequest {
                namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                    instance_id,
                    namespace: Arc::from(database),
                },
                context: context.clone(),
            })
            .map(|views| {
                let mut names = views
                    .into_iter()
                    .map(|view| view.view.to_string())
                    .collect::<Vec<_>>();
                names.sort();
                names.dedup();
                names
            })
            .map_err(|error| error.to_string())
    }

    fn analyze_external_view(
        &self,
        catalog: &str,
        database: &str,
        query: &sqlparser::ast::Query,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<ViewColumnDefinition>, String> {
        let catalog_service_snapshot =
            crate::catalog_application::query_catalog::catalog_service_snapshot(self);
        let provider =
            crate::catalog_application::query_materializer::build_catalog_service_provider(
                Some(catalog),
                &catalog_service_snapshot,
                self.connector_control(),
                context.clone(),
                novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
                self.catalog_application(),
            );
        let columns =
            novarocks_sql::planning::catalog::analyze_view_query(query, &provider, database)?
                .into_iter()
                .map(|column| {
                    Ok(ViewColumnDefinition {
                        name: column.name,
                        data_type: view_sqlparser_data_type(&column.data_type)?,
                        nullable: column.nullable,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
        if columns.is_empty() {
            return Err("CREATE VIEW: SELECT produced no output columns".to_string());
        }
        Ok(columns)
    }
}

fn resolved_external_view(
    view: novarocks_spi::connector::ConnectorViewMetadataValue,
) -> ResolvedExternalView {
    ResolvedExternalView {
        sql: view.definition.sql.to_string(),
        dialect: match view.definition.dialect {
            ConnectorViewDialect::StarRocks => "starrocks".to_string(),
        },
        default_database: view.default_namespace.to_string(),
        column_names: view
            .column_names
            .into_iter()
            .map(|name| name.to_string())
            .collect(),
        comment: view.comment.map(|comment| comment.to_string()),
        properties: view
            .properties
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    }
}

fn view_sqlparser_data_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<sqlparser::ast::DataType, String> {
    use novarocks_catalog::schema::SqlType;
    use sqlparser::ast::{
        ArrayElemTypeDef, DataType, Ident, ObjectName, ObjectNamePart, StructBracketKind,
        StructField, TimezoneInfo,
    };

    fn custom(name: &str, modifiers: Vec<String>) -> DataType {
        DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
            modifiers,
        )
    }

    fn convert(data_type: SqlType) -> DataType {
        match data_type {
            SqlType::TinyInt => DataType::TinyInt(None),
            SqlType::SmallInt => DataType::SmallInt(None),
            SqlType::Int => DataType::Int(None),
            SqlType::BigInt => DataType::BigInt(None),
            SqlType::LargeInt => custom("LARGEINT", vec![]),
            SqlType::Float => DataType::Float(sqlparser::ast::ExactNumberInfo::None),
            SqlType::Double => DataType::Double(sqlparser::ast::ExactNumberInfo::None),
            SqlType::Decimal { precision, scale } => {
                custom("DECIMAL128", vec![precision.to_string(), scale.to_string()])
            }
            SqlType::String => DataType::String(None),
            SqlType::Json => DataType::JSON,
            SqlType::Binary => DataType::Varbinary(None),
            SqlType::Bitmap => custom("BITMAP", vec![]),
            SqlType::Hll => custom("HLL", vec![]),
            SqlType::Boolean => DataType::Boolean,
            SqlType::Date => DataType::Date,
            SqlType::DateTime => DataType::Datetime(None),
            SqlType::DateTimeNs => custom("DATETIME_NS", vec![]),
            SqlType::Time => DataType::Time(None, TimezoneInfo::None),
            SqlType::Array(element) => {
                DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(convert(*element))))
            }
            SqlType::Map(key, value) => {
                DataType::Map(Box::new(convert(*key)), Box::new(convert(*value)))
            }
            SqlType::Struct(fields) => DataType::Struct(
                fields
                    .into_iter()
                    .map(|(name, field_type)| StructField {
                        field_name: Some(Ident::new(name)),
                        field_type: convert(field_type),
                        options: None,
                    })
                    .collect(),
                StructBracketKind::AngleBrackets,
            ),
            SqlType::Variant => custom("VARIANT", vec![]),
        }
    }

    Ok(convert(novarocks_sql::syntax::arrow_data_type_to_sql_type(
        data_type,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::syntax::StarRocksDialect;
    use sqlparser::ast as sqlast;
    use sqlparser::parser::Parser;

    #[derive(Default)]
    struct FakeViewEngine;

    impl ViewEngine for FakeViewEngine {
        fn resolve_external_view(
            &self,
            _target: &ViewTarget,
            _context: &ConnectorRequestContext,
        ) -> Result<ExternalViewResolution, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn create_external_view(
            &self,
            _request: CreateExternalViewRequest,
            _context: &ConnectorRequestContext,
        ) -> Result<(), String> {
            unreachable!("empty view service must not access the engine")
        }

        fn drop_external_view(
            &self,
            _target: &ViewTarget,
            _context: &ConnectorRequestContext,
            _policy: DropPolicy,
        ) -> Result<(), String> {
            unreachable!("empty view service must not access the engine")
        }

        fn load_external_view(
            &self,
            _target: &ViewTarget,
            _context: &ConnectorRequestContext,
        ) -> Result<Option<ResolvedExternalView>, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn list_external_views(
            &self,
            _catalog: &str,
            _database: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<Vec<String>, String> {
            unreachable!("empty view service must not access the engine")
        }

        fn analyze_external_view(
            &self,
            _catalog: &str,
            _database: &str,
            _query: &sqlast::Query,
            _context: &ConnectorRequestContext,
        ) -> Result<Vec<ViewColumnDefinition>, String> {
            unreachable!("empty view service must not access the engine")
        }
    }

    fn parse_query(sql: &str) -> Box<sqlast::Query> {
        let mut parser = Parser::new(&StarRocksDialect).try_with_sql(sql).unwrap();
        match parser.parse_statement().unwrap() {
            sqlast::Statement::Query(q) => q,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn empty_view_service_rejects_view_ddl_but_leaves_queries_unchanged() {
        let service: Arc<dyn ViewService> = Arc::new(EmptyViewService);
        let engine = FakeViewEngine;
        let ctx = ViewRequestContext {
            current_catalog: None,
            current_database: "db",
            connector_context: None,
        };
        assert!(
            service
                .try_handle_statement(&engine, "CREATE VIEW v AS SELECT 1", ctx)
                .unwrap_err()
                .contains("view service is not injected")
        );
        let mut query = parse_query("SELECT * FROM t");
        service.rewrite_query(&engine, &mut query, ctx).unwrap();
        assert_eq!(query.to_string(), "SELECT * FROM t");
    }
}
