// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::session_error::QueryServiceError;
use novarocks_parser::Span;
use novarocks_parser::ast;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use novarocks_sql::semantic::{CatalogSqlCommand, ObjectName, StatisticsSqlCommand};
use novarocks_sql::sql_mode::{self, SqlSemanticSettings};

use super::{
    ProductSqlCommand,
    kill_query::{KillQueryCall, KillQuerySqlError, resolves_kill_query},
    lower_object_name,
    property_evaluation::validate_create_authorization,
    session_admit::SessionAdmitError,
};

mod prepared;
pub use prepared::{
    AdmittedTrinoStatement, TrinoDescriptionAction, TrinoDescriptionKind, TrinoStatementSource,
    admit_trino_statement_request, admit_trino_statement_request_in_namespace,
};

pub(super) const UNSUPPORTED_STATEMENT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.unsupported_statement"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};
const NOT_IN_TRANSACTION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.not_in_transaction"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const PROCEDURE_NOT_REGISTERED: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.procedure_not_found"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

const UNKNOWN_TYPE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.unknown_type"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShowAdmitError {
    MissingCatalog,
    MissingSchema,
    CatalogNotFound,
    SchemaNotFound,
    TableNotFound,
    NameTooLong,
}

impl ShowAdmitError {
    const fn descriptor(self) -> ErrorCodeDescriptor {
        let code = match self {
            Self::MissingCatalog => "sql.admit.show_missing_catalog",
            Self::MissingSchema => "sql.admit.show_missing_schema",
            Self::CatalogNotFound => "sql.admit.show_catalog_not_found",
            Self::SchemaNotFound => "sql.admit.show_schema_not_found",
            Self::TableNotFound => "sql.admit.show_table_not_found",
            Self::NameTooLong => "sql.admit.show_name_too_long",
        };
        ErrorCodeDescriptor {
            code: ErrorCodeId::new(code),
            phase: ErrorPhase::Admit,
            status: ErrorCodeStatus::Active,
        }
    }

    pub fn into_service(
        self,
        source: &str,
        span: Span,
        message: impl Into<String>,
    ) -> QueryServiceError {
        QueryServiceError::from_user_error(UserError::from_descriptor(
            self.descriptor(),
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        ))
    }
}

pub const TRINO_STATEMENT_ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    UNSUPPORTED_STATEMENT,
    NOT_IN_TRANSACTION,
    PROCEDURE_NOT_REGISTERED,
    UNKNOWN_TYPE,
    ShowAdmitError::MissingCatalog.descriptor(),
    ShowAdmitError::MissingSchema.descriptor(),
    ShowAdmitError::CatalogNotFound.descriptor(),
    ShowAdmitError::SchemaNotFound.descriptor(),
    ShowAdmitError::TableNotFound.descriptor(),
    ShowAdmitError::NameTooLong.descriptor(),
    KillQuerySqlError::Arguments.descriptor(),
    KillQuerySqlError::ArgumentType.descriptor(),
    KillQuerySqlError::InvalidArgument.descriptor(),
    KillQuerySqlError::Missing.descriptor(),
    KillQuerySqlError::NotRunning.descriptor(),
    KillQuerySqlError::PermissionDenied.descriptor(),
    prepared::PREPARED_STATEMENT_NOT_FOUND,
    prepared::EXPRESSION_NOT_CONSTANT,
    prepared::INVALID_PARAMETER_USAGE,
];

pub fn procedure_not_registered(name: &ast::ObjectName) -> QueryServiceError {
    let procedure = name
        .parts
        .last()
        .expect("parsed CALL name has an identifier");
    QueryServiceError::from_user_error(UserError::from_descriptor(
        PROCEDURE_NOT_REGISTERED,
        format!(
            "Procedure not registered: system.{}",
            procedure.value.to_ascii_lowercase()
        ),
        None,
        RetryClass::Never,
    ))
}

pub(super) fn type_not_found(source: &str, span: Span, message: String) -> QueryServiceError {
    QueryServiceError::from_user_error(UserError::from_descriptor(
        UNKNOWN_TYPE,
        message,
        Some(span.to_user_error_location(source)),
        RetryClass::Never,
    ))
}

pub fn unsupported_statement(source: &str, span: Span, subject: &str) -> QueryServiceError {
    unsupported_statement_detail(source, span, format!("{subject} is not supported"))
}

pub(super) fn unsupported_statement_detail(
    source: &str,
    span: Span,
    message: String,
) -> QueryServiceError {
    QueryServiceError::from_user_error(UserError::from_descriptor(
        UNSUPPORTED_STATEMENT,
        message,
        Some(span.to_user_error_location(source)),
        RetryClass::Never,
    ))
}

pub fn no_transaction_in_progress() -> QueryServiceError {
    QueryServiceError::from_user_error(UserError::from_descriptor(
        NOT_IN_TRANSACTION,
        "No transaction in progress",
        None,
        RetryClass::Never,
    ))
}

/// Executable inputs selected by admission. Preparation carries the one
/// concrete leaf an existing owner must lower before a product command exists.
#[expect(
    clippy::large_enum_variant,
    reason = "Move the parser-owned preparation AST into its command owner without an additional allocation."
)]
#[derive(Debug)]
pub enum TrinoStatementAction {
    Query(TrinoQueryAction),
    Session(TrinoSessionAction),
    Product(ProductSqlCommand),
    Prepare(TrinoStatementPreparation),
    Describe(TrinoDescriptionAction),
}

#[derive(Debug)]
pub enum TrinoQueryAction {
    Read(ast::Query),
    Show(TrinoShowAction),
    Explain(ast::Query),
    ExplainAnalyze(ast::Query),
}

#[derive(Debug)]
pub enum TrinoShowAction {
    Tables(ast::TrinoShowTables),
    Schemas(ast::TrinoShowSchemas),
    Catalogs(ast::TrinoShowCatalogs),
    Columns(ast::TrinoShowColumns),
    Functions(ast::TrinoShowFunctions),
    Session(ast::TrinoShowSession),
}

#[derive(Debug)]
pub enum TrinoSessionAction {
    Use {
        catalog: Option<ast::Ident>,
        schema: ast::Ident,
        span: Span,
    },
    Set {
        name: ast::ObjectName,
        value: ast::Expr,
    },
    Reset {
        name: ast::ObjectName,
        span: Span,
    },
    SetTimeZone(ast::TrinoSetTimeZone),
    Prepare {
        name: String,
        sql: Arc<str>,
    },
    DeallocatePrepare {
        name: String,
    },
}

/// Source-bearing requests to the owning preparation path. These values
/// cannot be sent to ProductCommandRouter: evaluation, binding, and the
/// owner's remaining capability checks precede any execution command.
#[derive(Debug)]
pub enum TrinoStatementPreparation {
    CreateCatalog(ast::TrinoCreateCatalog),
    CreateSchema(ast::TrinoCreateSchema),
    DropSchemaCascade(ast::TrinoDropSchema),
    RenameSchema(ast::TrinoRenameSchema),
    CreateTable(ast::TrinoCreateTable),
    CreateTableAsSelect(ast::TrinoCreateTableAsSelect),
    RenameTable(ast::TrinoRenameTable),
    AddColumn(ast::TrinoAddColumn),
    RenameColumn(ast::TrinoRenameColumn),
    DropColumn(ast::TrinoDropColumn),
    SetColumnType(ast::TrinoSetColumnType),
    DropNotNullConstraint(ast::TrinoDropNotNullConstraint),
    SetTableProperties {
        name: ast::ObjectName,
        properties: ast::AstVec<ast::StatementProperty>,
        span: Span,
    },
    CommentTable(ast::TrinoComment),
    CommentColumn(ast::TrinoComment),
    TableExecute(ast::TrinoTableExecute),
    Analyze(ast::TrinoAnalyze),
    CreateBranch(ast::TrinoCreateBranch),
    DropBranch(ast::TrinoDropBranch),
    FastForwardBranch(ast::TrinoFastForwardBranch),
    ShowBranches(ast::TrinoShowBranches),
    Insert(ast::TrinoInsert),
    Delete(ast::TrinoDelete),
    DeleteEntireBranch {
        table: ObjectName,
        branch: String,
    },
    Update(ast::TrinoUpdate),
    Merge(ast::TrinoMerge),
    Call(ast::TrinoCall),
    KillQuery(Box<KillQueryCall>),
    CreateView(ast::TrinoCreateView),
    DropView(ast::TrinoDropView),
    ShowCreateView(ast::TrinoShowCreate),
    ShowCreateMaterializedView(ast::TrinoShowCreate),
    CreateMaterializedView(ast::TrinoCreateMaterializedView),
    DropMaterializedView(ast::TrinoDropMaterializedView),
    RefreshMaterializedView(ast::TrinoRefreshMaterializedView),
    ExplainRefreshMaterializedView(ast::TrinoRefreshMaterializedView, Span),
    ExplainDelete(ast::TrinoDelete),
    SetMaterializedViewProperties {
        name: ast::ObjectName,
        properties: ast::AstVec<ast::StatementProperty>,
        span: Span,
    },
}

/// Refuse uncaptured settings before publishing a definition or replaying its query.
pub fn admit_persisted_definition_semantics(
    source: &str,
    preparation: &TrinoStatementPreparation,
    caller: &SqlSemanticSettings,
) -> Result<(), QueryServiceError> {
    use TrinoStatementPreparation as Preparation;

    let (definition, span) = match preparation {
        Preparation::CreateView(node) => (Some(&node.query), node.span),
        Preparation::CreateMaterializedView(node) => (Some(&node.query), node.span),
        Preparation::RefreshMaterializedView(node) => (None, node.span),
        Preparation::ExplainRefreshMaterializedView(_, span) => (None, *span),
        Preparation::SetMaterializedViewProperties {
            properties, span, ..
        } if properties
            .iter()
            .any(|property| property.name.value.eq_ignore_ascii_case("partitioning")) =>
        {
            (None, *span)
        }
        _ => return Ok(()),
    };
    let strict = match definition {
        Some(query) => sql_mode::query_uses_error_if_overflow(caller, query).map_err(|error| {
            QueryServiceError::from_user_error(error.to_user_error(Some(source)))
        })?,
        None => caller.sql_mode().error_if_overflow(),
    };
    if !strict {
        return Ok(());
    }
    let message = "unsupported semantic setting: ERROR_IF_OVERFLOW is not captured for persistent VIEW or MATERIALIZED VIEW definition replay";
    Err(QueryServiceError::from_user_error(
        SessionAdmitError::PersistedDefinitionSemanticsUnsupported
            .to_user_error(source, span, message),
    ))
}

/// Select an ordinary statement owner before any provider or session effects.
/// Request dispatch uses `admit_trino_statement_request` to retain source and bindings.
pub fn admit_trino_statement(
    source: &str,
    statement: ast::TrinoStatement,
) -> Result<TrinoStatementAction, QueryServiceError> {
    admit_trino_statement_in_namespace(source, statement, None, None)
}

pub fn admit_trino_statement_in_namespace(
    source: &str,
    statement: ast::TrinoStatement,
    current_catalog: Option<&str>,
    current_schema: Option<&str>,
) -> Result<TrinoStatementAction, QueryServiceError> {
    prepared::admit_ordinary_statement(source, statement, current_catalog, current_schema)
}

fn select_trino_statement_owner(
    source: &str,
    statement: ast::TrinoStatement,
    current_catalog: Option<&str>,
    current_schema: Option<&str>,
) -> Result<TrinoStatementAction, QueryServiceError> {
    use TrinoStatementAction::{Prepare, Product, Query, Session};
    use TrinoStatementPreparation as Preparation;
    use ast::TrinoStatement as Statement;

    let span = statement.span();
    let unsupported = |subject| Err(unsupported_statement(source, span, subject));
    let catalog = |command| Product(ProductSqlCommand::Catalog(Box::new(command)));
    let action = match statement {
        Statement::Query(query) => Query(TrinoQueryAction::Read(query)),
        Statement::Use(node) => Session(TrinoSessionAction::Use {
            catalog: node.catalog,
            schema: node.schema,
            span,
        }),
        Statement::SetSession(node) => Session(TrinoSessionAction::Set {
            name: node.name,
            value: node.value,
        }),
        Statement::ResetSession(node) => Session(TrinoSessionAction::Reset {
            name: node.name,
            span,
        }),
        Statement::Explain(node) => return admit_trino_explain(source, node),
        Statement::ExplainAnalyze(node) => Query(TrinoQueryAction::ExplainAnalyze(
            admit_trino_explain_analyze(source, node)?,
        )),
        Statement::Commit(_) | Statement::Rollback(_) => {
            return Err(no_transaction_in_progress());
        }
        Statement::CreateCatalog(node) => {
            if !node.not_exists {
                validate_create_authorization(
                    source,
                    node.principal.as_ref(),
                    "CREATE CATALOG AUTHORIZATION",
                )?;
            }
            Prepare(Preparation::CreateCatalog(node))
        }
        Statement::DropCatalog(node) => {
            if node.cascade {
                return unsupported("DROP CATALOG CASCADE");
            }
            catalog(CatalogSqlCommand::DropCatalog {
                name: node.catalog_name.value,
                if_exists: node.exists,
            })
        }
        Statement::CreateSchema(node) => {
            if !node.not_exists {
                validate_create_authorization(
                    source,
                    node.principal.as_ref(),
                    "CREATE SCHEMA AUTHORIZATION",
                )?;
            }
            Prepare(Preparation::CreateSchema(node))
        }
        Statement::DropSchema(node) => {
            if node.cascade {
                // The current namespace FORCE owner has additional child
                // deletion policy. Keep CASCADE explicit until that path is bound.
                Prepare(Preparation::DropSchemaCascade(node))
            } else {
                catalog(CatalogSqlCommand::DropDatabase {
                    name: lower_object_name(&node.schema_name),
                    if_exists: node.exists,
                    force: false,
                })
            }
        }
        Statement::RenameSchema(_) => return unsupported("ALTER SCHEMA RENAME"),
        Statement::CreateTable(node) => {
            if node.save_mode == ast::StatementSaveMode::Replace {
                return unsupported("CREATE OR REPLACE TABLE");
            }
            // The original action checks the target before its LIKE and column
            // validator so IF NOT EXISTS can finish without analyzing them.
            Prepare(Preparation::CreateTable(node))
        }
        Statement::CreateTableAsSelect(node) => {
            if node.save_mode == ast::StatementSaveMode::Replace {
                return unsupported("CREATE OR REPLACE TABLE AS");
            }
            Prepare(Preparation::CreateTableAsSelect(node))
        }
        Statement::DropTable(node) => catalog(CatalogSqlCommand::DropTable {
            name: lower_object_name(&node.table_name),
            if_exists: node.exists,
        }),
        Statement::TruncateTable(node) => catalog(CatalogSqlCommand::TruncateTable {
            table: lower_object_name(&node.table_name),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main()
                .as_str()
                .to_owned(),
        }),
        Statement::RenameTable(node) => Prepare(Preparation::RenameTable(node)),
        Statement::AddColumn(node) => Prepare(Preparation::AddColumn(node)),
        Statement::RenameColumn(node) => Prepare(Preparation::RenameColumn(node)),
        Statement::DropColumn(node) => Prepare(Preparation::DropColumn(node)),
        Statement::SetColumnType(node) => Prepare(Preparation::SetColumnType(node)),
        Statement::DropNotNullConstraint(node) => Prepare(Preparation::DropNotNullConstraint(node)),
        Statement::SetDefaultValue(_) => return unsupported("ALTER COLUMN SET DEFAULT"),
        Statement::DropDefaultValue(_) => return unsupported("ALTER COLUMN DROP DEFAULT"),
        Statement::SetProperties(node) => match node.kind {
            ast::SetPropertiesKind::Table => Prepare(Preparation::SetTableProperties {
                name: node.name,
                properties: node.properties,
                span,
            }),
            ast::SetPropertiesKind::MaterializedView => {
                Prepare(Preparation::SetMaterializedViewProperties {
                    name: node.name,
                    properties: node.properties,
                    span,
                })
            }
        },
        Statement::TableExecute(node) => Prepare(Preparation::TableExecute(node)),
        Statement::Comment(node) => match node.kind {
            ast::CommentKind::Table => Prepare(Preparation::CommentTable(node)),
            ast::CommentKind::Column => Prepare(Preparation::CommentColumn(node)),
            ast::CommentKind::View => return unsupported("COMMENT ON VIEW"),
            ast::CommentKind::MaterializedView => {
                return unsupported("COMMENT ON MATERIALIZED VIEW");
            }
        },
        Statement::Analyze(node) => Prepare(Preparation::Analyze(node)),
        Statement::CreateBranch(node) => Prepare(Preparation::CreateBranch(node)),
        Statement::DropBranch(node) => Prepare(Preparation::DropBranch(node)),
        Statement::FastForwardBranch(node) => Prepare(Preparation::FastForwardBranch(node)),
        Statement::ShowBranches(node) => Prepare(Preparation::ShowBranches(node)),
        Statement::Insert(node) => Prepare(Preparation::Insert(node)),
        Statement::Delete(node) => {
            if let Some(predicate) = &node.predicate {
                super::dml_admission::admit_delete_predicate(source, predicate)
                    .map_err(QueryServiceError::from_user_error)?;
            }
            if node.predicate.is_none()
                && node.table.query_period.is_none()
                && let Some(branch) = &node.table.branch
            {
                Prepare(Preparation::DeleteEntireBranch {
                    table: lower_object_name(&node.table.name),
                    branch: branch.value.clone(),
                })
            } else {
                Prepare(Preparation::Delete(node))
            }
        }
        Statement::Update(node) => Prepare(Preparation::Update(node)),
        Statement::Merge(node) => Prepare(Preparation::Merge(node)),
        Statement::Call(node)
            if resolves_kill_query(&node.name, current_catalog, current_schema) =>
        {
            Prepare(Preparation::KillQuery(Box::new(KillQueryCall::bind(
                source, node,
            )?)))
        }
        Statement::Call(node) => Prepare(Preparation::Call(node)),
        Statement::CreateView(node) => Prepare(Preparation::CreateView(node)),
        Statement::DropView(node) => Prepare(Preparation::DropView(node)),
        Statement::RenameView(_) => return unsupported("ALTER VIEW RENAME"),
        Statement::RefreshView(_) => return unsupported("REFRESH VIEW"),
        Statement::CreateMaterializedView(node) => {
            if node.replace {
                return unsupported("CREATE OR REPLACE MATERIALIZED VIEW");
            }
            if node.grace_period.is_some() {
                return unsupported("CREATE MATERIALIZED VIEW GRACE PERIOD");
            }
            if node.when_stale.is_some() {
                return unsupported("CREATE MATERIALIZED VIEW WHEN STALE");
            }
            if node.comment.is_some() {
                return unsupported("CREATE MATERIALIZED VIEW COMMENT");
            }
            Prepare(Preparation::CreateMaterializedView(node))
        }
        Statement::DropMaterializedView(node) => Prepare(Preparation::DropMaterializedView(node)),
        Statement::RefreshMaterializedView(node) => {
            Prepare(Preparation::RefreshMaterializedView(node))
        }
        Statement::RenameMaterializedView(_) => {
            return unsupported("ALTER MATERIALIZED VIEW RENAME");
        }
        Statement::MaterializedViewExecute(_) => {
            return unsupported("ALTER MATERIALIZED VIEW EXECUTE");
        }
        Statement::ShowCreate(node) => match node.kind {
            ast::ShowCreateKind::Table => catalog(CatalogSqlCommand::ShowCreateTable {
                name: lower_object_name(&node.name),
            }),
            ast::ShowCreateKind::View => Prepare(Preparation::ShowCreateView(node)),
            ast::ShowCreateKind::MaterializedView => {
                Prepare(Preparation::ShowCreateMaterializedView(node))
            }
            ast::ShowCreateKind::Schema => return unsupported("SHOW CREATE SCHEMA"),
            ast::ShowCreateKind::Function => return unsupported("SHOW CREATE FUNCTION"),
        },
        Statement::ShowStats(node) => {
            let ast::Relation::Table(table) = node.relation else {
                return unsupported("SHOW STATS FOR a query");
            };
            if table.query_period.is_some() || table.branch.is_some() {
                return unsupported("SHOW STATS FOR a versioned table");
            }
            Product(ProductSqlCommand::Statistics(
                StatisticsSqlCommand::ShowStats {
                    table: lower_object_name(&table.name),
                },
            ))
        }
        Statement::CreateRole(_) => return unsupported("CREATE ROLE"),
        Statement::DropRole(_) => return unsupported("DROP ROLE"),
        Statement::GrantRoles(_) => return unsupported("GRANT roles"),
        Statement::RevokeRoles(_) => return unsupported("REVOKE roles"),
        Statement::Grant(_) => return unsupported("GRANT"),
        Statement::Revoke(_) => return unsupported("REVOKE"),
        Statement::Deny(_) => return unsupported("DENY"),
        Statement::SetRole(_) => return unsupported("SET ROLE"),
        Statement::ShowGrants(_) => return unsupported("SHOW GRANTS"),
        Statement::ShowRoles(_) => return unsupported("SHOW ROLES"),
        Statement::ShowRoleGrants(_) => return unsupported("SHOW ROLE GRANTS"),
        Statement::SetAuthorization(node) => {
            return Err(unsupported_statement(
                source,
                span,
                &format!("ALTER {} SET AUTHORIZATION", node.owned_entity_kind),
            ));
        }
        Statement::ShowTables(node) => Query(TrinoQueryAction::Show(TrinoShowAction::Tables(node))),
        Statement::ShowSchemas(node) => {
            Query(TrinoQueryAction::Show(TrinoShowAction::Schemas(node)))
        }
        Statement::ShowCatalogs(node) => {
            Query(TrinoQueryAction::Show(TrinoShowAction::Catalogs(node)))
        }
        Statement::ShowColumns(node) => {
            Query(TrinoQueryAction::Show(TrinoShowAction::Columns(node)))
        }
        Statement::ShowFunctions(node) => {
            Query(TrinoQueryAction::Show(TrinoShowAction::Functions(node)))
        }
        Statement::ShowSession(node) => {
            Query(TrinoQueryAction::Show(TrinoShowAction::Session(node)))
        }
        Statement::SetPath(_) => return unsupported("SET PATH"),
        Statement::SetTimeZone(node) => Session(TrinoSessionAction::SetTimeZone(node)),
        Statement::Prepare(_) => return unsupported("PREPARE"),
        Statement::Deallocate(_) => return unsupported("DEALLOCATE PREPARE"),
        Statement::Execute(_) => return unsupported("EXECUTE"),
        Statement::ExecuteImmediate(_) => return unsupported("EXECUTE IMMEDIATE"),
        Statement::DescribeInput(_) => return unsupported("DESCRIBE INPUT"),
        Statement::DescribeOutput(_) => return unsupported("DESCRIBE OUTPUT"),
        Statement::CreateFunction(_) => return unsupported("CREATE FUNCTION"),
        Statement::DropFunction(_) => return unsupported("DROP FUNCTION"),
        Statement::SetSessionAuthorization(_) => return unsupported("SET SESSION AUTHORIZATION"),
        Statement::ResetSessionAuthorization(_) => {
            return unsupported("RESET SESSION AUTHORIZATION");
        }
        Statement::StartTransaction(_) => return unsupported("START TRANSACTION"),
    };
    Ok(action)
}

fn admit_explain_options(
    source: &str,
    options: &[ast::TrinoExplainOption],
) -> Result<(), QueryServiceError> {
    for option in options {
        let unsupported = match option.kind {
            ast::TrinoExplainOptionKind::Type(ast::TrinoExplainType::Validate) => {
                "EXPLAIN TYPE VALIDATE"
            }
            ast::TrinoExplainOptionKind::Type(ast::TrinoExplainType::Io) => "EXPLAIN TYPE IO",
            ast::TrinoExplainOptionKind::Format(ast::TrinoExplainFormat::Graphviz) => {
                "EXPLAIN FORMAT GRAPHVIZ"
            }
            ast::TrinoExplainOptionKind::Format(ast::TrinoExplainFormat::Json) => {
                "EXPLAIN FORMAT JSON"
            }
            ast::TrinoExplainOptionKind::Type(
                ast::TrinoExplainType::Logical | ast::TrinoExplainType::Distributed,
            )
            | ast::TrinoExplainOptionKind::Format(ast::TrinoExplainFormat::Text) => continue,
        };
        return Err(unsupported_statement(source, option.span, unsupported));
    }
    Ok(())
}

fn admit_trino_explain(
    source: &str,
    explain: ast::TrinoExplain,
) -> Result<TrinoStatementAction, QueryServiceError> {
    admit_explain_options(source, &explain.options)?;
    match explain.statement.into_inner() {
        ast::TrinoStatement::Query(query) => Ok(TrinoStatementAction::Query(
            TrinoQueryAction::Explain(query),
        )),
        ast::TrinoStatement::Delete(delete) => {
            if let Some(predicate) = &delete.predicate {
                super::dml_admission::admit_delete_predicate(source, predicate)
                    .map_err(QueryServiceError::from_user_error)?;
            }
            Ok(TrinoStatementAction::Prepare(
                TrinoStatementPreparation::ExplainDelete(delete),
            ))
        }
        ast::TrinoStatement::RefreshMaterializedView(refresh) => Ok(TrinoStatementAction::Prepare(
            TrinoStatementPreparation::ExplainRefreshMaterializedView(refresh, explain.span),
        )),
        statement => Err(unsupported_statement(
            source,
            statement.span(),
            "EXPLAIN for this statement",
        )),
    }
}

fn admit_trino_explain_analyze(
    source: &str,
    explain: ast::TrinoExplainAnalyze,
) -> Result<ast::Query, QueryServiceError> {
    match explain.statement.into_inner() {
        ast::TrinoStatement::Query(query) => Ok(query),
        statement => Err(unsupported_statement(
            source,
            statement.span(),
            "EXPLAIN ANALYZE for this statement",
        )),
    }
}

#[cfg(test)]
mod retired_ddl_tests {
    use super::*;
    use novarocks_parser::parser::parse_statement;

    #[test]
    fn retired_statement_spelling_has_a_syntax_error() {
        for source in [
            "SHOW PROCESSLIST",
            "SHOW FULL PROCESSLIST",
            "CREATE TABLE IF NOT EXISTS ice.db.orders (id INT NOT NULL DEFAULT 7 COMMENT 'key') PRIMARY KEY(id) PARTITION BY day(id) DISTRIBUTED BY HASH(id) BUCKETS 4 COMMENT 'orders' PROPERTIES('format-version'='3')",
            "CREATE TABLE ice.db.measures (id BIGINT SUM NOT NULL DEFAULT 7 COMMENT 'id') AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4 COMMENT 'table' PROPERTIES('format-version'='3')",
            "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city",
        ] {
            let error = parse_statement(source).unwrap_err().to_user_error(source);
            assert_eq!(
                error.code().as_str(),
                "sql.parse.unexpected_token",
                "{source}"
            );
        }
    }

    #[test]
    fn current_catalog_add_column_and_show_commands_retain_their_owners() {
        let source = "CREATE CATALOG IF NOT EXISTS lake USING iceberg COMMENT 'catalog'";
        assert!(matches!(
            admit_trino_statement(source, parse_statement(source).unwrap()).unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::CreateCatalog(_)),
        ));
        let source = "ALTER TABLE ice.db.orders ADD COLUMN address.zip INTEGER DEFAULT 94107";
        assert!(matches!(
            admit_trino_statement(source, parse_statement(source).unwrap()).unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::AddColumn(_)),
        ));
        let source = "SHOW STATS FOR ice.db.orders";
        assert!(matches!(
            admit_trino_statement(source, parse_statement(source).unwrap()).unwrap(),
            TrinoStatementAction::Product(ProductSqlCommand::Statistics(_)),
        ));
        assert!(crate::statement_kind::StatementKind::ShowStats.is_row_kind());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::parser::parse_statement;

    fn admit(source: &str) -> Result<TrinoStatementAction, QueryServiceError> {
        let statement = parse_statement(source).unwrap_or_else(|error| panic!("{source}: {error}"));
        admit_trino_statement(source, statement)
    }

    #[test]
    fn execute_immediate_resolves_the_kill_procedure_in_the_request_namespace() {
        use crate::session_value::PreparedStatementSnapshot;

        let source = "EXECUTE IMMEDIATE 'CALL kill_query(?)' USING 'target'";
        let snapshot = PreparedStatementSnapshot::default();
        let admitted = admit_trino_statement_request_in_namespace(
            source.into(),
            parse_statement(source).unwrap(),
            &snapshot,
            Some("system"),
            Some("runtime"),
        )
        .unwrap();
        assert_eq!(admitted.source.sql().as_ref(), "CALL kill_query(?)");
        assert!(admitted.parameters.is_some());
        assert!(matches!(
            admitted.action,
            TrinoStatementAction::Prepare(TrinoStatementPreparation::KillQuery(_))
        ));

        let admitted = admit_trino_statement_request_in_namespace(
            source.into(),
            parse_statement(source).unwrap(),
            &snapshot,
            Some("other"),
            Some("runtime"),
        )
        .unwrap();
        assert!(matches!(
            admitted.action,
            TrinoStatementAction::Prepare(TrinoStatementPreparation::Call(_))
        ));
    }

    #[test]
    fn persisted_definition_admission_keeps_the_frozen_caller_policy() {
        use novarocks_sql::sql_mode::SqlMode;

        let default = SqlSemanticSettings::default();
        let strict = default
            .clone()
            .with_sql_mode(SqlMode::from_assignment("ERROR_IF_OVERFLOW"));
        for source in [
            "CREATE VIEW v AS SELECT 1",
            "CREATE OR REPLACE VIEW v AS SELECT 1",
            "CREATE MATERIALIZED VIEW v AS SELECT 1",
            "REFRESH MATERIALIZED VIEW v",
            "EXPLAIN REFRESH MATERIALIZED VIEW v",
            "ALTER MATERIALIZED VIEW v SET PROPERTIES partitioning = ARRAY['x']",
        ] {
            let TrinoStatementAction::Prepare(preparation) = admit(source).unwrap() else {
                panic!("expected definition or replay preparation: {source}");
            };
            assert!(admit_persisted_definition_semantics(source, &preparation, &default).is_ok());
            for (settings, feature) in [(&strict, "ERROR_IF_OVERFLOW")] {
                let error = admit_persisted_definition_semantics(source, &preparation, settings)
                    .unwrap_err();
                let user = error.user_error().unwrap();
                assert_eq!(
                    user.code().as_str(),
                    "sql.admit.persisted_definition_semantics_unsupported"
                );
                assert!(user.message().contains(feature), "{source}: {user}");
                assert_eq!(user.location().unwrap().column(), 1);
            }
        }
        for source in [
            "CREATE VIEW v AS VALUES 1",
            "CREATE VIEW v AS SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1",
            "DROP VIEW v",
            "DROP MATERIALIZED VIEW v",
            "ALTER MATERIALIZED VIEW v SET PROPERTIES primary_key = ARRAY['x']",
        ] {
            let TrinoStatementAction::Prepare(preparation) = admit(source).unwrap() else {
                panic!("expected typed VIEW or MATERIALIZED VIEW preparation: {source}");
            };
            let settings = if source.contains("SET_VAR") {
                &default
            } else {
                &strict
            };
            assert!(admit_persisted_definition_semantics(source, &preparation, settings).is_ok());
        }
    }

    #[test]
    fn only_the_unfiltered_explicit_branch_selects_whole_branch_delete() {
        let TrinoStatementAction::Prepare(TrinoStatementPreparation::DeleteEntireBranch {
            table,
            branch,
        }) = admit(r#"DELETE FROM lake."schema.with.dot".t @"Audit-Ref""#).unwrap()
        else {
            panic!("expected whole-branch delete");
        };
        assert_eq!(table.parts, ["lake", "schema.with.dot", "t"]);
        assert_eq!(branch, "Audit-Ref");

        for sql in [
            "DELETE FROM lake.db.t",
            "DELETE FROM lake.db.t @dev WHERE id = 2",
        ] {
            assert!(matches!(
                admit(sql).unwrap(),
                TrinoStatementAction::Prepare(TrinoStatementPreparation::Delete(_))
            ));
        }

        let sql = "DELETE FROM lake.db.t @dev";
        let ast::TrinoStatement::Delete(mut node) = parse_statement(sql).unwrap() else {
            panic!("expected DELETE grammar");
        };
        // The grammar cannot produce a historical write target. Keep the
        // existing predicate DELETE guard for constructed ASTs as well.
        node.table.query_period = Some(ast::QueryPeriod {
            start: None,
            end: None,
            range_type: ast::QueryPeriodRangeType::Version,
            span: node.table.span,
        });
        assert!(matches!(
            admit_trino_statement(sql, ast::TrinoStatement::Delete(node)).unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::Delete(_))
        ));
    }

    #[test]
    fn read_and_explain_select_actual_query_inputs() {
        assert!(matches!(
            admit("SELECT 1").unwrap(),
            TrinoStatementAction::Query(TrinoQueryAction::Read(_))
        ));
        for sql in [
            "EXPLAIN SELECT 1",
            "EXPLAIN (TYPE LOGICAL) SELECT 1",
            "EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) SELECT 1",
        ] {
            assert!(matches!(
                admit(sql).unwrap(),
                TrinoStatementAction::Query(TrinoQueryAction::Explain(_))
            ));
        }
        let sql = "EXPLAIN ANALYZE VERBOSE WITH SESSION query_timeout = 1 + 1 SELECT 1";
        let TrinoStatementAction::Query(TrinoQueryAction::ExplainAnalyze(query)) =
            admit(sql).unwrap()
        else {
            panic!("EXPLAIN ANALYZE query");
        };
        assert_eq!(query.session_properties.len(), 1);
        assert_eq!(
            query.session_properties[0].name.parts[0].value,
            "query_timeout"
        );
        assert!(matches!(
            query.session_properties[0].value,
            ast::Expr::Binary(_)
        ));

        assert!(matches!(
            admit("EXPLAIN REFRESH MATERIALIZED VIEW lake.db.v").unwrap(),
            TrinoStatementAction::Prepare(
                TrinoStatementPreparation::ExplainRefreshMaterializedView(..)
            )
        ));
    }

    #[test]
    fn explain_delete_keeps_the_admitted_write_target_and_original_predicate() {
        for sql in [
            "EXPLAIN DELETE FROM lake.db.t WHERE part > 10",
            "EXPLAIN (TYPE LOGICAL, FORMAT TEXT) DELETE FROM lake.db.t WHERE part > 10",
            "EXPLAIN DELETE FROM lake.db.t",
        ] {
            let TrinoStatementAction::Prepare(TrinoStatementPreparation::ExplainDelete(delete)) =
                admit(sql).unwrap()
            else {
                panic!("Expected the read-only DELETE explanation");
            };
            assert!(
                delete
                    .table
                    .name
                    .parts
                    .iter()
                    .map(|part| part.value.as_str())
                    .eq(["lake", "db", "t"])
            );
            assert_eq!(delete.predicate.is_some(), sql.contains("WHERE"), "{sql}");
        }
        let sql = "EXPLAIN DELETE FROM lake.db.t WHERE part IN (SELECT 1)";
        let refused = admit(sql).unwrap_err();
        let execution = admit("DELETE FROM lake.db.t WHERE part IN (SELECT 1)").unwrap_err();
        assert_eq!(
            refused.user_error().unwrap().code(),
            execution.user_error().unwrap().code()
        );
    }

    #[test]
    fn explain_rejects_the_actual_option_before_the_statement_kind() {
        for option in ["TYPE VALIDATE", "TYPE IO", "FORMAT GRAPHVIZ", "FORMAT JSON"] {
            let sql = format!("EXPLAIN (FORMAT TEXT, {option}) SHOW TABLES");
            let error = admit(&sql).unwrap_err();
            let user = error.user_error().unwrap();
            assert_eq!(user.code(), UNSUPPORTED_STATEMENT.code);
            assert_eq!(user.message(), format!("EXPLAIN {option} is not supported"));
            assert_eq!(
                user.location().unwrap().column() as usize,
                sql.find(option).unwrap() + 1
            );
        }
    }

    #[test]
    fn session_actions_keep_original_names_and_value_expression() {
        let sql = r#"  USE "a.b"."C.D""#;
        let TrinoStatementAction::Session(TrinoSessionAction::Use {
            catalog: Some(catalog),
            schema,
            span,
        }) = admit(sql).unwrap()
        else {
            panic!("qualified USE")
        };
        assert!(catalog.quoted && schema.quoted);
        assert_eq!(
            (catalog.value.as_str(), schema.value.as_str()),
            ("a.b", "C.D")
        );
        assert_eq!((span.start(), span.end()), (2, sql.len()));
        assert!(matches!(
            admit("USE db").unwrap(),
            TrinoStatementAction::Session(TrinoSessionAction::Use { catalog: None, .. })
        ));
        let TrinoStatementAction::Session(TrinoSessionAction::Set { name, value }) =
            admit(r#"SET SESSION lake."p.name" = 2 + 3"#).unwrap()
        else {
            panic!("scoped SET")
        };
        assert_eq!(name.parts[1].value, "p.name");
        assert!(name.parts[1].quoted);
        assert!(matches!(value, ast::Expr::Binary(_)));

        let sql = r#"  RESET SESSION lake."p.name""#;
        let TrinoStatementAction::Session(TrinoSessionAction::Reset { name, span }) =
            admit(sql).unwrap()
        else {
            panic!("scoped RESET")
        };
        assert_eq!(name.parts[1].value, "p.name");
        assert_eq!((span.start(), span.end()), (2, sql.len()));
    }

    #[test]
    fn set_time_zone_admits_the_original_expression_or_local() {
        let sql = "  SET TIME ZONE INTERVAL '-05:30' HOUR TO MINUTE";
        let TrinoStatementAction::Session(TrinoSessionAction::SetTimeZone(node)) =
            admit(sql).unwrap()
        else {
            panic!("original SET TIME ZONE")
        };
        assert!(matches!(
            node.time_zone,
            Some(ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Interval(_),
                ..
            }))
        ));
        assert_eq!((node.span.start(), node.span.end()), (2, sql.len()));
        assert!(matches!(
            admit("SET TIME ZONE LOCAL").unwrap(),
            TrinoStatementAction::Session(TrinoSessionAction::SetTimeZone(ast::TrinoSetTimeZone {
                time_zone: None,
                ..
            }))
        ));
    }

    #[test]
    fn show_create_materialized_view_is_a_read_preparation() {
        assert!(matches!(
            admit("SHOW CREATE MATERIALIZED VIEW db.v").unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::ShowCreateMaterializedView(_))
        ));
    }

    #[test]
    fn show_tables_and_describe_admit_typed_metadata_queries() {
        assert!(matches!(
            admit("SHOW TABLES").unwrap(),
            TrinoStatementAction::Query(TrinoQueryAction::Show(TrinoShowAction::Tables(_)))
        ));
        assert!(matches!(
            admit("DESCRIBE db.t").unwrap(),
            TrinoStatementAction::Query(TrinoQueryAction::Show(TrinoShowAction::Columns(_)))
        ));
    }

    #[test]
    fn parsed_only_statements_and_transactions_return_distinct_typed_errors() {
        for (sql, subject) in [
            (
                "SET SESSION AUTHORIZATION 'reader'",
                "SET SESSION AUTHORIZATION",
            ),
            ("RESET SESSION AUTHORIZATION", "RESET SESSION AUTHORIZATION"),
            ("CREATE ROLE reader", "CREATE ROLE"),
            ("PREPARE q FROM SELECT 1", "PREPARE"),
            ("EXECUTE q", "EXECUTE"),
            ("SET PATH a.b", "SET PATH"),
            ("START TRANSACTION READ ONLY", "START TRANSACTION"),
            ("SHOW STATS FOR (SELECT 1)", "SHOW STATS FOR a query"),
        ] {
            let error = admit(sql).unwrap_err();
            let user = error.user_error().unwrap();
            assert_eq!(user.code(), UNSUPPORTED_STATEMENT.code, "{sql}");
            assert_eq!(
                user.message(),
                format!("{subject} is not supported"),
                "{sql}"
            );
            assert_eq!(user.location().unwrap().column(), 1);
        }
        for sql in ["COMMIT", "COMMIT WORK", "ROLLBACK", "ROLLBACK WORK"] {
            let error = admit(sql).unwrap_err();
            let user = error.user_error().unwrap();
            assert_eq!(user.code(), NOT_IN_TRANSACTION.code);
            assert!(user.location().is_none());
        }
    }

    #[test]
    fn existing_products_keep_name_parts_and_complex_leaves_require_preparation() {
        let TrinoStatementAction::Product(ProductSqlCommand::Statistics(
            StatisticsSqlCommand::ShowStats { table },
        )) = admit(r#"SHOW STATS FOR lake."s.e"."t.n""#).unwrap()
        else {
            panic!("named statistics table")
        };
        assert_eq!(table.parts, ["lake", "s.e", "t.n"]);

        let TrinoStatementAction::Product(ProductSqlCommand::Catalog(command)) =
            admit("DROP TABLE IF EXISTS lake.db.t").unwrap()
        else {
            panic!("drop table");
        };
        assert!(parse_statement("DROP TABLE IF EXISTS lake.db.t FORCE").is_err());
        let CatalogSqlCommand::DropTable { name, if_exists } = *command else {
            panic!("exact drop table command");
        };
        assert_eq!(name.parts, ["lake", "db", "t"]);
        assert!(if_exists);
        assert!(matches!(
            admit("CREATE TABLE t (id BIGINT)").unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::CreateTable(_))
        ));
        assert!(matches!(
            admit("DROP SCHEMA db CASCADE").unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::DropSchemaCascade(_))
        ));
        assert!(matches!(
            admit("INSERT INTO t @branch1 VALUES 1").unwrap(),
            TrinoStatementAction::Prepare(TrinoStatementPreparation::Insert(_))
        ));
    }

    #[test]
    fn unsupported_mv_profiles_are_refused_without_a_catalog_binding() {
        for (sql, subject) in [
            (
                "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT 1",
                "CREATE OR REPLACE MATERIALIZED VIEW",
            ),
            (
                "CREATE MATERIALIZED VIEW mv GRACE PERIOD INTERVAL '1' DAY AS SELECT 1",
                "CREATE MATERIALIZED VIEW GRACE PERIOD",
            ),
            (
                "CREATE MATERIALIZED VIEW mv WHEN STALE FAIL AS SELECT 1",
                "CREATE MATERIALIZED VIEW WHEN STALE",
            ),
            (
                "CREATE MATERIALIZED VIEW mv COMMENT 'description' AS SELECT 1",
                "CREATE MATERIALIZED VIEW COMMENT",
            ),
            (
                "ALTER MATERIALIZED VIEW mv EXECUTE optimize(where_only => TRUE) WHERE id > 0",
                "ALTER MATERIALIZED VIEW EXECUTE",
            ),
        ] {
            let error = admit(sql).unwrap_err();
            let user = error.user_error().unwrap();
            assert_eq!(user.code(), UNSUPPORTED_STATEMENT.code);
            assert_eq!(user.message(), format!("{subject} is not supported"));
        }
        assert!(matches!(
            admit("ALTER MATERIALIZED VIEW mv SET PROPERTIES primary_key = ARRAY['v']").unwrap(),
            TrinoStatementAction::Prepare(
                TrinoStatementPreparation::SetMaterializedViewProperties { .. }
            )
        ));
    }

    #[test]
    fn unsupported_create_options_are_rejected_from_their_real_nodes() {
        use crate::sql::{
            dml_admission::DmlAdmissionError,
            property_evaluation::table::validate_trino_create_table,
        };
        use novarocks_sql::compiler::SqlCompileControl;

        let error = admit("CREATE OR REPLACE TABLE t (id BIGINT)").unwrap_err();
        let user = error.user_error().unwrap();
        assert_eq!(user.code(), UNSUPPORTED_STATEMENT.code);
        assert_eq!(user.message(), "CREATE OR REPLACE TABLE is not supported");

        let control = SqlCompileControl::unbounded();
        for (sql, message) in [
            (
                "CREATE TABLE t (LIKE s INCLUDING PROPERTIES)",
                "CREATE TABLE LIKE INCLUDING PROPERTIES is not supported",
            ),
            (
                "CREATE TABLE t (id BIGINT, LIKE s)",
                "CREATE TABLE LIKE must be the only table element",
            ),
        ] {
            let TrinoStatementAction::Prepare(TrinoStatementPreparation::CreateTable(table)) =
                admit(sql).unwrap()
            else {
                panic!("CREATE TABLE must reach target preparation: {sql}");
            };
            let ast::TrinoTableElement::Like(like) = table.elements.last().unwrap() else {
                panic!("expected the original LIKE element: {sql}");
            };
            let error = validate_trino_create_table(sql, &table, &control).unwrap_err();
            assert_eq!(
                error.user_error(),
                Some(
                    &DmlAdmissionError::CreateTableUnsupportedForm
                        .to_user_error(sql, like.span, message)
                ),
                "{sql}"
            );
        }
    }
}
