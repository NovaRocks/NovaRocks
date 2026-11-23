// SPDX-License-Identifier: Apache-2.0

//! SHOW binding and statement-owned candidate rows.
use super::*;
use crate::query::compiler::FrontendQueryCompilerError;
use crate::system_tables::SystemValueRef;
use arrow::datatypes::DataType;
use novarocks_parser::{Span, ast};
use novarocks_query_application::{
    sql::{
        catalog::SessionCatalogService,
        session::registry::{
            CatalogPropertyMetadata, SessionCatalogPropertyAdmission, SessionPropertyInputs,
            SessionPropertyRegistry, SessionPropertyValue,
        },
        trino_admission::{ShowAdmitError, TrinoShowAction},
    },
    system_tables::SystemTableLookup,
};
use novarocks_spi::connector::{ConnectorOperationControl, DiscoveryColumnProjection};
use novarocks_sql::system_table::{
    StatementSystemRelation, SystemTableBinding, SystemTableColumn, SystemTableDeclaration,
};
use novarocks_type_contract::{CharacterKind, CharacterType, FunctionKind, FunctionValueType};
use novarocks_types::naming::{INFORMATION_SCHEMA, SYSTEM_CATALOG};
use novarocks_workload_control::{ResultPayloadBytes, WorkError};

pub(crate) struct ShowInput {
    pub(crate) action: TrinoShowAction,
    pub(crate) source: Arc<str>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) properties: Arc<SessionPropertyRegistry>,
    pub(crate) catalogs: SessionCatalogService,
}

pub(crate) struct PreparedShowQuery {
    pub(crate) query: Box<ast::Query>,
    pub(crate) private_relation: Option<(StatementSystemRelation, Arc<SystemTableDeclaration>)>,
}

#[derive(Default)]
pub(super) struct ShowFacts {
    pub(super) schema: Option<(String, String)>,
    pub(super) relation: Option<DiscoveredRelation>,
    rows: Option<ShowRows>,
}

const CATALOG_COLUMNS: &[(&str, &str)] = &[("catalog_name", "Catalog")];
const FUNCTION_COLUMNS: &[(&str, &str)] = &[
    ("function_name", "Function"),
    ("return_type", "Return Type"),
    ("argument_types", "Argument Types"),
    ("function_type", "Function Type"),
    ("deterministic", "Deterministic"),
    ("description", "Description"),
];
const SESSION_COLUMNS: &[(&str, &str)] = &[
    ("property_name", "Name"),
    ("property_value", "Value"),
    ("default_value", "Default"),
    ("property_type", "Type"),
    ("description", "Description"),
];

#[derive(Default)]
enum ShowCell {
    #[default]
    Empty,
    Text(String),
    Boolean(bool),
    Int64(i64),
}

pub(super) struct ShowRows {
    kind: StatementSystemRelation,
    declaration: Arc<SystemTableDeclaration>,
    rows: Vec<[ShowCell; 6]>,
    _bytes: ResultPayloadBytes,
}

fn show_provider_error(error: SystemTableError) -> FrontendQueryCompilerError {
    FrontendQueryCompilerError::from_compile(sql_error(error))
}

fn show_error(
    error: ShowAdmitError,
    show: &ShowInput,
    span: Span,
    message: impl Into<String>,
) -> FrontendQueryCompilerError {
    FrontendQueryCompilerError::Application(error.into_service(&show.source, span, message))
}

fn catalog_in_session(show: &ShowInput, span: Span) -> Result<&str, FrontendQueryCompilerError> {
    show.catalog.as_deref().ok_or_else(|| {
        show_error(
            ShowAdmitError::MissingCatalog,
            show,
            span,
            "Catalog must be specified when session catalog is not set",
        )
    })
}

fn schema_in_session(show: &ShowInput, span: Span) -> Result<&str, FrontendQueryCompilerError> {
    show.schema.as_deref().ok_or_else(|| {
        show_error(
            ShowAdmitError::MissingSchema,
            show,
            span,
            "Schema must be specified when session schema is not set",
        )
    })
}

fn schema_scope(
    show: &ShowInput,
    name: Option<&ast::ObjectName>,
    span: Span,
) -> Result<(String, String), FrontendQueryCompilerError> {
    let parts = name.map_or(&[][..], |name| &name.parts);
    let (catalog, schema) = match parts {
        [] => (
            catalog_in_session(show, span)?,
            schema_in_session(show, span)?,
        ),
        [schema] => (catalog_in_session(show, span)?, schema.value.as_str()),
        [catalog, schema] => (catalog.value.as_str(), schema.value.as_str()),
        _ => {
            return Err(show_error(
                ShowAdmitError::NameTooLong,
                show,
                name.expect("qualified input").span,
                "Schema name has too many parts",
            ));
        }
    };
    Ok((catalog.to_ascii_lowercase(), schema.to_ascii_lowercase()))
}

fn table_scope(
    show: &ShowInput,
    name: &ast::ObjectName,
    span: Span,
) -> Result<(String, String, String), FrontendQueryCompilerError> {
    let (catalog, schema, table) = match name.parts.as_slice() {
        [table] => {
            let schema = schema_in_session(show, span)?;
            (
                catalog_in_session(show, span)?,
                schema,
                table.value.as_str(),
            )
        }
        [schema, table] => (
            catalog_in_session(show, span)?,
            schema.value.as_str(),
            table.value.as_str(),
        ),
        [catalog, schema, table] => (
            catalog.value.as_str(),
            schema.value.as_str(),
            table.value.as_str(),
        ),
        _ => {
            return Err(show_error(
                ShowAdmitError::NameTooLong,
                show,
                name.span,
                "Table name has too many parts",
            ));
        }
    };
    Ok((
        catalog.to_ascii_lowercase(),
        schema.to_ascii_lowercase(),
        table.to_ascii_lowercase(),
    ))
}

impl FrontendSystemTableFacts {
    pub(in crate::query_execution::completion_facts) async fn prepare_show_query(
        &self,
        show: ShowInput,
        functions: Arc<novarocks_functions::PureEngineFunctionCatalog>,
        inputs: &SessionPropertyInputs,
    ) -> Result<PreparedShowQuery, FrontendQueryCompilerError> {
        let names = self.catalog_names().map_err(show_provider_error)?;
        let context = &self.scope.connector_context;
        let mut facts = ShowFacts::default();
        let (query, private_relation) = match &show.action {
            TrinoShowAction::Schemas(node) => {
                let catalog = match &node.catalog {
                    Some(ident) => ident.value.to_ascii_lowercase(),
                    None => catalog_in_session(&show, node.span)?.to_ascii_lowercase(),
                };
                self.require_show_catalog(&names, &show, node.span, &catalog)?;
                if catalog != SYSTEM_CATALOG
                    && !self
                        .supports_discovery(&catalog)
                        .map_err(show_provider_error)?
                {
                    // The addressed catalog authors its own capability refusal.
                    self.schema_names(&catalog, context)
                        .await
                        .map_err(show_provider_error)?;
                }
                (
                    QueryShape::new(node.span)
                        .select(&[("schema_name", "Schema")])
                        .like(
                            "schema_name",
                            node.like_pattern.as_deref(),
                            node.escape.as_deref(),
                        )
                        .order(&["schema_name"])
                        .finish(&[&catalog, INFORMATION_SCHEMA, "schemata"]),
                    None,
                )
            }
            TrinoShowAction::Tables(node) => {
                let (catalog, schema) = schema_scope(&show, node.schema.as_ref(), node.span)?;
                self.require_show_catalog(&names, &show, node.span, &catalog)?;
                self.require_show_schema(&show, node.span, &catalog, &schema)
                    .await?;
                facts.schema = Some((catalog.clone(), schema.clone()));
                (
                    QueryShape::new(node.span)
                        .select(&[("table_name", "Table")])
                        .equal("table_schema", &schema)
                        .like(
                            "table_name",
                            node.like_pattern.as_deref(),
                            node.escape.as_deref(),
                        )
                        .order(&["table_name"])
                        .finish(&[&catalog, INFORMATION_SCHEMA, "tables"]),
                    None,
                )
            }
            TrinoShowAction::Columns(node) => {
                let (catalog, schema, table) = table_scope(&show, &node.table, node.span)?;
                self.require_show_catalog(&names, &show, node.span, &catalog)?;
                self.require_show_schema(&show, node.span, &catalog, &schema)
                    .await?;
                let registry = &self.owners.system_relations;
                if catalog == SYSTEM_CATALOG || schema == INFORMATION_SCHEMA {
                    let binding = show_binding(&catalog);
                    if !matches!(
                        registry.declare(&binding, &schema, &table),
                        SystemTableLookup::Declared(_)
                    ) {
                        return Err(missing_table(&show, node.span, &catalog, &schema, &table));
                    }
                } else {
                    facts.relation = Some(
                        self.discover_relation(
                            &catalog,
                            &schema,
                            &table,
                            None,
                            DiscoveryProjection {
                                columns: Some(DiscoveryColumnProjection {
                                    data_type: true,
                                    comment: true,
                                    default_expression: false,
                                }),
                                view_definition: false,
                            },
                        )
                        .await
                        .map_err(show_provider_error)?
                        .ok_or_else(|| {
                            missing_table(&show, node.span, &catalog, &schema, &table)
                        })?,
                    );
                }
                facts.schema = Some((catalog.clone(), schema.clone()));
                (
                    QueryShape::new(node.span)
                        .select(&[("column_name", "Column"), ("data_type", "Type")])
                        .select_non_null("extra_info", "Extra")
                        .select_non_null("comment", "Comment")
                        .equal("table_schema", &schema)
                        .equal("table_name", &table)
                        .like(
                            "column_name",
                            node.like_pattern.as_deref(),
                            node.escape.as_deref(),
                        )
                        .order(&["ordinal_position"])
                        .finish(&[&catalog, INFORMATION_SCHEMA, "columns"]),
                    None,
                )
            }
            TrinoShowAction::Catalogs(node) => {
                let kind = StatementSystemRelation::ShowCatalogs;
                let mut builder = ShowRowsBuilder::new(self, kind, names.len())?;
                for name in &names {
                    self.check_show_progress()?;
                    builder.text_row(&[name])?;
                }
                let rows = builder.finish(CATALOG_COLUMNS, false)?;
                let declaration = Arc::clone(&rows.declaration);
                facts.rows = Some(rows);
                (
                    QueryShape::new(node.span)
                        .select(CATALOG_COLUMNS)
                        .like(
                            "catalog_name",
                            node.like_pattern.as_deref(),
                            node.escape.as_deref(),
                        )
                        .order(&["catalog_name"])
                        .finish(&["show_catalogs"]),
                    Some((kind, declaration)),
                )
            }
            TrinoShowAction::Functions(node) => {
                let kind = StatementSystemRelation::ShowFunctions;
                let global = node.schema.is_none();
                let builtin_scope = match &node.schema {
                    None => false,
                    Some(name) => {
                        let (catalog, schema) = schema_scope(&show, Some(name), node.span)?;
                        catalog == SYSTEM_CATALOG && schema == "builtin"
                    }
                };
                // Trino lists global scalars/aggregates/windows separately
                // from schema-scoped table functions.
                let overloads = || {
                    functions.public_function_overloads().filter(|entry| {
                        global || (builtin_scope && entry.kind == FunctionKind::Table)
                    })
                };
                let count = overloads().count();
                let mut builder = ShowRowsBuilder::new(self, kind, count)?;
                for entry in overloads() {
                    self.check_show_progress()?;
                    let label = match entry.kind {
                        FunctionKind::Scalar => "scalar",
                        FunctionKind::Aggregate => "aggregate",
                        FunctionKind::Window => "window",
                        FunctionKind::Table => "table",
                    };
                    builder.function_row(
                        [
                            entry.name,
                            entry.return_type,
                            entry.argument_types,
                            label,
                            entry.description,
                        ],
                        entry.deterministic,
                    )?;
                }
                let rows = builder.finish(FUNCTION_COLUMNS, count == 0)?;
                let declaration = Arc::clone(&rows.declaration);
                facts.rows = Some(rows);
                (
                    QueryShape::new(node.span)
                        .select(FUNCTION_COLUMNS)
                        .like(
                            "function_name",
                            if count == 0 {
                                None
                            } else {
                                node.like_pattern.as_deref()
                            },
                            node.escape.as_deref(),
                        )
                        .order(&[
                            "function_name",
                            "return_type",
                            "argument_types",
                            "function_type",
                        ])
                        .finish(&["show_functions"]),
                    Some((kind, declaration)),
                )
            }
            TrinoShowAction::Session(node) => {
                let kind = StatementSystemRelation::ShowSession;
                let mut catalogs: Vec<Arc<CatalogPropertyMetadata>> = Vec::new();
                for name in &names {
                    self.check_show_progress()?;
                    match show
                        .catalogs
                        .session_property_catalog(name)
                        .map_err(FrontendQueryCompilerError::Application)?
                    {
                        SessionCatalogPropertyAdmission::Ready(metadata) => catalogs.push(metadata),
                        SessionCatalogPropertyAdmission::Absent => {}
                        SessionCatalogPropertyAdmission::Unavailable(error) => {
                            return Err(FrontendQueryCompilerError::Application(error));
                        }
                    }
                }
                let count = show
                    .properties
                    .visible_properties(inputs, &catalogs)
                    .count();
                let mut builder = ShowRowsBuilder::new(self, kind, count)?;
                for property in show.properties.visible_properties(inputs, &catalogs) {
                    self.check_show_progress()?;
                    builder.session_row(
                        property.catalog,
                        property.name,
                        property.override_value,
                        property.default_value,
                        property.value_type.sql_name(),
                        property.description,
                    )?;
                }
                let rows = builder.finish(SESSION_COLUMNS, false)?;
                let declaration = Arc::clone(&rows.declaration);
                facts.rows = Some(rows);
                (
                    QueryShape::new(node.span)
                        .select(SESSION_COLUMNS)
                        .like(
                            "property_name",
                            node.like_pattern.as_deref(),
                            node.escape.as_deref(),
                        )
                        .order(&["display_order"])
                        .finish(&["show_session"]),
                    Some((kind, declaration)),
                )
            }
        };
        let mut output = self
            .show
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if output.is_some() {
            return Err(FrontendQueryCompilerError::Engine(
                "SHOW facts already installed for this statement".into(),
            ));
        }
        *output = Some(facts);
        Ok(PreparedShowQuery {
            query,
            private_relation,
        })
    }

    fn check_show_progress(&self) -> Result<(), FrontendQueryCompilerError> {
        self.scope
            .connector_context
            .original_operation_control()
            .check_active()
            .map_err(|error| show_provider_error(error.into()))
    }

    fn require_show_catalog(
        &self,
        names: &[String],
        show: &ShowInput,
        span: Span,
        catalog: &str,
    ) -> Result<(), FrontendQueryCompilerError> {
        if !names.iter().any(|name| name == catalog) {
            return Err(show_error(
                ShowAdmitError::CatalogNotFound,
                show,
                span,
                format!("Catalog '{catalog}' not found"),
            ));
        }
        Ok(())
    }

    async fn require_show_schema(
        &self,
        show: &ShowInput,
        span: Span,
        catalog: &str,
        schema: &str,
    ) -> Result<(), FrontendQueryCompilerError> {
        let exists = if schema == INFORMATION_SCHEMA {
            true
        } else if catalog == SYSTEM_CATALOG {
            self.owners
                .system_relations
                .schemas(&show_binding(catalog))
                .iter()
                .any(|name| name == schema)
        } else {
            self.schema_exists(catalog, schema, &self.scope.connector_context)
                .await
                .map_err(show_provider_error)?
        };
        if !exists {
            return Err(show_error(
                ShowAdmitError::SchemaNotFound,
                show,
                span,
                format!("Schema '{schema}' does not exist"),
            ));
        }
        Ok(())
    }

    pub(super) async fn snapshot_show(
        &self,
        kind: StatementSystemRelation,
        request: &SystemTableRequest<'_>,
    ) -> Result<SystemTableSnapshot, SystemTableError> {
        let rows = self
            .show
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_mut()
            .and_then(|facts| facts.rows.take())
            .ok_or_else(|| {
                SystemTableError::InvalidRequest("SHOW has no original candidates".into())
            })?;
        if rows.kind != kind || rows.declaration != request.declaration {
            return Err(SystemTableError::Internal(
                "SHOW candidate declaration differs".into(),
            ));
        }
        self.begin_snapshot(Arc::clone(&request.declaration), &request.required_columns)?;
        let columns = request.declaration.columns.len();
        let mut batch_rows = 0;
        if !request.key_constraint.is_none() {
            for row in &rows.rows {
                request
                    .context
                    .original_operation_control()
                    .check_active()?;
                let values = row.each_ref().map(|value| match value {
                    ShowCell::Empty => SystemValueRef::Utf8(""),
                    ShowCell::Text(value) => SystemValueRef::Utf8(value),
                    ShowCell::Boolean(value) => SystemValueRef::Boolean(*value),
                    ShowCell::Int64(value) => SystemValueRef::Int64(*value),
                });
                if self.stage_row(&values[..columns])? == RowStage::FlushRequired {
                    self.materialize_batch().await?;
                    if self.stage_row(&values[..columns])? != RowStage::Staged {
                        return Err(SystemTableError::Internal(
                            "SHOW row does not fit its empty snapshot batch".into(),
                        ));
                    }
                    batch_rows = 0;
                }
                batch_rows += 1;
                if batch_rows == request.batch_target_rows {
                    self.materialize_batch().await?;
                    batch_rows = 0;
                }
            }
        }
        drop(rows);
        if batch_rows != 0 {
            self.materialize_batch().await?;
        }
        self.finish_snapshot().await
    }
}

fn missing_table(
    show: &ShowInput,
    span: Span,
    catalog: &str,
    schema: &str,
    table: &str,
) -> FrontendQueryCompilerError {
    show_error(
        ShowAdmitError::TableNotFound,
        show,
        span,
        format!("Table '{catalog}.{schema}.{table}' does not exist"),
    )
}

fn show_binding(catalog: &str) -> SystemTableBinding {
    if catalog == SYSTEM_CATALOG {
        SystemTableBinding::System
    } else {
        SystemTableBinding::Catalog(catalog.to_owned())
    }
}

struct ShowRowsBuilder {
    kind: StatementSystemRelation,
    rows: Vec<[ShowCell; 6]>,
    widths: [usize; 6],
    bytes: ResultPayloadBytes,
}

fn overflow() -> FrontendQueryCompilerError {
    show_provider_error(work_error(WorkError::ArithmeticOverflow))
}

impl ShowRowsBuilder {
    fn new(
        source: &FrontendSystemTableFacts,
        kind: StatementSystemRelation,
        count: usize,
    ) -> Result<Self, FrontendQueryCompilerError> {
        let layout = std::alloc::Layout::array::<[ShowCell; 6]>(count).map_err(|_| overflow())?;
        let bytes = source
            .scope
            .capacity
            .try_window_alias()
            .and_then(|window| {
                window.reserve_payload_bytes(
                    u64::try_from(layout.size()).map_err(|_| WorkError::ArithmeticOverflow)?,
                )
            })
            .map_err(|error| show_provider_error(work_error(error)))?;
        Ok(Self {
            kind,
            rows: Vec::with_capacity(count),
            widths: [0; 6],
            bytes,
        })
    }

    fn append(&mut self, row: [ShowCell; 6]) -> Result<(), FrontendQueryCompilerError> {
        if self.rows.len() == self.rows.capacity() {
            return Err(FrontendQueryCompilerError::Engine(
                "SHOW candidate cardinality changed after capture".into(),
            ));
        }
        self.rows.push(row);
        Ok(())
    }

    fn copy(
        &mut self,
        ordinal: usize,
        value: &str,
    ) -> Result<ShowCell, FrontendQueryCompilerError> {
        self.widths[ordinal] = self.widths[ordinal].max(value.chars().count());
        self.reserve_text(value.len())?;
        Ok(ShowCell::Text(value.to_owned()))
    }

    fn reserve_text(&mut self, bytes: usize) -> Result<(), FrontendQueryCompilerError> {
        self.bytes
            .try_grow(u64::try_from(bytes).map_err(|_| overflow())?)
            .map_err(|error| show_provider_error(work_error(error)))
    }

    fn text_row(&mut self, values: &[&str]) -> Result<(), FrontendQueryCompilerError> {
        let mut row = std::array::from_fn(|_| ShowCell::Empty);
        for (ordinal, value) in values.iter().enumerate() {
            row[ordinal] = self.copy(ordinal, value)?;
        }
        self.append(row)
    }

    fn function_row(
        &mut self,
        values: [&str; 5],
        deterministic: bool,
    ) -> Result<(), FrontendQueryCompilerError> {
        let [name, result, arguments, kind, description] = values;
        let row = [
            self.copy(0, name)?,
            self.copy(1, result)?,
            self.copy(2, arguments)?,
            self.copy(3, kind)?,
            ShowCell::Boolean(deterministic),
            self.copy(5, description)?,
        ];
        self.append(row)
    }

    fn session_row(
        &mut self,
        catalog: Option<&str>,
        name: &str,
        override_value: Option<&str>,
        default_value: Option<&SessionPropertyValue>,
        input_type: &str,
        description: &str,
    ) -> Result<(), FrontendQueryCompilerError> {
        let name = if let Some(catalog) = catalog {
            let len = catalog
                .len()
                .checked_add(name.len())
                .and_then(|len| len.checked_add(1))
                .ok_or_else(overflow)?;
            self.reserve_text(len)?;
            let mut qualified = String::with_capacity(len);
            qualified.push_str(catalog);
            qualified.push('.');
            qualified.push_str(name);
            self.widths[0] = self.widths[0].max(qualified.chars().count());
            ShowCell::Text(qualified)
        } else {
            self.copy(0, name)?
        };
        let default = match default_value {
            Some(SessionPropertyValue::Varchar(text)) => std::borrow::Cow::Borrowed(text.as_str()),
            Some(value) => std::borrow::Cow::Owned(
                value
                    .render()
                    .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?,
            ),
            None => std::borrow::Cow::Borrowed(""),
        };
        let row = [
            name,
            self.copy(1, override_value.unwrap_or(default.as_ref()))?,
            self.copy(2, default.as_ref())?,
            self.copy(3, input_type)?,
            self.copy(4, description)?,
            ShowCell::Int64(i64::try_from(self.rows.len()).map_err(|_| overflow())?),
        ];
        self.append(row)
    }

    fn finish(
        self,
        columns: &[(&str, &str)],
        unbounded: bool,
    ) -> Result<ShowRows, FrontendQueryCompilerError> {
        let mut declaration = Vec::with_capacity(
            columns.len() + usize::from(self.kind == StatementSystemRelation::ShowSession),
        );
        for (ordinal, (name, _)) in columns.iter().enumerate() {
            let value_type = if self.kind == StatementSystemRelation::ShowFunctions && ordinal == 4
            {
                FunctionValueType::new(DataType::Boolean, false)
            } else if unbounded {
                FunctionValueType::new(DataType::Utf8, false)
            } else {
                bounded_text(self.widths[ordinal])?
            };
            declaration.push(SystemTableColumn {
                name: (*name).into(),
                value_type,
                hidden: false,
            });
        }
        if self.kind == StatementSystemRelation::ShowSession {
            declaration.push(SystemTableColumn {
                name: "display_order".into(),
                value_type: FunctionValueType::new(DataType::Int64, false),
                hidden: true,
            });
        }
        let table = match self.kind {
            StatementSystemRelation::ShowCatalogs => "show_catalogs",
            StatementSystemRelation::ShowFunctions => "show_functions",
            StatementSystemRelation::ShowSession => "show_session",
        };
        Ok(ShowRows {
            kind: self.kind,
            declaration: Arc::new(SystemTableDeclaration {
                binding: SystemTableBinding::System,
                schema: "metadata".into(),
                table: table.into(),
                columns: declaration.into(),
                key_columns: Arc::from([]),
            }),
            rows: self.rows,
            _bytes: self.bytes,
        })
    }
}

fn bounded_text(width: usize) -> Result<FunctionValueType, FrontendQueryCompilerError> {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let character = u32::try_from(width)
        .ok()
        .and_then(|width| CharacterType::new(CharacterKind::Varchar, width))
        .ok_or_else(overflow)?;
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(
        ValueKind::BoundedCharacter(character),
        false,
        storage.clone(),
    )
    .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
    let layout = ValueLayout::try_new(&DataType::Utf8, storage)
        .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
    FunctionValueType::try_from_value(semantic, layout)
        .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))
}

struct QueryShape {
    span: Span,
    projections: Vec<ast::SelectItem>,
    predicates: Vec<ast::Expr>,
    order: Vec<ast::SortItem>,
}

impl QueryShape {
    fn new(span: Span) -> Self {
        Self {
            span,
            projections: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
        }
    }

    fn select(mut self, columns: &[(&str, &str)]) -> Self {
        for (name, alias) in columns {
            self.projections.push(self.output(self.column(name), alias));
        }
        self
    }

    fn select_non_null(mut self, name: &str, alias: &str) -> Self {
        let expression = ast::Expr::Coalesce(ast::AstBox::new(ast::CoalesceExpr {
            operands: vec![self.column(name), self.text("")].into(),
            span: self.span,
        }));
        self.projections.push(self.output(expression, alias));
        self
    }

    fn equal(mut self, column: &str, value: &str) -> Self {
        self.predicates.push(ast::Expr::Predicated(ast::AstBox::new(
            ast::PredicatedExpr {
                value: self.column(column),
                predicate: ast::Predicate::Comparison(ast::ComparisonPredicate {
                    operator: ast::ComparisonOperator::Equal,
                    right: self.text(value),
                    span: self.span,
                }),
                span: self.span,
            },
        )));
        self
    }

    fn like(mut self, column: &str, pattern: Option<&str>, escape: Option<&str>) -> Self {
        if let Some(pattern) = pattern {
            self.predicates.push(ast::Expr::Predicated(ast::AstBox::new(
                ast::PredicatedExpr {
                    value: self.column(column),
                    predicate: ast::Predicate::Like(ast::LikePredicate {
                        negated: false,
                        pattern: self.text(pattern),
                        escape: escape.map(|escape| self.text(escape)),
                        span: self.span,
                    }),
                    span: self.span,
                },
            )));
        }
        self
    }

    fn order(mut self, columns: &[&str]) -> Self {
        self.order = columns
            .iter()
            .map(|column| ast::SortItem {
                sort_key: self.column(column),
                ordering: ast::SortOrdering::Ascending,
                null_ordering: ast::NullOrdering::Undefined,
                span: self.span,
            })
            .collect();
        self
    }

    fn ident(&self, name: &str) -> ast::Ident {
        ast::Ident {
            value: name.to_owned(),
            quoted: true,
            span: self.span,
        }
    }

    fn column(&self, name: &str) -> ast::Expr {
        ast::Expr::Identifier(self.ident(name))
    }

    fn text(&self, value: &str) -> ast::Expr {
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(value.to_owned()),
            span: self.span,
        })
    }

    fn output(&self, expression: ast::Expr, alias: &str) -> ast::SelectItem {
        ast::SelectItem::SingleColumn(ast::AstBox::new(ast::SingleColumn {
            expression,
            alias: Some(self.ident(alias)),
            span: self.span,
        }))
    }

    fn finish(self, relation: &[&str]) -> Box<ast::Query> {
        let name = ast::ObjectName {
            parts: relation.iter().map(|part| self.ident(part)).collect(),
            span: self.span,
        };
        let selection = match self.predicates.len() {
            0 => None,
            1 => self.predicates.into_iter().next(),
            _ => Some(ast::Expr::Logical(ast::LogicalExpr {
                operator: ast::LogicalOperator::And,
                terms: self.predicates.into(),
                span: self.span,
            })),
        };
        Box::new(ast::Query {
            session_properties: Default::default(),
            functions: Default::default(),
            with: None,
            body: ast::AstBox::new(ast::SetExpr::Select(ast::AstBox::new(ast::Select {
                distinct: false,
                projection: self.projections.into(),
                from: Some(ast::Relation::Table(ast::AstBox::new(ast::Table {
                    name,
                    query_period: None,
                    branch: None,
                    span: self.span,
                }))),
                selection,
                group_by: None,
                having: None,
                windows: Default::default(),
                modifiers: ast::QueryModifiers {
                    order_by: Some(ast::OrderBy {
                        items: self.order.into(),
                        span: self.span,
                    }),
                    ..Default::default()
                },
                span: self.span,
            }))),
            modifiers: ast::QueryModifiers::default(),
            span: self.span,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::ValueKind;

    #[test]
    fn show_metadata_widths_count_unicode_and_keep_declaration_for_filtered_rows() {
        let (_control, _root, capacity, _auth) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = capacity.window_alias();
        let before = window.retained_payload_bytes();
        let mut builder = ShowRowsBuilder {
            kind: StatementSystemRelation::ShowCatalogs,
            rows: Vec::with_capacity(2),
            widths: [0; 6],
            bytes: window.reserve_payload_bytes(0).unwrap(),
        };
        builder.text_row(&["👩‍💻é"]).unwrap();
        builder.text_row(&["a"]).unwrap();
        let mut rows = builder.finish(CATALOG_COLUMNS, false).unwrap();
        let value_type = &rows.declaration.columns[0].value_type;
        let ValueKind::BoundedCharacter(character) = value_type.value_type().kind() else {
            panic!("catalog name must be a bounded varchar");
        };
        assert_eq!(character.length(), 4);
        // Whether the subsequent LIKE retains all, some or zero rows does not
        // change the declaration authored from the visible candidate set.
        rows.rows.clear();
        assert_eq!(character.length(), 4);
        assert!(window.retained_payload_bytes() > before);
        drop(rows);
        assert_eq!(window.retained_payload_bytes(), before);

        let ValueKind::BoundedCharacter(empty) =
            bounded_text(0).unwrap().value_type().kind().clone()
        else {
            panic!("an empty non-function candidate set has a bounded varchar");
        };
        assert_eq!(empty.length(), 0);
    }
}
