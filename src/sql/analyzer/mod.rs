use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::ast::{
    ColumnRef, CompareOp, Expr, Literal, ObjectName, OrderByExpr, ProjectionItem, QueryStmt,
    Statement,
};
use crate::standalone::catalog::{ColumnDef, InMemoryCatalog, TableDef, normalize_identifier};
use crate::standalone::iceberg::{
    IcebergCatalogRegistry, IcebergLoadedTable, load_table as load_iceberg_table,
};

#[derive(Clone, Debug)]
pub(crate) struct AnalyzerContext<'a> {
    pub(crate) current_catalog: Option<&'a str>,
    pub(crate) current_database: &'a str,
}

#[derive(Clone, Debug)]
pub(crate) enum AnalyzedStatement {
    Query(AnalyzedQuery),
    Passthrough(Statement),
}

#[derive(Clone, Debug)]
pub(crate) struct AnalyzedQuery {
    pub(crate) source: QuerySource,
    pub(crate) bound: BoundQuery,
    pub(crate) order_by: Vec<OrderByExpr>,
}

#[derive(Clone, Debug)]
pub(crate) enum QuerySource {
    Local {
        resolved: ResolvedLocalTableName,
        table: TableDef,
    },
    Iceberg {
        #[allow(dead_code)]
        resolved: ResolvedIcebergTableName,
        loaded: IcebergLoadedTable,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedLocalTableName {
    pub(crate) database: String,
    pub(crate) table: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedIcebergTableName {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundScanColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
    pub(crate) scan_slot_id: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundOutputColumn {
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) nullable: bool,
    pub(crate) scan_column_index: usize,
    pub(crate) output_slot_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundPredicate {
    pub(crate) scan_column_index: usize,
    pub(crate) op: CompareOp,
    pub(crate) literal: Literal,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundQuery {
    pub(crate) scan_columns: Vec<BoundScanColumn>,
    pub(crate) output_columns: Vec<BoundOutputColumn>,
    pub(crate) predicate: Option<BoundPredicate>,
    pub(crate) needs_project: bool,
}

pub(crate) fn analyze_statement(
    stmt: Statement,
    ctx: AnalyzerContext<'_>,
    local_catalog: &InMemoryCatalog,
    iceberg_catalogs: &IcebergCatalogRegistry,
) -> Result<AnalyzedStatement, String> {
    match stmt {
        Statement::Query(query) => {
            analyze_query(query, ctx, local_catalog, iceberg_catalogs).map(AnalyzedStatement::Query)
        }
        other => Ok(AnalyzedStatement::Passthrough(other)),
    }
}

fn analyze_query(
    query: QueryStmt,
    ctx: AnalyzerContext<'_>,
    local_catalog: &InMemoryCatalog,
    iceberg_catalogs: &IcebergCatalogRegistry,
) -> Result<AnalyzedQuery, String> {
    let source = if ctx.current_catalog.is_none() {
        match query.from.name.parts.len() {
            1 | 2 => {
                let resolved = resolve_local_table_name(&query.from.name, ctx.current_database)?;
                let table = local_catalog.get(&resolved.database, &resolved.table)?;
                QuerySource::Local { resolved, table }
            }
            3 => {
                let resolved = resolve_iceberg_table_name_explicit(&query.from.name)?;
                let entry = iceberg_catalogs.get(&resolved.catalog)?;
                let loaded = load_iceberg_table(&entry, &resolved.namespace, &resolved.table)?;
                QuerySource::Iceberg { resolved, loaded }
            }
            _ => {
                return Err(format!(
                    "unsupported table name `{}`; expected `<table>`, `<database>.<table>`, or `<catalog>.<database>.<table>`",
                    query.from.name.parts.join(".")
                ));
            }
        }
    } else {
        let resolved = resolve_iceberg_table_name(
            query.from.name.clone(),
            ctx.current_catalog,
            ctx.current_database,
        )?;
        let entry = iceberg_catalogs.get(&resolved.catalog)?;
        let loaded = load_iceberg_table(&entry, &resolved.namespace, &resolved.table)?;
        QuerySource::Iceberg { resolved, loaded }
    };

    let columns = match &source {
        QuerySource::Local { table, .. } => &table.columns,
        QuerySource::Iceberg { loaded, .. } => &loaded.columns,
    };
    let bound = bind_query(&query, columns)?;
    Ok(AnalyzedQuery {
        source,
        bound,
        order_by: query.order_by,
    })
}

pub(crate) fn resolve_local_table_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "local table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn resolve_iceberg_table_name(
    name: ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<ResolvedIcebergTableName, String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        (Some(catalog), [namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        (_, [catalog, namespace, table]) => Ok(ResolvedIcebergTableName {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "iceberg table name must be `<table>`/`<database>.<table>` with current catalog or `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn resolve_iceberg_namespace_name(
    name: ObjectName,
    current_catalog: Option<&str>,
) -> Result<(String, String), String> {
    match (
        normalize_optional_identifier(current_catalog)?,
        name.parts.as_slice(),
    ) {
        (Some(catalog), [namespace]) => Ok((catalog, normalize_identifier(namespace)?)),
        (_, [catalog, namespace]) => Ok((
            normalize_identifier(catalog)?,
            normalize_identifier(namespace)?,
        )),
        _ => Err(format!(
            "iceberg database name must be `<database>` with current catalog or `<catalog>.<database>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

pub(crate) fn resolve_iceberg_table_name_explicit(
    name: &ObjectName,
) -> Result<ResolvedIcebergTableName, String> {
    let [catalog, namespace, table] = name.parts.as_slice() else {
        return Err(format!(
            "iceberg table name must be `<catalog>.<database>.<table>`, got `{}`",
            name.parts.join(".")
        ));
    };
    Ok(ResolvedIcebergTableName {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

pub(crate) fn normalize_optional_identifier(raw: Option<&str>) -> Result<Option<String>, String> {
    raw.map(normalize_identifier).transpose()
}

fn bind_query(query: &QueryStmt, columns: &[ColumnDef]) -> Result<BoundQuery, String> {
    let mut column_by_name = HashMap::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        column_by_name.insert(normalize_identifier(&column.name)?, idx);
    }

    let projection_indices = expand_projection(&query.projection, columns, &column_by_name)?;
    let mut scan_columns = Vec::new();
    let mut scan_index_by_source = HashMap::new();
    let mut output_columns = Vec::with_capacity(projection_indices.len());

    for &source_idx in &projection_indices {
        let scan_column_index = ensure_scan_column(
            &mut scan_columns,
            &mut scan_index_by_source,
            &columns[source_idx],
            source_idx,
        )?;
        let column = &columns[source_idx];
        output_columns.push(BoundOutputColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            scan_column_index,
            output_slot_id: None,
        });
    }

    let predicate = match query.selection.as_ref() {
        Some(expr) => Some(bind_predicate(
            expr,
            &mut scan_columns,
            &mut scan_index_by_source,
            columns,
            &column_by_name,
        )?),
        None => None,
    };

    for (idx, column) in scan_columns.iter_mut().enumerate() {
        column.scan_slot_id =
            i32::try_from(idx + 1).map_err(|_| "too many scan columns".to_string())?;
    }

    let needs_project = output_columns.len() != scan_columns.len()
        || output_columns
            .iter()
            .enumerate()
            .any(|(output_idx, column)| column.scan_column_index != output_idx);

    if needs_project {
        for (idx, column) in output_columns.iter_mut().enumerate() {
            column.output_slot_id = Some(
                i32::try_from(scan_columns.len() + idx + 1)
                    .map_err(|_| "too many output columns".to_string())?,
            );
        }
    }

    Ok(BoundQuery {
        scan_columns,
        output_columns,
        predicate,
        needs_project,
    })
}

fn expand_projection(
    projection: &[ProjectionItem],
    columns: &[ColumnDef],
    column_by_name: &HashMap<String, usize>,
) -> Result<Vec<usize>, String> {
    match projection {
        [ProjectionItem::Wildcard] => Ok((0..columns.len()).collect()),
        _ => projection
            .iter()
            .map(|item| match item {
                ProjectionItem::Wildcard => {
                    Err("wildcard projection cannot be combined with explicit columns".to_string())
                }
                ProjectionItem::Column(column) => resolve_column_index(column, column_by_name),
            })
            .collect(),
    }
}

fn resolve_column_index(
    column: &ColumnRef,
    column_by_name: &HashMap<String, usize>,
) -> Result<usize, String> {
    let name = normalize_identifier(&column.name)?;
    column_by_name
        .get(&name)
        .copied()
        .ok_or_else(|| format!("unknown column: {}", column.name))
}

fn ensure_scan_column(
    scan_columns: &mut Vec<BoundScanColumn>,
    scan_index_by_source: &mut HashMap<usize, usize>,
    column: &ColumnDef,
    source_idx: usize,
) -> Result<usize, String> {
    if let Some(&scan_idx) = scan_index_by_source.get(&source_idx) {
        return Ok(scan_idx);
    }
    let scan_idx = scan_columns.len();
    scan_columns.push(BoundScanColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        scan_slot_id: 0,
    });
    scan_index_by_source.insert(source_idx, scan_idx);
    Ok(scan_idx)
}

fn bind_predicate(
    expr: &Expr,
    scan_columns: &mut Vec<BoundScanColumn>,
    scan_index_by_source: &mut HashMap<usize, usize>,
    columns: &[ColumnDef],
    column_by_name: &HashMap<String, usize>,
) -> Result<BoundPredicate, String> {
    let Expr::Comparison { left, op, right } = expr;
    let source_idx = resolve_column_index(left, column_by_name)?;
    let column = &columns[source_idx];
    validate_literal_for_column(right, &column.data_type)?;
    let scan_column_index =
        ensure_scan_column(scan_columns, scan_index_by_source, column, source_idx)?;
    Ok(BoundPredicate {
        scan_column_index,
        op: *op,
        literal: right.clone(),
    })
}

fn validate_literal_for_column(literal: &Literal, column_type: &DataType) -> Result<(), String> {
    match literal {
        Literal::Null => Ok(()),
        Literal::Bool(_) if matches!(column_type, DataType::Boolean) => Ok(()),
        Literal::Int(_) => match column_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64 => Ok(()),
            other => Err(format!(
                "integer literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Float(_) => match column_type {
            DataType::Float32 | DataType::Float64 => Ok(()),
            other => Err(format!(
                "float literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::String(_) => match column_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Timestamp(_, _) => Ok(()),
            other => Err(format!(
                "string literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Date(_) => match column_type {
            DataType::Date32 | DataType::Timestamp(_, _) => Ok(()),
            other => Err(format!(
                "date literal is not supported for column type {:?}",
                other
            )),
        },
        Literal::Bool(_) => Err(format!(
            "boolean literal is not supported for column type {:?}",
            column_type
        )),
    }
}
