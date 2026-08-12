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

//! Provider-neutral materialized-view target schema helpers.

use crate::sql::planner::vocabulary::{
    BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, JOIN_APPLY_KEY_COLUMN_NAME,
};

pub(crate) fn apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::BigInt,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn join_apply_key_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::String,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn branch_id_table_column() -> crate::sql::parser::ast::TableColumnDef {
    crate::sql::parser::ast::TableColumnDef {
        name: BRANCH_ID_COLUMN_NAME.to_string(),
        data_type: novarocks_catalog::schema::SqlType::Int,
        nullable: false,
        aggregation: None,
        default: None,
    }
}

pub(crate) fn iceberg_mv_physical_select_sql(select_sql: &str) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV physical SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV physical SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("iceberg MV physical SELECT expects a SELECT query".to_string());
    };
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("iceberg MV physical SELECT expects a SELECT body".to_string());
    };

    validate_reserved_projection_output_names(
        select,
        &[(HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key")],
    )?;
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(_)
            | sqlparser::ast::SelectItem::ExprWithAlias { .. } => {}
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => {
                return Err(
                    "iceberg MV physical SELECT requires explicit projection columns".to_string(),
                );
            }
        }
    }

    select
        .projection
        .push(sqlparser::ast::SelectItem::ExprWithAlias {
            expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
            alias: sqlparser::ast::Ident::new(HIDDEN_APPLY_KEY_COLUMN_NAME),
        });
    Ok(stmt.to_string())
}

pub(crate) fn validate_reserved_projection_output_names(
    select: &sqlparser::ast::Select,
    reserved: &[(&str, &str)],
) -> Result<(), String> {
    for item in &select.projection {
        let output_name = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => Some(expr.to_string()),
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => None,
        };
        let Some(output_name) = output_name else {
            continue;
        };
        for (reserved_name, purpose) in reserved {
            if output_name.eq_ignore_ascii_case(reserved_name) {
                return Err(format!(
                    "Iceberg MV output column name {reserved_name} is reserved for internal {purpose}"
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn ensure_base_row_lineage_contract(
    observation: &crate::mv::storage_observation::MvSchemaValidationObservation,
    base_fqn: &str,
) -> Result<(), String> {
    if !observation.is_format_v3() || !observation.stored_row_lineage_enabled() {
        return Err(format!(
            "iceberg-backed materialized views require base table {base_fqn} to be Iceberg format-version=3 with write.row-lineage=true; \
             upgrade the table or recreate it with TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")"
        ));
    }
    Ok(())
}
