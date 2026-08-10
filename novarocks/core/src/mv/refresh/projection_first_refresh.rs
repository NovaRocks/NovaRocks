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

//! Projection/filter MV full-read preparation.
//!
//! This module owns exact snapshot pinning and physical projection shaping.
//! The standalone engine supplies query execution through an invocation-local
//! callback; lifecycle and Iceberg writes stay in the engine.

use std::collections::HashSet;

use crate::mv::refresh::pin::{RefreshSnapshotPin, inject_pin_as_for_version_as_of};
use crate::mv::refresh::target_apply::{
    iceberg_mv_physical_select_sql, validate_reserved_projection_output_names,
};
use crate::sql::planner::vocabulary::{BRANCH_ID_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME};
use novarocks_execution::exec::chunk::Chunk;

pub(crate) fn prepare_projection_full_read_sql(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|error| format!("iceberg projection full-read SELECT normalize error: {error}"))?;
    let mut statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("iceberg projection full-read SELECT parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut statement else {
        return Err("iceberg projection full read expects a SELECT query".to_string());
    };
    inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    iceberg_mv_physical_select_sql(&statement.to_string())
}

pub(crate) fn prepare_projection_first_refresh_chunks<F>(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str) -> Result<Vec<Chunk>, String>,
{
    let physical_sql =
        prepare_projection_full_read_sql(select_sql, pin, current_catalog, current_database)?;
    read(&physical_sql)
}

pub(crate) fn prepare_union_projection_full_read_sql(
    select_sql: &str,
    branch_count: usize,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    if branch_count < 2 {
        return Err("iceberg UNION ALL MV full refresh requires at least 2 branches".to_string());
    }
    let branch_count_i32 = i32::try_from(branch_count).map_err(|_| {
        format!("iceberg UNION ALL MV full refresh branch count {branch_count} does not fit in i32")
    })?;

    let normalized =
        crate::sql::parser::dialect::normalize_for_raw_parse(select_sql).map_err(|error| {
            format!("iceberg UNION ALL MV full-read SELECT normalize error: {error}")
        })?;
    let mut statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("iceberg UNION ALL MV full-read SELECT parse error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut statement else {
        return Err("iceberg UNION ALL MV full refresh expects a SELECT query".to_string());
    };
    inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;

    let mut validated_branch_count = 0_usize;
    let mut saw_union_all = false;
    validate_union_projection_set_expr(
        query.body.as_ref(),
        branch_count,
        &mut validated_branch_count,
        &mut saw_union_all,
    )?;
    if !saw_union_all {
        return Err("iceberg UNION ALL MV full refresh requires an actual UNION ALL".to_string());
    }
    if validated_branch_count != branch_count {
        return Err(format!(
            "iceberg UNION ALL MV full refresh expected {branch_count} branches, rewrote {validated_branch_count}"
        ));
    }

    let mut next_branch_id = 0_i32;
    append_union_projection_hidden_columns(query.body.as_mut(), &mut next_branch_id)?;
    debug_assert_eq!(next_branch_id, branch_count_i32);
    Ok(statement.to_string())
}

fn validate_union_projection_set_expr(
    set_expr: &sqlparser::ast::SetExpr,
    branch_count: usize,
    validated_branch_count: &mut usize,
    saw_union_all: &mut bool,
) -> Result<(), String> {
    match set_expr {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if *op != sqlparser::ast::SetOperator::Union
                || *set_quantifier != sqlparser::ast::SetQuantifier::All
            {
                return Err("iceberg UNION ALL MV full refresh supports UNION ALL only".to_string());
            }
            *saw_union_all = true;
            validate_union_projection_set_expr(
                left.as_ref(),
                branch_count,
                validated_branch_count,
                saw_union_all,
            )?;
            validate_union_projection_set_expr(
                right.as_ref(),
                branch_count,
                validated_branch_count,
                saw_union_all,
            )
        }
        sqlparser::ast::SetExpr::Query(query) => validate_union_projection_set_expr(
            query.body.as_ref(),
            branch_count,
            validated_branch_count,
            saw_union_all,
        ),
        sqlparser::ast::SetExpr::Select(select) => {
            if *validated_branch_count >= branch_count {
                return Err(format!(
                    "iceberg UNION ALL MV full refresh found more than {branch_count} branches"
                ));
            }
            validate_reserved_projection_output_names(
                select,
                &[
                    (HIDDEN_APPLY_KEY_COLUMN_NAME, "apply key"),
                    (BRANCH_ID_COLUMN_NAME, "branch id"),
                ],
            )?;
            *validated_branch_count += 1;
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

fn append_union_projection_hidden_columns(
    set_expr: &mut sqlparser::ast::SetExpr,
    next_branch_id: &mut i32,
) -> Result<(), String> {
    match set_expr {
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            append_union_projection_hidden_columns(left.as_mut(), next_branch_id)?;
            append_union_projection_hidden_columns(right.as_mut(), next_branch_id)
        }
        sqlparser::ast::SetExpr::Query(query) => {
            append_union_projection_hidden_columns(query.body.as_mut(), next_branch_id)
        }
        sqlparser::ast::SetExpr::Select(select) => {
            let branch_id = *next_branch_id;
            *next_branch_id = next_branch_id
                .checked_add(1)
                .ok_or_else(|| "iceberg UNION ALL MV branch id overflow".to_string())?;
            select
                .projection
                .push(sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("_row_id")),
                    alias: sqlparser::ast::Ident::new(HIDDEN_APPLY_KEY_COLUMN_NAME),
                });
            select
                .projection
                .push(sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Cast {
                        kind: sqlparser::ast::CastKind::Cast,
                        expr: Box::new(sqlparser::ast::Expr::Value(
                            sqlparser::ast::Value::Number(branch_id.to_string(), false).into(),
                        )),
                        data_type: sqlparser::ast::DataType::Int(None),
                        array: false,
                        format: None,
                    },
                    alias: sqlparser::ast::Ident::new(BRANCH_ID_COLUMN_NAME),
                });
            Ok(())
        }
        _ => Err("iceberg UNION ALL MV full refresh expects SELECT branches".to_string()),
    }
}

pub(crate) fn prepare_union_projection_first_refresh_chunks<F>(
    select_sql: &str,
    branch_count: usize,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str) -> Result<Vec<Chunk>, String>,
{
    let physical_sql = prepare_union_projection_full_read_sql(
        select_sql,
        branch_count,
        pin,
        current_catalog,
        current_database,
    )?;
    read(&physical_sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[("ice.db.fact", 42, "fact-uuid")])
    }

    fn union_pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[
            ("ice.db.a", 11, "a-uuid"),
            ("ice.db.b", 22, "b-uuid"),
            ("ice.db.c", 33, "c-uuid"),
        ])
    }

    #[test]
    fn single_preparation_injects_exact_pin_and_physical_apply_key() {
        let mut reads = 0;
        let chunks = prepare_projection_first_refresh_chunks(
            "SELECT id, name FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
            &mut |physical_sql| {
                reads += 1;
                assert!(physical_sql.contains("VERSION AS OF 42"), "{physical_sql}");
                assert!(physical_sql.contains("_row_id"), "{physical_sql}");
                assert!(
                    physical_sql.contains("__nova_base_row_id"),
                    "{physical_sql}"
                );
                Ok(Vec::new())
            },
        )
        .expect("prepare single projection first refresh");

        assert_eq!(reads, 1);
        assert!(chunks.is_empty());
    }

    #[test]
    fn single_preparation_rejects_conflicting_explicit_time_travel_before_read() {
        let mut reads = 0;
        let error = prepare_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.fact VERSION AS OF 7",
            &pin(),
            Some("ice"),
            "db",
            &mut |_| {
                reads += 1;
                Ok(Vec::new())
            },
        )
        .expect_err("conflicting time travel must fail");

        assert_eq!(reads, 0);
        assert!(error.contains("must not write explicit"), "{error}");
    }

    #[test]
    fn single_preparation_rejects_wildcard_and_reserved_alias_before_read() {
        for (select_sql, expected) in [
            (
                "SELECT * FROM ice.db.fact",
                "requires explicit projection columns",
            ),
            (
                "SELECT id AS __nova_base_row_id FROM ice.db.fact",
                "reserved for internal apply key",
            ),
        ] {
            let mut reads = 0;
            let error = prepare_projection_first_refresh_chunks(
                select_sql,
                &pin(),
                Some("ice"),
                "db",
                &mut |_| {
                    reads += 1;
                    Ok(Vec::new())
                },
            )
            .expect_err("invalid physical projection must fail");

            assert_eq!(reads, 0, "sql={select_sql}");
            assert!(error.contains(expected), "sql={select_sql} error={error}");
        }
    }

    #[test]
    fn single_callback_failure_propagates_without_chunks() {
        let error = prepare_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.fact",
            &pin(),
            Some("ice"),
            "db",
            &mut |_| Err("projection read failed".to_string()),
        )
        .expect_err("callback failure must propagate");

        assert_eq!(error, "projection read failed");
    }

    #[test]
    fn union_preparation_pins_nested_branches_and_appends_hidden_columns_left_to_right() {
        let mut reads = 0;
        let chunks = prepare_union_projection_first_refresh_chunks(
            "SELECT id, name FROM ice.db.a UNION ALL (SELECT id, name FROM ice.db.b UNION ALL SELECT id, name FROM ice.db.c)",
            3,
            &union_pin(),
            Some("ice"),
            "db",
            &mut |physical_sql| {
                reads += 1;
                for snapshot in [11, 22, 33] {
                    assert!(
                        physical_sql.contains(&format!("VERSION AS OF {snapshot}")),
                        "{physical_sql}"
                    );
                }
                assert_eq!(
                    physical_sql.matches("AS __nova_base_row_id").count(),
                    3,
                    "{physical_sql}"
                );
                assert_eq!(
                    physical_sql.matches("AS __branch_id__").count(),
                    3,
                    "{physical_sql}"
                );
                let branch_0 = physical_sql.find("CAST(0 AS INT) AS __branch_id__").unwrap();
                let branch_1 = physical_sql.find("CAST(1 AS INT) AS __branch_id__").unwrap();
                let branch_2 = physical_sql.find("CAST(2 AS INT) AS __branch_id__").unwrap();
                assert!(branch_0 < branch_1 && branch_1 < branch_2, "{physical_sql}");
                Ok(Vec::new())
            },
        )
        .expect("prepare nested union projection first refresh");

        assert_eq!(reads, 1);
        assert!(chunks.is_empty());
    }

    #[test]
    fn union_preparation_preserves_wildcard_branches() {
        let mut reads = 0;
        prepare_union_projection_first_refresh_chunks(
            "SELECT * FROM ice.db.a UNION ALL SELECT b.* FROM ice.db.b AS b",
            2,
            &union_pin(),
            Some("ice"),
            "db",
            &mut |physical_sql| {
                reads += 1;
                assert!(physical_sql.contains("SELECT *, _row_id AS __nova_base_row_id"));
                assert!(physical_sql.contains("SELECT b.*, _row_id AS __nova_base_row_id"));
                Ok(Vec::new())
            },
        )
        .expect("union wildcard branches must remain supported");

        assert_eq!(reads, 1);
    }

    #[test]
    fn union_preparation_rejects_invalid_shape_before_read() {
        let cases = [
            ("SELECT id FROM ice.db.a", 0, "at least 2 branches"),
            ("SELECT id FROM ice.db.a", 1, "at least 2 branches"),
            ("SELECT id FROM ice.db.a", 2, "requires an actual UNION ALL"),
            (
                "SELECT id FROM ice.db.a UNION SELECT id FROM ice.db.b",
                2,
                "supports UNION ALL only",
            ),
            (
                "SELECT id FROM ice.db.a INTERSECT SELECT id FROM ice.db.b",
                2,
                "supports UNION ALL only",
            ),
            (
                "SELECT id FROM ice.db.a EXCEPT SELECT id FROM ice.db.b",
                2,
                "supports UNION ALL only",
            ),
            (
                "SELECT id FROM ice.db.a UNION ALL VALUES (1)",
                2,
                "expects SELECT branches",
            ),
            (
                "SELECT id FROM ice.db.a UNION ALL SELECT id FROM ice.db.b",
                3,
                "expected 3 branches, rewrote 2",
            ),
            (
                "SELECT id FROM ice.db.a UNION ALL SELECT id FROM ice.db.b UNION ALL SELECT id FROM ice.db.c",
                2,
                "found more than 2 branches",
            ),
        ];

        for (select_sql, branch_count, expected) in cases {
            let mut reads = 0;
            let error = prepare_union_projection_first_refresh_chunks(
                select_sql,
                branch_count,
                &union_pin(),
                Some("ice"),
                "db",
                &mut |_| {
                    reads += 1;
                    Ok(Vec::new())
                },
            )
            .expect_err("invalid union shape must fail");
            assert_eq!(reads, 0, "sql={select_sql}");
            assert!(error.contains(expected), "sql={select_sql} error={error}");
        }

        let mut reads = 0;
        let error = prepare_union_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.a UNION ALL SELECT id FROM ice.db.b",
            (i32::MAX as usize) + 1,
            &union_pin(),
            Some("ice"),
            "db",
            &mut |_| {
                reads += 1;
                Ok(Vec::new())
            },
        )
        .expect_err("branch id overflow must fail");
        assert_eq!(reads, 0);
        assert!(error.contains("does not fit in i32"), "{error}");
    }

    #[test]
    fn union_preparation_rejects_reserved_aliases_before_read() {
        for (select_sql, expected) in [
            (
                "SELECT id AS __nova_base_row_id FROM ice.db.a UNION ALL SELECT id FROM ice.db.b",
                "reserved for internal apply key",
            ),
            (
                "SELECT id FROM ice.db.a UNION ALL SELECT id AS __branch_id__ FROM ice.db.b",
                "reserved for internal branch id",
            ),
        ] {
            let mut reads = 0;
            let error = prepare_union_projection_first_refresh_chunks(
                select_sql,
                2,
                &union_pin(),
                Some("ice"),
                "db",
                &mut |_| {
                    reads += 1;
                    Ok(Vec::new())
                },
            )
            .expect_err("reserved union alias must fail");

            assert_eq!(reads, 0, "sql={select_sql}");
            assert!(error.contains(expected), "sql={select_sql} error={error}");
        }
    }

    #[test]
    fn union_callback_failure_propagates_without_chunks() {
        let error = prepare_union_projection_first_refresh_chunks(
            "SELECT id FROM ice.db.a UNION ALL SELECT id FROM ice.db.b",
            2,
            &union_pin(),
            Some("ice"),
            "db",
            &mut |_| Err("union projection read failed".to_string()),
        )
        .expect_err("callback failure must propagate");

        assert_eq!(error, "union projection read failed");
    }
}
