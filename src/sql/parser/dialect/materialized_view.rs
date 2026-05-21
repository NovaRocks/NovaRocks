//! Parsing for `CREATE / DROP / REFRESH / SHOW MATERIALIZED VIEW[S]` statements.
//!
//! Only the Phase 1 subset is accepted; unsupported clauses (ORDER BY,
//! REFRESH ASYNC/IMMEDIATE, missing DISTRIBUTED BY) are rejected
//! with an explicit error so that users pasting StarRocks DDL see a clear
//! signal rather than silent fallthrough.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, IcebergPartitionFieldExpr,
    MaterializedViewDistribution, RefreshMaterializedViewStmt, ShowMaterializedViewsStmt,
    Statement,
};
use crate::sql::parser::dialect::create_table::parse_partition_field_expr;

/// Check if the current position looks like `CREATE MATERIALIZED VIEW ...`.
/// The parser is not advanced.
pub(crate) fn looks_like_create_materialized_view(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::CREATE)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEW")
}

/// Parse `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name>
///   [COMMENT '...']
///   [PARTITION BY col[, ...]]    -- parsed and retained
///   DISTRIBUTED BY HASH(col, ...) [BUCKETS n]
///   [REFRESH [DEFERRED] MANUAL]  -- IMMEDIATE / ASYNC rejected
///   [ORDER BY ...]               -- rejected
///   [PROPERTIES(...)]            -- parsed and retained on the AST node
///   AS <query>`
pub(crate) fn parse_create_materialized_view(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::MATERIALIZED)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::VIEW)
        .map_err(|e| e.to_string())?;

    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    // Optional COMMENT '...' (parsed and dropped).
    if parser.parse_keyword(Keyword::COMMENT) {
        parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV comment failed: {e}"))?;
    }

    let partition_by = parse_partition_by(parser)?;

    // Required DISTRIBUTED BY clause.
    let distribution = parse_distributed_by(parser)?;
    if distribution.is_none() {
        return Err(
            "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause"
                .to_string(),
        );
    }

    // Optional REFRESH clause.
    let refresh_manual_explicit = if parser.parse_keyword(Keyword::REFRESH) {
        parse_refresh_clause(parser)?
    } else {
        false
    };

    // Optional PRIMARY KEY (col, ...) clause — IVM Phase-2 opt-in marker.
    let primary_key = if parser.parse_keyword(Keyword::PRIMARY) {
        parser
            .expect_keyword(Keyword::KEY)
            .map_err(|e| format!("expected KEY after PRIMARY: {e}"))?;
        parser
            .expect_token(&Token::LParen)
            .map_err(|e| format!("expected ( after PRIMARY KEY: {e}"))?;
        let mut cols: Vec<String> = Vec::new();
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            let ident = parser
                .parse_identifier()
                .map_err(|e| format!("parse PRIMARY KEY column failed: {e}"))?;
            let name = ident.value;
            if cols.iter().any(|c| c.eq_ignore_ascii_case(&name)) {
                return Err(format!("duplicate column `{name}` in PRIMARY KEY clause"));
            }
            cols.push(name);
            if parser.consume_token(&Token::RParen) {
                break;
            }
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected , or ) in PRIMARY KEY column list: {e}"))?;
        }
        if cols.is_empty() {
            return Err("PRIMARY KEY clause requires at least one column".to_string());
        }
        Some(cols)
    } else {
        None
    };

    // Reject ORDER BY (mirroring StarRocks clause ordering).
    if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
        return Err("ORDER BY is not supported on materialized views yet".to_string());
    }

    // Optional PROPERTIES(...) — parsed and retained on the AST node. Note:
    // PROPERTIES is not a sqlparser keyword, so we detect it textually.
    let properties = if peek_word_eq(parser, 0, "PROPERTIES") {
        parser.next_token(); // PROPERTIES
        parse_properties(parser)?
    } else {
        Vec::new()
    };

    parser
        .expect_keyword(Keyword::AS)
        .map_err(|e| format!("expected AS before MV query: {e}"))?;
    let query = parser
        .parse_query()
        .map_err(|e| format!("parse MV query failed: {e}"))?;
    // Use the parsed query's Display to produce a canonical SELECT body. This
    // is sufficient for Phase 1 because `select_sql` is re-parsed on every
    // REFRESH — exact whitespace preservation is not required.
    let select_sql = query.to_string();

    Ok(Statement::CreateMaterializedView(
        CreateMaterializedViewStmt {
            name,
            if_not_exists,
            partition_by,
            distribution,
            refresh_manual_explicit,
            select_sql,
            select_query: *query,
            properties,
            primary_key,
        },
    ))
}

fn parse_partition_by(
    parser: &mut Parser<'_>,
) -> Result<Option<Vec<IcebergPartitionFieldExpr>>, String> {
    if !parser.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
        return Ok(None);
    }

    let mut fields = Vec::new();
    if parser.consume_token(&Token::LParen) {
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            fields.push(parse_partition_field_expr(parser)?);
            if parser.consume_token(&Token::RParen) {
                break;
            }
            parser
                .expect_token(&Token::Comma)
                .map_err(|e| format!("expected , or ) in PARTITION BY field list: {e}"))?;
        }
    } else {
        fields.push(parse_partition_field_expr(parser)?);
        while parser.consume_token(&Token::Comma) {
            fields.push(parse_partition_field_expr(parser)?);
        }
    }

    if fields.is_empty() {
        return Err("PARTITION BY requires at least one field".to_string());
    }
    Ok(Some(fields))
}

fn parse_distributed_by(
    parser: &mut Parser<'_>,
) -> Result<Option<MaterializedViewDistribution>, String> {
    // `DISTRIBUTED` is not a sqlparser keyword; detect it via peek_word_eq.
    if !peek_word_eq(parser, 0, "DISTRIBUTED") {
        return Ok(None);
    }
    parser.next_token(); // DISTRIBUTED
    parser
        .expect_keyword(Keyword::BY)
        .map_err(|e| format!("expected BY after DISTRIBUTED: {e}"))?;
    parser
        .expect_keyword(Keyword::HASH)
        .map_err(|e| format!("expected HASH after DISTRIBUTED BY: {e}"))?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after HASH: {e}"))?;
    let mut hash_columns = Vec::new();
    loop {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse hash column failed: {e}"))?;
        hash_columns.push(ident.value);
        if parser.consume_token(&Token::RParen) {
            break;
        }
        parser
            .expect_token(&Token::Comma)
            .map_err(|e| format!("expected , or ) in hash column list: {e}"))?;
    }
    let bucket_count = if peek_word_eq(parser, 0, "BUCKETS") {
        parser.next_token(); // BUCKETS
        let value = parser
            .parse_literal_uint()
            .map_err(|e| format!("parse BUCKETS count failed: {e}"))?;
        Some(value as u32)
    } else {
        None
    };
    Ok(Some(MaterializedViewDistribution {
        hash_columns,
        bucket_count,
    }))
}

fn parse_refresh_clause(parser: &mut Parser<'_>) -> Result<bool, String> {
    // `REFRESH` already consumed by caller.
    if parser.parse_keyword(Keyword::IMMEDIATE) {
        return Err("REFRESH IMMEDIATE is not supported yet".to_string());
    }
    // DEFERRED is optional per StarRocks grammar
    // (`REFRESH (IMMEDIATE | DEFERRED)? (ASYNC ... | MANUAL)`).
    let _ = parser.parse_keyword(Keyword::DEFERRED);
    // ASYNC is not a sqlparser keyword; detect it textually.
    if peek_word_eq(parser, 0, "ASYNC") {
        parser.next_token();
        return Err("REFRESH ASYNC is not supported yet".to_string());
    }
    // MANUAL is not a sqlparser keyword; detect it textually.
    if !peek_word_eq(parser, 0, "MANUAL") {
        return Err("expected REFRESH [DEFERRED] MANUAL".to_string());
    }
    parser.next_token(); // MANUAL
    Ok(true)
}

/// Parse `(k = v, ...)` and return the key-value pairs.
fn parse_properties(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after PROPERTIES: {e}"))?;
    let mut out = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        let key = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property key failed: {e}"))?;
        parser
            .expect_token(&Token::Eq)
            .map_err(|e| format!("expected = in MV property: {e}"))?;
        let value = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property value failed: {e}"))?;
        out.push((key, value));
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(|e| format!("expected ) or , in PROPERTIES: {e}"))?;
        break;
    }
    Ok(out)
}

/// Check if the current position looks like `DROP MATERIALIZED VIEW ...`.
/// The parser is not advanced.
pub(crate) fn looks_like_drop_materialized_view(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::DROP)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEW")
}

/// Parse `DROP MATERIALIZED VIEW [IF EXISTS] <name>`.
///
/// Rejects `FORCE` explicitly so users pasting StarRocks DDL get a clear
/// error instead of silently dropping a MV with a modifier we don't honor.
pub(crate) fn parse_drop_materialized_view(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::DROP)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::MATERIALIZED)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::VIEW)
        .map_err(|e| e.to_string())?;

    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    if parser.parse_keyword(Keyword::FORCE) {
        return Err("DROP MATERIALIZED VIEW ... FORCE is not supported".to_string());
    }

    Ok(Statement::DropMaterializedView(DropMaterializedViewStmt {
        name,
        if_exists,
    }))
}

/// Check if the current position looks like `REFRESH MATERIALIZED VIEW ...`.
/// The parser is not advanced.
pub(crate) fn looks_like_refresh_materialized_view(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::REFRESH)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEW")
}

/// Parse `REFRESH MATERIALIZED VIEW <name> [WITH SYNC MODE]`.
///
/// Rejects `PARTITION START(...) END(...)` and `WITH ASYNC MODE`
/// because Phase 1 only supports whole-MV synchronous refresh.
pub(crate) fn parse_refresh_materialized_view(
    parser: &mut Parser<'_>,
) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::REFRESH)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::MATERIALIZED)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::VIEW)
        .map_err(|e| e.to_string())?;

    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    // Optional FULL keyword: `REFRESH MATERIALIZED VIEW <name> FULL [WITH SYNC MODE]`.
    let full = peek_word_eq(parser, 0, "FULL") && {
        parser.next_token();
        true
    };

    if parser.parse_keyword(Keyword::PARTITION) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... PARTITION START(...) END(...) is not supported yet"
                .to_string(),
        );
    }
    if parser.parse_keyword(Keyword::WITH) {
        if peek_word_eq(parser, 0, "ASYNC") {
            parser.next_token();
            return Err(
                "REFRESH MATERIALIZED VIEW ... WITH ASYNC MODE is not supported yet".to_string(),
            );
        }
        if !peek_word_eq(parser, 0, "SYNC") {
            return Err(
                "expected SYNC or ASYNC after REFRESH MATERIALIZED VIEW ... WITH".to_string(),
            );
        }
        parser.next_token();
        if !peek_word_eq(parser, 0, "MODE") {
            return Err("expected MODE after REFRESH MATERIALIZED VIEW ... WITH SYNC".to_string());
        }
        parser.next_token();
    }

    Ok(Statement::RefreshMaterializedView(
        RefreshMaterializedViewStmt { name, full },
    ))
}

/// Check if the current position looks like `SHOW MATERIALIZED VIEWS ...`.
/// The parser is not advanced.
pub(crate) fn looks_like_show_materialized_views(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::SHOW)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEWS")
}

/// Parse `SHOW MATERIALIZED VIEWS [FROM <db>]`.
///
/// Rejects `LIKE '...'` and `WHERE ...` so the Phase 1 output schema stays
/// predictable; clients that need filtering can do it client-side.
pub(crate) fn parse_show_materialized_views(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::MATERIALIZED)
        .map_err(|e| e.to_string())?;
    parser
        .expect_keyword(Keyword::VIEWS)
        .map_err(|e| e.to_string())?;

    let database = if parser.parse_keyword(Keyword::FROM) {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse database name after FROM: {e}"))?;
        Some(ident.value)
    } else {
        None
    };

    if parser.parse_keyword(Keyword::LIKE) {
        return Err("SHOW MATERIALIZED VIEWS LIKE '...' is not supported yet".to_string());
    }
    if parser.parse_keyword(Keyword::WHERE) {
        return Err("SHOW MATERIALIZED VIEWS WHERE ... is not supported yet".to_string());
    }

    Ok(Statement::ShowMaterializedViews(
        ShowMaterializedViewsStmt { database },
    ))
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ast::{IcebergPartitionFieldExpr, Statement};
    use crate::sql::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).expect("parse ok");
        assert_eq!(stmts.len(), 1, "exactly one stmt");
        stmts.pop().unwrap()
    }

    #[test]
    fn parse_create_mv_with_distributed_by_and_refresh_deferred_manual() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW analytics.orders_mv \
             DISTRIBUTED BY HASH(k1) BUCKETS 4 \
             REFRESH DEFERRED MANUAL \
             AS SELECT k1, sum(v2) AS total \
                 FROM iceberg_cat.ns.orders \
                 GROUP BY k1",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(mv.name.parts, vec!["analytics", "orders_mv"]);
        assert!(!mv.if_not_exists);
        assert_eq!(
            mv.distribution
                .as_ref()
                .expect("distribution clause")
                .hash_columns,
            vec!["k1".to_string()],
        );
        assert_eq!(
            mv.distribution
                .as_ref()
                .expect("distribution clause")
                .bucket_count,
            Some(4)
        );
        assert!(mv.refresh_manual_explicit);
    }

    #[test]
    fn parse_create_mv_accepts_refresh_manual_without_deferred() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 4 \
             REFRESH MANUAL \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert!(mv.refresh_manual_explicit);
    }

    #[test]
    fn parse_create_mv_with_if_not_exists_and_comment_and_properties_parsed() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 \
             COMMENT 'demo' \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PROPERTIES('storage_volume' = 'svc', 'replication_num' = '1') \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert!(mv.if_not_exists);
        assert_eq!(mv.name.parts, vec!["mv1"]);
    }

    #[test]
    fn parse_create_mv_accepts_simple_partition_by() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             PARTITION BY k1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(
            mv.partition_by,
            Some(vec![IcebergPartitionFieldExpr::Identity {
                column: "k1".to_string()
            }])
        );
    }

    #[test]
    fn parse_create_mv_accepts_iceberg_partition_transforms() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             PARTITION BY (year(ts), month(ts), day(ts), hour(ts), bucket(tenant_id, 8), truncate(region, 4), void(deleted_at), tenant_id) \
             DISTRIBUTED BY HASH(tenant_id) BUCKETS 1 \
             AS SELECT tenant_id, ts, region, deleted_at FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(
            mv.partition_by,
            Some(vec![
                IcebergPartitionFieldExpr::Year {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Day {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Hour {
                    column: "ts".to_string()
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "tenant_id".to_string(),
                    num_buckets: 8
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "region".to_string(),
                    width: 4
                },
                IcebergPartitionFieldExpr::Void {
                    column: "deleted_at".to_string()
                },
                IcebergPartitionFieldExpr::Identity {
                    column: "tenant_id".to_string()
                }
            ])
        );
    }

    #[test]
    fn parse_create_mv_rejects_order_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             ORDER BY (k1) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("order by"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_refresh_async() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH ASYNC \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("refresh async")
                || err.to_lowercase().contains("not supported"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_refresh_immediate() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH IMMEDIATE \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("immediate")
                || err.to_lowercase().contains("not supported"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_requires_distributed_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("distributed by"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_drop_mv_with_if_exists() {
        let stmt = parse_one("DROP MATERIALIZED VIEW IF EXISTS analytics.mv1");
        let drop = match stmt {
            Statement::DropMaterializedView(d) => d,
            other => panic!("unexpected: {other:?}"),
        };
        assert!(drop.if_exists);
        assert_eq!(drop.name.parts, vec!["analytics", "mv1"]);
    }

    #[test]
    fn parse_drop_mv_rejects_force() {
        let err = crate::sql::parser::parse_sql("DROP MATERIALIZED VIEW mv1 FORCE")
            .expect_err("should reject");
        assert!(err.to_lowercase().contains("force"), "err={err}");
    }

    #[test]
    fn parse_refresh_mv() {
        let stmt = parse_one("REFRESH MATERIALIZED VIEW analytics.mv1");
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert_eq!(r.name.parts, vec!["analytics", "mv1"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_refresh_mv_rejects_partition_range() {
        let err = crate::sql::parser::parse_sql(
            "REFRESH MATERIALIZED VIEW mv1 PARTITION START ('2024-01-01') END ('2024-02-01')",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("partition")
                || err.to_lowercase().contains("not supported"),
            "err={err}"
        );
    }

    #[test]
    fn parse_refresh_mv_accepts_sync_modifier() {
        let stmt = parse_one("REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE");
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert_eq!(r.name.parts, vec!["mv1"]);
                assert!(
                    !r.full,
                    "expected full=false for plain REFRESH WITH SYNC MODE"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_refresh_mv_rejects_async_modifier() {
        let err = crate::sql::parser::parse_sql("REFRESH MATERIALIZED VIEW mv1 WITH ASYNC MODE")
            .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("async") || err.to_lowercase().contains("not supported"),
            "err={err}"
        );
    }

    #[test]
    fn parse_refresh_mv_full_sets_full_flag() {
        let stmt = parse_one("REFRESH MATERIALIZED VIEW foo FULL");
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert!(r.full, "expected full=true for REFRESH ... FULL");
                assert_eq!(r.name.parts, vec!["foo"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_refresh_mv_without_full_has_full_false() {
        let stmt = parse_one("REFRESH MATERIALIZED VIEW foo");
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert!(!r.full, "expected full=false for plain REFRESH");
                assert_eq!(r.name.parts, vec!["foo"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_refresh_mv_full_with_sync_mode() {
        let stmt = parse_one("REFRESH MATERIALIZED VIEW foo FULL WITH SYNC MODE");
        match stmt {
            Statement::RefreshMaterializedView(r) => {
                assert!(r.full, "expected full=true");
                assert_eq!(r.name.parts, vec!["foo"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_show_materialized_views_no_filters() {
        let stmt = parse_one("SHOW MATERIALIZED VIEWS");
        match stmt {
            Statement::ShowMaterializedViews(s) => assert!(s.database.is_none()),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_show_materialized_views_from_db() {
        let stmt = parse_one("SHOW MATERIALIZED VIEWS FROM analytics");
        match stmt {
            Statement::ShowMaterializedViews(s) => {
                assert_eq!(s.database, Some("analytics".to_string()))
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parse_show_materialized_views_rejects_like_and_where() {
        let err_like = crate::sql::parser::parse_sql("SHOW MATERIALIZED VIEWS LIKE '%mv%'")
            .expect_err("should reject LIKE");
        assert!(
            err_like.to_lowercase().contains("like")
                || err_like.to_lowercase().contains("not supported"),
            "err={err_like}"
        );
        let err_where = crate::sql::parser::parse_sql("SHOW MATERIALIZED VIEWS WHERE name = 'mv1'")
            .expect_err("should reject WHERE");
        assert!(
            err_where.to_lowercase().contains("where")
                || err_where.to_lowercase().contains("not supported"),
            "err={err_where}"
        );
    }

    #[test]
    fn parse_create_materialized_view_keeps_storage_engine_property() {
        let sql = "CREATE MATERIALIZED VIEW mv1 \
            DISTRIBUTED BY HASH(k) BUCKETS 2 \
            PROPERTIES('storage_engine' = 'iceberg', 'comment' = 'demo') \
            AS SELECT k, v FROM ice.ns.t";
        let stmt = parse_one(sql);
        let crate::sql::parser::ast::Statement::CreateMaterializedView(create) = stmt else {
            panic!("expected CREATE MATERIALIZED VIEW");
        };
        assert_eq!(
            create.properties,
            vec![
                ("storage_engine".to_string(), "iceberg".to_string()),
                ("comment".to_string(), "demo".to_string()),
            ],
        );
    }

    #[test]
    fn parse_create_mv_with_primary_key_captures_columns() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY (order_id, line_id) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(
            mv.primary_key.as_deref(),
            Some(["order_id".to_string(), "line_id".to_string()].as_slice()),
        );
    }

    #[test]
    fn parse_create_mv_without_primary_key_keeps_field_none() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert!(mv.primary_key.is_none());
    }

    #[test]
    fn parse_create_mv_rejects_empty_primary_key_list() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY () \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("primary key"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_duplicate_primary_key_columns() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PRIMARY KEY (order_id, order_id) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("duplicate"),
            "unexpected err: {err}"
        );
    }
}
