//! Parsing for `CREATE / DROP / REFRESH / SHOW MATERIALIZED VIEW[S]` statements.
//!
//! Only the Phase 1 subset is accepted; unsupported clauses (PARTITION BY,
//! ORDER BY, REFRESH ASYNC/IMMEDIATE, missing DISTRIBUTED BY) are rejected
//! with an explicit error so that users pasting StarRocks DDL see a clear
//! signal rather than silent fallthrough.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use super::{convert_object_name, peek_word_eq};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt, Statement,
};

/// Check if the current position looks like `CREATE MATERIALIZED VIEW ...`.
/// The parser is not advanced.
pub(crate) fn looks_like_create_materialized_view(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::CREATE)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEW")
}

/// Parse `CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name>
///   [COMMENT '...']
///   [PARTITION BY ...]           -- rejected
///   DISTRIBUTED BY HASH(col, ...) [BUCKETS n]
///   [REFRESH DEFERRED MANUAL]    -- IMMEDIATE / ASYNC rejected
///   [ORDER BY ...]               -- rejected
///   [PROPERTIES(...)]            -- parsed and dropped
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

    // Reject PARTITION BY up-front.
    if parser.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
        return Err("PARTITION BY is not supported on materialized views yet".to_string());
    }

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

    // Reject ORDER BY (mirroring StarRocks clause ordering).
    if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
        return Err("ORDER BY is not supported on materialized views yet".to_string());
    }

    // Optional PROPERTIES(...) — parsed and dropped in Phase 1. Note:
    // PROPERTIES is not a sqlparser keyword, so we detect it textually.
    if peek_word_eq(parser, 0, "PROPERTIES") {
        parser.next_token(); // PROPERTIES
        parse_and_drop_properties(parser)?;
    }

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
            distribution,
            refresh_manual_explicit,
            select_sql,
            select_query: *query,
        },
    ))
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
    // ASYNC is not a sqlparser keyword; detect it textually.
    if peek_word_eq(parser, 0, "ASYNC") {
        parser.next_token();
        return Err("REFRESH ASYNC is not supported yet".to_string());
    }
    parser
        .expect_keyword(Keyword::DEFERRED)
        .map_err(|e| format!("expected REFRESH DEFERRED MANUAL: {e}"))?;
    // MANUAL is not a sqlparser keyword; detect it textually.
    if !peek_word_eq(parser, 0, "MANUAL") {
        return Err("expected REFRESH DEFERRED MANUAL".to_string());
    }
    parser.next_token(); // MANUAL
    Ok(true)
}

/// Parse `(k = v, ...)` and discard — PROPERTIES contents are ignored in
/// Phase 1 because MV storage and replication are managed by the lake.
fn parse_and_drop_properties(parser: &mut Parser<'_>) -> Result<(), String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after PROPERTIES: {e}"))?;
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        let _key = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property key failed: {e}"))?;
        parser
            .expect_token(&Token::Eq)
            .map_err(|e| format!("expected = in MV property: {e}"))?;
        let _val = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property value failed: {e}"))?;
        if !parser.consume_token(&Token::Comma) {
            parser
                .expect_token(&Token::RParen)
                .map_err(|e| format!("expected , or ) in MV properties: {e}"))?;
            break;
        }
    }
    Ok(())
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

/// Parse `REFRESH MATERIALIZED VIEW <name>`.
///
/// Rejects `PARTITION START(...) END(...)` and `WITH {SYNC|ASYNC} MODE`
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

    if parser.parse_keyword(Keyword::PARTITION) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... PARTITION START(...) END(...) is not supported yet"
                .to_string(),
        );
    }
    if parser.parse_keyword(Keyword::WITH) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... WITH {SYNC|ASYNC} MODE is not supported yet".to_string(),
        );
    }

    Ok(Statement::RefreshMaterializedView(
        RefreshMaterializedViewStmt { name },
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
    use crate::sql::parser::ast::Statement;
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
    fn parse_create_mv_with_if_not_exists_and_comment_and_properties_ignored() {
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
    fn parse_create_mv_rejects_partition_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             PARTITION BY k1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("partition by"),
            "unexpected err: {err}"
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
    fn parse_refresh_mv_rejects_async_modifier() {
        let err = crate::sql::parser::parse_sql("REFRESH MATERIALIZED VIEW mv1 WITH ASYNC MODE")
            .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("async") || err.to_lowercase().contains("not supported"),
            "err={err}"
        );
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
}
