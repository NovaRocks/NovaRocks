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

use novarocks_parser::{
    ast::{Fold, MaterializedViewExplainLevel, MaterializedViewStatement, Statement, Visit},
    parse,
    printer::print_statements,
};

#[test]
fn create_materialized_view_preserves_the_embedded_query_slice() {
    let source = "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv \
        COMMENT 'daily' PARTITION BY (bucket(order_id, 16), order_day) \
        DISTRIBUTED BY HASH (order_id) BUCKETS 3 \
        REFRESH DEFERRED ASYNC EVERY INTERVAL 2 HOURS PRIMARY KEY (order_id) \
        PROPERTIES ('storage_engine' = 'iceberg') AS SELECT order_id FROM orders";
    let statements = parse(source).expect("CREATE MATERIALIZED VIEW should parse");
    let [Statement::MaterializedView(MaterializedViewStatement::Create(create))] =
        statements.as_slice()
    else {
        panic!("expected typed CREATE MATERIALIZED VIEW");
    };
    assert_eq!(
        novarocks_parser::printer::print_query(&create.query),
        "SELECT order_id FROM orders"
    );
    assert_eq!(
        print_statements(&statements),
        "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv COMMENT 'daily' \
         PARTITION BY (bucket(order_id, 16), order_day) DISTRIBUTED BY HASH (order_id) \
         BUCKETS 3 REFRESH DEFERRED ASYNC EVERY INTERVAL 2 HOURS PRIMARY KEY (order_id) \
         PROPERTIES ('storage_engine' = 'iceberg') AS SELECT order_id FROM orders"
    );
}

#[test]
fn materialized_view_query_is_typed_and_excludes_outside_trivia_and_semicolon() {
    let source = "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH (id) BUCKETS 1 AS \
        /* outside query */ SELECT id /* inside */ FROM orders  /* trailing */ ;";
    let statements = parse(source).expect("CREATE MATERIALIZED VIEW should parse");
    let [Statement::MaterializedView(MaterializedViewStatement::Create(create))] =
        statements.as_slice()
    else {
        panic!("expected typed CREATE MATERIALIZED VIEW");
    };
    assert_eq!(
        novarocks_parser::printer::print_query(&create.query),
        "SELECT id FROM orders"
    );
    assert_eq!(
        create.raw_query_source,
        "SELECT id /* inside */ FROM orders"
    );
    assert_eq!(
        source.get(create.query_source_span.start()..create.query_source_span.end()),
        Some(create.raw_query_source.as_str())
    );
    assert_eq!(
        create.query.span,
        novarocks_parser::Span::new(
            source.find("SELECT").expect("query start"),
            source.find("  /* trailing */").expect("query end"),
        )
    );
}

#[test]
fn materialized_view_body_source_keeps_internal_trivia_and_statement_boundaries() {
    let body =
        "select /* 内部 */ `MiXeD` , sum(v1) AS total -- per key\n  from orders   group by `MiXeD`";
    let source = format!(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH (`MiXeD`) BUCKETS 1 \
         AS /* outside query */\n{body} -- outside query\n; SELECT 9"
    );
    let statements = parse(&source).expect("MV body with internal trivia should parse");
    let Statement::MaterializedView(MaterializedViewStatement::Create(create)) = &statements[0]
    else {
        panic!("expected typed CREATE MATERIALIZED VIEW");
    };
    assert_eq!(create.raw_query_source, body);
    assert_eq!(
        source.get(create.query_source_span.start()..create.query_source_span.end()),
        Some(body)
    );
    assert_eq!(statements.len(), 2);
}

#[test]
fn materialized_view_body_source_excludes_outer_trivia_at_eof() {
    let source = "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH (x) BUCKETS 1 \
                  AS /* before */ SELECT 1 AS x /* tail */  ";
    let statements = parse(source).expect("MV body at EOF should parse");
    let [Statement::MaterializedView(MaterializedViewStatement::Create(create))] =
        statements.as_slice()
    else {
        panic!("expected typed CREATE MATERIALIZED VIEW");
    };
    assert_eq!(create.raw_query_source, "SELECT 1 AS x");
    assert_eq!(
        source.get(create.query_source_span.start()..create.query_source_span.end()),
        Some(create.raw_query_source.as_str())
    );
}

#[test]
fn materialized_view_body_source_includes_parentheses_with_and_query_suffixes() {
    for body in [
        "(SELECT 1 AS x)",
        "WITH c(x) AS (VALUES (1)) SELECT x FROM c",
        "SELECT 1 AS x OFFSET 0 ROWS",
        "SELECT 1 AS x FETCH FIRST 2 ROWS ONLY",
    ] {
        let source = format!(
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH (x) BUCKETS 1 \
             AS /* before */ {body} /* after */;"
        );
        let statements = parse(&source).expect("complete MV query body should parse");
        let [Statement::MaterializedView(MaterializedViewStatement::Create(create))] =
            statements.as_slice()
        else {
            panic!("expected typed CREATE MATERIALIZED VIEW");
        };
        assert_eq!(create.raw_query_source, body);
        assert_eq!(
            source.get(create.query_source_span.start()..create.query_source_span.end()),
            Some(body)
        );
    }
}

#[test]
fn mv_command_family_round_trips_through_the_typed_ast() {
    let source = "DROP MATERIALIZED VIEW IF EXISTS analytics.mv; \
        ALTER MATERIALIZED VIEW analytics.mv SET TBLPROPERTIES ('ttl' = '7'); \
        ALTER MATERIALIZED VIEW analytics.mv PAUSE REFRESH; \
        ALTER MATERIALIZED VIEW analytics.mv RESUME REFRESH; \
        ALTER MATERIALIZED VIEW analytics.mv REPARTITION BY (truncate(name, 4)); \
        REFRESH MATERIALIZED VIEW analytics.mv FULL WITH SYNC MODE; \
        EXPLAIN REFRESH MATERIALIZED VIEW analytics.mv; \
        EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW analytics.mv; \
        EXPLAIN COSTS REFRESH MATERIALIZED VIEW analytics.mv";
    let statements = parse(source).expect("MV command family should parse");
    assert_eq!(statements.len(), 9);
    let [
        _,
        _,
        _,
        _,
        _,
        _,
        Statement::MaterializedView(MaterializedViewStatement::ExplainRefresh(default)),
        Statement::MaterializedView(MaterializedViewStatement::ExplainRefresh(verbose)),
        Statement::MaterializedView(MaterializedViewStatement::ExplainRefresh(costs)),
    ] = statements.as_slice()
    else {
        panic!("expected typed EXPLAIN REFRESH statements");
    };
    assert_eq!(default.level, MaterializedViewExplainLevel::Default);
    assert_eq!(verbose.level, MaterializedViewExplainLevel::Verbose);
    assert_eq!(costs.level, MaterializedViewExplainLevel::Costs);
    let printed = print_statements(&statements);
    assert_eq!(
        printed,
        "DROP MATERIALIZED VIEW IF EXISTS analytics.mv; \
         ALTER MATERIALIZED VIEW analytics.mv SET TBLPROPERTIES ('ttl' = '7'); \
         ALTER MATERIALIZED VIEW analytics.mv PAUSE REFRESH; \
         ALTER MATERIALIZED VIEW analytics.mv RESUME REFRESH; \
         ALTER MATERIALIZED VIEW analytics.mv REPARTITION BY (truncate(name, 4)); \
         REFRESH MATERIALIZED VIEW analytics.mv FULL WITH SYNC MODE; \
          EXPLAIN REFRESH MATERIALIZED VIEW analytics.mv; \
         EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW analytics.mv; \
         EXPLAIN COSTS REFRESH MATERIALIZED VIEW analytics.mv"
    );
    let reparsed = parse(&printed).expect("printed MV commands should parse");
    assert_eq!(reparsed.len(), statements.len());
    assert!(
        reparsed
            .iter()
            .all(|statement| matches!(statement, Statement::MaterializedView(_)))
    );
}

#[test]
fn mv_visitor_and_folder_reach_the_query_and_nested_names() {
    struct Count {
        identifiers: usize,
    }

    impl Visit for Count {
        fn visit_ident(&mut self, _: &novarocks_parser::ast::Ident) {
            self.identifiers += 1;
        }
    }

    struct Rename;

    impl Fold for Rename {
        fn fold_ident(
            &mut self,
            mut ident: novarocks_parser::ast::Ident,
        ) -> novarocks_parser::ast::Ident {
            if ident.value == "orders_mv" {
                ident.value = "renamed_mv".to_owned();
            }
            ident
        }
    }

    let [statement] = parse(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH (id) BUCKETS 1 \
         AS SELECT id FROM orders",
    )
    .expect("MV should parse")
    .try_into()
    .expect("one statement");
    let mut count = Count { identifiers: 0 };
    count.visit_statement(&statement);
    assert!(count.identifiers >= 3);

    let renamed = Rename.fold_statement(statement);
    assert_eq!(
        print_statements(&[renamed]),
        "CREATE MATERIALIZED VIEW renamed_mv DISTRIBUTED BY HASH (id) BUCKETS 1 \
         AS SELECT id FROM orders"
    );
}

#[test]
fn mv_drift_corpus_rejections_remain_parser_domain_failures() {
    for source in [
        "CREATE MATERIALIZED VIEW reject_mv AS SELECT 1",
        "CREATE MATERIALIZED VIEW reject_mv DISTRIBUTED BY HASH (k1) AS SELECT 1",
        "DROP MATERIALIZED VIEW reject_mv FORCE",
        "ALTER MATERIALIZED VIEW reject_mv SET FOO",
        "REFRESH MATERIALIZED VIEW reject_mv WITH BROKEN",
        "SHOW MATERIALIZED VIEWS LIKE 'reject%'",
        "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW reject_mv",
        "CREATE MATERIALIZED VIEW reject_mv DISTRIBUTED BY HASH (k1) BUCKETS 1 PRIMARY KEY () AS SELECT k1 FROM source_table",
    ] {
        let error = parse(source).expect_err(source);
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}

#[test]
fn empty_primary_key_keeps_the_command_specific_diagnostic() {
    let source = "CREATE MATERIALIZED VIEW reject_mv DISTRIBUTED BY HASH (k1) BUCKETS 1 PRIMARY KEY () AS SELECT k1 FROM source_table";
    let error = parse(source)
        .expect_err("empty PRIMARY KEY must fail")
        .to_user_error(source)
        .to_string();
    assert!(
        error.contains("PRIMARY KEY clause requires at least one column"),
        "{error}"
    );
}

#[test]
fn show_materialized_views_is_retired_syntax() {
    for source in [
        "SHOW MATERIALIZED VIEWS",
        "SHOW MATERIALIZED VIEWS FROM analytics",
    ] {
        assert_eq!(
            parse(source)
                .unwrap_err()
                .to_user_error(source)
                .code()
                .as_str(),
            "sql.parse.unexpected_token"
        );
    }
}
