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
    Span,
    ast::{Fold, Statement, ViewStatement, Visit},
    parse,
    printer::print_statements,
};

#[test]
fn create_view_owns_a_typed_embedded_query_and_span() {
    let source = "  CREATE OR REPLACE VIEW analytics.orders_v \
        COMMENT 'daily' AS /* outside query */ SELECT order_id /* inside */ AS order_id, order_day AS `order day`\nFROM orders  /* trailing */ ;";
    let statements = parse(source).expect("CREATE VIEW should parse");
    let [Statement::View(ViewStatement::Create(create))] = statements.as_slice() else {
        panic!("expected typed CREATE VIEW");
    };

    assert!(create.or_replace);
    assert!(!create.if_not_exists);
    let novarocks_parser::ast::SetExpr::Select(select) = create.query.body.as_ref() else {
        panic!("SELECT view definition")
    };
    assert_eq!(select.projection.len(), 2);
    assert_eq!(
        novarocks_parser::printer::print_query(&create.query),
        "SELECT order_id AS order_id, order_day AS `order day` FROM orders"
    );
    let query_start = source.find("SELECT").expect("query start");
    let query_end = source.find("  /* trailing */").expect("query end");
    assert_eq!(create.query.span, Span::new(query_start, query_end));
    assert_eq!(
        print_statements(&[Statement::View(ViewStatement::Create(create.clone()))]),
        "CREATE OR REPLACE VIEW analytics.orders_v COMMENT 'daily' AS \
         SELECT order_id AS order_id, order_day AS `order day` FROM orders"
    );
}

#[test]
fn view_command_family_round_trips_through_the_typed_ast() {
    let source = "CREATE VIEW IF NOT EXISTS analytics.orders_v AS SELECT order_id FROM orders; \
        DROP VIEW IF EXISTS analytics.orders_v; SHOW VIEWS FROM analytics; \
        SHOW CREATE VIEW analytics.orders_v";
    let statements = parse(source).expect("view family should parse");
    assert_eq!(statements.len(), 4);
    assert!(matches!(
        statements[0],
        Statement::View(ViewStatement::Create(_))
    ));
    assert!(matches!(
        statements[1],
        Statement::View(ViewStatement::Drop(_))
    ));
    assert!(matches!(
        statements[2],
        Statement::View(ViewStatement::Show(_))
    ));
    assert!(matches!(
        statements[3],
        Statement::View(ViewStatement::ShowCreate(_))
    ));
    let printed = print_statements(&statements);
    assert_eq!(
        printed,
        "CREATE VIEW IF NOT EXISTS analytics.orders_v AS SELECT order_id FROM orders; \
         DROP VIEW IF EXISTS analytics.orders_v; SHOW VIEWS FROM analytics; \
         SHOW CREATE VIEW analytics.orders_v"
    );
    let reparsed = parse(&printed).expect("printed views should parse");
    assert_eq!(reparsed, statements);
}

#[test]
fn view_visitor_and_folder_reach_query_and_nested_names() {
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
            if ident.value == "orders_v" {
                ident.value = "renamed_v".to_owned();
            }
            ident
        }
    }

    let [statement] = parse("CREATE VIEW orders_v COMMENT 'v' AS SELECT id AS named FROM orders")
        .expect("view should parse")
        .try_into()
        .expect("one statement");
    let mut count = Count { identifiers: 0 };
    count.visit_statement(&statement);
    assert!(count.identifiers >= 4);

    let renamed = Rename.fold_statement(statement);
    assert_eq!(
        print_statements(&[renamed]),
        "CREATE VIEW renamed_v COMMENT 'v' AS SELECT id AS named FROM orders"
    );
}

#[test]
fn view_drift_corpus_rejections_remain_parser_domain_failures() {
    for source in [
        "CREATE VIEW broken",
        "CREATE OR REPLACE VIEW IF NOT EXISTS broken AS SELECT 1",
        "CREATE VIEW broken () AS SELECT 1",
        "DROP VIEW broken FORCE",
        "SHOW VIEWS LIKE 'broken%'",
        "SHOW VIEWS WHERE name = 'broken'",
        "SHOW CREATE VIEW",
    ] {
        let error = parse(source).expect_err(source);
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}
