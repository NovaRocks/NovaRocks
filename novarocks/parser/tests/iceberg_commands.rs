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
    ParseError, ParserError, Span,
    ast::{DmlStatement, Ident, Literal, LiteralKind, Statement, Visit},
    parse,
    printer::print_statement,
};

fn statement(sql: &str) -> Statement {
    let mut statements = parse(sql).expect("Iceberg command should parse");
    assert_eq!(statements.len(), 1);
    statements.remove(0)
}

#[test]
fn parses_schema_properties_partition_references_and_add_files() {
    let schema = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city",
    );
    assert!(matches!(schema, Statement::Iceberg(_)));

    let properties = statement(
        "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('write.format.default' = 'parquet')",
    );
    assert!(matches!(properties, Statement::Iceberg(_)));

    let partition = statement("ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32)");
    assert!(matches!(partition, Statement::Iceberg(_)));

    let identity_partition =
        statement("ALTER TABLE ice.db.orders ADD PARTITION COLUMN identity(user_id)");
    assert!(matches!(identity_partition, Statement::Iceberg(_)));

    let reference =
        statement("ALTER TABLE ice.db.orders CREATE OR REPLACE BRANCH dev AS OF VERSION 42");
    assert!(matches!(reference, Statement::Iceberg(_)));

    let files = statement("ALTER TABLE ice.db.orders ADD FILES FROM 's3://warehouse/staged'");
    assert!(matches!(files, Statement::Iceberg(_)));
}

#[test]
fn schema_default_preserves_negative_numeric_literal() {
    let parsed = statement("ALTER TABLE ice.db.orders ADD COLUMN offset INT DEFAULT -7");
    assert_eq!(
        print_statement(&parsed),
        "ALTER TABLE ice.db.orders ADD COLUMN offset INT DEFAULT -7"
    );
}

#[test]
fn schema_default_checks_the_complete_signed_i64_literal() {
    struct Defaults(Vec<Literal>);
    impl Visit for Defaults {
        fn visit_literal(&mut self, literal: &Literal) {
            self.0.push(literal.clone());
        }
    }
    for (spelling, expected) in [
        ("-9223372036854775808", "-9223372036854775808"),
        ("9223372036854775807", "9223372036854775807"),
        (
            "- /* minimum */ 9223372036854775808",
            "-9223372036854775808",
        ),
    ] {
        let source = format!("ALTER TABLE t ADD COLUMN value BIGINT DEFAULT {spelling}");
        let parsed = statement(&source);
        let mut defaults = Defaults(Vec::new());
        defaults.visit_statement(&parsed);
        assert_eq!(defaults.0.len(), 1);
        assert_eq!(defaults.0[0].kind, LiteralKind::Number(expected.to_owned()));
        let start = source.find(spelling).unwrap();
        assert_eq!(defaults.0[0].span, Span::new(start, source.len()));
        let canonical = format!("ALTER TABLE t ADD COLUMN value BIGINT DEFAULT {expected}");
        assert_eq!(print_statement(&parsed), canonical);
        assert_eq!(print_statement(&statement(&canonical)), canonical);
    }
}

#[test]
fn schema_default_rejects_out_of_range_integer_with_original_span() {
    for spelling in ["9223372036854775808", "-9223372036854775809"] {
        let source = format!("ALTER TABLE t ADD COLUMN value BIGINT DEFAULT {spelling}");
        let error = parse(&source).expect_err("ordinary integer must fit signed i64");
        assert_eq!(
            error,
            ParserError::Parse(ParseError::UnexpectedToken {
                expected: "signed 64-bit integer literal",
                found: format!("`{spelling}`"),
                span: Span::new(source.find(spelling).unwrap(), source.len()),
            })
        );
        assert_eq!(
            error.to_user_error(&source).code().as_str(),
            "sql.parse.unexpected_token"
        );
    }
}

#[test]
fn quoted_identifiers_case_and_semicolon_round_trip_canonically() {
    let source = "alter table `ice`.`db`.`orders` add partition column TrUnCaTe(`user.id`, 16);";
    let parsed = statement(source);
    let canonical = print_statement(&parsed);
    assert_eq!(
        canonical,
        "ALTER TABLE `ice`.`db`.`orders` ADD PARTITION COLUMN truncate(`user.id`, 16)"
    );
    assert_eq!(print_statement(&statement(&canonical)), canonical);
}

#[test]
fn schema_types_preserve_parameterized_and_nested_shapes() {
    let parsed = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name VARCHAR(32), attrs MAP<STRING, ARRAY<DECIMAL(12, 3)>>>",
    );
    let canonical = print_statement(&parsed);
    assert_eq!(
        canonical,
        "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name VARCHAR(32), attrs MAP<STRING, ARRAY<DECIMAL(12, 3)>>>"
    );
    assert_eq!(print_statement(&statement(&canonical)), canonical);
}

#[test]
fn malformed_owned_iceberg_commands_are_typed_errors() {
    for sql in [
        "ALTER TABLE t ADD FILES FROM not_a_string",
        "ALTER TABLE t ADD PARTITION COLUMN bucket(id)",
        "ALTER TABLE t CREATE BRANCH dev AS OF VERSION",
        "ALTER TABLE t UNSET TBLPROPERTIES ()",
    ] {
        let error = parse(sql).expect_err("owned malformed command must fail");
        assert_eq!(
            error.to_user_error(sql).code().as_str(),
            "sql.parse.unexpected_token",
            "{sql}"
        );
    }
}

#[test]
fn equality_delete_is_owned_by_the_dml_grammar() {
    let sql = "ALTER TABLE t ADD EQUALITY DELETE (id) VALUES (1)";
    assert!(matches!(
        statement(sql),
        Statement::Dml(DmlStatement::AddEqualityDelete(_))
    ));
}

#[test]
fn visitor_reaches_iceberg_nested_syntax() {
    struct CountVisitor(usize);

    impl Visit for CountVisitor {
        fn visit_ident(&mut self, _: &Ident) {
            self.0 += 1;
        }

        fn visit_literal(&mut self, _: &Literal) {
            self.0 += 1;
        }
    }

    let statement = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city",
    );
    let mut visitor = CountVisitor(0);
    visitor.visit_statement(&statement);
    assert_eq!(visitor.0, 9);
}
