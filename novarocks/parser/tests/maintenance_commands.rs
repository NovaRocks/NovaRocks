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
    ParserError,
    ast::{MaintenanceStatement, Statement},
    parse,
    printer::print_statements,
};

fn roundtrip(source: &str) -> String {
    let statements = parse(source).expect("maintenance statement should parse");
    let printed = print_statements(&statements);
    let reparsed = parse(&printed).expect("printed maintenance statement should parse");
    assert_eq!(print_statements(&reparsed), printed);
    printed
}

fn parse_error_code(source: &str) -> String {
    let error = parse(source).expect_err("owned malformed maintenance input must fail");
    assert!(matches!(error, ParserError::Parse(_)));
    error.to_user_error(source).code().as_str().to_owned()
}

fn parse_error_message(source: &str) -> String {
    parse(source)
        .expect_err("owned malformed maintenance input must fail")
        .to_user_error(source)
        .to_string()
}

#[test]
fn generic_call_retains_named_args_maps_and_timestamp_literals() {
    let source = "call `ice`.system.rewrite_manifests(\
        `table` => 'db.orders',\
        options => map('rewrite-all', 'true'),\
        older_than => timestamp '2026-01-01 00:00:00',\
        where => 'id > 10')";
    let statements = parse(source).expect("generic CALL should parse without procedure admission");
    let [Statement::Maintenance(MaintenanceStatement::Call(call))] = statements.as_slice() else {
        panic!("expected CALL maintenance statement");
    };
    assert_eq!(call.procedure.parts.len(), 3);
    assert_eq!(call.arguments.len(), 4);
    assert!(
        call.arguments
            .iter()
            .all(|argument| argument.name.is_some())
    );
    assert_eq!(
        roundtrip(source),
        "CALL `ice`.system.rewrite_manifests(`table` => 'db.orders', options => MAP('rewrite-all', 'true'), older_than => TIMESTAMP '2026-01-01 00:00:00', where => 'id > 10')"
    );
}

#[test]
fn generic_call_allows_positional_args_without_procedure_support_lookup() {
    assert_eq!(
        roundtrip("CALL unknown.admin.any_procedure('db.orders', FALSE, NULL)"),
        "CALL unknown.admin.any_procedure('db.orders', FALSE, NULL)"
    );
}

#[test]
fn alter_table_maintenance_forms_roundtrip() {
    let cases = [
        (
            "ALTER TABLE `ice`.`db`.`orders` OPTIMIZE;",
            "ALTER TABLE `ice`.`db`.`orders` OPTIMIZE",
        ),
        (
            "ALTER TABLE ice.db.orders REWRITE MANIFESTS",
            "ALTER TABLE ice.db.orders REWRITE MANIFESTS",
        ),
        (
            "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS RETAIN LAST 3 OLDER THAN '2026-01-01T00:00:00Z'",
            "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS RETAIN LAST 3 OLDER THAN '2026-01-01T00:00:00Z'",
        ),
        (
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN 1700000000000",
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN 1700000000000",
        ),
    ];

    for (source, expected) in cases {
        assert_eq!(roundtrip(source), expected);
    }
}

#[test]
fn show_alter_table_optimize_is_retired_syntax() {
    let source = "SHOW ALTER TABLE OPTIMIZE FROM ice.analytics WHERE TableName = 'orders'";
    assert_eq!(
        parse(source)
            .unwrap_err()
            .to_user_error(source)
            .code()
            .as_str(),
        "sql.parse.unexpected_token"
    );
}

#[test]
fn malformed_owned_forms_have_stable_typed_parse_errors() {
    for source in [
        "CALL ice.system.rewrite_manifests(table => 'db.orders', 'extra')",
        "CALL ice.system.rewrite_manifests(table => 'db.orders', TABLE => 'db.other')",
        "CALL ice.system.rewrite_manifests(options => MAP('only-a-key'))",
        "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS",
        "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS OLDER THAN 1 OLDER THAN 2",
        "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES",
        "SHOW ALTER TABLE OPTIMIZE WHERE TableName 'orders'",
    ] {
        assert_eq!(
            parse_error_code(source),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}

#[test]
fn malformed_maintenance_forms_keep_command_specific_diagnostics() {
    for (source, expected) in [
        (
            "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS",
            "EXPIRE SNAPSHOTS requires at least",
        ),
        (
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES",
            "REMOVE ORPHAN FILES requires OLDER THAN",
        ),
        (
            "ALTER TABLE ice.db.orders REWRITE MANIFESTS WHERE size_in_bytes < 100",
            "REWRITE MANIFESTS without unsupported trailing clauses",
        ),
        (
            "ALTER TABLE ice.db.orders EXPIRE SNAPSHOTS OLDER THAN 1 OLDER THAN 2",
            "duplicate OLDER THAN clause",
        ),
        (
            "ALTER TABLE ice.db.orders REMOVE ORPHAN FILES OLDER THAN 1 WHERE size_in_bytes < 100",
            "REMOVE ORPHAN FILES without unsupported trailing clauses",
        ),
    ] {
        let error = parse_error_message(source);
        assert!(error.contains(expected), "{source}: {error}");
    }
}
