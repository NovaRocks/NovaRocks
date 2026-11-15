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
    ast::{Statement, StatisticsStatement},
    parse,
    printer::print_statements,
};

#[test]
fn statistics_command_corpus_is_typed_and_round_trips() {
    let source =
        "analyze table `ice`.db.t (`a`, b); SHOW STATS FOR db.t; SHOW STATS FOR `ice`.`db`.`t`";
    let statements = parse(source).expect("statistics corpus parse");
    let Statement::Statistics(StatisticsStatement::AnalyzeTable(analyze)) = &statements[0] else {
        panic!("ANALYZE");
    };
    assert_eq!(
        analyze
            .columns
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let Statement::Statistics(StatisticsStatement::ShowStats(show)) = &statements[1] else {
        panic!("SHOW STATS");
    };
    assert_eq!(
        show.name
            .parts
            .iter()
            .map(|value| value.value.as_str())
            .collect::<Vec<_>>(),
        vec!["db", "t"]
    );
    let printed = print_statements(&statements);
    assert_eq!(
        printed,
        "ANALYZE TABLE `ice`.db.t (`a`, b); SHOW STATS FOR db.t; SHOW STATS FOR `ice`.`db`.`t`"
    );
    assert_eq!(print_statements(&parse(&printed).unwrap()), printed);
}

#[test]
fn retired_statistics_forms_are_syntax_errors() {
    for source in [
        "SHOW ANALYZE",
        "SHOW ANALYZE JOBS",
        "CANCEL ANALYZE 1",
        "KILL ANALYZE 1",
        "SHOW TABLE STATS t",
        "DROP STATS t",
        "DROP HISTOGRAM ON t (c)",
        "DROP MULTIPLE COLUMNS STATS t",
        "ANALYZE FULL TABLE t",
        "ANALYZE SAMPLE TABLE t",
        "ANALYZE TABLE t WITH SYNC MODE",
        "SHOW BASIC STATS META",
        "SHOW HISTOGRAM STATS META",
    ] {
        assert_eq!(
            parse(source)
                .unwrap_err()
                .to_user_error(source)
                .code()
                .as_str(),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}
#[test]
fn malformed_statistics_forms_have_parse_errors() {
    for source in ["ANALYZE TABLE t (a,", "SHOW STATS t", "SHOW STATS FOR"] {
        assert_eq!(
            parse(source)
                .unwrap_err()
                .to_user_error(source)
                .code()
                .as_str(),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}
#[test]
fn show_stats_subqueries_are_explicitly_unsupported() {
    let source = "SHOW STATS FOR (SELECT 1)";
    assert_eq!(
        parse(source)
            .unwrap_err()
            .to_user_error(source)
            .code()
            .as_str(),
        "sql.parse.unsupported_statement"
    );
}
