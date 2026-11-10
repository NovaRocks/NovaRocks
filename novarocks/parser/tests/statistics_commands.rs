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
    let source = "analyze full table `ice`.db.t (`a`, b) with sync mode; SHOW ANALYZE JOBS; KILL ANALYZE 018f8c30-8a95-7b4e-b515-4da6f2aeb419; SHOW TABLE STATS db.t; DROP STATS db.t; DROP HISTOGRAM ON db.t (`a`, b); DROP MULTIPLE COLUMNS STATS db.t";
    let statements = parse(source).expect("statistics corpus parse");
    assert!(matches!(
        statements[0],
        Statement::Statistics(StatisticsStatement::AnalyzeTable(_))
    ));
    assert!(matches!(
        statements[1],
        Statement::Statistics(StatisticsStatement::ShowAnalyzeJobs(_))
    ));
    assert!(matches!(
        statements[2],
        Statement::Statistics(StatisticsStatement::CancelAnalyze(_))
    ));
    assert!(matches!(
        statements[3],
        Statement::Statistics(StatisticsStatement::ShowTableStats(_))
    ));
    assert!(matches!(
        statements[4],
        Statement::Statistics(StatisticsStatement::DropStats(_))
    ));
    assert!(matches!(
        statements[5],
        Statement::Statistics(StatisticsStatement::DropHistogram(_))
    ));
    assert!(matches!(
        statements[6],
        Statement::Statistics(StatisticsStatement::DropMultipleColumnsStats(_))
    ));
    let printed = print_statements(&statements);
    let reparsed = parse(&printed).expect("printed statistics corpus parse");
    assert_eq!(print_statements(&reparsed), printed);
}

#[test]
fn malformed_statistics_forms_have_parse_errors() {
    let error = parse("ANALYZE TABLE t (a,").expect_err("unterminated columns");
    assert_eq!(
        error.to_user_error("ANALYZE TABLE t (a,").code().as_str(),
        "sql.parse.unexpected_token"
    );
    for source in [
        "SHOW BASIC STATS",
        "SHOW BASIC STATS META",
        "SHOW HISTOGRAM STATS META",
    ] {
        let error = parse(source).expect_err("retired metadata command must fail in parsing");
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token"
        );
    }
}
