// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

mod optimize;

use super::super::property_evaluation::DdlPropertyConstantError;
use super::*;
use novarocks_constant_contract::ConstantValue;
use novarocks_parser::parser::parse_expression;
use novarocks_sql::compiler::{RequiredConstantTarget, SqlCompileControl};

struct UnreachedEvaluation;
impl DdlPropertyConstantPort for UnreachedEvaluation {
    fn evaluate(
        &mut self,
        _: &Expr,
        _: RequiredConstantTarget,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        panic!("argument shape must be refused before evaluating its value")
    }
}
#[test]
fn table_procedure_argument_shape_preserves_pin_error_identities() {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    for (sql, expected) in [
        (
            "expire_snapshots(1, retain_last => 2)",
            "sql.admit.invalid_table_procedure_arguments",
        ),
        (
            "optimize_manifests(1)",
            "sql.admit.invalid_table_procedure_arguments",
        ),
        (
            "expire_snapshots(retain_last => 1, RETAIN_LAST => 2)",
            "sql.admit.duplicate_table_procedure_argument",
        ),
        (
            "expire_snapshots(unknown => 1)",
            "sql.admit.invalid_table_procedure_argument",
        ),
    ] {
        // These are real shared CallArgument nodes from the Trino parser.
        // The statement-level TableExecute producer is a separate consumer.
        let Expr::FunctionCall(call) = parse_expression(sql).unwrap() else {
            panic!("procedure argument syntax")
        };
        let procedure =
            resolve_table_procedure(&provider, call.name.parts.last().unwrap()).unwrap();
        let args = call
            .arguments
            .iter()
            .map(TableProcedureInputArgument::from)
            .collect::<Vec<_>>();
        let error = bind_arguments(
            procedure,
            &args,
            TableProcedureBindingSource {
                sql,
                catalog: "iceberg",
                statement_span: call.span,
            },
            &mut UnreachedEvaluation,
            &SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        let user = error.user_error().unwrap();
        assert_eq!(user.code().as_str(), expected, "{sql}");
        assert!(user.location().is_some());
    }
    let Expr::Identifier(name) = parse_expression("missing_procedure").unwrap() else {
        panic!("real procedure identifier");
    };
    let error = resolve_table_procedure(&provider, &name).unwrap_err();
    let user = error.user_error().unwrap();
    assert_eq!(user.code().as_str(), "sql.admit.table_procedure_not_found");
    assert!(user.location().is_none());
}

#[test]
fn equality_delete_resolution_matches_the_build_profile() {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let Expr::Identifier(name) = parse_expression("add_equality_delete").unwrap() else {
        panic!("real procedure name");
    };
    let result = resolve_table_procedure(&provider, &name);
    #[cfg(novarocks_dev_controls)]
    assert!(result.unwrap().dev_only());
    #[cfg(not(novarocks_dev_controls))]
    assert_eq!(
        result.unwrap_err().user_error().unwrap().code().as_str(),
        "sql.admit.table_procedure_not_found"
    );
}
