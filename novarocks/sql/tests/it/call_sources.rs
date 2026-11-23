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
mod substring {
    //! The actual SQL case with no-op folding retains the original emitted call and request authors.
    //! Prior native PASS alone does not establish that a call survived real constant folding.
    use arrow::datatypes::DataType;
    use novarocks_sql_testkit::sql_source;
    use {
        novarocks_expression_contract::ExprKind,
        novarocks_physical_plan::{FunctionArgumentType, StaticFunctionArgument},
    };

    use novarocks_type_contract::{CharacterKind, CharacterType, ValueKind, ValueLogicalType};
    const SQL: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/project/sql/project_string_int_argument_semantics.sql"
    ));
    fn check() {
        let (_, queries) = SQL
            .split_once("-- query 1\n")
            .expect("successful query block");
        let (query, _) = queries
            .split_once("-- query 2\n")
            .expect("next query block");
        let query = query.trim().strip_suffix(';').expect("terminated query");
        let owner = sql_source(query, DataType::Int64);
        let mut count = 0;
        for fragment in owner.plan().fragments().values() {
            for (unit, node) in fragment
                .nodes()
                .values()
                .filter_map(|node| node.expressions.as_deref())
                .flat_map(|unit| unit.definitions().iter().map(move |node| (unit, node)))
            {
                let id = node.id;
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/substr/v1" {
                    continue;
                }
                let request = unit.requests().get(&id).unwrap();
                assert_eq!(args.len(), 3);
                assert_eq!(request.logical_argument_count, 3);
                assert_eq!(request.arguments.len(), 3);
                assert_eq!(function.argument_types.len(), 3);
                for (index, ((argument, selected), child)) in request
                    .arguments
                    .iter()
                    .zip(function.argument_types.iter())
                    .zip(args.iter())
                    .enumerate()
                {
                    let StaticFunctionArgument::Value { value_type, .. } = argument else {
                        panic!("real source value argument")
                    };
                    let FunctionArgumentType::Value(bound) = selected else {
                        panic!("real selected value argument")
                    };
                    assert_eq!(value_type, bound);
                    assert_eq!(bound.logical_type(), ValueLogicalType::Physical);
                    assert_eq!(
                        bound.data_type().clone(),
                        if index == 0 {
                            DataType::Utf8
                        } else {
                            DataType::Int64
                        }
                    );
                    assert_eq!(&unit.definition(*child).unwrap().ty, bound);
                }
                assert_eq!(node.ty, function.result_type);
                assert_eq!(
                    node.ty.value_type().kind(),
                    &ValueKind::BoundedCharacter(
                        CharacterType::new(CharacterKind::Varchar, 3).unwrap()
                    )
                );
                eprintln!(
                    "substr actual retained source mode=compiled fragment={:?} definition={id:?} signature={function:?} original_request={request:?} ordered_children={args:?}",
                    fragment.id()
                );
                count += 1;
            }
        }
        assert!(
            count == 3,
            "actual corpus SQL keeps a real function call with no-op fold"
        );
        eprintln!("substr actual retained call count mode=compiled count={count}");
    }

    #[test]
    fn substring_actual_candidate_sql_kept_call_source() {
        check()
    }
}
mod canonical_calls {
    use arrow::datatypes::DataType;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use novarocks_sql::compiler::{
        PackageSemanticsError, SqlCompileControl, check_pure_call_definitions_observed,
    };
    use novarocks_sql_testkit::sql_source;
    #[test]
    fn e08s1_late_actual_source_same_journal_supported_canonical_calls_and_over_are_accepted() {
        {
            let source = sql_source(
                "SELECT reverse(CAST(k AS VARCHAR)), count(k) OVER () FROM fixture",
                DataType::Int32,
            );
            check_pure_call_definitions_observed(&source, &SqlCompileControl::unbounded()).unwrap();
            let policy = novarocks_sql_testkit::sql_fixture_constant_policy();
            assert_eq!(source.plan().statement_constant_policy(), policy);
            source.require_statement_constant_policy(policy).unwrap();
            let mut foreign = policy;
            foreign.max_rows -= 1;
            let error = source
                .require_statement_constant_policy(foreign)
                .unwrap_err();
            assert!(matches!(
                error,
                PackageSemanticsError::StatementPolicyMismatch
            ));
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        }
    }
}

#[test]
fn deterministic_aggregate_filter_occurrences_share_the_original_update() {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::NodeKind;
    use novarocks_sql_testkit::sql_source;

    let authored = sql_source(
        "SELECT count(*) FILTER (WHERE k > 0) AS a,
                count(*) FILTER (WHERE k > 0) AS b FROM fixture.source",
        DataType::Int64,
    );
    let updates = authored
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| fragment.nodes().values())
        .filter_map(|node| match &node.kind {
            NodeKind::Aggregate { calls, .. } => Some(calls.iter()),
            _ => None,
        })
        .flatten()
        .filter(|call| call.binding.phase.consumes_logical_arguments())
        .collect::<Vec<_>>();
    assert_eq!(updates.len(), 1);
    assert!(updates[0].filter_root.is_some());
    assert!(updates[0].arguments.is_empty());
}
