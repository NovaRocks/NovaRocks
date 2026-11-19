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

//! Original SQL metadata and root reachability for the F3 JSON_OBJECT cases.

use arrow::datatypes::{DataType, Field};
use novarocks_functions::FunctionArgumentType;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use serde_json::{Value, json};

use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;

// Only the declared input schema is needed by this SQL metadata test. The
// original five-row fixture's values are not evaluated to author these facts.
fn declared_fields() -> Vec<Field> {
    [
        ("id", DataType::Int32),
        ("k", DataType::Utf8),
        ("v", DataType::Utf8),
        ("n", DataType::Int64),
        ("flag", DataType::Boolean),
        ("day", DataType::Date32),
        ("amount", DataType::Decimal128(18, 4)),
    ]
    .into_iter()
    .map(|(name, data_type)| Field::new(name, data_type, true))
    .collect()
}

fn type_metadata(ty: &FunctionValueType) -> Value {
    let field = match ty.data_type() {
        DataType::List(field) => {
            json!({"name":field.name(), "nullable":field.is_nullable(), "data_type":format!("{:?}",field.data_type()), "metadata":field.metadata()})
        }
        _ => Value::Null,
    };
    json!({"physical_type":format!("{:?}",ty.data_type()),"nullable":ty.nullable(),"logical_type":format!("{:?}",ty.logical_type()),"field":field})
}
const ORIGINAL_SQL: &[(usize, &str)] = &[
    (
        4,
        r#"SELECT id, json_object(k: v, 'n': n, 'b': flag, 'day': day, 'amount': amount, 'dup': v, 'dup': n) AS object_value
FROM t ORDER BY id;"#,
    ),
    (
        5,
        r#"SELECT id, json_object(k: NULL) AS odd_tail FROM t ORDER BY id;"#,
    ),
    (
        6,
        r#"SELECT id, json_object(k: [id, n]) AS debug_value FROM t ORDER BY id;"#,
    ),
    (
        7,
        r#"SELECT id, json_object(k: v, 'n': n) AS selected_value FROM t WHERE id IN (1, 3, 5) ORDER BY id;"#,
    ),
    (
        8,
        r#"SELECT id, json_object(k: v) AS empty_value FROM t WHERE id < 0 ORDER BY id;"#,
    ),
    (
        9,
        r#"SELECT json_object(IF(id = 2, NULL, [id, n]): n) AS complex_key FROM t WHERE id = 1;"#,
    ),
    (
        10,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]): n) AS unused_error FROM t) s ORDER BY id;"#,
    ),
    (
        11,
        r#"SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]): n) AS unused_error FROM t) s WHERE id = 2 ORDER BY id;"#,
    ),
];

fn ancestors(
    plan: &novarocks_physical_plan::PhysicalPlan,
    start: (
        novarocks_physical_plan::FragmentId,
        novarocks_physical_plan::NodeId,
    ),
) -> std::collections::BTreeSet<(
    novarocks_physical_plan::FragmentId,
    novarocks_physical_plan::NodeId,
)> {
    use novarocks_physical_plan::NodeKind;
    let mut seen = std::collections::BTreeSet::new();
    let mut pending = vec![start];
    while let Some((fragment, node)) = pending.pop() {
        if !seen.insert((fragment, node)) {
            continue;
        }
        let current = &plan.fragments()[&fragment].nodes()[&node];
        pending.extend(current.inputs.iter().map(|id| (fragment, *id)));
        if let NodeKind::ExchangeSource { edge, .. } = &current.kind {
            let source = plan.edges()[edge].source.fragment;
            pending.push((source, plan.fragments()[&source].root()));
        }
    }
    seen
}
#[test]
fn json_object_legacy_native_oracle_original_sql_metadata_and_error_preservation() {
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::NodeKind;

    let fields = declared_fields();
    let mut records = Vec::new();
    for (query, sql) in ORIGINAL_SQL {
        let source = sql_source_with_declared_fields(sql, &fields);
        let plan = source.plan();
        let result = plan.result_port().unwrap();
        let live = ancestors(plan, (result.fragment, result.output.node));
        let mut owners = Vec::new();
        let mut call_metadata = Vec::new();
        for fragment in plan.fragments().values() {
            for (_source_node, unit, id, expr) in super::sql_test_source::unit_definitions(fragment)
            {
                let ExprKind::FunctionCall { function, .. } = &expr.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/json_object/v1" {
                    continue;
                }
                assert_eq!(expr.ty.logical_type(), ValueLogicalType::Json);
                assert_eq!(expr.ty.data_type(), &DataType::Utf8);
                assert_eq!(function.result_type, expr.ty);
                assert!(
                    live.contains(&(fragment.id(), _source_node.id)),
                    "original throwing use remains root-reachable"
                );
                let NodeKind::Project { expressions } = &fragment.nodes()[&_source_node.id].kind
                else {
                    panic!("original throwing projection owner")
                };
                assert!(
                    expressions
                        .iter()
                        .any(|(root, _)| unit.root(*root).unwrap().definition == *id)
                );
                call_metadata.push(json!({"result_type":type_metadata(&function.result_type),"argument_types":function.argument_types.iter().map(|a| match a { FunctionArgumentType::Value(v) => type_metadata(v), _ => panic!("original value channel") }).collect::<Vec<_>>()}));
                owners.push((fragment.id(), _source_node.id));
            }
        }
        assert!(!owners.is_empty(), "original JSON call is not pruned");
        if *query == 11 {
            let filters = live
                .iter()
                .filter(|(f, n)| {
                    matches!(plan.fragments()[f].nodes()[n].kind, NodeKind::Filter { .. })
                })
                .collect::<Vec<_>>();
            assert!(!filters.is_empty());
            assert!(
                filters.iter().any(|site| owners
                    .iter()
                    .all(|owner| ancestors(plan, **site).contains(owner))),
                "outer filter remains above the throwing project"
            );
            for (f, n) in &live {
                if let NodeKind::Scan {
                    relation,
                    residuals,
                    ..
                } = &plan.fragments()[f].nodes()[n].kind
                {
                    assert!(residuals.is_empty());
                    assert!(
                        relation.predicate_guarantees().is_empty(),
                        "no provider predicate hides a pushed filter"
                    );
                }
            }
        }
        let headers = result
            .fields
            .iter()
            .map(|f| f.alias.as_deref().unwrap_or(&f.name))
            .collect::<Vec<_>>();
        let expected: &[&str] = match query {
            4 => &["id", "object_value"],
            5 => &["id", "odd_tail"],
            6 => &["id", "debug_value"],
            7 => &["id", "selected_value"],
            8 => &["id", "empty_value"],
            9 => &["complex_key"],
            10 | 11 => &["id"],
            _ => unreachable!(),
        };
        assert_eq!(headers, expected);
        records.push(json!({"query":query,"sql":sql,"headers":headers,"fields":format!("{:?}",result.fields),"full_plan":format!("{plan:#?}"),"reachable_json_project_count":owners.len(),"call_metadata":call_metadata}));
    }
    if let Ok(path) = std::env::var("C16_ORIGINAL_SQL_OUT") {
        std::fs::write(path, serde_json::to_string_pretty(&json!({"mode":"CompiledPackage","fold":"NoOp original source fixture","queries":records})).unwrap()+"\n").unwrap();
    }
}
