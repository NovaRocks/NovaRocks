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

//! SQL JSON_OBJECT result metadata and throwing projection reachability.

use arrow::datatypes::{DataType, Field};
use novarocks_functions::{FunctionArgumentType, builtin::json_sql::JsonFunction};
use novarocks_type_contract::ValueLogicalType;

use crate::exec::expr::sql_test_source::sql_source_with_declared_fields;

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

const SQL_CASES: &[(usize, &str)] = &[
    (
        4,
        r#"SELECT id, JSON_OBJECT(k VALUE v, 'n' VALUE n, 'b' VALUE flag, 'day' VALUE day, 'amount' VALUE amount, 'dup' VALUE v, 'dup' VALUE n RETURNING JSON) AS object_value
FROM t ORDER BY id;"#,
    ),
    (
        5,
        r#"SELECT id, JSON_OBJECT(k VALUE NULL) AS odd_tail FROM t ORDER BY id;"#,
    ),
    (
        6,
        r#"SELECT id, JSON_OBJECT(k VALUE CAST(ARRAY[id, n] AS JSON) FORMAT JSON RETURNING JSON) AS debug_value FROM t ORDER BY id;"#,
    ),
    (
        7,
        r#"SELECT id, JSON_OBJECT(k VALUE v, 'n' VALUE n RETURNING JSON) AS selected_value FROM t WHERE id IN (1, 3, 5) ORDER BY id;"#,
    ),
    (
        8,
        r#"SELECT id, JSON_OBJECT(k VALUE v RETURNING JSON) AS empty_value FROM t WHERE id < 0 ORDER BY id;"#,
    ),
    (
        9,
        r#"SELECT JSON_OBJECT(IF(id = 2, NULL, k) VALUE n RETURNING JSON) AS nullable_key FROM t WHERE id = 2;"#,
    ),
    (
        10,
        r#"SELECT id FROM (SELECT id, JSON_OBJECT(IF(id = 2, NULL, k) VALUE n RETURNING JSON) AS unused_error FROM t) s ORDER BY id;"#,
    ),
    (
        11,
        r#"SELECT id FROM (SELECT id, JSON_OBJECT(IF(id = 2, NULL, k) VALUE n RETURNING JSON) AS unused_error FROM t) s WHERE id = 2 ORDER BY id;"#,
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
fn json_object_native_sql_metadata_and_error_preservation() {
    use novarocks_expression_contract::ExprKind;
    use novarocks_physical_plan::{NodeKind, ResultValueDomain};

    let fields = declared_fields();
    let object = JsonFunction::Object {
        null_on_null: true,
        unique: false,
    }
    .function_id();
    for (query, sql) in SQL_CASES {
        let source = sql_source_with_declared_fields(sql, &fields);
        let plan = source.plan();
        let result = plan.result_port().unwrap();
        let live = ancestors(plan, (result.fragment, result.output.node));
        let mut owners = Vec::new();
        for fragment in plan.fragments().values() {
            for (_source_node, unit, id, expr) in super::sql_test_source::unit_definitions(fragment)
            {
                let ExprKind::FunctionCall { function, .. } = &expr.kind else {
                    continue;
                };
                if function.function_id != object {
                    continue;
                }
                assert_eq!(expr.ty.logical_type(), ValueLogicalType::Json);
                assert_eq!(expr.ty.data_type(), &DataType::LargeBinary);
                assert_eq!(function.result_type, expr.ty);
                let arguments = &function.argument_types;
                let (keys, values) = arguments.split_at(arguments.len() / 2);
                assert!(!keys.is_empty());
                assert_eq!(keys.len(), values.len());
                for key in keys {
                    let FunctionArgumentType::Value(key) = key else {
                        panic!("expected character key value");
                    };
                    assert_eq!(key.data_type(), &DataType::Utf8);
                }
                for value in values {
                    let FunctionArgumentType::Value(value) = value else {
                        panic!("expected JSON member value");
                    };
                    assert_eq!(value.logical_type(), ValueLogicalType::Json);
                    assert_eq!(value.data_type(), &DataType::LargeBinary);
                }
                assert!(
                    live.contains(&(fragment.id(), _source_node.id)),
                    "throwing JSON call remains root-reachable"
                );
                let NodeKind::Project { expressions } = &fragment.nodes()[&_source_node.id].kind
                else {
                    panic!("expected JSON projection owner")
                };
                assert!(expressions.iter().any(|(root, _)| {
                    let uses = unit.control_flow().uses();
                    let mut pending = vec![unit.root_use(*root).unwrap()];
                    while let Some(use_id) = pending.pop() {
                        let occurrence = &uses[&use_id];
                        if occurrence.definition == *id {
                            return true;
                        }
                        pending.extend(occurrence.arguments.iter().copied());
                    }
                    false
                }));
                owners.push((fragment.id(), _source_node.id));
            }
        }
        assert!(!owners.is_empty(), "throwing JSON projection must remain");
        if *query == 11 {
            let scans = live
                .iter()
                .copied()
                .filter(|(fragment, node)| {
                    let NodeKind::Scan {
                        relation,
                        residuals,
                        ..
                    } = &plan.fragments()[fragment].nodes()[node].kind
                    else {
                        return false;
                    };
                    residuals.len() == 1
                        || matches!(
                            relation.predicate_guarantees(),
                            [guarantee] if guarantee.kind == novarocks_physical_plan::PredicateGuaranteeKind::Exact
                        )
                })
                .collect::<Vec<_>>();
            let [scan] = scans.as_slice() else {
                panic!("one Scan applies the necessary id predicate");
            };
            assert!(
                owners
                    .iter()
                    .all(|owner| ancestors(plan, *owner).contains(scan))
            );
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
            9 => &["nullable_key"],
            10 | 11 => &["id"],
            _ => unreachable!(),
        };
        assert_eq!(headers, expected);
        let field = result.fields.last().unwrap();
        match query {
            4 | 6 | 7 | 8 | 9 => {
                assert_eq!(field.domain, ResultValueDomain::Json);
                assert_eq!(field.ty.logical_type(), ValueLogicalType::Json);
                assert_eq!(field.ty.data_type(), &DataType::LargeBinary);
            }
            5 => {
                assert_eq!(field.domain, ResultValueDomain::Plain);
                assert_eq!(field.ty.data_type(), &DataType::Utf8);
            }
            _ => {}
        }
    }
}
