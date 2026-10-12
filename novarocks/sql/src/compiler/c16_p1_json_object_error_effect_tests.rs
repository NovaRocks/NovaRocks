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

//! Real SQL binding and optimizer rules preserve whole-call Data in both modes.
use super::*;
use crate::optimizer::{OptimizedOperatorNode, operator::Operator, scalar::ScalarNode};
use arrow::datatypes::DataType;
use novarocks_functions::PureKernelAbi;
use novarocks_type_contract::FunctionIntrinsicRowError;

fn compile(sql: &str, mode: SqlPhysicalEmissionMode) -> SqlOptimizedOutput {
    compile_with_id_type(sql, mode, DataType::Int64)
}

fn compile_with_id_type(
    sql: &str,
    mode: SqlPhysicalEmissionMode,
    id_type: DataType,
) -> SqlOptimizedOutput {
    let source = crate::planner::table::test_sql_scan_source(
        crate::planner::table::SqlScanKind::ConnectorRead,
    );
    let crate::planner::table::ScanSource::Sql(scan) = &source else {
        unreachable!()
    };
    let statistics = crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
        crate::planning::dml::DmlStatisticsEvidence::Missing {
            binding: scan.binding,
            label: format!(
                "{}.{}.{}",
                scan.table.catalog, scan.table.namespace, scan.table.table
            ),
            reason: "test fixture has no published statistics".into(),
        },
    ]);
    let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
    catalog.create_database("db").unwrap();
    catalog
        .register(
            "db",
            crate::planner::table::TableDef {
                name: "t".into(),
                columns: ["id", "n"]
                    .into_iter()
                    .map(|name| novarocks_types::schema::ColumnDef {
                        name: name.into(),
                        data_type: if name == "id" {
                            id_type.clone()
                        } else {
                            DataType::Int64
                        },
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source,
            },
        )
        .unwrap();
    let snapshot = SqlPlannerTableSnapshot::new(&catalog);
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        &snapshot,
        crate::functions::builtin_sql_function_catalog(),
        noop_constant_evaluator(),
        None,
        crate::constant::test_constant_policy(),
        mode,
        control.clone(),
    ))
    .unwrap()
    .into_pending()
    .unwrap();
    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics, control))
        .unwrap()
        .into_optimized_output()
        .unwrap()
}

fn whole_data_project(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> bool {
    let Operator::PhysicalProject(project) = &node.op else {
        return false;
    };
    let arena = node.execution_props.scalar_arena.as_ref().unwrap();
    project.items.iter().any(|item| {
        let ScalarNode::FunctionCall { binding, .. } = arena.node(item.expr) else {
            return false;
        };
        if binding.function_id.as_str() != "builtin.scalar/json_object/v1" {
            return false;
        }
        assert!(binding.declared_scalar_invocation_data);
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            binding.admitted_execution_abi,
            match mode {
                SqlPhysicalEmissionMode::OriginalNativeV1 => None,
                SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration =>
                    Some(PureKernelAbi::ScalarInvocationV1),
            }
        );
        assert!(crate::optimizer::scalar_expr::can_fail(arena, item.expr));
        assert_eq!(arena.data_type(item.expr), &DataType::Utf8);
        true
    })
}
fn subtree_has_whole_data(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> bool {
    whole_data_project(node, mode)
        || node
            .children
            .iter()
            .any(|child| subtree_has_whole_data(child, mode))
}
fn count_preserved_filter(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> usize {
    let own = match &node.op {
        Operator::PhysicalFilter(_) => usize::from(
            node.children
                .iter()
                .any(|child| subtree_has_whole_data(child, mode)),
        ),
        Operator::PhysicalScan(scan) => {
            assert!(
                scan.predicates.is_empty(),
                "no predicate hides the throwing project"
            );
            0
        }
        _ => 0,
    };
    own + node
        .children
        .iter()
        .map(|child| count_preserved_filter(child, mode))
        .sum::<usize>()
}

#[test]
fn c16_p1_json_object_real_sql_pruning_and_filter_rules_preserve_whole_data_in_both_modes() {
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for filtered in [false, true] {
            let suffix = if filtered {
                "WHERE id = 2 ORDER BY id"
            } else {
                "ORDER BY id"
            };
            let sql = format!(
                "SELECT id FROM (SELECT id, json_object(IF(id = 2, NULL, [id, n]), n) AS unused_error FROM t) s {suffix}"
            );
            let output = compile(&sql, mode);
            assert!(
                subtree_has_whole_data(&output.optimized_tree, mode),
                "PruneProjectColumns must retain the unreferenced whole-call Data use"
            );
            if filtered {
                assert!(
                    count_preserved_filter(&output.optimized_tree, mode) > 0,
                    "PushFilterThroughProject must retain the outer filter above Data"
                );
            }
        }
    }
}

#[test]
fn c16_p1_json_object_native_nested_index_keeps_accurate_types_and_whole_data_in_both_modes() {
    fn check(node: &OptimizedOperatorNode, mode: SqlPhysicalEmissionMode) -> usize {
        let mut count = 0;
        if let Operator::PhysicalProject(project) = &node.op {
            let arena = node.execution_props.scalar_arena.as_ref().unwrap();
            for item in &project.items {
                let ScalarNode::FunctionCall { binding, args, .. } = arena.node(item.expr) else {
                    continue;
                };
                if binding.function_id.as_str() != "builtin.scalar/json_object/v1" {
                    continue;
                }
                assert!(whole_data_project(node, mode));
                let ScalarNode::FunctionCall {
                    binding,
                    args: access,
                    ..
                } = arena.node(args[0])
                else {
                    panic!("nested key's actual bound access");
                };
                assert_eq!(
                    binding.function_id.as_str(),
                    "builtin.scalar/__array_element_at/v1"
                );
                assert_eq!(access.len(), 2);
                let inner = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Int64,
                    true,
                )));
                assert_eq!(arena.data_type(args[0]), &inner);
                assert!(arena.nullable(args[0]));
                assert_eq!(
                    arena.data_type(access[0]),
                    &DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
                        "item", inner, true
                    )))
                );
                let ScalarNode::Cast { child, target, .. } = arena.node(access[1]) else {
                    panic!("native SQL index cast");
                };
                assert_eq!(target, &DataType::Int32);
                assert!(arena.nullable(access[1]));
                assert_eq!(arena.data_type(*child), &DataType::Int64);
                assert!(arena.nullable(*child));
                let ScalarNode::FunctionCall {
                    binding,
                    args: branches,
                    ..
                } = arena.node(*child)
                else {
                    panic!("primitive IF source");
                };
                assert_eq!(binding.function_id.as_str(), "builtin.scalar/if/v1");
                assert_eq!(branches.len(), 3);
                assert_eq!(arena.data_type(branches[0]), &DataType::Boolean);
                for (branch, nullable) in branches[1..].iter().zip([true, false]) {
                    assert_eq!(arena.data_type(*branch), &DataType::Int64);
                    assert_eq!(arena.nullable(*branch), nullable);
                }
                count += 1;
            }
        }
        count
            + node
                .children
                .iter()
                .map(|child| check(child, mode))
                .sum::<usize>()
    }
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for filtered in [false, true] {
            let suffix = if filtered {
                "WHERE id = 2 ORDER BY id"
            } else {
                "ORDER BY id"
            };
            let sql = format!(
                "SELECT id FROM (SELECT id, json_object([[id, n]][IF(id = 2, NULL, 1)], n) AS unused_error FROM t) s {suffix}"
            );
            let output = compile_with_id_type(&sql, mode, DataType::Int32);
            assert_eq!(check(&output.optimized_tree, mode), 1);
            if filtered {
                assert!(count_preserved_filter(&output.optimized_tree, mode) > 0);
            }
        }
    }
}
