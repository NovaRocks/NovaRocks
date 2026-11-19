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
//! Public SQL completion inputs for owner and compile-to-execute tests.
use crate::sql_source_with_single_field_and_core_residuals;
use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::ExprKind;
use novarocks_physical_plan::NodeKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;

/// Borrow definitions with their actual node and unit namespace; no table is rebuilt.
pub fn unit_definitions(
    fragment: &novarocks_physical_plan::Fragment,
) -> impl Iterator<
    Item = (
        &novarocks_physical_plan::PhysicalNode,
        &novarocks_expression_contract::BoundExprSet,
        &novarocks_expression_contract::ExprId,
        &novarocks_expression_contract::ExprNode,
    ),
> {
    fragment.nodes().values().flat_map(|node| {
        node.expressions.iter().flat_map(move |unit| {
            unit.definitions()
                .iter()
                .map(move |definition| (node, unit.as_ref(), &definition.id, definition))
        })
    })
}

/// Original single-column SQL author for the signed membership fixtures.
pub fn signed_inlist_source(sql: &str, dtype: DataType) -> SqlAuthoredPhysicalPlan {
    sql_source_with_single_field_and_core_residuals(sql, Field::new("k", dtype, true))
}

/// Check the original membership definitions and report their cardinality.
pub fn inspect_inlist_source(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut n = 0;
    for (&id, fragment) in source.plan().fragments() {
        for (_source_node, unit, _, node) in unit_definitions(fragment) {
            if let ExprKind::InList {
                expr,
                list,
                negated,
            } = &node.kind
            {
                n += 1;
                assert_eq!(node.ty.data_type().clone(), DataType::Boolean);
                let input = unit.definition(*expr).unwrap();
                println!(
                    "actual original IN source fragment={id:?} input={:?} result={:?} negated={negated} candidates={}",
                    input.ty,
                    node.ty,
                    list.len()
                );
                for value in list.iter() {
                    println!(
                        "actual original IN candidate {:?}",
                        unit.definition(*value).unwrap().ty
                    );
                }
            }
        }
    }
    n
}

/// Check the original LIKE operands and result's full nullable carrier facts.
pub fn inspect_like_source(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut n = 0;
    for (&id, fragment) in source.plan().fragments() {
        for (_source_node, unit, definition, node) in unit_definitions(fragment) {
            if let ExprKind::Like {
                expr,
                pattern,
                negated,
            } = &node.kind
            {
                n += 1;
                let input = unit.definition(*expr).unwrap();
                let pattern = unit.definition(*pattern).unwrap();
                println!(
                    "actual original LIKE fragment={id:?} definition={definition:?} operand={:?} pattern={:?} result={:?} negated={negated}",
                    input.ty, pattern.ty, node.ty
                );
                assert_eq!(input.ty.data_type().clone(), DataType::Utf8);
                assert_eq!(pattern.ty.data_type().clone(), DataType::Utf8);
                assert_eq!(node.ty.data_type().clone(), DataType::Boolean);
                assert_eq!(
                    node.ty.nullable(),
                    input.ty.nullable() || pattern.ty.nullable()
                );
            } else if let ExprKind::FunctionCall { function, args } = &node.kind
                && function.function_id.as_str() == "builtin.scalar/__like_with_escape/v1"
            {
                n += 1;
                assert_eq!(args.len(), 3);
                let mut nullable = false;
                for argument in args.iter() {
                    let ty = &unit.definition(*argument).unwrap().ty;
                    assert_eq!(ty.data_type(), &DataType::Utf8);
                    nullable |= ty.nullable();
                }
                assert_eq!(node.ty.data_type(), &DataType::Boolean);
                assert_eq!(node.ty.nullable(), nullable);
            }
        }
    }
    n
}

/// Author the original single-column ordered scan with core residuals.
pub fn ordered_scan_source() -> SqlAuthoredPhysicalPlan {
    sql_source_with_single_field_and_core_residuals(
        "SELECT k FROM t_filter_is_not_null_and_range WHERE k IS NOT NULL AND k >= 10 AND k < 20",
        Field::new("k", DataType::Int32, true),
    )
}
/// Check the original ordered scan declaration and residual count.
pub fn inspect_ordered_scan_source(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut counts = Vec::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                residuals,
                relation,
                ..
            } = &node.kind
            {
                assert_eq!(relation.schema().len(), 1);
                assert_eq!(relation.schema()[0].ty.data_type().clone(), DataType::Int32);
                assert!(relation.schema()[0].ty.nullable());
                assert!(relation.predicate_guarantees().is_empty());
                for (ordinal, id) in residuals.iter().enumerate() {
                    let unit = fragment.unit(node.id).unwrap();
                    let e = unit.definition(unit.root(*id).unwrap().definition).unwrap();
                    println!(
                        "original scan residual ordinal={ordinal} id={id:?} type={:?} kind={:?}",
                        e.ty, e.kind
                    );
                    assert_eq!(e.ty.data_type().clone(), DataType::Boolean);
                }
                counts.push(residuals.len());
            }
        }
    }
    assert_eq!(counts.len(), 1);
    assert!(
        counts[0] >= 2,
        "original optimizer must retain multiple residuals"
    );
    counts[0]
}
