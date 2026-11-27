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

//! Compiled UnionAll: each branch's normalizing Project evaluates its slot
//! reads through compiled roots and owns the union's channels; the union only
//! fans the branches in. The oracle is the literal rows mapped per branch.

use std::sync::Arc;

use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_physical_plan::{
    ConstantPools, Distribution, FragmentId, NodeId, NodeKind, NullOrdering, RequiredInputs,
    SetOperationKind, SortDirection, SortExpr, SortMode, ValueId, ValueOrigin,
};
use novarocks_type_contract::EvaluationDemand;
use novarocks_type_contract::FunctionValueType;

use super::family_fixture::{cell, compile, int64, int64_rows, package, run, values};

/// Branch `k` holds rows `(a, b)`; the union output is `(b, a)` of every
/// branch, so each normalizer reorders its input channels.
fn branch_rows(branch: usize) -> Vec<(i64, Option<i64>)> {
    match branch {
        0 => vec![(1, Some(10)), (2, Some(20))],
        1 => vec![(3, None), (4, Some(40)), (5, Some(50))],
        _ => vec![(6, Some(60))],
    }
}

/// `Values_k(a, b)` for each branch, `UnionAll(b, a)`, and optionally a
/// downstream global sort by the union's second column.
fn program(branches: usize, sorted: bool, dop: usize) -> (Arc<LocalProgram>, Vec<NodeId>) {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            FragmentId::new(41),
            &fixture_sources,
        );
    let union = NodeId::new(100);
    let mut inputs = Vec::new();
    let mut mappings = Vec::new();
    for branch in 0..branches {
        let node = NodeId::new(u32::try_from(branch).unwrap());
        // Only branch 1 carries a NULL in `b`; the union output widens it.
        let b_type = int64(branch == 1);
        let rows = branch_rows(branch)
            .into_iter()
            .map(|(a, b)| vec![cell(Some(a)), cell(b)])
            .collect::<Vec<_>>();
        let columns = values(&mut builder, node, &[int64(false), b_type], &rows);
        inputs.push(node);
        mappings.push(Box::from([columns[1], columns[0]]));
    }
    let output_types: [FunctionValueType; 2] = [int64(true), int64(false)];
    let outputs = output_types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: union,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<ValueId>>();
    builder
        .add_row_consuming(
            union,
            inputs.clone().into_boxed_slice(),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            outputs.clone().into_boxed_slice(),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: mappings.into_boxed_slice(),
            },
        )
        .unwrap();
    let root = if sorted {
        let sort = NodeId::new(200);
        let key = builder
            .unit(sort)
            .unwrap()
            .add(None, int64(false), ExprKind::input(outputs[1]))
            .unwrap();
        let key = builder
            .unit(sort)
            .unwrap()
            .root(key, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .add_sort(
                sort,
                union,
                Box::from([SortExpr {
                    expr: key,
                    direction: SortDirection::Descending,
                    null_ordering: NullOrdering::Last,
                }]),
                SortMode::Global,
            )
            .unwrap();
        sort
    } else {
        union
    };
    let max_dop = u32::try_from(dop).unwrap();
    (
        compile(package(builder, root, ConstantPools::empty(), max_dop), dop),
        inputs,
    )
}

/// Every branch row as the union's `(b, a)`.
fn oracle(branches: usize) -> Vec<Vec<Option<i64>>> {
    (0..branches)
        .flat_map(branch_rows)
        .map(|(a, b)| vec![b, Some(a)])
        .collect()
}

#[test]
fn compiled_union_all_keeps_every_branch_row_in_the_union_channels() {
    let (program, _) = program(2, false, 1);
    let union = program.graph().root();
    let ProgramNodeKind::UnionAll { inputs } = program.graph().nodes()[union.index()].kind() else {
        panic!("the compiler emits a local UnionAll root");
    };
    for input in inputs {
        assert!(matches!(
            program.graph().nodes()[input.index()].kind(),
            ProgramNodeKind::Project {
                is_subordinate: true,
                ..
            }
        ));
    }
    // UNION ALL promises no order: compare the row multisets.
    let mut rows = int64_rows(&run(&program));
    let mut expected = oracle(2);
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn compiled_union_all_of_three_branches_feeds_a_compiled_sort_at_two_drivers() {
    let (program, _) = program(3, true, 2);
    assert_eq!(program.graph().profile().pipeline_dop().get(), 2);
    let rows = int64_rows(&run(&program));
    let mut expected = oracle(3);
    expected.sort_by(|left, right| right[1].cmp(&left[1]));
    assert_eq!(rows, expected);
}

#[test]
fn compiled_union_all_refuses_a_branch_that_does_not_own_the_union_layout() {
    let (program, _) = program(2, false, 1);
    let union = program.graph().root();
    let ProgramNodeKind::UnionAll { inputs } = program.graph().nodes()[union.index()].kind() else {
        panic!("the compiler emits a local UnionAll root");
    };
    assert!(super::validate_set_branches(&program, union, inputs, "UnionAll").is_ok());
    // A branch's raw Values child is not its normalizer.
    let ProgramNodeKind::Project { input: raw, .. } =
        program.graph().nodes()[inputs[0].index()].kind()
    else {
        panic!("normalizer");
    };
    let error =
        super::validate_set_branches(&program, union, &[*raw, inputs[1]], "UnionAll").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("branch 0 is not a normalizer owning the set layout"),
        "{error}"
    );
    let error = super::validate_set_branches(
        &program,
        union,
        &[ProgramNodeId::new(inputs[1].index())],
        "UnionAll",
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("fewer than two branches"),
        "{error}"
    );
}
