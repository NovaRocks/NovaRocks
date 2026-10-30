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

//! Compiled row assertions: a global row count or at most one row per key,
//! judged over the whole instance input and passing rows through unchanged.

use std::sync::Arc;

use novarocks_local_program::{AssertRowsMode, LocalProgram, ProgramNodeKind, RowAssertion};
use novarocks_physical_plan::{
    ConstantPools, FragmentBuilder, FragmentId, NodeId, RowCountAssertion, RowCountAssertionSpec,
    ValueId,
};

use super::family_fixture::{cell, compile, int64, int64_rows, package, run, try_run, values};

/// `Values(k, v) -> AssertOneRow(spec) -> Result`.
fn program(
    rows: &[(Option<i64>, i64)],
    spec: impl FnOnce(&[ValueId]) -> RowCountAssertionSpec,
) -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(51));
    let source = NodeId::new(0);
    let assert = NodeId::new(1);
    let cells = rows
        .iter()
        .map(|(k, v)| vec![cell(*k), cell(Some(*v))])
        .collect::<Vec<_>>();
    let columns = values(&mut builder, source, &[int64(true), int64(false)], &cells);
    builder
        .add_assert_one_row(assert, source, spec(&columns))
        .unwrap();
    compile(package(builder, assert, ConstantPools::empty(), 1), 1)
}

fn global(desired_rows: u64, comparison: RowCountAssertion) -> RowCountAssertionSpec {
    RowCountAssertionSpec::Global {
        subject: "SELECT v FROM t".into(),
        desired_rows,
        comparison,
    }
}

fn keyed(columns: &[ValueId]) -> RowCountAssertionSpec {
    RowCountAssertionSpec::PerKeyAtMostOne {
        applicability: novarocks_physical_plan::AssertionApplicability::AllRows,
        keys: Box::from([columns[0]]),
        labels: Box::from(["k".into()]),
        message: "duplicate merge source row".into(),
    }
}

fn expected(rows: &[(Option<i64>, i64)]) -> Vec<Vec<Option<i64>>> {
    rows.iter().map(|(k, v)| vec![*k, Some(*v)]).collect()
}

#[test]
fn compiled_global_assertion_passes_rows_through_when_the_count_holds() {
    let rows = [(Some(7), 70)];
    let single = program(&rows, |_| global(1, RowCountAssertion::Eq));
    let ProgramNodeKind::AssertNumRows { mode, .. } = single.graph().nodes()[1].kind() else {
        panic!("the compiler emits a local AssertNumRows");
    };
    assert!(matches!(
        mode,
        AssertRowsMode::Global {
            desired_num_rows: Some(1),
            assertion: RowAssertion::Eq,
            ..
        }
    ));
    assert_eq!(int64_rows(&run(&single)), expected(&rows));

    let three = [(Some(1), 10), (None, 20), (Some(3), 30)];
    for comparison in [
        RowCountAssertion::Le,
        RowCountAssertion::Ge,
        RowCountAssertion::Eq,
    ] {
        let counted = program(&three, |_| global(3, comparison));
        assert_eq!(int64_rows(&run(&counted)), expected(&three));
    }
}

#[test]
fn compiled_global_assertion_fails_the_fragment_when_the_count_is_violated() {
    let three = [(Some(1), 10), (None, 20), (Some(3), 30)];
    for (desired, comparison) in [
        (1, RowCountAssertion::Eq),
        (3, RowCountAssertion::Gt),
        (3, RowCountAssertion::Lt),
        (3, RowCountAssertion::Ne),
    ] {
        let error = try_run(&program(&three, |_| global(desired, comparison)))
            .expect_err("the violated assertion fails the fragment");
        assert!(error.contains("assert_num_rows failed"), "{error}");
        assert!(error.contains("actual=3 row(s)"), "{error}");
        assert!(error.contains("SELECT v FROM t"), "{error}");
    }
    // An empty input still reaches the final count check.
    let error = try_run(&program(&[], |_| global(1, RowCountAssertion::Eq)))
        .expect_err("no row violates an exactly-one assertion");
    assert!(error.contains("actual=0 row(s)"), "{error}");
}

#[test]
fn compiled_keyed_assertion_allows_distinct_keys_and_refuses_a_duplicate() {
    let distinct = [(Some(1), 10), (Some(2), 20), (None, 30)];
    let unique = program(&distinct, keyed);
    let ProgramNodeKind::AssertNumRows { mode, .. } = unique.graph().nodes()[1].kind() else {
        panic!("the compiler emits a local AssertNumRows");
    };
    let AssertRowsMode::PerKeyAtMostOne { key_slots, .. } = mode else {
        panic!("keyed assertion");
    };
    assert_eq!(
        key_slots.as_slice(),
        &unique.graph().nodes()[0].output_layout().slots()[..1]
    );
    assert_eq!(int64_rows(&run(&unique)), expected(&distinct));

    let duplicate = [(Some(1), 10), (Some(2), 20), (Some(1), 30)];
    let error = try_run(&program(&duplicate, keyed)).expect_err("a repeated key fails");
    assert!(
        error.contains("duplicate merge source row: duplicate k=1"),
        "{error}"
    );
    // The existing key owner treats two NULL keys as the same key.
    let nulls = [(None, 10), (None, 20)];
    let error = try_run(&program(&nulls, keyed)).expect_err("a repeated NULL key fails");
    assert!(error.contains("duplicate k=<NULL>"), "{error}");
}

/// The applicability is an original materialized nullable Boolean column.
/// Duplicate bypass keys deliberately remain in the result unchanged.
fn conditional_program(rows: &[(Option<i64>, i64, Option<bool>)], dop: usize) -> Arc<LocalProgram> {
    use novarocks_physical_plan::{AssertionApplicability, LiteralValue};
    let mut builder = FragmentBuilder::new(FragmentId::new(52));
    let source = NodeId::new(0);
    let assertion = NodeId::new(1);
    let cells = rows
        .iter()
        .map(|(key, value, applies)| {
            vec![
                cell(*key),
                cell(Some(*value)),
                applies.map_or(LiteralValue::Null, LiteralValue::Boolean),
            ]
        })
        .collect::<Vec<_>>();
    let columns = values(
        &mut builder,
        source,
        &[
            int64(true),
            int64(false),
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Boolean,
                true,
            ),
        ],
        &cells,
    );
    builder
        .add_assert_one_row(
            assertion,
            source,
            RowCountAssertionSpec::PerKeyAtMostOne {
                applicability: AssertionApplicability::WhenTrue(columns[2]),
                keys: Box::from([columns[0]]),
                labels: Box::from(["target".into()]),
                message: "selected target repeated".into(),
            },
        )
        .unwrap();
    compile(
        package(builder, assertion, ConstantPools::empty(), dop as u32),
        dop,
    )
}

#[test]
fn compiled_keyed_builder_keeps_when_true_and_parallel_driver_key_authority() {
    use crate::exec::pipeline::{
        binding::{ExchangeBindings, ScanBindings},
        dependency::DependencyManager,
    };
    use crate::runtime::runtime_state::RuntimeErrorState;
    let rows = [
        (Some(7), 10, Some(true)),
        (Some(7), 20, Some(false)),
        (Some(7), 30, None),
        (None, 40, Some(false)),
        (None, 50, None),
    ];
    for dop in [1, 3, 4] {
        let program = conditional_program(&rows, dop);
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        let graph = super::build_compiled_pipeline_graph_for_test(
            &program,
            ExchangeBindings::default(),
            ScanBindings::default(),
            crate::runtime::fragment::CompiledWriterBindings::default(),
            None,
            DependencyManager::new(),
            dop as i32,
            Some(1),
            runtime.function_set().clone(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap();
        let assertion_pipelines = graph
            .pipelines
            .iter()
            .filter(|pipeline| {
                pipeline
                    .factories
                    .iter()
                    .any(|factory| factory.name().starts_with("AssertNumRows"))
            })
            .collect::<Vec<_>>();
        assert_eq!(assertion_pipelines.len(), 1);
        assert_eq!(
            assertion_pipelines[0].dop, dop as i32,
            "keyed assertion retains real partition drivers before the independent result gather"
        );
        let output = run(&program);
        let mut values = output
            .iter()
            .flat_map(|chunk| {
                chunk.columns()[1]
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, vec![10, 20, 30, 40, 50]);
        let duplicate = conditional_program(
            &[
                (Some(7), 10, Some(true)),
                (Some(7), 20, Some(false)),
                (Some(7), 30, Some(true)),
            ],
            dop,
        );
        let failure =
            try_run(&duplicate).expect_err("same selected key must reach one driver authority");
        assert!(
            failure.contains("selected target repeated: duplicate target=7"),
            "{failure}"
        );
    }
}

#[test]
fn compiled_conditional_builder_preserves_all_bypass_rows_with_null_keys() {
    let rows = (0..12)
        .map(|row| (None, row, if row % 2 == 0 { Some(false) } else { None }))
        .collect::<Vec<_>>();
    for dop in [1, 3, 4] {
        let output = run(&conditional_program(&rows, dop));
        let mut actual = output
            .iter()
            .flat_map(|chunk| {
                chunk.columns()[1]
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(
            actual,
            (0..12).collect::<Vec<_>>(),
            "bypass rows never enter the target-key decoder or duplicate set"
        );
    }
}
