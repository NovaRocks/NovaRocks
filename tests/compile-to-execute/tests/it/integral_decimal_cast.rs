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

//! Actual required signed source binding; no name-based re-resolution or fake provider.
use arrow::array::{Array, Decimal128Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_execution::exec::expr::compiled_program::CompiledExpressionInstance;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{ProgramRootInput, StaticExprKind, root_input_layout};
use novarocks_sql_testkit::sql_source;
use novarocks_test_compile_to_execute::programs::programs_with_catalogue;

use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integral Decimal CAST never waits")
    }
}
#[test]
fn integral_decimal128_actual_sql_required_i64_to_four_zero_bound_source_and_frame() {
    let target = DataType::Decimal128(4, 0);
    let source = sql_source(
        "SELECT CAST(k AS DECIMAL(4,0)) FROM fixture.source",
        DataType::Int64,
    );
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for (_, node) in fragment.expressions().iter() {
            if let novarocks_physical_plan::ExprKind::Cast {
                expr,
                target: bound,
                decimal_overflow_policy,
                ..
            } = &node.kind
                && *bound == target
                && *fragment.expressions().get(*expr).unwrap().ty.data_type() == DataType::Int64
            {
                count += 1;
                assert_eq!(*decimal_overflow_policy, DecimalOverflowPolicy::OutputNull);
            }
        }
    }
    assert_eq!(count, 1);
    let functions = novarocks_sql_testkit::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        for (&site, &root) in snapshot.bindings() {
            let invocation = &snapshot.flows()[&site.arena()].uses()[&root];
            if matches!(
                snapshot.roots().arenas()[&site.arena()]
                    .node(invocation.definition)
                    .unwrap()
                    .kind(),
                StaticExprKind::PreparedCast { .. }
            ) {
                assert!(found.is_none());
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual SQL CAST root");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("actual scan input")
    };
    let schema = program
        .checked()
        .channels()
        .channel_layout(node, role)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    // Pure input data. For target scale zero, the stored coefficient is the
    // signed input itself; all three magnitudes are below 10^4.
    let input = Arc::new(Int64Array::from(vec![-100, 0, 150]));
    let expected = [Some(-100_i128), Some(0), Some(150)];
    let data = RecordBatch::try_new(schema, vec![input]).unwrap();
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let output = frame
            .evaluate(&data, Selection::try_sparse(3, &rows).unwrap(), &Control)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(output.values().data_type(), &target);
        assert_eq!(output.selection(), Selection::try_sparse(3, &rows).unwrap());
        let values = output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.len(), rows.len());
        let actual = (0..values.len())
            .map(|ordinal| (!values.is_null(ordinal)).then(|| values.value(ordinal)))
            .collect::<Vec<_>>();
        let selected_expected = rows.iter().map(|row| expected[*row]).collect::<Vec<_>>();
        assert_eq!(actual, selected_expected);
    }
}
