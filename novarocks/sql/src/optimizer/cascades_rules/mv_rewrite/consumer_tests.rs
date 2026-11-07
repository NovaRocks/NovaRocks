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

use super::super::predicate_split::check_containment;
use super::*;
use crate::analysis::{ExprKind, TypedExpr};
use crate::common::BinOp;
use crate::optimizer::scalar::ScalarNode;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{ConstantPool, ConstantValue, FunctionResultType};
use novarocks_type_contract::{CompileControlError, DecimalOverflowPolicy, FunctionValueType};
use std::sync::{Arc, Mutex};

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refusal: None,
        }
    }
    fn at(index: usize, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            refusal: Some((index, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        if let Some((at, _)) = self.refusal {
            assert!(index <= at, "refused control was called again");
        }
        events.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}
fn integer(values: Vec<i64>, ordinal: u32, metadata: HashMap<String, String>) -> ConstantValue {
    let ty = FunctionValueType::new(DataType::Int64, false);
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    ConstantPool::try_new(
        Arc::new(Field::new("source.constant", DataType::Int64, false).with_metadata(metadata)),
        ty,
        array.to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn insert(arena: &mut ScalarArena, value: ConstantValue) -> ScalarId {
    let ty = value.value_type().clone();
    arena
        .intern_observed(
            ScalarNode::Constant(value),
            ty,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
}
fn sum(arena: &mut ScalarArena, value: ConstantValue, output: u32) -> ScalarAggregateSpec {
    let expression = TypedExpr {
        value_type: value.value_type().clone(),
        kind: ExprKind::Constant(value.clone()),
    };
    let resolved = crate::functions::resolve_sql_aggregate_binding(
        crate::functions::builtin_sql_function_catalog(),
        "sum",
        &[expression],
        &[],
        false,
        crate::constant::test_constant_policy(),
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap();
    ScalarAggregateSpec {
        output_column_id: ColumnId(output),
        name: "sum".into(),
        distinct: false,
        source: crate::binding::AggregateArgumentSource::uncertified(
            vec![insert(arena, value)],
            vec![],
            crate::binding::SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::OutputNull),
        ),
    }
}
struct RollupFixture {
    query: ScalarArena,
    query_key: ScalarId,
    query_call: ScalarAggregateSpec,
    mv: ScalarArena,
    mv_aggregate: SpjgAggregate,
    outputs: Vec<SpjgOutput>,
    names: HashMap<ColumnId, String>,
}
impl RollupFixture {
    fn new() -> Self {
        let mut query = ScalarArena::new();
        let query_key = insert(&mut query, integer(vec![-111, 42], 1, HashMap::new()));
        let query_call = sum(&mut query, integer(vec![999, 42], 1, HashMap::new()), 77);
        let mut mv = ScalarArena::new();
        let mv_key = insert(&mut mv, integer(vec![42, -222], 0, HashMap::new()));
        let first = sum(&mut mv, integer(vec![42, 333], 0, HashMap::new()), 88);
        let last = sum(&mut mv, integer(vec![444, 42], 1, HashMap::new()), 89);
        let different = sum(&mut mv, integer(vec![43], 0, HashMap::new()), 90);
        let outputs = [first, last, different]
            .into_iter()
            .map(|call| SpjgOutput {
                name: format!("sum{}", call.output_column_id.0),
                column_id: call.output_column_id,
                expr: SpjgOutputExpr::Aggregate(call),
            })
            .collect();
        Self {
            query,
            query_key,
            query_call,
            mv,
            mv_aggregate: SpjgAggregate {
                group_by: vec![mv_key],
            },
            outputs,
            names: HashMap::new(),
        }
    }
    fn plan(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<Option<RollupPlan>, SqlCompileError> {
        plan_rollup(
            &[self.query_key],
            std::slice::from_ref(&self.query_call),
            &self.query,
            &self.names,
            &self.mv_aggregate,
            &self.outputs,
            &self.mv,
            &self.names,
            control,
        )
    }
}

#[test]
fn actual_cv_rollup_keeps_selected_values_complete_type_and_last_exact_output() {
    let mut fixture = RollupFixture::new();
    let plan = fixture.plan(&Control::good()).unwrap().unwrap();
    assert!(matches!(plan.kind, RollupKind::Direct));
    assert_eq!(plan.items.len(), 1);
    assert_eq!(
        plan.items[0].mv_output_index, 1,
        "last exact output wins across independent selected pools"
    );
    let FunctionResultType::Scalar(result_type) =
        &fixture.query_call.source.binding().selected.result_type
    else {
        panic!("scalar SUM result");
    };
    let mut work = CompileCheckpoints::try_new(
        crate::optimizer::test_optimizer_control(),
        CompilePhase::LowerProgram,
    )
    .unwrap();
    let NormExpr::Call {
        value_type,
        argument_order,
        ..
    } = norm_agg(
        &fixture.query,
        &fixture.query_call,
        &fixture.names,
        &mut work,
    )
    .unwrap()
    .unwrap()
    else {
        panic!("aggregate norm");
    };
    assert_eq!(&value_type, result_type);
    assert!(matches!(argument_order, NormArgumentOrder::Ordered));
    work.finish().unwrap();
    fixture.query_call = sum(&mut fixture.query, integer(vec![43], 0, HashMap::new()), 77);
    assert_eq!(
        fixture.plan(&Control::good()).unwrap().unwrap().items[0].mv_output_index,
        2
    );
    fixture.query_key = insert(
        &mut fixture.query,
        integer(
            vec![42],
            0,
            HashMap::from([("provider.fact".into(), "different".into())]),
        ),
    );
    assert!(
        fixture.plan(&Control::good()).unwrap().is_some(),
        "source field metadata does not author value identity"
    );
    fixture.query_key = insert(&mut fixture.query, integer(vec![41], 0, HashMap::new()));
    assert!(
        fixture.plan(&Control::good()).unwrap().is_none(),
        "different selected values cannot reuse the grouping dimension"
    );
}

// Keep this fixture on the exact residual path with a real non-containment counterexample.
fn predicate(arena: &mut ScalarArena, number: i64, ordinal: u32, unused: i64) -> ScalarId {
    let column = arena
        .intern_observed(
            ScalarNode::ColumnRef(ColumnId(1)),
            FunctionValueType::new(DataType::Int64, true),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
    let values = if ordinal == 0 {
        vec![number, unused]
    } else {
        vec![unused, number]
    };
    let value = insert(arena, integer(values, ordinal, HashMap::new()));
    arena
        .intern_observed(
            ScalarNode::BinaryOp {
                left: column,
                op: BinOp::Ne,
                right: value,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Boolean, true),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
}
struct ResidualFixture {
    query: ScalarArena,
    query_ids: Vec<ScalarId>,
    mv: ScalarArena,
    mv_ids: Vec<ScalarId>,
    names: HashMap<ColumnId, String>,
}
impl ResidualFixture {
    fn new(mv_number: i64) -> Self {
        let mut query = ScalarArena::new();
        let query_ids = vec![
            predicate(&mut query, 42, 1, -111),
            predicate(&mut query, 43, 0, -222),
        ];
        let mut mv = ScalarArena::new();
        let mv_ids = vec![predicate(&mut mv, mv_number, 0, 999)];
        Self {
            query,
            query_ids,
            mv,
            mv_ids,
            names: HashMap::from([(ColumnId(1), "a".into())]),
        }
    }
    fn check(
        &self,
        control: &dyn PureCompileControl,
    ) -> Result<Option<super::super::predicate_split::ContainmentResult>, SqlCompileError> {
        check_containment(
            &self.query_ids,
            &self.query,
            &self.mv_ids,
            &self.mv,
            &self.names,
            &self.names,
            control,
        )
    }
}
#[test]
fn actual_cv_residuals_preserve_compensation_order_and_selected_values() {
    let fixture = ResidualFixture::new(42);
    assert_eq!(
        fixture
            .check(&Control::good())
            .unwrap()
            .unwrap()
            .compensation,
        vec![fixture.query_ids[1]]
    );
    let mismatch = ResidualFixture::new(41);
    assert!(
        mismatch.check(&Control::good()).unwrap().is_none(),
        "a != 42 AND a != 43 does not imply a != 41; the selected CV value must remain exact"
    );
}
fn assert_cause(error: SqlCompileError, cause: CompileControlError) {
    assert!(matches!(
        (error, cause),
        (SqlCompileError::Cancelled, CompileControlError::Cancelled)
            | (
                SqlCompileError::DeadlineExceeded,
                CompileControlError::DeadlineExceeded
            )
            | (
                SqlCompileError::ResourceExhausted,
                CompileControlError::ResourceExhausted
            )
    ));
}
fn check_callbacks<T>(run: impl Fn(&Control) -> Result<T, SqlCompileError>) {
    let good = Control::good();
    assert!(run(&good).is_ok());
    let trace = good.trace();
    assert!(trace.len() >= 2);
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusal = Control::at(index, cause.clone());
            match run(&refusal) {
                Err(error) => assert_cause(error, cause),
                Ok(_) => panic!("control refusal must be fatal"),
            }
            assert_eq!(refusal.trace(), trace[..=index]);
        }
    }
}
#[test]
fn actual_cv_rollup_and_residual_control_preserve_every_callback_and_ordinary_miss_tail() {
    let rollup = RollupFixture::new();
    check_callbacks(|control| rollup.plan(control));
    let residual = ResidualFixture::new(42);
    check_callbacks(|control| residual.check(control));
    let miss = ResidualFixture::new(41);
    let good = Control::good();
    assert!(miss.check(&good).unwrap().is_none());
    assert!(
        good.trace()
            .last()
            .is_some_and(|(phase, _)| *phase == CompilePhase::LowerProgram)
    );
    check_callbacks(|control| miss.check(control));
    let empty = ScalarArena::new();
    let names = HashMap::new();
    check_callbacks(|control| check_containment(&[], &empty, &[], &empty, &names, &names, control));
}
