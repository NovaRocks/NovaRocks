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

use super::function::FunctionKind;
use super::{
    ExprArena, ExprEvaluation, ExprEvaluationContext, ExprEvaluationControl, ExprId, ExprNode,
    LiteralValue,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::pipeline::operator::input_construction::*;
use crate::runtime::fragment::{ExecutionFailure, ExecutionFailureCause, ExecutionResult};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, ListArray, ListBuilder},
    datatypes::{DataType, Field},
};
use novarocks_functions::{KernelDiagnostic, KernelFailure};
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageHold, ValueTypeStorageLifetime};
use novarocks_types::SlotId;
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct ControlState {
    calls: usize,
    stop: Option<usize>,
    cause: Option<ExecutionFailure>,
}
#[derive(Default)]
struct Control(Mutex<ControlState>);
impl Control {
    fn calls(&self) -> usize {
        self.0.lock().unwrap().calls
    }
    fn arm(&self, after: usize, cause: impl Into<ExecutionFailure>) {
        let mut s = self.0.lock().unwrap();
        s.stop = Some(s.calls + after);
        s.cause = Some(cause.into());
    }
}
impl ExprEvaluationControl for Control {
    fn checkpoint(&self) -> ExecutionResult<()> {
        let mut s = self.0.lock().unwrap();
        let at = s.calls;
        s.calls += 1;
        if let Some(stop) = s.stop {
            assert!(at <= stop, "checkpoint after first refusal");
            if at == stop {
                return Err(s.cause.clone().unwrap());
            }
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> ExecutionResult<()> {
        ExprEvaluationControl::checkpoint(self)
    }
}
impl InputConstructionWorkControl for Control {
    fn checkpoint(&self, _: usize) -> Result<(), ConstructionFailure> {
        ExprEvaluationControl::checkpoint(self).map_err(ConstructionFailure::from)
    }
}

#[derive(Default)]
struct Requests {
    calls: usize,
    bytes: usize,
    stop: Option<usize>,
    cause: Option<KernelFailure>,
}
#[derive(Default)]
struct Host(Mutex<Requests>);
impl Host {
    fn calls(&self) -> usize {
        self.0.lock().unwrap().calls
    }
    fn arm(&self, after: usize, cause: KernelFailure) {
        let mut s = self.0.lock().unwrap();
        s.stop = Some(s.calls + after);
        s.cause = Some(cause);
    }
    fn request_bytes(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        let mut s = self.0.lock().unwrap();
        let at = s.calls;
        s.calls += 1;
        if let Some(stop) = s.stop {
            assert!(at <= stop, "host request after first refusal");
            if at == stop {
                return Err(ConstructionFailure::Caller(Box::new(
                    s.cause.clone().unwrap().into(),
                )));
            }
        }
        s.bytes += bytes;
        Ok(())
    }
}
impl InputConstructionRequestHost for Host {
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.request_bytes(layout.size())
    }
    fn observe_opaque_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.request_bytes(bytes)
    }
    fn release_requested_bytes(&self, bytes: usize) {
        let mut s = self.0.lock().unwrap();
        assert!(bytes <= s.bytes);
        s.bytes -= bytes;
    }
}
fn factory(host: Arc<Host>, control: Arc<Control>) -> Arc<ObservedInputConstructionFactory> {
    ObservedInputConstructionFactory::try_new(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        host,
        control,
        Arc::new(LockedArrowInputConstructionModel),
    )
    .unwrap()
}
struct SourceStorage;
impl ValueTypeStorageHold for SourceStorage {}
fn typed_input(columns: Vec<ArrayRef>) -> Chunk {
    let storage = ValueTypeStorageLifetime::held(Arc::new(SourceStorage));
    let rows = columns[0].len();
    let slots = columns
        .iter()
        .enumerate()
        .map(|(i, array)| {
            let ty = FunctionValueType::new(array.data_type().clone(), true);
            ChunkSlotSchema::try_new_typed(
                SlotId::new(u32::try_from(i + 1).unwrap()),
                Arc::new(Field::new(format!("c{i}"), array.data_type().clone(), true)),
                ty.typed_binding().clone(),
                None,
                storage.clone(),
            )
            .unwrap()
        })
        .collect();
    let schema = Arc::new(ChunkSchema::try_new_typed(slots, storage.clone()).unwrap());
    Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
        schema,
        columns,
        rows,
        storage,
        |_| Ok(()),
    )
    .unwrap()
}
fn list_input() -> Chunk {
    let mut b = ListBuilder::new(Int64Builder::new());
    b.values().append_value(1);
    b.values().append_null();
    b.values().append_value(2);
    b.append(true);
    b.values().append_null();
    b.append(true);
    b.append(true);
    typed_input(vec![
        Arc::new(b.finish()),
        Arc::new(Int64Array::from(vec![Some(2), None, None])),
    ])
}
fn call(a: &mut ExprArena, name: &'static str, args: Vec<ExprId>) -> ExprId {
    let ty = if name == "array_contains" {
        DataType::Boolean
    } else {
        DataType::Int64
    };
    a.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Array(name),
            args,
        },
        ty,
    )
}
fn slot(a: &mut ExprArena, id: u32, ty: DataType) -> ExprId {
    a.push_typed(ExprNode::SlotId(SlotId::new(id)), ty)
}
fn literal(a: &mut ExprArena, n: i64) -> ExprId {
    a.push_typed(ExprNode::Literal(LiteralValue::Int64(n)), DataType::Int64)
}
fn pair(a: &mut ExprArena, array: ExprId, probe: ExprId) -> (ExprId, ExprId) {
    (
        call(a, "array_contains", vec![array, probe]),
        call(a, "array_position", vec![array, probe]),
    )
}
fn bools(a: &ArrayRef) -> Vec<Option<bool>> {
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn causes() -> Vec<KernelFailure> {
    vec![
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InstanceFailed,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
        KernelFailure::Operational(KernelDiagnostic::new("exact operation")),
    ]
}
fn originating_kernel(e: &ExecutionFailure) -> &KernelFailure {
    match e.cause() {
        ExecutionFailureCause::Kernel(k) => k,
        ExecutionFailureCause::Construction(ConstructionFailure::Caller(c)) => {
            originating_kernel(c)
        }
        other => panic!("typed kernel cause was lost: {other:?}"),
    }
}

#[test]
fn typed_array_membership_context_keeps_constant_and_column_null_position_oracles() {
    let input = list_input();
    let mut a = ExprArena::default();
    let src = slot(&mut a, 1, input.columns()[0].data_type().clone());
    let probe = slot(&mut a, 2, DataType::Int64);
    let two = literal(&mut a, 2);
    let one = literal(&mut a, 1);
    let three = literal(&mut a, 3);
    let constant = a.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![one, two, three],
        },
        input.columns()[0].data_type().clone(),
    );
    let cases = [
        (
            pair(&mut a, src, probe),
            vec![Some(true), Some(true), Some(false)],
            vec![Some(3), Some(1), Some(0)],
        ),
        (
            pair(&mut a, constant, probe),
            vec![Some(true), Some(false), Some(false)],
            vec![Some(2), Some(0), Some(0)],
        ),
        (
            pair(&mut a, src, two),
            vec![Some(true), Some(false), Some(false)],
            vec![Some(3), Some(0), Some(0)],
        ),
        (
            pair(&mut a, constant, two),
            vec![Some(true); 3],
            vec![Some(2); 3],
        ),
    ];
    let host = Arc::new(Host::default());
    let control = Arc::new(Control::default());
    let f = factory(host.clone(), control.clone());
    let owner = f.begin_input_construction(&input).unwrap();
    let context = ExprEvaluationContext::new(&a, owner.as_ref(), control.as_ref());
    let before = host.calls();
    for ((contains, position), expected_contains, expected_position) in cases {
        assert_eq!(
            bools(&context.eval(contains, &input).unwrap()),
            expected_contains
        );
        assert_eq!(
            ints(&context.eval(position, &input).unwrap()),
            expected_position
        );
    }
    assert!(
        host.calls() > before,
        "actual slice wrapper requests must reach the installed host"
    );
    drop(context);
    drop(owner);
    drop(f);
    assert_eq!(host.0.lock().unwrap().bytes, 0);
}

#[test]
fn typed_nested_membership_calls_keep_the_same_runtime_loan() {
    let input = list_input();
    let mut a = ExprArena::default();
    let src = slot(&mut a, 1, input.columns()[0].data_type().clone());
    let probe = slot(&mut a, 2, DataType::Int64);
    let position = call(&mut a, "array_position", vec![src, probe]);
    let wrapped = a.push_typed(
        ExprNode::ArrayExpr {
            elements: vec![position],
        },
        input.columns()[0].data_type().clone(),
    );
    let one = literal(&mut a, 1);
    let root = call(&mut a, "array_contains", vec![wrapped, one]);
    let c = Arc::new(Control::default());
    let f = factory(Arc::new(Host::default()), c.clone());
    let o = f.begin_input_construction(&input).unwrap();
    assert_eq!(
        bools(
            &ExprEvaluationContext::new(&a, o.as_ref(), c.as_ref())
                .eval(root, &input)
                .unwrap()
        ),
        vec![Some(false), Some(true), Some(false)]
    );
}
fn lambda_fixture() -> (ExprArena, ExprId, Chunk) {
    let mut outer = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    outer.values().values().append_value(1);
    outer.values().values().append_value(2);
    outer.values().append(true);
    outer.values().values().append_value(3);
    outer.values().append(true);
    outer.append(true);
    outer.values().values().append_null();
    outer.values().append(true);
    outer.append(true);
    outer.append(true);
    let input = typed_input(vec![Arc::new(outer.finish())]);
    let mut a = ExprArena::default();
    let source = slot(&mut a, 1, input.columns()[0].data_type().clone());
    let list_ty = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let arg = slot(&mut a, 7, list_ty);
    let two = literal(&mut a, 2);
    let body = call(&mut a, "array_contains", vec![arg, two]);
    let lambda = a.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots: vec![SlotId::new(7)],
            common_sub_exprs: vec![],
            is_nondeterministic: false,
        },
        DataType::Null,
    );
    let root = a.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::ArrayMap,
            args: vec![lambda, source],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
    );
    (a, root, input)
}
#[test]
fn actual_array_map_flat_lambda_view_preserves_untyped_facts_and_context() {
    let (a, root, input) = lambda_fixture();
    let c = Arc::new(Control::default());
    let h = Arc::new(Host::default());
    let f = factory(h.clone(), c.clone());
    let o = f.begin_input_construction(&input).unwrap();
    let before = h.calls();
    let out = ExprEvaluationContext::new(&a, o.as_ref(), c.as_ref())
        .eval(root, &input)
        .unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(bools(&list.value(0)), vec![Some(true), Some(false)]);
    assert_eq!(bools(&list.value(1)), vec![Some(false)]);
    assert_eq!(bools(&list.value(2)), Vec::<Option<bool>>::new());
    assert!(h.calls() > before);
}

#[test]
fn array_map_source_geometry_probe_cannot_swallow_any_typed_control_cause() {
    let (a, root, input) = lambda_fixture();
    let control = Arc::new(Control::default());
    let f = factory(Arc::new(Host::default()), control.clone());
    let o = f.begin_input_construction(&input).unwrap();
    let start = control.calls();
    ExprEvaluationContext::new(&a, o.as_ref(), control.as_ref())
        .eval(root, &input)
        .unwrap();
    let checkpoints = control.calls() - start;
    assert!(checkpoints > 4);
    for cause in causes() {
        for stop in 0..checkpoints {
            let c = Arc::new(Control::default());
            let h = Arc::new(Host::default());
            let f = factory(h.clone(), c.clone());
            let o = f.begin_input_construction(&input).unwrap();
            c.arm(stop, cause.clone());
            let failure = ExprEvaluationContext::new(&a, o.as_ref(), c.as_ref())
                .eval(root, &input)
                .unwrap_err();
            assert_eq!(originating_kernel(&failure), &cause, "checkpoint {stop}");
            drop(o);
            drop(f);
            assert_eq!(h.0.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn actual_slice_host_refusals_remain_typed_through_array_map_probe() {
    let (a, root, input) = lambda_fixture();
    let c = Arc::new(Control::default());
    let h = Arc::new(Host::default());
    let f = factory(h.clone(), c.clone());
    let o = f.begin_input_construction(&input).unwrap();
    let start = h.calls();
    ExprEvaluationContext::new(&a, o.as_ref(), c.as_ref())
        .eval(root, &input)
        .unwrap();
    let requests = h.calls() - start;
    assert!(requests > 0);
    for cause in causes() {
        for stop in 0..requests {
            let c = Arc::new(Control::default());
            let h = Arc::new(Host::default());
            let f = factory(h.clone(), c.clone());
            let o = f.begin_input_construction(&input).unwrap();
            h.arm(stop, cause.clone());
            let failure = ExprEvaluationContext::new(&a, o.as_ref(), c.as_ref())
                .eval(root, &input)
                .unwrap_err();
            assert_eq!(originating_kernel(&failure), &cause, "host request {stop}");
            drop(o);
            drop(f);
            assert_eq!(h.0.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn reference_evaluation_has_no_authority_to_create_typed_constant_view() {
    let input = list_input();
    let mut a = ExprArena::default();
    let src = slot(&mut a, 1, input.columns()[0].data_type().clone());
    let two = literal(&mut a, 2);
    let root = call(&mut a, "array_contains", vec![src, two]);
    assert_eq!(
        a.eval(root, &input).unwrap_err(),
        ConstructionFailure::MissingOwner.to_string()
    );
}

#[test]
fn array_map_probe_preserves_original_pipeline_failure_and_operator_context() {
    use crate::runtime::fragment::PipelineOperation;
    let (arena, root, input) = lambda_fixture();
    let control = Arc::new(Control::default());
    let f = factory(Arc::new(Host::default()), control.clone());
    let owner = f.begin_input_construction(&input).unwrap();
    let start = control.calls();
    ExprEvaluationContext::new(&arena, owner.as_ref(), control.as_ref())
        .eval(root, &input)
        .unwrap();
    let checkpoints = control.calls() - start;
    let original = ExecutionFailure::from("original fragment pipeline failure")
        .at_operator(37, PipelineOperation::Push);
    for stop in 0..checkpoints {
        let control = Arc::new(Control::default());
        let host = Arc::new(Host::default());
        let f = factory(host.clone(), control.clone());
        let owner = f.begin_input_construction(&input).unwrap();
        control.arm(stop, original.clone());
        let failure = ExprEvaluationContext::new(&arena, owner.as_ref(), control.as_ref())
            .eval(root, &input)
            .unwrap_err();
        assert_eq!(failure, original, "checkpoint {stop}");
        drop(owner);
        drop(f);
        assert_eq!(host.0.lock().unwrap().bytes, 0);
    }
}

#[test]
fn real_runtime_control_preserves_first_failure_for_checkpoint_and_wait() {
    use crate::exec::operators::compiled_expression::RuntimeKernelControl;
    use crate::runtime::fragment::PipelineOperation;
    use crate::runtime::runtime_state::RuntimeErrorState;
    let mut originals: Vec<ExecutionFailure> = causes().into_iter().map(Into::into).collect();
    originals.push(ExecutionFailure::from("cancelled").at_operator(17, PipelineOperation::Pull));
    for original in originals {
        let state = Arc::new(RuntimeErrorState::default());
        let control = RuntimeKernelControl::new(state.clone());
        assert!(ExprEvaluationControl::checkpoint(&control).is_ok());
        assert!(ExprEvaluationControl::wait(&control, Duration::ZERO).is_ok());
        state.set_error(original.clone());
        state.set_error("later error must not replace the first cause");
        assert_eq!(
            ExprEvaluationControl::checkpoint(&control).unwrap_err(),
            original
        );
        assert_eq!(
            ExprEvaluationControl::wait(&control, Duration::ZERO).unwrap_err(),
            original
        );
    }
}
