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

use arrow::{
    array::{Array, ArrayRef, Int32Array, Int64Array, LargeBinaryArray, StringArray},
    datatypes::Schema,
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprEvaluationFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts, ScopeLayouts,
    testkit::{
        ClaimAuthor, CountingAllocator, HarnessControl, add_call, compile_fixture_unit,
        constant_policy, declare_call_controls, finish_unit, fixture_scope, register_call_requests,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    AggregateStateAllocator, DataFailureReason, EvaluatedArgument, FunctionArgument, KernelFailure,
    PreparedPureKernel, ScalarInvocationActivation, ScalarInvocationScope, Selection,
    json_value::{Kind, Node},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, convert::Infallible, sync::Arc};

#[derive(Debug, Eq, PartialEq)]
enum Event {
    Begin,
    Admit(ScalarCallPhase, ScalarCallRequestFacts),
}

#[derive(Default)]
struct RecordingHost {
    events: Vec<Event>,
}

impl ScalarCallHost for RecordingHost {
    type Refusal = Infallible;

    fn begin_call(&mut self) {
        self.events.push(Event::Begin);
    }

    fn run_admitted(
        &mut self,
        phase: ScalarCallPhase,
        facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
        self.events.push(Event::Admit(phase, facts));
        body()
    }

    fn callback_request_facts(
        &mut self,
        _: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
        panic!("these installed scalar owners have no invocation callback")
    }
}

fn fixture(name: &str, values: Vec<ArrayRef>) -> (Arc<CompiledExprSet>, RootOrdinal, RecordBatch) {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let policy = constant_policy();
    let types: Vec<_> = values
        .iter()
        .map(|column| FunctionValueType::new(column.data_type().clone(), true))
        .collect();
    let port = ExprPort {
        values: types
            .iter()
            .enumerate()
            .map(|(index, ty)| (ValueId::new(u32::try_from(index).unwrap()), ty.clone()))
            .collect(),
    };
    let schema = Arc::new(Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(index, ty)| ty.try_to_field(format!("input_{index}")).unwrap())
            .collect::<Vec<_>>(),
    ));
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    let arguments = port
        .values
        .iter()
        .map(|(id, ty)| draft.add(None, ty.clone(), ExprKind::input(*id)).unwrap())
        .collect();
    let owner = ClaimAuthor::with_exact_ordinary_binding(
        &functions,
        name,
        types
            .into_iter()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect(),
        policy,
        &HarnessControl,
    );
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, arguments);
    let root = draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let mut constants = ConstantPools::empty();
    let parameters = SemanticParameters::try_new([]).unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut draft, &functions, &constants, &parameters, &mut work);
    let sealed = draft.seal(vec![port.clone()], &mut work).unwrap();
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        &mut work,
    );
    work.finish().unwrap();
    let compiled = compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[fixture_scope(&port, schema.clone())],
            all_ports: None,
        },
        policy,
        &functions,
    );
    (
        compiled,
        root,
        RecordBatch::try_new(schema, values).unwrap(),
    )
}

fn input(batch: &RecordBatch) -> EvaluationInput<'_> {
    EvaluationInput::Batch {
        batch,
        source: None,
    }
}

fn complete_batch(batch: &RecordBatch) -> ScalarInvocationScope<'_> {
    let rows = Selection::all(batch.num_rows());
    ScalarInvocationScope::new(rows, rows)
}

#[test]
fn profiled_scalar_admits_the_body_with_the_original_requests() {
    let (program, root, batch) = fixture(
        "bitwise_left_shift",
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 1])),
        ],
    );
    let use_id = program.root(root).unwrap().use_id;
    let PreparedPureKernel::Scalar(scalar) = program.calls()[&use_id].prepared() else {
        panic!("expected the actual scalar owner");
    };
    let instance =
        novarocks_functions::ScalarEvaluationInstance::instantiate(scalar.clone()).unwrap();
    let arguments: Vec<_> = batch
        .columns()
        .iter()
        .map(EvaluatedArgument::Column)
        .collect();
    let requests = instance
        .invocation_resource_profile()
        .unwrap()
        .requests_for(Selection::all(2), &arguments)
        .unwrap();
    let expected = ScalarCallRequestFacts::new(requests.bytes(), requests.requests());
    let mut frame = CompiledExpressionInstance::try_new(program, root, &HarnessControl).unwrap();
    let mut host = RecordingHost::default();
    let out = frame
        .evaluate(
            input(&batch),
            complete_batch(&batch),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap();
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[4, 4]
    );
    assert_eq!(
        host.events,
        vec![Event::Begin, Event::Admit(ScalarCallPhase::Body, expected)]
    );
}

#[test]
fn native_json_scalar_keeps_its_result_grant_after_the_call_frame_exits() {
    let (program, root, batch) = fixture(
        "json_parse",
        vec![Arc::new(StringArray::from(vec!["null"]))],
    );
    let allocator = Arc::new(CountingAllocator::new(usize::MAX));
    let mut host = RecordingHost::default();
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        program,
        root,
        &HarnessControl,
        Some(allocator.clone()),
    )
    .unwrap();
    let output = frame
        .evaluate(
            input(&batch),
            complete_batch(&batch),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap();
    let json = output
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(matches!(
        Node::from_root(json.value(0)).unwrap().kind().unwrap(),
        Kind::Null
    ));
    assert_eq!(host.events, vec![Event::Begin]);
    let alias = json.values().clone();
    drop(output);
    drop(frame);
    assert!(allocator.outstanding() > 0);
    drop(alias);
    assert_eq!(allocator.outstanding(), 0);
}

#[test]
fn unprofiled_scalar_only_observes_the_call_start() {
    let (program, root, batch) = fixture("abs", vec![Arc::new(Int64Array::from(vec![-5]))]);
    let mut frame = CompiledExpressionInstance::try_new(program, root, &HarnessControl).unwrap();
    let mut host = RecordingHost::default();
    let out = frame
        .evaluate(
            input(&batch),
            complete_batch(&batch),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap();
    let value = out
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("BIGINT ABS retains the BIGINT carrier");
    assert_eq!(value.value(0), 5);
    assert_eq!(host.events, vec![Event::Begin]);
}

#[test]
fn selected_json_data_keeps_its_original_domain_and_allows_successful_reentry() {
    let (program, root, batch) = fixture(
        "json_parse",
        vec![Arc::new(StringArray::from(vec!["{]", "{]", "{]", "null"]))],
    );
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        program,
        root,
        &HarnessControl,
        Some(Arc::new(CountingAllocator::new(usize::MAX))),
    )
    .unwrap();
    let mut host = RecordingHost::default();
    let actual = Selection::all(4);
    let selected = Selection::try_sparse(4, &[1, 3]).unwrap();
    let output = frame
        .evaluate(
            input(&batch),
            ScalarInvocationScope::new(actual, selected),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap();
    let [error] = output.errors() else {
        panic!("only the malformed selected row returns a row error");
    };
    assert_eq!(output.selection().row(error.selected_ordinal()), Some(1));
    assert_eq!(
        error.reason(),
        DataFailureReason::Domain(
            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
        )
    );
    assert_eq!(error.message(), "Invalid JSON input");
    assert!(output.values().is_null(0));
    assert!(output.values().is_valid(1));

    let selected = Selection::try_sparse(4, &[3]).unwrap();
    let output = frame
        .evaluate(
            input(&batch),
            ScalarInvocationScope::new(actual, selected),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    assert!(output.values().is_valid(0));
    assert_eq!(host.events, vec![Event::Begin, Event::Begin]);
}

#[test]
fn a_profiled_scalar_rejects_demand_outside_the_actual_domain_before_admission() {
    let (program, root, batch) = fixture(
        "bitwise_left_shift",
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![2, 1])),
        ],
    );
    let mut frame = CompiledExpressionInstance::try_new(program, root, &HarnessControl).unwrap();
    let actual = Selection::try_sparse(2, &[0]).unwrap();
    let outside = Selection::try_sparse(2, &[1]).unwrap();
    let mut host = RecordingHost::default();
    let failure = frame
        .evaluate(
            input(&batch),
            ScalarInvocationScope::new(actual, outside),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap_err();
    assert!(matches!(
        failure,
        ExprEvaluationFailure::Kernel(KernelFailure::InvalidProgram(_))
    ));
    assert!(host.events.is_empty());
    assert_eq!(
        frame
            .evaluate(
                input(&batch),
                complete_batch(&batch),
                ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut host,
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(host.events.is_empty());
}
