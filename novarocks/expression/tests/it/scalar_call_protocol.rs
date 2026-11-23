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
    array::{Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray},
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
    AggregateStateAllocator, EvaluatedArgument, FunctionArgument,
    InvocationScalarEvaluationInstance, KernelFailure, PreparedPureKernel,
    ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, convert::Infallible, sync::Arc};

#[derive(Debug, Eq, PartialEq)]
enum Event {
    Begin,
    Admit(ScalarCallPhase, ScalarCallRequestFacts),
    Callback,
}

#[derive(Default)]
struct RecordingHost {
    events: Vec<Event>,
    callback: ScalarCallRequestFacts,
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
        self.events.push(Event::Callback);
        Ok(self.callback)
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
        .map(|(id, ty)| draft.add(None, ty.clone(), ExprKind::Value(*id)).unwrap())
        .collect();
    let owner = ClaimAuthor::with_exact_eager_binding(
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
    declare_call_controls(
        &mut draft,
        &functions,
        &constants,
        &parameters,
        policy,
        &mut work,
    );
    let sealed = draft.seal(vec![port.clone()], &mut work).unwrap();
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        policy,
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
fn whole_invocation_admits_prefix_then_callback_and_body() {
    let (program, root, batch) = fixture(
        "json_object",
        vec![
            Arc::new(StringArray::from(vec!["key"])),
            Arc::new(StringArray::from(vec!["value"])),
        ],
    );
    let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(CountingAllocator::new(usize::MAX));
    let use_id = program.root(root).unwrap().use_id;
    let PreparedPureKernel::ScalarInvocation(prepared) = program.calls()[&use_id].prepared() else {
        panic!("expected the actual invocation owner");
    };
    let instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(allocator.clone()),
    )
    .unwrap();
    let profile = instance.invocation_resource_profile().unwrap();
    let prefix = profile.preparation_prefix();
    let arguments: Vec<_> = batch
        .columns()
        .iter()
        .map(EvaluatedArgument::Column)
        .collect();
    let body = profile
        .requests_for(
            &instance,
            Selection::all(1),
            &arguments,
            ScalarInvocationActivation::Activated,
            &HarnessControl,
        )
        .unwrap();
    assert!(body.requires_host_callback());
    let mut host = RecordingHost {
        callback: ScalarCallRequestFacts::new(32, 2),
        ..RecordingHost::default()
    };
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        program,
        root,
        &HarnessControl,
        Some(allocator),
    )
    .unwrap();
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
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        r#"{"key": "value"}"#
    );
    assert_eq!(
        host.events,
        vec![
            Event::Begin,
            Event::Admit(
                ScalarCallPhase::Preparation,
                ScalarCallRequestFacts::new(prefix.bytes(), prefix.requests())
            ),
            Event::Callback,
            Event::Admit(
                ScalarCallPhase::Body,
                ScalarCallRequestFacts::new(body.bytes() + 32, body.requests() + 2)
            ),
        ]
    );
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
fn invocation_data_keeps_its_original_domain_and_failed_reentry() {
    let (program, root, batch) = fixture(
        "json_object",
        vec![
            Arc::new(StringArray::from(vec!["key"])),
            Arc::new(Date32Array::from(vec![-1000000000])),
        ],
    );
    let mut frame = CompiledExpressionInstance::try_new_with_allocator(
        program,
        root,
        &HarnessControl,
        Some(Arc::new(CountingAllocator::new(usize::MAX))),
    )
    .unwrap();
    let mut host = RecordingHost::default();
    let original = frame
        .evaluate(
            input(&batch),
            complete_batch(&batch),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut host,
        )
        .unwrap_err();
    let ExprEvaluationFailure::Data(data) = original else {
        panic!("original typed Data")
    };
    assert_eq!(
        data.message(),
        "json_object invalid date32 day value: -1000000000"
    );
    assert_eq!((data.batch_rows(), data.source_row(0)), (1, Some(0)));
    host.events.clear();
    assert_eq!(
        frame
            .evaluate(
                input(&batch),
                complete_batch(&batch),
                ScalarInvocationActivation::Activated,
                &HarnessControl,
                &mut host
            )
            .unwrap_err(),
        ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(host.events.is_empty());
}

#[test]
fn a_profiled_owner_refuses_the_explicit_actual_domain_before_admission() {
    use novarocks_expression::testkit::call_scoped_invocation;
    use novarocks_functions::{EvaluatedSourceLoan, KernelDiagnostic, ScalarInvocationSource};
    let (program, root, batch) = fixture(
        "json_object",
        vec![
            Arc::new(StringArray::from(vec!["key"])),
            Arc::new(StringArray::from(vec!["value"])),
        ],
    );
    let use_id = program.root(root).unwrap().use_id;
    let PreparedPureKernel::ScalarInvocation(prepared) = program.calls()[&use_id].prepared() else {
        panic!("expected the actual invocation owner");
    };
    let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared.clone(),
        Some(Arc::new(CountingAllocator::new(usize::MAX))),
    )
    .unwrap();
    assert!(instance.invocation_resource_profile().is_some());
    let sources: Vec<_> = batch
        .columns()
        .iter()
        .map(|value| {
            ScalarInvocationSource::Array(EvaluatedSourceLoan::new(value, Selection::all(1)))
        })
        .collect();
    let mut host = RecordingHost::default();
    let cause = call_scoped_invocation(
        &mut host,
        &mut instance,
        complete_batch(&batch),
        &sources,
        ScalarInvocationActivation::Activated,
        &HarnessControl,
    )
    .unwrap_err();
    assert_eq!(
        cause,
        ScalarCallFailure::Kernel(KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "scoped invocation has no installed measurement source transport"
        ),))
    );
    assert_eq!(host.events, vec![Event::Begin]);
    assert!(!instance.is_failed());
}
