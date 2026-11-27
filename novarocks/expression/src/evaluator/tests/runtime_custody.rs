// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Observe private call instances independently of execution's funding implementation.
use super::super::scalar_invocation::CallInstance;
use crate::testkit::{
    ClaimAuthor, HarnessControl as Control, PassThroughCallHost, add_call, compile_fixture_unit,
    declare_call_controls, finish_unit, fixture_scope,
    guarded_fixtures::{fixture_constant_policy, unit_draft},
    register_call_requests,
};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, CompiledFilterConjunctionInstance,
    EvaluationEnvironment, EvaluationInput, ExprEvaluationFailure, ScalarCallFailure,
    ScalarCallHost, ScalarCallPhase, ScalarCallRequestFacts, ScopeLayouts,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootScope, ValueId,
};
use novarocks_functions::{
    EvaluatedArgument, FunctionArgument, PreparedPureKernel, PureEngineFunctionCatalog,
    ScalarInvocationActivation, ScalarInvocationScope, Selection, builtin::json_sql::JsonFunction,
    testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, EvaluationDemand, FunctionValueType,
    SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

fn assert_selected_json_parse(unit: &CompiledExprSet) {
    let parse = JsonFunction::Parse.function_id();
    assert!(unit.calls().values().any(|call| matches!(
        call.prepared(),
        PreparedPureKernel::Scalar(kernel) if kernel.contract().function_id() == &parse,
    )));
}

fn nullable_port(fields: &[(&str, DataType)]) -> (ExprPort, Arc<Schema>) {
    let values = fields
        .iter()
        .enumerate()
        .map(|(ordinal, (_, carrier))| {
            (
                ValueId::new(u32::try_from(ordinal + 1).unwrap()),
                FunctionValueType::new(carrier.clone(), true),
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, carrier)| Field::new(*name, carrier.clone(), true))
            .collect::<Vec<_>>(),
    ));
    (ExprPort { values }, schema)
}

fn input(builder: &mut BoundExprSetBuilder, column: &(ValueId, FunctionValueType)) -> ExprId {
    builder
        .add(None, column.1.clone(), ExprKind::Value(column.0))
        .unwrap()
}

fn argument(ty: &FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty.clone(),
        constant: None,
    }
}

fn compile(
    mut builder: BoundExprSetBuilder,
    port: ExprPort,
    schema: Arc<Schema>,
    catalogue: &PureEngineFunctionCatalog,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
) -> Arc<CompiledExprSet> {
    let scope = fixture_scope(&port, schema);
    let mut constants = ConstantPools::empty();
    let parameters = SemanticParameters::try_new([]).unwrap();
    register_call_requests(&mut builder, authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut builder, catalogue, &constants, &parameters, &mut work);
    let sealed = builder.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        catalogue,
        sealed,
        authors,
        &constants,
        &parameters,
        &mut work,
    );
    work.finish().unwrap();
    compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: None,
        },
        fixture_constant_policy(),
        catalogue,
    )
}

#[derive(Debug, Eq, PartialEq)]
enum Refusal {
    MissingMemory,
}

struct RejectScalarHost;

impl ScalarCallHost for RejectScalarHost {
    type Refusal = Refusal;

    fn begin_call(&mut self) {}

    fn run_admitted(
        &mut self,
        phase: ScalarCallPhase,
        _facts: ScalarCallRequestFacts,
        _body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Refusal>>,
    ) -> Result<(), ScalarCallFailure<Refusal>> {
        assert_eq!(phase, ScalarCallPhase::Body);
        Err(ScalarCallFailure::Host(Refusal::MissingMemory))
    }

    fn callback_request_facts(
        &mut self,
        _allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Refusal>> {
        panic!("ordinary scalar call has no whole-invocation callback")
    }
}

#[test]
fn runtime_scalar_memory_required_helper_missing_binding_fails_root_without_scalar_call() {
    let catalogue = installed_builtin_owner_catalogue();
    let (port, schema) = nullable_port(&[
        ("local_1_0", DataType::Int64),
        ("local_1_1", DataType::Int32),
        ("local_1_2", DataType::Boolean),
    ]);
    let mut builder = unit_draft();
    let value = input(&mut builder, &port.values[0]);
    let count = input(&mut builder, &port.values[1]);
    let mut authors = BTreeMap::new();
    let owner = ClaimAuthor::with_legacy_binding(
        &catalogue,
        "bitwise_left_shift",
        vec![argument(&port.values[0].1), argument(&port.values[1].1)],
        ControlShape::Eager,
        None,
        fixture_constant_policy(),
        &Control,
    );
    let call = add_call(&mut builder, &mut authors, owner, vec![value, count]);
    let root = builder
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile(builder, port, schema.clone(), &catalogue, &authors);
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
    let counts: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            values.clone(),
            counts.clone(),
            Arc::new(BooleanArray::from(vec![Some(true)])),
        ],
    )
    .unwrap();
    let mut actual = CompiledExpressionInstance::try_new(unit, root, &Control).unwrap();
    let selection = Selection::all(1);
    let result = actual.evaluate(
        EvaluationInput::Batch {
            batch: &batch,
            source: None,
        },
        ScalarInvocationScope::new(selection, selection),
        ScalarInvocationActivation::Activated,
        &Control,
        &mut RejectScalarHost,
    );
    assert_eq!(
        result.unwrap_err(),
        ExprEvaluationFailure::Host(Refusal::MissingMemory)
    );
    assert!(actual.failed);
    let call = actual.instances.values_mut().next().unwrap();
    let CallInstance::Kernel(instance) = call else {
        panic!("original ScalarV1");
    };
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&counts),
    ];
    // The scalar owner remains uncalled, while its enclosing root is failed.
    instance
        .evaluate(Selection::all(1), &args, &Control)
        .unwrap();
}

#[test]
fn runtime_json_parse_empty_demand_constructs_no_call_instance() {
    let catalogue = installed_builtin_owner_catalogue();
    let (port, schema) = nullable_port(&[("v", DataType::Utf8)]);
    let mut builder = unit_draft();
    let value = input(&mut builder, &port.values[0]);
    let mut authors = BTreeMap::new();
    let owner = ClaimAuthor::with_exact_eager_binding(
        &catalogue,
        "json_parse",
        vec![argument(&port.values[0].1)],
        fixture_constant_policy(),
        &Control,
    );
    let call = add_call(&mut builder, &mut authors, owner, vec![value]);
    let root = builder
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile(builder, port, schema.clone(), &catalogue, &authors);
    assert_selected_json_parse(&unit);
    let full = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["[1,2]", "{]", "null"]))],
    )
    .unwrap();
    let empty = full.slice(0, 0);
    let mut actual = CompiledExpressionInstance::try_new(unit, root, &Control).unwrap();
    let selection = Selection::all(0);
    let output = actual
        .evaluate(
            EvaluationInput::Batch {
                batch: &empty,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::ValidateOnly,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.values().is_empty());
    assert!(actual.instances.is_empty());
}

#[test]
fn runtime_json_parse_ordered_filter_suppresses_unused_call_before_instance_creation() {
    let catalogue = installed_builtin_owner_catalogue();
    let (port, schema) = nullable_port(&[("flag", DataType::Boolean), ("v", DataType::Utf8)]);
    let mut builder = unit_draft();
    let flag = input(&mut builder, &port.values[0]);
    let value = input(&mut builder, &port.values[1]);
    let mut authors = BTreeMap::new();
    let json_owner = ClaimAuthor::with_exact_eager_binding(
        &catalogue,
        "json_parse",
        vec![argument(&port.values[1].1)],
        fixture_constant_policy(),
        &Control,
    );
    let json = add_call(&mut builder, &mut authors, json_owner, vec![value]);
    let condition = builder
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: json,
                negated: false,
            },
        )
        .unwrap();
    let roots = [flag, condition].map(|expr| {
        builder
            .root(expr, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap()
    });
    let unit = compile(builder, port, schema.clone(), &catalogue, &authors);
    assert_selected_json_parse(&unit);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![false; 3])),
            Arc::new(StringArray::from(vec!["{]"; 3])),
        ],
    )
    .unwrap();
    let mut actual = CompiledFilterConjunctionInstance::try_new_with_allocator(
        &roots.map(|root| (unit.clone(), root)),
        &Control,
        None,
    )
    .unwrap();
    let output = actual
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            Selection::all(3),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false); 3],
    );
    assert_eq!(actual.constructed_root_call_instances(1), 0);
}
