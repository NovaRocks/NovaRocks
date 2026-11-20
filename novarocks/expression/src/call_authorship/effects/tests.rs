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

use super::*;
use crate::{
    EvaluationEnvironment, author_call_controls,
    testkit::{
        ClaimAuthor, HarnessControl, add_call, constant_policy, register_call_requests,
        register_constant,
    },
};
use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantPools, ConstantValue};
use novarocks_expression_contract::{BoundExprSetBuilder, RootScope, UnitLimits};
use novarocks_functions::FunctionArgument;
use novarocks_type_contract::{DecimalOverflowPolicy, EvaluationDemand, FunctionValueType};

#[test]
fn missing_or_foreign_guarded_child_summary_is_refused() {
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let policy = constant_policy();
    let parameters = SemanticParameters::default();
    let ty = FunctionValueType::new(DataType::Boolean, false);
    let mut constants = ConstantPools::empty();
    let value = ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty.clone(),
        true,
        policy,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    let reference = register_constant(&mut constants, &value);
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let leaf = draft
        .add(None, ty.clone(), ExprKind::Constant(reference))
        .unwrap();
    let mut authors = BTreeMap::new();
    let call = add_call(
        &mut draft,
        &mut authors,
        ClaimAuthor::with_legacy_binding(
            &functions,
            "if",
            vec![
                FunctionArgument::Value {
                    value_type: ty,
                    constant: None
                };
                3
            ],
            ControlShape::If,
            None,
            policy,
            &HarnessControl,
        ),
        vec![leaf; 3],
    );
    draft
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let decimal = BTreeMap::from([(call, DecimalOverflowPolicy::ReportError)]);
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        constant_policy: policy,
        functions: &functions,
        decimal_overflow: &decimal,
    };
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    for (id, shape) in author_call_controls(&draft, &inputs, &mut work).unwrap() {
        draft.definition_control(id, shape).unwrap();
    }
    let unit = draft.seal(Vec::new(), &mut work).unwrap();
    let root = unit.root_use_ids()[0];
    let invocation = &unit.uses()[&root];
    assert_eq!(invocation.control, ControlShape::If);
    let guarded = invocation.arguments[1];
    assert_ne!(
        unit.uses()[&guarded].context.region,
        invocation.context.region
    );
    let requests = materialize_requests(&unit, &inputs, &mut work).unwrap();
    let mut summaries = BTreeMap::new();
    for &child in &invocation.arguments {
        summaries.insert(
            child,
            ScopedExpressionEffects::pure_value(unit.uses()[&child].context),
        );
    }

    let run = |summaries: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>| {
        author_call(
            &unit,
            &inputs,
            unit.definition(call).unwrap(),
            root,
            &requests,
            summaries,
            &mut CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap(),
            &mut BTreeMap::new(),
        )
    };
    assert!(run(&summaries).is_ok());
    let original = summaries.remove(&guarded).unwrap();
    let error = run(&summaries).unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("package call effects: Scalar(MissingChildEffects({guarded:?}))"),
    );
    summaries.insert(
        guarded,
        ScopedExpressionEffects::pure_value(invocation.context),
    );
    assert_eq!(
        run(&summaries).unwrap_err().to_string(),
        "package call effects: Effects(ProofScopeMismatch)",
    );
    summaries.insert(guarded, original);
    assert!(run(&summaries).is_ok());
    work.finish().unwrap();
}
