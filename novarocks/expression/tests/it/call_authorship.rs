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

//! Public call-author contract: installed control shapes, claims and effect summaries.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression::{
    CallAuthorInputs, EvaluationEnvironment, author_call_controls, author_unit_calls,
    testkit::{ClaimAuthor, HarnessControl, add_call, register_call_requests},
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::FunctionArgument;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameters,
};

const POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 16,
    max_array_nodes: 128,
    max_logical_elements: 1024,
    max_retained_buffer_bytes: 1 << 20,
    max_type_depth: 64,
    max_type_nodes: 4096,
    max_dictionary_depth: 64,
    max_metadata_bytes: 1 << 20,
    max_library_validation_work: 1 << 20,
    max_library_validation_bytes: 1 << 20,
};

fn utf8() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, true)
}

#[test]
fn upper_call_is_eager_and_carries_no_source_fact() {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let input = ValueId::new(1);
    let ty = utf8();
    let argument = draft
        .add(None, ty.clone(), ExprKind::Value(input))
        .unwrap();
    let owner = ClaimAuthor::with_exact_eager_binding(
        &functions,
        "upper",
        vec![FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        }],
        POLICY,
        &HarnessControl,
    );
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, vec![argument]);
    draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let decimal = BTreeMap::from([(call, DecimalOverflowPolicy::ReportError)]);
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        constant_policy: POLICY,
        functions: &functions,
        decimal_overflow: &decimal,
    };
    let controls = author_call_controls(&draft, &inputs, &mut work).unwrap();
    assert_eq!(controls.len(), 1);
    assert_eq!(controls[0].0, call);
    assert_eq!(controls[0].1.shape, ControlShape::Eager);
    assert!(controls[0].1.temporal_sources.is_none());
    for (id, control) in controls {
        draft.definition_control(id, control).unwrap();
    }
    let sealed = draft
        .seal(
            vec![ExprPort {
                values: vec![(input, ty)].into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let authored = author_unit_calls(&sealed, &inputs, &mut work).unwrap();
    assert_eq!(authored.claims.len(), 1);
    let claim = authored.claims.values().next().unwrap();
    assert_eq!(claim.decimal_overflow_policy, DecimalOverflowPolicy::ReportError);
    assert!(claim.regexp_count_pattern_source.is_none());
    assert!(claim.to_base64_byte_source.is_none());
    assert!(claim.temporal_source.is_none());
    assert_eq!(authored.summaries.len(), sealed.uses().len());
}

#[test]
fn missing_overflow_policy_is_an_invariant() {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let ty = utf8();
    let argument = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    let owner = ClaimAuthor::with_exact_eager_binding(
        &functions,
        "upper",
        vec![FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        }],
        POLICY,
        &HarnessControl,
    );
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, vec![argument]);
    draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let empty = BTreeMap::new();
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        constant_policy: POLICY,
        functions: &functions,
        decimal_overflow: &empty,
    };
    for (id, control) in author_call_controls(&draft, &inputs, &mut work).unwrap() {
        draft.definition_control(id, control).unwrap();
    }
    let sealed = draft
        .seal(
            vec![ExprPort {
                values: vec![(ValueId::new(1), ty)].into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let error = match author_unit_calls(&sealed, &inputs, &mut work) {
        Err(error) => error,
        Ok(_) => panic!("missing overflow policy was accepted"),
    };
    let text = error.to_string();
    assert!(
        text.contains("decimal overflow policy"),
        "unexpected refusal text: {text}"
    );
}
