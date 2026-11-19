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

//! Constant-only inputs evaluated through the production unit compiler.
use arrow::{
    array::ArrayRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprCompileError, ScopeLayouts, compile_expr_set_pure,
    testkit::{HarnessControl, PassThroughCallHost, constant_policy, register_constant},
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, RootOrdinal, RootScope, UnitLimits,
};
use novarocks_functions::{
    ConstantValue, ScalarInvocationActivation, ScalarInvocationScope, SelectedValues, Selection,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

pub(super) fn add_constant(
    draft: &mut BoundExprSetBuilder,
    constants: &mut ConstantPools,
    array: ArrayRef,
    ty: FunctionValueType,
) -> ExprId {
    let value = ConstantValue::from_scalar_array(
        Arc::new(ty.try_to_field("literal").unwrap()),
        ty.clone(),
        array.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    let reference = register_constant(constants, &value);
    draft.add(None, ty, ExprKind::Constant(reference)).unwrap()
}

pub(super) fn primitive(
    arrays: impl IntoIterator<Item = ArrayRef>,
    result_type: FunctionValueType,
    kind: impl FnOnce(Vec<ExprId>) -> ExprKind,
    parameters: &SemanticParameters,
) -> Result<Arc<CompiledExprSet>, ExprCompileError> {
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let mut constants = ConstantPools::empty();
    let inputs = arrays
        .into_iter()
        .map(|array| {
            let ty = FunctionValueType::new(array.data_type().clone(), array.is_nullable());
            add_constant(&mut draft, &mut constants, array, ty)
        })
        .collect::<Vec<_>>();
    let root = draft.add(None, result_type, kind(inputs)).unwrap();
    draft
        .root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(Vec::new(), &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    let mut work =
        CompileCheckpoints::try_new(&HarnessControl, CompilePhase::LowerProgram).unwrap();
    let compiled = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters,
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        constant_policy(),
        &novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        &mut work,
    );
    work.finish_result(compiled, ExprCompileError::from)
}

pub(super) fn evaluate(unit: Arc<CompiledExprSet>) -> SelectedValues<'static> {
    let root = RootOrdinal::new(0);
    let schema = unit.scope(RootScope::AllPorts).unwrap().schema().clone();
    assert!(unit.scopes().is_empty());
    assert!(schema.fields().is_empty());
    let batch = RecordBatch::try_new_with_options(
        schema,
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let mut instance = CompiledExpressionInstance::try_new(unit, root, &HarnessControl).unwrap();
    let selection = Selection::all(1);
    instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &HarnessControl,
            &mut PassThroughCallHost,
        )
        .unwrap()
}
