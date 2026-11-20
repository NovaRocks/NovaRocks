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
    datatypes::{Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, ScopeLayouts,
    testkit::{
        ClaimAuthor, HarnessControl, compile_fixture_unit, declare_call_controls, finish_unit,
        fixture_scope, register_call_requests,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::PureEngineFunctionCatalog;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

pub(super) const COMPILE_POLICY: ConstantPolicy = ConstantPolicy {
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

pub(super) struct StringFixture {
    pub(super) unit: Arc<CompiledExprSet>,
    pub(super) root: RootOrdinal,
    pub(super) schema: SchemaRef,
}
impl StringFixture {
    pub(super) fn instance(&self) -> CompiledExpressionInstance {
        CompiledExpressionInstance::try_new(self.unit.clone(), self.root, &HarnessControl).unwrap()
    }

    pub(super) fn batch(&self, arrays: Vec<arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(self.schema.clone(), arrays).unwrap()
    }
}

pub(super) fn string_fixture(
    functions: &PureEngineFunctionCatalog,
    inputs: &[(ValueId, String, FunctionValueType)],
    build: impl FnOnce(
        &mut BoundExprSetBuilder,
        &mut ConstantPools,
        &mut BTreeMap<ExprId, ClaimAuthor>,
    ) -> ExprId,
) -> StringFixture {
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let mut constants = ConstantPools::empty();
    let mut authors = BTreeMap::new();
    let expression = build(&mut draft, &mut constants, &mut authors);
    let root = draft
        .root(expression, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let schema = Arc::new(Schema::new(
        inputs
            .iter()
            .map(|(_, name, ty)| Field::new(name, ty.data_type().clone(), ty.nullable()))
            .collect::<Vec<_>>(),
    ));
    let port = ExprPort {
        values: inputs.iter().map(|(id, _, ty)| (*id, ty.clone())).collect(),
    };
    let scope = fixture_scope(&port, schema.clone());
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    declare_call_controls(
        &mut draft,
        functions,
        &constants,
        &parameters,
        COMPILE_POLICY,
        &mut work,
    );
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let neutral = finish_unit(
        functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        COMPILE_POLICY,
        &mut work,
    );
    work.finish().unwrap();
    let unit = compile_fixture_unit(
        &neutral,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        COMPILE_POLICY,
        functions,
    );
    StringFixture { unit, root, schema }
}
