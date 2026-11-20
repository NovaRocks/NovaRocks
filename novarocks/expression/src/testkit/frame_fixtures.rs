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

use super::{
    ClaimAuthor, HarnessControl, compile_fixture_unit, declare_call_controls, finish_unit,
    fixture_scope, register_call_requests,
};
use crate::{CompiledExprSet, EvaluationEnvironment, ScopeLayouts};
use arrow::datatypes::{Field, Schema, SchemaRef};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure, PureEngineFunctionCatalog};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub const POLICY: ConstantPolicy = ConstantPolicy {
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

pub struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("guarded numeric calls must not wait")
    }
}

pub struct FrameFixture {
    pub unit: Arc<CompiledExprSet>,
    pub root: RootOrdinal,
    pub schema: SchemaRef,
}

pub(super) fn draft() -> BoundExprSetBuilder {
    BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    })
}

pub(super) fn finish_frame(
    functions: &PureEngineFunctionCatalog,
    mut draft: BoundExprSetBuilder,
    root: (ExprId, EvaluationDemand),
    columns: Vec<(ValueId, FunctionValueType)>,
    parameters: SemanticParameters,
    mut constants: ConstantPools,
    authors: BTreeMap<ExprId, ClaimAuthor>,
) -> FrameFixture {
    let (expression, demand) = root;
    let root = draft.root(expression, demand, RootScope::Port(0)).unwrap();
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(ordinal, (_, ty))| {
                let name = if demand == EvaluationDemand::TruthOnly {
                    format!("original_input_{ordinal}")
                } else {
                    format!("local_1_{ordinal}")
                };
                Field::new(name, ty.data_type().clone(), ty.nullable())
            })
            .collect::<Vec<_>>(),
    ));
    let port = ExprPort {
        values: columns.into_boxed_slice(),
    };
    let scope = fixture_scope(&port, schema.clone());
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    declare_call_controls(
        &mut draft,
        functions,
        &constants,
        &parameters,
        POLICY,
        &mut work,
    );
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        functions,
        sealed,
        &authors,
        &constants,
        &parameters,
        POLICY,
        &mut work,
    );
    work.finish().unwrap();
    let unit = compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        POLICY,
        functions,
    );
    FrameFixture { unit, root, schema }
}

#[cfg(novarocks_dev_controls)]
pub mod array;
pub mod guarded;
