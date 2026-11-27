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

//! Byte fixture inputs authored as checked units.

use arrow::{
    array::{ArrayRef, BinaryArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPools, ConstantValidation};
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ScopeLayouts,
    testkit::{
        ClaimAuthor, HarnessControl, add_call, compile_fixture_unit, declare_call_controls,
        finish_unit, fixture_scope, register_call_requests,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionArgument, FunctionKind,
    InstalledPureKernel, PureEngineFunctionCatalog,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc};

pub(super) struct StatefulFixture {
    pub(super) unit: Arc<CompiledExprSet>,
    pub(super) root: RootOrdinal,
}
impl StatefulFixture {
    pub(super) fn schema(&self) -> &SchemaRef {
        self.unit.scope(RootScope::Port(0)).unwrap().schema()
    }

    pub(super) fn batch(&self, arrays: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(self.schema().clone(), arrays).unwrap()
    }
}

// The original FB+LC fixture compiled under this small constant policy.
const POLICY: ConstantValidation = novarocks_constant_contract::ConstantValidation::bounded(
    novarocks_constant_contract::ConstantPolicy {
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
    },
);

fn functions(
    actual: &EngineFunctionCatalog,
    declarations: &[(&str, Option<usize>)],
) -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for &(name, count) in declarations {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        if let Some(count) = count {
            assert_eq!(declaration.overloads().len(), count);
        }
        for overload in declaration.overloads() {
            let entry = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &HarnessControl,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: entry.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|record| record.state_format.clone()),
            });
        }
        builder.register(definition.clone()).unwrap();
    }
    builder.seal_pure(installed).unwrap()
}

fn input_unit(types: &[FunctionValueType]) -> (BoundExprSetBuilder, ExprPort, Vec<ExprId>) {
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let values = types
        .iter()
        .enumerate()
        .map(|(i, ty)| (ValueId::new(91 + u32::try_from(i).unwrap()), ty.clone()))
        .collect::<Box<[_]>>();
    let children = values
        .iter()
        .map(|(value, ty)| {
            draft
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap()
        })
        .collect();
    (draft, ExprPort { values }, children)
}

fn call_author(
    catalogue: &PureEngineFunctionCatalog,
    name: &str,
    args: &[FunctionValueType],
) -> ClaimAuthor {
    ClaimAuthor::with_exact_ordinary_binding(
        catalogue,
        name,
        args.iter()
            .map(|ty| FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            })
            .collect(),
        POLICY,
        &HarnessControl,
    )
}

fn checked_fixture(
    catalogue: &PureEngineFunctionCatalog,
    mut draft: BoundExprSetBuilder,
    port: ExprPort,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    mut constants: ConstantPools,
    result: ExprId,
) -> StatefulFixture {
    let root = draft
        .root(result, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, authors, &mut constants);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut draft, catalogue, &constants, &parameters, &mut work);

    // The original physical input was the first Project after zero-column
    // Values. Those non-result columns were named local_1_{ordinal}.
    let fields = port
        .values
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| {
            Field::new(
                format!("local_1_{i}"),
                ty.data_type().clone(),
                ty.nullable(),
            )
        })
        .collect::<Vec<_>>();
    let scope = fixture_scope(&port, Arc::new(Schema::new(fields)));
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        catalogue,
        sealed,
        authors,
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
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        POLICY,
        catalogue,
    );
    StatefulFixture {
        unit: compiled,
        root,
    }
}

fn direct_call(
    catalogue: &PureEngineFunctionCatalog,
    name: &str,
    inputs: &[FunctionValueType],
    result_type: DataType,
) -> StatefulFixture {
    let (mut draft, port, children) = input_unit(inputs);
    let owner = call_author(catalogue, name, inputs);
    assert_eq!(owner.result().data_type(), &result_type);
    let mut authors = BTreeMap::new();
    let result = add_call(&mut draft, &mut authors, owner, children);
    checked_fixture(
        catalogue,
        draft,
        port,
        &authors,
        ConstantPools::empty(),
        result,
    )
}

pub(super) fn binary_program(name: &str) -> StatefulFixture {
    let actual = build_builtin_engine_function_catalog().unwrap();
    direct_call(
        &functions(&actual, &[(name, Some(2))]),
        name,
        &[FunctionValueType::new(DataType::Utf8, true)],
        DataType::Binary,
    )
}

pub(super) fn binary_values(array: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect()
}

pub(super) const GUARDED_DIAGNOSTICS: [&str; 3] = [
    "guarded-origin-invalid",
    "guarded-origin-internal",
    "guarded-origin-operational",
];
