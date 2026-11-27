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

//! The original RAND call, literal backing and exact ordered input port.

use super::{
    ClaimAuthor, HarnessControl, add_call, compile_fixture_unit, declare_call_controls,
    finish_unit, fixture_scope, register_call_requests, register_constant,
};
use crate::{CompiledExprSet, EvaluationEnvironment, ScopeLayouts};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_constant_contract::{
    ConstantPoolId, ConstantPools, ConstantValidation, ConstantValue,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    FunctionArgument, KernelEvaluationControl, KernelFailure, testkit::rng_subset,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub const POLICY: ConstantValidation = novarocks_constant_contract::ConstantValidation::bounded(
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

pub const UNIT_LIMITS: UnitLimits = UnitLimits {
    expression_semantic_depth: 256,
};

pub struct RuntimeControl;

impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("compiled RAND expression must not wait")
    }
}

#[derive(Clone, Copy)]
pub enum BoundMode {
    Input,
    DirectConstant,
}

impl BoundMode {
    fn bound_type(self) -> FunctionValueType {
        FunctionValueType::new(DataType::Int64, matches!(self, Self::Input))
    }

    fn bound_constant(self, pools: &mut ConstantPools) -> Option<ConstantValue> {
        if matches!(self, Self::Input) {
            return None;
        }
        let ty = self.bound_type();
        let value = ConstantValue::from_i64(
            Arc::new(ty.try_to_field("fixture").unwrap()),
            ty,
            1,
            POLICY,
            CompilePhase::FunctionSpecialization,
            &HarnessControl,
        )
        .unwrap();
        // Keep one original backing for the physical literal and the first
        // resolution request. Its table address precedes source admissions.
        pools
            .insert(ConstantPoolId::new(u32::MAX), value.pool().clone())
            .unwrap();
        Some(value)
    }
}

fn i64_source(pools: &mut ConstantPools, ty: &FunctionValueType, value: i64) -> ExprKind {
    let value = ConstantValue::from_i64(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        POLICY,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    ExprKind::Constant(register_constant(pools, &value))
}

pub fn bool_source(pools: &mut ConstantPools, ty: &FunctionValueType, value: bool) -> ExprKind {
    let value = ConstantValue::from_boolean(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        value,
        POLICY,
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    ExprKind::Constant(register_constant(pools, &value))
}

fn source_columns(
    mode: BoundMode,
    dictionary: bool,
    pools: &mut ConstantPools,
) -> Vec<(ValueId, FunctionValueType, ExprKind)> {
    let extra = FunctionValueType::new(DataType::Int64, false);
    let bound = mode.bound_type();
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let mut columns = vec![
        (
            ValueId::new(0),
            extra.clone(),
            i64_source(pools, &extra, 999),
        ),
        (
            ValueId::new(100),
            bound.clone(),
            i64_source(pools, &bound, 1),
        ),
        (
            ValueId::new(7),
            boolean.clone(),
            bool_source(pools, &boolean, true),
        ),
    ];
    if dictionary {
        let ty = FunctionValueType::new(
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let null = ConstantValue::null(
            Arc::new(ty.try_to_field("source").unwrap()),
            ty.clone(),
            POLICY,
            CompilePhase::Validate,
            &HarnessControl,
        )
        .unwrap();
        columns.push((
            ValueId::new(400),
            ty,
            ExprKind::Constant(register_constant(pools, &null)),
        ));
    }
    columns
}

pub fn rng_unit(mode: BoundMode, twin: bool, dictionary: bool) -> Arc<CompiledExprSet> {
    let functions = rng_subset();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let constant = mode.bound_constant(&mut constants);
    let columns = source_columns(mode, dictionary, &mut constants);
    let port = ExprPort {
        values: columns.into_iter().map(|(id, ty, _)| (id, ty)).collect(),
    };
    let schema = Arc::new(Schema::new(
        port.values
            .iter()
            .enumerate()
            .map(|(ordinal, (_, ty))| {
                Field::new(
                    format!("local_1_{ordinal}"),
                    ty.typed_binding().layout().data_type().clone(),
                    ty.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ));
    let layout = fixture_scope(&port, schema);
    let bound = ValueId::new(100);
    let bound_type = mode.bound_type();
    let argument = match &constant {
        Some(value) => ExprKind::Constant(register_constant(&mut constants, value)),
        None => ExprKind::Value(bound),
    };
    let owner = ClaimAuthor::with_legacy_binding(
        &functions,
        "rand",
        vec![FunctionArgument::Value {
            value_type: bound_type.clone(),
            constant,
        }],
        ControlShape::Eager,
        None,
        POLICY,
        &HarnessControl,
    )
    .with_decimal_policy(DecimalOverflowPolicy::OutputNull);
    let mut draft = BoundExprSetBuilder::new(UNIT_LIMITS);
    let argument = draft.add(None, bound_type.clone(), argument).unwrap();
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, vec![argument]);
    register_call_requests(&mut draft, &authors, &mut constants);
    draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let identity_a = draft
        .add(None, bound_type.clone(), ExprKind::Value(bound))
        .unwrap();
    let identity_b = draft.add(None, bound_type, ExprKind::Value(bound)).unwrap();
    if twin {
        draft
            .root(call, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    for definition in [identity_a, identity_b] {
        draft
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut draft, &functions, &constants, &parameters, &mut work);
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        &functions,
        sealed,
        &authors,
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
            ports: std::slice::from_ref(&layout),
            all_ports: Some(&layout),
        },
        POLICY,
        &functions,
    )
}

#[cfg(test)]
pub(crate) fn source_unit() -> Arc<CompiledExprSet> {
    let functions = rng_subset();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let mode = BoundMode::DirectConstant;
    let _bound_constant = mode.bound_constant(&mut constants);
    let mut draft = BoundExprSetBuilder::new(UNIT_LIMITS);
    let definitions = source_columns(mode, false, &mut constants)
        .into_iter()
        .map(|(_, ty, kind)| draft.add(None, ty, kind).unwrap())
        .collect::<Vec<_>>();
    for definition in definitions {
        draft
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let port = ExprPort {
        values: Box::default(),
    };
    let layout = fixture_scope(&port, Arc::new(Schema::empty()));
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(vec![port], &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&layout),
            all_ports: Some(&layout),
        },
        POLICY,
        &functions,
    )
}
