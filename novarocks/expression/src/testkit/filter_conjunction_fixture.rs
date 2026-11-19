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

//! The ordered TruthOnly units behind the multi-root Filter frame tests.

use super::{
    ClaimAuthor, HarnessControl, add_call, compile_fixture_unit, declare_call_controls,
    finish_unit, fixture_scope, register_call_requests, register_constant,
};
use crate::{
    CompiledExprSet, CompiledFilterConjunctionInstance, EvaluationEnvironment, ScopeLayouts,
};
use arrow::{
    array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools, ConstantValue};
use novarocks_expression_contract::{
    BinaryOperator, BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope,
    UnitLimits, ValueId,
};
use novarocks_functions::{
    FunctionArgument, KernelEvaluationControl, KernelFailure, PureEngineFunctionCatalog,
    testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

const POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 64,
    max_array_nodes: 1024,
    max_logical_elements: 4096,
    max_retained_buffer_bytes: 1 << 20,
    max_type_depth: 64,
    max_type_nodes: 4096,
    max_dictionary_depth: 64,
    max_metadata_bytes: 1 << 20,
    max_library_validation_work: 1 << 20,
    max_library_validation_bytes: 1 << 20,
};
const OUTPUT_NULL: DecimalOverflowPolicy = DecimalOverflowPolicy::OutputNull;
const ALLOW_THROW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(0),
    expected_key: SemanticParameterKey::AllowThrowException,
};

pub struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("these Filter roots do not wait")
    }
}

#[derive(Clone, Copy)]
pub enum FilterShape {
    TwoCounts,
    CountAndRandom,
    DateAndRandom,
}

pub struct Fixture {
    pub unit: Arc<CompiledExprSet>,
    pub roots: [RootOrdinal; 2],
    shape: FilterShape,
}

fn constant(
    draft: &mut BoundExprSetBuilder,
    pools: &mut ConstantPools,
    value: &ConstantValue,
    ty: FunctionValueType,
) -> ExprId {
    let constant = register_constant(pools, value);
    draft.add(None, ty, ExprKind::Constant(constant)).unwrap()
}

fn integer_literal(
    draft: &mut BoundExprSetBuilder,
    pools: &mut ConstantPools,
    value: i32,
) -> ExprId {
    let ty = FunctionValueType::new(DataType::Int32, false);
    let value = ConstantValue::from_i32(
        Arc::new(Field::new("literal", DataType::Int32, false)),
        ty.clone(),
        value,
        POLICY,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    constant(draft, pools, &value, ty)
}

fn cast(draft: &mut BoundExprSetBuilder, expr: ExprId, ty: FunctionValueType) -> ExprId {
    draft
        .add(
            None,
            ty.clone(),
            ExprKind::Cast {
                expr,
                target: ty.data_type().clone(),
                decimal_overflow_policy: OUTPUT_NULL,
                allow_throw_exception: ALLOW_THROW,
            },
        )
        .unwrap()
}

fn comparison(
    draft: &mut BoundExprSetBuilder,
    left: ExprId,
    op: BinaryOperator,
    right: ExprId,
) -> ExprId {
    let nullable = draft.definition(left).unwrap().ty.nullable()
        || draft.definition(right).unwrap().ty.nullable();
    draft
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, nullable),
            ExprKind::Binary {
                op,
                left,
                right,
                decimal_overflow_policy: OUTPUT_NULL,
                allow_throw_exception: None,
            },
        )
        .unwrap()
}

fn count_eq_zero(
    draft: &mut BoundExprSetBuilder,
    pools: &mut ConstantPools,
    count: &(ValueId, FunctionValueType),
) -> ExprId {
    let left = draft
        .add(None, count.1.clone(), ExprKind::Value(count.0))
        .unwrap();
    let zero = integer_literal(draft, pools, 0);
    let right = cast(draft, zero, FunctionValueType::new(DataType::Int64, false));
    comparison(draft, left, BinaryOperator::Eq, right)
}

fn present_date(draft: &mut BoundExprSetBuilder, max: &(ValueId, FunctionValueType)) -> ExprId {
    let max = draft
        .add(None, max.1.clone(), ExprKind::Value(max.0))
        .unwrap();
    let date = cast(draft, max, FunctionValueType::new(DataType::Date32, true));
    draft
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: date,
                negated: true,
            },
        )
        .unwrap()
}

fn rand_lt_half(
    draft: &mut BoundExprSetBuilder,
    pools: &mut ConstantPools,
    authors: &mut BTreeMap<ExprId, ClaimAuthor>,
    functions: &PureEngineFunctionCatalog,
) -> ExprId {
    let seed_type = FunctionValueType::new(DataType::Int64, false);
    let source_seed = ConstantValue::from_i64(
        Arc::new(Field::new("literal", DataType::Int64, false)),
        seed_type.clone(),
        13,
        POLICY,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    // SQL emits its canonical BIGINT seed directly. Its frozen request
    // retains the same backing as the actual constant definition.
    let seed = constant(draft, pools, &source_seed, seed_type.clone());
    let owner = ClaimAuthor::with_legacy_binding(
        functions,
        "rand",
        vec![FunctionArgument::Value {
            value_type: seed_type,
            constant: Some(source_seed),
        }],
        ControlShape::Eager,
        None,
        POLICY,
        &HarnessControl,
    )
    .with_decimal_policy(OUTPUT_NULL);
    let random = add_call(draft, authors, owner, vec![seed]);
    let decimal = FunctionValueType::new(DataType::Decimal128(2, 1), false);
    let half = ConstantValue::from_decimal128(
        Arc::new(Field::new("literal", DataType::Decimal128(2, 1), false)),
        decimal.clone(),
        5,
        POLICY,
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .unwrap();
    let half = constant(draft, pools, &half, decimal);
    let half = cast(
        draft,
        half,
        FunctionValueType::new(DataType::Float64, false),
    );
    comparison(draft, random, BinaryOperator::Lt, half)
}

pub fn fixture(shape: FilterShape) -> Fixture {
    let catalog = installed_builtin_owner_catalogue();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let mut constants = ConstantPools::empty();
    let mut authors = BTreeMap::new();

    // The original Filter ports are the complete immediate Aggregate outputs.
    let key = (
        ValueId::new(0),
        FunctionValueType::new(DataType::Int32, true),
    );
    let count = FunctionValueType::new(DataType::Int64, true);
    let values = match shape {
        FilterShape::TwoCounts => vec![(ValueId::new(2), count.clone()), (ValueId::new(3), count)],
        FilterShape::CountAndRandom => vec![(ValueId::new(2), count)],
        FilterShape::DateAndRandom => vec![(
            ValueId::new(2),
            FunctionValueType::new(DataType::Float64, true),
        )],
    };
    let first = match shape {
        FilterShape::TwoCounts | FilterShape::CountAndRandom => {
            count_eq_zero(&mut draft, &mut constants, &values[0])
        }
        FilterShape::DateAndRandom => present_date(&mut draft, &values[0]),
    };
    let second = match shape {
        FilterShape::TwoCounts => count_eq_zero(&mut draft, &mut constants, &values[1]),
        FilterShape::CountAndRandom | FilterShape::DateAndRandom => {
            rand_lt_half(&mut draft, &mut constants, &mut authors, &catalog)
        }
    };
    let roots = [first, second].map(|definition| {
        draft
            .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap()
    });
    let port = ExprPort {
        values: std::iter::once(key).chain(values).collect(),
    };
    let schema = Arc::new(Schema::new(
        port.values
            .iter()
            .enumerate()
            .map(|(ordinal, (_, ty))| {
                Field::new(
                    format!("local_aggregate_1_{ordinal}"),
                    ty.data_type().clone(),
                    ty.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ));
    let scope = fixture_scope(&port, schema);
    let parameters = SemanticParameters::try_new([(
        ALLOW_THROW.id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(
        &mut draft,
        &catalog,
        &constants,
        &parameters,
        POLICY,
        &mut work,
    );
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        &catalog,
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
        &catalog,
    );
    for root in roots {
        let root = unit.root(root).unwrap();
        let occurrence = &unit.control_flow().uses()[&root.use_id];
        assert_eq!(occurrence.definition, root.definition);
        assert_eq!(occurrence.context.demand, EvaluationDemand::TruthOnly);
    }
    Fixture { unit, roots, shape }
}

pub fn batch(f: &Fixture, counts: &[i64], doubles: &[Option<f64>]) -> RecordBatch {
    let keys = Arc::new(Int32Array::from_iter_values(
        (0..counts.len()).map(|n| i32::try_from(n + 1).unwrap()),
    ));
    let mut arrays: Vec<ArrayRef> = vec![keys];
    let counts_array = || Arc::new(Int64Array::from_iter_values(counts.iter().copied()));
    match f.shape {
        FilterShape::TwoCounts => arrays.extend([counts_array() as ArrayRef, counts_array()]),
        FilterShape::CountAndRandom => arrays.push(counts_array()),
        FilterShape::DateAndRandom => {
            assert_eq!(counts.len(), doubles.len());
            arrays.push(Arc::new(Float64Array::from(doubles.to_vec())));
        }
    }
    RecordBatch::try_new(f.unit.scopes()[0].schema().clone(), arrays).unwrap()
}

pub fn bits(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

pub fn group(f: &Fixture) -> CompiledFilterConjunctionInstance {
    let roots = f.roots.map(|root| (f.unit.clone(), root));
    CompiledFilterConjunctionInstance::try_new_with_allocator(&roots, &Control, None).unwrap()
}
