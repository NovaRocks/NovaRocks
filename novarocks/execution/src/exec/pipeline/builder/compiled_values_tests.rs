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

//! A multi-row VALUES whose common type is assigned through explicit cast
//! cells, compiled by local-compiler and run Values -> Project -> Result. Each
//! cast (or arithmetic) cell is a dynamic ValuesCell root evaluated once by the
//! compiled evaluator; the oracle is plain Rust integer conversion.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions::{ConstantPool, KernelFailure, Selection};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind,
};
use novarocks_physical_plan::{
    BinaryOperator, ConstantPoolId, ConstantPools, ConstantReference, ExprId, ExprKind, Fragment,
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageAdmission,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, LiteralValue,
    NodeId, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, RequiredContracts, ResultField, ResultPort,
    ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
    arithmetic_result_value_type_with_op,
};

use super::family_fixture::{FixtureControl, compile, constant_policy, int64_rows, run};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::fragment::io::NoopFragmentEventSink;
use crate::runtime::fragment::{ExecutionFailure, ExecutionFailureCause};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

const ALLOW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(3),
    expected_key: SemanticParameterKey::AllowThrowException,
};
const VALUES: NodeId = NodeId::new(1);
const PROJECT: NodeId = NodeId::new(2);
const OVERFLOW: &str = "Arithmetic overflow: Overflow happened on: 9223372036854775807 + 1";

fn pool(array: ArrayRef, nullable: bool) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("analyzed").unwrap()),
        ty,
        array.to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap()
}
// Analyzed source constants: TINYINT, SMALLINT and a NULL TINYINT.
const TINYINT: u32 = 0;
const SMALLINT: u32 = 1;
const NULL_TINYINT: u32 = 2;
fn constant_pools() -> Vec<ConstantPool> {
    vec![
        pool(Arc::new(Int8Array::from(vec![1, -5, -128])), false),
        pool(Arc::new(Int16Array::from(vec![300])), false),
        pool(Arc::new(Int8Array::from(vec![None::<i8>])), true),
    ]
}

#[derive(Clone, Debug)]
enum Cell {
    /// A literal already of the column type.
    Literal(LiteralValue),
    /// `CAST(constant AS BIGINT)` over an analyzed source constant.
    Cast(u32, u32),
    /// `9223372036854775807 + 1`, whose evaluation fails with a row error.
    Overflow,
}

fn column_types() -> [FunctionValueType; 2] {
    [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int64, false),
    ]
}

/// `SELECT c0, c1 FROM (VALUES ...)`, with the common BIGINT type assigned
/// by cast cells exactly as the FE authors it.
fn values_program(rows: &[Vec<Cell>]) -> Arc<LocalProgram> {
    let pools = constant_pools();
    // The package constant namespace is closed: publish only cited pools.
    let mut constants = ConstantPools::empty();
    for (id, pool) in pools.iter().enumerate() {
        let cited = rows
            .iter()
            .flatten()
            .any(|cell| matches!(cell, Cell::Cast(source, _) if *source as usize == id));
        if cited {
            constants
                .insert(ConstantPoolId::new(id as u32), pool.clone())
                .unwrap();
        }
    }
    let types = column_types();
    let mut builder = FragmentBuilder::new(FragmentId::new(57));
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: VALUES,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut cells = Vec::new();
    for row in rows {
        let mut exprs = Vec::new();
        for (column, cell) in row.iter().enumerate() {
            let ty = types[column].clone();
            let expr = match cell {
                Cell::Literal(value) => builder
                    .add_expression(VALUES, ty, ExprKind::Literal(value.clone()))
                    .unwrap(),
                Cell::Cast(source, ordinal) => {
                    let constant = builder
                        .add_expression(
                            VALUES,
                            pools[*source as usize].value_type().clone(),
                            ExprKind::Constant(ConstantReference {
                                pool: ConstantPoolId::new(*source),
                                ordinal: *ordinal,
                            }),
                        )
                        .unwrap();
                    builder
                        .add_expression(
                            VALUES,
                            ty,
                            ExprKind::Cast {
                                expr: constant,
                                target: DataType::Int64,
                                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                                allow_throw_exception: ALLOW,
                            },
                        )
                        .unwrap()
                }
                Cell::Overflow => {
                    let operand = FunctionValueType::new(DataType::Int64, false);
                    let mut sum = arithmetic_result_value_type_with_op(
                        &operand,
                        &operand,
                        novarocks_type_contract::ArithmeticOperator::Add,
                    )
                    .unwrap();
                    sum.nullable = true;
                    assert_eq!(sum, ty, "BIGINT + BIGINT fills the nullable BIGINT column");
                    let left = builder
                        .add_expression(
                            VALUES,
                            operand.clone(),
                            ExprKind::Literal(LiteralValue::Int64(i64::MAX)),
                        )
                        .unwrap();
                    let right = builder
                        .add_expression(VALUES, operand, ExprKind::Literal(LiteralValue::Int64(1)))
                        .unwrap();
                    builder
                        .add_expression(
                            VALUES,
                            sum,
                            ExprKind::Binary {
                                op: BinaryOperator::Add,
                                left,
                                right,
                                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                                allow_throw_exception: Some(ALLOW),
                            },
                        )
                        .unwrap()
                }
            };
            exprs.push(expr);
        }
        cells.push(exprs.into_boxed_slice());
    }
    builder
        .add_values(
            VALUES,
            cells.into_boxed_slice(),
            columns.clone().into_boxed_slice(),
        )
        .unwrap();
    let mut assignments = Vec::new();
    let mut projected = Vec::new();
    for &input in &columns {
        let ty = builder.value(input).unwrap().ty.clone();
        let expr = builder
            .add_expression(PROJECT, ty.clone(), ExprKind::Value(input))
            .unwrap();
        let output = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: PROJECT,
                    expr,
                },
            )
            .unwrap();
        assignments.push((expr, output));
        projected.push(output);
    }
    builder
        .add_project(
            PROJECT,
            VALUES,
            assignments.into_boxed_slice(),
            projected.into_boxed_slice(),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            PROJECT,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    compile(package(fragment, constants), 1)
}

/// Author one eager use per actual occurrence, children before parents.
fn visit(
    fragment: &Fragment,
    definition: ExprId,
    demand: EvaluationDemand,
    next: &mut u32,
    uses: &mut Vec<ExpressionInvocation<ExprId>>,
) -> ExpressionUseId {
    let use_id = ExpressionUseId::new(*next);
    *next += 1;
    let children = match &fragment.expressions().get(definition).unwrap().kind {
        ExprKind::Cast { expr, .. } => vec![*expr],
        ExprKind::Binary { left, right, .. } => vec![*left, *right],
        ExprKind::Constant(_) | ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
        other => panic!("fixture authors no control for {other:?}"),
    };
    let arguments = children
        .into_iter()
        .map(|child| visit(fragment, child, EvaluationDemand::Value, next, uses))
        .collect::<Vec<_>>();
    uses.push(ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id,
            domain: EvaluationDomainId::new(0),
            demand,
        },
        definition,
        control: ControlShape::Eager,
        arguments: arguments.into_boxed_slice(),
    });
    use_id
}

fn package(fragment: Fragment, constants: ConstantPools) -> Arc<FragmentPackage> {
    package_with_parameters(
        fragment,
        constants,
        SemanticParameters::try_new([(
            ALLOW.id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap(),
    )
}

fn package_with_parameters(
    fragment: Fragment,
    constants: ConstantPools,
    parameters: SemanticParameters,
) -> Arc<FragmentPackage> {
    let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let mut next = 0;
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (site, root) in roots.sites() {
        bindings.push((
            *site,
            visit(&fragment, root.expr, root.demand, &mut next, &mut uses),
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &FixtureControl).unwrap();
    let output = fragment.nodes()[&fragment.root()].output.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: crate::test_result_domain::result_value_domain(
                    &fragment.values()[value].ty,
                ),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let id = fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                constants,
                version: PlanVersionId::try_new([57; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                expression_uses: uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 64 * 1024 * 1024,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 16 * 1024 * 1024,
                    max_coexisting_bytes: 256 * 1024 * 1024,
                    max_projection_work: 16 * 1024 * 1024,
                },
            },
            &FixtureControl,
        )
        .unwrap(),
    )
}

/// The independent oracle: an analyzed constant converted to BIGINT.
fn oracle(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::Literal(LiteralValue::Int64(value)) => Some(*value),
        Cell::Literal(LiteralValue::Null) => None,
        Cell::Cast(TINYINT, ordinal) => Some(i64::from([1_i8, -5, -128][*ordinal as usize])),
        Cell::Cast(SMALLINT, ordinal) => Some(i64::from([300_i16][*ordinal as usize])),
        Cell::Cast(NULL_TINYINT, _) => None,
        other => panic!("no successful oracle for {other:?}"),
    }
}

fn cell_site(row: u32, column: u32) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(0),
        role: ProgramNodeExpressionRole::ValuesCell { row, column },
    }
}

fn failure(program: &Arc<LocalProgram>) -> ExecutionFailure {
    let state = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkHandle::new();
    let prepared = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        None,
        1,
        state,
        Arc::new(NoopFragmentEventSink),
    )
    .expect("compiled program prepares drivers");
    let failure = prepared
        .start()
        .join()
        .expect_err("a failing Values cell fails the fragment");
    assert!(
        output.take_chunks().iter().all(|chunk| chunk.len() == 0),
        "no Values row is published before every cell succeeded"
    );
    failure
}

#[test]
fn values_cast_cells_run_through_a_projection_with_exact_values_and_nulls() {
    let rows = vec![
        vec![
            Cell::Cast(TINYINT, 0),
            Cell::Literal(LiteralValue::Int64(10)),
        ],
        vec![Cell::Literal(LiteralValue::Null), Cell::Cast(SMALLINT, 0)],
        vec![Cell::Cast(TINYINT, 1), Cell::Cast(TINYINT, 2)],
        vec![
            Cell::Cast(NULL_TINYINT, 0),
            Cell::Literal(LiteralValue::Int64(20)),
        ],
    ];
    let program = values_program(&rows);
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("the first local node is the Values source")
    };
    let dynamic = values
        .dynamic_cells()
        .iter()
        .map(|cell| (cell.row, cell.column))
        .collect::<Vec<_>>();
    assert_eq!(dynamic, vec![(0, 0), (1, 1), (2, 0), (2, 1), (3, 0)]);
    let expected = rows
        .iter()
        .map(|row| row.iter().map(oracle).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let chunks = run(&program);
    assert_eq!(int64_rows(&chunks), expected);
    for chunk in &chunks {
        let schema = chunk.batch.schema();
        assert!(schema.field(0).is_nullable());
        assert!(!schema.field(1).is_nullable());
    }
}

#[test]
fn values_failing_cell_is_a_typed_required_error_of_its_own_root() {
    let rows = vec![
        vec![
            Cell::Cast(TINYINT, 0),
            Cell::Literal(LiteralValue::Int64(10)),
        ],
        vec![Cell::Overflow, Cell::Cast(SMALLINT, 0)],
        vec![Cell::Overflow, Cell::Literal(LiteralValue::Int64(30))],
    ];
    let program = values_program(&rows);
    let failure = failure(&program);
    let ExecutionFailureCause::RequiredRow(required) = failure.cause() else {
        panic!("expected a required-expression row error, got {failure:?}")
    };
    // Row-major evaluation reports the first failing cell, as v1 did.
    assert_eq!(required.root(), cell_site(1, 0));
    assert_eq!(required.batch_row(), 0);
    assert_eq!(required.error().message(), OVERFLOW);
}

#[test]
fn values_cell_root_is_evaluated_only_over_its_empty_one_row_port() {
    let program = values_program(&[
        vec![
            Cell::Cast(TINYINT, 0),
            Cell::Literal(LiteralValue::Int64(10)),
        ],
        vec![Cell::Literal(LiteralValue::Null), Cell::Cast(SMALLINT, 0)],
    ]);
    let control = RuntimeKernelControl::new(Arc::new(RuntimeErrorState::default()));
    let empty = |rows: usize| {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap()
    };
    let mut instance =
        CompiledExpressionInstance::try_new(Arc::clone(&program), cell_site(1, 1), &control)
            .unwrap();
    let output = instance
        .evaluate(&empty(1), Selection::all(1), &control)
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        vec![300]
    );
    let wide = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![5]))],
    )
    .unwrap();
    for (input, rows) in [(empty(2), 2), (wide, 1)] {
        let mut instance =
            CompiledExpressionInstance::try_new(Arc::clone(&program), cell_site(1, 1), &control)
                .unwrap();
        assert!(matches!(
            instance.evaluate(&input, Selection::all(rows), &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    // A constant cell is backing, not a root.
    assert!(
        CompiledExpressionInstance::try_new(Arc::clone(&program), cell_site(0, 1), &control)
            .is_err()
    );
}

#[path = "compiled_root_result_boundary_tests.rs"]
pub(crate) mod root_result_boundary_tests;
