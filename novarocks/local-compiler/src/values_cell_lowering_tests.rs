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

//! A multi-row VALUES keeps each literal at its analyzed type and assigns the
//! column's common type through an explicit cast cell. Each cast cell stays a
//! dynamic `ValuesCell` root, prepared exactly as a Project root; each
//! constant cell is materialized backing and its root use is retired.

use super::*;
use arrow_array::{Int8Array, Int16Array};
use novarocks_local_program::{StaticValuesBacking, StaticValuesCell};
use novarocks_type_contract::EvaluationDemand;

#[derive(Clone, Copy)]
enum Cell {
    /// A constant of exactly the column type.
    Constant(u32, u32),
    /// A constant of its analyzed type, cast to the column type.
    Cast(u32, u32),
}

fn pools() -> Vec<(ConstantPoolId, ConstantPool)> {
    vec![
        (
            ConstantPoolId::new(0),
            checked_pool(
                Arc::new(Int64Array::from(vec![None::<i64>])),
                true,
                ValueLogicalType::Physical,
            ),
        ),
        (
            ConstantPoolId::new(1),
            checked_pool(
                Arc::new(Int64Array::from(vec![7, 9])),
                false,
                ValueLogicalType::Physical,
            ),
        ),
        (
            ConstantPoolId::new(2),
            checked_pool(
                Arc::new(Int8Array::from(vec![1, 2])),
                false,
                ValueLogicalType::Physical,
            ),
        ),
        (
            ConstantPoolId::new(3),
            checked_pool(
                Arc::new(Int16Array::from(vec![300])),
                false,
                ValueLogicalType::Physical,
            ),
        ),
    ]
}

/// Column 0 is nullable BIGINT and column 1 non-null BIGINT:
/// `(CAST(1 AS BIGINT), 7), (NULL, CAST(2 AS BIGINT)), (CAST(300 AS BIGINT), 9)`.
fn rows() -> Vec<Vec<Cell>> {
    vec![
        vec![Cell::Cast(2, 0), Cell::Constant(1, 0)],
        vec![Cell::Constant(0, 0), Cell::Cast(2, 1)],
        vec![Cell::Cast(3, 0), Cell::Constant(1, 1)],
    ]
}

fn cast_package(chain: bool) -> Arc<FragmentPackage> {
    let types = [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int64, false),
    ];
    cast_package_with_cells(pools(), &types, rows(), chain)
}

fn cast_package_with_cells(
    pools: Vec<(ConstantPoolId, ConstantPool)>,
    types: &[FunctionValueType],
    rows: Vec<Vec<Cell>>,
    chain: bool,
) -> Arc<FragmentPackage> {
    let mut constants = ConstantPools::empty();
    for (id, pool) in &pools {
        constants.insert(*id, pool.clone()).unwrap();
    }
    let values_node = NodeId::new(u32::MAX);
    let mut builder = FragmentBuilder::new(FragmentId::new(172));
    let outputs = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: values_node,
                        output_ordinal: ordinal as u32,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut cells = Vec::new();
    for row in rows {
        let mut exprs = Vec::new();
        for (column, cell) in row.into_iter().enumerate() {
            let (pool, ordinal) = match cell {
                Cell::Constant(pool, ordinal) | Cell::Cast(pool, ordinal) => (pool, ordinal),
            };
            let constant = builder
                .define(
                    values_node,
                    pools[pool as usize].1.value_type().clone(),
                    ExprKind::Constant(ConstantReference {
                        pool: ConstantPoolId::new(pool),
                        ordinal,
                    }),
                )
                .unwrap();
            let definition = match cell {
                Cell::Constant(..) => constant,
                Cell::Cast(..) => builder
                    .define(
                        values_node,
                        types[column].clone(),
                        ExprKind::Cast {
                            expr: constant,
                            target: types[column].data_type().clone(),
                            cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                        },
                    )
                    .unwrap(),
            };
            exprs.push(builder.root_definition(
                values_node,
                definition,
                RootScope::AllPorts,
                EvaluationDemand::Value,
            ));
        }
        cells.push(exprs.into_boxed_slice());
    }
    builder
        .add_values(
            values_node,
            cells.into_boxed_slice(),
            outputs.clone().into_boxed_slice(),
        )
        .unwrap();
    let root = if chain {
        let project = NodeId::new(7);
        let mut assignments = Vec::new();
        let mut projected = Vec::new();
        for &input in &outputs {
            let ty = builder.value(input).unwrap().ty.clone();
            let expr = builder
                .add_root(
                    project,
                    ty.clone(),
                    ExprKind::input(input),
                    RootScope::Port(0),
                    EvaluationDemand::Value,
                )
                .unwrap();
            let output = builder
                .add_value(
                    ty,
                    ValueOrigin::Expr {
                        node: project,
                        root: expr,
                    },
                )
                .unwrap();
            assignments.push((expr, output));
            projected.push(output);
        }
        builder
            .add_project(
                project,
                values_node,
                assignments.into_boxed_slice(),
                projected.into_boxed_slice(),
            )
            .unwrap();
        project
    } else {
        values_node
    };
    let fragment = builder
        .finish_fixture(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control::good()).unwrap();
    let output = fragment.nodes()[&root].output.clone();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(index, &value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("v{index}").into(),
                alias: None,
                value,
                ty: fragment.values()[&value].ty.clone(),
            })
            .collect(),
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([172; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment,
                constants,

                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::default(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning,
                layouts: Default::default(),
            },
            package_admission(),
            &Control::good(),
        )
        .unwrap(),
    )
}

fn values_node(program: &LocalProgram) -> &novarocks_local_program::StaticValues {
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("Values")
    };
    values
}

#[test]
fn values_cast_cells_compile_as_dynamic_roots_beside_constant_backing() {
    let program = compile(cast_package(false), &Control::good()).unwrap();
    let values = values_node(&program);
    assert!(values.batch().is_none());
    let StaticValuesBacking::Cells {
        rows,
        constants,
        dynamic,
    } = values.backing()
    else {
        panic!("cast cells keep cell sources")
    };
    assert_eq!(rows, 3);
    // Each column keeps only its constant cells, in row order and exactly in
    // the column carrier; a cast cell has no placeholder.
    let nullable = constants[0].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(nullable.len(), 1);
    assert!(nullable.is_null(0));
    assert_eq!(
        constants[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        vec![7, 9]
    );
    let positions = dynamic
        .iter()
        .map(|StaticValuesCell { row, column, .. }| (*row, *column))
        .collect::<Vec<_>>();
    assert_eq!(positions, vec![(0, 0), (1, 1), (2, 0)]);

    assert_eq!(
        dynamic
            .iter()
            .map(|cell| (cell.row, cell.column))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 1), (2, 0)]
    );
    let mut dynamic_uses = 0;
    for cell in dynamic {
        let unit = program.unit(cell.definition.unit).unwrap();
        let root = unit.root(cell.definition.root).unwrap();
        let flow = unit.control_flow();
        assert!(unit.scope(root.scope).unwrap().schema().fields().is_empty());
        let invocation = &flow.uses()[&root.use_id];
        assert_eq!(invocation.definition, root.definition);
        assert!(matches!(
            unit.definitions().node(root.definition).unwrap().kind(),
            StaticExprKind::PreparedCast { .. }
        ));
        let recipe = unit
            .recipes()
            .cast(root.use_id)
            .expect("prepared cast recipe");
        assert_eq!(recipe.result_type().data_type(), &DataType::Int64);
        assert_eq!(recipe.result_type().nullable(), cell.column == 0);
        let operand = &flow.uses()[&invocation.arguments[0]];
        assert!(matches!(
            unit.definitions().node(operand.definition).unwrap().kind(),
            StaticExprKind::Constant(_)
        ));
        dynamic_uses += 1 + invocation.arguments.len();
    }
    assert_eq!(dynamic_uses, 6, "three executed casts and their operands");
    // The complete unit retains its source roots; Values only executes its dynamic cells.
    let unit = program.unit(dynamic[0].definition.unit).unwrap();
    assert_eq!(unit.roots().len(), 6);
}

#[test]
fn values_cast_cells_feed_a_projection_through_ordinary_channels() {
    let program = compile(cast_package(true), &Control::good()).unwrap();
    assert_eq!(values_node(&program).dynamic_cells().len(), 3);
    let ProgramNodeKind::Project { exprs, .. } = program.graph().nodes()[1].kind() else {
        panic!("Project")
    };
    assert_eq!(exprs.len(), 2);
    for (ordinal, reference) in exprs.iter().enumerate() {
        let unit = program.unit(reference.unit).unwrap();
        let root = unit.root(reference.root).unwrap();
        assert_eq!(unit.input(root.use_id).unwrap().ordinal, ordinal as u32);
        assert!(unit.control_flow().root_use_ids().contains(&root.use_id));
    }
    assert_eq!(values_node(&program).dynamic_cells().len() + exprs.len(), 5);
}

#[test]
fn values_cast_cells_observe_every_original_compile_refusal() {
    let package = cast_package(false);
    let lowered = lower(&package, &Control::good()).unwrap();
    let baseline = Control::good();
    let (kind, _) = direct(&package, &lowered, &baseline).unwrap();
    assert!(
        matches!(kind, ProgramNodeKind::Values { values } if values.dynamic_cells().len() == 3)
    );
    let trace = baseline.trace();
    for position in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control::refusing(position, cause.clone());
            is_control(direct(&package, &lowered, &refused).unwrap_err(), cause);
            assert_eq!(refused.trace(), trace[..position]);
        }
    }
}

#[test]
fn values_constant_cells_keep_one_complete_batch() {
    // Without a dynamic cell, the Values backing stays one constant batch.
    let source = checked_pool(
        Arc::new(Int64Array::from(vec![4, 5])),
        false,
        ValueLogicalType::Physical,
    );
    let program = compile(
        build(
            std::slice::from_ref(&source),
            &[vec![(0, 1)], vec![(0, 0)]],
            &[0],
            false,
        ),
        &Control::good(),
    )
    .unwrap();
    let values = values_node(&program);
    assert!(values.dynamic_cells().is_empty());
    assert_eq!(values.batch().unwrap().num_rows(), 2);
}

#[test]
fn values_dynamic_list_cells_keep_complete_type_and_cross_pool_constant_backing() {
    use arrow_array::{ListArray, types::Int64Type};
    let first: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![Some(10), None]),
        None,
    ]));
    let second: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
        Some(vec![]),
        Some(vec![Some(20)]),
    ]));
    let third: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
        vec![Some(30), Some(40)],
    )]));
    let pools = [first, second, third]
        .into_iter()
        .enumerate()
        .map(|(index, array)| {
            (
                ConstantPoolId::new(index as u32),
                checked_pool(array, true, ValueLogicalType::Physical),
            )
        })
        .collect::<Vec<_>>();
    let ty = pools[0].1.value_type().clone();
    let package = cast_package_with_cells(
        pools,
        std::slice::from_ref(&ty),
        vec![
            vec![Cell::Constant(0, 0)],
            vec![Cell::Cast(1, 0)],
            vec![Cell::Constant(2, 0)],
            vec![Cell::Cast(0, 1)],
            vec![Cell::Cast(1, 1)],
        ],
        false,
    );
    let program = compile(package, &Control::good()).unwrap();
    let values = values_node(&program);
    assert_eq!(
        values
            .dynamic_cells()
            .iter()
            .map(|cell| (cell.row, cell.column))
            .collect::<Vec<_>>(),
        vec![(1, 0), (3, 0), (4, 0)]
    );
    assert_eq!(
        values.layout().schema().field(0).data_type(),
        ty.data_type()
    );
    let StaticValuesBacking::Cells {
        rows, constants, ..
    } = values.backing()
    else {
        panic!("mixed List cells keep original roots and constant backing");
    };
    assert_eq!(rows, 5);
    let constant = constants[0].as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(constant.len(), 2);
    let actual = (0..constant.len())
        .map(|row| {
            let value = constant.value(row);
            value
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![vec![Some(10), None], vec![Some(30), Some(40)]]);
}
