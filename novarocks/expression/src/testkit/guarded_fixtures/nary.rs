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
    Fixture, add_call, author, compile_project_unit, fixture_constant_policy, project_port,
    unit_draft,
};
use crate::{CompiledExprSet, testkit::checked_constants};
use arrow::{array::BooleanArray, datatypes::DataType, record_batch::RecordBatch};
use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope, ValueId,
};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{ControlShape, EvaluationDemand, FunctionValueType};
use std::{collections::BTreeMap, sync::Arc};

fn functions() -> PureEngineFunctionCatalog {
    functions_with_if(false)
}
fn functions_with_if(include_if: bool) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    // The catalogue requires a nonempty seal. This actual unused RAND subset
    // grants no function-call identity to the intrinsic Boolean package.
    let mut manifest = ["()->f64;strict;legacy", "(i64)->f64;strict;legacy"]
        .into_iter()
        .map(|overload| InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!("builtin.scalar/rand/{overload}"))
                    .unwrap(),
                implementation: PureImplementationId::try_new("builtin.scalar/rand/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        })
        .collect::<Vec<_>>();
    if include_if {
        builder
            .register(
                actual
                    .definition("if", FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        manifest.push(InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/if/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.scalar/if/(bool,any<T>,any<T>)->any<T>;widen;legacy",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new("builtin.scalar/if/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::ControlIntrinsicV1,
            },
            aggregate_state_format: None,
        });
    }
    builder.seal_pure(manifest).unwrap()
}
#[derive(Clone, Copy)]
pub enum Connective {
    And,
    Or,
}
impl Connective {
    pub fn shape(self) -> ControlShape {
        match self {
            Self::And => ControlShape::Conjunction,
            Self::Or => ControlShape::Disjunction,
        }
    }
    fn kind(self, args: Box<[ExprId]>) -> ExprKind {
        match self {
            Self::And => ExprKind::Conjunction { args },
            Self::Or => ExprKind::Disjunction { args },
        }
    }
}
#[derive(Clone, Copy)]
pub enum Use {
    Value,
    TruthOnly,
}

fn source_columns(
    builder: &mut BoundExprSetBuilder,
    source_pools: &mut ConstantPools,
) -> (ExprPort, [ExprId; 3]) {
    let values = [
        ValueId::new(3),
        ValueId::new(100),
        ValueId::new(u32::MAX - 32),
    ];
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let port = project_port(values.map(|value| (value, boolean.clone())), source_pools);
    let operands = values.map(|value| {
        builder
            .add(None, boolean.clone(), ExprKind::Value(value))
            .unwrap()
    });
    (port, operands)
}

pub fn program(op: Connective, mode: Use, width: usize) -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let mut builder = unit_draft();
    let (port, operands) = source_columns(&mut builder, &mut source_pools);
    let order = [operands[2], operands[0], operands[1]];
    let args = (0..width)
        .map(|ordinal| order[ordinal % 3])
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let connective = builder
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, true),
            op.kind(args),
        )
        .unwrap();
    let root = builder
        .root(
            connective,
            match mode {
                Use::Value => EvaluationDemand::Value,
                Use::TruthOnly => EvaluationDemand::TruthOnly,
            },
            RootScope::Port(0),
        )
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions(), &BTreeMap::new(), source_pools);
    assert!(unit.calls().is_empty());
    Fixture { unit, root }
}

pub fn filter_roots(width: usize) -> (Arc<CompiledExprSet>, Vec<RootOrdinal>) {
    let mut source_pools = ConstantPools::empty();
    let mut builder = unit_draft();
    let (port, operands) = source_columns(&mut builder, &mut source_pools);
    let order = [operands[2], operands[0], operands[1]];
    let roots = (0..width)
        .map(|ordinal| {
            builder
                .root(
                    order[ordinal % 3],
                    EvaluationDemand::TruthOnly,
                    RootScope::Port(0),
                )
                .unwrap()
        })
        .collect();
    let unit = compile_project_unit(builder, port, &functions(), &BTreeMap::new(), source_pools);
    (unit, roots)
}

pub fn dual_demand_program() -> Fixture {
    let mut source_pools = ConstantPools::empty();
    let functions = functions_with_if(true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let arguments = [
        FunctionArgument::Value {
            value_type: boolean.clone(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Boolean, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: boolean.clone(),
            constant: None,
        },
    ];
    let owner = author(&functions, "if", arguments.to_vec(), ControlShape::If)
        .with_decimal_policy(novarocks_type_contract::DecimalOverflowPolicy::OutputNull);
    let mut builder = unit_draft();
    let (port, operands) = source_columns(&mut builder, &mut source_pools);
    let shared = builder
        .add(
            None,
            boolean,
            ExprKind::Conjunction {
                args: Box::from([operands[2], operands[0], operands[1]]),
            },
        )
        .unwrap();
    let yes = builder
        .add(
            None,
            (FunctionValueType::new(DataType::Boolean, false)).clone(),
            checked_constants::kind(
                &mut source_pools,
                checked_constants::boolean(
                    &(FunctionValueType::new(DataType::Boolean, false)),
                    true,
                    fixture_constant_policy(),
                ),
            ),
        )
        .unwrap();
    // The same physical definition is required first as an IF condition
    // (TruthOnly), and then as its ELSE result (Value) in another domain.
    let mut authors = BTreeMap::new();
    let result = add_call(&mut builder, &mut authors, owner, vec![shared, yes, shared]);
    let ExprKind::FunctionCall { function, .. } = &builder.definition(result).unwrap().kind else {
        panic!("the original IF call definition")
    };
    assert_eq!(function.function_id.as_str(), "builtin.scalar/if/v1");
    let output_root = builder
        .root(result, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = compile_project_unit(builder, port, &functions, &authors, source_pools);
    Fixture {
        unit,
        root: output_root,
    }
}

pub fn batch(unit: &CompiledExprSet, columns: [Vec<Option<bool>>; 3]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(unit.scope(RootScope::Port(0)).unwrap().schema()),
        columns
            .into_iter()
            .map(|values| Arc::new(BooleanArray::from(values)) as arrow::array::ArrayRef)
            .collect(),
    )
    .unwrap()
}

// Rows are authored in physical column order A,B,C. The connective evaluates
// C,A,B, and the SQL truth tables below are independent of scheduler output.
const ROWS: [[Option<bool>; 3]; 9] = [
    [Some(true), Some(true), Some(true)],
    [Some(true), Some(false), Some(true)],
    [Some(true), None, Some(true)],
    [Some(true), Some(false), None],
    [None, Some(true), Some(false)],
    [Some(true), Some(true), None],
    [Some(false), Some(false), Some(false)],
    [None, None, None],
    [Some(false), None, Some(true)],
];
pub const AND: [Option<bool>; 9] = [
    Some(true),
    Some(false),
    None,
    Some(false),
    Some(false),
    None,
    Some(false),
    None,
    Some(false),
];
pub const OR: [Option<bool>; 9] = [
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(false),
    None,
    Some(true),
];
pub fn sparse_batch(program: &Fixture) -> (RecordBatch, [usize; 9]) {
    let rows = [1, 3, 5, 7, 9, 11, 13, 15, 17];
    let mut columns = [vec![None; 24], vec![None; 24], vec![None; 24]];
    for (original, row) in rows.iter().zip(ROWS) {
        for (column, value) in columns.iter_mut().zip(row) {
            column[*original] = value;
        }
    }
    (batch(&program.unit, columns), rows)
}
