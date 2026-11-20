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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use novarocks_functions::ConstantValue;
use novarocks_type_contract::{CompileControlError, PureCompileControl};

use super::*;
use crate::column_id::ColumnId;
use crate::common::LiteralValue;
use crate::compiler::SqlFoldDependencyObserver;
use crate::optimizer::operator::{ProjectOp, ScalarProjectItem};
use crate::optimizer::scalar::HashableLiteral;

#[derive(Default)]
pub(super) struct FoldUnits(AtomicUsize);

impl SqlFoldDependencyObserver for FoldUnits {
    fn observe_fold_unit_observed(
        &self,
        _source: &SqlAuthoredScalarUnit,
        _result: Option<&ConstantValue>,
        _control: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub(super) fn install_inputs(ctx: &mut RewriteContext<'_>, arena: Rc<RefCell<ScalarArena>>) {
    ctx.set_scalar_arena(arena);
    ctx.set_function_catalog(crate::functions::builtin_sql_function_catalog().snapshot());
    ctx.set_sealed_functions(
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
    );
}

pub(super) struct Fixture {
    pub(super) arena: Rc<RefCell<ScalarArena>>,
    pub(super) ctx: RewriteContext<'static>,
    units: Arc<FoldUnits>,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let arena = Rc::new(RefCell::new(ScalarArena::new()));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        install_inputs(&mut ctx, Rc::clone(&arena));
        let units = Arc::new(FoldUnits::default());
        ctx.set_fold_dependency_observer(Some(units.clone()));
        Self { arena, ctx, units }
    }

    pub(super) fn calls(&self) -> usize {
        self.units.0.load(Ordering::Relaxed)
    }
    pub(super) fn intern(
        &self,
        node: ScalarNode,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> ScalarId {
        self.arena.borrow_mut().intern(node, value_type)
    }

    pub(super) fn int_literal(&self, value: i64) -> ScalarId {
        self.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )
    }

    pub(super) fn column(&self, id: u32) -> ScalarId {
        self.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        )
    }

    pub(super) fn function_call(
        &self,
        name: &str,
        args: Vec<ScalarId>,
        distinct: bool,
        volatility: FunctionVolatility,
        data_type: DataType,
        nullable: bool,
    ) -> ScalarId {
        let binding = if matches!(name, "abs" | "sec_to_time" | "time_to_sec") {
            let arguments = args
                .iter()
                .map(|id| novarocks_functions::FunctionArgument::Value {
                    value_type: self.arena.borrow().value_type(*id).clone(),
                    constant: None,
                })
                .collect::<Vec<_>>();
            let resolved = crate::functions::builtin_sql_function_catalog()
                .resolve_scalar_binding(
                    name,
                    &arguments,
                    crate::optimizer::test_optimizer_control(),
                )
                .expect("eligible fold fixture has an installed owner");
            crate::binding::SqlFunctionBinding::new(
                resolved,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            )
        } else {
            // Gated calls intentionally have no installed fixture owner: the
            // syntax gates must reject before preparation or evaluation.
            crate::optimizer::scalar::test_function_binding(
                &self.arena.borrow(),
                name,
                &args,
                data_type.clone(),
                nullable,
                volatility,
            )
        };
        self.intern(
            ScalarNode::FunctionCall {
                binding,
                name: name.to_string(),
                args,
                distinct,
                volatility,
            },
            novarocks_type_contract::FunctionValueType::new(data_type, nullable),
        )
    }

    pub(super) fn binary(&self, op: BinOp, left: ScalarId, right: ScalarId) -> ScalarId {
        self.binary_typed(op, left, right, DataType::Int64, true)
    }

    pub(super) fn binary_typed(
        &self,
        op: BinOp,
        left: ScalarId,
        right: ScalarId,
        data_type: DataType,
        nullable: bool,
    ) -> ScalarId {
        self.intern(
            ScalarNode::BinaryOp {
                op,
                left,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(data_type, nullable),
        )
    }

    pub(super) fn node(&self, id: ScalarId) -> ScalarNode {
        self.arena.borrow().node(id).clone()
    }

    pub(super) fn data_type(&self, id: ScalarId) -> DataType {
        self.arena.borrow().data_type(id).clone()
    }

    pub(super) fn nullable(&self, id: ScalarId) -> bool {
        self.arena.borrow().nullable(id)
    }

    pub(super) fn apply(&mut self, plan: OptExpr) -> RewriteResult {
        FoldConstant
            .apply(plan, &mut self.ctx)
            .expect("unbounded fold fixture must succeed")
    }

    pub(super) fn matches(&self, plan: &OptExpr) -> bool {
        FoldConstant.matches(plan, &self.ctx)
    }
}

pub(super) fn project(expr: ScalarId) -> OptExpr {
    OptExpr::leaf(Operator::LogicalProject(ProjectOp {
        items: vec![ScalarProjectItem {
            expr,
            output_name: "c".to_string(),
            output_column_id: ColumnId::new_for_test(1),
            expr_display: None,
        }],
        output_qualifier: None,
    }))
}

pub(super) fn project_expr(plan: &OptExpr) -> ScalarId {
    let Operator::LogicalProject(project) = &plan.op else {
        panic!("expected LogicalProject, got {:?}", plan.op);
    };
    project.items[0].expr
}

pub(super) fn changed(result: RewriteResult) -> OptExpr {
    match result {
        RewriteResult::Changed(plan) => plan,
        other => panic!("expected Changed, got {other:?}"),
    }
}

pub(super) fn assert_int_literal(fixture: &Fixture, id: ScalarId, expected: i64) {
    match fixture.node(id) {
        ScalarNode::Constant(value) => {
            assert_eq!(
                value
                    .signed_integer_observed(
                        CompilePhase::Validate,
                        crate::optimizer::test_optimizer_control()
                    )
                    .unwrap(),
                Some(expected)
            );
        }
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))) => {
            assert_eq!(value, expected)
        }
        other => panic!("expected checked integer value {expected}, got {other:?}"),
    }
}
