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

//! Public observation of complete optional-fold units before plan publication.

use crate::fixtures::{compile_table_free, expression_sources, session, text};
use arrow::datatypes::DataType;
use novarocks_expression_contract::{BoundExprSet, ExprId, ExprKind, ExprNode, RootOrdinal};
use novarocks_functions::{ConstantPolicy, ConstantValue};
use novarocks_physical_plan::ConstantPools;
use novarocks_sql::compiler::{
    SqlAuthoredScalarUnit, SqlCompileControl, SqlCompileError, SqlCompileProgressError,
    SqlFoldDependencyObserver,
};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) struct FoldReceipt {
    pub(super) unit: Arc<BoundExprSet>,
    root: RootOrdinal,
    constants: ConstantPools,
    pub(super) result: Option<ConstantValue>,
    pub(super) parameters: novarocks_type_contract::SemanticParameters,
    policy: ConstantPolicy,
}

impl FoldReceipt {
    pub(super) fn root(&self) -> &ExprNode {
        self.unit
            .definition(self.unit.root(self.root).unwrap().definition)
            .unwrap()
    }

    pub(super) fn root_use(&self) -> novarocks_type_contract::ExpressionUseId {
        self.unit.root_use(self.root).unwrap()
    }

    pub(super) fn constant(&self, definition: ExprId) -> ConstantValue {
        let source = self.unit.definition(definition).unwrap();
        let ExprKind::Constant(reference) = &source.kind else {
            panic!("expected a constant argument: {source:?}");
        };
        let control = SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let value = self
            .constants
            .resolve_observed(*reference, &source.ty, &mut work)
            .unwrap();
        work.finish().unwrap();
        assert_eq!(value.value_type(), &source.ty);
        value
    }

    pub(super) fn is_call(&self, id: &str) -> bool {
        matches!(&self.root().kind, ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == id)
    }
}

#[derive(Default)]
pub(super) struct FoldProbe {
    pub(super) receipts: Mutex<Vec<FoldReceipt>>,
    attempts: AtomicUsize,
    refusal: Option<CompileControlError>,
    publications: AtomicUsize,
}

impl SqlFoldDependencyObserver for FoldProbe {
    fn observe_fold_unit_observed(
        &self,
        source: &SqlAuthoredScalarUnit,
        result: Option<&ConstantValue>,
        _control: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if let Some(cause) = &self.refusal {
            return Err(cause.clone());
        }
        assert!(source.unit().ports().is_empty());
        assert_eq!(source.unit().roots().len(), 1);
        self.receipts.lock().unwrap().push(FoldReceipt {
            unit: source.unit().clone(),
            root: source.root(),
            constants: source.constants().clone(),
            result: result.cloned(),
            parameters: source.parameters().clone(),
            policy: source.constant_policy(),
        });
        Ok(())
    }

    fn observe_published_source_observed(
        &self,
        _source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
        _control: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        self.publications.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl FoldProbe {
    pub(super) fn control(self: &Arc<Self>) -> SqlCompileControl {
        SqlCompileControl::unbounded().with_fold_dependency_observer(self.clone())
    }
}

const UPPER: &str = "builtin.scalar/upper/v1";

#[test]
fn sql_fold_dependency_real_upper_removed_from_plan_keeps_exact_binding_and_cv() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile_table_free(
        "SELECT upper('Straße') AS folded",
        session(Default::default(), true),
        &probe.control(),
    )
    .unwrap();
    assert!(
        expression_sources(&owner)
            .all(|(_, _, node)| !matches!(node.kind, ExprKind::FunctionCall { .. }))
    );
    let receipts = probe.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.is_call(UPPER))
        .unwrap();
    let ExprKind::FunctionCall { function, args } = &receipt.root().kind else {
        unreachable!()
    };
    assert_eq!(args.len(), 1);
    assert_eq!(text(&receipt.constant(args[0])), "Straße");
    let result = receipt.result.as_ref().unwrap();
    assert_eq!(text(result), "STRASSE");
    assert_eq!(
        result.value_type().data_type(),
        function.result_type.data_type()
    );
    assert_eq!(function.result_type, receipt.root().ty);
    assert!(receipts.iter().all(|receipt| receipt.result.is_some()));
}

#[test]
fn sql_fold_dependency_two_actual_functions_both_recorded_before_replacement() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile_table_free(
        "SELECT upper('first') AS a, upper('second') AS b",
        session(Default::default(), true),
        &probe.control(),
    )
    .unwrap();
    assert!(
        expression_sources(&owner)
            .all(|(_, _, node)| !matches!(node.kind, ExprKind::FunctionCall { .. }))
    );
    let receipts = probe.receipts.lock().unwrap();
    let mut calls = receipts
        .iter()
        .filter(|receipt| receipt.is_call(UPPER))
        .map(|receipt| {
            let ExprKind::FunctionCall { args, .. } = &receipt.root().kind else {
                unreachable!()
            };
            (
                text(&receipt.constant(args[0])).to_owned(),
                text(receipt.result.as_ref().unwrap()),
            )
        })
        .collect::<Vec<_>>();
    calls.sort_unstable();
    assert_eq!(
        calls,
        [
            ("first".to_owned(), "FIRST"),
            ("second".to_owned(), "SECOND")
        ]
    );
}

#[test]
fn sql_fold_dependency_cast_intrinsic_retains_exact_source_result_policy() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile_table_free(
        "SELECT CAST(12 AS VARCHAR) AS folded_cast",
        session(Default::default(), true),
        &probe.control(),
    )
    .unwrap();
    assert!(
        expression_sources(&owner)
            .all(|(_, _, node)| !matches!(node.kind, ExprKind::FunctionCall { .. }))
    );
    let receipts = probe.receipts.lock().unwrap();
    let (receipt, input) = receipts
        .iter()
        .find_map(|receipt| match &receipt.root().kind {
            ExprKind::Cast {
                expr,
                target: DataType::Utf8,
                ..
            } => Some((receipt, *expr)),
            _ => None,
        })
        .expect("cast invocation was omitted");
    assert!(receipt.unit.requests().is_empty());
    assert_eq!(receipt.constant(input).try_i64().unwrap(), Some(12));
    assert_eq!(receipt.root().ty.data_type(), &DataType::Utf8);
    assert_eq!(text(receipt.result.as_ref().unwrap()), "12");
    assert_eq!(receipt.policy, sql_fixture_constant_policy());
}

#[test]
fn sql_fold_dependency_observation_refusal_keeps_cause_and_prevents_publication() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let probe = Arc::new(FoldProbe {
            refusal: Some(cause.clone()),
            ..FoldProbe::default()
        });
        let Err(SqlCompileProgressError::Compile(error)) = compile_table_free(
            "SELECT upper('never')",
            session(Default::default(), true),
            &probe.control(),
        ) else {
            panic!("the fold observation refusal must stop original compilation");
        };
        assert_eq!(error, SqlCompileError::from(cause));
        assert_eq!(probe.attempts.load(Ordering::Relaxed), 1);
        assert_eq!(probe.publications.load(Ordering::Relaxed), 0);
        assert!(probe.receipts.lock().unwrap().is_empty());
    }
}

#[test]
fn sql_fold_dependency_query_owned_receipts_are_isolated_and_released() {
    let left = Arc::new(FoldProbe::default());
    let right = Arc::new(FoldProbe::default());
    let left_weak = Arc::downgrade(&left);
    let right_weak = Arc::downgrade(&right);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            compile_table_free(
                "SELECT upper('left')",
                session(Default::default(), true),
                &left.control(),
            )
            .unwrap()
        });
        scope.spawn(|| {
            compile_table_free(
                "SELECT upper('right')",
                session(Default::default(), true),
                &right.control(),
            )
            .unwrap()
        });
    });
    for (probe, expected) in [(&left, "left"), (&right, "right")] {
        let receipts = probe.receipts.lock().unwrap();
        let calls = receipts
            .iter()
            .filter(|receipt| receipt.is_call(UPPER))
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1);
        let ExprKind::FunctionCall { args, .. } = &calls[0].root().kind else {
            unreachable!()
        };
        assert_eq!(text(&calls[0].constant(args[0])), expected);
    }
    drop(left);
    drop(right);
    assert!(left_weak.upgrade().is_none());
    assert!(right_weak.upgrade().is_none());
}

#[test]
fn sql_fold_dependency_original_unconstrained_binding_is_not_selected_result_constraint() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile_table_free(
        "SELECT upper('unconstrained') AS folded",
        session(Default::default(), true),
        &probe.control(),
    )
    .unwrap();
    assert!(
        expression_sources(&owner)
            .all(|(_, _, node)| !matches!(node.kind, ExprKind::FunctionCall { .. }))
    );
    let receipts = probe.receipts.lock().unwrap();
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.is_call(UPPER))
        .unwrap();
    let root = receipt.root();
    let ExprKind::FunctionCall { function, .. } = &root.kind else {
        unreachable!()
    };
    let request = &receipt.unit.requests()[&root.id];
    let claim = &receipt.unit.claims()[&receipt.root_use()];
    assert_eq!(function.result_type, root.ty);
    assert!(request.expected_result_type.is_none());
    assert_eq!(
        claim.decimal_overflow_policy,
        novarocks_sql::sql_mode::SqlSemanticSettings::default()
            .sql_mode()
            .decimal_overflow_policy(),
    );
    assert!(receipt.result.is_some());
}
