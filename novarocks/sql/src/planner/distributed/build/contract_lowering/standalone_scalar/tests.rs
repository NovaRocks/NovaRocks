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

use super::super::lowered_scalar_source_tests::{lower_call, policy, text};
use super::*;
use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
};
use novarocks_constant_contract::{ConstantError, ConstantPool};
use novarocks_failure_contract::FailureProjection;
use novarocks_parser::ast::AstBox;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn functions() -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
    crate::functions::builtin_sql_function_catalog().snapshot()
}
fn emit(
    expression: &TypedExpr,
    control: &dyn PureCompileControl,
) -> Result<StandaloneScalarEmission, ContractLoweringError> {
    lower_required_constant_unit(
        expression,
        functions(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        control,
    )
}
fn root(source: &StandaloneScalarEmission) -> &novarocks_expression_contract::ExprNode {
    source
        .unit
        .definition(source.unit.root(source.root).unwrap().definition)
        .unwrap()
}

#[test]
fn standalone_original_brands_definition_journal_and_canonical_sources_are_exact() {
    let control = Control::default();
    let expression = lower_call(text("ABC"), DecimalOverflowPolicy::ReportError);
    let mut first = emit(&expression, &control).unwrap();
    let mut second = emit(&expression, &control).unwrap();
    assert_eq!(first.root, second.root);
    assert_eq!(root(&first).id, root(&second).id);
    assert_ne!(first.brand, second.brand);
    assert_eq!(first.brand, first.brand.clone());
    {
        let loan = first.source_loan();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let receipt = loan
            .checked_expression_source_observed(root(&first), &mut work)
            .unwrap();
        let repeated = loan
            .checked_expression_source_observed(root(&first), &mut work)
            .unwrap();
        assert!(std::ptr::eq(receipt.source(), repeated.source()));
        assert!(std::ptr::eq(receipt.captured(), repeated.captured()));
        assert!(matches!(
            loan.checked_expression_source_observed(root(&second), &mut work),
            Err(
                super::super::super::lowered_draft::SqlSourceJournalError::InvalidSource(
                    "expression journal loans a foreign unit definition"
                )
            )
        ));
        work.finish().unwrap();
    }
    std::mem::swap(&mut first.journal, &mut second.journal);
    {
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        assert!(matches!(
            first
                .source_loan()
                .checked_expression_source_observed(root(&first), &mut work),
            Err(super::super::super::lowered_draft::SqlSourceJournalError::MissingEntry)
        ));
        work.finish().unwrap();
    }
    std::mem::swap(&mut first.journal, &mut second.journal);
    // A separately resolved original binding is genuinely foreign. Merely
    // repeating the same TypedExpr legitimately shares its logical binding.
    let other = emit(
        &lower_call(text("ABC"), DecimalOverflowPolicy::ReportError),
        &control,
    )
    .unwrap();
    let foreign = other
        .journal
        .expression_entries
        .values()
        .next()
        .unwrap()
        .canonical_operational
        .clone()
        .unwrap();
    let entry = first
        .journal
        .expression_entries
        .values_mut()
        .next()
        .unwrap();
    assert!(!foreign.belongs_to(entry.captured.captured()));
    entry.canonical_operational = Some(foreign);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let result =
        neutral_requests::transfer_expression_request_observed(entry, &mut work, &mut |_, _| {
            panic!("foreign canonical source must be rejected before constant registration")
        });
    assert!(
        matches!(result,Err(ContractLoweringError::InvalidFunctionBinding{detail})
        if detail=="neutral expression request has another original source")
    );
    work.finish().unwrap();
}

#[test]
fn standalone_selected_cv_and_nullability_keep_actual_sources_and_every_control_prefix() {
    let control = Control::default();
    let ty = ValueType::new(DataType::Utf8, true);
    let field = Arc::new(ty.try_to_field("selected_standalone_source").unwrap());
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec![Some("hidden"), Some("MiXeD-é"), None]).to_data(),
        policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    for ordinal in [1, 2] {
        let cv = pool.value(ordinal).unwrap();
        let expression = lower_call(
            TypedExpr {
                kind: ExprKind::Constant(cv.clone()),
                value_type: ty.clone(),
            },
            DecimalOverflowPolicy::ReportError,
        );
        let source = emit(&expression, &control).unwrap();
        assert!(source.unit.ports().is_empty());
        assert_eq!(source.unit.roots().len(), 1);
        assert_eq!(source.unit.claims().len(), 1);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let receipt = source
            .source_loan()
            .checked_expression_source_observed(root(&source), &mut work)
            .unwrap();
        let novarocks_functions::FunctionArgument::Value {
            constant: Some(captured),
            ..
        } = &receipt.captured().request().arguments[0]
        else {
            panic!("actual selected source CV")
        };
        assert_eq!(captured.ordinal(), ordinal);
        assert!(Arc::ptr_eq(captured.pool().field_ref(), &field));
        assert_eq!(
            captured.pool().backing_identity(),
            cv.pool().backing_identity()
        );
        assert_eq!(captured.try_utf8().unwrap(), cv.try_utf8().unwrap());
        let ContractExprKind::FunctionCall { args, .. } = &root(&source).kind else {
            panic!("actual LOWER definition")
        };
        let ContractExprKind::Constant(reference) = &source.unit.definition(args[0]).unwrap().kind
        else {
            panic!("actual selected constant")
        };
        let emitted = source
            .constants
            .resolve_source_observed(*reference, &mut work)
            .unwrap();
        assert_eq!(emitted.ordinal(), ordinal);
        assert!(Arc::ptr_eq(emitted.pool().field_ref(), &field));
        assert_eq!(
            emitted.pool().backing_identity(),
            cv.pool().backing_identity()
        );
        work.finish().unwrap();
    }
    let cast = TypedExpr {
        kind: ExprKind::Cast {
            cast_mode: Some(novarocks_type_contract::TrinoCastMode::Try),
            expr: AstBox::new(text("not-an-integer")),
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        },
        value_type: ValueType::new(DataType::Int64, true),
    };
    let negate = crate::analyzer::negation_for_test(
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Decimal("7".into())),
            value_type: ValueType::new(DataType::Decimal128(2, 0), false),
        },
        &Control::default(),
    )
    .unwrap();
    for (expression, nullable) in [(&cast, true), (&negate, false)] {
        let baseline_control = Control::default();
        let source = emit(expression, &baseline_control).unwrap();
        assert_eq!(
            source.value_type().data_type(),
            expression.value_type.data_type()
        );
        assert_eq!(source.value_type().nullable(), nullable);
        assert!(source.unit.ports().is_empty());
    }
    let call = lower_call(text("ABC"), DecimalOverflowPolicy::ReportError);
    for expression in [&cast, &negate, &call] {
        let baseline_control = Control::default();
        emit(expression, &baseline_control).unwrap();
        let baseline = baseline_control.trace();
        assert!(!baseline.is_empty());
        for cause in CAUSES {
            for stop in 0..baseline.len() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(
                    matches!(emit(expression,&control),Err(ContractLoweringError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), baseline[..=stop]);
            }
        }
    }
    // This limit originates at lowering, before any ordinary finishing footer.
    // Arming the next callback must not replace the actual source Limit.
    let small = ConstantValidation::bounded(novarocks_constant_contract::ConstantPolicy {
        max_retained_buffer_bytes: 1,
        ..crate::constant::test_constant_fixture()
    });
    let expression = text("an actual over-limit source literal");
    let baseline_control = Control::default();
    let result = lower_required_constant_unit(
        &expression,
        functions(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        small,
        &baseline_control,
    );
    let (expected_debug, expected_fact) = match result {
        Err(ContractLoweringError::ConstantSource(source @ ConstantError::Limit(_))) => {
            (format!("{source:?}"), source.failure_fact())
        }
        _ => panic!("the actual literal factory must own the source Limit"),
    };
    let baseline = baseline_control.trace();
    for cause in CAUSES {
        let control = Control {
            trace: Mutex::new(Vec::new()),
            refusal: Some((baseline.len(), cause.clone())),
        };
        match lower_required_constant_unit(
            &expression,
            functions(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            false,
            small,
            &control,
        ) {
            Err(ContractLoweringError::ConstantSource(source @ ConstantError::Limit(_))) => {
                assert_eq!(format!("{source:?}"), expected_debug);
                assert_eq!(source.failure_fact(), expected_fact);
            }
            _ => panic!("later control must not replace the original literal source Limit"),
        }
        assert_eq!(control.trace(), baseline);
    }
}

/// A real producer's checked intermediate, not a published source/claim token.
/// Shared only inside cfg(test), so the FX owner's private core can reject
/// foreign environment loans without widening any production API.
#[test]
fn standalone_runtime_dead_branch_still_admits_its_actual_static_call_source() {
    use crate::compiler::SqlFunctionCatalog;
    use novarocks_functions::{EngineFunctionCatalogBuilder, FunctionDefinition, FunctionKind};

    // A constant-false CASE branch is runtime-dead but remains reachable from
    // the original root. The same declaration is admissible only when its
    // exact installed owner is present.
    let query = novarocks_parser::parser::parse_query(
        "SELECT CASE WHEN FALSE THEN sequence(CAST(1 AS BIGINT), CAST(2 AS BIGINT)) ELSE NULL END AS mapped",
    )
    .unwrap();
    let (query, _, _) = crate::analyzer::analyze(
        &query,
        &crate::catalog::memory::PlannerMemoryCatalog::default(),
        "default",
    )
    .unwrap();
    let crate::analysis::QueryBody::Select(select) = query.body else {
        panic!("one original scalar projection")
    };
    let expression = select.projection.into_iter().next().unwrap().expr;
    let ExprKind::Case { when_then, .. } = &expression.kind else {
        panic!("analysis retains the original CASE")
    };
    assert_eq!(when_then.len(), 1);
    assert!(matches!(
        when_then[0].0.kind,
        ExprKind::Literal(LiteralValue::Bool(false))
    ));

    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut source = StandaloneDraft::new(functions(), false, policy());
    let definition = source
        .emission(&control, &mut work)
        .lower_expression(&expression, &BTreeMap::new())
        .unwrap();
    source
        .emission(&control, &mut work)
        .root_definition(
            definition,
            novarocks_type_contract::EvaluationDemand::Value,
            novarocks_expression_contract::RootScope::AllPorts,
        )
        .unwrap();
    source.rebind_nullability(&control, &mut work).unwrap();
    let parameters = scalar_emission::freeze_semantic_parameters(
        source.root_used,
        source.allow_throw_exception,
        BTreeMap::new(),
    )
    .unwrap();
    let loan = SqlUnitSourceLoan {
        site: ScalarSourceSite::Standalone(source.brand.clone()),
        definitions: UnitDefinitions::from_builder(
            ScalarUnitOwner::Standalone(source.brand.clone()),
            &source.draft,
        ),
        constants: source.constants.pools(),
        parameters: &parameters,
        journal: &source.journal,
        functions: &source.functions,
    };
    let actual = source
        .draft
        .definitions()
        .find(|node| {
            matches!(&node.kind, ContractExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == "builtin.scalar/sequence/v1")
        })
        .expect("the actual emitter retains the runtime-dead original call");
    let original = loan
        .checked_expression_source_observed(actual, &mut work)
        .unwrap();
    let canonical = original.canonical_operational().unwrap();
    assert!(canonical.belongs_to(original.captured()));
    let selected = &canonical.selected().overload;
    assert!(selected.as_str().starts_with("builtin.scalar/sequence/"));
    assert_eq!(original.captured().request().logical_argument_count, 2);
    let presence = source.functions.snapshot_for_scalar_presence();
    unit_authorship::admit_scalar_unit_sources_observed(
        &loan,
        &source.draft,
        presence.as_ref(),
        &control,
        &mut work,
    )
    .unwrap();
    let admitted = emit(&expression, &Control::default()).unwrap();
    assert!(admitted.unit().definitions().iter().any(|definition| {
        matches!(&definition.kind, ContractExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == "builtin.scalar/sequence/v1"
                && &function.overload == selected)
    }));

    // Keep the real declaration and resolver, withhold just its installed
    // implementation, and leave the rest of the original catalogue intact.
    let installed = crate::functions::builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for original in installed.definitions() {
        let definition =
            if original.kind() == FunctionKind::Scalar && original.canonical_name() == "sequence" {
                FunctionDefinition::try_new_bound(
                    original.canonical_name(),
                    original.visibility(),
                    original.binding_declaration().unwrap().clone(),
                    Arc::new(original.binding_resolver().unwrap().clone()),
                )
                .unwrap()
            } else {
                original.clone()
            };
        builder.register(definition).unwrap();
    }
    let declared = builder.seal_bound().unwrap();
    let presence = declared.snapshot_for_scalar_presence();
    assert!(matches!(
        unit_authorship::admit_scalar_unit_sources_observed(
            &loan, &source.draft, presence.as_ref(), &control, &mut work
        ),
        Err(ContractLoweringError::FunctionBinding(
            novarocks_functions::FunctionBindingError::UnavailableImplementation(ref overload)
        )) if overload == selected
    ));
    work.finish().unwrap();
    // The production standalone entry reaches the same original capability
    // refusal before neutral transfer, seal, effects or a published source.
    assert!(matches!(
        lower_required_constant_unit(
            &expression,
            declared.snapshot(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            false,
            policy(),
            &Control::default(),
        ),
        Err(ContractLoweringError::FunctionBinding(
            novarocks_functions::FunctionBindingError::UnavailableImplementation(ref overload)
        )) if overload == selected
    ));
}
