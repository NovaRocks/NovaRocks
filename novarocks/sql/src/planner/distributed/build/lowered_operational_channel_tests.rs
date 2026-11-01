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

use super::*;
use novarocks_functions::{FunctionArgumentType, FunctionBindingRequest};
use novarocks_physical_plan::ConstantPools;

fn project_with_pools(
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    pools: &ConstantPools,
    control: &dyn PureCompileControl,
) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = receipt.operational_arguments_observed(pools, &mut work);
    if matches!(&result, Err(SqlOperationalProjectionError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(in crate::planner::distributed::build) fn selected_source_owner() -> (
    SqlAuthoredPhysicalPlan,
    ConstantPool,
    Arc<arrow::datatypes::Field>,
) {
    let ty = ValueType::new(DataType::Utf8, true);
    let field = Arc::new(
        ty.try_to_field("source_ordinal_not_zero")
            .unwrap()
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "provider-origin".to_owned(),
                "original".to_owned(),
            )])),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec![Some("unused"), Some("MiXeD-中国"), None]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let expr = TypedExpr {
        kind: ExprKind::Constant(pool.value(1).unwrap()),
        value_type: ty,
    };
    (
        authored(lower_call(expr, DecimalOverflowPolicy::ReportError)),
        pool,
        field,
    )
}

fn foreign_namespace(
    owner: &SqlAuthoredPhysicalPlan,
    receipt: &CheckedExpressionLogicalSourceEntry<'_>,
    changed_field: bool,
) -> ConstantPools {
    let emitted = physical_constant(owner, receipt.fragment(), receipt.arguments()[0]);
    let expression = receipt
        .fragment()
        .expressions()
        .get(receipt.arguments()[0])
        .unwrap();
    let ContractExprKind::Constant(reference) = expression.kind else {
        panic!("actual reference")
    };
    let field = if changed_field {
        Arc::new(emitted.pool().field_ref().as_ref().clone().with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider-origin".to_owned(),
                "foreign".to_owned(),
            )]),
        ))
    } else {
        emitted.pool().field_ref().clone()
    };
    // A new checked pool with the same immutable Arrow data and selected value
    // is not the original source owner, even when its Field Arc is retained.
    let foreign = ConstantPool::try_new(
        field,
        emitted.value_type().clone(),
        emitted.pool().array().to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        foreign.value(1).unwrap().try_utf8().unwrap(),
        Some("MiXeD-中国")
    );
    assert_ne!(
        foreign.backing_identity(),
        emitted.pool().backing_identity()
    );
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, foreign).unwrap();
    pools
}

#[test]
fn operational_channels_none_does_not_demand_a_pool_for_cast_or_nested_constants() {
    for argument in [
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(text("MiXeD")),
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: ValueType::new(DataType::Utf8, false),
        },
        TypedExpr {
            kind: ExprKind::Nested(Box::new(text("MiXeD"))),
            value_type: ValueType::new(DataType::Utf8, false),
        },
    ] {
        let owner = authored(lower_call(argument, DecimalOverflowPolicy::OutputNull));
        let (fragment, source) = scalar_source(&owner);
        let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
        assert!(constant(&receipt.captured().request().arguments[0]).is_none());
        assert_eq!(
            physical_constant(&owner, fragment, receipt.arguments()[0])
                .try_utf8()
                .unwrap(),
            Some("MiXeD")
        );
        // This fails if projection reconstructs Some from the emitted Constant:
        // there is deliberately no namespace available to read its address.
        let arguments =
            project_with_pools(&receipt, &ConstantPools::empty(), &Control::default()).unwrap();
        let FunctionArgument::Value {
            value_type,
            constant,
        } = &arguments[0]
        else {
            panic!("Value")
        };
        assert_eq!(value_type, &ValueType::new(DataType::Utf8, false));
        assert!(constant.is_none());
    }
}

#[test]
fn operational_channels_nonzero_some_preserves_full_source_field_and_backing() {
    let (owner, pool, field) = selected_source_owner();
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    let arguments = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    let original = constant(&receipt.captured().request().arguments[0]).unwrap();
    let projected = constant(&arguments[0]).unwrap();
    assert_eq!(projected.ordinal(), 1);
    assert_eq!(projected.try_utf8().unwrap(), Some("MiXeD-中国"));
    assert_eq!(
        projected.value_type(),
        &ValueType::new(DataType::Utf8, true)
    );
    assert_eq!(projected.value_type(), original.value_type());
    assert_eq!(projected.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(projected.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(projected.pool().field_ref(), &field));
    assert_eq!(
        projected
            .pool()
            .field_ref()
            .metadata()
            .get("provider-origin")
            .unwrap(),
        "original"
    );
    assert_eq!(receipt.captured().constant_policy(), policy());
    assert_eq!(
        receipt.captured().binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
}

#[test]
fn operational_channels_canonical_null_keeps_distinct_emission_and_exact_original_catalogue() {
    let argument = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ValueType::new(DataType::Null, true),
    };
    let owner = authored(lower_call(argument, DecimalOverflowPolicy::OutputNull));
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    let original = constant(&receipt.captured().request().arguments[0]).unwrap();
    let arguments = operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    let projected = constant(&arguments[0]).unwrap();
    let emitted = physical_constant(&owner, fragment, receipt.arguments()[0]);
    assert_eq!(original.value_type(), &ValueType::new(DataType::Null, true));
    assert_eq!(
        projected.value_type(),
        &ValueType::new(DataType::Utf8, true)
    );
    assert_eq!(projected.try_utf8().unwrap(), None);
    assert_ne!(
        projected.pool().backing_identity(),
        original.pool().backing_identity()
    );
    assert!(Arc::ptr_eq(
        projected.pool().array(),
        emitted.pool().array()
    ));
    assert!(Arc::ptr_eq(
        projected.pool().field_ref(),
        emitted.pool().field_ref()
    ));
    // Projection reuses the actual typed emission's backing while preserving
    // the distinct captured Null source.
    let bound = receipt.captured().binding().resolved();
    let catalogue = owner.function_catalog();
    assert!(
        catalogue
            .select_exact_overload_observed(
                &bound.function_id,
                bound.kind,
                &bound.selected.overload,
                receipt.captured().request(),
                &Control::default(),
            )
            .is_err()
    );
    let selected = catalogue
        .select_exact_overload_observed(
            &bound.function_id,
            bound.kind,
            &bound.selected.overload,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: Some(&source.ty),
            },
            &Control::default(),
        )
        .unwrap();
    assert_eq!(selected.overload, bound.selected.overload);
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(ValueType::new(
            DataType::Utf8,
            true
        ))]
    );
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(ValueType::new(DataType::Utf8, true))
    );
    // This asserts metadata selection only; no call effects, final snapshot or
    // whole FE publication proof is constructed by this fixture.
}

#[test]
fn operational_channels_equal_content_foreign_pool_and_field_cannot_replace_original_source() {
    let (owner, _, _) = selected_source_owner();
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    operational_arguments(&owner, &receipt, &Control::default()).unwrap();
    for changed_field in [false, true] {
        let foreign = foreign_namespace(&owner, &receipt, changed_field);
        assert!(matches!(
            project_with_pools(&receipt, &foreign, &Control::default()),
            Err(SqlOperationalProjectionError::InvalidSource(
                "operational constant has a different original Field, backing or ordinal"
            ))
        ));
    }
    assert!(matches!(
        project_with_pools(&receipt, &ConstantPools::empty(), &Control::default()),
        Err(SqlOperationalProjectionError::Reference(
            novarocks_physical_plan::ConstantReferenceError::MissingPool(_)
        ))
    ));
}

#[test]
fn operational_channels_success_and_ordinary_refusal_stop_at_every_original_control_prefix() {
    let (owner, _, _) = selected_source_owner();
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    let foreign = foreign_namespace(&owner, &receipt, false);
    for (pools, success) in [(owner.plan().constants(), true), (&foreign, false)] {
        let baseline = Control::default();
        let result = project_with_pools(&receipt, pools, &baseline);
        assert_eq!(result.is_ok(), success);
        let trace = baseline.trace();
        assert!(trace.len() > 1);
        assert_eq!(trace[0], (CompilePhase::FunctionSpecialization, 0));
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(matches!(project_with_pools(&receipt, pools, &control),
                    Err(SqlOperationalProjectionError::Control(actual)) if actual == cause));
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn original_emission_retains_operational_request_and_selected_constant_backing_together() {
    let (owner, pool, field) = selected_source_owner();
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    let canonical = receipt.canonical_operational().unwrap();
    let request = canonical.request();
    assert_eq!(request.logical_argument_count, 1);
    assert_eq!(
        request.expected_result_type,
        receipt.captured().binding().result_constraint()
    );
    assert!(Arc::ptr_eq(
        canonical.selected(),
        receipt.canonical_selection().unwrap()
    ));
    let value = constant(&request.arguments[0]).unwrap();
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.try_utf8().unwrap(), Some("MiXeD-中国"));
    assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
    let cloned = owner.clone();
    let again = loan(&cloned, fragment, source, &Control::default()).unwrap();
    assert!(Arc::ptr_eq(
        canonical,
        again.canonical_operational().unwrap()
    ));
    assert!(std::ptr::eq(
        request.arguments,
        again.canonical_operational().unwrap().request().arguments
    ));
}

#[test]
fn original_emission_retained_none_is_not_reconstructed_from_physical_constant() {
    let argument = TypedExpr {
        kind: ExprKind::Cast {
            expr: Box::new(text("MiXeD")),
            target: DataType::Utf8,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: ValueType::new(DataType::Utf8, false),
    };
    let owner = authored(lower_call(argument, DecimalOverflowPolicy::OutputNull));
    let (fragment, source) = scalar_source(&owner);
    let receipt = loan(&owner, fragment, source, &Control::default()).unwrap();
    assert_eq!(
        physical_constant(&owner, fragment, receipt.arguments()[0])
            .try_utf8()
            .unwrap(),
        Some("MiXeD")
    );
    let canonical = receipt.canonical_operational().unwrap();
    assert!(constant(&canonical.request().arguments[0]).is_none());
    assert_eq!(
        canonical.request().expected_result_type,
        receipt.captured().binding().result_constraint()
    );
    assert!(Arc::ptr_eq(
        canonical.selected(),
        receipt.canonical_selection().unwrap()
    ));
}
