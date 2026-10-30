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

//! Actual FE casts that change an authored logical value domain.
//!
//! The request's function catalog owns conversion selection. This module does
//! not infer identity from carriers or install execution implementations.

use novarocks_parser::Span;
use novarocks_type_contract::{
    FunctionValueType, PureCompileControl, preserves_value_domains_observed,
};

use crate::analysis::{ExprKind, LiteralValue, TypedExpr, function_argument};
use crate::analyze_error::AnalyzeError;

impl super::AnalyzerContext<'_> {
    /// Preserve ordinary carrier CASTs, but select the exact conversion owner
    /// whenever the root or a nested logical identity changes. Literal NULL
    /// alone can use the existing typed-NULL declaration without evaluation.
    pub(super) fn cast_to_value_type(
        &self,
        source: TypedExpr,
        target: FunctionValueType,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        cast_to_value_type(
            source,
            target,
            span,
            self.function_catalog,
            &self.sql_semantics,
            self.constant_policy,
            self.control,
        )
    }
}

/// The sole assignment/CAST conversion author, also consumed by exact internal
/// root-output declarations after ordinary query analysis has completed.
pub(super) fn cast_to_value_type(
    source: TypedExpr,
    target: FunctionValueType,
    span: Span,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    super::helpers::validate_value_type(&source.value_type, control)?;
    super::helpers::validate_value_type(&target, control)?;
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    if source.value_type == target {
        return Ok(source);
    }
    // The semantic cast walk observes the actual caller's control; its
    // checked owner preserves nested nominal domains across carrier casts.
    if constant_is_null(&source, constant_policy, control)? {
        return typed_null_conversion(source, target, constant_policy, control);
    }
    let carrier_cast = preserves_value_domains_observed(
        source.value_type.value_type(),
        target.value_type(),
        |units| {
            control
                .checkpoint(
                    novarocks_type_contract::CompilePhase::Validate,
                    u32::try_from(units).expect("one checked semantic node"),
                )
                .map_err(AnalyzeError::control)
        },
    )?;
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    if carrier_cast
        || novarocks_type_contract::is_default_json_structure_cast(&source.value_type, &target)
    {
        // This exact existing intrinsic consumes the complete target,
        // including JSON children; it is not a same-shape domain bridge.
        return Ok(TypedExpr {
            kind: ExprKind::Cast {
                target: target.data_type().clone(),
                expr: Box::new(source),
                decimal_overflow_policy: sql_semantics.sql_mode().decimal_overflow_policy(),
            },
            value_type: target,
        });
    }
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    // Resolver internals are a bounded owner operation, not a claim of
    // cooperative traversal or runtime resource authorization.
    let result = convert_value_domain_with_catalog(
        function_catalog,
        source,
        target,
        sql_semantics.sql_mode().decimal_overflow_policy(),
        constant_policy,
        control,
    )
    .map_err(|error| error.at_type_mismatch(span))?;
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    Ok(result)
}

/// A narrow existing binder adapter: the caller owns request observation and
/// the supplied catalog remains the sole selected conversion authority.
pub(super) fn convert_value_domain_with_catalog(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    source: TypedExpr,
    target: FunctionValueType,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    control
        .checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)
        .map_err(AnalyzeError::control)?;
    if constant_is_null(&source, constant_policy, control)? {
        return typed_null_conversion(source, target, constant_policy, control);
    }
    let intermediate =
        novarocks_functions::builtin::value_conversion::conversion_intermediate_type(
            &source.value_type,
            &target,
        )
        .map_err(|error| match error {
            novarocks_functions::FunctionBindingError::NoMatchingOverload
                if source.value_type.logical_type()
                    == novarocks_type_contract::ValueLogicalType::Physical
                    && matches!(
                        source.value_type.data_type(),
                        arrow::datatypes::DataType::Decimal128(_, _)
                            | arrow::datatypes::DataType::Decimal256(_, _)
                    )
                    && target.logical_type()
                        == novarocks_type_contract::ValueLogicalType::LargeInt
                    && target.data_type() == &arrow::datatypes::DataType::FixedSizeBinary(16)
                    && target.nullable() =>
            {
                AnalyzeError::unsupported_expression_without_span(format!(
                    "CAST from {:?} to nominal LARGEINT is not implemented",
                    source.value_type.data_type(),
                ))
            }
            error => AnalyzeError::internal(error.to_string()),
        })?
        .ok_or_else(|| {
            AnalyzeError::internal(
                "a logical-domain conversion requires an exact admitted implementation",
            )
        })?;
    let argument = function_argument(&source, constant_policy, control)
        .map_err(AnalyzeError::function_binding)?;
    let binding = catalog
        .resolve_value_conversion_binding(&argument, &intermediate, control)
        .map_err(AnalyzeError::function_binding)?;
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        return Err(AnalyzeError::internal(
            "value conversion owner must produce a scalar result",
        ));
    };
    let value_type = result.clone();
    let converted = TypedExpr {
        value_type,
        kind: ExprKind::FunctionCall {
            name: novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME.to_string(),
            volatility: binding.semantics.volatility,
            binding: crate::binding::SqlFunctionBinding::new_with_conversion_constraint(
                binding,
                decimal_overflow_policy,
                intermediate,
                target.clone(),
            ),
            args: vec![source],
            distinct: false,
        },
    };
    if converted
        .value_type
        .exactly_equals_checked_observed(&target, || {
            control
                .checkpoint(novarocks_type_contract::CompilePhase::Validate, 1)
                .map_err(AnalyzeError::control)
        })?
    {
        Ok(converted)
    } else {
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(converted),
                target: target.data_type().clone(),
                decimal_overflow_policy,
            },
            value_type: target,
        })
    }
}

/// Checked materialized NULLs remain checked values; syntax NULLs retain their
/// syntax shape until their admitted construction boundary.
pub(super) fn constant_is_null(
    source: &TypedExpr,
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, AnalyzeError> {
    match &source.kind {
        ExprKind::Literal(LiteralValue::Null) => Ok(true),
        ExprKind::Constant(value) => {
            function_argument(source, policy, control).map_err(AnalyzeError::function_binding)?;
            value
                .is_null_observed(
                    novarocks_type_contract::CompilePhase::FunctionSpecialization,
                    control,
                )
                .map_err(novarocks_functions::FunctionBindingError::from)
                .map_err(AnalyzeError::function_binding)
        }
        _ => Ok(false),
    }
}
fn typed_null_conversion(
    source: TypedExpr,
    mut target: FunctionValueType,
    policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<TypedExpr, AnalyzeError> {
    target = target
        .with_nullable(true)
        .expect("validated root NULL contract");
    let kind = match source.kind {
        ExprKind::Constant(_) => {
            let field = arrow::datatypes::Field::new(
                "literal",
                target.data_type().clone(),
                target.nullable(),
            );
            let value = novarocks_functions::ConstantValue::null(
                std::sync::Arc::new(field),
                target.clone(),
                policy,
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                control,
            )
            .map_err(novarocks_functions::FunctionBindingError::from)
            .map_err(AnalyzeError::function_binding)?;
            ExprKind::Constant(value)
        }
        ExprKind::Literal(LiteralValue::Null) => ExprKind::Literal(LiteralValue::Null),
        _ => {
            return Err(AnalyzeError::internal(
                "typed NULL conversion requires an actual NULL constant",
            ));
        }
    };
    Ok(TypedExpr {
        kind,
        value_type: target,
    })
}

#[cfg(test)]
mod checked_constant_tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;
    use novarocks_functions::{ConstantPool, ConstantValue};
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
    };
    use std::sync::{Arc, Mutex};

    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            if let Some((at, _)) = self.stop {
                assert!(trace.len() < at, "no callback after original refusal");
            }
            trace.push((phase, units));
            match self.stop.clone() {
                Some((at, error)) if trace.len() == at => Err(error),
                _ => Ok(()),
            }
        }
    }
    fn control(stop: Option<(usize, CompileControlError)>) -> Control {
        Control {
            trace: Default::default(),
            stop,
        }
    }
    fn source() -> TypedExpr {
        let value_type = FunctionValueType::new(DataType::Int64, false);
        let pool = ConstantPool::try_new(
            Arc::new(value_type.try_to_field("source").unwrap()),
            value_type.clone(),
            Int64Array::from(vec![-99, 42]).to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::FunctionSpecialization,
            &control(None),
        )
        .unwrap();
        TypedExpr {
            kind: ExprKind::Constant(pool.value(1).unwrap()),
            value_type,
        }
    }
    #[test]
    fn materialized_conversion_binding_retains_original_pool_and_selected_ordinal() {
        let source = source();
        let ExprKind::Constant(original) = &source.kind else {
            panic!("checked source");
        };
        let target = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let output = convert_value_domain_with_catalog(
            crate::functions::builtin_sql_function_catalog(),
            source.clone(),
            target.clone(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &control(None),
        )
        .unwrap();
        assert_eq!(output.value_type, target);
        let ExprKind::FunctionCall { args, .. } = output.kind else {
            panic!("actual selected conversion");
        };
        let ExprKind::Constant(retained) = &args[0].kind else {
            panic!("checked source must remain checked");
        };
        assert_eq!(retained.try_i64().unwrap(), Some(42));
        assert_eq!(retained.ordinal(), 1);
        assert!(Arc::ptr_eq(
            original.pool().array(),
            retained.pool().array()
        ));
        assert!(Arc::ptr_eq(
            original.pool().field_ref(),
            retained.pool().field_ref()
        ));
        let baseline = control(None);
        convert_value_domain_with_catalog(
            crate::functions::builtin_sql_function_catalog(),
            source.clone(),
            target.clone(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &baseline,
        )
        .unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=trace.len() {
                let original_control = control(Some((at, error.clone())));
                let failure = convert_value_domain_with_catalog(
                    crate::functions::builtin_sql_function_catalog(),
                    source.clone(),
                    target.clone(),
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    crate::constant::test_constant_policy(),
                    &original_control,
                )
                .unwrap_err();
                assert_eq!(failure.control_error(), Some(error.clone()));
                assert_eq!(*original_control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
    #[test]
    fn materialized_typed_null_conversion_never_reconstructs_syntax_literal() {
        let source_type = FunctionValueType::new(DataType::Int64, true);
        let value = ConstantValue::null(
            Arc::new(source_type.try_to_field("source").unwrap()),
            source_type.clone(),
            crate::constant::test_constant_policy(),
            CompilePhase::FunctionSpecialization,
            &control(None),
        )
        .unwrap();
        let target =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let output = convert_value_domain_with_catalog(
            crate::functions::builtin_sql_function_catalog(),
            TypedExpr {
                kind: ExprKind::Constant(value),
                value_type: source_type,
            },
            target,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &control(None),
        )
        .unwrap();
        assert!(output.value_type.nullable());
        assert_eq!(output.value_type.logical_type(), ValueLogicalType::Json);
        let ExprKind::Constant(value) = output.kind else {
            panic!("materialized NULL retains the checked owner");
        };
        assert_eq!(value.value_type(), &output.value_type);
        assert_eq!(value.try_utf8().unwrap(), None);
        assert!(value.pool().field_ref().metadata().as_arc().is_none());
        assert!(value.pool().source_metadata_namespace().is_none());
    }
}
