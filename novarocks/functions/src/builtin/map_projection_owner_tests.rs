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
use crate::{
    FunctionArgument, FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
    specialize_scalar,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> MapProjectionOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        name,
        &signatures,
        novarocks_type_contract::FunctionKind::Scalar,
    )
    .unwrap();
    MapProjectionOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a MapProjectionOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}

fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control(name, sources, crate::binding_test_control())
}
pub(crate) fn prepared_with_control(
    name: &str,
    sources: &[FunctionValueType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if operation(name).is_none() {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = sources
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(i as u32 + 42)))
        .collect::<Vec<_>>();
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|call| call.into_prepared())
}

#[test]
fn map_parts_actual_generic_declarations_freeze_logical_child_and_real_result_field() {
    for name in ["map_keys", "map_values"] {
        for logical in [
            ValueLogicalType::Physical,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Json,
        ] {
            let item = match logical {
                ValueLogicalType::LargeInt => FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    true,
                    logical,
                )
                .unwrap(),
                ValueLogicalType::Json => {
                    FunctionValueType::try_with_logical_type(DataType::Utf8, true, logical).unwrap()
                }
                _ => FunctionValueType::new(DataType::Int32, true),
            };
            let mut key = item.clone();
            key = key
                .with_nullable(false)
                .expect("valid function nullability declaration");
            let fields = vec![
                Arc::new(key.try_to_field("authored-key").unwrap()),
                Arc::new(item.try_to_field("authored-value").unwrap()),
            ]
            .into();
            let entries = Arc::new(
                Field::new("authored-entries", DataType::Struct(fields), false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "source-fact".into(),
                        "preserved".into(),
                    )]),
                ),
            );
            let source = FunctionValueType::from_legacy_declared_projection(
                DataType::Map(entries.clone(), true),
                true,
            );
            let p = prepared_for_test(name, std::slice::from_ref(&source)).unwrap();
            assert_eq!(
                p.contract().selected().argument_types.as_ref(),
                &[crate::FunctionArgumentType::Value(source)]
            );
            let DataType::List(field) = p.contract().result_type().data_type() else {
                unreachable!()
            };
            assert_eq!(
                p.contract()
                    .result_type()
                    .child_at(0)
                    .unwrap()
                    .logical_type(),
                logical
            );
            assert_eq!(field.data_type(), item.data_type());
            assert!(p.contract().result_type().nullable());
            assert!(field.is_nullable());
            assert_eq!(
                p.contract().effects().own_row_error,
                FunctionIntrinsicRowError::MayRaise
            );
            assert_eq!(
                p.contract().effects().null_behavior,
                FunctionNullBehavior::Strict
            );
            let crate::FunctionArgumentType::Value(selected) =
                &p.contract().selected().argument_types[0]
            else {
                unreachable!()
            };
            let DataType::Map(actual, sorted) = selected.data_type() else {
                unreachable!()
            };
            assert!(actual.metadata().is_empty());
            assert!(matches!(actual.data_type(), DataType::Struct(fields) if fields.len() == 2));
            assert!(*sorted);
        }
    }
}
