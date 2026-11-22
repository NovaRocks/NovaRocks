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

use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel, OperatorFunctionCatalogError,
    PureCatalogError, builtin::catalogue::builtin_engine_function_catalog, testkit,
    validate_installed_operator_functions,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, OperatorFunction, PureCompileControl,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[test]
fn full_catalogue_validation_accepts_actual_builtin_owners() {
    let catalog = testkit::shared_installed_builtin_owner_catalogue();
    validate_installed_operator_functions(&catalog, &Control).unwrap();
}

#[test]
fn otherwise_valid_partial_catalogue_has_no_operator_definition() {
    let catalog = testkit::rng_subset();
    assert!(matches!(
        validate_installed_operator_functions(&catalog, &Control),
        Err(OperatorFunctionCatalogError::MissingDefinition(id))
            if id == OperatorFunction::Negation.function_id()
    ));
}

#[test]
fn an_operator_definition_without_all_of_its_installation_records_cannot_seal() {
    let metadata = builtin_engine_function_catalog();
    let id = OperatorFunction::Negation.function_id();
    let definition = metadata.definition_by_id(&id).unwrap();
    let mut installed: Vec<_> = definition
        .binding_declaration()
        .unwrap()
        .overloads()
        .iter()
        .map(|overload| {
            let actual = metadata
                .pure_overload_declaration_observed(
                    &id,
                    FunctionKind::Scalar,
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            InstalledPureKernel {
                function: id.clone(),
                kind: FunctionKind::Scalar,
                implementation: actual.implementation().clone(),
                aggregate_state_format: None,
            }
        })
        .collect();
    assert!(installed.pop().is_some());
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition.clone()).unwrap();
    assert!(matches!(
        builder.seal_pure(installed),
        Err(PureCatalogError::InstalledManifestMismatch)
    ));
}

#[test]
fn binary_operator_resolution_uses_hidden_identity_and_exact_operand_types() {
    use arrow_schema::DataType;
    use novarocks_functions::{
        FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionBindingRequest,
        FunctionIntrinsicRowError, FunctionResultType, FunctionValueType, FunctionVisibility,
    };

    let metadata = builtin_engine_function_catalog();
    let operator = OperatorFunction::Divide;
    let arguments = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int16, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Decimal128(3, 2), true),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let id = operator.function_id();
    let definition = metadata.definition_by_id(&id).unwrap();
    assert_eq!(definition.visibility(), FunctionVisibility::Hidden);
    assert!(matches!(
        metadata.resolve_bound_user(
            operator.catalog_name(),
            FunctionKind::Scalar,
            request,
            &Control
        ),
        Err(FunctionBindingError::HiddenFunction | FunctionBindingError::UnknownFunction)
    ));
    let selected = metadata
        .resolve_bound_trusted_by_id(&id, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let [
        FunctionArgumentType::Value(left),
        FunctionArgumentType::Value(right),
    ] = &selected.selected.argument_types[..]
    else {
        panic!("binary operator must freeze two value arguments");
    };
    assert_eq!(
        left,
        &FunctionValueType::new(DataType::Decimal128(5, 0), false)
    );
    assert_eq!(
        right,
        &FunctionValueType::new(DataType::Decimal128(3, 2), true)
    );
    assert_eq!(
        selected.selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Decimal128(13, 6), true)),
    );
    let effects = definition
        .binding_declaration()
        .unwrap()
        .effect_declaration(&selected.selected.overload)
        .unwrap();
    assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::MayRaise);
}
