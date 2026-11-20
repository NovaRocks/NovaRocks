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
