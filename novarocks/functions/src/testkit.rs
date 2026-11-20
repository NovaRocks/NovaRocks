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

//! Catalogues built from the installed Functions owners for development tests.

use crate::{
    EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

struct FixtureCompileControl;

impl PureCompileControl for FixtureCompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

pub fn rng_subset() -> PureEngineFunctionCatalog {
    let actual = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("rand", FunctionKind::Scalar)
        .unwrap()
        .clone();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    // This deliberately seals a real RAND-only subset, not the Server catalogue.
    // These independent records name the actual installed implementation ABI.
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

// This fixture reads installed records from the same actual builtin metadata
// owners as Server's seal_candidate_pure_catalog. It never fabricates an ABI,
// implementation identity or state-format record. An uncovered definition is
// absent from the seal as in the actual candidate; required source calls still
// fail explicitly during the real LocalCompiler path.
pub fn shared_installed_builtin_owner_catalogue() -> std::sync::Arc<PureEngineFunctionCatalog> {
    static SHARED: std::sync::OnceLock<std::sync::Arc<PureEngineFunctionCatalog>> =
        std::sync::OnceLock::new();
    std::sync::Arc::clone(
        SHARED.get_or_init(|| std::sync::Arc::new(installed_builtin_owner_catalogue())),
    )
}

pub fn installed_builtin_owner_catalogue() -> PureEngineFunctionCatalog {
    let metadata = crate::builtin::catalogue::builtin_engine_function_catalog();
    let control = FixtureCompileControl;
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for definition in metadata.definitions() {
        let Some(declaration) = definition.binding_declaration() else {
            continue;
        };
        if declaration.validate_complete_effects().is_err() {
            continue;
        }
        let records: Result<Vec<_>, crate::FunctionSpecializationFailure> = declaration
            .overloads()
            .iter()
            .map(|overload| {
                let actual = metadata.pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &control,
                )?;
                Ok(InstalledPureKernel {
                    function: declaration.function_id().clone(),
                    kind: declaration.kind(),
                    implementation: actual.implementation().clone(),
                    aggregate_state_format: overload
                        .aggregate
                        .as_ref()
                        .map(|a| a.state_format.clone()),
                })
            })
            .collect();
        let Ok(records) = records else {
            continue;
        };
        builder.register(definition.clone()).unwrap();
        installed.extend(records);
    }
    builder.seal_pure(installed).unwrap()
}
