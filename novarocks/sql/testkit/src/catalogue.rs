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
// This fixture reads installed records from the same actual builtin metadata
// owners as Server's seal_candidate_pure_catalog. It never fabricates an ABI,
// implementation identity or state-format record. An uncovered definition is
// absent from the seal as in the actual candidate; required source calls still
// fail explicitly during the real LocalCompiler path.
pub fn installed_builtin_owner_catalogue() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::{EngineFunctionCatalogBuilder, InstalledPureKernel};
    let metadata = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for definition in metadata.definitions() {
        let Some(declaration) = definition.binding_declaration() else {
            continue;
        };
        if declaration.validate_complete_effects().is_err() {
            continue;
        }
        let records: Result<Vec<_>, novarocks_functions::FunctionSpecializationFailure> =
            declaration
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
