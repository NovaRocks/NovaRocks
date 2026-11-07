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

//! The explicit, small first-party catalogue used by FE test compositions.
use super::*;
use novarocks_functions::{
    EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionKind, InstalledPureKernel,
};
use novarocks_plan_codec::physical_package_v2::{PackageDecodeLimits, PackageEncodeLimits};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_sql::compiler::SqlCompileControl;

pub(crate) fn test_required_constant_interpreter() -> RequiredConstantInterpreter {
    test_product().0
}

pub(super) fn test_product() -> (RequiredConstantInterpreter, Arc<EngineFunctionCatalog>) {
    let compile = SqlCompileControl::unbounded();
    let metadata = Arc::new(
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap(),
    );
    // A small real catalogue fixture, not a second production seal.
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for name in ["lower", "concat", "if", "coalesce", "rand"] {
        let definition = metadata.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        declaration.validate_complete_effects().unwrap();
        for overload in declaration.overloads() {
            let pure = metadata
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &compile,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: pure.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    let pure = Arc::new(builder.seal_pure(installed).unwrap());
    let providers = Arc::new(
        PureProviderProgramCatalog::<ConnectorCodecError>::try_new(&[], vec![], &compile).unwrap(),
    );
    let limits = PackageDecodeLimits::FROZEN;
    let carrier = CompiledPackageCarrier::new(
        limits.admission,
        PackageEncodeLimits::FROZEN,
        Arc::new(FragmentDecodeResourceModel::try_new(&compile).unwrap()),
        limits.wire,
    );
    let interpreter = RequiredConstantInterpreter::new(
        pure,
        providers,
        carrier,
        crate::application::test_constant_policy(),
    );
    (interpreter, metadata)
}
