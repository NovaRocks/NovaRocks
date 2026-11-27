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

//! Test-owned allocation authority for the public expression-frame seam.
//! The original 8 MiB fixture budget and last-release assertion are preserved.
pub(super) use novarocks_expression::testkit::CountingAllocator;

pub(super) fn compile_sql_package_roundtrip(
    source: &novarocks_sql::compiler::SqlAuthoredPhysicalPlan,
    fields: &[arrow::datatypes::Field],
    mut inspect: impl FnMut(&novarocks_physical_plan::FragmentPackage),
) -> std::collections::BTreeMap<
    novarocks_physical_plan::FragmentId,
    std::sync::Arc<novarocks_local_program::LocalProgram>,
> {
    use novarocks_plan_codec::{
        physical_package_v2::{
            decode_fragment_package, encode_fragment_package, test_support::decode_limits,
        },
        resource_preflight_v2::FragmentDecodeResourceModel,
    };
    use novarocks_test_compile_to_execute::compiler_results::{
        compile_options, source_packages_with_declared_read_fields, validate_package,
    };
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use prost::Message;
    use std::sync::Arc;

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
    }
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let functions = novarocks_sql_testkit::installed_builtin_owner_catalogue();
    source_packages_with_declared_read_fields(source, fields)
        .into_iter()
        .map(|(id, package)| {
            let original = encode_fragment_package(&package, &Control)
                .unwrap()
                .encode_to_vec();
            let decoded = Arc::new(
                decode_fragment_package(&original, &model, &decode_limits(), &Control).unwrap(),
            );
            assert_eq!(
                encode_fragment_package(&decoded, &Control)
                    .unwrap()
                    .encode_to_vec(),
                original
            );
            inspect(&decoded);
            let program = novarocks_local_compiler::compile_fragment(
                validate_package(decoded),
                &functions,
                compile_options(),
                &Control,
            )
            .unwrap();
            (id, Arc::new(program))
        })
        .collect()
}
