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

//! Server-owned production compiled-package interpreter.
//!
//! Every role freezes and interprets the same carrier. The sealed pure
//! function catalogue digest enters Native compatibility, so processes with
//! different installed support cannot share an island. Nothing per task or
//! per statement selects an interpreter.

// Design: ADR-0185 (docs/adr/ADR-0185-compiled-packages-are-the-sole-static-plan-path.md)

use novarocks_execution::runtime::function_set::SealedExecutionFunctionSet;
use novarocks_native_adapter::backend_application::CompiledStaticPlan;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use novarocks_version::StaticPlanInterpreter;

use crate::compiled_composition::CompiledPackage;
use crate::provider_manifest::ServerProviderManifest;

/// Composed once before compatibility material and before either role starts.
pub struct ServerStaticPlan {
    compiled: CompiledPackage,
}

impl ServerStaticPlan {
    pub fn compose(
        function_set: &SealedExecutionFunctionSet,
        provider_manifest: &ServerProviderManifest,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            compiled: CompiledPackage::compose(function_set, provider_manifest)?,
        })
    }

    /// Compatibility retains the exact pure catalogue the backend executes.
    pub fn compatibility_component(&self) -> StaticPlanInterpreter {
        StaticPlanInterpreter::CompiledPackage {
            pure_function_catalog_digest: self.compiled.pure_function_catalog_digest(),
        }
    }

    pub fn backend_interpreter(&self) -> CompiledStaticPlan {
        self.compiled.backend_interpreter()
    }

    pub fn frontend_carrier(&self) -> novarocks_frontend_application::CompiledPackageCarrier {
        self.compiled.frontend_carrier()
    }
}

/// Startup composition runs once, before any listener opens, over statically
/// sized inputs; nothing could cancel it, so this control never refuses.
pub(crate) struct CompositionControl;

impl PureCompileControl for CompositionControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_binary_composes_compiled_in_both_roles_with_one_pure_digest() {
        let functions = crate::composition::compose_process_function_set().expect("function set");
        let manifest = ServerProviderManifest::seal().expect("provider manifest");
        let plan = ServerStaticPlan::compose(&functions, &manifest).expect("static plan");
        let seal =
            crate::compiled_composition::seal_pure_catalog(functions.catalog()).expect("seal");
        assert_eq!(
            plan.compatibility_component(),
            StaticPlanInterpreter::CompiledPackage {
                pure_function_catalog_digest: seal.catalog.digest(),
            }
        );
        plan.backend_interpreter();
        let carrier = plan.frontend_carrier();
        let another_carrier = plan.frontend_carrier();
        assert!(std::sync::Arc::ptr_eq(
            carrier.receiver_model(),
            another_carrier.receiver_model()
        ));
        let actual = carrier.receiver_limits();
        let expected = novarocks_plan_codec::physical_package_v2::PackageDecodeLimits::FROZEN.wire;
        assert_eq!(
            [
                actual.max_input_bytes,
                actual.max_requested_heap_bytes,
                actual.max_message_occurrences,
                actual.max_scalar_elements,
                actual.max_field_occurrences,
                actual.max_copied_bytes,
                actual.max_initialization_bytes,
                actual.max_wire_depth
            ],
            [
                expected.max_input_bytes,
                expected.max_requested_heap_bytes,
                expected.max_message_occurrences,
                expected.max_scalar_elements,
                expected.max_field_occurrences,
                expected.max_copied_bytes,
                expected.max_initialization_bytes,
                expected.max_wire_depth
            ]
        );
        let admission = carrier.admission();
        let limits = carrier.limits();
        let receiver = novarocks_plan_codec::physical_package_v2::PackageDecodeLimits::FROZEN;
        assert_eq!(admission.plan_limits, receiver.admission.plan_limits);
        assert_eq!(
            admission.source_retained_bytes,
            receiver.admission.source_retained_bytes
        );
        assert_eq!(
            admission.property_projection_limits.max_request_bytes,
            receiver
                .admission
                .property_projection_limits
                .max_request_bytes
        );
        assert_eq!(
            admission.property_projection_limits.max_coexisting_bytes,
            receiver
                .admission
                .property_projection_limits
                .max_coexisting_bytes
        );
        assert_eq!(
            admission.property_projection_limits.max_projection_work,
            receiver
                .admission
                .property_projection_limits
                .max_projection_work
        );
        assert_eq!(
            limits.source_retained_bytes,
            admission.source_retained_bytes
        );
    }
}
