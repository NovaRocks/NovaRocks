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

//! Production compiled-package composition and pure function support boundary.
//!
//! Complete installed pure-owner coverage decides which definitions are sealed.
//! The exact seal digest participates in Native compatibility; its deterministic
//! exclusions are reported by `--build-info` for capability restoration work.

use std::sync::Arc;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::runtime::function_set::SealedExecutionFunctionSet;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionDefinition,
    InstalledPureKernel, PureEngineFunctionCatalog, validate_installed_operator_functions,
};
use novarocks_native_adapter::backend_application::CompiledStaticPlan;
use novarocks_plan_codec::physical_package_v2::{
    FROZEN_CONSTANT_POLICY, PackageDecodeLimits, PackageEncodeLimits,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorCodecError;

use crate::provider_manifest::ServerProviderManifest;
use crate::static_plan::CompositionControl;

/// FE and BE share the codec owner's structural constant policy. A deployment
/// cannot silently broaden or narrow the static wire language.
pub(crate) fn frontend_constant_policy(
    configured: ConstantPolicy,
) -> anyhow::Result<ConstantPolicy> {
    anyhow::ensure!(
        configured == FROZEN_CONSTANT_POLICY,
        "runtime.frontend_constant_policy must equal the frozen compiled-package policy; configured={configured:?}; frozen={FROZEN_CONSTANT_POLICY:?}"
    );
    Ok(FROZEN_CONSTANT_POLICY)
}

/// Everything the production backend interpreter is composed from.
pub(crate) struct CompiledPackage {
    decode_limits: PackageDecodeLimits,
    decode_model: Arc<FragmentDecodeResourceModel>,
    constants: ConstantPolicy,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<ConnectorCodecError>>,
}

impl CompiledPackage {
    pub(crate) fn compose(
        function_set: &SealedExecutionFunctionSet,
        provider_manifest: &ServerProviderManifest,
    ) -> anyhow::Result<Self> {
        let seal = seal_pure_catalog(function_set.catalog())?;
        tracing::info!(
            installed_definitions = seal.installed_definitions,
            excluded_definitions = seal.excluded.len(),
            pure_function_catalog_digest = %hex::encode(seal.catalog.digest()),
            "compiled-package interpreter composes a sealed pure catalogue"
        );
        tracing::debug!(excluded = ?seal.excluded, "pure catalogue exclusions");
        Ok(Self {
            decode_limits: PackageDecodeLimits::FROZEN,
            decode_model: Arc::new(FragmentDecodeResourceModel::try_new(&CompositionControl)?),
            constants: FROZEN_CONSTANT_POLICY,
            functions: Arc::new(seal.catalog),
            providers: Arc::new(provider_manifest.compose_pure_program_catalog()?),
        })
    }

    pub(crate) fn pure_function_catalog_digest(&self) -> [u8; 32] {
        self.functions.digest()
    }

    /// The frontend freezes packages under the receiver's own admission.
    pub(crate) fn frontend_carrier(
        &self,
    ) -> novarocks_frontend_application::CompiledPackageCarrier {
        novarocks_frontend_application::CompiledPackageCarrier::new(
            self.decode_limits.admission,
            PackageEncodeLimits::FROZEN,
            Arc::clone(&self.decode_model),
            self.decode_limits.wire,
        )
    }

    pub(crate) fn backend_interpreter(&self) -> CompiledStaticPlan {
        CompiledStaticPlan::new(
            Arc::clone(&self.decode_model),
            self.decode_limits.clone(),
            Arc::clone(&self.functions),
            Arc::clone(&self.providers),
            self.constants,
        )
    }

    /// Required FE constants use the same sealed function catalogue as the BE.
    /// Live Root capacity is supplied separately for each actual computation.
    pub(crate) fn frontend_required_constant_interpreter(
        &self,
    ) -> novarocks_frontend_application::RequiredConstantInterpreter {
        novarocks_frontend_application::RequiredConstantInterpreter::new(
            Arc::clone(&self.functions),
            self.constants,
        )
    }
}

/// The production pure catalogue and the definitions outside its support boundary.
pub(crate) struct PureCatalogSeal {
    pub(crate) catalog: PureEngineFunctionCatalog,
    pub(crate) installed_definitions: usize,
    /// A deterministic name, kind and reason for every excluded definition.
    pub(crate) excluded: Vec<PureCatalogExclusion>,
}

/// The support gap for one complete function definition.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct PureCatalogExclusion {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) reason: String,
}

/// Seals exactly the process definitions whose every overload has an
/// installed pure owner with complete effects.
///
/// Each record is read from the owner the metadata catalogue itself carries;
/// none is authored here, and a definition with any uncovered overload is
/// left out whole rather than sealed partially. Because the records come from
/// the same owners `seal_pure` compares them with, that comparison proves only
/// those owners' consistency. It is not an independent installation manifest:
/// no execution-side producer of one exists yet. The resulting digest differs
/// from the process metadata catalogue's digest whenever anything is left out,
/// so it enters the process compatibility identity through the interpreter
/// component.
pub(crate) fn seal_pure_catalog(
    metadata: &EngineFunctionCatalog,
) -> anyhow::Result<PureCatalogSeal> {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    let mut installed_definitions = 0;
    let mut excluded = Vec::new();
    for definition in metadata.definitions() {
        match installed_records(metadata, definition) {
            Ok(records) => {
                builder.register(definition.clone()).map_err(|error| {
                    anyhow::anyhow!(
                        "register pure definition {}: {error}",
                        definition.canonical_name()
                    )
                })?;
                installed.extend(records);
                installed_definitions += 1;
            }
            Err(reason) => excluded.push(PureCatalogExclusion {
                name: definition.canonical_name().to_owned(),
                kind: format!("{:?}", definition.kind()),
                reason,
            }),
        }
    }
    excluded.sort_by(|left, right| (&left.name, &left.kind).cmp(&(&right.name, &right.kind)));
    anyhow::ensure!(
        installed_definitions > 0,
        "no process function definition has an installed pure owner"
    );
    let catalog = builder
        .seal_pure(installed)
        .map_err(|error| anyhow::anyhow!("seal the production pure function catalogue: {error}"))?;
    validate_installed_operator_functions(&catalog, &CompositionControl)?;
    Ok(PureCatalogSeal {
        catalog,
        installed_definitions,
        excluded,
    })
}

fn installed_records(
    metadata: &EngineFunctionCatalog,
    definition: &FunctionDefinition,
) -> Result<Vec<InstalledPureKernel>, String> {
    let declaration = definition
        .binding_declaration()
        .ok_or_else(|| "no binding declaration".to_owned())?;
    declaration
        .validate_complete_effects()
        .map_err(|error| error.to_string())?;
    declaration
        .overloads()
        .iter()
        .filter(|overload| !definition.is_declared_only_pure_overload(&overload.identity))
        .map(|overload| {
            let installed = metadata
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .map_err(|error| format!("{}: {error}", overload.identity.as_str()))?;
            Ok(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: installed.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|aggregate| aggregate.state_format.clone()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composition::compose_process_function_set;
    use novarocks_functions::FunctionKind;

    #[test]
    fn frontend_and_backend_constants_share_one_frozen_policy() {
        assert_eq!(
            frontend_constant_policy(FROZEN_CONSTANT_POLICY).unwrap(),
            PackageDecodeLimits::FROZEN.constant_policy
        );
        let mut wider = FROZEN_CONSTANT_POLICY;
        wider.max_rows += 1;
        assert!(frontend_constant_policy(wider).is_err());
        let mut narrower = FROZEN_CONSTANT_POLICY;
        narrower.max_dictionary_depth -= 1;
        assert!(frontend_constant_policy(narrower).is_err());
    }

    #[test]
    fn collection_lambda_overloads_are_installed_in_the_production_seal() {
        use novarocks_functions::{FunctionIntrinsicRowError, PureKernelAbi};
        use novarocks_type_contract::{ArgumentControl, EvaluationDemand, FunctionNullBehavior};

        let function_set = compose_process_function_set().unwrap();
        let seal = seal_pure_catalog(function_set.catalog()).unwrap();
        let catalog = seal.catalog.metadata();
        for (name, expected_row_error) in [
            ("transform", FunctionIntrinsicRowError::NoRowError),
            ("filter", FunctionIntrinsicRowError::NoRowError),
            ("any_match", FunctionIntrinsicRowError::NoRowError),
            ("all_match", FunctionIntrinsicRowError::NoRowError),
            ("array_sort", FunctionIntrinsicRowError::MayRaise),
            ("map_filter", FunctionIntrinsicRowError::NoRowError),
            ("transform_keys", FunctionIntrinsicRowError::MayRaise),
            ("transform_values", FunctionIntrinsicRowError::NoRowError),
        ] {
            let definition = catalog.definition(name, FunctionKind::Scalar).unwrap();
            let declaration = definition.binding_declaration().unwrap();
            let comparator_or_lambda = novarocks_functions::FunctionOverloadId::try_new(format!(
                "builtin.scalar/{name}/lambda-v1"
            ))
            .unwrap();
            let installed = catalog
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    FunctionKind::Scalar,
                    &comparator_or_lambda,
                    &CompositionControl,
                )
                .unwrap();
            assert_eq!(installed.implementation().abi, PureKernelAbi::HigherOrderV1);
            assert_eq!(installed.effects().own_row_error, expected_row_error);
            assert_eq!(
                installed.effects().null_behavior,
                FunctionNullBehavior::ControlDefined
            );
            assert_eq!(
                installed.effects().argument_control,
                ArgumentControl::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                }
            );
            assert!(seal.catalog.public_function_overloads().any(|entry| {
                entry.name == name
                    && entry.function_id == declaration.function_id()
                    && entry.overload_id == &comparator_or_lambda
            }));
            if name == "array_sort" {
                let natural = seal
                    .catalog
                    .public_function_overloads()
                    .find(|entry| entry.name == name && entry.overload_id != &comparator_or_lambda)
                    .expect("natural and comparator sort use one public definition");
                assert_eq!(natural.function_id, declaration.function_id());
                let installed = catalog
                    .pure_overload_declaration_observed(
                        natural.function_id,
                        FunctionKind::Scalar,
                        natural.overload_id,
                        &CompositionControl,
                    )
                    .unwrap();
                assert_eq!(installed.implementation().abi, PureKernelAbi::ScalarV1);
                assert_eq!(
                    installed.effects().null_behavior,
                    FunctionNullBehavior::Strict
                );
                assert_eq!(installed.effects().argument_control, ArgumentControl::Eager);
            }
        }
    }

    #[test]
    fn aggregate_window_inventory_uses_the_production_seal_and_selected_types() {
        use arrow::datatypes::DataType;
        use novarocks_functions::{
            FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionValueType,
            PureCallLifecycle, PureKernelAbi,
        };

        let function_set = compose_process_function_set().unwrap();
        let seal = seal_pure_catalog(function_set.catalog()).unwrap();
        let catalog = seal.catalog.metadata();

        for row in seal.catalog.public_function_overloads() {
            if !matches!(row.kind, FunctionKind::Aggregate | FunctionKind::Window) {
                continue;
            }
            let installed = catalog
                .pure_overload_declaration_observed(
                    row.function_id,
                    row.kind,
                    row.overload_id,
                    &CompositionControl,
                )
                .unwrap();
            println!(
                "window-public-overload\t{}\t{}\t{}\t{}\t{}\t{:?}",
                row.name,
                row.function_id.as_str(),
                row.overload_id.as_str(),
                row.argument_types,
                row.return_type,
                installed.implementation().abi,
            );
        }
        for definition in catalog.definitions() {
            let Some(binding) = definition.binding_declaration() else {
                continue;
            };
            for overload in binding.overloads() {
                if binding.kind() == FunctionKind::Aggregate
                    && definition.is_declared_only_pure_overload(&overload.identity)
                {
                    println!(
                        "window-declared-only\t{}\t{}\t{}\t{}\t{}",
                        definition.canonical_name(),
                        binding.function_id().as_str(),
                        overload.identity.as_str(),
                        overload.argument_pattern,
                        overload.result_pattern,
                    );
                }
            }
        }

        let samples: &[(&str, &[DataType], usize, bool, bool)] = &[
            ("count_if", &[DataType::Boolean], 1, false, true),
            ("count_if", &[DataType::Boolean], 1, true, false),
            ("bool_and", &[DataType::Boolean], 1, false, true),
            ("var_pop", &[DataType::Float64], 1, false, true),
            ("stddev_pop", &[DataType::Float64], 1, false, true),
            ("min", &[DataType::Utf8], 1, false, true),
            ("max", &[DataType::Utf8], 1, false, true),
            ("min", &[DataType::Int64, DataType::Int64], 2, false, false),
            (
                "map_agg",
                &[DataType::Int64, DataType::Utf8],
                2,
                false,
                false,
            ),
            (
                "array_agg",
                &[DataType::Utf8, DataType::Int64],
                1,
                false,
                true,
            ),
            (
                "array_agg",
                &[DataType::Utf8, DataType::Utf8],
                1,
                true,
                true,
            ),
            (
                "listagg",
                &[
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Boolean,
                    DataType::Utf8,
                    DataType::Boolean,
                    DataType::Utf8,
                ],
                5,
                true,
                true,
            ),
        ];
        for &(name, types, logical_count, distinct, available) in samples {
            let arguments = types
                .iter()
                .map(|ty| FunctionArgument::Value {
                    value_type: FunctionValueType::new(ty.clone(), true),
                    constant: None,
                })
                .collect::<Vec<_>>();
            let binding = catalog
                .resolve_bound_user(
                    name,
                    FunctionKind::Aggregate,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: logical_count,
                        expected_result_type: None,
                    },
                    &CompositionControl,
                )
                .unwrap();
            let installed = catalog
                .pure_overload_declaration_observed(
                    &binding.function_id,
                    binding.kind,
                    &binding.selected.overload,
                    &CompositionControl,
                )
                .unwrap();
            let ordered = arguments.len() > logical_count;
            let admission = installed.admit_selected_lifecycle_observed(
                &binding.selected,
                logical_count,
                PureCallLifecycle::AggregateWindow { distinct, ordered },
                &CompositionControl,
            );
            if available {
                assert_eq!(
                    installed.implementation().abi,
                    PureKernelAbi::AggregateWindowV1
                );
                assert!(admission.is_ok(), "{name} {types:?}: {admission:?}");
            } else {
                assert!(
                    matches!(&admission, Err(FunctionBindingError::UnavailableImplementation(id))
                        if *id == binding.selected.overload),
                    "{name} {types:?}: {admission:?}"
                );
            }
            println!(
                "window-selected-profile\t{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{:?}",
                name,
                binding.function_id.as_str(),
                binding.selected.overload.as_str(),
                types,
                logical_count,
                distinct,
                ordered,
                admission,
            );
        }
    }

    /// The production seal holds exactly the definitions with installed pure
    /// owners: RAND is one, so it is sealed with its owner's own records, and
    /// the seal is a strict subset whose digest is not the metadata digest.
    #[test]
    fn the_seal_is_exactly_the_installed_pure_owners() {
        let function_set = compose_process_function_set().expect("process function set");
        let metadata = function_set.catalog();
        let seal = seal_pure_catalog(metadata).expect("production seal");

        assert!(seal.installed_definitions > 0);
        assert_eq!(
            seal.installed_definitions + seal.excluded.len(),
            metadata.definitions().len(),
            "every process definition is either sealed or reported excluded"
        );
        let rand = seal
            .catalog
            .metadata()
            .definition("rand", FunctionKind::Scalar)
            .expect("RAND has an installed pure owner")
            .binding_declaration()
            .expect("RAND binding");
        for overload in rand.overloads() {
            seal.catalog
                .metadata()
                .pure_overload_declaration_observed(
                    rand.function_id(),
                    rand.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .expect("every RAND overload is sealed with its installed implementation");
        }
        for definition in seal.catalog.metadata().definitions() {
            assert!(
                installed_records(metadata, definition).is_ok(),
                "{} was sealed without complete installed coverage",
                definition.canonical_name()
            );
        }
        if !seal.excluded.is_empty() {
            assert_ne!(seal.catalog.digest(), metadata.digest());
        }
        // Both the compatibility digest and the reported support gaps are deterministic.
        let again = seal_pure_catalog(metadata).expect("second seal");
        assert_eq!(again.catalog.digest(), seal.catalog.digest());
        assert_eq!(again.excluded, seal.excluded);
        assert!(
            seal.excluded
                .windows(2)
                .all(|pair| { (&pair[0].name, &pair[0].kind) < (&pair[1].name, &pair[1].kind) })
        );
        assert!(seal.excluded.iter().all(|entry| !entry.reason.is_empty()));
    }
}
