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
//! Single real SQL source, its exact source-journal effects, package extraction,
//! installed provider contract fixture, LocalCompiler and the actual Frame.
//! This fixture does not execute a connector or claim a host memory grant.
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_connector_contract::*;
use novarocks_functions::*;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeKind,
};
use novarocks_physical_plan::*;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, author_fragment_package_semantics};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueTypeStorageLifetime,
};
use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric transaction never waits")
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 64,
        max_array_nodes: 1024,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for name in ["if", "coalesce"] {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        for signature in definition.canonical_signatures() {
            installed.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{signature}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::ControlIntrinsicV1,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(installed).unwrap()
}
// The same canonicalizing provider seam used by existing LocalCompiler tests.
// This closes the exact private/public recipe contract, not an Iceberg service.
struct Port;
impl ConnectorReadProgramCompiler for Port {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        control.checkpoint(CompilePhase::ProviderValidation, 0)?;
        let recipe = input.scan().recipe();
        let result = ConnectorReadRelationRecipeDraft::try_new(
            recipe.binding().clone(),
            recipe.relation().clone(),
            recipe.columns().to_vec(),
        )
        .map_err(|error| {
            PureProviderCompileError::Provider(ConnectorError::new(
                novarocks_connector_contract::ConnectorErrorKind::InvalidRequest,
                error.to_string(),
            ))
        });
        control.checkpoint(CompilePhase::ProviderValidation, 0)?;
        result
    }
}
fn providers() -> PureProviderProgramCatalog<ConnectorError> {
    let id = ConnectorProviderId::parse("iceberg").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(id.clone(), true, false)],
        vec![PureProviderProgramDefinition::new(
            id,
            Some(Arc::new(Port)
                as Arc<
                    dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                >),
            None,
        )],
        &Control,
    )
    .unwrap()
}
fn frozen_reads(
    source: &SqlAuthoredPhysicalPlan,
) -> BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead> {
    frozen_reads_with_residual_mode(source, false)
}
fn frozen_reads_with_residual_mode(
    source: &SqlAuthoredPhysicalPlan,
    admit_original_scan_residuals: bool,
) -> BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead> {
    frozen_reads_with_explicit_source_shape(source, admit_original_scan_residuals, false)
}
pub fn frozen_reads_with_explicit_source_shape(
    source: &SqlAuthoredPhysicalPlan,
    admit_original_scan_residuals: bool,
    admit_required_inlist_shape: bool,
) -> BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead> {
    let mut reads = BTreeMap::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                occurrence,
                relation,
                read_budget,
                provider_outputs,
                residuals,
                derived_values,
            } = &node.kind
            {
                if !admit_original_scan_residuals {
                    assert!(residuals.is_empty());
                }
                // Residual definitions remain in the original physical Scan.
                // This fixture supplies no provider predicate guarantees.
                assert!(derived_values.is_empty());
                assert!(relation.predicate_guarantees().is_empty());
                assert_eq!(
                    relation.provided_properties().distribution,
                    Distribution::Unconstrained
                );
                assert!(relation.provided_properties().ordering.is_empty());
                assert_eq!(relation.schema().len(), 1);
                assert_eq!(provider_outputs.len(), 1);
                assert_eq!(provider_outputs[0].0, relation.schema()[0].column);
                let field = &relation.schema()[0];
                if !admit_required_inlist_shape {
                    assert!(!field.ty.nullable());
                }
                let carrier = match field.ty.data_type() {
                    DataType::Int8 => ConnectorValueType::TinyInt,
                    DataType::Int16 => ConnectorValueType::SmallInt,
                    DataType::Int32 => ConnectorValueType::Integer,
                    DataType::Int64 => ConnectorValueType::BigInt,
                    DataType::Float32 => ConnectorValueType::Real,
                    DataType::Float64 => ConnectorValueType::Double,
                    DataType::Utf8 if admit_required_inlist_shape => connector_type_for_value_type(
                        &field.ty,
                    )
                    .expect("actual required Utf8 source has its exact connector projection"),
                    // Keep full precision/signed scale and logical class from
                    // the sole connector assignment projection author.
                    DataType::Decimal128(..) => connector_type_for_value_type(&field.ty).expect(
                        "actual Decimal128 source must have its exact connector projection",
                    ),
                    _ => panic!("closed native nonnull test source has an unexpected carrier"),
                };
                let read = relation.read();
                let recipe = ConnectorReadRelationRecipeDraft::try_new(
                    read.binding.clone(),
                    read.relation.clone(),
                    vec![field.column.column_payload.clone()],
                )
                .unwrap();
                let scan = FrozenConnectorScan::try_new(
                    recipe,
                    vec![StaticScanAssignment::new(Arc::from("k"), carrier)],
                    TupleDomain::all(),
                    TupleDomain::all(),
                    None,
                    Vec::new(),
                    NonZeroU64::new(read_budget.max_batch_rows).unwrap(),
                    NonZeroU64::new(read_budget.max_batch_bytes).unwrap(),
                )
                .unwrap();
                let facts = ConnectorReadStaticFacts::try_new(
                    read.input_version.clone(),
                    relation.selection_digest(),
                    ConnectorReadProperties::try_new(
                        ConnectorReadDistribution::Unconstrained,
                        Vec::new(),
                    )
                    .unwrap(),
                    ConnectorReadArtifactCoverage::NoArtifactInputs,
                    Vec::new(),
                )
                .unwrap();
                let storage = ValueTypeStorageLifetime::pure();
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "k",
                    field.ty.data_type().clone(),
                    field.ty.nullable(),
                )]));
                let execution = ConnectorReadExecutionSchema::try_new(
                    schema.clone(),
                    vec![field.ty.clone()].into_boxed_slice(),
                    storage.clone(),
                )
                .unwrap();
                let source_shape =
                    ConnectorSourceSchemaShape::try_new(schema, storage.clone()).unwrap();
                let source_schema = ConnectorSourceSchemaFacts::try_new(
                    source_shape,
                    std::collections::HashMap::new(),
                    Box::new([]),
                    storage,
                )
                .unwrap();
                let public = ConnectorReadPublicFacts::try_new_typed_with_control(
                    facts,
                    None,
                    &execution,
                    &source_schema,
                    &Control,
                )
                .unwrap();
                assert!(
                    reads
                        .insert(
                            *occurrence,
                            FrozenConnectorRead::try_new(scan, public).unwrap()
                        )
                        .is_none()
                );
            }
        }
    }
    reads
}
pub fn programs(source: &SqlAuthoredPhysicalPlan) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    let functions = catalogue();
    programs_with_catalogue(source, &functions)
}
pub fn programs_with_catalogue(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    programs_with_original_frozen_reads(source, functions, false, false)
}
pub fn programs_with_catalogue_and_original_scan_residuals(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    programs_with_original_frozen_reads(source, functions, true, false)
}
pub fn programs_with_catalogue_and_original_inlist_source(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    programs_with_original_frozen_reads(source, functions, true, true)
}
fn programs_with_original_frozen_reads(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
    admit_original_scan_residuals: bool,
    admit_required_inlist_shape: bool,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    let semantics = author_fragment_package_semantics(source, policy(), &Control).unwrap();
    let uses = semantics
        .iter()
        .map(|(&id, s)| (id, s.expression_uses.clone()))
        .collect();
    let calls = semantics
        .iter()
        .map(|(&id, s)| (id, s.calls.clone()))
        .collect();
    let pruning = semantics
        .iter()
        .map(|(&id, s)| (id, s.pruning.clone()))
        .collect();
    let admissions = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FragmentPackageAdmission {
                    plan_limits: PlanLimits::FROZEN,
                    source_retained_bytes: 64 << 20,
                    property_projection_limits: PropertyProofProjectionLimits {
                        max_request_bytes: 16 << 20,
                        max_coexisting_bytes: 256 << 20,
                        max_projection_work: 16 << 20,
                    },
                },
            )
        })
        .collect();
    let packages = extract_fragment_packages(
        source.plan(),
        &if admit_required_inlist_shape {
            frozen_reads_with_explicit_source_shape(source, admit_original_scan_residuals, true)
        } else if admit_original_scan_residuals {
            frozen_reads_with_residual_mode(source, true)
        } else {
            frozen_reads(source)
        },
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap();
    let providers = providers();
    packages
        .into_iter()
        .map(|(id, package)| {
            let compiled = compile_fragment(
                validate_fragment_providers(Arc::new(package), &providers, &Control).unwrap(),
                functions,
                LocalCompileOptions {
                    pipeline_dop: NonZeroUsize::new(1).unwrap(),
                    root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
                    kernel_abi: KernelAbiVersion::CURRENT,
                    constants: policy(),
                },
                &Control,
            )
            .unwrap();
            (id, Arc::new(compiled))
        })
        .collect()
}
pub fn producer_root(
    programs: &BTreeMap<FragmentId, Arc<LocalProgram>>,
) -> (Arc<LocalProgram>, ProgramExpressionRootSite) {
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { exprs, .. } = node.kind() {
                let id = node.local_id().unwrap();
                for ordinal in 0..exprs.len() {
                    let root = ProgramExpressionRootSite::Node {
                        node: id,
                        role: ProgramNodeExpressionRole::ProjectOutput {
                            expression: u32::try_from(ordinal).unwrap(),
                        },
                    };
                    let snapshot = program
                        .checked()
                        .channels()
                        .expressions()
                        .resolved_calls()
                        .snapshot();
                    if snapshot.bindings().contains_key(&root) {
                        assert!(
                            found.is_none(),
                            "this closed source must retain one producer project root"
                        );
                        found = Some((program.clone(), root));
                    }
                }
            }
        }
    }
    found.expect("actual SQL source retains its producer projection")
}
pub fn batch(
    program: &LocalProgram,
    root: ProgramExpressionRootSite,
    input: ArrayRef,
) -> RecordBatch {
    let ProgramExpressionRootSite::Node { node, .. } = root else {
        unreachable!()
    };
    let ProgramNodeKind::Project { input: parent, .. } =
        program.graph().nodes()[node.index()].kind()
    else {
        unreachable!()
    };
    RecordBatch::try_new(
        program.graph().nodes()[parent.index()]
            .output_layout()
            .schema()
            .clone(),
        vec![input],
    )
    .unwrap()
}
