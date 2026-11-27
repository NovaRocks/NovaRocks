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
//! SQL-authored package extraction and LocalCompiler inputs.
//! These fixtures stop at LocalProgram; runtime assertions live in tests/it.
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
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::*;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};
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
    admit_extended_source_shape: bool,
) -> BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead> {
    let mut reads = BTreeMap::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                occurrence,
                relation,
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
                if !admit_extended_source_shape {
                    assert!(!field.ty.nullable());
                }
                let carrier = match field.ty.data_type() {
                    DataType::Int8 => ConnectorValueType::TinyInt,
                    DataType::Int16 => ConnectorValueType::SmallInt,
                    DataType::Int32 => ConnectorValueType::Integer,
                    DataType::Int64 => ConnectorValueType::BigInt,
                    DataType::Float32 => ConnectorValueType::Real,
                    DataType::Float64 => ConnectorValueType::Double,
                    DataType::Utf8 if admit_extended_source_shape => connector_type_for_value_type(
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
pub fn programs_with_catalogue_and_nullable_source(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    programs_with_original_frozen_reads(source, functions, false, true)
}

fn fixture_package_admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 << 20,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 << 20,
            max_coexisting_bytes: 256 << 20,
            max_projection_work: 16 << 20,
        },
    }
}

fn fixture_local_options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        // The host supplies these fixture admission limits.
        constants: policy().into(),
    }
}

fn fragment_packages_with_original_frozen_reads(
    source: &SqlAuthoredPhysicalPlan,
    admit_original_scan_residuals: bool,
    admit_extended_source_shape: bool,
) -> BTreeMap<FragmentId, FragmentPackage> {
    let pruning = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| {
            (
                id,
                FrozenFragmentPruning::try_new(id, Vec::new(), &Control).unwrap(),
            )
        })
        .collect();
    let admissions = source
        .plan()
        .fragments()
        .keys()
        .map(|&id| (id, fixture_package_admission()))
        .collect();
    extract_fragment_packages(
        source.plan(),
        &if admit_extended_source_shape {
            frozen_reads_with_explicit_source_shape(source, admit_original_scan_residuals, true)
        } else if admit_original_scan_residuals {
            frozen_reads_with_residual_mode(source, true)
        } else {
            frozen_reads(source)
        },
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &Control,
    )
    .unwrap()
}

fn programs_with_original_frozen_reads(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
    admit_original_scan_residuals: bool,
    admit_extended_source_shape: bool,
) -> BTreeMap<FragmentId, Arc<LocalProgram>> {
    let providers = providers();
    fragment_packages_with_original_frozen_reads(
        source,
        admit_original_scan_residuals,
        admit_extended_source_shape,
    )
    .into_iter()
    .map(|(id, package)| {
        let compiled = compile_fragment(
            validate_fragment_providers(Arc::new(package), &providers, &Control).unwrap(),
            functions,
            fixture_local_options(),
            &Control,
        )
        .unwrap();
        (id, Arc::new(compiled))
    })
    .collect()
}
pub fn producer_root(
    programs: &BTreeMap<FragmentId, Arc<LocalProgram>>,
) -> (Arc<LocalProgram>, ProgramRootRef) {
    let mut found = None;
    for program in programs.values() {
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { exprs, .. } = node.kind() {
                for &root in exprs {
                    let unit = program.unit(root.unit).expect("actual Project unit exists");
                    let compiled = unit.root(root.root).expect("actual Project root exists");
                    let novarocks_type_contract::FunctionArgumentType::Value(_) =
                        &unit.types()[compiled.definition.index()]
                    else {
                        panic!("actual producer Project root has a complete scalar type")
                    };
                    assert!(
                        found.is_none(),
                        "this closed source must retain one producer project root"
                    );
                    found = Some((program.clone(), root));
                }
            }
        }
    }
    found.expect("actual SQL source retains its producer projection")
}
pub fn batch(program: &LocalProgram, root: ProgramRootRef, input: ArrayRef) -> RecordBatch {
    let mut parents = program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Project { input, exprs, .. } if exprs.contains(&root) => Some(*input),
            _ => None,
        });
    let parent = parents
        .next()
        .expect("actual root belongs to its producer Project");
    assert!(
        parents.next().is_none(),
        "this closed root has one producer Project"
    );
    RecordBatch::try_new(
        program.graph().nodes()[parent.index()]
            .output_layout()
            .schema()
            .clone(),
        vec![input],
    )
    .unwrap()
}

/// Enumerate actual retained roots without a fragment-global expression bank.
pub fn compiled_roots(
    program: &novarocks_local_program::LocalProgram,
) -> impl Iterator<
    Item = (
        novarocks_local_program::ProgramRootRef,
        &std::sync::Arc<novarocks_expression::CompiledExprSet>,
        &novarocks_expression::CompiledRoot,
    ),
> {
    program
        .units()
        .iter()
        .enumerate()
        .flat_map(|(index, unit)| {
            unit.roots().iter().enumerate().map(move |(ordinal, root)| {
                (
                    novarocks_local_program::ProgramRootRef::new(
                        novarocks_local_program::ProgramUnitId::new(index),
                        novarocks_expression_contract::RootOrdinal::new(
                            u32::try_from(ordinal).unwrap(),
                        ),
                    ),
                    unit,
                    root,
                )
            })
        })
}

/// A positive IN call can stand alone or be the exact operand of the selected NOT.
pub fn membership_use(
    unit: &novarocks_expression::CompiledExprSet,
    root: novarocks_type_contract::ExpressionUseId,
) -> Option<novarocks_type_contract::ExpressionUseId> {
    use novarocks_functions::PreparedPureKernel;
    use novarocks_type_contract::OperatorFunction;
    let call = unit.calls().get(&root)?;
    if matches!(call.prepared(), PreparedPureKernel::Membership(_)) {
        return Some(root);
    }
    if call.call_contract().function_id().as_str()
        != OperatorFunction::BooleanNot.function_id_text()
    {
        return None;
    }
    let [operand] = unit.control_flow().uses()[&root].arguments.as_ref() else {
        return None;
    };
    matches!(
        unit.calls().get(operand)?.prepared(),
        PreparedPureKernel::Membership(_)
    )
    .then_some(*operand)
}

/// Find the original signed membership root in the checked compiler output.
pub fn compiled_signed_inlist_root(
    source: &SqlAuthoredPhysicalPlan,
    functions: &PureEngineFunctionCatalog,
) -> (Arc<LocalProgram>, ProgramRootRef) {
    let programs = programs_with_catalogue_and_original_inlist_source(source, functions);
    let mut found = None;
    for program in programs.values() {
        for (site, unit, root) in compiled_roots(program) {
            if membership_use(unit, root.use_id).is_some() {
                assert!(found.is_none(), "one actual IN root");
                found = Some((program.clone(), site));
            }
        }
    }
    found.expect("actual SQL IN root survived original lowering")
}

/// Resolve a real Input use through its root's actual Project input layout.
pub fn project_input_slot_for_use(
    program: &novarocks_local_program::LocalProgram,
    site: novarocks_local_program::ProgramRootRef,
    use_id: novarocks_type_contract::ExpressionUseId,
) -> novarocks_types::SlotId {
    let unit = program.unit(site.unit).unwrap();
    let root = unit.root(site.root).unwrap();
    let binding = unit.input(use_id).expect("actual compiled Input use");
    assert_eq!(binding.scope, root.scope);
    let input = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            novarocks_local_program::ProgramNodeKind::Project { input, exprs, .. }
                if exprs.contains(&site) =>
            {
                Some(*input)
            }
            _ => None,
        })
        .expect("actual root belongs to its producer Project");
    program.graph().nodes()[input.index()]
        .output_layout()
        .slots()[binding.ordinal as usize]
}

#[test]
fn approx_distinct_final_local_compiler_refuses_a_nonconstant_frozen_accuracy() {
    use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source};
    use std::error::Error;

    let source = sql_source(
        "SELECT approx_distinct(k, 2.3E-2) FROM fixture.source",
        DataType::Int64,
    );
    let (package, site) = fragment_packages_with_original_frozen_reads(&source, false, false)
        .into_values()
        .find_map(|package| {
            let site = package.fragment().nodes().values().find_map(|node| {
                let NodeKind::Aggregate { calls, .. } = &node.kind else {
                    return None;
                };
                let [call] = calls.as_ref() else {
                    return None;
                };
                matches!(call.binding.phase, AggregatePhase::Final { .. }).then_some(
                    PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: 0,
                    },
                )
            })?;
            Some((package, site))
        })
        .expect("fixed DOUBLE selection yields a Final package");
    let providers = providers();
    let functions = installed_builtin_owner_catalogue();
    let compile = |package| {
        compile_fragment(
            validate_fragment_providers(Arc::new(package), &providers, &Control).unwrap(),
            &functions,
            fixture_local_options(),
            &Control,
        )
    };
    compile(package.clone()).expect("the genuine Final package prepares");

    let mut input = package.into_input();
    let mut requests = input
        .fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(&site, request)| (site, request.clone()))
        .collect::<Vec<_>>();
    let request = &mut requests
        .iter_mut()
        .find(|(actual, _)| *actual == site)
        .expect("exact Final original request")
        .1;
    let [
        _,
        StaticFunctionArgument::Value {
            value_type,
            constant,
        },
    ] = request.arguments.as_mut()
    else {
        panic!("the Final request retains both original logical arguments");
    };
    assert_eq!(value_type.data_type(), &DataType::Float64);
    assert!(
        constant.take().is_some(),
        "positive fixture had a proven constant"
    );
    assert_eq!(
        input.constants.entries().len(),
        1,
        "accuracy is the Final package's only constant"
    );
    input.constants = ConstantPools::empty();
    input.fragment = input
        .fragment
        .with_call_requests_observed(requests, &Control)
        .unwrap();
    let package = FragmentPackage::try_new(input, fixture_package_admission(), &Control)
        .expect("the neutral package retains the genuine Final state input and signature");
    let error = compile(package).expect_err("LocalCompiler cannot prepare this Final");
    let cause: &(dyn Error + 'static) = &error;
    let owner_failure = std::iter::successors(Some(cause), |&cause| cause.source())
        .find_map(|cause| cause.downcast_ref::<FunctionSpecializationFailure>());
    let Some(FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(detail))) =
        owner_failure
    else {
        panic!("expected the original phase-qualification failure, got {error:?}");
    };
    assert_eq!(
        detail.message(),
        "approx_distinct stages require fixed legal accuracy"
    );
}
