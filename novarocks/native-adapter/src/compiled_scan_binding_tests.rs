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

//! The compiled scan binder against the installed fixture read execution,
//! and a compiled scan package for host-level tests.
//!
//! The fixture read execution belongs to provider `fixture` over catalog
//! `test.typed` at generation `[1; 32]`; its codec decodes every column
//! payload as one provider column. Its pure program compiler keeps the
//! frozen private bytes, because the fixture provider has none.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadArtifactCoverage, ConnectorReadBinding,
    ConnectorReadInputVersion, ConnectorReadProgramCompiler, ConnectorReadProperties,
    ConnectorReadPublicFacts, ConnectorReadRelationPayload, ConnectorReadRelationRecipeDraft,
    ConnectorReadStaticFacts, ConnectorValueType, Domain, PureProviderCompileError,
    PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
    StaticScanAssignment, StaticScanDynamicFilter, TupleDomain,
};
use novarocks_execution::exec::node::scan::BoundScanRanges;
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_spi::connector::ConnectorStopOwner;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::SlotId;

use super::*;
use crate::typed_connector_test_support::test_support;

/// The physical scan node every fixture scan is addressed by.
pub(crate) const SCAN_NODE: i32 = 10;

// The fixture provider owns one complete metadata generation. This exact
// opaque coverage statement belongs to that fixture, not a product provider.
const FIXTURE_METADATA_COVERAGE: &[u8] = b"fixture.metadata/complete-generation-1";

struct Unbounded;
impl PureCompileControl for Unbounded {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

/// The fixture provider's pure read compiler: it validates nothing private
/// and keeps the frozen bytes, so the public facts pass unchanged.
struct FixturePort;
impl ConnectorReadProgramCompiler for FixturePort {
    type Error = ConnectorError;
    fn compile_private(
        &self,
        input: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        work.step()?;
        work.finish()?;
        Ok(input.scan().recipe().clone())
    }
}

/// The pure provider catalog that seals fixture reads.
pub(crate) fn fixture_providers() -> PureProviderProgramCatalog<ConnectorError> {
    let provider = ConnectorProviderId::parse("fixture").unwrap();
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            true,
            false,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            Some(Arc::new(FixturePort)
                as Arc<
                    dyn ConnectorReadProgramCompiler<Error = ConnectorError>,
                >),
            None,
        )],
        &Unbounded,
    )
    .unwrap()
}

/// A read binding over the fixture catalog generation, owned by `provider`.
fn binding(provider: &str) -> ConnectorReadBinding {
    let instance = ConnectorInstanceId::try_from_canonical("test.typed").unwrap();
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(provider).unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
    )
}

fn payload(
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
    value: &'static [u8],
) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        Bytes::from_static(value),
    )
}

fn column_payloads(binding: &ConnectorReadBinding) -> Vec<ConnectorEncodedPayload> {
    [b"column-1" as &'static [u8], b"column-2"]
        .into_iter()
        .map(|bytes| payload(binding, ConnectorCodecCategory::ReadColumn, bytes))
        .collect()
}

/// The provider's projected fields, which a compiled scan layout keeps.
fn public_schema() -> Schema {
    Schema::new_with_metadata(
        ["v0", "v1"]
            .into_iter()
            .enumerate()
            .map(|(id, name)| {
                Field::new(name, DataType::Int64, false).with_metadata(HashMap::from([(
                    "provider.field-id".into(),
                    (id + 1).to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
        HashMap::from([("provider.schema".into(), "generation-1".into())]),
    )
}

// This fixture owns a fresh, single-reserve map. Its original backing
// invoice bounds source inspection; it never grants construction capacity.
fn bounded_source_attributes(entries: Vec<(String, String)>) -> (HashMap<String, String>, usize) {
    let facts = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
        String,
        String,
    >(entries.len())
    .expect("the locked fresh source map model");
    let mut source_bytes = std::mem::size_of::<HashMap<String, String>>()
        .checked_add(facts.request_bytes_upper_bound)
        .expect("source map extent");
    let mut attributes = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        source_bytes = source_bytes
            .checked_add(key.capacity())
            .unwrap()
            .checked_add(value.capacity())
            .unwrap();
        assert!(attributes.insert(key, value).is_none());
    }
    (attributes, source_bytes)
}

fn source_fixture_work(
    work: &mut CompileCheckpoints<'_>,
    units: usize,
) -> Result<(), PureProviderCompileError<ConnectorError>> {
    for _ in 0..units {
        work.step()?;
    }
    Ok(())
}

/// One complete frozen read of `v0, v1` owned by `provider`.
fn frozen_read(
    provider: &str,
    unenforced: TupleDomain<ScanColumnId>,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
) -> FrozenConnectorRead {
    frozen_read_kind(
        provider,
        unenforced,
        dynamic_filters,
        ConnectorReadRelationKind::Table,
    )
}

fn frozen_read_kind(
    provider: &str,
    unenforced: TupleDomain<ScanColumnId>,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
    kind: ConnectorReadRelationKind,
) -> FrozenConnectorRead {
    let binding = binding(provider);
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding.clone(),
        ConnectorReadRelationPayload::new(
            kind,
            payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
            payload(&binding, ConnectorCodecCategory::ReadView, b"transaction"),
        ),
        column_payloads(&binding),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        ["v0", "v1"]
            .into_iter()
            .map(|name| StaticScanAssignment::new(Arc::from(name), ConnectorValueType::BigInt))
            .collect(),
        TupleDomain::all(),
        unenforced,
        None,
        dynamic_filters,
        NonZeroU64::new(1024).unwrap(),
        NonZeroU64::new(1 << 20).unwrap(),
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![]).unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        if kind == ConnectorReadRelationKind::SystemTable {
            FIXTURE_METADATA_COVERAGE.to_vec()
        } else {
            vec![]
        },
    )
    .unwrap();
    // Complete execution semantics and original provider attributes have
    // independent authors, even where their physical shapes agree.
    let original = public_schema();
    let shape_schema = Arc::new(Schema::new(
        ["v0", "v1"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
    let execution = novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
        shape_schema.clone(),
        ["v0", "v1"]
            .into_iter()
            .map(|_| novarocks_type_contract::FunctionValueType::new(DataType::Int64, false))
            .collect::<Box<[_]>>(),
        storage.clone(),
    )
    .unwrap();
    let shape = novarocks_connector_contract::ConnectorSourceSchemaShape::try_new(
        shape_schema,
        storage.clone(),
    )
    .unwrap();
    let fields = original
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let (attributes, source_bytes) = bounded_source_attributes(vec![(
                "provider.field-id".into(),
                (ordinal + 1).to_string(),
            )]);
            assert_eq!(
                &attributes,
                &std::collections::HashMap::<String, String>::from(field.metadata())
            );
            let mut work =
                CompileCheckpoints::try_new(&Unbounded, CompilePhase::ProviderValidation).unwrap();
            let facts = novarocks_connector_contract::ConnectorSourceFieldFacts::try_new_observed(
                Box::from([ordinal as u32]),
                None,
                None,
                attributes,
                &[],
                storage.clone(),
                source_bytes,
                |_| {
                    Unbounded
                        .checkpoint(CompilePhase::ProviderValidation, 0)
                        .map_err(PureProviderCompileError::<ConnectorError>::Control)
                },
                |units| source_fixture_work(&mut work, units),
            )
            .unwrap();
            work.finish().unwrap();
            facts
        })
        .collect::<Box<[_]>>();
    let (schema_attributes, schema_source_bytes) =
        bounded_source_attributes(vec![("provider.schema".into(), "generation-1".into())]);
    assert_eq!(
        &schema_attributes,
        &std::collections::HashMap::<String, String>::from(original.metadata())
    );
    let mut work =
        CompileCheckpoints::try_new(&Unbounded, CompilePhase::ProviderValidation).unwrap();
    let source_facts = novarocks_connector_contract::ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
        shape,
        schema_attributes,
        fields,
        storage,
        schema_source_bytes,
        |_| Unbounded.checkpoint(CompilePhase::ProviderValidation, 0)
            .map_err(PureProviderCompileError::<ConnectorError>::Control),
        |units| source_fixture_work(&mut work, units),
    ).unwrap();
    work.finish().unwrap();
    let public = ConnectorReadPublicFacts::try_new_typed_with_control(
        source,
        (kind == ConnectorReadRelationKind::SystemTable).then(|| {
            novarocks_connector_contract::ConnectorReadMetadataKind::try_new("fixture.metadata")
                .unwrap()
        }),
        &execution,
        &source_facts,
        &Unbounded,
    )
    .unwrap();
    assert!(
        public
            .execution_schema()
            .unwrap()
            .schema()
            .metadata()
            .is_empty()
    );
    assert_eq!(
        public.source_schema_facts().unwrap().schema_attributes(),
        &std::collections::HashMap::<String, String>::from(original.metadata())
    );
    FrozenConnectorRead::try_new(scan, public).unwrap()
}

fn recipe(read: &FrozenConnectorRead) -> ConnectorReadProgramRecipe {
    ConnectorReadProgramRecipe::try_compile_with_provider(read, &FixturePort, &Unbounded)
        .expect("the fixture provider seals its read")
}

fn layout() -> StaticLayout {
    let value = novarocks_type_contract::FunctionValueType::new(DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![
        Field::new("v0", DataType::Int64, false),
        Field::new("v1", DataType::Int64, false),
    ]));

    StaticLayout::try_new_typed_for_compile(
        schema,
        Arc::from([SlotId::new(1), SlotId::new(2)]),
        vec![
            (value.typed_binding().clone(), None),
            (value.typed_binding().clone(), None),
        ],
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut |_| {
            Unbounded
                .checkpoint(CompilePhase::LowerProgram, 0)
                .map_err(novarocks_local_program::LayoutCompileError::Control)
        },
        &Unbounded,
    )
    .unwrap()
}

fn bind(
    runtime: &TypedScanRuntime,
    recipe: &ConnectorReadProgramRecipe,
    layout: &StaticLayout,
) -> Result<Arc<dyn ScanSource>, CompiledScanBindingError> {
    bind_filtered(
        runtime,
        recipe,
        &[],
        &CompiledRuntimeFilterEndpoints::default(),
        layout,
    )
}

/// Binds a scan whose consumer sites are `consumers`, under the package
/// binding table `endpoints`.
fn bind_filtered(
    runtime: &TypedScanRuntime,
    recipe: &ConnectorReadProgramRecipe,
    consumers: &[FilterConsumerAtExpr],
    endpoints: &CompiledRuntimeFilterEndpoints,
    layout: &StaticLayout,
) -> Result<Arc<dyn ScanSource>, CompiledScanBindingError> {
    let options = QueryOptions::default();
    bind_compiled_scan(
        SCAN_NODE,
        recipe,
        consumers,
        layout,
        &CompiledScanTask {
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
            runtime,
            fragment_instance_id: UniqueId::new(3, 4),
            query_options: &options,
            stop: ConnectorStopOwner::new().view(),
            runtime_filters: endpoints,
        },
    )
}

/// A blocking membership consumer site of `binding_id` over `v0`.
fn consumer(binding_id: u32) -> FilterConsumerAtExpr {
    use novarocks_local_program::{
        FilterConsumerActivation, FilterNullSemantics, FilterReduction, ProgramExprId,
        StaticFilterConsumer, StaticFilterContract,
    };
    FilterConsumerAtExpr {
        expr_id: ProgramExprId::new(0),
        consumer: StaticFilterConsumer::try_new(
            binding_id,
            7,
            FilterConsumerActivation::BlockingSnapshot,
            StaticFilterContract::membership(
                &novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                FilterNullSemantics::NeverMatches,
            )
            .expect("membership contract"),
            FilterReduction::SetUnion,
        )
        .expect("blocking membership consumer"),
    }
}

/// Binding 3 consumes runtime filter 7 at the fixture scan's source.
fn scan_source_binding() -> CompiledRuntimeFilterEndpoints {
    use crate::compiled_runtime_filter::{
        CompiledRuntimeFilterEndpoint, CompiledRuntimeFilterRole,
    };
    CompiledRuntimeFilterEndpoints::try_from_endpoints([(
        3,
        CompiledRuntimeFilterEndpoint::new(
            7,
            u32::try_from(SCAN_NODE).unwrap(),
            CompiledRuntimeFilterRole::Consumer { scan_source: true },
        ),
    )])
    .unwrap()
}

fn refusal(
    runtime: &TypedScanRuntime,
    recipe: &ConnectorReadProgramRecipe,
    layout: &StaticLayout,
) -> CompiledScanBindingError {
    match bind(runtime, recipe, layout) {
        Ok(_) => panic!("the compiled read must be refused"),
        Err(error) => error,
    }
}

/// Nothing was registered for the scan node when this registration wins.
fn assert_unregistered(runtime: &TypedScanRuntime) {
    runtime
        .register_read_execution(SCAN_NODE, test_support::installed_read_execution())
        .expect("a refused bind registers no read execution");
}

#[test]
fn a_compiled_read_binds_the_installed_execution_of_its_binding() {
    let runtime = test_support::typed_scan_runtime();
    let source = bind(
        &runtime,
        &recipe(&frozen_read("fixture", TupleDomain::all(), vec![])),
        &layout(),
    )
    .expect("the fixture read binds");
    assert_eq!(source.profile_name().as_deref(), Some("TypedConnectorScan"));
    // Split delivery decodes against the execution this scan node registered.
    let duplicate = runtime
        .register_read_execution(SCAN_NODE, test_support::installed_read_execution())
        .expect_err("the scan node is registered once");
    assert_eq!(
        duplicate,
        novarocks_worker::read_attempt::ReadExecutionRegistrationError::Duplicate {
            plan_node_id: SCAN_NODE
        }
    );
    // The provider opens only once admission installed the fragment tracker.
    let early = source
        .bind(BoundScanRanges::None)
        .err()
        .expect("no provider before admission");
    assert_eq!(
        novarocks_execution_contract::failure::FailureProjection::failure_fact(&early),
        novarocks_execution_contract::failure::FailureProjection::failure_fact(
            &novarocks_worker::ConnectorResourceBindingError::BeforeAdmission
        )
    );
    runtime
        .install_connector_resource_tracker(MemTracker::new_root("admitted-fragment"))
        .expect("admission installs the fragment tracker");
    source
        .bind(BoundScanRanges::None)
        .expect("a split-driven scan binds its empty range binding");
    assert!(
        source
            .bind(BoundScanRanges::SchemaSelection { should_scan: true })
            .is_err(),
        "a frozen range is not a split-driven scan's assignment"
    );
}

#[test]
fn system_table_and_table_execute_reads_register_the_split_execution() {
    for kind in [
        ConnectorReadRelationKind::SystemTable,
        ConnectorReadRelationKind::TableExecute,
    ] {
        let runtime = test_support::typed_scan_runtime();
        let read = frozen_read_kind("fixture", TupleDomain::all(), vec![], kind);
        let source = bind(&runtime, &recipe(&read), &layout()).expect("supported relation binds");
        assert_eq!(source.profile_name().as_deref(), Some("TypedConnectorScan"));
        assert!(
            runtime
                .register_read_execution(SCAN_NODE, test_support::installed_read_execution())
                .is_err()
        );
    }
}

#[test]
fn unsupported_relation_kinds_keep_their_binding_refusal() {
    use novarocks_execution_contract::failure::FailureProjection;
    for kind in [
        ConnectorReadRelationKind::TableFunction,
        ConnectorReadRelationKind::MergeTable,
    ] {
        let runtime = test_support::typed_scan_runtime();
        let read = frozen_read_kind("fixture", TupleDomain::all(), vec![], kind);
        let error = refusal(&runtime, &recipe(&read), &layout());
        assert_eq!(
            error.failure_fact(),
            ScanBindingContractFailure::UnsupportedRelation(kind).failure_fact()
        );
        assert_unregistered(&runtime);
    }
}

#[test]
fn a_compiled_read_of_another_binding_is_refused_before_registration() {
    // Same catalog generation, another provider: the installed execution of
    // that catalog is not the one this read was frozen and sealed for.
    let runtime = test_support::typed_scan_runtime();
    let error = refusal(
        &runtime,
        &recipe(&frozen_read("other", TupleDomain::all(), vec![])),
        &layout(),
    );
    assert_eq!(error.scan_node(), Some(SCAN_NODE));
    assert!(
        error
            .detail()
            .contains("does not match the installed read execution"),
        "{error}"
    );
    assert_unregistered(&runtime);
}

// The fixture codec decodes both column payloads as one provider column, so
// ordinals 0 and 1 assign the same column. Its predicate is stated once, at
// ordinal 0; a later ordinal is not that statement and is never merged.
#[test]
fn facts_about_a_column_assigned_twice_are_bound_at_its_first_ordinal() {
    let at = |ordinal| {
        TupleDomain::with_column_domains(BTreeMap::from([(
            ScanColumnId::new(ordinal),
            Domain::not_null(ConnectorValueType::BigInt),
        )]))
        .unwrap()
    };
    let runtime = test_support::typed_scan_runtime();
    bind(
        &runtime,
        &recipe(&frozen_read("fixture", at(0), vec![])),
        &layout(),
    )
    .expect("the first ordinal states the column's predicate");

    let runtime = test_support::typed_scan_runtime();
    let error = refusal(
        &runtime,
        &recipe(&frozen_read("fixture", at(1), vec![])),
        &layout(),
    );
    assert!(
        error
            .detail()
            .contains("stated at ordinal 1 of a provider column first assigned at ordinal 0"),
        "{error}"
    );
    assert_unregistered(&runtime);
}

// The connector prunes nothing in this milestone, so a filtered read binds
// the same typed source as an unfiltered one once its frozen dynamic filter
// names exactly the scan's consumer binding of that filter.
#[test]
fn a_compiled_read_binds_dynamic_filters_that_match_its_consumer_bindings() {
    let runtime = test_support::typed_scan_runtime();
    let source = bind_filtered(
        &runtime,
        &recipe(&frozen_read(
            "fixture",
            TupleDomain::all(),
            vec![StaticScanDynamicFilter::new(7, Arc::from("v0"))],
        )),
        &[consumer(3)],
        &scan_source_binding(),
        &layout(),
    )
    .expect("dynamic filter 7 is binding 3's");
    assert_eq!(source.profile_name().as_deref(), Some("TypedConnectorScan"));
    runtime
        .register_read_execution(SCAN_NODE, test_support::installed_read_execution())
        .expect_err("the filtered scan registered its read execution");
}

#[test]
fn dynamic_filters_that_do_not_match_the_consumer_bindings_are_refused_before_registration() {
    use crate::compiled_runtime_filter::CompiledRuntimeFilterBindingError as Refusal;
    let filtered = |filter_id| {
        recipe(&frozen_read(
            "fixture",
            TupleDomain::all(),
            vec![StaticScanDynamicFilter::new(filter_id, Arc::from("v0"))],
        ))
    };
    let unfiltered = recipe(&frozen_read("fixture", TupleDomain::all(), vec![]));
    for (recipe, consumers, expected) in [
        // A dynamic filter no consumer site of the scan binds.
        (
            filtered(7),
            vec![],
            Refusal::MissingDynamicConsumer { filter: 7 },
        ),
        // A dynamic filter of another filter than the site's binding.
        (
            filtered(8),
            vec![consumer(3)],
            Refusal::MissingDynamicConsumer { filter: 8 },
        ),
        // A scan-source consumer whose dynamic filter the read does not state.
        (
            unfiltered,
            vec![consumer(3)],
            Refusal::MissingFrozenDynamicFilter { filter: 7 },
        ),
        // A site binding the package does not number.
        (
            filtered(7),
            vec![consumer(4)],
            Refusal::UnnumberedScanBinding { binding: 4 },
        ),
    ] {
        let runtime = test_support::typed_scan_runtime();
        let error = match bind_filtered(
            &runtime,
            &recipe,
            &consumers,
            &scan_source_binding(),
            &layout(),
        ) {
            Ok(_) => panic!("the mismatched read must be refused"),
            Err(error) => error,
        };
        assert_eq!(error.scan_node(), Some(SCAN_NODE));
        assert_eq!(
            crate::backend_test_support::find_source::<Refusal>(&error),
            Some(&expected)
        );
        use novarocks_execution_contract::failure::FailureProjection;
        assert_eq!(error.failure_fact(), expected.failure_fact());
        assert_unregistered(&runtime);
    }
}

#[test]
fn reads_a_compiled_scan_cannot_run_are_refused_before_registration() {
    let runtime = test_support::typed_scan_runtime();
    let narrow = StaticLayout::try_new(
        Arc::new(Schema::new(vec![public_schema().field(0).clone()])),
        Arc::from([SlotId::new(1)]),
    )
    .unwrap();
    let narrow = refusal(
        &runtime,
        &recipe(&frozen_read("fixture", TupleDomain::all(), vec![])),
        &narrow,
    );
    assert!(
        narrow.detail().contains("1 slots for 2 assignments"),
        "{narrow}"
    );
    assert_unregistered(&runtime);
}

/// The v2 bytes of a producer package `SELECT v0, v1 FROM t WHERE v0 > 7`
/// over the fixture provider, streaming to a Gather receiver. Returns the
/// bytes and the receiver node.
pub(crate) fn scan_producer_package() -> (Vec<u8>, u32) {
    scan_producer_package_kind(ConnectorReadRelationKind::Table)
}

pub(crate) fn system_scan_producer_package() -> (Vec<u8>, u32) {
    scan_producer_package_kind(ConnectorReadRelationKind::SystemTable)
}

fn scan_producer_package_kind(kind: ConnectorReadRelationKind) -> (Vec<u8>, u32) {
    use novarocks_physical_plan::*;
    use novarocks_plan_codec::physical_package_v2::encode_fragment_package;
    use novarocks_plan_codec::physical_package_v2::test_support::encode_limits;
    use novarocks_type_contract::{
        ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
        ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
        ExpressionInvocation, ExpressionUseId, control_argument_semantics,
    };
    use prost::Message;

    const PRODUCER: FragmentId = FragmentId::new(1);
    const CONSUMER: FragmentId = FragmentId::new(2);
    const EDGE: EdgeId = EdgeId::new(5);
    const SCAN: NodeId = NodeId::new(10);
    const RECEIVER: NodeId = NodeId::new(20);
    let int64 = || ValueType::new(DataType::Int64, false);

    let binding = binding("fixture");
    let relation_payload = ConnectorReadRelationPayload::new(
        kind,
        payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding, ConnectorCodecCategory::ReadView, b"transaction"),
    );
    let columns = column_payloads(&binding)
        .into_iter()
        .map(|column_payload| ProviderColumnReference { column_payload })
        .collect::<Vec<_>>();
    let mut builder = FragmentBuilder::new(PRODUCER);
    let provider = columns
        .iter()
        .map(|column| {
            builder
                .add_value(
                    int64(),
                    ValueOrigin::ProviderField {
                        scan_node: SCAN,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let column = builder
        .add_expression(SCAN, int64(), ExprKind::Value(provider[0]))
        .unwrap();
    // The v2 encoder publishes literals only as checked constants: `7` is
    // the one row of its own pool.
    let pool = ConstantPoolId::new(7007);
    let literal = builder
        .add_expression(
            SCAN,
            int64(),
            ExprKind::Constant(ConstantReference { pool, ordinal: 0 }),
        )
        .unwrap();
    let residual = builder
        .add_expression(
            SCAN,
            ValueType::new(DataType::Boolean, false),
            ExprKind::Binary {
                left: column,
                op: BinaryOperator::Gt,
                right: literal,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    builder
        .add_scan(
            SCAN,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(if kind == ConnectorReadRelationKind::SystemTable {
                    Relation::Metadata(MetadataRelation {
                        read: ProviderReadReference {
                            binding: binding.clone(),
                            input_version: ExactInputVersion::try_new([9]).unwrap(),
                            relation: relation_payload,
                        },

                        selection_digest: [7; 32],
                        schema: columns
                            .iter()
                            .map(|column| RelationField {
                                column: column.clone(),
                                ty: int64(),
                            })
                            .collect(),
                        predicate_guarantees: Box::from([PredicateGuarantee {
                            predicate: residual,
                            kind: PredicateGuaranteeKind::PruningOnly,
                        }]),
                        provided_properties: PhysicalProperties {
                            distribution: Distribution::Unconstrained,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ordering: Box::default(),
                        },
                        kind: MetadataRelationKind::try_new("fixture.metadata").unwrap(),
                        coverage_evidence: FIXTURE_METADATA_COVERAGE.into(),
                    })
                } else {
                    Relation::Data(DataRelation {
                        read: ProviderReadReference {
                            binding: binding.clone(),
                            input_version: ExactInputVersion::try_new([9]).unwrap(),
                            relation: relation_payload,
                        },

                        selection_digest: [7; 32],
                        schema: columns
                            .iter()
                            .map(|column| RelationField {
                                column: column.clone(),
                                ty: int64(),
                            })
                            .collect(),
                        predicate_guarantees: Box::from([PredicateGuarantee {
                            predicate: residual,
                            kind: PredicateGuaranteeKind::PruningOnly,
                        }]),
                        provided_properties: PhysicalProperties {
                            distribution: Distribution::Unconstrained,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ordering: Box::default(),
                        },
                    })
                }),
                read_budget: ScanReadBudget {
                    max_batch_rows: 1024,
                    max_batch_bytes: 1 << 20,
                },
                provider_outputs: columns
                    .iter()
                    .cloned()
                    .zip(provider.iter().copied())
                    .collect(),
                residuals: Box::from([residual]),
                derived_values: Box::default(),
            },
            provider.clone().into_boxed_slice(),
        )
        .unwrap();
    let dop = PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    };
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    // Explicit fixture admission; these values are not production defaults.
    let constants = novarocks_functions::ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    plan.insert_constant_pool(
        pool,
        ConstantPool::try_new(
            Arc::new(int64().try_to_field("literal").unwrap()),
            int64(),
            arrow::array::Array::to_data(&arrow::array::Int64Array::from(vec![7_i64])),
            constants,
            CompilePhase::Validate,
            &Unbounded,
        )
        .unwrap(),
    )
    .unwrap();
    plan.add_fragment(
        builder
            .finish_definition(
                SCAN,
                FragmentSink::Stream { edge: EDGE },
                dop,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    )
    .unwrap();
    let mut consumer = FragmentBuilder::new(CONSUMER);
    let imports = provider
        .iter()
        .map(|&source| {
            let imported = consumer
                .add_value(
                    int64(),
                    ValueOrigin::ExchangeImport {
                        edge: EDGE,
                        source_value: source,
                    },
                )
                .unwrap();
            (source, imported)
        })
        .collect::<Vec<_>>();
    let received = imports
        .iter()
        .map(|(_, imported)| *imported)
        .collect::<Box<[_]>>();
    consumer
        .add_exchange_source(
            RECEIVER,
            EDGE,
            imports.clone().into_boxed_slice(),
            received.clone(),
            Distribution::Singleton,
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    plan.add_fragment(
        consumer
            .finish_definition(
                RECEIVER,
                FragmentSink::Result,
                dop,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    )
    .unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: EDGE,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: PRODUCER,
            projection: provider.clone().into_boxed_slice(),
        },
        destination: EdgeDestination {
            fragment: CONSUMER,
            node: RECEIVER,
            receive_mapping: imports.into_boxed_slice(),
        },
        // Gather: the edge places every row on one destination.
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: CONSUMER,
        output: OutputPort {
            node: RECEIVER,
            columns: received.clone(),
        },
        fields: received
            .iter()
            .zip(["v0", "v1"])
            .map(|(value, name)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: name.into(),
                alias: None,
                value: *value,
                ty: int64(),
            })
            .collect(),
    })
    .unwrap();
    let plan = plan.finish_observed(&Unbounded).unwrap();

    // One complete eager use tree per physical root site in one root domain.
    fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
        struct Author<'a> {
            fragment: &'a Fragment,
            next: u32,
            uses: Vec<ExpressionInvocation<ExprId>>,
        }
        impl Author<'_> {
            fn visit(&mut self, expr: ExprId, demand: EvaluationDemand) -> ExpressionUseId {
                let id = ExpressionUseId::new(self.next);
                self.next += 1;
                let args = match &self.fragment.expressions().get(expr).unwrap().kind {
                    ExprKind::Binary { left, right, .. } => vec![*left, *right],
                    ExprKind::Constant(_) | ExprKind::Value(_) => vec![],
                    _ => panic!("the scan fixture has only comparisons, values and constants"),
                };
                let arguments = args
                    .iter()
                    .enumerate()
                    .map(|(ordinal, &child)| {
                        let (child_demand, guard) = control_argument_semantics(
                            ControlShape::Eager,
                            args.len(),
                            ordinal,
                            demand,
                        )
                        .unwrap();
                        assert!(guard.is_none());
                        self.visit(child, child_demand)
                    })
                    .collect::<Box<[_]>>();
                self.uses.push(ExpressionInvocation {
                    context: ExpressionEffectContext {
                        use_id: id,
                        domain: EvaluationDomainId::new(0),
                        demand,
                    },
                    definition: expr,
                    control: ControlShape::Eager,
                    arguments,
                });
                id
            }
        }
        let roots = PhysicalExpressionRoots::try_new(fragment, &Unbounded).unwrap();
        let mut author = Author {
            fragment,
            next: 0,
            uses: vec![],
        };
        let bindings = roots
            .sites()
            .iter()
            .map(|(&site, root)| (site, author.visit(root.expr, root.demand)))
            .collect();
        let flow = ExpressionControlFlow::<ExprId>::try_new(
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }],
            author.uses,
            fragment.expressions(),
            CompilePhase::Validate,
            &Unbounded,
        )
        .unwrap();
        PhysicalRootUses::try_new(fragment, flow, bindings, &Unbounded).unwrap()
    }

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &Unbounded).unwrap(),
        );
        uses.insert(id, root_uses);
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &Unbounded).unwrap(),
        );
        // Explicit small-fixture admission; not a production default.
        admissions.insert(
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
        );
    }
    let scans = BTreeMap::from([(
        ProviderReadOccurrenceId::new(0),
        frozen_read_kind("fixture", TupleDomain::all(), vec![], kind),
    )]);
    let mut packages = extract_fragment_packages(
        &plan,
        &scans,
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &Unbounded,
    )
    .unwrap();
    let package = packages.remove(&PRODUCER).unwrap();
    let bytes = encode_fragment_package(&package, &encode_limits(), &Unbounded)
        .expect("v2 bytes")
        .encode_to_vec();
    (bytes, RECEIVER.get())
}

// Reuses the actual admitted-binding fixture and public Worker owner, without
// another fake registry. Refusal must preserve the first exact capability.
#[test]
fn duplicate_read_registration_keeps_the_original_capability() {
    use novarocks_worker::read_attempt::{ReadExecutionRegistrationError, TypedReadAttemptContext};
    let context = TypedReadAttemptContext::new();
    let first = test_support::installed_read_execution();
    let original = first.provider_factory();
    let second = test_support::installed_read_execution();
    assert!(!Arc::ptr_eq(&original, &second.provider_factory()));
    context.register(SCAN_NODE, first).expect("first binding");
    assert!(
        context.resolve(SCAN_NODE).is_none(),
        "unpublished bindings remain private"
    );
    assert_eq!(
        context.register(SCAN_NODE, second),
        Err(ReadExecutionRegistrationError::Duplicate {
            plan_node_id: SCAN_NODE
        })
    );
    context.publish();
    let resolved = context.resolve(SCAN_NODE).expect("published original");
    assert!(Arc::ptr_eq(&original, &resolved.provider_factory()));
}
