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

//! A real Frontend plan over a real Paimon filesystem table, frozen through
//! the production provider-read freeze, packaged by the compiled carrier, and
//! compiled by the same provider and local compilers a backend runs.
//!
//! The Paimon provider is the real one: its control role binding opens the
//! table from a local warehouse, negotiates and freezes the read, and
//! publishes the read's public schema; its pure recipe compiler checks the
//! authored read. Only the catalog, statistics and materialized-view answers
//! are assembled here, from the same production owners a statement uses,
//! because the Frontend host that composes them needs a StateStore.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_connector_contract::{
    PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
};
use novarocks_connector_paimon::io::{PaimonFsAuthorizedListing, PaimonHostFileIo};
use novarocks_connector_paimon::role_binding::{
    PaimonControlRoleBindingFactory, PaimonReadRecipeCompiler, PaimonRoleFileIoFactory,
};
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, NodeKind, PipelineDopDomain, PlanVersionId,
    PredicateGuaranteeKind, ScanReadBudget,
};
use novarocks_query_application::preparation::{
    CatalogFactPort, CompletedPlanWithAccess, FinalPlanCompletionDriver, MaterializedViewFactPort,
    QueryCompletionFactSource, StatisticsFactPort,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion, ConnectorCodecError,
    ConnectorControlRoleBindingFactory, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner, MaterializationContext,
    StorageAccessDomainId,
};
use novarocks_sql::compiler::{
    CatalogRelationFact, CatalogRelationNeed, DEFAULT_COMPLETION_LIMITS, MaterializedViewFact,
    MaterializedViewNeed, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    StatisticsFact, StatisticsNeed, builtin_sql_function_catalog,
};
use novarocks_workload_control::{
    ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};

use super::author_frozen_reads;
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_catalog::{QueryCatalogService, new_query_catalog_service};
use crate::catalog_application::query_materializer::{
    CatalogServiceMaterializer, iceberg_table_binding_loader,
};
use crate::query_execution::package_freeze::{
    CompiledPackageCarrier, PackageFreezeError, StaticPlanCarrier,
};
use crate::query_execution::physical_encoding::{EncodedCompletedPlan, encode_completed_plan};
use crate::query_execution::provider_read_facts::{
    FrontendProviderReadFacts, FrozenProviderRead, FrozenReadEncoding,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;

const CATALOG: &str = "paimon_cat";
const SCAN_SQL: &str = "SELECT c FROM t WHERE c > 1";

/// Paimon's own on-disk schema of an append-only table with one nullable INT
/// column, exactly as a Paimon writer leaves `schema-0`. The table has no
/// snapshot yet, which the provider freezes as a read of no data files.
const TABLE_SCHEMA: &str = r#"{
  "version" : 3,
  "id" : 0,
  "fields" : [ { "id" : 0, "name" : "c", "type" : "INT" } ],
  "highestFieldId" : 0,
  "partitionKeys" : [ ],
  "primaryKeys" : [ ],
  "options" : { },
  "timeMillis" : 0
}"#;

/// File access to a warehouse on the local filesystem; the only part of the
/// Paimon provider a Server composition supplies.
struct LocalPaimonFiles;

impl PaimonRoleFileIoFactory for LocalPaimonFiles {
    fn bind_file_io(
        &self,
        _properties: &CatalogProperties,
        warehouse: &str,
        _request: &ConnectorRequestContext,
    ) -> Result<PaimonHostFileIo, ConnectorError> {
        let unavailable = |error: novarocks_fs::FileError| {
            ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
        };
        let access = novarocks_fs::FsAccessResolver::new()
            .resolve_location(StorageAccessDomainId::from_bytes([3; 32]), warehouse, None)
            .map_err(unavailable)?;
        PaimonHostFileIo::try_new(
            access,
            warehouse,
            novarocks_fs::FileCancellation::new(),
            Arc::new(PaimonFsAuthorizedListing),
        )
        .map_err(unavailable)
    }
}

/// One Paimon catalog generation published in a Frontend control host, over
/// a temporary warehouse holding table `db.t`.
struct PaimonFixture {
    _warehouse: tempfile::TempDir,
    runtime: tokio::runtime::Runtime,
    host: Arc<ConnectorControlHost>,
}

fn paimon_fixture() -> PaimonFixture {
    let directory = tempfile::tempdir().expect("temporary directory");
    let warehouse = directory.path().join("warehouse");
    let schema = warehouse.join("db.db").join("t").join("schema");
    std::fs::create_dir_all(&schema).expect("table schema directory");
    std::fs::write(schema.join("schema-0"), TABLE_SCHEMA).expect("table schema");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let factory = Arc::new(PaimonControlRoleBindingFactory::new(
        Arc::new(LocalPaimonFiles),
        runtime.handle().clone(),
    ));
    let host = Arc::new(
        ConnectorControlHost::with_role_factories(vec![factory.clone()])
            .expect("Paimon control host"),
    );
    let properties = CatalogProperties::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse(CATALOG).expect("instance id"),
            CatalogVersion::from_bytes([5; 32]),
        ),
        ConnectorProviderId::parse("paimon").expect("provider id"),
        1,
        vec![
            CatalogProperty::new("paimon.catalog.type", "filesystem").expect("catalog type"),
            CatalogProperty::new("warehouse", warehouse.to_string_lossy()).expect("warehouse"),
        ],
        Vec::new(),
    )
    .expect("catalog properties");
    let normalized = factory
        .normalize_and_validate(properties)
        .expect("Paimon catalog properties");
    let binding = runtime
        .block_on(factory.materialize(
            normalized,
            MaterializationContext::new(Instant::now() + Duration::from_secs(60)),
        ))
        .expect("Paimon control role binding");
    host.register_role_binding(binding)
        .expect("publish the Paimon generation");
    PaimonFixture {
        _warehouse: directory,
        runtime,
        host,
    }
}

/// Relation lookups answered by the production catalog materializer, which
/// also admits each relation's binding into the statement's binding store.
struct FixtureCatalogFacts {
    host: Arc<ConnectorControlHost>,
    catalogs: Arc<QueryCatalogService>,
    bindings: Arc<QueryTableBindingStore>,
    context: ConnectorRequestContext,
    blocking: ConnectorBlockingIoSupervisor,
}

#[async_trait]
impl CatalogFactPort for FixtureCatalogFacts {
    async fn resolve_relations(
        &self,
        needs: &[CatalogRelationNeed],
    ) -> Result<Vec<CatalogRelationFact>, String> {
        let needs = needs.to_vec();
        let host = Arc::clone(&self.host);
        let catalogs = Arc::clone(&self.catalogs);
        let bindings = Arc::clone(&self.bindings);
        let context = self.context.clone();
        self.blocking
            .spawn_ordinary(move || {
                let loader = iceberg_table_binding_loader(host.as_ref(), context);
                let materializer = CatalogServiceMaterializer::new(
                    Some(CATALOG),
                    catalogs.as_ref(),
                    bindings,
                    loader,
                );
                needs
                    .iter()
                    .map(|need| {
                        crate::query_execution::completion_facts::catalog_fact(&materializer, need)
                    })
                    .collect()
            })
            .finish()
            .await
            .map_err(|error| format!("fixture catalog lane: {error}"))?
    }
}

/// Statistics answered by the production unified resolver.
struct FixtureStatisticsFacts {
    bindings: Arc<QueryTableBindingStore>,
    context: ConnectorRequestContext,
    blocking: ConnectorBlockingIoSupervisor,
}

#[async_trait]
impl StatisticsFactPort for FixtureStatisticsFacts {
    async fn resolve_statistics(
        &self,
        needs: &[StatisticsNeed],
    ) -> Result<Vec<StatisticsFact>, String> {
        let needs = needs.to_vec();
        let bindings = Arc::clone(&self.bindings);
        let context = self.context.clone();
        self.blocking
            .spawn_ordinary(move || {
                let resolver = crate::connector::UnifiedStatisticsResolver::default();
                needs
                    .iter()
                    .map(|need| {
                        crate::query_execution::planning::statistics::resolve_statistics_need(
                            &resolver, &bindings, need, &context,
                        )
                    })
                    .collect()
            })
            .finish()
            .await
            .map_err(|error| format!("fixture statistics lane: {error}"))?
    }
}

/// No materialized view exists in this fixture, which is an answer rather
/// than a failure: rewrite is an accelerator.
struct NoMaterializedViews;

#[async_trait]
impl MaterializedViewFactPort for NoMaterializedViews {
    async fn resolve_materialized_views(
        &self,
        needs: &[MaterializedViewNeed],
    ) -> Result<Vec<MaterializedViewFact>, String> {
        needs
            .iter()
            .map(|need| {
                MaterializedViewFact::missing(need, "the fixture has no materialized views")
                    .map_err(|error| error.to_string())
            })
            .collect()
    }
}

async fn query_scope() -> (
    (
        novarocks_workload_control::PendingQueryRoot,
        novarocks_workload_control::QueryConcurrencyPermit,
    ),
    novarocks_workload_control::WorkScope,
    novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024,
            control_bytes: 128,
            per_scope_bytes: 896,
        },
    )
    .expect("workload control");
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .expect("query result capacity");
    control.mark_ready().expect("workload control ready");
    let root = control
        .root_admission()
        .begin_query_root(WorkRequest::new(WorkClass::Query))
        .expect("query root");
    let scope = root.owner.scope();
    let (permit, window) = scope
        .admit_query_with_result(ResultWindowClass::Local)
        .expect("query result admission")
        .await
        .expect("query result grant");
    let capacity =
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &scope,
            window.retain_alias(),
        )
        .expect("original query result binding");
    ((root, permit), scope, capacity)
}

fn request_for(
    sql: &str,
    emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
) -> SqlFinalPlanCompileRequest {
    SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([9; 16]).expect("plan version"),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: Some(CATALOG.to_string()),
            current_database: "db".to_string(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        crate::query_execution::constant_eval::constant_evaluator(),
        crate::application::test_constant_policy(),
        emission_mode,
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: MAX_SCAN_BATCH_ROWS,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
}

impl PaimonFixture {
    /// Complete one statement against the Paimon catalog, freezing every scan
    /// through the production provider-read freeze.
    fn complete(&self, sql: &str) -> CompletedPlanWithAccess<FrozenProviderRead> {
        self.complete_with_emission(sql, compiled_carrier().sql_emission_mode())
    }

    fn complete_with_emission(
        &self,
        sql: &str,
        emission_mode: novarocks_sql::compiler::SqlPhysicalEmissionMode,
    ) -> CompletedPlanWithAccess<FrozenProviderRead> {
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            ConnectorStopOwner::new().view(),
            1,
            1,
        )
        .expect("connector request context");
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let blocking = ConnectorBlockingIoSupervisor::new(self.runtime.handle().clone());
        let (_root, scope, capacity) = self.runtime.block_on(query_scope());
        let source = QueryCompletionFactSource::new(
            Arc::new(FixtureCatalogFacts {
                host: Arc::clone(&self.host),
                catalogs: Arc::new(new_query_catalog_service()),
                bindings: Arc::clone(&bindings),
                context: context.clone(),
                blocking: blocking.clone(),
            }),
            Arc::new(FixtureStatisticsFacts {
                bindings: Arc::clone(&bindings),
                context: context.clone(),
                blocking: blocking.clone(),
            }),
            Arc::new(NoMaterializedViews),
            Arc::new(FrontendProviderReadFacts::new(
                Arc::clone(&self.host),
                bindings,
                crate::query_execution::compiler::typed_connector_session()
                    .expect("connector session"),
                context,
                blocking,
                capacity,
            )),
        );
        self.runtime
            .block_on(
                FinalPlanCompletionDriver::new(Arc::new(source))
                    .complete(request_for(sql, emission_mode), &scope),
            )
            .unwrap_or_else(|failure| panic!("{sql}: completes against Paimon: {failure}"))
    }
}

/// Package admission within the test encode limits' envelope. This is not a
/// production sizing.
fn compiled_carrier() -> StaticPlanCarrier {
    StaticPlanCarrier::CompiledPackage(CompiledPackageCarrier::new(
        novarocks_physical_plan::FragmentPackageAdmission {
            plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
            source_retained_bytes: 64 << 20,
            property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
                max_request_bytes: 16 << 20,
                max_coexisting_bytes: 256 << 20,
                max_projection_work: 16 << 20,
            },
        },
        novarocks_plan_codec::physical_package_v2::test_support::encode_limits(),
    ))
}

fn encode(
    completed: CompletedPlanWithAccess<FrozenProviderRead>,
    carrier: &StaticPlanCarrier,
) -> Result<EncodedCompletedPlan, novarocks_plan_codec::PhysicalEncodeError> {
    encode_completed_plan(
        completed,
        &novarocks_sql::compiler::build_builtin_engine_function_catalog()
            .expect("builtin engine function catalog"),
        carrier,
        crate::application::test_constant_policy(),
        None,
        false,
        &SqlCompileControl::unbounded(),
    )
}

/// The installed pure Paimon read program, as a read-only manifest entry.
/// This is a fixture catalogue, not the Server's sealed manifest.
fn paimon_programs() -> PureProviderProgramCatalog<ConnectorCodecError> {
    let provider = ConnectorProviderId::parse("paimon").expect("provider id");
    PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            true,
            false,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            Some(Arc::new(PaimonReadRecipeCompiler)),
            None,
        )],
        &SqlCompileControl::unbounded(),
    )
    .expect("Paimon pure program catalogue")
}

/// A real sealed RAND-only subset; the scan and its residual bind no
/// function, but the compiler refuses an empty pure catalog. This is not the
/// Server catalogue.
fn sealed_rand_subset() -> novarocks_functions::PureEngineFunctionCatalog {
    use novarocks_functions::{
        EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
        InstalledPureKernel, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    };
    let actual = novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
        .expect("builtin catalogue");
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .expect("rand definition")
                .clone(),
        )
        .expect("register rand");
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
        .expect("sealed rand subset")
}

/// The read the freeze of a plan's single scan left behind.
fn frozen_encodings(
    completed: CompletedPlanWithAccess<FrozenProviderRead>,
) -> (
    Arc<novarocks_physical_plan::PhysicalPlan>,
    BTreeMap<novarocks_physical_plan::ProviderReadOccurrenceId, FrozenReadEncoding>,
) {
    let plan = Arc::clone(completed.candidate().plan());
    let (_, reads) = completed.into_parts();
    let encodings = reads
        .into_occurrences()
        .into_iter()
        .map(|(occurrence, read)| (occurrence, read.access.encoding))
        .collect();
    (plan, encodings)
}

/// `SELECT c FROM t WHERE c > 1` over a real Paimon table freezes a compiled
/// package whose scan carries the provider's own public facts: the package
/// receiver decodes it, the provider's pure compiler accepts the authored
/// read, and the local compiler addresses the scan by its physical node.
#[test]
fn a_paimon_scan_freezes_a_compiled_package_the_provider_and_local_compilers_accept() {
    use novarocks_local_compiler::{
        LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };
    use novarocks_plan_codec::physical_package_v2::decode_fragment_package;
    use novarocks_plan_codec::physical_package_v2::test_support::decode_limits;
    use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;

    let fixture = paimon_fixture();
    let completed = fixture.complete(SCAN_SQL);
    let plan = Arc::clone(completed.candidate().plan());
    let scans = plan
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment
                .nodes()
                .values()
                .filter(|node| matches!(node.kind, NodeKind::Scan { .. }))
                .map(move |node| (fragment.id(), node))
        })
        .collect::<Vec<_>>();
    let [(scan_fragment, scan_node)] = scans.as_slice() else {
        panic!("{SCAN_SQL}: plans exactly one scan, got {}", scans.len());
    };
    let NodeKind::Scan {
        relation,
        residuals,
        ..
    } = &scan_node.kind
    else {
        unreachable!("filtered to scans");
    };
    // Paimon declines the filter, so the engine keeps it as the scan's one
    // residual and the provider only prunes.
    assert_eq!(residuals.len(), 1, "{SCAN_SQL}: one residual predicate");
    assert!(
        relation
            .predicate_guarantees()
            .iter()
            .all(|guarantee| guarantee.kind == PredicateGuaranteeKind::PruningOnly),
        "{SCAN_SQL}: Paimon guarantees no predicate"
    );

    let encoded = encode(completed, &compiled_carrier())
        .unwrap_or_else(|error| panic!("{SCAN_SQL}: the compiled carrier encodes: {error}"));
    // The attempt reads the same scan facts and capability as a plan tree
    // would: one read, scheduled at the scan's physical node.
    assert_eq!(encoded.access.iter().count(), 1, "one read capability");
    let [scan_facts] = encoded.plan_facts.scans() else {
        panic!("{SCAN_SQL}: one attempt scan fact");
    };
    assert_eq!(
        scan_facts.plan_node_id,
        i32::try_from(scan_node.id.get()).expect("wire node id")
    );
    assert_eq!(scan_facts.fragment_id, scan_fragment.get());
    assert_eq!(scan_facts.assignments.len(), 1);
    assert!(
        encoded.plan_facts.scheduling().fragments[&scan_fragment.get()].has_scans(),
        "scheduling places the scan's fragment as a scan fragment"
    );

    let control = SqlCompileControl::unbounded();
    let model = FragmentDecodeResourceModel::try_new(&control).expect("decode model");
    let providers = paimon_programs();
    let functions = sealed_rand_subset();
    let mut validated_reads = 0;
    for fragment in plan.fragments().values() {
        let id = fragment.id().get();
        let artifact = encoded.native.get(id).expect("artifact");
        let carried = novarocks_task_codec::creation::decode_static_package(
            artifact.content(),
            novarocks_proto_codec::FieldPath::root("frozen_fragment"),
        )
        .unwrap_or_else(|error| panic!("fragment {id} is a compiled carrier: {error}"));
        let decoded =
            decode_fragment_package(carried.package(), &model, &decode_limits(), &control)
                .unwrap_or_else(|error| panic!("fragment {id} package receives: {error}"));
        if fragment.id() == *scan_fragment {
            let [(node, read)] = decoded.scans().iter().collect::<Vec<_>>()[..] else {
                panic!("fragment {id} carries exactly its scan's frozen read");
            };
            assert_eq!(*node, scan_node.id);
            // The public field is Paimon's own: its name and its field ID,
            // not anything the statement called the column.
            let field = read.public_facts().schema().field(0);
            assert_eq!(field.name(), "c");
            assert_eq!(
                field.metadata().get("PARQUET:field_id").map(String::as_str),
                Some("0")
            );
            assert!(field.is_nullable());
            assert_eq!(
                read.scan().work_source(),
                novarocks_connector_contract::ConnectorReadWorkSource::RuntimeSplits
            );
            assert!(read.public_facts().metadata_kind().is_none());
        } else {
            assert!(decoded.scans().is_empty(), "fragment {id} reads nothing");
        }
        let result_sink = decoded.result().is_some();
        let validated = validate_fragment_providers(Arc::new(decoded), &providers, &control)
            .unwrap_or_else(|error| panic!("fragment {id} validates providers: {error}"));
        validated_reads += validated.reads().len();
        let program = compile_fragment(
            validated,
            &functions,
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(1).unwrap(),
                // The frozen profile places a result sink on one driver.
                root_sink_dop: result_sink.then(|| NonZeroUsize::new(1).unwrap()),
                kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
                constants: crate::application::test_constant_policy(),
                exchange_wait: Duration::from_secs(120),
            },
            &control,
        )
        .unwrap_or_else(|error| panic!("fragment {id} compiles: {error}"));
        let scan_nodes = program
            .scan_inputs()
            .values()
            .map(|input| input.scan_node)
            .collect::<Vec<_>>();
        if fragment.id() == *scan_fragment {
            assert_eq!(
                scan_nodes,
                [scan_node.id.get()],
                "the program addresses its scan"
            );
        } else {
            assert!(scan_nodes.is_empty());
        }
    }
    assert_eq!(validated_reads, 1, "the provider compiled exactly one read");
}

/// The same statement still freezes into the plan tree, with the same attempt
/// scan facts: the compiled author reads the freeze, it does not change it.
#[test]
fn a_paimon_scan_still_freezes_into_the_plan_tree() {
    let fixture = paimon_fixture();
    let tree = encode(
        fixture.complete_with_emission(SCAN_SQL, StaticPlanCarrier::PlanTree.sql_emission_mode()),
        &StaticPlanCarrier::PlanTree,
    )
    .unwrap_or_else(|error| panic!("{SCAN_SQL}: the plan tree encodes: {error}"));
    let packaged = encode(fixture.complete(SCAN_SQL), &compiled_carrier())
        .unwrap_or_else(|error| panic!("{SCAN_SQL}: the compiled carrier encodes: {error}"));
    assert_eq!(tree.access.iter().count(), 1);
    let ([tree_scan], [packaged_scan]) = (tree.plan_facts.scans(), packaged.plan_facts.scans())
    else {
        panic!("{SCAN_SQL}: each carrier states one attempt scan");
    };
    assert_eq!(tree_scan.fragment_id, packaged_scan.fragment_id);
    assert_eq!(tree_scan.plan_node_id, packaged_scan.plan_node_id);
    assert_eq!(
        tree_scan
            .assignments
            .iter()
            .map(|assignment| (assignment.variable().to_owned(), assignment.value_type()))
            .collect::<Vec<_>>(),
        packaged_scan
            .assignments
            .iter()
            .map(|assignment| (assignment.variable().to_owned(), assignment.value_type()))
            .collect::<Vec<_>>()
    );
    assert_eq!(tree.topology, packaged.topology);
}

/// Every fact of the authored read comes from the plan or the freeze; one
/// the freeze does not hold is refused, never filled in.
#[test]
fn a_frozen_read_the_freeze_cannot_complete_is_refused() {
    let fixture = paimon_fixture();
    let control = SqlCompileControl::unbounded();
    let (plan, encodings) = frozen_encodings(fixture.complete(SCAN_SQL));
    let authored = author_frozen_reads(&plan, &encodings, &control).expect("the freeze authors");
    assert_eq!(
        authored.keys().collect::<Vec<_>>(),
        encodings.keys().collect::<Vec<_>>()
    );

    // A provider that declined to publish the public fields leaves the
    // compiled read with none, and its own reason is reported.
    let (plan, mut encodings) = frozen_encodings(fixture.complete(SCAN_SQL));
    for encoding in encodings.values_mut() {
        encoding.public_schema = Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "fixture provider publishes no schema",
        ));
    }
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &control),
        Err(PackageFreezeError::Read(reason))
            if reason.contains("published no public read schema")
                && reason.contains("fixture provider publishes no schema")
    ));

    // An assignment the plan's relation does not have cannot be paired.
    let (plan, mut encodings) = frozen_encodings(fixture.complete(SCAN_SQL));
    for encoding in encodings.values_mut() {
        let repeated = encoding.assignments[0].clone();
        encoding.assignments.push(repeated);
    }
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &control),
        Err(PackageFreezeError::Read(reason)) if reason.contains("relation has 1 columns")
    ));

    // A freeze no scan reads is a pairing defect.
    let (plan, mut encodings) = frozen_encodings(fixture.complete(SCAN_SQL));
    let (_, other) = frozen_encodings(fixture.complete(SCAN_SQL));
    let spare = other.into_values().next().expect("a second freeze");
    encodings.insert(
        novarocks_physical_plan::ProviderReadOccurrenceId::new(u32::MAX),
        spare,
    );
    assert!(matches!(
        author_frozen_reads(&plan, &encodings, &control),
        Err(PackageFreezeError::Facts(reason)) if reason.contains("scanned by no node")
    ));
}
