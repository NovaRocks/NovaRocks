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

//! Actual SQLite/QB1 refresh reader, named SourceRead gate and public receipt.
//! Tiny immutable provider facts exercise FE routing, not Iceberg discovery.

use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_mv_application::persistence::{
    codec::PhysicalFieldLogicalIdentity,
    identity::FieldIdentity,
    keyed_documents::{MvKeyedObservedCurrentDocuments, StoredKeyedMvProjection},
    test_support::{KeyedProjectionFixture, fixture_mapping, fixture_type, observed_current_keyed},
};
use novarocks_spi::connector::document_storage::*;
use novarocks_spi::connector::read_stack::adapter::*;
use novarocks_spi::connector::read_stack::*;
use novarocks_spi::connector::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::AtomicUsize,
};

type CompileResult<T> =
    Result<T, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InitialSourceSchema {
    Original,
    MissingBoundField,
    ChangedBoundType,
    UnrelatedField,
    ReplacedObject,
}

pub(super) struct SourceFixture {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    catalog: CatalogHandle,
    projection: StoredKeyedMvProjection,
    current: MvKeyedObservedCurrentDocuments,
    target_names: BTreeMap<FieldIdentity, String>,
    initial_observations: AtomicUsize,
    preparations: AtomicUsize,
    requests: Mutex<Vec<ConnectorReadSourcePreparationRequest>>,
    historical_requests: Mutex<Vec<ConnectorReadSourcePreparationRequest>>,
    schema_projections: AtomicUsize,
    captured_context: Mutex<Option<ConnectorRequestContext>>,
    snapshot: i64,
    background_composition: bool,
    initial_schema: InitialSourceSchema,
}
/// Build the visible-only target for a non-aggregate MV. Source occurrences
/// retain their own identities, including for UNION ALL and self joins.
fn row_projection(
    fixture: &mut KeyedProjectionFixture,
    self_join: bool,
) -> BTreeMap<FieldIdentity, String> {
    let field = |id| FieldIdentity::try_new(vec![id]).unwrap();
    fixture.definition.query.effective_sql = if self_join {
        "SELECT o.amount AS amount FROM ice.sales.orders o JOIN ice.sales.orders o2 ON o.order_id=o2.order_id".into()
    } else {
        "SELECT o.amount AS amount FROM ice.sales.orders o UNION ALL SELECT o2.amount AS amount FROM ice.sales.orders o2".into()
    };
    for occurrence in &mut fixture.binding.occurrences {
        occurrence
            .paths
            .iter_mut()
            .find(|path| path.nodes[0].name_at_binding == "amount")
            .unwrap()
            .leaf_type = fixture_type("double");
    }
    let output = &mut fixture.binding.outputs[0];
    output.name = "amount".into();
    if self_join {
        output
            .source_fields
            .retain(|value| value.occurrence_id == 7);
    }
    fixture.interpretation.outputs[0].source_type = fixture_type("double");
    fixture.interpretation.branches.clear();
    fixture.interpretation.aggregates.clear();
    fixture.interpretation.state_slots.clear();
    fixture.interpretation.apply_key = None;
    fixture.interpretation.target.fields.retain_mut(|binding| {
        if matches!(
            binding.logical_identity,
            PhysicalFieldLogicalIdentity::Output(_)
        ) {
            binding.source_type = fixture_type("double");
            true
        } else {
            false
        }
    });
    fixture.mapping = fixture_mapping(&fixture.binding, &fixture.interpretation);
    BTreeMap::from([(field(31), "amount".into())])
}

impl SourceFixture {
    fn new(self_join: bool, background_composition: bool) -> Arc<Self> {
        let mut fixture = KeyedProjectionFixture::new(
            novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        let target_names = row_projection(&mut fixture, self_join);
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().unwrap(),
        };
        let catalog = CatalogHandle::new(
            ConnectorInstanceId::parse("ice").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let current = observed_current_keyed(&projection.facts, catalog.clone());
        let mut source = Arc::new(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            epoch: current.view.owner.incarnation,
            catalog,
            projection,
            current,
            target_names,
            initial_observations: AtomicUsize::new(0),
            preparations: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            historical_requests: Mutex::new(Vec::new()),
            schema_projections: AtomicUsize::new(0),
            captured_context: Mutex::new(None),
            snapshot: 18,
            background_composition,
            initial_schema: InitialSourceSchema::Original,
        });
        let facts = source.bind_original_projection();
        let source_mut = Arc::get_mut(&mut source).unwrap();
        source_mut.current = observed_current_keyed(&facts, source_mut.catalog.clone());
        source_mut.projection.facts = facts;
        *source_mut.schema_projections.get_mut() = 0;
        source
    }

    fn bind_original_projection(
        self: &Arc<Self>,
    ) -> novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection {
        use crate::catalog_application::{
            query_bindings::{QueryTableBindingKey, QueryTableBindingStore},
            query_catalog::connector_table_materialization_from_metadata,
            query_materializer::connector_query_binding_from_materialization,
        };
        use crate::mv::domain::{
            analysis::refresh_property::derive_fragment_property,
            analysis_adapter::analyze_mv_select_with_captured_catalog,
            persistence::source_bindings::observe_mv_binding,
            refresh::definition::{read_stored_mv_query, stored_mv_identity},
        };
        use novarocks_mv_application::persistence::{
            create_documents::{MvInterpretationFacts, build_mv_refresh_interpretation},
            exact_revision::persist_exact_connector_revision,
            storage_mapping::{build_storage_mapping, storage_mapping_requests},
        };
        use novarocks_sql::compiler::{
            CatalogRelationFact, SqlCapturedCatalogPreparation, SqlCompileControl,
            builtin_sql_function_catalog,
        };

        let context = crate::connector::test_request_context();
        let host = self.control_host();
        let pin = crate::connector::metadata_binding_typed(host.as_ref(), "ice").unwrap();
        let metadata = |source| {
            crate::connector::schema_creation::with_schema_creation(&context, |creation| {
                self.metadata(source, creation)
            })
            .unwrap()
        };
        let source = metadata(true);
        let materialization =
            connector_table_materialization_from_metadata(source.clone(), pin.clone()).unwrap();
        let bindings = QueryTableBindingStore::try_new().unwrap();
        let binding = bindings
            .resolve_or_insert_with_id(
                QueryTableBindingKey::strict_base("ice", "sales", "orders"),
                |id| {
                    connector_query_binding_from_materialization(
                        materialization,
                        "ice",
                        "sales",
                        "orders",
                        id,
                    )
                },
            )
            .unwrap();
        let definition = self.projection.facts.definition();
        let query = read_stored_mv_query(
            &definition.query.effective_sql,
            &stored_mv_identity(&self.projection),
        )
        .unwrap();
        let query = crate::mv::domain::analysis::canonicalize_iceberg_mv_select_query(
            &query,
            Some("ice"),
            "sales",
        );
        let control = SqlCompileControl::unbounded();
        let preparation = SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("ice"),
            "sales",
            control.clone(),
        )
        .unwrap();
        let relation = bindings.binding(binding).unwrap().resolved.clone();
        let relations = preparation
            .needs()
            .iter()
            .map(|need| CatalogRelationFact::resolved(need, relation.clone()).unwrap())
            .collect::<Vec<_>>();
        let catalog = preparation.capture(&relations).unwrap();
        let analysis = analyze_mv_select_with_captured_catalog(
            Some("ice"),
            &catalog,
            "sales",
            &query,
            builtin_sql_function_catalog(),
            crate::application::test_constant_policy(),
            control.clone(),
        )
        .unwrap();
        let sql = analysis.refresh_input.create_persistence_facts().unwrap();
        let observed =
            observe_mv_binding(self.as_ref(), &sql, &bindings, &[], context.clone()).unwrap();
        let property = derive_fragment_property(&analysis, &control).unwrap();
        let storage = metadata(false);
        let schema = crate::mv::domain::storage_observation::observe_schema_validation(
            self.as_ref(),
            &pin,
            &storage,
            context.clone(),
        )
        .unwrap();
        let interpretation = build_mv_refresh_interpretation(
            MvInterpretationFacts {
                definition,
                sql_facts: &sql,
                source_observations: &observed.relations,
                runtime_layout: &crate::mv::domain::iceberg_refresh::stateless_mv_runtime_layout(),
                target_identity: &property.identity,
                apply_key_column_name: None,
                branch_column_name: None,
            },
            schema.exact_schema(),
        )
        .unwrap();
        let requests = storage_mapping_requests(&sql, &interpretation).unwrap();
        let mapped = self
            .observe_current_storage_mapping(&pin, &storage, &requests, context)
            .unwrap();
        let mapping = build_storage_mapping(&sql, &requests, mapped.columns).unwrap();
        let prior = self
            .exact_semantic_revision(&source.table, ConnectorReadSelector::SnapshotId(17))
            .unwrap();
        let (object_id, native_data_version) = persist_exact_connector_revision(&prior).unwrap();
        let mut publication = self.current.publication.clone().unwrap();
        publication.inputs = observed
            .document
            .occurrences
            .iter()
            .map(
                |occurrence| novarocks_mv_application::persistence::codec::PublicationInput {
                    relation_occurrence_id: occurrence.occurrence_id,
                    object_id: object_id.clone(),
                    native_data_version: native_data_version.clone(),
                },
            )
            .collect();
        KeyedProjectionFixture {
            target: novarocks_mv_application::product::MvTarget::from_parts(
                Some("ice"),
                "sales",
                "mv",
            ),
            object_id: self.current.view.object_id.clone(),
            definition: definition.clone(),
            binding: observed.document,
            mapping,
            interpretation,
            configuration: self.projection.facts.configuration().clone(),
            publication: Some(publication),
            metadata_version: self.current.view.metadata_version.clone(),
            file_totals: self.current.view.current_snapshot_file_totals,
        }
        .build()
        .unwrap()
    }
    fn object(&self) -> ConnectorTableObjectId {
        novarocks_mv_application::persistence::exact_revision::restore_persisted_object(
            &self.projection.facts.binding().occurrences[0].object_id,
        )
        .unwrap()
    }
    fn initial_object(&self) -> ConnectorTableObjectId {
        if self.initial_schema == InitialSourceSchema::ReplacedObject {
            ConnectorTableObjectId::try_new(Bytes::from_static(b"replacement-source-object"))
                .unwrap()
        } else {
            self.object()
        }
    }
    fn source_identity(&self) -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: self.descriptor.instance_id.clone(),
            namespace: "sales".into(),
            table: "orders".into(),
        }
    }
    fn source_fields(&self) -> Vec<MvObservedSourceField> {
        [
            (2, "amount", "double", true),
            (1, "order_id", "bigint", false),
        ]
        .into_iter()
        .map(|(id, name, kind, nullable)| {
            MvObservedSourceField::try_new(
                Bytes::from(vec![id]),
                name.into(),
                fixture_type(kind),
                nullable,
            )
            .unwrap()
        })
        .collect()
    }
    fn observed_source_fields(&self, initial: bool) -> Vec<MvObservedSourceField> {
        let mut fields = self.source_fields();
        if initial {
            match self.initial_schema {
                InitialSourceSchema::MissingBoundField => {
                    fields.retain(|field| field.name() != "amount");
                }
                InitialSourceSchema::ChangedBoundType => {
                    let amount = fields
                        .iter_mut()
                        .find(|field| field.name() == "amount")
                        .unwrap();
                    *amount = MvObservedSourceField::try_new(
                        amount.provider_field_id().clone(),
                        amount.name().to_owned(),
                        fixture_type("bigint"),
                        amount.nullable(),
                    )
                    .unwrap();
                }
                InitialSourceSchema::UnrelatedField => fields.push(
                    MvObservedSourceField::try_new(
                        Bytes::from_static(b"unrelated-current-field"),
                        "unrelated_current".to_owned(),
                        fixture_type("bigint"),
                        true,
                    )
                    .unwrap(),
                ),
                InitialSourceSchema::Original | InitialSourceSchema::ReplacedObject => {}
            }
        }
        fields
    }
    fn target_fields(&self) -> Vec<(u32, MvObservedSourceField)> {
        let mut seen = BTreeSet::new();
        self.projection
            .facts
            .interpretation()
            .target
            .fields
            .iter()
            .filter(|v| seen.insert(v.target_field_id.clone()))
            .enumerate()
            .map(|(i, v)| {
                (
                    u32::try_from(i).unwrap(),
                    MvObservedSourceField::try_new(
                        Bytes::copy_from_slice(v.target_field_id.as_bytes()),
                        self.target_names[&v.target_field_id].clone(),
                        v.source_type.clone(),
                        v.nullable,
                    )
                    .unwrap(),
                )
            })
            .collect()
    }
    fn metadata(
        &self,
        source: bool,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> CompileResult<ConnectorTableMetadata> {
        self.metadata_for_projection(
            source,
            true,
            if source { self.snapshot } else { 11 },
            creation,
        )
    }
    fn statistics_version(snapshot: i64) -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(Bytes::copy_from_slice(&snapshot.to_be_bytes())).unwrap()
    }
    fn metadata_for_projection(
        &self,
        source: bool,
        initial: bool,
        snapshot: i64,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> CompileResult<ConnectorTableMetadata> {
        let identity = if source {
            self.source_identity()
        } else {
            self.projection.facts.source_revision().target.clone()
        };
        let mut fields: Vec<Field> = if source {
            vec![
                Field::new("amount", DataType::Float64, true),
                Field::new("order_id", DataType::Int64, false),
                Field::new("_row_id", DataType::Int64, false),
            ]
        } else {
            self.target_fields()
                .iter()
                .map(|(_, v)| {
                    Field::new(
                        v.name(),
                        crate::mv::domain::rewrite::context::arrow_type_from_provider_type(
                            v.source_type(),
                        )
                        .unwrap(),
                        v.nullable(),
                    )
                })
                .collect()
        };
        if source && initial {
            match self.initial_schema {
                InitialSourceSchema::MissingBoundField => {
                    fields.retain(|field| field.name() != "amount");
                }
                InitialSourceSchema::ChangedBoundType => {
                    let amount = fields
                        .iter_mut()
                        .find(|field| field.name() == "amount")
                        .unwrap();
                    *amount = Field::new("amount", DataType::Int64, true);
                }
                InitialSourceSchema::UnrelatedField => {
                    fields.push(Field::new("unrelated_current", DataType::Int64, true));
                }
                InitialSourceSchema::Original | InitialSourceSchema::ReplacedObject => {}
            }
        }
        let types = fields
            .iter()
            .map(|v| {
                novarocks_type_contract::FunctionValueType::new(
                    v.data_type().clone(),
                    v.is_nullable(),
                )
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let context = crate::connector::test_request_context();
        let column_facts = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, _)| {
                ConnectorTableColumnPlanningFact::new(
                    u32::try_from(i).unwrap(),
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    if source && schema.field(i).name() == "_row_id" {
                        ConnectorTableColumnRole::RowLineageSystem
                    } else {
                        ConnectorTableColumnRole::Ordinary
                    },
                )
            })
            .collect();
        let facts = if source {
            ConnectorTablePlanningFacts::try_new_snapshot_source(
                &schema,
                column_facts,
                vec![],
                vec![],
                &context,
            )
        } else {
            ConnectorTablePlanningFacts::try_new(
                &schema,
                column_facts,
                vec![],
                vec![],
                vec![],
                &context,
            )
        }
        .unwrap();
        let result = crate::connector::schema_creation::declared_test_metadata_with_creation(
            ConnectorTableMetadataHeader {
                identity,
                planning_facts: facts,
                definition_facts: ConnectorTableDefinitionFacts::empty(),
                version: Some(if source {
                    Bytes::from_static(b"source-schema")
                } else {
                    self.projection.facts.metadata_version().payload().clone()
                }),
                statistics_data_version: Some(Self::statistics_version(snapshot)),
                table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    Bytes::from_static(match (source, initial, snapshot) {
                        (true, true, 18) => b"original-source-18",
                        (true, false, 18) => b"prepared-source-to-18",
                        (true, false, 17) => b"prepared-source-from-17",
                        (false, _, 11) => b"original-target-11",
                        _ => panic!("unexpected original projection snapshot {snapshot}"),
                    }),
                )
                .unwrap(),
            },
            schema,
            types,
            creation,
        )?;
        Ok(
            result.with_source_read_facts(ConnectorProcessLocalPayload::new(
                ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.epoch,
                },
                Arc::new(snapshot),
            )),
        )
    }
    fn control_host(self: &Arc<Self>) -> Arc<novarocks_catalog_application::ConnectorControlHost> {
        let template = novarocks_catalog_application::test_support::test_control_binding_for(
            self.descriptor.instance_id.clone(),
            self.epoch.to_bytes()[0],
        );
        let control = ConnectorControlBinding::try_new_with_statistics(
            self.descriptor.clone(),
            self.epoch,
            self.clone(),
            template.planning().clone(),
            template.execution_distribution().clone(),
            None,
            Some(self.clone()),
        )
        .unwrap()
        .with_catalog_properties(
            CatalogProperties::new(
                self.catalog.clone(),
                self.descriptor.provider_id.clone(),
                1,
                vec![],
                vec![],
            )
            .unwrap(),
        )
        .unwrap()
        .try_with_document_storage(Some(
            ConnectorDocumentStorageBinding::try_new(
                self.descriptor.clone(),
                self.epoch,
                Some(self.clone()),
                None,
            )
            .unwrap(),
        ))
        .unwrap();
        let adapter = Arc::new(ReadRuntimeAdapter::new(self.clone()));
        let host = Arc::new(novarocks_catalog_application::ConnectorControlHost::new());
        host.register_role_binding(
            ConnectorControlRoleBinding::try_new(
                NormalizedCatalogProperties::try_new(control.catalog_properties().unwrap().clone())
                    .unwrap(),
                Arc::new(control),
                Some(ConnectorControlReadBinding::new(
                    adapter.clone(),
                    adapter.clone(),
                    Some(Arc::new(SourceRequestFactory(adapter))),
                    Arc::new(NoEncoding),
                )),
                None,
            )
            .unwrap(),
        )
        .unwrap();
        host
    }

    pub(super) fn ports(
        self: &Arc<Self>,
        readiness: Arc<MvReadinessPort>,
        entrance: Arc<ManagementEntrance>,
    ) -> IcebergMvCorePorts {
        let host = self.control_host();
        if self.background_composition {
            // Exercise the same composition factory used by the production
            // background bindings, then the real SourceRead/public adapter path.
            return {
                let trc_catalog = Arc::new(
                    crate::catalog_application::query_catalog::new_query_catalog_service(),
                );
                let trc_controls = host.clone();
                let trc_system_relations = std::sync::Arc::new(
                    novarocks_query_application::system_tables::SystemTableRegistry::try_new([])
                        .expect("sealed declaration fixture"),
                );
                crate::capabilities::MvBackgroundPorts::new(
                    Arc::new(
                        novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap(),
                    ),
                    trc_catalog.clone(),
                    None,
                    trc_controls.clone(),
                    readiness,
                    self.clone(),
                    entrance,
                    crate::application::test_constant_policy(),
                    Arc::new(novarocks_query_application::view::QueryViewService::new()),
                    trc_system_relations,
                    novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                )
            }
            .into_core_ports(host);
        }
        {
            let trc_catalog =
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service());
            let trc_controls = host.clone();
            let trc_system_relations = std::sync::Arc::new(
                novarocks_query_application::system_tables::SystemTableRegistry::try_new([])
                    .expect("sealed declaration fixture"),
            );
            IcebergMvCorePorts::new_with_management_entrance(
                Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
                trc_catalog.clone(),
                None,
                trc_controls.clone(),
                host,
                readiness,
                self.clone(),
                entrance,
                crate::application::test_constant_policy(),
                Arc::new(novarocks_query_application::view::QueryViewService::new()),
                trc_system_relations,
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            )
        }
    }
}
// The retained control generation authenticates this closed fixture author.
// Its preparation below returns tiny immutable facts and never submits work;
// a new CPU dispatch would be an unexpected change to this test's boundary.
impl ProviderMetadataCpuSource for SourceFixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn run_metadata(&self) {
        panic!("tiny immutable Source preparation must not submit metadata CPU work")
    }
}
impl ConnectorMetadata for SourceFixture {
    fn provider_metadata_cpu_author(
        &self,
    ) -> Option<(ConnectorProviderBindingKey, std::any::TypeId)> {
        Some((
            ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.epoch,
            },
            std::any::TypeId::of::<SourceFixture>(),
        ))
    }
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }
    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        panic!("no listing in refresh preparation")
    }
    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        panic!("no existence fallback")
    }
    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        panic!("no table listing")
    }
    fn prepare_source_table(
        &self,
        request: ConnectorTableRequest,
    ) -> ConnectorTablePreparationFuture<'_> {
        Box::pin(async move {
            assert_eq!(request.table, self.source_identity());
            *self.captured_context.lock().unwrap() = Some(request.context.clone());
            self.initial_observations.fetch_add(1, Ordering::SeqCst);
            Ok(ConnectorTablePreparation::new(
                self.descriptor.instance_id.clone(),
                true,
            ))
        })
    }
    fn prepare_table(&self, request: ConnectorTableRequest) -> ConnectorTablePreparationFuture<'_> {
        Box::pin(async move {
            assert_eq!(
                request.table,
                self.projection.facts.source_revision().target,
                "source must never late-reload Current through generic metadata"
            );
            Ok(ConnectorTablePreparation::new(
                self.descriptor.instance_id.clone(),
                false,
            ))
        })
    }
    fn create_prepared_table(
        &self,
        prepared: ConnectorTablePreparation,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> CompileResult<ConnectorTableMetadata> {
        self.metadata(
            prepared.into_facts::<bool>(&self.descriptor.instance_id)?,
            creation,
        )
    }
    fn load_table(
        &self,
        _: ConnectorTableRequest,
        _: &mut ConnectorReadSchemaCreation<'_>,
    ) -> CompileResult<ConnectorTableMetadata> {
        panic!("no synchronous catalog fallback")
    }
    fn exact_semantic_revision(
        &self,
        table: &novarocks_spi::connector::ConnectorTableHandle,
        selector: ConnectorReadSelector,
    ) -> Result<ConnectorExactSemanticRevision, ConnectorError> {
        let source = match table.payload().as_ref() {
            b"original-source-18" | b"prepared-source-to-18" | b"prepared-source-from-17" => true,
            b"original-target-11" => false,
            _ => panic!("unknown original table revision request"),
        };
        let snapshot = match selector {
            ConnectorReadSelector::Current => match table.payload().as_ref() {
                b"prepared-source-from-17" => 17,
                _ if source => self.snapshot,
                _ => 11,
            },
            ConnectorReadSelector::SnapshotId(id) => id,
            _ => panic!("no other selector"),
        };
        assert!(if source {
            matches!(snapshot, 17 | 18)
        } else {
            snapshot == 11
        });
        let object = if source {
            if table.payload().as_ref() == b"original-source-18" {
                self.initial_object()
            } else {
                self.object()
            }
        } else {
            self.projection
                .facts
                .source_revision()
                .target_object_id
                .clone()
        };
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            self.descriptor.provider_id.clone(),
            &object,
            Some(snapshot),
        )
    }
    fn read_selector_from_exact_revision(
        &self,
        table: &novarocks_spi::connector::ConnectorTableHandle,
        revision: &ConnectorExactSemanticRevision,
    ) -> Result<ConnectorReadSelector, ConnectorError> {
        let source = match table.payload().as_ref() {
            b"original-source-18" | b"prepared-source-to-18" | b"prepared-source-from-17" => true,
            b"original-target-11" => false,
            _ => panic!("unknown original table revision selector request"),
        };
        let object = if source {
            if table.payload().as_ref() == b"original-source-18" {
                self.initial_object()
            } else {
                self.object()
            }
        } else {
            self.projection
                .facts
                .source_revision()
                .target_object_id
                .clone()
        };
        assert_eq!(revision.object_identity().value(), object.as_bytes());
        let Some(ConnectorCanonicalReadPoint::Snapshot(Some(id))) = revision.canonical_read_point()
        else {
            panic!("exact original table revision")
        };
        assert!(if source {
            matches!(id, 17 | 18)
        } else {
            id == 11
        });
        Ok(ConnectorReadSelector::SnapshotId(id))
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct Column;
impl ColumnHandle for Column {}
#[derive(Debug)]
pub(super) struct Split;
impl novarocks_spi::connector::read_stack::ConnectorSplit for Split {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}
impl ProviderReadRuntime for SourceFixture {
    type Table = ();
    type Execution = ();
    type Column = Column;
    type Transaction = ();
    type Split = Split;
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) {}
}
impl ProviderReadMetadata for SourceFixture {
    type PreparedSourceRead = ();
    type PreparedRelation = ();
    fn prepare_source_read<'a>(
        &'a self,
        _: &'a ConnectorSession,
        request: &'a ConnectorReadSourcePreparationRequest,
        _: &'a Arc<dyn ProviderMetadataCpuPort>,
        _: &'a ConnectorOriginalResultScope,
        _: &'a mut Option<ConnectorProcessLocalPayload>,
    ) -> futures::future::BoxFuture<'a, Result<ProviderPreparedSourceRead<()>, ConnectorError>>
    {
        Box::pin(async move {
            request
                .observed_source_facts()
                .validate_owner(&ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.epoch,
                })?;
            assert_eq!(
                request.name(),
                &SchemaTableName::try_new("sales", "orders").unwrap()
            );
            assert_eq!(request.object_id(), &self.object());
            assert_eq!(
                request.observed_table().payload().as_ref(),
                b"original-source-18"
            );
            let from = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                self.descriptor.provider_id.clone(),
                &self.object(),
                Some(17),
            )?;
            let to = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                self.descriptor.provider_id.clone(),
                &self.object(),
                Some(18),
            )?;
            if request.selection() == ConnectorReadSourceSelection::Snapshot(17) {
                assert!(request.from_revision().is_none());
                assert_eq!(request.to_revision(), &from);
                self.historical_requests
                    .lock()
                    .unwrap()
                    .push(request.clone());
                let context = crate::connector::test_request_context();
                let metadata =
                    crate::connector::schema_creation::with_schema_creation(&context, |creation| {
                        self.metadata_for_projection(true, false, 17, creation)
                    })
                    .map_err(crate::connector::schema_creation::connector_schema_error)?;
                return Ok(ProviderPreparedSourceRead::snapshot((), Arc::new(metadata)));
            }
            assert_eq!(
                request.selection(),
                ConnectorReadSourceSelection::ChangeWindow(ConnectorReadChangeWindow::new(17, 18))
            );
            assert_eq!(request.from_revision(), Some(&from));
            assert_eq!(request.to_revision(), &to);
            self.preparations.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            let context = crate::connector::test_request_context();
            let metadata =
                crate::connector::schema_creation::with_schema_creation(&context, |creation| {
                    self.metadata_for_projection(true, false, 18, creation)
                })
                .map_err(crate::connector::schema_creation::connector_schema_error)?;
            Ok(ProviderPreparedSourceRead::change_window(
                (),
                ConnectorChangeWindowFacts::Difference {
                    negative_weight: ConnectorChangeWindowNegativeWeight::Absent,
                    partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
                    volume: ConnectorChangeWindowVolume {
                        source_event_rows_upper_bound: 1,
                        source_data_file_bytes: 128,
                    },
                },
                Arc::new(metadata),
            ))
        })
    }
    fn project_prepared_source_read(
        &self,
        _: &ConnectorSession,
        _: &ConnectorReadSourcePreparationRequest,
        _: &(),
        _: ConnectorReadSourceProjection,
    ) -> Result<(), ConnectorError> {
        panic!("EXPLAIN must not open execution relations")
    }
    fn prepare_relation<'a>(
        &'a self,
        _: &'a ConnectorSession,
        _: &'a ConnectorReadRelationPreparationRequest,
    ) -> futures::future::BoxFuture<'a, Result<Option<()>, ConnectorError>> {
        panic!("no generic read preparation")
    }
    fn create_prepared_relation(
        &self,
        _: (),
    ) -> Result<Option<ProviderReadOpenedRelation<()>>, ConnectorError> {
        panic!("no generic relation")
    }
    fn get_table_handle(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadRelationVersion,
        _: Option<&str>,
    ) -> Result<Option<()>, ConnectorError> {
        panic!("no Current relation fallback")
    }
    fn get_pinned_file_set_handle(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: &ConnectorPinnedFileSet,
    ) -> Result<Option<()>, ConnectorError> {
        panic!("no pinned file set")
    }
    fn get_column_bindings(
        &self,
        _: &ConnectorSession,
        _: &(),
    ) -> Result<Vec<ProviderReadColumnBinding<Column>>, ConnectorError> {
        panic!("EXPLAIN catalog comes from source metadata")
    }
    fn apply_filter(
        &self,
        _: &ConnectorSession,
        _: &(),
        _: &Constraint<Column>,
    ) -> ProviderReadFilterResult<(), Column> {
        panic!("no runtime pushdown")
    }
    fn apply_projection(
        &self,
        _: &ConnectorSession,
        _: &(),
        _: &[Assignment<Column>],
    ) -> Result<Option<()>, ConnectorError> {
        panic!("no runtime projection")
    }
    fn apply_limit(
        &self,
        _: &ConnectorSession,
        _: &(),
        _: u64,
    ) -> Result<Option<ProviderReadLimitApplication<()>>, ConnectorError> {
        panic!("no runtime limit")
    }
    fn get_system_table_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<()>>, ConnectorError> {
        panic!("no runtime metadata relation")
    }
    fn get_change_window_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadChangeWindow,
    ) -> Result<Option<()>, ConnectorError> {
        panic!("EXPLAIN must reuse the original prepared source receipt")
    }
}
impl ProviderReadSplitManager for SourceFixture {
    fn get_splits<'a>(
        &'a self,
        _: &'a ConnectorSession,
        _: &'a (),
        _: &'a [Assignment<Column>],
        _: &'a BTreeSet<Column>,
        _: &'a Constraint<Column>,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Box<dyn ProviderReadSplitSource<Self>>, ConnectorError>,
    > {
        panic!("EXPLAIN must never enumerate splits")
    }
}
struct SourceRequestFactory(Arc<ReadRuntimeAdapter<SourceFixture>>);
impl ConnectorReadRequestControlFactory for SourceRequestFactory {
    fn for_planning(
        &self,
        _: &ConnectorPlanningContext,
    ) -> Result<ConnectorReadRequestControl, ConnectorError> {
        Ok(ConnectorReadRequestControl::unsupported_attempt_access(
            self.0.clone(),
            self.0.clone(),
        ))
    }
}
struct NoEncoding;
impl ConnectorReadWireEncoder for NoEncoding {
    fn owner(&self) -> &str {
        "iceberg"
    }
    fn encode_relation_payload(
        &self,
        _: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError> {
        panic!("EXPLAIN does not freeze provider execution")
    }
    fn encode_column_payload(
        &self,
        _: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        panic!("EXPLAIN does not freeze columns")
    }
    fn encode_transaction_payload(
        &self,
        _: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        panic!("EXPLAIN does not freeze transaction")
    }
    fn encode_split_payload(
        &self,
        _: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError> {
        panic!("EXPLAIN does not freeze splits")
    }
}
impl MvStorageObservationPort for SourceFixture {
    fn observe_source_binding(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        paths: &[MvSourceFieldPath],
        _: ConnectorRequestContext,
    ) -> Result<MvSourceBindingObservation, ConnectorError> {
        use novarocks_type_contract::CanonicalFactWriter;

        assert_eq!(metadata.table.payload().as_ref(), b"original-source-18");
        let object_id = self.initial_object();
        let fields = self.observed_source_fields(true);
        let mut path_fact = CanonicalFactWriter::new("fixture.mv.source-paths", 1);
        let mut type_fact = CanonicalFactWriter::new("fixture.mv.source-types", 1);
        path_fact.sequence(paths.len());
        type_fact.sequence(paths.len());
        let paths = paths
            .iter()
            .map(|path| {
                let [coordinate] = path.fields.as_slice() else {
                    panic!("expected a scalar path");
                };
                let field = &fields[usize::try_from(coordinate.ordinal).unwrap()];
                assert_eq!(field.name(), coordinate.name);
                path_fact.bytes(field.provider_field_id());
                path_fact.text(field.name());
                path_fact.boolean(path.whole_subtree);
                type_fact.bytes(&field.source_type().canonical_bytes());
                type_fact.boolean(field.nullable());
                MvSourcePathBinding {
                    fields: vec![MvSourceFieldBinding {
                        provider_field_id: field.provider_field_id().clone(),
                        required: !field.nullable(),
                        kind: MvSourceFieldKind::Scalar,
                    }],
                    leaf_type: field.source_type().clone(),
                    whole_subtree: path.whole_subtree,
                }
            })
            .collect();
        Ok(MvSourceBindingObservation {
            catalog_instance: self.descriptor.instance_id.clone(),
            object_fact: MvSourceBindingFact::from_canonical_bytes(object_id.as_bytes().to_vec()),
            object_id,
            object_kind: MvSourceObjectKind::Table,
            paths,
            path_fact: MvSourceBindingFact::from_canonical_bytes(path_fact.finish()),
            type_fact: MvSourceBindingFact::from_canonical_bytes(type_fact.finish()),
            read_declarations_fact: MvSourceBindingFact::from_canonical_bytes(
                b"fixture-declarations".to_vec(),
            ),
        })
    }

    fn observe_current_storage_mapping(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        requests: &[MvStorageColumnRequest],
        _: ConnectorRequestContext,
    ) -> Result<MvCurrentStorageMapping, ConnectorError> {
        assert_eq!(metadata.table.payload().as_ref(), b"original-target-11");
        let fields = self.target_fields();
        let columns = requests
            .iter()
            .map(|request| {
                let field = fields
                    .iter()
                    .map(|(_, field)| field)
                    .find(|field| field.provider_field_id() == &request.field_id)
                    .unwrap();
                MvObservedStorageMapping {
                    output_ordinal: request.output_ordinal,
                    field_id: request.field_id.clone(),
                    logical_type: request.logical_type.clone(),
                    storage_type: field.source_type().clone(),
                    storage_nullable: field.nullable(),
                    declaration: MvStorageColumnDeclaration {
                        read_type: request.logical_type.clone(),
                        write_type: request.logical_type.clone(),
                        provider_declaration: Bytes::from_static(b"fixture-storage"),
                        storage_format_version: 1,
                        encode_identity: Bytes::from_static(b"fixture-encode"),
                        decode_identity: Bytes::from_static(b"fixture-decode"),
                        content_contract: Bytes::from_static(b"fixture-content"),
                    },
                    pushdown: MvStoragePushdownClass::Identity,
                }
            })
            .collect();
        Ok(MvCurrentStorageMapping {
            target_object: self.current.view.object_id.clone(),
            target_version: self.current.view.metadata_version.clone(),
            columns,
        })
    }

    fn observe_source_schema(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvSourceSchemaObservation, ConnectorError> {
        let (initial, snapshot) = match metadata.table.payload().as_ref() {
            b"original-source-18" => (true, 18),
            b"prepared-source-to-18" => (false, 18),
            b"prepared-source-from-17" => (false, 17),
            _ => panic!("source schema must project the original pin or a retained snapshot"),
        };
        self.schema_projections.fetch_add(1, Ordering::SeqCst);
        novarocks_spi::connector::MvSourceSchemaObservation::try_new(
            metadata.identity.clone(),
            if initial {
                self.initial_object()
            } else {
                self.object()
            },
            metadata.version.clone().unwrap(),
            novarocks_spi::connector::MvSourceFormatVersion::V3,
            Some(snapshot),
            true,
            self.observed_source_fields(initial),
            &context,
        )
    }
    fn observe_refresh_base(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvRefreshBaseObservation, ConnectorError> {
        assert_eq!(metadata.identity, self.source_identity());
        novarocks_spi::connector::MvRefreshBaseObservation::try_new(
            metadata.identity.clone(),
            self.initial_object(),
            Some(self.snapshot),
            &context,
        )
    }
    fn observe_schema_validation(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvSchemaValidationObservation, ConnectorError> {
        let target = &self.projection.facts.interpretation().target;
        novarocks_spi::connector::MvSchemaValidationObservation::try_new(
            metadata.identity.clone(),
            self.projection
                .facts
                .source_revision()
                .target_object_id
                .clone(),
            self.projection.facts.metadata_version().clone(),
            Bytes::copy_from_slice(target.schema_version.as_bytes()),
            Bytes::copy_from_slice(target.partition_spec_version.as_bytes()),
            true,
            self.target_fields(),
            vec![],
            &context,
        )
    }
    fn observe_refresh_target(
        &self,
        _: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvRefreshTargetObservation, ConnectorError> {
        let fields = self
            .target_fields()
            .iter()
            .map(|(_, v)| i32::from(v.provider_field_id()[0]))
            .collect();
        novarocks_spi::connector::MvRefreshTargetObservation::try_new(
            metadata.identity.clone(),
            "original-target".into(),
            3,
            MvObservedPartitionSpec::new(0, vec![]),
            Some(11),
            BTreeMap::from([("main".into(), 11)]),
            fields,
            vec![11],
            BTreeMap::new(),
            &context,
        )
    }
    fn observe_created_target(
        &self,
        _: &ConnectorControlPlanningPin,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvCreatedTargetObservation, ConnectorError> {
        panic!("EXPLAIN cannot create target")
    }
    fn observe_maintenance_metadata(
        &self,
        _: &ConnectorControlPlanningPin,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        panic!("no maintenance during EXPLAIN")
    }
}
impl StatisticsReader for SourceFixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }

    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        assert!(request.row_mutation_current.is_none() && request.narrowed_read.is_none());
        let retention = request
            .observed_source
            .as_ref()
            .expect("original statistics pin");
        retention.validate_owner(&ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.epoch,
        })?;
        let snapshot = **retention.value::<i64>(self.descriptor.instance_id.as_str())?;
        let (rows, bytes) = match (snapshot, request.table.payload().as_ref()) {
            (17, b"prepared-source-from-17") => (1, 128),
            (18, b"prepared-source-to-18" | b"original-source-18") => (2, 256),
            (11, b"original-target-11") => (
                self.current
                    .publication
                    .as_ref()
                    .and_then(|publication| publication.logical_result_rows)
                    .expect("fixture target row count"),
                128,
            ),
            _ => panic!("statistics do not describe the retained snapshot {snapshot}"),
        };
        let data_version = Self::statistics_version(snapshot);
        assert_eq!(request.data_version, data_version);
        let metrics = request
            .metrics
            .metrics()
            .iter()
            .map(|metric| {
                let count = match metric {
                    StatisticsMetric::RowCount => rows,
                    StatisticsMetric::DataFileBytes => bytes,
                    _ => panic!("unexpected manifest volume metric {metric:?}"),
                };
                (
                    metric.clone(),
                    StatisticsMetricState::Available(StatisticsMetricObservation::new(
                        StatisticsMetricValue::U64(count),
                        data_version.clone(),
                        StatisticsMetricSource::CurrentManifest,
                        StatisticsNumericNature::Exact,
                        StatisticsBasisRelation::Identical,
                    )),
                )
            })
            .collect();
        StatisticsEvidence::try_new(
            data_version.clone(),
            StatisticsEvidenceRevision::try_new(data_version.as_bytes().clone())?,
            StatisticsRowCoverage::AllVisibleRows,
            metrics,
        )
    }

    fn read_statistics_async(
        &self,
        request: StatisticsReadRequest,
    ) -> futures::future::BoxFuture<'_, Result<StatisticsEvidence, ConnectorError>> {
        Box::pin(async move { self.read_statistics(request) })
    }
}

impl ConnectorStatistics for SourceFixture {}

impl ConnectorDocumentStorageObservation for SourceFixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn observe_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        assert_eq!(request.target, self.current.view.table);
        assert_eq!(request.owner, self.current.view.owner);
        assert_eq!(request.object_id, self.current.view.object_id);
        assert_eq!(request.catalog_handle, self.catalog);
        Ok(self.current.view.clone())
    }
    fn observe_keyed_documents_async(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> futures::future::BoxFuture<'_, Result<KeyedDocumentView, KeyedDocumentReadError>> {
        Box::pin(async move { self.observe_keyed_documents(request) })
    }
    fn observe_full_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        panic!("no recovery in stable EXPLAIN")
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        panic!("EXPLAIN has no effect")
    }
    fn resolve_keyed_commit(
        &self,
        _: KeyedDocumentObservationRequest,
        _: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        panic!("EXPLAIN has no commit")
    }
    fn discover_documents(
        &self,
        _: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        panic!("no discovery")
    }
}

async fn seed(fixture: &mut Fixture, source: &SourceFixture) {
    let state = fixture.state.as_mut().unwrap();
    let store = state.store.as_ref().unwrap();
    let requests = Arc::new(
        store
            .execution_requests(state.binding.transaction_deadline, None)
            .unwrap(),
    );
    let readiness = fixture
        .readiness_service
        .as_ref()
        .unwrap()
        .in_requests(requests);
    let mut future =
        Box::pin(readiness.seed_projection(uuid::Uuid::now_v7(), source.projection.facts.clone()));
    tokio::time::timeout(
        Duration::from_secs(5),
        with_original_driver(
            &fixture.control,
            &mut fixture.driver,
            std::future::poll_fn(|cx| {
                store.poll_progress(cx).unwrap();
                state.io.as_ref().unwrap().poll_continuing(cx).unwrap();
                future.as_mut().poll(cx)
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn original_explain_union_occurrences_prepare_once_across_a_and_b_without_splits() {
    explain_counted(false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_explain_self_join_binds_one_window_for_both_occurrences() {
    explain_counted(true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn background_composed_generation_prepares_actual_source_read_once() {
    explain_counted(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_explain_classifies_initial_definition_drift_without_preparing_frozen_to() {
    for (drift, expected) in [
        (
            InitialSourceSchema::MissingBoundField,
            "MV refresh: InvalidDefinition",
        ),
        (
            InitialSourceSchema::ChangedBoundType,
            "MV refresh: InvalidDefinition",
        ),
        (
            InitialSourceSchema::ReplacedObject,
            "MV refresh: NeedsRebuild",
        ),
    ] {
        let mut source = SourceFixture::new(false, false);
        Arc::get_mut(&mut source).unwrap().initial_schema = drift;
        let mut fixture = Fixture::with_source_explain(
            Some(novarocks_sql::compiler::ExplainLevel::Costs),
            Some(source.clone()),
        )
        .await;
        seed(&mut fixture, &source).await;
        let result = fixture.business().await;
        let rows = match result {
            Ok(StatementActionValue::Local(rows)) => rows,
            Err(error) => panic!("definition drift must render EXPLAIN for {drift:?}: {error:?}"),
            _ => panic!("definition drift must render local rows for {drift:?}"),
        };
        let state = fixture.state.as_ref().unwrap();
        assert!(
            state.local_health().is_none(),
            "definition refusal is not Native health"
        );
        let mut cursor = rows.into_cursor();
        let mut output = [0; 4096];
        let rendered = cursor.step(&mut output).unwrap();
        assert!(rendered.completed_rows > 0);
        let first_row_len = u32::from_le_bytes(output[..4].try_into().unwrap()) as usize;
        let first_row: Vec<String> = serde_json::from_slice(&output[4..4 + first_row_len]).unwrap();
        assert_eq!(first_row, [expected], "{drift:?}");
        assert_eq!(source.initial_observations.load(Ordering::SeqCst), 1);
        assert_eq!(source.preparations.load(Ordering::SeqCst), 0);
        assert!(source.requests.lock().unwrap().is_empty());
        drop(cursor);
        fixture.close(false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_explain_uses_initial_schema_for_admission_and_frozen_to_for_reads() {
    explain_counted_with_initial_schema(false, false, InitialSourceSchema::UnrelatedField).await;
}

async fn explain_counted(self_join: bool, background_composition: bool) {
    explain_counted_with_initial_schema(
        self_join,
        background_composition,
        InitialSourceSchema::Original,
    )
    .await;
}
async fn explain_counted_with_initial_schema(
    self_join: bool,
    background_composition: bool,
    initial_schema: InitialSourceSchema,
) {
    let mut source = SourceFixture::new(self_join, background_composition);
    Arc::get_mut(&mut source).unwrap().initial_schema = initial_schema;
    let mut fixture = Fixture::with_source_explain(
        Some(novarocks_sql::compiler::ExplainLevel::Costs),
        Some(source.clone()),
    )
    .await;
    seed(&mut fixture, &source).await;
    let result = fixture.business().await;
    let state = fixture.state.as_ref().unwrap();
    assert!(state.local_health().is_none());
    let explain = state.explain.as_ref().unwrap();
    match explain.result.as_ref() {
        Some(Ok(MvRefreshPreparationOutput::Explain(_))) => {}
        Some(Err(error)) => panic!("original EXPLAIN preparation failed: {error}"),
        _ => panic!("original EXPLAIN did not return its rendered plan"),
    }
    let Ok(StatementActionValue::Local(rows)) = &result else {
        panic!("original EXPLAIN must transfer its rendered Local result")
    };
    assert!(rows.row_count() > 0);
    assert_eq!(
        rows.columns().map(|column| column.name).collect::<Vec<_>>(),
        ["Explain String"]
    );
    assert_eq!(
        source.initial_observations.load(Ordering::SeqCst),
        1,
        "actual source metadata callback must pin the object once"
    );
    assert_eq!(
        source.preparations.load(Ordering::SeqCst),
        1,
        "A, B and each occurrence must reuse one actual public-adapter preparation callback"
    );
    assert!(
        source.schema_projections.load(Ordering::SeqCst) >= 2,
        "each D occurrence must consume To schema"
    );
    // EXPLAIN reached B's final exact overlays; A's production policy helper
    // is a different caller. Exercise that actual helper against this SAME
    // original gate/receipt, once for each original D occurrence.
    let from = source.requests.lock().unwrap()[0]
        .from_revision()
        .unwrap()
        .clone();
    let context = source.captured_context.lock().unwrap().take().unwrap();
    let table = novarocks_types::naming::TableIdentity::new("ice", "sales", "orders");
    if initial_schema == InitialSourceSchema::UnrelatedField {
        let io = explain.reader.original_io_for_test();
        let captured = io.captured_sources().require(&table).unwrap();
        assert!(
            captured
                .observed
                .schema()
                .field_with_name("unrelated_current")
                .is_ok()
        );
        assert!(
            captured
                .metadata()
                .unwrap()
                .schema()
                .field_with_name("unrelated_current")
                .is_err()
        );
        // The same fake Provider projects the retained source facts; this call
        // cannot load metadata or start SourceRead work (both callbacks panic).
        let actual_read_schema = io
            .schema(fixture.explain_ports.as_ref().unwrap(), &table, &context)
            .await
            .unwrap();
        assert!(
            actual_read_schema
                .fields()
                .iter()
                .all(|field| field.name != "unrelated_current")
        );
    }
    let projections_before = source.schema_projections.load(Ordering::SeqCst);
    let mut occurrences = BTreeSet::new();
    for occurrence in &source.projection.facts.binding().occurrences {
        let (window_facts,observation)=crate::mv::domain::refresh::rewrite_context::project_prepared_change_window_for_table_original(
            explain.reader.original_io_for_test(),source.as_ref(),&table,&from,17,18,&context,
        ).unwrap();
        assert!(matches!(
            window_facts,
            ConnectorChangeWindowFacts::Difference {
                negative_weight: ConnectorChangeWindowNegativeWeight::Absent,
                ..
            }
        ));
        crate::mv::domain::refresh::schema_contract::validate_relation_occurrence_schema(
            occurrence,
            &observation,
        )
        .unwrap();
        assert!(occurrences.insert(occurrence.occurrence_id));
    }
    assert_eq!(occurrences, BTreeSet::from([0, 1]));
    assert_eq!(
        source.schema_projections.load(Ordering::SeqCst),
        projections_before + 2
    );
    assert_eq!(
        source.preparations.load(Ordering::SeqCst),
        1,
        "A may only project B's original completed receipt"
    );
    drop(context);
    {
        let history = source.historical_requests.lock().unwrap();
        let [read] = history.as_slice() else {
            panic!(
                "cost comparison must obtain one exact historical snapshot for the repeated table"
            );
        };
        assert_eq!(read.selection(), ConnectorReadSourceSelection::Snapshot(17));
        assert_eq!(read.to_revision(), &from);
        assert_eq!(read.object_id(), &source.object());
    }
    {
        let requests = source.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].from_revision().unwrap().canonical_read_point(),
            Some(ConnectorCanonicalReadPoint::Snapshot(Some(17)))
        );
        assert_eq!(
            requests[0].to_revision().canonical_read_point(),
            Some(ConnectorCanonicalReadPoint::Snapshot(Some(18)))
        );
    }
    drop(result);
    fixture.close(false).await;
}
