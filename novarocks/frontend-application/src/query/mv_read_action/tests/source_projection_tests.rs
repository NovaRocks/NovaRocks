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
    codec::{ApplyKeyKind, ExpressionKind, PhysicalFieldLogicalIdentity},
    identity::FieldIdentity,
    keyed_documents::{MvKeyedObservedCurrentDocuments, StoredKeyedMvProjection},
    test_support::{KeyedProjectionFixture, observed_current_keyed},
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
    schema_projections: AtomicUsize,
    revision_selectors: Mutex<Vec<ConnectorReadSelector>>,
    captured_context: Mutex<Option<ConnectorRequestContext>>,
    snapshot: i64,
    background_composition: bool,
    initial_schema: InitialSourceSchema,
}
/// Use only the existing row-level projection capability. The pure keyed
/// fixture supplies durable identities; SQL owns internal target labels/types.
fn row_projection(
    fixture: &mut KeyedProjectionFixture,
    self_join: bool,
) -> BTreeMap<FieldIdentity, String> {
    use novarocks_sql::planning::mv::{SqlMvInternalTargetColumn, mv_internal_target_column};
    use novarocks_types::schema::SqlType;
    let field = |id| FieldIdentity::try_new(vec![id]).unwrap();
    fixture.definition.query.effective_sql = if self_join {
        "SELECT o.amount AS amount FROM ice.sales.orders o JOIN ice.sales.orders o2 ON o.order_id=o2.order_id".into()
    } else {
        "SELECT o.amount AS amount FROM ice.sales.orders o UNION ALL SELECT o2.amount AS amount FROM ice.sales.orders o2".into()
    };
    for occurrence in &mut fixture.definition.relation_occurrences {
        occurrence
            .fields
            .iter_mut()
            .find(|v| v.name_at_binding == "amount")
            .unwrap()
            .type_signature = "double".into();
    }
    let output = &mut fixture.definition.outputs[0];
    output.name = "amount".into();
    output.type_signature = "double".into();
    output.expression.kind = ExpressionKind::Field;
    output.expression.function_identity = None;
    if self_join {
        output
            .expression
            .source_fields
            .retain(|v| v.occurrence_id == 7);
        fixture.interpretation.branches.clear();
    }
    fixture.interpretation.outputs[0].type_signature = "double".into();
    fixture.interpretation.aggregates.clear();
    fixture.interpretation.state_slots.clear();
    fixture.interpretation.apply_key.kind = if self_join {
        ApplyKeyKind::JoinRowKey
    } else {
        ApplyKeyKind::BaseRowId
    };
    let apply = mv_internal_target_column(if self_join {
        SqlMvInternalTargetColumn::JoinApplyKey
    } else {
        SqlMvInternalTargetColumn::ApplyKey
    });
    assert!(!apply.nullable);
    let apply_type = match apply.data_type {
        SqlType::BigInt => "bigint",
        SqlType::String => "string",
        other => panic!("unexpected projection apply-key type {other:?}"),
    };
    fixture
        .interpretation
        .target
        .fields
        .retain_mut(|binding| match &binding.logical_identity {
            PhysicalFieldLogicalIdentity::Output(_) => {
                binding.type_signature = "double".into();
                true
            }
            PhysicalFieldLogicalIdentity::ApplyKey(_) => {
                binding.type_signature = apply_type.into();
                binding.nullable = apply.nullable;
                true
            }
            PhysicalFieldLogicalIdentity::Branch(_) => !self_join,
            PhysicalFieldLogicalIdentity::State(_) => false,
        });
    let mut names = BTreeMap::from([(field(31), "amount".into()), (field(34), apply.name)]);
    if !self_join {
        let branch = mv_internal_target_column(SqlMvInternalTargetColumn::BranchId);
        assert_eq!(branch.data_type, SqlType::Int);
        assert!(!branch.nullable);
        names.insert(field(35), branch.name);
    }
    for binding in &fixture.interpretation.target.fields {
        assert!(names.contains_key(&binding.target_field_id));
    }
    assert!(fixture.interpretation.aggregates.is_empty());
    assert!(fixture.interpretation.state_slots.is_empty());
    names
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
        Arc::new(Self {
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
            schema_projections: AtomicUsize::new(0),
            revision_selectors: Mutex::new(Vec::new()),
            captured_context: Mutex::new(None),
            snapshot: 18,
            background_composition,
            initial_schema: InitialSourceSchema::Original,
        })
    }
    fn object(&self) -> ConnectorTableObjectId {
        novarocks_mv_application::persistence::exact_revision::restore_persisted_object(
            &self.projection.facts.definition().relation_occurrences[0].object_id,
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
        self.projection.facts.definition().relation_occurrences[0]
            .fields
            .iter()
            .map(|v| {
                MvObservedSourceField::try_new(
                    Bytes::copy_from_slice(v.field_id.as_bytes()),
                    v.name_at_binding.clone(),
                    v.type_signature.clone(),
                    v.nullable,
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
                        "bigint".to_owned(),
                        amount.nullable(),
                    )
                    .unwrap();
                }
                InitialSourceSchema::UnrelatedField => fields.push(
                    MvObservedSourceField::try_new(
                        Bytes::from_static(b"unrelated-current-field"),
                        "unrelated_current".to_owned(),
                        "bigint".to_owned(),
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
                        v.type_signature.clone(),
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
        self.metadata_for_projection(source, true, creation)
    }
    fn metadata_for_projection(
        &self,
        source: bool,
        initial: bool,
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
                        match v.type_signature() {
                            "decimal(18,2)" => DataType::Decimal128(18, 2),
                            "decimal(38,2)" => DataType::Decimal128(38, 2),
                            "double" => DataType::Float64,
                            "string" => DataType::Utf8,
                            "bigint" => DataType::Int64,
                            "binary" => DataType::Binary,
                            "integer" => DataType::Int32,
                            other => panic!("undeclared target type {other}"),
                        },
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
        let facts = ConnectorTablePlanningFacts::try_new_snapshot_source(
            &schema,
            schema
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
                .collect(),
            vec![],
            vec![],
            &context,
        )
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
                statistics_data_version: None,
                table: novarocks_spi::connector::ConnectorTableHandle::try_new(
                    self.descriptor.instance_id.clone(),
                    Bytes::from_static(if source && initial {
                        b"original-source-18"
                    } else if source {
                        b"prepared-source-to-18"
                    } else {
                        b"original-target-11"
                    }),
                )
                .unwrap(),
            },
            schema,
            types,
            creation,
        )?;
        Ok(if source {
            result.with_source_read_facts(ConnectorProcessLocalPayload::new(
                ConnectorProviderBindingKey {
                    instance_id: self.descriptor.instance_id.clone(),
                    incarnation: self.epoch,
                },
                Arc::new(self.snapshot),
            ))
        } else {
            result
        })
    }
    pub(super) fn ports(
        self: &Arc<Self>,
        readiness: Arc<MvReadinessPort>,
        entrance: Arc<ManagementEntrance>,
    ) -> IcebergMvCorePorts {
        let template = novarocks_catalog_application::test_support::test_control_binding_for(
            self.descriptor.instance_id.clone(),
            self.epoch.to_bytes()[0],
        );
        let control = ConnectorControlBinding::try_new(
            self.descriptor.clone(),
            self.epoch,
            self.clone(),
            template.planning().clone(),
            template.execution_distribution().clone(),
            None,
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
        if self.background_composition {
            // Exercise the same composition factory used by the production
            // background bindings, then the real SourceRead/public adapter path.
            return crate::capabilities::MvBackgroundPorts::new(
                Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
                None,
                host.clone(),
                readiness,
                self.clone(),
                entrance,
                crate::application::test_constant_policy(),
            )
            .into_core_ports(host);
        }
        IcebergMvCorePorts::new_with_management_entrance(
            Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
            Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
            None,
            host.clone(),
            host,
            readiness,
            self.clone(),
            entrance,
            crate::application::test_constant_policy(),
        )
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
            b"original-source-18" | b"prepared-source-to-18" => true,
            b"original-target-11" => false,
            _ => panic!("unknown original table revision request"),
        };
        if source {
            self.revision_selectors.lock().unwrap().push(selector);
        }
        let snapshot = match selector {
            ConnectorReadSelector::Current => {
                if source {
                    self.snapshot
                } else {
                    11
                }
            }
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
            b"original-source-18" | b"prepared-source-to-18" => true,
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
    type FrozenSource = ();
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
            assert!(
                matches!(request.selection(),ConnectorReadSourceSelection::ChangeWindow(w) if w==ConnectorReadChangeWindow::new(17,18))
            );
            assert_eq!(
                request.from_revision().unwrap().canonical_read_point(),
                Some(ConnectorCanonicalReadPoint::Snapshot(Some(17)))
            );
            assert_eq!(
                request.to_revision().canonical_read_point(),
                Some(ConnectorCanonicalReadPoint::Snapshot(Some(18)))
            );
            self.preparations.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request.clone());
            let context = crate::connector::test_request_context();
            let metadata =
                crate::connector::schema_creation::with_schema_creation(&context, |creation| {
                    self.metadata_for_projection(true, false, creation)
                })
                .map_err(crate::connector::schema_creation::connector_schema_error)?;
            Ok(ProviderPreparedSourceRead::change_window(
                (),
                ConnectorChangeWindowAdmission::Incremental {
                    has_inserts: true,
                    has_deletes: false,
                    partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
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
    fn get_table_execute_plan(
        &self,
        _: &ConnectorSession,
        _: &SchemaTableName,
        _: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<()>, ConnectorError> {
        panic!("EXPLAIN cannot open a table procedure")
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
    fn observe_source_schema(
        &self,
        _: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::MvSourceSchemaObservation, ConnectorError> {
        let initial = match metadata.table.payload().as_ref() {
            b"original-source-18" => true,
            b"prepared-source-to-18" => false,
            _ => panic!("source schema must project the original pin or its prepared To"),
        };
        self.schema_projections.fetch_add(1, Ordering::SeqCst);
        novarocks_spi::connector::MvSourceSchemaObservation::try_new(
            metadata.identity.clone(),
            if initial {
                self.initial_object()
            } else {
                self.object()
            },
            Bytes::from_static(&[1]),
            true,
            true,
            self.observed_source_fields(initial),
            &context,
        )
    }
    fn observe_refresh_base(
        &self,
        _: &ConnectorControlPlanningLease,
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
        _: &ConnectorControlPlanningLease,
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
            true,
            self.target_fields(),
            vec![],
            &context,
        )
    }
    fn observe_refresh_target(
        &self,
        _: &ConnectorControlPlanningLease,
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
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvCreatedTargetObservation, ConnectorError> {
        panic!("EXPLAIN cannot create target")
    }
    fn observe_maintenance_metadata(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        panic!("no maintenance during EXPLAIN")
    }
}
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
async fn original_explain_self_join_binds_one_window_before_existing_join_key_refusal() {
    explain_counted(true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn background_composed_generation_prepares_actual_source_read_once() {
    explain_counted(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn original_explain_rejects_initial_definition_drift_before_preparing_frozen_to() {
    for (drift, expected) in [
        (
            InitialSourceSchema::MissingBoundField,
            "MV source field is missing for relation occurrence 7",
        ),
        (
            InitialSourceSchema::ChangedBoundType,
            "MV source field type or nullability changed for relation occurrence 7",
        ),
        (
            InitialSourceSchema::ReplacedObject,
            "MV source object changed for relation occurrence 7",
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
        let error = result
            .as_ref()
            .err()
            .expect("initial D drift must reject EXPLAIN");
        assert!(error.message().contains(expected), "{drift:?}: {error:?}");
        let state = fixture.state.as_ref().unwrap();
        assert!(
            state.local_health().is_none(),
            "definition refusal is not Native health"
        );
        assert!(matches!(
            state.explain.as_ref().unwrap().result,
            Some(Err(_))
        ));
        assert_eq!(source.initial_observations.load(Ordering::SeqCst), 1);
        assert_eq!(source.preparations.load(Ordering::SeqCst), 0);
        assert!(source.requests.lock().unwrap().is_empty());
        drop(result);
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
    if self_join {
        // B8 already owns the missing installed join-key implementation. This
        // test proves the preceding window/binding contract, not join execution.
        let error = result
            .as_ref()
            .err()
            .expect("original B8 capability refusal");
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.analyze.unavailable_implementation"
        );
        assert!(
            error.message().contains(
                "builtin.scalar/join_row_key/(binary,i64,binary,i64)->utf8;strict;legacy"
            )
        );
        assert!(matches!(explain.result, Some(Err(_))));
    } else {
        assert!(matches!(
            explain.result,
            Some(Ok(MvRefreshPreparationOutput::Explain(_)))
        ));
        let Ok(StatementActionValue::Local(rows)) = &result else {
            panic!("original EXPLAIN must transfer its rendered Local result")
        };
        assert!(rows.row_count() > 0);
        assert_eq!(
            rows.columns().map(|column| column.name).collect::<Vec<_>>(),
            ["Explain String"]
        );
    }
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
        let ports = source.ports(state.readiness.clone(), state.entrance.clone());
        let actual_read_schema = io.schema(&ports, &table, &context).await.unwrap();
        assert!(
            actual_read_schema
                .fields()
                .iter()
                .all(|field| field.name != "unrelated_current")
        );
    }
    let projections_before = source.schema_projections.load(Ordering::SeqCst);
    let mut occurrences = BTreeSet::new();
    for occurrence in &source.projection.facts.definition().relation_occurrences {
        let (admission,observation)=crate::mv::domain::refresh::rewrite_context::project_prepared_change_window_for_table_original(
            explain.reader.original_io_for_test(),source.as_ref(),&table,&from,17,18,&context,
        ).unwrap();
        assert!(matches!(
            admission,
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: true,
                has_deletes: false,
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
    assert_eq!(occurrences, BTreeSet::from([7, 8]));
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
    {
        let selectors = source.revision_selectors.lock().unwrap();
        assert!(
            selectors.contains(&ConnectorReadSelector::SnapshotId(17)),
            "B must validate the actual original From exact revision"
        );
        assert!(
            selectors.contains(&ConnectorReadSelector::SnapshotId(18)),
            "B must validate the actual frozen To exact revision"
        );
    }
    assert_eq!(result.is_ok(), !self_join);
    drop(result);
    fixture.close(false).await;
}
