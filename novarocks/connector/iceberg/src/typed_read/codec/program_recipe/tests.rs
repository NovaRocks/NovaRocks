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

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use arrow::datatypes::{DataType, FieldRef, Schema as ArrowSchema};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorEnvelopeHeader, ConnectorExpression,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
    ConnectorReadDistribution, ConnectorReadInputVersion, ConnectorReadMetadataKind,
    ConnectorReadProgramCompileError, ConnectorReadProgramRecipe, ConnectorReadProperties,
    ConnectorReadPublicFacts, ConnectorReadRelationKind, ConnectorReadRelationPayload,
    ConnectorReadStaticFacts, ConnectorValue, ConnectorValueType, FrozenConnectorScan,
    ScanColumnId, StaticScanAssignment, StaticScanDynamicFilter, TupleDomain,
    connector_type_for_value_type,
};
use novarocks_spi::connector::read_stack::{ConnectorTableHandle as _, SchemaTableName};
use novarocks_type_contract::{CompileControlError, FunctionValueType};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::super::program_fields::test_ingress::source_field as fixture_source_field;
use super::*;
use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};
use crate::provider_types::IcebergReadView;
use crate::typed_read::read_static_facts::iceberg_final_static_facts;
use crate::typed_read::table_handle::tests::{
    partitioned_handle, partitioned_schema, table_handle_params,
};
use crate::typed_read::{
    HiveTransactionHandle, IcebergChangeWindowHandle, IcebergChangeWindowHandleParams,
    IcebergColumnHandle, IcebergInsertTableHandle, IcebergInsertTableHandleParams,
    IcebergMergeTableHandle, IcebergRewriteArtifactContentId,
    IcebergRewritePositionDeleteFilesHandle, IcebergSystemTableReference,
    IcebergSystemTableReferenceParams, IcebergSystemTableType, IcebergTableExecuteHandle,
    IcebergTableExecuteHandleParams, IcebergTableExecuteProcedureHandle, IcebergTableHandle,
    REWRITE_POSITION_DELETE_OUTPUT_COLUMNS, TableChangesFunctionHandle,
    TableChangesFunctionHandleParams, change_op_column_handle,
};

#[derive(Clone)]
struct Fixture {
    relation: IcebergRuntimeRelation,
    columns: Vec<IcebergColumnHandle>,
    fields: Vec<FieldRef>,
    source: ConnectorReadStaticFacts<ScanColumnId>,
    domains: BTreeMap<usize, novarocks_type_contract::ValueLogicalType>,
    published_source: Option<novarocks_connector_contract::ConnectorSourceSchemaFacts>,
    published_bindings: Option<Vec<FunctionValueType>>,
    metadata: Option<ConnectorReadMetadataKind>,
    work_source: ConnectorReadWorkSource,
}

fn source_for(relation: &IcebergRuntimeRelation) -> ConnectorReadStaticFacts<ScanColumnId> {
    let actual = iceberg_final_static_facts(relation).unwrap();
    assert!(actual.properties().ordering().is_empty());
    let distribution = match actual.properties().distribution() {
        ConnectorReadDistribution::Unconstrained => ConnectorReadDistribution::Unconstrained,
        ConnectorReadDistribution::Singleton => ConnectorReadDistribution::Singleton,
        _ => panic!("fixture must project the actual declared provider guarantee"),
    };
    ConnectorReadStaticFacts::try_new(
        actual.input_version().clone(),
        actual.selection_digest(),
        ConnectorReadProperties::try_new(distribution, vec![]).unwrap(),
        actual.artifact_coverage().clone(),
        actual.coverage_evidence().to_vec(),
    )
    .unwrap()
}

fn column_field(column: &IcebergColumnHandle) -> FieldRef {
    let ty = crate::typed_read::column_handle::parse_type(column.type_json(), "fixture").unwrap();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::new(
            column.base_field_id(),
            column.base_column_identity().name(),
            ty,
            !column.nullable(),
        ))])
        .build()
        .unwrap();
    let arrow = crate::schema_mapping::sql_read_schema_from_iceberg(&schema).unwrap();
    Arc::clone(&arrow.fields()[0])
}

impl Fixture {
    fn table() -> Self {
        let handle = partitioned_handle();
        let schema = handle.parse_table_schema().unwrap();
        let columns = [2, 1, 2]
            .into_iter()
            .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
            .collect::<Vec<_>>();
        let relation = IcebergRuntimeRelation::Table(handle);
        Self {
            source: source_for(&relation),
            domains: BTreeMap::new(),
            published_source: None,
            published_bindings: None,
            fields: columns.iter().map(column_field).collect(),
            relation,
            columns,
            metadata: None,
            work_source: ConnectorReadWorkSource::RuntimeSplits,
        }
    }
    // This fixture authors separate source facts and complete values explicitly.
    // Arrow format cells are fixture data for IDs/defaults/attributes only;
    // they never select any semantic value domain.
    fn complete_public_parts(
        &self,
    ) -> (
        novarocks_connector_contract::ConnectorReadExecutionSchema,
        novarocks_connector_contract::ConnectorSourceSchemaFacts,
    ) {
        use novarocks_connector_contract::*;
        use novarocks_type_contract::{ValueLayout, ValueLogicalType, ValueTypeStorageLifetime};
        let storage = ValueTypeStorageLifetime::pure();
        let mut facts = Vec::new();
        let source_fields = self
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                fixture_source_field(field, &mut vec![ordinal as u32], &mut facts)
            })
            .collect::<Vec<_>>();
        let shape = ConnectorSourceSchemaShape::try_new(
            Arc::new(ArrowSchema::new(source_fields)),
            storage.clone(),
        )
        .unwrap();
        // The fixture is this map's actual fresh creator; no growth/removal
        // occurs before handing off its source iteration work provenance.
        let mut root_attributes = HashMap::with_capacity(1);
        root_attributes.insert("fixture.root".to_owned(), "preserved".to_owned());
        let root_invoice = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
            String,
            String,
        >(1)
        .unwrap()
        .request_bytes_upper_bound
            + "fixture.root".len()
            + "preserved".len();
        let source = ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
            shape,
            root_attributes,
            facts.into_boxed_slice(),
            storage.clone(),
            root_invoice,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
        .unwrap();
        let bindings = self
            .fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let declared = fixture_creation(|creation| {
                    let ty = crate::typed_read::column_handle::parse_type(
                        self.columns[ordinal].type_json(),
                        "fixture",
                    )
                    .unwrap();
                    crate::schema_mapping::typed_value::from_declaration(
                        &ty,
                        field.is_nullable(),
                        None,
                        self.columns[ordinal].scalar_integer_domain(),
                        creation,
                    )
                    .unwrap()
                    .binding
                });
                let layout = ValueLayout::try_new(field.data_type(), storage.clone()).unwrap();
                if let Some(original) = self
                    .published_bindings
                    .as_ref()
                    .and_then(|bindings| bindings.get(ordinal))
                {
                    if original.data_type() == layout.data_type() {
                        return original.with_nullable(field.is_nullable()).unwrap();
                    }
                }
                let domain = self
                    .domains
                    .get(&ordinal)
                    .copied()
                    .or_else(|| match &self.relation {
                        IcebergRuntimeRelation::SystemTable(reference) => {
                            Some(crate::typed_read::system_page_source::system_column_domain(
                                reference.system_table_type(),
                                field.name(),
                            ))
                        }
                        _ => None,
                    });
                if let Some(domain) = domain {
                    FunctionValueType::try_with_logical_type(
                        layout.data_type().clone(),
                        field.is_nullable(),
                        domain,
                    )
                    .unwrap()
                } else if declared.data_type() == layout.data_type() {
                    declared
                } else {
                    FunctionValueType::try_with_logical_type(
                        layout.data_type().clone(),
                        field.is_nullable(),
                        ValueLogicalType::Physical,
                    )
                    .unwrap()
                }
            })
            .collect::<Vec<_>>();
        let fields = self
            .fields
            .iter()
            .zip(&bindings)
            .map(|(field, ty)| {
                Arc::new(arrow::datatypes::Field::new(
                    field.name(),
                    ty.data_type().clone(),
                    ty.nullable(),
                ))
            })
            .collect::<Vec<_>>();
        let execution = ConnectorReadExecutionSchema::try_new(
            Arc::new(ArrowSchema::new(fields)),
            bindings.into_boxed_slice(),
            storage,
        )
        .unwrap();
        (execution, self.published_source.clone().unwrap_or(source))
    }
    fn build(&self) -> FrozenConnectorRead {
        self.build_modified(None, None, None)
    }
    fn build_modified(
        &self,
        kind: Option<ConnectorReadRelationKind>,
        table_bytes: Option<bytes::Bytes>,
        revision: Option<u32>,
    ) -> FrozenConnectorRead {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
        );
        let codecs = IcebergReadTypes::wire_codecs();
        let payload = |category, bytes| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(
                        revision.unwrap_or(ICEBERG_READ_CODEC_REVISION),
                    )
                    .unwrap(),
                ),
                bytes,
            )
        };
        let recipe = ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            ConnectorReadRelationPayload::new(
                kind.unwrap_or(self.relation.kind()),
                payload(
                    ConnectorCodecCategory::ReadTable,
                    table_bytes.unwrap_or_else(|| codecs.encode_table(&self.relation).unwrap()),
                ),
                payload(
                    ConnectorCodecCategory::ReadView,
                    codecs
                        .encode_read_view(&IcebergReadView::new(HiveTransactionHandle::new(
                            true, [3; 16],
                        )))
                        .unwrap(),
                ),
            ),
            self.columns
                .iter()
                .map(|column| {
                    payload(
                        ConnectorCodecCategory::ReadColumn,
                        codecs.encode_column(column).unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap();
        let (execution, source_schema) = self.complete_public_parts();
        let assignments = execution
            .bindings()
            .iter()
            .enumerate()
            .map(|(ordinal, ty)| {
                StaticScanAssignment::new(
                    Arc::from(format!("output_{ordinal}")),
                    connector_type_for_value_type(ty).unwrap(),
                )
            })
            .collect();
        let remaining = ConnectorExpression::Constant {
            value: Some(ConnectorValue::Boolean(true)),
            value_type: ConnectorValueType::Boolean,
        };
        let scan = FrozenConnectorScan::try_new(
            recipe,
            assignments,
            TupleDomain::all(),
            TupleDomain::all(),
            Some(remaining),
            vec![StaticScanDynamicFilter::new(901, Arc::from("output_0"))],
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            self.work_source,
        )
        .unwrap();
        let public = ConnectorReadPublicFacts::try_new_typed(
            self.source.clone(),
            self.metadata.clone(),
            execution,
            source_schema,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        FrozenConnectorRead::try_new(scan, public).unwrap()
    }
}

struct Control {
    calls: Mutex<Vec<u32>>,
    failure: Option<(usize, CompileControlError)>,
    quantum: Option<CompileControlError>,
}
impl Control {
    fn accept() -> Self {
        Self {
            calls: Mutex::new(vec![]),
            failure: None,
            quantum: None,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        let mut calls = self.calls.lock().unwrap();
        calls.push(units);
        if let Some((at, error)) = self.failure
            && calls.len() == at
        {
            return Err(error);
        }
        if units == 256
            && let Some(error) = self.quantum
        {
            return Err(error);
        }
        Ok(())
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn assert_unchanged(frozen: &FrozenConnectorRead) {
    let result = ConnectorReadProgramRecipe::try_compile_with_provider(
        frozen,
        &IcebergReadRecipeCompiler,
        &Control::accept(),
    )
    .unwrap();
    assert_eq!(result.frozen(), frozen);
}
fn assert_provider(frozen: &FrozenConnectorRead, kind: ConnectorCodecErrorKind) {
    let error = ConnectorReadProgramRecipe::try_compile_with_provider(
        frozen,
        &IcebergReadRecipeCompiler,
        &Control::accept(),
    )
    .unwrap_err();
    assert!(
        matches!(error, ConnectorReadProgramCompileError::Provider(ref error) if error.kind() == kind),
        "unexpected error: {error:?}"
    );
}

#[test]
fn complete_table_preserves_reordered_repeated_columns_variables_predicates_and_rf() {
    let fixture = Fixture::table();
    let frozen = fixture.build();
    assert_eq!(
        fixture
            .columns
            .iter()
            .map(IcebergColumnHandle::base_field_id)
            .collect::<Vec<_>>(),
        vec![2, 1, 2]
    );
    assert_eq!(
        frozen
            .scan()
            .assignments()
            .iter()
            .map(|assignment| assignment.variable())
            .collect::<Vec<_>>(),
        vec!["output_0", "output_1", "output_2"]
    );
    assert_unchanged(&frozen);
}

fn system_fixture(kind: IcebergSystemTableType) -> Fixture {
    let schema = partitioned_schema();
    let specs = vec![PartitionSpec::unpartition_spec()];
    let output = crate::typed_read::system_relation_schema(kind, &schema, &specs).unwrap();
    let columns = crate::typed_read::system_relation_columns(kind, &schema, &specs).unwrap();
    let reference = IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
        schema_table_name: SchemaTableName::try_new("db", "t").unwrap(),
        system_table_type: kind,
        metadata_file_location: "s3://warehouse/db/t/metadata/v1.json".into(),
        table_uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".into(),
        snapshot_id: Some(11),
    })
    .unwrap();
    let relation = IcebergRuntimeRelation::SystemTable(reference);
    Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: output.fields().to_vec(),
        relation,
        columns,
        metadata: Some(ConnectorReadMetadataKind::try_new(kind.suffix()).unwrap()),
        work_source: if kind.produces_splits() {
            ConnectorReadWorkSource::RuntimeSplits
        } else {
            ConnectorReadWorkSource::WholeRelation
        },
    }
}
fn window_fixture() -> Fixture {
    let schema = partitioned_schema();
    let columns = [1, 2]
        .into_iter()
        .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
        .collect::<Vec<_>>();
    let handle = IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: SchemaTableName::try_new("db", "t").unwrap(),
        table_schema_json: serde_json::to_string(&schema).unwrap(),
        columns: columns.clone(),
        name_mapping_json: None,
        from_snapshot_id_exclusive: 10,
        to_snapshot_id_inclusive: 20,
        from_read_domain: crate::delete_semantics::test_read_domain(
            &schema,
            &[PartitionSpec::unpartition_spec()],
            10,
        ),
        to_read_domain: crate::delete_semantics::test_read_domain(
            &schema,
            &[PartitionSpec::unpartition_spec()],
            20,
        ),
        partition_spec_jsons: BTreeMap::from([(
            0,
            serde_json::to_string(&PartitionSpec::unpartition_spec()).unwrap(),
        )]),
    })
    .unwrap();
    let relation = IcebergRuntimeRelation::ChangeWindow(handle);
    let mut columns = columns;
    columns.push(change_op_column_handle().unwrap());
    Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: columns.iter().map(column_field).collect(),
        relation,
        columns,
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    }
}
fn rewrite_fixture() -> Fixture {
    let handle = partitioned_handle();
    let execute = IcebergTableExecuteHandle::try_new(IcebergTableExecuteHandleParams {
        schema_table_name: handle.schema_table_name().clone(),
        procedure_id: crate::typed_read::IcebergProcedureId::RewritePositionDeleteFiles,
        table_location: handle.table_location().into(),
        procedure_handle: Some(
            IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(
                IcebergRewritePositionDeleteFilesHandle::try_new(
                    handle,
                    IcebergRewriteArtifactContentId::try_new(
                        novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([1; 16]),
                        "12".repeat(32),
                    )
                    .unwrap(),
                    "34".repeat(32),
                )
                .unwrap(),
            ),
        ),
    })
    .unwrap();
    let relation = IcebergRuntimeRelation::TableExecute(execute);
    let columns = REWRITE_POSITION_DELETE_OUTPUT_COLUMNS
        .into_iter()
        .map(|(name, metadata)| {
            crate::typed_read::table_execute::rewrite_position_delete_pseudo_column(name, metadata)
                .unwrap()
        })
        .collect::<Vec<_>>();
    Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: columns.iter().map(column_field).collect(),
        relation,
        columns,
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    }
}

#[test]
fn complete_system_read_uses_actual_metadata_schema_kind_evidence_and_execution_mode() {
    for kind in [
        IcebergSystemTableType::Files,
        IcebergSystemTableType::Entries,
        IcebergSystemTableType::Snapshots,
        IcebergSystemTableType::History,
        IcebergSystemTableType::Refs,
        IcebergSystemTableType::Manifests,
        IcebergSystemTableType::Partitions,
    ] {
        let fixture = system_fixture(kind);
        assert_eq!(fixture.fields.len(), fixture.columns.len());
        assert!(fixture.fields.len() > 1);
        assert!(!fixture.source.coverage_evidence().is_empty());
        assert_unchanged(&fixture.build());
        assert_eq!(fixture.metadata.as_ref().unwrap().as_str(), kind.suffix());
    }
}

#[test]
fn complete_change_window_retains_exact_required_change_sign_and_endpoint_facts() {
    let fixture = window_fixture();
    let sign = fixture.fields.last().unwrap();
    assert_eq!(sign.name(), crate::typed_read::ICEBERG_CHANGE_OP_COLUMN);
    assert_eq!(sign.data_type(), &DataType::Int32);
    assert!(!sign.is_nullable());
    assert_unchanged(&fixture.build());
    let mut wrong = fixture.clone();
    wrong.fields[2] = Arc::new(sign.as_ref().clone().with_nullable(true));
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
}

#[test]
fn complete_table_execute_rewrite_retains_two_optional_pseudo_columns_and_exact_coverage() {
    let fixture = rewrite_fixture();
    assert_eq!(fixture.columns.len(), 2);
    assert!(fixture.fields.iter().all(|field| field.is_nullable()));
    assert!(matches!(
        fixture.source.artifact_coverage(),
        ConnectorReadArtifactCoverage::Exact { .. }
    ));
    assert_unchanged(&fixture.build());
    let mut wrong = fixture.clone();
    wrong.fields[0] = Arc::new(wrong.fields[0].as_ref().clone().with_nullable(false));
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
}

#[test]
fn codec_legal_table_function_and_merge_remain_unsupported_as_complete_static_reads() {
    let base = crate::typed_boundary::final_static_facts_tests::table_handle();
    let schema = base.parse_table_schema().unwrap();
    let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
    let function = TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
        schema_table_name: base.schema_table_name().clone(),
        table_schema_json: base.table_schema_json().into(),
        columns: vec![column.clone()],
        name_mapping_json: None,
        start_snapshot_id: 40,
        end_snapshot_id: 41,
    })
    .unwrap();
    let insert = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
        schema_table_name: base.schema_table_name().clone(),
        table_schema_json: base.table_schema_json().into(),
        table_location: base.table_location().into(),
        format_version: base.format_version(),
        spec_id: None,
    })
    .unwrap();
    let merge = IcebergMergeTableHandle::try_new(base.clone(), insert).unwrap();
    for relation in [
        IcebergRuntimeRelation::TableFunction(function),
        IcebergRuntimeRelation::MergeTable(merge),
    ] {
        let codecs = IcebergReadTypes::wire_codecs();
        let bytes = codecs.encode_table(&relation).unwrap();
        // The DTO and private domain are legal. Borrowing real Table facts
        // here is intentionally a negative probe: there is no source author
        // for these two final-read variants, so no synthetic success is claimed.
        let fixture = Fixture {
            relation,
            columns: vec![column.clone()],
            fields: vec![column_field(&column)],
            source: source_for(&IcebergRuntimeRelation::Table(base.clone())),
            domains: BTreeMap::new(),
            published_source: None,
            published_bindings: None,
            metadata: None,
            work_source: ConnectorReadWorkSource::RuntimeSplits,
        };
        assert!(!bytes.is_empty());
        assert!(
            matches!(iceberg_final_static_facts(&fixture.relation), Err(ref error) if error.kind() == ConnectorErrorKind::Unsupported)
        );
        assert_provider(&fixture.build(), ConnectorCodecErrorKind::Unsupported);
    }
}

fn changed_source(
    fixture: &Fixture,
    version: Option<ConnectorReadInputVersion>,
    digest: Option<[u8; 32]>,
    evidence: Option<Vec<u8>>,
    distribution: Option<ConnectorReadDistribution<ScanColumnId>>,
    coverage: Option<ConnectorReadArtifactCoverage>,
) -> ConnectorReadStaticFacts<ScanColumnId> {
    ConnectorReadStaticFacts::try_new(
        version.unwrap_or_else(|| fixture.source.input_version().clone()),
        digest.unwrap_or(fixture.source.selection_digest()),
        ConnectorReadProperties::try_new(
            distribution.unwrap_or_else(|| fixture.source.properties().distribution().clone()),
            vec![],
        )
        .unwrap(),
        coverage.unwrap_or_else(|| fixture.source.artifact_coverage().clone()),
        evidence.unwrap_or_else(|| fixture.source.coverage_evidence().to_vec()),
    )
    .unwrap()
}

#[test]
fn complete_source_version_selection_evidence_distribution_and_coverage_mutations_refuse() {
    let fixture = Fixture::table();
    let mut changed = fixture.clone();
    changed.source = changed_source(
        &fixture,
        Some(ConnectorReadInputVersion::try_new(vec![9; 32]).unwrap()),
        None,
        None,
        None,
        None,
    );
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    changed.source = changed_source(&fixture, None, Some([9; 32]), None, None, None);
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    changed.source = changed_source(&fixture, None, None, Some(vec![8]), None, None);
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    changed.source = changed_source(
        &fixture,
        None,
        None,
        None,
        Some(ConnectorReadDistribution::Singleton),
        None,
    );
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    changed.source = changed_source(
        &fixture,
        None,
        None,
        None,
        None,
        Some(ConnectorReadArtifactCoverage::exact([3; 32], [4; 32], vec![5]).unwrap()),
    );
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    // Selection mutation is genuine: the admitted limited table keeps its
    // source version, but must not reuse the unlimited table's digest.
    let limited = match fixture.relation.clone() {
        IcebergRuntimeRelation::Table(table) => {
            IcebergRuntimeRelation::Table(table.apply_limit(1).unwrap().into_handle())
        }
        _ => unreachable!(),
    };
    assert_eq!(
        source_for(&limited).input_version(),
        fixture.source.input_version()
    );
    assert_ne!(
        source_for(&limited).selection_digest(),
        fixture.source.selection_digest()
    );
    changed = fixture.clone();
    changed.relation = limited;
    assert_provider(
        &changed.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
}

#[test]
fn complete_metadata_kind_work_relation_kind_revision_and_private_bytes_refuse() {
    let fixture = system_fixture(IcebergSystemTableType::Snapshots);
    let mut wrong = fixture.clone();
    wrong.metadata = Some(ConnectorReadMetadataKind::try_new("$files").unwrap());
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    wrong = fixture.clone();
    wrong.work_source = ConnectorReadWorkSource::RuntimeSplits;
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    let files = system_fixture(IcebergSystemTableType::Files);
    wrong = files.clone();
    wrong.work_source = ConnectorReadWorkSource::WholeRelation;
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    let table = Fixture::table();
    assert_provider(
        &table.build_modified(Some(ConnectorReadRelationKind::ChangeWindow), None, None),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    assert_provider(
        &table.build_modified(None, Some(bytes::Bytes::new()), None),
        ConnectorCodecErrorKind::MissingField,
    );
    let bad_revision = table.build_modified(None, None, Some(ICEBERG_READ_CODEC_REVISION + 1));
    assert!(matches!(
        ConnectorReadProgramRecipe::try_compile_with_provider(
            &bad_revision,
            &IcebergReadRecipeCompiler,
            &Control::accept()
        ),
        Err(ConnectorReadProgramCompileError::Provider(_))
    ));
}

#[test]
fn complete_public_field_name_null_carrier_field_id_and_nominal_mutations_refuse() {
    let fixture = Fixture::table();
    let mut wrong = fixture.clone();
    wrong.fields[1] = Arc::new(
        wrong.fields[1]
            .as_ref()
            .clone()
            .with_name("alias_is_not_source_name"),
    );
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    wrong = fixture.clone();
    wrong.fields[1] = Arc::new(wrong.fields[1].as_ref().clone().with_nullable(true));
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    wrong = fixture.clone();
    wrong.fields[1] = Arc::new(
        wrong.fields[1]
            .as_ref()
            .clone()
            .with_data_type(DataType::Int32),
    );
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    wrong = fixture.clone();
    let mut metadata = wrong.fields[1].metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.into(), "999".into());
    wrong.fields[1] = Arc::new(wrong.fields[1].as_ref().clone().with_metadata(metadata));
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    wrong = fixture.clone();
    wrong
        .domains
        .insert(0, novarocks_type_contract::ValueLogicalType::Json);
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
}

#[test]
fn complete_read_preserves_public_unknown_metadata_without_reauthoring_it() {
    let mut fixture = Fixture::table();
    let mut metadata = fixture.fields[0].metadata().clone();
    metadata.insert(
        "provider.uninterpreted".into(),
        "retain exact source annotation".into(),
    );
    fixture.fields[0] = Arc::new(fixture.fields[0].as_ref().clone().with_metadata(metadata));
    let frozen = fixture.build();
    assert_unchanged(&frozen);
    assert_eq!(
        frozen
            .public_facts()
            .source_schema_facts()
            .unwrap()
            .schema_attributes()
            .get("fixture.root")
            .unwrap(),
        "preserved"
    );
    assert_eq!(
        frozen
            .public_facts()
            .source_schema_facts()
            .unwrap()
            .field_fact(&[0])
            .unwrap()
            .attributes()
            .get("provider.uninterpreted")
            .unwrap(),
        "retain exact source annotation"
    );
}

#[test]
fn complete_narrow_integer_schema_uses_exact_field_domain_and_rejects_other_narrow_recipe() {
    use crate::scalar_integer_domain::ScalarIntegerDomain;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            17,
            "tiny",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let handle = IcebergTableHandle::try_new(table_handle_params(&schema, None))
        .unwrap()
        .with_scalar_integer_domains(BTreeMap::from([(17, ScalarIntegerDomain::Int8)]))
        .unwrap();
    let column = IcebergColumnHandle::base_column_of(&schema, 17)
        .unwrap()
        .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int8))
        .unwrap();
    let field = Arc::new(
        column_field(&column)
            .as_ref()
            .clone()
            .with_data_type(ScalarIntegerDomain::Int8.data_type()),
    );
    let relation = IcebergRuntimeRelation::Table(handle);
    let mut fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        relation,
        columns: vec![column],
        fields: vec![field],
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    assert_eq!(fixture.fields[0].data_type(), &DataType::Int8);
    assert_unchanged(&fixture.build());
    fixture.columns[0] = fixture.columns[0]
        .clone()
        .with_scalar_integer_domain(Some(ScalarIntegerDomain::Int16))
        .unwrap();
    assert_provider(
        &fixture.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
}

#[test]
fn complete_nested_path_and_source_field_identity_are_exact_not_name_guesses() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            10,
            "record",
            Type::Struct(crate::iceberg::spec::StructType::new(vec![
                Arc::new(NestedField::required(
                    11,
                    "left",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    12,
                    "right",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])),
        ))])
        .build()
        .unwrap();
    let handle = IcebergTableHandle::try_new(table_handle_params(&schema, None)).unwrap();
    let base = IcebergColumnHandle::base_column_of(&schema, 10).unwrap();
    let left = base.dereference(&[11]).unwrap();
    let arrow = crate::typed_read::schema_binding::annotated_read_schema(&schema).unwrap();
    let field =
        crate::typed_read::schema_binding::dereference_target_field(&arrow.fields()[0], &[11])
            .unwrap();
    // A required child beneath an optional source struct retains the actual
    // projected handle's widened root NULL contract.
    let field = Arc::new(field.as_ref().clone().with_nullable(left.nullable()));
    let relation = IcebergRuntimeRelation::Table(handle);
    let mut fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        relation,
        columns: vec![left],
        fields: vec![field],
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    assert_unchanged(&fixture.build());
    fixture.columns[0] = base.dereference(&[12]).unwrap();
    assert_provider(
        &fixture.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
    // A legal column handle from another source schema must not become the
    // selected field merely because its carrier and output count agree.
    fixture.columns[0] = IcebergColumnHandle::base_column(&NestedField::required(
        999,
        "left",
        Type::Primitive(PrimitiveType::Long),
    ))
    .unwrap();
    assert_provider(
        &fixture.build(),
        ConnectorCodecErrorKind::InconsistentFields,
    );
}

fn assert_control(frozen: &FrozenConnectorRead, control: &Control, expected: CompileControlError) {
    let result = ConnectorReadProgramRecipe::try_compile_with_provider(
        frozen,
        &IcebergReadRecipeCompiler,
        control,
    );
    assert!(
        matches!(result, Err(ConnectorReadProgramCompileError::Control(actual)) if actual == expected),
        "unexpected result: {result:?}"
    );
}
fn wide_fixture() -> Fixture {
    let schema = Schema::builder()
        .with_fields(
            (1..=512)
                .map(|id| {
                    Arc::new(NestedField::required(
                        id,
                        format!("column_{}_{}", "x".repeat(128), id),
                        Type::Primitive(PrimitiveType::Long),
                    ))
                })
                .collect::<Vec<_>>(),
        )
        .build()
        .unwrap();
    let handle = IcebergTableHandle::try_new(table_handle_params(&schema, None)).unwrap();
    // The full source schema makes actual source hashing exceed a quantum;
    // projecting one column retains legal complete public/private scan facts.
    assert!(handle.table_schema_json().len() > 256 * 256);
    let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
    let relation = IcebergRuntimeRelation::Table(handle);
    Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        relation,
        fields: vec![column_field(&column)],
        columns: vec![column],
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    }
}

#[test]
fn complete_read_three_control_causes_refuse_entry_before_private_decode() {
    let frozen = Fixture::table().build_modified(None, Some(bytes::Bytes::new()), None);
    for error in causes() {
        let control = Control {
            failure: Some((1, error)),
            ..Control::accept()
        };
        assert_control(&frozen, &control, error);
        assert_eq!(*control.calls.lock().unwrap(), vec![0]);
    }
}

#[test]
fn complete_read_actual_wide_source_or_wire_work_refuses_first_256_without_recheck() {
    let frozen = wide_fixture().build();
    for error in causes() {
        let control = Control {
            quantum: Some(error),
            ..Control::accept()
        };
        assert_control(&frozen, &control, error);
        let calls = control.calls.lock().unwrap();
        assert_eq!(calls.last(), Some(&256));
        assert_eq!(calls.iter().filter(|units| **units == 256).count(), 1);
        assert!(calls.iter().all(|units| *units <= 256));
    }
}

#[test]
fn complete_read_success_publication_tail_and_ordinary_error_tail_preserve_three_causes() {
    let success = Fixture::table().build();
    let mut wrong = Fixture::table();
    wrong.fields[1] = Arc::new(wrong.fields[1].as_ref().clone().with_nullable(true));
    let failure = wrong.build();
    for frozen in [&success, &failure] {
        let reference = Control::accept();
        let result = ConnectorReadProgramRecipe::try_compile_with_provider(
            frozen,
            &IcebergReadRecipeCompiler,
            &reference,
        );
        if std::ptr::eq(frozen, &success) {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(ConnectorReadProgramCompileError::Provider(_))
            ));
        }
        let calls = reference.calls.lock().unwrap().clone();
        assert!(calls.len() > 2);
        for error in causes() {
            let control = Control {
                failure: Some((calls.len(), error)),
                ..Control::accept()
            };
            assert_control(frozen, &control, error);
            assert_eq!(*control.calls.lock().unwrap(), calls);
        }
    }
}

/// The read stage of `validate_fragment_providers`: one installed pure catalog
/// carrying only the Iceberg read compiler.
fn iceberg_catalog() -> novarocks_connector_contract::PureProviderProgramCatalog<ConnectorCodecError>
{
    use novarocks_connector_contract::{PureProviderManifestEntry, PureProviderProgramDefinition};
    let provider = ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap();
    novarocks_connector_contract::PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            true,
            false,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            Some(Arc::new(IcebergReadRecipeCompiler)),
            None,
        )],
        &Control::accept(),
    )
    .unwrap()
}

fn fixture_creation<T>(
    run: impl FnOnce(&mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>) -> T,
) -> T {
    let mut request = |_: std::alloc::Layout| Ok(());
    let mut work = |_: usize| Ok(());
    let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut work,
    );
    run(&mut creation)
}
fn fixture_public_read_schema(
    relation: &IcebergRuntimeRelation,
    columns: &[IcebergColumnHandle],
) -> Result<
    novarocks_spi::connector::read_stack::ConnectorReadPublicSchema,
    novarocks_spi::connector::ConnectorError,
> {
    let columns: Vec<_> = columns.iter().collect();
    fixture_creation(|creation| {
        crate::typed_read::codec::public_read_schema(relation, &columns, creation)
    })
    .map_err(|cause| match cause {
        novarocks_connector_contract::PureProviderCompileError::Provider(cause) => cause,
        novarocks_connector_contract::PureProviderCompileError::Control(cause) => {
            panic!("unexpected fixture control: {cause}")
        }
    })
}
/// The fixture with its public fields replaced by the provider's own published
/// schema for the same relation and columns.
fn published(fixture: &Fixture) -> Fixture {
    let schema = match &fixture.relation {
        IcebergRuntimeRelation::SystemTable(reference) => {
            let schema = partitioned_schema();
            let spec = PartitionSpec::unpartition_spec();
            let columns: Vec<_> = fixture.columns.iter().collect();
            fixture_creation(|creation| {
                crate::typed_read::codec::system_public_read_schema(
                    reference.system_table_type(),
                    &columns,
                    &schema,
                    &[&spec],
                    creation,
                )
            })
            .unwrap()
        }
        relation => fixture_public_read_schema(relation, &fixture.columns).unwrap(),
    };
    let mut published = fixture.clone();
    published.fields = schema.schema().fields().to_vec();
    published.published_source = schema.source_schema_facts().cloned();
    published.published_bindings = schema
        .execution_schema()
        .map(|schema| schema.bindings().to_vec());
    assert!(schema.execution_schema().is_some());
    assert!(
        schema
            .schema()
            .fields()
            .iter()
            .all(|field| field.metadata().is_empty())
    );
    published
}

#[test]
fn every_final_relation_kind_publishes_a_schema_its_pure_compiler_accepts() {
    let catalog = iceberg_catalog();
    let mut fixtures = vec![Fixture::table(), window_fixture(), rewrite_fixture()];
    for kind in [
        IcebergSystemTableType::Files,
        IcebergSystemTableType::Entries,
        IcebergSystemTableType::Snapshots,
        IcebergSystemTableType::History,
        IcebergSystemTableType::Refs,
        IcebergSystemTableType::Manifests,
        IcebergSystemTableType::Partitions,
    ] {
        let mut fixture = system_fixture(kind);
        // Reordered output with a repeat keeps each column's own field.
        fixture.columns.reverse();
        fixture.columns.push(fixture.columns[0].clone());
        fixtures.push(fixture);
    }
    for fixture in fixtures {
        let frozen = published(&fixture).build();
        let recipe = catalog
            .compile_read(&frozen, &Control::accept())
            .unwrap_or_else(|error| {
                panic!(
                    "{:?} public schema refused: {error}",
                    fixture.relation.kind()
                )
            });
        assert_eq!(recipe.frozen(), &frozen);
    }
}

#[test]
fn published_schema_carries_ids_defaults_and_nested_domains_and_passes_the_pure_catalog() {
    use crate::iceberg::spec::{ListType, Literal, MapType, StructType};
    use crate::typed_read::schema_binding::IcebergMetadataColumn;

    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long))
                    .with_initial_default(Literal::long(7)),
            ),
            Arc::new(NestedField::optional(
                2,
                "point",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::required(
                        3,
                        "x",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        4,
                        "label",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])),
            )),
            Arc::new(NestedField::optional(
                5,
                "tags",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    6,
                    Type::Primitive(PrimitiveType::String),
                    false,
                )))),
            )),
            Arc::new(NestedField::optional(
                7,
                "attrs",
                Type::Map(MapType::new(
                    Arc::new(NestedField::map_key_element(
                        8,
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::map_value_element(
                        9,
                        Type::Primitive(PrimitiveType::Long),
                        false,
                    )),
                )),
            )),
        ])
        .build()
        .unwrap();
    let base = |id| IcebergColumnHandle::base_column_of(&schema, id).unwrap();
    let file = crate::typed_read::table_execute::rewrite_position_delete_pseudo_column(
        "_file",
        IcebergMetadataColumn::Path,
    )
    .unwrap();
    let columns = vec![
        base(1),
        base(2),
        base(2).dereference(&[3]).unwrap(),
        base(5),
        base(7),
        file,
        base(1),
    ];
    let relation = IcebergRuntimeRelation::Table(
        IcebergTableHandle::try_new(table_handle_params(&schema, None)).unwrap(),
    );
    let schema_out = fixture_public_read_schema(&relation, &columns).unwrap();
    let fields = schema_out.schema().fields();
    let source = schema_out.source_schema_facts().unwrap();
    let id = |path: &[u32]| source.field_fact(path).unwrap().field_id().unwrap();
    assert_eq!(
        fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
        ["id", "point", "x", "tags", "attrs", "_file", "id"]
    );
    assert!(fields.iter().all(|field| field.metadata().is_empty()));
    assert_eq!(
        (0..fields.len())
            .map(|ordinal| id(&[ordinal as u32]))
            .collect::<Vec<_>>(),
        [1, 2, 3, 5, 7, IcebergMetadataColumn::Path.field_id(), 1]
    );
    assert_eq!(
        source
            .field_fact(&[0])
            .unwrap()
            .initial_default()
            .unwrap()
            .as_json(),
        "7"
    );
    assert_eq!([id(&[1, 0]), id(&[1, 1])], [3, 4]);
    assert_eq!(id(&[3, 0]), 6);
    assert_eq!([id(&[4, 0, 0]), id(&[4, 0, 1])], [8, 9]);
    assert!(
        source.field_fact(&[4, 0]).is_none(),
        "Map entries are physical shape, not an Iceberg field ID"
    );
    // A required child read through an optional parent is nullable.
    assert!(schema.field_by_id(3).unwrap().required);
    assert!(fields[2].is_nullable());
    assert!(
        schema_out
            .logical_types()
            .iter()
            .all(|logical| *logical == novarocks_type_contract::ValueLogicalType::Physical)
    );

    // The catalog projection the plan's engine types come from agrees with
    // the read author on every base column's exact nested domain.
    let projected = crate::schema_mapping::annotate_read_schema_from_scan_model(
        &crate::scalar_integer_domain::sql_schema(&schema, &HashMap::new()).unwrap(),
        &crate::schema_facts::iceberg_schema_def(&schema),
    )
    .unwrap();
    for (ordinal, name) in [(0, "id"), (1, "point"), (3, "tags"), (4, "attrs")] {
        let catalog = projected.field_with_name(name).unwrap();
        assert!(
            novarocks_type_contract::arrow_data_types_exact(
                fields[ordinal].data_type(),
                novarocks_type_contract::ValueLayout::try_new(
                    catalog.data_type(),
                    novarocks_type_contract::ValueTypeStorageLifetime::pure()
                )
                .unwrap()
                .data_type()
            ),
            "{name}: published {:?} differs from catalog {:?}",
            fields[ordinal].data_type(),
            catalog.data_type()
        );
        assert_eq!(fields[ordinal].is_nullable(), catalog.is_nullable());
    }

    let fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: fields.to_vec(),
        relation,
        columns,
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    let frozen = published(&fixture).build();
    let recipe = iceberg_catalog()
        .compile_read(&frozen, &Control::accept())
        .unwrap();
    assert_eq!(recipe.frozen(), &frozen);
}

#[test]
fn relations_without_a_public_field_author_refuse_explicitly() {
    let base = crate::typed_boundary::final_static_facts_tests::table_handle();
    let schema = base.parse_table_schema().unwrap();
    let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
    let function = TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
        schema_table_name: base.schema_table_name().clone(),
        table_schema_json: base.table_schema_json().into(),
        columns: vec![column.clone()],
        name_mapping_json: None,
        start_snapshot_id: 40,
        end_snapshot_id: 41,
    })
    .unwrap();
    let insert = IcebergInsertTableHandle::try_new(IcebergInsertTableHandleParams {
        schema_table_name: base.schema_table_name().clone(),
        table_schema_json: base.table_schema_json().into(),
        table_location: base.table_location().into(),
        format_version: base.format_version(),
        spec_id: None,
    })
    .unwrap();
    let merge = IcebergMergeTableHandle::try_new(base.clone(), insert).unwrap();
    let system = system_fixture(IcebergSystemTableType::Snapshots).relation;
    // A metadata relation is authored from its frozen system schema only.
    for relation in [
        IcebergRuntimeRelation::TableFunction(function),
        IcebergRuntimeRelation::MergeTable(merge),
        system,
    ] {
        let error =
            fixture_public_read_schema(&relation, std::slice::from_ref(&column)).unwrap_err();
        assert_eq!(
            error.kind(),
            ConnectorErrorKind::Unsupported,
            "{:?}",
            relation.kind()
        );
    }

    // A column the frozen source does not have is not authored by guess.
    let foreign = IcebergColumnHandle::base_column(&NestedField::required(
        99,
        "ghost",
        Type::Primitive(PrimitiveType::Long),
    ))
    .unwrap();
    let error =
        fixture_public_read_schema(&IcebergRuntimeRelation::Table(base), &[column, foreign])
            .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn frozen_provider_nominal_properties_and_native_nested_domains_survive_complete_read() {
    use crate::iceberg::spec::{
        FormatVersion, ListType, MapType, SortOrder, StructType, TableMetadataBuilder,
    };
    use novarocks_type_contract::{ValueKind, ValueLogicalType};
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "native_hll",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::required(
                2,
                "native_bitmap",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::required(
                3,
                "wide_integer",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::optional(
                4,
                "nested",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::required(
                        5,
                        "uuid",
                        Type::Primitive(PrimitiveType::Uuid),
                    )),
                    Arc::new(NestedField::optional(
                        6,
                        "variants",
                        Type::List(ListType::new(Arc::new(NestedField::list_element(
                            7,
                            Type::Variant(crate::iceberg::spec::VariantType),
                            true,
                        )))),
                    )),
                    Arc::new(NestedField::optional(
                        8,
                        "map",
                        Type::Map(MapType::new(
                            Arc::new(NestedField::map_key_element(
                                9,
                                Type::Primitive(PrimitiveType::String),
                            )),
                            Arc::new(NestedField::map_value_element(
                                10,
                                Type::Primitive(PrimitiveType::Uuid),
                                false,
                            )),
                        )),
                    )),
                ])),
            )),
        ])
        .build()
        .unwrap();
    let metadata = TableMetadataBuilder::new(
        schema.clone(),
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "file:///nominal-source".into(),
        FormatVersion::V3,
        HashMap::from([
            ("novarocks.logical_type.native_hll".into(), "hll".into()),
            (
                "novarocks.logical_type.native_bitmap".into(),
                "bitmap".into(),
            ),
            (
                "novarocks.logical_type.wide_integer".into(),
                "largeint".into(),
            ),
        ]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let declarations =
        crate::schema_mapping::nominal_declaration::from_metadata(&metadata).unwrap();
    let mut params = table_handle_params(&schema, None);
    params.format_version = 3;
    let table = IcebergTableHandle::try_new(params)
        .unwrap()
        .with_nominal_declarations(declarations)
        .unwrap();
    let relation = IcebergRuntimeRelation::Table(table);
    let columns = (1..=4)
        .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
        .collect::<Vec<_>>();
    let public = fixture_public_read_schema(&relation, &columns).unwrap();
    let execution = public.execution_schema().unwrap();
    assert_eq!(
        execution.binding(0).unwrap().logical_type(),
        ValueLogicalType::Hll
    );
    assert_eq!(
        execution.binding(1).unwrap().logical_type(),
        ValueLogicalType::Bitmap
    );
    assert_eq!(
        execution.binding(2).unwrap().logical_type(),
        ValueLogicalType::LargeInt
    );
    let nested = execution.binding(3).unwrap();
    assert_eq!(
        nested.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Uuid
    );
    let variants = nested.child_at(1).unwrap();
    assert!(matches!(variants.value_type().kind(), ValueKind::List(_)));
    assert_eq!(
        variants.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Variant
    );
    assert!(!variants.child_at(0).unwrap().nullable());
    assert_eq!(
        nested
            .child_at(2)
            .unwrap()
            .child_at(1)
            .unwrap()
            .logical_type(),
        ValueLogicalType::Uuid
    );
    let source = public.source_schema_facts().unwrap();
    assert_eq!(source.field_fact(&[3, 1, 0]).unwrap().field_id(), Some(7));
    assert!(source.field_fact(&[3, 2, 0]).is_none());
    let fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: public.schema().fields().to_vec(),
        relation,
        columns,
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    let frozen = published(&fixture).build();
    assert_unchanged(&frozen);
    let bytes = IcebergReadTypes::wire_codecs()
        .encode_table(&fixture.relation)
        .unwrap();
    assert!(!bytes.is_empty());
}

#[test]
fn complete_nominal_table_and_window_use_exact_declarations_and_refuse_old_revisions() {
    use crate::schema_mapping::nominal_declaration::NominalSourceDomain;
    let schema = Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "n",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
            Arc::new(NestedField::required(
                2,
                "h",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::optional(
                3,
                "b",
                Type::Primitive(PrimitiveType::Binary),
            )),
        ])
        .build()
        .unwrap();
    let domains = BTreeMap::from([
        (1, NominalSourceDomain::LargeInt),
        (2, NominalSourceDomain::Hll),
        (3, NominalSourceDomain::Bitmap),
    ]);
    let handle = IcebergTableHandle::try_new(table_handle_params(&schema, None))
        .unwrap()
        .with_nominal_declarations(domains.clone())
        .unwrap();
    let columns = [3, 1, 2, 1]
        .map(|id| IcebergColumnHandle::base_column_of(&schema, id).unwrap())
        .to_vec();
    let fields = columns.iter().map(column_field).collect::<Vec<_>>();
    let relation = IcebergRuntimeRelation::Table(handle.clone());
    let fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        relation,
        columns: columns.clone(),
        fields: fields.clone(),
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    let fixture = published(&fixture);
    assert_unchanged(&fixture.build());
    for revision in [2, 3, 4, 5, 6, 7] {
        let old = fixture.build_modified(None, None, Some(revision));
        assert_provider(&old, ConnectorCodecErrorKind::VersionMismatch);
    }
    for revision in [3, 4, 5, 6, 7] {
        let old = fixture.build_modified(
            None,
            Some(bytes::Bytes::from_static(b"invalid private body")),
            Some(revision),
        );
        let error = ConnectorReadProgramRecipe::try_compile_with_provider(
            &old,
            &IcebergReadRecipeCompiler,
            &Control::accept(),
        )
        .unwrap_err();
        let ConnectorReadProgramCompileError::Provider(error) = error else {
            panic!("expected exact provider ABI rejection");
        };
        assert_eq!(error.kind(), ConnectorCodecErrorKind::VersionMismatch);
        assert_eq!(error.path().to_string(), "header.codec_revision");
    }
    let invalid_current = fixture.build_modified(
        None,
        Some(bytes::Bytes::from_static(b"invalid private body")),
        None,
    );
    let error = ConnectorReadProgramRecipe::try_compile_with_provider(
        &invalid_current,
        &IcebergReadRecipeCompiler,
        &Control::accept(),
    )
    .unwrap_err();
    assert!(
        matches!(error, ConnectorReadProgramCompileError::Provider(ref error)
        if error.kind() != ConnectorCodecErrorKind::VersionMismatch)
    );
    let mut wrong = fixture.clone();
    let mut wrong_declarations = domains.clone();
    wrong_declarations.insert(3, NominalSourceDomain::Hll);
    wrong.relation = IcebergRuntimeRelation::Table(
        handle
            .clone()
            .with_nominal_declarations(wrong_declarations)
            .unwrap(),
    );
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);

    let to = handle.read_domain().unwrap().clone();
    let from = crate::delete_semantics::test_read_domain(&schema, &[], 10);
    let window = IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: handle.schema_table_name().clone(),
        table_schema_json: handle.table_schema_json().into(),
        columns: columns[..3].to_vec(),
        name_mapping_json: None,
        from_snapshot_id_exclusive: 10,
        to_snapshot_id_inclusive: 11,
        from_read_domain: from,
        to_read_domain: to,
        partition_spec_jsons: handle.partition_spec_jsons().clone(),
    })
    .unwrap()
    .with_nominal_declarations(domains)
    .unwrap();
    let relation = IcebergRuntimeRelation::ChangeWindow(window);
    let window_fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        relation,
        columns,
        fields,
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    assert_unchanged(&published(&window_fixture).build());
}

#[test]
fn recursive_nominal_table_window_and_projected_struct_field_keep_exact_frozen_domains() {
    use crate::schema_mapping::nominal_declaration::{self, NominalSourceDomain};
    let metadata = nominal_declaration::recursive_tests::metadata();
    let schema = metadata.current_schema();
    let declarations = nominal_declaration::from_metadata(&metadata).unwrap();
    let table = IcebergTableHandle::try_new(table_handle_params(schema, None))
        .unwrap()
        .with_nominal_declarations(declarations.clone())
        .unwrap();
    let root = IcebergColumnHandle::base_column_of(schema, 1).unwrap();
    let hll_id = *declarations
        .iter()
        .find(|(_, domain)| **domain == NominalSourceDomain::Hll)
        .unwrap()
        .0;
    let columns = vec![root.clone(), root.dereference(&[hll_id]).unwrap(), root];
    let relation = IcebergRuntimeRelation::Table(table.clone());
    let public = fixture_public_read_schema(&relation, &columns).unwrap();
    let bindings = public.execution_schema().unwrap();
    assert_eq!(
        bindings.binding(1).unwrap().logical_type(),
        novarocks_type_contract::ValueLogicalType::Hll
    );
    assert_eq!(
        bindings
            .binding(0)
            .unwrap()
            .child_at(0)
            .unwrap()
            .child_at(0)
            .unwrap()
            .logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
    let fixture = Fixture {
        source: source_for(&relation),
        domains: BTreeMap::new(),
        published_source: None,
        published_bindings: None,
        fields: public.schema().fields().to_vec(),
        relation,
        columns: columns.clone(),
        metadata: None,
        work_source: ConnectorReadWorkSource::RuntimeSplits,
    };
    let fixture = published(&fixture);
    assert_unchanged(&fixture.build());
    let mut wrong = fixture.clone();
    let mut drift = declarations.clone();
    drift.insert(hll_id, NominalSourceDomain::Bitmap);
    wrong.relation =
        IcebergRuntimeRelation::Table(table.clone().with_nominal_declarations(drift).unwrap());
    assert_provider(&wrong.build(), ConnectorCodecErrorKind::InconsistentFields);
    let mut erased = fixture.clone();
    erased.relation = IcebergRuntimeRelation::Table(
        table
            .clone()
            .with_nominal_declarations(BTreeMap::new())
            .unwrap(),
    );
    assert_provider(&erased.build(), ConnectorCodecErrorKind::InconsistentFields);
    let to = table.read_domain().unwrap().clone();
    let from = crate::delete_semantics::test_read_domain(schema, &[], 10);
    let window = IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: table.schema_table_name().clone(),
        table_schema_json: table.table_schema_json().into(),
        // A table read may repeat projections; the canonical change window's
        // owned column set must contain each exact handle once.
        columns: columns[..2].to_vec(),
        name_mapping_json: None,
        from_snapshot_id_exclusive: 10,
        to_snapshot_id_inclusive: 11,
        from_read_domain: from,
        to_read_domain: to,
        partition_spec_jsons: table.partition_spec_jsons().clone(),
    })
    .unwrap()
    .with_nominal_declarations(declarations)
    .unwrap();
    let relation = IcebergRuntimeRelation::ChangeWindow(window);
    let fixture = Fixture {
        source: source_for(&relation),
        relation,
        columns: columns[..2].to_vec(),
        fields: fixture.fields[..2].to_vec(),
        ..fixture
    };
    let fixture = published(&fixture);
    let bindings = fixture.published_bindings.as_ref().unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(
        bindings[0]
            .child_at(0)
            .unwrap()
            .child_at(0)
            .unwrap()
            .logical_type(),
        novarocks_type_contract::ValueLogicalType::LargeInt
    );
    assert_eq!(
        bindings[1].logical_type(),
        novarocks_type_contract::ValueLogicalType::Hll
    );
    assert_unchanged(&fixture.build());
}
