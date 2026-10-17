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

//! Regression coverage for provider-authored delete session inputs.
use super::*;
use crate::commit::write_stack::domain::{IcebergWriterHandle, IcebergWriterOutput};
use novarocks_spi::connector::{
    ConnectorWriteFieldBinding, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
};

fn field_requests(fields: &[ConnectorWriteFieldBinding]) -> Vec<ConnectorWriteFieldRequest> {
    fields
        .iter()
        .map(|field| {
            ConnectorWriteFieldRequest::try_new_typed(
                field.field().clone(),
                field
                    .execution_type()
                    .expect("provider complete declaration")
                    .clone(),
            )
            .unwrap()
        })
        .collect()
}

fn delete_input(input: &ConnectorWriteInputShape) -> ConnectorWriteInputRequest {
    match input {
        ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::DeletionVector {
            identity_fields: field_requests(identity_fields),
            partition_source_fields: field_requests(partition_source_fields),
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputRequest::PositionDelete {
            identity_fields: field_requests(identity_fields),
            partition_source_fields: field_requests(partition_source_fields),
        },
        _ => panic!("expected provider delete input"),
    }
}

fn identity_request(path: &str, pos: &str) -> ConnectorWriteInputRequest {
    ConnectorWriteInputRequest::DeletionVector {
        identity_fields: [(path, DataType::Utf8), (pos, DataType::Int64)]
            .into_iter()
            .map(|(name, data_type)| {
                ConnectorWriteFieldRequest::try_new_typed(
                    Field::new(name, data_type.clone(), false),
                    novarocks_type_contract::FunctionValueType::new(data_type, false),
                )
                .unwrap()
            })
            .collect(),
        partition_source_fields: Vec::new(),
    }
}

fn seed_position_rewrite(lake: &FrozenRewriteLake) {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteFinishPublication, ConnectorWriteFinishRequest,
    };
    lake.write_rows(
        novarocks_spi::connector::ConnectorWriteIntent::Append,
        &[11, 22],
        "source",
    );
    let table = lake.table();
    let files = lake
        .executor
        .block_on(crate::manifest::extract_data_files_with_stats(&table))
        .unwrap();
    assert_eq!(files.len(), 1);
    let file = &files[0];
    let session = lake
        .control
        .begin_write(ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: identity_request("_file", "_pos"),
            base: None,
            flavor: ConnectorWriteSessionFlavor::Ordinary,
            context: request_context(),
        })
        .unwrap();
    let mut vector = crate::commit::DeletionVector::new();
    vector.insert(0).unwrap();
    let written = lake
        .executor
        .block_on(crate::commit::write_single_deletion_vector_puffin(
            table.file_io(),
            &format!("{}/data/delete.puffin", table.metadata().location()),
            &file.path,
            &vector,
        ))
        .unwrap();
    let fragment = IcebergCommitFragment::deletion_vector(
        dv_artifact(
            &written.path,
            &file.path,
            written.cardinality,
            written.file_size_in_bytes,
            written.content_offset,
            written.content_size_in_bytes,
        )
        .unwrap(),
    );
    let target = session
        .targets()
        .iter()
        .find(|target| {
            lake.adapter
                .writer_handle(target.handle())
                .unwrap()
                .branch()
                == IcebergWriteBranch::DeletionVector
        })
        .unwrap();
    let set = prepared(
        &lake.adapter,
        vec![(target.ordinal(), fragment)],
        &session.expected_targets(),
    );
    let result = lake
        .control
        .finish_write(ConnectorWriteFinishRequest {
            commit: session.commit_handle(),
            prepared: set,
            statistics: Vec::new(),
            publication: ConnectorWriteFinishPublication::None,
            context: request_context(),
        })
        .unwrap();
    assert!(
        matches!(result, ExternalMutationOutcome::KnownCommitted { .. }),
        "{result:?}"
    );
}

fn physical_bytes(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn walk(root: &std::path::Path, out: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(&entry.path(), out);
            } else {
                out.insert(entry.path(), fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, &mut out);
    out
}

#[test]
fn provider_position_rewrite_preparation_begins_with_its_exact_identity_roles() {
    use novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest;
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V3);
    seed_position_rewrite(&lake);
    let before = physical_bytes(lake._warehouse.path());
    let plan = lake.plan(true);
    assert_eq!(plan.cohorts().len(), 1);
    let prep = plan.cohorts()[0].preparation();
    let mut request = lake.request(&plan);
    request.input = delete_input(prep.input());
    let ConnectorWriteInputRequest::DeletionVector {
        identity_fields, ..
    } = &request.input
    else {
        unreachable!()
    };
    assert_eq!(
        identity_fields
            .iter()
            .map(|field| (
                field.field().name().as_str(),
                field.field().data_type(),
                field.field().is_nullable()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("file_path", &DataType::Utf8, false),
            ("pos", &DataType::Int64, false)
        ]
    );
    let session = lake
        .control
        .begin_write(request)
        .expect("provider preparation must survive session re-freeze");
    assert_eq!(session.targets().len(), 1);
    let writer = lake
        .adapter
        .writer_handle(session.targets()[0].handle())
        .unwrap();
    assert_eq!(writer.branch(), IcebergWriteBranch::DeletionVector);
    assert_eq!(
        writer.table().base_snapshot_id(),
        lake.table().metadata().current_snapshot_id()
    );
    assert_eq!(
        session.targets()[0].rewrite_group_digest(),
        Some(plan.cohorts()[0].group_digest())
    );
    assert!(matches!(
        lake.control
            .abort_write(ConnectorWriteSessionAbortRequest {
                commit: session.commit_handle(),
                context: request_context(),
            })
            .unwrap(),
        ConnectorWriteAbortOutcome::KnownUncommitted { .. }
    ));
    assert_eq!(physical_bytes(lake._warehouse.path()), before);
}

#[test]
fn provider_position_rewrite_rejects_identity_role_type_nullability_and_owner_changes() {
    let lake = FrozenRewriteLake::new_version(crate::iceberg::spec::FormatVersion::V3);
    seed_position_rewrite(&lake);
    let plan = lake.plan(true);
    let before = physical_bytes(lake._warehouse.path());
    for case in 0..6 {
        let mut request = lake.request(&plan);
        request.input = delete_input(plan.cohorts()[0].preparation().input());
        let ConnectorWriteInputRequest::DeletionVector {
            identity_fields, ..
        } = &mut request.input
        else {
            unreachable!()
        };
        match case {
            0 => identity_fields[0] = identity_fields[1].clone(),
            1 => {
                identity_fields[0] = ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("file_path", DataType::Int64, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                )
                .unwrap()
            }
            2 => {
                identity_fields[0] = ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("file_path", DataType::Utf8, true),
                    novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
                )
                .unwrap()
            }
            3 => {
                identity_fields[0] = ConnectorWriteFieldRequest::try_new_typed(
                    Field::new("unknown", DataType::Utf8, false),
                    novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
                )
                .unwrap()
            }
            4 => {
                let content = plan
                    .frozen()
                    .value::<crate::distributed_rewrite::FrozenRewritePlanContent>("rewrite-local")
                    .unwrap();
                let mut owner = lake.control.binding_key().clone();
                owner.incarnation = ProviderBindingEpoch::new();
                request.flavor = ConnectorWriteSessionFlavor::DistributedRewrite {
                    shape: plan.shape(),
                    frozen: novarocks_spi::connector::ConnectorProcessLocalPayload::new(
                        owner,
                        Arc::clone(content),
                    ),
                };
            }
            5 => {
                request.flavor = ConnectorWriteSessionFlavor::Ordinary;
            }
            _ => unreachable!(),
        }
        let error = lake
            .control
            .begin_write(request)
            .expect_err("invalid input must not acquire a session");
        if case == 4 {
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(
                error.message(),
                "process-local payload belongs to another connector generation"
            );
        }
        assert_eq!(
            physical_bytes(lake._warehouse.path()),
            before,
            "case {case}"
        );
    }
}

fn facts(base: Option<i64>, version: u8) -> IcebergWriteTableFacts {
    IcebergWriteTableFacts::try_new(
        "uuid".into(),
        "db".into(),
        "t".into(),
        "file:///table".into(),
        "file:///table/data".into(),
        "main".into(),
        base,
        if base.is_some() { 4 } else { 0 },
        0,
        0,
        version,
    )
    .unwrap()
}
fn output(branch: IcebergWriteBranch) -> IcebergWriterOutput {
    IcebergWriterOutput::try_new(
        if branch == IcebergWriteBranch::DeletionVector {
            IcebergFileFormat::Puffin
        } else {
            IcebergFileFormat::Parquet
        },
        parquet::basic::Compression::SNAPPY,
        None,
    )
    .unwrap()
}

#[test]
fn empty_base_delete_recipe_accepts_only_an_empty_old_delete_set() {
    for (branch, version) in [
        (IcebergWriteBranch::PositionDelete, 2),
        (IcebergWriteBranch::DeletionVector, 3),
    ] {
        let recipe = IcebergWriterHandle::try_new_delete(
            branch,
            facts(None, version),
            output(branch),
            Vec::new(),
        )
        .expect("empty table has no old deletes to merge");
        assert_eq!(recipe.table().base_snapshot_id(), None);
        assert!(recipe.old_deletes().is_empty());
    }
}

#[test]
fn delete_recipe_retains_exact_snapshot_and_missing_base_rejections() {
    for (branch, version) in [
        (IcebergWriteBranch::PositionDelete, 2),
        (IcebergWriteBranch::DeletionVector, 3),
    ] {
        let target = || merge_target("file:///table/data/a.parquet", 2, Vec::new());
        assert!(
            IcebergWriterHandle::try_new_delete(
                branch,
                facts(None, version),
                output(branch),
                vec![target()]
            )
            .is_err()
        );
        assert!(
            IcebergWriterHandle::try_new_delete(
                branch,
                facts(Some(78), version),
                output(branch),
                vec![target()]
            )
            .is_err()
        );
        let exact = IcebergWriterHandle::try_new_delete(
            branch,
            facts(Some(77), version),
            output(branch),
            vec![target()],
        )
        .unwrap();
        assert_eq!(
            exact.old_deletes()["file:///table/data/a.parquet"].base_snapshot_id(),
            77
        );
        assert!(
            IcebergWriterHandle::try_new_delete(
                branch,
                facts(Some(77), version),
                output(branch),
                vec![target(), target()]
            )
            .is_err()
        );
    }
}

#[test]
fn empty_v2_delete_preparation_begins_and_aborts_without_publication() {
    empty_delete_session(crate::iceberg::spec::FormatVersion::V2, false);
}
#[test]
fn empty_existing_variant_v3_delete_preparation_begins_and_aborts_without_publication() {
    empty_delete_session(crate::iceberg::spec::FormatVersion::V3, true);
}

fn empty_delete_session(version: crate::iceberg::spec::FormatVersion, variant: bool) {
    use novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest;
    use novarocks_spi::connector::{
        ConnectorMetadata, ConnectorRowMutationIntent, ConnectorRowMutationPreparationOutcome,
        ConnectorRowMutationPreparationRequest, ConnectorTableIdentity, ConnectorTableRequest,
        ConnectorTableResolution, ConnectorWriteOperationId,
    };
    let lake = FrozenRewriteLake::new_version(version);
    let name = if variant { "existing_variant" } else { "t" };
    let namespace = if variant { "variant_db" } else { "db" };
    if variant {
        // The external fixture owns this existing Variant schema, not SQL CREATE admission.
        let schema = serde_json::from_value(serde_json::json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"},{"id":2,"name":"v","required":false,"type":"variant"}]})).unwrap();
        lake.executor.block_on(
            crate::catalog::admission_test_support::create_static_fixture_table(
                &lake.runtime,
                lake.control.binding_key().incarnation,
                crate::catalog::CatalogNamespaceName::new(namespace),
                crate::iceberg::TableCreation::builder()
                    .name(name.into())
                    .schema(schema)
                    .format_version(version)
                    .build(),
            ),
        );
    }
    let context = request_context();
    let provider = crate::metadata::IcebergMetadata::new(
        descriptor("rewrite-local"),
        lake.control.binding_key().incarnation,
        lake.runtime.clone(),
    );
    let loaded = crate::metadata::with_pure_schema_creation(&context, |creation| {
        provider
            .load_table(
                ConnectorTableRequest {
                    table: ConnectorTableIdentity {
                        instance_id: lake.control.binding_key().instance_id.clone(),
                        namespace: Arc::from(namespace),
                        table: Arc::from(name),
                    },
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context.clone(),
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
    })
    .unwrap();
    let preparation = match crate::commit::row_mutation_preparation::prepare_row_mutation(
        ConnectorRowMutationPreparationRequest {
            operation_id: ConnectorWriteOperationId::new(),
            table: loaded.table,
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: ConnectorRowMutationIntent::Delete,
            context: context.clone(),
        },
        lake.control.binding_key(),
    )
    .unwrap()
    {
        ConnectorRowMutationPreparationOutcome::Prepared(preparation) => preparation,
        ConnectorRowMutationPreparationOutcome::Denied(error) => {
            panic!("existing table DELETE denied: {error}")
        }
    };
    assert_eq!(preparation.base_version_ordinal(), None);
    let input = if variant {
        identity_request("_file", "_pos")
    } else {
        let ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } = identity_request("_file", "_pos")
        else {
            unreachable!()
        };
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
    };
    let before = physical_bytes(lake._warehouse.path());
    let session = lake
        .control
        .begin_write(ConnectorWriteBeginRequest {
            table: Arc::from(format!("{namespace}.{name}")),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input,
            base: None,
            flavor: ConnectorWriteSessionFlavor::RowMutation,
            context: context.clone(),
        })
        .expect("prepared empty-base DELETE must acquire its real session");
    assert_eq!(session.targets().len(), 1);
    let writer = lake
        .adapter
        .writer_handle(session.targets()[0].handle())
        .unwrap();
    assert_eq!(writer.table().base_snapshot_id(), None);
    assert_eq!(
        writer.branch(),
        if variant {
            IcebergWriteBranch::DeletionVector
        } else {
            IcebergWriteBranch::PositionDelete
        }
    );
    assert!(writer.old_deletes().is_empty());
    assert!(matches!(
        lake.control
            .abort_write(ConnectorWriteSessionAbortRequest {
                commit: session.commit_handle(),
                context
            })
            .unwrap(),
        ConnectorWriteAbortOutcome::KnownUncommitted { .. }
    ));
    let table = lake.executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &lake.runtime,
            &crate::iceberg::TableIdent::new(
                crate::iceberg::NamespaceIdent::new(namespace.to_string()),
                name.into(),
            ),
        ),
    );
    assert_eq!(table.metadata().current_snapshot_id(), None);
    assert!(
        lake.executor
            .block_on(crate::manifest::extract_data_files_with_stats(&table))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        physical_bytes(lake._warehouse.path()),
        before,
        "abort must not publish metadata or any data/delete artifact"
    );
}

#[test]
fn ordinary_equality_delete_sessions_apply_the_existing_table_shape_guards() {
    use novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest;
    use novarocks_spi::connector::{ConnectorErrorKind, ConnectorWriteFieldRequest};
    for (version, partitioned, expected) in [
        (
            crate::iceberg::spec::FormatVersion::V1,
            false,
            Some("ADD EQUALITY DELETE requires an Iceberg v2 or v3 table"),
        ),
        (
            crate::iceberg::spec::FormatVersion::V2,
            true,
            Some("ADD EQUALITY DELETE currently supports only unpartitioned iceberg tables"),
        ),
        (crate::iceberg::spec::FormatVersion::V2, false, None),
    ] {
        let lake = FrozenRewriteLake::new_version(version);
        let table_name = if partitioned {
            let schema = lake.table().metadata().current_schema().clone();
            let spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
                .add_partition_field("id", "id", crate::iceberg::spec::Transform::Identity)
                .unwrap()
                .build()
                .unwrap();
            lake.executor.block_on(
                crate::catalog::admission_test_support::create_static_fixture_table(
                    &lake.runtime,
                    lake.control.binding_key().incarnation,
                    crate::catalog::CatalogNamespaceName::new("partition_db"),
                    crate::iceberg::TableCreation::builder()
                        .name("partitioned".into())
                        .schema(schema.as_ref().clone())
                        .partition_spec(spec.into_unbound())
                        .format_version(version)
                        .build(),
                ),
            );
            "partition_db.partitioned"
        } else {
            "db.t"
        };
        let before = physical_bytes(lake._warehouse.path());
        let context = request_context();
        let result = lake.control.begin_write(ConnectorWriteBeginRequest {
            table: Arc::from(table_name),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: vec![
                    ConnectorWriteFieldRequest::try_new_typed(
                        Field::new("id", DataType::Int64, false),
                        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                    )
                    .unwrap(),
                ],
            },
            base: None,
            flavor: ConnectorWriteSessionFlavor::Ordinary,
            context: context.clone(),
        });
        match result {
            Err(error) => {
                let expected = expected.unwrap_or_else(|| panic!("valid equality delete: {error}"));
                assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
                assert_eq!(error.message(), expected);
            }
            Ok(session) => {
                lake.control
                    .abort_write(ConnectorWriteSessionAbortRequest {
                        commit: session.commit_handle(),
                        context,
                    })
                    .unwrap();
                assert!(
                    expected.is_none(),
                    "unsupported equality-delete session was admitted: {expected:?}"
                );
            }
        }
        assert_eq!(physical_bytes(lake._warehouse.path()), before);
    }
}
