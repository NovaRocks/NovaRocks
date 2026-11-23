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
use novarocks_spi::connector::ConnectorWriteInputRequest;
use std::fs;

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
    let table = lake.executor.block_on(
        crate::catalog::admission_test_support::load_static_fixture_table(
            &lake.runtime,
            &crate::iceberg::TableIdent::new(
                crate::iceberg::NamespaceIdent::new(namespace.to_string()),
                name.into(),
            ),
        ),
    );
    let shape = if variant {
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields: Vec::new(),
            partition_source_fields: Vec::new(),
        }
    } else {
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields: Vec::new(),
            partition_source_fields: Vec::new(),
        }
    };
    let input = crate::commit::write_preparation::freeze_position_mutation_input(
        table.metadata(),
        "main",
        None,
        &shape,
        &context,
    )
    .unwrap();
    let before = physical_bytes(lake._warehouse.path());
    let session = lake
        .control
        .begin_write(ConnectorWriteBeginRequest {
            catalog_session: Default::default(),
            table: Arc::from(format!("{namespace}.{name}")),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input,
            base: Some(preparation.base_version().clone()),
            flavor: ConnectorWriteSessionFlavor::RowMutation {
                admission: preparation.admission(),
            },
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
    assert!(writer.mutation_read().unwrap().snapshot().is_none());
    assert!(matches!(
        lake.control
            .abort_write(ConnectorWriteSessionAbortRequest {
                commit: session.commit_session(),
                production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::NotStarted,
                context,
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
            catalog_session: Default::default(),
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
                        commit: session.commit_session(),
                        production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::NotStarted,
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
