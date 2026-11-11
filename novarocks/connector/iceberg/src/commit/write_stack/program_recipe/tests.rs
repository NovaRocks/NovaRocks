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

//! Actual handle authors and the complete-input pure seal; no runtime factory.

use super::super::{domain::*, test_support::table_facts};
use super::*;
use crate::delete_file::IcebergFileFormat;
use crate::scan_model::IcebergSchemaFieldDef;
use crate::wire::dto;
use arrow::datatypes::{DataType, Field};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorWriteBinding, ConnectorWriteFieldBinding,
    ConnectorWriteFieldToken, ConnectorWriteRecipe, ConnectorWriteRecipeCompileError,
    PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
};
use novarocks_type_contract::CompileControlError;
use parquet::{arrow::PARQUET_FIELD_ID_META_KEY, basic::Compression};
use prost::Message;
use std::{collections::HashMap, sync::Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if let Some((at, cause)) = self.refusal.clone()
            && trace.len() == at
        {
            return Err(cause);
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
fn field(
    name: &str,
    ty: DataType,
    nullable: bool,
    id: i32,
    token: u32,
) -> ConnectorWriteFieldBinding {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&token.to_be_bytes());
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes(bytes),
        Field::new(name, ty, nullable).with_metadata(HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.into(), id.to_string()),
            ("source-marker".into(), format!("source-{token}")),
        ])),
    )
}
fn source(name: &str, id: i32) -> IcebergSchemaFieldDef {
    IcebergSchemaFieldDef {
        field_id: id,
        is_variant: false,
        name: name.into(),
        children: vec![],
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
    }
}
fn output(format: IcebergFileFormat) -> IcebergWriterOutput {
    IcebergWriterOutput::try_new(
        format,
        Compression::SNAPPY,
        (format == IcebergFileFormat::Parquet).then_some(1024),
    )
    .unwrap()
}
fn data_handle(fields: Option<Vec<IcebergSchemaFieldDef>>) -> dto::IcebergWriterHandle {
    let handle = IcebergWriterHandle::try_new_data(
        table_facts(),
        output(IcebergFileFormat::Parquet),
        IcebergDataBranchRecipe::try_new(
            fields.map(|fields| serde_json::json!({"type":"struct", "schema-id":1, "fields":fields.iter().map(|field| serde_json::json!({"id":field.field_id,"name":field.name,"required":false,"type":"long"})).collect::<Vec<_>>()}).to_string()),
            vec![],
            vec![],
            vec![],
            false,
        )
        .unwrap(),
    )
    .unwrap();
    encode(&handle)
}
fn encode(handle: &IcebergWriterHandle) -> dto::IcebergWriterHandle {
    IcebergWriteValueCodec::new("lake")
        .encode_writer_handle_value(handle)
        .unwrap()
}
fn delete_handle(branch: IcebergWriteBranch) -> dto::IcebergWriterHandle {
    let format = if branch == IcebergWriteBranch::DeletionVector {
        IcebergFileFormat::Puffin
    } else {
        IcebergFileFormat::Parquet
    };
    use crate::commit::write_stack::domain::{IcebergMutationReadRecipe, IcebergMutationSnapshot};
    let table = table_facts();
    let recipe = IcebergMutationReadRecipe::try_new(
        &table,
        "s3://b/wh/db/t/metadata/v4.metadata.json".to_owned(),
        format!("sha256:{}", "a".repeat(64)),
        Some(
            IcebergMutationSnapshot::try_new("s3://b/wh/db/t/metadata/snap-77.avro".to_owned(), 1)
                .unwrap(),
        ),
    )
    .unwrap();
    let group = (branch == IcebergWriteBranch::DeletionVector).then(|| {
        novarocks_connector_contract::ConnectorMutationContributionGroupId::from_bytes([4; 32])
    });
    encode(
        &IcebergWriterHandle::try_new_delete(branch, table, output(format))
            .unwrap()
            .try_with_mutation_read(recipe, group)
            .unwrap(),
    )
}
fn equality_handle() -> dto::IcebergWriterHandle {
    // Target IDs deliberately differ from public source metadata. Match order
    // is authored independently of target ID order, as the existing owner does.
    let columns = [("b", 22, "Utf8", true), ("a", 11, "Int64", false)]
        .into_iter()
        .map(|(name, id, _ty, _nullable)| {
            IcebergEqualityDeleteColumnFacts::try_new(name.into(), id).unwrap()
        })
        .collect();
    encode(
        &IcebergWriterHandle::try_new_equality_delete(
            table_facts(),
            output(IcebergFileFormat::Parquet),
            IcebergEqualityDeleteRecipe::try_new(columns, serde_json::json!({"type":"struct","schema-id":1,"fields":[{"id":22,"name":"b","required":false,"type":"string"},{"id":11,"name":"a","required":true,"type":"long"}]}).to_string()).unwrap(),
        )
        .unwrap(),
    )
}
fn equality_input() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::EqualityDelete {
        equality_fields: vec![
            field("b", DataType::Utf8, true, 922, 2),
            field("a", DataType::Int64, false, 911, 1),
        ],
    }
}
fn identity() -> Vec<ConnectorWriteFieldBinding> {
    vec![
        field("_file", DataType::Utf8, true, 700, 7),
        field("_pos", DataType::Int64, false, 701, 8),
    ]
}
fn data_input() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::Data {
        fields: vec![field("k1", DataType::Int64, false, 900, 1)],
    }
}
fn draft(
    raw: &dto::IcebergWriterHandle,
    input: ConnectorWriteInputShape,
) -> ConnectorWriteRecipeDraft {
    draft_with(
        raw.encode_to_vec(),
        input,
        crate::PROVIDER_ID,
        crate::wire::write::WRITE_CODEC_REVISION,
    )
}
fn draft_with(
    bytes: Vec<u8>,
    input: ConnectorWriteInputShape,
    provider: &str,
    revision: u32,
) -> ConnectorWriteRecipeDraft {
    let id = ConnectorInstanceId::try_from_canonical("lake").unwrap();
    let catalog = CatalogHandle::new(id.clone(), CatalogVersion::from_bytes([7; 32]));
    let provider = ConnectorProviderId::parse(provider).unwrap();
    let binding = ConnectorWriteBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: id,
        },
        catalog.clone(),
    );
    let payload = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(revision).unwrap(),
        ),
        bytes::Bytes::from(bytes),
    );
    ConnectorWriteRecipeDraft::try_new(binding, payload, input).unwrap()
}
fn compile(
    draft: &ConnectorWriteRecipeDraft,
    control: &Control,
) -> Result<ConnectorWriteRecipe, ConnectorWriteRecipeCompileError<ConnectorCodecError>> {
    ConnectorWriteRecipe::try_compile_with_provider(draft, &IcebergWriteRecipeCompiler, control)
}
fn assert_unchanged(draft: &ConnectorWriteRecipeDraft) {
    let compiled = compile(draft, &Control::default()).unwrap();
    assert_eq!(compiled.draft(), draft);
    assert!(std::ptr::eq(compiled.draft().input(), draft.input()));
    assert_eq!(compiled.draft().charged_bytes(), draft.charged_bytes());
    for (actual, original) in compiled
        .draft()
        .input()
        .fields_iter()
        .zip(draft.input().fields_iter())
    {
        assert_eq!(actual.token(), original.token());
        assert!(novarocks_connector_contract::arrow_fields_exact(
            actual.field(),
            original.field()
        ));
        assert_eq!(actual.field().metadata(), original.field().metadata());
    }
}
fn assert_provider_failure(draft: &ConnectorWriteRecipeDraft, text: &str) {
    match compile(draft, &Control::default()) {
        Err(ConnectorWriteRecipeCompileError::Provider(error)) => {
            assert!(error.to_string().contains(text), "{error}");
            assert_eq!(error.compile_control_error(), None);
        }
        other => panic!("expected provider refusal: {other:?}"),
    }
}

#[test]
fn data_private_recipe_keeps_all_actual_public_flavors_and_exact_fields() {
    let raw = data_handle(Some(vec![source("k1", 1), source("v1", 2)]));
    let shapes = [
        data_input(),
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field("k1", DataType::Int64, false, 900, 1)],
            row_identity_fields: identity(),
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![field("k1", DataType::Int64, false, 900, 1)],
            row_identity_fields: vec![
                field("_row_id", DataType::Int64, false, 702, 9),
                field(
                    "_last_updated_sequence_number",
                    DataType::Int64,
                    true,
                    703,
                    10,
                ),
            ],
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: identity(),
            partition_source_fields: vec![],
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: identity(),
            partition_source_fields: vec![],
        },
    ];
    for shape in shapes {
        assert_unchanged(&draft(&raw, shape));
    }
    assert_unchanged(&draft(&equality_handle(), equality_input()));
}

#[test]
fn private_delete_recipes_preserve_public_positional_columns_and_groups() {
    for branch in [
        IcebergWriteBranch::PositionDelete,
        IcebergWriteBranch::DeletionVector,
    ] {
        let raw = delete_handle(branch);
        let make = |fields| {
            if branch == IcebergWriteBranch::PositionDelete {
                ConnectorWriteInputShape::PositionDelete {
                    identity_fields: fields,
                    partition_source_fields: vec![],
                }
            } else {
                ConnectorWriteInputShape::DeletionVector {
                    identity_fields: fields,
                    partition_source_fields: vec![],
                }
            }
        };
        assert_unchanged(&draft(
            &raw,
            make(vec![
                field("_FiLe", DataType::Utf8, true, 700, 7),
                field("_PoS", DataType::Int64, true, 701, 8),
            ]),
        ));
        // The public declaration preserves field roles and tokens for native
        // planning of the physical position input.
        assert_unchanged(&draft(
            &raw,
            make(vec![
                field("path_alias", DataType::Utf8, false, 710, 17),
                field("position_alias", DataType::Int64, true, 711, 18),
                field("extra_identity", DataType::Int32, true, 712, 19),
            ]),
        ));
        let across_groups = if branch == IcebergWriteBranch::PositionDelete {
            ConnectorWriteInputShape::PositionDelete {
                identity_fields: vec![field("path", DataType::Utf8, true, 710, 17)],
                partition_source_fields: vec![field("position", DataType::Int64, true, 711, 18)],
            }
        } else {
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: vec![field("path", DataType::Utf8, true, 710, 17)],
                partition_source_fields: vec![field("position", DataType::Int64, true, 711, 18)],
            }
        };
        assert_unchanged(&draft(&raw, across_groups));
        let mut reversed = identity();
        reversed.reverse();
        assert_provider_failure(&draft(&raw, make(reversed)), "first input columns");
        assert_provider_failure(
            &draft(
                &raw,
                make(vec![
                    field("_file", DataType::Binary, true, 700, 7),
                    field("_pos", DataType::Int64, true, 701, 8),
                ]),
            ),
            "first input columns",
        );
        assert_provider_failure(&draft(&raw, data_input()), "public input shape");
    }
    assert_provider_failure(
        &draft(
            &delete_handle(IcebergWriteBranch::PositionDelete),
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: identity(),
                partition_source_fields: vec![],
            },
        ),
        "public input shape",
    );
}

#[test]
fn equality_requires_exact_names_order_and_public_role_with_provider_target_types() {
    let raw = equality_handle();
    assert_unchanged(&draft(&raw, equality_input()));
    let variants = [
        vec![
            field("B", DataType::Utf8, true, 922, 2),
            field("a", DataType::Int64, false, 911, 1),
        ],
        vec![
            field("b", DataType::Binary, true, 922, 2),
            field("a", DataType::Int64, false, 911, 1),
        ],
        vec![
            field("a", DataType::Int64, false, 911, 1),
            field("b", DataType::Utf8, true, 922, 2),
        ],
    ];
    for fields in variants {
        let candidate = draft(
            &raw,
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: fields,
            },
        );
        assert!(matches!(
            compile(&candidate, &Control::default()),
            Err(ConnectorWriteRecipeCompileError::Provider(_))
        ));
    }
    // Carrier type and nullable differences no longer replace provider target
    // facts. The closed batch adapter checks actual values before file output.
    assert_unchanged(&draft(
        &raw,
        ConnectorWriteInputShape::EqualityDelete {
            equality_fields: vec![
                field(
                    "b",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8)),
                    false,
                    922,
                    2,
                ),
                field("a", DataType::Int64, true, 911, 1),
            ],
        },
    ));
    assert_provider_failure(
        &draft(
            &raw,
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: vec![field("b", DataType::Utf8, true, 922, 2)],
            },
        ),
        "names 2 columns",
    );
    assert_provider_failure(&draft(&raw, data_input()), "public input shape");
    assert_provider_failure(
        &draft(&data_handle(Some(vec![source("k1", 1)])), equality_input()),
        "public input shape",
    );
}

#[test]
fn data_requires_real_private_source_and_keeps_shared_schema_refusals() {
    assert_provider_failure(
        &draft(&data_handle(None), data_input()),
        "frozen input schema",
    );
    assert_provider_failure(
        &draft(&data_handle(Some(vec![source("another", 1)])), data_input()),
        "absent from frozen target schema",
    );
    let mut raw = data_handle(Some(vec![source("k1", 1)]));
    assert_provider_failure(
        &draft(
            &raw,
            ConnectorWriteInputShape::Data {
                fields: vec![field(
                    "k1",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    false,
                    900,
                    1,
                )],
            },
        ),
        "unsupported",
    );
    raw.data.as_mut().unwrap().target_schema_json=Some(serde_json::json!({"type":"struct","schema-id":1,"fields":[{"id":1,"name":"k1","required":false,"type":"int"}]}).to_string());
    assert_unchanged(&draft(
        &raw,
        ConnectorWriteInputShape::Data {
            fields: vec![field("k1", DataType::Int8, false, 900, 1)],
        },
    ));
}

#[test]
fn private_wire_and_exact_provider_revision_cannot_fall_back_to_validated_draft() {
    let raw = data_handle(Some(vec![source("k1", 1)]));
    assert_provider_failure(
        &draft_with(
            raw.encode_to_vec(),
            data_input(),
            "paimon",
            crate::wire::write::WRITE_CODEC_REVISION,
        ),
        "another provider identity",
    );
    let wrong_revision = draft_with(
        raw.encode_to_vec(),
        data_input(),
        crate::PROVIDER_ID,
        crate::wire::write::WRITE_CODEC_REVISION + 1,
    );
    assert!(matches!(
        compile(&wrong_revision, &Control::default()),
        Err(ConnectorWriteRecipeCompileError::Provider(_))
    ));
    let mut malformed = raw.encode_to_vec();
    malformed.extend_from_slice(&[0x98, 0x06, 0]);
    assert_provider_failure(
        &draft_with(
            malformed,
            data_input(),
            crate::PROVIDER_ID,
            crate::wire::write::WRITE_CODEC_REVISION,
        ),
        "unknown",
    );
    let mut no_table = raw;
    no_table.table = None;
    assert!(matches!(
        compile(&draft(&no_table, data_input()), &Control::default()),
        Err(ConnectorWriteRecipeCompileError::Provider(_))
    ));
}

fn wide_data() -> ConnectorWriteRecipeDraft {
    let mut fields = vec![];
    let mut source_fields = vec![];
    for i in 0..320 {
        let name = format!("column_{i}");
        fields.push(field(
            &name,
            DataType::Int64,
            i % 2 == 0,
            i + 1000,
            i as u32 + 1,
        ));
        source_fields.push(source(&name, i + 1));
    }
    draft(
        &data_handle(Some(source_fields)),
        ConnectorWriteInputShape::Data { fields },
    )
}

#[test]
fn actual_long_schema_and_copy_keep_original_entry_quantum_and_tail_causes() {
    let draft = wide_data();
    let success = Control::default();
    compile(&draft, &success).unwrap();
    let trace = success.trace.lock().unwrap().clone();
    let quanta: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(i, (_, units))| (*units == 256).then_some(i + 1))
        .collect();
    assert!(!quanta.is_empty());
    let mut stops = vec![
        1,
        quanta[0],
        quanta[quanta.len() / 2],
        *quanta.last().unwrap(),
        trace.len(),
    ];
    stops.sort_unstable();
    stops.dedup();
    for cause in causes() {
        for &at in &stops {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(compile(&draft, &control), Err(ConnectorWriteRecipeCompileError::Control(error))
                if error == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn actual_input_field_copy_refuses_before_publication_without_rechecking_control() {
    let draft = wide_data();
    let success = Control::default();
    let mut work = CompileCheckpoints::try_new(&success, CompilePhase::ProviderValidation).unwrap();
    super::input_schema(draft.input(), &mut work).unwrap();
    work.finish().unwrap();
    let trace = success.trace.lock().unwrap().clone();
    let stops = [1, trace.len() / 2, trace.len()];
    for cause in causes() {
        for at in stops {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            let result = (|| {
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation)?;
                super::input_schema(draft.input(), &mut work)?;
                work.finish()?;
                Ok::<(), Failure>(())
            })();
            assert!(matches!(result, Err(Failure::Control(error)) if error == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn ordinary_source_failure_keeps_original_provider_fact_and_actual_control_prefix() {
    use novarocks_failure_contract::FailureProjection;
    let draft = draft(&data_handle(None), data_input());
    assert_provider_failure(&draft, "frozen input schema");
    let baseline = Control::default();
    let Err(ConnectorWriteRecipeCompileError::Provider(original)) = compile(&draft, &baseline)
    else {
        panic!("original frozen input error required");
    };
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for cause in causes() {
        for at in 1..=trace.len() {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(compile(&draft, &control), Err(ConnectorWriteRecipeCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
        let late = Control {
            refusal: Some((trace.len() + 1, cause)),
            ..Control::default()
        };
        let Err(ConnectorWriteRecipeCompileError::Provider(actual)) = compile(&draft, &late) else {
            panic!("late callback replaced frozen input error");
        };
        assert_eq!(actual.failure_fact(), original.failure_fact());
        assert_eq!(actual.path(), original.path());
        assert_eq!(actual.kind(), original.kind());
        assert_eq!(actual.detail(), original.detail());
        assert_eq!(*late.trace.lock().unwrap(), trace);
    }
}

#[test]
fn installed_pure_write_attachment_uses_real_compiler_without_runtime_adapter() {
    let provider = ConnectorProviderId::parse(crate::PROVIDER_ID).unwrap();
    let control = Control::default();
    let catalogue = PureProviderProgramCatalog::try_new(
        &[PureProviderManifestEntry::new(
            provider.clone(),
            false,
            true,
        )],
        vec![PureProviderProgramDefinition::new(
            provider,
            None,
            Some(Arc::new(IcebergWriteRecipeCompiler)),
        )],
        &control,
    )
    .unwrap();
    let draft = draft(&equality_handle(), equality_input());
    let compiled = catalogue.compile_write(&draft, &control).unwrap();
    assert_eq!(compiled.draft(), &draft);
    assert!(std::ptr::eq(compiled.draft().input(), draft.input()));
    // This single installed facet is a fixture, not a complete Server manifest
    // or native acceptance and grants no runtime or memory capability.
}
