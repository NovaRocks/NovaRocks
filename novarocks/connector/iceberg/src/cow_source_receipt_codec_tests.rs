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

// These are immutable typed-fact/codec laws, not an SDK observation or a
// substitute for the native original COW producer/adoption source witness.

use crate::{
    delete_semantics::{PinnedEndpointFacts, ReadDomain, ReadObservationId},
    iceberg::spec::{NestedField, PrimitiveType, Schema, Type},
    provider_types::IcebergReadTypes,
    typed_read::schema_binding::{
        ALWAYS_BOUND_METADATA_COLUMNS, IcebergMetadataColumn, ROW_LINEAGE_METADATA_COLUMNS,
    },
    typed_read::{
        IcebergColumnHandle, IcebergPinnedDataFileSet, IcebergRuntimeRelation, IcebergTableHandle,
        IcebergTableHandleParams,
    },
};
use novarocks_spi::connector::read_stack::{SchemaTableName, TupleDomain};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecErrorKind,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEnvelopeHeader,
    ConnectorInstanceId,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

fn original_table_facts() -> IcebergTableHandle {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Int),
        ))])
        .build()
        .unwrap();
    let domain = Arc::new(ReadDomain::new(
        ReadObservationId::try_new([31; 16]).unwrap(),
        PinnedEndpointFacts::try_new(
            uuid::Uuid::from_bytes([32; 16]),
            "s3://fixture/metadata/observed.json",
            41,
            &schema,
            &[],
        )
        .unwrap(),
    ));
    IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: SchemaTableName::try_new("fixture", "mutation").unwrap(),
        snapshot_id: Some(41),
        read_domain: Some(domain),
        table_schema_json: serde_json::to_string(&schema).unwrap(),
        spec_id: None,
        partition_spec_jsons: BTreeMap::new(),
        format_version: 3,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: BTreeSet::new(),
        name_mapping_json: None,
        table_location: "s3://fixture/mutation".into(),
        storage_properties: BTreeMap::new(),
        pinned_data_files: Some(
            IcebergPinnedDataFileSet::try_new(["s3://fixture/data.parquet"]).unwrap(),
        ),
    })
    .unwrap()
}
fn source_proof_facts() -> IcebergTableHandle {
    let mut raw = original_table_facts().to_proto();
    // Values are explicit wire-fact fixture data, never a minted source receipt.
    let proof = raw.frozen_cow_source.get_or_insert_with(Default::default);
    proof.source_digest = vec![37; 32];
    proof.signed_base = vec![38; 32];
    proof.data_file_path = "s3://fixture/data.parquet".into();
    for metadata in ALWAYS_BOUND_METADATA_COLUMNS
        .into_iter()
        .chain(ROW_LINEAGE_METADATA_COLUMNS)
    {
        proof.metadata_columns.push(
            IcebergColumnHandle::base_column(&NestedField::required(
                metadata.field_id(),
                metadata.column_name(),
                Type::Primitive(metadata.declared_type()),
            ))
            .unwrap()
            .to_proto(),
        );
    }
    IcebergTableHandle::from_proto(&raw).unwrap()
}
fn header() -> ConnectorEnvelopeHeader {
    let definition = crate::definition::iceberg_contract_definition().unwrap();
    let codecs = definition.read().codecs().declarations();
    let declaration = codecs
        .into_iter()
        .find(|d| d.category() == ConnectorCodecCategory::ReadTable)
        .unwrap();
    ConnectorEnvelopeHeader::new(
        declaration.provider_id().clone(),
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("fixture").unwrap(),
            CatalogVersion::from_bytes([39; 32]),
        ),
        ConnectorCodecCategory::ReadTable,
        declaration.revision(),
    )
}
fn limits(retained: usize) -> ConnectorDecodeLimits {
    ConnectorDecodeLimits::try_new(1 << 20, retained, 1 << 20, 100_000, 64).unwrap()
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if let Some((at, cause)) = self.refusal
            && trace.len() == at
        {
            return Err(cause);
        }
        Ok(())
    }
}

#[test]
fn cow_source_receipt_codec_keeps_original_domain_pin_and_signed_fields() {
    let table = source_proof_facts();
    let h = header();
    let mut ledger = ConnectorDecodeLedger::new(limits(1 << 20));
    let payload = IcebergReadTypes::wire_codecs()
        .encode_table(&IcebergRuntimeRelation::Table(table.clone()))
        .unwrap();
    let decoded = IcebergReadTypes::wire_codecs()
        .decode_table(&payload, &mut ConnectorDecodeContext::new(&h, &mut ledger))
        .unwrap();
    let IcebergRuntimeRelation::Table(decoded) = decoded else {
        panic!("table source")
    };
    assert_eq!(decoded, table);
    assert_eq!(decoded.read_domain(), table.read_domain());
    let proof = decoded.frozen_cow_source().unwrap();
    assert_eq!(proof.metadata_columns().len(), 4);
    assert!(proof.metadata_columns().iter().all(|c| !c.nullable()));
    assert!(original_table_facts().frozen_cow_source().is_none());
    assert!(IcebergMetadataColumn::LastUpdatedSequenceNumber.nullable());
}
#[test]
fn cow_source_receipt_codec_rejects_wrong_pin_and_nullable_proof() {
    let table = source_proof_facts();
    let mut raw = table.to_proto();
    raw.frozen_cow_source.as_mut().unwrap().data_file_path = "s3://fixture/other.parquet".into();
    let error = IcebergTableHandle::from_proto(&raw).unwrap_err();
    assert_eq!(
        error.message(),
        "Iceberg COW proof differs from its frozen table source"
    );
    let mut raw = table.to_proto();
    raw.frozen_cow_source.as_mut().unwrap().metadata_columns[3].nullable = true;
    let error = IcebergTableHandle::from_proto(&raw).unwrap_err();
    assert_eq!(
        error.message(),
        "Iceberg COW proof metadata fields differ from the signed source"
    );
}
#[test]
fn cow_source_receipt_codec_three_compile_causes_preserve_actual_prefix() {
    let payload = IcebergReadTypes::wire_codecs()
        .encode_table(&IcebergRuntimeRelation::Table(source_proof_facts()))
        .unwrap();
    let h = header();
    let success = Control::default();
    let mut ledger = ConnectorDecodeLedger::new(limits(1 << 20));
    let mut context =
        ConnectorDecodeContext::try_new_for_compile(&h, &mut ledger, &success).unwrap();
    IcebergReadTypes::wire_codecs()
        .decode_table(&payload, &mut context)
        .unwrap();
    let trace = success.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let control = Control {
                refusal: Some((at, cause)),
                ..Default::default()
            };
            let mut ledger = ConnectorDecodeLedger::new(limits(1 << 20));
            let result =
                match ConnectorDecodeContext::try_new_for_compile(&h, &mut ledger, &control) {
                    Err(error) => Err(error),
                    Ok(mut context) => {
                        IcebergReadTypes::wire_codecs().decode_table(&payload, &mut context)
                    }
                };
            assert_eq!(result.unwrap_err().compile_control_error(), Some(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
#[test]
fn cow_source_receipt_codec_real_ledger_refusal_is_capacity() {
    let payload = IcebergReadTypes::wire_codecs()
        .encode_table(&IcebergRuntimeRelation::Table(source_proof_facts()))
        .unwrap();
    let h = header();
    let mut ledger = ConnectorDecodeLedger::new(limits(1));
    let error = IcebergReadTypes::wire_codecs()
        .decode_table(&payload, &mut ConnectorDecodeContext::new(&h, &mut ledger))
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorCodecErrorKind::Capacity);
}
