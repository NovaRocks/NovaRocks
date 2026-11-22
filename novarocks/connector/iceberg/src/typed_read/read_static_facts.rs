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

//! One provider-owned projection of frozen Iceberg source and selection facts.
//!
//! This owner never opens metadata, enumerates splits or acquires runtime access.

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorReadArtifactCoverage, ConnectorReadDistribution, ConnectorReadInputVersion,
    ConnectorReadProperties, ConnectorReadStaticFacts,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use prost::Message;
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::column_handle::invalid;
use super::column_handle::unsupported;
use super::{IcebergColumnHandle, IcebergRuntimeRelation};

#[derive(Debug)]
pub(crate) enum ReadStaticFactsError {
    Source(ConnectorError),
    Control(CompileControlError),
}
impl std::fmt::Display for ReadStaticFactsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => std::fmt::Display::fmt(error, f),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ReadStaticFactsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
impl From<ConnectorError> for ReadStaticFactsError {
    fn from(error: ConnectorError) -> Self {
        Self::Source(error)
    }
}
impl From<CompileControlError> for ReadStaticFactsError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

struct Work<'a>(Option<CompileCheckpoints<'a>>);
impl Work<'_> {
    fn step(&mut self) -> Result<(), ReadStaticFactsError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ReadStaticFactsError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    // Frozen domain constructors bound their inputs. Prost, DTO authoring and
    // immutable constructor internals remain opaque here: these boundaries do
    // not claim internal cooperation or host allocation authorization.
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, ReadStaticFactsError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        Ok(result)
    }
    fn opaque_source<T>(
        &mut self,
        operation: impl FnOnce() -> Result<T, ConnectorError>,
    ) -> Result<T, ReadStaticFactsError> {
        self.flush()?;
        let value = operation()?;
        self.flush()?;
        Ok(value)
    }
}

pub(crate) fn iceberg_final_static_facts(
    relation: &IcebergRuntimeRelation,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ConnectorError> {
    match project(relation, &mut Work(None)) {
        Ok(value) => Ok(value),
        Err(ReadStaticFactsError::Source(error)) => Err(error),
        Err(ReadStaticFactsError::Control(_)) => {
            unreachable!("legacy source projection has no control port")
        }
    }
}

pub(crate) fn iceberg_final_static_facts_for_compile(
    relation: &IcebergRuntimeRelation,
    control: &dyn PureCompileControl,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    let mut work = Work(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = project(relation, &mut work);
    work.0
        .expect("compile projection retains its original checkpoint scope")
        .finish_result(result, ReadStaticFactsError::Control)
}

fn project(
    relation: &IcebergRuntimeRelation,
    work: &mut Work<'_>,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    match relation {
        IcebergRuntimeRelation::Table(handle) => {
            let input = work.opaque(|| {
                let mut input = handle.to_proto();
                input.unenforced_predicate = None;
                input.enforced_predicate = None;
                input.limit = None;
                input.projected_columns.clear();
                input.encode_to_vec()
            })?;
            let selection = work.opaque(|| handle.to_proto().encode_to_vec())?;
            static_facts_from_bytes(
                b"iceberg-final-static-table-v1",
                &input,
                &selection,
                unconstrained_read_properties(work)?,
                ConnectorReadArtifactCoverage::NoArtifactInputs,
                work,
            )
        }
        IcebergRuntimeRelation::SystemTable(reference) => {
            let frozen = work.opaque(|| reference.to_proto().encode_to_vec())?;
            let properties = unconstrained_read_properties(work)?;
            let input_digest =
                static_digest(b"iceberg-final-static-system-table-v1", &frozen, work)?;
            let selection_digest =
                static_digest(b"iceberg-final-static-system-table-v1", &frozen, work)?;
            work.opaque_source(|| {
                ConnectorReadStaticFacts::try_new(
                    ConnectorReadInputVersion::try_new(input_digest.as_slice())?,
                    selection_digest,
                    properties,
                    ConnectorReadArtifactCoverage::NoArtifactInputs,
                    frozen,
                )
            })
        }
        IcebergRuntimeRelation::ChangeWindow(handle) => {
            let frozen = work.opaque(|| handle.to_proto().encode_to_vec())?;
            static_facts_from_bytes(
                b"iceberg-final-static-change-window-v1",
                &frozen,
                &frozen,
                unconstrained_read_properties(work)?,
                ConnectorReadArtifactCoverage::NoArtifactInputs,
                work,
            )
        }
        IcebergRuntimeRelation::TableFunction(_) => Err(unsupported(
            "iceberg table-function relations do not publish final static read facts",
        )
        .into()),
        IcebergRuntimeRelation::MergeTable(_) => Err(unsupported(
            "iceberg merge relations do not publish final static read facts",
        )
        .into()),
    }
}

fn static_facts_from_bytes(
    domain: &[u8],
    input_identity: &[u8],
    selection: &[u8],
    properties: ConnectorReadProperties<IcebergColumnHandle>,
    artifact_coverage: ConnectorReadArtifactCoverage,
    work: &mut Work<'_>,
) -> Result<ConnectorReadStaticFacts<IcebergColumnHandle>, ReadStaticFactsError> {
    let input_digest = static_digest(domain, input_identity, work)?;
    let selection_digest = static_digest(domain, selection, work)?;
    work.opaque_source(|| {
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new(input_digest.as_slice())?,
            selection_digest,
            properties,
            artifact_coverage,
            Vec::new(),
        )
    })
}

fn unconstrained_read_properties(
    work: &mut Work<'_>,
) -> Result<ConnectorReadProperties<IcebergColumnHandle>, ReadStaticFactsError> {
    work.opaque_source(|| {
        ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, Vec::new())
    })
}

fn static_digest(
    domain: &[u8],
    bytes: &[u8],
    work: &mut Work<'_>,
) -> Result<[u8; 32], ReadStaticFactsError> {
    let mut digest = Sha256::new();
    digest.update(domain);
    work.step()?;
    digest.update((bytes.len() as u64).to_be_bytes());
    work.step()?;
    for chunk in bytes.chunks(256) {
        digest.update(chunk);
        work.step()?;
    }
    let result = digest.finalize().into();
    work.step()?;
    Ok(result)
}

#[cfg(test)]
pub(crate) fn decode_sha256_hex(value: &str) -> Result<[u8; 32], ConnectorError> {
    match decode_hex(value, &mut Work(None)) {
        Ok(value) => Ok(value),
        Err(ReadStaticFactsError::Source(error)) => Err(error),
        Err(ReadStaticFactsError::Control(_)) => {
            unreachable!("legacy digest parsing has no control port")
        }
    }
}
#[cfg(test)]
fn decode_hex(value: &str, work: &mut Work<'_>) -> Result<[u8; 32], ReadStaticFactsError> {
    if value.len() != 64 {
        return Err(invalid("iceberg artifact digest is not a SHA-256 hex value").into());
    }
    let mut digest = [0_u8; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        let parsed = value
            .get(index * 2..index * 2 + 2)
            .ok_or_else(|| invalid("iceberg artifact digest is not valid hexadecimal"))
            .and_then(|pair| {
                u8::from_str_radix(pair, 16)
                    .map_err(|_| invalid("iceberg artifact digest is not valid hexadecimal"))
            })?;
        work.step()?;
        *byte = parsed;
    }
    Ok(digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_boundary::final_static_facts_tests::table_handle;
    use crate::typed_read::{
        IcebergChangeWindowHandle, IcebergChangeWindowHandleParams, IcebergSystemTableReference,
        IcebergSystemTableReferenceParams, IcebergSystemTableType, TableChangesFunctionHandle,
        TableChangesFunctionHandleParams,
    };
    use novarocks_spi::connector::read_stack::{ConnectorTableHandle as _, SchemaTableName};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((index, error)) = self.refusal.clone() {
                assert!(
                    calls.len() <= index,
                    "control must not be called after refusal"
                );
                if calls.len() == index {
                    return Err(error);
                }
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
    fn system(kind: IcebergSystemTableType) -> IcebergRuntimeRelation {
        IcebergRuntimeRelation::SystemTable(
            IcebergSystemTableReference::try_new(IcebergSystemTableReferenceParams {
                schema_table_name: SchemaTableName::try_new("db", "orders").unwrap(),
                system_table_type: kind,
                metadata_file_location: "s3://warehouse/db/orders/metadata/v1.json".to_string(),
                table_uuid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
                snapshot_id: Some(41),
            })
            .unwrap(),
        )
    }
    fn change() -> IcebergRuntimeRelation {
        let table = table_handle();
        let schema = table.parse_table_schema().unwrap();
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| IcebergColumnHandle::base_column(field))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        IcebergRuntimeRelation::ChangeWindow(
            IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
                schema_table_name: table.schema_table_name().clone(),
                table_schema_json: table.table_schema_json().to_string(),
                columns,
                name_mapping_json: None,
                from_snapshot_id_exclusive: 41,
                to_snapshot_id_inclusive: 42,
                from_read_domain: crate::delete_semantics::test_read_domain(&schema, &[], 41),
                to_read_domain: crate::delete_semantics::test_read_domain(&schema, &[], 42),
                partition_spec_jsons: BTreeMap::new(),
            })
            .unwrap(),
        )
    }
    fn unsupported_function() -> IcebergRuntimeRelation {
        let table = table_handle();
        let schema = table.parse_table_schema().unwrap();
        let columns = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| IcebergColumnHandle::base_column(field))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        IcebergRuntimeRelation::TableFunction(
            TableChangesFunctionHandle::try_new(TableChangesFunctionHandleParams {
                schema_table_name: table.schema_table_name().clone(),
                table_schema_json: table.table_schema_json().to_string(),
                columns,
                name_mapping_json: None,
                start_snapshot_id: 41,
                end_snapshot_id: 42,
            })
            .unwrap(),
        )
    }

    #[test]
    fn compile_source_facts_match_the_actual_legacy_author_for_all_four_producers() {
        for relation in [
            IcebergRuntimeRelation::Table(table_handle()),
            system(IcebergSystemTableType::Files),
            system(IcebergSystemTableType::Snapshots),
            change(),
        ] {
            let control = Control::default();
            let actual = iceberg_final_static_facts_for_compile(&relation, &control).unwrap();
            assert_eq!(actual, iceberg_final_static_facts(&relation).unwrap());
            assert!(control.calls.lock().unwrap().len() > 1);
        }
    }
    #[test]
    fn metadata_is_unconstrained_and_retains_coverage() {
        for kind in [
            IcebergSystemTableType::Files,
            IcebergSystemTableType::Entries,
            IcebergSystemTableType::Snapshots,
            IcebergSystemTableType::History,
            IcebergSystemTableType::Refs,
            IcebergSystemTableType::Manifests,
        ] {
            let relation = system(kind);
            let facts =
                iceberg_final_static_facts_for_compile(&relation, &Control::default()).unwrap();
            assert!(matches!(
                facts.properties().distribution(),
                ConnectorReadDistribution::Unconstrained
            ));
            let IcebergRuntimeRelation::SystemTable(reference) = relation else {
                unreachable!()
            };
            assert_eq!(
                facts.coverage_evidence(),
                reference.to_proto().encode_to_vec()
            );
        }
    }
    #[test]
    fn source_fact_entry_preserves_all_three_primary_controls() {
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((1, cause.clone())),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&IcebergRuntimeRelation::Table(table_handle()), &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), [0]);
        }
    }
    #[test]
    fn source_fact_hashing_refuses_at_256_real_chunks_without_publication_or_recheck() {
        let mut raw = table_handle().to_proto();
        raw.table_schema_json.push_str(&" ".repeat(256 * 260));
        let relation = IcebergRuntimeRelation::Table(
            super::super::IcebergTableHandle::from_proto(&raw).unwrap(),
        );
        let baseline = Control::default();
        iceberg_final_static_facts_for_compile(&relation, &baseline).unwrap();
        let calls = baseline.calls.lock().unwrap();
        let at = calls.iter().position(|units| *units == 256).unwrap() + 1;
        drop(calls);
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(control.calls.lock().unwrap().last(), Some(&256));
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
    #[test]
    fn unsupported_source_preserves_first_authored_error_and_prior_control_prefix() {
        use novarocks_failure_contract::FailureProjection;
        let relation = unsupported_function();
        let original = iceberg_final_static_facts(&relation).unwrap_err();
        let baseline = Control::default();
        let Err(ReadStaticFactsError::Source(actual)) =
            iceberg_final_static_facts_for_compile(&relation, &baseline)
        else {
            panic!("ordinary source error expected");
        };
        assert_eq!(actual, original);
        let trace = baseline.calls.lock().unwrap().clone();
        assert_eq!(trace, [0]);
        for cause in causes() {
            let early = Control {
                calls: Default::default(),
                refusal: Some((1, cause.clone())),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &early), Err(ReadStaticFactsError::Control(actual)) if actual == cause)
            );
            assert_eq!(*early.calls.lock().unwrap(), trace);
            let late = Control {
                calls: Default::default(),
                refusal: Some((2, cause)),
            };
            let Err(ReadStaticFactsError::Source(actual)) =
                iceberg_final_static_facts_for_compile(&relation, &late)
            else {
                panic!("late callback replaced the source error");
            };
            assert_eq!(actual, original);
            assert_eq!(actual.failure_fact(), original.failure_fact());
            assert_eq!(*late.calls.lock().unwrap(), trace);
        }
    }
    #[test]
    fn opaque_source_returns_original_error_before_late_control_but_still_observes_success() {
        use std::cell::Cell;
        for cause in causes() {
            let late = Control {
                calls: Default::default(),
                refusal: Some((3, cause.clone())),
            };
            let mut work = Work(Some(
                CompileCheckpoints::try_new(&late, CompilePhase::ProviderValidation).unwrap(),
            ));
            let original = invalid("original source refusal");
            let Err(ReadStaticFactsError::Source(actual)) =
                work.opaque_source(|| Err::<(), _>(original.clone()))
            else {
                panic!("late control replaced original source");
            };
            assert_eq!(actual, original);
            assert_eq!(*late.calls.lock().unwrap(), [0, 0]);
            for at in [2, 3] {
                let control = Control {
                    calls: Default::default(),
                    refusal: Some((at, cause.clone())),
                };
                let mut work = Work(Some(
                    CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation)
                        .unwrap(),
                ));
                let called = Cell::new(false);
                let result = work.opaque_source(|| {
                    called.set(true);
                    Ok(7)
                });
                assert!(
                    matches!(result, Err(ReadStaticFactsError::Control(actual)) if actual == cause)
                );
                assert_eq!(called.get(), at == 3);
                assert_eq!(*control.calls.lock().unwrap(), vec![0; at]);
            }
        }
    }

    #[test]
    fn already_latched_control_remains_first_when_finishing_an_authored_source_error() {
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((2, cause.clone())),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            work.step().unwrap();
            assert_eq!(work.flush(), Err(cause.clone()));
            let result: Result<(), ReadStaticFactsError> = work.finish_result(
                Err(ReadStaticFactsError::Source(invalid(
                    "later source refusal",
                ))),
                ReadStaticFactsError::Control,
            );
            assert!(
                matches!(result, Err(ReadStaticFactsError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), [0, 1]);
        }
    }

    #[test]
    fn successful_source_fact_tail_refusal_never_publishes() {
        let relation = system(IcebergSystemTableType::Snapshots);
        let baseline = Control::default();
        iceberg_final_static_facts_for_compile(&relation, &baseline).unwrap();
        let at = baseline.calls.lock().unwrap().len();
        for cause in causes() {
            let control = Control {
                calls: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(iceberg_final_static_facts_for_compile(&relation, &control),
                Err(ReadStaticFactsError::Control(error)) if error == cause)
            );
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
}
