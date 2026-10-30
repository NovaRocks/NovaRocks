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

// Private COW constructor specialization. Ordinary constructors remain unchanged.
// Original cancellation and retention accompany the actual constructors.
use crate::commit::write_stack::domain::{IcebergDataBranchRecipe, IcebergWriteTableFacts};
use crate::iceberg::spec::TableMetadata;
use crate::metadata::existing_serde_projection::{self as projection, OriginalScope};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::fmt;

pub(super) enum Failure<E> {
    Original(E),
    Provider(ConnectorError),
    Projection(projection::Error<E>),
    Overflow,
    ShapeChanged,
}
impl<E> fmt::Debug for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Original(_) => "CowBeginOwn::Original",
            Self::Provider(_) => "CowBeginOwn::Provider",
            Self::Projection(_) => "CowBeginOwn::Projection",
            Self::Overflow => "CowBeginOwn::Overflow",
            Self::ShapeChanged => "CowBeginOwn::ShapeChanged",
        })
    }
}
impl<E> fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
fn exact<E>(text: &str) -> Result<String, Failure<E>> {
    let mut out = String::with_capacity(text.len());
    out.push_str(text);
    Ok(out)
}
pub(super) struct LoadedFacts {
    facts: IcebergWriteTableFacts,
    // Same neutral holder; fields above exit before it. No activity lease.
    _original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}
impl LoadedFacts {
    pub(super) fn into_facts(self) -> IcebergWriteTableFacts {
        self.facts
    }
}
pub(super) struct LoadedFactsInput<'a, E> {
    pub(super) metadata: &'a TableMetadata,
    pub(super) namespace: &'a str,
    pub(super) table: &'a str,
    pub(super) target_ref: &'a str,
    pub(super) base_snapshot_id: Option<i64>,
    pub(super) base_sequence_number: i64,
    pub(super) schema_id: i32,
    pub(super) spec_id: i32,
    pub(super) format_version: u8,
    pub(super) scope: &'a dyn OriginalScope<E>,
}
pub(super) fn loaded_facts<E>(input: LoadedFactsInput<'_, E>) -> Result<LoadedFacts, Failure<E>> {
    let LoadedFactsInput {
        metadata,
        namespace,
        table,
        target_ref,
        base_snapshot_id,
        base_sequence_number,
        schema_id,
        spec_id,
        format_version,
        scope,
    } = input;
    scope.check_active().map_err(Failure::Original)?;
    let mut uuid_buf = [0u8; 36];
    let uuid = metadata.uuid().hyphenated().encode_lower(&mut uuid_buf);
    let explicit_data = metadata.properties().get("write.data.path");
    let prefix = metadata.location().trim_end_matches('/');
    let data_len = match explicit_data {
        Some(value) => value.len(),
        None => prefix.len().checked_add(5).ok_or(Failure::Overflow)?,
    };
    let guard = scope.original_guard();
    let data = match explicit_data {
        Some(value) => exact(value)?,
        None => {
            let mut out = String::with_capacity(data_len);
            out.push_str(prefix);
            out.push_str("/data");
            out
        }
    };
    // Original constructor preserves UUID/ref/location/sequence/version order.
    let facts = IcebergWriteTableFacts::try_new(
        exact(uuid)?,
        exact(namespace)?,
        exact(table)?,
        exact(metadata.location())?,
        data,
        exact(target_ref)?,
        base_snapshot_id,
        base_sequence_number,
        schema_id,
        spec_id,
        format_version,
    )
    .map_err(Failure::Provider)?;
    scope.check_active().map_err(Failure::Original)?;
    Ok(LoadedFacts {
        facts,
        _original: guard,
    })
}

struct Count(usize);
impl fmt::Write for Count {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
        Ok(())
    }
}
fn transform_len<E>(transform: &crate::iceberg::spec::Transform) -> Result<usize, Failure<E>> {
    let mut count = Count(0);
    fmt::write(&mut count, format_args!("{transform}")).map_err(|_| Failure::Overflow)?;
    Ok(count.0)
}
fn transform<E>(value: &crate::iceberg::spec::Transform) -> Result<String, Failure<E>> {
    let n = transform_len(value)?;
    let mut text = String::with_capacity(n);
    fmt::write(&mut text, format_args!("{value}")).map_err(|_| Failure::ShapeChanged)?;
    if text.len() != n {
        return Err(Failure::ShapeChanged);
    }
    Ok(text)
}

// Caller has already performed the original signed-input validation and
// Parquet parameter validation, in that order.
// No schema Serialize is moved before those original semantic first errors.
pub(super) fn data_recipe<E>(
    metadata: &TableMetadata,
    row_lineage: bool,
    scope: &dyn OriginalScope<E>,
) -> Result<IcebergDataBranchRecipe, Failure<E>> {
    let schema = metadata.current_schema();
    let fields = metadata.default_partition_spec().fields();
    let mut sources = Vec::with_capacity(fields.len());
    let mut names = Vec::with_capacity(fields.len());
    let mut transforms = Vec::with_capacity(fields.len());
    for field in fields {
        scope.check_active().map_err(Failure::Original)?;
        let source = match schema.field_by_id(field.source_id) {
            Some(source) => source,
            None => {
                let mut count = Count(0);
                fmt::write(
                    &mut count,
                    format_args!(
                        "Iceberg partition field {} names unknown source id {}",
                        field.name, field.source_id
                    ),
                )
                .map_err(|_| Failure::Overflow)?;
                let mut text = String::with_capacity(count.0);
                fmt::write(
                    &mut text,
                    format_args!(
                        "Iceberg partition field {} names unknown source id {}",
                        field.name, field.source_id
                    ),
                )
                .map_err(|_| Failure::ShapeChanged)?;
                return Err(Failure::Provider(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    text,
                )));
            }
        };
        sources.push(exact(&source.name)?);
        names.push(exact(&field.name)?);
        transforms.push(transform(&field.transform)?);
    }
    // All partition lookup errors still precede the same public default codec.
    let length = projection::count(schema.as_ref(), scope).map_err(Failure::Projection)?;
    let target_schema_json =
        projection::exact_json(schema.as_ref(), length, scope).map_err(Failure::Projection)?;
    let backing = crate::commit::write_stack::domain::IcebergCowTargetSchemaBacking::from_checked(
        target_schema_json,
        scope.original_guard(),
    );
    scope.check_active().map_err(Failure::Original)?;
    let recipe =
        IcebergDataBranchRecipe::try_new_cow(backing, sources, names, transforms, row_lineage)
            .map_err(Failure::Provider)?;
    if let Err(original) = scope.check_active() {
        drop(recipe); // Actual unclaimed graph exits BEFORE returning original cause.
        return Err(Failure::Original(original));
    }
    Ok(recipe)
}
pub(super) struct SignedInput {
    shape: novarocks_spi::connector::ConnectorWriteInputShape,
    _original: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}
impl SignedInput {
    #[cfg(test)]
    pub(super) fn shape(&self) -> &novarocks_spi::connector::ConnectorWriteInputShape {
        &self.shape
    }
    pub(super) fn into_shape(self) -> novarocks_spi::connector::ConnectorWriteInputShape {
        self.shape
    }
}
pub(super) fn signed_input<E>(
    facts: &IcebergWriteTableFacts,
    request: &novarocks_spi::connector::ConnectorWriteInputRequest,
    scope: &dyn OriginalScope<E>,
) -> Result<SignedInput, Failure<E>> {
    use novarocks_spi::connector::{
        ConnectorWriteFieldBinding, ConnectorWriteFieldRequest, ConnectorWriteFieldToken,
        ConnectorWriteInputRequest, ConnectorWriteInputShape,
    };
    use sha2::{Digest, Sha256};
    scope.check_active().map_err(Failure::Original)?;
    let guard = scope.original_guard();
    struct HashWrite<'a>(&'a mut Sha256);
    impl fmt::Write for HashWrite<'_> {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0.update(text.as_bytes());
            Ok(())
        }
    }
    let sign = |tag: &str,
                fields: &[ConnectorWriteFieldRequest]|
     -> Result<Vec<ConnectorWriteFieldBinding>, Failure<E>> {
        let mut output = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            scope.check_active().map_err(Failure::Original)?;
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg.write-stack.field.v1\0");
            hasher.update(facts.table_uuid().as_bytes());
            hasher.update([0]);
            hasher.update(facts.target_ref().as_bytes());
            hasher.update([0]);
            hasher.update(tag.as_bytes());
            hasher.update([0]);
            hasher.update(index.to_be_bytes());
            hasher.update(field.field().name().as_bytes());
            hasher.update([0]);
            fmt::write(
                &mut HashWrite(&mut hasher),
                format_args!("{:?}", field.field().data_type()),
            )
            .map_err(|_| Failure::ShapeChanged)?;
            hasher.update([u8::from(field.field().is_nullable())]);
            let token = ConnectorWriteFieldToken::from_bytes(hasher.finalize().into());
            output.push(
                ConnectorWriteFieldBinding::try_new_typed(
                    token,
                    field.field().clone(),
                    field
                        .execution_type()
                        .ok_or_else(|| {
                            Failure::Provider(super::invalid(
                                "Iceberg session field has no frozen complete declaration",
                            ))
                        })?
                        .clone(),
                )
                .map_err(Failure::Provider)?,
            );
        }
        Ok(output)
    };
    let shape = match request {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: sign("data", fields)?,
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: sign("row-lineage-data", data_fields)?,
            row_identity_fields: sign("row-lineage-identity", row_identity_fields)?,
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::PositionDelete {
            identity_fields: sign("position-delete-identity", identity_fields)?,
            partition_source_fields: sign("position-delete-partition", partition_source_fields)?,
        },
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::DeletionVector {
            identity_fields: sign("deletion-vector-identity", identity_fields)?,
            partition_source_fields: sign("deletion-vector-partition", partition_source_fields)?,
        },
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: sign("equality-delete", equality_fields)?,
            }
        }
    };
    shape.validate().map_err(Failure::Provider)?;
    scope.check_active().map_err(Failure::Original)?;
    Ok(SignedInput {
        shape,
        _original: guard,
    })
}

#[cfg(test)]
mod signed_input_type_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::{
        ConnectorPayloadRetentionGuard, ConnectorWriteFieldRequest, ConnectorWriteInputRequest,
    };
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Primary {
        Stopped,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Event {
        Active,
    }
    struct Scope {
        stopped: bool,
        events: RefCell<Vec<Event>>,
        guard: ConnectorPayloadRetentionGuard,
    }
    impl Scope {
        fn new(stopped: bool) -> Self {
            Self {
                stopped,
                events: RefCell::new(Vec::new()),
                guard: ConnectorPayloadRetentionGuard::new(()),
            }
        }
    }
    // Exercise the original call's finite operation checker, not a new grant.
    impl OriginalScope<Primary> for Scope {
        fn check_active(&self) -> Result<(), Primary> {
            self.events.borrow_mut().push(Event::Active);
            if self.stopped {
                Err(Primary::Stopped)
            } else {
                Ok(())
            }
        }

        fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
            self.guard.clone()
        }
    }
    fn facts() -> IcebergWriteTableFacts {
        IcebergWriteTableFacts::try_new(
            "original-table".into(),
            "db".into(),
            "t".into(),
            "s3://warehouse/db/t".into(),
            "s3://warehouse/db/t/data".into(),
            "main".into(),
            Some(1),
            1,
            0,
            0,
            3,
        )
        .expect("original table generation")
    }
    fn field(name: &str, logical: ValueLogicalType, nullable: bool) -> ConnectorWriteFieldRequest {
        let ty = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            nullable,
            logical,
        )
        .expect("independent complete declaration");
        ConnectorWriteFieldRequest::try_new_typed(
            Field::new(name, DataType::FixedSizeBinary(16), nullable).with_metadata(
                [("provider.attribute".to_owned(), "opaque".to_owned())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
            ty,
        )
        .expect("source and complete declaration agree")
    }
    #[test]
    fn cow_signer_preserves_complete_request_types_and_original_role_tokens() {
        let data = field("v", ValueLogicalType::LargeInt, false);
        let identity = field("id", ValueLogicalType::Uuid, true);
        let requests = [
            ConnectorWriteInputRequest::Data {
                fields: vec![data.clone()],
            },
            ConnectorWriteInputRequest::RowLineage {
                data_fields: vec![data.clone()],
                row_identity_fields: vec![identity.clone()],
            },
            ConnectorWriteInputRequest::PositionDelete {
                identity_fields: vec![identity.clone()],
                partition_source_fields: vec![data.clone()],
            },
            ConnectorWriteInputRequest::DeletionVector {
                identity_fields: vec![identity.clone()],
                partition_source_fields: vec![data.clone()],
            },
            ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: vec![data.clone()],
            },
        ];
        for request in &requests {
            let scope = Scope::new(false);
            let actual = signed_input(&facts(), request, &scope).expect("bounded COW signer");
            let ordinary =
                super::super::sign_input_shape(&facts(), request).expect("original signer");
            assert_eq!(actual.shape(), &ordinary);
            for binding in actual.shape().fields_iter() {
                let expected = if binding.field().name() == "v" {
                    &data
                } else {
                    &identity
                };
                assert_eq!(binding.execution_type(), expected.execution_type());
                assert_eq!(binding.field(), expected.field());
                assert_eq!(binding.field().metadata()["provider.attribute"], "opaque");
                assert_eq!(
                    binding.execution_type().unwrap().data_type(),
                    &DataType::FixedSizeBinary(16)
                );
            }
            assert!(
                scope
                    .events
                    .borrow()
                    .iter()
                    .all(|event| matches!(event, Event::Active))
            );
        }
    }
    #[test]
    fn cow_signer_rejects_missing_complete_declaration_instead_of_reauthoring_carrier() {
        let request = ConnectorWriteInputRequest::Data {
            fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "v",
                DataType::FixedSizeBinary(16),
                false,
            ))],
        };
        let scope = Scope::new(false);
        let failure = signed_input(&facts(), &request, &scope)
            .err()
            .expect("missing source declaration");
        match failure {
            Failure::Provider(error) => {
                assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
                assert_eq!(
                    error.message(),
                    "Iceberg session field has no frozen complete declaration"
                );
            }
            other => panic!("unexpected failure: {other:?}"),
        }
    }
    #[test]
    fn cow_signer_preserves_first_original_stop_without_callback_tail() {
        let request = ConnectorWriteInputRequest::Data {
            fields: vec![field("v", ValueLogicalType::LargeInt, false)],
        };
        let stopped = Scope::new(true);
        assert!(matches!(
            signed_input(&facts(), &request, &stopped),
            Err(Failure::Original(Primary::Stopped))
        ));
        assert_eq!(*stopped.events.borrow(), [Event::Active]);
    }
}

#[cfg(test)]
mod target_schema_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Scope {
        checks: AtomicUsize,
        deny: Option<usize>,
        minted: AtomicUsize,
        exited: Arc<AtomicUsize>,
    }
    struct Holder(Arc<AtomicUsize>);
    impl Drop for Holder {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OriginalScope<&'static str> for Scope {
        fn check_active(&self) -> Result<(), &'static str> {
            let next = self.checks.fetch_add(1, Ordering::SeqCst) + 1;
            if self.deny == Some(next) {
                Err("original stopped")
            } else {
                Ok(())
            }
        }

        fn original_guard(&self) -> novarocks_spi::connector::ConnectorPayloadRetentionGuard {
            self.minted.fetch_add(1, Ordering::SeqCst);
            novarocks_spi::connector::ConnectorPayloadRetentionGuard::new(Holder(
                self.exited.clone(),
            ))
        }
    }
    fn scope(deny: Option<usize>) -> Scope {
        Scope {
            checks: AtomicUsize::new(0),
            deny,
            minted: AtomicUsize::new(0),
            exited: Arc::new(AtomicUsize::new(0)),
        }
    }
    #[test]
    fn cow_complete_schema_preserves_original_stop_and_last_recipe_guard() {
        let metadata = crate::commit::write_stack::test_support::staged_table_metadata();
        let denied = scope(Some(2));
        assert!(matches!(
            data_recipe(&metadata, false, &denied),
            Err(Failure::Projection(projection::Error::Control(
                "original stopped"
            )))
        ));
        assert_eq!(denied.minted.load(Ordering::SeqCst), 0);
        let accepted = scope(None);
        let recipe = data_recipe(&metadata, false, &accepted).unwrap();
        assert_eq!(accepted.minted.load(Ordering::SeqCst), 1);
        let actual_json = recipe.target_schema_json().unwrap();
        assert_eq!(
            actual_json,
            serde_json::to_string(metadata.current_schema().as_ref()).unwrap()
        );
        let clone = recipe.clone();
        drop(recipe);
        assert_eq!(accepted.exited.load(Ordering::SeqCst), 0);
        drop(clone);
        assert_eq!(accepted.exited.load(Ordering::SeqCst), 1);
    }
}
