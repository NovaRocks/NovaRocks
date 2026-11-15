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

//! Helpers shared by the write-control preparation paths.
//!
//! `prepare_write` and `prepare_row_mutation` resolve the same ref-scoped
//! facts before they diverge into their own field-signing rules. Keeping the
//! shared resolution here stops the two paths from drifting apart.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorWriteFieldRequest};

use crate::iceberg::spec::{Schema, TableMetadata};

/// Resolve the snapshot a write against `target_ref` will be based on.
///
/// `main` resolves to the table's current snapshot; any other ref resolves to
/// that branch's head. `Ok(None)` means the ref exists but has no snapshot yet.
pub(crate) fn write_target_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot_id());
    }
    crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

/// Render a resolved base snapshot for the `base_version` / preparation
/// payload strings. Kept next to the resolver so both preparation paths spell
/// a missing snapshot the same way.
pub(crate) fn snapshot_token(target_snapshot_id: Option<i64>) -> String {
    target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string())
}

/// Resolve each requested write column against the frozen target schema and
/// restate it with the Arrow type the Iceberg writers actually consume.
///
/// The shared metadata schema owner authors Variant/Binary/Timestamptz
/// carriers and every declared logical domain. Both write paths consume those
/// complete fields instead of reconstructing an untagged carrier.
pub(crate) fn exact_requested_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    exact_requested_write_fields_at_schema(metadata, metadata.current_schema(), requested, creation)
}

/// Resolve write fields against an already-frozen Iceberg schema.
pub(crate) fn exact_requested_write_fields_at_schema(
    metadata: &TableMetadata,
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    creation
        .work(0)
        .map_err(crate::metadata::metadata_creation_error)?;
    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let execution = crate::schema_mapping::catalog_value::table_with_document(
        &document,
        iceberg_schema,
        &[],
        None,
        creation,
    )
    .map_err(crate::metadata::metadata_creation_error)?;
    exact_requested_write_fields_from_declaration(
        &document,
        iceberg_schema,
        requested,
        &execution,
        creation,
    )
}

/// Borrow an already-authored provider declaration at the same frozen schema.
/// Source fields still come from the original format projection.
pub(crate) fn exact_requested_write_fields_from_declaration(
    document: &crate::schema_mapping::nominal_declaration::TableNominalDocument,
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let arrow_schema =
        crate::scalar_integer_domain::sql_schema_from_document(iceberg_schema, document)?;
    requested
        .iter()
        .map(|request| {
            creation.work(1).map_err(crate::metadata::metadata_creation_error)?;
            let requested_name = request.field().name();
            let (ordinal, _iceberg_field) = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name.eq_ignore_ascii_case(requested_name))
                .ok_or_else(|| {
                    invalid_write_activation(format!(
                        "Iceberg write input column `{requested_name}` is absent from the frozen target schema"
                    ))
                })?;
            let arrow_field = arrow_schema.field(ordinal);
            // The original source Field and the complete declaration have
            // independent owners. Only this catalog author establishes domains.
            let value = execution.bindings().get(ordinal).ok_or_else(|| {
                invalid_write_activation("frozen writer execution declaration is absent")
            })?;
            creation.work(0).map_err(crate::metadata::metadata_creation_error)?;
            ConnectorWriteFieldRequest::try_new_typed(arrow_field.clone(), value.clone())
        })
        .collect()
}

/// Canonical input and its independent exact read/storage declaration. Only
/// the original Iceberg/nominal author may grant the raw Fixed projection.
pub(crate) fn canonical_data_write_fields(
    metadata: &TableMetadata,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let schema = metadata.current_schema();
    let execution = crate::schema_mapping::catalog_value::table_with_document(
        &document,
        schema,
        &[],
        None,
        creation,
    )
    .map_err(crate::metadata::metadata_creation_error)?;
    canonical_data_write_fields_from_declaration(&document, schema, &execution, creation)
}

pub(crate) fn canonical_data_write_fields_from_declaration(
    document: &crate::schema_mapping::nominal_declaration::TableNominalDocument,
    schema: &Schema,
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let original = crate::scalar_integer_domain::sql_schema_from_document(schema, document)?;
    let integers = crate::scalar_integer_domain::of_schema(schema, &document.integers)?;
    // Stack backing is initialized once for the original clone grammar, not
    // requested as a heap allocation or recreated for each field.
    creation
        .work(novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound())
        .map_err(crate::metadata::metadata_creation_error)?;
    let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
    let mut fields = creation
        .reserve(schema.as_struct().fields().len())
        .map_err(crate::metadata::metadata_creation_error)?;
    for (ordinal, field) in schema.as_struct().fields().iter().enumerate() {
        let storage = execution.bindings().get(ordinal).ok_or_else(|| {
            invalid_write_activation(
                "canonical data input has no complete original storage declaration",
            )
        })?;
        let input = crate::schema_mapping::typed_value::from_write_field(
            field,
            !field.required,
            &document.nominals,
            integers.get(&field.id).copied(),
            creation,
        )
        .map_err(crate::metadata::metadata_creation_error)?
        .binding;
        let same = input
            .exactly_equals_checked_observed(storage, || creation.work(1))
            .map_err(crate::metadata::metadata_creation_error)?;
        let raw = if same {
            clone_write_field(original.field(ordinal), &mut scratch, creation)?
        } else {
            project_write_field(
                original.field(ordinal),
                input.data_type(),
                &mut scratch,
                creation,
            )?
        };
        let request = ConnectorWriteFieldRequest::try_new_typed(raw, input)?;
        fields.push(if same {
            request
        } else {
            request.with_storage_type(storage.clone())?
        });
    }
    Ok(fields)
}

/// Rebuild only changed source-layout paths. Every original Field attribute,
/// field ID, dictionary IPC flag, name and NULL declaration is preserved.
fn project_write_field<'source>(
    source: &'source arrow::datatypes::Field,
    target: &arrow::datatypes::DataType,
    scratch: &mut novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<
        'source,
    >,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<arrow::datatypes::Field, ConnectorError> {
    use arrow::datatypes::DataType;
    creation
        .work(1)
        .map_err(crate::metadata::metadata_creation_error)?;
    if source.data_type() == target {
        return clone_write_field(source, scratch, creation);
    }
    let data_type = match (source.data_type(), target) {
        (DataType::FixedSizeBinary(_), DataType::Binary) => DataType::Binary,
        (DataType::Struct(fields), DataType::Struct(targets)) if fields.len() == targets.len() => {
            let mut output = creation
                .reserve(fields.len())
                .map_err(crate::metadata::metadata_creation_error)?;
            for (field, target) in fields.iter().zip(targets) {
                let projected = project_write_field(field, target.data_type(), scratch, creation)?;
                output.push(
                    creation
                        .arc(projected)
                        .map_err(crate::metadata::metadata_creation_error)?,
                );
            }
            DataType::Struct(
                creation
                    .arrow_fields(output)
                    .map_err(crate::metadata::metadata_creation_error)?,
            )
        }
        (DataType::List(field), DataType::List(target)) => {
            let projected = project_write_field(field, target.data_type(), scratch, creation)?;
            DataType::List(
                creation
                    .arc(projected)
                    .map_err(crate::metadata::metadata_creation_error)?,
            )
        }
        (DataType::Map(field, sorted), DataType::Map(target, target_sorted))
            if sorted == target_sorted =>
        {
            let projected = project_write_field(field, target.data_type(), scratch, creation)?;
            DataType::Map(
                creation
                    .arc(projected)
                    .map_err(crate::metadata::metadata_creation_error)?,
                *sorted,
            )
        }
        _ => {
            return Err(invalid_write_activation(
                "canonical writer source layout differs beyond licensed raw Fixed leaves",
            ));
        }
    };
    rebuild_write_field(source, data_type, creation)
}

fn clone_write_field<'source>(
    source: &'source arrow::datatypes::Field,
    scratch: &mut novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<
        'source,
    >,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<arrow::datatypes::Field, ConnectorError> {
    let (request, work) = creation.observers();
    let mut allocations = |layout, occurrences| {
        for _ in 0..occurrences {
            request(layout).map_err(WriteValueFailure::Control)?;
        }
        Ok::<(), WriteValueFailure>(())
    };
    novarocks_type_contract::owned_resources::metadata_materialization::plain_field_clone_allocation_requests_observed(
        source,scratch,&mut ||work(1).map_err(WriteValueFailure::Control),&mut Some(&mut allocations),
    ).map_err(WriteValueFailure::into_error)?;
    work(source.name().len())
        .map_err(WriteValueFailure::Control)
        .map_err(WriteValueFailure::into_error)?;
    Ok(source.clone())
}

#[allow(
    deprecated,
    reason = "Preserve the actual original dictionary occurrence attributes"
)]
fn rebuild_write_field(
    source: &arrow::datatypes::Field,
    data_type: arrow::datatypes::DataType,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<arrow::datatypes::Field, ConnectorError> {
    let (request, work) = creation.observers();
    let mut allocations = |layout, occurrences| {
        for _ in 0..occurrences {
            request(layout).map_err(WriteValueFailure::Control)?;
        }
        Ok::<(), WriteValueFailure>(())
    };
    novarocks_type_contract::owned_resources::metadata_materialization::plain_field_header_allocation_requests_observed(
        source,&mut ||work(1).map_err(WriteValueFailure::Control),&mut Some(&mut allocations),
    ).map_err(WriteValueFailure::into_error)?;
    work(source.name().len())
        .map_err(WriteValueFailure::Control)
        .map_err(WriteValueFailure::into_error)?;
    let name = source.name().clone();
    let field = if matches!(
        source.data_type(),
        arrow::datatypes::DataType::Dictionary(..)
    ) {
        arrow::datatypes::Field::new_dict(
            name,
            data_type,
            source.is_nullable(),
            source.dict_id().ok_or_else(|| {
                invalid_write_activation("writer original Dictionary field has no occurrence ID")
            })?,
            source.dict_is_ordered().ok_or_else(|| {
                invalid_write_activation(
                    "writer original Dictionary field has no order declaration",
                )
            })?,
        )
    } else {
        arrow::datatypes::Field::new(name, data_type, source.is_nullable())
    };
    Ok(field.with_metadata(source.metadata().clone()))
}

enum WriteValueFailure {
    Metadata(novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError),
    Type(novarocks_type_contract::ValueTypeError),
    Control(novarocks_type_contract::CompileControlError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl From<novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError> for WriteValueFailure {
    fn from(cause:novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError)->Self {Self::Metadata(cause)}
}
impl From<novarocks_type_contract::ValueTypeError> for WriteValueFailure {
    fn from(cause: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(cause)
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for WriteValueFailure
{
    fn from(value: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(value)
    }
}
impl WriteValueFailure {
    fn into_error(self) -> ConnectorError {
        match self {
            Self::Metadata(cause)=>ConnectorError::new(
                if matches!(cause,novarocks_type_contract::owned_resources::metadata_materialization::MetadataMaterializationError::Arithmetic) {ConnectorErrorKind::ResourceExhausted} else {ConnectorErrorKind::Internal},
                format!("writer Field clone request is invalid: {cause:?}"),
            ),
            Self::Type(cause)=>ConnectorError::new(ConnectorErrorKind::Internal,format!("writer Field clone source is invalid: {cause:?}")),
            Self::Control(cause) => crate::metadata::metadata_creation_error(
                novarocks_connector_contract::PureProviderCompileError::Control(cause),
            ),
            Self::Resource(cause) => ConnectorError::new(
                if matches!(cause, novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel) {
                    novarocks_spi::connector::ConnectorErrorKind::Internal
                } else { novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted },
                format!("writer value root construction failed: {cause:?}"),
            ),
        }
    }
}

pub(crate) fn writer_value_with_nullable(
    value: &novarocks_type_contract::FunctionValueType,
    nullable: bool,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<novarocks_type_contract::FunctionValueType, ConnectorError> {
    if value.nullable() == nullable {
        return Ok(value.clone());
    }
    let storage = creation.storage();
    let (request, work) = creation.observers();
    value
        .with_nullable_observed(
            nullable,
            storage,
            |layout| request(layout).map_err(WriteValueFailure::Control),
            |units| work(units).map_err(WriteValueFailure::Control),
        )
        .map_err(WriteValueFailure::into_error)
}

/// Metadata roles are authored by the existing provider declaration, not the
/// SQL proposal's carrier or attributes. Original proposal fields stay intact.
pub(crate) fn exact_metadata_write_fields(
    _metadata: &TableMetadata,
    fields: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let mut authored = creation
        .reserve(fields.len())
        .map_err(crate::metadata::metadata_creation_error)?;
    for field in fields {
        creation
            .work(1)
            .map_err(crate::metadata::metadata_creation_error)?;
        let role = crate::typed_read::ALWAYS_BOUND_METADATA_COLUMNS
            .into_iter()
            .chain(crate::typed_read::ROW_LINEAGE_METADATA_COLUMNS)
            .find(|role| {
                role.column_name()
                    .eq_ignore_ascii_case(field.field().name())
            })
            .ok_or_else(|| {
                invalid_write_activation("writer identity field has no provider metadata role")
            })?;
        let value = crate::schema_mapping::catalog_value::metadata_column(role, creation)
            .map_err(crate::metadata::metadata_creation_error)?;
        // Keep the original source Field and its requested root NULL obligation;
        // the static role does not validate or borrow unrelated table data.
        let value = writer_value_with_nullable(&value, field.field().is_nullable(), creation)?;
        authored.push(ConnectorWriteFieldRequest::try_new_typed(
            field.field().clone(),
            value,
        )?);
    }
    Ok(authored)
}

pub(crate) fn position_mutation_base(
    metadata: &TableMetadata,
    location: Option<&str>,
    target_ref: &str,
    snapshot_id: Option<i64>,
) -> Result<novarocks_spi::connector::ConnectorWriteBaseVersion, ConnectorError> {
    use sha2::{Digest, Sha256};
    let metadata_identity = crate::read_snapshot::metadata_identity(metadata)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
    let metadata_object = location.map_or_else(
        || "none".to_owned(),
        |path| format!("{:x}", Sha256::digest(path.as_bytes())),
    );
    let snapshot = snapshot_token(snapshot_id);
    novarocks_spi::connector::ConnectorWriteBaseVersion::try_new(bytes::Bytes::from(format!(
        "iceberg/position-mutation-base/v2/{}/{target_ref}/{snapshot}/{metadata_object}/{metadata_identity}",
        metadata.uuid(),
    )))
}

pub(crate) fn position_mutation_read_schema(
    metadata: &TableMetadata,
    target_ref: &str,
    snapshot_id: Option<i64>,
) -> Result<std::sync::Arc<Schema>, ConnectorError> {
    match snapshot_id {
        Some(snapshot) if target_ref != "main" => {
            crate::typed_boundary::projection_schema_for_pinned_snapshot(metadata, snapshot)
        }
        _ => Ok(metadata.current_schema().clone()),
    }
}

pub(crate) fn invalid_write_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum FieldCloneEvent {
        Request { bytes: usize, align: usize },
        Work(usize),
    }

    #[allow(
        deprecated,
        reason = "Exercise exact source Dictionary occurrence cloning"
    )]
    fn field_clone_case(
        changed: bool,
        stop: Option<(usize, novarocks_type_contract::CompileControlError)>,
    ) -> (
        Result<arrow::datatypes::Field, ConnectorError>,
        Vec<FieldCloneEvent>,
    ) {
        use arrow::datatypes::{DataType, Field};
        use std::cell::RefCell;
        let source = if changed {
            Field::new("raw", DataType::FixedSizeBinary(4), true)
        } else {
            Field::new_dict(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
                37,
                true,
            )
        }
        .with_metadata(std::collections::HashMap::from([(
            "field-id".to_string(),
            "7".to_string(),
        )]));
        let events = RefCell::new(Vec::new());
        let observe = |event: FieldCloneEvent| {
            let mut events = events.borrow_mut();
            let at = events.len();
            if let Some((stop, _)) = &stop {
                assert!(at <= *stop, "callback after original refusal");
            }
            events.push(event);
            if let Some((stop, cause)) = &stop
                && at == *stop
            {
                return Err(cause.clone());
            }
            Ok(())
        };
        let mut request = |layout: std::alloc::Layout| {
            observe(FieldCloneEvent::Request {
                bytes: layout.size(),
                align: layout.align(),
            })
        };
        let mut work = |units| observe(FieldCloneEvent::Work(units));
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            &mut request,
            &mut work,
        );
        let result = (|| {
            creation.work(novarocks_type_contract::owned_resources::type_validation::scratch_work_upper_bound()).map_err(crate::metadata::metadata_creation_error)?;
            let mut scratch = [None; novarocks_type_contract::MAX_VALUE_TYPE_NODES];
            let output = if changed {
                project_write_field(&source, &DataType::Binary, &mut scratch, &mut creation)?
            } else {
                clone_write_field(&source, &mut scratch, &mut creation)?
            };
            assert_eq!(output.name(), source.name());
            assert_eq!(output.is_nullable(), source.is_nullable());
            assert_eq!(output.metadata(), source.metadata());
            assert!(
                std::sync::Arc::ptr_eq(
                    output.metadata().as_arc().unwrap(),
                    source.metadata().as_arc().unwrap()
                ),
                "Metadata clone shares its original backing"
            );
            if !changed {
                assert_eq!(output.dict_id(), Some(37));
                assert_eq!(output.dict_is_ordered(), Some(true));
                assert_eq!(output.data_type(), source.data_type());
            }
            Ok(output)
        })();
        let trace = events.borrow().clone();
        (result, trace)
    }

    #[test]
    fn canonical_field_clones_observe_only_actual_names_dictionary_boxes_and_shared_metadata() {
        let (changed, events) = field_clone_case(true, None);
        assert_eq!(
            changed.unwrap().data_type(),
            &arrow::datatypes::DataType::Binary
        );
        let requests = events
            .into_iter()
            .filter_map(|event| match event {
                FieldCloneEvent::Request { bytes, align } => Some((bytes, align)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            requests,
            vec![(3, 1)],
            "changed Field moves its already-created dtype and clones one name"
        );
        let (same, events) = field_clone_case(false, None);
        assert!(same.is_ok());
        let requests = events
            .into_iter()
            .filter_map(|event| match event {
                FieldCloneEvent::Request { bytes, align } => Some((bytes, align)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let dtype = std::alloc::Layout::new::<arrow::datatypes::DataType>();
        assert_eq!(
            requests,
            vec![
                (dtype.size(), dtype.align()),
                (dtype.size(), dtype.align()),
                (4, 1)
            ],
            "same Field clones each actual Dictionary box once, no metadata tree"
        );
    }

    #[test]
    fn canonical_field_clone_requests_preserve_every_original_caller_refusal_prefix() {
        use novarocks_failure_contract::FailureProjection;
        for changed in [false, true] {
            let (result, trace) = field_clone_case(changed, None);
            assert!(result.is_ok());
            for at in 0..trace.len() {
                for cause in [
                    novarocks_type_contract::CompileControlError::Cancelled,
                    novarocks_type_contract::CompileControlError::DeadlineExceeded,
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                ] {
                    let (result, actual) = field_clone_case(changed, Some((at, cause.clone())));
                    let error = result.expect_err("original caller refuses");
                    assert_eq!(error.failure_fact(), cause.failure_fact());
                    assert_eq!(actual, trace[..=at]);
                }
            }
        }
    }

    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder, TableMetadataBuilder,
        Type,
    };
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    fn context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("original fixture request context")
    }

    fn metadata(properties: HashMap<String, String>) -> TableMetadata {
        let fields = [
            (1, "uuid", Type::Primitive(PrimitiveType::Uuid)),
            (
                2,
                "variant",
                Type::Variant(crate::iceberg::spec::VariantType),
            ),
            (3, "hll", Type::Primitive(PrimitiveType::Binary)),
            (4, "bitmap", Type::Primitive(PrimitiveType::Binary)),
            (5, "largeint", Type::Primitive(PrimitiveType::Fixed(16))),
            (6, "plain_fixed", Type::Primitive(PrimitiveType::Fixed(16))),
            (7, "plain_binary", Type::Primitive(PrimitiveType::Binary)),
        ]
        .into_iter()
        .map(|(id, name, field_type)| Arc::new(NestedField::optional(id, name, field_type)))
        .collect::<Vec<_>>();
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(fields)
                .build()
                .expect("schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/write_domain_test".to_string(),
            FormatVersion::V3,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn frozen_write_fields_keep_provider_uuid_variant_and_declared_opaque_domains() {
        let metadata = metadata(HashMap::from([(crate::schema_mapping::nominal_declaration::PROPERTY.into(),r#"{"version":1,"fields":[{"id":3,"domain":"hll"},{"id":4,"domain":"bitmap"},{"id":5,"domain":"largeint"}]}"#.into())]));
        let expected = [
            (
                "uuid",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            ("variant", DataType::LargeBinary, ValueLogicalType::Variant),
            ("hll", DataType::Binary, ValueLogicalType::Hll),
            ("bitmap", DataType::Binary, ValueLogicalType::Bitmap),
            (
                "largeint",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            ),
            (
                "plain_fixed",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
            ),
            ("plain_binary", DataType::Binary, ValueLogicalType::Physical),
        ];
        // Names select the frozen provider fields. A caller's placeholder
        // carrier cannot author their target type or logical domain.
        let requests = expected
            .iter()
            .map(|(name, _, _)| {
                ConnectorWriteFieldRequest::new(Field::new(
                    name.to_uppercase(),
                    DataType::Null,
                    true,
                ))
            })
            .collect::<Vec<_>>();
        let fields = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            exact_requested_write_fields(&metadata, &requests, creation)
        })
        .expect("exact write fields");
        let authored =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .expect("provider schema");
        for (ordinal, (request, (name, carrier, domain))) in fields.iter().zip(expected).enumerate()
        {
            assert_eq!(request.field(), authored.field(ordinal));
            assert_eq!(request.field().name(), name);
            assert_eq!(
                request
                    .execution_type()
                    .expect("provider complete declaration")
                    .clone(),
                FunctionValueType::try_with_logical_type(carrier, true, domain)
                    .expect("expected provider type")
            );
        }
    }

    #[test]
    fn canonical_data_fields_separate_raw_fixed_storage_without_reinterpreting_nominal_domains() {
        let metadata = metadata(HashMap::from([(
            crate::schema_mapping::nominal_declaration::PROPERTY.into(),
            r#"{"version":1,"fields":[{"id":3,"domain":"hll"},{"id":4,"domain":"bitmap"},{"id":5,"domain":"largeint"}]}"#.into(),
        )]));
        let original =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap();
        let fields = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            canonical_data_write_fields(&metadata, creation)
        })
        .unwrap();
        assert_eq!(fields.len(), 7);
        for (ordinal, request) in fields.iter().enumerate() {
            assert_eq!(request.field().name(), original.field(ordinal).name());
            assert_eq!(
                request.field().is_nullable(),
                original.field(ordinal).is_nullable()
            );
            assert_eq!(
                request.field().metadata(),
                original.field(ordinal).metadata()
            );
            let input = request.execution_type().unwrap();
            if ordinal == 5 {
                assert_eq!(input.data_type(), &DataType::Binary);
                assert_eq!(input.logical_type(), ValueLogicalType::Physical);
                assert_eq!(request.field().data_type(), &DataType::Binary);
                assert_eq!(
                    request.storage_type(),
                    Some(&FunctionValueType::new(DataType::FixedSizeBinary(16), true))
                );
            } else {
                assert_eq!(request.field(), original.field(ordinal));
                assert!(request.storage_type().is_none());
            }
        }
        for (ordinal, domain) in [
            (0, ValueLogicalType::Uuid),
            (1, ValueLogicalType::Variant),
            (2, ValueLogicalType::Hll),
            (3, ValueLogicalType::Bitmap),
            (4, ValueLogicalType::LargeInt),
        ] {
            assert_eq!(
                fields[ordinal].execution_type().unwrap().logical_type(),
                domain
            );
        }
    }

    #[test]
    fn nested_canonical_fields_preserve_every_source_field_id_and_required_flag() {
        use crate::iceberg::spec::{ListType, MapType, StructType};
        let fixed = || Type::Primitive(PrimitiveType::Fixed(4));
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    11,
                    "record",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::required(12, "bytes", fixed())),
                        Arc::new(NestedField::optional(
                            13,
                            "identity",
                            Type::Primitive(PrimitiveType::Uuid),
                        )),
                    ])),
                )),
                Arc::new(NestedField::optional(
                    14,
                    "items",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        15,
                        fixed(),
                        true,
                    )))),
                )),
                Arc::new(NestedField::required(
                    16,
                    "entries",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::map_key_element(17, fixed())),
                        Arc::new(NestedField::map_value_element(18, fixed(), false)),
                    )),
                )),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/nested".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let original =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .unwrap();
        let fields = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            canonical_data_write_fields(&metadata, creation)
        })
        .unwrap();
        fn check(source: &Field, input: &Field) {
            assert_eq!(input.name(), source.name());
            assert_eq!(input.metadata(), source.metadata());
            assert_eq!(input.is_nullable(), source.is_nullable());
            match (source.data_type(), input.data_type()) {
                (DataType::Struct(left), DataType::Struct(right)) => {
                    assert_eq!(left.len(), right.len());
                    for (left, right) in left.iter().zip(right) {
                        check(left, right);
                    }
                }
                (DataType::List(left), DataType::List(right)) => check(left, right),
                (DataType::Map(left, sorted), DataType::Map(right, other)) => {
                    assert_eq!(sorted, other);
                    check(left, right);
                }
                (DataType::FixedSizeBinary(4), DataType::Binary) => {}
                (left, right) => assert_eq!(left, right),
            }
        }
        for (source, request) in original.fields().iter().zip(&fields) {
            check(source, request.field());
            let storage = request
                .storage_type()
                .expect("nested Fixed storage remains independent");
            assert_eq!(storage.nullable(), source.is_nullable());
            assert_eq!(
                request.execution_type().unwrap().nullable(),
                source.is_nullable()
            );
        }
        let DataType::Struct(children) = fields[0].field().data_type() else {
            panic!("record")
        };
        assert_eq!(children[0].data_type(), &DataType::Binary);
        assert!(!children[0].is_nullable());
        assert_eq!(children[1].data_type(), &DataType::FixedSizeBinary(16));
        let DataType::List(element) = fields[1].field().data_type() else {
            panic!("items")
        };
        assert_eq!(element.data_type(), &DataType::Binary);
        assert!(!element.is_nullable());
        let DataType::Map(entries, _) = fields[2].field().data_type() else {
            panic!("entries")
        };
        let DataType::Struct(parts) = entries.data_type() else {
            panic!("entry fields")
        };
        assert_eq!(parts[0].data_type(), &DataType::Binary);
        assert!(!parts[0].is_nullable());
        assert_eq!(parts[1].data_type(), &DataType::Binary);
        assert!(parts[1].is_nullable());
    }

    #[test]
    fn frozen_write_fields_reject_conflicting_provider_domain_declarations() {
        for (name, domain) in [
            ("plain_fixed", "hll"),
            ("plain_binary", "largeint"),
            ("uuid", "largeint"),
        ] {
            let metadata = metadata(HashMap::from([(
                crate::schema_mapping::nominal_declaration::PROPERTY.into(),
                format!(
                    r#"{{"version":1,"fields":[{{"id":{},"domain":"{domain}"}}]}}"#,
                    match name {
                        "plain_fixed" => 6,
                        "plain_binary" => 7,
                        "uuid" => 1,
                        _ => unreachable!(),
                    }
                ),
            )]));
            let request = ConnectorWriteFieldRequest::new(Field::new(name, DataType::Null, true));
            let error = crate::metadata::with_pure_schema_creation(&context(), |creation| {
                exact_requested_write_fields(&metadata, &[request], creation)
            })
            .expect_err("a conflicting provider declaration cannot be silently erased");
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
    }
    #[test]
    fn cancelled_writer_type_author_refuses_before_the_operation() {
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        stop.request_stop();
        let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            stop.view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .unwrap();
        let error = crate::metadata::with_pure_schema_creation::<()>(&context, |_| {
            panic!("no constructor tail after cancellation")
        })
        .expect_err("original cancellation is observed first");
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }
}
