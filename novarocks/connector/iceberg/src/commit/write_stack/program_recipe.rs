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

//! Complete-input writer validation without a generation-bound runtime adapter.

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use novarocks_connector_contract::{
    ConnectorWriteInputShape, ConnectorWriteRecipeCompiler, ConnectorWriteRecipeDraft,
    MAX_CONNECTOR_WRITER_HANDLE_BYTES, PureProviderCompileError,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorFieldPath,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use super::codec::IcebergWriteValueCodec;
use super::domain::{IcebergWriteBranch, IcebergWriterHandle};
use crate::commit::frozen_write::{
    FrozenDataWriteFacts, FrozenWriteSchemaError, prepare_frozen_write_schema,
};

type Failure = PureProviderCompileError<ConnectorCodecError>;
const COPY_BYTES: usize = 1024;

/// A pure installed definition, independent of a catalog runtime generation.
/// The caller's seal owns the exact binding, public fields and field tokens.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergWriteRecipeCompiler;

impl ConnectorWriteRecipeCompiler for IcebergWriteRecipeCompiler {
    type Error = ConnectorCodecError;

    fn compile_private(
        &self,
        draft: &ConnectorWriteRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteRecipeDraft, Failure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = compile(draft, control, &mut work);
        work.finish_result(result, Failure::Control)
    }
}

fn compile(
    draft: &ConnectorWriteRecipeDraft,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorWriteRecipeDraft, Failure> {
    let binding = draft.binding();
    if binding.descriptor().provider_id.as_str() != crate::PROVIDER_ID {
        return Err(invalid(
            "binding",
            "Iceberg writer has another provider identity",
        ));
    }
    draft
        .payload()
        .header()
        .validate_expected::<ConnectorCodecError>(
            &binding.descriptor().provider_id,
            binding.catalog_handle(),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(crate::wire::write::WRITE_CODEC_REVISION)
                .expect("Iceberg write codec revision is non-zero"),
        )
        .map_err(lift)?;
    work.step()?;
    work.flush()?;
    let mut ledger = ConnectorDecodeLedger::new(IcebergWriteValueCodec::decode_limits(
        MAX_CONNECTOR_WRITER_HANDLE_BYTES,
    ));
    let mut context =
        ConnectorDecodeContext::try_new_for_compile(draft.payload().header(), &mut ledger, control)
            .map_err(lift)?;
    let wire = crate::wire::write::decode_writer_handle(draft.payload().payload(), &mut context)
        .map_err(lift)?;
    context.flush_compile_control().map_err(lift)?;
    let owner = copy_text(binding.descriptor().instance_id.as_str(), work)?;
    let value = IcebergWriteValueCodec::new(Arc::<str>::from(owner))
        .decode_writer_handle_value(&wire, &mut context)
        .map_err(lift)?;
    context.flush_compile_control().map_err(lift)?;
    work.step()?;

    match (value.branch(), draft.input()) {
        // Ordinary flavor writers use a DATA recipe even when their declared
        // input includes row identity or delete-role fields. The flavor owner
        // decides which columns reach the physical data/delete branches.
        (
            IcebergWriteBranch::Data,
            ConnectorWriteInputShape::Data { .. }
            | ConnectorWriteInputShape::RowLineage { .. }
            | ConnectorWriteInputShape::PositionDelete { .. }
            | ConnectorWriteInputShape::DeletionVector { .. },
        ) => {
            let schema = data_input_schema(draft.input(), work)?;
            validate_data(&value, &schema, control, work)?;
        }
        (IcebergWriteBranch::EqualityDelete, ConnectorWriteInputShape::EqualityDelete { .. }) => {
            let schema = input_schema(draft.input(), work)?;
            let equality = value.equality().ok_or_else(|| {
                invalid(
                    "equality",
                    "Iceberg equality-delete handle has no match recipe",
                )
            })?;
            work.flush()?;
            super::equality_schema::equality_delete_target_for_compile(equality, &schema, control)
                .map_err(|error| match error {
                    super::equality_schema::EqualitySchemaError::Control(error) => {
                        Failure::Control(error)
                    }
                    super::equality_schema::EqualitySchemaError::Source(error) => {
                        invalid("equality", error.to_string())
                    }
                })?;
            work.step()?;
        }
        (
            IcebergWriteBranch::PositionDelete,
            ConnectorWriteInputShape::PositionDelete {
                identity_fields, ..
            },
        ) => validate_position_layout(
            draft.input(),
            identity_fields.len(),
            2,
            &[
                Some(DataType::Utf8),
                Some(DataType::Int64),
                Some(DataType::Int32),
                None,
            ],
            work,
        )?,
        (
            IcebergWriteBranch::DeletionVector,
            ConnectorWriteInputShape::DeletionVector {
                identity_fields, ..
            },
        ) if value.position_group().is_some() => validate_position_layout(
            draft.input(),
            identity_fields.len(),
            3,
            &[
                Some(DataType::Utf8),
                Some(DataType::Int32),
                None,
                Some(DataType::Int64),
                Some(DataType::Int32),
                Some(DataType::Int32),
                Some(DataType::Int64),
                Some(DataType::Int64),
                Some(DataType::Binary),
            ],
            work,
        )?,
        (IcebergWriteBranch::DeletionVector, ConnectorWriteInputShape::DeletionVector { .. })
            if value.position_rewrite().is_some() =>
        {
            // Standalone maintenance still consumes its original positional
            // relation. The runtime checks each row against the frozen target.
            let mut fields = draft.input().fields_iter();
            let file_matches = fields
                .next()
                .is_some_and(|field| field.field().data_type() == &DataType::Utf8);
            work.step()?;
            let position_matches = fields
                .next()
                .is_some_and(|field| field.field().data_type() == &DataType::Int64);
            work.step()?;
            if !file_matches || !position_matches {
                return Err(invalid(
                    "input",
                    "Iceberg delete writer requires first input columns Utf8 and Int64",
                ));
            }
            work.step()?;
        }
        _ => {
            return Err(invalid(
                "input",
                "Iceberg writer branch differs from its public input shape",
            ));
        }
    }

    // Raw and domain admission plus the complete public-input checks above
    // certify the existing private recipe. Preserve its already bounded Bytes
    // and immutable public snapshot rather than reauthoring a second schema or
    // creating a runtime capability. Canonical re-encoding is unnecessary.
    work.flush()?;
    let compiled = draft.clone();
    work.step()?;
    Ok(compiled)
}

fn validate_position_layout(
    input: &ConnectorWriteInputShape,
    identity_count: usize,
    expected_identity_count: usize,
    types: &[Option<DataType>],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let invalid_layout = || {
        invalid(
            "input",
            "Iceberg fixed position writer input has the wrong layout",
        )
    };
    if identity_count != expected_identity_count || input.field_count() != types.len() {
        return Err(invalid_layout());
    }
    for (binding, expected) in input.fields_iter().zip(types) {
        work.step()?;
        let field = binding.field();
        // The mutation source author signs the exact historical Struct and
        // the loaded endpoint checks it again against the actual file spec.
        let matches = match expected {
            Some(data_type) => !field.is_nullable() && field.data_type() == data_type,
            None => field.is_nullable() && matches!(field.data_type(), DataType::Struct(_)),
        };
        if !matches {
            return Err(invalid_layout());
        }
    }
    Ok(())
}

fn input_schema(
    input: &ConnectorWriteInputShape,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaRef, Failure> {
    let mut fields = Vec::with_capacity(input.field_count());
    for binding in input.fields_iter() {
        // Field clone is an opaque Arrow operation on the draft's validated
        // depth/aggregate-byte envelope; its internals require host admission.
        work.flush()?;
        let field = binding.field().clone();
        work.flush()?;
        fields.push(field);
        work.step()?;
    }
    let schema = Arc::new(Schema::new(fields));
    work.step()?;
    Ok(schema)
}

/// Freeze only the columns this flavor sends to a physical DATA writer.
/// File/position identities belong to delete and row-rewrite control, while
/// reserved lineage columns are real DATA fields with reserved definitions.
fn data_input_schema(
    input: &ConnectorWriteInputShape,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaRef, Failure> {
    let mut selected = Vec::new();
    match input {
        ConnectorWriteInputShape::Data { fields } => selected.extend(fields.iter()),
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => {
            selected.extend(data_fields.iter());
            for field in row_identity_fields {
                let name = field.field().name();
                if name.eq_ignore_ascii_case(crate::row_lineage_synth::ICEBERG_ROW_ID_COL)
                    || name.eq_ignore_ascii_case(
                        crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL,
                    )
                {
                    selected.push(field);
                }
                work.step()?;
            }
        }
        ConnectorWriteInputShape::PositionDelete {
            partition_source_fields,
            ..
        }
        | ConnectorWriteInputShape::DeletionVector {
            partition_source_fields,
            ..
        } => selected.extend(partition_source_fields.iter()),
        ConnectorWriteInputShape::EqualityDelete { .. } => {
            unreachable!("equality branch uses its own target")
        }
    }
    let mut fields = Vec::with_capacity(selected.len());
    for binding in selected {
        work.flush()?;
        fields.push(binding.field().clone());
        work.flush()?;
        work.step()?;
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn validate_data(
    handle: &IcebergWriterHandle,
    schema: &SchemaRef,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let recipe = handle.data().ok_or_else(|| {
        invalid(
            "data",
            "Iceberg data writer handle carries no data branch recipe",
        )
    })?;
    let source = recipe.target_schema_json().ok_or_else(|| {
        invalid(
            "data",
            "Iceberg data writer handle carries no frozen input schema",
        )
    })?;
    let facts = FrozenDataWriteFacts {
        format_version: handle.table().format_version(),
        table_location: copy_text(handle.table().table_location(), work)?,
        data_location: copy_text(handle.table().data_location(), work)?,
        target_partition_spec_id: handle.table().default_partition_spec_id(),
        partition_source_column_names: copy_texts(recipe.partition_source_column_names(), work)?,
        partition_column_names: copy_texts(recipe.partition_column_names(), work)?,
        transform_exprs: copy_texts(recipe.transform_exprs(), work)?,
        target_schema_json: copy_text(source, work)?,
        parquet_row_group_size_bytes: handle.output().parquet_row_group_size_bytes(),
    };
    work.step()?;
    work.flush()?;
    // The shared provider-owned algorithm retains exact type/ID/partition
    // semantics. It cannot acquire FileIO or annotate from external metadata.
    prepare_frozen_write_schema(schema, &facts, control).map_err(|error| match error {
        FrozenWriteSchemaError::Control(error) => Failure::Control(error),
        FrozenWriteSchemaError::Source(message) => invalid("data", message),
    })?;
    work.step()?;
    Ok(())
}

fn copy_texts(
    values: &[String],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<String>, Failure> {
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        result.push(copy_text(value, work)?);
        work.step()?;
    }
    Ok(result)
}

fn copy_text(value: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, Failure> {
    let mut result = String::with_capacity(value.len());
    let mut at = 0;
    while at < value.len() {
        let end = value.floor_char_boundary(at + (value.len() - at).min(COPY_BYTES));
        result.push_str(&value[at..end]);
        at = end;
        work.step()?;
    }
    Ok(result)
}

fn lift(error: ConnectorCodecError) -> Failure {
    match error.compile_control_error() {
        Some(cause) => Failure::Control(cause.clone()),
        None => Failure::Provider(error),
    }
}

fn invalid(path: &'static str, message: impl AsRef<str>) -> Failure {
    Failure::Provider(ConnectorCodecError::new(
        ConnectorFieldPath::root("iceberg_writer").field(path),
        ConnectorCodecErrorKind::InvalidValue,
        message,
    ))
}

#[cfg(test)]
mod tests;
