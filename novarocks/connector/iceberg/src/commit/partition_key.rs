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

//! Partition affinity uses the same SDK values as the staged data splitter.

use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Int64Array, new_null_array};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_connector_contract::{
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorWriteBinding,
    ConnectorWriteFieldBinding, ConnectorWriteLayoutRecipe, ConnectorWriteLayoutRecipeDraft,
};
use novarocks_spi::connector::write_stack::ConnectorWriteLayoutPreference;
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorError, ConnectorErrorKind, ConnectorFieldPath, ConnectorPartitionKeyEvaluator,
    ConnectorWriteLayoutExecution,
};

use super::frozen_write::parse_partition_transform;
use super::write_adaptation::{adapt_batch, validate_batch_schema};
use super::write_stack::codec::{decode_write_layout, encode_write_layout};
use super::write_stack::domain::IcebergDataBranchRecipe;
use crate::iceberg::arrow::{
    PartitionValueCalculator, arrow_struct_to_literal, schema_to_arrow_schema,
};
use crate::iceberg::spec::{Literal, PartitionSpec, Schema, Transform};

pub(crate) fn partition_layout(
    binding: &ConnectorWriteBinding,
    spec_id: i32,
    data: &IcebergDataBranchRecipe,
    consumed: &[&ConnectorWriteFieldBinding],
) -> Result<Option<ConnectorWriteLayoutPreference>, ConnectorError> {
    if data.partition_source_column_names().is_empty() {
        return Ok(None);
    }
    let bad_metadata = |message| ConnectorError::new(ConnectorErrorKind::CorruptData, message);
    let schema_json = data
        .target_schema_json()
        .ok_or_else(|| bad_metadata("missing frozen Iceberg partition source schema".to_owned()))?;
    let schema: Schema = crate::metadata_json::from_str(schema_json)
        .map_err(|error| bad_metadata(error.to_string()))?;
    let schema = Arc::new(schema);
    let mut partition_spec = PartitionSpec::builder(schema.clone()).with_spec_id(spec_id);
    let mut sources = BTreeSet::new();
    let mut tokens = Vec::new();
    let mut fields = Vec::new();
    for ((source, name), transform) in data
        .partition_source_column_names()
        .iter()
        .zip(data.partition_column_names())
        .zip(data.transform_exprs())
    {
        let transform = parse_partition_transform(transform)
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::Unsupported, message))?;
        let field = schema
            .field_by_name_case_insensitive(source)
            .ok_or_else(|| {
                bad_metadata(format!(
                    "Iceberg partition source column {source} is missing from schema"
                ))
            })?;
        if sources.insert(field.id) {
            let token = consumed
                .iter()
                .find(|input| input.field().name().eq_ignore_ascii_case(source))
                .ok_or_else(|| {
                    bad_metadata(format!(
                        "Iceberg partition source {source} is absent from the writer input"
                    ))
                })?
                .token();
            tokens.push(token);
            fields.push(field.clone());
        }
        partition_spec = partition_spec
            .add_partition_field(
                schema.name_by_field_id(field.id).ok_or_else(|| {
                    bad_metadata(format!(
                        "Iceberg partition source {source} has no canonical name"
                    ))
                })?,
                name.clone(),
                transform,
            )
            .map_err(|error| bad_metadata(error.to_string()))?;
    }
    let spec = partition_spec
        .build()
        .map_err(|error| bad_metadata(error.to_string()))?;
    if spec.is_unpartitioned() {
        // An all-void spec uses the same unpartitioned writer as an empty spec.
        return Ok(None);
    }
    let projected = Schema::builder()
        .with_schema_id(schema.schema_id())
        .with_fields(fields)
        .build()
        .map_err(|error| bad_metadata(error.to_string()))?;
    let header = ConnectorEnvelopeHeader::new(
        binding.descriptor().provider_id.clone(),
        binding.catalog_handle().clone(),
        ConnectorCodecCategory::WriteLayout,
        write_revision(),
    );
    let payload = ConnectorEncodedPayload::new(header, encode_write_layout(&projected, &spec)?);
    let recipe =
        ConnectorWriteLayoutRecipeDraft::try_new(binding.clone(), payload, tokens.clone())?;
    Ok(Some(ConnectorWriteLayoutPreference::PartitionAffinity {
        inputs: tokens,
        recipe,
    }))
}

pub(super) fn write_revision() -> ConnectorCodecRevision {
    ConnectorCodecRevision::try_new(crate::contract_revision::ICEBERG_CONTRACT_REVISION)
        .expect("Iceberg revision is nonzero")
}

pub(super) fn bad_recipe(message: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root("iceberg_write_layout"),
        ConnectorCodecErrorKind::InvalidValue,
        message,
    )
}

pub(crate) fn layout_facts(
    draft: &ConnectorWriteLayoutRecipeDraft,
) -> Result<(Schema, PartitionSpec), ConnectorCodecError> {
    let binding = draft.binding();
    let identity = &binding.descriptor().provider_id;
    if identity.as_str() != crate::PROVIDER_ID {
        return Err(bad_recipe("write layout belongs to another provider"));
    }
    draft
        .payload()
        .header()
        .validate_expected::<ConnectorCodecError>(
            identity,
            binding.catalog_handle(),
            ConnectorCodecCategory::WriteLayout,
            write_revision(),
        )?;
    let (schema, spec) = decode_write_layout(draft.payload())?;
    let fields = schema.as_struct().fields();
    let partition_fields = spec.fields();
    let partition_sources = partition_fields
        .iter()
        .map(|field| field.source_id)
        .collect::<BTreeSet<_>>();
    if fields.len() != draft.inputs().len()
        || fields.len() != partition_sources.len()
        || fields
            .iter()
            .any(|field| !partition_sources.contains(&field.id))
        || spec.is_unpartitioned()
        || partition_fields.iter().any(|field| {
            matches!(
                field.transform,
                Transform::Unknown | Transform::Bucket(0) | Transform::Truncate(0)
            )
        })
    {
        return Err(bad_recipe(
            "Iceberg layout recipe must carry its exact partition source fields and transforms",
        ));
    }
    // Rebind the original spec's identities against the SDK's own schema laws.
    let spec = spec
        .into_unbound()
        .bind(Arc::new(schema.clone()))
        .map_err(|error| bad_recipe(error.to_string()))?;
    Ok((schema, spec))
}

pub(crate) struct IcebergWriteLayoutExecution {
    binding: ConnectorWriteBinding,
}

impl IcebergWriteLayoutExecution {
    pub(crate) fn new(binding: ConnectorWriteBinding) -> Self {
        Self { binding }
    }
}

impl ConnectorWriteLayoutExecution for IcebergWriteLayoutExecution {
    fn open(
        &self,
        recipe: &ConnectorWriteLayoutRecipe,
        input: &ArrowSchema,
    ) -> Result<Arc<dyn ConnectorPartitionKeyEvaluator>, ConnectorError> {
        let draft = recipe.draft();
        if draft.binding() != &self.binding {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg partition layout must use its exact catalog generation",
            ));
        }
        let (schema, spec) = layout_facts(draft).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::CorruptData, error.to_string())
        })?;
        Ok(Arc::new(IcebergPartitionKeyEvaluator::new(
            &schema, &spec, input,
        )?))
    }
}

struct IcebergPartitionKeyEvaluator {
    calculator: PartitionValueCalculator,
    input: ArrowSchemaRef,
    target: ArrowSchemaRef,
    transformed_inputs: Box<[bool]>,
}

impl IcebergPartitionKeyEvaluator {
    fn new(
        schema: &Schema,
        spec: &PartitionSpec,
        input: &ArrowSchema,
    ) -> Result<Self, ConnectorError> {
        let invalid = |message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);
        let target = schema_to_arrow_schema(schema).map_err(|error| invalid(error.to_string()))?;
        // The receiver's checked write projection owns rejection of required
        // NULL data. Keep those rows routable up to that original boundary.
        let target = Arc::new(ArrowSchema::new(
            target
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        if input.fields().len() != target.fields().len() {
            return Err(invalid(
                "Iceberg partition layout input width differs from its recipe".to_owned(),
            ));
        }
        let actual_sources = spec
            .fields()
            .iter()
            .filter(|field| field.transform != Transform::Void)
            .map(|field| field.source_id)
            .collect::<BTreeSet<_>>();
        let transformed_inputs = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| actual_sources.contains(&field.id))
            .collect::<Box<_>>();
        // Slot names are engine-local. The recipe binds their occurrences
        // positionally. Void-only sources can be typed NULL without reading
        // their original values, including nested retired source columns.
        let input = Arc::new(ArrowSchema::new(
            input
                .fields()
                .iter()
                .zip(target.fields())
                .zip(&transformed_inputs)
                .map(|((source, to), &transformed)| {
                    if transformed {
                        Arc::new(source.as_ref().clone().with_name(to.name()))
                    } else {
                        to.clone()
                    }
                })
                .collect::<Vec<_>>(),
        ));
        validate_batch_schema(&input, &target).map_err(invalid)?;
        let calculator = PartitionValueCalculator::try_new(spec, schema)
            .map_err(|error| invalid(error.to_string()))?;
        Ok(Self {
            calculator,
            input,
            target,
            transformed_inputs,
        })
    }
}

impl ConnectorPartitionKeyEvaluator for IcebergPartitionKeyEvaluator {
    fn partition_keys(&self, input: &RecordBatch) -> Result<Int64Array, ConnectorError> {
        if input.num_rows() == 0 {
            return Ok(Int64Array::new_null(0));
        }
        let sources = input
            .columns()
            .iter()
            .zip(self.input.fields())
            .zip(&self.transformed_inputs)
            .map(|((array, field), &transformed)| {
                if transformed {
                    array.clone()
                } else {
                    new_null_array(field.data_type(), input.num_rows())
                }
            })
            .collect();
        let input = RecordBatch::try_new(self.input.clone(), sources).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
        })?;
        let adapted = adapt_batch(&input, &self.target)
            .map_err(super::write_adaptation::WriteAdaptationError::into_connector_error)?;
        let partition_values = self
            .calculator
            .calculate(&adapted)
            .map_err(sdk_layout_error)?;
        let tuples = arrow_struct_to_literal(&partition_values, self.calculator.partition_type())
            .map_err(sdk_layout_error)?;
        let keys = tuples
            .into_iter()
            .map(|value| {
                match value {
                    Some(Literal::Struct(tuple)) => {
                        // Hash exactly the tuple type the SDK splitter uses for
                        // equality, including nulls and float canonicalization.
                        let mut hash = DefaultHasher::new();
                        tuple.hash(&mut hash);
                        Ok(hash.finish() as i64)
                    }
                    _ => Err(ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "Iceberg partition calculator returned a non-struct row",
                    )),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Int64Array::from(keys))
    }
}

fn sdk_layout_error(error: crate::iceberg::Error) -> ConnectorError {
    let kind = match error.kind() {
        crate::iceberg::ErrorKind::FeatureUnsupported => ConnectorErrorKind::Unsupported,
        _ => ConnectorErrorKind::InvalidRequest,
    };
    ConnectorError::new(kind, error.to_string())
}

#[cfg(test)]
mod tests;
