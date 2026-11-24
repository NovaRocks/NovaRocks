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

//! MV document errors and exact prepared-target validation.
use crate::persistence::codec::{InterpretationDocument, PersistenceCodecError};
use crate::persistence::validation::ValidationError;
use novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
pub enum MvDocumentError {
    KeyedStorage(novarocks_spi::connector::KeyedDocumentReadError),
    Codec(PersistenceCodecError),
    Contract(String),
    Connector(novarocks_spi::connector::ConnectorError),
    Validation(ValidationError),
}

impl std::fmt::Display for MvDocumentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyedStorage(error) => {
                write!(formatter, "MV keyed document storage failed: {error}")
            }
            Self::Codec(error) => write!(formatter, "MV document codec failed: {error}"),
            Self::Contract(message) => write!(formatter, "invalid MV document contract: {message}"),
            Self::Connector(error) => {
                write!(formatter, "MV document storage contract failed: {error}")
            }
            Self::Validation(error) => write!(formatter, "MV document validation failed: {error}"),
        }
    }
}

impl std::error::Error for MvDocumentError {}

impl From<novarocks_spi::connector::KeyedDocumentReadError> for MvDocumentError {
    fn from(error: novarocks_spi::connector::KeyedDocumentReadError) -> Self {
        Self::KeyedStorage(error)
    }
}

impl From<PersistenceCodecError> for MvDocumentError {
    fn from(value: PersistenceCodecError) -> Self {
        Self::Codec(value)
    }
}

impl From<novarocks_spi::connector::ConnectorError> for MvDocumentError {
    fn from(value: novarocks_spi::connector::ConnectorError) -> Self {
        Self::Connector(value)
    }
}

impl From<ValidationError> for MvDocumentError {
    fn from(value: ValidationError) -> Self {
        Self::Validation(value)
    }
}

pub(crate) fn validate_create_target(
    interpretation: &InterpretationDocument,
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<(), MvDocumentError> {
    if interpretation.target.object_id.as_bytes() != target.object_id().as_bytes().as_ref()
        || interpretation.target.schema_version.as_bytes() != target.schema_version().as_ref()
        || interpretation.target.partition_spec_version.as_bytes()
            != target.partition_spec_version().as_ref()
    {
        return Err(MvDocumentError::Contract(
            "interpretation target does not match the provider-prepared target".to_string(),
        ));
    }
    let prepared_partition_fields = target
        .partition_fields()
        .iter()
        .map(crate::persistence::codec::TargetPartitionFieldBinding::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(MvDocumentError::Contract)?;
    if interpretation.target.partition_fields != prepared_partition_fields {
        return Err(MvDocumentError::Contract(
            "interpretation partition binding does not match the provider-prepared target"
                .to_string(),
        ));
    }
    // Every prepared column must be bound, and every binding must name a
    // prepared column.
    //
    // This compares what is bound, not where it sits. The target's column
    // order is the one the statement asked for, while L orders its bindings by
    // logical identity and lets several typed identities share one physical
    // column; the two orders have no reason to coincide, and requiring it made
    // any view of more than a couple of columns unconstructible. The prepared
    // list's own shape -- dense ordinals, distinct field identities -- is
    // guaranteed by the type that carries it.
    let prepared_fields = target
        .fields()
        .iter()
        .map(|prepared| (prepared.provider_field_id().as_ref(), prepared))
        .collect::<BTreeMap<_, _>>();
    let bound_fields = interpretation
        .target
        .fields
        .iter()
        .map(|field| field.target_field_id.as_bytes())
        .collect::<BTreeSet<_>>();
    if !prepared_fields.keys().copied().eq(bound_fields)
        || interpretation.target.fields.iter().any(|binding| {
            let prepared = prepared_fields[binding.target_field_id.as_bytes()];
            prepared.source_type() != Some(&binding.source_type)
                || prepared.nullable() != binding.nullable
        })
    {
        return Err(MvDocumentError::Contract(
            "interpretation target bindings and the provider-prepared columns are not the same set"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    include!("document_target_tests.rs");
}
