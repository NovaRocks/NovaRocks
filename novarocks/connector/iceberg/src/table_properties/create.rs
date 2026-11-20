// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Provider composition of already evaluated CREATE table properties.
//!
//! The original schema author still parses and binds partition strings. This
//! module owns no catalog lookup, schema reconstruction or publication.

use super::{ExtraPropertyWrite, project_raw_table_property_set};
use crate::iceberg::spec::FormatVersion;
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPropertyChange,
    DdlPropertyValue,
};
use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fmt,
    sync::Arc,
};

pub(crate) struct PreparedCreateTableProperties {
    pub(crate) format_version: FormatVersion,
    pub(crate) location: Option<String>,
    pub(crate) partitioning: Vec<Option<String>>,
    pub(crate) raw_properties: BTreeMap<String, String>,
}

impl PreparedCreateTableProperties {
    pub(crate) fn copy_raw_properties(
        &self,
        control: &dyn ConnectorOperationControl,
    ) -> Result<BTreeMap<String, String>, ConnectorError> {
        self.raw_properties
            .iter()
            .map(|(key, value)| Ok((copy_text(key, control)?, copy_text(value, control)?)))
            .collect()
    }

    pub(crate) fn copy_location(
        &self,
        control: &dyn ConnectorOperationControl,
    ) -> Result<Option<String>, ConnectorError> {
        self.location
            .as_deref()
            .map(|value| copy_text(value, control))
            .transpose()
    }
}

#[derive(Debug)]
enum CreatePropertyContractFailure {
    MissingRequired(&'static str),
    RawSetProducedUnset,
}
impl fmt::Display for CreatePropertyContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRequired(name) => {
                write!(f, "Evaluated CREATE properties omit required {name}")
            }
            Self::RawSetProducedUnset => {
                f.write_str("CREATE Set projection produced an Unset instruction")
            }
        }
    }
}
impl Error for CreatePropertyContractFailure {}
impl FailureProjection for CreatePropertyContractFailure {
    fn failure_fact(&self) -> FailureFact {
        ConnectorErrorKind::Internal.failure_fact()
    }
}
fn contract(cause: CreatePropertyContractFailure) -> ConnectorError {
    ConnectorError::from_domain(ConnectorErrorKind::Internal, cause)
}

/// Required non-NULL defaults come from the declaration/evaluation owner.
/// Missing values are contract failures, never an instruction to guess them.
pub(crate) fn prepare_create_table_properties(
    properties: &[(Arc<str>, DdlPropertyValue)],
    comment: Option<&str>,
    control: &dyn ConnectorOperationControl,
) -> Result<PreparedCreateTableProperties, ConnectorError> {
    prepare_create_table_properties_inner(properties, comment, control)
        .map_err(novarocks_spi::connector::InvalidTableProperty::from_property_error)
}

/// Keep ordinary CTAS on its existing v3, warehouse-rooted staging profile.
pub(crate) fn prepare_ctas_storage(
    properties: &[(Arc<str>, DdlPropertyValue)],
    comment: Option<&str>,
    control: &dyn ConnectorOperationControl,
) -> Result<PreparedCreateTableProperties, ConnectorError> {
    let storage = prepare_create_table_properties(properties, comment, control)?;
    if storage.format_version != FormatVersion::V3 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg CTAS requires format_version = 3",
        ));
    }
    if storage.location.is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg CTAS does not support an explicit table location",
        ));
    }
    // A raw metadata path property stays an existing writer property. This
    // profile does not use the MV-only final table relocation contract.
    Ok(storage)
}

/// Validate evaluated user properties for the existing document-managed staged
/// path. The caller keeps application markers separate and binds partitioning
/// against the admitted materialized-view output.
pub(crate) fn prepare_mv_staged_storage(
    properties: &[(Arc<str>, DdlPropertyValue)],
    control: &dyn ConnectorOperationControl,
) -> Result<(PreparedCreateTableProperties, Option<String>), ConnectorError> {
    let mut storage = prepare_create_table_properties(properties, None, control)?;
    if storage.format_version != FormatVersion::V3 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg materialized-view storage requires format_version = 3",
        ));
    }
    if storage.location.as_deref().is_some_and(str::is_empty) {
        return Err(
            novarocks_spi::connector::InvalidTableProperty::from_property_error(invalid(
                "Iceberg materialized-view location cannot be empty",
            )),
        );
    }
    let metadata_path = storage
        .raw_properties
        .remove(crate::catalog::staged_metadata_path::WRITE_METADATA_PATH);
    Ok((storage, metadata_path))
}

fn prepare_create_table_properties_inner(
    properties: &[(Arc<str>, DdlPropertyValue)],
    comment: Option<&str>,
    control: &dyn ConnectorOperationControl,
) -> Result<PreparedCreateTableProperties, ConnectorError> {
    control.check_active()?;
    let mut seen = HashSet::new();
    let mut has_format = false;
    let mut format_version = None;
    let mut partitioning = None;
    let mut location = None;
    let mut raw_properties = BTreeMap::new();
    for (name, value) in properties {
        control.check_active()?;
        if !seen.insert(name.as_ref()) {
            return Err(invalid(format!(
                "Duplicate Iceberg table property '{name}'"
            )));
        }
        match name.as_ref() {
            "format_version" => format_version = Some(typed_format_version(value)?),
            "partitioning" => {
                let DdlPropertyValue::ArrayVarchar(fields) = value else {
                    return Err(invalid(
                        "Iceberg partitioning requires a non-NULL array(varchar)",
                    ));
                };
                let mut retained = Vec::with_capacity(fields.len());
                for field in fields {
                    control.check_active()?;
                    retained.push(
                        field
                            .as_deref()
                            .map(|text| copy_text(text, control))
                            .transpose()?,
                    );
                }
                partitioning = Some(retained);
            }
            "location" => {
                let DdlPropertyValue::Varchar(value) = value else {
                    return Err(invalid("Iceberg location requires a non-NULL varchar"));
                };
                location = Some(copy_text(value, control)?);
            }
            _ => {
                let changes = project_raw_table_property_set(
                    name,
                    value,
                    ExtraPropertyWrite::Create,
                    control,
                )?;
                for change in changes {
                    control.check_active()?;
                    let ConnectorPropertyChange::Set { key, value } = change else {
                        return Err(contract(CreatePropertyContractFailure::RawSetProducedUnset));
                    };
                    if raw_properties.contains_key(key.as_ref()) {
                        return Err(invalid(format!("Duplicate Iceberg raw property '{key}'")));
                    }
                    raw_properties.insert(copy_text(&key, control)?, copy_text(&value, control)?);
                }
                if name.as_ref() == "format" {
                    has_format = true;
                }
            }
        }
    }
    if !has_format {
        return Err(contract(CreatePropertyContractFailure::MissingRequired(
            "format",
        )));
    }
    let format_version = format_version.ok_or_else(|| {
        contract(CreatePropertyContractFailure::MissingRequired(
            "format_version",
        ))
    })?;
    let partitioning = partitioning.ok_or_else(|| {
        contract(CreatePropertyContractFailure::MissingRequired(
            "partitioning",
        ))
    })?;
    if let Some(comment) = comment {
        if raw_properties.contains_key("comment") {
            return Err(invalid("Duplicate Iceberg raw property 'comment'"));
        }
        raw_properties.insert("comment".to_owned(), copy_text(comment, control)?);
    }
    control.check_active()?;
    Ok(PreparedCreateTableProperties {
        format_version,
        location,
        partitioning,
        raw_properties,
    })
}

// This is the integer-to-version boundary. The original private internal raw
// creator keeps its string parser and separate input contract.
pub(crate) fn typed_format_version(
    value: &DdlPropertyValue,
) -> Result<FormatVersion, ConnectorError> {
    match value {
        DdlPropertyValue::Integer(1) => Ok(FormatVersion::V1),
        DdlPropertyValue::Integer(2) => Ok(FormatVersion::V2),
        DdlPropertyValue::Integer(3) => Ok(FormatVersion::V3),
        DdlPropertyValue::Integer(_) => Err(invalid("Iceberg format_version must be 1, 2, or 3")),
        _ => Err(invalid(
            "Iceberg format_version requires a non-NULL integer",
        )),
    }
}
fn copy_text(
    value: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<String, ConnectorError> {
    for _ in value.as_bytes().chunks(256) {
        control.check_active()?;
    }
    let value = value.to_owned();
    control.check_active()?;
    Ok(value)
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
