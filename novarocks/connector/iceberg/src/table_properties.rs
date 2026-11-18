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

//! Finite Iceberg DDL declarations and user extra-property validation.
//!
//! Declarations do not grant a setter or publication capability. Dedicated
//! format-version, partition-spec and location changes retain their owners.

mod legacy;
pub(crate) use legacy::{
    LegacyAlterSetPreparation, prepare_legacy_alter_set_table_property_inputs,
};
pub use legacy::{
    LegacyPreparedTableProperty, LegacyPreparedTablePropertyValue,
    LegacyTablePropertyPreparationFailure, prepare_legacy_create_table_partitioning_property,
    prepare_legacy_create_table_property_inputs,
};

mod create;
pub(crate) use create::{
    PreparedCreateTableProperties, prepare_create_table_properties, typed_format_version,
};

mod projection;
pub use projection::project_partial_raw_table_property;
pub(crate) use projection::project_raw_table_property_set;

mod partitioning_evolution;
pub(crate) use partitioning_evolution::prepare_full_partitioning_change;

mod partitioning_binding;
pub(crate) use partitioning_binding::bind_create_table_partitioning;

mod partitioning;
pub use partitioning::{parse_table_partitioning_element, parse_table_partitioning_property};

mod data_size;
pub use data_size::{
    DataSizeFailureReason, DataSizeParseFailure, DataSizeProperty, DataSizePropertyFailure,
    DataSizeRangeFailure, NormalizedDataSize, normalize_data_size_property,
};

use crate::catalog_control::catalog_mutation::{
    default_create_format_version, reject_variant_shredding_properties,
    reject_variant_shredding_property, reserved_property,
};
use crate::iceberg::spec::FormatVersion;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPropertyChange, DdlPropertyDeclaration,
    DdlPropertyDeclarationError, DdlPropertyType, DdlPropertyValue,
};
use std::{collections::HashSet, sync::Arc};

/// Recognized Trino names whose corresponding writer/storage policy is absent.
/// Their raw keys remain ordinary storage keys unless an original guard rejects
/// that key independently. A declaration is not synthesized for these names.
pub const UNSUPPORTED_TABLE_PROPERTY_NAMES: &[&str] = &[
    "sorted_by",
    "compression_codec",
    "delete_after_commit_enabled",
    "max_previous_versions",
    "orc_bloom_filter_columns",
    "orc_bloom_filter_fpp",
    "parquet_bloom_filter_columns",
    "object_store_layout_enabled",
];

/// Defaults bind this provider's current no-override configuration. In
/// particular max_commit_retry has no catalog-level override; NULL here is
/// that property default, not the retry reader's separate numeric default.
/// Native maintenance/statistics NULL defaults explicitly restore an absent
/// raw key. The application keeps DEFAULT distinct from an explicit SQL NULL.
pub fn table_property_declarations()
-> Result<Vec<DdlPropertyDeclaration>, DdlPropertyDeclarationError> {
    use DdlPropertyType as T;
    use DdlPropertyValue as V;
    let format_version = match default_create_format_version() {
        FormatVersion::V1 => 1,
        FormatVersion::V2 => 2,
        FormatVersion::V3 => 3,
    };
    [
        ("format", T::Varchar, V::Varchar("PARQUET".into()), false),
        (
            "format_version",
            T::Integer,
            V::Integer(format_version),
            false,
        ),
        (
            "partitioning",
            T::ArrayVarchar,
            V::ArrayVarchar(vec![]),
            false,
        ),
        ("location", T::Varchar, V::Null, false),
        ("max_commit_retry", T::Integer, V::Null, false),
        ("parquet_writer_row_group_size", T::Varchar, V::Null, false),
        ("target_max_file_size", T::Varchar, V::Null, false),
        ("gc_enabled", T::Boolean, V::Null, false),
        ("data_location", T::Varchar, V::Null, false),
        ("statistics_collect_on_write", T::Boolean, V::Null, false),
        ("maintenance_enabled", T::Boolean, V::Null, false),
        ("extra_properties", T::MapVarcharVarchar, V::Null, true),
    ]
    .into_iter()
    .map(|(name, kind, default, hidden)| DdlPropertyDeclaration::new(name, kind, default, hidden))
    .collect()
}

/// This is the property scope's canonical name, not a quoted SQL identifier.
pub fn is_recognized_unsupported_table_property(canonical_name: &str) -> bool {
    UNSUPPORTED_TABLE_PROPERTY_NAMES.contains(&canonical_name)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExtraPropertyWrite {
    Create,
    Alter,
}

/// Project only a checked D30 value. SQL expression evaluation, NULL-key
/// rejection and scope errors remain with Query Application. Ordered values
/// reach this owner before any map collection can overwrite a duplicate key.
pub fn extra_property_sets(
    value: &DdlPropertyValue,
    operation: ExtraPropertyWrite,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    let DdlPropertyValue::MapVarcharVarchar(entries) = value else {
        return Err(invalid("extra_properties requires map(varchar, varchar)"));
    };
    // The reference property decoder rejects NULL values before normalizing
    // keys. Do not silently remove a nullable entry or replace it with text.
    if let Some((key, _)) = entries.iter().find(|(_, value)| value.is_none()) {
        return Err(invalid(format!(
            "extra property value cannot be NULL: {key}"
        )));
    }
    let mut seen = HashSet::new();
    let mut pairs: Vec<(Arc<str>, Arc<str>)> = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let key = normalized_extra_property_key(key);
        if !seen.insert(key.clone()) {
            return Err(duplicate_extra_property(&key));
        }
        let value = value
            .as_ref()
            .ok_or_else(|| invalid("extra property value is NULL"))?;
        pairs.push((key.into(), value.as_str().into()));
    }
    if operation == ExtraPropertyWrite::Create {
        // Preserve the original create author's whole-input variant prepass.
        reject_variant_shredding_properties(&pairs)?;
    }
    let mut changes = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        if operation == ExtraPropertyWrite::Alter {
            reject_variant_shredding_property(&key)?;
        }
        reject_extra_property_key(&key, operation)?;
        changes.push(ConnectorPropertyChange::Set { key, value });
    }
    Ok(changes)
}

/// The unset-extra-properties operation supplies these actual keys.
/// This is not a blanket reset inferred from extra_properties=DEFAULT. Missing
/// key checks and exact table-state requirements stay with property_updates.
pub fn extra_property_unsets(
    keys: &[String],
    if_exists: bool,
) -> Result<Vec<ConnectorPropertyChange>, ConnectorError> {
    let mut seen = HashSet::new();
    let mut changes = Vec::with_capacity(keys.len());
    for key in keys {
        let key = normalized_extra_property_key(key);
        if !seen.insert(key.clone()) {
            return Err(duplicate_extra_property(&key));
        }
        // The original mutation author permits removing an already stored
        // variant key; only Set calls the variant capability guard.
        reject_extra_property_key(&key, ExtraPropertyWrite::Alter)?;
        changes.push(ConnectorPropertyChange::Unset {
            key: key.into(),
            if_exists,
        });
    }
    Ok(changes)
}

fn normalized_extra_property_key(key: &str) -> String {
    key.to_lowercase()
}
fn duplicate_extra_property(key: &str) -> ConnectorError {
    invalid(format!("duplicate normalized extra property: {key}"))
}

fn reject_extra_property_key(
    key: &str,
    operation: ExtraPropertyWrite,
) -> Result<(), ConnectorError> {
    crate::document_storage::keyed::write::reject_reserved_property(key)?;
    if crate::schema_mapping::nominal_declaration::reserved_property(key) {
        return Err(invalid(match operation {
            ExtraPropertyWrite::Create => {
                "Iceberg nominal declarations come from column definitions"
            }
            ExtraPropertyWrite::Alter => {
                "Iceberg nominal declarations are owned by schema mutations"
            }
        }));
    }
    // This exact historical CREATE key is opaque metadata. ADR-0233 keeps
    // UPDATE and MERGE on MOR regardless of its stored value. ALTER retains
    // the original user-statement namespace guard.
    if let Some(reason) = reserved_property(key)
        && !(operation == ExtraPropertyWrite::Create && key == "novarocks.update.mode")
    {
        return Err(invalid(format!(
            "Iceberg table property `{key}` is reserved: {reason}"
        )));
    }
    if declared_raw_property(key) {
        return Err(invalid(format!(
            "extra property `{key}` has a dedicated declared property"
        )));
    }
    Ok(())
}

fn declared_raw_property(key: &str) -> bool {
    matches!(
        key,
        "format-version"
            | "format_version"
            | "write.format.default"
            | "commit.retry.num-retries"
            | "write.parquet.row-group-size-bytes"
            | "write.target-file-size-bytes"
            | "gc.enabled"
            | "write.data.path"
            | "novarocks.maintenance.enabled"
    ) || key == crate::stats_assembler::COLLECT_ON_WRITE_PROPERTY
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
#[path = "table_properties/create_error_tests.rs"]
mod create_error_tests;
