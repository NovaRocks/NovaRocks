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

//! Bind one observed declaration, including when a catalog reuses version IDs
//! after history expiry. This digest grants neither a lease nor lookup authority.

use crate::iceberg::spec::{ViewMetadata, ViewRepresentation, ViewVersion};
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::{
    ConnectorError, ConnectorViewColumn, read_stack::ConnectorReadSchemaCreation,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

type Failure = PureProviderCompileError<ConnectorError>;

pub(super) fn observed(
    metadata: &ViewMetadata,
    version: &ViewVersion,
    columns: &[ConnectorViewColumn],
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<str>, Failure> {
    let mut digest = Sha256::new();
    let mut write = |bytes: &[u8]| -> Result<(), Failure> {
        creation.work(1)?;
        digest.update(bytes);
        Ok(())
    };
    write(b"novarocks-iceberg-view-declaration-v1")?;
    write(metadata.uuid().as_bytes())?;
    write(&metadata.current_version_id().to_le_bytes())?;
    write(&version.schema_id().to_le_bytes())?;
    write(&version.timestamp_ms().to_le_bytes())?;
    match version.default_catalog() {
        Some(catalog) => {
            write(&[1])?;
            text(catalog, &mut write)?;
        }
        None => write(&[0])?,
    }
    write(&(version.default_namespace().iter().count() as u64).to_le_bytes())?;
    for component in version.default_namespace().iter() {
        text(component, &mut write)?;
    }
    write(&(version.representations().iter().count() as u64).to_le_bytes())?;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql) = representation;
        text(&sql.dialect, &mut write)?;
        text(&sql.sql, &mut write)?;
    }
    let mut summary: Vec<_> = version.summary().iter().collect();
    summary.sort_by(|left, right| left.0.cmp(right.0));
    write(&(summary.len() as u64).to_le_bytes())?;
    for (key, value) in summary {
        text(key, &mut write)?;
        text(value, &mut write)?;
    }
    write(&(columns.len() as u64).to_le_bytes())?;
    for column in columns {
        text(&column.name, &mut write)?;
        match &column.source_field_name {
            Some(name) => {
                write(&[1])?;
                text(name, &mut write)?;
            }
            None => write(&[0])?,
        }
        novarocks_type_contract::write_value_type_identity_observed(
            column.value_type.value_type(),
            &mut write,
        )?;
        novarocks_type_contract::write_value_layout_identity_observed(
            column.value_type.typed_binding().layout(),
            &mut write,
        )?;
    }
    Ok(format!("iceberg-view:{:x}", digest.finalize()).into())
}
fn text(value: &str, write: &mut impl FnMut(&[u8]) -> Result<(), Failure>) -> Result<(), Failure> {
    write(&(value.len() as u64).to_le_bytes())?;
    for chunk in value.as_bytes().chunks(1024) {
        write(chunk)?;
    }
    Ok(())
}
