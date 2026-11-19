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

use std::collections::BTreeMap;

use novarocks_parser::ast::{Ident, ObjectName};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorProviderBindingKey, ConnectorProviderId,
};
use novarocks_types::naming::{
    DEFAULT_CATALOG, TableIdentity, normalize_identifier, resolve_catalog_namespace_name,
};

use crate::planning::catalog::MetadataTableKind;

/// An observation of the selected catalog at the original statement's admission boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogQualification {
    Absent,
    Unavailable,
    Ready {
        provider: ConnectorProviderId,
        binding: ConnectorProviderBindingKey,
    },
}

/// The observations for catalog names actually encountered in the original statement.
/// A catalog reported absent or unavailable cannot be observed again by a saved view.
#[derive(Clone, Debug, Default)]
pub struct CatalogQualifications {
    entries: BTreeMap<ConnectorInstanceId, CatalogQualification>,
}

impl CatalogQualifications {
    pub fn observation(&self, instance: &ConnectorInstanceId) -> Option<&CatalogQualification> {
        self.entries.get(instance)
    }

    pub fn record(
        &mut self,
        instance: ConnectorInstanceId,
        observation: CatalogQualification,
    ) -> Result<(), super::SqlCompileError> {
        use std::collections::btree_map::Entry;
        if let CatalogQualification::Ready { binding, .. } = &observation
            && binding.instance_id != instance
        {
            return Err(super::SqlCompileError::InvalidRequest(
                "catalog qualification observed a different connector instance".into(),
            ));
        }
        match self.entries.entry(instance) {
            Entry::Occupied(existing) if existing.get() != &observation => {
                Err(super::SqlCompileError::InvalidRequest(
                    "catalog qualification changed during one statement".into(),
                ))
            }
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(entry) => {
                entry.insert(observation);
                Ok(())
            }
        }
    }

    pub(super) fn merge(&mut self, other: &Self) -> Result<(), super::SqlCompileError> {
        for (instance, observation) in &other.entries {
            self.record(instance.clone(), observation.clone())?;
        }
        Ok(())
    }

    pub(super) fn entries(
        &self,
    ) -> impl Iterator<Item = (&ConnectorInstanceId, &CatalogQualification)> {
        self.entries.iter()
    }

    /// The caller owns complete-name CTE precedence.
    pub(super) fn classify<'a>(
        &self,
        name: &'a ObjectName,
        catalog: Option<&str>,
        database: &str,
    ) -> Option<QualifiedMetadata<'a, '_>> {
        let (namespace, instance, authored) = quoted_catalog(name, catalog, database)?;
        let CatalogQualification::Ready { provider, binding } = self.observation(&instance)? else {
            return None;
        };
        if provider.as_str() != "iceberg" {
            return None;
        }
        let (base, suffix) = authored.value.split_once('$')?;
        let kind = MetadataTableKind::parse(suffix).ok()?;
        let table = normalize_identifier(base).ok()?;
        if !table.eq_ignore_ascii_case(base) {
            return None;
        }
        Some(QualifiedMetadata {
            relation: TableIdentity::new(&namespace.catalog, &namespace.namespace, &table),
            kind,
            authored,
            binding,
        })
    }
}

pub(super) struct QualifiedMetadata<'a, 'q> {
    pub(super) relation: TableIdentity,
    pub(super) kind: MetadataTableKind,
    pub(super) authored: &'a Ident,
    pub(super) binding: &'q ConnectorProviderBindingKey,
}

pub(super) fn candidate_catalog(
    name: &ObjectName,
    catalog: Option<&str>,
    database: &str,
) -> Option<ConnectorInstanceId> {
    quoted_catalog(name, catalog, database).map(|(_, instance, _)| instance)
}

/// Discover the selected catalog without interpreting the quoted relation suffix.
fn quoted_catalog<'a>(
    name: &'a ObjectName,
    catalog: Option<&str>,
    database: &str,
) -> Option<(
    novarocks_types::naming::CatalogNamespaceIdentity,
    ConnectorInstanceId,
    &'a Ident,
)> {
    let (authored, prefix) = name.parts.split_last()?;
    if !authored.quoted || !authored.value.contains('$') {
        return None;
    }
    let scope = match prefix {
        [] => vec![database.to_owned()],
        [namespace] => vec![namespace.value.clone()],
        [catalog, namespace] => vec![catalog.value.clone(), namespace.value.clone()],
        _ => return None,
    };
    let namespace =
        resolve_catalog_namespace_name(&scope, Some(catalog.unwrap_or(DEFAULT_CATALOG))).ok()?;
    // These components are already decoded. Do not reinterpret whitespace or
    // an embedded pair of backticks as a different catalog or namespace.
    let authored_catalog = if let [catalog, _] = scope.as_slice() {
        catalog.as_str()
    } else {
        catalog.unwrap_or(DEFAULT_CATALOG)
    };
    if !namespace.catalog.eq_ignore_ascii_case(authored_catalog)
        || !namespace.namespace.eq_ignore_ascii_case(scope.last()?)
    {
        return None;
    }
    if namespace.catalog == DEFAULT_CATALOG
        || crate::system_table::is_system_table_name(&namespace.catalog, &namespace.namespace)
    {
        return None;
    }
    let instance = ConnectorInstanceId::try_from_canonical(&namespace.catalog).ok()?;
    Some((namespace, instance, authored))
}

#[cfg(test)]
pub(crate) fn iceberg_metadata_qualifications_for_test(catalog: &str) -> CatalogQualifications {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let binding = novarocks_spi::connector::ConnectorProviderBinding::try_new(
        provider.clone(),
        catalog,
        [51; 16],
        "unit-test-binding",
    )
    .unwrap();
    let binding = binding.binding_key().clone();
    let mut qualifications = CatalogQualifications::default();
    qualifications
        .record(
            binding.instance_id.clone(),
            CatalogQualification::Ready { provider, binding },
        )
        .unwrap();
    qualifications
}

#[cfg(test)]
pub(crate) fn resolve_quoted_iceberg_metadata_for_test(
    name: &ObjectName,
    current_catalog: Option<&str>,
    database: &str,
    provider: &dyn crate::catalog::IcebergMetadataTableProvider,
) -> Option<crate::catalog::QuotedMetadataTable> {
    let catalog = current_catalog.unwrap_or("ice");
    let qualifications = iceberg_metadata_qualifications_for_test(catalog);
    let found = qualifications.classify(name, Some(catalog), database)?;
    let resolved = provider.get_iceberg_metadata_table(
        Some(&found.relation.catalog),
        &found.relation.namespace,
        &found.relation.table,
        found.kind.provider_kind(),
    );
    Some(crate::catalog::QuotedMetadataTable {
        relation: found.relation,
        kind: found.kind,
        resolved,
    })
}
