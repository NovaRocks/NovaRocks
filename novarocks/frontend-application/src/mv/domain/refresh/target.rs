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

//! Iceberg MV target identity and statement-name resolution.

use crate::mv::domain::analysis::resolve_mv_name;
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_sql::semantic::ObjectName;
use novarocks_types::naming::{TableIdentity, normalize_identifier};

/// A normalized Iceberg MV target identity.  Refresh planning and the domain
/// persistence paths share this value without acquiring query assembly state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergMvTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl From<&TableIdentity> for IcebergMvTarget {
    fn from(target: &TableIdentity) -> Self {
        Self {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        }
    }
}

/// Resolves the SQL-level MV name to its normalized Iceberg target identity.
pub fn resolve_refresh_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name_parts: &[String],
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(
        &ObjectName {
            parts: name_parts.to_vec(),
        },
        current_database,
    )?;
    Ok(IcebergMvTarget {
        catalog: normalize_identifier(catalog)?,
        namespace,
        table,
    })
}

/// Resolves a SQL-level MV name to the public repository target used by
/// frontend command admission, without exposing the internal Iceberg planning
/// identity.
pub fn resolve_refresh_mv_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name_parts: &[String],
) -> Result<MvTarget, String> {
    let target = resolve_refresh_target(current_catalog, current_database, name_parts)?;
    Ok(MvTarget {
        catalog: Some(target.catalog),
        database: target.namespace,
        name: target.table,
    })
}

/// Validate the original target fence using an already observed exact Current.
/// This pure author is shared by synchronous and asynchronous observation.
pub(crate) fn validate_target_snapshot_observed(
    target: &IcebergMvTarget,
    mv_definition: &StoredKeyedMvProjection,
    binding: &MvTargetBinding,
    observed: &novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
) -> Result<(), String> {
    if observed.view.current_snapshot != binding.current_snapshot_id() {
        return Err(format!(
            "MV target {}.{}.{} changed during refresh planning",
            target.catalog, target.namespace, target.table
        ));
    }
    mv_definition
        .facts
        .validate_refresh_fence(&observed.view)
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget as ProductMvTarget;

    #[test]
    fn target_snapshot_comes_only_from_the_publication_output() {
        let mut fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        fixture.metadata_version = novarocks_spi::connector::ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"later-configuration-metadata"),
            Some(12),
        )
        .expect("metadata version");
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("projection"),
        };

        assert_eq!(projection.facts.publication_snapshot(), Some(12));
    }

    #[test]
    fn never_published_is_not_a_published_zero_row_result() {
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
                None,
            )
            .build()
            .expect("never published projection"),
        };
        assert_eq!(projection.facts.publication_snapshot(), None);
        let mut empty = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        let publication = empty.publication.as_mut().expect("published fixture");
        publication.logical_result_rows = Some(0);
        let empty = StoredKeyedMvProjection {
            mv_id: 1,
            facts: empty.build().unwrap(),
        };
        assert_eq!(empty.facts.publication_snapshot(), Some(11));
    }

    #[test]
    fn iceberg_mv_target_from_table_identity_preserves_exact_case() {
        let identity = TableIdentity {
            catalog: "TargetCase".to_string(),
            namespace: "NameSpace".to_string(),
            table: "MvTable".to_string(),
        };

        let target = IcebergMvTarget::from(&identity);

        assert_eq!(target.catalog, identity.catalog);
        assert_eq!(target.namespace, identity.namespace);
        assert_eq!(target.table, identity.table);
    }
    #[test]
    fn first_refresh_accepts_an_empty_slot_change_parent() {
        let mut fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        fixture.metadata_version = novarocks_spi::connector::ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"empty-slot-11"),
            Some(11),
        )
        .unwrap();
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().unwrap(),
        };
        assert!(projection.facts.never_published());
        assert_eq!(projection.facts.metadata_version().snapshot_id(), Some(11));
        let (_, current) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                Some(12),
                false,
            );
        projection
            .facts
            .validate_refresh_fence(&current)
            .expect("another known-empty slot parent remains a first refresh");
    }

    #[test]
    fn first_refresh_rechecks_both_file_totals_and_rejects_missing_totals() {
        let fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        let projection = fixture.build().unwrap();
        let (_, mut current) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                Some(12),
                false,
            );
        current.current_snapshot_file_totals.data_files = None;
        assert!(projection.validate_refresh_fence(&current).is_err());
        current.current_snapshot_file_totals.data_files = Some(0);
        current.current_snapshot_file_totals.delete_files = Some(1);
        assert!(projection.validate_refresh_fence(&current).is_err());
        current.current_snapshot_file_totals.delete_files = Some(0);
        current.current_snapshot_file_totals.data_files = Some(1);
        assert!(projection.validate_refresh_fence(&current).is_err());
    }
}
