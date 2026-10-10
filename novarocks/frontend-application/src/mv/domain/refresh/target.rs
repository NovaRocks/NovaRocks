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
use crate::mv::domain::refresh::target_binding::{
    MvTargetBinding, load_mv_target_binding_with_ports, load_mv_target_binding_with_ports_typed,
};
use novarocks_mv_application::persistence::projection::{MvPublicationState, StoredMvProjection};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorError, ConnectorRequestContext};
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

/// Loads the sealed target binding used by one Iceberg MV refresh attempt.
///
/// This capability deliberately receives only the two admitted ports, the
/// normalized domain target, and the request context.  It does not depend on
/// the aggregate refresh source or query assembly state.
pub fn load_iceberg_mv_target_binding(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    target: &IcebergMvTarget,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    load_mv_target_binding_with_ports(
        connector_control,
        storage_observation,
        &TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context,
    )
}

/// Typed counterpart used by the background refresh path. A caller must map
/// the returned connector category to its retry policy instead of inferring
/// one from text after it has crossed the preparation boundary.
pub(crate) fn load_iceberg_mv_target_binding_typed(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    target: &IcebergMvTarget,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, ConnectorError> {
    #[cfg(debug_assertions)]
    if consume_scheduler_transient_preparation_fault(target) {
        return Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            format!(
                "test-injected temporary connector unavailability while preparing {}.{}.{}",
                target.catalog, target.namespace, target.table
            ),
        ));
    }
    load_mv_target_binding_with_ports_typed(
        connector_control,
        storage_observation,
        &TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context,
    )
}

/// Debug-only, one-shot cross-process seam for the scheduler recovery test.
/// The injected provider-style error enters at the typed binding boundary, so
/// the test proves its category survives planning and background scheduling.
#[cfg(debug_assertions)]
fn consume_scheduler_transient_preparation_fault(target: &IcebergMvTarget) -> bool {
    let Some(directory) = std::env::var_os("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR") else {
        return false;
    };
    let trigger = std::path::PathBuf::from(directory).join(format!(
        "mvx4-scheduler-transient-preparation-{}.trigger",
        target.table
    ));
    let consumed = trigger.with_extension("consumed");
    std::fs::rename(trigger, consumed).is_ok()
}

/// Validates that the persisted MV definition still names the target snapshot
/// that was observed for refresh planning.
pub fn validate_target_snapshot(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvProjection,
    binding: &MvTargetBinding,
) -> Result<(), String> {
    let actual = binding.current_snapshot_id();
    let expected = published_target_snapshot_id(mv_definition)?;
    if actual != expected {
        return Err(format!(
            "target table {}.{}.{} was modified outside NovaRocks: expected snapshot {:?}, current snapshot {:?}",
            target.catalog, target.namespace, target.table, expected, actual
        ));
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn recorded_target_snapshot_id(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvProjection,
) -> Result<i64, String> {
    published_target_snapshot_id(mv_definition)?.ok_or_else(|| {
        format!(
            "iceberg materialized view {}.{}.{} has no recorded target snapshot",
            target.catalog, target.namespace, target.table
        )
    })
}

fn published_target_snapshot_id(projection: &StoredMvProjection) -> Result<Option<i64>, String> {
    match projection.facts.publication() {
        MvPublicationState::NeverPublished => Ok(None),
        MvPublicationState::Published(publication) => publication
            .output_version()
            .snapshot_id()
            .map(Some)
            .ok_or_else(|| "published Iceberg MV output has no provider snapshot ID".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
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
        let projection = StoredMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("projection"),
        };

        assert_eq!(published_target_snapshot_id(&projection).unwrap(), Some(11));
    }

    #[test]
    fn never_published_is_not_a_published_zero_row_result() {
        let projection = StoredMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
                None,
            )
            .build()
            .expect("never published projection"),
        };
        assert_eq!(published_target_snapshot_id(&projection).unwrap(), None);
        let mut empty = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        let publication = empty.publication.as_mut().expect("published fixture");
        publication.output.empty_result = true;
        publication.statistics.logical_result_rows = Some(0);
        empty.storage_rows = Some(0);
        let empty = StoredMvProjection {
            mv_id: 1,
            facts: empty.build().unwrap(),
        };
        assert_eq!(published_target_snapshot_id(&empty).unwrap(), Some(11));
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
}
