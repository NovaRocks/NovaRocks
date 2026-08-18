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

//! Backend target resolution for standalone connector dispatch.
//!
//! This is the one place that maps a parsed SQL object name plus session
//! context into the backend name and normalized catalog/namespace/table
//! identifiers used by connector traits.

use std::sync::Arc;

use crate::catalog_application::CatalogApplicationPort;
use novarocks_catalog::identifier::{resolve_catalog_namespace_name, resolve_catalog_table_name};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, ConnectorWriteLease,
};
use novarocks_sql::syntax::ObjectName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetBackend {
    pub backend_name: &'static str,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

// Ownership: this drops the local catalog snapshot for an already-resolved
// `TargetBackend`. Both the target type and `QueryCatalogService` are owned by
// catalog_application, and the operation is pure catalog cache maintenance with
// no plan, fragment, or write-transaction state, so it belongs beside the
// resolution that produced the target rather than in a DML writer.
pub fn invalidate_iceberg_caches(
    state: &impl crate::catalog_application::query_catalog::CatalogServiceSource,
    target: &TargetBackend,
) -> Result<(), String> {
    state
        .catalog_service()
        .invalidate_table(&target.catalog, &target.namespace, &target.table)
}

const DEFAULT_CATALOG_NAME: &str = "default_catalog";

fn is_default_catalog(value: &str) -> bool {
    value.eq_ignore_ascii_case(DEFAULT_CATALOG_NAME)
}

fn default_catalog_error() -> String {
    "default_catalog is not a user table catalog; create an external Iceberg catalog and SET catalog before using persistent tables".to_string()
}

fn missing_current_catalog_error(kind: &str) -> String {
    format!(
        "{kind} requires an Iceberg catalog; create an external Iceberg catalog and SET catalog before using persistent tables"
    )
}

fn reject_default_catalog_reference(
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<(), String> {
    if current_catalog.is_some_and(is_default_catalog)
        || name
            .parts
            .first()
            .is_some_and(|part| is_default_catalog(part))
    {
        return Err(default_catalog_error());
    }
    Ok(())
}

/// The single catalog fact target resolution needs: the optional catalog
/// application that admits a catalog name.
///
/// Core owns this contract because target resolution is a Core operation, but
/// it deliberately owns no implementation: every implementor is a
/// composition-side capability value that holds the port, so each `impl` lives
/// with its own owner instead of being gathered here.
pub trait CatalogAdmission {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort>;
}

pub fn resolve_table_target(
    admission: &impl CatalogAdmission,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("CREATE TABLE"));
    }

    let resolved =
        resolve_catalog_table_name(name.parts.as_slice(), current_catalog, current_database)?;
    require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub fn resolve_existing_table_target(
    admission: &impl CatalogAdmission,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("Table operation"));
    }

    let resolved =
        resolve_catalog_table_name(name.parts.as_slice(), current_catalog, current_database)?;
    require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub fn resolve_namespace_target(
    admission: &impl CatalogAdmission,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<TargetBackend, String> {
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() == 1 {
        return Err(missing_current_catalog_error("CREATE DATABASE"));
    }

    let resolved = resolve_catalog_namespace_name(name.parts.as_slice(), current_catalog)?;
    require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        backend_name: "iceberg",
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: String::new(),
    })
}

fn require_catalog_admission(
    admission: &impl CatalogAdmission,
    catalog: &str,
) -> Result<(), String> {
    let Some(application) = admission.catalog_application() else {
        return Ok(());
    };
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    application
        .admit_catalog(&instance_id)
        .require_ready(&instance_id)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Loads the Iceberg table handle for an already-resolved write target.
///
/// Like `invalidate_iceberg_caches`, this acts on the resolver's own
/// `TargetBackend` and names no query-assembly type: it checks the lease
/// against the resolved instance and reads the table through it.
pub fn iceberg_connector_table_handle(
    exact_lease: &ConnectorWriteLease,
    target: &TargetBackend,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorTableHandle, String> {
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    if exact_lease.binding_key().instance_id != instance_id {
        return Err("Iceberg write lease does not match the target connector instance".to_string());
    }
    let metadata = exact_lease
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| {
            format!("load Iceberg write target through connector metadata: {error}")
        })?;
    Ok(metadata.table)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog_application::{
        CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind,
        CatalogApplicationPort, CatalogCreateCommand, CatalogDropCommand,
        CatalogRuntimeObservation,
    };

    struct AbsentCatalogApplication;

    impl CatalogApplicationPort for AbsentCatalogApplication {
        fn create_catalog(
            &self,
            _command: CatalogCreateCommand,
        ) -> Result<CatalogRuntimeObservation, CatalogApplicationError> {
            Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "not used by backend resolver test",
            ))
        }

        fn drop_catalog(
            &self,
            _command: CatalogDropCommand,
        ) -> Result<(), CatalogApplicationError> {
            Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "not used by backend resolver test",
            ))
        }

        fn admit_catalog(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> CatalogAdmission {
            CatalogAdmission::Absent
        }
    }

    struct TestCatalogAdmission {
        application: Arc<dyn CatalogApplicationPort>,
    }

    impl super::CatalogAdmission for TestCatalogAdmission {
        fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
            Some(self.application.as_ref())
        }
    }

    #[test]
    fn external_table_target_requires_catalog_admission_when_port_is_configured() {
        let admission = TestCatalogAdmission {
            application: Arc::new(AbsentCatalogApplication),
        };
        let error = resolve_existing_table_target(
            &admission,
            &ObjectName {
                parts: vec![
                    "warehouse".to_string(),
                    "sales".to_string(),
                    "orders".to_string(),
                ],
            },
            None,
            "default_db",
        )
        .expect_err("absent attachment must block an external table target");
        assert_eq!(error, "unknown catalog `warehouse`");
    }

    #[test]
    fn persistent_table_target_without_current_catalog_is_rejected() {
        let admission = TestCatalogAdmission {
            application: Arc::new(AbsentCatalogApplication),
        };

        let error = resolve_table_target(
            &admission,
            &ObjectName {
                parts: vec!["t_no_catalog".to_string()],
            },
            None,
            "default_db",
        )
        .expect_err("CREATE TABLE without a current Iceberg catalog must fail");
        assert_eq!(
            error,
            "CREATE TABLE requires an Iceberg catalog; create an external Iceberg catalog and SET catalog before using persistent tables"
        );

        let error = resolve_namespace_target(
            &admission,
            &ObjectName {
                parts: vec!["db_no_catalog".to_string()],
            },
            None,
        )
        .expect_err("CREATE DATABASE without a current Iceberg catalog must fail");
        assert_eq!(
            error,
            "CREATE DATABASE requires an Iceberg catalog; create an external Iceberg catalog and SET catalog before using persistent tables"
        );
    }

    #[test]
    fn default_catalog_is_not_a_user_table_catalog() {
        let admission = TestCatalogAdmission {
            application: Arc::new(AbsentCatalogApplication),
        };
        let expected = "default_catalog is not a user table catalog; create an external Iceberg catalog and SET catalog before using persistent tables";

        // Qualified by name, regardless of the session catalog.
        let error = resolve_table_target(
            &admission,
            &ObjectName {
                parts: vec![
                    "default_catalog".to_string(),
                    "db1".to_string(),
                    "t_default_catalog".to_string(),
                ],
            },
            None,
            "default_db",
        )
        .expect_err("a default_catalog-qualified table must be rejected");
        assert_eq!(error, expected);

        // Selected through the session, with an unqualified name.
        let error = resolve_table_target(
            &admission,
            &ObjectName {
                parts: vec!["t_default_catalog".to_string()],
            },
            Some("default_catalog"),
            "db1",
        )
        .expect_err("a session on default_catalog must be rejected");
        assert_eq!(error, expected);

        // The name check is case-insensitive, so casing cannot bypass it.
        let error = resolve_existing_table_target(
            &admission,
            &ObjectName {
                parts: vec![
                    "DEFAULT_CATALOG".to_string(),
                    "db1".to_string(),
                    "t_default_catalog".to_string(),
                ],
            },
            None,
            "default_db",
        )
        .expect_err("default_catalog rejection must ignore casing");
        assert_eq!(error, expected);
    }
}
