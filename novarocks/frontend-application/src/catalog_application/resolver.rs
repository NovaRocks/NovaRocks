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

use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_spi::connector::ConnectorProviderId;
use novarocks_sql::semantic::ObjectName;
use novarocks_types::naming::{resolve_catalog_namespace_name, resolve_catalog_table_name};

/// Preserve typed virtual-target refusal before any external catalog lookup.
/// Existing target/preparation diagnostics retain their original text channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetResolutionError {
    Message(String),
    Unsupported(novarocks_spi::connector::ConnectorError),
}
impl From<String> for TargetResolutionError {
    fn from(message: String) -> Self {
        Self::Message(message)
    }
}
impl std::fmt::Display for TargetResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(message) => f.write_str(message),
            Self::Unsupported(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for TargetResolutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Message(_) => None,
            Self::Unsupported(error) => Some(error),
        }
    }
}
impl TargetResolutionError {
    pub(crate) fn into_service(
        self,
    ) -> novarocks_query_application::session_error::QueryServiceError {
        use novarocks_query_application::session_error::{
            QueryServiceError, QueryServiceErrorKind,
        };
        match self {
            Self::Message(message) => {
                QueryServiceError::new(QueryServiceErrorKind::Internal, message)
            }
            Self::Unsupported(error) => QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ),
        }
    }
}
impl From<TargetResolutionError> for novarocks_query_application::api::CommandError {
    fn from(error: TargetResolutionError) -> Self {
        match error {
            TargetResolutionError::Message(message) => Self::from(message),
            TargetResolutionError::Unsupported(error) => {
                Self::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(error))
            }
        }
    }
}
impl From<TargetResolutionError> for novarocks_table_maintenance::runtime::TerminalError {
    fn from(error: TargetResolutionError) -> Self {
        match error {
            TargetResolutionError::Message(message) => Self::pre_dispatch_failed(message),
            TargetResolutionError::Unsupported(error) => Self::from_original_cause(error),
        }
    }
}
pub(crate) fn require_external_target(
    catalog: &str,
    namespace: &str,
) -> Result<(), TargetResolutionError> {
    if novarocks_sql::system_table::is_system_table_name(catalog, namespace) {
        return Err(TargetResolutionError::Unsupported(
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "system catalogs and information_schema have no mutable external table target",
            ),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetBackend {
    pub provider_id: ConnectorProviderId,
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
    "default_catalog is not a user table catalog; create an external catalog and SET catalog before using persistent tables".to_string()
}

fn missing_current_catalog_error(kind: &str) -> String {
    format!(
        "{kind} requires an external catalog; create one and SET catalog before using persistent tables"
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
) -> Result<TargetBackend, TargetResolutionError> {
    let resolved = resolve_catalog_table_name(
        name.parts.as_slice(),
        // The local session catalog is used only to identify virtual
        // targets. Missing external selection is rejected below, before
        // any provider admission; it grants no default external target.
        Some(current_catalog.unwrap_or(DEFAULT_CATALOG_NAME)),
        current_database,
    )?;
    require_external_target(&resolved.catalog, &resolved.namespace)?;
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("CREATE TABLE").into());
    }

    let provider_id = require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        provider_id,
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
) -> Result<TargetBackend, TargetResolutionError> {
    let resolved = resolve_catalog_table_name(
        name.parts.as_slice(),
        // The local session catalog is used only to identify virtual
        // targets. Missing external selection is rejected below, before
        // any provider admission; it grants no default external target.
        Some(current_catalog.unwrap_or(DEFAULT_CATALOG_NAME)),
        current_database,
    )?;
    require_external_target(&resolved.catalog, &resolved.namespace)?;
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() <= 2 {
        return Err(missing_current_catalog_error("Table operation").into());
    }

    let provider_id = require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        provider_id,
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: resolved.table,
    })
}

pub fn resolve_namespace_target(
    admission: &impl CatalogAdmission,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<TargetBackend, TargetResolutionError> {
    let resolved = resolve_catalog_namespace_name(
        name.parts.as_slice(),
        Some(current_catalog.unwrap_or(DEFAULT_CATALOG_NAME)),
    )?;
    require_external_target(&resolved.catalog, &resolved.namespace)?;
    reject_default_catalog_reference(name, current_catalog)?;
    if current_catalog.is_none() && name.parts.len() == 1 {
        return Err(missing_current_catalog_error("CREATE DATABASE").into());
    }

    let provider_id = require_catalog_admission(admission, &resolved.catalog)?;
    Ok(TargetBackend {
        provider_id,
        catalog: resolved.catalog,
        namespace: resolved.namespace,
        table: String::new(),
    })
}

fn require_catalog_admission(
    admission: &impl CatalogAdmission,
    catalog: &str,
) -> Result<ConnectorProviderId, String> {
    let Some(application) = admission.catalog_application() else {
        return Err(format!(
            "catalog `{catalog}` cannot be resolved without the frontend catalog application"
        ));
    };
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    application
        .admit_catalog(&instance_id)
        .require_ready(&instance_id)
        .map(|observation| observation.provider_id)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::ConnectorInstanceId;
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

    struct ReadyCatalogApplication {
        provider_id: ConnectorProviderId,
    }

    impl CatalogApplicationPort for ReadyCatalogApplication {
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

        fn admit_catalog(&self, instance_id: &ConnectorInstanceId) -> CatalogAdmission {
            CatalogAdmission::Ready(CatalogRuntimeObservation {
                attachment_id: uuid::Uuid::nil(),
                instance_id: instance_id.clone(),
                provider_id: self.provider_id.clone(),
                generation: 7,
            })
        }
    }

    struct ForbiddenCatalogAdmission;
    impl super::CatalogAdmission for ForbiddenCatalogAdmission {
        fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
            panic!("virtual target refusal must precede external catalog admission")
        }
    }

    #[test]
    fn virtual_write_targets_are_typed_refusals_before_catalog_admission() {
        use novarocks_spi::connector::ConnectorErrorKind;
        for (parts, catalog, database) in [
            (vec!["system", "metadata", "materialized_views"], None, "db"),
            (vec!["metadata", "materialized_views"], Some("system"), "db"),
            (vec!["materialized_views"], Some("SYSTEM"), "metadata"),
            (
                vec!["warehouse", "information_schema", "tables"],
                None,
                "db",
            ),
            (vec!["information_schema", "tables"], None, "db"),
            (vec!["tables"], Some("warehouse"), "INFORMATION_SCHEMA"),
        ] {
            let name = ObjectName {
                parts: parts.into_iter().map(str::to_owned).collect(),
            };
            for result in [
                resolve_table_target(&ForbiddenCatalogAdmission, &name, catalog, database),
                resolve_existing_table_target(&ForbiddenCatalogAdmission, &name, catalog, database),
            ] {
                let error = result.expect_err("virtual targets cannot be mutated");
                let TargetResolutionError::Unsupported(cause) = &error else {
                    panic!("readonly refusal must retain its typed Connector fact: {error}")
                };
                assert_eq!(cause.kind(), ConnectorErrorKind::Unsupported);
                let command = novarocks_query_application::api::CommandError::from(error.clone());
                assert!(command.failure_cause().is_some());
                assert!(error.into_service().failure_cause().is_some());
            }
        }
    }

    #[test]
    fn virtual_namespace_mutations_are_refused_before_external_admission() {
        for (parts, catalog) in [
            (vec!["system", "metadata"], None),
            (vec!["anything"], Some("system")),
            (vec!["warehouse", "information_schema"], None),
            (vec!["information_schema"], None),
        ] {
            let name = ObjectName {
                parts: parts.into_iter().map(str::to_owned).collect(),
            };
            assert!(matches!(
                resolve_namespace_target(&ForbiddenCatalogAdmission, &name, catalog),
                Err(TargetResolutionError::Unsupported(_))
            ));
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
        assert_eq!(error.to_string(), "unknown catalog `warehouse`");
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
        .expect_err("CREATE TABLE without a current external catalog must fail");
        assert_eq!(
            error.to_string(),
            "CREATE TABLE requires an external catalog; create one and SET catalog before using persistent tables"
        );

        let error = resolve_namespace_target(
            &admission,
            &ObjectName {
                parts: vec!["db_no_catalog".to_string()],
            },
            None,
        )
        .expect_err("CREATE DATABASE without a current external catalog must fail");
        assert_eq!(
            error.to_string(),
            "CREATE DATABASE requires an external catalog; create one and SET catalog before using persistent tables"
        );
    }

    #[test]
    fn default_catalog_is_not_a_user_table_catalog() {
        let admission = TestCatalogAdmission {
            application: Arc::new(AbsentCatalogApplication),
        };
        let expected = "default_catalog is not a user table catalog; create an external catalog and SET catalog before using persistent tables";

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
        assert_eq!(error.to_string(), expected);

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
        assert_eq!(error.to_string(), expected);

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
        assert_eq!(error.to_string(), expected);
    }

    #[test]
    fn external_target_uses_the_admitted_catalog_provider_identity() {
        let admission = TestCatalogAdmission {
            application: Arc::new(ReadyCatalogApplication {
                provider_id: ConnectorProviderId::parse("paimon").expect("provider ID"),
            }),
        };

        let target = resolve_existing_table_target(
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
        .expect("ready Paimon catalog target");

        assert_eq!(target.provider_id.as_str(), "paimon");
        assert_eq!(target.catalog, "warehouse");
        assert_eq!(target.namespace, "sales");
        assert_eq!(target.table, "orders");
    }
}
