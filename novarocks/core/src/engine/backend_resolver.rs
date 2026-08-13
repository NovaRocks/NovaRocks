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
use crate::engine::StandaloneState;
use crate::engine::domain::{
    DmlExecutionKernel, MaintenanceExecutionKernel, MvExecutionKernel, QueryPreparationKernel,
    ViewExecutionKernel,
};
use crate::sql::parser::ast::ObjectName;
use novarocks_catalog::identifier::{resolve_catalog_namespace_name, resolve_catalog_table_name};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetBackend {
    pub(crate) backend_name: &'static str,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
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

pub(crate) trait CatalogAdmission {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort>;
}

impl CatalogAdmission for StandaloneState {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }
}

impl CatalogAdmission for Arc<StandaloneState> {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.as_ref().catalog_application()
    }
}

macro_rules! impl_kernel_catalog_admission {
    ($kernel:ty) => {
        impl CatalogAdmission for $kernel {
            fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
                self.catalog_application().map(Arc::as_ref)
            }
        }
    };
}

impl_kernel_catalog_admission!(QueryPreparationKernel);
impl_kernel_catalog_admission!(DmlExecutionKernel);
impl_kernel_catalog_admission!(MvExecutionKernel);
impl_kernel_catalog_admission!(ViewExecutionKernel);
impl_kernel_catalog_admission!(MaintenanceExecutionKernel);

pub(crate) fn resolve_table_target(
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

pub(crate) fn resolve_existing_table_target(
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

pub(crate) fn resolve_namespace_target(
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

#[cfg(test)]
mod tests {
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

    #[test]
    fn external_table_target_requires_catalog_admission_when_port_is_configured() {
        let state = Arc::new(StandaloneState {
            catalog_application: Some(Arc::new(AbsentCatalogApplication)),
            ..Default::default()
        });
        let error = resolve_existing_table_target(
            &state,
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
}
