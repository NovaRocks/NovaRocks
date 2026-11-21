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

//! Validation shared by the two explicit retention procedures.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorMaintenanceRetention};

use crate::catalog_config::{
    EXPIRE_SNAPSHOTS_MIN_RETENTION, EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY,
    IcebergCatalogMaintenanceConfig, REMOVE_ORPHAN_FILES_MIN_RETENTION,
    REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY,
};

#[derive(Clone, Copy)]
pub(super) enum RetentionProcedure {
    ExpireSnapshots,
    RemoveOrphanFiles,
}

pub(super) fn validate_retention_procedure(
    procedure: RetentionProcedure,
    format_version: u8,
    location_provider: Option<&str>,
    table_name: &str,
    retention: ConnectorMaintenanceRetention,
    configuration: IcebergCatalogMaintenanceConfig,
) -> Result<(), ConnectorError> {
    let (name, catalog_minimum, config_property, session_property) = match procedure {
        RetentionProcedure::ExpireSnapshots => (
            "expire_snapshots",
            configuration.expire_snapshots_min_retention,
            EXPIRE_SNAPSHOTS_MIN_RETENTION,
            EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY,
        ),
        RetentionProcedure::RemoveOrphanFiles => (
            "remove_orphan_files",
            configuration.remove_orphan_files_min_retention,
            REMOVE_ORPHAN_FILES_MIN_RETENTION,
            REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY,
        ),
    };
    if format_version > 3 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!("{name} is not supported for Iceberg table format version > 3. Table {table_name} format version is {format_version}."),
        ).with_public_failure_detail());
    }
    if let Some(provider) = location_provider {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "Table {table_name} specifies {provider} as a location provider. Writing to Iceberg tables with custom location provider is not supported."
            ),
        ).with_public_failure_detail());
    }
    let minimum = retention.session_minimum.unwrap_or(catalog_minimum);
    if retention.requested.is_shorter_than(minimum) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!(
                "Retention specified ({}) is shorter than the minimum retention configured in the system ({}). Minimum retention can be changed with {} configuration property or iceberg.{} session property",
                retention.requested, minimum, config_property, session_property,
            ),
        ).with_public_failure_detail());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorMaintenanceDuration;

    fn inputs(retention: &str, minimum: Option<&str>) -> ConnectorMaintenanceRetention {
        ConnectorMaintenanceRetention {
            requested: ConnectorMaintenanceDuration::parse(retention).unwrap(),
            session_minimum: minimum.map(|text| ConnectorMaintenanceDuration::parse(text).unwrap()),
        }
    }

    #[test]
    fn each_retention_procedure_honors_the_exact_threshold_and_original_units() {
        let catalog = IcebergCatalogMaintenanceConfig::default();
        for procedure in [
            RetentionProcedure::ExpireSnapshots,
            RetentionProcedure::RemoveOrphanFiles,
        ] {
            assert!(
                validate_retention_procedure(
                    procedure,
                    3,
                    None,
                    "s.t",
                    inputs("168h", None),
                    catalog
                )
                .is_ok()
            );
            let error = validate_retention_procedure(
                procedure,
                3,
                None,
                "s.t",
                inputs("24h", Some("168h")),
                catalog,
            )
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(error.message().contains("Retention specified (24.00h) is shorter than the minimum retention configured in the system (168.00h)"));
            assert!(
                validate_retention_procedure(
                    procedure,
                    3,
                    None,
                    "s.t",
                    inputs("24h", Some("1d")),
                    catalog
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn location_provider_and_future_formats_refuse_both_procedures() {
        let catalog = IcebergCatalogMaintenanceConfig::default();
        for procedure in [
            RetentionProcedure::ExpireSnapshots,
            RetentionProcedure::RemoveOrphanFiles,
        ] {
            for (format, provider, detail) in [
                (4, None, "Table s.t format version is 4."),
                (
                    3,
                    Some("custom.Writer"),
                    "Table s.t specifies custom.Writer as a location provider.",
                ),
            ] {
                let error = validate_retention_procedure(
                    procedure,
                    format,
                    provider,
                    "s.t",
                    inputs("1d", None),
                    catalog,
                )
                .unwrap_err();
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.public_failure_detail().unwrap().contains(detail));
            }
        }
    }
}
