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

//! Test-only server half of the IMV statelessness harness.
//!
//! `novarocks_imv_stateless_rebuild` observes the current D/L/P/C document
//! set directly from the lake and reports its exact document revisions.
//!
//! Because this "bypass the runtime caches and rebuild from the lake" surface
//! must never exist on a production path, the procedure is guarded behind the
//! explicit finite MV domain grants for the current FE launch. It is
//! wired only through the standalone CALL dispatch and is exercised by the
//! sql-test runner's `@imv_stateless_rebuild` directive.
//!
//! The historical `package` and `provenance` level names remain in the
//! test-only procedure vocabulary. They now mean readable D/L/C and an exact
//! published P respectively. An MV created but never refreshed has no P and
//! reports `package`.
//!
//! There was a `full` level that proved the Accelerator is a rebuildable
//! cache in-process: it cleared one MV's records and rebuilt them from the
//! lake. It is retired. Clearing an MV's record is, to this design, the same
//! event as the provider having dropped it, so the rebuild could only
//! reinstall a read-only candidate -- reading a view's documents is not
//! owning it -- and the process was left unable to manage a target it had
//! just proven it could rebuild, with no way back that does not go through a
//! recovery barrier and an operator declaration.
//!
//! The `wipe` level is what remains of that idea, and it has the right shape:
//! it clears the Accelerator and returns, and the deployment then restarts the
//! frontend so startup rediscovery does the rebuilding through the ordinary
//! path, barrier and all. The whole property is proven end to end by the
//! `mv-storage-contract` suite on the product topology.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;

use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_parser::ast::{CallStatement, LiteralKind, MaintenanceValue};
use novarocks_query_application::api::{
    QueryResult, ResultField as QueryResultColumn, build_arrow_query_result,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
};
use novarocks_types::naming::normalize_identifier;

pub const PROCEDURE_NAME: &str = "novarocks_imv_stateless_rebuild";

/// Fidelity level a stateless rebuild is expected to reconstruct. Mirrors the
/// sql-test runner's `ImvStatelessLevel`, but is a separate type because the
/// runner lives in a different crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StatelessLevel {
    Baseline,
    Package,
    Provenance,
    Wipe,
}

impl StatelessLevel {
    fn from_sql(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "baseline" => Ok(Self::Baseline),
            "package" => Ok(Self::Package),
            "provenance" => Ok(Self::Provenance),
            "wipe" => Ok(Self::Wipe),
            other => Err(format!(
                "unknown stateless rebuild level `{other}`; expected one of baseline, package, provenance, wipe"
            )),
        }
    }

    fn as_sql(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Package => "package",
            Self::Provenance => "provenance",
            Self::Wipe => "wipe",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvStatelessRebuildRequest {
    pub catalog: String,
    pub namespace: String,
    pub mv: String,
    pub required_level: StatelessLevel,
}

impl ImvStatelessRebuildRequest {
    /// Lowers the one test-only procedure directly from parser-owned syntax.
    /// A non-target procedure remains a route miss for the command router.
    pub(crate) fn from_typed_call(
        statement: &CallStatement,
        current_database: &str,
    ) -> Result<Option<Self>, String> {
        let name_parts = statement
            .procedure
            .parts
            .iter()
            .map(|part| normalize_identifier(&part.value))
            .collect::<Result<Vec<_>, _>>()?;
        let [catalog, namespace, procedure] = name_parts.as_slice() else {
            return Err("CALL procedure name must be catalog.system.procedure".to_string());
        };
        if procedure != PROCEDURE_NAME {
            return Ok(None);
        }
        if namespace != "system" {
            return Err("Iceberg procedures must use system namespace".to_string());
        }

        let table = typed_string_argument(statement, "table")
            .ok_or_else(|| format!("{PROCEDURE_NAME} requires a `table` argument"))?;
        let (namespace, mv) = split_table_reference(table, current_database)?;
        let required_level = match typed_string_argument(statement, "level") {
            Some(level) => StatelessLevel::from_sql(level)?,
            None => StatelessLevel::Package,
        };
        Ok(Some(Self {
            catalog: catalog.clone(),
            namespace,
            mv,
            required_level,
        }))
    }
}

fn typed_string_argument<'a>(statement: &'a CallStatement, name: &str) -> Option<&'a str> {
    statement.arguments.iter().find_map(|argument| {
        let argument_name = argument.name.as_ref()?;
        (normalize_identifier(&argument_name.value).ok()?.as_str() == name).then(|| {
            let MaintenanceValue::Literal(literal) = &argument.value else {
                return None;
            };
            let LiteralKind::String(value) = &literal.kind else {
                return None;
            };
            Some(value.as_str())
        })?
    })
}

/// Split a `table` argument into `(namespace, mv)`. A bare name inherits the
/// current database as its namespace; a two-part `namespace.mv` is used as-is;
/// anything with more parts is rejected.
fn split_table_reference(table: &str, current_database: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = table.split('.').collect();
    match parts.as_slice() {
        [mv] => Ok((current_database.to_string(), (*mv).to_string())),
        [namespace, mv] => Ok(((*namespace).to_string(), (*mv).to_string())),
        _ => Err(format!(
            "{PROCEDURE_NAME} `table` must be `<mv>` or `<namespace>.<mv>`, got `{table}`"
        )),
    }
}

pub fn execute_typed_novarocks_imv_stateless_rebuild(
    connector_control: &dyn ConnectorControlResolver,
    readiness: &MvReadinessPort,
    product: &novarocks_mv_application::service::MvKeyedProductService,
    statement: &CallStatement,
    current_database: &str,
    connector_context: ConnectorRequestContext,
) -> Result<Option<StatementResult>, String> {
    let Some(req) = ImvStatelessRebuildRequest::from_typed_call(statement, current_database)?
    else {
        return Ok(None);
    };
    let controls = readiness
        .test_controls()
        .ok_or("MV test CALL requires explicit authorization")?;
    let claim = controls.claim_call(
        &req.catalog,
        &req.namespace,
        &req.mv,
        req.required_level.as_sql(),
    )?;
    execute_request_with_context(
        connector_control,
        readiness,
        product,
        &req,
        connector_context,
        claim,
    )
    .map(Some)
}

fn execute_request_with_context(
    connector_control: &dyn ConnectorControlResolver,
    readiness: &MvReadinessPort,
    product: &novarocks_mv_application::service::MvKeyedProductService,
    req: &ImvStatelessRebuildRequest,
    connector_context: ConnectorRequestContext,
    mut claim: novarocks_failpoint::scope::ActionClaim,
) -> Result<StatementResult, String> {
    let instance_id = ConnectorInstanceId::parse(&req.catalog)
        .map_err(|error| format!("parse stateless rebuild catalog identity: {error}"))?;
    let exact_lease = ConnectorControlResolver::acquire_current(connector_control, &instance_id)
        .map_err(|error| format!("acquire stateless rebuild catalog generation: {error}"))?;
    let table = ConnectorTableIdentity {
        instance_id,
        namespace: Arc::from(req.namespace.as_str()),
        table: Arc::from(req.mv.as_str()),
    };
    let loaded_table = crate::connector::metadata_load_connector_table_with_planning_pin(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )
    .map_err(|error| format!("load stateless rebuild table metadata: {error}"))?;
    // `wipe` is deliberately not a rebuild level. It proves the exact MV exists
    // in the lake, clears only the closed current Accelerator family, and
    // returns with admission permanently stopped. The runner must then kill/restart
    // that FE; startup observation is the only permitted rebuild path.
    //
    // The existence proof reads the same exact document set used by the
    // non-destructive levels below.
    let documents = observe_current_documents(
        readiness,
        &exact_lease,
        &loaded_table,
        connector_context,
        req,
    )?;
    if req.required_level == StatelessLevel::Wipe {
        readiness.wipe_accelerator_for_test(product, uuid::Uuid::now_v7(), &mut claim)?;
        return Ok(StatementResult::Query(build_rebuild_result(
            StatelessLevel::Wipe,
            &hex::encode(documents.versions.definition_revision().as_bytes()),
            &hex::encode(documents.versions.interpretation_revision().as_bytes()),
            documents
                .publication_revision
                .map(|revision| hex::encode(revision.as_bytes()))
                .as_deref(),
            "accelerator-wiped",
        )?));
    }

    claim.record_hit().map_err(|error| error.to_string())?;
    let publication_revision = documents
        .publication_revision
        .map(|revision| hex::encode(revision.as_bytes()));
    let available = available_level(publication_revision.is_some());
    // For the non-destructive levels the procedure reports the level it CAN
    // reconstruct; the sql-test runner asserts `available >= required`, so
    // `required_level` is not gated here.
    Ok(StatementResult::Query(build_rebuild_result(
        available,
        &hex::encode(documents.versions.definition_revision().as_bytes()),
        &hex::encode(documents.versions.interpretation_revision().as_bytes()),
        publication_revision.as_deref(),
        "lake-documents",
    )?))
}

fn available_level(has_publication: bool) -> StatelessLevel {
    if has_publication {
        StatelessLevel::Provenance
    } else {
        StatelessLevel::Package
    }
}

/// Read the view's own documents straight from the lake, so a level that
/// clears the cache cannot clear the cache of an MV that is not there.
fn observe_current_documents(
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    exact_lease: &novarocks_spi::connector::ConnectorControlPlanningPin,
    loaded_table: &novarocks_spi::connector::ConnectorTableMetadata,
    context: ConnectorRequestContext,
    req: &ImvStatelessRebuildRequest,
) -> Result<
    novarocks_mv_application::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    String,
> {
    let binding = exact_lease
        .binding()
        .metadata()
        .capture_table_object_binding(
            novarocks_spi::connector::ConnectorTableObjectCaptureRequest {
                table: loaded_table.identity.clone(),
                resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                selector: novarocks_spi::connector::ConnectorTableObjectSelector::Current,
                context: context.clone(),
            },
        )
        .map_err(|error| format!("bind stateless rebuild target object: {error}"))?;
    let document_storage_binding = exact_lease
        .derive_document_storage_lease()
        .map_err(|error| format!("derive stateless rebuild document lease: {error}"))?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &document_storage_binding,
        loaded_table.identity.clone(),
        binding.object_id.clone(),
        context,
    );
    readiness
        .candidate_reader()
        .observe_current(&document_storage_binding, request)
        .map_err(|e| {
            format!(
                "MV '{}.{}' has no readable keyed lake documents in catalog '{}': {e}",
                req.namespace, req.mv, req.catalog
            )
        })
}

fn build_rebuild_result(
    available: StatelessLevel,
    definition_revision: &str,
    interpretation_revision: &str,
    publication_revision: Option<&str>,
    rebuild_source: &str,
) -> Result<QueryResult, String> {
    let columns = vec![
        column("AvailableLevel", false),
        column("DefinitionRevision", false),
        column("InterpretationRevision", false),
        column("PublicationRevision", true),
        column("RebuildSource", false),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![available.as_sql().to_string()])),
        Arc::new(StringArray::from(vec![definition_revision.to_string()])),
        Arc::new(StringArray::from(vec![interpretation_revision.to_string()])),
        Arc::new(StringArray::from(vec![
            publication_revision.map(str::to_string),
        ])),
        Arc::new(StringArray::from(vec![rebuild_source.to_string()])),
    ];
    build_query_result(columns, arrays)
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    build_arrow_query_result(columns, arrays)
        .map_err(|e| format!("build stateless rebuild result failed: {e}"))
}

fn column(name: &str, nullable: bool) -> QueryResultColumn {
    QueryResultColumn::new(
        name,
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, nullable),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{
        ast::{MaintenanceStatement, Statement},
        parse,
    };

    #[test]
    fn available_level_tracks_exact_publication_presence() {
        assert_eq!(available_level(false), StatelessLevel::Package);
        assert_eq!(available_level(true), StatelessLevel::Provenance);
    }

    #[test]
    fn level_round_trips_case_insensitive() {
        for (input, expected) in [
            ("baseline", StatelessLevel::Baseline),
            ("Package", StatelessLevel::Package),
            ("PROVENANCE", StatelessLevel::Provenance),
            ("wipe", StatelessLevel::Wipe),
        ] {
            let parsed = StatelessLevel::from_sql(input).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(StatelessLevel::from_sql(parsed.as_sql()).unwrap(), expected);
        }
    }

    #[test]
    fn level_rejects_unknown() {
        let err = StatelessLevel::from_sql("partial").unwrap_err();
        assert!(err.contains("unknown stateless rebuild level"), "{err}");
    }

    fn parse_request(
        sql: &str,
        current_database: &str,
    ) -> Result<ImvStatelessRebuildRequest, String> {
        let statements = parse(sql).map_err(|error| error.to_string())?;
        let [Statement::Maintenance(MaintenanceStatement::Call(statement))] = statements.as_slice()
        else {
            return Err("expected typed CALL statement".to_string());
        };
        ImvStatelessRebuildRequest::from_typed_call(statement, current_database)?
            .ok_or_else(|| "expected stateless rebuild procedure".to_string())
    }

    #[test]
    fn from_call_parses_two_part_table_and_level() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'analytics.mv_orders', level => 'baseline')",
            "default_db",
        )
        .unwrap();
        assert_eq!(req.catalog, "ice");
        assert_eq!(req.namespace, "analytics");
        assert_eq!(req.mv, "mv_orders");
        assert_eq!(req.required_level, StatelessLevel::Baseline);
    }

    #[test]
    fn from_call_bare_table_defaults_namespace_to_current_database() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'mv_orders')",
            "analytics",
        )
        .unwrap();
        assert_eq!(req.namespace, "analytics");
        assert_eq!(req.mv, "mv_orders");
    }

    #[test]
    fn from_call_defaults_level_to_package() {
        let req = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'analytics.mv_orders')",
            "default_db",
        )
        .unwrap();
        assert_eq!(req.required_level, StatelessLevel::Package);
    }

    #[test]
    fn typed_call_normalizes_quoted_procedure_and_argument_identifiers() {
        let req = parse_request(
            "CALL `ICE`.`SYSTEM`.`NOVAROCKS_IMV_STATELESS_REBUILD`(\
                `TABLE` => 'analytics.mv_orders', `LEVEL` => 'WIPE')",
            "default_db",
        )
        .unwrap();

        assert_eq!(req.catalog, "ice");
        assert_eq!(req.required_level, StatelessLevel::Wipe);
    }

    #[test]
    fn from_call_requires_table_argument() {
        let err = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(level => 'package')",
            "default_db",
        )
        .unwrap_err();
        assert!(err.contains("requires a `table` argument"), "{err}");
    }

    #[test]
    fn from_call_rejects_three_part_table() {
        let err = parse_request(
            "CALL ice.system.novarocks_imv_stateless_rebuild(table => 'ice.analytics.mv_orders')",
            "default_db",
        )
        .unwrap_err();
        assert!(
            err.contains("`table` must be `<mv>` or `<namespace>.<mv>`"),
            "{err}"
        );
    }

    #[test]
    fn typed_call_lowerer_defers_non_target_procedures() {
        let statements = parse("CALL ice.system.rewrite_manifests(table => 'analytics.mv_orders')")
            .expect("generic typed CALL should parse");
        let [Statement::Maintenance(MaintenanceStatement::Call(statement))] = statements.as_slice()
        else {
            panic!("expected typed CALL statement");
        };

        assert_eq!(
            ImvStatelessRebuildRequest::from_typed_call(statement, "default_db").unwrap(),
            None
        );
    }
}
