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

//! Test-only server half of the W0 IMV statelessness harness.
//!
//! `novarocks_imv_stateless_rebuild` is a probe that rediscovers an MV
//! package's descriptor **purely from the lake** (MV table descriptor
//! properties, never SQLite) and
//! returns a one-row report describing the fidelity level the server can
//! currently reconstruct plus the descriptor content hash.
//!
//! Because this "bypass the runtime caches and rebuild from the lake" surface
//! must never exist on a production path, the procedure is guarded behind the
//! `NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD` environment flag. It is
//! wired only through the standalone CALL dispatch and is exercised by the
//! sql-test runner's `@imv_stateless_rebuild` directive.
//!
//! W1 (MV package descriptors) already carries the definition, the visible
//! schema, and the base dependencies, all covered by the descriptor content
//! hash, so the server can reconstruct the `package` level today. W3a adds the
//! `provenance` level: when the MV table's current snapshot carries a
//! `provenance.v1` record (stamped by every MV refresh, encoded by the
//! Provider's own provenance codec), the server also
//! reports `ProvenanceHash`/`WaterlineHash` derived from it. An MV that was
//! created but never refreshed (no current snapshot, or a snapshot without
//! provenance) still reports `package` with those hashes NULL.
//!
//! W4 lights up the `full` level: instead of only *reading* the lake, the
//! procedure proves SQLite is a rebuildable cache by clearing the MV's SQLite
//! records (`drop_by_target`) and rebuilding them purely from the lake
//! (`rebuild_one_lake_package_if_missing`), confirming the definition
//! reappears. It then reports `AvailableLevel = full`, `RebuildSource = lake`,
//! with the descriptor/provenance/waterline hashes derived from the rebuilt
//! state. Because the clear is destructive, `full` is reached only when the
//! request asks for it, still under the test-only env guard.

use std::sync::{Arc, atomic::AtomicBool};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::mv::domain::persistence::semantic::MvRefreshDesiredConfiguration;
use crate::mv::domain::repository::MvRepository;
use crate::mv::domain::storage_observation::{
    MvLakePackageObservation, MvLakePublication, MvLakePublishedProjection,
};
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::runtime::statement_result::StatementResult;
use novarocks_parser::ast::{CallStatement, LiteralKind, MaintenanceValue};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
};
use novarocks_types::naming::normalize_identifier;

pub const PROCEDURE_NAME: &str = "novarocks_imv_stateless_rebuild";
const TEST_ENABLE_ENV: &str = "NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD";

/// Fidelity level a stateless rebuild is expected to reconstruct. Mirrors the
/// sql-test runner's `ImvStatelessLevel`, but is a separate type because the
/// runner lives in a different crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StatelessLevel {
    Baseline,
    Package,
    Provenance,
    Full,
}

impl StatelessLevel {
    fn from_sql(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "baseline" => Ok(Self::Baseline),
            "package" => Ok(Self::Package),
            "provenance" => Ok(Self::Provenance),
            "full" => Ok(Self::Full),
            other => Err(format!(
                "unknown stateless rebuild level `{other}`; expected one of baseline, package, provenance, full"
            )),
        }
    }

    fn as_sql(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Package => "package",
            Self::Provenance => "provenance",
            Self::Full => "full",
        }
    }
}

/// Pure, race-free guard so tests never touch process env or construct state.
fn ensure_stateless_rebuild_enabled(flag: Option<&str>) -> Result<(), String> {
    if flag == Some("1") {
        Ok(())
    } else {
        Err(format!(
            "{PROCEDURE_NAME} is test-only; set {TEST_ENABLE_ENV}=1 to enable"
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvStatelessRebuildRequest {
    pub catalog: String,
    pub namespace: String,
    pub mv: String,
    pub required_level: StatelessLevel,
}

/// Test-only semantic projection compared across an accelerator wipe. It
/// deliberately omits StateStore identity, runtime attempt fields, record
/// versions and next-run bookkeeping.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MvRebuildEquivalenceSnapshot {
    query_definition: crate::common::persisted_query_definition::PersistedQueryDefinition,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    schema_contract: crate::mv::domain::persistence::schema::MvSchemaContract,
    partition_spec: Option<crate::mv::domain::persistence::schema::MvPartitionContract>,
    refresh: MvRefreshDesiredConfiguration,
    created_at_ms: i64,
    publication: MvLakePublishedProjection,
}

fn equivalence_snapshot(
    definition: &crate::mv::domain::persistence::definition::StoredMvDefinition,
    package: &MvLakePackageObservation,
) -> Result<MvRebuildEquivalenceSnapshot, String> {
    let schema_contract = definition.schema_contract.clone().ok_or_else(|| {
        "stateless rebuild equivalence requires a rebuilt MV schema contract".to_string()
    })?;
    Ok(MvRebuildEquivalenceSnapshot {
        query_definition: definition.query_definition.clone(),
        base_table_refs: definition.base_table_refs.clone(),
        primary_key_columns: definition.primary_key_columns.clone(),
        partition_spec: definition.partition_spec.clone(),
        schema_contract,
        refresh: MvRefreshDesiredConfiguration::new(
            definition.refresh_policy.clone(),
            definition.refresh_paused,
            definition.refresh_interval_ms,
            definition.max_staleness_ms,
        )?,
        created_at_ms: definition.created_at_ms,
        publication: package
            .published_projection()
            .map_err(|error| format!("project lake publication for equivalence: {error}"))?,
    })
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
    mv_storage_observation: &dyn MvStorageObservationPort,
    mv_repository: &dyn MvRepository,
    statement: &CallStatement,
    current_database: &str,
    connector_context: ConnectorRequestContext,
) -> Result<Option<StatementResult>, String> {
    let Some(req) = ImvStatelessRebuildRequest::from_typed_call(statement, current_database)?
    else {
        return Ok(None);
    };
    ensure_stateless_rebuild_enabled(std::env::var(TEST_ENABLE_ENV).ok().as_deref())?;
    execute_request_with_context(
        connector_control,
        mv_storage_observation,
        mv_repository,
        &req,
        connector_context,
    )
    .map(Some)
}

/// Guard-free core of the procedure. `execute_typed_novarocks_imv_stateless_rebuild`
/// checks the test-only env flag before calling this; the lib-harness tests
/// call it directly so they can exercise the `full` round-trip without racing
/// on process env.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn execute_request(
    connector_control: &dyn ConnectorControlResolver,
    mv_storage_observation: &dyn MvStorageObservationPort,
    mv_repository: &dyn MvRepository,
    req: &ImvStatelessRebuildRequest,
) -> Result<StatementResult, String> {
    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(false)))?;
    execute_request_with_context(
        connector_control,
        mv_storage_observation,
        mv_repository,
        req,
        context,
    )
}

fn execute_request_with_context(
    connector_control: &dyn ConnectorControlResolver,
    mv_storage_observation: &dyn MvStorageObservationPort,
    mv_repository: &dyn MvRepository,
    req: &ImvStatelessRebuildRequest,
    connector_context: ConnectorRequestContext,
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
    let loaded_table = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )
    .map_err(|error| format!("load stateless rebuild table metadata: {error}"))?;
    let package = crate::mv::domain::storage_observation::observe_lake_package(
        mv_storage_observation,
        &exact_lease,
        &loaded_table,
        connector_context.clone(),
    )
    .map_err(|error| format!("observe stateless rebuild lake package: {error}"))?
    .ok_or_else(|| {
        format!(
            "MV '{}.{}' not found among lake-native Iceberg MV packages in catalog '{}'",
            req.namespace, req.mv, req.catalog
        )
    })?;

    let descriptor_hash = package.descriptor.content_hash()?;
    let (provenance_hash, waterline_hash, available) = publication_level(&package.publication);

    // W4 `full`: the levels above only *read* the lake to prove the descriptor
    // (and, for `provenance`, the current-snapshot provenance) can be
    // reconstructed. `full` additionally proves SQLite is a rebuildable cache
    // by clearing the MV's SQLite records and rebuilding them purely from the
    // lake, then reporting the descriptor/provenance/waterline hashes derived
    // from that rebuilt state. Because this is destructive it stays gated
    // behind the test-only env flag (checked by the caller) and runs only when
    // the requested level is `full`.
    if req.required_level == StatelessLevel::Full {
        let source_revision = package
            .source_revision()
            .map_err(|error| format!("derive stateless rebuild source revision: {error}"))?;
        clear_sqlite_and_rebuild_from_lake(mv_repository, &package)?;
        let reloaded_table = crate::connector::metadata_load_connector_table_with_planning_lease(
            &exact_lease,
            connector_context.clone(),
            &table.namespace,
            &table.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(|error| format!("reload stateless rebuild table metadata: {error}"))?;
        let reobserved = crate::mv::domain::storage_observation::observe_lake_package(
            mv_storage_observation,
            &exact_lease,
            &reloaded_table,
            connector_context,
        )
        .map_err(|error| format!("reobserve stateless rebuild lake package: {error}"))?
        .ok_or_else(|| "stateless rebuild source changed: lake package disappeared".to_string())?;
        if reobserved
            .source_revision()
            .map_err(|error| format!("derive reobserved source revision: {error}"))?
            != source_revision
        {
            return Err("stateless rebuild source changed during accelerator wipe".to_string());
        }
        // The descriptor/provenance hashes are functions of the lake package,
        // which the round-trip left untouched, so they are identical to the
        // pre-rebuild values computed above. Reporting them from here documents
        // that the `full` result is derived from the rebuilt state.
        return Ok(StatementResult::Query(build_rebuild_result(
            StatelessLevel::Full,
            &descriptor_hash,
            provenance_hash.as_deref(),
            waterline_hash.as_deref(),
            "lake",
        )?));
    }

    let rebuild_source = "lake-mv-table";
    // For the non-destructive levels the procedure reports the level it CAN
    // reconstruct; the sql-test runner asserts `available >= required`, so
    // `required_level` is not gated here.

    Ok(StatementResult::Query(build_rebuild_result(
        available,
        &descriptor_hash,
        provenance_hash.as_deref(),
        waterline_hash.as_deref(),
        rebuild_source,
    )?))
}

/// Destructive `full`-level round-trip proving SQLite is a rebuildable cache:
/// drop the MV's SQLite records (definition + target lookup + dependencies +
/// partition states) WITHOUT touching the lake MV table, then rebuild them
/// purely from the lake and confirm the definition reappeared. If SQLite had no
/// record to begin with, or the rebuild failed to restore it, statelessness is
/// unproven and we fail loud.
///
/// The rebuild is *targeted* at the single observed lake package
/// (`rebuild_one_lake_package_if_missing`) rather than sweeping every
/// registered catalog via `rebuild_imv_cache_from_lake`, so the probe touches
/// only its own target.
fn clear_sqlite_and_rebuild_from_lake(
    mv_repository: &dyn MvRepository,
    package: &MvLakePackageObservation,
) -> Result<(), String> {
    // 1. Confirm the SQLite definition currently exists; the round-trip is only
    //    meaningful if there is a cached record to clear.
    let target = crate::mv::domain::model::MvTarget {
        catalog: Some(package.table.instance_id.as_str().to_string()),
        database: package.table.namespace.to_string(),
        name: package.table.table.to_string(),
    };
    let existing = mv_repository
        .find_by_target(&target)
        .map_err(|e| format!("look up MV definition before full rebuild failed: {e}"))?;
    let Some(existing) = existing else {
        return Err(format!(
            "{PROCEDURE_NAME} full level: MV '{}.{}' has no repository definition to clear (target {}.{}.{}); cannot prove a clear+rebuild round-trip",
            package.table.namespace,
            package.table.table,
            package.table.instance_id.as_str(),
            package.table.namespace,
            package.table.table
        ));
    };
    let before = equivalence_snapshot(&existing, package)?;

    // 2. Clear only rebuildable accelerator records. Historical refresh
    // records remain intact, and the repository rejects an active refresh.
    // The lake MV table is untouched — exactly the "SQLite forgot, lake
    // remembers" state.
    let dropped = mv_repository
        .wipe_rebuildable_projection_by_target(&target)
        .map_err(|e| format!("clear MV repository definition for full rebuild failed: {e}"))?;
    if !dropped {
        return Err(format!(
            "{PROCEDURE_NAME} full level: expected to clear MV repository definition for target {}.{}.{}",
            package.table.instance_id.as_str(),
            package.table.namespace,
            package.table.table
        ));
    }

    // 3. Rebuild the single target MV purely from the lake package.
    crate::mv::domain::lake_rebuild::rebuild_one_lake_package_if_missing_with_repository(
        mv_repository,
        package,
    )?;

    // 4. Confirm the definition reappeared. If it did not, statelessness failed:
    //    the lake package did not carry enough to reconstruct the SQLite record.
    let rebuilt = mv_repository
        .find_by_target(&target)
        .map_err(|e| format!("verify MV definition after full rebuild failed: {e}"))?;
    let Some(rebuilt) = rebuilt else {
        return Err(format!(
            "{PROCEDURE_NAME} full level: MV repository definition for target {}.{}.{} did not reappear after lake rebuild; statelessness not proven",
            package.table.instance_id.as_str(),
            package.table.namespace,
            package.table.table
        ));
    };
    let after = equivalence_snapshot(&rebuilt, package)?;
    if before != after {
        return Err(format!(
            "{PROCEDURE_NAME} full level: rebuilt accelerator semantics differ from the pre-wipe projection"
        ));
    }

    Ok(())
}

/// Pure level-selection: given the observed package publication state, decide the
/// `(ProvenanceHash, WaterlineHash, AvailableLevel)` triple.
fn publication_level(
    publication: &MvLakePublication,
) -> (Option<String>, Option<String>, StatelessLevel) {
    match publication {
        MvLakePublication::Published(facts) => (
            Some(facts.provenance_hash.clone()),
            Some(facts.waterline_hash.clone()),
            StatelessLevel::Provenance,
        ),
        MvLakePublication::NeverPublished => (None, None, StatelessLevel::Package),
    }
}

/// Build the fixed one-row rebuild report. Columns are all `Utf8`; the three
/// hash columns are nullable because `ProvenanceHash`/`WaterlineHash` are only
/// populated once the MV table's current snapshot carries a
/// `provenance.v1` record (see `execute_request`).
fn build_rebuild_result(
    available: StatelessLevel,
    descriptor_hash: &str,
    provenance_hash: Option<&str>,
    waterline_hash: Option<&str>,
    rebuild_source: &str,
) -> Result<QueryResult, String> {
    let columns = vec![
        column("AvailableLevel", false),
        column("DescriptorHash", true),
        column("ProvenanceHash", true),
        column("WaterlineHash", true),
        column("RebuildSource", false),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![available.as_sql().to_string()])),
        Arc::new(StringArray::from(vec![Some(descriptor_hash.to_string())])),
        Arc::new(StringArray::from(vec![provenance_hash.map(str::to_string)])),
        Arc::new(StringArray::from(vec![waterline_hash.map(str::to_string)])),
        Arc::new(StringArray::from(vec![rebuild_source.to_string()])),
    ];
    build_query_result(columns, arrays)
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build stateless rebuild result failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn column(name: &str, nullable: bool) -> QueryResultColumn {
    QueryResultColumn {
        name: name.to_string(),
        data_type: DataType::Utf8,
        nullable,
        logical_type: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::storage_observation::{
        MvLakePublication, MvPublishedBaseFact, MvPublishedLakeFacts, MvPublishedRefreshTechnique,
    };
    use bytes::Bytes;
    use novarocks_parser::{
        ast::{MaintenanceStatement, Statement},
        parse,
    };

    fn object_id(bytes: &[u8]) -> novarocks_spi::connector::ConnectorTableObjectId {
        novarocks_spi::connector::ConnectorTableObjectId::try_new(Bytes::copy_from_slice(bytes))
            .expect("valid opaque table object ID")
    }

    #[test]
    fn guard_rejects_when_flag_absent() {
        let err = ensure_stateless_rebuild_enabled(None).unwrap_err();
        assert!(err.contains("test-only"), "unexpected error: {err}");
        let err = ensure_stateless_rebuild_enabled(Some("0")).unwrap_err();
        assert!(err.contains("test-only"), "unexpected error: {err}");
    }

    #[test]
    fn guard_accepts_when_flag_enabled() {
        assert!(ensure_stateless_rebuild_enabled(Some("1")).is_ok());
    }

    fn sample_publication() -> MvLakePublication {
        MvLakePublication::Published(
            MvPublishedLakeFacts::try_new(
                201,
                1,
                1,
                "token-1".to_string(),
                MvPublishedRefreshTechnique::Full,
                vec![MvPublishedBaseFact {
                    table_fqn: "ice.sales.orders".to_string(),
                    object_id: object_id(&[0, 0xff, b'o', b'r', b'd', b'e', b'r', b's']),
                    from_snapshot: None,
                    to_snapshot: 200,
                }],
                "fp-abc".to_string(),
                3,
                "provenance-hash".to_string(),
                "waterline-hash".to_string(),
            )
            .expect("valid publication"),
        )
    }

    #[test]
    fn publication_level_reports_provenance_with_observed_hashes() {
        let publication = sample_publication();
        let (provenance_hash, waterline_hash, available) = publication_level(&publication);

        assert_eq!(available, StatelessLevel::Provenance);
        assert_eq!(provenance_hash.as_deref(), Some("provenance-hash"));
        assert_eq!(waterline_hash.as_deref(), Some("waterline-hash"));
    }

    #[test]
    fn publication_level_falls_back_to_package_when_never_published() {
        let (provenance_hash, waterline_hash, available) =
            publication_level(&MvLakePublication::NeverPublished);

        assert_eq!(available, StatelessLevel::Package);
        assert_eq!(provenance_hash, None);
        assert_eq!(waterline_hash, None);
    }

    #[test]
    fn level_round_trips_case_insensitive() {
        for (input, expected) in [
            ("baseline", StatelessLevel::Baseline),
            ("Package", StatelessLevel::Package),
            ("PROVENANCE", StatelessLevel::Provenance),
            ("Full", StatelessLevel::Full),
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
                `TABLE` => 'analytics.mv_orders', `LEVEL` => 'FULL')",
            "default_db",
        )
        .unwrap();

        assert_eq!(req.catalog, "ice");
        assert_eq!(req.required_level, StatelessLevel::Full);
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
