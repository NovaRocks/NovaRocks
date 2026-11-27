#[path = "mv_control_plan.rs"]
pub(super) mod control_plan;
mod unknown_registration;

use novarocks_query_application::api::ExecutionFailurePresentation;

use super::mv_uea7::{ManagedMvRestFixture, property, status, wait_for_status_phase};
use super::mv_uea7::{
    mv_public_error, mv_response_needs_cancel, mv_response_with_cleanup, mv_rows, mv_text_lines,
};
use super::mv_uea7_documents::load_metadata;
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{
    Scenario, ScenarioBinary, ScenarioBinaryLayout, ScenarioContext, ScenarioLaunchConfig,
};
use anyhow::{Context, Result, bail, ensure};
use control_plan::FeLaunchIntent;
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_cluster_harness::publication_catalog::{
    FixtureControl, MutationCounts, PublicationAction, PublicationCatalogFixture, PublicationFault,
    RetainedMvCommit,
};
use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};
use novarocks_failpoint::observation_marker::{
    MV_SCAN_PRUNING_COVERAGE, RF_SPLIT_INITIAL_WAIT_RESOLVED,
};
use reqwest::blocking::Client;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const RESTART_MAINTENANCE_CADENCE: &str = "iceberg_maintenance_tick_interval_ms = 1000\n";

fn configure_restart_maintenance_cadence(launch: &mut ScenarioLaunchConfig) {
    // A restart can need one pass for catalog rediscovery and another after
    // the fixture has proved settlement and supplied the declaration.
    let overlay = launch.config_overlay.fe.get_or_insert_with(String::new);
    overlay.push_str("\n[standalone_server]\n");
    overlay.push_str(RESTART_MAINTENANCE_CADENCE);
}

/// The task protocol's only fault that fails a participant which was admitted,
/// published RUNNING, and then failed on its own. It replaces the retired
/// protocol's `start-ack-suppress` here because a suppressed start acknowledged
/// nothing, while a staged MV snapshot only exists once a task really ran.
const TASK_EXECUTION_FAILURE: &str = "task-execution-failure";

/// Stable evidence that the injection above actually fired.
const TASK_EXECUTION_FAILURE_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_EXECUTION_FAILURE_INJECTED;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(MvStateStoreRestart::default()),
        Box::new(MvSchedulerRecovery::default()),
        Box::new(MvRewriteBindingBarrier::default()),
        Box::new(MvCurrentDependencyRecheck::default()),
        Box::new(MvLegacyInterpretationRebuild::default()),
        Box::new(MvRefreshConfigurationInterleaving::default()),
        Box::new(MvStagedPublishedRecovery::default()),
        Box::new(MvContentPendingRecovery::default()),
        Box::new(MvContentPendingRecovery {
            mode: ContentRecoveryMode::SlotChange,
            ..Default::default()
        }),
        Box::new(MvContentPendingRecovery {
            mode: ContentRecoveryMode::Budget,
            ..Default::default()
        }),
        Box::new(MvNameGraceRestart::default()),
        Box::new(MvContentEffect::new(RetainedMvCommit::Publication)),
        Box::new(MvContentEffect::new(RetainedMvCommit::Retirement)),
        Box::new(MvContentEffect::new(RetainedMvCommit::PendingMarker)),
        Box::new(MvContentEffect::unknown(RetainedMvCommit::Publication)),
        Box::new(MvContentEffect::unknown(RetainedMvCommit::Retirement)),
        Box::new(MvContentEffect::unknown(RetainedMvCommit::PendingMarker)),
        Box::new(unknown_registration::MvUnknownRegistration::new(
            unknown_registration::RegistrationEffect::PendingReleased,
        )),
        Box::new(unknown_registration::MvUnknownRegistration::new(
            unknown_registration::RegistrationEffect::ManagementSlot,
        )),
        Box::new(MvFirstRefreshStaging::default()),
        Box::new(MvBaseIdentityReplacement::default()),
        Box::new(MvLakePublicationRestartRebuild::default()),
    ]
}

#[derive(Default)]
struct MvNameGraceRestart {
    fixture: Mutex<Option<(ManagedMvRestFixture, PublicationCatalogFixture)>>,
}

impl Scenario for MvNameGraceRestart {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/name-grace-restart"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (rest, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_name_grace")?;
        configure_restart_maintenance_cadence(&mut launch);
        let proxy = PublicationCatalogFixture::start(rest.rest_uri().to_owned())?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
        ensure!(slot.is_none(), "managed MV fixture was initialized twice");
        *slot = Some((rest, proxy));
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        use super::mv_uea7_documents::load_metadata;

        const CATALOG: &str = "system_mv_name_grace";
        const TARGET: &str = "orders_mv";
        const CREATE: &str = "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders";
        const READ: &str = "SELECT k1, v2 FROM orders_mv ORDER BY k1";
        require_three_backends(context)?;
        let (create_catalog, rest_uri, proxy) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
            let (rest, proxy) = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                rest.create_catalog_sql()
                    .replace(rest.rest_uri(), proxy.uri()),
                rest.rest_uri().to_owned(),
                proxy.control()?,
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, CATALOG, &create_catalog, true)?;
        execute(
            context,
            &mut conn,
            "create the original MV generation",
            CREATE,
        )?;
        refresh(context, &mut conn, TARGET)?;
        let original = load_metadata(context, &rest_uri, "ns", TARGET)?;

        context.handle().defer_fe_mv_startup_isolation();
        let fault = proxy.arm_next_for_table(
            PublicationAction::CatalogMutation.as_str(),
            PublicationFault::BeforeDispatchHoldAcrossFrontendExit.as_str(),
            "ns",
            TARGET,
        )?;
        let user = context.sql_user().to_owned();
        let port = context.trino_port();
        let deadline = context.deadline();
        let original_drop = spawn_cancellable_sql_actor(move |cancellation| {
            let mut session = SqlSession::new(&user, port, deadline)?;
            session.execute(format!("USE {CATALOG}.ns"))?;
            run_original_mv_statement(
                &mut session,
                "DROP MATERIALIZED VIEW orders_mv",
                deadline,
                &cancellation,
            )
        });
        fault.wait_until_retained(deadline)?;
        context
            .handle()
            .kill_fe()
            .context("kill the FE while the proxy owns its original DROP")?;
        expect_refresh_failure_after_fe_exit(
            context,
            original_drop,
            "join the original DROP client after FE exit",
        )?;
        drop(conn);
        restart_frontend(
            context,
            "restart FE while the original DROP remains retained",
        )?;

        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, CATALOG)?;
        let blocked = conn
            .execute(CREATE)
            .expect_err("startup grace must fence a same-name CREATE");
        let refusal = mv_public_error(&blocked)?;
        ensure!(
            refusal.message.contains("MV name operations")
                && refusal
                    .message
                    .contains("deployment settlement declaration"),
            "same-name CREATE did not receive the startup grace refusal: {refusal:?}"
        );
        assert_rows(
            context,
            &mut conn,
            READ,
            &[(1, 10), (2, 20)],
            "read the old generation during name grace",
        )?;
        refresh_nonaggregate_after_restart(context, &mut conn, CATALOG, TARGET)?;

        let evidence = fault.finish()?;
        ensure!(
            evidence.summary().contains("downstream-success"),
            "retained old DROP did not succeed at the catalog: {}",
            evidence.summary()
        );
        let loaded = Client::builder()
            .no_proxy()
            .timeout(context.remaining("confirm the old DROP landed at REST")?)
            .build()?
            .get(format!(
                "{}/v1/namespaces/ns/tables/{TARGET}",
                rest_uri.trim_end_matches('/')
            ))
            .send()?;
        ensure!(
            loaded.status() == reqwest::StatusCode::NOT_FOUND,
            "old generation was still visible at REST after the original DROP"
        );

        context
            .handle()
            .declare_fe_mv_startup_effects_settled(deadline)?;
        execute(
            context,
            &mut conn,
            "create the successor generation after ending startup grace",
            CREATE,
        )?;
        refresh(context, &mut conn, TARGET)?;
        let successor = load_metadata(context, &rest_uri, "ns", TARGET)?;
        ensure!(
            original["table-uuid"].as_str().is_some()
                && successor["table-uuid"].as_str().is_some()
                && original["table-uuid"] != successor["table-uuid"],
            "same-name CREATE did not publish a new exact table identity"
        );
        assert_rows(
            context,
            &mut conn,
            READ,
            &[(1, 10), (2, 20)],
            "read the successor generation after the old DROP completed",
        )
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?
            .take();
        if let Some((mut rest, mut proxy)) = fixture {
            control_plan::with_cleanup(proxy.shutdown(), rest.shutdown())
        } else {
            Ok(())
        }
    }
}

struct MvContentEffect {
    owner: MvNameGraceRestart,
    kind: RetainedMvCommit,
    unknown: bool,
    initial_fault: super::native_sql_controls::SqlIntentOwner,
}

impl MvContentEffect {
    const REFRESH: &str = "REFRESH MATERIALIZED VIEW orders_mv";

    fn new(kind: RetainedMvCommit) -> Self {
        Self {
            owner: MvNameGraceRestart::default(),
            kind,
            unknown: false,
            initial_fault: Default::default(),
        }
    }

    fn unknown(kind: RetainedMvCommit) -> Self {
        Self {
            unknown: true,
            ..Self::new(kind)
        }
    }

    fn run_refresh_with_missing_insert(
        &self,
        context: &mut ScenarioContext,
        conn: &mut SqlSession,
    ) -> Result<()> {
        let selected = self.initial_fault.take(0)?;
        let pending = conn.submit(Self::REFRESH)?;
        let (pending, observation) = selected.arm_pending(context, pending, Self::REFRESH)?;
        let cancel = pending.cancel_handle();
        let updated = conn.run_pending(pending);
        let hits = (|| {
            let mut hits = 0;
            for &(index, kind) in &selected.faults {
                hits += usize::from(context.handle().selected_query_lifecycle_fault_hit(
                    index,
                    kind,
                    &selected.token,
                )?);
            }
            ensure!(
                hits == 1,
                "the original one-row content insertion had {hits} fault hits"
            );
            Ok(())
        })();
        let mut cleanup = if mv_response_needs_cancel(&updated) {
            cancel
                .delete(selected.deadline)
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        for &(index, kind) in &selected.faults {
            let ended =
                context
                    .handle()
                    .end_selected_query_lifecycle_fault(index, kind, &selected.token);
            cleanup = control_plan::with_cleanup(cleanup, ended);
        }
        cleanup = control_plan::with_cleanup(
            cleanup,
            context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, selected.deadline),
        );
        mv_response_with_cleanup(updated, control_plan::with_cleanup(hits, cleanup))??;
        Ok(())
    }
}

impl Scenario for MvContentEffect {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match (self.unknown, self.kind) {
            (false, RetainedMvCommit::Publication) => "mv/content-late-landing",
            (false, RetainedMvCommit::Retirement) => "mv/content-late-retirement",
            (false, RetainedMvCommit::PendingMarker) => "mv/content-late-pending-marker",
            (true, RetainedMvCommit::Publication) => "mv/content-unknown-publication",
            (true, RetainedMvCommit::Retirement) => "mv/content-unknown-retirement",
            (true, RetainedMvCommit::PendingMarker) => "mv/content-unknown-pending-marker",
            (_, RetainedMvCommit::PendingReleased | RetainedMvCommit::ManagementSlot) => {
                unreachable!("these effects have separate scenarios")
            }
        }
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        self.owner.launch_config(scenario_root)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        if self.kind != RetainedMvCommit::Retirement {
            return Ok(None);
        }
        let fault = novarocks_failpoint::QueryLifecycleFaultKind::MvContentApplyDropInsert.as_str();
        let mut plan = self.initial_fault.freeze(
            vec![super::native_sql_controls::StatementSpec {
                sql: Self::REFRESH.into(),
                faults: (0..cluster_size).map(|index| (index, fault)).collect(),
                observations: vec![],
            }],
            &[],
            cluster_size,
            binary,
            deadline,
        )?;
        if !self.unknown {
            let successor = plan.reserve_launch(RoleSlot::Frontend)?;
            plan.authorize_ordinary_launch(&successor, binary.to_path_buf())?;
        }
        plan.validate_selected_intents(cluster_size)?;
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        use super::mv_uea7_documents::load_metadata;

        const CATALOG: &str = "system_mv_name_grace";
        const TARGET: &str = "orders_mv";
        const READ: &str = "SELECT k1, v2 FROM orders_mv ORDER BY k1";
        require_three_backends(context)?;
        let (create_catalog, rest_uri, proxy) = {
            let fixture = self
                .owner
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
            let (rest, proxy) = fixture.as_ref().context("managed MV fixture is missing")?;
            (
                rest.create_catalog_sql()
                    .replace(rest.rest_uri(), proxy.uri()),
                rest.rest_uri().to_owned(),
                proxy.control()?,
            )
        };

        let mut conn = connect(context)?;
        prepare_content_effect_source(context, &mut conn, CATALOG, &create_catalog)?;

        if self.kind == RetainedMvCommit::Retirement {
            self.run_refresh_with_missing_insert(context, &mut conn)?;
            assert_rows(
                context,
                &mut conn,
                READ,
                &[(2, 20)],
                "verify the original target is missing the injected row",
            )?;
            execute(
                context,
                &mut conn,
                "retract the missing source occurrence",
                "DELETE FROM orders WHERE k1 = 3",
            )?;
            assert_content_refresh_explain(context, &mut conn, "ContentLocate")?;
        }

        let original = load_metadata(context, &rest_uri, "ns", TARGET)?;
        if self.unknown {
            return observe_unknown_content_effect(
                context, &mut conn, &rest_uri, &proxy, self.kind, &original,
            );
        }
        let original_snapshot = original["current-snapshot-id"]
            .as_i64()
            .context("the original visible MV has no Current snapshot")?;
        context.handle().defer_fe_mv_startup_isolation();
        let fault = proxy.retain_mv_commit("ns", TARGET, self.kind)?;
        let deadline = context.deadline();
        let old_refresh = spawn_refresh(
            context.sql_user().to_owned(),
            context.trino_port(),
            CATALOG,
            TARGET,
            deadline,
        );
        fault.wait_until_retained(deadline)?;
        context
            .handle()
            .kill_fe()
            .context("kill the FE while the proxy owns its original main-advancing publication")?;
        expect_refresh_failure_after_fe_exit(
            context,
            old_refresh,
            "join the old refresh after the FE exits",
        )?;
        drop(conn);
        restart_frontend(context, "replace FE while the stale request remains held")?;

        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, CATALOG)?;
        require_nonaggregate_management_after_restart(context, &mut conn, CATALOG, TARGET)?;
        let strategy = if self.kind == RetainedMvCommit::PendingMarker {
            "ContentLocate"
        } else {
            "StrictRecovery"
        };
        assert_content_refresh_explain(context, &mut conn, strategy)?;
        if self.kind == RetainedMvCommit::Retirement {
            let retirement = conn
                .execute(Self::REFRESH)
                .expect_err("the successor must report the genuine delete deficit");
            ensure!(
                mv_public_error(&retirement)?
                    .message
                    .contains("requires an explicit rebuild"),
                "the replacement FE did not retire the original insufficient target"
            );
        } else {
            refresh(context, &mut conn, TARGET)?;
            assert_rows(
                context,
                &mut conn,
                READ,
                &[(2, 20), (3, 30)],
                "read the successor publication",
            )?;
        }
        let successor = load_metadata(context, &rest_uri, "ns", TARGET)?;
        let successor_snapshot = successor["current-snapshot-id"]
            .as_i64()
            .context("the replacement FE produced no Current snapshot")?;
        ensure!(
            successor_snapshot != original_snapshot,
            "the replacement FE did not advance main before releasing the old request"
        );
        if self.kind == RetainedMvCommit::Retirement {
            let selected = successor["snapshots"]
                .as_array()
                .and_then(|snapshots| {
                    snapshots.iter().find(|snapshot| {
                        snapshot["snapshot-id"].as_i64() == Some(successor_snapshot)
                    })
                })
                .context("REST did not retain the replacement FE's Current retirement snapshot")?;
            ensure!(
                selected["summary"]
                    .get("novarocks.doc.v2.record.novarocks.mv~retirement")
                    .is_some()
                    && selected["summary"]
                        .get("novarocks.doc.v2.record.novarocks.mv~publication")
                        .is_none(),
                "the replacement FE's Current snapshot does not prove retirement"
            );
        }

        let evidence = fault.finish()?;
        let trace = evidence.summary();
        fs::write(
            context.scenario_root().join("retained-mv-request.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "original_snapshot":original_snapshot,
                "successor_snapshot":successor_snapshot,
                "effect":self.kind,
                "events":evidence.trace(),
            }))?,
        )?;
        ensure!(
            trace.contains("downstream-conflict")
                && trace.matches("request-forwarded").count() == 1,
            "old publication did not encounter one actual conditional conflict: {trace}"
        );
        let after_release = load_metadata(context, &rest_uri, "ns", TARGET)?;
        assert_rest_snapshot_state_unchanged(&successor, &after_release)?;
        if self.kind == RetainedMvCommit::PendingMarker {
            let marker_properties = |metadata: &serde_json::Value| {
                metadata["properties"]
                    .as_object()
                    .into_iter()
                    .flat_map(|props| {
                        props
                            .iter()
                            .filter(|(key, _)| {
                                key.starts_with("novarocks.doc.v2.mark.novarocks.mv~pending~")
                            })
                            .map(|(key, value)| (key.clone(), value.clone()))
                    })
                    .collect::<std::collections::BTreeMap<_, _>>()
            };
            ensure!(
                marker_properties(&successor) == marker_properties(&after_release),
                "the old Pending marker entered the successor's metadata"
            );
        }
        let status = status(context, &mut conn, CATALOG, TARGET)?;
        ensure!(
            property(&status, "UnsettledEffects")? == "0",
            "the stale effect created an MV management barrier"
        );
        context
            .handle()
            .declare_fe_mv_startup_effects_settled(deadline)?;
        if self.kind == RetainedMvCommit::Retirement {
            execute(
                context,
                &mut conn,
                "make the base bag distinct from retired storage",
                "INSERT INTO orders VALUES (4, 40, 'new')",
            )?;
            execute(
                context,
                &mut conn,
                "allow ordinary rewrite selection",
                "SET SESSION enable_materialized_view_rewrite = true",
            )?;
            assert_rows(
                context,
                &mut conn,
                "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1",
                &[(2, 20), (4, 40)],
                "prove a reader does not use the retired target",
            )
        } else {
            assert_rows(
                context,
                &mut conn,
                READ,
                &[(2, 20), (3, 30)],
                "prove the old request did not alter the reader's bag",
            )
        }
    }

    fn teardown(&self) -> Result<()> {
        self.owner.teardown()
    }
}
fn prepare_content_effect_source(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    create_catalog: &str,
) -> Result<()> {
    setup_orders_fixture_rest(context, conn, catalog, create_catalog, false)?;
    execute(
        context,
        conn,
        "keep separate cold and visible input files",
        "ALTER TABLE orders ADD COLUMN payload VARCHAR",
    )?;
    execute(
        context,
        conn,
        "write the filtered source file",
        "INSERT INTO orders SELECT -id, -id, to_hex(sha256(to_utf8(CAST(id AS VARCHAR)))) FROM TABLE(sequence(1, 4096)) AS cold(id)",
    )?;
    execute(
        context,
        conn,
        "seed visible rows",
        "INSERT INTO orders VALUES (1, 10, 'first'), (2, 20, 'retained')",
    )?;
    execute(
        context,
        conn,
        "create a visible projection MV",
        "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders WHERE v2 > 0",
    )?;
    refresh(context, conn, "orders_mv")?;
    execute(
        context,
        conn,
        "remove a source occurrence",
        "DELETE FROM orders WHERE k1 = 1",
    )?;
    execute(
        context,
        conn,
        "add the next source occurrence",
        "INSERT INTO orders VALUES (3, 30, 'next')",
    )?;
    assert_content_refresh_explain(context, conn, "ContentLocate")
}

fn require_no_unknown_content_barrier(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
) -> Result<()> {
    context.action("observe zero unsettled effects on the original target");
    let state = status(context, conn, "system_mv_name_grace", "orders_mv")?;
    ensure!(
        property(&state, "UnsettledEffects")? == "0",
        "the lost response created an MV management barrier: {state:?}"
    );
    Ok(())
}

fn observe_unknown_content_effect(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    rest_uri: &str,
    proxy: &FixtureControl,
    kind: RetainedMvCommit,
    original: &serde_json::Value,
) -> Result<()> {
    const TARGET: &str = "orders_mv";
    const READ: &str = "SELECT k1, v2 FROM orders_mv ORDER BY k1";
    const SOURCE: &str = "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1";
    const PUBLICATION: &str = "novarocks.doc.v2.record.novarocks.mv~publication";
    const RETIREMENT: &str = "novarocks.doc.v2.record.novarocks.mv~retirement";
    const PENDING: &str = "novarocks.doc.v2.mark.novarocks.mv~pending~";
    const INVALID: &str = "novarocks.doc.v2.mark.novarocks.mv~invalid~";
    let original_snapshot = original["current-snapshot-id"]
        .as_i64()
        .context("the original MV has no Current snapshot")?;
    execute(
        context,
        conn,
        "disable MV rewrite for the independent source bag",
        "SET SESSION enable_materialized_view_rewrite = false",
    )?;
    let response_fault = if kind == RetainedMvCommit::Publication {
        PublicationFault::AfterCommitBeforeResponseHideTableLoad
    } else {
        PublicationFault::AfterCommitBeforeResponse
    };
    let fault = proxy.fault_mv_commit("ns", TARGET, kind, response_fault)?;
    context.action("drop the original MV response after its exact REST effect commits");
    let response = conn.execute(MvContentEffect::REFRESH);
    let evidence = fault.finish()?;
    let committed = load_metadata(context, rest_uri, "ns", TARGET)?;
    let committed_snapshot = committed["current-snapshot-id"]
        .as_i64()
        .context("REST did not retain the current MV snapshot")?;
    fs::write(
        context.scenario_root().join("unknown-content-effect.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "effect": kind,
            "original_snapshot": original_snapshot,
            "committed_snapshot": committed_snapshot,
            "response": response.as_ref().err().map(ToString::to_string),
            "events": evidence.trace(),
        }))?,
    )?;
    let trace = evidence.summary();
    ensure!(
        trace.contains("response-dropped-after-downstream-success")
            && trace.matches("request-forwarded").count() == 1,
        "the selected effect did not lose one actual successful response: {trace}"
    );
    let failure = response.expect_err("refresh must report the original response loss");
    let failure = mv_public_error(&failure)?;
    require_no_unknown_content_barrier(context, conn)?;
    if kind == RetainedMvCommit::PendingMarker {
        // A proved committed Pending may receive the original NotStarted
        // release. The failed statement cannot publish target rows.
        assert_rest_snapshot_state_unchanged(original, &committed)?;
        let old = original["properties"]
            .as_object()
            .context("missing original MV properties")?;
        let properties = committed["properties"]
            .as_object()
            .context("missing committed MV properties")?;
        let newly_pending = properties
            .keys()
            .filter(|key| key.starts_with(PENDING) && !old.contains_key(*key))
            .collect::<Vec<_>>();
        let [key] = newly_pending.as_slice() else {
            bail!(
                "the lost response did not produce exactly one Pending marker: {newly_pending:?}"
            );
        };
        let release = key.replacen("~pending~", "~pending-released~", 1);
        let strategy = if properties.contains_key(&release) {
            "ContentLocate"
        } else {
            "StrictRecovery"
        };
        assert_content_refresh_explain(context, conn, strategy)?;
        refresh(context, conn, TARGET)?;
        let expected = [(2, 20), (3, 30)];
        assert_rows(
            context,
            conn,
            READ,
            &expected,
            "publish the same original source window",
        )?;
        require_no_unknown_content_barrier(context, conn)?;
        return assert_rows(
            context,
            conn,
            SOURCE,
            &expected,
            "compare recovery with the independent source bag",
        );
    }
    ensure!(
        committed_snapshot != original_snapshot,
        "the selected REST effect did not advance main"
    );
    let snapshot = committed["snapshots"]
        .as_array()
        .and_then(|snapshots| {
            snapshots
                .iter()
                .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(committed_snapshot))
        })
        .context("REST did not retain the committed content snapshot")?;
    if kind == RetainedMvCommit::Retirement {
        ensure!(
            snapshot["summary"].get(RETIREMENT).is_some()
                && snapshot["summary"].get(PUBLICATION).is_none()
                && committed["properties"]
                    .as_object()
                    .is_some_and(|properties| {
                        properties.keys().any(|key| key.starts_with(INVALID))
                    }),
            "the lost retirement response has no durable retirement and Invalid evidence"
        );
        let next = conn
            .execute(MvContentEffect::REFRESH)
            .expect_err("the next refresh must still respect Invalid");
        ensure!(
            mv_public_error(&next)?
                .message
                .contains("requires an explicit rebuild"),
            "the next refresh did not confirm the original retirement: {next}"
        );
        let after = load_metadata(context, rest_uri, "ns", TARGET)?;
        assert_rest_snapshot_state_unchanged(&committed, &after)?;
        return require_no_unknown_content_barrier(context, conn);
    }
    ensure!(
        kind == RetainedMvCommit::Publication
            && failure.error_name == "ICEBERG_COMMIT_ERROR"
            && failure.error_type == "EXTERNAL"
            && failure.message.contains("[CommitUnknown]")
            && snapshot["summary"].get(PUBLICATION).is_some()
            && snapshot["summary"].get(RETIREMENT).is_none(),
        "the lost data publication response did not preserve its typed outcome and durable record: {failure:?}"
    );
    assert_rows(
        context,
        conn,
        READ,
        &[(2, 20), (3, 30)],
        "read the already committed publication",
    )?;
    execute(
        context,
        conn,
        "retract a new source occurrence",
        "DELETE FROM orders WHERE k1 = 2",
    )?;
    execute(
        context,
        conn,
        "add a new source occurrence",
        "INSERT INTO orders VALUES (4, 40, 'successor')",
    )?;
    refresh(context, conn, TARGET)?;
    let expected = [(3, 30), (4, 40)];
    assert_rows(
        context,
        conn,
        READ,
        &expected,
        "refresh from the already committed watermark",
    )?;
    require_no_unknown_content_barrier(context, conn)?;
    assert_rows(
        context,
        conn,
        SOURCE,
        &expected,
        "compare the successor with the independent source bag",
    )
}

#[derive(Default)]
struct MvContentPendingRecovery {
    fixture: Mutex<Option<ContentRecoveryFixture>>,
    mode: ContentRecoveryMode,
}

struct ContentRecoveryFixture {
    rest: ManagedMvRestFixture,
    proxy: Option<PublicationCatalogFixture>,
}

struct CommittedContentWindow {
    phase: &'static str,
    token: &'static str,
    retract: &'static str,
    append: &'static str,
    expected: [(i32, i64); 2],
}

const COMMITTED_CONTENT_WINDOWS: [CommittedContentWindow; 2] = [
    CommittedContentWindow {
        phase: "write-committed",
        token: "content-write-committed",
        retract: "DELETE FROM orders WHERE k1 = 2",
        append: "INSERT INTO orders VALUES (4, 40, 'next-write')",
        expected: [(3, 30), (4, 40)],
    },
    CommittedContentWindow {
        phase: "publication-committed",
        token: "content-publication-committed",
        retract: "DELETE FROM orders WHERE k1 = 3",
        append: "INSERT INTO orders VALUES (5, 50, 'next-publication')",
        expected: [(4, 40), (5, 50)],
    },
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ContentRecoveryMode {
    #[default]
    Restart,
    SlotChange,
    Budget,
}

impl MvContentPendingRecovery {
    const CATALOG: &str = "system_mv_content_pending";
    const TARGET: &str = "orders_mv";
    const CLOSE_TOKEN: &str = "content-pending-exact-enumeration-close";
    const MV_READ: &str = "SELECT k1, v2 FROM orders_mv ORDER BY k1";
    const SOURCE_READ: &str = "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1";
}

impl Scenario for MvContentPendingRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.mode {
            ContentRecoveryMode::Restart => "mv/content-strict-recovery-restart",
            ContentRecoveryMode::SlotChange => "mv/content-pending-slot-change",
            ContentRecoveryMode::Budget => "mv/content-deployment-budget",
        }
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) = ManagedMvRestFixture::start(scenario_root, Self::CATALOG)?;
        if self.mode != ContentRecoveryMode::Budget {
            configure_restart_maintenance_cadence(&mut launch);
        }
        if self.mode == ContentRecoveryMode::Budget {
            launch
                .config_overlay
                .fe
                .get_or_insert_with(String::new)
                .push_str("\n[runtime]\noptimizer_query_mem_limit_bytes = 1\n");
        }
        let proxy = if self.mode != ContentRecoveryMode::SlotChange {
            Some(PublicationCatalogFixture::start(
                fixture.rest_uri().to_owned(),
            )?)
        } else {
            None
        };
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
        ensure!(slot.is_none(), "managed MV fixture was initialized twice");
        *slot = Some(ContentRecoveryFixture {
            rest: fixture,
            proxy,
        });
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        if self.mode == ContentRecoveryMode::Budget {
            return Ok(None);
        }
        let mut launches = vec![FeLaunchIntent::MvActions(vec![
            control_plan::close_content_target(
                Self::CATALOG,
                "ns",
                Self::TARGET,
                Self::CLOSE_TOKEN,
            ),
        ])];
        if self.mode == ContentRecoveryMode::Restart {
            for window in &COMMITTED_CONTENT_WINDOWS {
                launches.push(FeLaunchIntent::MvActions(vec![control_plan::recovery(
                    window.token,
                    Self::CATALOG,
                    "ns",
                    Self::TARGET,
                    window.phase,
                    window.token,
                    &scenario_root.join("content-committed-faults"),
                )?]));
            }
        }
        launches.push(FeLaunchIntent::Ordinary);
        control_plan::plan(cluster_size, binary, deadline, launches).map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (create_catalog_sql, rest_uri, proxy) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            let sql = fixture.rest.create_catalog_sql();
            let rest_uri = fixture.rest.rest_uri();
            let catalog_sql = match &fixture.proxy {
                Some(proxy) => sql.replace(rest_uri, proxy.uri()),
                None => sql.to_owned(),
            };
            (
                catalog_sql,
                rest_uri.to_owned(),
                fixture
                    .proxy
                    .as_ref()
                    .map(|proxy| proxy.control())
                    .transpose()?,
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(
            context,
            &mut conn,
            Self::CATALOG,
            &create_catalog_sql,
            false,
        )?;
        execute(
            context,
            &mut conn,
            "widen the original source files",
            "ALTER TABLE orders ADD COLUMN payload VARCHAR",
        )?;
        execute(
            context,
            &mut conn,
            "write a separate filtered source file",
            "INSERT INTO orders SELECT -id, -id, to_hex(sha256(to_utf8(CAST(id AS VARCHAR)))) FROM TABLE(sequence(1, 4096)) AS cold(id)",
        )?;
        execute(
            context,
            &mut conn,
            "seed the visible source rows",
            "INSERT INTO orders VALUES (1, 10, 'first'), (2, 20, 'retained')",
        )?;
        execute(
            context,
            &mut conn,
            "create a projection MV",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL', partitioning = ARRAY['k1']) AS SELECT k1, v2 FROM orders WHERE v2 > 0",
        )?;
        refresh(context, &mut conn, Self::TARGET)?;
        execute(
            context,
            &mut conn,
            "retract a visible source row",
            "DELETE FROM orders WHERE k1 = 1",
        )?;
        execute(
            context,
            &mut conn,
            "append the next visible source row",
            "INSERT INTO orders VALUES (3, 30, 'next')",
        )?;
        if self.mode == ContentRecoveryMode::Budget {
            let proxy = proxy
                .as_ref()
                .context("budget scenario has no REST recorder")?;
            let before = proxy.mutation_counts("ns", Self::TARGET);
            let status_before = status(context, &mut conn, Self::CATALOG, Self::TARGET)?;
            let plan = query(
                context,
                &mut conn,
                "EXPLAIN REFRESH MATERIALIZED VIEW orders_mv",
                "prove the original deployment budget rejects the content candidate",
            )?;
            let lines = mv_text_lines(&plan)?;
            ensure!(
                lines
                    .iter()
                    .any(|(line,)| line == "MV refresh: FullReplace")
                    && lines.iter().any(|(line,)| {
                        line.starts_with("ContentLocate unavailable: MV content location estimate")
                            && line.contains("exceeds its deployment budget 1 bytes")
                    }),
                "a low deployment budget did not select its admitted complete fallback: {lines:?}"
            );
            let after_explain = proxy.mutation_counts("ns", Self::TARGET);
            refresh(context, &mut conn, Self::TARGET)?;
            assert_content_refresh_explain(context, &mut conn, "MetadataOnly")?;
            let after_refresh = proxy.mutation_counts("ns", Self::TARGET);
            let status_after = status(context, &mut conn, Self::CATALOG, Self::TARGET)?;
            fs::write(
                context
                    .scenario_root()
                    .join("content-budget-target-commits.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "target": Self::TARGET,
                    "before": before.table_commit_forwarded,
                    "after_explain": after_explain.table_commit_forwarded,
                    "after_refresh": after_refresh.table_commit_forwarded,
                    "status_before": status_before,
                    "status_after": status_after,
                }))?,
            )?;
            require_content_commit_count(before, after_explain, 0)?;
            require_content_commit_count(before, after_refresh, 1)?;
            for name in ["Phase", "UnsettledEffects"] {
                ensure!(
                    property(&status_before, name)? == property(&status_after, name)?,
                    "budget refusal changed the target management status {name}"
                );
            }
            assert_rows(
                context,
                &mut conn,
                "SELECT k1, v2 FROM orders_mv ORDER BY k1",
                &[(2, 20), (3, 30)],
                "validate the independent source bag after the admitted FullReplace",
            )?;
            execute(
                context,
                &mut conn,
                "disable MV rewrite for the full source check",
                "SET SESSION enable_materialized_view_rewrite = false",
            )?;
            return assert_rows(
                context,
                &mut conn,
                "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1",
                &[(2, 20), (3, 30)],
                "read the complete original source after admission rejects ContentLocate",
            );
        }
        assert_content_refresh_explain(context, &mut conn, "ContentLocate")?;

        let before_enumeration = proxy
            .as_ref()
            .map(|proxy| content_catalog_observation(context, &rest_uri, proxy))
            .transpose()?;
        context.action("close the exact content target scan before its original end");
        let failed = conn
            .execute("REFRESH MATERIALIZED VIEW orders_mv")
            .expect_err("incomplete target enumeration must fail the refresh");
        let failure = mv_public_error(&failed)
            .context("incomplete target enumeration must report a public statement error")?;
        ensure!(
            failure
                .message
                .contains("complete scan enumeration and delivery"),
            "refresh did not report its missing enumeration proof: {failure:?}"
        );
        let log = context.handle().fe_server_info_log()?;
        ensure!(
            log.lines().any(|line| line.contains(
                novarocks_failpoint::observation_marker::MV_CONTENT_TARGET_ENUMERATION_CLOSED
            ) && line.contains(Self::CLOSE_TOKEN)),
            "the exact content-target control did not consume its grant"
        );
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "prove no data publication after incomplete target enumeration",
        )?;
        assert_content_refresh_explain(context, &mut conn, "StrictRecovery")?;
        let original_pending =
            if let Some((proxy, (before, counts))) = proxy.as_ref().zip(before_enumeration) {
                let (after, after_counts) = content_catalog_observation(context, &rest_uri, proxy)?;
                assert_rest_snapshot_state_unchanged(&before, &after)?;
                require_content_commit_count(counts, after_counts, 1)?;
                let properties = after["properties"]
                    .as_object()
                    .context("the Pending MV has no table properties")?;
                let mut added_pending = properties.iter().filter(|(key, _)| {
                    key.starts_with("novarocks.doc.v2.mark.novarocks.mv~pending~")
                        && before["properties"].get(key.as_str()).is_none()
                });
                let (key, value) = added_pending
                    .next()
                    .context("the original Pending is absent")?;
                ensure!(
                    added_pending.next().is_none(),
                    "the unlanded ContentLocate wrote more than one Pending"
                );
                Some((key.clone(), value.clone()))
            } else {
                None
            };
        if self.mode == ContentRecoveryMode::SlotChange {
            execute(
                context,
                &mut conn,
                "change the scheduling slot while the original content window is Pending",
                "ALTER MATERIALIZED VIEW orders_mv SET PROPERTIES refresh_paused = true",
            )?;
            assert_content_refresh_explain(context, &mut conn, "StrictRecovery")?;
        }

        drop(conn);
        restart_frontend(
            context,
            "replace the FE while the original content window is Pending",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, Self::CATALOG)?;
        require_nonaggregate_management_after_restart(
            context,
            &mut conn,
            Self::CATALOG,
            Self::TARGET,
        )?;
        assert_content_refresh_explain(context, &mut conn, "StrictRecovery")?;
        // This MANUAL view has no background refresh. The only statement in
        // this FE log interval is the successful strict replay.
        let before_replay = proxy
            .as_ref()
            .map(|proxy| content_catalog_observation(context, &rest_uri, proxy))
            .transpose()?;
        let before_pruning = context.handle().fe_server_info_log()?;
        let refreshed = refresh(context, &mut conn, Self::TARGET);
        let pruning = record_content_recovery_pruning(context, &before_pruning);
        refreshed?;
        if let Some(((proxy, (before, counts)), (key, value))) =
            proxy.as_ref().zip(before_replay).zip(original_pending)
        {
            let (after, after_counts) = content_catalog_observation(context, &rest_uri, proxy)?;
            let (snapshot, record) = current_content_publication(&after)?;
            ensure!(
                before["properties"].get(key.as_str()) == Some(&value)
                    && after["properties"].get(key.as_str()).is_none()
                    && current_content_publication(&before)?.1 != record,
                "strict replay did not atomically consume the original Pending and publish a new P"
            );
            require_content_commit_count(counts, after_counts, 1)?;
            fs::write(
                context.scenario_root().join("content-strict-replay.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "replayed_pending_key": key,
                    "baseline_snapshot_after_readmission": before["current-snapshot-id"],
                    "committed_snapshot": snapshot,
                    "publication": record.frame,
                    "forwarded_commits": after_counts.table_commit_forwarded - counts.table_commit_forwarded,
                    "successful_commits": after_counts.table_commit_succeeded - counts.table_commit_succeeded,
                }))?,
            )?;
        }
        if self.mode == ContentRecoveryMode::SlotChange {
            let slot = query(
                context,
                &mut conn,
                &format!(
                    "SELECT refresh_paused FROM system.runtime.materialized_view_states \
                     WHERE catalog_name = '{}' AND schema_name = 'ns' AND name = '{}'",
                    Self::CATALOG,
                    Self::TARGET
                ),
                "read the persisted scheduling slot after strict recovery",
            )?;
            ensure!(
                slot.rows == [vec![TrinoValue::Boolean(true)]],
                "strict recovery did not preserve the paused scheduling slot: {:?}",
                slot.rows
            );
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(2, 20), (3, 30)],
            "compare the recovered MV with the independently derived source rows",
        )?;
        execute(
            context,
            &mut conn,
            "disable MV rewrite for the full source check",
            "SET SESSION enable_materialized_view_rewrite = false",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders WHERE v2 > 0 ORDER BY k1",
            &[(2, 20), (3, 30)],
            "verify the full source result for the recovered window",
        )?;
        pruning?;
        if let Some(proxy) = proxy {
            for window in &COMMITTED_CONTENT_WINDOWS {
                conn = Self::restart_at_content_commit(context, conn, &rest_uri, &proxy, window)?;
            }
        }
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV fixture lock poisoned"))?
            .take();
        match fixture {
            Some(ContentRecoveryFixture {
                mut rest,
                proxy: Some(mut proxy),
            }) => control_plan::with_cleanup(proxy.shutdown(), rest.shutdown()),
            Some(ContentRecoveryFixture {
                mut rest,
                proxy: None,
            }) => rest.shutdown(),
            None => Ok(()),
        }
    }
}

impl MvContentPendingRecovery {
    fn restart_at_content_commit(
        context: &mut ScenarioContext,
        mut conn: SqlSession,
        rest_uri: &str,
        proxy: &FixtureControl,
        window: &CommittedContentWindow,
    ) -> Result<SqlSession> {
        execute(
            context,
            &mut conn,
            "change the content window",
            window.retract,
        )?;
        execute(context, &mut conn, "add the replacement row", window.append)?;
        assert_content_refresh_explain(context, &mut conn, "ContentLocate")?;
        let (baseline, counts_before) = content_catalog_observation(context, rest_uri, proxy)?;
        let trigger = FileTrigger::create(
            &context
                .scenario_root()
                .join("content-committed-faults")
                .join(format!("mv-refresh-at-{}.trigger", window.phase)),
            &format!("token={}\n", window.token),
        )?;
        let producer = spawn_refresh(
            context.sql_user().to_owned(),
            context.trino_port(),
            Self::CATALOG,
            Self::TARGET,
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &producer,
            &format!(
                "{} phase={} token={}",
                novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE,
                window.phase,
                window.token,
            ),
            "observe the ContentLocate Action holding its actual commit",
        )?;

        let (committed, counts_held) = content_catalog_observation(context, rest_uri, proxy)?;
        let (snapshot, record) = current_content_publication(&committed)?;
        let (_, baseline_publication) = current_content_publication(&baseline)?;
        let committed_snapshot = committed["snapshots"]
            .as_array()
            .context("the committed MV has no snapshot list")?
            .iter()
            .find(|entry| entry["snapshot-id"].as_i64() == Some(snapshot))
            .context("the committed MV lacks its Current snapshot")?;
        let summary = &committed_snapshot["summary"];
        ensure!(
            record != baseline_publication
                && committed_snapshot["parent-snapshot-id"] == baseline["current-snapshot-id"]
                && committed["refs"]["main"]["snapshot-id"].as_i64() == Some(snapshot)
                && summary["added-records"].as_str() == Some("1")
                && summary["added-position-deletes"]
                    .as_str()
                    .and_then(|count| count.parse::<u64>().ok())
                    .is_some_and(|count| count > 0),
            "{} did not publish a new ContentLocate record and its one-insert row delta under the original main",
            window.phase,
        );
        require_content_commit_count(counts_before, counts_held, 2).with_context(|| {
            format!(
                "{} must commit one Pending and one publication",
                window.phase
            )
        })?;

        context.action(format!(
            "kill FE at the original ContentLocate {}",
            window.phase
        ));
        context.handle().kill_fe()?;
        trigger.remove()?;
        expect_refresh_failure_after_fe_exit(
            context,
            producer,
            "join the original ContentLocate client after FE exit",
        )?;
        drop(conn);
        restart_frontend(context, "restart after the committed ContentLocate Action")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, Self::CATALOG)?;
        assert_rows(
            context,
            &mut conn,
            Self::MV_READ,
            &window.expected,
            "read the committed ContentLocate rows after FE replacement",
        )?;
        require_nonaggregate_management_after_restart(
            context,
            &mut conn,
            Self::CATALOG,
            Self::TARGET,
        )?;
        let admitted = load_metadata(context, rest_uri, "ns", Self::TARGET)?;
        let (_, admitted_record) = current_content_publication(&admitted)?;
        ensure!(
            admitted_record == record,
            "automatic slot readmission lost the exact committed ContentLocate record"
        );
        let readmitted_counts = proxy.mutation_counts("ns", Self::TARGET);
        refresh(context, &mut conn, Self::TARGET)?;
        let refreshed = load_metadata(context, rest_uri, "ns", Self::TARGET)?;
        assert_rest_snapshot_state_unchanged(&admitted, &refreshed)?;
        ensure!(
            proxy.mutation_counts("ns", Self::TARGET) == readmitted_counts,
            "refresh dispatched another target mutation for a ContentLocate window already committed"
        );

        execute(
            context,
            &mut conn,
            "disable MV rewrite for the full source check",
            "SET SESSION enable_materialized_view_rewrite = false",
        )?;
        assert_rows(
            context,
            &mut conn,
            Self::SOURCE_READ,
            &window.expected,
            "read the full original source at the committed window",
        )?;
        fs::write(
            context
                .scenario_root()
                .join(format!("content-{}.json", window.phase)),
            serde_json::to_vec_pretty(&serde_json::json!({
                "phase": window.phase,
                "baseline_snapshot": baseline["current-snapshot-id"],
                "committed_snapshot": snapshot,
                "readmitted_snapshot": admitted["current-snapshot-id"],
                "publication": record.frame,
                "added_records": summary["added-records"],
                "added_position_deletes": summary["added-position-deletes"],
                "forwarded_commits": counts_held.table_commit_forwarded - counts_before.table_commit_forwarded,
                "successful_commits": counts_held.table_commit_succeeded - counts_before.table_commit_succeeded,
                "pending_catalog_requests_at_hold": counts_held.table_commit_pending,
            }))?,
        )?;
        Ok(conn)
    }
}

fn current_content_publication(
    metadata: &serde_json::Value,
) -> Result<(i64, super::mv_uea7_documents::wire::Record)> {
    let graph = super::mv_uea7_documents::wire::Graph::from_metadata(metadata)?;
    let snapshot = graph.current.context("the MV has no Current snapshot")?;
    let record = graph
        .snapshots
        .into_iter()
        .find(|(id, _)| *id == snapshot)
        .and_then(|(_, record)| record)
        .context("the MV has no Current publication record")?;
    Ok((snapshot, record))
}

fn content_catalog_observation(
    context: &ScenarioContext,
    rest_uri: &str,
    proxy: &FixtureControl,
) -> Result<(serde_json::Value, MutationCounts)> {
    let target = MvContentPendingRecovery::TARGET;
    Ok((
        load_metadata(context, rest_uri, "ns", target)?,
        proxy.mutation_counts("ns", target),
    ))
}

fn require_content_commit_count(
    before: MutationCounts,
    after: MutationCounts,
    count: usize,
) -> Result<()> {
    ensure!(
        after
            .table_commit_forwarded
            .checked_sub(before.table_commit_forwarded)
            == Some(count)
            && after
                .table_commit_succeeded
                .checked_sub(before.table_commit_succeeded)
                == Some(count)
            && after.table_commit_pending == 0,
        "expected {count} successful target commits with no pending request, observed {before:?} to {after:?}"
    );
    Ok(())
}

fn record_content_recovery_pruning(context: &mut ScenarioContext, before: &str) -> Result<()> {
    let after = context.handle().fe_server_info_log()?;
    let appended = after
        .strip_prefix(before)
        .context("the recovery FE log was replaced or truncated during the refresh")?;
    let markers = appended
        .split_inclusive('\n')
        .filter(|line| {
            line.contains(MV_SCAN_PRUNING_COVERAGE) || line.contains(RF_SPLIT_INITIAL_WAIT_RESOLVED)
        })
        .collect::<Vec<_>>();
    let artifact = context
        .scenario_root()
        .join("strict-recovery-scan-pruning.log");
    fs::write(&artifact, markers.concat())
        .with_context(|| format!("write strict recovery scan evidence {}", artifact.display()))?;

    let coverage_marker = format!(r#"marker="{MV_SCAN_PRUNING_COVERAGE}""#);
    let pruned = markers.iter().any(|line| {
        line.contains(&coverage_marker)
            && line.contains(" identity_digest=[")
            && line
                .split_whitespace()
                .any(|field| field == "basis=DynamicFilter")
            && line.split_whitespace().any(|field| {
                field
                    .strip_prefix("files=")
                    .and_then(|count| count.parse::<u64>().ok())
                    .is_some_and(|count| count != 0)
            })
    });
    // Maintenance installs demand feedback only on its content target. A
    // retried refresh must use its latest target wait for the fail-open case.
    let target_wait = markers
        .iter()
        .rev()
        .find_map(|line| content_recovery_wait_outcome(line));
    ensure!(
        pruned || target_wait == Some("\"deadline_elapsed\""),
        "strict recovery needs a positive DynamicFilter ledger or its target wait deadline: {}",
        artifact.display()
    );
    Ok(())
}

fn content_recovery_wait_outcome(line: &str) -> Option<&str> {
    let (_, details) = line.trim_end().split_once(RF_SPLIT_INITIAL_WAIT_RESOLVED)?;
    let (execution, scan) = details
        .strip_prefix(" execution_id=")?
        .split_once(" scan=fragment ")?;
    let (query_high, query) = execution.split_once(':')?;
    let (query_low, attempt) = query.split_once(':')?;
    let query_high = query_high.parse::<i64>().ok()?;
    let query_low = query_low.parse::<i64>().ok()?;
    if query_high == 0 && query_low == 0 {
        return None;
    }
    attempt.parse::<std::num::NonZeroU64>().ok()?;
    let (fragment, node) = scan.split_once(" node ")?;
    let (node, outcome) = node.split_once(" outcome=")?;
    fragment.parse::<i32>().ok()?;
    node.parse::<i32>().ok()?;
    Some(outcome)
}

fn assert_content_refresh_explain(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    strategy: &str,
) -> Result<()> {
    let explain = query(
        context,
        conn,
        "EXPLAIN REFRESH MATERIALIZED VIEW orders_mv",
        "inspect the chosen content refresh strategy",
    )?;
    let expected = format!("MV refresh: {strategy}");
    let lines = mv_text_lines(&explain)?;
    ensure!(
        lines.iter().any(|(line,)| line == &expected),
        "missing {expected:?} in refresh plan: {lines:?}"
    );
    Ok(())
}

#[derive(Default)]
struct MvStateStoreRestart {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStateStoreRestart {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/state-store-restart"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_restart")?;
        configure_restart_maintenance_cadence(&mut launch);
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::Ordinary, FeLaunchIntent::Ordinary],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_restart";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create StateStore-backed materialized view",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read first MV publication before FE restart",
        )?;
        drop(conn);

        restart_frontend(context, "restart FE with the persisted StateStore")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read existing MV after FE restart",
        )?;
        require_nonaggregate_management_after_restart(context, &mut conn, catalog, "orders_mv")?;
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create a second MV after StateStore recovery",
            "CREATE MATERIALIZED VIEW orders_mv_2 WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        let views = query(
            context,
            &mut conn,
            &format!(
                "SELECT name FROM system.runtime.materialized_view_states WHERE catalog_name = '{}' AND schema_name = 'ns' ORDER BY name",
                catalog.replace('\'', "''")
            ),
            "list MV definitions after FE restart",
        )?;
        let names = mv_state_names(&views)?;
        if names != ["orders_mv", "orders_mv_2"] {
            bail!(
                "MV definitions did not survive StateStore restart: names={names:?}; {}",
                context.diagnostics()
            );
        }
        context
            .action("StateStore-backed MV definitions and visible publication survived FE restart");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvSchedulerRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvSchedulerRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/scheduler-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-scheduler-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create scheduler barrier directory {}",
                barrier_dir.display()
            )
        })?;
        clear_scheduler_markers(&barrier_dir)?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed"),
        )?;
        remove_if_exists(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
        )?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_scheduler")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str(
            r#"
[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
mv_refresh_scheduler_failure_backoff_ms = 100
mv_refresh_scheduler_max_failure_backoff_ms = 1000
"#,
        );
        fe_overlay.push_str(RESTART_MAINTENANCE_CADENCE);
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![
                    control_plan::scheduler_outcome(
                        "outcome-a",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_a",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_a"),
                    ),
                    control_plan::scheduler_outcome(
                        "outcome-b",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_b",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_b"),
                    ),
                    control_plan::scheduler_outcome(
                        "outcome-recovery-first",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-outcomes/first/orders_mv_recovery"),
                    ),
                    control_plan::scheduler(
                        "scheduler-a",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_a",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                    control_plan::scheduler(
                        "scheduler-b",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_b",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                    control_plan::scheduler(
                        "scheduler-recovery",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                ]),
                FeLaunchIntent::MvActions(vec![
                    control_plan::scheduler_outcome(
                        "outcome-recovery-second",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-outcomes/recovery/orders_mv_recovery"),
                    ),
                    control_plan::transient(
                        "recovery-transient",
                        "system_mv_scheduler",
                        "ns",
                        "orders_mv_recovery",
                        &scenario_root.join("mv-scheduler-barrier"),
                    ),
                ]),
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let barrier_dir = context.scenario_root().join("mv-scheduler-barrier");
        let outcome_root = context.scenario_root().join("mv-scheduler-outcomes");
        let hold_trigger = barrier_dir.join("mvx4-scheduler-hold.trigger");
        let _hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        context.action("armed scheduler admission barrier");

        let catalog = "system_mv_scheduler";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
        execute(
            context,
            &mut conn,
            "seed asynchronous MV source rows",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
        execute(
            context,
            &mut conn,
            "create first asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_a WITH (refresh_policy = 'INTERVAL', refresh_interval = '1s') AS SELECT k1, v2 FROM orders",
        )?;
        execute(
            context,
            &mut conn,
            "create second asynchronous scheduler MV",
            "CREATE MATERIALIZED VIEW orders_mv_b WITH (refresh_policy = 'INTERVAL', refresh_interval = '1s') AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_marker_count(
            context,
            &barrier_dir,
            1,
            "observe first scheduler admission",
        )?;
        thread::sleep(POLL_INTERVAL * 3);
        let admitted = marker_count(&barrier_dir)?;
        if admitted != 1 {
            bail!(
                "scheduler max_concurrent_refreshes=1 admitted {admitted} refreshes while held; {}",
                context.diagnostics()
            );
        }
        context.action("verified scheduler permit admitted exactly one native refresh");

        _hold.remove()?;
        context.action("released scheduler admission barrier");
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for first scheduler MV initial catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_a"),
                grant_id: "outcome-a",
                mv: "orders_mv_a",
            },
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20)],
            "wait for second scheduler MV initial catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_b"),
                grant_id: "outcome-b",
                mv: "orders_mv_b",
            },
        )?;
        execute(
            context,
            &mut conn,
            "mutate asynchronous MV source rows",
            "INSERT INTO orders VALUES (3, 30)",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_a ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for first scheduler MV incremental catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_a"),
                grant_id: "outcome-a",
                mv: "orders_mv_a",
            },
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_b ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "wait for second scheduler MV incremental catch-up",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("first/orders_mv_b"),
                grant_id: "outcome-b",
                mv: "orders_mv_b",
            },
        )?;
        execute(
            context,
            &mut conn,
            "pause first scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_a SET PROPERTIES refresh_paused = true",
        )?;
        execute(
            context,
            &mut conn,
            "pause second scheduler MV before recovery fault",
            "ALTER MATERIALIZED VIEW orders_mv_b SET PROPERTIES refresh_paused = true",
        )?;

        clear_scheduler_markers(&barrier_dir)?;
        let recovery_hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        execute(
            context,
            &mut conn,
            "create a scheduler MV for FE recovery",
            "CREATE MATERIALIZED VIEW orders_mv_recovery WITH (refresh_policy = 'INTERVAL', refresh_interval = '1s') AS SELECT k1, v2 FROM orders",
        )?;
        wait_for_file(
            context,
            &barrier_dir.join("mvx4-scheduler-admitted-orders_mv_recovery.marker"),
            "hold recovery MV scheduler refresh before FE replacement",
        )?;
        execute(
            context,
            &mut conn,
            "add rows while scheduler refresh is held",
            "INSERT INTO orders VALUES (4, 40)",
        )?;
        let _transient_preparation_fault = FileTrigger::create(
            &barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.trigger"),
            "inject one typed connector unavailability after FE restart\n",
        )?;
        context.action("armed one transient scheduler preparation fault for FE recovery");
        drop(conn);
        context.action("terminate held scheduler FE attempt for recovery");
        context
            .handle()
            .kill_fe()
            .context("kill FE during held scheduler refresh")?;
        recovery_hold.remove()?;
        restart_frontend(context, "restart FE after interrupted scheduler refresh")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        require_nonaggregate_management_after_restart(
            context,
            &mut conn,
            catalog,
            "orders_mv_recovery",
        )?;
        wait_for_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv_recovery ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30), (4, 40)],
            "wait for scheduler recovery to catch up durable MV",
            SchedulerOutcomeTarget {
                directory: outcome_root.join("recovery/orders_mv_recovery"),
                grant_id: "outcome-recovery-second",
                mv: "orders_mv_recovery",
            },
        )?;
        let consumed_fault =
            barrier_dir.join("mvx4-scheduler-transient-preparation-orders_mv_recovery.consumed");
        if !consumed_fault.exists() {
            bail!(
                "scheduler caught up without consuming the injected preparation fault; {}",
                context.diagnostics()
            );
        }
        context.action("verified the scheduler consumed its transient preparation fault");
        context.action("scheduler caught up automatically after FE restart and one transient preparation failure");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Proves that a distributed rewritten query consumes the M1 target snapshot
/// whose completed physical plan and read access were frozen, even if a normal
/// refresh publishes M2 before the query is dispatched to its backend tasks.
#[derive(Default)]
struct MvRewriteBindingBarrier {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRewriteBindingBarrier {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/rewrite-final-target-binding"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let barrier_dir = scenario_root.join("mv-rewrite-barrier");
        fs::create_dir_all(&barrier_dir).with_context(|| {
            format!(
                "create MV rewrite barrier directory {}",
                barrier_dir.display()
            )
        })?;
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_rewrite_binding")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::ObservedMvActions(vec![
                control_plan::rewrite(
                    "final-target-rewrite",
                    "system_mv_rewrite_binding",
                    "ns",
                    control_plan::REWRITE_BINDING_TOKEN,
                    &scenario_root.join("mv-rewrite-barrier"),
                ),
            ])],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_rewrite_binding";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let barrier_dir = context.scenario_root().join("mv-rewrite-barrier");
        let hold_trigger = barrier_dir.join("mvx4-rewrite-hold.trigger");
        let frozen_marker = barrier_dir.join("mvx4-completed-mv-target-frozen.marker");
        if frozen_marker.exists() {
            fs::remove_file(&frozen_marker).context("remove stale completed MV target marker")?;
        }
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, false)?;
        execute(
            context,
            &mut conn,
            "seed MV rewrite cost fixture",
            "INSERT INTO orders SELECT number % 3, CAST(number % 10 AS BIGINT) FROM TABLE(sequence(1, 1200)) t(number)",
        )?;
        execute(
            context,
            &mut conn,
            "create aggregate MV for strict target binding",
            "CREATE MATERIALIZED VIEW orders_agg_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;

        let rewritten: Vec<(String,)> = mv_text_lines(&query(
            context,
            &mut conn,
            "EXPLAIN SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1",
            "confirm the query selects the fresh MV target",
        )?)?;
        if !rewritten
            .iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_agg_mv"))
        {
            bail!(
                "MV rewrite was not selected before the binding barrier; {}",
                context.diagnostics()
            );
        }
        context.action("confirmed query selection uses the M1 MV publication");

        let hold = FileTrigger::create(&hold_trigger, "hold\n")?;
        let query = match control_plan::ObservedQuery::start(
            context,
            catalog,
            "ns",
            control_plan::REWRITE_BINDING_TOKEN,
        ) {
            Ok(query) => query,
            Err(error) => return control_plan::with_cleanup(Err(error), hold.remove()),
        };
        let phase = (|| -> Result<()> {
            wait_for_file_or_query(
                context,
                &frozen_marker,
                &query,
                "wait for completed M1 plan and read access to freeze",
            )?;
            context.action("observed completed query plan and access frozen on M1");
            execute(
                context,
                &mut conn,
                "advance source to S102 while query is held after M1 target freeze",
                "INSERT INTO orders VALUES (3, 99)",
            )?;
            refresh(context, &mut conn, "orders_agg_mv")?;
            context.action("published the normal M2 MV target before releasing the query");
            Ok(())
        })();
        let released = hold.remove();
        context.action("release rewritten query after M2 publication or failed barrier phase");
        let rows = query.finish_after_release(
            context,
            phase,
            released,
            "wait for rewritten query frozen against M1",
        )?;
        if rows != [(0, 1800), (1, 1800), (2, 1800)] {
            bail!(
                "rewritten query did not consume its frozen M1 target: rows={rows:?}; {}",
                context.diagnostics()
            );
        }
        context.action(
            "verified native 1FE+3BE query consumed M1 after concurrent S102/M2 publication",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvCurrentDependencyRecheck {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

#[derive(Default)]
struct MvRefreshConfigurationInterleaving {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvRefreshConfigurationInterleaving {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/refresh-configuration-interleaving"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_config_interleaving")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        let mut fe_overlay = launch.config_overlay.fe.take().unwrap_or_default();
        fe_overlay.push_str(
            r#"
[runtime]
query_blocking_worker_threads = 2

[standalone_server]
mv_refresh_scheduler_enabled = true
mv_refresh_scheduler_interval_ms = 100
mv_refresh_scheduler_max_concurrent = 1
"#,
        );
        launch.config_overlay.fe = Some(fe_overlay);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::MvActions(vec![
                control_plan::recovery(
                    "configuration-data-prepared",
                    "system_mv_config_interleaving",
                    "ns",
                    "orders_mv",
                    "data-prepared",
                    "before-configuration-write",
                    &scenario_root.join("mv-recovery-faults"),
                )?,
                control_plan::recovery(
                    "background-configuration-data-prepared",
                    "system_mv_config_interleaving",
                    "ns",
                    "background_mv",
                    "data-prepared",
                    "before-background-configuration-write",
                    &scenario_root.join("mv-recovery-faults"),
                )?,
                control_plan::scheduler_outcome(
                    "background-configuration-outcome",
                    "system_mv_config_interleaving",
                    "ns",
                    "background_mv",
                    &scenario_root.join("background-configuration-outcome"),
                ),
            ])],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_config_interleaving";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for concurrent configuration and refresh",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        let initial_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        execute(
            context,
            &mut conn,
            "create another independently managed MV",
            "CREATE MATERIALIZED VIEW parallel_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        let initial_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        execute(
            context,
            &mut conn,
            "advance source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-configuration-write\n",
        )?;
        let held_refresh = spawn_refresh(
            context.sql_user().to_string(),
            context.trino_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &held_refresh,
            &format!(
                "{} phase=data-prepared token=before-configuration-write",
                novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
            ),
            "wait for completed MV computation before configuration write",
        )?;
        execute(
            context,
            &mut conn,
            "pause another MV while orders_mv refresh remains held",
            "ALTER MATERIALIZED VIEW parallel_mv SET PROPERTIES refresh_paused = true",
        )?;
        let final_parallel_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "parallel_mv")?;
        if final_parallel_configuration == initial_parallel_configuration {
            bail!("other MV configuration did not commit while orders_mv was held");
        }

        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let user = context.sql_user().to_string();
        let port = context.trino_port();
        context.remaining("start concurrent MV configuration write")?;
        let deadline = context.deadline();
        let result_rx = spawn_cancellable_sql_actor(move |cancellation| {
            let mut writer = SqlSession::new(&user, port, deadline)?;

            writer.execute(format!("USE {catalog}.ns"))?;
            started_tx
                .send(())
                .context("signal configuration writer readiness")?;
            run_original_mv_statement(
                &mut writer,
                "ALTER MATERIALIZED VIEW orders_mv SET PROPERTIES refresh_paused = true",
                deadline,
                &cancellation,
            )?;
            Ok(())
        });
        started_rx
            .recv_timeout(context.remaining("wait for configuration writer readiness")?)
            .context("configuration writer did not reach its SQL request")?;
        context.action("configuration writer reached SQL while the refresh is held");
        prepared.remove()?;
        context.action("release the refresh and settle the waiting configuration write");
        match held_refresh.recv_timeout(context.remaining("receive held refresh")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error).context("held refresh failed"),
            Err(error) => bail!("held refresh did not finish: {error}"),
        }
        match result_rx.recv_timeout(context.remaining("receive configuration write")?) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error).context("concurrent configuration write failed"),
            Err(error) => bail!("concurrent configuration write did not finish: {error}"),
        }
        require_refresh_paused(context, &mut conn, "orders_mv", true)?;
        require_refresh_paused(context, &mut conn, "parallel_mv", true)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "orders_mv", 4)?;
        require_rest_snapshot_count(context, &rest_uri, "ns", "parallel_mv", 1)?;
        let metadata =
            super::mv_uea7_documents::load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&metadata)?;
        let last = graph
            .snapshots
            .last()
            .context("configuration slot snapshot missing")?;
        let previous = graph
            .snapshots
            .iter()
            .rev()
            .nth(1)
            .context("publication before slot change missing")?;
        ensure!(
            last.1 == previous.1,
            "configuration slot change did not carry P bytes"
        );
        let final_configuration =
            rest_configuration_revision(context, &rest_uri, "ns", "orders_mv")?;
        if final_configuration == initial_configuration {
            bail!("concurrent configuration write did not change the lake C revision");
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "read the completed refresh after the configuration write",
        )?;
        background_configuration_writer_conflict(context, &mut conn, &rest_uri)
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

fn background_configuration_writer_conflict(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    rest_uri: &str,
) -> Result<()> {
    use super::mv_uea7_documents::{
        external_configuration_slot_snapshot, load_metadata,
        wire::{CONFIG, Graph},
    };
    execute(
        context,
        conn,
        "create the first-refresh background target",
        "CREATE MATERIALIZED VIEW background_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
    )?;
    execute(
        context,
        conn,
        "create an independent paused configuration fixture",
        "CREATE MATERIALIZED VIEW paused_configuration_mv WITH (refresh_policy = 'INTERVAL', refresh_interval = '10s', refresh_paused = true) AS SELECT k1, v2 FROM orders",
    )?;
    let fault_dir = context.scenario_root().join("mv-recovery-faults");
    let prepared = FileTrigger::create(
        &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
        "token=before-background-configuration-write\n",
    )?;
    execute(
        context,
        conn,
        "enable the interval refresh with its first exact slot writer",
        "ALTER MATERIALIZED VIEW background_mv SET PROPERTIES refresh_policy = 'INTERVAL', refresh_interval = '10s'",
    )?;
    let marker = format!(
        "{} phase=data-prepared token=before-background-configuration-write",
        novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE,
    );
    loop {
        if context.handle().fe_log_contents()?.contains(&marker) {
            break;
        }
        context.remaining("wait for the original background full refresh to freeze its writer")?;
        thread::sleep(POLL_INTERVAL);
    }
    let initial = load_metadata(context, rest_uri, "ns", "background_mv")?;
    let original = Graph::from_metadata(&initial)?;
    let initial_slot = original
        .slots
        .get(CONFIG)
        .context("initial configuration slot missing")?;
    ensure!(
        initial_slot.writer == original.current,
        "original configuration writer was not the selected parent"
    );
    let paused_fixture = load_metadata(context, rest_uri, "ns", "paused_configuration_mv")?;
    let paused_config = Graph::from_metadata(&paused_fixture)?
        .slots
        .remove(CONFIG)
        .context("paused configuration fixture missing")?;
    ensure!(
        initial_slot.content != paused_config.content,
        "paused fixture used the original configuration bytes"
    );
    let paused = external_configuration_slot_snapshot(
        context,
        rest_uri,
        "background_mv",
        &initial,
        paused_config,
    )?;
    let resumed = external_configuration_slot_snapshot(
        context,
        rest_uri,
        "background_mv",
        &paused,
        initial_slot.clone(),
    )?;
    let paused = Graph::from_metadata(&paused)?;
    let resumed_graph = Graph::from_metadata(&resumed)?;
    let resumed_slot = &resumed_graph.slots[CONFIG];
    ensure!(
        resumed_slot.content == initial_slot.content
            && resumed_slot.writer == resumed_graph.current
            && resumed_slot.writer != initial_slot.writer
            && paused.slots[CONFIG].writer == paused.current
            && paused.current != resumed_graph.current,
        "external pause and same-value resume did not publish distinct configuration writer snapshots"
    );
    prepared.remove()?;
    let outcome = context
        .scenario_root()
        .join("background-configuration-outcome");
    let scope = context.handle().mv_scheduler_outcome_binding(
        "background-configuration-outcome",
        "system_mv_config_interleaving",
        "ns",
        "background_mv",
        &outcome,
    )?;
    let blocked = loop {
        if let Some(blocked) = read_scheduler_outcome(
            &outcome,
            "background-configuration-outcome",
            "system_mv_config_interleaving",
            "background_mv",
            &scope,
        )? {
            break blocked;
        }
        context.remaining("observe rejection of the originally admitted background writer")?;
        thread::sleep(POLL_INTERVAL);
    };
    use novarocks_execution_contract::failure::{FailureClass, domain};
    ensure!(
        blocked.fact.class() == FailureClass::Dependency
            && blocked.fact.reason().domain() == domain::CONNECTOR.get()
            && blocked.fact.reason().code() == 52,
        "expected the original provider validation conflict, got {blocked}"
    );
    let after = load_metadata(context, rest_uri, "ns", "background_mv")?;
    ensure!(
        after["snapshots"] == resumed["snapshots"],
        "the held background refresh published after its configuration writer changed"
    );
    Ok(())
}

fn require_refresh_paused(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    view: &str,
    expected: bool,
) -> Result<()> {
    let rows = materialized_view_states(conn)?;
    let name = mv_state_column(&rows, "name", "varchar")?;
    let paused = mv_state_column(&rows, "refresh_paused", "boolean")?;
    let targets = rows
        .rows
        .iter()
        .filter(|row| matches!(row.get(name), Some(TrinoValue::Text(name)) if name == view))
        .collect::<Vec<_>>();
    ensure!(
        targets.len() == 1,
        "MV state table omitted or duplicated {view}"
    );
    let Some(TrinoValue::Boolean(actual)) = targets[0].get(paused) else {
        bail!("MV state table omitted its typed refresh_paused");
    };
    if *actual != expected {
        bail!(
            "{view} refresh_paused is {actual}, expected {expected}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn require_rest_snapshot_count(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
    expected: usize,
) -> Result<()> {
    context.action("check the exact number of retained MV outputs through REST");
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact MV output count")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let snapshots = loaded["metadata"]["snapshots"]
        .as_array()
        .context("REST MV metadata lacks snapshots")?;
    if snapshots.len() != expected {
        bail!("MV has {} snapshots, expected {expected}", snapshots.len());
    }
    Ok(())
}

// Finite subcases belong to the existing Current recheck scenario. Its original
// orders/D-drift test below remains unchanged and uses a separate source table.
fn source_window_interleavings(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    rest: &str,
) -> Result<()> {
    for (label, mode, phase) in [
        ("first", "first", "source-pinned"),
        ("full", "full", "source-prepared"),
        ("delta", "delta", "source-prepared"),
    ] {
        let source = format!("window_{label}_source");
        let mv = format!("window_{label}_mv");
        create_window_fixture(context, conn, &source, &mv)?;
        let expected = if mode == "first" {
            vec![(1, 10), (2, 20)]
        } else {
            refresh(context, conn, &mv)?;
            execute(
                context,
                conn,
                "advance source before the held window",
                &format!("INSERT INTO {source} VALUES (3,30)"),
            )?;
            vec![(1, 10), (2, 20), (3, 30)]
        };
        let before = window_metadata(context, rest, &source, &format!("{label}-source-before"))?;
        let old_to = window_snapshot(&before)?;
        let next = if mode == "first" { 3 } else { 4 };
        let (trigger, held) = hold_source_refresh(context, catalog, &mv, phase, mode == "full")?;
        let phase_result = (|| {
            wait_for_fe_marker(
                context,
                &held,
                &format!(
                    "{} phase={phase} token=source-window-{mv}",
                    novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
                ),
                "wait for exact original source window boundary",
            )?;
            execute(
                context,
                conn,
                "append after the original source boundary",
                &format!("INSERT INTO {source} VALUES ({next},{})", next * 10),
            )?;
            Ok(())
        })();
        finish_source_hold(context, trigger, held, phase_result)??;
        let after = window_metadata(context, rest, &source, &format!("{label}-source-after"))?;
        ensure!(
            before["table-uuid"] == after["table-uuid"] && old_to != window_snapshot(&after)?,
            "append did not advance the same original source object"
        );
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &expected,
            "held refresh must read exactly its original To rows",
        )?;
        require_window_watermark(
            context,
            rest,
            &mv,
            old_to,
            &format!("{label}-published-old-to"),
        )?;
        refresh(context, conn, &mv)?;
        let mut all = expected;
        all.push((next, next as i64 * 10));
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &all,
            "only the next refresh includes the late append",
        )?;
        require_window_watermark(
            context,
            rest,
            &mv,
            window_snapshot(&after)?,
            &format!("{label}-published-next-to"),
        )?;
    }
    for label in ["uuid", "endpoint"] {
        let source = format!("window_{label}_source");
        let mv = format!("window_{label}_mv");
        create_window_fixture(context, conn, &source, &mv)?;
        refresh(context, conn, &mv)?;
        let target_before = window_metadata(context, rest, &mv, &format!("{label}-target-before"))?;
        execute(
            context,
            conn,
            "advance source before negative prepared window",
            &format!("INSERT INTO {source} VALUES (3,30)"),
        )?;
        let selected =
            window_metadata(context, rest, &source, &format!("{label}-selected-source"))?;
        let selected_to = window_snapshot(&selected)?;
        let replacement_location = if label == "uuid" {
            execute(
                context,
                conn,
                "create independently owned replacement source",
                "CREATE TABLE window_uuid_replacement (k1 INT,v2 BIGINT) WITH (format_version = 3)",
            )?;
            execute(
                context,
                conn,
                "seed replacement source",
                "INSERT INTO window_uuid_replacement VALUES (9,90)",
            )?;
            Some(window_metadata_location(
                context,
                rest,
                "window_uuid_replacement",
            )?)
        } else {
            None
        };
        let (trigger, held) = hold_source_refresh(context, catalog, &mv, "source-prepared", false)?;
        let phase_result = (|| {
            wait_for_fe_marker(
                context,
                &held,
                &format!(
                    "{} phase=source-prepared token=source-window-{mv}",
                    novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
                ),
                "wait for negative exact prepared-source boundary",
            )?;
            if let Some(location) = &replacement_location {
                externally_drop_rest_table(context, rest, "ns", &source)?;
                // The original fixture's formal REST register author avoids a
                // same-name FE CREATE/cache/protection dependency entirely.
                register_window_source(context, rest, &source, location)?;
                let replacement = window_metadata(context, rest, &source, "uuid-replaced-source")?;
                ensure!(
                    replacement["table-uuid"] != selected["table-uuid"],
                    "external replacement did not change the actual UUID"
                );
            } else {
                execute(
                    context,
                    conn,
                    "move main off the selected endpoint before expiry",
                    &format!("INSERT INTO {source} VALUES (4,40)"),
                )?;
                let head = window_metadata(context, rest, &source, "endpoint-new-head")?;
                ensure!(
                    window_snapshot(&head)? != selected_to,
                    "expiry target is still main"
                );
                let expired = super::mv_uea7_documents::commit_updates(
                    context,
                    rest,
                    "ns",
                    &source,
                    &head,
                    vec![
                        serde_json::json!({"action":"remove-snapshots","snapshot-ids":[selected_to]}),
                    ],
                    "expire-held-source-endpoint",
                )?;
                ensure!(
                    expired["table-uuid"] == selected["table-uuid"]
                        && !expired["snapshots"]
                            .as_array()
                            .context("expired source has no snapshots")?
                            .iter()
                            .any(|v| v["snapshot-id"].as_i64() == Some(selected_to)),
                    "selected To was not actually removed from the same object"
                );
            }
            Ok(())
        })();
        let outcome = finish_source_hold(context, trigger, held, phase_result)?;
        require_window_failure(outcome, if label == "uuid" { 20 } else { 2 })?;
        let target_after =
            window_metadata(context, rest, &mv, &format!("{label}-target-after-failure"))?;
        require_window_publication_unchanged(&target_before, &target_after)?;
        wait_window_target_cleanup(context, rest, &mv, &target_before, label)?;
        assert_rows(
            context,
            conn,
            &format!("SELECT k1,v2 FROM {mv} ORDER BY k1"),
            &[(1, 10), (2, 20)],
            "failed prepared Source must preserve the complete previous publication",
        )?;
    }
    Ok(())
}
fn create_window_fixture(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    source: &str,
    mv: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "create isolated source-window table",
        &format!("CREATE TABLE {source} (k1 INT,v2 BIGINT) WITH (format_version = 3)"),
    )?;
    execute(
        context,
        conn,
        "seed isolated source-window table",
        &format!("INSERT INTO {source} VALUES (1,10),(2,20)"),
    )?;
    execute(
        context,
        conn,
        "create isolated source-window MV",
        &format!(
            "CREATE MATERIALIZED VIEW {mv} WITH (refresh_policy = 'MANUAL') AS SELECT k1,v2 FROM {source}"
        ),
    )
}
fn hold_source_refresh(
    context: &ScenarioContext,
    catalog: &str,
    mv: &str,
    phase: &str,
    full: bool,
) -> Result<(FileTrigger, JoinedSqlResult)> {
    let trigger = FileTrigger::create(
        &context
            .scenario_root()
            .join("mv-recovery-faults")
            .join(format!("mv-refresh-at-{phase}.trigger")),
        &format!("token=source-window-{mv}\n"),
    )?;
    let user = context.sql_user().to_string();
    let port = context.trino_port();
    let deadline = context.deadline();
    let catalog = catalog.to_string();
    let sql = format!("REFRESH MATERIALIZED VIEW {mv}");
    let held = spawn_cancellable_sql_actor(move |cancellation| {
        let mut conn = SqlSession::new(&user, port, deadline)?;

        conn.execute(format!("USE {catalog}.ns"))?;
        if full {
            let qualified = format!(
                "\"{}\".incremental_refresh_enabled",
                catalog.replace('"', "\"\"")
            );
            conn.execute(format!("SET SESSION {qualified} = false"))?;
        }
        run_original_mv_statement(&mut conn, &sql, deadline, &cancellation)?;
        Ok(())
    });
    Ok((trigger, held))
}
fn finish_source_hold(
    context: &ScenarioContext,
    trigger: FileTrigger,
    held: JoinedSqlResult,
    phase: Result<()>,
) -> Result<Result<()>> {
    // Release before join even when marker/mutation/receive already failed.
    // The same original actor retains its work deadline; joining is not a
    // fresh producer allowance and the post-join drain never waits.
    let released = trigger.remove();
    let (mut sql, mut received) = match context.remaining("receive original held source refresh") {
        Ok(remaining) => match held.result.recv_timeout(remaining) {
            Ok(outcome) => (Some(outcome), Ok(())),
            Err(error) => (None, Err(error).context("receive original MV SQL result")),
        },
        Err(error) => (None, Err(error)),
    };
    let joined = held.join();
    if sql.is_none() {
        match held.result.try_recv() {
            Ok(outcome) => sql = Some(outcome),
            Err(error) => {
                received = Err(match received {
                    Ok(()) => {
                        anyhow::anyhow!(error).context("original MV SQL result absent after join")
                    }
                    Err(primary) => primary.context(format!("post-join SQL result: {error}")),
                });
            }
        }
    }
    source_hold_outcomes(sql, phase, received, joined, released)
}

/// A SQL refusal is an expected nested outcome only when every original
/// marker/receive/join/release obligation succeeded. Keep its typed cause as
/// primary when infrastructure also failed, without declaring an expected pass.
fn source_hold_outcomes(
    sql: Option<Result<()>>,
    phase: Result<()>,
    received: Result<()>,
    joined: Result<()>,
    released: Result<()>,
) -> Result<Result<()>> {
    let infrastructure = [
        ("source-window phase", phase),
        ("original SQL receive/deadline", received),
        ("original SQL join", joined),
        ("original source-hold release", released),
    ];
    if infrastructure.iter().all(|(_, value)| value.is_ok()) {
        return sql.context("original MV SQL result missing after join");
    }
    // Flatten before combining cleanup: Some(Err(sql)) is not an outer success.
    let mut primary = sql.and_then(Result::err);
    for (label, outcome) in infrastructure {
        if let Err(error) = outcome {
            primary = Some(match primary {
                Some(original) => original.context(format!("{label}: {error:#}")),
                None => error.context(label),
            });
        }
    }
    Err(primary.expect("one failed original infrastructure obligation"))
}

fn window_metadata(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    label: &str,
) -> Result<serde_json::Value> {
    let value = super::mv_uea7_documents::load_metadata(context, rest, "ns", table)?;
    let dir = context.scenario_root().join("source-window-evidence");
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join(format!("{label}.json")),
        serde_json::to_vec_pretty(&value)?,
    )?;
    Ok(value)
}
fn window_snapshot(metadata: &serde_json::Value) -> Result<i64> {
    metadata["current-snapshot-id"]
        .as_i64()
        .filter(|id| *id >= 0)
        .context("source window has no actual snapshot")
}
fn window_record(metadata: &serde_json::Value) -> Result<super::mv_uea7_documents::wire::Record> {
    let current = window_snapshot(metadata)?;
    let snapshot = metadata["snapshots"]
        .as_array()
        .context("window target has no snapshots")?
        .iter()
        .find(|v| v["snapshot-id"].as_i64() == Some(current))
        .context("window target main is absent")?;
    let frame = snapshot["summary"][super::mv_uea7_documents::wire::RECORD]
        .as_str()
        .context("window target main has no publication")?;
    super::mv_uea7_documents::wire::record(frame)
}
fn require_window_watermark(
    context: &ScenarioContext,
    rest: &str,
    mv: &str,
    expected: i64,
    label: &str,
) -> Result<()> {
    let metadata = window_metadata(context, rest, mv, label)?;
    let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&metadata)?;
    let record = window_record(&metadata)?;
    ensure!(
        graph.record_snapshot_watermarks(&record)? == [Some(expected)],
        "MV publication did not retain its exact original Source To watermark"
    );
    Ok(())
}
fn require_window_publication_unchanged(
    before: &serde_json::Value,
    after: &serde_json::Value,
) -> Result<()> {
    ensure!(
        before["current-snapshot-id"] == after["current-snapshot-id"]
            && window_record(before)? == window_record(after)?,
        "failed source refresh changed target main or exact P"
    );
    let before_graph = super::mv_uea7_documents::wire::Graph::from_metadata(before)?;
    let after_graph = super::mv_uea7_documents::wire::Graph::from_metadata(after)?;
    ensure!(
        before_graph.roots == after_graph.roots,
        "failed source refresh changed original target document roots"
    );
    Ok(())
}
fn wait_window_target_cleanup(
    context: &ScenarioContext,
    rest: &str,
    mv: &str,
    before: &serde_json::Value,
    label: &str,
) -> Result<()> {
    let expected = before["refs"]
        .as_object()
        .context("target has no original ref inventory")?;
    let mut observation = 0usize;
    loop {
        context.remaining("observe actual failed-write target ref cleanup")?;
        let after = window_metadata(context, rest, mv, &format!("{label}-cleanup-{observation}"))?;
        require_window_publication_unchanged(before, &after)?;
        let actual = after["refs"]
            .as_object()
            .context("target has no cleanup ref inventory")?;
        if actual.keys().all(|name| expected.contains_key(name)) {
            // Prior housekeeping refs may disappear independently. Only refs
            // introduced by this failed refresh must settle here; main/P are
            // checked separately and never inferred from this inventory.
            return Ok(());
        }
        observation = observation
            .checked_add(1)
            .context("target cleanup observation ordinal overflow")?;
        // Poll an actual catalog fact under the original deadline; this does
        // not declare Native/Source exit, which the original teardown owns.
        thread::sleep(POLL_INTERVAL);
    }
}

fn require_window_failure(outcome: Result<()>, reason: u16) -> Result<()> {
    let Err(failure) = outcome else {
        bail!("source identity/endpoint drift succeeded");
    };
    let original = failure
        .chain()
        .find_map(|value| value.downcast_ref::<StatementFailure>())
        .context("source drift did not retain an original typed SQL error")?;
    let server = mv_public_error(original)?;
    ensure!(
        server.error_code == 65542
            && server.error_name == "REMOTE_TASK_ERROR"
            && server.error_type == "INTERNAL_ERROR"
            && server.message.starts_with("[query.execution.dependency]")
            && server.message.contains(&format!("(reason 4:{reason})")),
        "wrong original public Source failure: {server:?}; {failure:#}"
    );
    Ok(())
}
fn window_metadata_location(context: &ScenarioContext, rest: &str, table: &str) -> Result<String> {
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read original replacement metadata pointer")?)
        .build()?
        .get(format!(
            "{}/v1/namespaces/ns/tables/{table}",
            rest.trim_end_matches('/')
        ))
        .send()?
        .error_for_status()?
        .json()?;
    loaded["metadata-location"]
        .as_str()
        .map(str::to_owned)
        .context("replacement source has no actual metadata-location")
}
fn register_window_source(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    metadata_location: &str,
) -> Result<()> {
    // Same formal register request used by catalog_publication::register_source.
    let response = Client::builder()
        .no_proxy()
        .timeout(context.remaining("register exact replacement source")?)
        .build()?
        .post(format!(
            "{}/v1/namespaces/ns/register",
            rest.trim_end_matches('/')
        ))
        .json(&serde_json::json!({"name":table,"metadata-location":metadata_location}))
        .send()?;
    ensure!(
        response.status().is_success(),
        "external source registration failed: {}",
        response.status()
    );
    Ok(())
}

fn rest_configuration_revision(
    context: &ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    let loaded: serde_json::Value = Client::builder()
        .no_proxy()
        .timeout(context.remaining("read exact lake configuration revision")?)
        .build()?
        .get(&url)
        .send()?
        .error_for_status()?
        .json()?;
    let graph = super::mv_uea7_documents::wire::Graph::from_metadata(&loaded["metadata"])?;
    graph.slots[super::mv_uea7_documents::wire::CONFIG].content_revision()
}

impl Scenario for MvCurrentDependencyRecheck {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/current-dependency-recheck"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir)?;
        let (fixture, launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_dependency_recheck")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        let mut grants = vec![control_plan::recovery(
            "dependency-data-prepared",
            "system_mv_dependency_recheck",
            "ns",
            "orders_mv",
            "data-prepared",
            "before-current-dependency-recheck",
            &scenario_root.join("mv-recovery-faults"),
        )?];
        for (mv, phase) in [
            ("window_first_mv", "source-pinned"),
            ("window_full_mv", "source-prepared"),
            ("window_delta_mv", "source-prepared"),
            ("window_uuid_mv", "source-prepared"),
            ("window_endpoint_mv", "source-prepared"),
        ] {
            grants.push(control_plan::recovery(
                &format!("source-window-{mv}"),
                "system_mv_dependency_recheck",
                "ns",
                mv,
                phase,
                &format!("source-window-{mv}"),
                &scenario_root.join("mv-recovery-faults"),
            )?);
        }
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::MvActions(grants)],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        use super::mv_uea7_documents::{commit_updates, load_metadata, wire::Graph};

        require_three_backends(context)?;
        let catalog = "system_mv_dependency_recheck";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        source_window_interleavings(context, &mut conn, catalog, &rest_uri)?;
        execute(
            context,
            &mut conn,
            "create MV for Current dependency recheck",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        execute(
            context,
            &mut conn,
            "advance the source before the held refresh",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let prepared = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-data-prepared.trigger"),
            "token=before-current-dependency-recheck\n",
        )?;
        let held_refresh = spawn_refresh(
            context.sql_user().to_string(),
            context.trino_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &held_refresh,
            &format!(
                "{} phase=data-prepared token=before-current-dependency-recheck",
                novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
            ),
            "wait for BE computation before Current recheck",
        )?;
        let before_injection = load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        let graph = Graph::from_metadata(&before_injection)?;
        let missing_key = format!("novarocks.doc.v2.rev.{}", graph.definition);
        context.action(format!(
            "remove current definition version {missing_key} without changing main"
        ));
        // This external metadata-only write leaves snapshot OCC satisfied so
        // the held refresh must reobserve its Current documents before commit.
        let after_injection = commit_updates(
            context,
            &rest_uri,
            "ns",
            "orders_mv",
            &before_injection,
            vec![serde_json::json!({"action":"remove-properties","removals":[&missing_key]})],
            "remove-current-definition",
        )?;
        assert_rest_snapshot_state_unchanged(&before_injection, &after_injection)
            .context("external D removal changed the MV snapshot state")?;
        prepared.remove()?;
        context.action("verify an externally changed D stops the old refresh before commit");
        match held_refresh.recv_timeout(context.remaining("receive stale MV refresh")?) {
            Ok(Err(error))
                if format!("{error:#}")
                    .to_ascii_lowercase()
                    .contains("corrupt")
                    && format!("{error:#}").contains(&missing_key) => {}
            Ok(Err(error)) => {
                return Err(error).context("stale MV refresh failed for another reason");
            }
            Ok(Ok(())) => bail!("stale MV refresh published after D changed"),
            Err(error) => bail!("stale MV refresh did not finish: {error}"),
        }
        let after_rejection = load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        assert_rest_snapshot_state_unchanged(&before_injection, &after_rejection)
            .context("rejected stale refresh changed the MV snapshot state")?;
        context.action("Current D drift rejected with the MV snapshot state unchanged");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLegacyInterpretationRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
    old_launch: Mutex<Option<ScenarioLaunchConfig>>,
}

impl Scenario for MvLegacyInterpretationRebuild {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/legacy-interpretation-rebuild"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[ScenarioBinary::Primary, ScenarioBinary::Compatible]
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) = ManagedMvRestFixture::start_private_with_budget(
            scenario_root,
            "system_mv_legacy_interpretation",
            budget,
        )?;
        let deployment = format!(
            "rwm10b-legacy-{}",
            novarocks_types::BackendProcessId::new_v7()
        );
        launch.native_trust_fixture = launch
            .native_trust_fixture
            .with_deployment_id(&deployment)?;
        let mut old = launch.clone();
        old.binary_layout = ScenarioBinaryLayout {
            frontend: ScenarioBinary::Compatible,
            backends: vec![ScenarioBinary::Compatible; 3],
        };
        configure_restart_maintenance_cadence(&mut launch);
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        ensure!(
            slot.is_none(),
            "managed MV fixture was initialized more than once"
        );
        *slot = Some(fixture);
        *self
            .old_launch
            .lock()
            .map_err(|_| anyhow::anyhow!("old MV launch lock poisoned"))? = Some(old);
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        use super::mv_uea7_documents::wire::Graph;

        require_three_backends(context)?;
        let catalog = "system_mv_legacy_interpretation";
        let (create_catalog_sql, rest_uri) = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = slot
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let old_launch = self
            .old_launch
            .lock()
            .map_err(|_| anyhow::anyhow!("old MV launch lock poisoned"))?
            .take()
            .context("old MV producer launch was not prepared")?;
        let create = "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders";
        context.action("launch the exact old producer on the private REST lake");
        let mut old = context.launch_peer_cluster("legacy-producer", old_launch)?;
        old.retain_runtime_artifacts();
        let produced = (|| -> Result<_> {
            let mut conn = SqlSession::new(
                old.sql_user(),
                old.runtime().fe_trino_port,
                context.deadline(),
            )?;
            setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
            execute(
                context,
                &mut conn,
                "create the MV with the original producer",
                create,
            )?;
            refresh(context, &mut conn, "orders_mv")?;
            assert_rows(
                context,
                &mut conn,
                "SELECT k1, v2 FROM orders_mv ORDER BY k1",
                &[(1, 10), (2, 20)],
                "read the original producer's materialized rows",
            )?;
            let metadata = load_metadata(context, &rest_uri, "ns", "orders_mv")?;
            let keyed_apply_layout = old_mv_uses_keyed_apply_layout(&metadata)?;
            fs::write(
                context.scenario_root().join("old-mv-metadata.json"),
                serde_json::to_vec_pretty(&metadata)?,
            )?;
            Ok((metadata, keyed_apply_layout))
        })();
        let old_exit = old
            .shutdown()
            .context("stop the original FE and all three BEs");
        let (metadata, keyed_apply_layout) = match (produced, old_exit) {
            (Ok(produced), Ok(())) => produced,
            (Err(e), Ok(())) | (Ok(_), Err(e)) => return Err(e),
            (Err(production), Err(cleanup)) => {
                bail!("{production:#}; old producer cleanup: {cleanup:#}")
            }
        };

        let mut conn = connect(context)?;
        execute(
            context,
            &mut conn,
            "attach only the old REST catalog to the fresh candidate StateStore",
            &create_catalog_sql,
        )?;
        drop(conn);
        restart_frontend(
            context,
            "cold start the candidate with only lake discovery for the old MV",
        )?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        if keyed_apply_layout {
            assert_keyed_apply_layout_needs_rebuild(
                context, &mut conn, catalog, &rest_uri, &metadata,
            )?;
        } else {
            assert_legacy_mv_is_retired(context, &mut conn, catalog, &rest_uri, &metadata)?;
        }

        execute(
            context,
            &mut conn,
            "drop the old object through its original managed target",
            "DROP MATERIALIZED VIEW IF EXISTS orders_mv",
        )?;
        let status = Client::builder()
            .no_proxy()
            .timeout(context.remaining("confirm exact lake target removal")?)
            .build()?
            .get(format!(
                "{}/v1/namespaces/ns/tables/orders_mv",
                rest_uri.trim_end_matches('/')
            ))
            .send()?
            .status();
        ensure!(
            status == reqwest::StatusCode::NOT_FOUND,
            "REST still sees the old target: {status}"
        );
        execute(
            context,
            &mut conn,
            "repeat IF EXISTS against the absent target",
            "DROP MATERIALIZED VIEW IF EXISTS orders_mv",
        )?;
        execute(
            context,
            &mut conn,
            "recreate the MV in the current format",
            create,
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        let rebuilt = load_metadata(context, &rest_uri, "ns", "orders_mv")?;
        let graph = Graph::from_metadata(&rebuilt)?;
        assert_keyed_mv_layout(&rebuilt, &graph, false)?;
        ensure!(
            rebuilt["table-uuid"] != metadata["table-uuid"],
            "recreated MV did not publish a new table identity"
        );
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read the recreated and refreshed current-format MV",
        )
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

fn assert_legacy_mv_is_retired(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    rest_uri: &str,
    metadata: &serde_json::Value,
) -> Result<()> {
    let states = materialized_view_states(conn)?;
    ensure!(
        mv_state_text(&states, "orders_mv", "manageability")? == "RETIRED",
        "runtime MV inventory did not identify the retired target"
    );
    let inventory = query(
        context,
        conn,
        &format!(
            "SELECT name, comment, definition FROM system.metadata.materialized_views WHERE catalog_name = '{catalog}' AND schema_name = 'ns' AND name = 'orders_mv'"
        ),
        "discover the old MV without a local Accelerator",
    )?;
    ensure!(
        matches!(inventory.rows.as_slice(), [row] if matches!(row.as_slice(),
            [TrinoValue::Text(name), TrinoValue::Text(comment), TrinoValue::Null]
            if name == "orders_mv" && comment.contains("DROP only"))),
        "old MV metadata inventory omitted or misclassified the target"
    );
    for sql in [
        "SELECT * FROM orders_mv",
        "REFRESH MATERIALIZED VIEW orders_mv",
    ] {
        context.action(format!("reject retired MV command {sql}"));
        let error = conn
            .execute(sql)
            .expect_err("retired MV command must fail closed");
        let public = mv_public_error(&error)?;
        ensure!(
            public.message.contains("LegacyFormat") || public.message.contains("DROP only"),
            "retired MV command returned another error: {}",
            public.message
        );
    }
    ensure!(
        &load_metadata(context, rest_uri, "ns", "orders_mv")? == metadata,
        "retired MV reads or refresh mutated exact lake metadata"
    );
    Ok(())
}

fn assert_keyed_apply_layout_needs_rebuild(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    rest_uri: &str,
    metadata: &serde_json::Value,
) -> Result<()> {
    use super::mv_uea7_documents::wire::Graph;

    require_nonaggregate_management_after_restart(context, conn, catalog, "orders_mv")?;
    let before_refusal = load_metadata(context, rest_uri, "ns", "orders_mv")?;
    let original = Graph::from_metadata(metadata)?;
    let readmitted = Graph::from_metadata(&before_refusal)?;
    assert_keyed_mv_layout(&before_refusal, &readmitted, true)?;
    ensure!(
        metadata["table-uuid"].as_str().is_some()
            && before_refusal["table-uuid"] == metadata["table-uuid"],
        "readmission changed the original MV table UUID"
    );
    ensure!(
        before_refusal["current-schema-id"] == metadata["current-schema-id"]
            && before_refusal["schemas"] == metadata["schemas"],
        "readmission changed the original keyed MV schema"
    );
    let original_publication = original
        .snapshots
        .iter()
        .find(|(id, _)| Some(*id) == original.current)
        .and_then(|(_, record)| record.as_ref())
        .context("original keyed MV has no Current publication")?;
    ensure!(
        readmitted.snapshots.iter().any(|(id, record)| {
            Some(*id) == readmitted.current && record.as_ref() == Some(original_publication)
        }),
        "readmission did not carry the exact old keyed publication to Current"
    );

    assert_content_refresh_explain(context, conn, "NeedsRebuild")?;
    context.action("reject a refresh against the old required apply key");
    let error = conn
        .execute("REFRESH MATERIALIZED VIEW orders_mv")
        .expect_err("old apply layout must require replacement");
    let public = mv_public_error(&error)?;
    ensure!(
        public.error_name == "NOT_SUPPORTED"
            && public
                .message
                .contains("Legacy MV storage requires hidden maintenance fields"),
        "old apply layout returned {}: {}",
        public.error_name,
        public.message
    );
    let after_refusal = load_metadata(context, rest_uri, "ns", "orders_mv")?;
    assert_rest_snapshot_state_unchanged(&before_refusal, &after_refusal)?;
    execute(
        context,
        conn,
        "enable query rewriting after the rebuild refusal",
        "SET SESSION enable_materialized_view_rewrite = true",
    )?;
    let plan = mv_text_lines(&query(
        context,
        conn,
        "EXPLAIN SELECT k1, v2 FROM orders ORDER BY k1",
        "verify the old target is unavailable to rewriting",
    )?)?;
    ensure!(
        !plan
            .iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_mv")),
        "query rewrite selected the old apply layout: {plan:?}"
    );
    assert_rows(
        context,
        conn,
        "SELECT k1, v2 FROM orders ORDER BY k1",
        &[(1, 10), (2, 20)],
        "verify the original query still reads the source",
    )
}

#[derive(Default)]
struct MvStagedPublishedRecovery {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStagedPublishedRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/staged-published-recovery"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fault_dir = scenario_root.join("mv-recovery-faults");
        fs::create_dir_all(&fault_dir).with_context(|| {
            format!("create MV recovery fault directory {}", fault_dir.display())
        })?;
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_recovery")?;
        configure_restart_maintenance_cadence(&mut launch);
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![control_plan::recovery(
                    "staged-write-committed",
                    "system_mv_recovery",
                    "ns",
                    "orders_mv",
                    "write-committed",
                    "staged-before-publication",
                    &scenario_root.join("mv-recovery-faults"),
                )?]),
                FeLaunchIntent::MvActions(vec![control_plan::recovery(
                    "published-before-cleanup",
                    "system_mv_recovery",
                    "ns",
                    "orders_mv",
                    "publication-committed",
                    "published-before-cleanup",
                    &scenario_root.join("mv-recovery-faults"),
                )?]),
                FeLaunchIntent::Ordinary,
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fault_dir = context.scenario_root().join("mv-recovery-faults");
        let catalog = "system_mv_recovery";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV for staged and published recovery",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;

        // A canonical publication is one commit: the rows and the publication
        // document land together, so there is no staged-but-unpublished state
        // to crash into. What this window still proves is that a crash between
        // that commit and the frontend's own record of it leaves the committed
        // publication visible and re-refreshable.
        let staged = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-write-committed.trigger"),
            "token=staged-before-publication\n",
        )?;
        context.action("armed committed-before-record crash barrier");
        let staged_refresh = spawn_refresh(
            context.sql_user().to_string(),
            context.trino_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &staged_refresh,
            &format!(
                "{} phase=write-committed token=staged-before-publication",
                novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
            ),
            "wait for committed-before-record barrier",
        )?;
        context.action("kill FE between the publication commit and its record");
        context
            .handle()
            .kill_fe()
            .context("kill FE at staged recovery barrier")?;
        staged.remove()?;
        expect_refresh_failure_after_fe_exit(
            context,
            staged_refresh,
            "staged refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after committed-before-record crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify the committed publication survived the unrecorded crash",
        )?;
        refresh_nonaggregate_after_restart(context, &mut conn, catalog, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify recovery re-refreshes onto the same published rows",
        )?;
        execute(
            context,
            &mut conn,
            "add source row before publication-committed crash",
            "INSERT INTO orders VALUES (3, 30)",
        )?;

        let published = FileTrigger::create(
            &fault_dir.join("mv-refresh-at-publication-committed.trigger"),
            "token=published-before-cleanup\n",
        )?;
        context.action("armed publication-committed crash barrier");
        let published_refresh = spawn_refresh(
            context.sql_user().to_string(),
            context.trino_port(),
            catalog,
            "orders_mv",
            context.deadline(),
        );
        wait_for_fe_marker(
            context,
            &published_refresh,
            &format!(
                "{} phase=publication-committed token=published-before-cleanup",
                novarocks_failpoint::observation_marker::MV_RECOVERY_PHASE
            ),
            "wait for publication recovery barrier",
        )?;
        context.action("kill FE after MV main publication and before cleanup");
        context
            .handle()
            .kill_fe()
            .context("kill FE at publication recovery barrier")?;
        published.remove()?;
        expect_refresh_failure_after_fe_exit(
            context,
            published_refresh,
            "publication refresh client after FE termination",
        )?;
        restart_frontend(context, "restart FE after publication-committed crash")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20), (3, 30)],
            "verify published snapshot remains visible after recovery",
        )?;
        refresh_nonaggregate_after_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("staged and published crash windows converged through public MV behavior");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvFirstRefreshStaging {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvFirstRefreshStaging {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/first-refresh-staging"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, launch) = ManagedMvRestFixture::start(scenario_root, "system_mv_staging")?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::first_refresh_plan(cluster_size, binary, original_deadline).map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_staging";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;

        execute(
            context,
            &mut conn,
            "create first-refresh projection MV",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged projection first refresh publishes only completed main snapshot",
        )?;

        execute(
            context,
            &mut conn,
            "create first-refresh aggregate MV",
            "CREATE MATERIALIZED VIEW orders_agg_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, SUM(v2) AS total_v2 FROM orders GROUP BY k1",
        )?;
        refresh(context, &mut conn, "orders_agg_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total_v2 FROM orders_agg_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "verify staged aggregate first refresh",
        )?;

        execute(
            context,
            &mut conn,
            "create MV used to prove failed first refresh is not published",
            "CREATE MATERIALIZED VIEW orders_start_fault_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        // The refresh has to fail from inside a task that was really admitted
        // and really started, because that is what leaves a staged main
        // snapshot behind for the publication rule to discard. A refused
        // admission would prove nothing about staging. The fault fires on the
        // backend it is armed for; the MV's base table is a connector scan, so
        // its scan fragment is placed on every live backend and that backend
        // does get a task.
        let injected_baseline = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        let deadline = context.deadline();
        // The actual connection is idle after catalog/database setup. Arm the
        // exact outer REFRESH bytes before its real statement admission.
        let pending = conn.submit(control_plan::FIRST_REFRESH_SQL)?;
        let cancel = pending.cancel_handle();
        let observation = match context
            .handle()
            .arm_selected_statement_observation_for_client_query(
                control_plan::FIRST_REFRESH_TOKEN,
                &pending.query_id,
                control_plan::FIRST_REFRESH_SQL,
                deadline,
            ) {
            Ok(observation) => observation,
            Err(primary) => {
                return control_plan::with_cleanup(
                    Err(primary),
                    cancel.delete(deadline).map_err(Into::into),
                );
            }
        };
        let armed = context.handle().arm_selected_query_lifecycle_fault(
            0,
            TASK_EXECUTION_FAILURE,
            control_plan::FIRST_REFRESH_TOKEN,
        );
        if let Err(error) = armed {
            let drained = context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, deadline);
            return control_plan::with_cleanup(
                Err(error),
                control_plan::with_cleanup(drained, cancel.delete(deadline).map_err(Into::into)),
            );
        }
        context.action("armed an injected task execution failure for MV first refresh");
        let refresh_result = conn.run_pending(pending);
        let cancelled = if mv_response_needs_cancel(&refresh_result) {
            cancel.delete(deadline).map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        let cleanup_result = context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear injected task execution failure");
        // The original pending returned its actual response before observation drain.
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, deadline);
        let refresh_result = mv_response_with_cleanup(
            refresh_result,
            control_plan::with_cleanup(
                cancelled,
                control_plan::with_cleanup(cleanup_result, drained),
            ),
        )?;
        let error = refresh_result
            .expect_err("an injected task execution failure must fail the MV first refresh");
        mv_public_error(&error)?;
        if error.to_string().is_empty() {
            bail!("injected task execution failure returned an empty MV refresh error");
        }
        // Without this the remaining assertions would also hold for a refresh
        // that failed for an unrelated reason, or for one that never ran a
        // distributed task at all.
        let injected = total_be_marker_count(context, TASK_EXECUTION_FAILURE_MARKER)?;
        if injected <= injected_baseline {
            bail!(
                "MV first refresh failed without the injected task execution failure firing; \
                 expected a new {TASK_EXECUTION_FAILURE_MARKER} across the BE logs, \
                 count stayed at {injected_baseline}"
            );
        }
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_start_fault_mv ORDER BY k1",
            &[],
            "verify failed first refresh never publishes a partial main snapshot",
        )?;
        context.action("validated native first-refresh staging publishes no partial main snapshot");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvBaseIdentityReplacement {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvBaseIdentityReplacement {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/base-identity-replacement"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_base_identity")?;
        configure_restart_maintenance_cadence(&mut launch);
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![FeLaunchIntent::Ordinary, FeLaunchIntent::Ordinary],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_base_identity";
        let (create_catalog_sql, rest_uri) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with a durable base-object binding",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read publication before replacing its base table",
        )?;

        use super::mv_uea7_documents::{load_metadata, wire::Graph};
        let old_source = load_metadata(context, &rest_uri, "ns", "orders")?;
        let old_target =
            Graph::from_metadata(&load_metadata(context, &rest_uri, "ns", "orders_mv")?)?;
        drop(conn);

        let replace = |context: &mut ScenarioContext, rows: &str| -> Result<SqlSession> {
            externally_drop_rest_table(context, &rest_uri, "ns", "orders")?;
            let mut conn = connect(context)?;
            select_catalog_and_database(context, &mut conn, catalog)?;
            execute(
                context,
                &mut conn,
                "recreate base table under the same logical name",
                "CREATE TABLE orders (k1 INT, v2 BIGINT) WITH (format_version = 3)",
            )?;
            execute(context, &mut conn, "seed replacement base table", rows)?;
            Ok(conn)
        };
        let mut conn = replace(context, "INSERT INTO orders VALUES (9, 90)")?;
        execute(
            context,
            &mut conn,
            "use the unrewritten source query as the replacement oracle",
            "SET SESSION enable_materialized_view_rewrite = false",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        for sql in [
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            "SELECT k1, v2 FROM orders ORDER BY k1",
        ] {
            assert_rows(
                context,
                &mut conn,
                sql,
                &[(9, 90)],
                "validate the replacement bag",
            )?;
        }
        let source = load_metadata(context, &rest_uri, "ns", "orders")?;
        let target = Graph::from_metadata(&load_metadata(context, &rest_uri, "ns", "orders_mv")?)?;
        let record = target
            .snapshots
            .iter()
            .find_map(|(id, record)| {
                (Some(*id) == target.current)
                    .then_some(record.as_ref())
                    .flatten()
            })
            .context("replacement refresh has no Current publication")?;
        let snapshot = source["current-snapshot-id"]
            .as_i64()
            .context("replacement lacks Current")?;
        let source_uuid = source["table-uuid"]
            .as_str()
            .context("replacement source lacks its table UUID")?;
        ensure!(
            source["table-uuid"] != old_source["table-uuid"]
                && target.record_iceberg_source_uuids(record)? == [source_uuid]
                && target.definition == old_target.definition
                && target.binding != old_target.binding
                && target.record_snapshot_watermarks(record)? == [Some(snapshot)],
            "replacement P does not retain the new binding and its exact source watermark"
        );
        drop(conn);
        // Preserve the separate restart case. An owner change before a fresh
        // publication still exercises the original incarnation readmission.
        drop(replace(context, "INSERT INTO orders VALUES (10, 100)")?);

        restart_frontend(context, "restart FE after same-name base replacement")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_mv_quarantined_after_base_replacement(context, &mut conn, catalog, "orders_mv")?;
        context.action(
            "verified FE restart quarantines the MV rather than bind a same-name replacement base",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

#[derive(Default)]
struct MvLakePublicationRestartRebuild {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvLakePublicationRestartRebuild {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_recovery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/lake-publication-restart-rebuild"
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) =
            ManagedMvRestFixture::start(scenario_root, "system_mv_lake_rebuild")?;
        configure_restart_maintenance_cadence(&mut launch);
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        if slot.is_some() {
            bail!("managed MV fixture was initialized more than once");
        }
        *slot = Some(fixture);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::MvActions(vec![
                    control_plan::observation(
                        "lake-provenance",
                        "system_mv_lake_rebuild",
                        "ns",
                        "orders_mv",
                        "provenance",
                    ),
                    control_plan::wipe("accelerator-family-wipe"),
                ]),
                FeLaunchIntent::Ordinary,
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let catalog = "system_mv_lake_rebuild";
        let create_catalog_sql = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .as_ref()
            .context("managed MV fixture is missing after cluster launch")?
            .create_catalog_sql()
            .to_owned();
        let mut conn = connect(context)?;
        setup_orders_fixture_rest(context, &mut conn, catalog, &create_catalog_sql, true)?;
        execute(
            context,
            &mut conn,
            "create MV with canonical lake documents",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, v2 FROM orders",
        )?;
        refresh(context, &mut conn, "orders_mv")?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read newly published lake-native MV before FE restart",
        )?;
        let rows = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(\"table\" => 'ns.orders_mv', level => 'provenance')"
            ),
            "confirm the published MV has exact lake documents",
        )?;
        let report = rows
            .rows
            .first()
            .context("lake document observation returned no report row")?;
        let level = mv_report_text(&rows, report, 0)?;
        let source = mv_report_text(&rows, report, 4)?;
        if level != "provenance" || source != "lake-documents" {
            bail!(
                "unexpected lake document report level={level:?}, source={source:?}; {}",
                context.diagnostics()
            );
        }
        let wiped = query(
            context,
            &mut conn,
            &format!(
                "CALL {catalog}.system.novarocks_imv_stateless_rebuild(\"table\" => 'ns.orders_mv', level => 'wipe')"
            ),
            "wipe only the MV Accelerator after proving lake documents",
        )?;
        let wipe_report = wiped
            .rows
            .first()
            .context("MV Accelerator wipe returned no report row")?;
        if mv_report_text(&wiped, wipe_report, 0)? != "wipe"
            || mv_report_text(&wiped, wipe_report, 4)? != "accelerator-wiped"
        {
            bail!("unexpected MV Accelerator wipe report: {wipe_report:?}");
        }
        drop(conn);
        restart_frontend(context, "restart FE after MV Accelerator wipe")?;
        let mut conn = connect(context)?;
        select_catalog_and_database(context, &mut conn, catalog)?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, v2 FROM orders_mv ORDER BY k1",
            &[(1, 10), (2, 20)],
            "read MV rediscovered from canonical lake publication",
        )?;
        refresh_nonaggregate_after_restart(context, &mut conn, catalog, "orders_mv")?;
        context.action("verified MV wipe, restart and automatic refresh from lake documents");
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

/// Counts one marker across every backend's log.
///
/// A single injected failure happens on one backend, so the sum is the honest
/// quantity here: counting the number of distinct backends that saw it would
/// assert a fanout the injection never claims.
fn total_be_marker_count(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let be_count = context.handle().be_count();
    let mut total = 0;
    for index in 0..be_count {
        total += context
            .handle()
            .be_log_count(index, marker)
            .with_context(|| format!("count {marker} in BE[{index}] log"))?;
    }
    Ok(total)
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let be_count = context.handle().be_count();
    if be_count != 3 {
        bail!(
            "{} requires native 1FE+3BE, but runner launched {} BE(s)",
            context.name(),
            be_count
        );
    }
    context.action("confirmed native 1FE+3BE topology");
    Ok(())
}

fn connect(context: &mut ScenarioContext) -> Result<SqlSession> {
    context.remaining("connect MV SQL client")?;
    context.action("connect through public Trino protocol");
    SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())
        .map_err(Into::into)
}

fn setup_orders_fixture_rest(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    create_catalog_sql: &str,
    seed_rows: bool,
) -> Result<()> {
    execute(
        context,
        conn,
        "create private REST Iceberg catalog",
        create_catalog_sql,
    )?;
    execute(
        context,
        conn,
        "create MV fixture namespace",
        &format!("CREATE SCHEMA {catalog}.ns"),
    )?;
    select_catalog_and_database(context, conn, catalog)?;
    execute(
        context,
        conn,
        "create MV source table",
        "CREATE TABLE orders (k1 INT, v2 BIGINT) WITH (format_version = 3)",
    )?;
    if seed_rows {
        execute(
            context,
            conn,
            "seed MV source table",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
    }
    Ok(())
}

fn select_catalog_and_database(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "select MV catalog and namespace",
        &format!("USE {catalog}.ns"),
    )
}

fn externally_drop_rest_table(
    context: &mut ScenarioContext,
    rest_uri: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest_uri.trim_end_matches('/')
    );
    context.action("drop original base table through the private Iceberg REST catalog");
    Client::builder()
        .no_proxy()
        .timeout(context.remaining("drop base table through external REST catalog")?)
        .build()
        .context("build external REST catalog client")?
        .delete(&url)
        .send()
        .with_context(|| format!("delete original base table at {url}"))?
        .error_for_status()
        .with_context(|| {
            format!("REST catalog rejected deletion of original base table at {url}")
        })?;
    Ok(())
}

/// Select the independently checked migration by the producer's actual B
/// format. The keyed layout is rebuildable through DROP and CREATE, while
/// the retired layout retains its original DROP-only assertions.
fn old_mv_uses_keyed_apply_layout(metadata: &serde_json::Value) -> Result<bool> {
    use super::mv_uea7_documents::wire::{Graph, ROOT, Version};

    let properties = &metadata["properties"];
    let root = properties[ROOT]
        .as_str()
        .context("old MV has no keyed root")?;
    let binding: Version = serde_json::from_str(
        properties[format!("novarocks.doc.v2.rev.{root}")]
            .as_str()
            .context("old MV has no binding envelope")?,
    )?;
    if binding.format.version == 2 {
        assert_keyed_mv_layout(metadata, &Graph::from_metadata(metadata)?, true)?;
        return Ok(true);
    }
    assert_legacy_mv_generation(metadata)?;
    Ok(false)
}

fn assert_keyed_mv_layout(
    metadata: &serde_json::Value,
    graph: &super::mv_uea7_documents::wire::Graph,
    expects_apply_key: bool,
) -> Result<()> {
    let interpretation = graph
        .versions
        .get(&graph.interpretation)
        .context("keyed MV lacks its referenced interpretation")?;
    let binding = graph
        .versions
        .get(&graph.binding)
        .context("keyed MV lacks its original binding")?;
    ensure!(
        binding.format.version == 2 && interpretation.format.version == 3,
        "keyed MV did not use the expected B2/L3 format"
    );
    let apply_key = optional_protobuf_bytes_field(&interpretation.bytes()?, 6)?;
    ensure!(
        apply_key.as_ref().is_none_or(|field| !field.is_empty())
            && apply_key.is_some() == expects_apply_key,
        "unexpected apply-key presence in the keyed interpretation"
    );
    let fields = metadata["schemas"]
        .as_array()
        .and_then(|schemas| {
            schemas
                .iter()
                .find(|schema| schema["schema-id"] == metadata["current-schema-id"])
        })
        .and_then(|schema| schema["fields"].as_array())
        .context("keyed MV lacks its current Iceberg schema")?;
    if expects_apply_key {
        ensure!(
            fields
                .iter()
                .any(|field| field["name"] == "__nova_base_row_id" && field["required"] == true),
            "old keyed MV lacks its required physical apply key"
        );
    } else {
        ensure!(
            fields
                .iter()
                .map(|field| field["name"].as_str())
                .eq([Some("k1"), Some("v2")]),
            "recreated MV schema differs from its two visible columns"
        );
    }
    ensure!(
        graph.snapshots.iter().any(|(id, publication)| {
            Some(*id) == graph.current
                && publication
                    .as_ref()
                    .and_then(|record| record.binding.as_deref())
                    == Some(graph.binding.as_str())
        }),
        "keyed MV lacks a Current publication on its binding"
    );
    Ok(())
}

fn assert_legacy_mv_generation(metadata: &serde_json::Value) -> Result<()> {
    use super::mv_uea7_documents::wire::{RECORD, ROOT, Version, record};

    let properties = &metadata["properties"];
    let root = properties[ROOT]
        .as_str()
        .context("old MV has no keyed root")?;
    let read_version = |identity: &str, name: &str, format: u32| -> Result<Version> {
        let key = format!("novarocks.doc.v2.rev.{identity}");
        let raw = properties[&key]
            .as_str()
            .with_context(|| format!("missing {key}"))?;
        let version: Version = serde_json::from_str(raw)?;
        ensure!(
            version.encoded()? == raw
                && version.identity()? == identity
                && version.id == identity
                && version.owner == "novarocks.mv"
                && version.name == name
                && version.format.owner == "novarocks.mv"
                && version.format.name == name
                && version.format.version == format,
            "old producer did not persist the expected {name} generation"
        );
        Ok(version)
    };
    let binding = read_version(root, "binding", 1)?;
    ensure!(
        binding
            .refs
            .iter()
            .map(|r| r.rel.as_str())
            .eq(["definition", "interpretation"]),
        "old B1 relationships changed"
    );
    let definition = read_version(&binding.refs[0].id, "definition", 1)?;
    let interpretation = read_version(&binding.refs[1].id, "interpretation", 2)?;
    ensure!(
        definition.refs.is_empty()
            && interpretation.refs.len() == 1
            && interpretation.refs[0].rel == "definition"
            && interpretation.refs[0].id == definition.id,
        "old L2 does not reference its D1"
    );
    let current = metadata["current-snapshot-id"]
        .as_i64()
        .context("old MV lacks Current")?;
    let publication = metadata["snapshots"]
        .as_array()
        .context("old MV lacks snapshots")?
        .iter()
        .find(|s| s["snapshot-id"].as_i64() == Some(current))
        .and_then(|s| s["summary"][RECORD].as_str())
        .context("old MV lacks a publication on Current")?;
    ensure!(
        record(publication)?.binding.as_deref() == Some(root),
        "old producer publication is not bound to B1"
    );
    Ok(())
}

pub(super) fn protobuf_bytes_field(
    input: &[u8],
    expected_field: u64,
) -> Result<std::ops::Range<usize>> {
    optional_protobuf_bytes_field(input, expected_field)?
        .context("L document lacks its target schema field")
}

fn optional_protobuf_bytes_field(
    input: &[u8],
    expected_field: u64,
) -> Result<Option<std::ops::Range<usize>>> {
    let mut offset = 0;
    let mut selected = None;
    while offset < input.len() {
        let key = protobuf_varint(input, &mut offset)?;
        let field = key >> 3;
        match key & 7 {
            0 => {
                protobuf_varint(input, &mut offset)?;
            }
            2 => {
                let len = usize::try_from(protobuf_varint(input, &mut offset)?)?;
                let end = offset
                    .checked_add(len)
                    .context("protobuf field length overflow")?;
                if end > input.len() {
                    bail!("protobuf field exceeds L document");
                }
                if field == expected_field && selected.replace(offset..end).is_some() {
                    bail!("L document repeats protobuf field {expected_field}");
                }
                offset = end;
            }
            wire => bail!("unsupported L fixture protobuf wire type {wire}"),
        }
    }
    Ok(selected)
}

fn protobuf_varint(input: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *input.get(*offset).context("truncated L protobuf varint")?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("L protobuf varint exceeds 64 bits")
}

fn assert_rest_snapshot_state_unchanged(
    before: &serde_json::Value,
    after: &serde_json::Value,
) -> Result<()> {
    for (name, original, observed) in [
        (
            "Current",
            &before["current-snapshot-id"],
            &after["current-snapshot-id"],
        ),
        (
            "main",
            &before["refs"]["main"]["snapshot-id"],
            &after["refs"]["main"]["snapshot-id"],
        ),
    ] {
        let expected = original
            .as_i64()
            .with_context(|| format!("original REST MV metadata has no {name} snapshot ID"))?;
        let actual = observed
            .as_i64()
            .with_context(|| format!("observed REST MV metadata has no {name} snapshot ID"))?;
        ensure!(
            actual == expected,
            "REST MV {name} snapshot changed from {expected} to {actual}"
        );
    }
    let expected = before["snapshots"]
        .as_array()
        .context("original REST MV metadata has no snapshot list")?;
    let actual = after["snapshots"]
        .as_array()
        .context("observed REST MV metadata has no snapshot list")?;
    ensure!(
        actual == expected,
        "REST MV snapshot list changed: observed {} snapshots, expected {}",
        actual.len(),
        expected.len()
    );
    Ok(())
}

fn mv_report_text<'a>(
    result: &StatementResult,
    row: &'a [TrinoValue],
    index: usize,
) -> Result<&'a str> {
    ensure!(
        result.is_result_set()
            && result
                .columns
                .get(index)
                .is_some_and(|column| column.type_signature.raw_type == "varchar"),
        "MV report omitted its declared text column {index}"
    );
    let Some(TrinoValue::Text(value)) = row.get(index) else {
        bail!("MV report omitted its non-null text field {index}")
    };
    Ok(value)
}
fn materialized_view_states(conn: &mut SqlSession) -> Result<StatementResult> {
    let catalog = conn
        .session()
        .catalog
        .as_deref()
        .context("MV state read requires its selected catalog")?
        .replace('\'', "''");
    conn.execute(format!("SELECT name, refresh_paused, manageability, manageability_reason FROM system.runtime.materialized_view_states WHERE catalog_name = '{catalog}' AND schema_name = 'ns' ORDER BY name"))
        .context("read materialized-view state facts")
}
fn mv_state_column(result: &StatementResult, name: &str, ty: &str) -> Result<usize> {
    let columns = result
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name == name)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    ensure!(
        result.is_result_set()
            && columns.len() == 1
            && result.columns[columns[0]].type_signature.raw_type == ty,
        "MV state table omitted or duplicated declared {ty} column {name}"
    );
    Ok(columns[0])
}
fn mv_state_names(result: &StatementResult) -> Result<Vec<String>> {
    let index = mv_state_column(result, "name", "varchar")?;
    result
        .rows
        .iter()
        .map(|row| mv_report_text(result, row, index).map(str::to_owned))
        .collect()
}
fn mv_state_text(result: &StatementResult, view: &str, field: &str) -> Result<String> {
    let name = mv_state_column(result, "name", "varchar")?;
    let value = mv_state_column(result, field, "varchar")?;
    let targets = result
        .rows
        .iter()
        .filter(|row| matches!(row.get(name), Some(TrinoValue::Text(name)) if name == view))
        .collect::<Vec<_>>();
    ensure!(
        targets.len() == 1,
        "MV state table omitted or duplicated exact target {view}"
    );
    mv_report_text(result, targets[0], value).map(str::to_owned)
}

fn execute(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    action: &str,
    sql: &str,
) -> Result<()> {
    context.remaining(action)?;
    context.action(action);
    conn.execute(sql)
        .with_context(|| format!("{action}: {sql}"))
        .map(|_| ())
}

fn query(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    sql: &str,
    action: &str,
) -> Result<StatementResult> {
    context.remaining(action)?;
    context.action(action);
    conn.execute(sql)
        .with_context(|| format!("{action}: {sql}"))
}

fn refresh(context: &mut ScenarioContext, conn: &mut SqlSession, mv: &str) -> Result<()> {
    execute(
        context,
        conn,
        "refresh materialized view",
        &format!("REFRESH MATERIALIZED VIEW {mv}"),
    )
}

fn assert_mv_quarantined_after_base_replacement(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    context.remaining("verify MV is quarantined after same-name base replacement")?;
    context.action("verify MV is quarantined after same-name base replacement");
    let views = materialized_view_states(conn)?;
    let manageability = mv_state_text(&views, mv, "manageability")?;
    let reason = mv_state_text(&views, mv, "manageability_reason")?;
    if manageability != "UNAVAILABLE"
        || !reason
            .contains("published MV base occurrence 0 no longer resolves to its frozen object")
    {
        bail!(
            "same-name replacement MV is listed as {manageability:?} ({reason:?}), expected source identity quarantine; {}",
            context.diagnostics()
        );
    }
    let closed = status(context, conn, catalog, mv)?;
    let challenge = property(&closed, "Challenge")?;
    let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
    context.remaining("reject readmission onto a same-name replacement base")?;
    context.action("reject readmission onto a same-name replacement base");
    let readmission_error = match conn.execute(format!(
        "CALL novarocks_mv_resume_management('{catalog}', 'ns', '{mv}', \
         '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
         'the system scenario replaced the declared frontend process before this statement')"
    )) {
        Err(error) => error,
        Ok(_) => bail!("a replacement base readmitted the old MV unexpectedly"),
    };
    let public = mv_public_error(&readmission_error)?;
    if !public
        .message
        .contains("same-name relation was rebuilt with a different object identity")
    {
        bail!("replacement-base readmission failed for another reason: {readmission_error}");
    }
    let error = match conn.execute(format!("REFRESH MATERIALIZED VIEW {mv}")) {
        Err(error) => error,
        Ok(_) => {
            bail!(
                "a quarantined MV accepted refresh unexpectedly; {}",
                context.diagnostics()
            )
        }
    };
    let message = mv_public_error(&error)?.message.clone();
    if !message.contains("MV target requires a successful fresh Current observation") {
        bail!(
            "refresh after same-name base replacement returned unexpected error {message:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

fn assert_rows(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
) -> Result<()> {
    let actual: Vec<(i32, i64)> = mv_rows(&query(context, conn, sql, action)?)?;
    if actual != expected {
        bail!(
            "{action} returned {actual:?}, expected {expected:?}; {}",
            context.diagnostics()
        );
    }
    Ok(())
}

/// Retain the original domain fact in the error chain. Display is the producer's
/// safe canonical projection, explicitly not an observed client SQL response.
#[derive(Debug)]
struct SchedulerRequestBlocked {
    fact: novarocks_execution_contract::failure::FailureFact,
    public_message: String,
}
impl std::fmt::Display for SchedulerRequestBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MV scheduler request outcome: {}; original fact={:?}; recovery row oracle not reached",
            self.public_message, self.fact
        )
    }
}
impl std::error::Error for SchedulerRequestBlocked {}

fn check_scheduler_outcome(
    directory: &Path,
    grant_id: &str,
    mv: &str,
    expected: &novarocks_failpoint::target_binding::RoleProcessReceipt,
) -> Result<()> {
    if let Some(blocked) =
        read_scheduler_outcome(directory, grant_id, "system_mv_scheduler", mv, expected)?
    {
        return Err(anyhow::Error::new(blocked).context(format!(
            "actual Blocked scheduler request recorded at {}",
            directory.join("scheduler-outcome.json").display()
        )));
    }
    Ok(())
}

fn read_scheduler_outcome(
    directory: &Path,
    grant_id: &str,
    catalog: &str,
    mv: &str,
    expected: &novarocks_failpoint::target_binding::RoleProcessReceipt,
) -> Result<Option<SchedulerRequestBlocked>> {
    use prost::Message;
    use std::io::Read;
    expected
        .validate()
        .context("invalid authenticated scheduler observation scope")?;
    ensure!(
        expected.role == novarocks_failpoint::session::SessionRole::Fe,
        "scheduler observation expected scope is not FE"
    );
    let error_path = directory.join("scheduler-outcome-error.json");
    ensure!(
        !error_path.exists(),
        "MV scheduler observation rejected its frozen binding; outcome UNKNOWN; {}",
        error_path.display()
    );
    let path = directory.join("scheduler-outcome.json");
    const MAX_BYTES: usize = 64 * 1024;
    let file = match fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("open finite scheduler outcome {}", path.display()));
        }
    };
    let mut raw = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut raw)
        .context("read bounded scheduler outcome; outcome UNKNOWN")?;
    ensure!(
        raw.len() <= MAX_BYTES,
        "finite scheduler outcome is oversized; outcome UNKNOWN"
    );
    let record: serde_json::Value =
        serde_json::from_slice(&raw).context("decode finite scheduler outcome; outcome UNKNOWN")?;
    ensure!(
        record["schema_version"] == 1
            && record["semantic_scope"] == "scheduler-request-outcome"
            && record["decision"] == "Blocked"
            && record["grant_id"] == grant_id
            && record["target"]["catalog"] == catalog
            && record["target"]["namespace"] == "ns"
            && record["target"]["mv"] == mv,
        "finite scheduler outcome has a foreign target/grant or invalid shape; outcome UNKNOWN"
    );
    ensure!(
        record["session_id"] == expected.session_id
            && record["launch_id"] == expected.launch_id
            && record["process"] == expected.process_id,
        "finite scheduler outcome differs from the current authenticated FE session/launch/process; outcome UNKNOWN"
    );
    ensure!(
        record["admission_observation_sequence"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "scheduler outcome has no admitted request; outcome UNKNOWN"
    );
    let first = &record["first_admitted_source"];
    let blocked = &record["blocked_request_source"];
    for key in [
        "mv_id",
        "target",
        "target_object",
        "definition_revision",
        "interpretation_revision",
        "configuration_revision",
        "deployment_owner",
        "process_incarnation",
    ] {
        ensure!(
            !first[key].is_null() && first[key] == blocked[key],
            "scheduler outcome rebound frozen target field {key}; outcome UNKNOWN"
        );
    }
    for source in [first, blocked] {
        for key in [
            "target",
            "target_object",
            "metadata_version",
            "publication_output_version",
            "deployment_owner",
            "process_incarnation",
        ] {
            ensure!(
                source[key]
                    .as_str()
                    .is_some_and(|s| !s.is_empty() && s.len() <= 8192),
                "invalid scheduler source identity; outcome UNKNOWN"
            );
        }
        if !source["publication_revision"].is_null() {
            let bytes: Vec<u8> = serde_json::from_value(source["publication_revision"].clone())
                .context("invalid publication source revision; outcome UNKNOWN")?;
            ensure!(
                bytes.len() == 32,
                "invalid publication source revision width; outcome UNKNOWN"
            );
        }
        ensure!(
            source["mv_id"].as_i64().is_some(),
            "scheduler outcome omitted actual mv_id; outcome UNKNOWN"
        );
        for key in [
            "definition_revision",
            "interpretation_revision",
            "configuration_revision",
        ] {
            let bytes: Vec<u8> = serde_json::from_value(source[key].clone())
                .context("invalid source revision; outcome UNKNOWN")?;
            ensure!(
                bytes.len() == 32,
                "invalid source revision width; outcome UNKNOWN"
            );
        }
        ensure!(
            source.get("publication_revision").is_some()
                && source["metadata_version"].is_string()
                && source["publication_output_version"].is_string(),
            "scheduler outcome omitted request P/provider revision; outcome UNKNOWN"
        );
    }
    for key in [
        "actual_publication_attempt",
        "actual_task_attempt",
        "actual_base_snapshots",
    ] {
        ensure!(
            record[key] == "NOT_OBSERVED",
            "scheduler outcome claimed an unobserved pipeline witness; outcome UNKNOWN"
        );
    }
    ensure!(
        record["failure_identity"] == "Source",
        "Blocked scheduler outcome lacks original typed Source identity; outcome UNKNOWN"
    );
    let bytes: Vec<u8> = serde_json::from_value(record["failure_fact"].clone())
        .context("Blocked scheduler outcome has no typed Source fact; outcome UNKNOWN")?;
    let dto = novarocks_proto_models::novarocks::FailureFact::decode(bytes.as_slice())
        .context("invalid scheduler failure fact; outcome UNKNOWN")?;
    let fact = novarocks_task_codec::status::decode_failure_fact(
        &dto,
        novarocks_proto_codec::FieldPath::root("scheduler_request_outcome").field("failure_fact"),
    )?;
    ensure!(
        novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec() == bytes,
        "scheduler outcome has noncanonical failure bytes; outcome UNKNOWN"
    );
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    let public_message = ExecutionFailureInput::new(
        fact,
        novarocks_execution_contract::SafeDetail::truncating(
            "MV scheduler request blocked; source diagnostic retained in FE role logs",
        ),
        ExecutionFailureSite::Coordinator,
    )
    .user_error()
    .to_string();
    ensure!(
        record["public_message"] == public_message,
        "scheduler outcome message disagrees with its decoded domain fact; outcome UNKNOWN"
    );
    Ok(Some(SchedulerRequestBlocked {
        fact,
        public_message,
    }))
}

struct SchedulerOutcomeTarget {
    directory: PathBuf,
    grant_id: &'static str,
    mv: &'static str,
}

fn wait_for_rows(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    sql: &str,
    expected: &[(i32, i64)],
    action: &str,
    outcome: SchedulerOutcomeTarget,
) -> Result<()> {
    context.action(action);
    loop {
        let scope = context.handle().mv_scheduler_outcome_binding(
            outcome.grant_id,
            "system_mv_scheduler",
            "ns",
            outcome.mv,
            &outcome.directory,
        )?;
        check_scheduler_outcome(&outcome.directory, outcome.grant_id, outcome.mv, &scope)?;
        let actual = conn
            .execute(sql)
            .map_err(anyhow::Error::from)
            .and_then(|result| mv_rows(&result));
        if let Ok(rows) = actual
            && rows == expected
        {
            return Ok(());
        }
        if context.remaining(action).is_err() {
            let observed = conn
                .execute(sql)
                .map_err(anyhow::Error::from)
                .and_then(|result| mv_rows(&result));
            // Keep the controlled fixture result in the structured evidence so
            // an asynchronous refresh timeout remains diagnosable after the
            // harness redacts its full process-log diagnostic.
            context.action(format!(
                "{action} timed out with observed rows {observed:?}"
            ));
            bail!(
                "timed out waiting for {action}; expected={expected:?}; observed={observed:?}; {}",
                context.diagnostics()
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn restart_frontend(context: &mut ScenarioContext, action: &str) -> Result<()> {
    context.action(action);
    let deadline = context.deadline();
    let action = action.to_owned();
    context
        .handle()
        .restart_fe_until(deadline)
        .with_context(|| action)
}

fn wait_for_marker_count(
    context: &mut ScenarioContext,
    directory: &Path,
    expected: usize,
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        if marker_count(directory)? >= expected {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_file(context: &mut ScenarioContext, path: &Path, action: &str) -> Result<()> {
    context.action(action);
    while !path.exists() {
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn wait_for_file_or_query(
    context: &mut ScenarioContext,
    path: &Path,
    query: &control_plan::ObservedQuery,
    action: &str,
) -> Result<()> {
    context.action(action);
    while !path.exists() {
        if query.is_finished() {
            bail!(
                "rewritten query completed before its completed-plan barrier; {}",
                context.diagnostics()
            );
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
    Ok(())
}

fn marker_count(directory: &Path) -> Result<usize> {
    let count = fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mvx4-scheduler-admitted-")
        })
        .count();
    Ok(count)
}

fn clear_scheduler_markers(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read scheduler marker directory {}", directory.display()))?
    {
        let entry = entry
            .with_context(|| format!("read scheduler marker entry in {}", directory.display()))?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("mvx4-scheduler-admitted-")
        {
            fs::remove_file(entry.path()).with_context(|| {
                format!("remove stale scheduler marker {}", entry.path().display())
            })?;
        }
    }
    Ok(())
}

fn wait_for_fe_marker(
    context: &mut ScenarioContext,
    producer: &JoinedSqlResult,
    marker: &str,
    action: &str,
) -> Result<()> {
    context.action(action);
    loop {
        if producer
            .observe_required_marker(|| Ok(context.handle().fe_log_contents()?.contains(marker)))?
        {
            return Ok(());
        }
        context.remaining(action)?;
        thread::sleep(POLL_INTERVAL);
    }
}

struct JoinedSqlResult {
    result: Receiver<Result<()>>,
    actor: Mutex<Option<thread::JoinHandle<Result<()>>>>,
    cancellation: Arc<Mutex<MvActorCancellation>>,
}
impl JoinedSqlResult {
    // Every caller holds an explicit refresh trigger until this marker. A
    // completed SQL producer without the marker cannot still reach the barrier.
    fn observe_required_marker(
        &self,
        mut marker_present: impl FnMut() -> Result<bool>,
    ) -> Result<bool> {
        // Keep the result untouched once the barrier is satisfied: the caller
        // still owns its later post-mutation or post-crash result assertion.
        if marker_present()? {
            return Ok(true);
        }
        let finished = self
            .actor
            .lock()
            .map_err(|_| anyhow::anyhow!("MV SQL actor lock poisoned"))?
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished);
        if !finished {
            return Ok(false);
        }
        // Read again after producer exit, so a marker emitted between the first
        // read and SQL completion still belongs to the satisfied barrier.
        if marker_present()? {
            return Ok(true);
        }
        match self.result.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {
                self.join()?;
                bail!("original MV SQL producer exited without publishing its result")
            }
            Ok(Err(primary)) => {
                let primary = match self.join() {
                    Ok(()) => primary,
                    Err(cleanup) => {
                        primary.context(format!("original MV actor cleanup: {cleanup:#}"))
                    }
                };
                Err(primary.context("original MV SQL failed before its required FE marker"))
            }
            Ok(Ok(())) => {
                self.join()?;
                bail!("original MV SQL succeeded without reaching its required FE marker")
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                self.join()?;
                bail!("original MV SQL producer closed without publishing its result")
            }
        }
    }
    fn recv_timeout(&self, timeout: Duration) -> Result<Result<()>> {
        self.recv_timeout_inner(timeout, false)
    }
    // Only the two crash scenarios call this after kill_fe has actually waited
    // for this original FE and its role session to exit, before any restart.
    fn recv_timeout_after_original_frontend_exit(&self, timeout: Duration) -> Result<Result<()>> {
        self.recv_timeout_inner(timeout, true)
    }
    fn recv_timeout_inner(&self, timeout: Duration, frontend_exited: bool) -> Result<Result<()>> {
        let result = self
            .result
            .recv_timeout(timeout)
            .context("receive original MV SQL result");
        let cleanup = if frontend_exited {
            let joined = self.join_reader();
            let retired = self
                .cancellation
                .lock()
                .map_err(|_| anyhow::anyhow!("original MV cancellation owner poisoned"))
                .map(|mut state| {
                    state.active.take();
                });
            control_plan::with_cleanup(joined, retired)
        } else {
            let cancelled = if result.is_err() {
                self.cancel_original()
            } else {
                Ok(())
            };
            control_plan::with_cleanup(cancelled, self.join())
        };
        match (result, cleanup) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(primary), Err(cleanup)) | (Ok(Err(primary)), Err(cleanup)) => {
                control_plan::with_cleanup(Err(primary), Err(cleanup))
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
    fn cancel_original(&self) -> Result<()> {
        let active = {
            let mut state = self
                .cancellation
                .lock()
                .map_err(|_| anyhow::anyhow!("original MV cancellation owner poisoned"))?;
            state.requested = true;
            state.active.clone()
        };
        if let Some((cancel, deadline)) = active {
            cancel.delete(deadline)?;
            self.cancellation
                .lock()
                .map_err(|_| anyhow::anyhow!("original MV cancellation owner poisoned"))?
                .active
                .take();
        }
        Ok(())
    }
    fn join_reader(&self) -> Result<()> {
        if let Some(actor) = self
            .actor
            .lock()
            .map_err(|_| anyhow::anyhow!("MV SQL actor lock poisoned"))?
            .take()
        {
            actor
                .join()
                .map_err(|_| anyhow::anyhow!("original MV SQL actor panicked"))
                .and_then(|result| result)
        } else {
            Ok(())
        }
    }
    fn join(&self) -> Result<()> {
        let joined = self.join_reader();
        // The original Join owner retains nonterminal cleanup until exact
        // DELETE or confirmed exit of the same original FE, never reader EOF.
        let cancelled = self.cancel_original();
        control_plan::with_cleanup(joined, cancelled)
    }
}
impl Drop for JoinedSqlResult {
    fn drop(&mut self) {
        let cancelled = self.cancel_original();
        if let Err(error) = control_plan::with_cleanup(cancelled, self.join()) {
            eprintln!("Original MV SQL actor cleanup failed: {error:#}");
        }
    }
}

fn spawn_refresh(
    user: String,
    port: u16,
    catalog: &str,
    mv: &str,
    deadline: Instant,
) -> JoinedSqlResult {
    let catalog = catalog.to_string();
    let mv = mv.to_string();
    spawn_cancellable_sql_actor(move |cancellation| {
        let mut conn = SqlSession::new(&user, port, deadline)?;

        conn.execute(format!("USE {catalog}.ns"))?;
        run_original_mv_statement(
            &mut conn,
            &format!("REFRESH MATERIALIZED VIEW {mv}"),
            deadline,
            &cancellation,
        )?;
        Ok(())
    })
}

#[derive(Default)]
struct MvActorCancellation {
    requested: bool,
    active: Option<(novarocks_test_trino_client::CancelHandle, Instant)>,
}
fn run_original_mv_statement(
    session: &mut SqlSession,
    sql: &str,
    deadline: Instant,
    cancellation: &Arc<Mutex<MvActorCancellation>>,
) -> Result<()> {
    let pending = session.submit(sql)?;
    let cancel = pending.cancel_handle();
    let requested = {
        let mut state = cancellation
            .lock()
            .map_err(|_| anyhow::anyhow!("original MV cancellation owner poisoned"))?;
        ensure!(
            state.active.is_none(),
            "one actual target per original MV SQL actor"
        );
        state.active = Some((cancel.clone(), deadline));
        state.requested
    };
    // External cleanup before POST publication still addresses this SAME query
    // before its first GET; it never cancels a previous completed statement.
    if requested {
        cancel.delete(deadline)?;
    }
    let outcome = session.run_pending(pending);
    if !mv_response_needs_cancel(&outcome) {
        cancellation
            .lock()
            .map_err(|_| anyhow::anyhow!("original MV cancellation owner poisoned"))?
            .active
            .take();
    }
    // Return the actual reader error with its original capability still held.
    // The existing JoinedSqlResult owns the later physical cleanup decision.
    outcome.map(|_| ()).map_err(Into::into)
}
#[cfg(test)]
fn spawn_sql_actor(operation: impl FnOnce() -> Result<()> + Send + 'static) -> JoinedSqlResult {
    spawn_cancellable_sql_actor(move |_| operation())
}
fn spawn_cancellable_sql_actor(
    operation: impl FnOnce(Arc<Mutex<MvActorCancellation>>) -> Result<()> + Send + 'static,
) -> JoinedSqlResult {
    let (sender, receiver) = mpsc::sync_channel(1);
    let cancellation = Arc::new(Mutex::new(MvActorCancellation::default()));
    let original = cancellation.clone();
    let actor = thread::spawn(move || -> Result<()> {
        match sender.send(operation(original)) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(Err(primary))) => {
                Err(primary.context("MV SQL result owner closed before delivery"))
            }
            Err(mpsc::SendError(Ok(()))) => bail!("MV SQL result owner closed before delivery"),
        }
    });
    JoinedSqlResult {
        result: receiver,
        actor: Mutex::new(Some(actor)),
        cancellation,
    }
}

fn expect_refresh_failure_after_fe_exit(
    context: &mut ScenarioContext,
    receiver: JoinedSqlResult,
    action: &str,
) -> Result<()> {
    context.action(action);
    let timeout = context.remaining(action)?;
    let original = receiver
        .recv_timeout_after_original_frontend_exit(timeout)
        .with_context(|| format!("receive and join original MV refresh after FE exit: {action}"))?;
    match original {
        Err(error)
            if error.chain().any(|cause| {
                matches!(
                    cause.downcast_ref::<StatementFailure>(),
                    Some(StatementFailure::Transport(_))
                )
            }) =>
        {
            Ok(())
        }
        Err(error) => Err(error.context(format!(
            "{action} did not retain its original transport failure"
        ))),
        Ok(()) => bail!("{action} unexpectedly succeeded"),
    }
}

fn refresh_nonaggregate_after_restart(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    require_nonaggregate_management_after_restart(context, conn, catalog, mv)?;
    refresh(context, conn, mv)
}

fn require_nonaggregate_management_after_restart(
    context: &mut ScenarioContext,
    conn: &mut SqlSession,
    catalog: &str,
    mv: &str,
) -> Result<()> {
    let available = wait_for_status_phase(
        context,
        conn,
        catalog,
        mv,
        "MANAGEABLE",
        "wait for automatic management admission after FE replacement",
    )?;
    ensure!(
        property(&available, "UnsettledEffects")? == "0",
        "non-aggregate MV shows an unsettled effect after automatic management admission"
    );
    Ok(())
}

struct FileTrigger {
    path: PathBuf,
    removed: bool,
}

impl FileTrigger {
    fn create(path: &Path, contents: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create trigger directory {}", parent.display()))?;
        }
        fs::write(path, contents).with_context(|| format!("write trigger {}", path.display()))?;
        Ok(Self {
            path: path.to_owned(),
            removed: false,
        })
    }

    fn remove(mut self) -> Result<()> {
        remove_if_exists(&self.path)?;
        self.removed = true;
        Ok(())
    }
}

impl Drop for FileTrigger {
    fn drop(&mut self) {
        if !self.removed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove trigger {}", path.display())),
    }
}

#[cfg(test)]
mod control_plan_tests {
    use super::*;
    use serde_json::{Value, json};

    struct PlanInputs {
        root: tempfile::TempDir,
        binary: PathBuf,
    }
    impl PlanInputs {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let binary = root.path().join("binary-selection");
            // Selection-only fixtures; no executable is started or self-report trusted.
            fs::write(&binary, b"selection fixture").unwrap();
            Self { root, binary }
        }
        fn summaries(&self, scenario: &dyn Scenario) -> Value {
            let deadline = Instant::now() + Duration::from_secs(5);
            let plan = scenario
                .test_session_plan(
                    &ScenarioLaunchConfig::default(),
                    self.root.path(),
                    3,
                    &self.binary,
                    deadline,
                )
                .unwrap()
                .unwrap();
            assert_eq!(plan.deadline(), deadline);
            let summaries = plan.planned_launch_summaries();
            assert!(
                summaries
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|launch| launch["binary"] == json!(self.binary))
            );
            summaries
        }
    }
    fn frontend_launches(value: &Value) -> Vec<&Value> {
        value
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"] == "fe")
            .collect()
    }
    fn actions(launch: &Value) -> Vec<&str> {
        launch["grants"]
            .as_array()
            .unwrap()
            .iter()
            .map(|grant| grant["action"].as_str().unwrap())
            .collect()
    }
    fn assert_backends_ordinary(value: &Value) {
        for launch in value
            .as_array()
            .unwrap()
            .iter()
            .filter(|launch| launch["role"].as_str().unwrap().starts_with("be-"))
        {
            assert_eq!(launch["ordinary"], true);
            assert!(launch["grants"].as_array().unwrap().is_empty());
        }
    }

    #[test]
    fn staged_recovery_grants_follow_the_two_actual_restart_boundaries() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvStagedPublishedRecovery::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 3);
        assert_eq!(actions(launches[0]), ["recovery-hold"]);
        assert_eq!(actions(launches[1]), ["recovery-hold"]);
        assert_eq!(launches[0]["ordinary"], false);
        assert_eq!(launches[1]["ordinary"], false);
        assert_ne!(launches[0]["session_id"], launches[1]["session_id"]);
        assert_eq!(launches[2]["ordinary"], true);
        assert!(actions(launches[2]).is_empty());
        assert_backends_ordinary(&value);
    }

    fn scheduler_expected_receipt() -> novarocks_failpoint::target_binding::RoleProcessReceipt {
        novarocks_failpoint::target_binding::RoleProcessReceipt {
            schema_version: 1,
            session_id: "original-session".into(),
            launch_id: "original-launch".into(),
            role: novarocks_failpoint::session::SessionRole::Fe,
            process_id: "019be123-4567-7abc-8def-0123456789ab".into(),
        }
    }
    fn scheduler_outcome_record() -> serde_json::Value {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let fact = FailureFact::new(
            FailureClass::Unsupported,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(1).unwrap()),
        );
        scheduler_outcome_record_for_fact(fact)
    }
    fn scheduler_outcome_record_for_fact(
        fact: novarocks_execution_contract::failure::FailureFact,
    ) -> serde_json::Value {
        use prost::Message;
        let source = json!({"mv_id":7,"target":"actual target","target_object":"actual object digest",
            "definition_revision":vec![1u8;32],"interpretation_revision":vec![2u8;32],"configuration_revision":vec![3u8;32],
            "publication_revision":null,"metadata_version":"actual metadata revision","publication_output_version":"None",
            "deployment_owner":"owner","process_incarnation":"incarnation"});
        json!({"schema_version":1,"semantic_scope":"scheduler-request-outcome","decision":"Blocked",
            "grant_id":"outcome-a","session_id":"original-session","launch_id":"original-launch","process":scheduler_expected_receipt().process_id,
            "target":{"catalog":"system_mv_scheduler","namespace":"ns","mv":"orders_mv_a"},
            "first_admitted_source":source,"blocked_request_source":source,"admission_observation_sequence":2,
            "failure_identity":"Source", "failure_fact":novarocks_task_codec::status::encode_failure_fact(fact).encode_to_vec(),
            "public_message":novarocks_query_application::execution_error::ExecutionFailureInput::new(fact,
                novarocks_execution_contract::SafeDetail::truncating("MV scheduler request blocked; source diagnostic retained in FE role logs"),
                novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator).user_error().to_string(),
            "actual_publication_attempt":"NOT_OBSERVED","actual_task_attempt":"NOT_OBSERVED","actual_base_snapshots":"NOT_OBSERVED"})
    }
    #[test]
    fn scheduler_late_peek_preserves_typed_blocked_error_and_read_does_not_mutate() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt()
            )
            .is_ok()
        );
        let bytes = serde_json::to_vec(&scheduler_outcome_record()).unwrap();
        let path = dir.path().join("scheduler-outcome.json");
        fs::write(&path, &bytes).unwrap();
        for _ in 0..2 {
            let error = check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt(),
            )
            .unwrap_err();
            let blocked = error.downcast_ref::<SchedulerRequestBlocked>().unwrap();
            assert_eq!(
                blocked.fact.class(),
                novarocks_execution_contract::failure::FailureClass::Unsupported
            );
        }
        assert_eq!(fs::read(path).unwrap(), bytes);
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let invariant = FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
        );
        let second = tempfile::tempdir().unwrap();
        let record = scheduler_outcome_record_for_fact(invariant);
        fs::write(
            second.path().join("scheduler-outcome.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let error = check_scheduler_outcome(
            second.path(),
            "outcome-a",
            "orders_mv_a",
            &scheduler_expected_receipt(),
        )
        .unwrap_err();
        let blocked = error.downcast_ref::<SchedulerRequestBlocked>().unwrap();
        assert_eq!(blocked.fact, invariant);
        assert_eq!(
            blocked.public_message,
            record["public_message"].as_str().unwrap()
        );
        assert!(!blocked.public_message.contains("UnsupportedCompiledShape"));
    }
    #[test]
    fn scheduler_unknown_corruption_foreign_target_and_rebinding_are_not_typed_refusals() {
        for variant in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let mut record = scheduler_outcome_record();
            match variant {
                0 => record["failure_fact"] = serde_json::Value::Null,
                1 => record["target"]["mv"] = json!("other"),
                2 => {
                    record["blocked_request_source"]["configuration_revision"] =
                        json!(vec![9u8; 32])
                }
                3 => record["failure_fact"] = json!(vec![255u8]),
                4 => record["actual_task_attempt"] = json!("invented"),
                5 => record["session_id"] = json!("another-session"),
                6 => record["launch_id"] = json!("another-launch"),
                7 => record["process"] = json!("019be123-4567-7abc-8def-0123456789ac"),
                8 => {
                    record["public_message"] = json!("[UnsupportedCompiledShape] forged diagnostic")
                }
                9 => record["extra"] = json!("x".repeat(65537)),
                10 => {
                    use novarocks_execution_contract::failure::{
                        FailureClass, FailureFact, FailureReason, domain,
                    };
                    use prost::Message;
                    let invariant = FailureFact::new(
                        FailureClass::Invariant,
                        FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
                    );
                    record["failure_fact"] = json!(
                        novarocks_task_codec::status::encode_failure_fact(invariant)
                            .encode_to_vec()
                    );
                    // Keep the old Unsupported display: its fact is now Invariant.
                }
                11 => {
                    record["public_message"] =
                        json!("[query.execution.invariant] forged diagnostic")
                }
                _ => unreachable!(),
            }
            fs::write(
                dir.path().join("scheduler-outcome.json"),
                serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            let error = check_scheduler_outcome(
                dir.path(),
                "outcome-a",
                "orders_mv_a",
                &scheduler_expected_receipt(),
            )
            .unwrap_err();
            assert!(error.downcast_ref::<SchedulerRequestBlocked>().is_none());
        }
    }

    #[test]
    fn scheduler_successor_only_receives_its_remaining_transient_action() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvSchedulerRecovery::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(
            actions(launches[0]),
            [
                "scheduler-outcome",
                "scheduler-outcome",
                "scheduler-outcome",
                "scheduler-hold",
                "scheduler-hold",
                "scheduler-hold"
            ]
        );
        assert_eq!(
            actions(launches[1]),
            ["scheduler-outcome", "transient-preparation"]
        );
        assert_backends_ordinary(&value);
    }

    #[test]
    fn lake_wipe_is_not_inherited_by_the_ordinary_successor() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvLakePublicationRestartRebuild::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(
            actions(launches[0]),
            ["document-observation", "accelerator-wipe"]
        );
        assert_eq!(launches[0]["binary"], json!(input.binary));
        assert_eq!(launches[1]["ordinary"], true);
        assert_eq!(launches[1]["binary"], json!(input.binary));
        assert!(actions(launches[1]).is_empty());
        assert_backends_ordinary(&value);
    }

    #[test]
    fn retained_state_restart_does_not_request_any_control_capability() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvStateStoreRestart::default());
        assert_eq!(frontend_launches(&value).len(), 2);
        for launch in value.as_array().unwrap() {
            assert_eq!(launch["ordinary"], true);
            assert!(actions(launch).is_empty());
            assert_eq!(launch["binary"], json!(input.binary));
        }
    }

    #[test]
    fn first_refresh_fault_targets_only_the_selected_backend_launch() {
        let input = PlanInputs::new();
        let value = input.summaries(&MvFirstRefreshStaging::default());
        let launches = value.as_array().unwrap();
        let frontend = launches
            .iter()
            .find(|launch| launch["role"] == "fe")
            .unwrap();
        assert_eq!(
            actions(frontend),
            ["bind-task-execution-failure", "observe"]
        );
        let backend = launches
            .iter()
            .find(|launch| launch["role"] == "be-0")
            .unwrap();
        assert_eq!(backend["ordinary"], false);
        assert_eq!(actions(backend), ["task-execution-failure"]);
        for launch in launches
            .iter()
            .filter(|launch| launch["role"] == "be-1" || launch["role"] == "be-2")
        {
            assert_eq!(launch["ordinary"], true);
            assert!(actions(launch).is_empty());
        }
        assert!(
            launches
                .iter()
                .flat_map(|launch| actions(launch))
                .all(|action| action != "accelerator-wipe")
        );
    }

    #[test]
    fn storage_rewrite_observer_does_not_survive_the_ordinary_restart_slot() {
        let input = PlanInputs::new();
        let value =
            input.summaries(&crate::scenarios::mv_uea7_storage::MvStorageContract::default());
        let launches = frontend_launches(&value);
        assert_eq!(launches.len(), 2);
        assert_eq!(actions(launches[0]), ["rewrite-hold", "observe"]);
        assert_eq!(launches[1]["ordinary"], true);
        assert!(actions(launches[1]).is_empty());
        assert_backends_ordinary(&value);
    }

    #[test]
    fn selected_mv_control_requires_an_existing_primary_binary() {
        let input = PlanInputs::new();
        let result = MvLakePublicationRestartRebuild::default().test_session_plan(
            &ScenarioLaunchConfig::default(),
            input.root.path(),
            3,
            &input.root.path().join("missing-primary"),
            Instant::now() + Duration::from_secs(5),
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("binary is missing")
        );
    }
}

#[cfg(test)]
mod refresh_marker_tests {
    use super::*;

    fn query_failure() -> anyhow::Error {
        let error = serde_json::from_value::<novarocks_test_trino_client::TrinoError>(serde_json::json!({
            "errorCode":46,"errorName":"TABLE_NOT_FOUND","errorType":"USER_ERROR","message":"original refresh SQL failed"
        })).unwrap();
        anyhow::Error::from(StatementFailure::Trino(error)).context("execute original refresh")
    }
    fn assert_original_sql_error(error: &anyhow::Error) {
        let Some(StatementFailure::Trino(actual)) = error.downcast_ref::<StatementFailure>() else {
            panic!("original typed SQL error missing: {error:#}");
        };
        assert_eq!(actual.error_code, 46);
        assert_eq!(actual.error_name, "TABLE_NOT_FOUND");
        assert_eq!(actual.error_type, "USER_ERROR");
        assert_eq!(actual.message, "original refresh SQL failed");
    }

    #[test]
    fn source_hold_expected_refusal_keeps_the_original_typed_sql_cause() {
        let outcome =
            source_hold_outcomes(Some(Err(query_failure())), Ok(()), Ok(()), Ok(()), Ok(()))
                .unwrap();
        assert_original_sql_error(&outcome.unwrap_err());
    }

    #[test]
    fn source_hold_release_failure_cannot_replace_an_original_sql_failure() {
        let error = source_hold_outcomes(
            Some(Err(query_failure())),
            Ok(()),
            Ok(()),
            Ok(()),
            Err(anyhow::anyhow!("original trigger release failed")),
        )
        .unwrap_err();
        assert_original_sql_error(&error);
        assert!(format!("{error:#}").contains("original trigger release failed"));
    }

    #[test]
    fn source_hold_deadline_and_join_failure_remain_contexts_of_observed_sql_failure() {
        let error = source_hold_outcomes(
            Some(Err(query_failure())),
            Ok(()),
            Err(anyhow::anyhow!("original remaining deadline expired")),
            Err(anyhow::anyhow!("original actor join failed")),
            Ok(()),
        )
        .unwrap_err();
        assert_original_sql_error(&error);
        let detail = format!("{error:#}");
        assert!(detail.contains("original remaining deadline expired"));
        assert!(detail.contains("original actor join failed"));
        // Successful SQL plus an infrastructure failure is never returned as
        // either an expected SQL refusal or a successful held refresh.
        assert!(
            source_hold_outcomes(
                Some(Ok(())),
                Ok(()),
                Err(anyhow::anyhow!("original receive failed")),
                Ok(()),
                Ok(()),
            )
            .is_err()
        );
    }
    #[test]
    fn pre_marker_failure_preserves_original_sql_code_name_and_type() {
        let producer = spawn_sql_actor(|| Err(query_failure()));
        producer.join().unwrap();
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert_original_sql_error(&error);
    }

    #[test]
    fn pre_marker_connection_failure_is_published_before_any_sql() {
        let producer = spawn_sql_actor(|| {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "original connect",
            ))
            .context("connect original refresh actor")
        });
        producer.join().unwrap();
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::ConnectionRefused
        );
    }

    #[test]
    fn successful_producer_without_required_marker_fails_instead_of_waiting() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(false)).is_err());
    }

    #[test]
    fn satisfied_marker_does_not_consume_successful_producer_result() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn satisfied_marker_keeps_later_post_crash_failure_assertion_owned() {
        let producer = spawn_sql_actor(|| Err(query_failure()));
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        let error = producer.recv_timeout(Duration::ZERO).unwrap().unwrap_err();
        assert_original_sql_error(&error);
    }

    #[test]
    fn in_flight_producer_waits_without_consuming_its_future_result() {
        let (release, held) = mpsc::sync_channel(1);
        let producer = spawn_sql_actor(move || {
            held.recv().context("release held original SQL actor")?;
            Ok(())
        });
        assert!(!producer.observe_required_marker(|| Ok(false)).unwrap());
        release.send(()).unwrap();
        producer.join().unwrap();
        assert!(producer.observe_required_marker(|| Ok(true)).unwrap());
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn marker_published_during_producer_completion_is_not_missing() {
        let producer = spawn_sql_actor(|| Ok(()));
        producer.join().unwrap();
        let mut reads = 0;
        assert!(
            producer
                .observe_required_marker(|| {
                    reads += 1;
                    Ok(reads == 2)
                })
                .unwrap()
        );
        assert_eq!(reads, 2);
        producer.recv_timeout(Duration::ZERO).unwrap().unwrap();
    }

    #[test]
    fn closed_receiver_keeps_original_sql_error_in_producer_join() {
        let (release, held) = mpsc::sync_channel(1);
        let mut producer = spawn_sql_actor(move || {
            held.recv().context("release original producer")?;
            Err(query_failure())
        });
        let (replacement_sender, replacement_receiver) = mpsc::sync_channel(1);
        drop(replacement_sender);
        drop(std::mem::replace(
            &mut producer.result,
            replacement_receiver,
        ));
        release.send(()).unwrap();
        let error = producer.join().unwrap_err();
        assert_original_sql_error(&error);
    }

    #[test]
    fn finished_producer_with_empty_result_owner_does_not_wait_for_marker() {
        let (sender, receiver) = mpsc::sync_channel(1);
        // Keep a foreign sender alive so this is Empty, not Disconnected. The
        // owned producer has already exited without delivering its result.
        let actor = thread::spawn(|| Ok(()));
        while !actor.is_finished() {
            thread::yield_now();
        }
        let producer = JoinedSqlResult {
            result: receiver,
            actor: Mutex::new(Some(actor)),
            cancellation: Arc::new(Mutex::new(MvActorCancellation::default())),
        };
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert!(format!("{error:#}").contains("exited without publishing its result"));
        assert!(producer.actor.lock().unwrap().is_none());
        drop(sender);
    }

    #[test]
    fn disconnected_producer_without_result_is_not_a_marker_wait() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);
        let producer = JoinedSqlResult {
            result: receiver,
            actor: Mutex::new(None),
            cancellation: Arc::new(Mutex::new(MvActorCancellation::default())),
        };
        assert!(producer.observe_required_marker(|| Ok(false)).is_err());
    }

    #[test]
    fn panicked_producer_is_reported_before_marker_deadline() {
        let producer = spawn_sql_actor(|| panic!("original SQL actor panic"));
        // Wait for the real thread to exit without consuming its join result.
        while !producer
            .actor
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_finished()
        {
            thread::yield_now();
        }
        let error = producer.observe_required_marker(|| Ok(false)).unwrap_err();
        assert!(format!("{error:#}").contains("original MV SQL actor panicked"));
    }
}

#[cfg(test)]
mod original_refresh_cancellation_owner_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct OriginalHttpPeer {
        port: u16,
        stop: Arc<AtomicBool>,
        deletes: Arc<AtomicUsize>,
        worker: Option<thread::JoinHandle<Result<()>>>,
    }
    impl OriginalHttpPeer {
        fn start() -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let deletes = Arc::new(AtomicUsize::new(0));
            let stopped = stop.clone();
            let observed = deletes.clone();
            let worker = thread::spawn(move || -> Result<()> {
                let deadline = Instant::now() + Duration::from_secs(5);
                while !stopped.load(Ordering::SeqCst) {
                    ensure!(
                        Instant::now() < deadline,
                        "original HTTP peer deadline expired"
                    );
                    let mut stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                    let method = read_original_request(&mut stream)?;
                    match method.as_str() {
                        "POST" => {
                            let body = serde_json::to_vec(&serde_json::json!({
                                "id":"original-refresh-owner",
                                "nextUri":format!("http://127.0.0.1:{port}/v1/statement/original-refresh-owner/0")
                            }))?;
                            write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )?;
                            stream.write_all(&body)?;
                        }
                        // The original GET really loses its connection. The
                        // same peer remains available for its owner's DELETE.
                        "GET" => {}
                        "DELETE" => {
                            observed.fetch_add(1, Ordering::SeqCst);
                            stream.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
                        }
                        other => bail!("unexpected original HTTP request method {other}"),
                    }
                }
                Ok(())
            });
            Self {
                port,
                stop,
                deletes,
                worker: Some(worker),
            }
        }
        fn finish(mut self) -> usize {
            self.stop.store(true, Ordering::SeqCst);
            self.worker.take().unwrap().join().unwrap().unwrap();
            self.deletes.load(Ordering::SeqCst)
        }
    }
    impl Drop for OriginalHttpPeer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(worker) = self.worker.take() {
                worker.join().unwrap().unwrap();
            }
        }
    }
    fn read_original_request(stream: &mut TcpStream) -> Result<String> {
        let mut bytes = Vec::new();
        let mut byte = [0];
        while !bytes.ends_with(b"\r\n\r\n") {
            ensure!(
                bytes.len() < 4096,
                "original HTTP request headers exceed fixture cap"
            );
            stream.read_exact(&mut byte)?;
            bytes.push(byte[0]);
        }
        let headers = String::from_utf8(bytes)?;
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>())
            })
            .transpose()?
            .unwrap_or(0);
        ensure!(
            length <= 4096,
            "original HTTP request body exceeds fixture cap"
        );
        stream.read_exact(&mut vec![0; length])?;
        Ok(headers
            .split_whitespace()
            .next()
            .context("original HTTP method missing")?
            .to_owned())
    }
    #[test]
    fn interrupted_refresh_leaves_cancellation_with_its_original_join_owner() {
        let peer = OriginalHttpPeer::start();
        let port = peer.port;
        let producer = spawn_cancellable_sql_actor(move |cancellation| {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut session = SqlSession::new("root", port, deadline)?;
            run_original_mv_statement(&mut session, "SELECT 1", deadline, &cancellation)
        });
        let original = producer
            .result
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let before_join = peer.deletes.load(Ordering::SeqCst);
        let joined = producer.join();
        drop(producer);
        let after_join = peer.finish();
        eprintln!(
            "original MV SQL actor, HTTP GET/DELETE peer and cancellation owner actually exited before custody assertions"
        );
        assert!(joined.is_ok(), "original Join cleanup failed: {joined:?}");
        let error = original.unwrap_err();
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<StatementFailure>(),
            Some(StatementFailure::Transport(_))
        )));
        assert_eq!(
            before_join, 0,
            "the reader must return its original error before its Join owner chooses cleanup"
        );
        assert_eq!(
            after_join, 1,
            "a live original FE still requires its exact DELETE at Join"
        );
    }
}
