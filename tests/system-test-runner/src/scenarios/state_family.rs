use crate::actors::sql::{self, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;

const REQUIRED_BACKENDS: usize = 3;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(WipeStartRebuild)]
}

/// Destroy the whole frontend durable store and prove the lake is the truth.
///
/// The frontend's durable store carries only a catalog desired-state projection
/// and rebuildable accelerators. Wiping it must therefore lose the *attachment*
/// while losing none of the *data*: re-attaching the same warehouse has to
/// reproduce every row from the lake alone.
///
/// Two things make this stronger than "a query still works after a restart".
/// First, the pre-wipe catalog must be observably gone, which is what proves
/// the wipe did anything at all — a wipe that silently removed nothing would
/// otherwise let every later assertion pass for the wrong reason. Second, the
/// post-wipe catalog is a brand-new attachment, so its connector incarnation is
/// freshly minted and the rows it serves cannot have come from frontend state
/// that survived. The backends are deliberately left running, so their warm
/// connector bindings are exactly the thing a weaker assertion would mistake
/// for recovery.
struct WipeStartRebuild;

impl Scenario for WipeStartRebuild {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "state_family",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "state-family/wipe-start-rebuild"
    }

    // No `launch_config` override is needed: backends self-register, so the
    // restarted frontend rebuilds membership from their announcements and the
    // destroyed store contributes nothing to the topology barrier.

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;

        // The warehouse lives outside the durable store, so it is the lake for
        // the purposes of this scenario and must survive the wipe untouched.
        let warehouse = context
            .runtime_dir()
            .join("warehouses")
            .join("state-family");
        std::fs::create_dir_all(&warehouse)
            .with_context(|| format!("create warehouse {}", warehouse.display()))?;
        let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");

        context.remaining("connect state family client")?;
        let mut connection =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        attach_catalog(&mut connection, "wipe_before", &warehouse)?;
        connection
            .execute("CREATE DATABASE wipe_before.ns")
            .context("create pre-wipe namespace")?;
        connection
            .execute("CREATE TABLE wipe_before.ns.orders (id INT, amount INT)")
            .context("create pre-wipe table")?;
        connection
            .execute("INSERT INTO wipe_before.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
            .context("write pre-wipe rows")?;
        let before = attachment_rows(
            &connection
                .execute("SELECT id, amount FROM wipe_before.ns.orders ORDER BY id")
                .context("read pre-wipe rows")?,
        )?;
        ensure!(
            before == vec![(1, 10), (2, 20), (3, 30)],
            "unexpected pre-wipe rows: {before:?}"
        );
        context.action("published a lake table through a durable catalog attachment");

        // A local view in a non-external catalog is the one user-visible
        // behaviour this series changes: it is process runtime state now, so it
        // must not survive the frontend that defined it. Its body is
        // catalog-free on purpose, so its later absence is attributable to the
        // registry being process-local rather than to the attachment going away.
        connection
            .execute("CREATE VIEW local_probe AS SELECT 1")
            .context("create a local view")?;
        let local = sql::integer_column(
            &connection
                .execute("SELECT * FROM local_probe")
                .context("read through the local view before the wipe")?,
        )?;
        ensure!(
            local == vec![1],
            "unexpected local view rows before the wipe: {local:?}"
        );
        context.action("defined a local view that resolves on the defining frontend");

        drop(connection);
        let deadline = context.deadline();
        context
            .handle()
            .wipe_fe_state_store_and_restart_until(deadline)
            .context("wipe the FE durable state store and restart")?;
        context.action("destroyed the FE durable state store and restarted the FE");

        context.remaining("reconnect after wipe")?;
        let mut connection =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;

        // The wipe must be observable. Without this the rest of the scenario
        // would pass even if nothing had been destroyed. The identical query
        // succeeded above, so its failure now is attributable to the wipe, and
        // requiring the catalog name in the error keeps an unrelated failure
        // from being mistaken for the attachment being gone.
        let survived =
            connection.execute("SELECT id, amount FROM wipe_before.ns.orders ORDER BY id");
        let error = match survived {
            Ok(rows) => anyhow::bail!(
                "the pre-wipe catalog attachment still served rows after its durable \
                 store was destroyed, so the wipe removed nothing: {rows:?}"
            ),
            Err(StatementFailure::Trino(error)) => error.message,
            Err(error) => return Err(error.into()),
        };
        ensure!(
            error.contains("wipe_before"),
            "expected the pre-wipe catalog to be unresolvable after the wipe, got: {error}"
        );
        context.action("verified the pre-wipe catalog attachment no longer resolves");

        // Re-attaching the same warehouse must rebuild everything from the lake.
        attach_catalog(&mut connection, "wipe_after", &warehouse)?;
        let restored = attachment_rows(
            &connection
                .execute("SELECT id, amount FROM wipe_after.ns.orders ORDER BY id")
                .context("read rows through a post-wipe attachment")?,
        )?;
        ensure!(
            restored == before,
            "rows rebuilt from the lake differ from the published rows: {restored:?}"
        );
        context.action("re-attached the same warehouse and reproduced every row from the lake");

        // The local view must be gone, and for the right reason: the registry
        // never outlives its frontend.
        let local_after = connection.execute("SELECT * FROM local_probe");
        let local_error = match local_after {
            Ok(rows) => anyhow::bail!(
                "a local view outlived the frontend incarnation that defined it: {rows:?}"
            ),
            Err(StatementFailure::Trino(error)) => error.message,
            Err(error) => return Err(error.into()),
        };
        ensure!(
            local_error.contains("local_probe"),
            "expected the local view to be unresolvable after the restart, got: {local_error}"
        );
        context.action("verified the local view did not outlive its frontend");

        connection
            .execute("DROP CATALOG wipe_after")
            .context("drop the post-wipe catalog attachment")?;
        Ok(())
    }
}

fn attach_catalog(connection: &mut sql::SqlSession, name: &str, warehouse: &str) -> Result<()> {
    connection
        .execute(format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\"type\"=\"iceberg\",\
             \"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
        ))
        .with_context(|| format!("create catalog attachment {name}"))
        .map(|_| ())
}

fn attachment_rows(result: &StatementResult) -> Result<Vec<(i32, i32)>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 2,
        "state-family lake attachment requires two declared INT columns: {result:?}"
    );
    for column in &result.columns {
        ensure!(
            column.type_name == "integer"
                && column.type_signature.raw_type == "integer"
                && column.type_signature.arguments.is_empty(),
            "state-family lake attachment column does not match its declared INT type: {column:?}"
        );
    }
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(id), TrinoValue::Integer(amount)] => {
                Ok((i32::try_from(*id)?, i32::try_from(*amount)?))
            }
            row => anyhow::bail!(
                "state-family lake attachment requires two non-null INT cells: {row:?}"
            ),
        })
        .collect()
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, received 1FE+{count}BE",
        context.name()
    );
    Ok(())
}
