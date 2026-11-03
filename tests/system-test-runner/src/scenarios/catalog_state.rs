use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
const REQUIRED_BACKENDS: usize = 3;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(CatalogAttachmentRestart)]
}

struct CatalogAttachmentRestart;

impl Scenario for CatalogAttachmentRestart {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "catalog_state",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "catalog-state/catalog-attachment-restart"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect catalog attachment client")?,
        )?;
        let warehouse = context
            .runtime_dir()
            .join("warehouses")
            .join("catalog-attachment");
        std::fs::create_dir_all(&warehouse).with_context(|| {
            format!(
                "create catalog attachment warehouse {}",
                warehouse.display()
            )
        })?;
        let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
        connection
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG tst3_attachment PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
            ))
            .context("create durable catalog attachment")?;
        connection
            .query_drop("CREATE DATABASE tst3_attachment.ns")
            .context("create catalog attachment namespace")?;
        connection
            .query_drop("CREATE TABLE tst3_attachment.ns.orders (id INT, amount INT)")
            .context("create catalog attachment table")?;
        connection
            .query_drop("INSERT INTO tst3_attachment.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
            .context("write catalog attachment rows")?;
        let before: Vec<(i32, i32)> = connection
            .query("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
            .context("query catalog attachment before FE restart")?;
        ensure!(
            before == vec![(1, 10), (2, 20), (3, 30)],
            "unexpected attachment rows: {before:?}"
        );
        context.action("created attachment and verified a distributed public-SQL read");

        drop(connection);
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE with durable catalog attachment")?;
        context.action("restarted FE with the attachment in its durable StateStore");
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("reconnect after catalog attachment restart")?,
        )?;
        let restored: Vec<(i32, i32)> = connection
            .query("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
            .context("query catalog attachment restored by FE restart")?;
        ensure!(
            restored == before,
            "attachment rows changed after FE restart: {restored:?}"
        );
        context
            .action("verified catalog attachment restored and served the same distributed query");
        connection
            .query_drop("DROP CATALOG tst3_attachment")
            .context("drop durable catalog attachment")?;
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    ensure!(
        count == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, received 1FE+{count}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}
