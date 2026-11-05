use crate::actors::sql::{self, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
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
        context.remaining("connect catalog attachment client")?;
        let mut connection =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
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
            .execute(format!(
                "CREATE EXTERNAL CATALOG tst3_attachment PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
            ))
            .context("create durable catalog attachment")?;
        connection
            .execute("CREATE DATABASE tst3_attachment.ns")
            .context("create catalog attachment namespace")?;
        connection
            .execute("CREATE TABLE tst3_attachment.ns.orders (id INT, amount INT)")
            .context("create catalog attachment table")?;
        connection
            .execute("INSERT INTO tst3_attachment.ns.orders VALUES (1, 10), (2, 20), (3, 30)")
            .context("write catalog attachment rows")?;
        let before = attachment_rows(
            &connection
                .execute("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
                .context("query catalog attachment before FE restart")?,
        )?;
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
        context.remaining("reconnect after catalog attachment restart")?;
        let mut connection =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let restored = attachment_rows(
            &connection
                .execute("SELECT id, amount FROM tst3_attachment.ns.orders ORDER BY id")
                .context("query catalog attachment restored by FE restart")?,
        )?;
        ensure!(
            restored == before,
            "attachment rows changed after FE restart: {restored:?}"
        );
        context
            .action("verified catalog attachment restored and served the same distributed query");
        connection
            .execute("DROP CATALOG tst3_attachment")
            .context("drop durable catalog attachment")?;
        Ok(())
    }
}

fn attachment_rows(result: &StatementResult) -> Result<Vec<(i32, i32)>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 2,
        "catalog attachment requires two declared INT columns: {result:?}"
    );
    for column in &result.columns {
        ensure!(
            column.type_name == "integer"
                && column.type_signature.raw_type == "integer"
                && column.type_signature.arguments.is_empty(),
            "catalog attachment column does not match its declared INT type: {column:?}"
        );
    }
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(id), TrinoValue::Integer(amount)] => {
                Ok((i32::try_from(*id)?, i32::try_from(*amount)?))
            }
            row => anyhow::bail!("catalog attachment requires two non-null INT cells: {row:?}"),
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
    context.action("verified native 1FE+3BE topology");
    Ok(())
}
