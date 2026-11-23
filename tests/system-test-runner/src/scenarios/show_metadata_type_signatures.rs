// SPDX-License-Identifier: Apache-2.0

use super::result_delivery_support::await_idle;
use crate::{
    actors::trino_protocol::TrinoClient,
    scenario::{Scenario, ScenarioContext, ScenarioDeclaration},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use serde_json::Value;
use std::time::Instant;

pub(super) struct ShowMetadataTypeSignatures;

const PAUSED_CATALOG: &str = "trc7_paused_paimon";

impl Scenario for ShowMetadataTypeSignatures {
    fn name(&self) -> &'static str {
        "trino-protocol/show-metadata-type-signatures"
    }

    fn declaration(&self) -> ScenarioDeclaration {
        super::declarations::for_module("trino_protocol", self.name(), false, false)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "SHOW coverage requires 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "trino", "before", epoch)?;
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("trino-http"),
        )?;
        let warehouse = context.scenario_root().join("paused-paimon");
        let warehouse = warehouse.to_string_lossy().replace('\'', "''");
        let create = format!(
            "CREATE CATALOG {PAUSED_CATALOG} USING paimon WITH (\
             \"paimon.catalog.type\" = 'filesystem', \"warehouse\" = 'file://{warehouse}')"
        );
        let result = probe_show_signatures(&actor, &create);
        let cleanup = query(&actor, &format!("DROP CATALOG IF EXISTS {PAUSED_CATALOG}"));
        match (result, cleanup) {
            (Err(probe), Err(cleanup)) => anyhow::bail!("{probe:#}; catalog cleanup: {cleanup:#}"),
            (Err(error), _) | (_, Err(error)) => return Err(error),
            (Ok(()), Ok(_)) => {}
        }
        context.action("verified raw SHOW signatures, filtered empty results, paused provider and terminal pages");
        await_idle(context, "trino", "after", epoch)?;
        Ok(())
    }
}

fn probe_show_signatures(actor: &TrinoClient, create: &str) -> Result<()> {
    query(actor, create)?;
    let (catalogs, catalog_rows) = query(actor, "SHOW CATALOGS")?;
    ensure!(catalogs[0]["name"] == "Catalog");
    ensure!(catalog_rows.iter().any(|row| row[0] == PAUSED_CATALOG));
    check_widths(&catalogs, &catalog_rows)?;
    let (filtered_catalogs, rows) = query(actor, "SHOW CATALOGS LIKE 'system'")?;
    ensure!(rows == [serde_json::json!(["system"])]);
    ensure!(filtered_catalogs == catalogs);
    let (empty_catalogs, rows) = query(actor, "SHOW CATALOGS LIKE 'missing_trc7_catalog'")?;
    ensure!(rows.is_empty() && empty_catalogs == catalogs);

    for (full, filter) in [
        ("SHOW FUNCTIONS", "SHOW FUNCTIONS LIKE 'abs'"),
        ("SHOW SESSION", "SHOW SESSION LIKE 'missing_trc7_property'"),
    ] {
        let (columns, rows) = query(actor, full)?;
        ensure!(!rows.is_empty(), "{full} produced no visible candidates");
        check_widths(&columns, &rows)?;
        let (filtered, _) = query(actor, filter)?;
        ensure!(
            filtered == columns,
            "{filter} changed the candidate type signatures"
        );
    }
    let (columns, rows) = query(
        actor,
        "SHOW FUNCTIONS FROM missing_trc7_catalog.some_schema",
    )?;
    ensure!(rows.is_empty() && columns.len() == 6);
    for (ordinal, column) in columns.iter().enumerate() {
        let signature = &column["typeSignature"];
        ensure!(signature["rawType"] == if ordinal == 4 { "boolean" } else { "varchar" });
        ensure!(signature["arguments"].as_array().is_some_and(Vec::is_empty));
    }

    let (columns, rows) = query(actor, "SHOW TABLES FROM system.information_schema")?;
    ensure!(columns[0]["name"] == "Table");
    ensure!(
        rows == ["columns", "schemata", "tables", "views"].map(|name| serde_json::json!([name]))
    );

    let (columns, rows) = query(actor, "SHOW FUNCTIONS FROM system.builtin")?;
    ensure!(rows.iter().all(|row| row[3] == "table"));
    if !rows.is_empty() {
        check_widths(&columns, &rows)?;
    }

    let pages = actor.execute(&format!("SHOW SCHEMAS FROM {PAUSED_CATALOG}"))?;
    let final_page = pages
        .last()
        .context("paused catalog omitted its terminal page")?
        .json()?;
    ensure!(
        final_page["error"]["errorName"] == "NOT_SUPPORTED",
        "{final_page}"
    );
    ensure!(final_page.get("nextUri").is_none());
    Ok(())
}

fn check_widths(columns: &[Value], rows: &[Value]) -> Result<()> {
    for (ordinal, column) in columns.iter().enumerate() {
        let signature = &column["typeSignature"];
        if signature["rawType"] != "varchar" {
            ensure!(signature["rawType"] == "boolean");
            continue;
        }
        let width = rows
            .iter()
            .map(|row| {
                row[ordinal]
                    .as_str()
                    .map(|text| text.chars().count())
                    .context("SHOW text column contains a non-string")
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .context("missing full candidate rows")?;
        let arguments = signature["arguments"]
            .as_array()
            .context("missing varchar signature arguments")?;
        ensure!(arguments.len() == 1 && arguments[0]["kind"] == "LONG");
        ensure!(
            arguments[0]["value"].as_u64() == Some(width as u64),
            "{column}"
        );
    }
    Ok(())
}

fn query(actor: &TrinoClient, sql: &str) -> Result<(Vec<Value>, Vec<Value>)> {
    let pages = actor.execute(sql)?;
    let terminal = pages.last().context("query omitted its terminal page")?;
    ensure!(
        terminal.next_uri()?.is_none(),
        "{sql} did not finish at its last page"
    );
    let final_json = terminal.assert_success()?;
    ensure!(final_json["stats"]["state"] == "FINISHED", "{final_json}");
    let mut columns = None;
    let mut rows = Vec::new();
    for page in pages {
        let json = page.assert_success()?;
        if let Some(actual_columns) = json["columns"].as_array() {
            let frozen = columns.get_or_insert_with(|| actual_columns.clone());
            ensure!(
                frozen == actual_columns,
                "{sql} changed its returned column signatures"
            );
        }
        if let Some(page_rows) = json["data"].as_array() {
            rows.extend(page_rows.iter().cloned());
        }
    }
    Ok((columns.unwrap_or_default(), rows))
}
