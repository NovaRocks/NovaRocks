// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use novarocks_fs::FileCancellation;
use serde_json::Value;

use crate::fixture_object_store::FixtureObjectStore;
mod wire;

#[cfg(test)]
mod tests;

pub(crate) struct GraphExpectation<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) table: &'a str,
    pub(crate) publications: usize,
    pub(crate) table_commits: Option<usize>,
    pub(crate) failed_table_commits: usize,
    pub(crate) slot_changes: usize,
    logical_rows: Option<u64>,
    total_records: Option<u64>,
    delete_files_min: Option<u64>,
    metadata_only_last: bool,
    full_overwrite_last: bool,
    append_last: bool,
    interpretation_changes: bool,
}

/// Read the product's own REST metadata while the runner-owned isolated
/// catalog and native cluster are still alive. SQL result rows alone cannot
/// prove the retained P -> B -> D/L graph or the absence of document files.
pub(crate) fn assert_graph(suite: &str, directive: &str) -> Result<String> {
    ensure!(
        matches!(
            suite,
            "mv-storage-contract" | "mv-storage-physical-occ" | "mv-publication-v11"
        ),
        "@mv_rest_document_graph requires an isolated MV publication suite"
    );
    let expectation = parse_expectation(directive)?;
    let (namespace, table) = (expectation.namespace, expectation.table);
    let rest =
        std::env::var("NOVAROCKS_ICEBERG_REST_URI").context("isolated REST URI is unavailable")?;
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest.trim_end_matches('/')
    );
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .get(&url)
        .send()
        .with_context(|| format!("read exact MV REST metadata at {url}"))?
        .error_for_status()
        .with_context(|| format!("REST did not return MV metadata at {url}"))?;
    let value: Value = response.json().context("decode MV REST metadata")?;
    let summary = verify_graph(&value, &expectation)?;
    verify_no_document_files(&value["metadata"])?;
    Ok(summary)
}

pub(crate) fn parse_expectation(directive: &str) -> Result<GraphExpectation<'_>> {
    let (target, parameters) = directive
        .split_once(",publications=")
        .context("@mv_rest_document_graph requires <namespace>.<table>,publications=<count>")?;
    let mut parameters = parameters.split(',');
    let count = parameters.next().context("missing publication count")?;
    let mut metadata_only_last = false;
    let mut full_overwrite_last = false;
    let mut append_last = false;
    let mut interpretation_changes = false;
    let mut table_commits = None;
    let mut failed_table_commits = None;
    let mut slot_changes = None;
    let mut logical_rows = None;
    let mut total_records = None;
    let mut delete_files_min = None;
    for parameter in parameters {
        match parameter {
            "metadata-only-last=true" if !metadata_only_last => metadata_only_last = true,
            "full-overwrite-last=true" if !full_overwrite_last => full_overwrite_last = true,
            "append-last=true" if !append_last => append_last = true,
            "interpretation-changes=true" if !interpretation_changes => {
                interpretation_changes = true
            }
            value if value.starts_with("table-commits=") && table_commits.is_none() => {
                table_commits = Some(
                    value["table-commits=".len()..]
                        .parse::<usize>()
                        .context("invalid table-commits count")?,
                );
            }
            value
                if value.starts_with("failed-table-commits=") && failed_table_commits.is_none() =>
            {
                failed_table_commits = Some(
                    value["failed-table-commits=".len()..]
                        .parse::<usize>()
                        .context("invalid failed-table-commits count")?,
                );
            }
            value if value.starts_with("slot-changes=") && slot_changes.is_none() => {
                slot_changes = Some(
                    value["slot-changes=".len()..]
                        .parse::<usize>()
                        .context("invalid slot-changes count")?,
                );
            }
            value if value.starts_with("logical-rows=") && logical_rows.is_none() => {
                logical_rows = Some(
                    value["logical-rows=".len()..]
                        .parse::<u64>()
                        .context("invalid logical-rows count")?,
                );
            }
            value if value.starts_with("total-records=") && total_records.is_none() => {
                total_records = Some(
                    value["total-records=".len()..]
                        .parse::<u64>()
                        .context("invalid total-records count")?,
                );
            }
            value if value.starts_with("delete-files-min=") && delete_files_min.is_none() => {
                delete_files_min = Some(
                    value["delete-files-min=".len()..]
                        .parse::<u64>()
                        .context("invalid delete-files-min count")?,
                );
            }
            _ => anyhow::bail!("unknown or repeated MV document graph parameter {parameter}"),
        }
    }
    let slot_changes = slot_changes.unwrap_or_default();
    let publications = count
        .parse::<usize>()
        .context("invalid publication count")?;
    ensure!(
        !metadata_only_last || publications >= 2,
        "metadata-only-last requires at least two publications"
    );
    ensure!(
        !full_overwrite_last || publications >= 2,
        "full-overwrite-last requires at least two publications"
    );
    ensure!(
        !append_last || publications >= 2,
        "append-last requires at least two publications"
    );
    ensure!(
        !interpretation_changes || publications == 2,
        "interpretation-changes currently requires exactly two publications"
    );
    ensure!(
        usize::from(metadata_only_last)
            + usize::from(full_overwrite_last)
            + usize::from(append_last)
            <= 1,
        "last publication can have only one expected physical write shape"
    );
    ensure!(
        table_commits.is_none_or(|commits| commits > publications + slot_changes),
        "table-commits cannot be less than CREATE plus publication and slot commits"
    );
    let (namespace, table) = target
        .split_once('.')
        .context("document graph target requires <namespace>.<table>")?;
    ensure!(
        [namespace, table]
            .iter()
            .all(|part| !part.is_empty()
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
        "document graph target has an invalid REST identifier"
    );
    Ok(GraphExpectation {
        namespace,
        table,
        publications,
        table_commits,
        failed_table_commits: failed_table_commits.unwrap_or_default(),
        slot_changes,
        logical_rows,
        total_records,
        delete_files_min,
        metadata_only_last,
        full_overwrite_last,
        append_last,
        interpretation_changes,
    })
}

fn mv_document<'a>(
    properties: &'a wire::Properties,
    id: &str,
    name: &str,
    version: u32,
) -> Result<&'a wire::Document> {
    let document = properties
        .versions
        .get(id)
        .with_context(|| format!("missing {name} version {id}"))?;
    ensure!(
        document.owner == "novarocks.mv"
            && document.name == name
            && document.format
                == wire::Format {
                    owner: "novarocks.mv".into(),
                    name: name.into(),
                    version
                },
        "{id} has wrong MV document/format identity"
    );
    Ok(document)
}
struct BindingFacts {
    definition: String,
    interpretation: String,
    formats: Vec<wire::DataVersionFormat>,
}
fn binding_facts(properties: &wire::Properties, id: &str) -> Result<BindingFacts> {
    wire::closure(properties, id)?;
    let binding = mv_document(properties, id, "binding", 1)?;
    ensure!(binding.refs.len() == 2, "B must reference exactly D/L");
    let definition = binding
        .refs
        .get("definition")
        .context("B lacks definition relationship")?;
    let interpretation = binding
        .refs
        .get("interpretation")
        .context("B lacks interpretation relationship")?;
    let d = mv_document(properties, definition, "definition", 1)?;
    ensure!(d.refs.is_empty(), "D has unexpected relationships");
    let l = mv_document(properties, interpretation, "interpretation", 2)?;
    ensure!(
        l.refs.len() == 1 && l.refs.get("definition") == Some(definition),
        "L has inconsistent definition relationship"
    );
    ensure!(
        wire::interpretation_definition(&l.content)? == *definition,
        "L embeds another definition identity"
    );
    let formats = wire::binding(&binding.content)?;
    ensure!(
        formats.len() == wire::definition_occurrences(&d.content)?,
        "B/D occurrence count differs"
    );
    Ok(BindingFacts {
        definition: definition.clone(),
        interpretation: interpretation.clone(),
        formats,
    })
}
fn summary_number(summary: &Value, key: &str) -> Result<u64> {
    summary[key]
        .as_str()
        .with_context(|| format!("summary lacks {key}"))?
        .parse()
        .with_context(|| format!("invalid summary {key}"))
}
fn verify_graph(response: &Value, expectation: &GraphExpectation<'_>) -> Result<String> {
    let metadata = response
        .get("metadata")
        .context("REST response lacks metadata")?;
    let properties = wire::properties(&metadata["properties"])?;
    let root = properties
        .roots
        .get("novarocks.mv")
        .context("MV has no binding root")?;
    let current_binding = binding_facts(&properties, root)?;
    for id in properties.roots.values() {
        wire::closure(&properties, id)?;
    }
    let configuration = properties
        .slots
        .get("novarocks.mv~configuration")
        .context("MV lacks configuration slot")?;
    ensure!(
        configuration.format
            == wire::Format {
                owner: "novarocks.mv".into(),
                name: "configuration".into(),
                version: 1
            },
        "wrong C format"
    );
    ensure!(
        !configuration.content.is_empty(),
        "empty configuration payload"
    );
    let management = properties
        .slots
        .get("novarocks.storage~management")
        .context("MV lacks management slot")?;
    ensure!(
        management.format
            == wire::Format {
                owner: "novarocks.mv".into(),
                name: "management".into(),
                version: 1
            },
        "wrong management slot format"
    );
    wire::management(&management.content)?;
    let snapshots = metadata["snapshots"]
        .as_array()
        .context("REST metadata lacks snapshots")?;
    ensure!(
        snapshots.len() == expectation.publications + expectation.slot_changes,
        "retained snapshots {} differ from publications {} + slot changes {}",
        snapshots.len(),
        expectation.publications,
        expectation.slot_changes
    );
    let mut by_id = BTreeMap::new();
    let mut by_sequence = BTreeMap::new();
    for snapshot in snapshots {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .context("snapshot lacks ID")?;
        let sequence = snapshot["sequence-number"]
            .as_u64()
            .context("snapshot lacks sequence number")?;
        ensure!(
            by_id.insert(id, snapshot).is_none()
                && by_sequence.insert(sequence, snapshot).is_none(),
            "duplicate snapshot ID/sequence"
        );
    }
    let current = metadata["current-snapshot-id"]
        .as_i64()
        .filter(|v| *v != -1);
    ensure!(
        current.is_some() == !snapshots.is_empty(),
        "current snapshot presence differs from retained fixture snapshots"
    );
    if let Some(id) = current {
        ensure!(by_id.contains_key(&id), "current snapshot is not retained");
        ensure!(
            metadata["refs"]["main"]["snapshot-id"].as_i64() == Some(id),
            "current and main ref differ"
        );
    }
    let mut publications = BTreeMap::<[u8; 16], String>::new();
    let mut slots = 0;
    let mut slot_ids = BTreeSet::new();
    let mut interpretations = BTreeSet::new();
    let mut last_publication = None;
    for snapshot in by_sequence.values() {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .context("snapshot lacks ID")?;
        let records = wire::records(&snapshot["summary"])?;
        for frame in records.values() {
            if let Some(root) = wire::record(frame)?.binding {
                wire::closure(&properties, &root)?;
            }
        }
        let parent = snapshot["parent-snapshot-id"]
            .as_i64()
            .and_then(|id| by_id.get(&id).copied());
        let operation = snapshot["summary"]["operation"]
            .as_str()
            .context("snapshot lacks operation")?;
        ensure!(
            matches!(operation, "append" | "overwrite" | "delete" | "replace"),
            "unknown Iceberg operation"
        );
        let mut files_changed = false;
        // Iceberg omits zero delta counters. A present malformed counter is
        // never treated as zero or an unknown successful observation.
        for key in [
            "added-data-files",
            "deleted-data-files",
            "added-delete-files",
            "removed-delete-files",
        ] {
            if !snapshot["summary"][key].is_null() {
                files_changed |= summary_number(&snapshot["summary"], key)? > 0;
            }
        }
        let frame = records.get(wire::PUBLICATION_KEY);
        let mut is_publication = false;
        if let Some(frame) = frame {
            let record = wire::record(frame)?;
            let binding = record.binding.context("MV P is unbound")?;
            let facts = binding_facts(&properties, &binding)?;
            let p = wire::publication(&record.payload, &facts.formats)?;
            ensure!(
                facts.definition == current_binding.definition,
                "historical P changed D unexpectedly"
            );
            interpretations.insert(facts.interpretation);
            if !expectation.interpretation_changes {
                ensure!(
                    binding == *root,
                    "ordinary publications do not share one B/D/L"
                );
            }
            if let Some(previous) = publications.get(&p.id) {
                ensure!(
                    previous == frame,
                    "publication ID reused with different bytes"
                );
            } else {
                publications.insert(p.id, frame.clone());
                is_publication = true;
            }
            if Some(id) == current {
                if let Some(minimum) = expectation.delete_files_min {
                    ensure!(
                        summary_number(&snapshot["summary"], "total-delete-files")? >= minimum,
                        "target did not retain the required DV/delete files"
                    );
                }
                ensure!(binding == *root, "current P and root differ");
                if let Some(rows) = expectation.logical_rows {
                    ensure!(
                        p.rows == Some(rows),
                        "logical P result rows {:?}, expected {rows}",
                        p.rows
                    );
                }
                if let Some(total) = expectation.total_records {
                    ensure!(
                        summary_number(&snapshot["summary"], "total-records")? == total,
                        "physical total-records differs"
                    );
                }
            }
        }
        if is_publication {
            ensure!(
                operation != "replace",
                "new MV publication is mislabeled replace"
            );
            ensure!(
                files_changed || operation == "append",
                "zero-file publication is not append"
            );
            last_publication = Some(*snapshot);
        } else {
            // These isolated fixtures contain only publications and explicit
            // slot changes. Maintenance carries are tested by their own owner.
            slots += 1;
            slot_ids.insert(id);
            if Some(id) == current {
                ensure!(
                    [configuration, management]
                        .iter()
                        .any(|slot| slot.writer == Some(id)),
                    "current slot snapshot has no exact slot writer"
                );
            }
            if let Some(parent) = parent {
                ensure!(operation == "replace", "slot with parent is not replace");
                ensure!(
                    records == wire::records(&parent["summary"])?,
                    "slot did not carry all parent record bytes"
                );
                for key in ["total-data-files", "total-delete-files", "total-records"] {
                    ensure!(
                        snapshot["summary"][key] == parent["summary"][key],
                        "slot changed parent {key}"
                    );
                }
            } else {
                ensure!(
                    snapshot["parent-snapshot-id"].is_null(),
                    "slot parent is not retained"
                );
                ensure!(
                    operation == "append" && records.is_empty(),
                    "first slot must be empty append"
                );
                for key in ["total-data-files", "total-records"] {
                    ensure!(
                        summary_number(&snapshot["summary"], key)? == 0,
                        "first slot has nonzero {key}"
                    );
                }
            }
        }
    }
    ensure!(
        publications.len() == expectation.publications,
        "unique publications {}, expected {}",
        publications.len(),
        expectation.publications
    );
    if expectation.slot_changes > 0 {
        ensure!(
            [configuration, management]
                .iter()
                .any(|slot| slot.writer.is_some()),
            "slot snapshots exist but both slots have initial writer"
        );
    }
    ensure!(
        slots == expectation.slot_changes,
        "slot snapshots {slots}, expected {}",
        expectation.slot_changes
    );
    ensure!(
        !expectation.interpretation_changes || interpretations.len() == 2,
        "repartition did not retain two resolvable L versions"
    );
    for slot in [configuration, management] {
        if let Some(writer) = slot.writer
            && let Some(snapshot) = by_id.get(&writer)
        {
            ensure!(
                slot_ids.contains(&writer),
                "slot writer identifies a publication rather than slot snapshot"
            );
            let records = wire::records(&snapshot["summary"])?;
            let parent = snapshot["parent-snapshot-id"]
                .as_i64()
                .and_then(|id| by_id.get(&id));
            ensure!(
                parent.is_none_or(|p| wire::records(&p["summary"]).is_ok_and(|r| r == records)),
                "slot writer does not identify a carrying snapshot"
            );
        }
    }
    if let Some(last) = last_publication {
        let summary = &last["summary"];
        if expectation.metadata_only_last {
            for key in ["added-delete-files", "removed-delete-files"] {
                ensure!(
                    summary[key].is_null() || summary_number(summary, key)? == 0,
                    "metadata-only publication changed delete files"
                );
            }
            ensure!(
                summary_number(summary, "added-data-files")? == 0
                    && summary_number(summary, "added-records")? == 0
                    && (summary["deleted-data-files"].is_null()
                        || summary_number(summary, "deleted-data-files")? == 0),
                "metadata-only publication changed files/records"
            );
        }
        if expectation.full_overwrite_last {
            ensure!(
                summary["operation"] == "overwrite",
                "FULL publication is not overwrite"
            );
        }
        if expectation.append_last {
            ensure!(
                summary["operation"] == "append"
                    && summary_number(summary, "added-data-files")? > 0,
                "incremental publication did not append data files"
            );
        }
    }
    Ok(format!(
        "{} publications, {slots} slot snapshots resolve current B/D/L; private-bytes={}; current={current:?}; no document files",
        publications.len(),
        properties.private_bytes
    ))
}
fn s3_bucket_and_key(location: &str) -> Result<(&str, &str)> {
    let path = location
        .strip_prefix("s3://")
        .with_context(|| format!("oracle location is not S3: {location}"))?;
    let (bucket, key) = path
        .split_once('/')
        .context("S3 oracle location lacks key")?;
    ensure!(
        !bucket.is_empty() && !key.is_empty(),
        "invalid oracle S3 location"
    );
    Ok((bucket, key))
}
fn document_prefixes<'a>(
    metadata: &'a Value,
    warehouse: &'a str,
) -> Result<[(&'a str, String); 2]> {
    let location = metadata["location"]
        .as_str()
        .context("REST metadata lacks table location")?;
    let (bucket, key) = s3_bucket_and_key(location)?;
    let (warehouse_bucket, warehouse_key) = s3_bucket_and_key(warehouse)?;
    ensure!(
        bucket == warehouse_bucket
            && key.starts_with(&format!("{}/", warehouse_key.trim_end_matches('/'))),
        "table is outside isolated warehouse"
    );
    Ok([
        (
            bucket,
            format!(
                "{}/metadata/novarocks-documents/",
                key.trim_end_matches('/')
            ),
        ),
        (
            warehouse_bucket,
            format!(
                "{}/_novarocks/document-staging/",
                warehouse_key.trim_end_matches('/')
            ),
        ),
    ])
}
fn verify_no_document_files(metadata: &Value) -> Result<()> {
    let warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .context("isolated warehouse URI unavailable")?;
    let endpoint = std::env::var("AWS_S3_ENDPOINT").context("MinIO endpoint unavailable")?;
    let access = std::env::var("AWS_S3_ACCESS_KEY_ID").context("MinIO access key unavailable")?;
    let secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY").context("MinIO secret unavailable")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let storage = FixtureObjectStore::new(endpoint, access, secret)?;
    for (bucket, prefix) in document_prefixes(metadata, &warehouse)? {
        let location = format!("s3://{bucket}/{prefix}");
        let has_files = runtime
            .block_on(async {
                let cancellation = FileCancellation::new();
                let access = storage.resolve(&location, &cancellation).await?;
                storage.has_files(&access, &location, &cancellation).await
            })
            .with_context(|| format!("LIST document prefix s3://{bucket}/{prefix}"))?;
        ensure!(
            !has_files,
            "unexpected document object below s3://{bucket}/{prefix}"
        );
    }
    Ok(())
}
