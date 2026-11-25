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
    full_refreshes: usize,
    logical_rows: Option<u64>,
    total_records: Option<u64>,
    delete_files_min: Option<u64>,
    metadata_only_last: bool,
    full_refresh_last: bool,
    append_last: bool,
    interpretation_changes: bool,
    binding_changes: Option<usize>,
    marker: Option<MarkerExpectation>,
    source: Option<&'a str>,
    decimal_mapping: Option<&'a str>,
}

#[derive(Clone, Copy)]
enum MarkerExpectation {
    Clear,
    InvalidDefinition(wire::MarkerScope),
}

/// Read the product's own REST metadata while the runner-owned isolated
/// catalog and native cluster are still alive. SQL result rows alone cannot
/// prove the retained P -> B -> D/L/M graph or the absence of document files.
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
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("read exact MV REST metadata at {url}"))?
        .error_for_status()
        .with_context(|| format!("REST did not return MV metadata at {url}"))?;
    let value: Value = response.json().context("decode MV REST metadata")?;
    let summary = verify_graph(&value, &expectation)?;
    if let Some(source) = expectation.source {
        let source_url = format!(
            "{}/v1/namespaces/{namespace}/tables/{source}",
            rest.trim_end_matches('/')
        );
        let source_metadata: Value = client.get(source_url).send()?.error_for_status()?.json()?;
        verify_exact_source(
            &value["metadata"],
            namespace,
            source,
            &source_metadata["metadata"],
        )?;
    }
    verify_no_staging_files(
        &value["metadata"],
        expectation.publications == 0 && expectation.slot_changes == 0,
    )?;
    Ok(summary)
}

pub(crate) fn parse_expectation(directive: &str) -> Result<GraphExpectation<'_>> {
    let (target, parameters) = directive
        .split_once(",publications=")
        .context("@mv_rest_document_graph requires <namespace>.<table>,publications=<count>")?;
    let mut parameters = parameters.split(',');
    let count = parameters.next().context("missing publication count")?;
    let mut metadata_only_last = false;
    let mut full_refresh_last = false;
    let mut append_last = false;
    let mut interpretation_changes = false;
    let mut binding_changes = None;
    let mut marker = None;
    let mut source = None;
    let mut decimal_mapping = None;
    let mut table_commits = None;
    let mut failed_table_commits = None;
    let mut slot_changes = None;
    let mut full_refreshes = None;
    let mut logical_rows = None;
    let mut total_records = None;
    let mut delete_files_min = None;
    for parameter in parameters {
        match parameter {
            "metadata-only-last=true" if !metadata_only_last => metadata_only_last = true,
            "full-refresh-last=true" if !full_refresh_last => full_refresh_last = true,
            "append-last=true" if !append_last => append_last = true,
            "interpretation-changes=true" if !interpretation_changes => {
                interpretation_changes = true
            }
            "markers=clear" if marker.is_none() => marker = Some(MarkerExpectation::Clear),
            "markers=invalid-definition-binding" if marker.is_none() => {
                marker = Some(MarkerExpectation::InvalidDefinition(
                    wire::MarkerScope::Binding,
                ))
            }
            "markers=invalid-definition-publication" if marker.is_none() => {
                marker = Some(MarkerExpectation::InvalidDefinition(
                    wire::MarkerScope::Publication,
                ))
            }
            value if value.starts_with("source=") && source.is_none() => {
                source = Some(rest_identifier(&value["source=".len()..])?);
            }
            value if value.starts_with("decimal-mapping=") && decimal_mapping.is_none() => {
                decimal_mapping = Some(rest_identifier(&value["decimal-mapping=".len()..])?);
            }
            value if value.starts_with("binding-changes=") && binding_changes.is_none() => {
                binding_changes = Some(
                    value["binding-changes=".len()..]
                        .parse::<usize>()
                        .context("invalid binding-changes count")?,
                );
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
            value if value.starts_with("full-refreshes=") && full_refreshes.is_none() => {
                full_refreshes = Some(
                    value["full-refreshes=".len()..]
                        .parse::<usize>()
                        .context("invalid full-refreshes count")?,
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
    let full_refreshes = full_refreshes.unwrap_or_default();
    let publications = count
        .parse::<usize>()
        .context("invalid publication count")?;
    ensure!(
        !metadata_only_last || publications >= 2,
        "metadata-only-last requires at least two publications"
    );
    ensure!(
        full_refreshes <= publications && (!full_refresh_last || full_refreshes > 0),
        "full refresh count and last-publication shape must agree with the publication count"
    );
    ensure!(
        !append_last || publications >= 2,
        "append-last requires at least two publications"
    );
    ensure!(
        binding_changes.is_none_or(|changes| changes > 0 && changes < publications),
        "binding changes must be supported by consecutive retained publications"
    );
    ensure!(
        !interpretation_changes || publications == 2,
        "interpretation-changes currently requires exactly two publications"
    );
    ensure!(
        usize::from(metadata_only_last) + usize::from(full_refresh_last) + usize::from(append_last)
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
        full_refreshes,
        logical_rows,
        total_records,
        delete_files_min,
        metadata_only_last,
        full_refresh_last,
        append_last,
        interpretation_changes,
        binding_changes,
        marker,
        source,
        decimal_mapping,
    })
}

fn rest_identifier(input: &str) -> Result<&str> {
    ensure!(
        !input.is_empty()
            && input
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_'),
        "invalid REST identifier {input}"
    );
    Ok(input)
}

fn verify_exact_source(
    metadata: &Value,
    namespace: &str,
    name: &str,
    source: &Value,
) -> Result<()> {
    let current = metadata["current-snapshot-id"]
        .as_i64()
        .context("MV has no Current P")?;
    let snapshot = metadata["snapshots"]
        .as_array()
        .context("MV lacks snapshot list")?
        .iter()
        .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(current))
        .context("Current snapshot missing from history")?;
    let frame = snapshot["summary"][wire::PUBLICATION_KEY]
        .as_str()
        .context("Current lacks P")?;
    let record = wire::record(frame)?;
    let binding_id = record.binding.context("P lacks B")?;
    let properties = wire::properties(&metadata["properties"])?;
    let binding = mv_document(&properties, &binding_id, "binding", 2)?;
    let captured = wire::binding(&binding.content)?;
    let [occurrence] = captured.occurrences.as_slice() else {
        anyhow::bail!("source assertion requires one actual B occurrence");
    };
    let [format] = captured.formats.as_slice() else {
        anyhow::bail!("source assertion requires one declared watermark");
    };
    ensure!(
        occurrence.namespace == namespace
            && occurrence.table == name
            && format.provider == "iceberg"
            && format.name == "connector-snapshot-id"
            && format.version == 1
            && format.maximum == 9,
        "P/B points at a different source locator or format"
    );
    let uuid = source["table-uuid"]
        .as_str()
        .context("source REST table has no UUID")?;
    ensure!(
        wire::iceberg_object_identity(occurrence.object)? == uuid.as_bytes(),
        "P/B points at another Iceberg source object"
    );
    let snapshot = source["current-snapshot-id"]
        .as_i64()
        .filter(|id| *id != -1)
        .context("source REST table has no Current snapshot")?;
    let mut expected = vec![1];
    expected.extend_from_slice(&snapshot.to_be_bytes());
    ensure!(
        wire::publication(&record.payload, &captured.formats)?.watermarks == [expected],
        "P does not hold the actual Current source snapshot"
    );
    Ok(())
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
    let binding = mv_document(properties, id, "binding", 2)?;
    ensure!(binding.refs.len() == 3, "B must reference exactly D/L/M");
    let definition = binding
        .refs
        .get("definition")
        .context("B lacks definition relationship")?;
    let interpretation = binding
        .refs
        .get("interpretation")
        .context("B lacks interpretation relationship")?;
    let d = mv_document(properties, definition, "definition", 2)?;
    ensure!(d.refs.is_empty(), "D has unexpected relationships");
    let l = mv_document(properties, interpretation, "interpretation", 3)?;
    ensure!(
        l.refs.len() == 1 && l.refs.get("definition") == Some(definition),
        "L has inconsistent definition relationship"
    );
    ensure!(
        wire::interpretation_definition(&l.content)? == *definition,
        "L embeds another definition identity"
    );
    let mapping = binding
        .refs
        .get("mapping")
        .context("B lacks mapping relationship")?;
    let m = mv_document(properties, mapping, "mapping", 1)?;
    ensure!(m.refs.is_empty(), "M has unexpected relationships");
    wire::definition(&d.content)?;
    let binding = wire::binding(&binding.content)?;
    wire::mapping(&m.content, &binding.outputs)?;
    Ok(BindingFacts {
        definition: definition.clone(),
        interpretation: interpretation.clone(),
        formats: binding.formats,
    })
}
fn verify_decimal_mapping(
    metadata: &Value,
    properties: &wire::Properties,
    root: &str,
    name: &str,
) -> Result<()> {
    let binding = mv_document(properties, root, "binding", 2)?;
    let mapping = mv_document(properties, &binding.refs["mapping"], "mapping", 1)?;
    let binding = wire::binding(&binding.content)?;
    let columns = wire::mapping(&mapping.content, &binding.outputs)?;
    let (output, column) = binding
        .outputs
        .iter()
        .zip(&columns)
        .find(|(output, _)| output.name == name)
        .context("B/M has no requested decimal output")?;
    let schema = metadata["schemas"]
        .as_array()
        .context("REST metadata has no schemas")?
        .iter()
        .find(|schema| schema["schema-id"] == metadata["current-schema-id"])
        .context("REST metadata has no current schema")?;
    let field = schema["fields"]
        .as_array()
        .context("REST schema has no fields")?
        .iter()
        .find(|field| field["name"].as_str() == Some(name))
        .context("REST schema has no requested decimal field")?;
    wire::assert_decimal_mapping(
        column,
        output,
        field,
        metadata["format-version"]
            .as_u64()
            .context("REST metadata lacks storage format version")?,
    )
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
    if let Some(name) = expectation.decimal_mapping {
        verify_decimal_mapping(metadata, &properties, root, name)?;
    }
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
        snapshots.len()
            == expectation.publications + expectation.slot_changes + expectation.full_refreshes,
        "retained snapshots {} differ from publications {} + slot changes {} + FULL deletes {}",
        snapshots.len(),
        expectation.publications,
        expectation.slot_changes,
        expectation.full_refreshes,
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
    let (full_deletes, full_appends) =
        full_refresh_pairs(&by_sequence, metadata["table-uuid"].as_str())?;
    ensure!(
        full_appends.len() == expectation.full_refreshes,
        "FULL pairs {}, expected {}",
        full_appends.len(),
        expectation.full_refreshes
    );
    let mut publications = BTreeMap::<[u8; 16], String>::new();
    let mut slots = 0;
    let mut slot_ids = BTreeSet::new();
    let mut interpretations = BTreeSet::new();
    let markers = wire::markers(&properties)?;
    let mut active_publication = None;
    let mut ordered_bindings = Vec::new();
    let mut last_publication = None;
    for snapshot in by_sequence.values() {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .context("snapshot lacks ID")?;
        if full_deletes.contains(&id) {
            continue;
        }
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
            if !expectation.interpretation_changes && expectation.binding_changes.is_none() {
                ensure!(
                    binding == *root,
                    "ordinary publications do not share one B/D/L/M"
                );
            }
            if let Some(previous) = publications.get(&p.id) {
                ensure!(
                    previous == frame,
                    "publication ID reused with different bytes"
                );
            } else {
                publications.insert(p.id, frame.clone());
                ordered_bindings.push(binding.clone());
                is_publication = true;
            }
            if full_appends.contains(&id) {
                let total = summary_number(&snapshot["summary"], "total-records")?;
                ensure!(
                    is_publication && p.rows == Some(total),
                    "FULL append must carry a new P with exact physical and logical rows"
                );
                if total > 0 {
                    ensure!(
                        snapshot["first-row-id"].as_u64().is_some()
                            && snapshot["added-rows"].as_u64() == Some(total),
                        "FULL append must introduce lineage for every rebuilt row"
                    );
                }
            }
            if Some(id) == current {
                active_publication = Some(p.id);
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
    if let Some(changes) = expectation.binding_changes {
        let actual = ordered_bindings
            .windows(2)
            .filter(|pair| pair[0] != pair[1])
            .count();
        ensure!(
            actual == changes,
            "binding changed {actual} times, expected {changes}"
        );
    }
    if let Some(expected) = expectation.marker {
        match expected {
            MarkerExpectation::Clear => ensure!(markers.is_empty(), "MV retains marker properties"),
            MarkerExpectation::InvalidDefinition(scope) => {
                let expected_identity = match scope {
                    wire::MarkerScope::Publication => active_publication
                        .context("publication-scoped marker requires Current P")?
                        .to_vec(),
                    wire::MarkerScope::Binding => {
                        ensure!(
                            active_publication.is_none(),
                            "B-scoped marker requires an unpublished target"
                        );
                        wire::decode_identity(root)?
                    }
                };
                ensure!(
                    matches!(markers.as_slice(), [marker] if marker.name == "invalid-definition"
                        && marker.scope == scope && marker.identity == expected_identity),
                    "MV lacks the single expected invalid-definition marker with its exact scope"
                );
            }
        }
    }
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
        if expectation.full_refresh_last {
            ensure!(
                last["snapshot-id"]
                    .as_i64()
                    .is_some_and(|id| full_appends.contains(&id)),
                "last publication is not a FULL delete/append pair"
            );
        }
        if expectation.append_last {
            ensure!(
                summary["operation"] == "append"
                    && last["snapshot-id"]
                        .as_i64()
                        .is_some_and(|id| !full_appends.contains(&id))
                    && summary_number(summary, "added-data-files")? > 0,
                "incremental publication did not append data files"
            );
        }
    }
    Ok(format!(
        "{} publications, {slots} slot snapshots resolve current B/D/L/M; private-bytes={}; current={current:?}; no document files",
        publications.len(),
        properties.private_bytes
    ))
}
const FULL_REFRESH_STAGE: &str = "novarocks.mv.full-refresh-stage.v1";

fn full_refresh_pairs(
    snapshots: &BTreeMap<u64, &Value>,
    table_uuid: Option<&str>,
) -> Result<(BTreeSet<i64>, BTreeSet<i64>)> {
    const MARKER: &str = "novarocks.write.session.v1";
    let mut deletes = BTreeSet::new();
    let mut appends = BTreeSet::new();
    let mut previous_id = None;
    for (&sequence, snapshot) in snapshots {
        let id = snapshot["snapshot-id"]
            .as_i64()
            .context("snapshot lacks ID")?;
        let summary = &snapshot["summary"];
        if !summary[FULL_REFRESH_STAGE].is_null()
            || (summary["operation"] == "delete" && summary[wire::PUBLICATION_KEY].is_null())
        {
            ensure!(
                summary["operation"] == "delete" && summary[wire::PUBLICATION_KEY].is_null(),
                "FULL delete stage contains a publication record or has a different operation"
            );
            ensure!(
                previous_id == snapshot["parent-snapshot-id"].as_i64(),
                "FULL delete did not extend the retained prior head"
            );
            ensure!(
                summary.as_object().is_some_and(|summary| summary
                    .keys()
                    .all(|key| key == FULL_REFRESH_STAGE || !key.starts_with("novarocks."))),
                "FULL delete summary contains a private field other than its stage witness"
            );
            for key in ["total-records", "total-data-files", "total-delete-files"] {
                ensure!(
                    summary_number(summary, key)? == 0,
                    "FULL delete has nonzero {key}"
                );
            }
            let append = sequence
                .checked_add(1)
                .and_then(|next| snapshots.get(&next))
                .context("FULL delete lacks a consecutive append")?;
            let append_summary = &append["summary"];
            let marker = append_summary[MARKER]
                .as_str()
                .filter(|marker| !marker.is_empty())
                .context("FULL append lacks a write session marker")?;
            ensure!(
                append["parent-snapshot-id"].as_i64() == Some(id)
                    && append_summary["operation"] == "append"
                    && append_summary[wire::PUBLICATION_KEY].is_string()
                    && snapshots
                        .values()
                        .filter(|s| s["summary"][MARKER].as_str() == Some(marker))
                        .count()
                        == 1,
                "FULL append did not extend its delete with one unique marked publication"
            );
            let witness: Value = serde_json::from_str(
                summary[FULL_REFRESH_STAGE]
                    .as_str()
                    .context("FULL delete lacks its stage witness")?,
            )
            .context("FULL delete stage witness is invalid JSON")?;
            let operation: [u8; 16] = serde_json::from_value(witness["operation_id"].clone())
                .context("FULL delete stage witness has no operation identity")?;
            let operation_hex: String =
                operation.iter().map(|byte| format!("{byte:02x}")).collect();
            let nonce: [u8; 16] = serde_json::from_value(witness["attempt_nonce"].clone())
                .context("FULL delete stage witness has no attempt nonce")?;
            ensure!(
                witness["version"] == 1
                    && witness.as_object().is_some_and(|fields| fields.len() == 8)
                    && witness["table_uuid"].as_str() == table_uuid
                    && table_uuid.is_some()
                    && witness["asserted_parent"] == snapshot["parent-snapshot-id"]
                    && witness["delete_snapshot"] == snapshot["snapshot-id"]
                    && witness["append_snapshot"] == append["snapshot-id"]
                    && u32::try_from(witness["attempt_ordinal"].as_u64().unwrap_or(u64::MAX))
                        .is_ok()
                    && nonce[6] >> 4 == 7
                    && nonce[8] >> 6 == 2
                    && operation_hex == marker.replace('-', ""),
                "FULL delete stage does not bind the exact table, parent, session and selected pair"
            );
            deletes.insert(id);
            appends.insert(
                append["snapshot-id"]
                    .as_i64()
                    .context("FULL append lacks ID")?,
            );
        }
        previous_id = Some(id);
    }
    Ok((deletes, appends))
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
fn verify_no_staging_files(metadata: &Value, verify_managed_create: bool) -> Result<()> {
    let warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .context("isolated warehouse URI unavailable")?;
    let endpoint = std::env::var("AWS_S3_ENDPOINT").context("MinIO endpoint unavailable")?;
    let access = std::env::var("AWS_S3_ACCESS_KEY_ID").context("MinIO access key unavailable")?;
    let secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY").context("MinIO secret unavailable")?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let storage = FixtureObjectStore::new(endpoint, access, secret)?;
    let mut prefixes = document_prefixes(metadata, &warehouse)?.to_vec();
    if verify_managed_create {
        let location = metadata["location"]
            .as_str()
            .context("REST metadata lacks table location")?;
        let (bucket, key) = s3_bucket_and_key(location)?;
        let (warehouse_bucket, warehouse_key) = s3_bucket_and_key(&warehouse)?;
        prefixes.extend([
            (
                bucket,
                format!("{}/data/_staging/", key.trim_end_matches('/')),
            ),
            (
                warehouse_bucket,
                format!(
                    "{}/_novarocks/ctas-staging/v1/",
                    warehouse_key.trim_end_matches('/')
                ),
            ),
        ]);
    }
    for (bucket, prefix) in prefixes {
        let location = format!("s3://{bucket}/{prefix}");
        let has_files = runtime
            .block_on(async {
                let cancellation = FileCancellation::new();
                let access = storage.resolve(&location, &cancellation).await?;
                storage.has_files(&access, &location, &cancellation).await
            })
            .with_context(|| format!("LIST staging prefix s3://{bucket}/{prefix}"))?;
        ensure!(
            !has_files,
            "unexpected object below staging prefix s3://{bucket}/{prefix}"
        );
    }
    Ok(())
}
