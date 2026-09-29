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

//! Independent comparison against frozen Java planFiles receipts.
//!
//! The oracle reads immutable fixture objects and never changes them. Its
//! interval/null/NaN proof is separate from the provider's selection predicate.
//! The receipt's legacy partition spelling is accepted only for the named
//! empty/one-integer-p or partition_key fixture shapes; it is not a general partition codec.

use anyhow::{Context, Result, bail, ensure};
use novarocks_connector_iceberg::delete_semantics::{
    DeleteContentAddress, DeleteFact, DeleteFormat, DeleteKind, TypedPartition,
};
use novarocks_connector_iceberg::iceberg::TableIdent;
use novarocks_connector_iceberg::iceberg::io::FileIO;
use novarocks_connector_iceberg::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, FormatVersion, Literal, ManifestStatus,
    PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Struct, TableMetadata, Type,
};
use novarocks_connector_iceberg::iceberg::table::Table;
use novarocks_connector_iceberg::read_snapshot::build_read_snapshot_at;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const FILE_PATH_ID: i32 = i32::MAX - 101;

#[derive(Debug, Serialize)]
pub struct ClosureOracleSummary {
    pub cases_verified: usize,
    pub planning_errors_matched: usize,
    pub data_files_verified: usize,
    pub logical_members_verified: usize,
    pub load_members_verified: usize,
    pub safe_statistics_differences: usize,
    pub artifacts_verified: usize,
    pub artifact_bytes_verified: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct Descriptor {
    path: String,
    format: String,
    kind: String,
    offset: Option<i64>,
    length: Option<i64>,
    sequence: i64,
    spec: i32,
    typed_partition: String,
    fields: Vec<i32>,
    field_types: Vec<String>,
    target: Option<String>,
    record_count: u64,
    file_size: u64,
    key_metadata_sha256: String,
}

#[derive(Clone)]
struct Observed {
    descriptor: Descriptor,
    file: DataFile,
}

/// Validate the existing receipt against exact bytes, then compare every file.
/// `file_io` is configured by the scenario owner; credentials never enter output.
pub async fn verify_receipt(
    receipt_path: &Path,
    file_io: FileIO,
    output_dir: &Path,
) -> Result<ClosureOracleSummary> {
    let receipt_bytes = std::fs::read(receipt_path).context("read Java oracle receipt")?;
    let receipt: Value =
        serde_json::from_slice(&receipt_bytes).context("parse Java oracle receipt")?;
    verify_inputs(
        array(&receipt, "artifacts")?,
        array(&receipt, "cases")?,
        file_io,
        output_dir,
        json!({"receipt_sha256":format!("{:x}",Sha256::digest(receipt_bytes))}),
    )
    .await
}

/// Normalize the frozen scale JSONL dictionaries without rewriting the receipt.
/// All observations, including both streaming-window endpoints, are checked.
pub async fn verify_scale_receipt(
    receipt_path: &Path,
    file_io: FileIO,
    output_dir: &Path,
) -> Result<ClosureOracleSummary> {
    let receipt_bytes = std::fs::read(receipt_path).context("read scale oracle receipt")?;
    let receipt: Value = serde_json::from_slice(&receipt_bytes)?;
    let parent = receipt_path
        .parent()
        .context("scale receipt has no directory")?;
    let mut sources = BTreeMap::from([(
        "receipt".to_string(),
        format!("{:x}", Sha256::digest(&receipt_bytes)),
    )]);
    let artifacts = read_sidecar(parent, &receipt, "artifacts", &mut sources)?;
    let plans = read_sidecar(parent, &receipt, "java_plans", &mut sources)?;
    let members = read_sidecar(parent, &receipt, "java_delete_members", &mut sources)?;
    let cases = normalize_scale_cases(&receipt, plans, members)?;
    ensure!(
        artifacts.len() as u64 == uint(&receipt, "artifact_count")?,
        "scale artifact count differs from receipt"
    );
    verify_inputs(
        &artifacts,
        &cases,
        file_io,
        output_dir,
        json!({"source_sha256":sources,"normalization":"in-memory scale dictionary projection; original bytes unchanged"}),
    )
    .await
}

fn read_sidecar(
    parent: &Path,
    receipt: &Value,
    field: &str,
    sources: &mut BTreeMap<String, String>,
) -> Result<Vec<Value>> {
    let name = string(receipt, field)?;
    ensure!(
        Path::new(name).components().count() == 1,
        "scale sidecar must be a sibling filename"
    );
    let bytes =
        std::fs::read(parent.join(name)).with_context(|| format!("read scale sidecar {name}"))?;
    sources.insert(name.to_string(), format!("{:x}", Sha256::digest(&bytes)));
    std::str::from_utf8(&bytes)?
        .lines()
        .enumerate()
        .map(|(line, text)| {
            serde_json::from_str(text)
                .with_context(|| format!("parse scale sidecar {name} line {}", line + 1))
        })
        .collect()
}

fn normalize_scale_cases(
    receipt: &Value,
    plans: Vec<Value>,
    members: Vec<Value>,
) -> Result<Vec<Value>> {
    let mut dictionaries = BTreeMap::<String, serde_json::Map<String, Value>>::new();
    for member in members {
        ensure!(
            string(&member, "record")? == "scale-delete-member",
            "unexpected scale member record"
        );
        let name = string(&member, "case")?.to_string();
        let id = uint(&member, "member_id")?.to_string();
        ensure!(
            member["content"].is_object(),
            "scale member has no content descriptor"
        );
        ensure!(
            dictionaries
                .entry(name)
                .or_default()
                .insert(id, member["content"].clone())
                .is_none(),
            "duplicate scale delete member id"
        );
    }
    let mut tasks = BTreeMap::<String, BTreeMap<u64, Value>>::new();
    for plan in plans {
        ensure!(
            string(&plan, "record")? == "scale-plan-file",
            "unexpected scale plan record"
        );
        let name = string(&plan, "case")?.to_string();
        let ordinal = uint(&plan, "data_ordinal")?;
        let ids = array(&plan, "delete_member_ids")?;
        ensure!(
            ids.len() as u64 == uint(&plan, "delete_list_size")?,
            "scale member list size differs"
        );
        let mut unique = BTreeSet::new();
        for id in ids {
            let id = id.as_u64().context("scale member id is not unsigned")?;
            ensure!(unique.insert(id), "scale task repeats a member id");
            ensure!(
                dictionaries
                    .get(&name)
                    .is_some_and(|dictionary| dictionary.contains_key(&id.to_string())),
                "scale task references a missing dictionary member"
            );
        }
        ensure!(
            tasks
                .entry(name)
                .or_default()
                .insert(ordinal, plan)
                .is_none(),
            "duplicate scale data ordinal"
        );
    }
    let raw_cases = array(receipt, "cases")?;
    ensure!(
        raw_cases.len() as u64 == uint(receipt, "case_count")?,
        "scale case count differs from receipt"
    );
    let mut names = BTreeSet::new();
    let mut normalized = Vec::new();
    for case in raw_cases {
        let name = string(case, "case")?;
        ensure!(names.insert(name), "duplicate scale case");
        ensure!(
            string(case, "java_planFiles")? == "success",
            "scale case did not plan successfully"
        );
        let plans = tasks.remove(name).unwrap_or_default();
        ensure!(
            plans.len() as u64 == uint(case, "plan_file_count")?,
            "scale task count differs"
        );
        ensure!(
            plans.keys().copied().eq(0..plans.len() as u64),
            "scale data ordinals are not contiguous"
        );
        let dictionary = dictionaries.remove(name).unwrap_or_default();
        ensure!(
            dictionary.len() as u64 == uint(case, "delete_dictionary_size")?,
            "scale dictionary count differs"
        );
        let mut normalized_case = case.clone();
        normalized_case["java_planFiles"] = json!({
            "status":"success",
            "tasks":plans.into_values().collect::<Vec<_>>(),
            "delete_dictionary":dictionary,
        });
        normalized.push(normalized_case);
    }
    ensure!(
        tasks.is_empty() && dictionaries.is_empty(),
        "scale sidecars contain observations absent from receipt"
    );
    Ok(normalized)
}

async fn verify_inputs(
    artifacts: &[Value],
    cases: &[Value],
    file_io: FileIO,
    output_dir: &Path,
    sources: Value,
) -> Result<ClosureOracleSummary> {
    ensure!(!cases.is_empty(), "oracle receipt has no cases");
    std::fs::create_dir_all(output_dir)?;
    let frozen_io = FileIO::new_with_memory();
    let mut summary = ClosureOracleSummary {
        cases_verified: 0,
        planning_errors_matched: 0,
        data_files_verified: 0,
        logical_members_verified: 0,
        load_members_verified: 0,
        safe_statistics_differences: 0,
        artifacts_verified: 0,
        artifact_bytes_verified: 0,
    };
    let mut inventory = BTreeMap::new();
    for artifact in artifacts {
        let path = string(artifact, "path")?;
        let expected_size = uint(artifact, "size")?;
        let expected_hash = string(artifact, "sha256")?;
        if let Some(previous) =
            inventory.insert(path.to_string(), (expected_size, expected_hash.to_string()))
        {
            ensure!(
                previous == (expected_size, expected_hash.to_string()),
                "receipt assigns conflicting bytes to {path}"
            );
            continue;
        }
        let bytes = file_io
            .new_input(path)?
            .read()
            .await
            .with_context(|| format!("read immutable oracle artifact {path}"))?;
        ensure!(
            bytes.len() as u64 == expected_size,
            "artifact size mismatch: {path}"
        );
        ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == expected_hash,
            "artifact digest mismatch: {path}"
        );
        // Subsequent planning observes exactly the verified bytes, even if an
        // external fixture owner replaces an object while the oracle runs.
        frozen_io.new_output(path)?.write(bytes).await?;
        summary.artifacts_verified += 1;
        summary.artifact_bytes_verified += expected_size;
    }
    let mut case_reports = Vec::new();
    let mut failures = Vec::new();
    for case in cases {
        let name = string(case, "case")?;
        match verify_case(case, &frozen_io, &inventory, &mut summary).await {
            Ok(report) => {
                summary.cases_verified += 1;
                case_reports.push(report);
            }
            Err(error) => {
                failures.push(format!("{name}: {error:#}"));
                case_reports
                    .push(json!({"case":name,"status":"failed","error":format!("{error:#}")}));
            }
        }
    }
    std::fs::write(
        output_dir.join("closure-oracle.json"),
        serde_json::to_vec_pretty(&json!({
            "sources":sources,
            "partition_parser_scope":"frozen empty or one integer p/partition_key partition only",
            "java_metric_projection":"Iceberg 1.11 DeleteFileIndex: data=all published columns; position=DELETE_FILE_PATH; equality=equality field IDs. Complete projected inventories and values checked; raw SDK statistics retained.",
            "summary":summary,"cases":case_reports,"failures":failures,
        }))?,
    )?;
    ensure!(
        failures.is_empty(),
        "closure oracle failed: {}",
        failures.join("; ")
    );
    Ok(summary)
}

async fn verify_case(
    case: &Value,
    io: &FileIO,
    inventory: &BTreeMap<String, (u64, String)>,
    summary: &mut ClosureOracleSummary,
) -> Result<Value> {
    let name = string(case, "case")?;
    let metadata_path = string(case, "metadata")?;
    ensure!(
        inventory.contains_key(metadata_path),
        "metadata is outside the verified artifact inventory"
    );
    let metadata = TableMetadata::read_from(io, metadata_path).await?;
    let snapshot_id = int(case, "snapshot")?;
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .context("receipt snapshot missing from metadata")?;
    let schema = snapshot.schema(&metadata)?;
    if let Some(expected_schema) = case.pointer("/scan/scan_schema") {
        let expected: Schema = serde_json::from_value(expected_schema.clone())?;
        ensure!(
            expected == *schema,
            "receipt scan schema differs from public snapshot API schema"
        );
    }
    let table = Table::builder()
        .file_io(io.clone())
        .metadata(metadata.clone())
        .metadata_location(metadata_path)
        .identifier(TableIdent::from_strs(["oracle", "frozen_input"])?)
        .disable_cache()
        .build()?;
    let rust = build_read_snapshot_at(&table, snapshot_id).await;
    let java = &case["java_planFiles"];
    if string(java, "status")? == "error" {
        let error = match rust {
            Err(error) => error,
            Ok(_) => bail!("Java-rejected input was accepted by Rust planning"),
        };
        summary.planning_errors_matched += 1;
        return Ok(
            json!({"case":name,"status":"matched_planning_error","java":java,"rust_error":error,
            "classification":"both reject after artifact identity verification; error-text equivalence is not claimed"}),
        );
    }
    ensure!(
        string(java, "status")? == "success",
        "unknown Java plan outcome"
    );
    let rust = rust.map_err(anyhow::Error::msg)?;
    let observed = observe_manifests(&table, snapshot_id, &schema).await?;
    let mut expected_files = BTreeMap::new();
    for task in array(java, "tasks")? {
        let path = string(&task["data"], "path")?;
        ensure!(
            expected_files.insert(path.to_string(), task).is_none(),
            "Java returns duplicate data file tasks: {path}"
        );
    }
    let actual_paths = rust
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<BTreeSet<_>>();
    ensure!(
        actual_paths == expected_files.keys().cloned().collect(),
        "data file inventory differs from Java"
    );
    ensure!(
        actual_paths.len() == rust.files.len(),
        "Rust returns duplicate data file observations"
    );
    let mut files_report = Vec::new();
    let mut report_dictionary = BTreeMap::new();
    for file in &rust.files {
        let task = expected_files[&file.path];
        let data = find_java_observation(&task["data"], &observed, &metadata, &schema)?;
        ensure!(
            data.file.content_type() == DataContentType::Data,
            "Java task data is not a data file"
        );
        ensure!(
            file.size as u64 == data.file.file_size_in_bytes()
                && file.record_count == Some(data.file.record_count() as i64)
                && file.data_sequence_number == Some(data.descriptor.sequence)
                && format!("{:x}", Sha256::digest(&file.manifest.key_metadata))
                    == data.descriptor.key_metadata_sha256,
            "Rust data facts differ from Java"
        );
        ensure!(
            file.logical_delete_set()
                .data()
                .partition()
                .to_json_string()
                == data.descriptor.typed_partition,
            "Rust data typed partition differs from Java"
        );
        let mut expected = BTreeMap::new();
        for member in java_delete_members(task, java)? {
            let fact = find_java_observation(member, &observed, &metadata, &schema)?;
            *expected.entry(fact.descriptor.clone()).or_insert(0usize) += 1;
        }
        let logical = bag(file
            .logical_delete_set()
            .members()
            .map(|fact| rust_descriptor(fact)))?;
        let loads = bag(file.deletes.members().map(|fact| rust_descriptor(fact)))?;
        let mut witnesses = Vec::new();
        for (descriptor, count) in &expected {
            ensure!(
                logical.get(descriptor).copied().unwrap_or(0) >= *count,
                "Java-applicable descriptor is missing from logical closure: {descriptor:?}"
            );
        }
        for (descriptor, count) in &logical {
            let expected_count = expected.get(descriptor).copied().unwrap_or(0);
            if *count > expected_count {
                let delete = find_descriptor(descriptor, &observed)?;
                let proof=statistics_proof(&data.file,&delete.file,&schema)
                    .with_context(||format!("Java omitted logical descriptor without independent safety proof: {descriptor:?}"))?;
                witnesses.push(json!({"direction":"java_omits_logical","member":descriptor,"multiplicity":count-expected_count,"proof":proof}));
            }
            let loaded = loads.get(descriptor).copied().unwrap_or(0);
            ensure!(
                loaded <= *count,
                "LoadView is not a subset of its logical closure"
            );
            if loaded < *count {
                let delete = find_descriptor(descriptor, &observed)?;
                let proof =
                    statistics_proof(&data.file, &delete.file, &schema).with_context(|| {
                        format!(
                            "Rust omitted a load without independent safety proof: {descriptor:?}"
                        )
                    })?;
                witnesses.push(json!({"direction":"rust_omits_load","member":descriptor,"multiplicity":count-loaded,"proof":proof}));
            }
        }
        ensure!(
            loads.keys().all(|key| logical.contains_key(key)),
            "LoadView contains a foreign member"
        );
        summary.data_files_verified += 1;
        summary.logical_members_verified += logical.values().sum::<usize>();
        summary.load_members_verified += loads.values().sum::<usize>();
        summary.safe_statistics_differences += witnesses.len();
        files_report.push(
            json!({"data":data.descriptor,"logical":intern_bag(&logical,&mut report_dictionary),"loads":intern_bag(&loads,&mut report_dictionary),
            "java":intern_bag(&expected,&mut report_dictionary),"safe_differences":witnesses}),
        );
    }
    let mut dictionary = report_dictionary
        .into_iter()
        .map(|(member, id)| (id, member))
        .collect::<Vec<_>>();
    dictionary.sort_by_key(|(id, _)| *id);
    Ok(
        json!({"case":name,"status":"verified","snapshot":snapshot_id,"files":files_report,
        "member_dictionary":dictionary.into_iter().map(|(id,member)|json!({"id":id,"member":member})).collect::<Vec<_>>()}),
    )
}

fn java_delete_members<'a>(task: &'a Value, java: &'a Value) -> Result<Vec<&'a Value>> {
    if let Some(members) = task.get("deletes") {
        return Ok(members
            .as_array()
            .context("Java deletes is not an array")?
            .iter()
            .collect());
    }
    let dictionary = java["delete_dictionary"]
        .as_object()
        .context("scale dictionary is missing")?;
    array(task, "delete_member_ids")?
        .iter()
        .map(|id| {
            let id = id.as_u64().context("scale member id is not unsigned")?;
            dictionary
                .get(&id.to_string())
                .context("scale member id is absent")
        })
        .collect()
}

fn intern_bag(
    bag: &BTreeMap<Descriptor, usize>,
    dictionary: &mut BTreeMap<Descriptor, usize>,
) -> Vec<Value> {
    bag.iter()
        .map(|(member, count)| {
            let next_id = dictionary.len();
            let id = *dictionary.entry(member.clone()).or_insert(next_id);
            json!({"id":id,"multiplicity":count})
        })
        .collect()
}
fn bag(items: impl Iterator<Item = Result<Descriptor>>) -> Result<BTreeMap<Descriptor, usize>> {
    let mut bag = BTreeMap::new();
    for item in items {
        *bag.entry(item?).or_insert(0) += 1;
    }
    Ok(bag)
}
fn find_descriptor<'a>(descriptor: &Descriptor, observed: &'a [Observed]) -> Result<&'a Observed> {
    observed
        .iter()
        .find(|item| item.descriptor == *descriptor)
        .context("descriptor has no independent raw manifest observation")
}

async fn observe_manifests(
    table: &Table,
    snapshot_id: i64,
    schema: &Schema,
) -> Result<Vec<Observed>> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .context("snapshot missing")?;
    let list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await?;
    let mut seen = BTreeSet::new();
    let mut observed = Vec::new();
    for manifest_file in list.entries() {
        if !seen.insert(&manifest_file.manifest_path) {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        let spec = metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .context("unknown manifest spec")?;
        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let sequence = match entry.sequence_number() {
                Some(value) => value,
                None if manifest.metadata().format_version == FormatVersion::V1 => 0,
                None if entry.status == ManifestStatus::Added => manifest_file.sequence_number,
                None => bail!("unresolved live entry in independent oracle observation"),
            };
            ensure!(
                sequence >= 0,
                "negative sequence in independent oracle observation"
            );
            let file = entry.data_file();
            let partition = TypedPartition::bind(spec, schema, file.partition())?.to_json_string();
            let mut fields = file.equality_ids().unwrap_or_default();
            fields.sort_unstable();
            let field_types = fields
                .iter()
                .map(|id| {
                    schema
                        .field_by_id(*id)
                        .map(|field| field.field_type.to_string())
                        .context("equality field absent from oracle schema")
                })
                .collect::<Result<Vec<_>>>()?;
            let format = format_name(file.file_format()).to_string();
            let kind = match (file.content_type(), file.file_format()) {
                (DataContentType::Data, _) => "DATA",
                (DataContentType::EqualityDeletes, _) => "EQUALITY",
                (DataContentType::PositionDeletes, DataFileFormat::Puffin) => "DV",
                (DataContentType::PositionDeletes, _) => "POSITION",
            }
            .to_string();
            observed.push(Observed {
                descriptor: Descriptor {
                    path: file.file_path().to_string(),
                    format,
                    kind,
                    offset: file.content_offset(),
                    length: file.content_size_in_bytes(),
                    sequence,
                    spec: spec.spec_id(),
                    typed_partition: partition,
                    fields,
                    field_types,
                    target: file
                        .referenced_data_file()
                        .or_else(|| exact_position_target(file)),
                    record_count: file.record_count(),
                    file_size: file.file_size_in_bytes(),
                    key_metadata_sha256: format!(
                        "{:x}",
                        Sha256::digest(file.key_metadata().unwrap_or_default())
                    ),
                },
                file: file.clone(),
            });
        }
    }
    Ok(observed)
}

fn exact_position_target(file: &DataFile) -> Option<String> {
    if file.content_type() != DataContentType::PositionDeletes {
        return None;
    }
    match (
        file.lower_bounds().get(&FILE_PATH_ID)?.literal(),
        file.upper_bounds().get(&FILE_PATH_ID)?.literal(),
    ) {
        (PrimitiveLiteral::String(a), PrimitiveLiteral::String(b)) if a == b && !a.is_empty() => {
            Some(a.clone())
        }
        _ => None,
    }
}
fn format_name(format: DataFileFormat) -> &'static str {
    match format {
        DataFileFormat::Parquet => "PARQUET",
        DataFileFormat::Puffin => "PUFFIN",
        DataFileFormat::Avro => "AVRO",
        DataFileFormat::Orc => "ORC",
    }
}
fn rust_descriptor(fact: &DeleteFact) -> Result<Descriptor> {
    let (kind, fields, target) = match fact.kind() {
        DeleteKind::Position { exact_target } => (
            "POSITION",
            Vec::new(),
            exact_target.as_ref().map(ToString::to_string),
        ),
        DeleteKind::DeletionVector { exact_target } => {
            ("DV", Vec::new(), Some(exact_target.to_string()))
        }
        DeleteKind::Equality(group) => (
            "EQUALITY",
            group.fields().iter().map(|(id, _)| *id).collect(),
            None,
        ),
    };
    let (offset, length) = match fact.address() {
        DeleteContentAddress::File(_) => (None, None),
        DeleteContentAddress::PuffinBlob { offset, length, .. } => {
            (Some(i64::try_from(*offset)?), Some(i64::try_from(*length)?))
        }
    };
    let field_types = match fact.kind() {
        DeleteKind::Equality(group) => group
            .fields()
            .iter()
            .map(|(_, ty)| ty.to_string())
            .collect(),
        _ => Vec::new(),
    };
    Ok(Descriptor {
        path: fact.address().path().to_string(),
        format: match fact.read().format {
            DeleteFormat::Parquet => "PARQUET",
            DeleteFormat::Puffin => "PUFFIN",
            DeleteFormat::Avro => "AVRO",
            DeleteFormat::Orc => "ORC",
        }
        .to_string(),
        kind: kind.to_string(),
        offset,
        length,
        sequence: fact.sequence().get(),
        spec: fact.partition().spec_id(),
        typed_partition: fact.partition().to_json_string(),
        fields,
        field_types,
        target,
        record_count: fact.read().record_count,
        file_size: fact.read().file_size,
        key_metadata_sha256: format!("{:x}", Sha256::digest(&fact.read().key_metadata)),
    })
}

fn find_java_observation<'a>(
    java: &Value,
    observed: &'a [Observed],
    metadata: &TableMetadata,
    schema: &Schema,
) -> Result<&'a Observed> {
    let path = string(java, "path")?;
    let spec_id = i32::try_from(int(java, "spec_id")?)?;
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .context("Java descriptor spec absent from metadata")?;
    let partition = parse_fixture_partition(string(java, "partition")?, spec, schema)?;
    let mut fields = java
        .get("equality_ids")
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .map(|id| {
                    id.as_i64()
                        .and_then(|v| i32::try_from(v).ok())
                        .context("invalid equality field id")
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    fields.sort_unstable();
    let content = string(java, "content")?;
    let format = string(java, "format")?;
    let kind = match (content, format) {
        ("DATA", _) => "DATA",
        ("EQUALITY_DELETES", _) => "EQUALITY",
        ("POSITION_DELETES", "PUFFIN") => "DV",
        ("POSITION_DELETES", _) => "POSITION",
        _ => bail!("unknown Java content kind"),
    };
    let candidate = observed
        .iter()
        .find(|item| {
            let d = &item.descriptor;
            d.path == path
                && d.spec == spec_id
                && d.typed_partition == partition
                && d.format == format
                && d.kind == kind
                && Some(d.sequence) == java["data_sequence"].as_i64()
                && Some(d.record_count) == java["record_count"].as_u64()
                && Some(d.file_size) == java["file_size"].as_u64()
                && d.fields == fields
                && d.offset == java["content_offset"].as_i64()
                && d.length == java["content_size"].as_i64()
                && item.file.referenced_data_file().as_deref()
                    == java["referenced_data_file"].as_str()
        })
        .with_context(|| format!("Java descriptor has no exact raw manifest match: {java}"))?;
    validate_java_metrics(java, &candidate.file, schema)?;
    Ok(candidate)
}

/// Iceberg 1.11 DeleteFileIndex.loadDeleteFiles deliberately copies only path
/// metrics for position deletes and equality-column metrics for equality deletes
/// (DeleteFileIndex.java:434-440; ContentFileUtil.copy delegates copyWithStats).
/// Task data files in this frozen includeColumnStats corpus keep all columns.
/// Compare the complete known projection, never an arbitrary subset. The raw
/// SDK DataFile remains intact for independent exclusion and read-fact evidence.
fn java_publishes_metric(file: &DataFile, id: i32) -> bool {
    match file.content_type() {
        DataContentType::Data => true,
        DataContentType::PositionDeletes => id == FILE_PATH_ID,
        DataContentType::EqualityDeletes => {
            file.equality_ids().is_some_and(|ids| ids.contains(&id))
        }
    }
}

fn validate_java_metrics(java: &Value, file: &DataFile, schema: &Schema) -> Result<()> {
    let path = file.file_path();
    for (key, actual) in [
        ("value_counts", file.value_counts()),
        ("null_value_counts", file.null_value_counts()),
        ("nan_value_counts", file.nan_value_counts()),
    ] {
        if java.get(key).is_some() {
            let expected = java[key]
                .as_object()
                .map(|map| {
                    map.iter()
                        .map(|(id, count)| {
                            Ok((
                                id.parse::<i32>()?,
                                count.as_u64().context("invalid metric count")?,
                            ))
                        })
                        .collect::<Result<BTreeMap<_, _>>>()
                })
                .transpose()?
                .unwrap_or_default();
            ensure!(
                expected
                    == actual
                        .iter()
                        .filter(|(id, _)| java_publishes_metric(file, **id))
                        .map(|(id, n)| (*id, *n))
                        .collect(),
                "Java {key} differs from raw manifest for {path}"
            );
        }
    }
    for (key, actual) in [
        ("lower_bounds_base64", file.lower_bounds()),
        ("upper_bounds_base64", file.upper_bounds()),
    ] {
        if let Some(expected) = java.get(key) {
            let expected = expected.as_object();
            let expected_ids = expected
                .into_iter()
                .flatten()
                .map(|(id, _)| id.parse::<i32>().map_err(anyhow::Error::from))
                .collect::<Result<BTreeSet<_>>>()?;
            let actual_ids = actual
                .keys()
                .filter(|id| java_publishes_metric(file, **id))
                .copied()
                .collect::<BTreeSet<_>>();
            ensure!(
                expected_ids == actual_ids,
                "Java {key} field inventory differs from the exact content projection of raw manifest for {path}"
            );
            for (id, bytes) in expected.into_iter().flatten() {
                let id = id.parse::<i32>()?;
                let actual = actual
                    .get(&id)
                    .context("Java bound absent from raw manifest")?;
                let ty = if id == FILE_PATH_ID {
                    PrimitiveType::String
                } else if id == FILE_PATH_ID - 1 {
                    PrimitiveType::Long
                } else {
                    match schema.field_by_id(id).map(|f| f.field_type.as_ref()) {
                        Some(Type::Primitive(ty)) => ty.clone(),
                        _ => actual.data_type().clone(),
                    }
                };
                let expected = Datum::try_from_bytes(
                    &decode_base64(bytes.as_str().context("bound is not base64 text")?)?,
                    ty.clone(),
                )?;
                let actual = promote(actual, &ty).context("raw bound has no legal promotion")?;
                ensure!(
                    expected == actual,
                    "Java {key} value differs from raw manifest for {path}, field {id}"
                );
            }
        }
    }
    Ok(())
}

fn decode_base64(text: &str) -> Result<Vec<u8>> {
    let bytes = text.as_bytes();
    ensure!(bytes.len() % 4 == 0, "invalid base64 length");
    let mut result = Vec::with_capacity(bytes.len() / 4 * 3);
    let digit = |byte: u8| -> Result<u32> {
        Ok(match byte {
            b'A'..=b'Z' => u32::from(byte - b'A'),
            b'a'..=b'z' => u32::from(byte - b'a') + 26,
            b'0'..=b'9' => u32::from(byte - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            _ => bail!("invalid base64 digit"),
        })
    };
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let last = index + 1 == bytes.len() / 4;
        ensure!(
            last || !chunk.contains(&b'='),
            "base64 padding is not final"
        );
        ensure!(
            chunk[2] != b'=' || chunk[3] == b'=',
            "invalid base64 padding"
        );
        let a = digit(chunk[0])?;
        let b = digit(chunk[1])?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            digit(chunk[2])?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            digit(chunk[3])?
        };
        let value = (a << 18) | (b << 12) | (c << 6) | d;
        result.push((value >> 16) as u8);
        if chunk[2] != b'=' {
            result.push((value >> 8) as u8);
        }
        if chunk[3] != b'=' {
            result.push(value as u8);
        }
    }
    Ok(result)
}

fn parse_fixture_partition(text: &str, spec: &PartitionSpec, schema: &Schema) -> Result<String> {
    let body = text
        .strip_prefix("PartitionData{")
        .and_then(|s| s.strip_suffix('}'))
        .context("unexpected fixture partition spelling")?;
    let partition_type = spec.partition_type(schema)?;
    let values = if body.is_empty() {
        ensure!(
            partition_type.fields().is_empty(),
            "empty Java tuple for nonempty spec"
        );
        Struct::empty()
    } else {
        ensure!(
            partition_type.fields().len() == 1
                && matches!(spec.fields()[0].name.as_str(), "p" | "partition_key"),
            "fixture tuple parser only supports one named p or partition_key field"
        );
        let value = body
            .strip_prefix(&format!("{}=", spec.fields()[0].name))
            .context("fixture partition field differs from its frozen spec")?;
        let literal = if value == "null" {
            None
        } else {
            Some(match partition_type.fields()[0].field_type.as_ref() {
                Type::Primitive(PrimitiveType::Int) => Literal::int(value.parse::<i32>()?),
                Type::Primitive(PrimitiveType::Long) => Literal::long(value.parse::<i64>()?),
                _ => bail!("fixture tuple parser only supports integer values"),
            })
        };
        Struct::from_iter([literal])
    };
    Ok(TypedPartition::bind(spec, schema, &values)?.to_json_string())
}

/// Independent sufficient proof: one equality field has no possible common
/// null, NaN, or ordinary value; or a position path interval excludes the file.
fn statistics_proof(data: &DataFile, delete: &DataFile, schema: &Schema) -> Result<Value> {
    if delete.record_count() == 0 {
        return Ok(json!({"reason":"empty_delete_file","record_count":0}));
    }
    match delete.content_type() {
        DataContentType::PositionDeletes if delete.file_format() != DataFileFormat::Puffin => {
            let lower = delete
                .lower_bounds()
                .get(&FILE_PATH_ID)
                .context("position path lower bound missing")?;
            let upper = delete
                .upper_bounds()
                .get(&FILE_PATH_ID)
                .context("position path upper bound missing")?;
            let (PrimitiveLiteral::String(lower), PrimitiveLiteral::String(upper)) =
                (lower.literal(), upper.literal())
            else {
                bail!("position bounds are not strings")
            };
            ensure!(lower <= upper, "position path bounds are inverted");
            ensure!(
                data.file_path() < lower.as_str() || data.file_path() > upper.as_str(),
                "position path intervals do not exclude this data file"
            );
            Ok(
                json!({"reason":"position_path_interval","data_path":data.file_path(),"lower":lower,"upper":upper}),
            )
        }
        DataContentType::EqualityDeletes => {
            for field in delete.equality_ids().context("equality field ids absent")? {
                let Some(field_type) = schema.field_by_id(field) else {
                    continue;
                };
                let Type::Primitive(field_type) = field_type.field_type.as_ref() else {
                    continue;
                };
                if let Some(proof) = field_disjoint(data, delete, field, field_type) {
                    return Ok(proof);
                }
            }
            bail!("no independently proven disjoint equality field")
        }
        _ => bail!("DV or non-delete descriptors cannot be statistically omitted"),
    }
}

fn field_disjoint(
    data: &DataFile,
    delete: &DataFile,
    id: i32,
    value_type: &PrimitiveType,
) -> Option<Value> {
    let a = metrics(data, id, value_type)?;
    let b = metrics(delete, id, value_type)?;
    if a.null_possible && b.null_possible || a.nan_possible && b.nan_possible {
        return None;
    }
    let disjoint = if !a.ordinary_possible || !b.ordinary_possible {
        true
    } else {
        let (al, au) = a.interval.as_ref()?;
        let (bl, bu) = b.interval.as_ref()?;
        al.partial_cmp(bu) == Some(Ordering::Greater)
            || bl.partial_cmp(au) == Some(Ordering::Greater)
    };
    disjoint.then(||json!({"reason":"equality_field_disjoint","field_id":id,"resolved_type":value_type.to_string(),
        "data":a.evidence,"delete":b.evidence}))
}
struct Metrics {
    null_possible: bool,
    nan_possible: bool,
    ordinary_possible: bool,
    interval: Option<(Datum, Datum)>,
    evidence: Value,
}
fn metrics(file: &DataFile, id: i32, value_type: &PrimitiveType) -> Option<Metrics> {
    let count = file.value_counts().get(&id).copied();
    let null = file.null_value_counts().get(&id).copied();
    let nan = file.nan_value_counts().get(&id).copied();
    if let Some(count) = count {
        if null.is_some_and(|n| n > count) || nan.is_some_and(|n| n > count) {
            return None;
        }
        if let (Some(null), Some(nan)) = (null, nan) {
            if null.checked_add(nan)? > count {
                return None;
            }
        }
    }
    let all_null = matches!((count,null),(Some(c),Some(n)) if c==n);
    let is_float = matches!(value_type, PrimitiveType::Float | PrimitiveType::Double);
    let null_possible = count != Some(0) && null != Some(0);
    let nan_possible = is_float && count != Some(0) && nan != Some(0) && !all_null;
    let ordinary_possible = match count {
        None => true,
        Some(c) => {
            c > null
                .unwrap_or(0)
                .saturating_add(if is_float { nan.unwrap_or(0) } else { 0 })
        }
    };
    let interval = file
        .lower_bounds()
        .get(&id)
        .zip(file.upper_bounds().get(&id))
        .and_then(|(lower, upper)| {
            let lower = promote(lower, value_type)?;
            let upper = promote(upper, value_type)?;
            if lower.is_nan()
                || upper.is_nan()
                || !matches!(
                    lower.partial_cmp(&upper),
                    Some(Ordering::Less | Ordering::Equal)
                )
            {
                None
            } else {
                Some((lower, upper))
            }
        });
    Some(Metrics {
        null_possible,
        nan_possible,
        ordinary_possible,
        evidence: json!({"value_count":count,"null_count":null,"nan_count":nan,
        "lower":interval.as_ref().map(|(l,_)|l.to_string()),"upper":interval.as_ref().map(|(_,u)|u.to_string()),
        "null_possible":null_possible,"nan_possible":nan_possible,"ordinary_possible":ordinary_possible}),
        interval,
    })
}
fn promote(datum: &Datum, target: &PrimitiveType) -> Option<Datum> {
    let source = datum.data_type();
    let legal = source == target
        || matches!(
            (source, target),
            (PrimitiveType::Int, PrimitiveType::Long)
                | (PrimitiveType::Float, PrimitiveType::Double)
        )
        || matches!((source,target),(PrimitiveType::Decimal{precision:a,scale:x},PrimitiveType::Decimal{precision:b,scale:y}) if a<=b && x==y);
    if !legal {
        return None;
    }
    Datum::try_from_bytes(&datum.to_bytes().ok()?, target.clone()).ok()
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string {key}"))
}
fn int(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("missing integer {key}"))
}
fn uint(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing unsigned integer {key}"))
}
fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("missing array {key}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_connector_iceberg::iceberg::spec::DataFileBuilder;
    use std::collections::HashMap;

    #[test]
    fn position_metric_projection_is_exact_and_keeps_raw_position_evidence() {
        let schema = Schema::builder().build().unwrap();
        let file = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("position".into())
            .file_format(DataFileFormat::Parquet)
            .record_count(4)
            .file_size_in_bytes(100)
            .value_counts(HashMap::from([(FILE_PATH_ID, 4), (FILE_PATH_ID - 1, 4)]))
            .lower_bounds(HashMap::from([
                (FILE_PATH_ID, Datum::string("a")),
                (FILE_PATH_ID - 1, Datum::long(2)),
            ]))
            .upper_bounds(HashMap::from([
                (FILE_PATH_ID, Datum::string("z")),
                (FILE_PATH_ID - 1, Datum::long(9)),
            ]))
            .build()
            .unwrap();
        let java = json!({"value_counts":{FILE_PATH_ID.to_string():4},
            "lower_bounds_base64":{FILE_PATH_ID.to_string():"YQ=="},
            "upper_bounds_base64":{FILE_PATH_ID.to_string():"eg=="}});
        validate_java_metrics(&java, &file, &schema).unwrap();
        assert_eq!(file.lower_bounds().len(), 2);
        assert_eq!(file.lower_bounds()[&(FILE_PATH_ID - 1)], Datum::long(2));
        let mut omitted = java.clone();
        omitted["lower_bounds_base64"] = json!({});
        assert!(validate_java_metrics(&omitted, &file, &schema).is_err());
        let mut extra = java.clone();
        extra["lower_bounds_base64"][&(FILE_PATH_ID - 1).to_string()] = json!("AgAAAAAAAAA=");
        assert!(validate_java_metrics(&extra, &file, &schema).is_err());
        let mut changed = java.clone();
        changed["value_counts"][&FILE_PATH_ID.to_string()] = json!(5);
        assert!(validate_java_metrics(&changed, &file, &schema).is_err());
        let mut changed = java;
        changed["lower_bounds_base64"][&FILE_PATH_ID.to_string()] = json!("Yg==");
        assert!(validate_java_metrics(&changed, &file, &schema).is_err());
    }

    #[test]
    fn equality_projection_is_exact_while_data_metric_inventory_is_unfiltered() {
        let schema = Schema::builder().build().unwrap();
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::EqualityDeletes)
            .file_path("equality".into())
            .file_format(DataFileFormat::Parquet)
            .record_count(4)
            .file_size_in_bytes(100)
            .equality_ids(Some(vec![1]))
            .value_counts(HashMap::from([(1, 4), (2, 4)]));
        let equality = builder.build().unwrap();
        validate_java_metrics(&json!({"value_counts":{"1":4}}), &equality, &schema).unwrap();
        assert!(
            validate_java_metrics(&json!({"value_counts":{"2":4}}), &equality, &schema).is_err()
        );
        assert!(
            validate_java_metrics(&json!({"value_counts":{"1":4,"2":4}}), &equality, &schema)
                .is_err()
        );
        builder.content(DataContentType::Data).equality_ids(None);
        let data = builder.build().unwrap();
        assert!(validate_java_metrics(&json!({"value_counts":{"1":4}}), &data, &schema).is_err());
        validate_java_metrics(&json!({"value_counts":{"1":4,"2":4}}), &data, &schema).unwrap();
    }

    /// Explicit local replay uses only the receipt's SHA-verified captured
    /// artifacts. It starts no server and writes no fixture or golden input.
    #[tokio::test]
    #[ignore = "requires the explicit frozen Java receipt and output directory"]
    async fn frozen_receipt_replay_without_native() {
        let receipt_path = std::path::PathBuf::from(
            std::env::var("NOVAROCKS_UEA4G_ORACLE_RECEIPT").expect("explicit receipt"),
        );
        let output = std::path::PathBuf::from(
            std::env::var("NOVAROCKS_UEA4G_ORACLE_OUTPUT").expect("explicit output"),
        );
        let receipt: Value =
            serde_json::from_slice(&std::fs::read(&receipt_path).unwrap()).unwrap();
        let io = FileIO::new_with_memory();
        let mut sdk_metrics = Vec::new();
        for artifact in array(&receipt, "artifacts").unwrap() {
            let bytes = std::fs::read(
                receipt_path
                    .parent()
                    .unwrap()
                    .join(string(artifact, "local_path").unwrap()),
            )
            .unwrap();
            assert_eq!(bytes.len() as u64, uint(artifact, "size").unwrap());
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                string(artifact, "sha256").unwrap()
            );
            if artifact["kind"] == "manifest" {
                let manifest =
                    novarocks_connector_iceberg::iceberg::spec::Manifest::parse_avro(&bytes)
                        .unwrap();
                for entry in manifest.entries() {
                    let file = entry.data_file();
                    if file.content_type() == DataContentType::PositionDeletes {
                        sdk_metrics.push(json!({"manifest_sha256":artifact["sha256"],"path":file.file_path(),
                            "value_counts":file.value_counts(),"null_value_counts":file.null_value_counts(),
                            "lower_bounds":file.lower_bounds().iter().map(|(id,value)|(id.to_string(),value.to_string())).collect::<BTreeMap<_,_>>(),
                            "upper_bounds":file.upper_bounds().iter().map(|(id,value)|(id.to_string(),value.to_string())).collect::<BTreeMap<_,_>>()}));
                    }
                }
            }
            io.new_output(string(artifact, "path").unwrap())
                .unwrap()
                .write(bytes::Bytes::from(bytes))
                .await
                .unwrap();
        }
        std::fs::create_dir_all(&output).unwrap();
        std::fs::write(
            output.join("sdk-raw-position-metrics.json"),
            serde_json::to_vec_pretty(&sdk_metrics).unwrap(),
        )
        .unwrap();
        let summary = verify_receipt(&receipt_path, io, &output).await.unwrap();
        assert_eq!(
            summary.cases_verified,
            array(&receipt, "cases").unwrap().len()
        );
    }

    #[test]
    fn scale_projection_retains_dictionary_members_and_both_endpoints() {
        let receipt = json!({"case_count":2,"cases":[
            {"case":"from","java_planFiles":"success","plan_file_count":1,"delete_dictionary_size":1},
            {"case":"to","java_planFiles":"success","plan_file_count":1,"delete_dictionary_size":1}
        ]});
        let members = ["from", "to"].into_iter().map(|name| json!({
            "record":"scale-delete-member","case":name,"member_id":0,
            "content":{"path":"same-physical-delete","data_sequence":if name=="from" {2} else {3}}
        })).collect();
        let plans = ["from", "to"]
            .into_iter()
            .map(|name| {
                json!({
                    "record":"scale-plan-file","case":name,"data_ordinal":0,
                    "data":{"path":"data"},"delete_member_ids":[0],"delete_list_size":1
                })
            })
            .collect();
        let cases = normalize_scale_cases(&receipt, plans, members).unwrap();
        assert_eq!(cases.len(), 2);
        for (case, sequence) in cases.iter().zip([2, 3]) {
            let java = &case["java_planFiles"];
            let members = java_delete_members(&java["tasks"][0], java).unwrap();
            assert_eq!(members.len(), 1);
            assert_eq!(members[0]["data_sequence"], sequence);
            assert!(java["tasks"][0].get("deletes").is_none());
        }
    }

    #[test]
    fn scale_projection_rejects_a_missing_member_instead_of_dropping_it() {
        let receipt = json!({"case_count":1,"cases":[
            {"case":"case","java_planFiles":"success","plan_file_count":1,"delete_dictionary_size":0}
        ]});
        let plans = vec![
            json!({"record":"scale-plan-file","case":"case","data_ordinal":0,
            "data":{"path":"data"},"delete_member_ids":[0],"delete_list_size":1}),
        ];
        assert!(normalize_scale_cases(&receipt, plans, Vec::new()).is_err());
    }

    fn bounded(lower: Datum, upper: Datum, nulls: Option<u64>, nans: Option<u64>) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::EqualityDeletes)
            .file_path("immutable-fixture".to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(10)
            .file_size_in_bytes(100)
            .value_counts(HashMap::from([(1, 10)]))
            .lower_bounds(HashMap::from([(1, lower)]))
            .upper_bounds(HashMap::from([(1, upper)]));
        if let Some(nulls) = nulls {
            builder.null_value_counts(HashMap::from([(1, nulls)]));
        }
        if let Some(nans) = nans {
            builder.nan_value_counts(HashMap::from([(1, nans)]));
        }
        builder.build().unwrap()
    }

    #[test]
    fn independent_witness_never_uses_disjoint_bounds_to_hide_null_matches() {
        let left = bounded(Datum::long(1), Datum::long(2), Some(1), None);
        let right = bounded(Datum::long(3), Datum::long(4), Some(1), None);
        assert!(field_disjoint(&left, &right, 1, &PrimitiveType::Long).is_none());
        let no_null = bounded(Datum::long(1), Datum::long(2), Some(0), None);
        assert!(field_disjoint(&no_null, &right, 1, &PrimitiveType::Long).is_some());
        let unknown = bounded(Datum::long(1), Datum::long(2), None, None);
        assert!(field_disjoint(&unknown, &right, 1, &PrimitiveType::Long).is_none());
    }

    #[test]
    fn independent_witness_requires_nan_evidence_and_promotes_numeric_values() {
        let left = bounded(Datum::float(-1.5), Datum::float(-0.25), Some(0), None);
        let right = bounded(Datum::double(0.25), Datum::double(1.5), Some(0), None);
        assert!(field_disjoint(&left, &right, 1, &PrimitiveType::Double).is_none());
        let left = bounded(Datum::float(-1.5), Datum::float(-0.25), Some(0), Some(0));
        assert!(field_disjoint(&left, &right, 1, &PrimitiveType::Double).is_some());
    }
}
