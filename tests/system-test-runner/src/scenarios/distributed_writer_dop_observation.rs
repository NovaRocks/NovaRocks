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

//! Actual stage and driver geometry for predeclared row-level statements.
//! Authenticated bindings select the execution/process; original typed markers
//! select each Task and each driver-local writer. Neither grants capacity or
//! substitutes for SQL, publication, resource convergence or process exit.
use super::*;
use anyhow::ensure;
use novarocks_execution_contract::TaskIdentity;
use novarocks_failpoint::target_binding::StatementExecutionBinding;
use novarocks_types::{StageId, TaskId, UniqueId};
use std::collections::BTreeMap;
use std::str::FromStr;

const PREPARED: &str = novarocks_failpoint::observation_marker::TASK_PREPARED_DOP;
const OPENED: &str = novarocks_failpoint::observation_marker::CONNECTOR_WRITER_OPENED;
const READER_CLOSE: &str = novarocks_failpoint::observation_marker::CONNECTOR_UNIT_READER_CLOSE;

const SHARD_OPENED: &str = novarocks_failpoint::observation_marker::POSITION_SHARD_DRIVER_OPENED;
const SHARD_FINISHED: &str =
    novarocks_failpoint::observation_marker::POSITION_SHARD_DRIVER_FINISHED;

#[derive(Clone, Copy, Debug)]
pub(in crate::scenarios::distributed_writer) enum Geometry {
    DataAndEmptyDelete,
    SameFileDelete,
    MixedSameFile,
    MultiFileDelete,
    V2Delete,
}
impl Geometry {
    fn report_suffix(self) -> &'static str {
        match self {
            Self::DataAndEmptyDelete => "",
            Self::SameFileDelete => "-same-file-delete",
            Self::MixedSameFile => "-mixed-same-file",
            Self::MultiFileDelete => "-multi-file-delete",
            Self::V2Delete => "-v2-delete",
        }
    }
}

pub(in crate::scenarios::distributed_writer) struct MutationExpectation<'a> {
    pub dop: i32,
    pub affected: u64,
    pub reader_rows: Option<u64>,
    pub geometry: Geometry,
    pub subject: &'a str,
}

pub(in crate::scenarios::distributed_writer) fn mutate_observed(
    context: &mut ScenarioContext,
    control: &mut SqlSession,
    selected: &SelectedStatement,
    table: &str,
    sql: &str,
    before_writers: &[WriteCounters],
    expectation: MutationExpectation<'_>,
) -> Result<()> {
    let MutationExpectation {
        dop,
        affected,
        reader_rows,
        geometry,
        subject,
    } = expectation;
    let before = snapshot_count(control, table)?;
    // The original collector sees one nominated statement at a time. Retain
    // the exact stdout prefix before arming this DELETE's reader observation.
    let reader_before = reader_rows
        .map(|_| {
            (0..context.handle().be_count())
                .map(|index| context.handle().be_stdout_contents(index, usize::MAX))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let pending = control.submit(sql)?;
    let (pending, observation) = selected.arm_pending(context, pending, sql)?;
    let (outcome, coordinated) =
        super::super::small_slots::query_drop(context, control, selected, pending, dop);
    let public_error = matches!(&outcome, Some(Err(StatementFailure::Trino(_))));
    let primary = (|| -> Result<()> {
        let query = outcome
            .context("original row-level SQL has no terminal outcome")
            .and_then(|outcome| outcome.with_context(|| subject.to_owned()));
        let result = super::super::writer_statement_actor::combine(query, coordinated)?;
        sql::expect_update_count(&result, affected)
            .with_context(|| format!("{subject}: expected logical I+U+D={affected}"))?;
        let snapshot = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, selected.deadline)?;
        let execution = snapshot
            .execution_id
            .as_deref()
            .context("successful row-level statement omitted its exact execution")?;
        let after_writers = all_write_counters(context)?;
        anyhow::ensure!(
            before_writers.len() == after_writers.len(),
            "writer counter backend inventory changed"
        );
        let mut backends = Vec::new();
        let mut total_reader_rows = 0_u64;
        let mut total_reader_batches = 0_u64;
        for (index, (before, after)) in before_writers.iter().zip(&after_writers).enumerate() {
            // These helpers read the existing private nomination files and
            // verify this exact launch/session/process/request/attempt.
            let grpc = context.handle().selected_native_observation_binding(
                index,
                novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
                &selected.token,
                None,
            )?;
            let writer = context.handle().selected_native_observation_binding(
                index,
                novarocks_failpoint::observation_marker::ObservationAction::WriterMarker.as_str(),
                &selected.token,
                None,
            )?;
            anyhow::ensure!(
                grpc == writer,
                "BE[{index}] writer and Task observation bindings differ"
            );
            // GrantedObservation owns these flushed stdout lines. Production
            // tracing, diagnostic locators and combined log history are not
            // inputs. The original reader enforces its actual source bounds.
            let stdout = context.handle().be_stdout_contents(index, usize::MAX)?;
            let mut backend = inventory(&stdout, grpc.as_ref(), execution, dop)?;
            if let Some(prefixes) = reader_before.as_ref() {
                let reader = context.handle().selected_native_observation_binding(
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker
                        .as_str(),
                    &selected.token,
                    None,
                )?;
                anyhow::ensure!(
                    reader == grpc,
                    "BE[{index}] reader and Task observation bindings differ"
                );
                let (facts, rows, batches) = reader_inventory(
                    prefixes
                        .get(index)
                        .context("reader stdout prefix backend inventory changed")?,
                    &stdout,
                    reader.as_ref(),
                )?;
                total_reader_rows = total_reader_rows
                    .checked_add(rows)
                    .context("reader row count overflow")?;
                total_reader_batches = total_reader_batches
                    .checked_add(batches)
                    .context("reader batch count overflow")?;
                backend["actual_reader_batches"] = facts;
            }
            // No other DML runs in this original window. Counters check that
            // the finite observer omitted no complete writer group; geometry
            // still comes from exact Task/driver identities, never this sum.
            let actual_opens = after.opens - before.opens;
            let observed_opens = backend["observed_writer_opens"]
                .as_u64()
                .context("writer inventory omitted its event count")?;
            anyhow::ensure!(
                actual_opens == observed_opens as f64,
                "BE[{index}] selected writer markers cover {observed_opens} opens, original counter delta={actual_opens}"
            );
            backend["original_writer_open_delta"] = serde_json::json!(actual_opens);
            backends.push(backend);
        }
        validate_geometry(&backends, geometry)?;
        if let Some(expected_rows) = reader_rows {
            anyhow::ensure!(
                total_reader_batches > 1 && total_reader_rows >= expected_rows,
                "one old file DELETE observed batches={total_reader_batches} rows_decoded={total_reader_rows}, expected multiple actual batches covering at least {expected_rows} rows"
            );
        }
        let suffix = geometry.report_suffix();
        std::fs::write(
            context
                .scenario_root()
                .join(format!("row-level-writer-dop-{dop}{suffix}.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "requested_dop":dop,"execution_id":execution,"backends":backends,
                "actual_reader_rows_decoded":reader_rows.map(|_|total_reader_rows),
                "actual_reader_batches_delivered":reader_rows.map(|_|total_reader_batches),
            }))?,
        )?;
        Ok(())
    })();
    // Drain the same original journal binding on every result path,
    // including missing or malformed observation evidence.
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
    let reentry = if public_error {
        super::super::writer_statement_actor::require_public_error_reentry(control)
    } else {
        Ok(())
    };
    super::super::writer_statement_actor::combine(
        primary,
        super::super::writer_statement_actor::combine(reentry, drained),
    )?;
    let after = snapshot_count(control, table)?;
    anyhow::ensure!(
        after == before + 1,
        "{subject}: snapshot delta={}, expected one",
        after - before
    );
    Ok(())
}

fn fields<'a>(line: &'a str, marker: &str) -> Result<Option<BTreeMap<&'a str, &'a str>>> {
    let Some(text) = line
        .strip_prefix(marker)
        .and_then(|rest| rest.strip_prefix(' '))
    else {
        return Ok(None);
    };
    let mut fields = BTreeMap::new();
    for token in text.split_whitespace() {
        let (name, value) = token
            .split_once('=')
            .context("incomplete typed writer/Task marker field")?;
        anyhow::ensure!(
            !name.is_empty() && !value.is_empty() && fields.insert(name, value).is_none(),
            "invalid or duplicate typed marker field"
        );
    }
    Ok(Some(fields))
}
fn field<'a>(fields: &BTreeMap<&str, &'a str>, name: &str) -> Result<&'a str> {
    fields
        .get(name)
        .copied()
        .with_context(|| format!("typed writer/Task marker omitted {name}"))
}
fn number<T: FromStr>(fields: &BTreeMap<&str, &str>, name: &str) -> Result<T> {
    field(fields, name)?
        .parse()
        .map_err(|_| anyhow!("typed writer/Task marker has invalid {name}"))
}
fn task_key(fields: &BTreeMap<&str, &str>) -> Result<String> {
    let text = field(fields, "task_key")?;
    let (hi, lo) = text
        .split_once(':')
        .context("typed writer/Task marker has invalid task_key")?;
    let id = UniqueId::new(
        u64::from_str_radix(hi, 16)? as i64,
        u64::from_str_radix(lo, 16)? as i64,
    );
    let canonical = format!("{:x}:{:x}", id.high(), id.low());
    anyhow::ensure!(
        text == canonical,
        "typed writer/Task marker has noncanonical task_key"
    );
    Ok(canonical)
}

#[derive(Clone)]
struct Prepared {
    identity: TaskIdentity,
    dop: i32,
}
struct Writers {
    task: TaskIdentity,
    plan_node: i32,
    role: WriterRole,
    drivers: BTreeSet<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriterRole {
    Data,
    V2PositionDelete,
    V3Materialize,
}
impl WriterRole {
    fn read(value: &str) -> Result<Self> {
        match value {
            "data" => Ok(Self::Data),
            "v2_position_delete" => Ok(Self::V2PositionDelete),
            "v3_materialize" => Ok(Self::V3Materialize),
            _ => bail!("unexpected observed writer role {value}"),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::V2PositionDelete => "v2_position_delete",
            Self::V3Materialize => "v3_materialize",
        }
    }
    fn dop(self, input_dop: i32) -> i32 {
        match self {
            Self::V3Materialize => 1,
            _ => input_dop,
        }
    }
}

fn validate_geometry(backends: &[serde_json::Value], geometry: Geometry) -> Result<()> {
    let groups = |field: &str| -> Vec<&serde_json::Value> {
        backends
            .iter()
            .flat_map(|be| be[field].as_array().unwrap())
            .collect()
    };
    let writers = groups("writer_groups");
    let shards = groups("position_shard_groups");
    let by_role = |role: WriterRole| -> Vec<&serde_json::Value> {
        writers
            .iter()
            .copied()
            .filter(|group| group["role"] == role.name())
            .collect()
    };
    let data = by_role(WriterRole::Data);
    let v2 = by_role(WriterRole::V2PositionDelete);
    let v3 = by_role(WriterRole::V3Materialize);
    let writer_backends = |role: WriterRole| {
        backends
            .iter()
            .filter(|be| {
                be["writer_groups"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|group| group["role"] == role.name())
            })
            .count()
    };
    let shard_backends = backends
        .iter()
        .filter(|be| !be["position_shard_groups"].as_array().unwrap().is_empty())
        .count();
    let nonempty = shards
        .iter()
        .map(|group| group["nonempty_authors"].as_u64().unwrap())
        .sum::<u64>();
    let shard_stages = shards
        .iter()
        .map(|group| group["stage"].as_u64().unwrap())
        .collect::<BTreeSet<_>>();
    let distinct_singleton = v3.len() == 1
        && shard_stages.len() == 1
        && v3[0]["stage"]
            .as_u64()
            .is_some_and(|stage| !shard_stages.contains(&stage));

    ensure!(
        data.iter()
            .all(|group| group["stage"].as_u64().is_some_and(|stage| {
                !shard_stages.contains(&stage)
                    && v3
                        .iter()
                        .all(|writer| writer["stage"].as_u64() != Some(stage))
            })),
        "data writers cannot share the position aggregation or materialization stage"
    );

    match geometry {
        Geometry::DataAndEmptyDelete => {
            ensure!(
                writer_backends(WriterRole::Data) >= 2
                    && v2.is_empty()
                    && distinct_singleton
                    && nonempty == 0,
                "unmatched MERGE requires distributed data writers and empty shard output with one materializer: {backends:?}"
            );
        }
        Geometry::SameFileDelete | Geometry::MultiFileDelete | Geometry::MixedSameFile => {
            let expects_data = matches!(geometry, Geometry::MixedSameFile);
            ensure!(
                (!data.is_empty()) == expects_data && v2.is_empty() && distinct_singleton,
                "v3 mutation requires one distinct materialization writer stage and its declared data branch: {backends:?}"
            );
            if matches!(geometry, Geometry::MultiFileDelete) {
                ensure!(
                    shard_backends >= 2 && nonempty > 0,
                    "multi-file v3 DELETE requires multiple authenticated aggregation backends and real shard output: {backends:?}"
                );
            } else {
                ensure!(
                    nonempty == 1,
                    "one source file needs exactly one nonempty shard author: {backends:?}"
                );
            }
        }
        Geometry::V2Delete => {
            ensure!(
                data.is_empty()
                    && v3.is_empty()
                    && shards.is_empty()
                    && writer_backends(WriterRole::V2PositionDelete) >= 2,
                "v2 DELETE must retain multiple distributed position writers: {backends:?}"
            );
        }
    }
    Ok(())
}

fn inventory(
    stdout: &str,
    binding: Option<&StatementExecutionBinding>,
    execution_text: &str,
    dop: i32,
) -> Result<serde_json::Value> {
    anyhow::ensure!(dop > 0, "writer DOP request must be positive");
    let mut raw = Vec::new();
    let mut prepared = BTreeMap::<String, Prepared>::new();
    for line in stdout.lines() {
        let Some(fields) = fields(line, PREPARED)? else {
            continue;
        };
        if field(&fields, "execution_id")? != execution_text {
            continue;
        }
        let binding =
            binding.context("prepared Task marker lacks its authenticated execution binding")?;
        let execution = binding.execution()?;
        anyhow::ensure!(
            execution_text
                == format!(
                    "{}:{}:{}",
                    execution.query_id().high(),
                    execution.query_id().low(),
                    execution.attempt_id().get()
                ),
            "Task marker selected another statement execution"
        );
        let process = binding.process()?;
        anyhow::ensure!(
            field(&fields, "backend")?.parse::<novarocks_types::BackendProcessId>()? == process,
            "prepared Task marker names another BE process"
        );
        let identity = TaskIdentity::new(
            execution,
            StageId::new(number(&fields, "stage")?)?,
            TaskId::new(number(&fields, "task")?)?,
            process,
        );
        let task_key = task_key(&fields)?;
        let task = Prepared {
            identity,
            dop: number(&fields, "dop")?,
        };
        anyhow::ensure!(
            prepared.insert(task_key, task).is_none(),
            "one prepared task_key has multiple Task observations"
        );
        raw.push(line.to_owned());
    }
    let mut groups = BTreeMap::<(u32, String, u32), Writers>::new();
    for line in stdout.lines() {
        let Some(fields) = fields(line, OPENED)? else {
            continue;
        };
        if field(&fields, "execution_id")? != execution_text {
            continue;
        }
        binding.context("writer marker lacks its authenticated execution binding")?;
        let task_key = task_key(&fields)?;
        let task = prepared
            .get(&task_key)
            .context("opened writer has no exact prepared Task")?;
        let role = WriterRole::read(field(&fields, "role")?)?;
        let expected_dop = role.dop(dop);
        ensure!(
            task.dop == expected_dop,
            "opened {role:?} writer Task has actual descriptor DOP {}, expected {expected_dop}",
            task.dop
        );
        let target = number::<u32>(&fields, "write_target")?;
        let ordinal = number::<u32>(&fields, "writer_ordinal")?;
        let driver = number::<u32>(&fields, "driver_id")?;
        let plan_node = number::<i32>(&fields, "plan_node")?;
        let group = groups
            .entry((target, task_key, ordinal))
            .or_insert_with(|| Writers {
                task: task.identity,
                plan_node,
                role,
                drivers: BTreeSet::new(),
            });
        anyhow::ensure!(
            group.plan_node == plan_node && group.task == task.identity && group.role == role,
            "physical writer group combines different node/Task owners"
        );
        anyhow::ensure!(
            group.drivers.insert(driver),
            "one driver-local writer was observed twice"
        );
        raw.push(line.to_owned());
    }
    let mut writers = Vec::new();
    let mut observed_opens = 0_usize;
    for ((target, task_key, ordinal), group) in groups {
        let descriptor_dop = group.role.dop(dop);
        let expected = (0..u32::try_from(descriptor_dop)?).collect::<BTreeSet<_>>();
        anyhow::ensure!(
            group.drivers == expected,
            "Task {} target {target} writer ordinal {ordinal} actual driver set {:?}, expected {:?}",
            group.task,
            group.drivers,
            expected
        );
        observed_opens += group.drivers.len();
        writers.push(
            serde_json::json!({"task":group.task.to_string(),"task_key":task_key,
            "stage":group.task.stage_id().get(),"target":target,"writer_ordinal":ordinal,
            "plan_node":group.plan_node,"driver_ids":group.drivers,"descriptor_dop":descriptor_dop,
            "role":group.role.name()}),
        );
    }
    let mut shard_groups = BTreeMap::<String, BTreeMap<u32, Option<u64>>>::new();
    for line in stdout.lines() {
        let (fields, finished) = if let Some(fields) = fields(line, SHARD_OPENED)? {
            (fields, false)
        } else if let Some(fields) = fields(line, SHARD_FINISHED)? {
            (fields, true)
        } else {
            continue;
        };
        if field(&fields, "execution_id")? != execution_text {
            continue;
        }
        let key = task_key(&fields)?;
        let task = prepared
            .get(&key)
            .context("shard driver has no exact prepared Task")?;
        ensure!(
            task.dop == dop,
            "shard Task {} has actual DOP {}, expected {dop}",
            task.identity,
            task.dop
        );
        let driver = number::<u32>(&fields, "driver_id")?;
        if finished {
            let emitted = shard_groups
                .get_mut(&key)
                .and_then(|drivers| drivers.get_mut(&driver))
                .context("finished shard driver never opened")?;
            ensure!(emitted.is_none(), "shard driver finished twice");
            *emitted = Some(number(&fields, "emitted_rows")?);
        } else {
            ensure!(
                shard_groups
                    .entry(key)
                    .or_default()
                    .insert(driver, None)
                    .is_none(),
                "shard driver opened twice"
            );
        }
        raw.push(line.to_owned());
    }
    let expected = (0..u32::try_from(dop)?).collect::<BTreeSet<_>>();
    let mut shards = Vec::new();
    for (key, drivers) in shard_groups {
        let task = &prepared[&key];
        let ordinals = drivers.keys().copied().collect::<BTreeSet<_>>();
        ensure!(
            ordinals == expected,
            "shard Task {} opened drivers {ordinals:?}, expected {expected:?}",
            task.identity
        );
        let emitted = drivers.values().try_fold(0_u64, |sum, rows| {
            sum.checked_add(rows.context("shard driver did not reach genuine EOS")?)
                .context("shard emitted rows overflow")
        })?;
        let authors = drivers
            .values()
            .filter(|rows| rows.is_some_and(|rows| rows > 0))
            .count();
        shards.push(
            serde_json::json!({"task":task.identity.to_string(),"task_key":key,
            "stage":task.identity.stage_id().get(),"driver_rows":drivers,"descriptor_dop":task.dop,
            "nonempty_authors":authors,"emitted_rows":emitted}),
        );
    }
    // The original private binding retains its run token. The derived report
    // needs process/execution attribution, never another copy of that token.
    let attribution = binding.map(|binding| {
        serde_json::json!({
            "schema_version":binding.schema_version,
            "target_session_id":binding.target_session_id,
            "backend_index":binding.backend_index,
            "backend_process_id":binding.backend_process_id,
            "query_id_hi":binding.query_id_hi,"query_id_lo":binding.query_id_lo,
            "attempt_id":binding.attempt_id,
        })
    });
    Ok(
        serde_json::json!({"validated_binding_attribution":attribution,"writer_groups":writers,"position_shard_groups":shards,"observed_writer_opens":observed_opens,"original_markers":raw}),
    )
}

/// Only this statement's append interval is eligible. The previous selected
/// execution was really closed before its successor starts; no reader grant
/// exists for the metadata/seed/read statements between the two DELETEs.
fn reader_inventory(
    before: &str,
    after: &str,
    binding: Option<&StatementExecutionBinding>,
) -> Result<(serde_json::Value, u64, u64)> {
    let delta = after
        .strip_prefix(before)
        .context("original reader stdout was replaced or truncated")?;
    let mut sources = BTreeSet::new();
    let mut markers = Vec::new();
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    for line in delta.lines() {
        let Some(fields) = fields(line, READER_CLOSE)? else {
            continue;
        };
        binding.context("reader CLOSE lacks its authenticated execution binding")?;
        let provider = field(&fields, "provider")?;
        let instance = field(&fields, "instance")?;
        let catalog_version = field(&fields, "catalog_version")?;
        let sequence = number::<u64>(&fields, "scheduled_split_sequence_id")?;
        anyhow::ensure!(
            provider == "iceberg" && sequence > 0,
            "one old Iceberg file observation names another provider or invalid split"
        );
        anyhow::ensure!(
            sources.insert((provider, instance, catalog_version, sequence)),
            "one original reader split closed more than once in this execution window"
        );
        rows = rows
            .checked_add(number::<u64>(&fields, "rows_decoded")?)
            .context("reader decoded rows overflow")?;
        batches = batches
            .checked_add(number::<u64>(&fields, "batches_delivered")?)
            .context("reader delivered batches overflow")?;
        markers.push(line.to_owned());
    }
    Ok((
        serde_json::json!({
            "stdout_prefix_bytes":before.len(),"rows_decoded":rows,
            "batches_delivered":batches,"original_close_markers":markers,
        }),
        rows,
        batches,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};

    fn binding() -> StatementExecutionBinding {
        StatementExecutionBinding::new(
            "row-level-be".into(),
            "row-level-statement".into(),
            0,
            BackendProcessId::new_v7(),
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
        )
        .unwrap()
    }
    fn prepared(binding: &StatementExecutionBinding, task_key: &str, dop: i32) -> String {
        format!(
            "{PREPARED} execution_id=1:2:1 stage=1 task=2 backend={} task_key={task_key} dop={dop}\n",
            binding.backend_process_id
        )
    }
    fn writer(task_key: &str, driver: u32) -> String {
        format!(
            "{OPENED} execution_id=1:2:1 task_key={task_key} plan_node=1 write_target=0 driver_id={driver} writer_ordinal=0 role=data\n"
        )
    }
    #[test]
    fn actual_writer_sets_require_every_driver_of_each_exact_prepared_task() {
        let binding = binding();
        for dop in [1, 4] {
            let mut log = prepared(&binding, "a:b", dop);
            for driver in 0..dop as u32 {
                log.push_str(&writer("a:b", driver));
            }
            let observed = inventory(&log, Some(&binding), "1:2:1", dop).unwrap();
            assert_eq!(
                observed["writer_groups"][0]["driver_ids"]
                    .as_array()
                    .unwrap()
                    .len(),
                dop as usize
            );
            assert!(inventory(&log, Some(&binding), "1:2:1", dop + 1).is_err());
            assert!(inventory(&log, None, "1:2:1", dop).is_err());
            assert!(
                inventory(
                    &(log.clone() + &writer("a:b", 0)),
                    Some(&binding),
                    "1:2:1",
                    dop
                )
                .is_err()
            );
            assert!(inventory(&(log + &writer("c:d", 0)), Some(&binding), "1:2:1", dop).is_err());
        }
    }
    #[test]
    fn one_missing_driver_or_wrong_backend_cannot_be_hidden_by_another_writer() {
        let binding = binding();
        let mut log = prepared(&binding, "a:b", 4) + &prepared(&binding, "c:d", 4);
        for driver in 0..4 {
            log.push_str(&writer("c:d", driver));
        }
        for driver in 0..3 {
            log.push_str(&writer("a:b", driver));
        }
        assert!(inventory(&log, Some(&binding), "1:2:1", 4).is_err());
        let wrong = log.replace(
            &binding.backend_process_id,
            &BackendProcessId::new_v7().to_string(),
        );
        assert!(inventory(&wrong, Some(&binding), "1:2:1", 4).is_err());
    }
    #[test]
    fn shard_roster_requires_genuine_eos_and_distinct_singleton_writer() {
        let binding = binding();
        let mut log = prepared(&binding, "a:b", 4)
            + &prepared(&binding, "c:d", 1).replace("stage=1 task=2", "stage=2 task=3");
        log.push_str(&writer("c:d", 0).replace("role=data", "role=v3_materialize"));
        let mut final_driver_eos = String::new();
        for driver in 0..4 {
            log.push_str(&format!(
                "{SHARD_OPENED} execution_id=1:2:1 task_key=a:b driver_id={driver}\n"
            ));
            let finish = format!(
                "{SHARD_FINISHED} execution_id=1:2:1 task_key=a:b driver_id={driver} emitted_rows={}\n",
                u64::from(driver == 1)
            );
            if driver == 3 {
                final_driver_eos = finish;
            } else {
                log.push_str(&finish);
            }
        }
        assert!(inventory(&log, Some(&binding), "1:2:1", 4).is_err());
        log.push_str(&final_driver_eos);
        let backend = inventory(&log, Some(&binding), "1:2:1", 4).unwrap();
        assert_eq!(backend["writer_groups"][0]["descriptor_dop"], 1);
        assert_eq!(backend["position_shard_groups"][0]["descriptor_dop"], 4);
        validate_geometry(std::slice::from_ref(&backend), Geometry::SameFileDelete).unwrap();
        assert!(validate_geometry(&[backend], Geometry::MultiFileDelete).is_err());
        let wrong_stage = log.replace("stage=2 task=3", "stage=1 task=3");
        let backend = inventory(&wrong_stage, Some(&binding), "1:2:1", 4).unwrap();
        assert!(validate_geometry(&[backend], Geometry::SameFileDelete).is_err());
    }
    fn reader(sequence: u64, rows: u64, batches: u64) -> String {
        format!(
            "{READER_CLOSE} provider=iceberg instance=original-catalog catalog_version=ab12 scheduled_split_sequence_id={sequence} rows_decoded={rows} batches_delivered={batches}\n"
        )
    }
    #[test]
    fn reader_batch_inventory_uses_only_the_original_statement_append_window() {
        let binding = binding();
        let before = reader(1, 999_999, 999_999);
        let after = before.clone() + &reader(1, 18_000, 5);
        let (facts, rows, batches) = reader_inventory(&before, &after, Some(&binding)).unwrap();
        assert_eq!((rows, batches), (18_000, 5));
        assert_eq!(facts["original_close_markers"].as_array().unwrap().len(), 1);
        assert!(reader_inventory(&before, &after, None).is_err());
        assert!(reader_inventory(&before, &reader(1, 18_000, 5), Some(&binding)).is_err());
        assert!(reader_inventory(&before, &(after + &reader(1, 1, 1)), Some(&binding)).is_err());
    }
    #[test]
    fn reader_close_without_actual_batch_metrics_is_not_cross_batch_evidence() {
        let binding = binding();
        let old = format!(
            "{READER_CLOSE} provider=iceberg instance=original-catalog catalog_version=ab12 scheduled_split_sequence_id=1\n"
        );
        assert!(reader_inventory("", &old, Some(&binding)).is_err());
        let (_, rows, batches) =
            reader_inventory("", &reader(1, 18_000, 1), Some(&binding)).unwrap();
        assert_eq!(
            (rows, batches),
            (18_000, 1),
            "row count never invents another delivered batch"
        );
    }
}
