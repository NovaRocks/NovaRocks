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

//! Join narrow Native marker evidence only within the selected, finished Task.

use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_failpoint::observation_marker::{
    ObservationAction, PARTICIPATION_ABORT_APPLIED, TASK_CONSTANT_IPC_RECORD, TASK_CREATE_APPLIED,
    TASK_TERMINAL_RETAINED, TASK_VALUES_BACKING, TASK_VALUES_SOURCE,
};
use serde::Serialize;

use crate::scenario::ScenarioContext;
use crate::scenarios::native_sql_controls::SelectedStatement;

use super::{Case, GROUPED_ROWS, MIB};

#[derive(Debug, Default, Serialize)]
pub(super) struct TaskEvidence {
    backend_index: usize,
    backend_process: String,
    stage: u32,
    task: u32,
    created: bool,
    terminal: Option<String>,
    records: Vec<PoolRecord>,
    values: Vec<ValuesBacking>,
    sources: Vec<ValuesSource>,
}

#[derive(Debug, Serialize)]
struct PoolRecord {
    fragment: u32,
    pool_id: u32,
    rows: usize,
    retained_bytes: usize,
    body_bytes: usize,
}

#[derive(Debug, Serialize)]
struct ValuesBacking {
    fragment: u32,
    node_id: u32,
    backing: String,
    rows: usize,
    columns: usize,
    retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
struct ValuesSource {
    fragment: u32,
    node_id: u32,
    column: usize,
    pool_id: u32,
    first_ordinal: usize,
    length: usize,
}

impl TaskEvidence {
    fn witness(&self, case: Case, require_layout: bool) -> Result<bool> {
        if self.terminal.as_deref() != Some("FINISHED") {
            return Ok(false);
        }
        // The preserved expression and the materialized optional-fold product
        // are both eligible. Their placement decision is independently probed.
        if !require_layout || matches!(case, Case::Fold | Case::Translate) {
            return Ok(true);
        }
        let dimensions = case.dimensions();
        for values in &self.values {
            if (values.rows, values.columns) != dimensions {
                continue;
            }
            let sources = self
                .sources
                .iter()
                .filter(|source| {
                    source.fragment == values.fragment && source.node_id == values.node_id
                })
                .copied()
                .collect::<Vec<_>>();
            let Some(records) = self.full_coverage(values, &sources)? else {
                continue;
            };
            let matched = match case {
                Case::Smoke => true,
                Case::Grouped => matches!(sources.as_slice(), [source]
                    if source.first_ordinal == 0 && source.length == GROUPED_ROWS
                        && records[0].rows == GROUPED_ROWS),
                Case::Single => matches!(records.as_slice(), [record]
                    if record.retained_bytes > 8 * MIB && record.body_bytes > 8 * MIB),
                Case::Grid => {
                    values.retained_bytes > 16 * MIB
                        && records.iter().all(|record| {
                            (1..8 * MIB).contains(&record.retained_bytes)
                                && (1..8 * MIB).contains(&record.body_bytes)
                        })
                }
                Case::Fold | Case::Translate => {
                    unreachable!("only Task exit is required for fold controls")
                }
            };
            if matched {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn full_coverage<'a>(
        &'a self,
        values: &ValuesBacking,
        sources: &[ValuesSource],
    ) -> Result<Option<Vec<&'a PoolRecord>>> {
        ensure!(
            matches!(values.backing.as_str(), "batch" | "cells"),
            "unknown Native StaticValues backing {}",
            values.backing
        );
        let mut counts = vec![0usize; values.columns];
        let mut records = BTreeMap::new();
        for source in sources {
            let count = counts
                .get_mut(source.column)
                .context("Values source column is outside its backing")?;
            ensure!(
                source.length > 0,
                "Values source marker must cover a nonempty run"
            );
            *count = count
                .checked_add(source.length)
                .context("Values source row count overflow")?;
            let Some(record) = self.records.iter().find(|record| {
                record.fragment == source.fragment && record.pool_id == source.pool_id
            }) else {
                return Ok(None);
            };
            ensure!(
                source
                    .first_ordinal
                    .checked_add(source.length)
                    .is_some_and(|end| end <= record.rows),
                "Values source runs outside the accepted IPC pool {:?}",
                record
            );
            records.insert(record.pool_id, record);
        }
        if counts.iter().any(|count| *count != values.rows) {
            return Ok(None);
        }
        Ok(Some(records.into_values().collect()))
    }
}

pub(super) fn await_exact_tasks(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    execution: &str,
    participating: &BTreeSet<usize>,
    case: Case,
    require_layout: bool,
) -> Result<Vec<TaskEvidence>> {
    loop {
        let mut tasks = Vec::new();
        let mut witness = false;
        for &index in participating {
            let binding = context.handle().selected_native_observation_binding(
                index,
                ObservationAction::TaskProtocol.as_str(),
                &selected.token,
                None,
            )?;
            let Some(binding) = binding else { continue };
            ensure!(
                binding.backend_index == index
                    && binding.statement_run_token == selected.token
                    && format!(
                        "{}:{}:{}",
                        binding.query_id_hi, binding.query_id_lo, binding.attempt_id
                    ) == execution,
                "selected Native observation binding differs from the terminal statement attempt"
            );
            let log = context.handle().be_log_contents(index)?;
            for task in read_tasks(&log, index, &binding.backend_process_id, execution)? {
                witness |= task.witness(case, require_layout)?;
                tasks.push(task);
            }
        }
        if witness {
            return Ok(tasks);
        }
        let remaining = context.remaining(&format!(
            "observe finished {} Task with matching constant layout for {execution}; observed {tasks:?}",
            case.label()
        ))?;
        thread::sleep(remaining.min(Duration::from_millis(25)));
    }
}

pub(super) fn await_refused_attempt_tasks(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    execution: &str,
) -> Result<Vec<TaskEvidence>> {
    use crate::scenarios::task_evidence::{
        PARTICIPATION_ESTABLISH_APPLIED, RELEASE_APPLIED, backends_with_marker,
    };

    let action = ObservationAction::TaskProtocol.as_str();
    loop {
        let established =
            backends_with_marker(context, PARTICIPATION_ESTABLISH_APPLIED, execution)?;
        let released = backends_with_marker(context, RELEASE_APPLIED, execution)?;
        let aborted = backends_with_marker(context, PARTICIPATION_ABORT_APPLIED, execution)?;
        let mut tasks = Vec::new();
        for index in 0..context.handle().be_count() {
            ensure!(
                context
                    .handle()
                    .selected_native_observation_binding(index, action, &selected.token, Some(2),)?
                    .is_none(),
                "the oversized fragment started a replacement execution on BE {index}"
            );
            let binding = context.handle().selected_native_observation_binding(
                index,
                action,
                &selected.token,
                Some(1),
            )?;
            let Some(binding) = binding else { continue };
            ensure!(
                binding.backend_index == index
                    && binding.statement_run_token == selected.token
                    && format!(
                        "{}:{}:{}",
                        binding.query_id_hi, binding.query_id_lo, binding.attempt_id
                    ) == execution,
                "Native observation belongs to another execution"
            );
            let log = context.handle().be_log_contents(index)?;
            tasks.extend(read_tasks(
                &log,
                index,
                &binding.backend_process_id,
                execution,
            )?);
        }
        // TaskTerminalRetained is emitted by the original disposal worker
        // after it drops the physical Task payload. AbortApplied identifies
        // the failed participation's cleanup operation.
        if tasks.iter().all(|task| {
            task.created
                && task.terminal.is_some()
                && (released.contains(&task.backend_index) || aborted.contains(&task.backend_index))
        }) && established
            .iter()
            .all(|index| released.contains(index) || aborted.contains(index))
        {
            return Ok(tasks);
        }
        let remaining = context.remaining(&format!(
            "observe terminal Tasks and actual participation exits for {execution}; established={established:?}, released={released:?}, aborted={aborted:?}, tasks={tasks:?}",
        ))?;
        thread::sleep(remaining.min(Duration::from_millis(25)));
    }
}

fn read_tasks(
    log: &str,
    index: usize,
    backend_process: &str,
    execution: &str,
) -> Result<Vec<TaskEvidence>> {
    let mut tasks = BTreeMap::new();
    for line in log.lines() {
        let mut words = line.split_whitespace();
        let Some(marker) = words.find(|word| {
            matches!(
                *word,
                TASK_CREATE_APPLIED
                    | TASK_TERMINAL_RETAINED
                    | TASK_CONSTANT_IPC_RECORD
                    | TASK_VALUES_BACKING
                    | TASK_VALUES_SOURCE
            )
        }) else {
            continue;
        };
        let mut fields = BTreeMap::new();
        for word in words {
            let Some((key, value)) = word.split_once('=') else {
                continue;
            };
            ensure!(
                fields.insert(key, value).is_none(),
                "duplicate Native marker field {key}"
            );
        }
        let fields = Fields(fields);
        if fields.get("execution_id")? != execution {
            continue;
        }
        ensure!(
            fields.get("backend")? == backend_process,
            "Native marker came from another BE process"
        );
        let stage = fields.number("stage")?;
        let task = fields.number("task")?;
        let evidence = tasks.entry((stage, task)).or_insert_with(|| TaskEvidence {
            backend_index: index,
            backend_process: backend_process.to_owned(),
            stage,
            task,
            ..Default::default()
        });
        match marker {
            TASK_CREATE_APPLIED => evidence.created = true,
            TASK_TERMINAL_RETAINED => {
                let state = fields.get("state")?;
                ensure!(
                    evidence.terminal.as_deref().is_none_or(|old| old == state),
                    "conflicting Task terminal states"
                );
                evidence.terminal = Some(state.into());
            }
            TASK_CONSTANT_IPC_RECORD => {
                let record = PoolRecord {
                    fragment: fields.number("fragment")?,
                    pool_id: fields.number("pool_id")?,
                    rows: fields.number("rows")?,
                    retained_bytes: fields.number("retained_bytes")?,
                    body_bytes: fields.number("body_bytes")?,
                };
                ensure!(
                    !evidence
                        .records
                        .iter()
                        .any(|previous| (previous.fragment, previous.pool_id)
                            == (record.fragment, record.pool_id)),
                    "duplicate accepted IPC pool observation"
                );
                evidence.records.push(record);
            }
            TASK_VALUES_BACKING => {
                let values = ValuesBacking {
                    fragment: fields.number("fragment")?,
                    node_id: fields.number("node_id")?,
                    backing: fields.get("backing")?.into(),
                    rows: fields.number("rows")?,
                    columns: fields.number("columns")?,
                    retained_bytes: fields.number("retained_bytes")?,
                };
                ensure!(
                    !evidence
                        .values
                        .iter()
                        .any(|previous| (previous.fragment, previous.node_id)
                            == (values.fragment, values.node_id)),
                    "duplicate Native Values backing observation"
                );
                evidence.values.push(values);
            }
            TASK_VALUES_SOURCE => evidence.sources.push(ValuesSource {
                fragment: fields.number("fragment")?,
                node_id: fields.number("node_id")?,
                column: fields.number("column")?,
                pool_id: fields.number("pool_id")?,
                first_ordinal: fields.number("first_ordinal")?,
                length: fields.number("length")?,
            }),
            _ => unreachable!("matched observation marker"),
        }
    }
    Ok(tasks.into_values().collect())
}

struct Fields<'a>(BTreeMap<&'a str, &'a str>);
impl<'a> Fields<'a> {
    fn get(&self, key: &str) -> Result<&'a str> {
        self.0
            .get(key)
            .copied()
            .with_context(|| format!("Native constant marker omitted {key}"))
    }
    fn number<N: std::str::FromStr>(&self, key: &str) -> Result<N>
    where
        N::Err: std::error::Error + Send + Sync + 'static,
    {
        self.get(key)?
            .parse()
            .with_context(|| format!("invalid Native constant marker field {key}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_grid_source_cannot_borrow_ipc_or_exit_from_another_task() {
        let mut log = String::new();
        let mut event = |marker, task, fields: &str| {
            log.push_str(&format!(
                "{marker} execution_id=2:3:1 backend=process stage=0 task={task} {fields}\n"
            ));
        };
        event(TASK_TERMINAL_RETAINED, 1, "state=FINISHED");
        event(
            TASK_VALUES_BACKING,
            2,
            "fragment=0 node_id=0 backing=batch rows=2 columns=3 retained_bytes=18874368",
        );
        for column in 0..3 {
            event(
                TASK_CONSTANT_IPC_RECORD,
                1,
                &format!(
                    "fragment=0 pool_id={column} rows=2 retained_bytes=6291456 body_bytes=6291456"
                ),
            );
            event(
                TASK_VALUES_SOURCE,
                2,
                &format!(
                    "fragment=0 node_id=0 column={column} pool_id={column} first_ordinal=0 length=2"
                ),
            );
        }
        assert!(
            read_tasks(&log, 0, "process", "2:3:1")
                .unwrap()
                .iter()
                .all(|task| !task.witness(Case::Grid, true).unwrap())
        );
        log.push_str(&format!("{TASK_TERMINAL_RETAINED} execution_id=2:3:1 backend=process stage=0 task=2 state=FINISHED\n"));
        for column in 0..3 {
            log.push_str(&format!("{TASK_CONSTANT_IPC_RECORD} execution_id=2:3:1 backend=process stage=0 task=2 fragment=0 pool_id={column} rows=2 retained_bytes=6291456 body_bytes=6291456\n"));
        }
        let tasks = read_tasks(&log, 0, "process", "2:3:1").unwrap();
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.witness(Case::Grid, true).unwrap())
                .count(),
            1
        );
        assert!(read_tasks(&log, 0, "process", "2:3:11").unwrap().is_empty());
    }
}
