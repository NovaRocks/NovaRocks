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

//! Exact q1 Task evidence and joined fixture actors; no new authority source.
use anyhow::{Context, Result, ensure};
use novarocks_failpoint::target_binding::StatementExecutionBinding;
use std::thread;

pub(super) fn created_for(line: &str, binding: &StatementExecutionBinding) -> Result<bool> {
    let fields = line.split_whitespace().collect::<Vec<_>>();
    if !fields.contains(&novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED) {
        return Ok(false);
    }
    let field = |name: &str| -> Result<&str> {
        let prefix = format!("{name}=");
        let mut values = fields
            .iter()
            .filter_map(|value| value.strip_prefix(&prefix));
        let value = values
            .next()
            .with_context(|| format!("TaskCreate omitted {name}"))?;
        ensure!(values.next().is_none(), "TaskCreate duplicated {name}");
        Ok(value)
    };
    let execution = format!(
        "{}:{}:{}",
        binding.query_id_hi, binding.query_id_lo, binding.attempt_id
    );
    if field("execution_id")? != execution || field("backend")? != binding.backend_process_id {
        return Ok(false);
    }
    ensure!(
        field("stage")?.parse::<u32>()? > 0 && field("task")?.parse::<u32>()? > 0,
        "TaskCreate has invalid exact task identity"
    );
    Ok(true)
}

pub(super) struct JoinedThread<T>(Option<thread::JoinHandle<T>>);
impl<T> JoinedThread<T> {
    pub fn new(thread: thread::JoinHandle<T>) -> Self {
        Self(Some(thread))
    }
    pub fn join(mut self) -> thread::Result<T> {
        self.0.take().expect("follow-up actor exists").join()
    }
}
impl<T> Drop for JoinedThread<T> {
    fn drop(&mut self) {
        if let Some(actor) = self.0.take() {
            let _ = actor.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};
    fn binding() -> StatementExecutionBinding {
        StatementExecutionBinding::new(
            "exact-session".into(),
            "q1".into(),
            0,
            BackendProcessId::new_v7(),
            QueryExecutionId::new(QueryId::new(10, 20), AttemptId::new(1).unwrap()).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn q1_creation_does_not_credit_q2_or_another_backend() {
        let binding = binding();
        let line = format!(
            "{} execution_id=10:20:1 stage=2 task=3 backend={}",
            novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
            binding.backend_process_id
        );
        assert!(created_for(&line, &binding).unwrap());
        assert!(!created_for(&line.replace("10:20:1", "10:21:1"), &binding).unwrap());
        assert!(
            !created_for(
                &line.replace(
                    &binding.backend_process_id,
                    &BackendProcessId::new_v7().to_string()
                ),
                &binding
            )
            .unwrap()
        );
        assert!(created_for(&format!("{line} execution_id=10:20:1"), &binding).is_err());
        assert!(created_for(&line.replace("task=3", "task=0"), &binding).is_err());
    }
    #[test]
    fn failure_cleanup_cannot_finish_while_follow_up_actor_is_still_live() {
        let (release, blocked) = std::sync::mpsc::sync_channel(0);
        let (started, drop_started) = std::sync::mpsc::sync_channel(0);
        let (completed, drop_completed) = std::sync::mpsc::sync_channel(0);
        let actor = JoinedThread::new(thread::spawn(move || {
            blocked.recv().unwrap();
        }));
        let cleanup = thread::spawn(move || {
            started.send(()).unwrap();
            drop(actor);
            completed.send(()).unwrap();
        });
        drop_started.recv().unwrap();
        let premature = drop_completed.recv_timeout(std::time::Duration::from_millis(20));
        release.send(()).unwrap();
        drop_completed
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        cleanup.join().unwrap();
        assert!(matches!(
            premature,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
    }
}
