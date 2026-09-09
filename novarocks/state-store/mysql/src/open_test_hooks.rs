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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};

static OPEN_GATES: OnceLock<Mutex<HashMap<String, Arc<OpenGateState>>>> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MysqlOpenGatePhase {
    AfterAdvisoryLock,
    AfterReadOnlyStart,
}

struct OpenGateState {
    database: String,
    phase: MysqlOpenGatePhase,
    reached: AtomicBool,
    released: AtomicBool,
    completed: AtomicBool,
    connection_id: AtomicU64,
    reached_notify: Notify,
    released_notify: Notify,
    completed_notify: Notify,
}

#[doc(hidden)]
pub struct MysqlOpenGateControl {
    state: Arc<OpenGateState>,
}

pub(crate) struct MysqlOpenGate {
    state: Arc<OpenGateState>,
}

pub fn arm_mysql_open_gate(
    database: &str,
    phase: MysqlOpenGatePhase,
) -> Result<MysqlOpenGateControl, StateStoreError> {
    let state = Arc::new(OpenGateState {
        database: database.to_owned(),
        phase,
        reached: AtomicBool::new(false),
        released: AtomicBool::new(false),
        completed: AtomicBool::new(false),
        connection_id: AtomicU64::new(0),
        reached_notify: Notify::new(),
        released_notify: Notify::new(),
        completed_notify: Notify::new(),
    });
    let mut gates = gate_registry().lock().map_err(|_| hook_error())?;
    if gates.contains_key(database) {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "a MySQL open test gate is already armed for the database",
        ));
    }
    gates.insert(database.to_owned(), Arc::clone(&state));
    Ok(MysqlOpenGateControl { state })
}

pub(crate) fn take_mysql_open_gate(
    database: &str,
    phase: MysqlOpenGatePhase,
) -> Option<MysqlOpenGate> {
    let mut gates = match gate_registry().lock() {
        Ok(gates) => gates,
        Err(poisoned) => poisoned.into_inner(),
    };
    if gates
        .get(database)
        .is_some_and(|state| state.phase == phase)
    {
        gates.remove(database).map(|state| MysqlOpenGate { state })
    } else {
        None
    }
}

impl MysqlOpenGate {
    pub(crate) async fn pause(&self, connection_id: u64) {
        self.state
            .connection_id
            .store(connection_id, Ordering::Release);
        self.state.reached.store(true, Ordering::Release);
        self.state.reached_notify.notify_waiters();
        wait_flag(&self.state.released, &self.state.released_notify).await;
    }
}

impl Drop for MysqlOpenGate {
    fn drop(&mut self) {
        self.state.completed.store(true, Ordering::Release);
        self.state.completed_notify.notify_waiters();
    }
}

impl MysqlOpenGateControl {
    pub async fn wait_reached(&self) {
        wait_flag(&self.state.reached, &self.state.reached_notify).await;
    }

    pub fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.released_notify.notify_waiters();
    }

    pub async fn wait_completed(&self) {
        wait_flag(&self.state.completed, &self.state.completed_notify).await;
    }

    pub fn connection_id(&self) -> u64 {
        self.state.connection_id.load(Ordering::Acquire)
    }
}

impl Drop for MysqlOpenGateControl {
    fn drop(&mut self) {
        let mut gates = match gate_registry().lock() {
            Ok(gates) => gates,
            Err(poisoned) => poisoned.into_inner(),
        };
        if gates
            .get(&self.state.database)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            gates.remove(&self.state.database);
        }
        self.release();
    }
}

async fn wait_flag(flag: &AtomicBool, notify: &Notify) {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if flag.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

fn gate_registry() -> &'static Mutex<HashMap<String, Arc<OpenGateState>>> {
    OPEN_GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn hook_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "MySQL open test gate registry is poisoned",
    )
}
