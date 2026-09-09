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

//! This provider's run of the shared StateStore behaviour suite.
//!
//! The suite lives in `novarocks-state-store-testkit` and is the same one the
//! reference in-memory store and the other providers answer to. It runs from
//! inside the crate rather than from `tests/` for one reason: the attempt group
//! needs an instance whose ceiling is small enough for saturation to be
//! observable, and choosing that ceiling is not something a server should be
//! able to configure.
//!
//! # Why every open is a new file
//!
//! One SQLite file has one owner, enforced by an exclusive lock, and the
//! attempt group deliberately opens two instances at once to check that a
//! capability from one is refused by the other. Each call therefore gets its
//! own database inside a directory that outlives the whole suite.

use std::cell::{Cell, RefCell};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_state_store_api::{
    Key, Precondition, StateStore, StateStoreLimits, StateStoreOpenRequest, Value,
};
use novarocks_state_store_testkit::conformance::{
    FaultGate, FaultInjectingStateStore, FaultStateStoreFactory, PostDispatchControl,
    PostDispatchController, PostDispatchScenario, StateStoreFactory, StateStoreFaultFixture,
    run_attempt_suite, run_basic_suite, run_fault_suite,
};
use tempfile::TempDir;

use super::SqliteStateStore;
use super::txn::{
    Mutation, TRANSACTION_ENVELOPE_BYTES, TestGate, accounted_mutation_bytes, provisional_version,
};

/// Small on purpose: the attempt group fills the instance to its ceiling, and
/// the contract default of 1024 would say nothing about the accounting.
const CONFORMANCE_ATTEMPT_CAPACITY: usize = 4;
const CONFORMANCE_MAX_KEY_BYTES: usize = 64;
const CONFORMANCE_MAX_VALUE_BYTES: usize = 64;
const CONFORMANCE_MAX_PAGE_SIZE: usize = 10;
const CONFORMANCE_MAX_OPERATIONS: usize = 8;
/// The key width the suite's budget cases use.
const CONFORMANCE_BUDGET_KEY_BYTES: usize = 3;

/// Limits sized so the suite's budget expectations describe this provider.
///
/// The suite asks for two things at once: that `max_transaction_operations`
/// mutations of one byte each fit, and that exactly four mutations of
/// `max_value_bytes` fit while the fifth does not. What a mutation costs is the
/// provider's own accounting, so the byte ceiling is derived from that
/// accounting rather than copied from another provider's number.
fn conformance_limits(sample: &SqliteStateStore) -> StateStoreLimits {
    let (attempt, observation) = sample.attempts.reserve().expect("reserve sizing attempt");
    let charge = |value_bytes: usize| {
        let key =
            Key::try_from(Bytes::from(vec![7; CONFORMANCE_BUDGET_KEY_BYTES])).expect("sizing key");
        let mutation = Mutation::Put {
            value: Value::try_from(Bytes::from(vec![7; value_bytes])).expect("sizing value"),
            precondition: Precondition::Any,
            provisional_version: provisional_version(attempt.id(), 1),
        };
        accounted_mutation_bytes(&key, &mutation).expect("sizing charge")
    };
    let small = charge(1);
    let large = charge(CONFORMANCE_MAX_VALUE_BYTES);
    drop(attempt);
    drop(observation);

    let staged = std::cmp::max(CONFORMANCE_MAX_OPERATIONS * small, 4 * large);
    let max_transaction_bytes = TRANSACTION_ENVELOPE_BYTES + staged;
    assert!(
        max_transaction_bytes < TRANSACTION_ENVELOPE_BYTES + 5 * large,
        "the suite requires a budget that funds four maximum values and refuses the fifth"
    );
    StateStoreLimits {
        max_key_bytes: CONFORMANCE_MAX_KEY_BYTES,
        max_value_bytes: CONFORMANCE_MAX_VALUE_BYTES,
        max_page_size: CONFORMANCE_MAX_PAGE_SIZE,
        max_transaction_operations: CONFORMANCE_MAX_OPERATIONS,
        max_transaction_bytes,
        ..StateStoreLimits::default()
    }
}

/// A directory of databases that outlives every instance opened from it.
struct SuiteDirectory {
    directory: TempDir,
    opened: Cell<u64>,
    limits: RefCell<Option<StateStoreLimits>>,
}

impl SuiteDirectory {
    fn new() -> Rc<Self> {
        Rc::new(Self {
            directory: TempDir::new().expect("conformance temporary directory"),
            opened: Cell::new(0),
            limits: RefCell::new(None),
        })
    }

    fn next_path(&self) -> PathBuf {
        let ordinal = self.opened.get();
        self.opened.set(ordinal + 1);
        self.directory
            .path()
            .join(format!("state-store-{ordinal}.sqlite"))
    }

    /// The suite's limits, measured once against a throwaway instance.
    async fn limits(&self) -> StateStoreLimits {
        if let Some(limits) = self.limits.borrow().clone() {
            return limits;
        }
        let sample = SqliteStateStore::open(self.next_path(), request(StateStoreLimits::default()))
            .await
            .expect("open sizing instance");
        let limits = conformance_limits(&sample);
        *self.limits.borrow_mut() = Some(limits.clone());
        limits
    }
}

async fn open_instance(directory: &Rc<SuiteDirectory>) -> Arc<SqliteStateStore> {
    let limits = directory.limits().await;
    Arc::new(
        SqliteStateStore::open_with_capacity(
            directory.next_path(),
            request(limits),
            NonZeroUsize::new(CONFORMANCE_ATTEMPT_CAPACITY).expect("conformance capacity"),
        )
        .await
        .expect("open conformance instance"),
    )
}

fn request(limits: StateStoreLimits) -> StateStoreOpenRequest {
    StateStoreOpenRequest {
        cluster_id: "conformance-cluster".to_owned(),
        limits,
        deadline: Instant::now() + Duration::from_secs(30),
    }
}

fn factory() -> StateStoreFactory {
    let directory = SuiteDirectory::new();
    Rc::new(move || {
        let directory = Rc::clone(&directory);
        Box::pin(async move {
            let store: Arc<dyn StateStore> = open_instance(&directory).await;
            Ok(store)
        })
    })
}

/// Holds a commit worker at the point where it is dispatched but has not
/// applied, so the fault group's scenarios are staged against the real worker
/// rather than a simulation of one.
struct SqlitePostDispatchController {
    fault: Arc<FaultInjectingStateStore>,
    store: Arc<SqliteStateStore>,
}

#[async_trait]
impl PostDispatchController for SqlitePostDispatchController {
    async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl> {
        let gate = FaultGate::new();
        let hold = self.store.arm_commit_hold();
        match scenario {
            PostDispatchScenario::CancelWaiterBeforeApply => {
                self.fault.pause_next_post_dispatch(gate.clone())
            }
            PostDispatchScenario::LoseCommittedResponse => {
                self.fault.lose_next_post_dispatch_response(gate.clone())
            }
        }
        Box::new(SqlitePostDispatchControl { gate, hold })
    }
}

struct SqlitePostDispatchControl {
    gate: FaultGate,
    hold: TestGate,
}

#[async_trait]
impl PostDispatchControl for SqlitePostDispatchControl {
    async fn wait_dispatched(&self) {
        self.gate.wait_reached().await;
        self.gate.wait_armed().await;
    }

    async fn wait_waiter_cancelled(&self) {
        self.gate.wait_cancelled().await;
    }

    async fn allow_provider_progress(&self) {
        // Waiting for the worker to arrive first is what makes "before apply"
        // mean it: the release cannot outrun the thread it is releasing.
        self.hold.wait_reached().await;
        self.hold.release().await;
    }

    async fn release_response(&self) {
        self.gate.release().await;
    }

    async fn wait_inner_dropped(&self) {
        self.gate.wait_inner_dropped().await;
    }
}

fn fault_factory() -> FaultStateStoreFactory {
    let directory = SuiteDirectory::new();
    Rc::new(move || {
        let directory = Rc::clone(&directory);
        Box::pin(async move {
            let sqlite = open_instance(&directory).await;
            let store: Arc<dyn StateStore> = Arc::clone(&sqlite) as Arc<dyn StateStore>;
            let fault = FaultInjectingStateStore::new(store);
            let controller: Arc<dyn PostDispatchController> =
                Arc::new(SqlitePostDispatchController {
                    fault: Arc::clone(&fault),
                    store: sqlite,
                });
            Ok(StateStoreFaultFixture::new(fault, controller))
        })
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_satisfies_the_basic_suite() {
    run_basic_suite(&factory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_satisfies_the_attempt_suite() {
    run_attempt_suite(&factory()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_satisfies_the_fault_suite() {
    run_fault_suite(&fault_factory()).await;
}
