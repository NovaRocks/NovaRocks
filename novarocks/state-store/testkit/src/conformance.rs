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

//! The behaviour every StateStore provider is expected to satisfy.
//!
//! The suite is split into three groups because they demand different things
//! of a provider:
//!
//! * [`run_basic_suite`] -- isolation, conflicts, preconditions, paging,
//!   limits, and atomicity. It needs nothing but an open store, so every
//!   provider runs it.
//! * [`run_attempt_suite`] -- write-attempt identity, terminal stability,
//!   in-doubt honesty, and admission accounting. Also store-only, and also
//!   mandatory: these are the rules a provider is most likely to get subtly
//!   wrong.
//! * [`run_fault_suite`] -- what happens when a commit is cancelled, its
//!   answer is lost, or its result is ambiguous. It needs a provider-supplied
//!   [`PostDispatchController`] that can genuinely hold a commit mid-flight, so
//!   only providers that can offer one run it.
//!
//! A provider with no fault controller implements [`StateStoreFactory`] alone
//! and still runs the two mandatory groups.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use novarocks_state_store_api::{
    AttemptOutcome, AttemptSupervisor, CommitObservation, CommitOutcome, CommitReceipt, Direction,
    Key, KeyRange, MAX_KEY_BYTES, Precondition, RangePage, RangeRequest, ReadTransaction,
    StateRecord, StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits, StoreIdentity,
    Value, WriteAttempt, WriteTransaction,
};
use tokio::sync::{oneshot, watch};

/// Opens one store instance. Every call must open a *fresh* instance: the
/// attempt group checks that a capability from one instance is refused by
/// another, which only means anything if two calls are two instances.
pub type StoreFuture =
    Pin<Box<dyn Future<Output = Result<Arc<dyn StateStore>, StateStoreError>> + 'static>>;
pub type StateStoreFactory = Rc<dyn Fn() -> StoreFuture>;

/// Opens a store together with the control surface the fault group needs.
pub type FaultStoreFuture =
    Pin<Box<dyn Future<Output = Result<StateStoreFaultFixture, StateStoreError>> + 'static>>;
pub type FaultStateStoreFactory = Rc<dyn Fn() -> FaultStoreFuture>;

pub struct StateStoreFaultFixture {
    pub store: Arc<dyn StateStore>,
    pub post_dispatch: Arc<dyn PostDispatchController>,
}

impl StateStoreFaultFixture {
    pub fn new(store: Arc<dyn StateStore>, post_dispatch: Arc<dyn PostDispatchController>) -> Self {
        Self {
            store,
            post_dispatch,
        }
    }
}

/// Storage isolation, conflict detection, preconditions, paging, limits, and
/// atomicity. Mandatory for every provider.
pub async fn run_basic_suite(factory: &StateStoreFactory) {
    snapshot_repeatable_read(factory).await;
    same_key_conflict(factory).await;
    write_skew_conflict(factory).await;
    range_phantom_conflict(factory).await;
    preconditions(factory).await;
    forward_reverse_pages(factory).await;
    limits_before_io(factory).await;
    arbitrary_binary_payloads(factory).await;
    atomic_commit(factory).await;
}

/// Write-attempt identity, terminal stability, in-doubt honesty, and admission
/// accounting. Mandatory for every provider.
pub async fn run_attempt_suite(factory: &StateStoreFactory) {
    attempt_scope_is_instance_local(factory).await;
    a_terminal_outcome_never_flips(factory).await;
    an_attempt_without_a_dispatch_is_not_committed(factory).await;
    a_published_terminal_outlives_its_evidence(factory).await;
    an_unresolved_answer_is_not_a_denial(factory).await;
    admission_saturates_without_stalling_observation(factory).await;
}

/// Cancellation after dispatch, response loss, and commit ambiguity. Only for
/// providers that can supply a real [`PostDispatchController`].
pub async fn run_fault_suite(factory: &FaultStateStoreFactory) {
    post_dispatch_cancel_reconciles(factory).await;
    post_dispatch_response_loss_reconciles(factory).await;
    commit_ambiguity_is_resolved_by_the_attempt(factory).await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostDispatchScenario {
    CancelWaiterBeforeApply,
    LoseCommittedResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedTerminal {
    Committed,
    NotCommitted,
    Either,
}

impl PostDispatchScenario {
    pub const fn expected_terminal(self) -> ExpectedTerminal {
        match self {
            // A provider may or may not have applied before the waiter went
            // away; both are honest, and the suite checks the store agrees
            // with whichever it reports.
            Self::CancelWaiterBeforeApply => ExpectedTerminal::Either,
            Self::LoseCommittedResponse => ExpectedTerminal::Committed,
        }
    }
}

/// Arms one mid-commit fault on the next commit of the fixture's store.
#[async_trait]
pub trait PostDispatchController: Send + Sync {
    async fn arm(&self, scenario: PostDispatchScenario) -> Box<dyn PostDispatchControl>;
}

#[async_trait]
pub trait PostDispatchControl: Send + Sync {
    async fn wait_dispatched(&self);
    async fn wait_waiter_cancelled(&self);
    async fn allow_provider_progress(&self);
    async fn release_response(&self);
    async fn wait_inner_dropped(&self);
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn key(bytes: impl Into<Vec<u8>>) -> Key {
    Key::try_from(Bytes::from(bytes.into())).expect("valid conformance key")
}

fn value(bytes: impl Into<Vec<u8>>) -> Value {
    Value::try_from(Bytes::from(bytes.into())).expect("valid conformance value")
}

async fn open(factory: &StateStoreFactory) -> Arc<dyn StateStore> {
    factory().await.expect("open conformance state store")
}

async fn open_fault(factory: &FaultStateStoreFactory) -> StateStoreFaultFixture {
    factory().await.expect("open conformance fault fixture")
}

/// Reserves one attempt and begins the write it authorises.
///
/// The observation comes back to the caller because the attempt itself is
/// consumed by the store: after this, the handle is the only way to learn what
/// happened.
async fn begin_attempt(
    store: &Arc<dyn StateStore>,
    purpose: &str,
) -> (Box<dyn WriteTransaction>, CommitObservation) {
    let (attempt, observation) = store
        .attempts()
        .reserve()
        .expect("reserve a conformance write attempt");
    let transaction = store
        .begin_write(attempt, purpose)
        .await
        .expect("begin conformance write");
    assert_eq!(
        transaction.attempt(),
        observation.id(),
        "a transaction must run under the attempt that authorised it"
    );
    (transaction, observation)
}

/// Commits, then checks the attempt's verdict says the same thing the caller
/// was told, and releases the observation so the slot is freed.
async fn commit_attempt(
    transaction: Box<dyn WriteTransaction>,
    observation: CommitObservation,
) -> CommitOutcome {
    let outcome = transaction.commit().await;
    let verdict = observation.outcome().await.expect("attempt verdict");
    match (&outcome, &verdict) {
        (CommitOutcome::Committed(receipt), AttemptOutcome::Committed(published)) => {
            assert_eq!(
                receipt, published,
                "the published receipt must be the one the caller was handed"
            );
        }
        (
            CommitOutcome::Conflict(_)
            | CommitOutcome::TransientBeforeCommit(_)
            | CommitOutcome::DefiniteFailure(_),
            AttemptOutcome::NotCommitted,
        ) => {}
        // Ambiguity is the one answer a caller may not act on, so the attempt
        // is allowed to be anything the store can actually prove.
        (CommitOutcome::CommitUnknown(_), _) => {}
        (outcome, verdict) => panic!(
            "a witnessed commit outcome and its attempt verdict disagree: outcome={outcome:?}, verdict={verdict:?}"
        ),
    }
    outcome
}

/// Aborts, checks the attempt is proven effect-free, and releases the slot.
async fn abort_attempt(transaction: Box<dyn WriteTransaction>, observation: CommitObservation) {
    transaction.abort().await.expect("abort conformance write");
    assert_eq!(
        observation.outcome().await.expect("attempt verdict"),
        AttemptOutcome::NotCommitted,
        "an aborted write never reached storage, so it is proven not committed"
    );
}

async fn commit_puts(store: &Arc<dyn StateStore>, rows: &[(Key, Value)]) -> CommitReceipt {
    let (mut transaction, observation) = begin_attempt(store, "state store conformance seed").await;
    for (key, value) in rows {
        transaction
            .put(key.clone(), value.clone(), Precondition::Any)
            .await
            .expect("stage conformance seed");
    }
    committed(commit_attempt(transaction, observation).await)
}

fn committed(outcome: CommitOutcome) -> CommitReceipt {
    match outcome {
        CommitOutcome::Committed(receipt) => receipt,
        other => panic!("expected committed outcome, got {other:?}"),
    }
}

fn assert_conflict(outcome: CommitOutcome) {
    assert!(matches!(outcome, CommitOutcome::Conflict(_)), "{outcome:?}");
}

async fn read_record(store: &Arc<dyn StateStore>, item: &Key) -> Option<StateRecord> {
    let mut reader = store.begin_read().await.expect("begin conformance read");
    let record = reader.get(item).await.expect("read conformance record");
    reader.abort().await.expect("abort conformance read");
    record
}

fn conformance_range(prefix: u8, direction: Direction, page_size: usize) -> RangeRequest {
    RangeRequest {
        range: KeyRange::new(key(vec![prefix, 0]), key(vec![prefix, 0xff]))
            .expect("bounded conformance range"),
        direction,
        page_size,
        continuation: None,
    }
}

async fn collect_pages(store: &Arc<dyn StateStore>, mut request: RangeRequest) -> Vec<Vec<u8>> {
    let mut reader = store.begin_read().await.expect("begin paginated read");
    let mut keys = Vec::new();
    loop {
        let page = reader.range(&request).await.expect("read range page");
        keys.extend(
            page.records
                .iter()
                .map(|record| record.key.as_bytes().to_vec()),
        );
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    reader.abort().await.expect("abort paginated read");
    keys
}

// ---------------------------------------------------------------------------
// Basic group
// ---------------------------------------------------------------------------

pub async fn snapshot_repeatable_read(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c01/snapshot".to_vec());
    commit_puts(&store, &[(item.clone(), value(b"before".to_vec()))]).await;
    let mut reader = store.begin_read().await.expect("begin snapshot read");
    let before = reader.get(&item).await.expect("first snapshot read");
    assert_eq!(
        before.as_ref().expect("initial snapshot value").value,
        value(b"before".to_vec())
    );
    commit_puts(&store, &[(item.clone(), value(b"after".to_vec()))]).await;
    assert_eq!(
        reader.get(&item).await.expect("repeat snapshot read"),
        before
    );
    reader.abort().await.expect("abort snapshot read");
}

pub async fn same_key_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c02/same-key".to_vec());
    commit_puts(&store, &[(item.clone(), value(b"initial".to_vec()))]).await;
    let (mut first, first_observation) = begin_attempt(&store, "same key first").await;
    let (mut second, second_observation) = begin_attempt(&store, "same key second").await;
    first.get(&item).await.expect("establish first snapshot");
    second.get(&item).await.expect("establish second snapshot");
    first
        .put(item.clone(), value(b"first".to_vec()), Precondition::Any)
        .await
        .expect("stage first write");
    second
        .put(item, value(b"second".to_vec()), Precondition::Any)
        .await
        .expect("stage second write");
    committed(commit_attempt(first, first_observation).await);
    assert_conflict(commit_attempt(second, second_observation).await);
}

pub async fn write_skew_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let left = key(b"c03/left".to_vec());
    let right = key(b"c03/right".to_vec());
    commit_puts(
        &store,
        &[
            (left.clone(), value(b"on".to_vec())),
            (right.clone(), value(b"on".to_vec())),
        ],
    )
    .await;
    let (mut first, first_observation) = begin_attempt(&store, "write skew first").await;
    let (mut second, second_observation) = begin_attempt(&store, "write skew second").await;
    for item in [&left, &right] {
        first.get(item).await.expect("first skew read");
        second.get(item).await.expect("second skew read");
    }
    first
        .delete(left, Precondition::Any)
        .await
        .expect("stage first skew delete");
    second
        .delete(right, Precondition::Any)
        .await
        .expect("stage second skew delete");
    committed(commit_attempt(first, first_observation).await);
    assert_conflict(commit_attempt(second, second_observation).await);
}

pub async fn range_phantom_conflict(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let request = conformance_range(4, Direction::Forward, 10);
    let (mut first, first_observation) = begin_attempt(&store, "phantom first").await;
    let (mut second, second_observation) = begin_attempt(&store, "phantom second").await;
    first.range(&request).await.expect("first phantom range");
    second.range(&request).await.expect("second phantom range");
    first
        .put(key(vec![4, 1]), value(b"first".to_vec()), Precondition::Any)
        .await
        .expect("stage first phantom");
    second
        .put(
            key(vec![4, 2]),
            value(b"second".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage second phantom");
    committed(commit_attempt(first, first_observation).await);
    assert_conflict(commit_attempt(second, second_observation).await);

    let deleted = key(vec![14, 1]);
    commit_puts(&store, &[(deleted.clone(), value(b"present".to_vec()))]).await;
    let delete_request = conformance_range(14, Direction::Forward, 10);
    let (mut delete_first, delete_first_observation) =
        begin_attempt(&store, "delete phantom first").await;
    let (mut delete_second, delete_second_observation) =
        begin_attempt(&store, "delete phantom second").await;
    delete_first
        .range(&delete_request)
        .await
        .expect("first delete phantom range");
    delete_second
        .range(&delete_request)
        .await
        .expect("second delete phantom range");
    delete_first
        .delete(deleted, Precondition::Any)
        .await
        .expect("stage phantom delete");
    delete_second
        .put(
            key(vec![14, 2]),
            value(b"insert".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage competing phantom insert");
    committed(commit_attempt(delete_first, delete_first_observation).await);
    assert_conflict(commit_attempt(delete_second, delete_second_observation).await);
}

pub async fn preconditions(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"c05/item".to_vec());
    let (mut absent, absent_observation) = begin_attempt(&store, "absent precondition").await;
    absent
        .put(item.clone(), value(b"v1".to_vec()), Precondition::Absent)
        .await
        .expect("stage absent write");
    committed(commit_attempt(absent, absent_observation).await);
    let original = read_record(&store, &item)
        .await
        .expect("precondition record");

    let (mut present, present_observation) = begin_attempt(&store, "present precondition").await;
    present
        .put(item.clone(), value(b"v2".to_vec()), Precondition::Present)
        .await
        .expect("stage present write");
    committed(commit_attempt(present, present_observation).await);

    let (mut versioned, versioned_observation) =
        begin_attempt(&store, "version precondition").await;
    versioned
        .put(
            item.clone(),
            value(b"v3".to_vec()),
            Precondition::Version(
                read_record(&store, &item)
                    .await
                    .expect("current version")
                    .version,
            ),
        )
        .await
        .expect("stage version write");
    committed(commit_attempt(versioned, versioned_observation).await);

    let (mut stale, stale_observation) = begin_attempt(&store, "stale precondition").await;
    stale
        .put(
            item.clone(),
            value(b"stale".to_vec()),
            Precondition::Version(original.version.clone()),
        )
        .await
        .expect("stage stale write");
    assert_conflict(commit_attempt(stale, stale_observation).await);

    let (mut absent_failure, absent_failure_observation) =
        begin_attempt(&store, "absent precondition failure").await;
    absent_failure
        .put(
            item.clone(),
            value(b"absent-failure".to_vec()),
            Precondition::Absent,
        )
        .await
        .expect("stage absent failure");
    assert_conflict(commit_attempt(absent_failure, absent_failure_observation).await);

    let missing = key(b"c05/missing".to_vec());
    let (mut present_failure, present_failure_observation) =
        begin_attempt(&store, "present precondition failure").await;
    present_failure
        .put(
            missing.clone(),
            value(b"present-failure".to_vec()),
            Precondition::Present,
        )
        .await
        .expect("stage present failure");
    assert_conflict(commit_attempt(present_failure, present_failure_observation).await);

    let (mut missing_version, missing_version_observation) =
        begin_attempt(&store, "missing version failure").await;
    missing_version
        .delete(missing, Precondition::Version(original.version.clone()))
        .await
        .expect("stage missing version failure");
    assert_conflict(commit_attempt(missing_version, missing_version_observation).await);

    let (mut any, any_observation) = begin_attempt(&store, "any precondition").await;
    any.delete(item, Precondition::Any)
        .await
        .expect("stage any delete");
    committed(commit_attempt(any, any_observation).await);
}

pub async fn forward_reverse_pages(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let rows = (1_u8..=5)
        .map(|suffix| (key(vec![6, suffix]), value(vec![suffix])))
        .collect::<Vec<_>>();
    commit_puts(&store, &rows).await;
    let forward = collect_pages(&store, conformance_range(6, Direction::Forward, 2)).await;
    let reverse = collect_pages(&store, conformance_range(6, Direction::Reverse, 2)).await;
    let expected = rows
        .iter()
        .map(|(key, _)| key.as_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(forward, expected);
    assert_eq!(reverse, expected.into_iter().rev().collect::<Vec<_>>());

    let boundary_rows = [
        (key(vec![0]), value(vec![0])),
        (key(vec![0, 0xff]), value(vec![1])),
        (key(vec![0xff]), value(vec![2])),
        (key(vec![0xff, 0xff]), value(vec![3])),
    ];
    commit_puts(&store, &boundary_rows).await;
    let boundary_request = RangeRequest {
        range: KeyRange::new(key(Vec::new()), key(vec![0xff, 0xff, 0xff]))
            .expect("bounded binary edge range"),
        direction: Direction::Forward,
        page_size: 2,
        continuation: None,
    };
    let mut reader = store.begin_read().await.expect("begin token read");
    let first = reader
        .range(&boundary_request)
        .await
        .expect("first binary edge page");
    let token = first.continuation.expect("binary edge continuation");
    assert_eq!(first.records.len(), 2);
    let wrong_direction = RangeRequest {
        direction: Direction::Reverse,
        continuation: Some(token.clone()),
        ..boundary_request.clone()
    };
    assert_eq!(
        reader
            .range(&wrong_direction)
            .await
            .expect_err("token direction mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    let wrong_range = RangeRequest {
        range: KeyRange::new(key(vec![0]), key(vec![0xff, 0xff, 0xff]))
            .expect("different token range"),
        continuation: Some(token),
        ..boundary_request.clone()
    };
    assert_eq!(
        reader
            .range(&wrong_range)
            .await
            .expect_err("token range mismatch")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    reader.abort().await.expect("abort token read");

    let (mut writer, writer_observation) = begin_attempt(&store, "write range freeze").await;
    assert!(
        writer
            .range(&boundary_request)
            .await
            .expect("paginated write range")
            .continuation
            .is_some()
    );
    assert_eq!(
        writer
            .put(key(vec![1]), value(vec![1]), Precondition::Any)
            .await
            .expect_err("write must freeze after continuation")
            .kind(),
        StateStoreErrorKind::InvalidRequest
    );
    abort_attempt(writer, writer_observation).await;
}

pub async fn limits_before_io(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let limits = store.limits().clone();
    assert!(limits.max_key_bytes < MAX_KEY_BYTES);
    let oversized = key(vec![11; limits.max_key_bytes + 1]);
    let visible = key(b"c11/visible".to_vec());
    let mut reader = store.begin_read().await.expect("begin limited read");
    assert_eq!(
        reader
            .get(&oversized)
            .await
            .expect_err("reject oversized get")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    commit_puts(&store, &[(visible.clone(), value(b"new".to_vec()))]).await;
    assert!(
        reader
            .get(&visible)
            .await
            .expect("valid get after limit rejection")
            .is_some()
    );
    reader.abort().await.expect("abort limited read");

    let mut page_reader = store.begin_read().await.expect("begin page-limit read");
    assert_eq!(
        page_reader
            .range(&RangeRequest {
                page_size: limits.max_page_size + 1,
                ..conformance_range(11, Direction::Forward, 1)
            })
            .await
            .expect_err("reject oversized page")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    let page_visible = key(b"c11/page-visible".to_vec());
    commit_puts(&store, &[(page_visible.clone(), value(b"new".to_vec()))]).await;
    assert!(
        page_reader
            .get(&page_visible)
            .await
            .expect("valid get after page rejection")
            .is_some()
    );
    page_reader.abort().await.expect("abort page-limit read");

    let (mut value_writer, value_observation) = begin_attempt(&store, "value budget").await;
    assert_eq!(
        value_writer
            .put(
                key(b"c11/value".to_vec()),
                value(vec![0; limits.max_value_bytes + 1]),
                Precondition::Any,
            )
            .await
            .expect_err("reject oversized value")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    abort_attempt(value_writer, value_observation).await;

    let (mut writer, writer_observation) = begin_attempt(&store, "operation budget").await;
    for index in 0..limits.max_transaction_operations {
        writer
            .put(
                key(vec![11, (index >> 8) as u8, index as u8]),
                value(b"v".to_vec()),
                Precondition::Any,
            )
            .await
            .expect("stage operation within budget");
    }
    assert_eq!(
        writer
            .put(
                key(vec![11, 0xff, 0xff]),
                value(b"v".to_vec()),
                Precondition::Any
            )
            .await
            .expect_err("reject operation over budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    committed(commit_attempt(writer, writer_observation).await);

    let (mut byte_writer, byte_observation) = begin_attempt(&store, "byte budget").await;
    for suffix in 1_u8..=4 {
        byte_writer
            .put(
                key(vec![11, 0xfe, suffix]),
                value(vec![suffix; limits.max_value_bytes]),
                Precondition::Any,
            )
            .await
            .expect("stage mutation within byte budget");
    }
    assert_eq!(
        byte_writer
            .put(
                key(vec![11, 0xfe, 5]),
                value(vec![5; limits.max_value_bytes]),
                Precondition::Any,
            )
            .await
            .expect_err("reject transaction over byte budget")
            .kind(),
        StateStoreErrorKind::LimitExceeded
    );
    committed(commit_attempt(byte_writer, byte_observation).await);
}

pub async fn arbitrary_binary_payloads(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(vec![13, 0, 0xff, 0, 0xfe]);
    let payload = value(vec![0xff, 0, 0xfe, 0, 0xfd]);
    commit_puts(&store, &[(item.clone(), payload.clone())]).await;
    let record = read_record(&store, &item)
        .await
        .expect("read arbitrary binary row");
    assert_eq!(record.key, item);
    assert_eq!(record.value, payload);
    assert!(!record.version.as_bytes().is_empty());
}

pub async fn atomic_commit(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let guard = key(b"c08/guard".to_vec());
    let partial = key(b"c08/partial".to_vec());
    commit_puts(&store, &[(guard.clone(), value(b"original".to_vec()))]).await;
    let stale = read_record(&store, &guard).await.expect("guard version");
    commit_puts(&store, &[(guard.clone(), value(b"new".to_vec()))]).await;

    // One failing precondition rejects the whole envelope, including the row
    // that would have been perfectly acceptable on its own.
    let (mut transaction, observation) = begin_attempt(&store, "atomic conflict").await;
    transaction
        .put(
            partial.clone(),
            value(b"must-not-commit".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage partial row");
    transaction
        .put(
            guard.clone(),
            value(b"stale".to_vec()),
            Precondition::Version(stale.version),
        )
        .await
        .expect("stage conflicting row");
    assert_conflict(commit_attempt(transaction, observation).await);
    assert_eq!(read_record(&store, &partial).await, None);
    assert_eq!(
        read_record(&store, &guard).await.expect("guard row").value,
        value(b"new".to_vec()),
        "a rejected envelope must not disturb the row it lost to"
    );

    // An explicit abort makes the same all-or-nothing statement.
    let abandoned = key(b"c08/abandoned".to_vec());
    let (mut aborted, aborted_observation) = begin_attempt(&store, "atomic abort").await;
    aborted
        .put(
            abandoned.clone(),
            value(b"must-not-commit".to_vec()),
            Precondition::Any,
        )
        .await
        .expect("stage abandoned row");
    abort_attempt(aborted, aborted_observation).await;
    assert_eq!(read_record(&store, &abandoned).await, None);

    // The remaining commit outcomes are ones a healthy store has no reason to
    // produce, so they are scripted. The wrapper is built here out of the same
    // store, which is why this still asks nothing of the provider.
    let scripted = FaultInjectingStateStore::new(Arc::clone(&store));
    let scripted_store: Arc<dyn StateStore> = scripted.clone();
    for (suffix, result) in [
        ("committed", ScriptedCommitResult::Committed),
        ("conflict", ScriptedCommitResult::Conflict),
        (
            "transient-before-commit",
            ScriptedCommitResult::TransientBeforeCommit,
        ),
        ("definite-failure", ScriptedCommitResult::DefiniteFailure),
    ] {
        let item = key(format!("c08/scripted-{suffix}").into_bytes());
        let (mut transaction, observation) =
            begin_attempt(&scripted_store, "scripted commit outcome").await;
        transaction
            .put(item.clone(), value(b"scripted".to_vec()), Precondition::Any)
            .await
            .expect("stage scripted row");
        scripted.script_next_pre_commit(result);
        // `commit_attempt` is what checks the caller's outcome and the
        // attempt's verdict tell the same story.
        let outcome = commit_attempt(transaction, observation).await;
        let durable = read_record(&store, &item).await;
        match result {
            ScriptedCommitResult::Committed => {
                committed(outcome);
                assert_eq!(
                    durable.expect("a committed row must be readable").value,
                    value(b"scripted".to_vec())
                );
            }
            ScriptedCommitResult::Conflict => {
                assert!(matches!(outcome, CommitOutcome::Conflict(_)), "{outcome:?}");
                assert_eq!(durable, None, "a conflicted commit leaves nothing behind");
            }
            ScriptedCommitResult::TransientBeforeCommit => {
                assert!(
                    matches!(outcome, CommitOutcome::TransientBeforeCommit(_)),
                    "{outcome:?}"
                );
                assert_eq!(
                    durable, None,
                    "'before commit' is a claim about write effect, not just timing"
                );
            }
            ScriptedCommitResult::DefiniteFailure => {
                assert!(
                    matches!(outcome, CommitOutcome::DefiniteFailure(_)),
                    "{outcome:?}"
                );
                assert_eq!(durable, None, "a definite failure leaves nothing behind");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Attempt group
// ---------------------------------------------------------------------------

pub async fn attempt_scope_is_instance_local(factory: &StateStoreFactory) {
    let issuer = open(factory).await;
    let other = open(factory).await;
    assert_ne!(
        issuer.attempts().scope(),
        other.attempts().scope(),
        "each opened instance mints its own scope"
    );

    let (attempt, observation) = issuer
        .attempts()
        .reserve()
        .expect("reserve on the issuing instance");
    assert_eq!(observation.id().scope(), issuer.attempts().scope());
    let rejected = other
        .begin_write(attempt, "capability from another instance")
        .await
        .err()
        .expect("a capability from another instance must be refused, not answered");
    assert_eq!(rejected.kind(), StateStoreErrorKind::InvalidRequest);

    // Refusal happens before anything is dispatched, so the attempt is proven
    // effect-free rather than left in doubt.
    assert_eq!(
        observation.outcome().await.expect("verdict"),
        AttemptOutcome::NotCommitted
    );
}

pub async fn a_terminal_outcome_never_flips(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"a02/terminal".to_vec());

    let (mut writer, committed_observation) = begin_attempt(&store, "terminal committed").await;
    writer
        .put(item.clone(), value(b"durable".to_vec()), Precondition::Any)
        .await
        .expect("stage committed row");
    let receipt = committed(writer.commit().await);
    assert_eq!(
        receipt.attempt,
        committed_observation.id(),
        "a receipt names the attempt that earned it"
    );
    let proven = AttemptOutcome::Committed(receipt);
    for round in 1..=3 {
        assert_eq!(
            committed_observation.outcome().await.expect("verdict"),
            proven,
            "a proven commit must not change on re-reading, round={round}"
        );
    }
    assert_eq!(
        committed_observation.peek().expect("peek"),
        Some(proven),
        "a terminal outcome is published, not re-derived on demand"
    );
    drop(committed_observation);

    // A proven denial is just as terminal.
    let stale = read_record(&store, &item).await.expect("staged row");
    commit_puts(&store, &[(item.clone(), value(b"moved-on".to_vec()))]).await;
    let (mut loser, loser_observation) = begin_attempt(&store, "terminal conflict").await;
    loser
        .put(
            item.clone(),
            value(b"stale".to_vec()),
            Precondition::Version(stale.version),
        )
        .await
        .expect("stage stale row");
    assert_conflict(loser.commit().await);
    for round in 1..=3 {
        assert_eq!(
            loser_observation.outcome().await.expect("verdict"),
            AttemptOutcome::NotCommitted,
            "a proven denial must not change on re-reading, round={round}"
        );
    }
    assert_eq!(
        read_record(&store, &item).await.expect("row").value,
        value(b"moved-on".to_vec()),
        "the denied attempt left nothing behind"
    );
}

pub async fn an_attempt_without_a_dispatch_is_not_committed(factory: &StateStoreFactory) {
    let store = open(factory).await;

    // Reserved and never handed to the store: there is nothing to be unsure of.
    let (attempt, observation) = store.attempts().reserve().expect("reserve");
    assert_eq!(
        observation.outcome().await.expect("verdict"),
        AttemptOutcome::NotCommitted
    );
    drop(attempt);
    assert_eq!(
        observation.outcome().await.expect("verdict after drop"),
        AttemptOutcome::NotCommitted
    );
    drop(observation);

    // Begun and staged, then aborted: still nothing reached storage.
    let item = key(b"a03/aborted".to_vec());
    let (mut transaction, observation) = begin_attempt(&store, "aborted attempt").await;
    transaction
        .put(item.clone(), value(b"never".to_vec()), Precondition::Any)
        .await
        .expect("stage abandoned row");
    abort_attempt(transaction, observation).await;
    assert_eq!(read_record(&store, &item).await, None);
}

pub async fn a_published_terminal_outlives_its_evidence(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"a04/published".to_vec());
    let (mut writer, observation) = begin_attempt(&store, "published terminal").await;
    writer
        .put(item.clone(), value(b"durable".to_vec()), Precondition::Any)
        .await
        .expect("stage published row");
    let receipt = committed(writer.commit().await);
    let proven = AttemptOutcome::Committed(receipt);

    // Published means recorded: reading it back costs no I/O, which is exactly
    // why the decision cannot depend on evidence that may already be gone.
    assert_eq!(observation.peek().expect("peek"), Some(proven.clone()));

    // Draining cleanup is what releases provider-private evidence. The verdict
    // must be indifferent to it.
    store
        .attempts()
        .drain_abandoned_attempts()
        .await
        .expect("drain cleanup debt");
    assert_eq!(observation.outcome().await.expect("verdict"), proven);
    assert_eq!(observation.peek().expect("peek"), Some(proven));
    assert_eq!(
        read_record(&store, &item).await.expect("row").value,
        value(b"durable".to_vec())
    );
}

pub async fn an_unresolved_answer_is_not_a_denial(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let item = key(b"a05/abandoned-dispatch".to_vec());
    let payload = value(b"in-doubt".to_vec());
    let (mut transaction, observation) = begin_attempt(&store, "abandoned dispatch").await;
    transaction
        .put(item.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage abandoned dispatch");

    // Poll the commit exactly once and then drop it. Whatever the provider put
    // in flight on that first poll now has nobody waiting for it. This needs no
    // fault hook, so every provider runs it.
    {
        let mut commit = transaction.commit();
        std::future::poll_fn(|context| {
            let _ = commit.as_mut().poll(context);
            Poll::Ready(())
        })
        .await;
        drop(commit);
    }

    // Committed, NotCommitted, and Unresolved are all legal answers here. What
    // is not legal is an answer the store's own contents contradict.
    let verdict = observation.outcome().await.expect("verdict");
    let record = read_record(&store, &item).await;
    match (&verdict, &record) {
        (AttemptOutcome::Committed(receipt), Some(row)) => {
            assert_eq!(receipt.attempt, observation.id());
            assert_eq!(row.value, payload);
        }
        (AttemptOutcome::Committed(_), None) => {
            panic!("an attempt reported committed must have left its row readable")
        }
        (AttemptOutcome::NotCommitted, None) => {}
        (AttemptOutcome::NotCommitted, Some(_)) => panic!(
            "a durable row was reported as not committed: absence of evidence is not evidence of absence"
        ),
        (AttemptOutcome::Unresolved, _) => {
            // Unresolved is not a terminal and confers no right to run the work
            // again, so nothing may be published for it.
            assert!(!verdict.is_terminal());
            assert_eq!(
                observation.peek().expect("peek"),
                None,
                "an undecided attempt must not be published as terminal"
            );
        }
    }

    // Asking again may resolve an unknown, but may never contradict a terminal.
    let again = observation.outcome().await.expect("verdict again");
    if verdict.is_terminal() {
        assert_eq!(again, verdict, "a terminal verdict must not be revisited");
    }
}

pub async fn admission_saturates_without_stalling_observation(factory: &StateStoreFactory) {
    let store = open(factory).await;
    let supervisor: &AttemptSupervisor = store.attempts();
    let capacity = supervisor.capacity();
    let already = supervisor.outstanding();
    assert!(
        already <= capacity,
        "an instance cannot be charged beyond its own ceiling"
    );

    // Reservation touches no storage, so filling the ceiling is pure admission
    // accounting.
    let mut held = Vec::with_capacity(capacity - already);
    for _ in already..capacity {
        held.push(supervisor.reserve().expect("reserve within the ceiling"));
    }
    assert_eq!(supervisor.outstanding(), capacity);

    let refused = supervisor
        .reserve()
        .expect_err("a full instance must refuse a new attempt");
    assert_eq!(refused.kind(), StateStoreErrorKind::Saturated);
    assert_ne!(
        refused.kind(),
        StateStoreErrorKind::LimitExceeded,
        "saturation is transient and effect-free, not a permanent property of the request"
    );

    // Admission is closed, but learning outcomes and reclaiming capacity are
    // not: a store that blocked those would deadlock itself at the ceiling.
    let (attempt, observation) = held.pop().expect("one attempt to observe");
    assert_eq!(
        observation
            .outcome()
            .await
            .expect("verdict while saturated"),
        AttemptOutcome::NotCommitted
    );
    supervisor
        .drain_abandoned_attempts()
        .await
        .expect("cleanup still drains while saturated");
    assert_eq!(
        supervisor.reserve().err().map(|error| error.kind()),
        Some(StateStoreErrorKind::Saturated),
        "observing an attempt does not free its slot while a handle is alive"
    );

    drop(attempt);
    drop(observation);
    let (recovered, recovered_observation) = supervisor
        .reserve()
        .expect("capacity returns once a resolved attempt releases its handles");
    drop(recovered);
    drop(recovered_observation);
    drop(held);
}

// ---------------------------------------------------------------------------
// Fault group
// ---------------------------------------------------------------------------

pub async fn post_dispatch_cancel_reconciles(factory: &FaultStateStoreFactory) {
    run_post_dispatch_scenario(factory, PostDispatchScenario::CancelWaiterBeforeApply).await;
}

pub async fn post_dispatch_response_loss_reconciles(factory: &FaultStateStoreFactory) {
    run_post_dispatch_scenario(factory, PostDispatchScenario::LoseCommittedResponse).await;
}

async fn run_post_dispatch_scenario(
    factory: &FaultStateStoreFactory,
    scenario: PostDispatchScenario,
) {
    let fixture = open_fault(factory).await;
    let store = fixture.store;
    let control = fixture.post_dispatch.arm(scenario).await;
    let item = key(match scenario {
        PostDispatchScenario::CancelWaiterBeforeApply => b"f01/post-dispatch-cancel".to_vec(),
        PostDispatchScenario::LoseCommittedResponse => b"f02/post-dispatch-response-loss".to_vec(),
    });
    let expected_value = value(b"authoritative".to_vec());
    let (mut transaction, observation) = begin_attempt(&store, "post-dispatch conformance").await;
    transaction
        .put(item.clone(), expected_value.clone(), Precondition::Any)
        .await
        .expect("stage post-dispatch row");
    let waiter = tokio::spawn(async move { transaction.commit().await });
    control.wait_dispatched().await;

    match scenario {
        PostDispatchScenario::CancelWaiterBeforeApply => {
            assert_repeated_unresolved(&observation, "before waiter cancellation").await;
        }
        PostDispatchScenario::LoseCommittedResponse => {
            let held = observation.outcome().await.expect("held verdict");
            assert!(
                matches!(
                    held,
                    AttemptOutcome::Unresolved | AttemptOutcome::Committed(_)
                ),
                "a held commit may be undecided or already proven, never denied: verdict={held:?}"
            );
        }
    }

    let terminal = match scenario {
        PostDispatchScenario::CancelWaiterBeforeApply => {
            waiter.abort();
            assert!(
                waiter
                    .await
                    .expect_err("cancel post-dispatch waiter")
                    .is_cancelled()
            );
            control.wait_waiter_cancelled().await;
            // Losing the waiter tells the store nothing about the write.
            assert_repeated_unresolved(&observation, "after waiter cancellation").await;
            control.release_response().await;
            control.wait_inner_dropped().await;
            control.allow_provider_progress().await;
            await_expected_terminal(&observation, scenario.expected_terminal()).await
        }
        PostDispatchScenario::LoseCommittedResponse => {
            control.allow_provider_progress().await;
            let terminal =
                await_expected_terminal(&observation, scenario.expected_terminal()).await;
            control.release_response().await;
            assert!(matches!(
                waiter.await.expect("join response-loss waiter"),
                CommitOutcome::CommitUnknown(_)
            ));
            control.wait_inner_dropped().await;
            terminal
        }
    };

    assert_terminal_matches_storage(&store, &observation, &item, &expected_value, &terminal).await;
}

pub async fn commit_ambiguity_is_resolved_by_the_attempt(factory: &FaultStateStoreFactory) {
    let fixture = open_fault(factory).await;
    let store = fixture.store;
    let control = fixture
        .post_dispatch
        .arm(PostDispatchScenario::LoseCommittedResponse)
        .await;
    let item = key(b"f03/ambiguous-commit".to_vec());
    let payload = value(b"durable-despite-the-unknown".to_vec());
    let (mut transaction, observation) = begin_attempt(&store, "commit ambiguity").await;
    transaction
        .put(item.clone(), payload.clone(), Precondition::Any)
        .await
        .expect("stage ambiguous row");
    let waiter = tokio::spawn(async move { transaction.commit().await });
    control.wait_dispatched().await;
    control.allow_provider_progress().await;
    control.release_response().await;

    let caller = waiter.await.expect("join ambiguous waiter");
    assert!(
        matches!(caller, CommitOutcome::CommitUnknown(_)),
        "the caller must be told the outcome is unknown rather than guessed: {caller:?}"
    );
    control.wait_inner_dropped().await;

    // The attempt owns the verdict, and here the verdict is Committed because
    // the write is durable. A store that read the caller's ignorance as a
    // denial would answer NotCommitted over a row it can still read.
    let terminal = await_expected_terminal(&observation, ExpectedTerminal::Committed).await;
    let AttemptOutcome::Committed(receipt) = &terminal else {
        panic!("an ambiguous commit that landed must resolve to Committed: {terminal:?}");
    };
    assert_eq!(receipt.attempt, observation.id());
    assert_eq!(
        read_record(&store, &item).await.expect("durable row").value,
        payload
    );
    assert_terminal_matches_storage(&store, &observation, &item, &payload, &terminal).await;
}

async fn assert_repeated_unresolved(observation: &CommitObservation, phase: &str) {
    for round in 1..=3 {
        assert_eq!(
            observation.outcome().await.expect("resolve held attempt"),
            AttemptOutcome::Unresolved,
            "a held commit must stay unresolved {phase}, round={round}",
        );
        assert_eq!(
            observation.peek().expect("peek held attempt"),
            None,
            "an unresolved attempt must publish nothing {phase}, round={round}",
        );
    }
}

async fn await_expected_terminal(
    observation: &CommitObservation,
    expected: ExpectedTerminal,
) -> AttemptOutcome {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let verdict = observation.outcome().await.expect("resolve attempt");
            match (&expected, &verdict) {
                (ExpectedTerminal::Committed, AttemptOutcome::Committed(_))
                | (ExpectedTerminal::NotCommitted, AttemptOutcome::NotCommitted)
                | (ExpectedTerminal::Either, AttemptOutcome::Committed(_))
                | (ExpectedTerminal::Either, AttemptOutcome::NotCommitted) => return verdict,
                (_, AttemptOutcome::Unresolved) => tokio::task::yield_now().await,
                _ => panic!(
                    "a post-dispatch attempt reached the wrong terminal: expected={expected:?}, actual={verdict:?}"
                ),
            }
        }
    })
    .await
    .expect("a post-dispatch attempt must reach a terminal state")
}

async fn assert_terminal_matches_storage(
    store: &Arc<dyn StateStore>,
    observation: &CommitObservation,
    item: &Key,
    expected_value: &Value,
    terminal: &AttemptOutcome,
) {
    let record = read_record(store, item).await;
    match terminal {
        AttemptOutcome::Committed(receipt) => {
            assert_eq!(receipt.attempt, observation.id());
            assert_eq!(
                record
                    .expect("a committed attempt leaves its row readable")
                    .value,
                expected_value.clone()
            );
        }
        AttemptOutcome::NotCommitted => {
            assert_eq!(record, None, "a denied attempt must leave nothing behind")
        }
        AttemptOutcome::Unresolved => panic!("a post-dispatch attempt stayed unresolved"),
    }
    for round in 1..=3 {
        assert_eq!(
            &observation.outcome().await.expect("repeat terminal"),
            terminal,
            "a post-dispatch terminal must not regress, round={round}"
        );
    }
}

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum ScriptedCommitResult {
    Committed,
    Conflict,
    TransientBeforeCommit,
    DefiniteFailure,
}

#[derive(Clone)]
pub struct FaultGate {
    reached: watch::Sender<bool>,
    armed: watch::Sender<bool>,
    cancelled: watch::Sender<bool>,
    inner_dropped: watch::Sender<bool>,
    release: watch::Sender<bool>,
}

impl FaultGate {
    pub fn new() -> Self {
        let (reached, _) = watch::channel(false);
        let (armed, _) = watch::channel(false);
        let (cancelled, _) = watch::channel(false);
        let (inner_dropped, _) = watch::channel(false);
        let (release, _) = watch::channel(false);
        Self {
            reached,
            armed,
            cancelled,
            inner_dropped,
            release,
        }
    }

    async fn pause(&self) {
        self.reached.send_replace(true);
        let mut release = self.release.subscribe();
        self.armed.send_replace(true);
        release
            .wait_for(|released| *released)
            .await
            .expect("fault gate release sender");
    }

    pub async fn wait_reached(&self) {
        let mut reached = self.reached.subscribe();
        reached
            .wait_for(|reached| *reached)
            .await
            .expect("fault gate reached sender");
    }

    pub async fn wait_armed(&self) {
        let mut armed = self.armed.subscribe();
        armed
            .wait_for(|armed| *armed)
            .await
            .expect("fault gate armed sender");
    }

    pub async fn wait_cancelled(&self) {
        let mut cancelled = self.cancelled.subscribe();
        cancelled
            .wait_for(|cancelled| *cancelled)
            .await
            .expect("fault gate cancellation sender");
    }

    fn publish_cancelled(&self) {
        self.cancelled.send_replace(true);
    }

    fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    fn publish_inner_dropped(&self) {
        self.inner_dropped.send_replace(true);
    }

    pub async fn wait_inner_dropped(&self) {
        let mut inner_dropped = self.inner_dropped.subscribe();
        inner_dropped
            .wait_for(|inner_dropped| *inner_dropped)
            .await
            .expect("fault gate inner-drop sender");
    }

    pub async fn release(&self) {
        self.release.send_replace(true);
    }
}

impl Default for FaultGate {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct FaultScript {
    begin: Option<StateStoreError>,
    operation: Option<StateStoreError>,
    pre_commit: Option<ScriptedCommitResult>,
    post_dispatch: Option<PostDispatchFault>,
}

struct PostDispatchFault {
    gate: FaultGate,
    lose_response: bool,
}

/// Wraps any store and injects failures a provider cannot be asked to produce
/// on demand. It owns no state of its own beyond the script, so it works over
/// every provider.
pub struct FaultInjectingStateStore {
    inner: Arc<dyn StateStore>,
    script: Arc<Mutex<FaultScript>>,
}

impl FaultInjectingStateStore {
    pub fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            script: Arc::new(Mutex::new(FaultScript::default())),
        })
    }

    pub fn fail_next_begin(&self, error: StateStoreError) {
        self.script.lock().expect("fault script").begin = Some(error);
    }

    pub fn fail_next_operation(&self, error: StateStoreError) {
        self.script.lock().expect("fault script").operation = Some(error);
    }

    pub fn script_next_pre_commit(&self, result: ScriptedCommitResult) {
        self.script.lock().expect("fault script").pre_commit = Some(result);
    }

    pub fn pause_next_post_dispatch(&self, gate: FaultGate) {
        self.script.lock().expect("fault script").post_dispatch = Some(PostDispatchFault {
            gate,
            lose_response: false,
        });
    }

    pub fn lose_next_post_dispatch_response(&self, gate: FaultGate) {
        self.script.lock().expect("fault script").post_dispatch = Some(PostDispatchFault {
            gate,
            lose_response: true,
        });
    }

    fn take_begin_error(&self) -> Option<StateStoreError> {
        self.script.lock().expect("fault script").begin.take()
    }
}

struct FaultReadTransaction {
    inner: Box<dyn ReadTransaction>,
    script: Arc<Mutex<FaultScript>>,
}

struct FaultWriteTransaction {
    inner: Box<dyn WriteTransaction>,
    script: Arc<Mutex<FaultScript>>,
}

fn take_operation_error(script: &Mutex<FaultScript>) -> Option<StateStoreError> {
    script.lock().expect("fault script").operation.take()
}

#[async_trait]
impl ReadTransaction for FaultReadTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.abort().await
    }
}

#[async_trait]
impl ReadTransaction for FaultWriteTransaction {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.abort().await
    }
}

fn scripted_failure(result: ScriptedCommitResult) -> CommitOutcome {
    let error =
        || StateStoreError::new(StateStoreErrorKind::Internal, "injected state store fault");
    match result {
        ScriptedCommitResult::Committed => unreachable!("committed faults use the real provider"),
        ScriptedCommitResult::Conflict => CommitOutcome::Conflict(error()),
        ScriptedCommitResult::TransientBeforeCommit => {
            CommitOutcome::TransientBeforeCommit(error())
        }
        ScriptedCommitResult::DefiniteFailure => CommitOutcome::DefiniteFailure(error()),
    }
}

struct FaultWaiterCancellation {
    gate: FaultGate,
    armed: bool,
}

impl FaultWaiterCancellation {
    fn new(gate: FaultGate) -> Self {
        Self { gate, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FaultWaiterCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.gate.publish_cancelled();
        }
    }
}

#[async_trait]
impl WriteTransaction for FaultWriteTransaction {
    fn attempt(&self) -> novarocks_state_store_api::AttemptId {
        self.inner.attempt()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        if let Some(error) = take_operation_error(&self.script) {
            return Err(error);
        }
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        let (pre_commit, post_dispatch) = {
            let mut script = self.script.lock().expect("fault script");
            (script.pre_commit.take(), script.post_dispatch.take())
        };
        if let Some(result) = pre_commit {
            if matches!(result, ScriptedCommitResult::Committed) {
                return self.inner.commit().await;
            }
            return match self.inner.abort().await {
                Ok(()) => scripted_failure(result),
                Err(error) => CommitOutcome::DefiniteFailure(error),
            };
        }
        let Some(post_dispatch) = post_dispatch else {
            return self.inner.commit().await;
        };
        let gate = post_dispatch.gate;
        let lose_response = post_dispatch.lose_response;
        let (outcome_tx, outcome_rx) = oneshot::channel();
        let supervisor_gate = gate.clone();
        tokio::spawn(async move {
            let mut commit = self.inner.commit();
            let mut ready = None;
            std::future::poll_fn(|context| {
                match commit.as_mut().poll(context) {
                    Poll::Ready(outcome) => ready = Some(outcome),
                    Poll::Pending => {}
                }
                Poll::Ready(())
            })
            .await;
            supervisor_gate.pause().await;
            if supervisor_gate.is_cancelled() {
                drop(commit);
                supervisor_gate.publish_inner_dropped();
                return;
            }
            let outcome = match ready {
                Some(outcome) => outcome,
                None => commit.await,
            };
            supervisor_gate.publish_inner_dropped();
            let delivered = if lose_response {
                CommitOutcome::CommitUnknown(StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "state store commit response was lost after dispatch",
                ))
            } else {
                outcome
            };
            let _ = outcome_tx.send(delivered);
        });
        let mut cancellation = FaultWaiterCancellation::new(gate);
        let outcome = outcome_rx.await.expect("fault commit supervisor");
        cancellation.disarm();
        outcome
    }
}

#[async_trait]
impl StateStore for FaultInjectingStateStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn attempts(&self) -> &AttemptSupervisor {
        // Attempts belong to the wrapped instance: a capability minted here has
        // to be one the real store will accept.
        self.inner.attempts()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        if let Some(error) = self.take_begin_error() {
            return Err(error);
        }
        Ok(Box::new(FaultReadTransaction {
            inner: self.inner.begin_read().await?,
            script: Arc::clone(&self.script),
        }))
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        if let Some(error) = self.take_begin_error() {
            // The attempt is dropped without ever being dispatched, so its slot
            // is released and no write effect can be attributed to it.
            return Err(error);
        }
        Ok(Box::new(FaultWriteTransaction {
            inner: self.inner.begin_write(attempt, purpose).await?,
            script: Arc::clone(&self.script),
        }))
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
}
