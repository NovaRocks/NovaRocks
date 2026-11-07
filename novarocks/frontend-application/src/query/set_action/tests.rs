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

//! Real planning-CPU ownership, cancellation and whole-SET refusal probes.
use super::*;
use crate::task_execution::blocking_io::tests::OriginalExecutorsFixture;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_query_application::sql::session::registry::SessionPropertyValue;
use novarocks_sql::compiler::{SqlCancellationObservation, SqlFunctionCatalog};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::sync::{
    Condvar, RwLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

struct CheckpointGate {
    armed: AtomicBool,
    cancelled: AtomicBool,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
    thread: Mutex<Option<std::thread::ThreadId>>,
}
impl CheckpointGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            release: Condvar::new(),
            thread: Mutex::new(None),
        })
    }
    fn release(&self) {
        *self.released.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.release.notify_all();
    }
}
impl SqlCancellationObservation for CheckpointGate {
    fn is_cancelled(&self) -> bool {
        if self.armed.swap(false, Ordering::SeqCst) {
            *self.thread.lock().unwrap() = Some(std::thread::current().id());
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                let waited = self
                    .release
                    .wait_timeout(released, Duration::from_secs(5))
                    .unwrap();
                released = waited.0;
                assert!(
                    !waited.1.timed_out() || *released,
                    "the test must release the actual CPU checkpoint"
                );
            }
        }
        self.cancelled.load(Ordering::SeqCst)
    }
}
struct ReleaseGate(Arc<CheckpointGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Fixture {
    executors: OriginalExecutorsFixture,
    cpu: Arc<FrontendPlanningCpuSource>,
    workload: WorkloadControl,
    root: RootWork,
    window: ResultWindowAlias,
    registry: Arc<SessionPropertyRegistry>,
    live: SessionSqlState,
}
impl Fixture {
    fn new(capacity: ResultCapacityConfig) -> Self {
        let (executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
            tokio::runtime::Handle::current(),
        );
        let cpu = FrontendPlanningCpuSource::new(supervisor);
        let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        workload.configure_result_capacity(capacity).unwrap();
        workload.mark_ready().unwrap();
        let (root, window) = workload
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let window = window.retain_alias();
        let registry = Arc::new(SessionPropertyRegistry::new().unwrap());
        let mut live = SessionSqlState::default();
        live.apply_effects(
            &novarocks_query_application::session_effects::SessionEffects::property(
                registry
                    .prepare_sql("pipeline_dop", SessionPropertyValue::Integer(7))
                    .unwrap(),
            ),
        )
        .unwrap();
        Self {
            executors,
            cpu,
            workload,
            root,
            window,
            registry,
            live,
        }
    }
    fn input(&self, sql: &str, control: SqlCompileControl) -> SessionPropertyCpuInput {
        let source: Arc<str> = Arc::from(sql);
        let mut statements = novarocks_parser::parse(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let ast::Statement::Session(ast::SessionStatement::Set(set)) = statements.pop().unwrap()
        else {
            panic!("one original SET")
        };
        let functions: Arc<dyn SqlFunctionCatalog> = Arc::new(
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .unwrap(),
        );
        SessionPropertyCpuInput {
            source,
            set,
            staged: self.live.clone(),
            registry: Arc::clone(&self.registry),
            interpreter: Arc::new(
                crate::query_execution::required_constant_eval::test_required_constant_interpreter(
                ),
            ),
            control,
            scope: self.root.owner.scope(),
            window: self.window.clone(),
            catalog: Arc::new(RwLock::new(
                novarocks_sql::planning::catalog::PlannerMemoryCatalog::default(),
            )),
            functions,
            policy: crate::application::test_constant_policy(),
        }
    }
    fn tail(&self) -> SetNativeTail {
        SetNativeTail {
            planning_cpu: Arc::clone(&self.cpu),
            preparation: None,
            prepared: None,
            health: None,
        }
    }
    async fn close(mut self, tail: SetNativeTail) {
        drop(tail);
        self.cpu.close().await.unwrap();
        drop(self.cpu);
        self.executors.shutdown().await;
        assert_eq!(
            self.window.retained_payload_bytes(),
            0,
            "output/source backing really dropped independently of CPU exit"
        );
        drop(self.window);
        self.root.owner.complete();
        self.root.business.release();
        assert_eq!(self.workload.snapshot().root_responsibilities, 0);
    }
}
fn poll_context() -> Context<'static> {
    Context::from_waker(std::task::Waker::noop())
}
async fn closed(tail: &mut SetNativeTail) {
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| tail.poll_closed(cx)),
    )
    .await
    .unwrap();
    assert!(tail.local_health_failure().is_none());
}
fn assert_old_live(fixture: &Fixture) {
    assert_eq!(fixture.live.execution_settings().pipeline_dop(), Some(7));
    assert_eq!(
        fixture
            .live
            .render_session_property("pipeline_dop")
            .as_deref(),
        Some("7")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn set_borrowed_wait_drop_keeps_real_cpu_and_result_custody_distinct() {
    let fixture = Fixture::new(ResultCapacityConfig::V1);
    let gate = CheckpointGate::new();
    let release = ReleaseGate(Arc::clone(&gate));
    let mut tail = fixture.tail();
    tail.start(fixture.input(
        "SET SESSION pipeline_dop = 2 + 0",
        SqlCompileControl::new(None, gate.clone()),
    ));
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| tail.poll_preparation(cx)));
    assert!(borrowed.as_mut().poll(&mut poll_context()).is_pending());
    drop(borrowed);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    assert_ne!(
        gate.thread.lock().unwrap().unwrap(),
        std::thread::current().id()
    );
    assert!(tail.preparation.is_some());
    assert!(tail.prepared.is_none());
    assert!(
        tail.poll_closed(&mut poll_context()).is_pending(),
        "a blocked actual worker is not closed"
    );
    assert_old_live(&fixture);
    drop(release);
    closed(&mut tail).await;
    let PreparedSessionProperties { staged, effects } = tail.prepared.take().unwrap().unwrap();
    assert_eq!(staged.execution_settings().pipeline_dop(), Some(2));
    assert_old_live(&fixture);
    assert!(
        fixture.window.retained_payload_bytes() > 0,
        "CPU exit does not destroy the received typed result"
    );
    drop(staged);
    assert!(
        fixture.window.retained_payload_bytes() > 0,
        "the original effect consumer still owns the CV"
    );
    drop(effects);
    assert_eq!(fixture.window.retained_payload_bytes(), 0);
    fixture.close(tail).await;
}

#[tokio::test(flavor = "current_thread")]
async fn set_cancel_observation_waits_for_original_cpu_exit_without_property14() {
    let fixture = Fixture::new(ResultCapacityConfig::V1);
    let gate = CheckpointGate::new();
    let release = ReleaseGate(Arc::clone(&gate));
    let mut tail = fixture.tail();
    tail.start(fixture.input(
        "SET SESSION pipeline_dop = 2 + 0",
        SqlCompileControl::new(None, gate.clone()),
    ));
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| tail.poll_preparation(cx)));
    assert!(borrowed.as_mut().poll(&mut poll_context()).is_pending());
    drop(borrowed);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    gate.cancelled.store(true, Ordering::SeqCst);
    assert!(tail.poll_closed(&mut poll_context()).is_pending());
    assert_old_live(&fixture);
    drop(release);
    closed(&mut tail).await;
    let error = match tail.prepared.take().unwrap() {
        Err(error) => error,
        Ok(_) => panic!("cancelled CPU cannot publish prepared state"),
    };
    assert!(
        error
            .user_error()
            .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_session_property")
    );
    assert_eq!(
        error.compile_control_error(),
        Some(novarocks_type_contract::CompileControlError::Cancelled)
    );
    assert_old_live(&fixture);
    drop(error);
    fixture.close(tail).await;
}

#[tokio::test(flavor = "current_thread")]
async fn set_whole_cpu_business_failure_is_atomic_and_resource_source_is_not_property14() {
    for resource_refusal in [false, true] {
        let mut capacity = ResultCapacityConfig::V1;
        if resource_refusal {
            capacity.all_objects_bytes[1] = 1;
        }
        let fixture = Fixture::new(capacity);
        let mut tail = fixture.tail();
        tail.start(fixture.input(
            "SET SESSION pipeline_dop = 2 + 0, query_timeout = 1 / 0",
            SqlCompileControl::unbounded(),
        ));
        closed(&mut tail).await;
        let error = match tail.prepared.take().unwrap() {
            Err(error) => error,
            Ok(_) => panic!("the complete batch must reject"),
        };
        assert_old_live(&fixture);
        if resource_refusal {
            assert!(
                error.user_error().is_none_or(
                    |error| error.code().as_str() != "sql.admit.invalid_session_property"
                )
            );
            let Some(novarocks_type_contract::CompileControlError::Source(source)) =
                error.compile_control_error()
            else {
                panic!("original Root resource refusal remains in the compile-control port");
            };
            assert!(source.downcast_ref::<WorkError>().is_some());
            assert_eq!(source.failure_fact().class(), FailureClass::Resource);
        } else {
            assert_eq!(
                error.user_error().unwrap().code().as_str(),
                "sql.admit.invalid_session_property"
            );
            assert!(
                error.message().contains("query_timeout"),
                "the later assignment authored the actual Data refusal"
            );
            assert_eq!(
                error.failure_cause().unwrap().failure_fact().class(),
                FailureClass::Data
            );
        }
        drop(error);
        fixture.close(tail).await;
    }
}

struct OriginalSetCpuPanic;
struct PanickingCheckpoint;
impl SqlCancellationObservation for PanickingCheckpoint {
    fn is_cancelled(&self) -> bool {
        // This is the actual synchronous SET body on the installed worker.
        // Its original input unwinds and drops before that worker reports exit.
        std::panic::panic_any(OriginalSetCpuPanic)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn set_original_cpu_worker_panic_health_never_becomes_property_data() {
    let fixture = Fixture::new(ResultCapacityConfig::V1);
    let mut tail = fixture.tail();
    tail.start(fixture.input(
        "SET SESSION pipeline_dop = 2 + 0",
        SqlCompileControl::new(None, Arc::new(PanickingCheckpoint)),
    ));
    let health = tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            let _ = tail.poll_closed(cx);
            tail.local_health_failure()
                .map_or(Poll::Pending, Poll::Ready)
        }),
    )
    .await
    .unwrap();
    let original = health
        .cause()
        .source()
        .and_then(|cause| {
            cause.downcast_ref::<novarocks_query_application::cpu::QueryBlockingFailure>()
        })
        .expect("the same installed worker's original failure");
    assert!(original.original_payload_is::<OriginalSetCpuPanic>());
    assert!(
        tail.prepared.is_none(),
        "worker panic is never a prepared business error or property14"
    );
    assert!(matches!(
        tail.poll_retirement(&mut poll_context()),
        Poll::Ready(Err(_))
    ));
    assert_old_live(&fixture);
    // Unwinding disposed the real SessionPropertyCpuInput. A health verdict
    // remains failed even when these input/result bytes have actually exited.
    assert_eq!(fixture.window.retained_payload_bytes(), 0);
    drop(health);
    drop(tail);
    // The real executor owners must still join and the original Root must
    // reach zero. No Source failure is reclassified as a successful close.
    let Fixture {
        mut executors,
        cpu,
        workload,
        root,
        window,
        ..
    } = fixture;
    assert!(
        cpu.close().await.is_err(),
        "the unhealthy original source must not claim close success"
    );
    drop(cpu);
    executors.shutdown().await;
    assert_eq!(window.retained_payload_bytes(), 0);
    drop(window);
    root.owner.complete();
    root.business.release();
    assert_eq!(workload.snapshot().root_responsibilities, 0);
}
