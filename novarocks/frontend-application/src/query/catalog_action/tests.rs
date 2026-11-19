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

//! Real Catalog property CPU ownership and close-before-result guards.

use super::*;
use crate::query::property_cpu_context::PropertyCpuContext;
use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use crate::task_execution::blocking_io::tests::OriginalExecutorsFixture;
use novarocks_sql::compiler::{SqlCancellationObservation, SqlFunctionCatalog};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::future::Future;
use std::sync::{
    Condvar, Mutex, RwLock,
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
}
impl Fixture {
    fn new() -> Self {
        let (executors, supervisor) = crate::task_execution::blocking_io::tests::original_executors(
            tokio::runtime::Handle::current(),
        );
        let cpu = FrontendPlanningCpuSource::new(supervisor);
        let workload = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let (root, window) = workload
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        Self {
            executors,
            cpu,
            workload,
            root,
            window: window.retain_alias(),
        }
    }
    fn input(&self, source: &str, control: SqlCompileControl) -> CatalogPropertyCpuInput {
        let statement = novarocks_query_application::sql::parse_single_statement(source).unwrap();
        let ast::TrinoStatement::CreateCatalog(statement) = statement else {
            panic!("one actual CREATE CATALOG");
        };
        let frozen = novarocks_query_application::sql::session::SessionSqlState::default();
        let functions: Arc<dyn SqlFunctionCatalog> = Arc::new(
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .unwrap(),
        );
        CatalogPropertyCpuInput {
            source: Arc::from(source),
            statement: statement,
            context: PropertyCpuContext {
                current_database: frozen.current_database().to_owned(),
                sql_semantics: frozen.sql_semantics().clone(),
                interpreter: Arc::new(crate::query_execution::required_constant_eval::test_required_constant_interpreter()),
                control,
                scope: self.root.owner.scope(),
                window: self.window.clone(),
                catalog: Arc::new(RwLock::new(novarocks_sql::planning::catalog::PlannerMemoryCatalog::default())),
                functions,
                policy: crate::application::test_constant_policy(),
            },
        }
    }
    fn tail(&self) -> CatalogNativeTail {
        CatalogNativeTail::new(self.cpu.clone())
    }
    async fn close(mut self, tail: CatalogNativeTail) {
        drop(tail);
        self.cpu.close().await.unwrap();
        drop(self.cpu);
        self.executors.shutdown().await;
        assert_eq!(self.window.retained_payload_bytes(), 0);
        drop(self.window);
        self.root.owner.complete();
        self.root.business.release();
        assert_eq!(self.workload.snapshot().root_responsibilities, 0);
    }
}
fn poll_context() -> Context<'static> {
    Context::from_waker(std::task::Waker::noop())
}
async fn closed(tail: &mut CatalogNativeTail) {
    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| tail.poll_cpu_closed(cx)),
    )
    .await
    .unwrap();
    assert!(tail.local_health().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn catalog_borrowed_wait_drop_preserves_actual_cpu_until_receive_and_close() {
    let fixture = Fixture::new();
    let gate = CheckpointGate::new();
    let release = ReleaseGate(gate.clone());
    let mut tail = fixture.tail();
    tail.start(fixture.input(
        "CREATE CATALOG IF NOT EXISTS Warehouse USING iceberg WITH (\"MixedKey\" = 'selected中')",
        SqlCompileControl::new(None, gate.clone()),
    ));
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| tail.poll_cpu_closed(cx)));
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
        tail.poll_cpu_closed(&mut poll_context()).is_pending(),
        "blocked actual CPU is not closed"
    );
    drop(release);
    closed(&mut tail).await;
    let command = tail
        .prepared
        .take()
        .expect("actual received result")
        .unwrap();
    assert_eq!(command.name, "Warehouse");
    assert!(command.if_not_exists);
    assert_eq!(
        command.properties,
        vec![
            ("type".into(), "iceberg".into()),
            ("MixedKey".into(), "selected中".into())
        ]
    );
    assert!(tail.preparation.is_none());
    drop(command);
    fixture.close(tail).await;
}

#[tokio::test(flavor = "current_thread")]
async fn catalog_cancelled_checkpoint_cannot_close_or_publish_before_actual_cpu_exit() {
    let fixture = Fixture::new();
    let gate = CheckpointGate::new();
    let release = ReleaseGate(gate.clone());
    let mut tail = fixture.tail();
    tail.start(fixture.input(
        "CREATE CATALOG Warehouse USING iceberg",
        SqlCompileControl::new(None, gate.clone()),
    ));
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| tail.poll_cpu_closed(cx)));
    assert!(borrowed.as_mut().poll(&mut poll_context()).is_pending());
    drop(borrowed);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    gate.cancelled.store(true, Ordering::SeqCst);
    assert!(tail.poll_cpu_closed(&mut poll_context()).is_pending());
    assert!(tail.prepared.is_none());
    drop(release);
    closed(&mut tail).await;
    let error = tail
        .prepared
        .take()
        .expect("actual received refusal")
        .unwrap_err();
    assert_eq!(
        error.compile_control_error(),
        Some(novarocks_type_contract::CompileControlError::Cancelled)
    );
    assert!(
        error
            .user_error()
            .is_none_or(|error| error.code().as_str() != "sql.admit.invalid_catalog_property")
    );
    drop(error);
    fixture.close(tail).await;
}

#[tokio::test(flavor = "current_thread")]
async fn catalog_duplicate_and_type_refusals_wait_for_real_close_without_partial_command() {
    for (source, duplicate) in [
        (
            "CREATE CATALOG Warehouse USING iceberg WITH (same = 'first', same = 'second')",
            true,
        ),
        (
            "CREATE CATALOG Warehouse USING iceberg WITH (first = 'selected', later = 12)",
            false,
        ),
    ] {
        let fixture = Fixture::new();
        let gate = CheckpointGate::new();
        let release = ReleaseGate(gate.clone());
        let mut tail = fixture.tail();
        let input = fixture.input(source, SqlCompileControl::new(None, gate.clone()));
        let property = &input.statement.properties[1];
        let refused_span = if duplicate {
            property.name.span
        } else {
            property.value.as_ref().unwrap().span()
        };
        let expected_location = refused_span.to_user_error_location(source);
        tail.start(input);
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| tail.poll_cpu_closed(cx)));
        assert!(borrowed.as_mut().poll(&mut poll_context()).is_pending());
        drop(borrowed);
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        assert!(tail.poll_cpu_closed(&mut poll_context()).is_pending());
        assert!(tail.prepared.is_none());
        drop(release);
        closed(&mut tail).await;
        let error = tail
            .prepared
            .take()
            .expect("complete batch result after actual exit")
            .unwrap_err();
        assert_eq!(
            error.user_error().unwrap().code().as_str(),
            "sql.admit.invalid_catalog_property"
        );
        assert_eq!(
            error.user_error().unwrap().location(),
            Some(&expected_location)
        );
        assert!(error.compile_control_error().is_none());
        if duplicate {
            assert!(error.message().contains("Duplicate property 'same'"));
        }
        assert!(tail.preparation.is_none());
        drop(error);
        fixture.close(tail).await;
    }
}
