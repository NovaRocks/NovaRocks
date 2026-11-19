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

//! Governed LIKE reads through the actual metadata and column projection owners.

use super::*;
use crate::query::table_property_computation::LikeColumnsCpuInput;
use crate::query_execution::like_source_read::LikeSourceRead;
use novarocks_query_application::api::CatalogTableReadBinding;
use novarocks_sql::semantic::TableColumnDef;
use novarocks_types::schema::{ColumnDefault, SqlType};
use std::{future::poll_fn, pin::pin};

const WAIT: Duration = Duration::from_secs(10);

fn source_table(instance: &ConnectorInstanceId) -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id: instance.clone(),
        namespace: "atomic".into(),
        table: "like_source".into(),
    }
}

fn seed_source(setup: &ConnectorCatalogMutationSession, instance: &ConnectorInstanceId) {
    let columns = vec![
        ConnectorColumnDefinition {
            name: "id".into(),
            data_type: ConnectorDataType::BigInt,
            nullable: false,
            default: Some(ConnectorDefaultValue::Int(7)),
        },
        ConnectorColumnDefinition {
            name: "narrow".into(),
            data_type: ConnectorDataType::SmallInt,
            nullable: true,
            default: None,
        },
        ConnectorColumnDefinition {
            name: "bytes".into(),
            data_type: ConnectorDataType::Binary,
            nullable: false,
            default: None,
        },
        ConnectorColumnDefinition {
            name: "items".into(),
            data_type: ConnectorDataType::Array(Box::new(ConnectorDataType::BigInt)),
            nullable: true,
            default: None,
        },
    ];
    let outcome = setup
        .execute_operation(
            ConnectorMutationOperationId::new(),
            ConnectorCatalogMutationOperation::CreateTable {
                table: source_table(instance),
                columns,
                comment: None,
                properties: vec![
                    ("format".into(), DdlPropertyValue::Varchar("PARQUET".into())),
                    ("format_version".into(), DdlPropertyValue::Integer(3)),
                    (
                        "partitioning".into(),
                        DdlPropertyValue::ArrayVarchar(vec![]),
                    ),
                ],
                policy: CreatePolicy::FailIfExists,
            },
            crate::connector::test_request_context(),
        )
        .unwrap();
    assert!(matches!(
        outcome,
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            finalization: ExternalMutationFinalization::Complete,
            cleanup: ExternalMutationFinalization::Complete,
            ..
        }
    ));
}

fn original_read(fixture: &Fixture) -> LikeSourceRead {
    let pin = fixture
        .host
        .acquire_current(&fixture.key.instance_id)
        .unwrap();
    assert_eq!(pin.binding().incarnation(), fixture.key.incarnation);
    let binding =
        CatalogTableReadBinding::try_new(source_table(&fixture.key.instance_id), pin).unwrap();
    LikeSourceRead::try_new(
        binding,
        fixture.command.as_ref().unwrap().clone(),
        fixture.creator.runtime(),
        fixture.query_cancellation.clone(),
        tokio::runtime::Handle::current(),
    )
    .unwrap()
}

fn poll_read(
    fixture: &mut Fixture,
    read: &mut LikeSourceRead,
    cx: &mut Context<'_>,
) -> Poll<Result<ConnectorTableMetadata, QueryServiceError>> {
    let mut context = GovernedActionContext::new(
        &fixture.scope,
        &mut fixture.owner,
        &fixture.cancellation,
        None,
        fixture.window.as_ref(),
    );
    read.poll_read(&mut context, cx)
}

async fn drop_borrowed_wait_during_capture(fixture: &mut Fixture, read: &mut LikeSourceRead) {
    tokio::time::timeout(
        WAIT,
        poll_fn(|cx| {
            drive(&mut fixture.driver);
            {
                let mut borrowed = pin!(poll_fn(|cx| poll_read(fixture, read, cx)));
                assert!(
                    borrowed.as_mut().poll(cx).is_pending(),
                    "observe a live capture before dropping its borrowed waiter"
                );
            }
            assert!(read.local_health_failure().is_none());
            if read.capture_in_flight_for_test() {
                return Poll::Ready(());
            }
            cx.waker().wake_by_ref();
            Poll::Pending
        }),
    )
    .await
    .expect("the original LIKE capture must start");
    fixture.retire_current();
    assert!(
        fixture.host.take_ready_retires().unwrap().is_empty(),
        "the same live LIKE source retains its original generation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_like_capture_survives_borrowed_wait_drop_and_projects_on_original_planning_cpu() {
    let mut fixture = Fixture::new_with_provider_setup(seed_source);
    let mut read = original_read(&fixture);
    drop_borrowed_wait_during_capture(&mut fixture, &mut read).await;

    let metadata = tokio::time::timeout(
        WAIT,
        poll_fn(|cx| {
            drive(&mut fixture.driver);
            let result = poll_read(&mut fixture, &mut read, cx);
            drive(&mut fixture.driver);
            assert!(read.local_health_failure().is_none());
            if result.is_pending() {
                cx.waker().wake_by_ref();
            }
            result
        }),
    )
    .await
    .expect("the original LIKE capture must finish")
    .unwrap();
    assert_eq!(metadata.identity, source_table(&fixture.key.instance_id));
    assert_eq!(
        crate::connector::connector_write_default_at(&metadata.planning_facts, 0),
        Some(ColumnDefault::Int64(7))
    );
    // Read delivery requires actual capture, metadata CPU and installed I/O close.
    assert!(
        read.poll_close(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(read.local_health_failure().is_none());
    assert!(fixture.host.take_ready_retires().unwrap().is_empty());
    drop(read);

    let witness = Arc::new(CpuWitness {
        cancellation: fixture.query_cancellation.clone(),
        first_thread: Mutex::new(None),
    });
    let planning = fixture.planning.as_ref().unwrap();
    let columns = tokio::time::timeout(
        WAIT,
        planning.like_columns(LikeColumnsCpuInput {
            metadata,
            control: SqlCompileControl::new(None, witness.clone()),
        }),
    )
    .await
    .expect("LIKE columns must exit their actual planning CPU task")
    .unwrap()
    .unwrap();
    let expected = vec![
        TableColumnDef {
            name: "id".into(),
            data_type: SqlType::BigInt,
            nullable: false,
            default: None,
        },
        TableColumnDef {
            name: "narrow".into(),
            data_type: SqlType::SmallInt,
            nullable: true,
            default: None,
        },
        TableColumnDef {
            name: "bytes".into(),
            data_type: SqlType::Binary,
            nullable: false,
            default: None,
        },
        TableColumnDef {
            name: "items".into(),
            data_type: SqlType::Array(Box::new(SqlType::BigInt)),
            nullable: true,
            default: None,
        },
    ];
    assert_eq!(columns, expected);
    assert_ne!(
        witness
            .first_thread
            .lock()
            .unwrap()
            .expect("LIKE CPU checkpoint"),
        std::thread::current().id()
    );
    drop(witness);
    tokio::time::timeout(WAIT, planning.close())
        .await
        .expect("the original LIKE planning CPU source must close")
        .unwrap();
    assert!(planning.local_health_failure().is_none());
    fixture.close_retired_resources(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn like_columns_and_create_publish_with_one_original_metadata_admission() {
    let mut fixture = Fixture::new_with_provider_setup(seed_source);
    let mut read = original_read(&fixture);
    let metadata = tokio::time::timeout(
        WAIT,
        poll_fn(|cx| {
            drive(&mut fixture.driver);
            let result = poll_read(&mut fixture, &mut read, cx);
            drive(&mut fixture.driver);
            if result.is_pending() {
                cx.waker().wake_by_ref();
            }
            result
        }),
    )
    .await
    .expect("source metadata must actually exit")
    .unwrap();
    let io = read
        .take_completed_io()
        .expect("one original positive I/O Source");
    let original_io = Arc::downgrade(&io);
    assert!(read.take_completed_io().is_none());
    drop(read);
    let columns = tokio::time::timeout(
        WAIT,
        fixture
            .planning
            .as_ref()
            .unwrap()
            .like_columns(LikeColumnsCpuInput {
                metadata,
                control: SqlCompileControl::unbounded(),
            }),
    )
    .await
    .expect("the original planning CPU must actually exit")
    .unwrap()
    .unwrap();
    let (mut action, operation_id) = fixture
        .action_for(
            "CREATE TABLE ice.atomic.events (LIKE ice.atomic.like_source)",
            Some(columns),
            Some(io),
        )
        .await;
    assert!(matches!(
        fixture.finish_action(&mut action).await.unwrap(),
        StatementActionValue::Ok(StatementCommandCompletion::Empty)
    ));
    {
        let original = original_io
            .upgrade()
            .expect("same I/O Source is still retained");
        let target = action.source.as_ref().unwrap().io.as_ref().unwrap();
        assert!(Arc::ptr_eq(&original, target));
    }
    let metadata = committed_metadata(&action, operation_id);
    let schema = metadata.current_schema();
    let fields = schema.as_struct().fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "narrow", "bytes", "items"]
    );
    assert!(fields[0].required);
    assert!(fields[0].write_default.is_none());
    fixture.close(action, false).await;
    assert!(original_io.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_like_capture_settles_original_cpu_and_io_after_root_cancellation() {
    let mut fixture = Fixture::new_with_provider_setup(seed_source);
    let mut read = original_read(&fixture);
    drop_borrowed_wait_during_capture(&mut fixture, &mut read).await;
    fixture
        .owner
        .as_ref()
        .unwrap()
        .cancel(CancellationReason::DeadlineExceeded);
    assert_eq!(
        fixture.cancellation.reason(),
        Some(CancellationReason::DeadlineExceeded)
    );
    tokio::time::timeout(
        WAIT,
        poll_fn(|cx| {
            drive(&mut fixture.driver);
            let closed = read.poll_close(cx);
            drive(&mut fixture.driver);
            assert!(read.local_health_failure().is_none());
            if closed.is_pending() {
                cx.waker().wake_by_ref();
            }
            closed
        }),
    )
    .await
    .expect("the cancelled LIKE source must retain every original until close");
    assert!(!read.capture_in_flight_for_test());
    assert!(fixture.host.take_ready_retires().unwrap().is_empty());
    drop(read);
    fixture.close_retired_resources(true).await;
}
