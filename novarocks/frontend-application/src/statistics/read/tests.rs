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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL statistics results preserve their typed fields and domain failures.
use super::*;
use arrow::{
    array::{Array, Float64Array, StringArray},
    datatypes::DataType,
};
fn target(catalog: &str, namespace: &str) -> application::StatisticsTableTarget {
    application::StatisticsTableTarget {
        catalog: catalog.into(),
        namespace: namespace.into(),
        table: "orders".into(),
    }
}
#[test]
fn result_projection_keeps_double_columns_and_last_summary_row() {
    let summary = TableStatisticsSummary {
        row_count: 3,
        columns: vec![novarocks_spi::connector::StatisticsSummaryColumn {
            name: "order_id".into(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            column_type: novarocks_spi::connector::StatisticsSummaryColumnType::Integer,
            null_count: Some(0),
            column_size: Some(24),
            lower_bound: Some(1.0),
            upper_bound: Some(3.0),
            ndv: Some(3),
        }],
    };
    let result = result_from_summary(&summary).unwrap();
    assert_eq!(
        result
            .columns
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec![
            "column_name",
            "data_size",
            "distinct_values_count",
            "nulls_fraction",
            "row_count",
            "low_value",
            "high_value"
        ]
    );
    assert_eq!(result.columns[1].data_type(), &DataType::Float64);
    assert_eq!(result.columns[2].data_type(), &DataType::Float64);
    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "order_id"
    );
    let rows = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(rows.is_null(0));
    assert_eq!(rows.value(1), 3.0);
}
#[test]
fn missing_table_has_a_sql_owned_identity_and_keeps_the_original_cause() {
    let error = target_error(
        ConnectorError::new(ConnectorErrorKind::NotFound, "raw missing table"),
        &target("ice", "db"),
    );
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "sql.analyze.unknown_table"
    );
    assert_eq!(
        error
            .failure_cause()
            .unwrap()
            .downcast_ref::<ConnectorError>()
            .unwrap()
            .kind(),
        ConnectorErrorKind::NotFound
    );
    assert!(error.message().contains("ice.db.orders"));
}
#[test]
fn unsupported_relations_keep_their_typed_unsupported_fact() {
    for table in [
        target("system", "metadata"),
        target("ice", "information_schema"),
    ] {
        assert_eq!(
            reject_system_relation(&table).unwrap_err().kind(),
            ConnectorErrorKind::Unsupported
        );
    }
    assert!(reject_system_relation(&target("ice", "db")).is_ok());
    assert_eq!(
        connector_error(unsupported_relation("views"))
            .execution_failure()
            .unwrap()
            .descriptor()
            .descriptor
            .code
            .as_str(),
        "query.execution.unsupported"
    );
}

#[test]
fn connector_projection_keeps_the_original_typed_cause() {
    use novarocks_failure_contract::FailureProjection;
    let original =
        ConnectorError::compile_interruption(novarocks_type_contract::CompileControlError::Source(
            novarocks_failure_contract::SharedFailureCause::new(WorkError::Capacity(
                "original read capacity",
            )),
        ));
    let service = connector_error(original.clone());
    assert_eq!(
        service.execution_failure().unwrap().fact(),
        original.failure_fact()
    );
    assert_eq!(
        service
            .failure_cause()
            .unwrap()
            .downcast_ref::<ConnectorError>(),
        Some(&original)
    );
}

fn io_profile() -> novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile {
    use std::num::{NonZeroU64, NonZeroUsize};
    novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::try_new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroU64::new(
            crate::query_execution::maintenance::io_admission::fixtures::body_metadata_bytes(),
        )
        .unwrap(),
        NonZeroU64::new(128).unwrap(),
        vec![novarocks_resource_contract::SharedDependency::LocalHost],
    )
    .unwrap()
}

#[test]
fn residual_withdraws_the_original_uninstalled_bootstrap_after_cancellation() {
    use crate::query_execution::maintenance::io_admission::fixtures::{
        drive_original, original_governor,
    };
    let (control, mut driver, root) = original_governor();
    let io = MetadataIoAdmission::new(
        root.owner.scope(),
        io_profile(),
        std::mem::size_of::<StatisticsReadAction>() as u64,
    )
    .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(!io.poll_progress(&root.owner, &mut cx).unwrap());
    // Grant the original reply without installing it into the source. A
    // cancelled residual must withdraw that exact bootstrap before success.
    drive_original(&mut driver);
    root.owner
        .cancel(novarocks_workload_control::CancellationReason::Requested);
    let mut ordinary = None;
    let mut closed = false;
    assert!(matches!(
        poll_read_io_retirement(&io, &mut ordinary, &mut closed, &mut cx),
        Poll::Ready(Ok(()))
    ));
    assert!(closed);
    assert!(matches!(io.root_capacity(), Err(WorkError::Released)));
    assert!(io.health_failure().is_none());
    drop(io);
    root.business.release();
    root.owner.complete_after_terminal_cancel_settled();
    control.close_admission();
    drive_original(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "the original reply and bootstrap actually retired"
    );
}

#[test]
fn residual_keeps_ordinary_cancellation_separate_from_real_request_exit() {
    use crate::query_execution::maintenance::io_admission::fixtures::{
        drive_original, original_governor,
    };
    use novarocks_resource_contract::SharedDependency;
    use novarocks_spi::connector::ConnectorIoAdmissionPort;
    let (control, mut driver, root) = original_governor();
    let io = MetadataIoAdmission::new(
        root.owner.scope(),
        io_profile(),
        std::mem::size_of::<StatisticsReadAction>() as u64,
    )
    .unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut ready = false;
    for _ in 0..256 {
        ready = io.poll_progress(&root.owner, &mut cx).unwrap();
        if ready {
            break;
        }
        drive_original(&mut driver);
    }
    assert!(ready);
    let request = io.request(SharedDependency::LocalHost).unwrap();
    let mut permit = None;
    for _ in 0..256 {
        if matches!(
            request.poll_admitted_in(&mut permit, &mut cx),
            Poll::Ready(Ok(()))
        ) {
            break;
        }
        io.poll_continuing(&mut cx).unwrap();
        drive_original(&mut driver);
    }
    assert!(
        permit.is_some(),
        "the exact request really obtained its grant"
    );
    // Cancellation alone is not an I/O refusal. Submit another real request,
    // then receive its original failed admission while the first grant lives.
    let refused_request = io.request(SharedDependency::LocalHost).unwrap();
    let mut refused_permit = None;
    assert!(
        refused_request
            .poll_admitted_in(&mut refused_permit, &mut cx)
            .is_pending()
    );
    root.owner
        .cancel(novarocks_workload_control::CancellationReason::Requested);
    let mut refusal = None;
    for _ in 0..256 {
        io.poll_continuing(&mut cx).unwrap();
        drive_original(&mut driver);
        match refused_request.poll_admitted_in(&mut refused_permit, &mut cx) {
            Poll::Ready(Err(error)) => {
                refusal = Some(error);
                break;
            }
            Poll::Ready(Ok(())) => panic!("a cancelled pending request cannot obtain a grant"),
            Poll::Pending => {}
        }
    }
    assert_eq!(
        refusal
            .expect("receive the original admission refusal")
            .kind(),
        ConnectorErrorKind::Cancelled
    );
    assert!(refused_permit.is_none());
    let cancelled = WorkError::Cancelled(novarocks_workload_control::CancellationReason::Requested);
    assert_eq!(io.failure(), Some(cancelled.clone()));
    drop(refused_request);
    let mut ordinary = None;
    let mut closed = false;
    assert!(poll_read_io_retirement(&io, &mut ordinary, &mut closed, &mut cx).is_pending());
    assert!(
        ordinary.is_none(),
        "the residual has not received actual quiescence yet"
    );
    assert!(!closed);
    assert!(io.health_failure().is_none());
    drop(permit);
    assert!(
        poll_read_io_retirement(&io, &mut ordinary, &mut closed, &mut cx).is_pending(),
        "a live request alias is still an original obligation"
    );
    drop(request);
    let mut exited = false;
    for _ in 0..256 {
        drive_original(&mut driver);
        match poll_read_io_retirement(&io, &mut ordinary, &mut closed, &mut cx) {
            Poll::Ready(Ok(())) => {
                exited = true;
                break;
            }
            Poll::Ready(Err(error)) => panic!("ordinary refusal became health: {error:?}"),
            Poll::Pending => {}
        }
    }
    assert!(exited && closed);
    assert_eq!(ordinary, Some(cancelled));
    assert!(io.health_failure().is_none());
    // After positive exit, idempotence must not reopen business progress and
    // reinterpret its retained cancellation or Released state as raw health.
    assert!(matches!(
        poll_read_io_retirement(&io, &mut ordinary, &mut closed, &mut cx),
        Poll::Ready(Ok(()))
    ));
    drop(io);
    root.business.release();
    root.owner.complete_after_terminal_cancel_settled();
    control.close_admission();
    drive_original(&mut driver);
    assert!(control.shutdown().is_ok(), "same original WC really drains");
}
