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

//! Projection consumes the same Source's received failure without replacing
//! the Product's original first error or its publication/control classification.
use super::*;
use std::sync::atomic::Ordering;
#[test]
fn product_primary_survives_two_cleanup_errors_after_real_cell_exit() {
    let (source, first, second) = super::super::io_admission::cleanup_tests::two_failures(false);
    let primary = novarocks_table_maintenance::runtime::TerminalError::known_uncommitted(
        "original Product rejection",
    );
    let mut call = MaintenanceCommandCall {
        future: None,
        result: Some(Err(primary.clone())),
        poll_unproven: false,
        original: MaintenanceActionState::default(),
        io: Some(Arc::clone(&source)),
    };
    call.project_saved_cleanup(source.cleanup_failure());
    let projected = call.result.as_ref().unwrap().as_ref().unwrap_err();
    assert_eq!(projected.state, primary.state);
    assert_eq!(projected.compile_control, primary.compile_control);
    assert_eq!(projected.execution_failure, primary.execution_failure);
    assert_eq!(
        projected.message,
        "original Product rejection (cleanup: original abort 1 failed)"
    );
    assert_eq!(first.load(Ordering::SeqCst), 0);
    assert_eq!(second.load(Ordering::SeqCst), 1);
    drop(call);
    assert_eq!(first.load(Ordering::SeqCst), 0);
    drop(source);
    assert_eq!(first.load(Ordering::SeqCst), 1);
}
#[test]
fn successful_product_keeps_success_and_the_original_cleanup_receipt() {
    let (source, first, _) = super::super::io_admission::cleanup_tests::two_failures(false);
    let mut call = MaintenanceCommandCall {
        future: None,
        result: Some(Ok(MaintenanceStatementResult::Ok)),
        poll_unproven: false,
        original: MaintenanceActionState::default(),
        io: Some(Arc::clone(&source)),
    };
    call.project_saved_cleanup(source.cleanup_failure());
    let disposition = call.publication_disposition();
    assert!(matches!(
        call.result,
        Some(Ok(MaintenanceStatementResult::Ok))
    ));
    assert_eq!(disposition, None);
    let diagnostic = source.cleanup_failure().unwrap();
    assert_eq!(diagnostic.message(), "original abort 1 failed");
    assert_eq!(first.load(Ordering::SeqCst), 0);
    drop(call);
    drop(source);
    assert_eq!(first.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_product_finalization_failure_keeps_its_original_classification() {
    use novarocks_table_maintenance::runtime::TerminalError;
    let (source, first, _) = super::super::io_admission::cleanup_tests::two_failures(false);
    let primary = TerminalError::known_committed_finalization_failed(
        "original required cache finalization failed",
    );
    let mut call = MaintenanceCommandCall {
        future: None,
        result: Some(Err(primary.clone())),
        poll_unproven: false,
        original: MaintenanceActionState::default(),
        io: Some(Arc::clone(&source)),
    };
    call.project_saved_cleanup(source.cleanup_failure());
    let error = call.result.as_ref().unwrap().as_ref().unwrap_err();
    assert_eq!(error.state, primary.state);
    assert_eq!(error.compile_control, primary.compile_control);
    assert_eq!(error.execution_failure, primary.execution_failure);
    assert_eq!(
        error.message,
        "original required cache finalization failed (cleanup: original abort 1 failed)"
    );
    assert_eq!(first.load(Ordering::SeqCst), 0);
    drop(call);
    drop(source);
    assert_eq!(first.load(Ordering::SeqCst), 1);
}
