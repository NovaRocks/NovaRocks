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

// Functional tests for the private existing-serializer projection.
use super::*;
use novarocks_spi::connector::ConnectorErrorKind;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    stop: AtomicBool,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope for Scope {
    fn check_active(&self) -> Result<(), ConnectorError> {
        if self.stop.load(Ordering::SeqCst) {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "original stopped projection",
            ))
        } else {
            Ok(())
        }
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
fn scope() -> (Scope, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Scope {
            stop: AtomicBool::new(false),
            guard: ConnectorPayloadRetentionGuard::new(Witness(drops.clone())),
        },
        drops,
    )
}
struct MidWriteScope {
    checks: std::cell::Cell<usize>,
    original: std::cell::RefCell<Option<ConnectorError>>,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope for MidWriteScope {
    fn check_active(&self) -> Result<(), ConnectorError> {
        self.checks.set(self.checks.get() + 1);
        if self.checks.get() >= 4 {
            return Err(self
                .original
                .borrow_mut()
                .take()
                .expect("write_all retried an original cancellation"));
        }
        Ok(())
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
#[test]
fn actual_serde_write_all_midwrite_cancellation_returns_first_error_without_retry() {
    let error = ConnectorError::new(ConnectorErrorKind::Cancelled, "original-midwrite-stop")
        .with_cleanup_context("original-provider-context");
    let message_ptr = error.message().as_ptr();
    let (ordinary, _) = scope();
    let original = MidWriteScope {
        checks: std::cell::Cell::new(0),
        original: std::cell::RefCell::new(Some(error)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let result = serialize(
        &["first", "second", "third"],
        &original,
        Some(&mut output),
        128,
    );
    assert!(
        !output.is_empty(),
        "real output prefix preceded cancellation"
    );
    assert_eq!(original.checks.get(), 4, "cancelled writes must not retry");
    match result {
        Err(Error::Control(raw)) => {
            assert_eq!(raw.message().as_ptr(), message_ptr);
            assert_eq!(raw.kind(), ConnectorErrorKind::Cancelled);
            assert!(raw.to_string().contains("original-provider-context"));
        }
        _ => panic!("original cancellation was replaced"),
    }
}
#[test]
fn subsequent_direct_writer_call_cannot_replace_or_recheck_first_cancellation() {
    use std::io::Write;
    let error = ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, "original-expiry");
    let message_ptr = error.message().as_ptr();
    let (ordinary, _) = scope();
    let original = MidWriteScope {
        checks: std::cell::Cell::new(3),
        original: std::cell::RefCell::new(Some(error)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let mut writer = Writer {
        scope: &original,
        original: None,
        length: 0,
        output: Some(&mut output),
        ceiling: 128,
        changed: false,
    };
    assert_eq!(
        writer.write(b"first").unwrap_err().kind(),
        std::io::ErrorKind::Other
    );
    assert_eq!(
        writer.write(b"second").unwrap_err().kind(),
        std::io::ErrorKind::Other
    );
    assert_eq!(original.checks.get(), 4);
    assert_eq!(writer.original.unwrap().message().as_ptr(), message_ptr);
    assert!(output.is_empty());
}

struct OpaqueCause {
    drops: Arc<AtomicUsize>,
}
impl std::fmt::Debug for OpaqueCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("original opaque cause must not be formatted")
    }
}
impl std::fmt::Display for OpaqueCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("original opaque cause must not be formatted")
    }
}
impl Drop for OpaqueCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct OpaqueScope {
    checks: std::cell::Cell<usize>,
    cause: std::cell::RefCell<Option<Box<OpaqueCause>>>,
    guard: ConnectorPayloadRetentionGuard,
}
impl OriginalScope<Box<OpaqueCause>> for OpaqueScope {
    fn check_active(&self) -> Result<(), Box<OpaqueCause>> {
        self.checks.set(self.checks.get() + 1);
        if self.checks.get() == 4 {
            return Err(self
                .cause
                .borrow_mut()
                .take()
                .expect("original cause consumed once"));
        }
        assert!(
            self.checks.get() < 4,
            "serializer retried after cancellation"
        );
        Ok(())
    }

    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
}
#[test]
fn actual_serde_midwrite_moves_opaque_cause_once_without_formatting_or_retry() {
    let drops = Arc::new(AtomicUsize::new(0));
    let cause = Box::new(OpaqueCause {
        drops: drops.clone(),
    });
    let identity = (&*cause) as *const OpaqueCause;
    let (ordinary, _) = scope();
    let original = OpaqueScope {
        checks: std::cell::Cell::new(0),
        cause: std::cell::RefCell::new(Some(cause)),
        guard: ordinary.guard.clone(),
    };
    let mut output = Vec::with_capacity(128);
    let result = serialize(
        &["first", "second", "third"],
        &original,
        Some(&mut output),
        128,
    );
    assert!(!output.is_empty());
    assert_eq!(original.checks.get(), 4);
    match result {
        Err(error) => {
            assert_eq!(format!("{error:?}"), "Control");
            assert_eq!(format!("{error}"), "Control");
            match error {
                Error::Control(cause) => {
                    assert_eq!((&*cause) as *const OpaqueCause, identity);
                    assert_eq!(drops.load(Ordering::SeqCst), 0);
                    drop(cause);
                }
                _ => panic!("original cause was replaced"),
            }
        }
        Ok(_) => panic!("cancelled serialization unexpectedly succeeded"),
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_json_count_is_an_exact_serializer_ceiling_not_a_graph_allowance() {
    let (scope, _) = scope();
    let value = ["escaped\nquoted\"", "unchanged"];
    let length = count(&value, &scope).expect("original serializer count");
    let actual = exact_json(&value, length, &scope).expect("exact original output");
    assert_eq!(actual, serde_json::to_string(&value).unwrap());
    assert!(matches!(
        exact_json(&value, length - 1, &scope),
        Err(Error::ShapeChanged)
    ));
    assert!(matches!(
        exact_json(&value, length + 1, &scope),
        Err(Error::ShapeChanged)
    ));
}
