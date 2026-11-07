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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]
//! Common utilities and helpers for integration tests.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::time::Duration;

use novarocks_types::UniqueId;

/// Generate a test query ID.
pub fn test_query_id() -> UniqueId {
    UniqueId::new(1234567890, 9876543210)
}

/// Generate a unique query ID based on test name.
pub fn unique_query_id(test_name: &str) -> UniqueId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    test_name.hash(&mut hasher);
    let hash = hasher.finish();

    UniqueId::new(hash as i64, (hash >> 32) as i64)
}

/// Wait for a condition to become true, with timeout.
pub fn wait_for<F>(mut condition: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Run `f` and fail the test if it does not finish within `timeout`.
pub fn run_with_timeout<F, T>(timeout: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });

    match rx.recv_timeout(timeout) {
        Ok(v) => v,
        Err(_) => panic!("test timed out after {:?}", timeout),
    }
}

/// Assert that a result is Ok and return the value.
#[macro_export]
macro_rules! assert_ok {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("Expected Ok, got Err: {:?}", e),
        }
    };
    ($result:expr, $message:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("{}: {:?}", $message, e),
        }
    };
}

/// Assert that a result is Err.
#[macro_export]
macro_rules! assert_err {
    ($result:expr) => {
        match $result {
            Ok(value) => panic!("Expected Err, got Ok: {:?}", value),
            Err(e) => e,
        }
    };
}
