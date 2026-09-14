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

//! MV-owned StateStore retry accounting.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub(crate) struct MvRepositoryMetrics {
    retries: AtomicU64,
    saturated_retries: AtomicU64,
    deadlines: AtomicU64,
    unresolved: AtomicU64,
}

impl novarocks_state_store_runtime::StateStoreRunMetrics for MvRepositoryMetrics {
    fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_saturated_retry(&self) {
        self.saturated_retries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_deadline(&self) {
        self.deadlines.fetch_add(1, Ordering::Relaxed);
    }

    fn record_unresolved(&self) {
        self.unresolved.fetch_add(1, Ordering::Relaxed);
    }
}
