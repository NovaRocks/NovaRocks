// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! A versioned wakeup; checking and sleeping share one synchronization order.

use crate::sync::{Condvar, Mutex};

#[derive(Debug, Default)]
pub struct Notify {
    version: Mutex<u64>,
    changed: Condvar,
}
impl Notify {
    pub fn version(&self) -> u64 {
        *self.version.lock().expect("runtime notification lock")
    }
    pub fn notify(&self) {
        let mut version = self.version.lock().expect("runtime notification lock");
        *version = version
            .checked_add(1)
            .expect("runtime notification version exhausted");
        self.changed.notify_all();
    }
    /// The caller must recheck its predicate after waking. Notifications do not prove completion.
    #[cfg(not(loom))]
    pub fn wait_after_timeout(&self, observed: u64, timeout: std::time::Duration) {
        let version = self.version.lock().expect("runtime notification lock");
        if *version == observed {
            drop(
                self.changed
                    .wait_timeout(version, timeout)
                    .expect("runtime timed notification wait"),
            );
        }
    }
    pub fn wait_after(&self, observed: u64) {
        let mut version = self.version.lock().expect("runtime notification lock");
        while *version == observed {
            version = self
                .changed
                .wait(version)
                .expect("runtime notification wait");
        }
    }
}
