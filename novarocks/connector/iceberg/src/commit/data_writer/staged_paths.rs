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

//! The original driver-local path inventory, shared only with its SDK path author.
use std::sync::{Arc, Mutex};

#[derive(Clone, Default, Debug)]
pub(crate) struct StagedPathJournal {
    paths: Arc<Mutex<Vec<String>>>,
}
impl StagedPathJournal {
    /// Install the exact generated path before any open/write can affect it.
    /// Pure strings only; no callback or asynchronous work under this lock.
    pub(crate) fn record(&self, path: &str) {
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(path.to_owned());
    }
    pub(crate) fn last(&self) -> Option<String> {
        self.paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last()
            .cloned()
    }
    /// Only after the original FileIO confirms deletion of this exact path.
    pub(crate) fn confirm_deleted(&self, path: &str) -> Result<(), String> {
        let mut paths = self.paths.lock().unwrap_or_else(|p| p.into_inner());
        if paths.last().is_none_or(|original| original != path) {
            return Err("staged path journal changed during exclusive cleanup".to_string());
        }
        paths.pop();
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<String> {
        self.paths.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}
