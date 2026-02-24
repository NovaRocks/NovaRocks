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
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::common::app_config;
use crate::common::types::UniqueId;
use crate::runtime::profile::clamp_u128_to_i64;
use crate::service::fe_report;
use crate::novarocks_logging::debug;

static REPORT_WORKER_STARTED: OnceLock<()> = OnceLock::new();
static REPORT_WORKER_STOP: AtomicBool = AtomicBool::new(false);

pub(crate) fn ensure_started() {
    REPORT_WORKER_STARTED.get_or_init(|| {
        std::thread::Builder::new()
            .name("profile_report".to_string())
            .spawn(run_report_worker)
            .expect("start report worker");
    });
}

#[allow(dead_code)]
pub(crate) fn stop() {
    REPORT_WORKER_STOP.store(true, Ordering::Release);
}

fn run_report_worker() {
    debug!("Profile report worker started");
    let mut last_report: HashMap<UniqueId, i64> = HashMap::new();

    loop {
        if REPORT_WORKER_STOP.load(Ordering::Acquire) {
            break;
        }

        let instances = fe_report::list_report_instances();
        let mut active: HashSet<UniqueId> = HashSet::with_capacity(instances.len());
        let now_ns = monotonic_now_ns();

        for (id, snapshot) in instances {
            active.insert(id);
            if !snapshot.enable_profile {
                continue;
            }
            let Some(interval_ns) = snapshot.report_interval_ns else {
                continue;
            };
            let last_ns = last_report.get(&id).copied().unwrap_or(0);
            if now_ns.saturating_sub(last_ns) < interval_ns {
                continue;
            }
            fe_report::report_exec_state(id);
            last_report.insert(id, now_ns);
        }

        last_report.retain(|id, _| active.contains(id));
        std::thread::sleep(Duration::from_secs(report_interval_seconds()));
    }

    debug!("Profile report worker stopped");
}

fn monotonic_now_ns() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    clamp_u128_to_i64(start.elapsed().as_nanos())
}

fn report_interval_seconds() -> u64 {
    let interval = app_config::config()
        .ok()
        .map(|cfg| cfg.runtime.profile_report_interval)
        .unwrap_or(30);
    interval.max(1) as u64
}
