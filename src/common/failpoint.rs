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

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use once_cell::sync::Lazy;
use rand::Rng;

pub const AGG_HASH_SET_BAD_ALLOC: &str = "agg_hash_set_bad_alloc";
pub const AGGREGATE_BUILD_HASH_MAP_BAD_ALLOC: &str = "aggregate_build_hash_map_bad_alloc";
pub const FORCE_RESET_AGGREGATOR_AFTER_STREAMING_SINK_FINISH: &str =
    "force_reset_aggregator_after_agg_streaming_sink_finish";
pub const SCAN_CHUNK_SLEEP_AFTER_READ: &str = "scan_chunk_sleep_after_read";

const SUPPORTED_FAIL_POINTS: [&str; 4] = [
    AGG_HASH_SET_BAD_ALLOC,
    AGGREGATE_BUILD_HASH_MAP_BAD_ALLOC,
    FORCE_RESET_AGGREGATOR_AFTER_STREAMING_SINK_FINISH,
    SCAN_CHUNK_SLEEP_AFTER_READ,
];

#[derive(Clone, Debug, PartialEq)]
pub enum FailPointMode {
    Enable,
    Disable,
    Probability(f64),
    EnableNTimes(i32),
}

static REGISTRY: Lazy<Mutex<HashMap<&'static str, FailPointMode>>> = Lazy::new(|| {
    let mut states = HashMap::new();
    for name in SUPPORTED_FAIL_POINTS {
        states.insert(name, FailPointMode::Disable);
    }
    Mutex::new(states)
});

pub fn update(name: &str, mode: FailPointMode) -> Result<(), String> {
    let mut registry = REGISTRY
        .lock()
        .map_err(|_| "failpoint registry lock poisoned".to_string())?;
    let Some(current) = registry.get_mut(name) else {
        return Err(format!("FailPoint {name} is not existed."));
    };
    match &mode {
        FailPointMode::Probability(probability) => {
            if !(0.0..=1.0).contains(probability) {
                return Err(format!(
                    "FailPoint {name} probability must be between 0 and 1, got {probability}"
                ));
            }
        }
        FailPointMode::EnableNTimes(n_times) => {
            if *n_times < 0 {
                return Err(format!(
                    "FailPoint {name} n_times must be non-negative, got {n_times}"
                ));
            }
        }
        FailPointMode::Enable | FailPointMode::Disable => {}
    }
    *current = mode;
    Ok(())
}

pub fn should_trigger(name: &str) -> bool {
    let mut registry = match REGISTRY.lock() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    let Some(mode) = registry.get_mut(name) else {
        return false;
    };
    match mode {
        FailPointMode::Enable => true,
        FailPointMode::Disable => false,
        FailPointMode::Probability(probability) => rand::thread_rng().gen_bool(*probability),
        FailPointMode::EnableNTimes(remaining) => {
            if *remaining <= 0 {
                return false;
            }
            *remaining -= 1;
            true
        }
    }
}

pub fn maybe_error(name: &str, message: &str) -> Result<(), String> {
    if should_trigger(name) {
        Err(message.to_string())
    } else {
        Ok(())
    }
}

pub fn sleep_if_triggered(name: &str, duration: Duration) {
    if should_trigger(name) {
        std::thread::sleep(duration);
    }
}
