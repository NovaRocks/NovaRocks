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

//! Deployable preparation limits and metric publication cadence.

use std::time::Duration;

/// Minimum interval between metric-only publications; lifecycle facts publish immediately.
pub const METRIC_PUBLISH_MIN_INTERVAL: Duration = Duration::from_millis(250);

/// Static preparation limits owned by the backend Worker. These bound queued
/// and running jobs, independently of active Task and completion limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskPreparationLimits {
    per_context: usize,
    tasks: usize,
    bytes: usize,
    workers: usize,
}

impl TaskPreparationLimits {
    pub fn try_new(
        per_context: usize,
        tasks: usize,
        bytes: usize,
        workers: usize,
    ) -> Result<Self, String> {
        if per_context == 0 || tasks == 0 || bytes == 0 || workers == 0 {
            return Err("preparation limits must be nonzero".to_owned());
        }
        if per_context > tasks {
            return Err(
                "per-context preparation task limit must not exceed the backend task limit"
                    .to_owned(),
            );
        }
        if workers > tasks {
            return Err(
                "preparation worker limit must not exceed the backend task limit".to_owned(),
            );
        }
        Ok(Self {
            per_context,
            tasks,
            bytes,
            workers,
        })
    }
    pub const fn per_context(self) -> usize {
        self.per_context
    }
    pub const fn tasks(self) -> usize {
        self.tasks
    }
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    pub const fn workers(self) -> usize {
        self.workers
    }
}

impl Default for TaskPreparationLimits {
    fn default() -> Self {
        Self {
            per_context: 4096,
            tasks: 32768,
            bytes: 256 * 1024 * 1024,
            workers: 4,
        }
    }
}

#[cfg(test)]
mod preparation_limit_tests {
    use super::TaskPreparationLimits;
    #[test]
    fn deployable_preparation_limits_reject_inverted_positions_and_zero_values() {
        for args in [
            (0, 2, 128, 1),
            (1, 0, 128, 1),
            (1, 2, 0, 1),
            (1, 2, 128, 0),
            (3, 2, 128, 1),
            (1, 2, 128, 3),
        ] {
            assert!(
                TaskPreparationLimits::try_new(args.0, args.1, args.2, args.3).is_err(),
                "{args:?}"
            );
        }
        assert!(TaskPreparationLimits::try_new(1, 2, 128, 1).is_ok());
    }
}
