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

//! Startup-frozen retry bounds for one task-update request.

use std::time::Duration;

// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)
/// Server-frozen retry policy for one TaskUpdate request.
///
/// The coordination owner carries this value into a round; protocol adapters
/// consume the validated bounds and never consult process-global configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskUpdateRetryPolicy {
    rpc_timeout: Duration,
    error_duration: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl TaskUpdateRetryPolicy {
    pub fn try_new(
        rpc_timeout: Duration,
        error_duration: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, String> {
        if rpc_timeout.is_zero()
            || error_duration.is_zero()
            || initial_backoff.is_zero()
            || max_backoff.is_zero()
        {
            return Err("task update retry durations must be greater than zero".to_owned());
        }
        if initial_backoff > max_backoff {
            return Err("task update retry initial backoff must not exceed max backoff".to_owned());
        }
        if rpc_timeout > error_duration {
            return Err("task update rpc timeout must not exceed error duration".to_owned());
        }
        if max_backoff > error_duration {
            return Err("task update retry max backoff must not exceed error duration".to_owned());
        }
        Ok(Self {
            rpc_timeout,
            error_duration,
            initial_backoff,
            max_backoff,
        })
    }

    pub const fn rpc_timeout(self) -> Duration {
        self.rpc_timeout
    }

    pub const fn error_duration(self) -> Duration {
        self.error_duration
    }

    pub const fn initial_backoff(self) -> Duration {
        self.initial_backoff
    }

    pub const fn max_backoff(self) -> Duration {
        self.max_backoff
    }
}

impl Default for TaskUpdateRetryPolicy {
    fn default() -> Self {
        Self::try_new(
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .expect("default task update retry policy is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_bounds() {
        assert!(
            TaskUpdateRetryPolicy::try_new(
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .is_err()
        );
        assert!(
            TaskUpdateRetryPolicy::try_new(
                Duration::from_secs(2),
                Duration::from_secs(1),
                Duration::from_millis(1),
                Duration::from_millis(1),
            )
            .is_err()
        );
    }

    #[test]
    fn exposes_the_validated_bounds() {
        let policy = TaskUpdateRetryPolicy::try_new(
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(2),
            Duration::from_millis(4),
        )
        .expect("bounds are valid");
        assert_eq!(policy.rpc_timeout(), Duration::from_millis(10));
        assert_eq!(policy.error_duration(), Duration::from_millis(20));
        assert_eq!(policy.initial_backoff(), Duration::from_millis(2));
        assert_eq!(policy.max_backoff(), Duration::from_millis(4));
    }
}
