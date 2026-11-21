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

//! Connector-owned stable reasons projected across the native boundary.

use crate::{FailureReason, domain};
use std::num::NonZeroU16;

/// Iceberg exhausted the configured number of concurrently open partition writers.
pub const ICEBERG_TOO_MANY_OPEN_PARTITIONS: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(50).unwrap());

/// A catalog transaction could not commit after exhausting its contention retries.
pub const COMMIT_TRANSACTION_CONFLICT: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(51).unwrap());

/// Iceberg row-delta validation found a change that invalidates the write.
pub const ICEBERG_COMMIT_VALIDATION_CONFLICT: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(52).unwrap());

/// The catalog may have applied the Iceberg write, but the outcome is unknown.
pub const ICEBERG_COMMIT_STATE_UNKNOWN: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(53).unwrap());

/// Iceberg metadata required by maintenance is missing or invalid.
pub const ICEBERG_INVALID_METADATA: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(54).unwrap());

/// An Iceberg maintenance filesystem operation failed.
pub const ICEBERG_FILESYSTEM_ERROR: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(55).unwrap());

/// An explicit Iceberg metadata maintenance procedure failed to commit.
pub const ICEBERG_COMMIT_ERROR: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(56).unwrap());
