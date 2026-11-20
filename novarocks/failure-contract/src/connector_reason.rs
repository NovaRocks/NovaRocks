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

/// A catalog rejected every eligible publication attempt due to competition.
pub const COMMIT_TRANSACTION_CONFLICT: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(51).unwrap());

// Reason 52 is reserved for row-level validation conflicts.
/// An Iceberg publication was dispatched without a known commit outcome.
pub const ICEBERG_COMMIT_STATE_UNKNOWN: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(53).unwrap());

// Reasons 54 and 55 belong to the Iceberg maintenance error contract.
/// An explicit Iceberg operation failed to commit.
pub const ICEBERG_COMMIT_ERROR: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(56).unwrap());

/// A table procedure rejected supplied or missing arguments.
pub const PROCEDURE_ARGUMENT_INVALID: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(57).unwrap());

/// ADD FILES was disabled in the selected catalog.
pub const ADD_FILES_DISABLED: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(58).unwrap());

/// The requested external source was neither an existing file nor a directory.
pub const LOCATION_NOT_FOUND: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(59).unwrap());

/// A file's physical type cannot be read as the bound target field.
pub const SOURCE_TYPE_MISMATCH: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(60).unwrap());

/// A required target field was absent or had NULL values.
pub const NOT_NULL_VIOLATION: FailureReason =
    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(61).unwrap());
