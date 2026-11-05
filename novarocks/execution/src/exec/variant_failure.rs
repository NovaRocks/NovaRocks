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
use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug)]
pub(crate) enum SdkStage {
    AssembleInput,
    Extract,
}
#[derive(Debug)]
struct VariantSdkFailure {
    stage: SdkStage,
    operation: &'static str,
    source: arrow::error::ArrowError,
}
impl fmt::Display for VariantSdkFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?}: {}", self.operation, self.stage, self.source)
    }
}
impl Error for VariantSdkFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for VariantSdkFailure {
    fn failure_fact(&self) -> FailureFact {
        use arrow::error::ArrowError;
        let (class, reason) = match (&self.stage, &self.source) {
            (_, ArrowError::MemoryError(_)) => (
                FailureClass::Resource,
                ExecutionReason::VariantKernelAllocation,
            ),
            (
                SdkStage::Extract,
                ArrowError::ParseError(_)
                | ArrowError::CastError(_)
                | ArrowError::JsonError(_)
                | ArrowError::ComputeError(_)
                | ArrowError::InvalidArgumentError(_)
                | ArrowError::ParquetError(_)
                | ArrowError::DivideByZero
                | ArrowError::ArithmeticOverflow(_),
            ) => (FailureClass::Data, ExecutionReason::VariantKernelValue),
            _ => (
                FailureClass::Invariant,
                ExecutionReason::VariantKernelContract,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(crate) fn sdk_at(
    operation: &'static str,
    stage: SdkStage,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(VariantSdkFailure {
        operation,
        stage,
        source,
    })
}
#[derive(Debug)]
struct VariantRowFailure {
    operation: &'static str,
    row: usize,
    source: novarocks_types::value::variant::VariantFailure,
}
impl fmt::Display for VariantRowFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: variant row {}: {}",
            self.operation, self.row, self.source
        )
    }
}
impl Error for VariantRowFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
impl FailureProjection for VariantRowFailure {
    fn failure_fact(&self) -> FailureFact {
        self.source.failure_fact()
    }
}
pub(crate) fn input_row(
    operation: &'static str,
    row: usize,
    source: novarocks_types::value::variant::VariantFailure,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(VariantRowFailure {
        operation,
        row,
        source,
    })
}
