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

//! Execution-owned filter operations project their own contract, SDK and
//! codec facts. Expression, Chunk and LARGEINT sources pass through unchanged.
use crate::runtime::task::{ExecutionFailure, ExecutionReason};
use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureFact, FailureProjection};
use std::{error::Error, fmt};
#[derive(Debug)]
enum FilterFailure {
    Contract(BoundedDetail),
    Arrow {
        operation: &'static str,
        source: arrow::error::ArrowError,
    },
    Utf8(std::str::Utf8Error),
    Narrow {
        operation: &'static str,
        value: i128,
        source: std::num::TryFromIntError,
    },
}
impl fmt::Display for FilterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::Arrow { operation, source } => write!(f, "runtime filter {operation}: {source}"),
            Self::Utf8(source) => write!(f, "runtime min/max utf8 decode failed: {source}"),
            Self::Narrow {
                operation,
                value,
                source,
            } => write!(
                f,
                "runtime filter {operation} cannot represent {value}: {source}"
            ),
        }
    }
}
impl Error for FilterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(_) => None,
            Self::Arrow { source, .. } => Some(source),
            Self::Utf8(source) => Some(source),
            Self::Narrow { source, .. } => Some(source),
        }
    }
}
impl FailureProjection for FilterFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::Contract(_) => (
                FailureClass::Invariant,
                ExecutionReason::FilterOperationContract,
            ),
            Self::Arrow {
                source: arrow::error::ArrowError::MemoryError(_),
                ..
            } => (
                FailureClass::Resource,
                ExecutionReason::FilterArrowAllocation,
            ),
            Self::Arrow { .. } => (
                FailureClass::Invariant,
                ExecutionReason::FilterArrowContract,
            ),
            Self::Utf8(_) => (FailureClass::Invariant, ExecutionReason::FilterWireUtf8),
            Self::Narrow { .. } => (
                FailureClass::Invariant,
                ExecutionReason::FilterDeclaredWidth,
            ),
        };
        FailureFact::new(class, reason.reason())
    }
}
pub(super) fn contract(detail: impl fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(FilterFailure::Contract(BoundedDetail::truncating_display(
        detail,
    )))
}
pub(super) fn arrow(operation: &'static str, source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(FilterFailure::Arrow { operation, source })
}
pub(super) fn utf8(source: std::str::Utf8Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(FilterFailure::Utf8(source))
}
pub(super) fn narrow(
    operation: &'static str,
    value: i128,
    source: std::num::TryFromIntError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(FilterFailure::Narrow {
        operation,
        value,
        source,
    })
}

#[cfg(test)]
mod sdk_source_properties {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    #[test]
    fn actual_filter_sdk_refusal_keeps_source_but_no_input_schema_graph() {
        let field = Arc::new(Field::new("x", DataType::Int32, true));
        let weak = Arc::downgrade(&field);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let sdk =
            arrow::compute::filter_record_batch(&batch, &BooleanArray::from(vec![true, true]))
                .unwrap_err();
        let expected = sdk.to_string();
        let error = arrow("filter batch", sdk);
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let mut source: &(dyn Error + 'static) = &error;
        loop {
            if let Some(sdk) = source.downcast_ref::<arrow::error::ArrowError>() {
                assert_eq!(sdk.to_string(), expected);
                break;
            }
            source = source.source().expect("actual Arrow refusal source");
        }
        drop(batch);
        assert!(weak.upgrade().is_none());
    }
}
