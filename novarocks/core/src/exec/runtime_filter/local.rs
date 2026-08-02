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
//! Runtime-filter key helpers.
//!
//! Responsibilities:
//! - Detects null composite keys while building runtime-filter values.
//!
//! Native runtime-filter data-plane state is injected as a backend-owned session.

use crate::exec::hash_table::key_builder::GroupKeyArrayView;

pub(in crate::exec::runtime_filter) fn row_has_null(
    views: &[GroupKeyArrayView<'_>],
    row: usize,
) -> bool {
    for view in views {
        let is_null = match view {
            GroupKeyArrayView::Int(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Float(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Boolean(arr) => arr.is_null(row),
            GroupKeyArrayView::Utf8(arr) => arr.is_null(row),
            GroupKeyArrayView::Dictionary(dict) => dict.is_null(row),
            GroupKeyArrayView::Date32(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampSecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMillisecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMicrosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampNanosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal128(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal256(arr) => arr.is_null(row),
            GroupKeyArrayView::LargeIntBinary(arr) => arr.is_null(row),
            GroupKeyArrayView::ListUtf8 { list, .. } => list.is_null(row),
            GroupKeyArrayView::ListInt32 { list, .. } => list.is_null(row),
            GroupKeyArrayView::Complex(arr) => arr.is_null(row),
        };
        if is_null {
            return true;
        }
    }
    false
}
