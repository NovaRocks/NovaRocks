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

use std::cell::RefCell;

use crate::query_execution::cancellation::QueryCancellationView;

thread_local! {
    static QUERY_CANCELLATION_VIEW: RefCell<Option<QueryCancellationView>> = const { RefCell::new(None) };
}

struct QueryCancellationViewGuard {
    previous: Option<QueryCancellationView>,
}

impl Drop for QueryCancellationViewGuard {
    fn drop(&mut self) {
        QUERY_CANCELLATION_VIEW.with(|cell| {
            cell.replace(self.previous.take());
        });
    }
}

pub(crate) fn with_query_cancellation_view<T>(
    cancellation: QueryCancellationView,
    f: impl FnOnce() -> T,
) -> T {
    let _guard = QUERY_CANCELLATION_VIEW.with(|cell| QueryCancellationViewGuard {
        previous: cell.replace(Some(cancellation)),
    });
    f()
}

pub(crate) fn current_query_cancellation_view() -> Option<QueryCancellationView> {
    QUERY_CANCELLATION_VIEW.with(|cell| cell.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};

    #[test]
    fn request_scoped_view_restores_state_after_panic() {
        let source = QueryCancellationSource::new();
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_query_cancellation_view(source.view(), || panic!("boom"));
        }));
        assert!(panic_result.is_err(), "closure should panic");
        assert!(current_query_cancellation_view().is_none());
    }

    #[test]
    fn captured_view_remains_live_after_scope_exit() {
        let source = QueryCancellationSource::new();
        let captured = with_query_cancellation_view(source.view(), || {
            current_query_cancellation_view().expect("request-scoped view is available")
        });
        assert!(current_query_cancellation_view().is_none());
        source.request(QueryCancellationReason::ClientDisconnected);
        assert!(captured.is_cancelled());
    }
}
