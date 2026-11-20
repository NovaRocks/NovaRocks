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

use super::*;
use arrow::array::{Array, DictionaryArray, StringViewArray};
use arrow::datatypes::Int32Type;
use futures::StreamExt;

#[test]
fn the_same_iceberg_view_scan_preserves_a_proven_dictionary_or_ordinary_view() {
    use crate::typed_read::reader_test_fixture::{FIRST_VALUE, SECOND_VALUE, open_view_scan};
    for keep_dictionary in [false, true] {
        let runtime = tokio_runtime();
        let dir = tempfile::tempdir().unwrap();
        let budget = ConnectorPollBudget::new();
        let mut stream = open_view_scan(
            &dir.path().join("real-file.parquet"),
            runtime.handle(),
            &budget,
            keep_dictionary,
        );
        let mut previous: Option<ArrayRef> = None;
        for (batch, expected) in [FIRST_VALUE, FIRST_VALUE, SECOND_VALUE, SECOND_VALUE]
            .into_iter()
            .enumerate()
        {
            budget.refill(64);
            let page = runtime.block_on(stream.next()).unwrap().unwrap();
            let (rows, columns) = page.into_columns().unwrap();
            assert_eq!(rows, 4);
            if keep_dictionary {
                let dictionary = columns[0]
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .unwrap();
                let values = dictionary
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(values.value(dictionary.keys().value(0) as usize), expected);
                assert!(dictionary.is_null(1));
                if let Some(previous) = &previous {
                    assert_eq!(Arc::ptr_eq(previous, dictionary.values()), batch % 2 != 0);
                }
                previous = Some(dictionary.values().clone());
            } else {
                let values = columns[0]
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                assert_eq!(values.value(0), expected);
                assert!(values.is_null(1));
            }
        }
        let read = stream.first_file_batch().unwrap();
        assert_eq!(
            (
                read.max_read_rows,
                read.max_read_bytes,
                read.reader_batch_rows,
                read.decoded_rows
            ),
            (4, 1 << 20, 4, 4)
        );
        budget.refill(64);
        assert!(runtime.block_on(stream.next()).is_none());
        runtime.block_on(stream.close()).unwrap();
    }
}

#[test]
fn decoder_budget_exhaustion_is_local_to_the_polling_source() {
    use crate::typed_read::reader_test_fixture::open_view_scan;
    use std::task::Poll;

    let runtime = tokio_runtime();
    let dir = tempfile::tempdir().unwrap();
    let first_budget = ConnectorPollBudget::new();
    let second_budget = ConnectorPollBudget::new();
    let mut first = open_view_scan(
        &dir.path().join("first.parquet"),
        runtime.handle(),
        &first_budget,
        false,
    );
    let mut second = open_view_scan(
        &dir.path().join("second.parquet"),
        runtime.handle(),
        &second_budget,
        false,
    );
    second_budget.refill(64);
    runtime.block_on(async {
        std::future::poll_fn(|cx| {
            assert!(first.poll_next_unpin(cx).is_pending());
            if first_budget.exhaustions() == 0 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        assert_eq!(first_budget.exhaustions(), 1);
        assert_eq!(second_budget.remaining(), 64);
        assert_eq!(second.next().await.unwrap().unwrap().position_count(), 4);
        assert_eq!(second_budget.exhaustions(), 0);
        assert_eq!(first_budget.remaining(), 0);
        first.close().await.unwrap();
        second.close().await.unwrap();
    });
}
