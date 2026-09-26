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
//! Common utilities and helpers for integration tests.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::time::Duration;

use novarocks_types::UniqueId;

/// Generate a test query ID.
pub fn test_query_id() -> UniqueId {
    UniqueId::new(1234567890, 9876543210)
}

/// Generate a unique query ID based on test name.
pub fn unique_query_id(test_name: &str) -> UniqueId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    test_name.hash(&mut hasher);
    let hash = hasher.finish();

    UniqueId::new(hash as i64, (hash >> 32) as i64)
}

/// Wait for a condition to become true, with timeout.
pub fn wait_for<F>(mut condition: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Run `f` and fail the test if it does not finish within `timeout`.
pub fn run_with_timeout<F, T>(timeout: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });

    match rx.recv_timeout(timeout) {
        Ok(v) => v,
        Err(_) => panic!("test timed out after {:?}", timeout),
    }
}

/// Assert that a result is Ok and return the value.
#[macro_export]
macro_rules! assert_ok {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("Expected Ok, got Err: {:?}", e),
        }
    };
    ($result:expr, $message:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("{}: {:?}", $message, e),
        }
    };
}

/// Assert that a result is Err.
#[macro_export]
macro_rules! assert_err {
    ($result:expr) => {
        match $result {
            Ok(value) => panic!("Expected Err, got Ok: {:?}", value),
            Err(e) => e,
        }
    };
}

// ---------------------------------------------------------------------------
// Function-test helpers (used by tests/function_*.rs integration tests)
// ---------------------------------------------------------------------------

/// Create a single-row Chunk for use in function unit tests.
pub fn chunk_len_1() -> novarocks_execution::exec::chunk::Chunk {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dummy",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .expect("chunk schema");
    novarocks_execution::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

/// Push an Int64 literal into the arena and return its ExprId.
pub fn literal_i64(
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    v: i64,
) -> novarocks_execution::exec::expr::ExprId {
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
}

/// Push a Float64 literal into the arena and return its ExprId.
pub fn literal_f64(
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    v: f64,
) -> novarocks_execution::exec::expr::ExprId {
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Float64(v)))
}

/// Push a UTF-8 string literal into the arena and return its ExprId.
pub fn literal_string(
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    v: &str,
) -> novarocks_execution::exec::expr::ExprId {
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

/// Push a typed NULL literal into the arena and return its ExprId.
pub fn typed_null(
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    data_type: arrow::datatypes::DataType,
) -> novarocks_execution::exec::expr::ExprId {
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue};
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}

/// Create a Chunk whose columns are bound to slot ids `1..=columns.len()` in order.
pub fn chunk_from_columns(
    columns: Vec<arrow::array::ArrayRef>,
) -> novarocks_execution::exec::chunk::Chunk {
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_execution::exec::chunk::ChunkSchema;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| Field::new(format!("c{}", idx + 1), column.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slot_ids = (1..=columns.len())
        .map(|slot| SlotId::new(slot as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)
            .expect("chunk schema");
    novarocks_execution::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

/// Push a reference to column `slot` of a `chunk_from_columns` chunk.
pub fn slot_ref(
    arena: &mut novarocks_execution::exec::expr::ExprArena,
    slot: u32,
    data_type: arrow::datatypes::DataType,
) -> novarocks_execution::exec::expr::ExprId {
    use novarocks_execution::exec::expr::ExprNode;
    use novarocks_types::SlotId;
    arena.push_typed(ExprNode::SlotId(SlotId::new(slot)), data_type)
}

/// Split every column of `columns` into single-row chunks, one per row.
///
/// Evaluating a function once per row this way reproduces per-row semantics,
/// which multi-row evaluation must match.
pub fn single_row_chunks(
    columns: &[arrow::array::ArrayRef],
) -> Vec<novarocks_execution::exec::chunk::Chunk> {
    let rows = columns.first().map_or(0, |column| column.len());
    (0..rows)
        .map(|row| chunk_from_columns(columns.iter().map(|column| column.slice(row, 1)).collect()))
        .collect()
}
