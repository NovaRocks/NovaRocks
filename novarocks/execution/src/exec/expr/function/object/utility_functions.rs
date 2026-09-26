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
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Int8Array, Int16Array, Int32Array, Int64Array,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub fn eval_sleep(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    arena
        .runtime_error_binding()
        .map_err(|_| "SLEEP requires an exact fragment runtime binding".to_string())?;
    arena.check_runtime_error()?;
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BooleanBuilder::with_capacity(input.len());

    macro_rules! eval_sleep_array {
        ($arr:expr, $context:expr) => {{
            for row in 0..$arr.len() {
                arena.check_runtime_error()?;
                if $arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let seconds = $arr.value(row).max(0) as u64;
                arena.wait_interruptibly(Duration::from_secs(seconds))?;
                builder.append_value(true);
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    if let Some(arr) = input.as_any().downcast_ref::<Int8Array>() {
        eval_sleep_array!(arr, "Int8");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int16Array>() {
        eval_sleep_array!(arr, "Int16");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int32Array>() {
        eval_sleep_array!(arr, "Int32");
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int64Array>() {
        eval_sleep_array!(arr, "Int64");
    }

    Err(format!(
        "sleep expects integer input, got {:?}",
        input.data_type()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn input(values: Vec<Option<i64>>) -> (ExprArena, ExprId, Chunk) {
        let slot = SlotId::new(1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "seconds",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[slot]).unwrap();
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema).unwrap();
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(slot), DataType::Int64);
        (arena, arg, chunk)
    }

    #[test]
    fn sleep_without_a_fragment_runtime_binding_is_rejected() {
        for values in [vec![Some(60)], vec![None], vec![Some(0)]] {
            let (arena, arg, chunk) = input(values);
            assert_eq!(
                eval_sleep(&arena, arg, &[arg], &chunk).unwrap_err(),
                "SLEEP requires an exact fragment runtime binding"
            );
        }
    }

    #[test]
    fn sleep_preserves_null_negative_zero_and_positive_results() {
        let (mut arena, arg, chunk) = input(vec![None, Some(-1), Some(0), Some(1)]);
        arena.bind_runtime_error(Arc::new(RuntimeErrorState::default()));
        let result = eval_sleep(&arena, arg, &[arg], &chunk).unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(result.is_null(0));
        for row in 1..4 {
            assert!(result.value(row));
        }
    }

    #[test]
    fn sleep_cancellation_stops_the_chunk_and_preserves_exact_owner() {
        let (mut arena, arg, chunk) = input(vec![Some(60), Some(60)]);
        let stopped = Arc::new(RuntimeErrorState::default());
        arena.bind_runtime_error(Arc::clone(&stopped));
        let (tx, rx) = std::sync::mpsc::channel();
        let thread =
            std::thread::spawn(move || tx.send(eval_sleep(&arena, arg, &[arg], &chunk)).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while stopped.waiting_count() == 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let entered_sleep = stopped.waiting_count() == 1;
        stopped.set_error("exact fragment cancelled".to_string());
        stopped.set_error("later failure".to_string());
        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("SLEEP must physically return after cancellation");
        assert_eq!(result.unwrap_err(), "exact fragment cancelled");
        thread.join().unwrap();
        assert!(
            entered_sleep,
            "SLEEP must enter its real interruptible wait"
        );
        assert_eq!(stopped.waiting_count(), 0);
        let (mut sibling, arg, chunk) = input(vec![Some(0)]);
        sibling.bind_runtime_error(Arc::new(RuntimeErrorState::default()));
        assert!(eval_sleep(&sibling, arg, &[arg], &chunk).is_ok());
        let (mut already_stopped, arg, chunk) = input(vec![Some(60)]);
        already_stopped.bind_runtime_error(stopped);
        assert_eq!(
            eval_sleep(&already_stopped, arg, &[arg], &chunk).unwrap_err(),
            "exact fragment cancelled"
        );
    }
}
