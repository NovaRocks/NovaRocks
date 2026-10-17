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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef};
use novarocks_functions::builtin::map_lookup_core::{
    MapLookupFailure, MapLookupInputs, lookup_observed,
};
use std::convert::Infallible;
pub fn eval_element_at<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let map_arr = arena.eval(args[0], chunk)?;
    let key_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let inputs = MapLookupInputs::new(&map_arr, &key_arr, check_arr.as_ref())?;
    inputs.validate_legacy_lengths()?;
    Ok((lookup_observed(
        &inputs,
        novarocks_functions::Selection::all(inputs.map.len()),
        |_, row| Ok::<_, Infallible>(inputs.legacy_rows(row)),
        arena.data_type(expr),
        |_, _| Ok::<(), Infallible>(()),
        |_, message| Err(message),
        &mut |_| Ok::<(), Infallible>(()),
    )
    .map_err(|failure| match failure {
        MapLookupFailure::Data(message) => message,
        MapLookupFailure::Control(never) => match never {},
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        chunk::ChunkSchema,
        expr::{ExprEvaluation, ExprNode},
    };
    use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
    use arrow::array::{Int32Array, MapArray, NullArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::OffsetBuffer;
    use novarocks_functions::{KernelDiagnostic, KernelFailure};
    use novarocks_types::SlotId;
    use std::{cell::RefCell, sync::Arc};

    struct FailingProbe {
        arena: ExprArena,
        map_id: ExprId,
        map: ArrayRef,
        cause: ExecutionFailure,
        calls: RefCell<Vec<ExprId>>,
    }
    impl ExprEvaluation for FailingProbe {
        type Error = ExecutionFailure;
        fn arena(&self) -> &ExprArena {
            &self.arena
        }
        fn eval(&self, id: ExprId, _: &Chunk) -> Result<ArrayRef, Self::Error> {
            self.calls.borrow_mut().push(id);
            if id == self.map_id {
                Ok(self.map.clone())
            } else {
                Err(self.cause.clone())
            }
        }
        fn slice_input(&self, _: &Chunk, _: usize, _: usize) -> Result<Chunk, Self::Error> {
            panic!("empty Map must not retry a failed probe")
        }
        fn recoverable_error(&self, _: &Self::Error) -> bool {
            false
        }
        fn annotate_error(
            &self,
            error: Self::Error,
            _: impl FnOnce(&str) -> String,
        ) -> Self::Error {
            error
        }
    }

    #[test]
    fn original_empty_map_lookup_eagerly_preserves_probe_data_and_control_failures() {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Null, false)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ];
        let entries = StructArray::new(
            fields.into(),
            vec![
                Arc::new(NullArray::new(0)),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0_i32, 0].into()),
            entries,
            None,
            false,
        ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "dummy",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![0]))],
        )
        .unwrap();
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut causes = vec![ExecutionFailure::from(
            "original probe row error".to_owned(),
        )];
        causes.extend(
            [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("original internal")),
                KernelFailure::Operational(KernelDiagnostic::new("original operational")),
                KernelFailure::InstanceFailed,
            ]
            .into_iter()
            .map(ExecutionFailure::from),
        );
        for cause in causes {
            let cause = cause.at_operator(17, PipelineOperation::Pull);
            let mut arena = ExprArena::default();
            let map_id =
                arena.push_typed(ExprNode::SlotId(SlotId::new(2)), map.data_type().clone());
            let probe_id = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Utf8);
            let output_id = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
            let evaluator = FailingProbe {
                arena,
                map_id,
                map: map.clone(),
                cause: cause.clone(),
                calls: RefCell::new(vec![]),
            };
            assert_eq!(
                eval_element_at(&evaluator, output_id, &[map_id, probe_id], &chunk).unwrap_err(),
                cause
            );
            assert_eq!(*evaluator.calls.borrow(), vec![map_id, probe_id]);
        }
    }
}
