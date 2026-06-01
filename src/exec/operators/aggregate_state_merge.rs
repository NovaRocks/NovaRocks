use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout;
use crate::connector::starrocks::table::mv_shape::AggregateMvShape;
use crate::engine::mv::iceberg_aggregate_state::merge_aggregate_state_chunks_for_change_stream;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone, Debug)]
pub struct AggregateStateMergePlan {
    pub(crate) old_input: Box<crate::exec::node::ExecNode>,
    pub(crate) delta_input: Box<crate::exec::node::ExecNode>,
    pub(crate) layout: AggregateMvLayout,
}

#[derive(Clone, Debug)]
pub struct AggregateStatePhysicalizePlan {
    pub(crate) input: Box<crate::exec::node::ExecNode>,
    pub(crate) layout: AggregateMvLayout,
    pub(crate) shape: AggregateMvShape,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum AggregateStateMergeInput {
    Old,
    Delta,
}

#[derive(Clone)]
pub(crate) struct AggregateStateMergeSharedState {
    inner: Arc<Mutex<AggregateStateMergeState>>,
    observable: Arc<Observable>,
}

struct AggregateStateMergeState {
    old_chunks: Vec<Chunk>,
    delta_chunks: Vec<Chunk>,
    remaining_old_producers: usize,
    remaining_delta_producers: usize,
    output: Option<Result<VecDeque<Chunk>, String>>,
}

impl AggregateStateMergeSharedState {
    pub(crate) fn new(old_producers: usize, delta_producers: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(AggregateStateMergeState {
                old_chunks: Vec::new(),
                delta_chunks: Vec::new(),
                remaining_old_producers: old_producers.max(1),
                remaining_delta_producers: delta_producers.max(1),
                output: None,
            })),
            observable: Arc::new(Observable::new()),
        }
    }

    fn push_chunk(&self, input: AggregateStateMergeInput, chunk: Chunk) {
        if chunk.is_empty() {
            return;
        }
        let mut guard = self.inner.lock().expect("aggregate state merge lock");
        match input {
            AggregateStateMergeInput::Old => guard.old_chunks.push(chunk),
            AggregateStateMergeInput::Delta => guard.delta_chunks.push(chunk),
        }
    }

    fn finish_input(&self, input: AggregateStateMergeInput) {
        let notify = self.observable.defer_notify();
        let should_notify = {
            let mut guard = self.inner.lock().expect("aggregate state merge lock");
            match input {
                AggregateStateMergeInput::Old => {
                    guard.remaining_old_producers = guard.remaining_old_producers.saturating_sub(1);
                }
                AggregateStateMergeInput::Delta => {
                    guard.remaining_delta_producers =
                        guard.remaining_delta_producers.saturating_sub(1);
                }
            }
            guard.remaining_old_producers == 0 && guard.remaining_delta_producers == 0
        };
        if should_notify {
            notify.arm();
        }
    }

    fn has_output(&self) -> bool {
        let guard = self.inner.lock().expect("aggregate state merge lock");
        match &guard.output {
            Some(Ok(chunks)) => !chunks.is_empty(),
            Some(Err(_)) => true,
            None => guard.remaining_old_producers == 0 && guard.remaining_delta_producers == 0,
        }
    }

    fn pop_output(&self, layout: &AggregateMvLayout) -> Result<Option<Chunk>, String> {
        let mut guard = self.inner.lock().expect("aggregate state merge lock");
        if guard.output.is_none() {
            if guard.remaining_old_producers != 0 || guard.remaining_delta_producers != 0 {
                return Ok(None);
            }
            let output = merge_aggregate_state_chunks_for_change_stream(
                &guard.old_chunks,
                &guard.delta_chunks,
                layout,
            )
            .map(VecDeque::from);
            guard.output = Some(output);
        }
        match guard.output.as_mut().expect("output initialized") {
            Ok(chunks) => Ok(chunks.pop_front()),
            Err(err) => Err(err.clone()),
        }
    }

    fn is_done(&self) -> bool {
        let guard = self.inner.lock().expect("aggregate state merge lock");
        matches!(&guard.output, Some(Ok(chunks)) if chunks.is_empty())
    }

    fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }
}

pub(crate) struct AggregateStateMergeSinkFactory {
    name: String,
    input: AggregateStateMergeInput,
    state: AggregateStateMergeSharedState,
}

impl AggregateStateMergeSinkFactory {
    pub(crate) fn new(
        input: AggregateStateMergeInput,
        state: AggregateStateMergeSharedState,
        node_id: i32,
    ) -> Self {
        let input_name = match input {
            AggregateStateMergeInput::Old => "Old",
            AggregateStateMergeInput::Delta => "Delta",
        };
        let name = if node_id >= 0 {
            format!("AggregateStateMerge{input_name}Sink (id={node_id})")
        } else {
            format!("AggregateStateMerge{input_name}Sink")
        };
        Self { name, input, state }
    }
}

impl OperatorFactory for AggregateStateMergeSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStateMergeSinkOperator {
            name: self.name.clone(),
            input: self.input,
            state: self.state.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct AggregateStateMergeSinkOperator {
    name: String,
    input: AggregateStateMergeInput,
    state: AggregateStateMergeSharedState,
    finished: bool,
}

impl Operator for AggregateStateMergeSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for AggregateStateMergeSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.finished {
            self.state.push_chunk(self.input, chunk);
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if !self.finished {
            self.finished = true;
            self.state.finish_input(self.input);
        }
        Ok(())
    }
}

pub(crate) struct AggregateStateMergeSourceFactory {
    name: String,
    plan: AggregateStateMergePlan,
    state: AggregateStateMergeSharedState,
}

impl AggregateStateMergeSourceFactory {
    pub(crate) fn new(
        plan: AggregateStateMergePlan,
        state: AggregateStateMergeSharedState,
        node_id: i32,
    ) -> Self {
        let name = if node_id >= 0 {
            format!("AggregateStateMergeSource (id={node_id})")
        } else {
            "AggregateStateMergeSource".to_string()
        };
        Self { name, plan, state }
    }
}

impl OperatorFactory for AggregateStateMergeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStateMergeSourceOperator {
            name: self.name.clone(),
            plan: self.plan.clone(),
            state: self.state.clone(),
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AggregateStateMergeSourceOperator {
    name: String,
    plan: AggregateStateMergePlan,
    state: AggregateStateMergeSharedState,
}

impl Operator for AggregateStateMergeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.state.is_done()
    }
}

impl ProcessorOperator for AggregateStateMergeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        self.state.has_output()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("aggregate state merge source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        self.state.pop_output(&self.plan.layout)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.state.observable())
    }
}

pub(crate) struct AggregateStatePhysicalizeProcessorFactory {
    name: String,
    plan: AggregateStatePhysicalizePlan,
}

impl AggregateStatePhysicalizeProcessorFactory {
    pub(crate) fn new(plan: AggregateStatePhysicalizePlan, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("AggregateStatePhysicalize (id={node_id})")
        } else {
            "AggregateStatePhysicalize".to_string()
        };
        Self { name, plan }
    }
}

impl OperatorFactory for AggregateStatePhysicalizeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(AggregateStatePhysicalizeProcessor {
            name: self.name.clone(),
            layout: self.plan.layout.clone(),
            shape: self.plan.shape.clone(),
            pending_output: None,
            finishing: false,
            finished: false,
        })
    }
}

struct AggregateStatePhysicalizeProcessor {
    name: String,
    layout: AggregateMvLayout,
    shape: AggregateMvShape,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
}

impl Operator for AggregateStatePhysicalizeProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for AggregateStatePhysicalizeProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err(
                "aggregate state physicalize received input while output buffer is full"
                    .to_string(),
            );
        }
        self.pending_output = Some(
            crate::connector::starrocks::table::mv_agg_state::materialize_aggregate_state_chunk(
                chunk,
                &self.layout,
                &self.shape,
            )?,
        );
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.pending_output.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, LargeBinaryBuilder, StringArray};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::connector::starrocks::table::ddl::starrocks_physical_column;
    use crate::connector::starrocks::table::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
    };
    use crate::connector::starrocks::table::mv_shape::{
        AggregateFunctionKind, AggregateMvShape, IncrementalMvShape, classify_incremental_mv_query,
    };
    use crate::connector::starrocks::table::state_codec::{encode_count_state, encode_sum_int64};
    use crate::engine::record_batch_to_chunk;
    use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::exec::pipeline::executor::execute_plan_with_pipeline;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::sql::parser::ast::SqlType;

    #[test]
    fn merge_operator_emits_delete_for_old_state_and_insert_for_new_state() {
        let old = aggregate_state_chunk_for_test(vec![("east", 2_i64, 300_i64)]);
        let delta = signed_delta_state_chunk_for_test(vec![("east", -1_i8, 1_i64, -100_i64)]);
        let output = run_merge_operator_for_test(vec![old], vec![delta]).unwrap();

        assert_eq!(output.batch.num_rows(), 2);
        assert_eq!(string_value(&output, "region", 0), "east");
        assert_eq!(int8_value(&output, "__change_op", 0), CHANGE_OP_DELETE);
        assert_eq!(int64_value(&output, "c", 0), 2);
        assert_eq!(int64_value(&output, "s", 0), 300);
        assert_eq!(string_value(&output, "region", 1), "east");
        assert_eq!(int8_value(&output, "__change_op", 1), CHANGE_OP_INSERT);
        assert_eq!(int64_value(&output, "c", 1), 1);
        assert_eq!(int64_value(&output, "s", 1), 200);
    }

    #[test]
    fn merge_operator_emits_insert_only_for_new_group() {
        let delta = signed_delta_state_chunk_for_test(vec![("west", 1_i8, 1_i64, 80_i64)]);
        let output = run_merge_operator_for_test(vec![], vec![delta]).unwrap();

        assert_eq!(output.batch.num_rows(), 1);
        assert_eq!(string_value(&output, "region", 0), "west");
        assert_eq!(int8_value(&output, "__change_op", 0), CHANGE_OP_INSERT);
        assert_eq!(int64_value(&output, "c", 0), 1);
        assert_eq!(int64_value(&output, "s", 0), 80);
    }

    #[test]
    fn merge_pipeline_physicalizes_delta_child_before_merging() {
        let layout = aggregate_layout_for_test();
        let old = aggregate_state_chunk_for_test(vec![("east", 2_i64, 300_i64)]);
        let delta_state = state_shaped_delta_chunk_for_test(vec![("east", -1_i64, -100_i64)]);
        let plan = ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::AggregateStateMerge(AggregateStateMergePlan {
                    old_input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: old,
                            node_id: 1,
                        }),
                    }),
                    delta_input: Box::new(ExecNode {
                        kind: ExecNodeKind::AggregateStatePhysicalize(
                            AggregateStatePhysicalizePlan {
                                input: Box::new(ExecNode {
                                    kind: ExecNodeKind::Values(ValuesNode {
                                        chunk: delta_state,
                                        node_id: 2,
                                    }),
                                }),
                                layout: layout.clone(),
                                shape: aggregate_shape_for_test(),
                            },
                        ),
                    }),
                    layout,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        execute_plan_with_pipeline(
            plan,
            false,
            std::time::Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            Arc::new(RuntimeState::default()),
            None,
            None,
            None,
        )
        .expect("execute aggregate state merge pipeline");
        let chunks = handle.take_chunks();
        let schema = chunks.first().expect("pipeline output").batch.schema();
        let batches = chunks
            .iter()
            .map(|chunk| chunk.batch.clone())
            .collect::<Vec<_>>();
        let output = record_batch_to_chunk(
            concat_batches(&schema, batches.iter()).expect("concat pipeline output"),
        )
        .expect("output chunk");

        assert_eq!(output.batch.num_rows(), 2);
        assert_eq!(int8_value(&output, "__change_op", 0), CHANGE_OP_DELETE);
        assert_eq!(int64_value(&output, "c", 0), 2);
        assert_eq!(int64_value(&output, "s", 0), 300);
        assert_eq!(int8_value(&output, "__change_op", 1), CHANGE_OP_INSERT);
        assert_eq!(int64_value(&output, "c", 1), 1);
        assert_eq!(int64_value(&output, "s", 1), 200);
    }

    fn run_merge_operator_for_test(
        old_chunks: Vec<Chunk>,
        delta_chunks: Vec<Chunk>,
    ) -> Result<Chunk, String> {
        let layout = aggregate_layout_for_test();
        let state = RuntimeState::default();
        let shared = AggregateStateMergeSharedState::new(1, 1);
        let old_factory =
            AggregateStateMergeSinkFactory::new(AggregateStateMergeInput::Old, shared.clone(), 7);
        let delta_factory =
            AggregateStateMergeSinkFactory::new(AggregateStateMergeInput::Delta, shared.clone(), 7);
        let source_factory = AggregateStateMergeSourceFactory::new(
            AggregateStateMergePlan {
                old_input: Box::new(crate::exec::node::ExecNode {
                    kind: crate::exec::node::ExecNodeKind::Values(
                        crate::exec::node::values::ValuesNode {
                            chunk: empty_chunk_for_test(),
                            node_id: -1,
                        },
                    ),
                }),
                delta_input: Box::new(crate::exec::node::ExecNode {
                    kind: crate::exec::node::ExecNodeKind::Values(
                        crate::exec::node::values::ValuesNode {
                            chunk: empty_chunk_for_test(),
                            node_id: -1,
                        },
                    ),
                }),
                layout,
            },
            shared,
            7,
        );
        let mut old_sink = old_factory.create(1, 0);
        let mut delta_sink = delta_factory.create(1, 0);
        let mut source = source_factory.create(1, 0);

        for chunk in old_chunks {
            old_sink
                .as_processor_mut()
                .expect("old sink processor")
                .push_chunk(&state, chunk)?;
        }
        old_sink
            .as_processor_mut()
            .expect("old sink processor")
            .set_finishing(&state)?;
        for chunk in delta_chunks {
            delta_sink
                .as_processor_mut()
                .expect("delta sink processor")
                .push_chunk(&state, chunk)?;
        }
        delta_sink
            .as_processor_mut()
            .expect("delta sink processor")
            .set_finishing(&state)?;

        let source = source
            .as_processor_mut()
            .expect("aggregate state merge source");
        let mut chunks = Vec::new();
        while source.has_output() {
            if let Some(chunk) = source.pull_chunk(&state)? {
                chunks.push(chunk);
            }
        }
        let schema = chunks
            .first()
            .map(|chunk| chunk.batch.schema())
            .ok_or_else(|| "expected non-empty change stream".to_string())?;
        let batches = chunks
            .iter()
            .map(|chunk| chunk.batch.clone())
            .collect::<Vec<_>>();
        let batch = concat_batches(&schema, batches.iter())
            .map_err(|e| format!("concat change stream for test failed: {e}"))?;
        record_batch_to_chunk(batch)
    }

    fn empty_chunk_for_test() -> Chunk {
        physical_chunk_for_test(Vec::new(), None)
    }

    fn aggregate_state_chunk_for_test(rows: Vec<(&str, i64, i64)>) -> Chunk {
        physical_chunk_for_test(rows, None)
    }

    fn signed_delta_state_chunk_for_test(rows: Vec<(&str, i8, i64, i64)>) -> Chunk {
        let rows = rows
            .into_iter()
            .map(|(region, sign, c, s)| {
                let signed_c = c * i64::from(sign);
                (region, signed_c, s)
            })
            .collect();
        physical_chunk_for_test(rows, Some(()))
    }

    fn state_shaped_delta_chunk_for_test(rows: Vec<(&str, i64, i64)>) -> Chunk {
        let mut count_state = LargeBinaryBuilder::new();
        let mut sum_state = LargeBinaryBuilder::new();
        for (_, c, s) in &rows {
            count_state.append_value(encode_count_state(*c));
            sum_state.append_value(encode_sum_int64(*c, *s));
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
                Field::new("__agg_state_s", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(region, _, _)| *region)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(count_state.finish()) as ArrayRef,
                Arc::new(sum_state.finish()) as ArrayRef,
            ],
        )
        .expect("state-shaped delta batch");
        record_batch_to_chunk(batch).expect("state-shaped delta chunk")
    }

    fn physical_chunk_for_test(rows: Vec<(&str, i64, i64)>, _delta: Option<()>) -> Chunk {
        let mut count_state = LargeBinaryBuilder::new();
        let mut sum_state = LargeBinaryBuilder::new();
        for (_, c, s) in &rows {
            count_state.append_value(encode_count_state(*c));
            sum_state.append_value(encode_sum_int64(*c, *s));
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
                Field::new("__agg_state_c", DataType::LargeBinary, false),
                Field::new("__agg_state_s", DataType::LargeBinary, false),
            ])),
            vec![
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(region, _, _)| row_id_for_region(region))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(region, _, _)| *region)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, c, _)| *c).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, s)| *s).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(count_state.finish()) as ArrayRef,
                Arc::new(sum_state.finish()) as ArrayRef,
            ],
        )
        .expect("aggregate state batch");
        record_batch_to_chunk(batch).expect("aggregate state chunk")
    }

    fn aggregate_layout_for_test() -> AggregateMvLayout {
        let row_id_column = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let region_column =
            starrocks_physical_column("region".to_string(), SqlType::String, true, true, false);
        let count_column =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let sum_column =
            starrocks_physical_column("s".to_string(), SqlType::BigInt, true, true, false);
        let count_state_column = starrocks_physical_column(
            "__agg_state_c".to_string(),
            SqlType::Binary,
            false,
            false,
            false,
        );
        let sum_state_column = starrocks_physical_column(
            "__agg_state_s".to_string(),
            SqlType::Binary,
            false,
            false,
            false,
        );

        AggregateMvLayout {
            row_id_column: row_id_column.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: "region".to_string(),
                    data_type: DataType::Utf8,
                    sql_type: SqlType::String,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    source_index: 1,
                },
                AggregateVisibleColumn {
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: true,
                    source_index: 2,
                },
            ],
            state_columns: vec![
                AggregateStateColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::LargeBinary,
                    sql_type: SqlType::Binary,
                    nullable: false,
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Count,
                    state_role: AggregateStateRole::Single,
                    count_star: true,
                },
                AggregateStateColumn {
                    name: "__agg_state_s".to_string(),
                    data_type: DataType::LargeBinary,
                    sql_type: SqlType::Binary,
                    nullable: false,
                    visible_source_index: 2,
                    aggregate_index: 1,
                    function: AggregateFunctionKind::Sum,
                    state_role: AggregateStateRole::Single,
                    count_star: false,
                },
            ],
            aggregate_input_types: vec![None, Some(DataType::Int64)],
            group_key_source_indexes: vec![0],
            physical_columns: vec![
                row_id_column,
                region_column,
                count_column,
                sum_column,
                count_state_column,
                sum_state_column,
            ],
        }
    }

    fn aggregate_shape_for_test() -> AggregateMvShape {
        let dialect = sqlparser::dialect::GenericDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(
            &dialect,
            "select region, count(*) as c, sum(amount) as s from ice.ns.orders group by region",
        )
        .expect("parse aggregate shape query");
        let sqlparser::ast::Statement::Query(query) =
            statements.into_iter().next().expect("one query")
        else {
            panic!("expected query");
        };
        let shape = classify_incremental_mv_query(&query).expect("classify aggregate shape");
        let IncrementalMvShape::Aggregate(shape) = shape else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn row_id_for_region(region: &str) -> String {
        format!("utf8:V:{region}")
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn string_value(chunk: &Chunk, name: &str, row: usize) -> String {
        let index = chunk.batch.schema().index_of(name).expect("column");
        let array = chunk
            .batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        array.value(row).to_string()
    }

    fn int64_value(chunk: &Chunk, name: &str, row: usize) -> i64 {
        let index = chunk.batch.schema().index_of(name).expect("column");
        chunk
            .batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array")
            .value(row)
    }

    fn int8_value(chunk: &Chunk, name: &str, row: usize) -> i8 {
        let index = chunk.batch.schema().index_of(name).expect("column");
        chunk
            .batch
            .column(index)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 array")
            .value(row)
    }
}
