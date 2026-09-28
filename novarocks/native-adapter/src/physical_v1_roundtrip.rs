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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_execution::exec::chunk::Chunk;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::expr::agg::{
    ExecutionFunctionSetBuilder, contribute_builtin_aggregate_implementations,
};
use novarocks_execution::exec::node::{ExecNodeKind, ExecPlanBuilder};
use novarocks_execution::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use novarocks_execution::exec::pipeline::executor::execute_native_plan_with_pipeline;
use novarocks_execution::exec::pipeline::operator::{Operator, ProcessorOperator};
use novarocks_execution::exec::pipeline::operator_factory::OperatorFactory;
use novarocks_execution::runtime::runtime_state::RuntimeState;
use novarocks_execution::runtime::{
    ExecutionRuntime, ExecutionRuntimeConfig, execution_runtime::ExecutionSpillStorageConfig,
};
use novarocks_physical_plan::{
    ChangeEventSpec, ChangeStreamRoute, Distribution, Edge, EdgeDestination, EdgeId, EdgeKind,
    EdgePartitioning, EdgeSource, ExprKind, FragmentBuilder, FragmentId, FragmentSink,
    JoinDistribution, JoinKey, JoinKind, JoinSide, LiteralValue, NestLoopJoinDistribution,
    NodeKind, OutputPort, PhysicalNode, PhysicalPlan, PhysicalProperties, PipelineDopDomain,
    PlanBuilder, PlanVersionId, ROOT_WRITE_RESULT_SCHEMA_REVISION, ResultField, ResultPort,
    RowMultiplicity, SetOperationKind, ValueId, ValueOrigin, ValueType,
    WRITER_MULTIPLEX_SCHEMA_REVISION, WriterDerivedKind, WriterFinishSpec, WriterRelationField,
    WriterRelationFieldRole, WriterRelationSchema, WriterTarget, WriterTargetField,
};
use novarocks_plan_codec::{PhysicalV1PrivateFacts, PhysicalV1WriteFact};
use novarocks_proto_models::{connector_write, plan};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorRowMutationEffect, ConnectorWriteFieldToken, ConnectorWriteRouteId,
};
use novarocks_types::SlotId;

use crate::fragment_decode_context::NativePlanDecodeContext;
use crate::fragment_plan_decode::decode_node;
use novarocks_native_adapter::fragment_sink::decode_fragment_sink_program;

fn singleton_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn unconstrained_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn append_i64_values(
    builder: &mut FragmentBuilder,
    literal_value: i64,
) -> (novarocks_physical_plan::NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let literal = builder
        .add_expression(
            node,
            ty.clone(),
            ExprKind::Literal(LiteralValue::Int64(literal_value)),
        )
        .unwrap();
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();
    (node, value)
}

fn append_empty_broadcast_i64_values(
    builder: &mut FragmentBuilder,
) -> (novarocks_physical_plan::NodeId, ValueId) {
    let node = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ValueType::new(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::Replicated,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::default(),
            },
        })
        .unwrap();
    (node, value)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HashJoinFixtureOutput {
    ReorderedPair,
    PreservedLeft,
    PreservedRight,
    NullableRight,
}

fn finish_hash_join_plan(
    fragment_id: u32,
    kind: JoinKind,
    distribution: JoinDistribution,
    left_literal: i64,
    right_literal: i64,
    output: HashJoinFixtureOutput,
) -> PhysicalPlan {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let (left, left_value) = append_i64_values(&mut builder, left_literal);
    let (right, right_value) = if distribution == JoinDistribution::BroadcastBuild {
        append_empty_broadcast_i64_values(&mut builder)
    } else {
        append_i64_values(&mut builder, right_literal)
    };
    let join = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let left_key_kind = if output == HashJoinFixtureOutput::ReorderedPair {
        ExprKind::Literal(LiteralValue::Int64(1))
    } else {
        ExprKind::Value(left_value)
    };
    let right_key_kind = if output == HashJoinFixtureOutput::ReorderedPair {
        ExprKind::Literal(LiteralValue::Int64(1))
    } else {
        ExprKind::Value(right_value)
    };
    let left_key = builder
        .add_expression(join, ty.clone(), left_key_kind)
        .unwrap();
    let right_key = builder
        .add_expression(join, ty.clone(), right_key_kind)
        .unwrap();
    let (outputs, null_extended): (Box<[ValueId]>, Box<[ValueId]>) = match output {
        HashJoinFixtureOutput::ReorderedPair => {
            (Box::from([right_value, left_value]), Box::default())
        }
        HashJoinFixtureOutput::PreservedLeft => (Box::from([left_value]), Box::default()),
        HashJoinFixtureOutput::PreservedRight => (Box::from([right_value]), Box::default()),
        HashJoinFixtureOutput::NullableRight => {
            let nullable_right = builder
                .add_value(
                    ValueType::new(DataType::Int64, true),
                    ValueOrigin::NullExtended {
                        node: join,
                        of: right_value,
                    },
                )
                .unwrap();
            (Box::from([nullable_right]), Box::from([nullable_right]))
        }
    };
    let (build_side, required_inputs) = match (kind, distribution) {
        (JoinKind::RightSemi | JoinKind::RightAnti, _) => (
            JoinSide::Left,
            Box::from([singleton_properties(), singleton_properties()]),
        ),
        (_, JoinDistribution::BroadcastBuild) => (
            JoinSide::Right,
            Box::from([
                singleton_properties(),
                PhysicalProperties {
                    distribution: Distribution::Broadcast,
                    row_multiplicity: RowMultiplicity::Replicated,
                    ordering: Box::default(),
                },
            ]),
        ),
        _ => (
            JoinSide::Right,
            Box::from([singleton_properties(), singleton_properties()]),
        ),
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs,
            output_properties: singleton_properties(),
            output: OutputPort {
                node: join,
                columns: outputs.clone(),
            },
            kind: NodeKind::HashJoin {
                kind,
                build_side,
                keys: Box::from([JoinKey {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }]),
                distribution,
                residual: None,
                null_extended,
            },
        })
        .unwrap();
    finish_result_plan(builder, fragment_id, join, outputs)
}

fn finish_nest_loop_projection_plan(fragment_id: u32) -> PhysicalPlan {
    let fragment_id = FragmentId::new(fragment_id);
    let mut builder = FragmentBuilder::new(fragment_id);
    let (left, _) = append_i64_values(&mut builder, 31);
    let (right, right_value) = append_i64_values(&mut builder, 47);
    let join = builder.reserve_node_id().unwrap();
    let outputs: Box<[ValueId]> = Box::from([right_value]);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: join,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([singleton_properties(), singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: join,
                columns: outputs.clone(),
            },
            kind: NodeKind::NestLoopJoin {
                kind: JoinKind::Cross,
                distribution: NestLoopJoinDistribution::Singleton,
                predicate: None,
                null_extended: Box::default(),
            },
        })
        .unwrap();
    finish_result_plan(builder, fragment_id, join, outputs)
}

fn finish_result_plan(
    builder: FragmentBuilder,
    fragment_id: FragmentId,
    root: novarocks_physical_plan::NodeId,
    outputs: Box<[ValueId]>,
) -> PhysicalPlan {
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let fields = outputs
        .iter()
        .enumerate()
        .map(|(ordinal, value)| ResultField {
            name: format!("column_{ordinal}").into(),
            alias: None,
            value: *value,
            ty: fragment.values()[value].ty.clone(),
        })
        .collect();
    let mut plan_builder =
        PlanBuilder::new(PlanVersionId::try_new([fragment_id.get() as u8; 16]).unwrap());
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            fragment: fragment_id,
            output: OutputPort {
                node: root,
                columns: outputs,
            },
            fields,
        })
        .unwrap();
    plan_builder.finish().unwrap()
}

#[derive(Clone, Default)]
struct TestResultHandle(Arc<Mutex<Vec<Chunk>>>);

struct TestResultSinkFactory(TestResultHandle);

impl OperatorFactory for TestResultSinkFactory {
    fn name(&self) -> &str {
        "TEST_RESULT_SINK"
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(TestResultSink {
            handle: self.0.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct TestResultSink {
    handle: TestResultHandle,
    finished: bool,
}

impl Operator for TestResultSink {
    fn name(&self) -> &str {
        "TEST_RESULT_SINK"
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

impl ProcessorOperator for TestResultSink {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        self.handle.0.lock().unwrap().push(chunk);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}

fn encode_decode_execute(plan: &PhysicalPlan) -> (Vec<Chunk>, Vec<SlotId>, ExecNodeKind) {
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_v1(
        plan,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
    )
    .unwrap();
    let fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let output_slots = decoded.layout.order().to_vec();
    let root_kind = decoded.node.kind.clone();
    let exec_plan = ExecPlanBuilder::new(arena, decoded.node).finish().unwrap();
    let handle = TestResultHandle::default();
    execute_native_plan_with_pipeline(
        exec_plan,
        false,
        Duration::from_millis(10),
        Box::new(TestResultSinkFactory(handle.clone())),
        ExchangeBindings::default(),
        ScanBindings::default(),
        None,
        None,
        1,
        Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(test_execution_runtime()),
        )),
        None,
        None,
        None,
    )
    .unwrap();
    let chunks = std::mem::take(&mut *handle.0.lock().unwrap());
    (chunks, output_slots, root_kind)
}

fn test_execution_runtime() -> Arc<ExecutionRuntime> {
    static RUNTIME: OnceLock<Arc<ExecutionRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        let mut functions = ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(functions.catalog_builder_mut())
            .unwrap();
        contribute_builtin_aggregate_implementations(&mut functions).unwrap();
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    spill_io_threads: 1,
                    spill_io_queue_capacity: 8,
                    spill_storage: ExecutionSpillStorageConfig::default(),
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                Arc::new(functions.seal().unwrap()),
                crate::backend_test_support::test_memory_authority(),
            )
            .unwrap(),
        )
    }))
}

struct OneWriteFact {
    ordinal: WriteTargetOrdinal,
    fact: PhysicalV1WriteFact,
}

impl PhysicalV1PrivateFacts for OneWriteFact {
    fn scan_fact(
        &self,
        _fragment: FragmentId,
        _node: novarocks_physical_plan::NodeId,
    ) -> Option<&novarocks_plan_codec::PhysicalV1ScanFact> {
        None
    }

    fn write_fact(&self, target: WriteTargetOrdinal) -> Option<&PhysicalV1WriteFact> {
        (target == self.ordinal).then_some(&self.fact)
    }
}

fn write_handle() -> ConnectorEncodedPayload {
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let instance = ConnectorInstanceId::try_from_canonical("lakehouse").unwrap();
    let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([3; 32]));
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider,
            catalog,
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    )
}

fn writer_relation_fields(
    builder: &mut FragmentBuilder,
    owner: novarocks_physical_plan::NodeId,
    root: bool,
) -> Vec<WriterRelationField> {
    use std::sync::Arc;

    let specs = if root {
        vec![
            ("kind", DataType::Int8, false, WriterRelationFieldRole::Kind),
            (
                "write_target_ordinal",
                DataType::Int32,
                true,
                WriterRelationFieldRole::TargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                WriterRelationFieldRole::RowCount,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                WriterRelationFieldRole::CommitFragment,
            ),
            (
                "input_fields",
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Int32,
                    false,
                ))),
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "blob_type",
                DataType::Utf8,
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "body",
                DataType::Binary,
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
            (
                "properties",
                DataType::Map(
                    Arc::new(arrow::datatypes::Field::new(
                        "entries",
                        DataType::Struct(arrow::datatypes::Fields::from(vec![
                            arrow::datatypes::Field::new("key", DataType::Utf8, false),
                            arrow::datatypes::Field::new("value", DataType::Utf8, false),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
                WriterRelationFieldRole::Auxiliary,
            ),
        ]
    } else {
        vec![
            ("kind", DataType::Int8, false, WriterRelationFieldRole::Kind),
            (
                "write_target_ordinal",
                DataType::Int32,
                false,
                WriterRelationFieldRole::TargetOrdinal,
            ),
            (
                "row_count",
                DataType::Int64,
                true,
                WriterRelationFieldRole::RowCount,
            ),
            (
                "commit_fragment",
                DataType::Binary,
                true,
                WriterRelationFieldRole::CommitFragment,
            ),
        ]
    };
    specs
        .into_iter()
        .map(|(name, data_type, nullable, role)| {
            let ty = ValueType::new(data_type, nullable);
            let value = builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: owner,
                        kind: match role {
                            WriterRelationFieldRole::Kind => WriterDerivedKind::RelationKind,
                            WriterRelationFieldRole::TargetOrdinal => {
                                WriterDerivedKind::WriteTargetOrdinal
                            }
                            WriterRelationFieldRole::RowCount => WriterDerivedKind::AffectedRows,
                            WriterRelationFieldRole::CommitFragment => {
                                WriterDerivedKind::CommitFragment
                            }
                            WriterRelationFieldRole::Auxiliary => {
                                WriterDerivedKind::RelationAuxiliary
                            }
                        },
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty,
                role,
            }
        })
        .collect()
}

fn finish_duplicate_router_plan() -> (PhysicalPlan, OneWriteFact) {
    let source_fragment = FragmentId::new(81);
    let writer_fragment = FragmentId::new(82);
    let finish_fragment = FragmentId::new(83);
    let router_edge = EdgeId::new(81);
    let finish_edge = EdgeId::new(82);
    let input_ty = ValueType::new(DataType::Int64, false);
    let token_a = ConnectorWriteFieldToken::from_bytes([1; 32]);
    let token_b = ConnectorWriteFieldToken::from_bytes([2; 32]);
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();

    let mut source_builder = FragmentBuilder::new(source_fragment);
    let (values, source_value) = append_i64_values(&mut source_builder, 9);
    let expand = source_builder.reserve_node_id().unwrap();
    let assignment = source_builder
        .add_expression(expand, input_ty.clone(), ExprKind::Value(source_value))
        .unwrap();
    let effect = source_builder
        .add_value(
            ValueType::new(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let routed = source_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::NodeOutput {
                node: expand,
                output_ordinal: 1,
            },
        )
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: expand,
            inputs: Box::from([values]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: expand,
                columns: Box::from([effect, routed]),
            },
            kind: NodeKind::ChangeEventExpand {
                events: Box::from([ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([(routed, Some(assignment))]),
                }]),
                effect_output: effect,
            },
        })
        .unwrap();
    let project = source_builder.reserve_node_id().unwrap();
    let effect_ref = source_builder
        .add_expression(
            project,
            ValueType::new(DataType::Int8, false),
            ExprKind::Value(effect),
        )
        .unwrap();
    let routed_ref = source_builder
        .add_expression(project, input_ty.clone(), ExprKind::Value(routed))
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: project,
            inputs: Box::from([expand]),
            required_inputs: Box::from([unconstrained_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: project,
                columns: Box::from([effect, routed, routed]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([
                    (effect_ref, effect),
                    (routed_ref, routed),
                    (routed_ref, routed),
                ]),
            },
        })
        .unwrap();
    let route_id = ConnectorWriteRouteId::from_bytes([8; 32]);
    let source = source_builder
        .finish_definition(
            project,
            FragmentSink::Router {
                effect,
                routes: Box::from([ChangeStreamRoute {
                    route_id,
                    write_target_ordinal: ordinal,
                    accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
                    input_mapping: Box::from([(token_a, routed), (token_b, routed)]),
                    partition_by: Box::default(),
                    edge: router_edge,
                }]),
            },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut writer_builder = FragmentBuilder::new(writer_fragment);
    let exchange = writer_builder.reserve_node_id().unwrap();
    let imported_a = writer_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: router_edge,
                source_value: routed,
            },
        )
        .unwrap();
    let imported_b = writer_builder
        .add_value(
            input_ty.clone(),
            ValueOrigin::ExchangeImport {
                edge: router_edge,
                source_value: routed,
            },
        )
        .unwrap();
    writer_builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: exchange,
                columns: Box::from([imported_a, imported_b]),
            },
            kind: NodeKind::ExchangeSource {
                edge: router_edge,
                imports: Box::from([(routed, imported_a), (routed, imported_b)]),
            },
        })
        .unwrap();
    let writer = writer_builder.reserve_node_id().unwrap();
    let writer_fields = writer_relation_fields(&mut writer_builder, writer, false);
    let handle = write_handle();
    writer_builder
        .insert_node_unchecked(PhysicalNode {
            id: writer,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: unconstrained_properties(),
            output: OutputPort {
                node: writer,
                columns: writer_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: handle.clone(),
                    write_target_ordinal: ordinal,
                    input: Box::from([imported_a, imported_b]),
                    required_distribution: Distribution::Singleton,
                    target_fields: Box::from([
                        WriterTargetField {
                            token: token_a,
                            input: imported_a,
                            ty: input_ty.clone(),
                            hidden: false,
                        },
                        WriterTargetField {
                            token: token_b,
                            input: imported_b,
                            ty: input_ty,
                            hidden: false,
                        },
                    ]),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: writer_fields.clone().into_boxed_slice(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let writer_stage = writer_builder
        .finish_definition(
            writer,
            FragmentSink::Stream { edge: finish_edge },
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut finish_builder = FragmentBuilder::new(finish_fragment);
    let finish_exchange = finish_builder.reserve_node_id().unwrap();
    let mut finish_input_fields = Vec::with_capacity(writer_fields.len());
    for field in &writer_fields {
        let imported = finish_builder
            .add_value(
                field.ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge: finish_edge,
                    source_value: field.value,
                },
            )
            .unwrap();
        finish_input_fields.push(WriterRelationField {
            value: imported,
            name: field.name.clone(),
            ty: field.ty.clone(),
            role: field.role,
        });
    }
    let finish_receive_mapping: Box<[(ValueId, ValueId)]> = writer_fields
        .iter()
        .zip(&finish_input_fields)
        .map(|(source, imported)| (source.value, imported.value))
        .collect();
    finish_builder
        .insert_node_unchecked(PhysicalNode {
            id: finish_exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: finish_exchange,
                columns: finish_input_fields
                    .iter()
                    .map(|field| field.value)
                    .collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: finish_edge,
                imports: finish_receive_mapping.clone(),
            },
        })
        .unwrap();
    let finish = finish_builder.reserve_node_id().unwrap();
    let root_fields = writer_relation_fields(&mut finish_builder, finish, true);
    finish_builder
        .insert_node_unchecked(PhysicalNode {
            id: finish,
            inputs: Box::from([finish_exchange]),
            required_inputs: Box::from([singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: finish,
                columns: root_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals: Box::from([ordinal]),
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: finish_input_fields.into_boxed_slice(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: root_fields.clone().into_boxed_slice(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let finish_stage = finish_builder
        .finish_definition(
            finish,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();

    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([81; 16]).unwrap());
    plan_builder.add_fragment(source).unwrap();
    plan_builder.add_fragment(writer_stage).unwrap();
    plan_builder.add_fragment(finish_stage).unwrap();
    plan_builder
        .add_edge(Edge {
            id: router_edge,
            kind: EdgeKind::ChangeStreamRouter,
            source: EdgeSource {
                fragment: source_fragment,
                projection: Box::from([routed, routed]),
            },
            destination: EdgeDestination {
                fragment: writer_fragment,
                node: exchange,
                receive_mapping: Box::from([(routed, imported_a), (routed, imported_b)]),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    plan_builder
        .add_edge(Edge {
            id: finish_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: writer_fragment,
                projection: writer_fields.iter().map(|field| field.value).collect(),
            },
            destination: EdgeDestination {
                fragment: finish_fragment,
                node: finish_exchange,
                receive_mapping: finish_receive_mapping,
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        })
        .unwrap();
    plan_builder
        .set_result_port(ResultPort {
            fragment: finish_fragment,
            output: OutputPort {
                node: finish,
                columns: root_fields.iter().map(|field| field.value).collect(),
            },
            fields: root_fields
                .iter()
                .map(|field| ResultField {
                    name: field.name.clone(),
                    alias: None,
                    value: field.value,
                    ty: field.ty.clone(),
                })
                .collect(),
        })
        .unwrap();
    let physical = plan_builder.finish().unwrap();
    let fact = PhysicalV1WriteFact {
        handle: connector_write::ConnectorWriterHandle {
            provider_payload: Some(
                novarocks_proto_codec::connector_common::encode_connector_payload_message(&handle),
            ),
        },
        field_names: std::collections::BTreeMap::from([
            (token_a.to_bytes(), Box::<str>::from("a")),
            (token_b.to_bytes(), Box::<str>::from("b")),
        ]),
    };
    (physical, OneWriteFact { ordinal, fact })
}

#[test]
fn physical_plan_finish_encode_decode_preserves_transparent_duplicate_layout() {
    let properties = singleton_properties();
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let values = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let literal = builder
        .add_expression(
            values,
            ty.clone(),
            ExprKind::Literal(LiteralValue::Int64(7)),
        )
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: values,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: properties.clone(),
            output: OutputPort {
                node: values,
                columns: Box::from([value]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([literal])]),
            },
        })
        .unwrap();

    let project = builder.reserve_node_id().unwrap();
    let reference = builder
        .add_expression(project, ty.clone(), ExprKind::Value(value))
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: project,
            inputs: Box::from([values]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties.clone(),
            output: OutputPort {
                node: project,
                columns: Box::from([value, value]),
            },
            kind: NodeKind::Project {
                expressions: Box::from([(reference, value), (reference, value)]),
            },
        })
        .unwrap();

    let limit = builder.reserve_node_id().unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: limit,
            inputs: Box::from([project]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: OutputPort {
                node: limit,
                columns: Box::from([value, value]),
            },
            kind: NodeKind::Limit {
                limit: Some(1),
                offset: 0,
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            limit,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([71; 16]).unwrap());
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            fragment: FragmentId::new(71),
            output: OutputPort {
                node: limit,
                columns: Box::from([value, value]),
            },
            fields: Box::from([
                ResultField {
                    name: "left".into(),
                    alias: None,
                    value,
                    ty: ty.clone(),
                },
                ResultField {
                    name: "right".into(),
                    alias: None,
                    value,
                    ty,
                },
            ]),
        })
        .unwrap();
    let physical = plan_builder.finish().unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_v1(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
    )
    .unwrap();
    let wire_fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        wire_fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    assert_eq!(decoded.layout.order().len(), 2);
    assert_ne!(decoded.layout.order()[0], decoded.layout.order()[1]);
    assert_eq!(
        decoded.layout.order(),
        &wire_fragment
            .output_columns
            .iter()
            .map(|column| SlotId::new(column.column_id))
            .collect::<Vec<_>>()
    );
}

#[test]
fn physical_plan_finish_encode_decode_preserves_set_op_fresh_output_layout() {
    let mut builder = FragmentBuilder::new(FragmentId::new(72));
    let (left, left_value) = append_i64_values(&mut builder, 1);
    let (right, right_value) = append_i64_values(&mut builder, 2);
    let set_op = builder.reserve_node_id().unwrap();
    let ty = ValueType::new(DataType::Int64, false);
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::NodeOutput {
                node: set_op,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: set_op,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([singleton_properties(), singleton_properties()]),
            output_properties: singleton_properties(),
            output: OutputPort {
                node: set_op,
                columns: Box::from([output]),
            },
            kind: NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: Box::from([Box::from([left_value]), Box::from([right_value])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            set_op,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut plan_builder = PlanBuilder::new(PlanVersionId::try_new([72; 16]).unwrap());
    plan_builder.add_fragment(fragment).unwrap();
    plan_builder
        .set_result_port(ResultPort {
            fragment: FragmentId::new(72),
            output: OutputPort {
                node: set_op,
                columns: Box::from([output]),
            },
            fields: Box::from([ResultField {
                name: "value".into(),
                alias: None,
                value: output,
                ty,
            }]),
        })
        .unwrap();
    let physical = plan_builder.finish().unwrap();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_v1(
        &physical,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
    )
    .unwrap();
    let wire_fragment = &encoded.fragments[0];
    let mut arena = ExprArena::default();
    let decoded = decode_node(
        wire_fragment.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    assert!(matches!(decoded.node.kind, ExecNodeKind::UnionAll(_)));
    assert_eq!(decoded.layout.order().len(), 1);
    assert_eq!(
        decoded.layout.order()[0],
        SlotId::new(wire_fragment.output_columns[0].column_id)
    );
}

#[test]
fn physical_plan_finish_encode_decode_preserves_duplicate_router_occurrences() {
    let (physical, facts) = finish_duplicate_router_plan();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let encoded = novarocks_plan_codec::encode_physical_plan_v1(&physical, &catalog, &facts)
        .expect("duplicate router occurrences have distinct v1 slots");
    let source = encoded
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == 81)
        .unwrap();
    let plan::data_sink::Kind::ChangeStreamRouter(router) =
        source.sink.as_ref().unwrap().kind.as_ref().unwrap()
    else {
        panic!("expected router sink")
    };
    assert_eq!(router.routes[0].input_ordinals, vec![1, 2]);

    let mut arena = ExprArena::default();
    let decoded_root = decode_node(
        source.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap();
    let program = decode_fragment_sink_program(source, &decoded_root.layout).unwrap();
    let novarocks_execution::exec::fragment::sink::FragmentSinkProgram::SplitDataStream(split) =
        program
    else {
        panic!("expected split stream")
    };
    assert_eq!(split.sinks().len(), 1);
    assert_eq!(split.sinks()[0].output_columns().len(), 2);
    assert_ne!(
        split.sinks()[0].output_columns()[0],
        split.sinks()[0].output_columns()[1]
    );
}

fn assert_i64_rows(chunks: &[Chunk], output_slots: &[SlotId], expected: &[Vec<Option<i64>>]) {
    let mut actual: Vec<Vec<Option<i64>>> = Vec::new();
    for chunk in chunks {
        for row in 0..chunk.len() {
            actual.push(
                output_slots
                    .iter()
                    .map(|slot| {
                        let column = chunk.column_by_slot_id(*slot).unwrap();
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        (!array.is_null(row)).then(|| array.value(row))
                    })
                    .collect(),
            );
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn physical_plan_hash_join_reorder_executes_through_terminal_projection() {
    let physical = finish_hash_join_plan(
        91,
        JoinKind::Inner,
        JoinDistribution::Singleton,
        11,
        23,
        HashJoinFixtureOutput::ReorderedPair,
    );
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published HashJoin output must be a terminal projection")
    };
    let ExecNodeKind::Join(join) = &project.input.kind else {
        panic!("terminal projection must directly wrap HashJoin")
    };
    assert_eq!(
        join.distribution_mode,
        novarocks_execution::exec::node::join::JoinDistributionMode::Partitioned
    );
    assert_i64_rows(&chunks, &slots, &[vec![Some(23), Some(11)]]);
}

#[test]
fn physical_plan_hash_join_broadcast_projection_decodes_and_executes() {
    let physical = finish_hash_join_plan(
        92,
        JoinKind::Inner,
        JoinDistribution::BroadcastBuild,
        11,
        0,
        HashJoinFixtureOutput::ReorderedPair,
    );
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published HashJoin output must be a terminal projection")
    };
    let ExecNodeKind::Join(join) = &project.input.kind else {
        panic!("terminal projection must directly wrap HashJoin")
    };
    assert_eq!(
        join.distribution_mode,
        novarocks_execution::exec::node::join::JoinDistributionMode::Broadcast
    );
    assert_i64_rows(&chunks, &slots, &[]);
}

#[test]
fn physical_plan_left_outer_nullable_projection_executes_after_residual_scope() {
    let physical = finish_hash_join_plan(
        93,
        JoinKind::LeftOuter,
        JoinDistribution::Singleton,
        11,
        23,
        HashJoinFixtureOutput::NullableRight,
    );
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published outer-join output must be a terminal projection")
    };
    assert!(matches!(project.input.kind, ExecNodeKind::Join(_)));
    assert_i64_rows(&chunks, &slots, &[vec![None]]);
}

#[test]
fn physical_plan_hash_join_semi_anti_preserved_sides_execute_losslessly() {
    for (id, kind, left, right, output, expected) in [
        (
            94,
            JoinKind::LeftSemi,
            5,
            5,
            HashJoinFixtureOutput::PreservedLeft,
            5,
        ),
        (
            95,
            JoinKind::LeftAnti,
            5,
            7,
            HashJoinFixtureOutput::PreservedLeft,
            5,
        ),
        (
            96,
            JoinKind::RightSemi,
            5,
            5,
            HashJoinFixtureOutput::PreservedRight,
            5,
        ),
        (
            97,
            JoinKind::RightAnti,
            5,
            7,
            HashJoinFixtureOutput::PreservedRight,
            7,
        ),
    ] {
        let physical =
            finish_hash_join_plan(id, kind, JoinDistribution::Singleton, left, right, output);
        let (chunks, slots, root) = encode_decode_execute(&physical);
        let ExecNodeKind::Project(project) = root else {
            panic!("published semi/anti output must be a terminal projection")
        };
        assert!(matches!(project.input.kind, ExecNodeKind::Join(_)));
        assert_i64_rows(&chunks, &slots, &[vec![Some(expected)]]);
    }
}

#[test]
fn physical_plan_nest_loop_subset_executes_through_terminal_projection() {
    let physical = finish_nest_loop_projection_plan(98);
    let (chunks, slots, root) = encode_decode_execute(&physical);
    let ExecNodeKind::Project(project) = root else {
        panic!("published NestLoopJoin output must be a terminal projection")
    };
    assert!(matches!(
        project.input.kind,
        ExecNodeKind::NestedLoopJoin(_)
    ));
    assert_i64_rows(&chunks, &slots, &[vec![Some(47)]]);
}
