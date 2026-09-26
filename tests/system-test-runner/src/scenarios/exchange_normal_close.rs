// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One real producer keeps sending to B after A's exact normal departure.

use std::path::Path;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use arrow_array::{Array, Int64Array};
use novarocks_cluster_harness::ServerHandle;
use novarocks_proto_models::{common, expr, novarocks as proto, plan};
use prost::Message;

use super::native_compatibility::{raw_operation_envelope, raw_unary};
use super::native_creation::{
    RawBackendSession, RawCreate, observe_task_terminal_and_stopped, outcome, session_control,
};
use super::query_lifecycle::{await_resource_convergence, resource_snapshot};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

const ROWS: [i64; 5] = [7, -11, 0, i64::MAX, i64::MIN];
const POLL: Duration = Duration::from_millis(50);

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(SameProducerNormalClose { multicast: false }),
        Box::new(SameProducerNormalClose { multicast: true }),
    ]
}

struct SameProducerNormalClose {
    multicast: bool,
}

impl Scenario for SameProducerNormalClose {
    fn name(&self) -> &'static str {
        if self.multicast {
            "task-execution/multicast-normal-close-keeps-sibling"
        } else {
            "task-execution/same-edge-normal-close-keeps-sibling"
        }
    }

    fn launch_config(&self, _: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        for index in 0..3 {
            launch
                .child_environment
                .be_by_index
                .entry(index)
                .or_default()
                .insert(
                    "NOVAROCKS_SQL_TEST_EMIT_EXCHANGE_SNAPSHOT_MARKER".into(),
                    "true".into(),
                );
        }
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "exchange evidence requires native 1FE+3BE"
        );
        let baseline = resource_snapshot(context)?;
        let mut sessions = Vec::new();
        let result = (|| -> Result<()> {
            sessions.push(RawBackendSession::establish(context, 0)?);
            sessions.push(RawBackendSession::establish_same_attempt(
                context,
                1,
                &sessions[0],
            )?);
            sessions.push(RawBackendSession::establish_same_attempt(
                context,
                2,
                &sessions[0],
            )?);
            let a = &sessions[0];
            let b = &sessions[1];
            let p = &sessions[2];
            let producer_base = RawCreate::values(&p, 3);
            let producer_identity = proto::TaskIdentity {
                query_execution_id: producer_base.context.query_execution_id,
                stage_id: 2,
                task_id: 3,
                backend_process_id: producer_base.context.backend_process_id.clone(),
            };
            let producer_finst = common::UniqueId {
                hi: 0x5b,
                lo: producer_base.kernel_key_low,
            };
            let receiver_a = receiver(&a, 1, false, &producer_identity, producer_finst)?;
            let receiver_b = receiver(&b, 2, true, &producer_identity, producer_finst)?;
            let producer = producer(
                &producer_base,
                producer_identity,
                producer_finst,
                &receiver_a,
                &receiver_b,
                context,
                self.multicast,
            )?;
            receiver_a.accept_and_await_installed(context, &a)?;
            receiver_b.accept_and_await_installed(context, &b)?;
            producer.accept_and_await_installed(context, &p)?;
            context.action(format!(
                "froze attempt={:?} P={:?} finst={producer_finst:?} A={:?} B={:?}; P Installed with edges Closed; multicast={}",
                producer.identity.query_execution_id, producer.identity, receiver_a.identity, receiver_b.identity, self.multicast,
            ));
            context.action(if self.multicast {
                "frozen P edge 1 -> exact A; P edge 2 -> exact B; both edges use the same P source finst and sender ordinal 0/count 1"
            } else {
                "frozen P edge 1 -> exact A and B; both destinations use the same P source finst and sender ordinal 0/count 1"
            });
            let cancel = session_control(
                &a,
                proto::task_control_operation::Control::CancelTask(proto::CancelTaskRequest {
                    identity: Some(receiver_a.identity.clone()),
                    reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                }),
            )?;
            ensure!(
                matches!(
                    outcome(&cancel),
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ),
                "exact normal Cancel failed: {cancel:?}"
            );
            // RawApplication declares this Task no longer needs input. This
            // is an explicit test authority, not a business unknown-A source.
            observe_task_terminal_and_stopped(
                context,
                &a,
                &receiver_a.identity,
                proto::TaskState::Canceled,
            )?;
            let edge_ids = if self.multicast { vec![1, 2] } else { vec![1] };
            let open = p.apply(
                proto::TaskOperation {
                    envelope: Some(raw_operation_envelope(1_000)),
                    operation: Some(proto::task_operation::Operation::UpdateTask(
                        proto::UpdateTaskRequest {
                            identity: Some(producer.identity.clone()),
                            domains: vec![proto::TaskDomainUpdate {
                                domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                                    proto::OpenExchangeEdgesDomain {
                                        version: 1,
                                        edge_ids,
                                    },
                                )),
                            }],
                        },
                    )),
                },
                "open the frozen producer after A actually stopped",
            )?;
            ensure!(
                matches!(
                    outcome(&open),
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ),
                "producer open failed: {open:?}"
            );
            let rows = fetch_rows(context, &b, &receiver_b.identity)?;
            ensure!(
                rows == ROWS,
                "B lost or changed rows after A closed: {rows:?}"
            );
            await_normal_close_marker(context, &p, &receiver_a, &producer)?;
            observe_task_terminal_and_stopped(
                context,
                &b,
                &receiver_b.identity,
                proto::TaskState::Finished,
            )?;
            observe_task_terminal_and_stopped(
                context,
                &p,
                &producer.identity,
                proto::TaskState::Finished,
            )?;
            context.action(format!("same frozen P received A's validated typed normal-close reply; B received exact BIGINT rows {rows:?} plus EOS; P and B Finished and actually stopped"));
            Ok(())
        })();
        // A failed fixture may still own a Closed producer or waiting B.
        // Abort first drives those live Tasks down; Quiesce alone only fences
        // admission. Successful evidence keeps the normal Release path.
        let mut cleanup_error = None;
        if result.is_err() {
            for session in &sessions {
                if let Err(error) = session_control(
                    session,
                    proto::task_control_operation::Control::AbortQueryContext(
                        proto::AbortQueryContextRequest {
                            query_context: Some(session.context_under(session.frontend, 1)),
                            cause: proto::QueryContextAbortCause::QueryFailed as i32,
                        },
                    ),
                ) {
                    cleanup_error.get_or_insert(error);
                }
            }
        }
        for session in &sessions {
            if let Err(error) = session.release_when_ready(context) {
                cleanup_error.get_or_insert(error);
            }
        }
        result?;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        await_resource_convergence(context, &baseline)?;
        Ok(())
    }
}

struct FrozenTask {
    operation: proto::TaskOperation,
    identity: proto::TaskIdentity,
    finst: common::UniqueId,
}

impl FrozenTask {
    fn accept_and_await_installed(
        &self,
        context: &mut ScenarioContext,
        session: &RawBackendSession,
    ) -> Result<()> {
        let accepted = session.apply(self.operation.clone(), "create exact exchange fixture")?;
        ensure!(
            outcome(&accepted) == proto::TaskOperationOutcome::Accepted,
            "fixture Create was not Accepted: {accepted:?}"
        );
        loop {
            let receipt = session.apply(
                self.operation.clone(),
                "observe frozen fixture installation",
            )?;
            let Some(proto::task_operation_receipt::Ack::CreateTask(ack)) = receipt.ack else {
                bail!("Create replay omitted ack: {receipt:?}");
            };
            let status = ack
                .current_status
                .context("Create omitted current status")?;
            ensure!(
                status.identity.as_ref() == Some(&self.identity),
                "Create changed frozen identity: {status:?}"
            );
            ensure!(
                !matches!(
                    proto::TaskState::try_from(status.state)?,
                    proto::TaskState::Failed
                        | proto::TaskState::Aborted
                        | proto::TaskState::Canceled
                ),
                "fixture task failed before normal close: {status:?}"
            );
            if status.installed == Some(true) {
                return Ok(());
            }
            thread::sleep(
                context
                    .remaining("await exchange fixture Installed")?
                    .min(POLL),
            );
        }
    }
}

fn carriers(base: &RawCreate) -> Result<(proto::FrozenFragment, proto::CreationMetadata)> {
    let operation = base.operation();
    let Some(proto::task_operation::Operation::CreateTask(create)) = operation.operation else {
        unreachable!()
    };
    Ok((
        proto::FrozenFragment::decode(create.frozen_fragment)?,
        proto::CreationMetadata::decode(create.creation_metadata)?,
    ))
}

fn freeze(frozen: proto::FrozenFragment, metadata: proto::CreationMetadata) -> FrozenTask {
    let descriptor = metadata.descriptor.as_ref().expect("fixture descriptor");
    FrozenTask {
        identity: descriptor.identity.clone().expect("fixture identity"),
        finst: descriptor.fragment_instance_id.expect("fixture finst"),
        operation: proto::TaskOperation {
            envelope: Some(raw_operation_envelope(15_000)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    frozen_fragment: frozen.encode_to_vec().into(),
                    creation_metadata: metadata.encode_to_vec().into(),
                },
            )),
        },
    }
}

fn column() -> common::OutputColumn {
    common::OutputColumn {
        column_id: 1,
        name: "v".into(),
        r#type: Some(bigint()),
        nullable: false,
        is_internal: false,
    }
}

fn bigint() -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: common::PrimitiveType::Bigint as i32,
            ..Default::default()
        })),
    }
}

fn receiver(
    session: &RawBackendSession,
    id: u32,
    result: bool,
    source: &proto::TaskIdentity,
    source_finst: common::UniqueId,
) -> Result<FrozenTask> {
    let mut base = RawCreate::values(session, id);
    base.pipeline_dop = 1;
    base.exchange_input = true;
    base.sink = if result {
        plan::data_sink::Kind::Result(true)
    } else {
        plan::data_sink::Kind::Noop(true)
    };
    let (mut frozen, mut metadata) = carriers(&base)?;
    let fragment = frozen.plan.as_mut().expect("fixture plan");
    fragment.output_columns = vec![column()];
    let Some(plan::distributed_node::Payload::Exchange(exchange)) = fragment
        .root
        .as_mut()
        .expect("fixture root")
        .payload
        .as_mut()
    else {
        unreachable!()
    };
    exchange.output_columns = vec![column()];
    let inbound = &mut metadata
        .descriptor
        .as_mut()
        .expect("fixture descriptor")
        .topology
        .as_mut()
        .expect("fixture topology")
        .inbound[0];
    inbound.sources = vec![proto::TaskExchangeSource {
        task: Some(source.clone()),
        fragment_instance_id: Some(source_finst),
        sender_ordinal: 0,
    }];
    Ok(freeze(frozen, metadata))
}

fn producer(
    base: &RawCreate,
    identity: proto::TaskIdentity,
    finst: common::UniqueId,
    a: &FrozenTask,
    b: &FrozenTask,
    context: &mut ScenarioContext,
    multicast: bool,
) -> Result<FrozenTask> {
    let mut base = base.clone();
    base.pipeline_dop = 1;
    let (mut frozen, mut metadata) = carriers(&base)?;
    let stream = || plan::DataStreamSink {
        dest_node_id: 10,
        target_fragment_id: 1,
        output_partition: Some(plan::DataPartition {
            kind: plan::PartitionKind::Unpartitioned as i32,
            exprs: Vec::new(),
        }),
        output_columns: vec![1],
        ..Default::default()
    };
    let fragment = frozen.plan.as_mut().expect("fixture plan");
    fragment.fragment_id = 2;
    fragment.output_columns = vec![column()];
    fragment
        .runtime_filter_bindings
        .as_mut()
        .expect("fixture RF table")
        .fragment_id = 2;
    fragment.sink = Some(plan::DataSink {
        kind: Some(if multicast {
            plan::data_sink::Kind::MultiCastDataStream(plan::MultiCastDataStreamSink {
                sinks: vec![stream(), stream()],
            })
        } else {
            plan::data_sink::Kind::DataStream(stream())
        }),
    });
    let root = fragment.root.as_mut().expect("fixture root");
    root.fragment_id = 2;
    root.payload = Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
        output_columns: vec![column()],
        kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
            columns: vec![column()],
            rows: ROWS
                .iter()
                .map(|value| plan::ExprList {
                    values: vec![expr::Expr {
                        r#type: Some(bigint()),
                        nullable: false,
                        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                            value: Some(common::LiteralValue {
                                value: Some(common::literal_value::Value::IntValue(*value)),
                            }),
                        })),
                    }],
                })
                .collect(),
        })),
    }));
    let descriptor = metadata.descriptor.as_mut().expect("fixture descriptor");
    descriptor.identity = Some(identity);
    descriptor.fragment_instance_id = Some(finst);
    let mut destination = |task: &FrozenTask, index: usize| -> proto::TaskExchangeDestination {
        proto::TaskExchangeDestination {
            task: Some(task.identity.clone()),
            fragment_instance_id: Some(task.finst),
            destination_node_id: 10,
            endpoint: Some(proto::QueryControlEndpoint {
                host: "127.0.0.1".into(),
                port: u32::from(context.handle().runtime().be[index].grpc),
            }),
        }
    };
    let edge = |edge_id, destinations| proto::TaskExchangeEdge {
        edge_id,
        destination_node_id: 10,
        partitioning: proto::ExchangePartitioning::Unpartitioned as i32,
        destinations,
        sender_ordinal: 0,
        sender_count: 1,
    };
    descriptor
        .topology
        .as_mut()
        .expect("fixture topology")
        .outbound = if multicast {
        vec![
            edge(1, vec![destination(a, 0)]),
            edge(2, vec![destination(b, 1)]),
        ]
    } else {
        vec![edge(1, vec![destination(a, 0), destination(b, 1)])]
    };
    metadata
        .assignment
        .as_mut()
        .expect("fixture assignment")
        .sink_edge_ids = if multicast { vec![1, 2] } else { vec![1] };
    Ok(freeze(frozen, metadata))
}

fn fetch_rows(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    identity: &proto::TaskIdentity,
) -> Result<Vec<i64>> {
    let mut rows = Vec::new();
    let mut sequence = 0;
    let mut acknowledged_packet_sequence = None;
    let mut accepted_eos = false;
    loop {
        context.remaining("fetch exact B data and EOS")?;
        let response: proto::FetchResultResponse = raw_unary(
            session.connector.clone(),
            "/novarocks.NovaRocksGrpc/FetchTaskResult",
            &session.authorization,
            proto::FetchTaskResultRequest {
                root_task: Some(identity.clone()),
                max_wait_millis: 100,
                acknowledged_packet_sequence,
                max_result_bytes: 1024 * 1024,
            },
        )?;
        ensure!(
            response.status != proto::fetch_result_response::Status::Error as i32,
            "B result failed: {}",
            response.message
        );
        if response.status == proto::fetch_result_response::Status::NotReady as i32 {
            continue;
        }
        if response.status == proto::fetch_result_response::Status::Eof as i32 {
            ensure!(
                accepted_eos
                    && response.eos
                    && response.result_arrow_ipc.is_empty()
                    && acknowledged_packet_sequence == Some(u64::try_from(response.packet_seq)?),
                "B EOF did not settle the exact acknowledged EOS packet: {response:?}"
            );
            return Ok(rows);
        }
        ensure!(
            !accepted_eos && response.status == proto::fetch_result_response::Status::Ready as i32,
            "B returned an invalid result status: {response:?}"
        );
        ensure!(
            response.packet_seq == sequence,
            "B packet sequence changed: expected {sequence}, got {}",
            response.packet_seq
        );
        sequence += 1;
        if !response.result_arrow_ipc.is_empty() {
            let chunks = novarocks_execution::runtime::exchange::decode_root_result_chunks(
                &response.result_arrow_ipc,
                None,
            )
            .map_err(anyhow::Error::msg)?;
            for chunk in chunks {
                ensure!(chunk.columns().len() == 1, "B returned another schema");
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("B column is not BIGINT")?;
                ensure!(values.null_count() == 0, "B introduced NULLs");
                rows.extend(values.values().iter().copied());
            }
        }
        // Acknowledge only after exact sequence and typed payload validation.
        // The final EOS is also acknowledged before waiting on real stop.
        acknowledged_packet_sequence = Some(u64::try_from(response.packet_seq)?);
        accepted_eos = response.eos;
    }
}

fn await_normal_close_marker(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    a: &FrozenTask,
    p: &FrozenTask,
) -> Result<()> {
    loop {
        let log = context.handle().be_log_contents(session.index)?;
        let destination = format!(
            "dest_finst={} ",
            novarocks_types::UniqueId::new(a.finst.hi, a.finst.lo)
        );
        let source = format!(
            "sender_finst={} ",
            novarocks_types::UniqueId::new(p.finst.hi, p.finst.lo)
        );
        let query = a
            .identity
            .query_execution_id
            .expect("fixture execution")
            .query_id
            .expect("fixture query");
        let process = novarocks_types::identity::BackendProcessId::try_from_bytes(
            a.identity
                .backend_process_id
                .as_ref()
                .expect("fixture backend")
                .value
                .as_slice()
                .try_into()
                .expect("backend UUID width"),
        )
        .map_err(anyhow::Error::msg)?;
        let task = format!(
            "destination_task={}/stage-{}/task-{}@{} ",
            novarocks_types::UniqueId::new(query.hi, query.lo),
            a.identity.stage_id,
            a.identity.task_id,
            process
        );
        let attempt = format!(
            "attempt={} ",
            a.identity
                .query_execution_id
                .expect("fixture execution")
                .attempt_id
        );
        if log.lines().any(|line| {
            line.contains("event=normal_close_validated")
                && line.contains(&destination)
                && line.contains(&source)
                && line.contains(&task)
                && line.contains(&attempt)
                && line.contains("node_id=10 ")
                && line.contains("sender_ordinal=0 ")
                && line.contains("sender_count=1 ")
        }) {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining("observe P Native validated A normal-close proof")?
                .min(POLL),
        );
    }
}
