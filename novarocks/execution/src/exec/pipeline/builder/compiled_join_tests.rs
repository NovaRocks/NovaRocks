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

//! Compiled joins end to end. Physical plans are authored with the real
//! builder, published as checked packages, compiled by local-compiler and run
//! through compiled pipelines only; multi-fragment plans travel through the
//! compiled exchange sinks and positional receivers over an in-process
//! loopback. Every expected relation comes from an independent nested-loop
//! oracle over the literal rows.

use crate::exec::pipeline::test_driver::TestDriver;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::DataType;
use novarocks_local_program::{
    JoinDistributionMode, JoinType, LocalProgram, NestedLoopJoinType, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeKind,
};
use novarocks_physical_plan::{
    BinaryOperator, Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning,
    EdgeSource, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentId, FragmentPackage,
    FragmentPackageAdmission, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning,
    JoinDistribution, JoinKey, JoinKind, JoinSide, LiteralValue, NestLoopJoinDistribution, NodeId,
    NodeKind, PhysicalExpressionRoots, PhysicalPlan, PhysicalRootUses, PlanBuilder, PlanLimits,
    PlanVersionId, PropertyProofProjectionLimits, ResultField, ResultPort, RowMultiplicity,
    ValueId, ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    ArithmeticOperator, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
    arithmetic_result_value_type_with_op, control_argument_semantics,
};
use novarocks_types::UniqueId;

use super::aggregate_fixture::{
    LoopbackTransmitter, destination, dop, hash, result_sink, stream_sink, try_compile, try_run,
};
use super::family_fixture::{FixtureControl, cell, int64, int64_rows, values};
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::fragment::ExecutionFailureCause;
use crate::runtime::fragment::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::fragment::instance::{ExchangeInputAssignment, ExchangeInputAssignments};
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::fragment::io::{ExchangeReceiverPort, NoopFragmentEventSink};
use crate::runtime::runtime_state::RuntimeState;

pub(super) type Row = (Option<i64>, Option<i64>);
pub(super) type Rows = Vec<Vec<Option<i64>>>;

/// `(k, x)`: duplicate keys, a NULL key, a NULL `x` and an unmatched key.
pub(super) const LEFT_ROWS: [Row; 9] = [
    (Some(1), Some(10)),
    (Some(1), Some(11)),
    (Some(2), Some(20)),
    (Some(3), Some(30)),
    (None, Some(40)),
    (Some(4), Some(50)),
    (Some(2), Some(5)),
    (Some(6), None),
    (None, Some(1)),
];
/// `(k, y)`: duplicate keys, a NULL key, a NULL `y` and an unmatched key.
pub(super) const RIGHT_ROWS: [Row; 9] = [
    (Some(1), Some(15)),
    (Some(1), Some(9)),
    (Some(2), Some(25)),
    (Some(2), Some(1)),
    (Some(5), Some(55)),
    (None, Some(60)),
    (Some(4), None),
    (Some(6), Some(70)),
    (None, Some(2)),
];

pub(super) const SINGLE: FragmentId = FragmentId::new(1);
const LEFT_SOURCE: FragmentId = FragmentId::new(2);
const RIGHT_SOURCE: FragmentId = FragmentId::new(3);
const JOIN: FragmentId = FragmentId::new(4);
const RESULT: FragmentId = FragmentId::new(5);
const TO_PROBE: EdgeId = EdgeId::new(21);
const TO_BUILD: EdgeId = EdgeId::new(22);
const TO_RESULT: EdgeId = EdgeId::new(23);

pub(super) const SINGLE_FINST: UniqueId = UniqueId::new(0xa1, 0x01);
const LEFT_FINST: UniqueId = UniqueId::new(0xa2, 0x01);
const RIGHT_FINST: UniqueId = UniqueId::new(0xa3, 0x01);
const JOIN_FINSTS: [UniqueId; 2] = [UniqueId::new(0xa4, 0x01), UniqueId::new(0xa4, 0x02)];
const RESULT_FINST: UniqueId = UniqueId::new(0xa5, 0x01);
/// Probe drivers of each multi-fragment join instance.
const JOIN_DOP: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(super) enum Family {
    Hash {
        build_side: JoinSide,
        null_safe: bool,
    },
    NestLoop,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Spec {
    pub(super) family: Family,
    pub(super) kind: JoinKind,
    /// `left.x < right.y`.
    pub(super) residual: bool,
    /// Publish a reordered subset through the selection Project.
    pub(super) permuted: bool,
}

impl Spec {
    pub(super) fn hash(kind: JoinKind, residual: bool) -> Self {
        Self {
            family: Family::Hash {
                build_side: JoinSide::Right,
                null_safe: false,
            },
            kind,
            residual,
            permuted: false,
        }
    }
    fn nested(kind: JoinKind, residual: bool) -> Self {
        Self {
            family: Family::NestLoop,
            kind,
            residual,
            permuted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Placement {
    /// Both inputs are Values in one fragment.
    Single,
    /// The probe arrives hash-partitioned and the build replicated.
    Broadcast,
    /// Both inputs arrive hash-partitioned under one scheme.
    Partitioned,
}

// ---------------------------------------------------------------------------
// Independent oracle.

fn residual_true(spec: Spec, left: Row, right: Row) -> bool {
    !spec.residual || matches!((left.1, right.1), (Some(x), Some(y)) if x < y)
}

fn pair_matches(spec: Spec, left: Row, right: Row) -> bool {
    let key = match spec.family {
        Family::Hash { null_safe, .. } => {
            if null_safe {
                left.0 == right.0
            } else {
                left.0.is_some() && left.0 == right.0
            }
        }
        Family::NestLoop => true,
    };
    key && residual_true(spec, left, right)
}

/// The physical output rows the plan must publish, in output order.
pub(super) fn oracle(spec: Spec, left: &[Row], right: &[Row]) -> Rows {
    let pair = |l: Row, r: Row| vec![l.0, l.1, r.0, r.1];
    let mut rows = Vec::new();
    match spec.kind {
        JoinKind::Inner | JoinKind::Cross => {
            for &l in left {
                for &r in right {
                    if pair_matches(spec, l, r) {
                        rows.push(if spec.permuted {
                            vec![r.1, l.1, l.0]
                        } else {
                            pair(l, r)
                        });
                    }
                }
            }
        }
        JoinKind::LeftOuter => {
            for &l in left {
                let matched = right
                    .iter()
                    .filter(|r| pair_matches(spec, l, **r))
                    .collect::<Vec<_>>();
                if matched.is_empty() {
                    rows.push(if spec.permuted {
                        vec![None, l.0]
                    } else {
                        vec![l.0, l.1, None, None]
                    });
                }
                for r in matched {
                    rows.push(if spec.permuted {
                        vec![r.1, l.0]
                    } else {
                        pair(l, *r)
                    });
                }
            }
        }
        JoinKind::RightOuter => {
            for &r in right {
                let matched = left
                    .iter()
                    .filter(|l| pair_matches(spec, **l, r))
                    .collect::<Vec<_>>();
                if matched.is_empty() {
                    rows.push(vec![None, None, r.0, r.1]);
                }
                for l in matched {
                    rows.push(pair(*l, r));
                }
            }
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti => {
            let semi = spec.kind == JoinKind::LeftSemi;
            for &l in left {
                if right.iter().any(|r| pair_matches(spec, l, *r)) == semi {
                    rows.push(vec![l.0, l.1]);
                }
            }
        }
        JoinKind::RightSemi | JoinKind::RightAnti => {
            let semi = spec.kind == JoinKind::RightSemi;
            for &r in right {
                if left.iter().any(|l| pair_matches(spec, *l, r)) == semi {
                    rows.push(vec![r.0, r.1]);
                }
            }
        }
        JoinKind::NullAwareLeftAnti => {
            for &l in left {
                // NOT IN: a row survives only when no build row can equal it.
                let keep = if right.is_empty() {
                    true
                } else if matches!(spec.family, Family::NestLoop) {
                    false
                } else if !spec.residual {
                    l.0.is_some() && right.iter().all(|r| r.0.is_some() && r.0 != l.0)
                } else if l.0.is_none() {
                    !right.iter().any(|r| residual_true(spec, l, *r))
                } else {
                    !right
                        .iter()
                        .any(|r| (r.0 == l.0 || r.0.is_none()) && residual_true(spec, l, *r))
                };
                if keep {
                    rows.push(vec![l.0, l.1]);
                }
            }
        }
        JoinKind::FullOuter => unreachable!("not a fixture kind"),
    }
    rows.sort();
    rows
}

// ---------------------------------------------------------------------------
// Physical authoring.

fn literal_rows(rows: &[Row]) -> Vec<Vec<LiteralValue>> {
    rows.iter().map(|(k, v)| vec![cell(*k), cell(*v)]).collect()
}

fn admission() -> FragmentPackageAdmission {
    // Explicit small-fixture invoice and independent projection ceilings.
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// Add the join of `left` and `right` and return its physical output.
fn add_join(
    builder: &mut FragmentBuilder,
    join: NodeId,
    left: (NodeId, [ValueId; 2]),
    right: (NodeId, [ValueId; 2]),
    spec: Spec,
    placement: Placement,
) -> Vec<ValueId> {
    let (left_node, l) = left;
    let (right_node, r) = right;
    let key = match spec.family {
        Family::Hash { null_safe, .. } => Some(JoinKey {
            left: builder
                .add_expression(join, int64(true), ExprKind::Value(l[0]))
                .unwrap(),
            right: builder
                .add_expression(join, int64(true), ExprKind::Value(r[0]))
                .unwrap(),
            null_safe,
        }),
        Family::NestLoop => None,
    };
    let residual = spec.residual.then(|| {
        let x = builder
            .add_expression(join, int64(true), ExprKind::Value(l[1]))
            .unwrap();
        let y = builder
            .add_expression(join, int64(true), ExprKind::Value(r[1]))
            .unwrap();
        builder
            .add_expression(
                join,
                FunctionValueType::new(DataType::Boolean, true),
                ExprKind::Binary {
                    left: x,
                    op: BinaryOperator::Lt,
                    right: y,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()
    });
    let mut extend = |of: ValueId| {
        builder
            .add_value(int64(true), ValueOrigin::NullExtended { node: join, of })
            .unwrap()
    };
    let output = match (spec.kind, spec.permuted) {
        (JoinKind::Inner | JoinKind::Cross, false) => vec![l[0], l[1], r[0], r[1]],
        (JoinKind::Inner, true) => vec![r[1], l[1], l[0]],
        (JoinKind::LeftOuter, false) => vec![l[0], l[1], extend(r[0]), extend(r[1])],
        (JoinKind::LeftOuter, true) => vec![extend(r[1]), l[0]],
        (JoinKind::RightOuter, false) => vec![extend(l[0]), extend(l[1]), r[0], r[1]],
        (JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti, false) => {
            l.to_vec()
        }
        (JoinKind::RightSemi | JoinKind::RightAnti, false) => r.to_vec(),
        other => panic!("no fixture output for {other:?}"),
    };
    let null_extended = output
        .iter()
        .copied()
        .filter(|value| {
            matches!(
                builder.value(*value).unwrap().origin,
                ValueOrigin::NullExtended { .. }
            )
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let left_properties = builder.node_output_properties(left_node).unwrap().clone();
    let right_properties = builder.node_output_properties(right_node).unwrap().clone();
    // The probe side's placement is the output's; a singleton join is
    // singleton.
    let probe_properties = match spec.family {
        Family::Hash {
            build_side: JoinSide::Left,
            ..
        } => &right_properties,
        _ => &left_properties,
    };
    let output_distribution = probe_properties.distribution.clone();
    let kind = match spec.family {
        Family::Hash { build_side, .. } => NodeKind::HashJoin {
            kind: spec.kind,
            keys: Box::from([key.unwrap()]),
            build_side,
            distribution: match placement {
                Placement::Single => JoinDistribution::Singleton,
                Placement::Broadcast => JoinDistribution::BroadcastBuild,
                Placement::Partitioned => JoinDistribution::Partitioned,
            },
            residual,
            null_extended,
        },
        Family::NestLoop => NodeKind::NestLoopJoin {
            kind: spec.kind,
            distribution: match placement {
                Placement::Single => NestLoopJoinDistribution::Singleton,
                Placement::Broadcast => NestLoopJoinDistribution::BroadcastRight,
                Placement::Partitioned => panic!("a nested-loop join is never partitioned"),
            },
            predicate: residual,
            null_extended,
        },
    };
    builder
        .add_join(
            join,
            [left_node, right_node],
            Box::from([left_properties, right_properties]),
            output.clone().into_boxed_slice(),
            output_distribution,
            kind,
        )
        .unwrap();
    output
}

/// Imports `sources` of `edge` as an ExchangeSource at `node`.
fn receive(
    builder: &mut FragmentBuilder,
    node: NodeId,
    edge: EdgeId,
    sources: &[ValueId],
    distribution: impl Fn(&[ValueId]) -> Distribution,
    multiplicity: RowMultiplicity,
) -> Vec<ValueId> {
    let imports = sources
        .iter()
        .map(|source| {
            builder
                .add_value(
                    int64(true),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: *source,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_exchange_source(
            node,
            edge,
            sources
                .iter()
                .copied()
                .zip(imports.iter().copied())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            imports.clone().into_boxed_slice(),
            distribution(&imports),
            multiplicity,
        )
        .unwrap();
    imports
}

fn edge(
    id: EdgeId,
    from: (FragmentId, &[ValueId]),
    to: (FragmentId, NodeId, &[ValueId]),
    source: Distribution,
    destination: Distribution,
) -> Edge {
    let replicated = destination == Distribution::Broadcast;
    Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: from.0,
            projection: Box::from(from.1),
        },
        destination: EdgeDestination {
            fragment: to.0,
            node: to.1,
            receive_mapping: from
                .1
                .iter()
                .copied()
                .zip(to.2.iter().copied())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: if replicated {
                RowMultiplicity::Replicated
            } else {
                RowMultiplicity::SingleCopy
            },
        },
    }
}

fn result_port(fragment: FragmentId, node: NodeId, output: &[ValueId]) -> ResultPort {
    ResultPort {
        scalar_schema: None,
        fragment,
        output: novarocks_physical_plan::OutputPort {
            node,
            columns: Box::from(output),
        },
        fields: output
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64(true)),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: int64(true),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}

fn version() -> PlanVersionId {
    PlanVersionId::try_new([83; 16]).unwrap()
}

/// `Values(left) JOIN Values(right) -> Result` in one fragment.
pub(super) fn single_plan(spec: Spec, left: &[Row], right: &[Row]) -> PhysicalPlan {
    let mut builder = FragmentBuilder::new(SINGLE);
    let left_node = builder.reserve_node_id().unwrap();
    let left_values = values(
        &mut builder,
        left_node,
        &[int64(true), int64(true)],
        &literal_rows(left),
    );
    let right_node = builder.reserve_node_id().unwrap();
    let right_values = values(
        &mut builder,
        right_node,
        &[int64(true), int64(true)],
        &literal_rows(right),
    );
    let join = builder.reserve_node_id().unwrap();
    let output = add_join(
        &mut builder,
        join,
        (left_node, [left_values[0], left_values[1]]),
        (right_node, [right_values[0], right_values[1]]),
        spec,
        Placement::Single,
    );
    let fragment = builder
        .finish_definition(
            join,
            FragmentSink::Result,
            dop(1),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(result_port(SINGLE, join, &output))
        .unwrap();
    plan.finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the single-fragment join plan validates: {error:?}"))
}

/// `Values(left) -> Stream(Hash[k])` and `Values(right) -> Stream(Broadcast
/// or Hash[k])` into two join instances, whose outputs are gathered into one
/// result instance.
fn exchange_plan(spec: Spec, placement: Placement, left: &[Row], right: &[Row]) -> PhysicalPlan {
    let source = |id: FragmentId, edge: EdgeId, rows: &[Row]| {
        let mut builder = FragmentBuilder::new(id);
        let node = builder.reserve_node_id().unwrap();
        let columns = values(
            &mut builder,
            node,
            &[int64(true), int64(true)],
            &literal_rows(rows),
        );
        let fragment = builder
            .finish_definition(
                node,
                FragmentSink::Stream { edge },
                dop(1),
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        (fragment, columns)
    };
    let (left_fragment, left_columns) = source(LEFT_SOURCE, TO_PROBE, left);
    let (right_fragment, right_columns) = source(RIGHT_SOURCE, TO_BUILD, right);

    let mut builder = FragmentBuilder::new(JOIN);
    let probe_node = builder.reserve_node_id().unwrap();
    let probe = receive(
        &mut builder,
        probe_node,
        TO_PROBE,
        &left_columns,
        |imports| hash(&imports[..1]),
        RowMultiplicity::SingleCopy,
    );
    let build_node = builder.reserve_node_id().unwrap();
    let build = match placement {
        Placement::Broadcast => receive(
            &mut builder,
            build_node,
            TO_BUILD,
            &right_columns,
            |_| Distribution::Broadcast,
            RowMultiplicity::Replicated,
        ),
        Placement::Partitioned => receive(
            &mut builder,
            build_node,
            TO_BUILD,
            &right_columns,
            |imports| hash(&imports[..1]),
            RowMultiplicity::SingleCopy,
        ),
        Placement::Single => unreachable!("an exchange plan has exchanged inputs"),
    };
    let join = builder.reserve_node_id().unwrap();
    let output = add_join(
        &mut builder,
        join,
        (probe_node, [probe[0], probe[1]]),
        (build_node, [build[0], build[1]]),
        spec,
        placement,
    );
    let join_fragment = builder
        .finish_definition(
            join,
            FragmentSink::Stream { edge: TO_RESULT },
            dop(u32::try_from(JOIN_DOP).unwrap()),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut builder = FragmentBuilder::new(RESULT);
    let receiver = builder.reserve_node_id().unwrap();
    let gathered = receive(
        &mut builder,
        receiver,
        TO_RESULT,
        &output,
        |_| Distribution::Singleton,
        RowMultiplicity::SingleCopy,
    );
    let result_fragment = builder
        .finish_definition(
            receiver,
            FragmentSink::Result,
            dop(1),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(left_fragment).unwrap();
    plan.add_fragment(right_fragment).unwrap();
    plan.add_fragment(join_fragment).unwrap();
    plan.add_fragment(result_fragment).unwrap();
    plan.add_edge(edge(
        TO_PROBE,
        (LEFT_SOURCE, &left_columns),
        (JOIN, probe_node, &probe),
        hash(&left_columns[..1]),
        hash(&probe[..1]),
    ))
    .unwrap();
    let (build_source, build_destination) = match placement {
        Placement::Broadcast => (Distribution::Broadcast, Distribution::Broadcast),
        _ => (hash(&right_columns[..1]), hash(&build[..1])),
    };
    plan.add_edge(edge(
        TO_BUILD,
        (RIGHT_SOURCE, &right_columns),
        (JOIN, build_node, &build),
        build_source,
        build_destination,
    ))
    .unwrap();
    plan.add_edge(edge(
        TO_RESULT,
        (JOIN, &output),
        (RESULT, receiver, &gathered),
        Distribution::Singleton,
        Distribution::Singleton,
    ))
    .unwrap();
    plan.set_result_port(result_port(RESULT, receiver, &gathered))
        .unwrap();
    plan.finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the exchanged join plan validates: {error:?}"))
}

/// One eager use per actual occurrence: a value or literal leaf, or a binary
/// comparison over its two ordered operands. Join fragments freeze no call.
fn freeze(fragment: &Fragment) -> PhysicalRootUses {
    struct Author<'a> {
        fragment: &'a Fragment,
        next: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
    }
    impl Author<'_> {
        fn visit(
            &mut self,
            expr: ExprId,
            domain: EvaluationDomainId,
            demand: EvaluationDemand,
        ) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next);
            self.next += 1;
            let children = match &self.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Binary { left, right, .. } => vec![*left, *right],
                ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
                other => panic!("join fixture has no {other:?}"),
            };
            let arguments = children
                .iter()
                .enumerate()
                .map(|(ordinal, &child)| {
                    let (child_demand, guard) = control_argument_semantics(
                        ControlShape::Eager,
                        children.len(),
                        ordinal,
                        demand,
                    )
                    .unwrap();
                    assert!(guard.is_none());
                    self.visit(child, domain, child_demand)
                })
                .collect::<Vec<_>>();
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain,
                    demand,
                },
                definition: expr,
                control: ControlShape::Eager,
                arguments: arguments.into_boxed_slice(),
            });
            id
        }
    }
    let domain = EvaluationDomainId::new(0);
    let mut author = Author {
        fragment,
        next: 0,
        uses: Vec::new(),
    };
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| (site, author.visit(root.expr, domain, root.demand)))
        .collect::<Vec<_>>();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        author.uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

pub(super) fn packages(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FragmentPackage> {
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let root_uses = freeze(fragment);
        calls.insert(
            *id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(*id, root_uses);
        pruning.insert(
            *id,
            FrozenFragmentPruning::try_new(*id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(*id, admission());
    }
    extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("join packages extract: {error:?}"))
}

pub(super) fn compile(
    package: FragmentPackage,
    pipeline_dop: usize,
    result: bool,
) -> Arc<LocalProgram> {
    let catalog = crate::exec::expr::compiled_program::tests::rng_subset();
    try_compile(package, &catalog, pipeline_dop, result)
        .unwrap_or_else(|error| panic!("join fragment compiles: {error}"))
}

/// Every receiver of `program`, registered for `instance` with `senders`
/// expected sender instances each.
fn register_all(
    program: &LocalProgram,
    instance: UniqueId,
    senders: usize,
    port: &Arc<dyn ExchangeReceiverPort>,
) -> ExchangeBindings {
    let assignments = program
        .exchange_inputs()
        .values()
        .map(|input| {
            (
                FragmentNodeId::new(i32::try_from(input.receiver_node).unwrap()),
                ExchangeInputAssignment::new(NonZeroUsize::new(senders).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = materialize_compiled_exchange_receivers(
        program,
        instance,
        &ExchangeInputAssignments::new(assignments),
        Arc::clone(port),
        Some(&crate::runtime::fragment::exchange::test_receiver_creation_factory()),
    )
    .expect("compiled receivers");
    for registration in registrations {
        port.register(registration).expect("register receiver");
    }
    bindings
}

// ---------------------------------------------------------------------------
// Runs.

pub(super) fn sorted(mut rows: Rows) -> Rows {
    rows.sort();
    rows
}

fn try_run_single(spec: Spec, left: &[Row], right: &[Row]) -> Result<Rows, String> {
    let mut packages = packages(&single_plan(spec, left, right));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    let output = ResultSinkHandle::new();
    try_run(
        &program,
        result_sink(&program, SINGLE_FINST, &output),
        ExchangeBindings::default(),
        SINGLE_FINST,
    )?;
    Ok(sorted(int64_rows(&output.take_chunks())))
}

fn run_single(spec: Spec, left: &[Row], right: &[Row]) -> Rows {
    try_run_single(spec, left, right)
        .unwrap_or_else(|error| panic!("{spec:?} runs in one fragment: {error}"))
}

/// What one exchanged run observed.
struct ExchangeRun {
    rows: Rows,
    /// Rows each join instance received on its probe and build receivers.
    probe_rows: [usize; 2],
    build_rows: [usize; 2],
    join: Arc<LocalProgram>,
}

fn run_exchanged(spec: Spec, placement: Placement, left: &[Row], right: &[Row]) -> ExchangeRun {
    let mut packages = packages(&exchange_plan(spec, placement, left, right));
    let left_source = compile(packages.remove(&LEFT_SOURCE).unwrap(), 1, false);
    let right_source = compile(packages.remove(&RIGHT_SOURCE).unwrap(), 1, false);
    let join = compile(packages.remove(&JOIN).unwrap(), JOIN_DOP, false);
    let result = compile(packages.remove(&RESULT).unwrap(), 1, true);

    let port = in_process_test_exchange_receiver_port();
    let transmitter = LoopbackTransmitter::new(Arc::clone(&port));
    let join_bindings = JOIN_FINSTS.map(|instance| register_all(&join, instance, 1, &port));
    let result_bindings = register_all(&result, RESULT_FINST, JOIN_FINSTS.len(), &port);

    for (program, instance) in [(&left_source, LEFT_FINST), (&right_source, RIGHT_FINST)] {
        let sink = stream_sink(
            program,
            instance,
            JOIN_FINSTS
                .iter()
                .map(|join| destination(*join, instance, 0, 1))
                .collect(),
            &transmitter,
        );
        try_run(program, sink, ExchangeBindings::default(), instance)
            .unwrap_or_else(|error| panic!("source runs: {error}"));
    }
    for (ordinal, (instance, bindings)) in JOIN_FINSTS.into_iter().zip(join_bindings).enumerate() {
        let sink = stream_sink(
            &join,
            instance,
            vec![destination(
                RESULT_FINST,
                instance,
                u32::try_from(ordinal).unwrap(),
                u32::try_from(JOIN_FINSTS.len()).unwrap(),
            )],
            &transmitter,
        );
        try_run(&join, sink, bindings, instance)
            .unwrap_or_else(|error| panic!("{spec:?} join instance runs: {error}"));
    }
    let output = ResultSinkHandle::new();
    try_run(
        &result,
        result_sink(&result, RESULT_FINST, &output),
        result_bindings,
        RESULT_FINST,
    )
    .unwrap_or_else(|error| panic!("result runs: {error}"));
    ExchangeRun {
        rows: sorted(int64_rows(&output.take_chunks())),
        probe_rows: JOIN_FINSTS.map(|instance| transmitter.rows(LEFT_FINST, instance)),
        build_rows: JOIN_FINSTS.map(|instance| transmitter.rows(RIGHT_FINST, instance)),
        join,
    }
}

fn join_kind(program: &LocalProgram) -> &ProgramNodeKind {
    program
        .graph()
        .nodes()
        .iter()
        .map(|node| node.kind())
        .find(|kind| {
            matches!(
                kind,
                ProgramNodeKind::Join { .. } | ProgramNodeKind::NestedLoopJoin { .. }
            )
        })
        .expect("one compiled join")
}

// ---------------------------------------------------------------------------
// Tests.

#[test]
fn inner_equi_join_with_residual_over_two_values_matches_the_oracle() {
    for permuted in [false, true] {
        let spec = Spec {
            permuted,
            ..Spec::hash(JoinKind::Inner, true)
        };
        let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
        // Duplicate keys meet, and the residual removes some equal pairs.
        assert_eq!(expected.len(), 4, "{expected:?}");
        assert_eq!(run_single(spec, &LEFT_ROWS, &RIGHT_ROWS), expected);
    }
    // Without the residual every equal non-NULL key pair survives.
    let spec = Spec::hash(JoinKind::Inner, false);
    let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
    assert_eq!(expected.len(), 10);
    assert_eq!(run_single(spec, &LEFT_ROWS, &RIGHT_ROWS), expected);
}

#[test]
fn left_outer_join_null_extends_unmatched_probe_rows() {
    for (residual, permuted) in [(false, false), (true, false), (true, true)] {
        let spec = Spec {
            permuted,
            ..Spec::hash(JoinKind::LeftOuter, residual)
        };
        let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
        assert!(expected.iter().any(|row| row.last() == Some(&None)));
        assert_eq!(
            run_single(spec, &LEFT_ROWS, &RIGHT_ROWS),
            expected,
            "{spec:?}"
        );
    }
    // An empty build NULL-extends every probe row.
    let spec = Spec::hash(JoinKind::LeftOuter, true);
    assert_eq!(
        run_single(spec, &LEFT_ROWS, &[]),
        oracle(spec, &LEFT_ROWS, &[])
    );
}

#[test]
fn semi_and_anti_joins_including_mirrored_right_kinds_match_the_oracle() {
    for kind in [
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
        JoinKind::RightSemi,
        JoinKind::RightAnti,
    ] {
        for residual in [false, true] {
            let mut spec = Spec::hash(kind, residual);
            if matches!(kind, JoinKind::RightSemi | JoinKind::RightAnti) {
                // The physical right side is preserved, so the left side is
                // the build and the compiler mirrors the kind.
                spec.family = Family::Hash {
                    build_side: JoinSide::Left,
                    null_safe: false,
                };
            }
            let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
            assert!(!expected.is_empty());
            assert_eq!(
                run_single(spec, &LEFT_ROWS, &RIGHT_ROWS),
                expected,
                "{spec:?}"
            );
        }
        // An empty build keeps nothing for semi and everything for anti.
        let spec = Spec::hash(kind, false);
        if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            assert_eq!(
                run_single(spec, &LEFT_ROWS, &[]),
                oracle(spec, &LEFT_ROWS, &[])
            );
        }
    }
    // The mirrored kind is the local probe-preserving one over the right input.
    let spec = Spec {
        family: Family::Hash {
            build_side: JoinSide::Left,
            null_safe: false,
        },
        ..Spec::hash(JoinKind::RightSemi, false)
    };
    let mut packages = packages(&single_plan(spec, &LEFT_ROWS, &RIGHT_ROWS));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    assert!(matches!(
        join_kind(&program),
        ProgramNodeKind::Join {
            join_type: JoinType::LeftSemi,
            distribution_mode: JoinDistributionMode::Broadcast,
            ..
        }
    ));
}

#[test]
fn null_safe_keys_match_null_to_null() {
    for kind in [JoinKind::Inner, JoinKind::LeftAnti] {
        let spec = Spec {
            family: Family::Hash {
                build_side: JoinSide::Right,
                null_safe: true,
            },
            ..Spec::hash(kind, false)
        };
        let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
        let ordinary = oracle(Spec::hash(kind, false), &LEFT_ROWS, &RIGHT_ROWS);
        assert_ne!(expected, ordinary, "the NULL keys must change the answer");
        assert_eq!(run_single(spec, &LEFT_ROWS, &RIGHT_ROWS), expected);
    }
}

#[test]
fn null_aware_anti_join_follows_not_in_semantics() {
    let no_null_build = RIGHT_ROWS
        .iter()
        .copied()
        .filter(|row| row.0.is_some())
        .collect::<Vec<_>>();
    for residual in [false, true] {
        let spec = Spec::hash(JoinKind::NullAwareLeftAnti, residual);
        for right in [&RIGHT_ROWS[..], &no_null_build[..], &[]] {
            assert_eq!(
                run_single(spec, &LEFT_ROWS, right),
                oracle(spec, &LEFT_ROWS, right),
                "{spec:?} over {} build rows",
                right.len()
            );
        }
    }
    // A NULL build key empties a residual-free NOT IN.
    let spec = Spec::hash(JoinKind::NullAwareLeftAnti, false);
    assert!(run_single(spec, &LEFT_ROWS, &RIGHT_ROWS).is_empty());
    assert!(!run_single(spec, &LEFT_ROWS, &no_null_build).is_empty());
}

#[test]
fn nested_loop_joins_with_a_predicate_match_the_oracle() {
    let mut specs = vec![Spec::nested(JoinKind::Cross, false)];
    for kind in [
        JoinKind::Inner,
        JoinKind::LeftOuter,
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
        JoinKind::RightSemi,
        JoinKind::RightAnti,
    ] {
        specs.push(Spec::nested(kind, true));
    }
    specs.push(Spec::nested(JoinKind::NullAwareLeftAnti, false));
    for spec in specs {
        let expected = oracle(spec, &LEFT_ROWS, &RIGHT_ROWS);
        assert_eq!(
            run_single(spec, &LEFT_ROWS, &RIGHT_ROWS),
            expected,
            "{spec:?}"
        );
        if !matches!(spec.kind, JoinKind::RightSemi | JoinKind::RightAnti) {
            assert_eq!(
                run_single(spec, &LEFT_ROWS, &[]),
                oracle(spec, &LEFT_ROWS, &[]),
                "{spec:?} over an empty build"
            );
        }
    }
    assert_eq!(
        oracle(
            Spec::nested(JoinKind::Cross, false),
            &LEFT_ROWS,
            &RIGHT_ROWS
        )
        .len(),
        LEFT_ROWS.len() * RIGHT_ROWS.len()
    );
    let mut packages = packages(&single_plan(
        Spec::nested(JoinKind::RightAnti, true),
        &LEFT_ROWS,
        &RIGHT_ROWS,
    ));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    assert!(matches!(
        join_kind(&program),
        ProgramNodeKind::NestedLoopJoin {
            join_type: NestedLoopJoinType::LeftAnti,
            ..
        }
    ));
}

const ALLOW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(3),
    expected_key: SemanticParameterKey::AllowThrowException,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Overflow {
    ProbeKey,
    BuildKey,
    Residual,
}

/// `value + 9223372036854775807`, which fails for every positive value.
fn overflowing(builder: &mut FragmentBuilder, owner: NodeId, value: ValueId) -> ExprId {
    let read = builder
        .add_expression(owner, int64(true), ExprKind::Value(value))
        .unwrap();
    let max = builder
        .add_expression(
            owner,
            int64(false),
            ExprKind::Literal(LiteralValue::Int64(i64::MAX)),
        )
        .unwrap();
    let ty =
        arithmetic_result_value_type_with_op(&int64(true), &int64(false), ArithmeticOperator::Add)
            .unwrap();
    assert_eq!(ty, int64(true));
    builder
        .add_expression(
            owner,
            ty,
            ExprKind::Binary {
                op: BinaryOperator::Add,
                left: read,
                right: max,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(ALLOW),
            },
        )
        .unwrap()
}

/// An inner equi-join of the fixture rows whose `at` expression overflows.
pub(super) fn overflow_plan(at: Overflow) -> PhysicalPlan {
    let mut builder = FragmentBuilder::new(SINGLE);
    let left_node = builder.reserve_node_id().unwrap();
    let l = values(
        &mut builder,
        left_node,
        &[int64(true), int64(true)],
        &literal_rows(&LEFT_ROWS),
    );
    let right_node = builder.reserve_node_id().unwrap();
    let r = values(
        &mut builder,
        right_node,
        &[int64(true), int64(true)],
        &literal_rows(&RIGHT_ROWS),
    );
    let join = builder.reserve_node_id().unwrap();
    let read = |builder: &mut FragmentBuilder, value| {
        builder
            .add_expression(join, int64(true), ExprKind::Value(value))
            .unwrap()
    };
    let key = JoinKey {
        left: if at == Overflow::ProbeKey {
            overflowing(&mut builder, join, l[0])
        } else {
            read(&mut builder, l[0])
        },
        right: if at == Overflow::BuildKey {
            overflowing(&mut builder, join, r[0])
        } else {
            read(&mut builder, r[0])
        },
        null_safe: false,
    };
    let residual = (at == Overflow::Residual).then(|| {
        let x = overflowing(&mut builder, join, l[1]);
        let y = read(&mut builder, r[1]);
        builder
            .add_expression(
                join,
                FunctionValueType::new(DataType::Boolean, true),
                ExprKind::Binary {
                    left: x,
                    op: BinaryOperator::Lt,
                    right: y,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
            )
            .unwrap()
    });
    let output = vec![l[0], l[1], r[0], r[1]];
    let required =
        [left_node, right_node].map(|node| builder.node_output_properties(node).unwrap().clone());
    builder
        .add_join(
            join,
            [left_node, right_node],
            Box::from(required),
            output.clone().into_boxed_slice(),
            Distribution::Singleton,
            NodeKind::HashJoin {
                kind: JoinKind::Inner,
                keys: Box::from([key]),
                build_side: JoinSide::Right,
                distribution: JoinDistribution::Singleton,
                residual,
                null_extended: Box::default(),
            },
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            join,
            FragmentSink::Result,
            dop(1),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(version()).with_semantic_parameters(
        SemanticParameters::try_new([(
            ALLOW.id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap(),
    );
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(result_port(SINGLE, join, &output))
        .unwrap();
    plan.finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the overflowing join plan validates: {error:?}"))
}

#[test]
fn key_and_residual_row_errors_are_required_errors_of_their_own_roots() {
    for (at, role) in [
        (
            Overflow::ProbeKey,
            ProgramNodeExpressionRole::JoinProbeKey { key: 0 },
        ),
        (
            Overflow::BuildKey,
            ProgramNodeExpressionRole::JoinBuildKey { key: 0 },
        ),
        (Overflow::Residual, ProgramNodeExpressionRole::JoinResidual),
    ] {
        let mut packages = packages(&overflow_plan(at));
        let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
        let join = program.graph().root();
        let output = ResultSinkHandle::new();
        let prepared = prepare_compiled_program_pipeline_execution(
            Arc::clone(&program),
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(output.clone())),
            ExchangeBindings::default(),
            None,
            1,
            Arc::new(RuntimeState::for_local_execution(
                None,
                None,
                None,
                None,
                None,
                Some(crate::runtime::mem_tracker::MemTracker::new_root(
                    "compiled join tests",
                )),
                Some(crate::runtime::execution_runtime::test_execution_runtime()),
            )),
            Arc::new(NoopFragmentEventSink),
        )
        .expect("the join prepares its drivers");
        let failure = prepared
            .start()
            .join()
            .expect_err("an overflowing join root fails the fragment");
        let ExecutionFailureCause::RequiredRow(required) = failure.cause() else {
            panic!("{at:?}: expected a required-expression row error, got {failure:?}")
        };
        assert_eq!(
            required.root(),
            ProgramExpressionRootSite::Node { node: join, role },
            "{at:?}"
        );
        // The first key row, or the first candidate pair, overflows.
        assert_eq!(required.batch_row(), 0, "{at:?}");
        assert!(
            required.error().message().contains("Arithmetic overflow"),
            "{at:?}: {}",
            required.error().message()
        );
        assert!(output.take_chunks().iter().all(|chunk| chunk.is_empty()));
    }
}

#[test]
fn broadcast_build_join_shares_one_replicated_build_across_probe_drivers() {
    for spec in [
        Spec::hash(JoinKind::Inner, true),
        Spec::hash(JoinKind::LeftOuter, true),
        Spec::hash(JoinKind::LeftSemi, false),
        Spec::hash(JoinKind::LeftAnti, true),
        Spec::hash(JoinKind::NullAwareLeftAnti, true),
        Spec::nested(JoinKind::Inner, true),
        Spec::nested(JoinKind::LeftAnti, true),
    ] {
        let run = run_exchanged(spec, Placement::Broadcast, &LEFT_ROWS, &RIGHT_ROWS);
        assert_eq!(run.rows, oracle(spec, &LEFT_ROWS, &RIGHT_ROWS), "{spec:?}");
        // The probe is split between the instances; each instance receives
        // the whole build.
        assert_eq!(run.probe_rows.iter().sum::<usize>(), LEFT_ROWS.len());
        assert!(run.probe_rows.iter().all(|rows| *rows > 0));
        assert_eq!(run.build_rows, [RIGHT_ROWS.len(); 2]);
        assert_eq!(
            run.join.graph().profile().pipeline_dop().get(),
            JOIN_DOP,
            "the probe runs at the pipeline width"
        );
    }
}

#[test]
fn partitioned_join_runs_each_instance_over_its_colocated_partition() {
    for spec in [
        Spec::hash(JoinKind::Inner, true),
        Spec::hash(JoinKind::LeftOuter, false),
        Spec::hash(JoinKind::LeftAnti, true),
        Spec {
            family: Family::Hash {
                build_side: JoinSide::Left,
                null_safe: false,
            },
            ..Spec::hash(JoinKind::RightSemi, true)
        },
    ] {
        let run = run_exchanged(spec, Placement::Partitioned, &LEFT_ROWS, &RIGHT_ROWS);
        assert_eq!(run.rows, oracle(spec, &LEFT_ROWS, &RIGHT_ROWS), "{spec:?}");
        // Both sides are split between the instances by the same scheme.
        assert_eq!(run.probe_rows.iter().sum::<usize>(), LEFT_ROWS.len());
        assert_eq!(run.build_rows.iter().sum::<usize>(), RIGHT_ROWS.len());
        assert!(run.probe_rows.iter().all(|rows| *rows > 0));
        assert!(run.build_rows.iter().all(|rows| *rows > 0));
        assert!(matches!(
            join_kind(&run.join),
            ProgramNodeKind::Join {
                distribution_mode: JoinDistributionMode::Broadcast,
                ..
            }
        ));
    }
}

type StepJoinOperators = (
    Arc<LocalProgram>,
    Box<dyn crate::exec::pipeline::operator::Operator>,
    Box<dyn crate::exec::pipeline::operator::Operator>,
    crate::exec::chunk::Chunk,
    Arc<crate::runtime::runtime_state::RuntimeErrorState>,
);

/// Direct preparation witnesses use the same genuine Physical package/compiler
/// and installed build/probe factories as the existing relational oracle.
fn schema_owner_join_operators(nested: bool) -> StepJoinOperators {
    let spec = if nested {
        Spec::nested(JoinKind::LeftAnti, false)
    } else {
        Spec::hash(JoinKind::LeftAnti, false)
    };
    step_join_operators(spec, &LEFT_ROWS, &[])
}

fn step_join_operators(spec: Spec, left_rows: &[Row], right_rows: &[Row]) -> StepJoinOperators {
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::operators::compiled_nljoin::{
        CompiledNlJoinPlan, CompiledNlJoinProbeProcessorFactory,
    };
    use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
    use crate::exec::operators::hashjoin::compiled_hash_join::{
        CompiledHashJoinBuildSinkFactory, CompiledHashJoinPlan,
        CompiledHashJoinProbeProcessorFactory,
    };
    use crate::exec::operators::{NlJoinBuildSinkFactory, NlJoinSharedState};
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    let nested = matches!(spec.family, Family::NestLoop);
    let mut packages = packages(&single_plan(spec, left_rows, right_rows));
    let program = compile(packages.remove(&SINGLE).unwrap(), 1, true);
    let (ordinal, node) = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| {
            matches!(
                node.kind(),
                ProgramNodeKind::Join { .. } | ProgramNodeKind::NestedLoopJoin { .. }
            )
        })
        .unwrap();
    let left = match node.kind() {
        ProgramNodeKind::Join { left, .. } | ProgramNodeKind::NestedLoopJoin { left, .. } => *left,
        _ => unreachable!(),
    };
    let source = &program.graph().nodes()[left.index()];
    let ProgramNodeKind::Values { values } = source.kind() else {
        unreachable!()
    };
    let input = Chunk::new_with_chunk_schema(
        values.batch().unwrap().clone(),
        ChunkSchema::from_compiled_layout(source.output_layout()).unwrap(),
    );
    let error = Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default());
    let node = novarocks_local_program::ProgramNodeId::new(ordinal);
    let (probe, build): (
        Box<dyn crate::exec::pipeline::operator::Operator>,
        Box<dyn crate::exec::pipeline::operator::Operator>,
    ) = if nested {
        let plan = Arc::new(CompiledNlJoinPlan::try_new(&program, node).unwrap());
        let state = Arc::new(NlJoinSharedState::new(17, DependencyManager::default()));
        (
            CompiledNlJoinProbeProcessorFactory::new(
                program.clone(),
                plan,
                state.clone(),
                error.clone(),
            )
            .create(1, 0),
            NlJoinBuildSinkFactory::new(state).create(1, 0),
        )
    } else {
        let plan = Arc::new(CompiledHashJoinPlan::try_new(&program, node).unwrap());
        let state = Arc::new(BroadcastJoinSharedState::new(
            17,
            DependencyManager::default(),
            1,
        ));
        (
            CompiledHashJoinProbeProcessorFactory::new(
                program.clone(),
                plan.clone(),
                state.clone(),
                error.clone(),
            )
            .create(1, 0),
            CompiledHashJoinBuildSinkFactory::new(
                program.clone(),
                plan,
                state,
                error.clone(),
                None,
            )
            .create(1, 0),
        )
    };
    (program, probe, build, input, error)
}

#[test]
fn compiled_join_schema_prepare_missing_owner_precedes_build_and_input() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    for nested in [false, true] {
        let (_, mut operator, _, input, _) = schema_owner_join_operators(nested);
        let first = operator.prepare().unwrap_err();
        assert!(matches!(
            first.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
        ));
        assert_eq!(operator.prepare().unwrap_err(), first);
        let mut processor = TestDriver::new(operator.as_mut(), &RuntimeState::default());
        assert_eq!(processor.accept_without_output(input).unwrap_err(), first);
        assert_eq!(processor.signal_eos().unwrap_err(), first);
        assert!(processor.drive().unwrap_err().same_occurrence(&first));
    }
}

#[test]
fn compiled_join_schema_prepare_keeps_first_memory_refusal_and_rolls_back() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::mem_tracker::MemTracker;
    for nested in [false, true] {
        let (_, mut operator, _, input, error) = schema_owner_join_operators(nested);
        let tracker = MemTracker::new_root("join first actual schema refusal");
        operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            error,
        ));
        let installed = tracker.current();
        assert!(installed > 0);
        tracker.install_limit_once(installed + 1).unwrap();
        let first = operator.prepare().unwrap_err();
        assert!(matches!(
            first.cause(),
            ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
        ));
        assert_eq!(tracker.current(), installed);
        assert_eq!(operator.prepare().unwrap_err(), first);
        let mut processor = TestDriver::new(operator.as_mut(), &RuntimeState::default());
        assert_eq!(processor.accept_without_output(input).unwrap_err(), first);
        assert_eq!(processor.signal_eos().unwrap_err(), first);
        assert_eq!(tracker.current(), installed);
        drop(processor);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_join_schema_last_slot_keeps_creator_after_plan_and_operators_exit() {
    use crate::runtime::mem_tracker::MemTracker;
    for nested in [false, true] {
        let (program, mut operator, mut build, input, error) = schema_owner_join_operators(nested);
        let weak = Arc::downgrade(&program);
        let tracker = MemTracker::new_root("join independent output schema creator");
        let runtime = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&tracker)),
            None,
            Arc::clone(&error),
        );
        let factory =
            super::aggregate_fixture::runtime_construction_factory(tracker.clone(), error);
        operator.set_construction_factory(factory.clone());
        build.set_construction_factory(factory);
        operator.set_mem_tracker(tracker.clone());
        build.set_mem_tracker(tracker.clone());
        operator.prepare().unwrap();
        build.prepare().unwrap();
        operator.bind_runtime_state(&runtime).unwrap();
        build.bind_runtime_state(&runtime).unwrap();
        {
            let mut build_calls = TestDriver::new(build.as_mut(), &runtime);
            build_calls.signal_eos().unwrap();
            assert!(build_calls.collect_until_finished().unwrap().is_empty());
        }
        let mut processor = TestDriver::new(operator.as_mut(), &runtime);
        processor.accept_without_output(input).unwrap();
        processor.signal_eos().unwrap();
        let output = processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .unwrap();
        assert_eq!(
            sorted(int64_rows(std::slice::from_ref(&output))),
            sorted(LEFT_ROWS.iter().map(|(k, x)| vec![*k, *x]).collect())
        );
        let schema = output.chunk_schema_ref();
        let slot = schema.slots()[0].clone();
        assert!(schema.is_typed());
        drop(processor);
        drop(operator);
        drop(build);
        drop(program);
        assert!(weak.upgrade().is_none());
        drop(output);
        assert!(tracker.current() > 0);
        drop(schema);
        assert!(tracker.current() > 0);
        drop(slot);
        assert_eq!(tracker.current(), 0);
    }
}

/// These closed fixtures declare the complete nested value independently of
/// Arrow's physical anonymous item Field. No metadata is a semantic author.
fn join_nested_fixture_slot(
    id: u32,
    json: bool,
    root_nullable: bool,
    child_nullable: bool,
) -> crate::exec::chunk::ChunkSlotSchema {
    use arrow::datatypes::Field;
    use novarocks_type_contract::{
        TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime,
    };
    let storage = ValueTypeStorageLifetime::pure();
    let child = ValueTypeRef::try_new(
        if json {
            ValueKind::Nominal(ValueLogicalType::Json)
        } else {
            ValueKind::String
        },
        child_nullable,
        storage.clone(),
    )
    .unwrap();
    let semantic =
        ValueTypeRef::try_new(ValueKind::List(child), root_nullable, storage.clone()).unwrap();
    let dtype = DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable)));
    let binding = TypedValueBinding::try_new(
        semantic,
        ValueLayout::try_new(&dtype, storage.clone()).unwrap(),
    )
    .unwrap();
    crate::exec::chunk::ChunkSlotSchema::try_new_typed(
        novarocks_types::SlotId::new(id),
        Arc::new(Field::new(format!("v{id}"), dtype, root_nullable)),
        binding,
        None,
        storage,
    )
    .unwrap()
}

fn join_nested_fixture_source(id: u32, json: bool) -> crate::exec::chunk::Chunk {
    use arrow::array::{ArrayRef, ListArray, StringArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let schema = Arc::new(
        crate::exec::chunk::ChunkSchema::try_new_typed(
            vec![join_nested_fixture_slot(id, json, false, true)],
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    );
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
        Arc::new(StringArray::from(vec![Some("1"), None])),
        None,
    ));
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
    crate::exec::chunk::Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
}

#[test]
fn compiled_join_gather_rejects_nested_domain_and_child_null_drift() {
    use crate::exec::operators::hashjoin::{
        JoinGatherError, gather_join_batch_typed, gather_probe_build_batches_typed,
    };
    use crate::runtime::mem_tracker::MemTracker;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let left = join_nested_fixture_source(901, true);
    let right = join_nested_fixture_source(902, false);
    let tracker = MemTracker::new_root("join complete nested binding refusal");
    let factory = super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    );
    let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::new(
        crate::runtime::runtime_state::RuntimeErrorState::default(),
    ));
    for (left_id, json, child_nullable) in
        [(901, false, true), (901, true, false), (903, true, true)]
    {
        let declared = Arc::new(
            crate::exec::chunk::ChunkSchema::try_new_typed(
                vec![
                    join_nested_fixture_slot(left_id, json, false, child_nullable),
                    join_nested_fixture_slot(902, false, false, true),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        let owner = factory.begin_typed_construction(&declared).unwrap();
        let scratch = factory.begin_typed_construction(&declared).unwrap();
        // Both real consumers use the same complete declaration author, before
        // any take allocation or nullable repair can hide source drift.
        assert!(matches!(
            gather_join_batch_typed(
                &left,
                &right,
                &[0],
                &[0],
                &declared,
                owner.as_ref(),
                scratch.as_ref(),
                &control
            ),
            Err(JoinGatherError::Source(_))
        ));
        assert!(matches!(
            gather_probe_build_batches_typed(
                &left,
                &right,
                &[0],
                &[0],
                &declared,
                true,
                false,
                owner.as_ref(),
                scratch.as_ref(),
                &control
            ),
            Err(JoinGatherError::Source(_))
        ));
    }
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_join_gather_null_extension_requires_declared_nullable_binding() {
    use crate::exec::operators::hashjoin::{JoinGatherError, gather_left_with_null_right_typed};
    use crate::runtime::mem_tracker::MemTracker;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    let left = join_nested_fixture_source(901, true);
    let right = join_nested_fixture_source(902, false);
    let tracker = MemTracker::new_root("join declared outer NULL extension");
    let factory = super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    );
    let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::new(
        crate::runtime::runtime_state::RuntimeErrorState::default(),
    ));
    let required = Arc::new(
        crate::exec::chunk::ChunkSchema::try_new_typed(
            vec![
                join_nested_fixture_slot(901, true, false, true),
                join_nested_fixture_slot(902, false, false, true),
            ],
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    );
    let owner = factory.begin_typed_construction(&required).unwrap();
    let scratch = factory.begin_typed_construction(&required).unwrap();
    assert!(matches!(
        gather_left_with_null_right_typed(
            &left,
            &[0],
            &right.chunk_schema_ref(),
            &required,
            owner.as_ref(),
            scratch.as_ref(),
            &control
        ),
        Err(JoinGatherError::Source(_))
    ));
    drop(scratch);
    drop(owner);
    let nullable = Arc::new(
        crate::exec::chunk::ChunkSchema::try_new_typed(
            vec![
                join_nested_fixture_slot(901, true, false, true),
                join_nested_fixture_slot(902, false, true, true),
            ],
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    );
    let owner = factory.begin_typed_construction(&nullable).unwrap();
    let scratch = factory.begin_typed_construction(&nullable).unwrap();
    let batch = gather_left_with_null_right_typed(
        &left,
        &[0],
        &right.chunk_schema_ref(),
        &nullable,
        owner.as_ref(),
        scratch.as_ref(),
        &control,
    )
    .unwrap()
    .unwrap();
    let chunk = batch.into_chunk(owner.as_ref()).unwrap();
    assert_eq!(chunk.record_batch().column(1).null_count(), 1);
    assert!(chunk.chunk_schema().slots()[1].typed_binding().nullable());
    assert_eq!(
        chunk.chunk_schema().slots()[0].typed_binding().value_type(),
        left.chunk_schema().slots()[0].typed_binding().value_type()
    );
    assert_eq!(
        chunk.chunk_schema().slots()[1]
            .typed_binding()
            .value_type()
            .child_type_at(0),
        right.chunk_schema().slots()[0]
            .typed_binding()
            .value_type()
            .child_type_at(0)
    );
    drop(scratch);
    drop(owner);
    drop(factory);
    assert!(tracker.current() > 0);
    drop(chunk);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_join_output_last_batch_ref_keeps_original_source_creator() {
    use crate::exec::chunk::{ArrayNormalizationStep, Chunk};
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::mem_tracker::MemTracker;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    for nested in [false, true] {
        let (program, mut operator, mut build, input, error) = schema_owner_join_operators(nested);
        let weak = Arc::downgrade(&program);
        let source_tracker = MemTracker::new_root("join input source construction");
        let source_factory = super::aggregate_fixture::runtime_construction_factory(
            source_tracker.clone(),
            error.clone(),
        );
        let source_owner = source_factory
            .begin_typed_construction(input.chunk_schema())
            .unwrap();
        // The fixture's source declaration/column Vec are explicitly pure. The
        // actual independent input wrapper has its own original BE creator.
        let input = Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
            input.record_batch().clone(),
            input.chunk_schema_ref(),
            ValueTypeStorageLifetime::pure(),
            source_owner.storage_lifetime(),
            |layout| source_owner.request(layout),
            |step: ArrayNormalizationStep<'_>| source_owner.observe(0, &[], step),
        )
        .unwrap();
        drop(source_owner);
        drop(source_factory);
        assert!(source_tracker.current() > 0);
        let tracker = MemTracker::new_root("join owned output construction");
        let runtime = RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            Some(Arc::clone(&tracker)),
            None,
            Arc::clone(&error),
        );
        let factory =
            super::aggregate_fixture::runtime_construction_factory(tracker.clone(), error);
        operator.set_construction_factory(factory.clone());
        build.set_construction_factory(factory);
        operator.set_mem_tracker(tracker.clone());
        build.set_mem_tracker(tracker.clone());
        operator.prepare().unwrap();
        build.prepare().unwrap();
        operator.bind_runtime_state(&runtime).unwrap();
        build.bind_runtime_state(&runtime).unwrap();
        {
            let mut build_calls = TestDriver::new(build.as_mut(), &runtime);
            build_calls.signal_eos().unwrap();
            assert!(build_calls.collect_until_finished().unwrap().is_empty());
        }
        let mut processor = TestDriver::new(operator.as_mut(), &runtime);
        processor.accept_without_output(input).unwrap();
        processor.signal_eos().unwrap();
        let output = processor
            .advance_to_boundary()
            .map(|step| step.output)
            .unwrap()
            .unwrap();
        assert_eq!(
            sorted(int64_rows(std::slice::from_ref(&output))),
            sorted(LEFT_ROWS.iter().map(|(k, x)| vec![*k, *x]).collect())
        );
        let projection = output.batch_projection();
        drop(processor);
        drop(operator);
        drop(build);
        drop(program);
        assert!(weak.upgrade().is_none());
        drop(output);
        assert!(source_tracker.current() > 0);
        assert!(tracker.current() > 0);
        drop(projection);
        assert_eq!(source_tracker.current(), 0);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_hash_build_prepare_missing_owner_preserves_input_and_first_cause() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let (_, probe, mut build, input, _) = schema_owner_join_operators(false);
    let source = input.batch_projection();
    let rows = int64_rows(std::slice::from_ref(&input));
    let first = build.prepare().unwrap_err();
    assert!(matches!(
        first.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    assert_eq!(build.prepare().unwrap_err(), first);
    use crate::exec::pipeline::{
        input::RawInput,
        step::InputResult,
        step_context::{StepContext, TurnWork},
    };
    let state = RuntimeState::default();
    let control = state.error_state();
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
    let original = input.columns()[0].clone();
    let step = build.as_mut();
    let InputResult::Rejected {
        input: returned,
        failure,
    } = step.try_push(&mut ctx, RawInput::for_test(input))
    else {
        panic!("failed preparation must reject before accepting input");
    };
    assert_eq!(failure, first);
    assert!(Arc::ptr_eq(&original, &returned.original().columns()[0]));
    assert_eq!(step.drive(&mut ctx).unwrap_err(), first);
    assert!(
        control.error().is_none(),
        "a local preparation cache is not a Task decision"
    );
    // This original view has no consumed rows or rewritten source declaration.
    assert_eq!(int64_rows(&[source.into_chunk()]), rows);
    drop(probe);
}

#[test]
fn compiled_nested_loop_step_waits_before_preparation_and_drains_after_eos() {
    use crate::exec::pipeline::{
        input::RawInput,
        step::{AdmissionNext, InputResult, NextStep},
        step_context::{StepContext, TurnWork},
    };
    use crate::runtime::mem_tracker::MemTracker;
    use std::num::NonZeroU64;
    let (program, mut probe, mut build, input, error) = schema_owner_join_operators(true);
    let runtime = RuntimeState::default();
    let tracker = MemTracker::new_root("nested-loop receiver steps");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    let factory =
        super::aggregate_fixture::runtime_construction_factory(tracker.clone(), error.clone());
    for operator in [&mut probe, &mut build] {
        operator.set_construction_factory(factory.clone());
        operator.set_mem_tracker(tracker.clone());
        operator.bind_runtime_state(&runtime).unwrap();
        operator.prepare().unwrap();
    }
    drop(factory);
    let original = input.columns()[0].clone();
    let mut turn = TurnWork::new(NonZeroU64::new(1).unwrap(), 0);
    let step = probe.as_mut();
    {
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&error, &mut turn, &mut value_conversions);
        let allocated = tracker.allocated();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = step.try_push(&mut ctx, RawInput::for_test(input))
        else {
            panic!("build dependency must leave the raw input on its edge");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(same),
        } = step.try_push(&mut ctx, input)
        else {
            panic!("unpublished build must keep waiting");
        };
        assert!(Arc::ptr_eq(&wait.observable, &same.observable));
        assert_eq!(wait.observed_generation, same.observed_generation);
        assert_eq!(
            tracker.allocated(),
            allocated,
            "no receiver preparation while build is absent"
        );
        let build_step = build.as_mut();
        build_step.finish(&mut ctx).unwrap();
        assert!(matches!(
            build_step.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        let applied = step.drive(&mut ctx).unwrap();
        assert!(applied.output.is_none());
        assert!(matches!(applied.next, NextStep::Runnable));
        step.finish(&mut ctx).unwrap();
        let allocated = tracker.allocated();
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert_eq!(tracker.allocated(), allocated);
    }
    turn.begin_turn(1).unwrap();
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&error, &mut turn, &mut value_conversions);
    let emitted = step.drive(&mut ctx).unwrap();
    assert!(matches!(emitted.next, NextStep::Runnable));
    let output = emitted.output.unwrap();
    assert_eq!(
        sorted(int64_rows(std::slice::from_ref(&output))),
        sorted(
            LEFT_ROWS
                .iter()
                .map(|(key, value)| vec![*key, *value])
                .collect()
        )
    );
    let ended = step.drive(&mut ctx).unwrap();
    assert!(ended.output.is_none());
    assert!(matches!(ended.next, NextStep::Finished));
    assert!(step.drive(&mut ctx).unwrap().output.is_none());
    drop(probe);
    drop(build);
    drop(program);
    assert!(tracker.current() > 0);
    drop(output);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_hash_probe_steps_page_duplicate_matches_and_keep_eos_behind_the_cursor() {
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::operators::hashjoin::MAX_JOIN_OUTPUT_ROWS_PER_BATCH;
    use crate::exec::pipeline::{
        input::RawInput,
        step::{AdmissionNext, InputResult, NextStep},
        step_context::{StepContext, TurnWork},
    };
    use crate::runtime::mem_tracker::MemTracker;
    use std::num::NonZeroU64;
    let rows = (0..129)
        .map(|value| (Some(1), Some(value)))
        .collect::<Vec<Row>>();
    let spec = Spec::hash(JoinKind::Inner, false);
    let (program, mut probe, mut build, input, error) = step_join_operators(spec, &rows, &rows);
    let right = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            ProgramNodeKind::Join { right, .. } => Some(*right),
            _ => None,
        })
        .unwrap();
    let source = &program.graph().nodes()[right.index()];
    let ProgramNodeKind::Values { values } = source.kind() else {
        unreachable!()
    };
    let build_input = Chunk::new_with_chunk_schema(
        values.batch().unwrap().clone(),
        ChunkSchema::from_compiled_layout(source.output_layout()).unwrap(),
    );
    let tracker = MemTracker::new_root("duplicate hash match cursor");
    let runtime = RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        Some(Arc::clone(&tracker)),
        None,
        Arc::clone(&error),
    );
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    let factory =
        super::aggregate_fixture::runtime_construction_factory(tracker.clone(), error.clone());
    for operator in [&mut probe, &mut build] {
        operator.set_construction_factory(factory.clone());
        operator.set_mem_tracker(tracker.clone());
        operator.bind_runtime_state(&runtime).unwrap();
        operator.prepare().unwrap();
    }
    drop(factory);
    let mut turn = TurnWork::new(NonZeroU64::new(1).unwrap(), 0);
    let step = probe.as_mut();
    {
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&error, &mut turn, &mut value_conversions);
        let allocated = tracker.allocated();
        let original = input.columns()[0].clone();
        let InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(wait),
        } = step.try_push(&mut ctx, RawInput::for_test(input))
        else {
            panic!("absent build leaves the original input unprepared");
        };
        assert!(Arc::ptr_eq(
            &input.original_for_test().columns()[0],
            &original
        ));
        assert_eq!(tracker.allocated(), allocated);
        let build_step = build.as_mut();
        assert!(matches!(
            build_step.try_push(&mut ctx, RawInput::for_test(build_input)),
            InputResult::Accepted(Ok(_))
        ));
        assert!(build_step.drive(&mut ctx).unwrap().output.is_none());
        build_step.finish(&mut ctx).unwrap();
        assert!(matches!(
            build_step.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert_ne!(wait.observable.generation(), wait.observed_generation);
        assert!(matches!(
            step.try_push(&mut ctx, input),
            InputResult::Accepted(Ok(_))
        ));
        let applied = step.drive(&mut ctx).unwrap();
        assert!(applied.output.is_none());
        assert!(matches!(applied.next, NextStep::Runnable));
        step.finish(&mut ctx).unwrap();
        let allocated = tracker.allocated();
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert_eq!(tracker.allocated(), allocated);
    }
    let mut output = Vec::new();
    for expected in [
        MAX_JOIN_OUTPUT_ROWS_PER_BATCH,
        rows.len() * rows.len() - MAX_JOIN_OUTPUT_ROWS_PER_BATCH,
    ] {
        turn.begin_turn(1).unwrap();
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&error, &mut turn, &mut value_conversions);
        let page = step.drive(&mut ctx).unwrap();
        assert!(matches!(page.next, NextStep::Runnable));
        let page = page.output.unwrap();
        assert_eq!(page.len(), expected);
        output.push(page);
        let allocated = tracker.allocated();
        let yielded = step.drive(&mut ctx).unwrap();
        assert!(yielded.output.is_none());
        assert!(matches!(yielded.next, NextStep::Yield));
        assert_eq!(tracker.allocated(), allocated);
    }
    assert_eq!(
        sorted(int64_rows(&output)),
        sorted(oracle(spec, &rows, &rows))
    );
    turn.begin_turn(1).unwrap();
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = StepContext::new(&error, &mut turn, &mut value_conversions);
    let tail = step.drive(&mut ctx).unwrap();
    assert!(tail.output.is_none());
    assert!(matches!(tail.next, NextStep::Runnable));
    assert!(matches!(
        step.drive(&mut ctx).unwrap().next,
        NextStep::Finished
    ));
    assert!(step.drive(&mut ctx).unwrap().output.is_none());
    drop(probe);
    drop(build);
    drop(program);
    assert!(tracker.current() > 0);
    drop(output);
    assert_eq!(tracker.current(), 0);
}
