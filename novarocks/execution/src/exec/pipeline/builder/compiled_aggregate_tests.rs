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

//! Aggregates run end to end through compiled programs only.
//!
//! The two-phase plans hash a Values relation by `v` to two Partial
//! instances of two drivers each, so one key's rows land on several partial
//! drivers; each partial states stream hashed by `k` to two Final instances,
//! also of two drivers, which the builder gathers before the Complete
//! aggregate. Every result is checked against an independent Rust oracle
//! over the same rows. Group-key equivalence is pinned for NULL, NaN and
//! signed zero keys.

use std::collections::BTreeMap;

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_local_program::{CompiledAggregateGrouping, LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    AggregateCallId, AggregateGrouping, AggregatePhase, AggregateSequenceId, Distribution, EdgeId,
    FragmentBuilder, FragmentId, FragmentSink, LiteralValue, OutputPort, PhysicalPlan, PlanBuilder,
    PlanVersionId, ResultField, ResultPort, ValueId,
};
use novarocks_type_contract::FunctionValueType;
use novarocks_types::UniqueId;

use super::aggregate_fixture::{
    Bound, CallSpec, LoopbackTransmitter, add_aggregate, bind, compile, destination, edge,
    extrema_catalog, finish, hash, packages, receive, register, result_sink, stream_sink, try_run,
    try_run_in_fixture, values,
};
use super::family_fixture::{FixtureControl, cell, int64, int64_rows};
use crate::exec::chunk::Chunk;
use crate::exec::operators::ResultSinkHandle;
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::runtime::fragment::io::exchange::test_support::Fixture;

const SOURCE: FragmentId = FragmentId::new(1);
const PARTIAL: FragmentId = FragmentId::new(2);
const FINAL: FragmentId = FragmentId::new(3);
const TO_PARTIAL: EdgeId = EdgeId::new(7);
const TO_FINAL: EdgeId = EdgeId::new(8);
/// Drivers of every partial and final instance.
const DOP: usize = 2;

const SOURCE_FINST: UniqueId = UniqueId::new(0x91, 0x01);
const PARTIAL_FINSTS: [UniqueId; 2] = [UniqueId::new(0x92, 0x01), UniqueId::new(0x92, 0x02)];
const FINAL_FINSTS: [UniqueId; 2] = [UniqueId::new(0x93, 0x01), UniqueId::new(0x93, 0x02)];

/// One literal row `(k, v)`.
type Row = (Option<i64>, Option<i64>);

/// Keys repeat with NULLs on both sides; key 7 has only NULL values, so its
/// MIN, MAX and COUNT(v) see no value at all.
const ROWS: [Row; 24] = [
    (Some(1), Some(10)),
    (None, Some(4)),
    (Some(2), Some(-3)),
    (Some(1), None),
    (Some(3), Some(8)),
    (Some(2), Some(9)),
    (None, None),
    (Some(1), Some(-20)),
    (Some(7), None),
    (Some(3), Some(8)),
    (Some(-5), Some(0)),
    (Some(2), Some(11)),
    (None, Some(-6)),
    (Some(1), Some(30)),
    (Some(7), None),
    (Some(3), Some(1)),
    (Some(2), Some(-3)),
    (Some(-5), Some(12)),
    (Some(1), Some(5)),
    (None, Some(15)),
    (Some(3), None),
    (Some(2), Some(40)),
    (Some(-5), Some(-1)),
    (Some(1), Some(2)),
];

/// `(COUNT(*), COUNT(v), MIN(v), MAX(v))` of one group.
type Aggregates = (i64, i64, Option<i64>, Option<i64>);

fn accumulate(group: &mut Aggregates, v: Option<i64>) {
    group.0 += 1;
    if let Some(v) = v {
        group.1 += 1;
        group.2 = Some(group.2.map_or(v, |min| min.min(v)));
        group.3 = Some(group.3.map_or(v, |max| max.max(v)));
    }
}

/// Independent grouped oracle: NULL keys form one group.
fn grouped_oracle(rows: &[Row]) -> BTreeMap<Option<i64>, Aggregates> {
    let mut groups = BTreeMap::<Option<i64>, Aggregates>::new();
    for (k, v) in rows {
        accumulate(groups.entry(*k).or_insert((0, 0, None, None)), *v);
    }
    groups
}

fn literal_rows(rows: &[Row]) -> Vec<Vec<LiteralValue>> {
    rows.iter().map(|(k, v)| vec![cell(*k), cell(*v)]).collect()
}

fn version() -> PlanVersionId {
    PlanVersionId::try_new([97; 16]).unwrap()
}

fn result_port(
    fragment: FragmentId,
    output: OutputPort,
    types: &[FunctionValueType],
) -> ResultPort {
    ResultPort {
        scalar_schema: None,
        fragment,
        fields: output
            .columns
            .iter()
            .zip(types)
            .enumerate()
            .map(|(ordinal, (value, ty))| ResultField {
                domain: crate::test_result_domain::result_value_domain(ty),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output,
    }
}

/// The calls of one aggregate node, each with whether its update phase reads
/// `v` (COUNT(*) reads nothing). A merge phase reads `states[i]` as the
/// state of call `i`.
struct Calls(Vec<(Bound, bool)>);

impl Calls {
    /// `COUNT(*), COUNT(v), MIN(v), MAX(v)`.
    fn extrema(catalog: &novarocks_functions::PureEngineFunctionCatalog) -> Self {
        Self(vec![
            (bind(catalog, "count", &[]), false),
            (bind(catalog, "count", &[int64(true)]), true),
            (bind(catalog, "min", &[int64(true)]), true),
            (bind(catalog, "max", &[int64(true)]), true),
        ])
    }
    /// `SUM(v)` over a nullable BIGINT.
    fn sum(catalog: &novarocks_functions::PureEngineFunctionCatalog) -> Self {
        Self(vec![(bind(catalog, "sum", &[int64(true)]), true)])
    }
    fn all(&self) -> impl Iterator<Item = &Bound> {
        self.0.iter().map(|(bound, _)| bound)
    }
    fn update(&self, v: ValueId, partial: bool) -> Vec<CallSpec<'_>> {
        self.0
            .iter()
            .enumerate()
            .map(|(ordinal, (bound, reads))| {
                let sequence = AggregateSequenceId::new(u32::try_from(ordinal + 1).unwrap());
                CallSpec {
                    bound,
                    phase: if partial {
                        AggregatePhase::Partial { sequence }
                    } else {
                        AggregatePhase::Single
                    },
                    id: AggregateCallId::new(u32::try_from(ordinal + 1).unwrap()),
                    arguments: if *reads { vec![v] } else { Vec::new() },
                    distinct: false,
                }
            })
            .collect()
    }
    fn merge<'a>(&'a self, states: &[ValueId]) -> Vec<CallSpec<'a>> {
        self.all()
            .zip(states)
            .enumerate()
            .map(|(ordinal, (bound, state))| CallSpec {
                bound,
                phase: AggregatePhase::Final {
                    sequence: AggregateSequenceId::new(u32::try_from(ordinal + 1).unwrap()),
                },
                id: AggregateCallId::new(u32::try_from(ordinal + 11).unwrap()),
                arguments: vec![*state],
                distinct: false,
            })
            .collect()
    }
}

/// `Values(k, v) -> Stream(Hash[v])` | `ExchangeSource -> Partial Aggregate
/// -> Stream(Hash[k] or Gather)` | `ExchangeSource -> Final Aggregate ->
/// Result`. A grouped plan groups by `k`; a group-less one aggregates all
/// rows and gathers every partial state to one Final.
fn two_phase_plan(calls: &Calls, rows: &[Row], grouped: bool) -> PhysicalPlan {
    let mut source = FragmentBuilder::new(SOURCE);
    let values_node = source.reserve_node_id().unwrap();
    let columns = values(
        &mut source,
        values_node,
        &[int64(true), int64(true)],
        &literal_rows(rows),
    );
    let source = finish(
        source,
        values_node,
        FragmentSink::Stream { edge: TO_PARTIAL },
        1,
    );

    let mut partial = FragmentBuilder::new(PARTIAL);
    let partial_receiver = partial.reserve_node_id().unwrap();
    let partial_inputs = receive(
        &mut partial,
        partial_receiver,
        TO_PARTIAL,
        &[(columns[0], int64(true)), (columns[1], int64(true))],
        |imports| hash(&imports[1..]),
    );
    let groups = if grouped {
        vec![partial_inputs[0]]
    } else {
        Vec::new()
    };
    let (partial_node, partial_output) = add_aggregate(
        &mut partial,
        partial_receiver,
        &groups,
        &calls.update(partial_inputs[1], true),
        AggregateGrouping::Partial,
    );
    let partial = finish(
        partial,
        partial_node,
        FragmentSink::Stream { edge: TO_FINAL },
        u32::try_from(DOP).unwrap(),
    );

    let mut last = FragmentBuilder::new(FINAL);
    let final_receiver = last.reserve_node_id().unwrap();
    let mut sent = Vec::new();
    if grouped {
        sent.push((partial_output[0], int64(true)));
    }
    let states = &partial_output[groups.len()..];
    for (state, bound) in states.iter().zip(calls.all()) {
        sent.push((*state, bound.intermediate_type()));
    }
    let final_inputs = receive(&mut last, final_receiver, TO_FINAL, &sent, |imports| {
        if grouped {
            hash(&imports[..1])
        } else {
            Distribution::Singleton
        }
    });
    let final_groups = if grouped {
        vec![final_inputs[0]]
    } else {
        Vec::new()
    };
    let (final_node, _) = add_aggregate(
        &mut last,
        final_receiver,
        &final_groups,
        &calls.merge(&final_inputs[final_groups.len()..]),
        AggregateGrouping::Complete,
    );
    let last = finish(
        last,
        final_node,
        FragmentSink::Result,
        u32::try_from(DOP).unwrap(),
    );
    let output = last.nodes()[&final_node].output.clone();
    let mut types = Vec::new();
    if grouped {
        types.push(int64(true));
    }
    types.extend(calls.all().map(Bound::result_type));

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(partial).unwrap();
    plan.add_fragment(last).unwrap();
    plan.add_edge(edge(
        TO_PARTIAL,
        (SOURCE, &columns),
        (PARTIAL, partial_receiver, &partial_inputs),
        hash(&columns[1..]),
        hash(&partial_inputs[1..]),
    ))
    .unwrap();
    let (to_final_source, to_final_destination) = if grouped {
        (hash(&partial_output[..1]), hash(&final_inputs[..1]))
    } else {
        (Distribution::Singleton, Distribution::Singleton)
    };
    plan.add_edge(edge(
        TO_FINAL,
        (PARTIAL, &partial_output),
        (FINAL, final_receiver, &final_inputs),
        to_final_source,
        to_final_destination,
    ))
    .unwrap();
    plan.set_result_port(result_port(FINAL, output, &types))
        .unwrap();
    plan.finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the two-phase aggregate plan validates: {error:?}"))
}

/// The grouping fact of the one Aggregate in `program`.
fn grouping(program: &LocalProgram) -> CompiledAggregateGrouping {
    let nodes = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| matches!(node.kind(), ProgramNodeKind::Aggregate { .. }))
        .map(|(index, _)| novarocks_local_program::ProgramNodeId::new(index))
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 1, "one aggregate per fixture fragment");
    assert_eq!(program.aggregates().len(), 1);
    program.aggregates()[&nodes[0]].grouping
}

/// What one two-phase run observed.
struct TwoPhaseRun {
    /// Rows each partial instance received.
    partial_inputs: [usize; 2],
    /// Rows each partial instance sent, per final instance.
    partial_outputs: [[usize; 2]; 2],
    /// Rows each final instance published.
    results: Vec<Vec<Vec<Option<i64>>>>,
}

fn run_two_phase(rows: &[Row], grouped: bool) -> TwoPhaseRun {
    let catalog = extrema_catalog();
    try_run_two_phase(&catalog, &Calls::extrema(&catalog), rows, grouped)
        .unwrap_or_else(|error| panic!("final runs: {error}"))
}

/// Run the plan; a Final failure is returned as text.
fn try_run_two_phase(
    catalog: &novarocks_functions::PureEngineFunctionCatalog,
    calls: &Calls,
    rows: &[Row],
    grouped: bool,
) -> Result<TwoPhaseRun, String> {
    let plan = two_phase_plan(calls, rows, grouped);
    let mut packages = packages(&plan, catalog);
    let source = compile(packages.remove(&SOURCE).unwrap(), catalog, 1, false);
    let partial = compile(packages.remove(&PARTIAL).unwrap(), catalog, DOP, false);
    let last = compile(packages.remove(&FINAL).unwrap(), catalog, DOP, true);
    assert_eq!(grouping(&partial), CompiledAggregateGrouping::Partial);
    assert_eq!(grouping(&last), CompiledAggregateGrouping::Complete);

    // A grouped Final runs on both hash instances; a group-less one on one.
    let finals = if grouped {
        &FINAL_FINSTS[..]
    } else {
        &FINAL_FINSTS[..1]
    };
    let fixture = Fixture::new();
    let port = fixture.port.clone();
    let transmitter = LoopbackTransmitter::new(fixture.transport.clone());
    let partial_bindings = PARTIAL_FINSTS
        .map(|instance| register(&fixture, &partial, instance, &[SOURCE_FINST], &port));
    let final_bindings = finals
        .iter()
        .map(|instance| register(&fixture, &last, *instance, &PARTIAL_FINSTS, &port))
        .collect::<Vec<_>>();

    let sink = stream_sink(
        &fixture,
        &source,
        SOURCE_FINST,
        PARTIAL_FINSTS
            .iter()
            .map(|instance| destination(*instance, SOURCE_FINST, 0, 1))
            .collect(),
        &transmitter,
    );
    try_run_in_fixture(
        &fixture,
        &source,
        sink,
        ExchangeBindings::default(),
        SOURCE_FINST,
    )
    .unwrap_or_else(|error| panic!("source runs: {error}"));
    for (ordinal, (instance, bindings)) in
        PARTIAL_FINSTS.into_iter().zip(partial_bindings).enumerate()
    {
        let sink = stream_sink(
            &fixture,
            &partial,
            instance,
            finals
                .iter()
                .map(|last| {
                    destination(
                        *last,
                        instance,
                        u32::try_from(ordinal).unwrap(),
                        u32::try_from(PARTIAL_FINSTS.len()).unwrap(),
                    )
                })
                .collect(),
            &transmitter,
        );
        try_run_in_fixture(&fixture, &partial, sink, bindings, instance)
            .unwrap_or_else(|error| panic!("partial runs: {error}"));
    }
    let mut results = Vec::new();
    for (instance, bindings) in finals.iter().zip(final_bindings) {
        let output = ResultSinkHandle::new();
        try_run_in_fixture(
            &fixture,
            &last,
            result_sink(&last, *instance, &output),
            bindings,
            *instance,
        )?;
        results.push(int64_rows(&output.take_chunks()));
    }
    let sent = |partial: UniqueId, index: usize| {
        finals
            .get(index)
            .map_or(0, |last| transmitter.rows(partial, *last))
    };
    Ok(TwoPhaseRun {
        partial_inputs: PARTIAL_FINSTS.map(|instance| transmitter.rows(SOURCE_FINST, instance)),
        partial_outputs: PARTIAL_FINSTS.map(|instance| [sent(instance, 0), sent(instance, 1)]),
        results,
    })
}

fn aggregates_of(row: &[Option<i64>]) -> Aggregates {
    (row[0].unwrap(), row[1].unwrap(), row[2], row[3])
}

#[test]
fn two_phase_grouped_count_min_max_merges_partial_states_across_instances_and_drivers() {
    let run = run_two_phase(&ROWS, true);
    // Both partial instances hold rows, so states of one key really merge
    // across instances, and both final instances own keys.
    assert_eq!(run.partial_inputs.iter().sum::<usize>(), ROWS.len());
    assert!(run.partial_inputs.iter().all(|rows| *rows > 0));
    assert!(run.results.iter().all(|rows| !rows.is_empty()));
    let mut actual = BTreeMap::new();
    for row in run.results.iter().flatten() {
        assert!(
            actual.insert(row[0], aggregates_of(&row[1..])).is_none(),
            "key {:?} is published twice",
            row[0]
        );
    }
    assert_eq!(actual, grouped_oracle(&ROWS));
    // The NULL key is one group, and a key with only NULL values keeps its
    // rows in COUNT(*) while MIN and MAX see no value.
    assert_eq!(actual[&None], (4, 3, Some(-6), Some(15)));
    assert_eq!(actual[&Some(7)], (2, 0, None, None));
    // A partial emits each of its groups once per driver that saw it, so it
    // never sends more states than it received rows.
    for (input, outputs) in run.partial_inputs.iter().zip(&run.partial_outputs) {
        assert!(outputs.iter().sum::<usize>() <= *input);
    }
}

#[test]
fn two_phase_grouped_aggregate_on_empty_input_publishes_no_group() {
    let run = run_two_phase(&[], true);
    assert_eq!(run.partial_inputs, [0, 0]);
    assert_eq!(run.partial_outputs, [[0, 0], [0, 0]]);
    assert!(run.results.iter().all(Vec::is_empty));
}

#[test]
fn two_phase_group_less_count_min_max_publishes_exactly_one_row() {
    let run = run_two_phase(&ROWS, false);
    // Each partial driver emits exactly one state row, and the gathered
    // Complete aggregate merges all four into one row.
    assert_eq!(run.partial_outputs, [[DOP, 0], [DOP, 0]]);
    let mut all = (0, 0, None, None);
    for (_, v) in ROWS {
        accumulate(&mut all, v);
    }
    let [rows] = run.results.as_slice() else {
        panic!("one final instance")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(aggregates_of(&rows[0]), all);
    assert_eq!(all, (24, 19, Some(-20), Some(40)));
}

#[test]
fn two_phase_group_less_aggregate_on_empty_input_publishes_the_initial_state() {
    let run = run_two_phase(&[], false);
    // Every partial driver still emits its one initial state, as v1 does.
    assert_eq!(run.partial_inputs, [0, 0]);
    assert_eq!(run.partial_outputs, [[DOP, 0], [DOP, 0]]);
    let [rows] = run.results.as_slice() else {
        panic!("one final instance")
    };
    assert_eq!(rows, &[vec![Some(0), Some(0), None, None]]);
}

/// `Values(k: DOUBLE) -> Single COUNT(*) GROUP BY k -> Result` in one
/// fragment at the given driver count.
fn float_key_plan(
    catalog: &novarocks_functions::PureEngineFunctionCatalog,
    keys: &[Option<f64>],
) -> PhysicalPlan {
    let count = bind(catalog, "count", &[]);
    let key_type = FunctionValueType::new(DataType::Float64, true);
    let mut only = FragmentBuilder::new(SOURCE);
    let values_node = only.reserve_node_id().unwrap();
    let rows = keys
        .iter()
        .map(|key| {
            vec![key.map_or(LiteralValue::Null, |key| {
                LiteralValue::Float64Bits(key.to_bits())
            })]
        })
        .collect::<Vec<_>>();
    let columns = values(
        &mut only,
        values_node,
        std::slice::from_ref(&key_type),
        &rows,
    );
    let (node, _) = add_aggregate(
        &mut only,
        values_node,
        &columns,
        &[CallSpec {
            bound: &count,
            phase: AggregatePhase::Single,
            id: AggregateCallId::new(1),
            arguments: Vec::new(),
            distinct: false,
        }],
        AggregateGrouping::Complete,
    );
    let only = finish(
        only,
        node,
        FragmentSink::Result,
        u32::try_from(DOP).unwrap(),
    );
    let port_output = only.nodes()[&node].output.clone();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(only).unwrap();
    plan.set_result_port(result_port(
        SOURCE,
        port_output,
        &[key_type, count.result_type()],
    ))
    .unwrap();
    plan.finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the single-phase aggregate plan validates: {error:?}"))
}

fn run_single(
    plan: &PhysicalPlan,
    catalog: &novarocks_functions::PureEngineFunctionCatalog,
) -> Vec<Chunk> {
    let mut packages = packages(plan, catalog);
    let program = compile(packages.remove(&SOURCE).unwrap(), catalog, DOP, true);
    assert_eq!(grouping(&program), CompiledAggregateGrouping::Complete);
    let output = ResultSinkHandle::new();
    try_run(
        &program,
        result_sink(&program, SOURCE_FINST, &output),
        ExchangeBindings::default(),
        SOURCE_FINST,
    )
    .unwrap_or_else(|error| panic!("single-phase aggregate runs: {error}"));
    output.take_chunks()
}

#[test]
fn group_keys_equate_null_every_nan_and_signed_zero_as_the_key_table_owns_them() {
    let catalog = extrema_catalog();
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    assert!(other_nan.is_nan() && other_nan.to_bits() != f64::NAN.to_bits());
    let keys = [
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        None,
        Some(other_nan),
        Some(1.5),
        None,
        Some(-0.0),
        Some(-f64::NAN),
    ];
    let plan = float_key_plan(&catalog, &keys);
    let declared = plan.result_port().unwrap().fields[0].ty.clone();
    assert_eq!(declared, FunctionValueType::new(DataType::Float64, true));
    let chunks = run_single(&plan, &catalog);
    let mut groups = Vec::new();
    for chunk in &chunks {
        // Actual NULL groups retain the original nullable frozen declaration;
        // runtime data must not invent a new output type or widen non-NULL facts.
        assert!(chunk.chunk_schema().is_typed());
        assert!(chunk.record_batch().schema().field(0).is_nullable());
        assert_eq!(
            chunk.chunk_schema().slots()[0].typed_binding(),
            declared.typed_binding(),
        );
        let key = chunk
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("DOUBLE key")
            .clone();
        let count = chunk
            .record_batch()
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("BIGINT count")
            .clone();
        for row in 0..chunk.len() {
            groups.push((
                (!key.is_null(row)).then(|| key.value(row)),
                count.value(row),
            ));
        }
    }
    // Groups appear in first-arrival order: zero, NaN, NULL, 1.5. Signed
    // zeros are one group represented by its first key; every NaN payload
    // and sign is one group; NULL keys are one group.
    assert_eq!(groups.len(), 4, "groups: {groups:?}");
    let (zero, zeros) = groups[0];
    assert_eq!((zero.map(|zero| zero == 0.0), zeros), (Some(true), 3));
    assert_eq!(zero.map(f64::is_sign_negative), Some(true));
    let (nan, nans) = groups[1];
    assert_eq!((nan.map(f64::is_nan), nans), (Some(true), 3));
    assert_eq!(groups[2], (None, 2));
    assert_eq!(groups[3], (Some(1.5), 1));
}

#[test]
fn single_phase_group_less_aggregate_on_empty_input_publishes_one_row() {
    let catalog = extrema_catalog();
    let calls = Calls::extrema(&catalog);
    let mut only = FragmentBuilder::new(SOURCE);
    let values_node = only.reserve_node_id().unwrap();
    let columns = values(&mut only, values_node, &[int64(true), int64(true)], &[]);
    let (node, _) = add_aggregate(
        &mut only,
        values_node,
        &[],
        &calls.update(columns[1], false),
        AggregateGrouping::Complete,
    );
    let only = finish(
        only,
        node,
        FragmentSink::Result,
        u32::try_from(DOP).unwrap(),
    );
    let port_output = only.nodes()[&node].output.clone();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(only).unwrap();
    let types = calls.all().map(Bound::result_type).collect::<Vec<_>>();
    plan.set_result_port(result_port(SOURCE, port_output, &types))
        .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the group-less plan validates: {error:?}"));
    let chunks = run_single(&plan, &catalog);
    assert_eq!(int64_rows(&chunks), [vec![Some(0), Some(0), None, None]]);
}

/// `SUM(v)` per group as the exact i128 sum of non-NULL values, NULL when the
/// group has none.
fn sum_oracle(rows: &[Row]) -> BTreeMap<Option<i64>, Option<i128>> {
    let mut groups = BTreeMap::<Option<i64>, Option<i128>>::new();
    for (k, v) in rows {
        let group = groups.entry(*k).or_insert(None);
        if let Some(v) = v {
            *group = Some(group.unwrap_or(0) + i128::from(*v));
        }
    }
    groups
}

/// Rows whose `v` values hash to both partial instances, so each group's
/// exact sum crosses instances, drivers and the Partial/Final split.
const SUM_ROWS: [Row; 12] = [
    (Some(1), Some(i64::MAX)),
    (Some(1), Some(1)),
    (Some(1), Some(-1)),
    (Some(2), Some(i64::MIN)),
    (Some(2), Some(-3)),
    (Some(2), Some(3)),
    (None, Some(7)),
    (None, None),
    (None, Some(-2)),
    (Some(3), None),
    (Some(4), Some(40)),
    (Some(4), Some(2)),
];

fn sum_catalog() -> novarocks_functions::PureEngineFunctionCatalog {
    super::aggregate_fixture::aggregate_catalog(&[(
        "sum",
        novarocks_functions::PureKernelAbi::AggregateWindowV1,
    )])
}

#[test]
fn two_phase_bigint_sum_is_exact_across_instances_drivers_and_phases() {
    let catalog = sum_catalog();
    let calls = Calls::sum(&catalog);
    // The partial state is the exact DECIMAL(38, 0) intermediate.
    assert_eq!(
        calls.0[0].0.intermediate_type(),
        FunctionValueType::new(DataType::Decimal128(38, 0), true)
    );
    let run = try_run_two_phase(&catalog, &calls, &SUM_ROWS, true).unwrap();
    assert!(run.partial_inputs.iter().all(|rows| *rows > 0));
    let mut actual = BTreeMap::new();
    for row in run.results.iter().flatten() {
        assert!(actual.insert(row[0], row[1].map(i128::from)).is_none());
    }
    let expected = sum_oracle(&SUM_ROWS);
    assert_eq!(actual, expected);
    // A running BIGINT add would overflow on key 1 or key 2 in some
    // arrival order; the exact sums are representable.
    assert_eq!(actual[&Some(1)], Some(i128::from(i64::MAX)));
    assert_eq!(actual[&Some(2)], Some(i128::from(i64::MIN)));
    assert_eq!(actual[&Some(3)], None);
}

#[test]
fn two_phase_bigint_sum_overflow_fails_only_the_final_value() {
    let catalog = sum_catalog();
    let calls = Calls::sum(&catalog);
    let rows = [
        (Some(1), Some(i64::MAX)),
        (Some(1), Some(1)),
        (Some(2), Some(5)),
    ];
    // Source and partial fragments succeed; the Final owning key 1 reports
    // the overflow when it builds the BIGINT result.
    let error = match try_run_two_phase(&catalog, &calls, &rows, true) {
        Ok(_) => panic!("SUM over BIGINT range must fail"),
        Err(error) => error,
    };
    assert!(error.contains("sum result overflows BIGINT"), "{error}");
    // Without the overflowing group every value is exact.
    let rows = [
        (Some(1), Some(i64::MAX)),
        (Some(1), Some(1)),
        (Some(1), Some(-2)),
    ];
    let run = try_run_two_phase(&catalog, &calls, &rows, true).unwrap();
    let published = run.results.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(published, [vec![Some(1), Some(i64::MAX - 1)]]);
}

#[test]
fn distinct_aggregate_the_owner_does_not_implement_is_an_explicit_compile_refusal() {
    let catalog = extrema_catalog();
    let count = bind(&catalog, "count", &[int64(true)]);
    let mut only = FragmentBuilder::new(SOURCE);
    let values_node = only.reserve_node_id().unwrap();
    let columns = values(
        &mut only,
        values_node,
        &[int64(true), int64(true)],
        &literal_rows(&ROWS),
    );
    let (node, _) = add_aggregate(
        &mut only,
        values_node,
        &columns[..1],
        &[CallSpec {
            bound: &count,
            phase: AggregatePhase::Single,
            id: AggregateCallId::new(1),
            arguments: vec![columns[1]],
            distinct: true,
        }],
        AggregateGrouping::Complete,
    );
    let only = finish(only, node, FragmentSink::Result, 1);
    let port_output = only.nodes()[&node].output.clone();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(only).unwrap();
    plan.set_result_port(result_port(
        SOURCE,
        port_output,
        &[int64(true), count.result_type()],
    ))
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the DISTINCT plan validates: {error:?}"));
    let mut packages = packages(&plan, &catalog);
    // COUNT(DISTINCT) has no pure lifecycle here: the frozen preparation
    // refuses it, never a fallback.
    let error =
        super::aggregate_fixture::try_compile(packages.remove(&SOURCE).unwrap(), &catalog, 1, true)
            .expect_err("COUNT(DISTINCT) is refused");
    assert!(error.contains("DISTINCT"), "{error}");
}

// A Project that publishes no column (as under `COUNT(*)` above a join's
// selection) still carries its input's row count.
#[test]
fn zero_width_project_carries_its_row_count_into_count_star() {
    let catalog = extrema_catalog();
    let count = bind(&catalog, "count", &[]);
    let mut only = FragmentBuilder::new(SOURCE);
    let values_node = only.reserve_node_id().unwrap();
    let rows = (1..=3)
        .map(|v| vec![LiteralValue::Int64(v)])
        .collect::<Vec<_>>();
    values(&mut only, values_node, &[int64(false)], &rows);
    let project = only.reserve_node_id().unwrap();
    only.add_project(project, values_node, Box::default(), Box::default())
        .unwrap();
    let (node, _) = add_aggregate(
        &mut only,
        project,
        &[],
        &[CallSpec {
            bound: &count,
            phase: AggregatePhase::Single,
            id: AggregateCallId::new(1),
            arguments: Vec::new(),
            distinct: false,
        }],
        AggregateGrouping::Complete,
    );
    let only = finish(
        only,
        node,
        FragmentSink::Result,
        u32::try_from(DOP).unwrap(),
    );
    let port_output = only.nodes()[&node].output.clone();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(only).unwrap();
    plan.set_result_port(result_port(SOURCE, port_output, &[count.result_type()]))
        .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the zero-width project plan validates: {error:?}"));
    let chunks = run_single(&plan, &catalog);
    assert_eq!(int64_rows(&chunks), [vec![Some(3)]]);
}

/// A singleton provider work unit may be assigned to only one of several
/// instances. Empty siblings still send their initial aggregate states and EOS.
#[test]
fn compiled_scan_partials_merge_one_unit_and_empty_instances_at_dop_one_and_two() {
    use super::aggregate_fixture::{
        aggregate_catalog, compile_with_providers, packages_with_reads, try_run_with_scans,
    };
    use crate::exec::pipeline::binding::ScanBindings;
    use crate::runtime::fragment::scan::compiled_fixture::{
        FixtureScanOp, add_typed_scan, providers, scan_chunk,
    };
    use novarocks_connector_contract::FrozenConnectorRead;
    use novarocks_physical_plan::ProviderReadOccurrenceId;
    let catalog = aggregate_catalog(&[
        (
            "count",
            novarocks_functions::PureKernelAbi::AggregateWindowV1,
        ),
        ("sum", novarocks_functions::PureKernelAbi::AggregateWindowV1),
    ]);
    let calls = Calls(vec![
        (bind(&catalog, "count", &[]), false),
        (bind(&catalog, "sum", &[int64(false)]), true),
    ]);
    for dop in [1, 2] {
        for has_unit in [true, false] {
            let mut partial = FragmentBuilder::new(PARTIAL);
            let scan = partial.reserve_node_id().unwrap();
            let (columns, read): (_, FrozenConnectorRead) =
                add_typed_scan(&mut partial, scan, &[int64(false), int64(false)], vec![]);
            let (aggregate, states) = add_aggregate(
                &mut partial,
                scan,
                &[],
                &calls.update(columns[1], true),
                AggregateGrouping::Partial,
            );
            let partial = finish(
                partial,
                aggregate,
                FragmentSink::Stream { edge: TO_FINAL },
                dop,
            );
            let mut last = FragmentBuilder::new(FINAL);
            let receiver = last.reserve_node_id().unwrap();
            let sent = states
                .iter()
                .zip(calls.all())
                .map(|(state, call)| (*state, call.intermediate_type()))
                .collect::<Vec<_>>();
            let inputs = receive(&mut last, receiver, TO_FINAL, &sent, |_| {
                Distribution::Singleton
            });
            let (aggregate, _) = add_aggregate(
                &mut last,
                receiver,
                &[],
                &calls.merge(&inputs),
                AggregateGrouping::Complete,
            );
            let last = finish(last, aggregate, FragmentSink::Result, dop);
            let output = last.nodes()[&aggregate].output.clone();
            let mut plan = PlanBuilder::new(version());
            plan.add_fragment(partial).unwrap();
            plan.add_fragment(last).unwrap();
            plan.add_edge(edge(
                TO_FINAL,
                (PARTIAL, &states),
                (FINAL, receiver, &inputs),
                Distribution::Singleton,
                Distribution::Singleton,
            ))
            .unwrap();
            plan.set_result_port(result_port(
                FINAL,
                output,
                &calls.all().map(Bound::result_type).collect::<Vec<_>>(),
            ))
            .unwrap();
            let plan = plan
                .finish_observed(&FixtureControl)
                .expect("scan aggregate plan");
            let mut packages = packages_with_reads(
                &plan,
                &catalog,
                &BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]),
            );
            let partial = compile_with_providers(
                packages.remove(&PARTIAL).unwrap(),
                &catalog,
                dop as usize,
                false,
                &providers(),
            );
            let last = compile(
                packages.remove(&FINAL).unwrap(),
                &catalog,
                dop as usize,
                true,
            );
            let fixture = Fixture::new();
            let port = fixture.port.clone();
            let transmitter = LoopbackTransmitter::new(fixture.transport.clone());
            let final_instance = FINAL_FINSTS[0];
            let bindings = register(&fixture, &last, final_instance, &PARTIAL_FINSTS, &port);
            let keys = bindings
                .node_ids()
                .map(
                    |node| crate::runtime::fragment::io::exchange_receiver::ExchangeReceiverKey {
                        fragment_instance_id: final_instance,
                        node_id: node,
                    },
                )
                .collect::<Vec<_>>();
            let mut scans = Vec::new();
            for (ordinal, instance) in PARTIAL_FINSTS.into_iter().enumerate() {
                let chunks = if has_unit && ordinal == 0 {
                    vec![scan_chunk(&partial, &[2, 3], &[2, 3])]
                } else {
                    vec![]
                };
                let op = FixtureScanOp::new(chunks, false);
                let mut scan_bindings = ScanBindings::default();
                scan_bindings.insert(
                    i32::try_from(partial.scan_inputs().values().next().unwrap().scan_node)
                        .unwrap(),
                    op.clone(),
                );
                let sink = stream_sink(
                    &fixture,
                    &partial,
                    instance,
                    vec![destination(
                        final_instance,
                        instance,
                        ordinal as u32,
                        PARTIAL_FINSTS.len() as u32,
                    )],
                    &transmitter,
                );
                try_run_with_scans(
                    &fixture,
                    &partial,
                    sink,
                    ExchangeBindings::default(),
                    scan_bindings,
                    instance,
                )
                .expect("each partial instance joins after its scan closes");
                assert_eq!(op.claims(), 1, "each instance claims its own source once");
                assert_eq!(op.closes(), 1, "each source closes before successful join");
                assert_eq!(
                    op.terminations(),
                    0,
                    "normal EOS closes the stream without canceling its source"
                );
                scans.push(op);
            }
            for key in &keys {
                assert_eq!(
                    port.snapshot(*key).unwrap().finished_senders,
                    PARTIAL_FINSTS.len(),
                    "the empty sibling's EOS participates too"
                );
            }
            let output = ResultSinkHandle::new();
            try_run_in_fixture(
                &fixture,
                &last,
                result_sink(&last, final_instance, &output),
                bindings,
                final_instance,
            )
            .expect("final instance joins after all senders end");
            let rows = int64_rows(&output.take_chunks());
            assert_eq!(
                rows,
                if has_unit {
                    vec![vec![Some(2), Some(5)]]
                } else {
                    vec![vec![Some(0), None]]
                },
                "dop={dop}, has_unit={has_unit}"
            );
            assert_eq!(scans.len(), 2, "both independent partial instances ran");
            for key in keys {
                port.remove(key);
                assert!(port.snapshot(key).is_none());
            }
        }
    }
}
