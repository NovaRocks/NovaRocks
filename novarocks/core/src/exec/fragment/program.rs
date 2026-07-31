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

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::fragment::error::{
    ExecPlanBuildError, ExecPlanInvariant, FragmentBindingError, FragmentBindingErrorKind,
    FragmentBindingTarget,
};
use crate::exec::fragment::sink::FragmentSinkProgram;
use crate::exec::node::{ExecNodeKind, ExecPlan};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentContractVersion(u16);

impl FragmentContractVersion {
    pub const CURRENT: Self = Self(1);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FragmentNodeId(i32);

impl FragmentNodeId {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeFilterId(i32);

impl RuntimeFilterId {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FragmentProgramOptions {
    contract_version: FragmentContractVersion,
}

impl FragmentProgramOptions {
    pub const fn new(contract_version: FragmentContractVersion) -> Self {
        Self { contract_version }
    }

    pub const fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanAssignmentKind {
    File,
    BrokerFile,
    SchemaSelection,
    StarRocksTablet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanSourceContract {
    assignment_kind: ScanAssignmentKind,
}

impl ScanSourceContract {
    pub const fn new(assignment_kind: ScanAssignmentKind) -> Self {
        Self { assignment_kind }
    }

    pub const fn assignment_kind(&self) -> ScanAssignmentKind {
        self.assignment_kind
    }
}

#[derive(Clone, Debug)]
pub struct ExchangeInputContract {
    expected_schema: ChunkSchemaRef,
}

impl ExchangeInputContract {
    pub fn new(expected_schema: ChunkSchemaRef) -> Self {
        Self { expected_schema }
    }

    pub fn expected_schema(&self) -> &ChunkSchemaRef {
        &self.expected_schema
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeFilterContract {
    build_filters: BTreeSet<RuntimeFilterId>,
    probe_filters: BTreeSet<RuntimeFilterId>,
}

impl RuntimeFilterContract {
    pub fn new(
        build_filters: BTreeSet<RuntimeFilterId>,
        probe_filters: BTreeSet<RuntimeFilterId>,
    ) -> Self {
        Self {
            build_filters,
            probe_filters,
        }
    }

    pub fn build_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.build_filters
    }

    pub fn probe_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.probe_filters
    }

    pub fn has_bindings(&self) -> bool {
        !self.build_filters.is_empty() || !self.probe_filters.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkKind {
    Result,
    Noop,
    Statistics,
    DataStream,
    MultiCastDataStream,
    SplitDataStream,
    StarRocksTable,
    ConnectorWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkAssignmentKind {
    StreamDestinations,
    DestinationGroups(NonZeroUsize),
    StarRocksTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkAssignmentRequirement {
    None,
    Required(FragmentSinkAssignmentKind),
}

#[derive(Clone, Debug)]
pub struct FragmentSinkSpec {
    program: FragmentSinkProgram,
    kind: FragmentSinkKind,
    assignment_requirement: FragmentSinkAssignmentRequirement,
}

impl FragmentSinkSpec {
    pub fn try_new(program: FragmentSinkProgram) -> Result<Self, FragmentBindingError> {
        use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
        use FragmentSinkAssignmentRequirement::{None, Required};

        program.validate().map_err(static_sink_binding_error)?;
        let (kind, assignment_requirement) = match &program {
            FragmentSinkProgram::Result => (FragmentSinkKind::Result, None),
            FragmentSinkProgram::Noop => (FragmentSinkKind::Noop, None),
            FragmentSinkProgram::Statistics(_) => (FragmentSinkKind::Statistics, None),
            FragmentSinkProgram::DataStream(_) => {
                (FragmentSinkKind::DataStream, Required(StreamDestinations))
            }
            FragmentSinkProgram::MultiCastDataStream(grouped) => {
                let count = non_empty_group_count(
                    FragmentSinkKind::MultiCastDataStream,
                    grouped.sinks().len(),
                )?;
                (
                    FragmentSinkKind::MultiCastDataStream,
                    Required(DestinationGroups(count)),
                )
            }
            FragmentSinkProgram::SplitDataStream(split) => {
                let count =
                    non_empty_group_count(FragmentSinkKind::SplitDataStream, split.sinks().len())?;
                (
                    FragmentSinkKind::SplitDataStream,
                    Required(DestinationGroups(count)),
                )
            }
            FragmentSinkProgram::StarRocksTable(_) => (
                FragmentSinkKind::StarRocksTable,
                Required(FragmentSinkAssignmentKind::StarRocksTable),
            ),
            FragmentSinkProgram::ConnectorWrite(_) => (FragmentSinkKind::ConnectorWrite, None),
        };
        Ok(Self {
            program,
            kind,
            assignment_requirement,
        })
    }

    pub const fn program(&self) -> &FragmentSinkProgram {
        &self.program
    }

    pub fn program_mut(&mut self) -> &mut FragmentSinkProgram {
        &mut self.program
    }

    pub const fn kind(&self) -> FragmentSinkKind {
        self.kind
    }

    pub const fn assignment_requirement(&self) -> FragmentSinkAssignmentRequirement {
        self.assignment_requirement
    }
}

fn static_sink_binding_error(error: ExecPlanBuildError) -> FragmentBindingError {
    let kind = match error.invariant() {
        ExecPlanInvariant::Expression => FragmentBindingErrorKind::ExpressionMismatch,
        _ => FragmentBindingErrorKind::InvalidAssignment,
    };
    FragmentBindingError::new(FragmentBindingTarget::Sink, kind, error.detail())
}

fn non_empty_group_count(
    kind: FragmentSinkKind,
    count: usize,
) -> Result<NonZeroUsize, FragmentBindingError> {
    NonZeroUsize::new(count).ok_or_else(|| {
        FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::InvalidAssignment,
            format!("sink {kind:?} requires at least one static branch"),
        )
    })
}

#[derive(Debug)]
pub struct FragmentProgram {
    root_plan_node_id: FragmentNodeId,
    plan: ExecPlan,
    sink: FragmentSinkSpec,
    program_options: FragmentProgramOptions,
    scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
    exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
    runtime_filters: RuntimeFilterContract,
}

impl FragmentProgram {
    pub fn new(
        plan: ExecPlan,
        sink: FragmentSinkSpec,
        program_options: FragmentProgramOptions,
        scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
        exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
        runtime_filters: RuntimeFilterContract,
    ) -> Self {
        let root_plan_node_id = FragmentNodeId::new(root_plan_node_id(&plan));
        Self {
            root_plan_node_id,
            plan,
            sink,
            program_options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        }
    }

    pub const fn root_plan_node_id(&self) -> FragmentNodeId {
        self.root_plan_node_id
    }

    pub fn plan(&self) -> &ExecPlan {
        &self.plan
    }

    pub fn plan_mut(&mut self) -> &mut ExecPlan {
        &mut self.plan
    }

    pub const fn sink(&self) -> &FragmentSinkSpec {
        &self.sink
    }

    pub fn sink_mut(&mut self) -> &mut FragmentSinkSpec {
        &mut self.sink
    }

    pub const fn program_options(&self) -> &FragmentProgramOptions {
        &self.program_options
    }

    pub fn scan_sources(&self) -> &BTreeMap<FragmentNodeId, ScanSourceContract> {
        &self.scan_sources
    }

    pub fn exchange_inputs(&self) -> &BTreeMap<FragmentNodeId, ExchangeInputContract> {
        &self.exchange_inputs
    }

    pub const fn runtime_filters(&self) -> &RuntimeFilterContract {
        &self.runtime_filters
    }
}

fn root_plan_node_id(plan: &ExecPlan) -> i32 {
    match &plan.root.kind {
        ExecNodeKind::AssertNumRows(node) => node.node_id,
        ExecNodeKind::Values(node) => node.node_id,
        ExecNodeKind::Project(node) => node.node_id,
        ExecNodeKind::Filter(node) => node.node_id,
        ExecNodeKind::Repeat(node) => node.node_id,
        ExecNodeKind::ChangeEventExpand(node) => node.node_id,
        ExecNodeKind::UnionAll(node) => node.node_id,
        ExecNodeKind::Limit(node) => node.node_id,
        ExecNodeKind::ExchangeSource(node) => node.node_id,
        ExecNodeKind::Scan(node) => node.node_id().unwrap_or(-1),
        ExecNodeKind::Fetch(node) => node.node_id,
        ExecNodeKind::LookUp(node) => node.node_id,
        ExecNodeKind::Aggregate(node) => node.node_id,
        ExecNodeKind::Join(node) => node.node_id,
        ExecNodeKind::NestedLoopJoin(node) => node.node_id,
        ExecNodeKind::Sort(node) => node.node_id,
        ExecNodeKind::TableFunction(node) => node.node_id,
        ExecNodeKind::Analytic(node) => node.node_id,
        ExecNodeKind::SetOp(node) => node.node_id,
        ExecNodeKind::RuntimeFilterConsumer(node) => node.owner_node_id,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprId};
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram,
    };
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::DataStreamPartitionType;

    use super::*;

    fn values_plan() -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Values(ValuesNode {
                    chunk: Chunk::default(),
                    node_id: 7,
                }),
            },
        }
    }

    fn root_not_minimum_node_id_plan() -> ExecPlan {
        ExecPlan {
            arena: ExprArena::default(),
            root: ExecNode {
                kind: ExecNodeKind::Filter(FilterNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::default(),
                            node_id: 3,
                        }),
                    }),
                    node_id: 99,
                    predicate: ExprId(0),
                }),
            },
        }
    }

    #[test]
    fn program_preserves_root_plan_node_id_instead_of_minimum_operator_id() {
        let program = FragmentProgram::new(
            root_not_minimum_node_id_plan(),
            FragmentSinkSpec::try_new(FragmentSinkProgram::Noop).expect("noop sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::default(),
        );

        assert_eq!(program.root_plan_node_id(), FragmentNodeId::new(99));
    }

    #[test]
    fn stable_ids_are_typed_ordered_keys() {
        assert_eq!(FragmentContractVersion::CURRENT.get(), 1);
        assert_eq!(FragmentContractVersion::new(9).get(), 9);
        assert_eq!(FragmentNodeId::new(11).get(), 11);
        assert_eq!(RuntimeFilterId::new(11).get(), 11);

        let nodes = BTreeMap::from([(FragmentNodeId::new(3), "scan")]);
        assert_eq!(nodes.get(&FragmentNodeId::new(3)), Some(&"scan"));
        let filters = BTreeSet::from([RuntimeFilterId::new(5), RuntimeFilterId::new(2)]);
        assert_eq!(
            filters.iter().map(|id| id.get()).collect::<Vec<_>>(),
            vec![2, 5]
        );
    }

    #[test]
    fn sink_assignment_requirement_is_derived_from_static_program() {
        use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
        use FragmentSinkAssignmentRequirement::{None, Required};

        let stream = DataStreamSinkProgram::try_new(
            9,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(1)],
            Option::None,
            ExprArena::default(),
        )
        .expect("data stream program");
        assert_eq!(
            FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(stream))
                .expect("data stream sink")
                .assignment_requirement(),
            Required(StreamDestinations)
        );
        for program in [FragmentSinkProgram::Result, FragmentSinkProgram::Noop] {
            assert_eq!(
                FragmentSinkSpec::try_new(program)
                    .expect("non-grouped sink")
                    .assignment_requirement(),
                None
            );
        }
        let branch = || {
            DataStreamSinkBranchProgram::try_new(
                9,
                Vec::new(),
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                vec![SlotId::new(1)],
                Option::None,
            )
            .expect("data stream branch")
        };
        let grouped = FragmentSinkSpec::try_new(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(vec![branch(), branch()], ExprArena::default())
                .expect("grouped stream program"),
        ))
        .expect("grouped sink");
        assert_eq!(
            grouped.assignment_requirement(),
            Required(DestinationGroups(
                std::num::NonZeroUsize::new(2).expect("non-zero group count")
            ))
        );

        let error = MultiCastDataStreamSinkProgram::try_new(Vec::new(), ExprArena::default())
            .expect_err("empty grouped sink is invalid at static build time");
        assert_eq!(error.invariant(), ExecPlanInvariant::Sink);
    }

    #[test]
    fn program_exposes_immutable_static_contracts() {
        let scan_sources = BTreeMap::from([(
            FragmentNodeId::new(10),
            ScanSourceContract::new(ScanAssignmentKind::File),
        )]);
        let expected_schema = Arc::new(ChunkSchema::empty());
        let exchange_inputs = BTreeMap::from([(
            FragmentNodeId::new(20),
            ExchangeInputContract::new(Arc::clone(&expected_schema)),
        )]);
        let runtime_filters = RuntimeFilterContract::new(
            BTreeSet::from([RuntimeFilterId::new(30)]),
            BTreeSet::from([RuntimeFilterId::new(31)]),
        );
        let options = FragmentProgramOptions::new(FragmentContractVersion::CURRENT);
        let program = FragmentProgram::new(
            values_plan(),
            FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink"),
            options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        );

        assert!(matches!(program.plan().root.kind, ExecNodeKind::Values(_)));
        assert_eq!(program.sink().kind(), FragmentSinkKind::Result);
        assert_eq!(
            program.program_options().contract_version(),
            FragmentContractVersion::CURRENT
        );
        assert_eq!(
            program
                .scan_sources()
                .get(&FragmentNodeId::new(10))
                .map(ScanSourceContract::assignment_kind),
            Some(ScanAssignmentKind::File)
        );
        assert!(Arc::ptr_eq(
            program
                .exchange_inputs()
                .get(&FragmentNodeId::new(20))
                .expect("exchange contract")
                .expected_schema(),
            &expected_schema
        ));
        assert_eq!(
            program
                .runtime_filters()
                .build_filters()
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![30]
        );
        assert_eq!(
            program
                .runtime_filters()
                .probe_filters()
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            vec![31]
        );
        assert!(program.runtime_filters().has_bindings());
    }
}
