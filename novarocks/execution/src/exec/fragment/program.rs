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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::exec::chunk::ChunkSchemaRef;
#[cfg(test)]
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
#[cfg(test)]
use crate::exec::fragment::sink::FragmentSinkProgram;
pub use novarocks_execution_contract::{FragmentContractVersion, FragmentNodeId, FragmentSinkKind};
pub use novarocks_local_program::{
    CompileProfile, FragmentProgramOptions, FragmentSinkAssignmentKind,
    FragmentSinkAssignmentRequirement, RuntimeFilterContract, RuntimeFilterId, ScanAssignmentKind,
    ScanSourceContract,
};
use novarocks_local_program::{LocalProgram, StaticSinkProgram};

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

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct FragmentSinkSpec {
    program: FragmentSinkProgram,
    kind: FragmentSinkKind,
    assignment_requirement: FragmentSinkAssignmentRequirement,
}

#[cfg(test)]
impl FragmentSinkSpec {
    pub fn try_new(program: FragmentSinkProgram) -> Result<Self, FragmentBindingError> {
        use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
        use FragmentSinkAssignmentRequirement::{None, Required};

        program.validate().map_err(static_sink_binding_error)?;
        let (kind, assignment_requirement) = match &program {
            FragmentSinkProgram::Result => (FragmentSinkKind::Result, None),
            FragmentSinkProgram::Noop => (FragmentSinkKind::Noop, None),
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

    pub fn into_program(self) -> FragmentSinkProgram {
        self.program
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

#[cfg(test)]
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

pub struct FragmentProgram {
    root_plan_node_id: FragmentNodeId,
    local_program: Arc<LocalProgram>,
    sink_kind: FragmentSinkKind,
    sink_assignment_requirement: FragmentSinkAssignmentRequirement,
    program_options: FragmentProgramOptions,
    scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
    exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
    runtime_filters: RuntimeFilterContract,
}

impl std::fmt::Debug for FragmentProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FragmentProgram")
            .field("root_plan_node_id", &self.root_plan_node_id)
            .field("local_program", &self.local_program)
            .field("sink_kind", &self.sink_kind)
            .field("program_options", &self.program_options)
            .finish_non_exhaustive()
    }
}

impl FragmentProgram {
    pub fn try_new(
        local_program: Arc<LocalProgram>,
        program_options: FragmentProgramOptions,
        scan_sources: BTreeMap<FragmentNodeId, ScanSourceContract>,
        exchange_inputs: BTreeMap<FragmentNodeId, ExchangeInputContract>,
        runtime_filters: RuntimeFilterContract,
    ) -> Result<Self, FragmentBindingError> {
        let sink = local_program.sink().ok_or_else(|| {
            FragmentBindingError::new(
                FragmentBindingTarget::Sink,
                FragmentBindingErrorKind::InvalidAssignment,
                "local fragment program requires a static sink",
            )
        })?;
        let (sink_kind, sink_assignment_requirement) = static_sink_spec(sink)?;
        let root_plan_node_id = FragmentNodeId::new(
            local_program.nodes()[local_program.root().index()].native_node_id(),
        );
        if root_plan_node_id.get() < 0 {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Program,
                FragmentBindingErrorKind::InvalidAssignment,
                "fragment root requires a non-negative node id",
            ));
        }
        Ok(Self {
            root_plan_node_id,
            local_program,
            sink_kind,
            sink_assignment_requirement,
            program_options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        })
    }

    pub const fn root_plan_node_id(&self) -> FragmentNodeId {
        self.root_plan_node_id
    }

    pub fn local_program(&self) -> &Arc<LocalProgram> {
        &self.local_program
    }

    pub const fn sink_kind(&self) -> FragmentSinkKind {
        self.sink_kind
    }

    pub const fn sink_assignment_requirement(&self) -> FragmentSinkAssignmentRequirement {
        self.sink_assignment_requirement
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

fn static_sink_spec(
    sink: &StaticSinkProgram,
) -> Result<(FragmentSinkKind, FragmentSinkAssignmentRequirement), FragmentBindingError> {
    use FragmentSinkAssignmentKind::{DestinationGroups, StreamDestinations};
    use FragmentSinkAssignmentRequirement::{None, Required};

    let spec = match sink {
        StaticSinkProgram::Result => (FragmentSinkKind::Result, None),
        StaticSinkProgram::Noop => (FragmentSinkKind::Noop, None),
        StaticSinkProgram::DataStream { .. } => {
            (FragmentSinkKind::DataStream, Required(StreamDestinations))
        }
        StaticSinkProgram::MultiCastDataStream { branches, .. } => (
            FragmentSinkKind::MultiCastDataStream,
            Required(DestinationGroups(non_empty_group_count(
                FragmentSinkKind::MultiCastDataStream,
                branches.len(),
            )?)),
        ),
        StaticSinkProgram::SplitDataStream { branches, .. } => (
            FragmentSinkKind::SplitDataStream,
            Required(DestinationGroups(non_empty_group_count(
                FragmentSinkKind::SplitDataStream,
                branches.len(),
            )?)),
        ),
    };
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram,
    };
    use crate::exec::node::filter::FilterNode;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan, ExternalSinkRequirement};
    use novarocks_types::SlotId;

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
        let mut arena = ExprArena::default();
        arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            arrow::datatypes::DataType::Boolean,
        );
        ExecPlan {
            arena,
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

    fn frozen(plan: ExecPlan, sink: StaticSinkProgram) -> Arc<LocalProgram> {
        let profile = plan
            .local_compile_profile(NonZeroUsize::new(1).unwrap(), None)
            .unwrap();
        let requirements = if matches!(sink, StaticSinkProgram::Result) {
            vec![ExternalSinkRequirement::Result]
        } else {
            Vec::new()
        };
        let (program, runtime) = plan
            .into_local_program_and_bindings(profile, BTreeMap::new(), requirements, sink)
            .unwrap();
        assert_eq!(runtime.scan_count(), 0);
        Arc::new(program)
    }

    #[test]
    fn program_preserves_root_plan_node_id_instead_of_minimum_operator_id() {
        let program = FragmentProgram::try_new(
            frozen(root_not_minimum_node_id_plan(), StaticSinkProgram::Noop),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::default(),
        )
        .unwrap();

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
        let program = FragmentProgram::try_new(
            frozen(values_plan(), StaticSinkProgram::Result),
            options,
            scan_sources,
            exchange_inputs,
            runtime_filters,
        )
        .unwrap();

        assert!(matches!(
            program.local_program().nodes()[program.local_program().root().index()].kind(),
            novarocks_local_program::ProgramNodeKind::Values { .. }
        ));
        assert_eq!(program.sink_kind(), FragmentSinkKind::Result);
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
