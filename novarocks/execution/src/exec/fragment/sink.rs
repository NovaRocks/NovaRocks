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

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::runtime::endpoint::FragmentDestination;
pub use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;

#[derive(Clone, Debug)]
pub enum FragmentSinkProgram {
    Result,
    Noop,
    DataStream(DataStreamSinkProgram),
    MultiCastDataStream(MultiCastDataStreamSinkProgram),
    SplitDataStream(SplitDataStreamSinkProgram),
}

impl FragmentSinkProgram {
    pub fn validate(&self) -> Result<(), ExecPlanBuildError> {
        match self {
            Self::Result | Self::Noop => Ok(()),
            Self::DataStream(program) => program.validate(),
            Self::MultiCastDataStream(program) => program.validate(),
            Self::SplitDataStream(program) => program.validate(),
        }
    }
}

/// Construction-time input for a distributed stream sink. The private
/// operator factories consume this value but do not define the public
/// fragment-construction contract.
#[derive(Clone)]
pub struct DataStreamSinkFactoryInput {
    pub dest_node_id: i32,
    pub output_exprs: Vec<ExprId>,
    pub output_partition_type: DataStreamPartitionType,
    pub output_partition_exprs: Vec<ExprId>,
    pub output_columns: Vec<SlotId>,
    pub destinations: Vec<FragmentDestination>,
}

impl DataStreamSinkFactoryInput {
    pub fn try_from_static_program(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        output_exprs: Vec<ExprId>,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        destinations: Vec<FragmentDestination>,
    ) -> Result<Self, String> {
        if !output_exprs.is_empty() {
            return Err("DATA_STREAM_SINK output_exprs are not supported".to_string());
        }
        let mut seen = HashSet::new();
        if let Some(slot_id) = output_columns
            .iter()
            .find(|slot_id| !seen.insert(**slot_id))
        {
            return Err(format!(
                "DATA_STREAM_SINK: duplicate output_columns slot id: {slot_id}"
            ));
        }
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        Ok(Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            destinations,
        })
    }

    pub fn try_new(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        output_exprs: Vec<ExprId>,
        output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<i32>,
        destinations: Vec<FragmentDestination>,
    ) -> Result<Self, String> {
        let mut seen = HashSet::new();
        let mut parsed_output_columns = Vec::with_capacity(output_columns.len());
        for raw in output_columns {
            let slot_id = SlotId::try_from(raw).map_err(|err| {
                format!("DATA_STREAM_SINK: invalid output_columns slot id: {err}")
            })?;
            if !seen.insert(slot_id) {
                return Err(format!(
                    "DATA_STREAM_SINK: duplicate output_columns slot id: {slot_id}"
                ));
            }
            parsed_output_columns.push(slot_id);
        }
        Self::try_from_static_program(
            dest_node_id,
            output_partition_type,
            output_exprs,
            output_partition_exprs,
            parsed_output_columns,
            destinations,
        )
    }
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "The lowered sink retains the optional limit for protocol compatibility."
)]
pub struct DataStreamSinkProgram {
    dest_node_id: i32,
    output_exprs: Vec<ExprId>,
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: Vec<ExprId>,
    output_columns: Vec<SlotId>,
    limit: Option<i64>,
    partition_arena: ExprArena,
}

impl DataStreamSinkProgram {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
        partition_arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        let program = Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
            partition_arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_stream_shape(
            "DATA_STREAM_SINK",
            &self.output_exprs,
            self.output_partition_type,
            &self.output_partition_exprs,
            &self.output_columns,
        )?;
        validate_expr_ids(
            &self.partition_arena,
            &self.output_partition_exprs,
            "DATA_STREAM_SINK partition",
        )
    }

    pub const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }

    pub fn output_exprs(&self) -> &[ExprId] {
        &self.output_exprs
    }

    pub const fn output_partition_type(&self) -> DataStreamPartitionType {
        self.output_partition_type
    }

    pub fn output_partition_exprs(&self) -> &[ExprId] {
        &self.output_partition_exprs
    }

    pub fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }

    pub const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }

    pub fn partition_arena_mut(&mut self) -> &mut ExprArena {
        &mut self.partition_arena
    }
}

#[derive(Clone, Debug)]
pub struct DataStreamSinkBranchProgram {
    dest_node_id: i32,
    output_exprs: Vec<ExprId>,
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: Vec<ExprId>,
    output_columns: Vec<SlotId>,
    limit: Option<i64>,
}

impl DataStreamSinkBranchProgram {
    pub fn try_new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
    ) -> Result<Self, ExecPlanBuildError> {
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        let program = Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
        };
        program.validate_shape("grouped DATA_STREAM_SINK branch")?;
        Ok(program)
    }

    pub fn into_program(
        self,
        partition_arena: ExprArena,
    ) -> Result<DataStreamSinkProgram, ExecPlanBuildError> {
        DataStreamSinkProgram::try_new(
            self.dest_node_id,
            self.output_exprs,
            self.output_partition_type,
            self.output_partition_exprs,
            self.output_columns,
            self.limit,
            partition_arena,
        )
    }

    fn validate_shape(&self, context: &str) -> Result<(), ExecPlanBuildError> {
        validate_stream_shape(
            context,
            &self.output_exprs,
            self.output_partition_type,
            &self.output_partition_exprs,
            &self.output_columns,
        )
    }

    pub const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }

    pub fn output_exprs(&self) -> &[ExprId] {
        &self.output_exprs
    }

    pub const fn output_partition_type(&self) -> DataStreamPartitionType {
        self.output_partition_type
    }

    pub fn output_partition_exprs(&self) -> &[ExprId] {
        &self.output_partition_exprs
    }

    pub fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }

    pub const fn limit(&self) -> Option<i64> {
        self.limit
    }
}

#[derive(Clone, Debug)]
pub struct MultiCastDataStreamSinkProgram {
    sinks: Vec<DataStreamSinkBranchProgram>,
    partition_arena: ExprArena,
}

impl MultiCastDataStreamSinkProgram {
    pub fn try_new(
        sinks: Vec<DataStreamSinkBranchProgram>,
        partition_arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            sinks,
            partition_arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_non_empty_group("MULTI_CAST_DATA_STREAM_SINK", self.sinks.len())?;
        for (index, sink) in self.sinks.iter().enumerate() {
            sink.validate_shape(&format!("MULTI_CAST_DATA_STREAM_SINK sink[{index}]"))?;
            validate_expr_ids(
                &self.partition_arena,
                sink.output_partition_exprs(),
                &format!("MULTI_CAST_DATA_STREAM_SINK sink[{index}] partition"),
            )?;
        }
        Ok(())
    }

    pub fn sinks(&self) -> &[DataStreamSinkBranchProgram] {
        &self.sinks
    }

    pub const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }

    pub fn partition_arena_mut(&mut self) -> &mut ExprArena {
        &mut self.partition_arena
    }
}

#[derive(Clone, Debug)]
pub struct SplitDataStreamSinkProgram {
    sinks: Vec<DataStreamSinkBranchProgram>,
    split_exprs: Vec<ExprId>,
    arena: ExprArena,
    /// Logical row-mutation routes are filters and can intentionally overlap
    /// (a Replace fans out to delete and replacement-data routes). Ordinary
    /// stream splits retain their historical first-match partition semantics.
    fanout: bool,
}

impl SplitDataStreamSinkProgram {
    pub fn try_new(
        sinks: Vec<DataStreamSinkBranchProgram>,
        split_exprs: Vec<ExprId>,
        arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        Self::try_new_with_fanout(sinks, split_exprs, arena, false)
    }

    pub fn try_new_with_fanout(
        sinks: Vec<DataStreamSinkBranchProgram>,
        split_exprs: Vec<ExprId>,
        arena: ExprArena,
        fanout: bool,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            sinks,
            split_exprs,
            arena,
            fanout,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_non_empty_group("SPLIT_DATA_STREAM_SINK", self.sinks.len())?;
        if self.split_exprs.len() != self.sinks.len() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                format!(
                    "SPLIT_DATA_STREAM_SINK split expression count {} does not match branch count {}",
                    self.split_exprs.len(),
                    self.sinks.len()
                ),
            ));
        }
        validate_expr_ids(
            &self.arena,
            &self.split_exprs,
            "SPLIT_DATA_STREAM_SINK split",
        )?;
        for (index, sink) in self.sinks.iter().enumerate() {
            sink.validate_shape(&format!("SPLIT_DATA_STREAM_SINK sink[{index}]"))?;
            validate_expr_ids(
                &self.arena,
                sink.output_partition_exprs(),
                &format!("SPLIT_DATA_STREAM_SINK sink[{index}] partition"),
            )?;
        }
        Ok(())
    }

    pub fn sinks(&self) -> &[DataStreamSinkBranchProgram] {
        &self.sinks
    }

    pub fn split_exprs(&self) -> &[ExprId] {
        &self.split_exprs
    }

    pub const fn fanout(&self) -> bool {
        self.fanout
    }

    pub const fn arena(&self) -> &ExprArena {
        &self.arena
    }

    pub fn arena_mut(&mut self) -> &mut ExprArena {
        &mut self.arena
    }
}

pub fn build_change_stream_split_predicate(
    arena: &mut ExprArena,
    effect_slot_id: SlotId,
    accepted_effects: &[novarocks_spi::connector::ConnectorRowMutationEffect],
) -> Result<ExprId, ExecPlanBuildError> {
    use crate::exec::expr::{ExprNode, LiteralValue};

    if accepted_effects.is_empty() {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Sink,
            "change-stream route must accept at least one logical effect",
        ));
    }
    let effect = arena.push_typed(ExprNode::SlotId(effect_slot_id), DataType::Int8);
    let mut predicates = Vec::with_capacity(accepted_effects.len());
    for accepted in accepted_effects {
        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Int8(*accepted as i8)),
            DataType::Int8,
        );
        predicates.push(arena.push_typed(ExprNode::Eq(effect, literal), DataType::Boolean));
    }
    let first = predicates.remove(0);
    Ok(predicates.into_iter().fold(first, |predicate, next| {
        arena.push_typed(ExprNode::Or(predicate, next), DataType::Boolean)
    }))
}

fn validate_stream_shape(
    context: &str,
    output_exprs: &[ExprId],
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: &[ExprId],
    output_columns: &[SlotId],
) -> Result<(), ExecPlanBuildError> {
    if !output_exprs.is_empty() {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!("{context} output_exprs are not supported"),
        ));
    }
    if !output_partition_type.requires_exprs() && !output_partition_exprs.is_empty() {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!("{context} non-hash partition type must not retain partition expressions"),
        ));
    }
    let mut seen = HashSet::new();
    if let Some(slot_id) = output_columns
        .iter()
        .find(|slot_id| !seen.insert(**slot_id))
    {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Sink,
            format!("{context} duplicate output column slot id {slot_id}"),
        ));
    }
    Ok(())
}

fn validate_expr_ids(
    arena: &ExprArena,
    exprs: &[ExprId],
    context: &str,
) -> Result<(), ExecPlanBuildError> {
    if let Some(expr_id) = exprs.iter().find(|expr_id| arena.node(**expr_id).is_none()) {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!(
                "{context} expression id {} is missing from its arena",
                expr_id.0
            ),
        ));
    }
    Ok(())
}

fn validate_non_empty_group(context: &str, count: usize) -> Result<(), ExecPlanBuildError> {
    if count == 0 {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Sink,
            format!("{context} requires at least one static branch"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram,
    };
    use crate::exec::expr::{ExprArena, ExprId, ExprNode};
    use crate::exec::fragment::error::ExecPlanInvariant;
    use crate::exec::fragment::program::{
        FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement, FragmentSinkSpec,
    };
    use crate::exec::fragment::sink::DataStreamPartitionType;
    use novarocks_types::SlotId;

    #[test]
    fn static_data_stream_program_contains_no_destinations() {
        let program = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            Some(9),
            ExprArena::default(),
        )
        .expect("valid stream program");

        assert_eq!(program.dest_node_id, 17);
        assert_eq!(program.output_columns, vec![SlotId::new(3)]);
        assert_eq!(program.limit, Some(9));
        assert!(
            program
                .partition_arena()
                .node(crate::exec::expr::ExprId(0))
                .is_none()
        );

        let spec = FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(program))
            .expect("static data stream sink");
        assert_eq!(
            spec.assignment_requirement(),
            FragmentSinkAssignmentRequirement::Required(
                FragmentSinkAssignmentKind::StreamDestinations
            )
        );
    }

    #[test]
    fn data_stream_program_rejects_duplicate_output_columns() {
        let error = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3), SlotId::new(3)],
            None,
            ExprArena::default(),
        )
        .expect_err("duplicate output columns must fail static construction");

        assert_eq!(error.invariant(), ExecPlanInvariant::Sink);
        assert!(error.detail().contains("duplicate output column slot id 3"));
    }

    #[test]
    fn data_stream_program_rejects_all_unsupported_output_exprs() {
        let mut arena = ExprArena::default();
        let valid_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);

        for output_expr in [valid_id, ExprId(99)] {
            let error = DataStreamSinkProgram::try_new(
                17,
                vec![output_expr],
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                vec![SlotId::new(3)],
                None,
                arena.clone(),
            )
            .expect_err("stream output expressions are unsupported");

            assert_eq!(error.invariant(), ExecPlanInvariant::Expression);
            assert!(error.detail().contains("output_exprs are not supported"));
        }
    }

    #[test]
    fn data_stream_partition_exprs_are_normalized_and_arena_checked() {
        let mut arena = ExprArena::default();
        let valid_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);

        let random = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Random,
            vec![ExprId(99)],
            vec![SlotId::new(3)],
            None,
            arena.clone(),
        )
        .expect("non-hash partition expressions are normalized away");
        assert!(random.output_partition_exprs().is_empty());

        let hash = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::HashPartitioned,
            vec![valid_id],
            vec![SlotId::new(3)],
            None,
            arena.clone(),
        )
        .expect("valid hash partition expression");
        assert_eq!(hash.output_partition_exprs(), &[valid_id]);

        let error = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::HashPartitioned,
            vec![ExprId(99)],
            vec![SlotId::new(3)],
            None,
            arena,
        )
        .expect_err("hash partition expression must belong to its arena");
        assert_eq!(error.invariant(), ExecPlanInvariant::Expression);
    }

    #[test]
    fn grouped_stream_programs_validate_partition_exprs_against_group_arena() {
        let branch = || {
            DataStreamSinkBranchProgram::try_new(
                17,
                Vec::new(),
                DataStreamPartitionType::HashPartitioned,
                vec![ExprId(99)],
                vec![SlotId::new(3)],
                None,
            )
            .expect("branch validation is completed by the group arena owner")
        };

        let multicast_error =
            MultiCastDataStreamSinkProgram::try_new(vec![branch()], ExprArena::default())
                .expect_err("multicast partition expression must belong to group arena");
        assert_eq!(multicast_error.invariant(), ExecPlanInvariant::Expression);
    }
}
