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
use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::change_op::ChangeStreamBranchKind;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, cast_with_special_rules};
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::runtime::connector_write_report::ConnectorStagedReportCollector;
use crate::runtime::endpoint::FragmentDestination;
use novarocks_spi::connector::{
    ConnectorExecutionBinding, ConnectorOpenWriterRequest, StatisticsMetricRequest,
};

#[derive(Clone, Debug)]
pub enum FragmentSinkProgram {
    Result,
    Noop,
    Statistics(StatisticsSinkProgram),
    DataStream(DataStreamSinkProgram),
    MultiCastDataStream(MultiCastDataStreamSinkProgram),
    SplitDataStream(SplitDataStreamSinkProgram),
    ConnectorWrite(ConnectorWriteSinkProgram),
}

impl FragmentSinkProgram {
    pub(crate) fn validate(&self) -> Result<(), ExecPlanBuildError> {
        match self {
            Self::Result | Self::Noop | Self::Statistics(_) => Ok(()),
            Self::DataStream(program) => program.validate(),
            Self::MultiCastDataStream(program) => program.validate(),
            Self::SplitDataStream(program) => program.validate(),
            Self::ConnectorWrite(program) => program.validate(),
        }
    }

    pub(crate) fn connector_staged_report_collector(
        &self,
    ) -> Option<ConnectorStagedReportCollector> {
        match self {
            Self::ConnectorWrite(program) => Some(program.report_collector()),
            _ => None,
        }
    }
}

/// Construction-time exchange partitioning contract. The implementation lives
/// in the private operator module, but decoders construct this neutral value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataStreamPartitionType {
    Unpartitioned,
    Random,
    HashPartitioned,
    BucketShuffleHashPartitioned,
}

impl DataStreamPartitionType {
    pub(crate) const fn display_name(self) -> &'static str {
        match self {
            Self::Unpartitioned => "UNPARTITIONED",
            Self::Random => "RANDOM",
            Self::HashPartitioned => "HASH_PARTITIONED",
            Self::BucketShuffleHashPartitioned => "BUCKET_SHUFFLE_HASH_PARTITIONED",
        }
    }

    pub const fn requires_exprs(self) -> bool {
        matches!(
            self,
            Self::HashPartitioned | Self::BucketShuffleHashPartitioned
        )
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

/// Provider-neutral terminal writer.  The program carries an already-resolved
/// BE execution binding and a FE-issued opaque handle; it has no provider
/// control capability and cannot commit external table state.
#[derive(Clone)]
pub struct ConnectorWriteSinkProgram {
    name: String,
    binding: Arc<ConnectorExecutionBinding>,
    request: ConnectorOpenWriterRequest,
    root_input_width: usize,
    input_ordinals: Option<Vec<usize>>,
    input_projection: Option<ConnectorWriteInputProjection>,
    report_collector: ConnectorStagedReportCollector,
}

#[derive(Clone)]
pub(crate) struct ConnectorWriteInputProjection {
    arena: ExprArena,
    exprs: Vec<ExprId>,
    schema: SchemaRef,
}

impl ConnectorWriteInputProjection {
    fn try_new(
        arena: ExprArena,
        exprs: Vec<ExprId>,
        schema: SchemaRef,
    ) -> Result<Self, ExecPlanBuildError> {
        if exprs.is_empty() || exprs.len() != schema.fields().len() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                "connector writer expression projection does not match its output schema",
            ));
        }
        validate_expr_ids(&arena, &exprs, "connector writer input")?;
        Ok(Self {
            arena,
            exprs,
            schema,
        })
    }

    pub(crate) fn project(&self, chunk: &Chunk) -> Result<RecordBatch, String> {
        let arrays = self
            .exprs
            .iter()
            .map(|expr| self.arena.eval(*expr, chunk))
            .collect::<Result<Vec<_>, _>>()?;
        let arrays = arrays
            .into_iter()
            .zip(self.schema.fields())
            .enumerate()
            .map(|(index, (array, field))| {
                if array.data_type() == field.data_type() {
                    return Ok(array);
                }
                let casted = if matches!(
                    field.data_type(),
                    DataType::FixedSizeBinary(width)
                        if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH
                ) {
                    cast_with_special_rules(&array, field.data_type())
                } else {
                    cast(array.as_ref(), field.data_type()).map_err(|error| error.to_string())
                };
                casted.map_err(|error| {
                    format!(
                        "connector writer projection cast failed at column {index} from {:?} to {:?}: {error}",
                        array.data_type(),
                        field.data_type()
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|error| format!("build connector writer projected batch: {error}"))
    }
}

impl std::fmt::Debug for ConnectorWriteSinkProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorWriteSinkProgram")
            .field("owner", self.request.handle.owner())
            .field("writer", self.request.handle.writer())
            .field("input_ordinals", &self.input_ordinals)
            .finish_non_exhaustive()
    }
}

impl ConnectorWriteSinkProgram {
    pub fn try_new(
        binding: Arc<ConnectorExecutionBinding>,
        request: ConnectorOpenWriterRequest,
        root_input_width: usize,
        input_ordinals: Option<Vec<usize>>,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            name: "CONNECTOR_WRITE_SINK".to_string(),
            binding,
            request,
            root_input_width,
            input_ordinals,
            input_projection: None,
            report_collector: ConnectorStagedReportCollector::default(),
        };
        program.validate()?;
        Ok(program)
    }

    pub fn try_new_with_expression_projection(
        binding: Arc<ConnectorExecutionBinding>,
        request: ConnectorOpenWriterRequest,
        root_input_width: usize,
        arena: ExprArena,
        exprs: Vec<ExprId>,
        schema: SchemaRef,
    ) -> Result<Self, ExecPlanBuildError> {
        if request.expected_schema.as_ref() != schema.as_ref() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                "connector writer request schema does not match its expression projection",
            ));
        }
        let program = Self {
            name: "CONNECTOR_WRITE_SINK".to_string(),
            binding,
            request,
            root_input_width,
            input_ordinals: None,
            input_projection: Some(ConnectorWriteInputProjection::try_new(
                arena, exprs, schema,
            )?),
            report_collector: ConnectorStagedReportCollector::default(),
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        self.request.handle.validate().map_err(|error| {
            ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                format!("connector writer handle: {error}"),
            )
        })?;
        if self.binding.key() != self.request.handle.owner() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                "connector writer handle owner does not match resolved execution binding",
            ));
        }
        if self.binding.write().is_none() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                "resolved connector execution binding has no write capability",
            ));
        }
        if let Some(ordinals) = &self.input_ordinals {
            if ordinals.is_empty()
                || ordinals
                    .iter()
                    .any(|ordinal| *ordinal >= self.root_input_width)
            {
                return Err(ExecPlanBuildError::new(
                    ExecPlanInvariant::Sink,
                    "connector writer input ordinals are empty or outside the root output schema",
                ));
            }
            let mut unique = HashSet::new();
            if ordinals.iter().any(|ordinal| !unique.insert(*ordinal)) {
                return Err(ExecPlanBuildError::new(
                    ExecPlanInvariant::Sink,
                    "connector writer input ordinals contain duplicates",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn binding(&self) -> &Arc<ConnectorExecutionBinding> {
        &self.binding
    }

    pub(crate) fn request(&self) -> &ConnectorOpenWriterRequest {
        &self.request
    }

    pub(crate) fn input_ordinals(&self) -> Option<&[usize]> {
        self.input_ordinals.as_deref()
    }

    pub(crate) fn input_projection(&self) -> Option<ConnectorWriteInputProjection> {
        self.input_projection.clone()
    }

    pub fn expression_projection_arena_mut(&mut self) -> Option<&mut ExprArena> {
        self.input_projection
            .as_mut()
            .map(|projection| &mut projection.arena)
    }

    pub(crate) fn report_collector(&self) -> ConnectorStagedReportCollector {
        self.report_collector.clone()
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// Typed metric set for the Core-internal distributed statistics terminal
/// sink. It deliberately contains no client result format or provider handle.
#[derive(Clone, Debug)]
pub struct StatisticsSinkProgram {
    metrics: StatisticsMetricRequest,
}

impl StatisticsSinkProgram {
    pub fn new(metrics: StatisticsMetricRequest) -> Self {
        Self { metrics }
    }

    pub fn metrics(&self) -> &StatisticsMetricRequest {
        &self.metrics
    }
}

#[derive(Clone, Debug)]
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

    pub(crate) const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }

    pub(crate) fn output_exprs(&self) -> &[ExprId] {
        &self.output_exprs
    }

    pub(crate) const fn output_partition_type(&self) -> DataStreamPartitionType {
        self.output_partition_type
    }

    pub fn output_partition_exprs(&self) -> &[ExprId] {
        &self.output_partition_exprs
    }

    pub(crate) fn output_columns(&self) -> &[SlotId] {
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
}

impl SplitDataStreamSinkProgram {
    pub fn try_new(
        sinks: Vec<DataStreamSinkBranchProgram>,
        split_exprs: Vec<ExprId>,
        arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            sinks,
            split_exprs,
            arena,
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

    pub const fn arena(&self) -> &ExprArena {
        &self.arena
    }

    pub fn arena_mut(&mut self) -> &mut ExprArena {
        &mut self.arena
    }
}

pub fn build_change_stream_split_predicate(
    arena: &mut ExprArena,
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
    branch_kind: ChangeStreamBranchKind,
) -> Result<ExprId, ExecPlanBuildError> {
    use crate::exec::expr::{ExprNode, LiteralValue};
    use crate::sql::common::{
        CHANGE_OP_DELETE, CHANGE_OP_INSERT, DATA_ROUTE_FRESH, DATA_ROUTE_REUSE,
    };

    let operation = arena.push_typed(ExprNode::SlotId(change_op_slot_id), DataType::Int8);
    let expected_operation = match branch_kind {
        ChangeStreamBranchKind::DeleteDv => CHANGE_OP_DELETE,
        ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData => CHANGE_OP_INSERT,
    };
    let operation_literal = arena.push_typed(
        ExprNode::Literal(LiteralValue::Int8(expected_operation as i8)),
        DataType::Int8,
    );
    let operation_matches = arena.push_typed(
        ExprNode::Eq(operation, operation_literal),
        DataType::Boolean,
    );

    let route_matches = match branch_kind {
        ChangeStreamBranchKind::DeleteDv => data_route_slot_id.map(|route_slot| {
            let route = arena.push_typed(ExprNode::SlotId(route_slot), DataType::Int32);
            arena.push_typed(ExprNode::IsNull(route), DataType::Boolean)
        }),
        ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData => {
            let route_slot = data_route_slot_id.ok_or_else(|| {
                ExecPlanBuildError::new(
                    ExecPlanInvariant::Sink,
                    "change-stream data branch requires a data route slot",
                )
            })?;
            let route = arena.push_typed(ExprNode::SlotId(route_slot), DataType::Int32);
            let expected_route = match branch_kind {
                ChangeStreamBranchKind::ReuseData => DATA_ROUTE_REUSE,
                ChangeStreamBranchKind::FreshData => DATA_ROUTE_FRESH,
                ChangeStreamBranchKind::DeleteDv => unreachable!(),
            };
            let route_literal = arena.push_typed(
                ExprNode::Literal(LiteralValue::Int32(expected_route)),
                DataType::Int32,
            );
            Some(arena.push_typed(ExprNode::Eq(route, route_literal), DataType::Boolean))
        }
    };

    Ok(match route_matches {
        Some(route_matches) => arena.push_typed(
            ExprNode::And(operation_matches, route_matches),
            DataType::Boolean,
        ),
        None => operation_matches,
    })
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
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprId, ExprNode};
    use crate::exec::fragment::error::ExecPlanInvariant;
    use crate::exec::fragment::program::{
        FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement, FragmentSinkSpec,
    };
    use crate::exec::fragment::sink::DataStreamPartitionType;

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
