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

//! Pure, task-independent facts used while compiling a local fragment program.
//!
//! This crate deliberately has no execution, transport, worker, connector SPI,
//! or async runtime dependency. A program's static representation moves here
//! as its node and expression types are separated from task-owned bindings.

mod connector_scan;
mod contract;
mod expressions;
mod layout;
mod program;
mod requirements;
mod runtime_filter;
mod sink;
mod values;

pub use connector_scan::{
    ScanColumnId, StaticConnectorScan, StaticConnectorScanError, StaticScanAssignment,
    StaticScanDynamicFilter,
};
pub use contract::{
    CompileProfile, FragmentProgramOptions, FragmentSinkAssignmentKind,
    FragmentSinkAssignmentRequirement, KernelAbiVersion, LayoutIdentity, RuntimeFilterContract,
    RuntimeFilterId, ScanAssignmentKind, ScanSourceContract,
};
pub use expressions::{
    ImmutableExpressions, MAX_STATIC_EXPRESSION_DEPTH, MAX_STATIC_EXPRESSION_DYNAMIC_BYTES,
    MAX_STATIC_EXPRESSIONS, ProgramExprId, StaticExprKind, StaticExprNode, StaticExpressionError,
    StaticFieldSchema, StaticFunctionKind, StaticLiteral,
};
pub use layout::{LayoutError, StaticLayout};
pub use program::{
    AggregateTopNFilter, AnalyticOutputColumn, AssertRowsMode, ChangeEventOutputExpr,
    ChangeEventSpec, FilterConsumerAtExpr, FilterProducerAtExpr, JoinDistributionMode, JoinType,
    LocalProgram, LocalProgramError, MAX_PROGRAM_EXPANDED_OCCURRENCES, MAX_PROGRAM_NODE_DEPTH,
    MAX_PROGRAM_NODES, NestedLoopJoinType, ProgramNode, ProgramNodeKind, ProjectExpressionSlot,
    RowAssertion, SetOpKind, SortExpression, SortTopNType, StaticAggregateCall,
    StaticAggregateOrder, StaticAggregateTypeSignature, StaticWindowFunction,
    StaticWriterProjection, StreamingPreaggregationMode, TableFunctionOutputSlot, UnpivotConstant,
    UnpivotMapping, UnpivotPassthrough, WindowBoundary, WindowFrame, WindowFunctionKind,
    WindowType, WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan, WriterPartialAggregateCall,
};
pub use requirements::{
    BindingRequirement, BindingRequirements, BindingRequirementsError, ProgramNodeId,
    ScanSourceKind,
};
pub use runtime_filter::{
    FilterConsumerActivation, FilterLateApplyGranularity, FilterNullOrder, FilterNullSemantics,
    FilterOrderKey, FilterProducerKind, FilterReduction, FilterSortDirection, StaticFilterConsumer,
    StaticFilterContract, StaticFilterError, StaticFilterProducer,
};
pub use sink::{
    MAX_STATIC_SINK_BRANCHES, MAX_STATIC_SINK_COLUMNS, MAX_STATIC_SINK_EXPRESSIONS,
    StaticSinkError, StaticSinkProgram, StaticStreamBranch,
};
pub use values::{MAX_STATIC_VALUES_BACKING_BYTES, StaticValues, StaticValuesError};
