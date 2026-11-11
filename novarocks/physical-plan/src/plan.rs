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

use novarocks_type_contract::SemanticParameters;

use novarocks_connector_contract::{
    ConnectorEncodedPayload, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
    ConnectorWriteRouteId, WriteTargetOrdinal,
};

use crate::{
    AggregateBinding, AggregateCallId, EdgeId, ExprArena, ExprId, FragmentId, NodeId, NullOrdering,
    PLAN_CONTRACT_REVISION, PlanVersionId, ProviderColumnReference, Relation, RuntimeFilterId,
    SortDirection, SortExpr, ValueId, ValueType,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterDerivedKind {
    RelationKind,
    AffectedRows,
    CommitFragment,
    ChangeEvent,
    RelationAuxiliary,
    WriteTargetOrdinal,
    GroupingKey,
}

pub const WRITER_MULTIPLEX_SCHEMA_REVISION: u32 = 1;
pub const ROOT_WRITE_RESULT_SCHEMA_REVISION: u32 = 2;

#[derive(Clone, Debug, PartialEq)]
pub enum ValueOrigin {
    /// Nullable Boolean produced by this null-aware mark join.
    JoinMark {
        node: NodeId,
    },
    /// A Task summary output; never a row in the relational root node.
    Summary {
        field: crate::SummaryField,
    },
    ProviderField {
        scan_node: NodeId,
        field: ProviderColumnReference,
    },
    Expr {
        node: NodeId,
        expr: ExprId,
    },
    NullExtended {
        node: NodeId,
        of: ValueId,
    },
    AggregateState {
        call: AggregateCallId,
        phase: crate::AggregatePhase,
    },
    AggregateResult {
        call: AggregateCallId,
    },
    NodeOutput {
        node: NodeId,
        output_ordinal: u32,
    },
    ExchangeImport {
        edge: EdgeId,
        source_value: ValueId,
    },
    CteImport {
        edge: EdgeId,
        producer_fragment: FragmentId,
        producer_value: ValueId,
    },
    WriterDerived {
        writer_node: NodeId,
        kind: WriterDerivedKind,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValueDef {
    pub id: ValueId,
    pub ty: ValueType,
    pub origin: ValueOrigin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputPort {
    pub node: NodeId,
    /// Ordered occurrences. Repeating the same value is legal and intentional.
    pub columns: Box<[ValueId]>,
}

/// Proven identity of one physically ambiguous result value. `Plain` says
/// that the result is the ordinary storage type, never an inferred JSON or
/// opaque value. SQL freezes this fact from the resolved value owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultValueDomain {
    Plain,
    Json,
    Variant,
    Hll,
    Bitmap,
    Object,
    Percentile,
}

impl ResultValueDomain {
    pub fn matches_storage(self, data_type: &arrow_schema::DataType) -> bool {
        use arrow_schema::DataType as D;
        match self {
            Self::Plain => true,
            Self::Json => matches!(data_type, D::Utf8 | D::LargeUtf8),
            Self::Variant => matches!(data_type, D::LargeBinary),
            Self::Hll | Self::Bitmap | Self::Object | Self::Percentile => {
                matches!(data_type, D::Binary | D::LargeBinary)
            }
        }
    }

    pub fn matches_scalar(self, value_type: &novarocks_result_contract::ScalarValueType) -> bool {
        use novarocks_result_contract::{ScalarOpaqueType as O, ScalarValueType as S};
        match (self, value_type) {
            (Self::Json, S::Json)
            | (Self::Variant, S::Variant)
            | (Self::Hll, S::Opaque(O::Hll))
            | (Self::Bitmap, S::Opaque(O::Bitmap))
            | (Self::Object, S::Opaque(O::Object))
            | (Self::Percentile, S::Opaque(O::Percentile)) => true,
            (Self::Plain, S::Json | S::Variant | S::Opaque(_)) => false,
            (Self::Plain, _) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultField {
    pub domain: ResultValueDomain,
    pub name: Box<str>,
    pub alias: Option<Box<str>>,
    pub value: ValueId,
    pub ty: ValueType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultPort {
    /// Exact one-column scalar projection, when the SQL owner has proved the
    /// complete declared identity against the final root carrier. Absence
    /// means that typed scalar preparation must fail; it is never inferred.
    pub scalar_schema: Option<novarocks_result_contract::ScalarSchema>,
    pub fragment: FragmentId,
    pub output: OutputPort,
    pub fields: Box<[ResultField]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashDefinition {
    pub algorithm: novarocks_type_contract::PartitionHashAlgorithm,
}

impl HashDefinition {
    pub const fn native_exchange() -> Self {
        Self {
            algorithm: novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1,
        }
    }
}

/// Complete identity and definition of one ordinary hash-partition space.
///
/// The count parameter is independent from a fragment's pipeline DOP. Task
/// assignment instantiates it once from live topology within its admissible
/// domain; the final plan carries no topology-derived count or route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashPartitionScheme {
    pub space: novarocks_type_contract::PartitionSpaceId,
    pub count: PartitionCountParameter,
    pub definition: HashDefinition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionCountParameter {
    pub id: novarocks_type_contract::PartitionCountParameterId,
    pub admissible: PartitionCountDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionCountDomain {
    pub min: u32,
    pub max: u32,
    pub requires_power_of_two: bool,
}

impl PartitionCountDomain {
    /// Whether one TaskAssignment-time count is a member of this domain.
    pub const fn admits(self, actual_count: u32) -> bool {
        actual_count >= self.min
            && actual_count <= self.max
            && (!self.requires_power_of_two || actual_count.is_power_of_two())
    }
}

/// Exact evidence for the ordinal domain of a bucket layout.
///
/// The digest binds the complete ordered bucket-ordinal domain supplied by the
/// planner, including ordinals whose current scan selection has no work. This
/// preserves the bucket-to-destination alignment required by bucket shuffle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketOrdinalDomainProof {
    pub first_ordinal: u32,
    pub ordinal_count: u32,
    pub evidence_digest: [u8; 32],
}

/// Complete identity and definition of one bucket partition space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPartitionScheme {
    pub space: novarocks_type_contract::PartitionSpaceId,
    pub bucket_count: u32,
    pub hash: novarocks_type_contract::PartitionHashAlgorithm,
    pub layout: novarocks_type_contract::BucketLayoutAlgorithm,
    pub ordinal_domain: BucketOrdinalDomainProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Distribution {
    Unconstrained,
    Singleton,
    RoundRobin,
    Broadcast,
    Hash {
        keys: Box<[ValueId]>,
        scheme: HashPartitionScheme,
    },
    /// TRUE rows are colocated by the exact key tuple; FALSE and NULL rows
    /// bypass key hashing and are distributed round-robin per row. This is
    /// never evidence that all input rows are colocated by `keys`.
    ConditionalKey {
        applies: ValueId,
        keys: Box<[ValueId]>,
        scheme: HashPartitionScheme,
    },
    BucketShuffle {
        keys: Box<[ValueId]>,
        scheme: BucketPartitionScheme,
    },
}

impl Distribution {
    /// Every value required to interpret this contract. The applicability
    /// mask is a reference, but is not part of the canonical hash key tuple.
    pub fn value_references(&self) -> impl Iterator<Item = ValueId> + '_ {
        let (applies, keys): (Option<ValueId>, &[ValueId]) = match self {
            Self::ConditionalKey { applies, keys, .. } => (Some(*applies), keys),
            Self::Hash { keys, .. } | Self::BucketShuffle { keys, .. } => (None, keys),
            _ => (None, &[]),
        };
        applies.into_iter().chain(keys.iter().copied())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderingKey {
    pub value: ValueId,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

/// Whether one logical row occurrence exists on one execution lane or may
/// have been copied onto multiple lanes.
///
/// This is independent from distribution layout. A repartition can change the
/// layout but cannot erase copies that already exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowMultiplicity {
    SingleCopy,
    Replicated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalProperties {
    pub distribution: Distribution,
    pub row_multiplicity: RowMultiplicity,
    pub ordering: Box<[OrderingKey]>,
}

/// What an operator requires of an input whose rows it passes through.
///
/// Passing rows through constrains neither layout nor order, but it cannot
/// erase copies that already exist: a replicated input stays replicated on the
/// other side. This is contract knowledge rather than planner knowledge, so it
/// lives next to the properties it derives.
pub fn passthrough_requirement(input: &PhysicalProperties) -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: input.row_multiplicity,
        ordering: Box::default(),
    }
}

/// Properties a filter preserves.
///
/// A broadcast input stays replica-equivalent only when every replica
/// evaluates the predicate identically.
pub fn derive_filter_output_properties(
    input: &PhysicalProperties,
    predicate_replica_deterministic: bool,
) -> PhysicalProperties {
    let distribution =
        if input.distribution == Distribution::Broadcast && !predicate_replica_deterministic {
            Distribution::Unconstrained
        } else {
            input.distribution.clone()
        };
    PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering: input.ordering.clone(),
    }
}

/// Derive the exact properties preserved by a projection.
///
/// Partitioning survives only when every partition key is still materialized.
/// Ordering survives as its longest consecutive output prefix. Broadcast
/// replica equivalence additionally requires deterministic expressions.
/// Properties that survive an operator which renames the values it passes on.
///
/// A layout or ordering stated over values the operator no longer produces
/// says nothing about its output, so any key that does not survive the mapping
/// drops the fact that depended on it rather than being carried forward under
/// a stale name.
pub fn remap_properties_through_values(
    input: &crate::PhysicalProperties,
    values: &BTreeMap<ValueId, ValueId>,
) -> crate::PhysicalProperties {
    let remap_keys = |keys: &[ValueId]| {
        keys.iter()
            .map(|key| values.get(key).copied())
            .collect::<Option<Vec<_>>>()
            .map(Vec::into_boxed_slice)
    };
    let distribution = match &input.distribution {
        Distribution::Singleton => Distribution::Singleton,
        Distribution::Broadcast => Distribution::Broadcast,
        Distribution::Hash { keys, scheme } => remap_keys(keys)
            .map(|keys| Distribution::Hash {
                keys,
                scheme: scheme.clone(),
            })
            .unwrap_or(Distribution::Unconstrained),
        Distribution::ConditionalKey {
            applies,
            keys,
            scheme,
        } => values
            .get(applies)
            .copied()
            .zip(remap_keys(keys))
            .map(|(applies, keys)| Distribution::ConditionalKey {
                applies,
                keys,
                scheme: scheme.clone(),
            })
            .unwrap_or(Distribution::Unconstrained),
        Distribution::BucketShuffle { keys, scheme } => remap_keys(keys)
            .map(|keys| Distribution::BucketShuffle {
                keys,
                scheme: scheme.clone(),
            })
            .unwrap_or(Distribution::Unconstrained),
        Distribution::Unconstrained | Distribution::RoundRobin => Distribution::Unconstrained,
    };
    let ordering = input
        .ordering
        .iter()
        .map_while(|key| {
            Some(crate::OrderingKey {
                value: values.get(&key.value).copied()?,
                direction: key.direction,
                null_ordering: key.null_ordering,
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    crate::PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering,
    }
}

pub fn derive_project_output_properties(
    input: &PhysicalProperties,
    output_values: &[ValueId],
    expressions_replica_deterministic: bool,
) -> PhysicalProperties {
    let output_values = output_values.iter().copied().collect::<BTreeSet<_>>();
    let distribution = match &input.distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| output_values.contains(key)) =>
        {
            input.distribution.clone()
        }
        Distribution::ConditionalKey { applies, keys, .. }
            if output_values.contains(applies)
                && keys.iter().all(|key| output_values.contains(key)) =>
        {
            input.distribution.clone()
        }
        Distribution::Hash { .. }
        | Distribution::ConditionalKey { .. }
        | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
        Distribution::Broadcast if !expressions_replica_deterministic => {
            Distribution::Unconstrained
        }
        distribution => distribution.clone(),
    };
    let ordering = input
        .ordering
        .iter()
        .take_while(|key| output_values.contains(&key.value))
        .copied()
        .collect::<Vec<_>>()
        .into_boxed_slice();
    PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinKind {
    Cross,
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
    LeftMark,
}

impl JoinKind {
    /// Whether a filter on this join's equality key may be pushed into that
    /// side.
    ///
    /// Such a filter removes rows that have no match, so it may be pushed
    /// into a side whose unmatched rows the join drops anyway. An outer join
    /// keeps its preserved side's unmatched rows, and an anti join keeps
    /// exactly the unmatched ones, so pushing into those sides would delete
    /// rows the statement asked for.
    pub const fn key_filter_reaches_side(self, side: JoinSide) -> bool {
        matches!(
            (self, side),
            (Self::Inner | Self::LeftSemi | Self::RightSemi, _)
                | (Self::LeftOuter, JoinSide::Right)
                | (Self::RightOuter, JoinSide::Left)
        )
    }

    /// Whether removing rows from this side of the join can only remove rows
    /// from its output.
    ///
    /// It can for every kind but an anti join's non-preserved side: there,
    /// removing a row makes more of the preserved side qualify, so the output
    /// grows rather than shrinks.
    pub const fn side_only_loses_rows(self, side: JoinSide) -> bool {
        !matches!(
            (self, side),
            (Self::LeftAnti | Self::NullAwareLeftAnti, JoinSide::Right)
                | (Self::LeftMark, _)
                | (Self::RightAnti, JoinSide::Left)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinDistribution {
    Colocated,
    Partitioned,
    BroadcastBuild,
    Singleton,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum JoinSide {
    Left,
    Right,
}

impl JoinSide {
    pub const fn input_ordinal(self) -> u32 {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }

    pub const fn opposite(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }
}

/// Closed execution placement modes for a nested-loop join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NestLoopJoinDistribution {
    /// Both inputs are globally gathered before evaluation.
    Singleton,
    /// The right input is replicated to every partition of the left input.
    BroadcastRight,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateCall {
    pub id: AggregateCallId,
    pub binding: AggregateBinding,
    pub arguments: Box<[ExprId]>,
    pub distinct: bool,
    pub order_by: Box<[SortExpr]>,
    pub output: ValueId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowExpression {
    pub expression: ExprId,
    pub output: ValueId,
}

/// One node-owned window partition/order contract shared by every call in the
/// node. Individual `ExprKind::WindowCall` values carry only function-specific
/// arguments, ordering, frame and NULL treatment.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Box<[SortExpr]>,
    pub order_by: Box<[SortExpr]>,
    pub expressions: Box<[WindowExpression]>,
}

/// One ordered output occurrence of a table-function node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionOutput {
    /// An outer-input value retained with the same value identity.
    PassThrough(ValueId),
    /// A value produced by one column of the bound relation result.
    FunctionResult { result_ordinal: u32, value: ValueId },
}

impl TableFunctionOutput {
    pub const fn value(self) -> ValueId {
        match self {
            Self::PassThrough(value) | Self::FunctionResult { value, .. } => value,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetOperationKind {
    UnionAll,
    Intersect,
    Except,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterTarget {
    /// Provider-private write handle with an exact public binding header.
    pub handle: ConnectorEncodedPayload,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub input: Box<[ValueId]>,
    pub required_distribution: Distribution,
    pub target_fields: Box<[WriterTargetField]>,
    pub output_schema: WriterRelationSchema,
    pub partial_aggregates: Box<[WriterAggregateCall]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterTargetField {
    pub token: ConnectorWriteFieldToken,
    /// Exact provider name frozen alongside this token. It is validated
    /// against the same private target before a runtime writer is created.
    pub provider_name: Box<str>,
    pub input: ValueId,
    pub ty: ValueType,
    pub hidden: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterRelationFieldRole {
    Kind,
    TargetOrdinal,
    RowCount,
    CommitFragment,
    Auxiliary,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterRelationField {
    pub value: ValueId,
    pub name: Box<str>,
    pub ty: ValueType,
    pub role: WriterRelationFieldRole,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterRelationSchema {
    pub revision: u32,
    pub fields: Box<[WriterRelationField]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterAggregateCall {
    pub input: ValueId,
    pub binding: AggregateBinding,
    pub output: ValueId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UnpivotConstant {
    Scalar(ExprId),
    /// Selected rows in the enclosing plan's original checked constant pools.
    /// These addresses do not authorize a logical domain or reconstruct data.
    Int32List(crate::ConstantReference),
    Utf8Map(crate::ConstantReference),
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpivotValueMapping {
    pub input: ValueId,
    pub constants: Box<[UnpivotConstant]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnpivotSpec {
    pub passthrough: Box<[(ValueId, ValueId)]>,
    pub value_output: ValueId,
    pub literal_outputs: Box<[ValueId]>,
    pub mappings: Box<[UnpivotValueMapping]>,
    pub max_output_rows: u64,
    pub max_output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterGroupedUnpivotMapping {
    /// Exact writer target whose final aggregate group selects this mapping.
    pub write_target_ordinal: WriteTargetOrdinal,
    pub input: ValueId,
    pub constants: Box<[UnpivotConstant]>,
}

/// Writer-statistics expansion after target-grouped final aggregation.
///
/// This is distinct from an ordinary relational Unpivot: the grouping key
/// selects a target-local mapping set before expansion. Without that key,
/// constants belonging to different write targets would be cross-expanded.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterGroupedUnpivotSpec {
    /// Exact subset of write targets that requested statistics expansion.
    /// Targets absent here still participate in the write and final summary.
    pub statistics_target_ordinals: Box<[WriteTargetOrdinal]>,
    pub grouping_input: ValueId,
    pub grouping_output: ValueId,
    pub passthrough_output: ValueId,
    pub value_output: ValueId,
    pub literal_outputs: Box<[ValueId]>,
    pub mappings: Box<[WriterGroupedUnpivotMapping]>,
    pub max_output_rows: u64,
    pub max_output_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterFinishSpec {
    pub count_mode: crate::FinishCountMode,
    pub inputs: crate::FinishInputs,
    pub summaries: crate::PerKind<crate::SummaryColumns>,
    pub expected_target_ordinals: Box<[WriteTargetOrdinal]>,
    pub input_schema: WriterRelationSchema,
    pub output_schema: WriterRelationSchema,
    pub final_aggregates: Box<[WriterAggregateCall]>,
    pub grouped_unpivot: Option<WriterGroupedUnpivotSpec>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinKey {
    pub left: ExprId,
    pub right: ExprId,
    pub null_safe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionTopNType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SortMode {
    Global,
    Analytic {
        partition_by: Box<[SortExpr]>,
    },
    PartitionTopN {
        partition_by: Box<[SortExpr]>,
        limit: u64,
        kind: PartitionTopNType,
    },
}

/// Whether an aggregate's groups are complete when it emits them.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AggregateGrouping {
    /// Later exact state consumers combine these groups. One key may be
    /// emitted repeatedly within a lane, across drivers or across batches.
    Partial,
    /// The required grouping domain is complete and each full key is emitted
    /// at most once by the entire task under its actual CompileProfile.
    /// This covers every driver and does not imply SQL-value finalization.
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopNPhase {
    Single,
    Partial { sequence: crate::TopNSequenceId },
    Final { sequence: crate::TopNSequenceId },
}

/// Exact unit counted and state responsibility of a TopN reduction.
#[derive(Clone, Debug, PartialEq)]
pub enum TopNReduction {
    Rows,
    /// Limit counts distinct complete grouping keys. Every contribution for
    /// each retained key is merged through these exact Intermediate calls.
    /// Output states have new identities; no contribution is a passthrough.
    /// This guarantees task-local uniqueness, not global group completion.
    GroupedStates {
        group_by: Box<[(ExprId, ValueId)]>,
        calls: Box<[AggregateCall]>,
        comparator: novarocks_type_contract::OrderedComparisonAlgorithm,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowCountAssertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowCountAssertionSpec {
    Global {
        subject: Box<str>,
        desired_rows: u64,
        comparison: RowCountAssertion,
    },
    PerKeyAtMostOne {
        applicability: crate::AssertionApplicability,
        declared_violation: Option<novarocks_failure_contract::FailureReason>,
        keys: Box<[ValueId]>,
        labels: Box<[Box<str>]>,
        message: Box<str>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupingOutput {
    pub output: ValueId,
    /// Ordered arguments of GROUPING/GROUPING_ID.
    pub arguments: Box<[ValueId]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeEventSpec {
    pub predicate: Option<ExprId>,
    pub effect: ConnectorRowMutationEffect,
    pub assignments: Box<[(ValueId, Option<ExprId>)]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeStreamRoute {
    pub route_id: ConnectorWriteRouteId,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub accepted_effects: Box<[ConnectorRowMutationEffect]>,
    pub input_mapping: Box<[(ConnectorWriteFieldToken, ValueId)]>,
    pub requirements: crate::WriteRouteRequirements,
    pub edge: EdgeId,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeKind {
    Scan {
        /// Query-local identity of this exact provider-read occurrence.
        ///
        /// Two scans may intentionally share one frozen provider relation and
        /// input version. The occurrence keeps their runtime-private
        /// capabilities distinct without changing provider relation identity.
        occurrence: crate::ProviderReadOccurrenceId,
        relation: Box<Relation>,
        read_budget: ScanReadBudget,
        provider_outputs: Box<[(ProviderColumnReference, ValueId)]>,
        /// Predicates that the engine must evaluate at the scan. This may also
        /// contain an `Exact` provider guarantee when deliberate rechecking is
        /// required, and may contain predicates that were never pushed down.
        residuals: Box<[ExprId]>,
        derived_values: Box<[ValueId]>,
    },
    /// Retains rows for which every predicate holds.
    ///
    /// The conjunct list is held here rather than folded into one expression so
    /// that predicate count never becomes expression depth, and so that
    /// consumers that reason per conjunct - pushdown, residual responsibility,
    /// runtime-filter placement - read the conjuncts directly instead of
    /// re-splitting a tree. Each predicate is a TruthOnly use; FALSE or NULL
    /// decides rejection. Scheduling must preserve the exact effect and
    /// conditional-domain responsibilities of each occurrence.
    Filter {
        predicates: Box<[ExprId]>,
    },
    Project {
        expressions: Box<[(ExprId, ValueId)]>,
    },
    Aggregate {
        group_by: Box<[(ExprId, ValueId)]>,
        calls: Box<[AggregateCall]>,
        /// Required grouping completion and task-wide output uniqueness.
        /// Call finalization is independent: Complete may emit intermediate
        /// states, while a finalizing call requires Complete. The compiler
        /// must meet this requirement across the selected local DOP.
        grouping: AggregateGrouping,
    },
    HashJoin {
        kind: JoinKind,
        condition: crate::HashJoinCondition<Box<[JoinKey]>, ExprId>,
        build_side: JoinSide,
        distribution: JoinDistribution,
        null_extended: Box<[ValueId]>,
    },
    NestLoopJoin {
        kind: JoinKind,
        distribution: NestLoopJoinDistribution,
        predicate: Option<ExprId>,
        null_extended: Box<[ValueId]>,
    },
    Sort {
        order_by: Box<[SortExpr]>,
        mode: SortMode,
    },
    TopN {
        order_by: Box<[SortExpr]>,
        limit: u64,
        offset: u64,
        phase: TopNPhase,
        reduction: TopNReduction,
    },
    Limit {
        limit: Option<u64>,
        offset: u64,
    },
    Window(WindowSpec),
    SetOp {
        kind: SetOperationKind,
        input_mappings: Box<[Box<[ValueId]>]>,
    },
    Values {
        rows: Box<[Box<[ExprId]>]>,
    },
    SystemRelation {
        output_fields: std::sync::Arc<[arrow_schema::FieldRef]>,
        data: crate::SystemRelationData,
    },
    Repeat {
        /// Every key any grouping set may group by, in the order their
        /// presence is written into a grouping id. A key that no set keeps and
        /// a key every set keeps both stand here: the first is not a
        /// `grouping_values` entry only by accident, and the second is not one
        /// at all, so neither list recovers this one.
        rollup_keys: Box<[ValueId]>,
        grouping_sets: Box<[Box<[ValueId]>]>,
        /// Exact replacement for grouping values that can become NULL in at
        /// least one grouping set: `(input, nullable output)`.
        grouping_values: Box<[(ValueId, ValueId)]>,
        grouping_outputs: Box<[GroupingOutput]>,
    },
    Unpivot {
        spec: UnpivotSpec,
    },
    GenerateSeries {
        start: ExprId,
        stop: ExprId,
        step: Option<ExprId>,
    },
    TableFunction {
        function: crate::BoundTableFunction,
        arguments: Box<[ExprId]>,
        outputs: Box<[TableFunctionOutput]>,
        left_outer: bool,
    },
    AssertOneRow(RowCountAssertionSpec),
    ChangeEventExpand {
        events: Box<[ChangeEventSpec]>,
        effect_output: ValueId,
    },
    ExchangeSource {
        edge: EdgeId,
        imports: Box<[(ValueId, ValueId)]>,
    },
    TableWriter {
        target: WriterTarget,
    },
    TableFinish(WriterFinishSpec),
}

/// Explicit per-reader output-batch budget for one scan.
///
/// These values are semantic resource limits in the final plan. They are
/// supplied by planning and never reconstructed by a codec or runtime default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanReadBudget {
    pub max_batch_rows: u64,
    pub max_batch_bytes: u64,
}

/// Borrow of the common grouping and exact aggregate-call contract.
pub type AggregateContractRef<'a> = (&'a [(ExprId, ValueId)], &'a [AggregateCall]);

impl NodeKind {
    /// Authorship is a closed node-family fact, never inferred from values.
    /// No current node family authors a Task summary.
    pub const fn summary_authorship(&self) -> Option<crate::SummaryKind> {
        None
    }

    /// The one aggregate state contract, shared by ordinary aggregation and
    /// key-budgeted reduction. No separate grouped-TopN binding vocabulary.
    pub fn aggregate_contract(&self) -> Option<AggregateContractRef<'_>> {
        match self {
            Self::Aggregate {
                group_by, calls, ..
            }
            | Self::TopN {
                reduction:
                    TopNReduction::GroupedStates {
                        group_by, calls, ..
                    },
                ..
            } => Some((group_by, calls)),
            _ => None,
        }
    }
    pub(crate) fn expression_references(&self, output: &mut Vec<ExprId>) {
        match self {
            Self::Scan {
                relation,
                residuals,
                ..
            } => {
                output.extend(
                    relation
                        .predicate_guarantees()
                        .iter()
                        .map(|guarantee| guarantee.predicate),
                );
                output.extend(residuals.iter().copied());
            }
            Self::Filter { predicates } => output.extend(predicates.iter().copied()),
            Self::Project { expressions } => {
                output.extend(expressions.iter().map(|(expr, _)| *expr));
            }
            Self::Aggregate {
                group_by, calls, ..
            } => {
                output.extend(group_by.iter().map(|(expression, _)| *expression));
                for call in calls {
                    output.extend(call.arguments.iter().copied());
                    output.extend(call.order_by.iter().map(|item| item.expr));
                }
            }
            Self::HashJoin { condition, .. } => {
                for key in condition
                    .keys()
                    .iter()
                    .chain(condition.scope_keys().into_iter().flatten())
                {
                    output.extend([key.left, key.right]);
                }
                output.extend(*condition.residual());
            }
            Self::NestLoopJoin { predicate, .. } => output.extend(*predicate),
            Self::Sort { order_by, mode } => {
                output.extend(order_by.iter().map(|item| item.expr));
                match mode {
                    SortMode::Global => {}
                    SortMode::Analytic { partition_by }
                    | SortMode::PartitionTopN { partition_by, .. } => {
                        output.extend(partition_by.iter().map(|item| item.expr));
                    }
                }
            }
            Self::TopN {
                order_by,
                reduction,
                ..
            } => {
                output.extend(order_by.iter().map(|item| item.expr));
                if let TopNReduction::GroupedStates {
                    group_by, calls, ..
                } = reduction
                {
                    output.extend(group_by.iter().map(|(expr, _)| *expr));
                    for call in calls {
                        output.extend(call.arguments.iter().copied());
                        output.extend(call.order_by.iter().map(|item| item.expr));
                    }
                }
            }
            Self::Window(spec) => {
                output.extend(spec.partition_by.iter().map(|item| item.expr));
                output.extend(spec.order_by.iter().map(|item| item.expr));
                output.extend(spec.expressions.iter().map(|item| item.expression));
            }
            Self::Values { rows } => {
                for row in rows {
                    output.extend(row.iter().copied());
                }
            }
            Self::GenerateSeries { start, stop, step } => {
                output.extend([*start, *stop]);
                output.extend(*step);
            }
            Self::TableFunction { arguments, .. } => output.extend(arguments.iter().copied()),
            Self::Unpivot { spec } => {
                for mapping in &spec.mappings {
                    for constant in &mapping.constants {
                        if let UnpivotConstant::Scalar(expression) = constant {
                            output.push(*expression);
                        }
                    }
                }
            }
            Self::ChangeEventExpand { events, .. } => {
                for event in events {
                    output.extend(event.predicate);
                    output.extend(event.assignments.iter().filter_map(|(_, expr)| *expr));
                }
            }
            Self::TableFinish(spec) => {
                if let Some(unpivot) = &spec.grouped_unpivot {
                    for mapping in &unpivot.mappings {
                        for constant in &mapping.constants {
                            if let UnpivotConstant::Scalar(expression) = constant {
                                output.push(*expression);
                            }
                        }
                    }
                }
            }
            Self::SystemRelation { .. }
            | Self::Limit { .. }
            | Self::SetOp { .. }
            | Self::Repeat { .. }
            | Self::AssertOneRow(_)
            | Self::ExchangeSource { .. }
            | Self::TableWriter { .. } => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalNode {
    pub id: NodeId,
    pub inputs: Box<[NodeId]>,
    pub required_inputs: Box<[PhysicalProperties]>,
    pub output_properties: PhysicalProperties,
    pub output: OutputPort,
    pub kind: NodeKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PipelineDopDomain {
    pub min: u32,
    pub max: u32,
    pub requires_power_of_two: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FragmentSink {
    Result,
    RootResult(Box<novarocks_result_contract::RootOutputContract>),
    Stream {
        edge: EdgeId,
    },
    Multicast {
        edges: Box<[EdgeId]>,
    },
    Router {
        effect: ValueId,
        routes: Box<[ChangeStreamRoute]>,
    },
    Noop,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fragment {
    id: FragmentId,
    root: NodeId,
    values: BTreeMap<ValueId, ValueDef>,
    expressions: ExprArena,
    nodes: BTreeMap<NodeId, PhysicalNode>,
    sink: FragmentSink,
    summaries: Box<[crate::SummaryOutput]>,
    dop_domain: PipelineDopDomain,
    stage_placement: crate::StagePlacementPolicy,
    runtime_filters: Box<[RuntimeFilterId]>,
    pub(crate) call_requests: crate::FragmentCallRequests,
}

impl Fragment {
    pub(crate) fn into_parts(self) -> FragmentParts {
        FragmentParts {
            id: self.id,
            root: self.root,
            values: self.values,
            expressions: self.expressions,
            nodes: self.nodes,
            sink: self.sink,
            summaries: self.summaries,
            dop_domain: self.dop_domain,
            stage_placement: self.stage_placement,
            runtime_filters: self.runtime_filters,
            call_requests: self.call_requests,
        }
    }

    pub const fn id(&self) -> FragmentId {
        self.id
    }

    pub const fn root(&self) -> NodeId {
        self.root
    }

    pub fn values(&self) -> &BTreeMap<ValueId, ValueDef> {
        &self.values
    }

    pub const fn expressions(&self) -> &ExprArena {
        &self.expressions
    }

    pub fn nodes(&self) -> &BTreeMap<NodeId, PhysicalNode> {
        &self.nodes
    }

    pub const fn sink(&self) -> &FragmentSink {
        &self.sink
    }

    pub fn summaries(&self) -> &[crate::SummaryOutput] {
        &self.summaries
    }

    pub const fn stage_placement(&self) -> &crate::StagePlacementPolicy {
        &self.stage_placement
    }

    pub const fn dop_domain(&self) -> PipelineDopDomain {
        self.dop_domain
    }

    pub const fn call_requests(&self) -> &crate::FragmentCallRequests {
        &self.call_requests
    }

    pub fn runtime_filters(&self) -> &[RuntimeFilterId] {
        &self.runtime_filters
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeKind {
    Stream,
    CteMulticast,
    ChangeStreamRouter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeSource {
    pub fragment: FragmentId,
    pub port: crate::FragmentOutputPort,
    pub projection: Box<[ValueId]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeDestination {
    pub fragment: FragmentId,
    pub node: NodeId,
    /// Ordered `(source value, destination import value)` mapping.
    pub receive_mapping: Box<[(ValueId, ValueId)]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Edge {
    pub id: EdgeId,
    pub role: crate::OutputRole,
    pub delivery: crate::DeliveryRequirement,
    pub kind: EdgeKind,
    pub source: EdgeSource,
    pub destination: EdgeDestination,
    pub partitioning: EdgePartitioning,
}

/// One exchange distribution expressed in both adjacent value domains.
///
/// The sender side names values in the source fragment. The receiver side
/// names the corresponding imported values in the destination fragment.
/// Keeping both forms avoids treating fragment-local `ValueId`s as global.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgePartitioning {
    pub source: Distribution,
    /// Multiplicity before this exchange distributes its source rows.
    pub source_multiplicity: RowMultiplicity,
    pub destination: Distribution,
    /// Multiplicity observed by the receiving fragment.
    pub destination_multiplicity: RowMultiplicity,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CutValue {
    pub value: ValueId,
    pub ty: ValueType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CutImport {
    pub source: CutValue,
    pub destination: ValueId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeStreamWriterCutField {
    pub token: ConnectorWriteFieldToken,
    pub source: ValueId,
    pub destination: ValueId,
}

/// Symmetric proof carried on both sides of a change-stream edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeStreamWriterCut {
    pub route_id: ConnectorWriteRouteId,
    pub write_target_ordinal: WriteTargetOrdinal,
    pub fields: Box<[ChangeStreamWriterCutField]>,
}

/// One writer relation field expressed in both adjacent fragment domains.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterResultCutField {
    pub source: ValueId,
    pub destination: ValueId,
    pub name: Box<str>,
    pub ty: ValueType,
    pub role: WriterRelationFieldRole,
}

/// Symmetric proof that one stream edge carries one complete writer relation.
#[derive(Clone, Debug, PartialEq)]
pub struct WriterResultCut {
    pub write_target_ordinal: WriteTargetOrdinal,
    pub schema_revision: u32,
    pub fields: Box<[WriterResultCutField]>,
}

/// Complete inbound cut supplied with one independently validated fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct InboundFragmentCut {
    pub edge: EdgeId,
    pub kind: EdgeKind,
    pub source_fragment: FragmentId,
    pub destination_node: NodeId,
    pub imports: Box<[CutImport]>,
    pub partitioning: EdgePartitioning,
    pub source_port: crate::FragmentOutputPort,
    pub payload: crate::ExchangePayloadContract,
    pub delivery: crate::DeliveryRequirement,
}

/// Complete outbound cut supplied with one independently validated fragment.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboundFragmentCut {
    pub edge: EdgeId,
    pub kind: EdgeKind,
    pub destination_fragment: FragmentId,
    /// The edge's destination ExchangeSource in the destination fragment: the
    /// sender's routing address. FE derives it from the plan edge.
    pub destination_node: NodeId,
    pub projection: Box<[CutValue]>,
    /// Exact source-to-destination value mapping at the peer boundary.
    pub destination_imports: Box<[CutImport]>,
    pub partitioning: EdgePartitioning,
    pub source_port: crate::FragmentOutputPort,
    pub payload: crate::ExchangePayloadContract,
    pub delivery: crate::DeliveryRequirement,
}

/// Which endpoint of one runtime filter a binding identity names.
///
/// The index is into that filter's own `producers` or `consumers`, so a
/// binding identity resolves back to the exact endpoint it was minted for
/// without a second lookup key.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterBindingRole {
    Producer(usize),
    Consumer(usize),
}

/// One local runtime-filter endpoint and the plan-global binding identity
/// [`crate::runtime_filter_bindings`] numbered it with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterBindingCut {
    pub binding_id: u32,
    pub filter: RuntimeFilterId,
    pub role: RuntimeFilterBindingRole,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FragmentCuts {
    pub inbound: Box<[InboundFragmentCut]>,
    pub outbound: Box<[OutboundFragmentCut]>,
    /// Complete static runtime-filter contracts with at least one local endpoint.
    pub runtime_filters: Box<[RuntimeFilter]>,
    /// Plan-global binding identities of every local runtime-filter endpoint,
    /// in numbering order. One package cannot recompute them: the numbering
    /// depends on every other fragment's attachments.
    pub runtime_filter_bindings: Box<[RuntimeFilterBindingCut]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterKind {
    Bloom,
    MinMax,
    InList,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterNullSemantics {
    NeverMatches,
    NullSafeEqual,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterOrderKey {
    pub ty: ValueType,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeFilterDomain {
    Membership {
        ty: ValueType,
        null_semantics: RuntimeFilterNullSemantics,
    },
    Ordered {
        key: RuntimeFilterOrderKey,
        inclusive: bool,
        comparator: novarocks_type_contract::OrderedComparisonAlgorithm,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterContributionKind {
    ValueDomainDelta,
    FinalDomainShard,
    OrderedBoundUpdate,
    FinalOrderedHullShard,
    ProducerClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLifecycle {
    CompleteOnce,
    MonotonicUpdates,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterReduction {
    SetUnion,
    TightenOrderedBound,
    UnionOrderedHull,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterCompletion {
    ProducerClosed,
    FencedCommittedDomain,
}

/// One node in a bounded, non-recursive runtime-filter coverage expression.
/// Composite children are arena indices in canonical strictly increasing
/// order and must precede their parent.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterCoverageNode {
    Witness(crate::RuntimeFilterWitnessId),
    AllOf { children: Box<[u32]> },
    AnyOf { children: Box<[u32]> },
}

/// Static proof that a runtime-filter completion condition covers its exact
/// producer set. The flat arena keeps untrusted depth away from Rust call
/// stacks during clone, comparison, transport and destruction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterCoverage {
    pub nodes: Box<[RuntimeFilterCoverageNode]>,
    pub root: u32,
}

/// Exact static producer-input progress that closes one producer witness.
///
/// `build_edges` names the producer input frontier: the hash-join build input
/// or the Aggregate TopN input. `non_build_edges` is the remainder of the
/// producer fragment's inbound frontier and is empty for Aggregate TopN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerProgress {
    /// Complete inbound exchange set under the join build input.
    pub build_edges: Box<[EdgeId]>,
    /// Complete inbound exchange set outside the join build input.
    pub non_build_edges: Box<[EdgeId]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterApplyPoint {
    NodeInput { input_ordinal: u32 },
    NodeOutput,
    ScanSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterProducerTarget {
    JoinBuildKey {
        equality: crate::RuntimeFilterEqualityWitnessId,
    },
    AggregateTopNKey {
        group_key_ordinal: u32,
        /// Exact TopN whose monotonic bound is contributed by this
        /// aggregate. The validator replays the parent/key/order proof rather
        /// than trusting planner placement.
        topn: NodeId,
        phase: TopNPhase,
        order_key_ordinal: u32,
        limit: u64,
        offset: u64,
        direction: crate::SortDirection,
        null_ordering: crate::NullOrdering,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RuntimeFilterArtifactCapability {
    Membership,
    OrderedRange,
    EmptyDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LateApplyGranularity {
    Row,
    Batch,
    RowGroup,
    Split,
    File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterConsumerActivation {
    BlockingSnapshot,
    /// Start without the filter, then install its one final snapshot only for
    /// work that has not crossed the named scheduling boundary.
    StartUnfilteredThenApplyComplete {
        late_apply: LateApplyGranularity,
    },
    /// Remain runnable while installing every monotonic update at the named
    /// scheduling boundary. There is no complete snapshot admission barrier.
    NonBlockingLive {
        late_apply: LateApplyGranularity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterConsumerTarget {
    JoinProbeKey {
        equality: crate::RuntimeFilterEqualityWitnessId,
    },
    ScanField {
        equality: crate::RuntimeFilterEqualityWitnessId,
        lineage: Box<[RuntimeFilterLineageStep]>,
    },
    AggregateTopNScanField {
        producer: crate::RuntimeFilterWitnessId,
        lineage: Box<[RuntimeFilterLineageStep]>,
    },
}

/// One checked reverse-lineage transition from a join probe key toward an
/// earlier scan field. Each variant names only information that the validator
/// can recompute from the immutable plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterLineageStep {
    FilterPassThrough {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
    },
    SortPassThrough {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
    },
    ProjectIdentity {
        fragment: FragmentId,
        node: NodeId,
        output_ordinal: u32,
    },
    /// Traverse one exact equality key of an inner hash join. `source_side`
    /// identifies the key currently named by the join output and
    /// `target_side` identifies the input/key followed by the reverse trace.
    JoinEquality {
        fragment: FragmentId,
        node: NodeId,
        key_ordinal: u32,
        source_side: JoinSide,
        target_side: JoinSide,
    },
    /// Follow one input of a join that republishes the value unchanged.
    ///
    /// Removing rows from that input can only remove rows from the join's
    /// output, never add any, and every output row it removes carries the
    /// value the filter rejected. The exception is an anti join's
    /// non-preserved side: removing rows there makes more of the other side
    /// qualify, so the output grows.
    JoinOutputPassThrough {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
    },
    AggregateGroupKey {
        fragment: FragmentId,
        node: NodeId,
        group_key_ordinal: u32,
    },
    UnionAllBranch {
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: u32,
        output_ordinal: u32,
    },
    ExchangeMapping {
        edge: EdgeId,
        mapping_ordinal: u32,
    },
}

/// Recomputable proof anchor tying one runtime-filter domain to one exact
/// hash-join equality key. Consumers may filter only the opposite key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterEqualityWitness {
    pub id: crate::RuntimeFilterEqualityWitnessId,
    pub fragment: FragmentId,
    pub join: NodeId,
    pub key_ordinal: u32,
    pub domain_side: JoinSide,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterProducer {
    pub witness: crate::RuntimeFilterWitnessId,
    pub endpoint: RuntimeFilterEndpoint,
    pub apply_point: RuntimeFilterApplyPoint,
    pub contribution_kinds: Box<[RuntimeFilterContributionKind]>,
    pub completion: RuntimeFilterCompletion,
    pub progress: RuntimeFilterProducerProgress,
    pub target: RuntimeFilterProducerTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterEndpoint {
    pub fragment: FragmentId,
    pub node: NodeId,
    pub values: Box<[ValueId]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterConsumer {
    pub endpoint: RuntimeFilterEndpoint,
    pub apply_point: RuntimeFilterApplyPoint,
    pub capabilities: Box<[RuntimeFilterArtifactCapability]>,
    pub activation: RuntimeFilterConsumerActivation,
    pub target: RuntimeFilterConsumerTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterPolicy {
    /// Maximum accepted bytes from one producer contribution.
    pub max_contribution_bytes: u64,
    /// Maximum materialized artifact bytes. Exceeding either byte limit,
    /// exhausting retries, reaching the deadline, or losing a producer closes
    /// the optimization without an artifact. Every consumer then continues
    /// unfiltered; a runtime filter never fails the query or waits forever.
    pub max_artifact_bytes: u64,
    pub deadline_ms: u64,
    pub max_retries: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilter {
    pub id: RuntimeFilterId,
    pub kind: RuntimeFilterKind,
    pub domain: RuntimeFilterDomain,
    pub lifecycle: RuntimeFilterLifecycle,
    pub reduction: RuntimeFilterReduction,
    pub availability_coverage: RuntimeFilterCoverage,
    pub terminal_coverage: RuntimeFilterCoverage,
    pub equality_witnesses: Box<[RuntimeFilterEqualityWitness]>,
    pub producers: Box<[RuntimeFilterProducer]>,
    pub consumers: Box<[RuntimeFilterConsumer]>,
    pub policy: RuntimeFilterPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequiredContracts {
    pub plan_contract_revision: u32,
}

impl Default for RequiredContracts {
    fn default() -> Self {
        Self {
            plan_contract_revision: PLAN_CONTRACT_REVISION,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanAnnotation {
    pub subject: AnnotationSubject,
    pub key: Box<str>,
    pub value: Box<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnnotationSubject {
    Plan,
    Fragment(FragmentId),
    Node(FragmentId, NodeId),
    Value(FragmentId, ValueId),
}

/// Complete immutable final physical plan.
#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    constants: crate::ConstantPools,
    parameters: SemanticParameters,
    version: PlanVersionId,
    fragments: BTreeMap<FragmentId, Fragment>,
    edges: BTreeMap<EdgeId, Edge>,
    runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    result_port: Option<ResultPort>,
    required: RequiredContracts,
    annotations: Box<[PlanAnnotation]>,
}

impl PhysicalPlan {
    pub fn constants(&self) -> &crate::ConstantPools {
        &self.constants
    }

    /// The sole immutable parameter-value authority for this plan.
    pub const fn parameters(&self) -> &SemanticParameters {
        &self.parameters
    }

    /// Freeze the final root purpose once the application has resolved the
    /// ordered render or domain facts. The complete plan is revalidated.
    pub fn with_root_output(
        mut self,
        contract: novarocks_result_contract::RootOutputContract,
    ) -> Result<Self, crate::ValidationErrors> {
        let mut errors = crate::validation::ValidationContext::new();
        let fragment = self
            .result_port
            .as_ref()
            .and_then(|port| self.fragments.get_mut(&port.fragment));
        match fragment {
            Some(fragment) if matches!(fragment.sink, FragmentSink::Result) => {
                fragment.sink = FragmentSink::RootResult(Box::new(contract));
            }
            _ => errors.push(crate::ValidationError::new(
                "result_port",
                "root purpose requires exactly one unfrozen result sink",
            )),
        }
        if !errors.is_empty() {
            return Err(crate::ValidationErrors::from_collector(errors));
        }
        crate::validate_plan(&self)?;
        Ok(self)
    }

    pub const fn version(&self) -> PlanVersionId {
        self.version
    }

    pub fn fragments(&self) -> &BTreeMap<FragmentId, Fragment> {
        &self.fragments
    }

    pub fn edges(&self) -> &BTreeMap<EdgeId, Edge> {
        &self.edges
    }

    pub fn runtime_filters(&self) -> &BTreeMap<RuntimeFilterId, RuntimeFilter> {
        &self.runtime_filters
    }

    pub const fn result_port(&self) -> Option<&ResultPort> {
        self.result_port.as_ref()
    }

    pub const fn required(&self) -> RequiredContracts {
        self.required
    }

    pub fn annotations(&self) -> &[PlanAnnotation] {
        &self.annotations
    }
}

pub(crate) struct PhysicalPlanParts {
    pub constants: crate::ConstantPools,
    pub parameters: SemanticParameters,
    pub version: PlanVersionId,
    pub fragments: BTreeMap<FragmentId, Fragment>,
    pub edges: BTreeMap<EdgeId, Edge>,
    pub runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    pub result_port: Option<ResultPort>,
    pub required: RequiredContracts,
    pub annotations: Box<[PlanAnnotation]>,
}

impl From<PhysicalPlanParts> for PhysicalPlan {
    fn from(parts: PhysicalPlanParts) -> Self {
        Self {
            constants: parts.constants,
            parameters: parts.parameters,
            version: parts.version,
            fragments: parts.fragments,
            edges: parts.edges,
            runtime_filters: parts.runtime_filters,
            result_port: parts.result_port,
            required: parts.required,
            annotations: parts.annotations,
        }
    }
}

pub(crate) struct FragmentParts {
    pub id: FragmentId,
    pub root: NodeId,
    pub values: BTreeMap<ValueId, ValueDef>,
    pub expressions: ExprArena,
    pub nodes: BTreeMap<NodeId, PhysicalNode>,
    pub sink: FragmentSink,
    pub summaries: Box<[crate::SummaryOutput]>,
    pub dop_domain: PipelineDopDomain,
    pub stage_placement: crate::StagePlacementPolicy,
    pub runtime_filters: Box<[RuntimeFilterId]>,
    pub call_requests: crate::FragmentCallRequests,
}

impl From<FragmentParts> for Fragment {
    fn from(parts: FragmentParts) -> Self {
        Self {
            id: parts.id,
            root: parts.root,
            values: parts.values,
            expressions: parts.expressions,
            nodes: parts.nodes,
            sink: parts.sink,
            summaries: parts.summaries,
            dop_domain: parts.dop_domain,
            stage_placement: parts.stage_placement,
            runtime_filters: parts.runtime_filters,
            call_requests: parts.call_requests,
        }
    }
}

#[cfg(test)]
mod conditional_partition_contract_tests {
    use super::*;

    fn distribution() -> Distribution {
        Distribution::ConditionalKey {
            applies: ValueId::new(1),
            keys: Box::new([ValueId::new(2)]),
            scheme: HashPartitionScheme {
                space: novarocks_type_contract::PartitionSpaceId::try_new([1; 32]).unwrap(),
                count: PartitionCountParameter {
                    id: novarocks_type_contract::PartitionCountParameterId::try_new([2; 32])
                        .unwrap(),
                    admissible: PartitionCountDomain {
                        min: 1,
                        max: 16,
                        requires_power_of_two: false,
                    },
                },
                definition: HashDefinition {
                    algorithm:
                        novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1,
                },
            },
        }
    }

    #[test]
    fn conditional_partition_references_mask_without_claiming_an_all_rows_hash() {
        let distribution = distribution();
        assert_eq!(
            distribution.value_references().collect::<Vec<_>>(),
            [ValueId::new(1), ValueId::new(2)]
        );
        let Distribution::ConditionalKey { keys, scheme, .. } = distribution.clone() else {
            unreachable!()
        };
        assert_ne!(distribution, Distribution::Hash { keys, scheme });
        let props = PhysicalProperties {
            distribution,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        assert_eq!(
            derive_project_output_properties(&props, &[ValueId::new(2)], true).distribution,
            Distribution::Unconstrained
        );
        let remap = BTreeMap::from([
            (ValueId::new(1), ValueId::new(3)),
            (ValueId::new(2), ValueId::new(4)),
        ]);
        assert_eq!(
            remap_properties_through_values(&props, &remap)
                .distribution
                .value_references()
                .collect::<Vec<_>>(),
            [ValueId::new(3), ValueId::new(4)]
        );
    }
}
