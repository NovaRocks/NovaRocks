// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use crate::api::QueryExecutionKind;
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};

use super::CompletedPhysicalPlanCandidate;

/// What one statement delivers.
///
/// A row-producing statement names each column it returns; a statement whose
/// only result is that it finished names none. The fields are the client's
/// own view of the result, which is all any consumer of this contract has
/// ever read from it.
#[derive(Clone, Debug)]
pub enum OutputContract {
    Rows(Arc<[crate::api::ResultField]>),
    CompletionOnly,
}

/// The event that closes this logical execution's recovery and output.
/// Public statement output and the root computation carrier remain separate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionFinishBoundary {
    ClientEof,
    TaskCompletion,
    ComputationReady,
}

impl OutputContract {
    /// The completed root sink is the authority for the row carrier. Keep the
    /// legacy carrier only for plans whose sink has not yet been migrated.
    fn row_carrier_from_plan(
        plan: &novarocks_physical_plan::PhysicalPlan,
    ) -> Result<Option<crate::api::ResultRowCarrier>, String> {
        use novarocks_physical_plan::FragmentSink;
        use novarocks_result_contract::{ClientRowProfile, RootOutputKind, RootProfileV1};
        let sink = plan
            .result_port()
            .and_then(|port| plan.fragments().get(&port.fragment))
            .map(|fragment| fragment.sink());
        match sink {
            Some(FragmentSink::RootResult(contract)) => {
                contract
                    .validate_purpose()
                    .map_err(|error| error.to_string())?;
                let profile = if contract.kind() == RootOutputKind::ClientRows {
                    Some(
                        ClientRowProfile::try_new(
                            RootProfileV1::SEGMENT_BYTES,
                            RootProfileV1::ROW_PAYLOAD_BYTES,
                        )
                        .map_err(|error| error.to_string())?,
                    )
                } else {
                    None
                };
                crate::api::ResultRowCarrier::relayed(contract.kind(), profile)
                    .map(Some)
                    .map_err(|error| error.to_string())
            }
            Some(FragmentSink::Result) => {
                Err("completed result sink has no frozen root purpose".into())
            }
            None => Ok(None),
            _ => Err("completed result port does not name a result sink".into()),
        }
    }

    /// The same contract, from a completed plan's own result port.
    ///
    /// A completed plan states what it delivers as part of being complete:
    /// the port names each field, and the name is the alias where the
    /// statement gave one, which is the name the client asked for.
    /// Preserve the original SQL public declaration from its actual source
    /// owner. Program sources retain the original exact physical port path.
    pub fn from_completed_candidate(
        kind: QueryExecutionKind,
        candidate: &CompletedPhysicalPlanCandidate,
    ) -> Result<Self, String> {
        Self::from_result_port(kind, candidate.original_public_result_port())
    }

    pub fn from_completed_plan(
        kind: QueryExecutionKind,
        plan: &novarocks_physical_plan::PhysicalPlan,
    ) -> Result<Self, String> {
        Self::from_result_port(kind, plan.result_port())
    }

    fn from_result_port(
        kind: QueryExecutionKind,
        port: Option<&novarocks_physical_plan::ResultPort>,
    ) -> Result<Self, String> {
        // A write plan's root port carries the connector commit relation for
        // its internal finish. It is not a client row result.
        if kind == QueryExecutionKind::Write {
            return Ok(Self::CompletionOnly);
        }
        match port {
            Some(result) if !result.fields.is_empty() => {
                use novarocks_physical_plan::ResultValueDomain as D;
                use novarocks_type_contract::ValueLogicalType as L;
                for field in &result.fields {
                    let complete = field.ty.logical_type();
                    let agrees = match field.domain {
                        D::Plain => matches!(complete, L::Physical | L::LargeInt | L::Uuid),
                        D::Json => complete == L::Json,
                        D::Variant => complete == L::Variant,
                        D::Hll => complete == L::Hll,
                        D::Bitmap => complete == L::Bitmap,
                        D::Object => complete == L::Object,
                        D::Percentile => complete == L::Percentile,
                    };
                    if !agrees {
                        return Err(
                            "completed result domain differs from its complete value binding"
                                .into(),
                        );
                    }
                }
                Ok(Self::Rows(
                    result
                        .fields
                        .iter()
                        .map(|field| {
                            crate::api::ResultField::new(
                                field.alias.as_deref().unwrap_or(&field.name).to_string(),
                                field.ty.clone(),
                            )
                        })
                        .collect(),
                ))
            }
            _ if kind == QueryExecutionKind::Read => {
                Err("completed read plan has no row output contract".to_string())
            }
            _ => Ok(Self::CompletionOnly),
        }
    }

    pub fn fields(&self) -> &[crate::api::ResultField] {
        match self {
            Self::Rows(fields) => fields,
            Self::CompletionOnly => &[],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ResidualResponsibility {
    EngineFilter,
    EngineLimit,
    EffectCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenEstimateUnknownReason {
    MissingRootFragment,
    FallbackRowEstimate,
    MissingCostEstimate,
    NonFinite,
    Negative,
    NotProjected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FrozenCostValue {
    Known(f64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenCostValue {
    pub const fn known(self) -> Option<f64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrozenCostEstimate {
    root_rows: FrozenCostValue,
    cpu: FrozenCostValue,
    memory: FrozenCostValue,
    network: FrozenCostValue,
}

impl FrozenCostEstimate {
    pub const fn new(
        root_rows: FrozenCostValue,
        cpu: FrozenCostValue,
        memory: FrozenCostValue,
        network: FrozenCostValue,
    ) -> Self {
        Self {
            root_rows,
            cpu,
            memory,
            network,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
        )
    }

    pub const fn root_rows(self) -> FrozenCostValue {
        self.root_rows
    }

    pub const fn cpu(self) -> FrozenCostValue {
        self.cpu
    }

    pub const fn memory(self) -> FrozenCostValue {
        self.memory
    }

    pub const fn network(self) -> FrozenCostValue {
        self.network
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenResourceValue {
    Known(u64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenResourceValue {
    pub const fn known(self) -> Option<u64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionResourceRequirements {
    minimum_memory_bytes: FrozenResourceValue,
}

impl ExecutionResourceRequirements {
    pub const fn new(minimum_memory_bytes: FrozenResourceValue) -> Self {
        Self {
            minimum_memory_bytes,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(FrozenResourceValue::Unknown(reason))
    }

    pub const fn minimum_memory_bytes(self) -> FrozenResourceValue {
        self.minimum_memory_bytes
    }
}

/// Immutable semantic input shared by every attempt of one completed plan.
#[derive(Clone, Debug)]
pub struct FrozenExecutionDescription {
    plan: crate::api::PlanSeal,
    candidate: CompletedPhysicalPlanCandidate,
    kind: QueryExecutionKind,
    scan_identities: Arc<[crate::api::PlanScanIdentity]>,
    output: OutputContract,
    row_carrier: Option<crate::api::ResultRowCarrier>,
    finish_boundary: ExecutionFinishBoundary,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
    residuals: Arc<[ResidualResponsibility]>,
    cost: FrozenCostEstimate,
    resources: ExecutionResourceRequirements,
}

impl FrozenExecutionDescription {
    /// Freeze the exact candidate and scan cover as one logical execution.
    pub fn for_completed_plan(
        kind: QueryExecutionKind,
        candidate: super::CompletedPhysicalPlanCandidate,
        scan_identities: Vec<crate::api::PlanScanIdentity>,
        output: OutputContract,
        effect: ExecutionEffect,
        recovery: RecoveryMode,
        residuals: Vec<ResidualResponsibility>,
        cost: FrozenCostEstimate,
        resources: ExecutionResourceRequirements,
    ) -> Result<Self, String> {
        validate_frozen_cost(cost)?;
        validate_effect_recovery(effect, recovery)?;
        let plan = candidate.plan().version();
        let expected_output = OutputContract::from_completed_candidate(kind, &candidate)?;
        let row_carrier = OutputContract::row_carrier_from_plan(candidate.plan())?;
        if matches!(output, OutputContract::Rows(_)) && row_carrier.is_none() {
            return Err("row output requires a frozen root result sink".into());
        }
        if output.fields() != expected_output.fields()
            || matches!(output, OutputContract::CompletionOnly)
                != matches!(expected_output, OutputContract::CompletionOnly)
        {
            return Err(
                "completed plan output differs from its frozen execution contract".to_string(),
            );
        }
        let expected_scans = candidate
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| {
                fragment
                    .nodes()
                    .values()
                    .filter(|&node| {
                        matches!(&node.kind, novarocks_physical_plan::NodeKind::Scan { .. })
                    })
                    .map(|node| {
                        let node_id = i32::try_from(node.id.get()).map_err(|_| {
                            "completed plan scan node id exceeds native scheduling range"
                                .to_string()
                        })?;
                        Ok(crate::api::PlanScanIdentity::new(
                            crate::api::PlanSeal::Version(plan),
                            fragment.id().get(),
                            node_id,
                        ))
                    })
            })
            .collect::<Result<std::collections::BTreeSet<_>, String>>()?;
        let supplied_scans = scan_identities
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if supplied_scans.len() != scan_identities.len() || supplied_scans != expected_scans {
            return Err(
                "completed plan scans differ from its frozen execution contract".to_string(),
            );
        }
        Ok(Self {
            plan: crate::api::PlanSeal::Version(plan),
            candidate,
            kind,
            scan_identities: scan_identities.into(),
            finish_boundary: match &output {
                OutputContract::Rows(_) => ExecutionFinishBoundary::ClientEof,
                OutputContract::CompletionOnly => ExecutionFinishBoundary::TaskCompletion,
            },
            output,
            row_carrier,
            effect,
            recovery,
            residuals: residuals.into(),
            cost,
            resources,
        })
    }

    /// Freezes internal consumption without changing the statement's public
    /// output, exact carrier, effect or recovery policy. A registered domain
    /// consumer must prove completion before the actor can accept readiness.
    pub fn into_internal_computation(mut self) -> Result<Self, String> {
        if self.row_carrier.is_none() {
            return Err("internal computation requires a frozen root result carrier".into());
        }
        self.finish_boundary = ExecutionFinishBoundary::ComputationReady;
        Ok(self)
    }

    pub const fn finish_boundary(&self) -> ExecutionFinishBoundary {
        self.finish_boundary
    }

    /// Check the exact intrinsic values before legacy native submission folds
    /// them into one admitted runtime option. Every definition is observed,
    /// including definitions that consume no parameter.
    pub fn validate_legacy_intrinsic_allow_throw_exception(
        &self,
        actual: bool,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_sql::compiler::SqlCompileError> {
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, SemanticParameterValue};

        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let plan = self.candidate.plan();
        let result = (|| -> Result<(), SqlCompileError> {
            for fragment in plan.fragments().values() {
                for (_, definition) in fragment.expressions().iter() {
                    let checked = (|| -> Result<(), SqlCompileError> {
                        for reference in definition.kind.intrinsic_parameter_references() {
                            let value = plan.parameters().require(*reference).map_err(|error| {
                                SqlCompileError::InvalidRequest(error.to_string())
                            })?;
                            if value != &SemanticParameterValue::AllowThrowException(actual) {
                                return Err(SqlCompileError::InvalidRequest(
                                    "legacy native runtime ALLOW_THROW_EXCEPTION differs from the exact intrinsic parameter".into(),
                                ));
                            }
                        }
                        Ok(())
                    })();
                    // The lookup/comparison is completed even on ordinary refusal.
                    work.step()?;
                    checked?;
                }
                work.step()?;
            }
            Ok(())
        })();
        if matches!(
            &result,
            Err(SqlCompileError::Cancelled
                | SqlCompileError::DeadlineExceeded
                | SqlCompileError::ResourceExhausted)
        ) {
            return result;
        }
        work.finish()?;
        result
    }

    pub const fn kind(&self) -> QueryExecutionKind {
        self.kind
    }

    pub(crate) const fn plan_identity(&self) -> crate::api::PlanSeal {
        self.plan
    }

    pub(crate) const fn completed_candidate(&self) -> &CompletedPhysicalPlanCandidate {
        &self.candidate
    }

    pub fn scan_identities(&self) -> &[crate::api::PlanScanIdentity] {
        &self.scan_identities
    }

    pub fn matches_plan_seal(&self, plan: crate::api::PlanSeal) -> bool {
        self.plan_identity() == plan
    }

    pub const fn output(&self) -> &OutputContract {
        &self.output
    }
    /// The exact frozen typed scalar contract, without exposing the plan owner.
    pub fn scalar_schema(&self) -> Option<&novarocks_result_contract::ScalarSchema> {
        use novarocks_physical_plan::FragmentSink;
        use novarocks_result_contract::FrozenRootOutput;
        let plan = self.candidate.plan();
        let fragment = plan.fragments().get(&plan.result_port()?.fragment)?;
        match fragment.sink() {
            FragmentSink::RootResult(contract) => match contract.output() {
                FrozenRootOutput::ScalarValue(schema) => Some(schema),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn row_carrier(&self) -> Result<crate::api::ResultRowCarrier, String> {
        self.row_carrier
            .ok_or_else(|| "execution has no root row carrier".into())
    }
    pub const fn effect(&self) -> ExecutionEffect {
        self.effect
    }
    pub const fn recovery(&self) -> RecoveryMode {
        self.recovery
    }
    pub fn residuals(&self) -> &[ResidualResponsibility] {
        &self.residuals
    }
    pub const fn cost(&self) -> FrozenCostEstimate {
        self.cost
    }
    pub const fn resources(&self) -> ExecutionResourceRequirements {
        self.resources
    }
}

/// Every known cost is a finite, non-negative quantity.
fn validate_frozen_cost(cost: FrozenCostEstimate) -> Result<(), String> {
    for (name, value) in [
        ("root rows", cost.root_rows()),
        ("cpu", cost.cpu()),
        ("memory", cost.memory()),
        ("network", cost.network()),
    ] {
        if let FrozenCostValue::Known(value) = value
            && (!value.is_finite() || value < 0.0)
        {
            return Err(format!(
                "frozen {name} cost must be finite and non-negative"
            ));
        }
    }
    Ok(())
}

/// An execution that commits outside this process cannot be retried by
/// running it again.
fn validate_effect_recovery(effect: ExecutionEffect, recovery: RecoveryMode) -> Result<(), String> {
    if matches!(effect, ExecutionEffect::External) && !matches!(recovery, RecoveryMode::NoRecovery)
    {
        return Err("external effects cannot use replay recovery".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn row_carrier_is_frozen_from_the_completed_root_sink() {
        use novarocks_result_contract::{
            FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
        };
        let completed = crate::completed_plan_fixture::completed_values_plan([42; 16]).await;
        let plan = completed.candidate().plan();
        assert!(
            OutputContract::row_carrier_from_plan(plan)
                .unwrap_err()
                .contains("frozen root purpose")
        );
        let completion = crate::completed_plan_fixture::completed_noop_plan([43; 16]);
        assert_eq!(
            OutputContract::row_carrier_from_plan(completion.candidate().plan()).unwrap(),
            None
        );
        for output in [
            FrozenRootOutput::CountOnly,
            FrozenRootOutput::InternalFacts(InternalResultDomain::CowSelectionArrowV1),
        ] {
            let kind = output.kind();
            let plan = plan
                .as_ref()
                .clone()
                .with_root_output(RootOutputContract::new(RootProfileId::V1, output))
                .unwrap();
            assert_eq!(
                OutputContract::row_carrier_from_plan(&plan).unwrap(),
                Some(crate::api::ResultRowCarrier::relayed(kind, None).unwrap())
            );
        }
    }

    #[tokio::test]
    async fn m07_completed_output_preserves_exact_producer_domains() {
        // The completed-output contract owns domain and alias preservation, not the
        // unavailable to_bitmap scalar. Source-free bitmap_agg preserves Bitmap;
        // bitmap_to_string independently crosses from that domain to Physical.
        use novarocks_type_contract::ValueLogicalType as T;
        for (sql, domain) in [
            ("select json_object('k', 1) as value", T::Json),
            ("select bitmap_agg(1) as value", T::Bitmap),
            ("select hll_hash(cast(1 as bigint)) as value", T::Hll),
            (
                "select percentile_hash(cast(1 as double)) as value",
                T::Percentile,
            ),
            (
                "select bitmap_to_string(bitmap_agg(1)) as value",
                T::Physical,
            ),
            (
                "select cast(json_object('k', 1) as varchar) as value",
                T::Physical,
            ),
        ] {
            let completed =
                crate::completed_plan_fixture::completed_literal_query([41; 16], sql).await;
            let port = completed.candidate().plan().result_port().unwrap();
            assert_eq!(
                port.fields[0].ty.logical_type(),
                domain,
                "{sql}: {:?}",
                port.fields[0]
            );
            let output = OutputContract::from_completed_plan(
                QueryExecutionKind::Read,
                completed.candidate().plan(),
            )
            .unwrap();
            assert_eq!(output.fields().len(), 1, "{sql}");
            assert_eq!(output.fields()[0].name(), "value", "{sql}");
            assert_eq!(output.fields()[0].logical_type(), domain, "{sql}");
        }
    }

    #[tokio::test]
    async fn completed_output_preserves_nested_semantics_in_independent_schema() {
        use novarocks_type_contract::{ValueKind, ValueLogicalType as L};
        let completed = crate::completed_plan_fixture::completed_literal_query(
            [42; 16],
            "select [parse_json('{}')] as nested",
        )
        .await;
        let output = OutputContract::from_completed_plan(
            QueryExecutionKind::Read,
            completed.candidate().plan(),
        )
        .unwrap();
        let producer = &completed
            .candidate()
            .original_public_result_port()
            .unwrap()
            .fields[0]
            .ty;
        assert_eq!(output.fields()[0].value_type(), producer);
        drop(completed);
        let ValueKind::List(item) = output.fields()[0].value_type().value_type().kind() else {
            panic!("completed list declaration lost");
        };
        assert_eq!(item.kind(), &ValueKind::Nominal(L::Json));
        assert_eq!(output.fields()[0].name(), "nested");
    }

    #[tokio::test]
    async fn completed_description_rejects_a_substituted_output_or_scan() {
        let candidate = crate::completed_plan_fixture::completed_values_plan([13; 16])
            .await
            .candidate()
            .clone();
        let raw_output =
            OutputContract::from_completed_plan(QueryExecutionKind::Read, candidate.plan())
                .unwrap();
        let raw_error = FrozenExecutionDescription::for_completed_plan(
            QueryExecutionKind::Read,
            candidate.clone(),
            Vec::new(),
            raw_output,
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap_err();
        assert!(raw_error.contains("frozen root purpose"), "{raw_error}");
        let render = novarocks_sql::compiler::client_render_schema(
            candidate.plan(),
            novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 0,
            },
        )
        .unwrap();
        let candidate = candidate
            .freeze_root_output(novarocks_result_contract::FrozenRootOutput::ClientRows(
                render,
            ))
            .unwrap();
        let output =
            OutputContract::from_completed_plan(QueryExecutionKind::Read, candidate.plan())
                .expect("values plan has a row result");
        let freeze = |candidate, scans, output| {
            FrozenExecutionDescription::for_completed_plan(
                QueryExecutionKind::Read,
                candidate,
                scans,
                output,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            )
        };
        assert!(
            freeze(
                candidate.clone(),
                Vec::new(),
                OutputContract::CompletionOnly
            )
            .is_err()
        );
        assert!(
            freeze(
                candidate.clone(),
                vec![crate::api::PlanScanIdentity::new(
                    crate::api::PlanSeal::Version(candidate.plan().version()),
                    1,
                    1,
                )],
                output.clone(),
            )
            .is_err()
        );
        assert!(freeze(candidate, Vec::new(), output).is_ok());
    }

    fn intrinsic_description(values: &[bool], nonconsumers: usize) -> FrozenExecutionDescription {
        use arrow::datatypes::DataType;
        use novarocks_physical_plan::{
            ExprKind, FragmentBuilder, FragmentId, FragmentSink, LiteralValue, PipelineDopDomain,
            PlanBuilder, PlanVersionId, ValueOrigin, ValueType,
        };
        use novarocks_type_contract::{
            DecimalOverflowPolicy, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
            SemanticParameterValue, SemanticParameters,
        };
        let mut fragment = FragmentBuilder::new(FragmentId::new(7));
        let node = fragment.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let mut row = Vec::new();
        for ordinal in 0..nonconsumers {
            row.push(
                fragment
                    .add_expression(
                        node,
                        ty.clone(),
                        ExprKind::Literal(LiteralValue::Int64(ordinal as i64)),
                    )
                    .unwrap(),
            );
        }
        let ids = [0, u32::MAX];
        assert!(values.len() <= ids.len());
        for (ordinal, _) in values.iter().enumerate() {
            let literal = fragment
                .add_expression(node, ty.clone(), ExprKind::Literal(LiteralValue::Int64(13)))
                .unwrap();
            row.push(
                fragment
                    .add_expression(
                        node,
                        ty.clone(),
                        ExprKind::Cast {
                            expr: literal,
                            target: DataType::Int64,
                            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                            allow_throw_exception: SemanticParameterRef {
                                id: SemanticParameterId::new(ids[ordinal]),
                                expected_key: SemanticParameterKey::AllowThrowException,
                            },
                        },
                    )
                    .unwrap(),
            );
        }
        let output = (0..row.len())
            .map(|ordinal| {
                fragment
                    .add_value(
                        ty.clone(),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: ordinal as u32,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        fragment
            .add_values(
                node,
                Box::from([row.into_boxed_slice()]),
                output.into_boxed_slice(),
            )
            .unwrap();
        let fragment = fragment
            .finish_definition(
                node,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let parameters =
            SemanticParameters::try_new(values.iter().enumerate().map(|(ordinal, value)| {
                (
                    SemanticParameterId::new(ids[ordinal]),
                    SemanticParameterValue::AllowThrowException(*value),
                )
            }))
            .unwrap();
        let mut builder = PlanBuilder::new(PlanVersionId::try_new([73; 16]).unwrap())
            .with_semantic_parameters(parameters);
        builder.add_fragment(fragment).unwrap();
        let candidate = CompletedPhysicalPlanCandidate::for_program(
            builder.finish().unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        FrozenExecutionDescription::for_completed_plan(
            QueryExecutionKind::Write,
            candidate,
            Vec::new(),
            OutputContract::CompletionOnly,
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap()
    }

    struct IntrinsicTrace {
        calls: std::sync::Mutex<Vec<(novarocks_type_contract::CompilePhase, u32)>>,
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    }
    impl novarocks_type_contract::PureCompileControl for IntrinsicTrace {
        fn checkpoint(
            &self,
            phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            match self.refuse.clone() {
                Some((ordinal, cause)) if calls.len() == ordinal + 1 => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn intrinsic_trace(
        refuse: Option<(usize, novarocks_type_contract::CompileControlError)>,
    ) -> IntrinsicTrace {
        IntrinsicTrace {
            calls: std::sync::Mutex::new(Vec::new()),
            refuse,
        }
    }

    #[test]
    fn legacy_intrinsic_gate_checks_sparse_scoped_values_against_actual_runtime_bool() {
        use novarocks_sql::compiler::SqlCompileError;
        for actual in [false, true] {
            let description = intrinsic_description(&[actual, actual], 1);
            assert!(
                description
                    .validate_legacy_intrinsic_allow_throw_exception(actual, &intrinsic_trace(None))
                    .is_ok()
            );
            assert!(matches!(
                description.validate_legacy_intrinsic_allow_throw_exception(
                    !actual,
                    &intrinsic_trace(None)
                ),
                Err(SqlCompileError::InvalidRequest(_))
            ));
            assert!(matches!(
                intrinsic_description(&[false, true], 1)
                    .validate_legacy_intrinsic_allow_throw_exception(
                        actual,
                        &intrinsic_trace(None)
                    ),
                Err(SqlCompileError::InvalidRequest(_))
            ));
            assert!(
                intrinsic_description(&[], 3)
                    .validate_legacy_intrinsic_allow_throw_exception(actual, &intrinsic_trace(None))
                    .is_ok()
            );
        }
    }

    #[test]
    fn legacy_intrinsic_gate_observes_nonconsumer_definitions_and_preserves_every_control_prefix() {
        use novarocks_type_contract::CompileControlError;
        let description = intrinsic_description(&[true], 320);
        let trace = intrinsic_trace(None);
        description
            .validate_legacy_intrinsic_allow_throw_exception(true, &trace)
            .unwrap();
        let success = trace.calls.into_inner().unwrap();
        assert_eq!(success.first().unwrap().1, 0);
        assert!(success.iter().any(|(_, units)| *units == 256));
        assert!(success.last().unwrap().1 > 0);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for ordinal in 0..success.len() {
                let trace = intrinsic_trace(Some((ordinal, cause.clone())));
                let error = description
                    .validate_legacy_intrinsic_allow_throw_exception(true, &trace)
                    .unwrap_err();
                assert_eq!(
                    error,
                    novarocks_sql::compiler::SqlCompileError::from(cause.clone())
                );
                assert_eq!(*trace.calls.lock().unwrap(), success[..=ordinal]);
            }
        }
    }

    #[test]
    fn legacy_intrinsic_gate_ordinary_refusal_observes_tail_and_keeps_primary_control() {
        use novarocks_type_contract::CompileControlError;
        let description = intrinsic_description(&[true], 3);
        let trace = intrinsic_trace(None);
        assert!(matches!(
            description.validate_legacy_intrinsic_allow_throw_exception(false, &trace),
            Err(novarocks_sql::compiler::SqlCompileError::InvalidRequest(_))
        ));
        let refused = trace.calls.into_inner().unwrap();
        assert_eq!(refused.len(), 2);
        assert!(refused[1].1 > 0);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for ordinal in 0..refused.len() {
                let trace = intrinsic_trace(Some((ordinal, cause.clone())));
                assert_eq!(
                    description
                        .validate_legacy_intrinsic_allow_throw_exception(false, &trace)
                        .unwrap_err(),
                    novarocks_sql::compiler::SqlCompileError::from(cause.clone())
                );
                assert_eq!(*trace.calls.lock().unwrap(), refused[..=ordinal]);
            }
        }
    }

    #[test]
    fn cost_and_effect_recovery_are_checked_before_publication() {
        assert!(
            validate_frozen_cost(FrozenCostEstimate::new(
                FrozenCostValue::Known(f64::NAN),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
                FrozenCostValue::Unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .is_err()
        );
        assert!(
            validate_effect_recovery(
                ExecutionEffect::External,
                RecoveryMode::RestartAttemptBeforeVisibility,
            )
            .is_err()
        );
    }
}
