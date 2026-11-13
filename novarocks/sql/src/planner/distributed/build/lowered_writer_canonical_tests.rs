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

use super::super::lowered_draft::{
    CheckedWriterAggregateLogicalSourceEntry, SqlSourceJournalError,
};
use super::tests::{column, dop, literal_int, values, version, write_handle};
use super::*;
use crate::compiler::{SqlAuthoredPhysicalPlan, SqlFunctionCatalog};
use crate::planner::distributed::write::auxiliary::{
    WriterStatisticsTargetInput, plan_writer_statistics,
};
use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;
use arrow::datatypes::{Field, Schema};
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_AGGREGATE_NAME, ICEBERG_THETA_STATE_FORMAT_IDENTITY, iceberg_theta_registration,
};
use novarocks_functions::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResolutionError, FunctionResultType, FunctionVolatility, ResolvedAggregateSignature,
    ResolvedFunctionSignature,
};
use novarocks_physical_plan::{PhysicalCallSite, PhysicalNode};
use novarocks_spi::connector::{
    StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn catalogue() -> Arc<dyn SqlFunctionCatalog> {
    let registration = iceberg_theta_registration().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(registration.definition().clone()).unwrap();
    Arc::new(builder.seal_bound().unwrap())
}
fn fixture(
    nullable: bool,
) -> (
    PhysicalPlanNode,
    WriterAuxiliaryPlan,
    Arc<dyn SqlFunctionCatalog>,
) {
    let functions = catalogue();
    // The provider-authored statistics declaration retains its original root.
    let schema = Schema::new(vec![Field::new("order_id", DataType::Int64, false)]);
    let requirement = StatisticsRequiredAggregation::try_new(
        StatisticsScanColumn::try_new(
            0,
            "order_id",
            FunctionValueType::new(DataType::Int64, false),
        )
        .unwrap(),
        ICEBERG_THETA_AGGREGATE_NAME,
        StatisticsArtifactIdentity::try_new(vec![1], "test-writer-stat-v1").unwrap(),
    )
    .unwrap();
    let declared_input =
        simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal);
    let auxiliary = plan_writer_statistics(
        &[WriterStatisticsTargetInput {
            target: WriteTargetOrdinal::try_new(0).unwrap(),
            input_schema: &schema,
            input_columns: &declared_input.contract.input_columns,
            requirements: &[requirement],
        }],
        functions.as_ref(),
        DecimalOverflowPolicy::OutputNull,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    let source = values(
        vec![column(1, "order_id", DataType::Int64, nullable)],
        vec![vec![literal_int(7)]],
    );
    (source, auxiliary, functions)
}
fn authored(
    source: &PhysicalPlanNode,
    auxiliary: &WriterAuxiliaryPlan,
    functions: Arc<dyn SqlFunctionCatalog>,
    control: &dyn PureCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, ContractLoweringError> {
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    lower_final_physical_write_plan(
        source,
        version(),
        dop(),
        FinalWriteLowering {
            reads: None,
            write: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
            write_target_ordinal: ordinal,
            auxiliary,
            targets: FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap(),
        },
        functions,
        false,
        crate::constant::test_constant_policy(),
        control,
    )?
    .finish_observed(control)
    .map_err(ContractLoweringError::from)
}
fn writer_call(
    owner: &SqlAuthoredPhysicalPlan,
    partial: bool,
) -> (
    &Fragment,
    &PhysicalNode,
    PhysicalCallSite,
    &WriterAggregateCall,
) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::TableWriter { target } if partial => {
                    assert_eq!(target.partial_aggregates.len(), 1);
                    return (
                        fragment,
                        node,
                        PhysicalCallSite::WriterPartial {
                            node: node.id,
                            call: 0,
                        },
                        &target.partial_aggregates[0],
                    );
                }
                NodeKind::TableFinish(finish) if !partial => {
                    assert_eq!(finish.final_aggregates.len(), 1);
                    return (
                        fragment,
                        node,
                        PhysicalCallSite::WriterFinal {
                            node: node.id,
                            call: 0,
                        },
                        &finish.final_aggregates[0],
                    );
                }
                _ => {}
            }
        }
    }
    panic!("missing actual writer lifecycle");
}
fn entry(
    owner: &SqlAuthoredPhysicalPlan,
    partial: bool,
) -> CheckedWriterAggregateLogicalSourceEntry<'_> {
    let (fragment, node, site, call) = writer_call(owner, partial);
    let control = crate::compiler::SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let entry = owner
        .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
        .unwrap();
    work.finish().unwrap();
    entry
}
fn assert_argument(argument: &FunctionArgument, nullable: bool) {
    let FunctionArgument::Value {
        value_type,
        constant,
    } = argument
    else {
        panic!("writer source is a Value channel")
    };
    assert_eq!(
        value_type,
        &FunctionValueType::new(DataType::Int64, nullable)
    );
    assert!(
        constant.is_none(),
        "a Values row literal does not make a column source constant"
    );
}

#[test]
fn real_theta_writer_partial_canonical_and_final_original_capture_are_distinct() {
    let (source, auxiliary, functions) = fixture(false);
    let owner = authored(&source, &auxiliary, functions.clone(), &Control::default()).unwrap();
    assert!(Arc::ptr_eq(owner.function_catalog(), &functions));
    let partial = entry(&owner, true);
    let canonical = partial.canonical().unwrap();
    assert!(canonical.belongs_to(partial.captured()));
    assert_eq!(
        partial.captured().binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::OutputNull
    );
    assert_eq!(
        canonical.selected().overload.as_str(),
        "iceberg/theta-stat/long/v1"
    );
    assert_eq!(
        canonical.selected().overload,
        partial.source().binding.function.overload
    );
    assert!(matches!(partial.phase(), AggregatePhase::Partial { .. }));
    assert_eq!(partial.runtime(), AggregateRuntimeDemand::Update);
    assert_eq!(canonical.request().logical_argument_count, 1);
    assert_argument(&partial.captured().request().arguments[0], false);
    assert_argument(&canonical.request().arguments[0], false);
    assert_eq!(
        canonical.selected().argument_types.as_ref(),
        partial.source().binding.function.argument_types.as_ref()
    );
    assert_eq!(
        canonical.selected().result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Binary, false))
    );
    let aggregate = canonical.selected().aggregate.as_ref().unwrap();
    assert_eq!(
        aggregate.intermediate_type,
        FunctionValueType::new(DataType::Binary, false)
    );
    assert_eq!(
        aggregate.state_format.as_str(),
        ICEBERG_THETA_STATE_FORMAT_IDENTITY
    );
    assert_eq!(
        partial.fragment().values()[&partial.source().input].ty,
        FunctionValueType::new(DataType::Int64, false)
    );
    assert_eq!(
        partial.fragment().values()[&partial.source().output].ty,
        FunctionValueType::new(DataType::Binary, true)
    );
    assert!(std::ptr::eq(partial.source(), writer_call(&owner, true).3));
    assert_eq!(partial.site(), writer_call(&owner, true).2);
    assert!(std::ptr::eq(partial.node(), writer_call(&owner, true).1));
    let final_call = entry(&owner, false);
    let final_canonical = final_call.canonical().unwrap();
    assert!(final_canonical.belongs_to(final_call.captured()));
    assert!(!Arc::ptr_eq(final_canonical, canonical));
    assert_argument(&final_canonical.request().arguments[0], false);
    assert_eq!(
        final_canonical.selected().argument_types.as_ref(),
        final_call.source().binding.function.argument_types.as_ref()
    );
    assert_eq!(
        final_canonical.selected().result_type,
        FunctionResultType::Scalar(final_call.source().binding.function.result_type.clone())
    );
    assert!(matches!(final_call.phase(), AggregatePhase::Final { .. }));
    assert_eq!(
        final_call.runtime(),
        AggregateRuntimeDemand::WriterState(final_call.source().input)
    );
    assert_argument(&final_call.captured().request().arguments[0], false);
    assert_eq!(
        final_call.source().binding.intermediate_type,
        FunctionValueType::new(DataType::Binary, false)
    );
    assert_eq!(
        final_call.source().binding.state_format.as_str(),
        ICEBERG_THETA_STATE_FORMAT_IDENTITY
    );
    assert_eq!(
        partial.captured().constant_policy(),
        final_call.captured().constant_policy()
    );
}

#[test]
fn writer_update_uses_checked_materialized_input_without_replacing_source_or_capture() {
    let (source, auxiliary, functions) = fixture(true);
    let owner = authored(&source, &auxiliary, functions, &Control::default()).unwrap();
    let partial = entry(&owner, true);
    let canonical = partial.canonical().unwrap();
    assert_argument(&partial.captured().request().arguments[0], false);
    // Required values are checked before materialization publishes W. The
    // original nullable source remains a separate value, never relabelled.
    assert_argument(&canonical.request().arguments[0], false);
    let NodeKind::TableWriter { target } = &partial.node().kind else {
        panic!("partial belongs to its writer");
    };
    let field = &target.target_fields[0];
    assert_eq!(partial.source().input, field.projected);
    assert_ne!(field.input, field.projected);
    assert!(partial.fragment().values()[&field.input].ty.nullable());
    assert!(
        !partial.fragment().values()[&partial.source().input]
            .ty
            .nullable()
    );
    assert_eq!(
        partial.source().binding.function.argument_types.as_ref(),
        canonical.selected().argument_types.as_ref()
    );
    assert!(canonical.belongs_to(partial.captured()));
    assert_argument(
        &entry(&owner, false).captured().request().arguments[0],
        false,
    );
    assert_eq!(
        partial.source().binding.intermediate_type,
        FunctionValueType::new(DataType::Binary, false)
    );
}

#[derive(Debug)]
struct WrongStateCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
}
impl SqlFunctionCatalog for WrongStateCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(Self {
            inner: self.inner.clone(),
        })
    }
    fn select_exact_overload_observed(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
        let selected = self
            .inner
            .select_exact_overload_observed(function, kind, overload, request, control)?;
        let mut bad = selected.as_ref().clone();
        // Deliberately adversarial metadata, not a Theta capability declaration.
        bad.aggregate.as_mut().unwrap().intermediate_type =
            FunctionValueType::new(DataType::Utf8, false);
        Ok(Arc::new(bad))
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.inner.resolve_scalar_signature(name, args, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.inner.contains_aggregate(name)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.inner.resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.inner.resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.inner.volatility(name)
    }
}
#[test]
fn writer_rejects_foreign_catalogue_and_returned_state_domain_without_retagging_auxiliary() {
    let (source, auxiliary, functions) = fixture(false);
    let unknown = authored(
        &source,
        &auxiliary,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        &Control::default(),
    );
    assert!(
        matches!(unknown, Err(ContractLoweringError::InvalidFunctionBinding {detail}) if detail == "canonical call selection: function is not registered"),
        "the exact retained identity must be installed"
    );
    let bad = authored(
        &source,
        &auxiliary,
        Arc::new(WrongStateCatalog {
            inner: functions.clone(),
        }),
        &Control::default(),
    );
    assert!(
        matches!(bad, Err(ContractLoweringError::InvalidWrite {detail}) if detail == "canonical writer state differs from its original auxiliary slot")
    );
    for rejected_catalogue in [
        crate::functions::builtin_sql_function_catalog().snapshot(),
        Arc::new(WrongStateCatalog { inner: functions }) as Arc<dyn SqlFunctionCatalog>,
    ] {
        let baseline = Control::default();
        assert!(authored(&source, &auxiliary, rejected_catalogue.clone(), &baseline).is_err());
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.iter().any(|(_, units)| *units > 0));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(
                    authored(&source, &auxiliary, rejected_catalogue.clone(), &control),
                    Err(ContractLoweringError::Control(actual)) if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn writer_checked_source_refuses_equal_foreign_owner_and_observes_every_prefix() {
    let (source, auxiliary, functions) = fixture(false);
    let owner = authored(&source, &auxiliary, functions.clone(), &Control::default()).unwrap();
    let foreign = authored(&source, &auxiliary, functions, &Control::default()).unwrap();
    let (fragment, node, site, call) = writer_call(&foreign, true);
    let run = |control: &dyn PureCompileControl| -> Result<(), SqlSourceJournalError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = owner
            .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
            .map(|_| ());
        if matches!(result, Err(SqlSourceJournalError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let baseline = Control::default();
    assert!(matches!(
        run(&baseline),
        Err(SqlSourceJournalError::InvalidSource(
            "aggregate journal loans a foreign plan or node"
        ))
    ));
    let trace = baseline.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    assert!(trace.iter().any(|(_, units)| *units > 0));
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(run(&control), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn actual_writer_lowering_preserves_each_original_control_refusal() {
    let (source, auxiliary, functions) = fixture(false);
    let baseline = Control::default();
    authored(&source, &auxiliary, functions.clone(), &baseline).unwrap();
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.iter().any(|(_, units)| *units > 0));
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(authored(&source,&auxiliary,functions.clone(),&control),Err(ContractLoweringError::Control(actual)) if actual == cause),
                "original refusal at {stop} must win"
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
