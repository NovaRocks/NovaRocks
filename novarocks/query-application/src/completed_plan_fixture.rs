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

//! One really completed plan, for tests of what happens to a plan afterwards.
//!
//! It is produced by the real completion driver rather than assembled, because
//! the things tested against it - pairing, activation, dispatch - are about a
//! plan that has actually been validated and paired with its capabilities.
//! A statement over literal rows needs no outside fact, so the fixture needs no
//! catalog, no provider and no statistics behind it.

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorReadRelationPayload, ConnectorReadWorkSource,
};
use novarocks_physical_plan::{
    Distribution, ExactInputVersion, FragmentBuilder, FragmentId, FragmentSink,
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, MetadataRelation, MetadataRelationKind, NodeKind,
    PhysicalProperties, PipelineDopDomain, PlanBuilder, PlanVersionId, ProviderColumnReference,
    ProviderReadOccurrenceId, ProviderReadReference, Relation, RelationField, RowMultiplicity,
    ScanReadBudget, ValueOrigin, ValueType,
};
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
    SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};
use novarocks_workload_control::{
    ResourceConfig, RootWork, WorkClass, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
};

use crate::preparation::{
    CompletedPhysicalPlanCandidate, CompletedPlanWithAccess, FinalPlanCompletionDriver,
    FinalPlanRuntimeAccess, ReadAccessSink, SqlCompletionFactSource,
};

struct NoFacts;

#[async_trait::async_trait]
impl SqlCompletionFactSource for NoFacts {
    type Access = ();

    async fn resolve(
        &self,
        _: &SqlNeedBatch,
        _: &ReadAccessSink<()>,
    ) -> Result<SqlFactBatch, String> {
        panic!("a VALUES plan asks for nothing")
    }
}

/// A completed plan over literal rows, carrying the given version.
pub(crate) async fn completed_values_plan(version: [u8; 16]) -> CompletedPlanWithAccess<()> {
    let (_root, scope) = query_scope();
    FinalPlanCompletionDriver::new(Arc::new(NoFacts))
        .complete(values_request(version), &scope)
        .await
        .unwrap_or_else(|error| panic!("VALUES completes without facts: {error}"))
}

/// Synchronous entry for lifecycle tests whose request factories are not async.
pub(crate) fn completed_values_plan_blocking(version: [u8; 16]) -> CompletedPlanWithAccess<()> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fixture runtime")
            .block_on(completed_values_plan(version))
    })
    .join()
    .expect("fixture thread")
}

/// A validated internal program with no client rows or provider reads.
pub(crate) fn completed_noop_plan(version: [u8; 16]) -> CompletedPlanWithAccess<()> {
    let mut fragment = FragmentBuilder::new(FragmentId::new(0));
    let root = fragment.reserve_node_id().expect("node id");
    fragment
        .add_values(root, Box::default(), Box::default())
        .expect("empty values");
    let fragment = fragment
        .finish_definition(
            root,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: true,
            },
        )
        .expect("no-op fragment");
    let mut builder = PlanBuilder::new(PlanVersionId::try_new(version).expect("plan version"));
    builder.add_fragment(fragment).expect("one fragment");
    let candidate = CompletedPhysicalPlanCandidate::for_program(builder.finish().expect("plan"))
        .expect("validated program");
    CompletedPlanWithAccess::try_pair(candidate, FinalPlanRuntimeAccess::default())
        .expect("no provider reads")
}

/// A validated scan candidate for native scan-work cover tests.
pub(crate) fn completed_scan_candidate(version: [u8; 16]) -> CompletedPhysicalPlanCandidate {
    let provider_id = ConnectorProviderId::parse("iceberg").expect("provider id");
    let instance_id = ConnectorInstanceId::parse("fixture").expect("instance id");
    let catalog = CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([3; 32]));
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider_id.clone(),
            instance_id,
        },
        catalog.clone(),
    );
    let encoded = |category, byte| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider_id.clone(),
                catalog.clone(),
                category,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            vec![byte].into(),
        )
    };
    let field = ProviderColumnReference {
        column_payload: encoded(ConnectorCodecCategory::ReadColumn, 33),
    };
    let properties = PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let relation = Relation::Metadata(MetadataRelation {
        kind: MetadataRelationKind::try_new("iceberg.manifest.entries").expect("relation kind"),
        read: ProviderReadReference {
            binding,
            input_version: ExactInputVersion::try_new(vec![9]).expect("input version"),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::SystemTable,
                encoded(ConnectorCodecCategory::ReadTable, 1),
                encoded(ConnectorCodecCategory::ReadView, 2),
            ),
        },
        work_source: ConnectorReadWorkSource::RuntimeSplits,
        selection_digest: [8; 32],
        schema: Box::from([RelationField {
            column: field.clone(),
            ty: ValueType::new(DataType::Int64, false),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: properties,
        artifact_inputs: Box::default(),
        coverage_evidence: Box::from([4]),
    });
    let mut fragment = FragmentBuilder::new(FragmentId::new(1));
    let root = fragment.reserve_node_id().expect("scan node id");
    let value = fragment
        .add_value(
            ValueType::new(DataType::Int64, false),
            ValueOrigin::ProviderField {
                scan_node: root,
                field: field.clone(),
            },
        )
        .expect("scan output");
    fragment
        .add_scan(
            root,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: ScanReadBudget {
                    max_batch_rows: 4096,
                    max_batch_bytes: 8 * 1024 * 1024,
                },
                provider_outputs: Box::from([(field, value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
            Box::from([value]),
        )
        .expect("scan node");
    let fragment = fragment
        .finish_definition(
            root,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
        )
        .expect("scan fragment");
    let mut builder = PlanBuilder::new(PlanVersionId::try_new(version).expect("plan version"));
    builder.add_fragment(fragment).expect("one fragment");
    CompletedPhysicalPlanCandidate::for_program(builder.finish().expect("scan plan"))
        .expect("validated scan candidate")
}

fn query_scope() -> (RootWork, WorkScope) {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1024,
            control_bytes: 128,
            per_scope_bytes: 896,
        },
    )
    .expect("workload control");
    control.mark_ready().expect("workload control ready");
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .expect("query root");
    let scope = root.owner.scope();
    (root, scope)
}

fn values_request(version: [u8; 16]) -> SqlFinalPlanCompileRequest {
    SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new(version).expect("plan version"),
        SqlStatementInput::sql("SELECT 1 AS a, CAST(NULL AS VARCHAR) AS b"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: Some("iceberg".to_string()),
            current_database: "db".to_string(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        noop_constant_evaluator(),
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: MAX_SCAN_BATCH_ROWS,
            max_batch_bytes: MAX_SCAN_BATCH_BYTES,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
}
