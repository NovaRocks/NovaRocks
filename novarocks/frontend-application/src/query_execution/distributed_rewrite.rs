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

//! Provider-neutral orchestration state for distributed table rewrites.
//!
//! The provider freezes its groups before this module is entered.  Core turns
//! those opaque group plans into one write session whose logical targets stand
//! one-to-one with them, keeps the exact composite lease alive, and commits the
//! union of every group's prepared write set once.  It intentionally knows
//! neither files, manifests, nor provider report formats.

use std::sync::Arc;

#[cfg(test)]
use novarocks_spi::connector::ConnectorWriteOperationId;
use novarocks_spi::connector::write_stack::{ConnectorWriteTargetPlan, WriteTargetOrdinal};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewriteReceipt, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorWriteCohortId, ConnectorWriteReceipt,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::query_execution::cohort_read::{QueryPinnedFileSetRead, QueryRewriteGroupRead};
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan,
};

/// Admit the synthetic source used by one pinned rewrite cohort read.
pub(crate) fn admit_pinned_rewrite_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    read: QueryPinnedFileSetRead,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::pinned_connector_read::admit_pinned_file_set_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
        read,
    )
}

/// Build the minimal physical source for one pinned rewrite cohort read.
/// Preparation freezes the relation restricted to exactly the files the
/// provider pinned for this cohort; no normal table lookup may run.
pub(crate) fn pinned_rewrite_scan_physical_plan(
    input_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    binding: SqlTableBindingId,
) -> Result<FrozenConnectorScanPlan, String> {
    crate::query_execution::pinned_connector_read::pinned_file_set_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

/// Admit the synthetic source used by one procedure cohort's group read.
pub(crate) fn admit_rewrite_group_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    read: QueryRewriteGroupRead,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::rewrite_group_read::admit_table_execute_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
        read,
    )
}

/// Build the minimal physical source for one procedure cohort's group read.
/// Preparation freezes the relation the group names; no normal table lookup
/// may run.
pub(crate) fn rewrite_group_scan_physical_plan(
    input_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    binding: SqlTableBindingId,
) -> Result<FrozenConnectorScanPlan, String> {
    crate::query_execution::rewrite_group_read::table_execute_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

/// State what one rewrite cohort's scan needs from the provider it reads.
///
/// The relation is the query-local synthetic name the cohort is planned
/// through, because that is what the plan's scan says; which cohort it names
/// is admitted beside the binding, and only the freeze puts the two together.
pub(crate) fn rewrite_cohort_provider_read_need(
    binding: SqlTableBindingId,
    input_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    cohort: &crate::catalog_application::query_bindings::QueryFrozenCohortRead,
) -> Result<novarocks_sql::compiler::ProviderReadNeed, String> {
    use crate::catalog_application::query_bindings::QueryFrozenCohortRead;

    let identity = frozen_rewrite_identity();
    let relation = novarocks_types::naming::TableIdentity::new(
        identity.catalog(),
        identity.namespace(),
        identity.table(),
    );
    let relation = match cohort {
        QueryFrozenCohortRead::PinnedFileSet(_) => {
            novarocks_sql::compiler::ProviderReadRelationNeed::PinnedFileSet { relation }
        }
        QueryFrozenCohortRead::TableExecute(_) => {
            novarocks_sql::compiler::ProviderReadRelationNeed::TableExecute { relation }
        }
    };
    novarocks_sql::compiler::ProviderReadNeed::for_program(
        REWRITE_COHORT_SCAN_OCCURRENCE,
        binding,
        relation,
        input_schema
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                (
                    Box::<str>::from(field.name().as_str()),
                    input_schema
                        .binding(ordinal)
                        .expect("validated connector execution schema covers every field")
                        .clone(),
                )
            }),
    )
    .map_err(|error| error.to_string())
}

/// The provider-read occurrence one rewrite cohort scans.
///
/// A cohort reads exactly one relation, so the occurrence that addresses it
/// is a constant rather than something an allocator has to hand out.
pub(crate) const REWRITE_COHORT_SCAN_OCCURRENCE: novarocks_physical_plan::ProviderReadOccurrenceId =
    novarocks_physical_plan::ProviderReadOccurrenceId::new(0);

fn frozen_rewrite_identity() -> FrozenConnectorScanIdentity {
    FrozenConnectorScanIdentity::new(
        "__distributed_rewrite",
        "__distributed_rewrite",
        "__connector_frozen_rewrite",
    )
}

/// One frozen rewrite operation.
///
/// The provider freezes its rewrite groups before this module is entered. Core
/// turns them into one write session whose logical targets stand in one-to-one
/// order with those groups, keeps the exact composite lease alive, and commits
/// once at the end.
///
/// An empty plan is a deterministic no-op and deliberately has no write
/// session: there is nothing to write, so there is nothing to commit.
#[derive(Clone)]
pub struct ConnectorDistributedRewriteSession {
    inner: Arc<ConnectorDistributedRewriteSessionInner>,
}

struct ConnectorDistributedRewriteSessionInner {
    plan: Arc<ConnectorDistributedRewritePlan>,
    lease: ConnectorDistributedRewriteLease,
    write_session: Option<Arc<ConnectorWriteSession>>,
}

impl ConnectorDistributedRewriteSession {
    /// Pure seal of the exact accepted session. Its original begin receiver
    /// remains an affirmative owner independently of this borrowed CPU work.
    pub(crate) fn from_original_session(
        plan: Arc<ConnectorDistributedRewritePlan>,
        lease: ConnectorDistributedRewriteLease,
        write_session: Option<Arc<ConnectorWriteSession>>,
    ) -> Result<Self, ConnectorError> {
        lease.validate_plan(plan.as_ref())?;
        match (&write_session, plan.cohorts().is_empty()) {
            (None, true) => {}
            (Some(session), false) => validate_rewrite_target_groups(
                plan.as_ref(),
                session
                    .targets()
                    .iter()
                    .map(|target| (target.ordinal(), target.rewrite_group_digest())),
            )?,
            _ => {
                return Err(invalid(
                    "Rewrite original session differs from its actual empty/nonempty plan",
                ));
            }
        }
        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session,
            }),
        })
    }
    /// Borrowed invoice for the exact original request constructor below.
    /// Typed value bindings clone their two immutable handles; Arrow root
    /// Field names/maps and the actual request Vecs are owned here.
    pub(crate) fn original_begin_request_from_identity(
        plan: &ConnectorDistributedRewritePlan,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
        context: ConnectorRequestContext,
    ) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
        rewrite_begin_request_from_identity(plan, table, context)
    }
    pub fn plan(&self) -> &ConnectorDistributedRewritePlan {
        &self.inner.plan
    }

    /// Exact composite lease retained from frozen planning through terminal
    /// commit or abort.  Provider-facing execution may use only this lease to
    /// plan the opaque frozen source.
    pub fn lease(&self) -> &ConnectorDistributedRewriteLease {
        &self.inner.lease
    }

    pub fn is_noop(&self) -> bool {
        self.inner.write_session.is_none()
    }

    pub(crate) fn write_session(&self) -> Option<&Arc<ConnectorWriteSession>> {
        self.inner.write_session.as_ref()
    }

    /// Which logical write target one frozen group writes to.
    ///
    /// The ordinal is the group's position in the frozen plan, which is the
    /// order the session sealed its targets in. Every group has exactly one,
    /// and a cohort id that this plan never froze has none.
    pub(crate) fn write_target_ordinal(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<WriteTargetOrdinal, ConnectorError> {
        rewrite_group_ordinal(&self.inner.plan, cohort_id)
    }

    /// The sealed target this group's query compiles its writer against.
    pub(crate) fn write_target(
        &self,
        cohort_id: ConnectorWriteCohortId,
    ) -> Result<&ConnectorWriteTargetPlan, ConnectorError> {
        let ordinal = self.write_target_ordinal(cohort_id)?;
        self.require_write_session()?
            .targets()
            .iter()
            .find(|target| target.ordinal() == ordinal)
            .ok_or_else(|| invalid("distributed rewrite session did not seal this group's target"))
    }

    /// Project a known-committed receipt only through the provider that froze
    /// this operation.  Callers must not decode the receipt in core.
    pub fn finalize_committed(
        &self,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
        self.inner.lease.finalize_rewrite(&self.inner.plan, receipt)
    }

    fn require_write_session(&self) -> Result<&Arc<ConnectorWriteSession>, ConnectorError> {
        self.inner
            .write_session
            .as_ref()
            .ok_or_else(|| invalid("distributed rewrite no-op has no write session"))
    }
}

/// A rewrite republishes exactly the rows its frozen groups read, so its writer
/// input is the provider-signed shape those groups froze.
///
/// It is deliberately not derived from the loaded table's schema. That schema is
/// the SQL-visible one and also carries the Iceberg metadata columns -- `_file`,
/// `_pos`, and, on a row-lineage table, `_row_id` and
/// `_last_updated_sequence_number` -- so a request built from it seals a target
/// wider than the group's own scan, and the write plan's sink and target then
/// disagree on column count. Projecting the signed shape cannot drift from what
/// planning admitted; rebuilding it from field names can.
///
/// Every frozen group signs the same preparation, because one rewrite reads one
/// relation through one scan schema. That is asserted rather than assumed: two
/// groups signing different inputs would seal one session whose targets
/// disagree about what their writers accept.
///
/// The provider derives one branch per frozen group from the loaded table rather
/// than from anything named here. What it cannot derive is which artifacts this
/// rewrite selected, so the plan's frozen shape travels in the flavor: it is
/// what decides whether the session seals data branches or delete branches, and
/// the provider refuses an input that disagrees with it.
/// Validate correspondence before any cohort is dispatched to a writer.
fn validate_rewrite_target_groups(
    plan: &ConnectorDistributedRewritePlan,
    mut targets: impl Iterator<
        Item = (
            novarocks_spi::connector::write_stack::WriteTargetOrdinal,
            Option<[u8; 32]>,
        ),
    >,
) -> Result<(), ConnectorError> {
    for (index, cohort) in plan.cohorts().iter().enumerate() {
        let Some((ordinal, digest)) = targets.next() else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite sealed fewer writers than frozen cohorts",
            ));
        };
        if ordinal.get() as usize != index || digest != Some(cohort.group_digest()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "distributed rewrite writer group differs from its frozen cohort",
            ));
        }
    }
    if targets.next().is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            "distributed rewrite sealed more writers than frozen cohorts",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn rewrite_begin_request(
    plan: &ConnectorDistributedRewritePlan,
    table: &novarocks_spi::connector::ConnectorTableMetadata,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
    rewrite_begin_request_from_identity(plan, &table.identity, context)
}

fn rewrite_begin_request_from_identity(
    plan: &ConnectorDistributedRewritePlan,
    table: &novarocks_spi::connector::ConnectorTableIdentity,
    context: ConnectorRequestContext,
) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest, String> {
    use novarocks_spi::connector::ConnectorWriteAdmissionPurpose;

    let mut cohorts = plan.cohorts().iter();
    let preparation = cohorts
        .next()
        .ok_or_else(|| "distributed rewrite plan froze no group to write".to_string())?
        .preparation();
    if cohorts.any(|cohort| cohort.preparation().digest() != preparation.digest()) {
        return Err("distributed rewrite groups signed different writer inputs".to_string());
    }

    Ok(
        novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest {
            table: Arc::from(
                format!("{}.{}", table.namespace, table.table).as_str(),
            ),
            target_ref: preparation.target_ref().clone(),
            intent: preparation.intent(),
            // A rewrite is arbitrated by the provider's ordinary base-state
            // compare and swap, so it presents as ordinary DML. What makes it
            // a rewrite is the flavor; saying it twice here would let the two
            // disagree.
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: crate::connector::write_target::write_input_request_for_shape(
                preparation.input(),
            )?,
            base: None,
            flavor:
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::DistributedRewrite { shape: plan.shape(), frozen: plan.frozen().clone() },
            context,
        },
    )
}

/// Which logical write target one frozen group writes to.
///
/// The ordinal is the group's position in the frozen plan, which is the order
/// the session sealed its targets in. A cohort id this plan never froze has
/// none, rather than falling through to a neighbour's writer.
fn rewrite_group_ordinal(
    plan: &ConnectorDistributedRewritePlan,
    cohort_id: ConnectorWriteCohortId,
) -> Result<WriteTargetOrdinal, ConnectorError> {
    let position = plan
        .cohorts()
        .iter()
        .position(|cohort| cohort.cohort_id() == cohort_id)
        .ok_or_else(|| invalid("distributed rewrite group is not part of the frozen plan"))?;
    let ordinal = u32::try_from(position)
        .map_err(|_| invalid("distributed rewrite group ordinal space exhausted"))?;
    Ok(WriteTargetOrdinal::try_new(ordinal)?)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

/// SQL declarations are projected from complete signed execution bindings.
/// Source annotations and physical binary/string resemblance confer no domain.
pub(crate) fn connector_execution_columns(
    schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
) -> Result<Vec<novarocks_types::schema::ColumnDef>, String> {
    schema
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let binding = schema
                .binding(ordinal)
                .ok_or_else(|| "connector execution field has no complete binding".to_string())?;
            novarocks_types::schema::ColumnDef::from_value_type(
                field.name().clone(),
                binding.clone(),
                None,
            )
            .map_err(|error| {
                format!(
                    "invalid connector execution column `{}`: {error}",
                    field.name()
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion, ConnectorControlBinding,
        ConnectorDistributedRewrite, ConnectorDistributedRewriteCohortPlan,
        ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
        ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMetadata, ConnectorProviderBinding, ConnectorProviderBindingKey,
        ConnectorProviderId, ConnectorScanPlanning, ConnectorTableHandle,
        ConnectorWriteBaseVersion, ConnectorWriteCohortId, ConnectorWriteControl,
        ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
        ConnectorWriteIntent, ConnectorWritePreparation, ProviderBindingEpoch,
    };

    use super::*;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .unwrap()
    }

    fn catalog_properties(instance: ConnectorInstanceId) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new("warehouse", "s3://rewrite-session").expect("valid warehouse"),
            ],
            Vec::new(),
        )
        .expect("valid catalog properties")
    }

    /// The signed writer contract one row-lineage rewrite group froze: the
    /// table's own data columns plus the two lineage columns the rewrite must
    /// carry forward, and nothing else.
    fn preparation(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableHandle,
        schema: &arrow::datatypes::SchemaRef,
        execution_schema: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    ) -> ConnectorWritePreparation {
        let binding = |index: usize, field: &Field| {
            ConnectorWriteFieldBinding::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                field.clone(),
                execution_schema
                    .binding(index)
                    .expect("complete frozen rewrite column declaration")
                    .clone(),
            )
            .expect("writer field matches its independently declared rewrite column")
        };
        let lineage = |name: &str| matches!(name, "_row_id" | "_last_updated_sequence_number");
        let data_fields = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !lineage(field.name()))
            .map(|(index, field)| binding(index, field.as_ref()))
            .collect();
        let row_identity_fields = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| lineage(field.name()))
            .map(|(index, field)| binding(index, field.as_ref()))
            .collect();
        ConnectorWritePreparation::try_new(
            owner,
            table,
            novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Overwrite,
            ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"base")).unwrap(),
            ConnectorWriteInputShape::RowLineage {
                data_fields,
                row_identity_fields,
            },
            Bytes::from_static(b"prepared"),
        )
        .unwrap()
    }

    /// The loaded target metadata a rewrite begins against.
    ///
    /// Its schema is the SQL-visible one, so it also carries the Iceberg
    /// metadata columns the provider exposes for reads. None of them is a
    /// writer input.
    fn table_metadata() -> novarocks_spi::connector::ConnectorTableMetadata {
        let instance = ConnectorInstanceId::parse("rewrite-session-instance").unwrap();
        crate::connector::schema_creation::declared_test_metadata(
            novarocks_spi::connector::ConnectorTableMetadataHeader {
                identity: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table"))
                    .unwrap(),
            },
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new("_file", DataType::Utf8, true),
                Field::new("_pos", DataType::Int64, true),
                Field::new("_row_id", DataType::Int64, true),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ])),
            vec![
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            ],
        )
    }

    struct TestMetadata {
        instance: ConnectorInstanceId,
    }

    struct TestPlanning {
        instance: ConnectorInstanceId,
    }

    impl ConnectorScanPlanning for TestPlanning {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }

        fn begin_scan(
            &self,
            _table: &ConnectorTableHandle,
            _request: novarocks_spi::connector::ConnectorBeginScanRequest,
        ) -> Result<novarocks_spi::connector::ConnectorScan, ConnectorError> {
            unreachable!("rewrite session does not plan scans")
        }

        fn plan_splits(
            &self,
            _scan: &novarocks_spi::connector::ConnectorScanHandle,
            _request: novarocks_spi::connector::ConnectorSplitPlanningRequest,
        ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError>
        {
            unreachable!("rewrite session does not plan scans")
        }
    }

    impl ConnectorMetadata for TestMetadata {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance
        }
        fn namespace_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn table_exists(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
        ) -> Result<bool, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn list_tables(
            &self,
            _request: novarocks_spi::connector::ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            unreachable!("rewrite session does not load metadata")
        }
        fn load_table(
            &self,
            _request: novarocks_spi::connector::ConnectorTableRequest,
            _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            novarocks_spi::connector::ConnectorTableMetadata,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            unreachable!("rewrite session does not load metadata")
        }
    }

    struct TestRewrite {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
    }

    impl ConnectorDistributedRewrite for TestRewrite {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.key
        }
        fn plan_rewrite(
            &self,
            _request: ConnectorDistributedRewritePlanningRequest,
            _creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorDistributedRewritePlan,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            unreachable!()
        }
        fn finalize_rewrite(
            &self,
            _plan: &ConnectorDistributedRewritePlan,
            _receipt: &ConnectorWriteReceipt,
        ) -> Result<ConnectorDistributedRewriteReceipt, ConnectorError> {
            unreachable!()
        }
    }

    struct TestWrite {
        key: ConnectorProviderBindingKey,
    }
    impl ConnectorWriteControl for TestWrite {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.key
        }
    }

    struct TestDistribution {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
    }
    impl ConnectorExecutionDistribution for TestDistribution {
        fn declaration(
            &self,
            _context: &ConnectorRequestContext,
        ) -> Result<ConnectorProviderBinding, ConnectorError> {
            ConnectorProviderBinding::iceberg(
                self.descriptor.instance_id.as_str(),
                self.key.incarnation.to_bytes(),
                "test",
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
        }
    }

    /// The frozen scan schema of a row-lineage rewrite: the table's data
    /// columns plus the two lineage columns, and none of the `_file` / `_pos`
    /// metadata columns a read exposes.
    fn rewrite_scan_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("_row_id", DataType::Int64, true),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
        ]))
    }

    fn fixture(
        cohorts: usize,
    ) -> (
        ConnectorDistributedRewritePlan,
        ConnectorDistributedRewriteLease,
    ) {
        fixture_with_group_schemas(&vec![rewrite_scan_schema(); cohorts])
    }

    /// One plan per frozen group schema. Every group of a real rewrite reads
    /// the same relation, so they normally share one; passing different ones
    /// builds the disagreement a real plan cannot contain.
    fn fixture_with_group_schemas(
        group_schemas: &[arrow::datatypes::SchemaRef],
    ) -> (
        ConnectorDistributedRewritePlan,
        ConnectorDistributedRewriteLease,
    ) {
        let cohorts = group_schemas.len();
        let provider = ConnectorProviderId::parse("rewrite-session-test").unwrap();
        let instance = ConnectorInstanceId::parse("rewrite-session-instance").unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: provider,
            instance_id: instance.clone(),
        };
        let key = ConnectorProviderBindingKey {
            instance_id: instance.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::new();
        let table =
            ConnectorTableHandle::try_new(instance.clone(), Bytes::from_static(b"table")).unwrap();
        let request = ConnectorDistributedRewritePlanningRequest::try_new(
            operation_id,
            key.clone(),
            novarocks_spi::connector::ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table: table.clone(),
                rewrite_all: true,
            },
            context(),
        )
        .unwrap();
        let cohort_plans = group_schemas
            .iter()
            .enumerate()
            .map(|(index, schema)| {
                let digest = [u8::try_from(index).unwrap_or_default(); 32];
                // Every column in these relation fixtures is declared BigInt.
                // The declaration remains explicit even for widened groups.
                let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
                let bindings = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                            novarocks_types::execution_value_type_from_declaration(
                                &novarocks_types::schema::SqlType::BigInt,
                                field.data_type(),
                                field.is_nullable(),
                                storage.clone(),
                            )
                            .expect("declared rewrite relation column"),
                        )
                        .expect("complete rewrite relation binding")
                    })
                    .collect::<Vec<_>>();
                let execution_schema =
                    novarocks_connector_contract::ConnectorReadExecutionSchema::try_new(
                        schema.clone(),
                        bindings.into_boxed_slice(),
                        storage,
                    )
                    .expect("complete rewrite relation schema");
                let preparation =
                    preparation(key.clone(), table.clone(), schema, &execution_schema);
                crate::connector::schema_creation::with_schema_creation(&context(), |creation| {
                    ConnectorDistributedRewriteCohortPlan::try_new_typed(
                        ConnectorWriteCohortId::derive(operation_id, b"test", digest).unwrap(),
                        novarocks_spi::connector::ConnectorRewriteCohortRead::DeleteArtifactGroup(
                            novarocks_spi::connector::ConnectorFrozenRewriteGroup::try_new(
                                "db",
                                "orders",
                                operation_id,
                                digest,
                                novarocks_spi::connector::ConnectorProcessLocalPayload::new(
                                    key.clone(),
                                    Arc::new(()),
                                ),
                            )
                            .unwrap(),
                        ),
                        schema.clone(),
                        execution_schema,
                        [3; 32],
                        preparation,
                        digest,
                        creation,
                    )
                })
                .unwrap()
            })
            .collect();
        let plan = ConnectorDistributedRewritePlan::try_new(
            &request,
            [1; 32],
            [2; 32],
            ConnectorDistributedRewritePlanSummary {
                groups: cohorts as u64,
                ..Default::default()
            },
            Bytes::from_static(b"plan"),
            cohort_plans,
            novarocks_spi::connector::ConnectorProcessLocalPayload::new(key.clone(), Arc::new(())),
        )
        .unwrap();
        let rewrite = Arc::new(TestRewrite {
            descriptor: descriptor.clone(),
            key: key.clone(),
        });
        let lease = ConnectorDistributedRewriteLease::new(
            descriptor.clone(),
            novarocks_spi::connector::ConnectorControlRuntimeId::from_bytes([7; 16]),
            key.incarnation,
            novarocks_spi::connector::ConnectorControlPlanningLease::new(
                Arc::new(
                    ConnectorControlBinding::try_new(
                        descriptor.clone(),
                        key.incarnation,
                        Arc::new(TestMetadata {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestPlanning {
                            instance: key.instance_id.clone(),
                        }),
                        Arc::new(TestDistribution {
                            descriptor: descriptor.clone(),
                            key: key.clone(),
                        }),
                        None,
                    )
                    .unwrap()
                    .with_catalog_properties(catalog_properties(instance.clone()))
                    .expect("control binding has catalog execution properties"),
                ),
                || {},
            ),
            Arc::new(TestMetadata {
                instance: instance.clone(),
            }),
            Arc::new(TestPlanning { instance }),
            rewrite,
            Arc::new(TestWrite { key: key.clone() }),
            Arc::new(TestDistribution { descriptor, key }),
            || {},
        )
        .unwrap();
        (plan, lease)
    }

    #[test]
    fn every_frozen_group_maps_to_its_own_dense_write_target_ordinal() {
        let (plan, _lease) = fixture(3);
        let ordinals = plan
            .cohorts()
            .iter()
            .map(|cohort| {
                rewrite_group_ordinal(&plan, cohort.cohort_id())
                    .expect("a frozen group has an ordinal")
                    .get()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            vec![0, 1, 2],
            "a group's ordinal is its position in the frozen plan, which is the order the \
             session seals its targets in"
        );
    }

    #[test]
    fn a_group_the_plan_never_froze_has_no_write_target() {
        let (plan, _lease) = fixture(2);
        let (foreign, _) = fixture(1);
        let foreign_id = foreign.cohorts()[0].cohort_id();
        let error = rewrite_group_ordinal(&plan, foreign_id)
            .expect_err("a foreign group must not resolve to a neighbour's writer");
        assert!(
            error.to_string().contains("not part of the frozen plan"),
            "the refusal must name what it found: {error}"
        );
    }

    #[test]
    fn empty_plan_is_noop_without_writer_session() {
        let (plan, lease) = fixture(0);
        let session =
            ConnectorDistributedRewriteSession::from_original_session(Arc::new(plan), lease, None)
                .unwrap();
        assert!(session.is_noop());
        assert!(session.write_session().is_none());
    }

    #[test]
    fn a_plan_with_frozen_groups_requires_its_original_writer_session() {
        let (plan, lease) = fixture(1);
        let Err(error) =
            ConnectorDistributedRewriteSession::from_original_session(Arc::new(plan), lease, None)
        else {
            panic!("a plan that froze a group must open a write session");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error.to_string().contains("actual empty/nonempty plan"),
            "{error}"
        );
    }

    /// The begin request describes the writer input the frozen groups signed,
    /// never the loaded table's SQL-visible schema.
    ///
    /// A rewrite writes exactly the rows its groups read, so its input is the
    /// groups' own scan shape. The table schema additionally carries the
    /// Iceberg metadata columns (`_file`, `_pos`, and the lineage pair), so a
    /// request built from it seals a target wider than the group's plan and the
    /// write plan's sink and target then disagree on column count.
    #[test]
    fn the_begin_request_projects_the_frozen_writer_input_not_the_table_schema() {
        use novarocks_spi::connector::ConnectorWriteInputRequest;

        let (plan, _lease) = fixture(2);
        let table = table_metadata();
        let request = rewrite_begin_request(&plan, &table, context())
            .expect("a frozen plan describes its own writer input");

        let ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } = &request.input
        else {
            panic!("a row-lineage rewrite must keep the shape its groups signed");
        };
        let names = |fields: &[novarocks_spi::connector::ConnectorWriteFieldRequest]| {
            fields
                .iter()
                .map(|field| field.field().name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(data_fields), vec!["value".to_string()]);
        assert_eq!(
            names(row_identity_fields),
            vec![
                "_row_id".to_string(),
                "_last_updated_sequence_number".to_string()
            ]
        );
        assert_eq!(
            data_fields.len() + row_identity_fields.len(),
            plan.cohorts()[0].scan_schema().fields().len(),
            "the writer input stands one-to-one with the frozen scan, not with the \
             {} column table schema",
            table.schema().fields().len()
        );
        assert_eq!(request.intent, ConnectorWriteIntent::Overwrite);
        assert_eq!(request.target_ref.as_str(), "main");
    }

    /// One rewrite reads one relation, so its groups sign one writer contract.
    /// Groups that disagree would seal a session whose targets accept different
    /// inputs, which no single plan could feed.
    #[test]
    fn groups_that_signed_different_writer_inputs_are_refused() {
        let widened = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
            Field::new("_row_id", DataType::Int64, true),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
        ]));
        let (plan, _lease) = fixture_with_group_schemas(&[rewrite_scan_schema(), widened]);
        let Err(error) = rewrite_begin_request(&plan, &table_metadata(), context()) else {
            panic!("two groups cannot sign different writer inputs");
        };
        assert!(error.contains("different writer inputs"), "{error}");
    }
    #[test]
    fn writer_groups_must_match_each_cohort_before_dispatch() {
        let (plan, _) = fixture(3);
        let expected = plan
            .cohorts()
            .iter()
            .enumerate()
            .map(|(i, cohort)| {
                (
                    novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(i as u32)
                        .unwrap(),
                    Some(cohort.group_digest()),
                )
            })
            .collect::<Vec<_>>();
        validate_rewrite_target_groups(&plan, expected.clone().into_iter()).unwrap();
        let mut wrong = expected.clone();
        wrong[0].1 = Some([255; 32]);
        assert_eq!(
            validate_rewrite_target_groups(&plan, wrong.into_iter())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Internal
        );
        let mut wrong = expected.clone();
        wrong[0].0 = wrong[1].0;
        assert_eq!(
            validate_rewrite_target_groups(&plan, wrong.into_iter())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Internal
        );
        let mut wrong = expected.clone();
        wrong.pop();
        assert_eq!(
            validate_rewrite_target_groups(&plan, wrong.into_iter())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Internal
        );
        let mut wrong = expected.clone();
        wrong.push(expected[0]);
        assert_eq!(
            validate_rewrite_target_groups(&plan, wrong.into_iter())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Internal
        );
        let mut wrong = expected;
        wrong[1].1 = None;
        assert_eq!(
            validate_rewrite_target_groups(&plan, wrong.into_iter())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Internal
        );
    }
}
