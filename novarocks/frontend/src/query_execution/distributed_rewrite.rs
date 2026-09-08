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

use novarocks_spi::connector::write_stack::{ConnectorWriteTargetPlan, WriteTargetOrdinal};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewriteReceipt, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorWriteAbortOutcome, ConnectorWriteCohortId,
    ConnectorWriteOperationId, ConnectorWriteReceipt, ExternalMutationOutcome,
};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::connector::distributed_rewrite_application::{
    DistributedRewriteApplicationSession, DistributedRewriteSealing, SealedDistributedRewrite,
};
use crate::query_execution::preparation::scan::{QueryPinnedFileSetRead, QueryRewriteGroupRead};
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::write_session::ConnectorWriteSession;
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::planning::query_execution::{
    FrozenConnectorScanIdentity, FrozenConnectorScanPlan,
};

/// The Frontend-owned maintenance session shape produced by query assembly.
pub(crate) type DistributedRewriteMaintenanceSession =
    DistributedRewriteApplicationSession<ConnectorDistributedRewriteSession>;

impl SealedDistributedRewrite for ConnectorDistributedRewriteSession {
    fn plan(&self) -> &ConnectorDistributedRewritePlan {
        ConnectorDistributedRewriteSession::plan(self)
    }

    fn is_noop(&self) -> bool {
        ConnectorDistributedRewriteSession::is_noop(self)
    }
}

impl DistributedRewriteSealing for QueryExecutionService {
    type Sealed = ConnectorDistributedRewriteSession;

    fn seal_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: crate::connector::control_host::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Self::Sealed, String> {
        self.begin_distributed_rewrite_operation_with_lease(
            plan,
            lease,
            write_stack,
            table,
            context,
        )
        .map_err(|error| error.to_string())
    }

    fn seal_noop_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
    ) -> Result<Self::Sealed, String> {
        ConnectorDistributedRewriteSession::noop(plan, lease).map_err(|error| error.to_string())
    }
}

/// Admit the synthetic source used by one pinned rewrite cohort read.
pub(crate) fn admit_pinned_rewrite_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::pinned_connector_read::admit_pinned_file_set_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
    )
}

/// Build the minimal physical source for one pinned rewrite cohort read.
/// Preparation freezes the relation restricted to exactly the files the
/// provider pinned for this cohort; no normal table lookup may run.
pub(crate) fn pinned_rewrite_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    crate::query_execution::pinned_connector_read::pinned_file_set_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

pub(crate) fn pinned_rewrite_read_resolver(
    binding: SqlTableBindingId,
    read: QueryPinnedFileSetRead,
) -> crate::query_execution::pinned_connector_read::PinnedFileSetReadResolver {
    crate::query_execution::pinned_connector_read::PinnedFileSetReadResolver::new(
        binding,
        frozen_rewrite_identity(),
        read,
    )
}

/// Admit the synthetic source used by one procedure cohort's group read.
pub(crate) fn admit_rewrite_group_scan_binding(
    bindings: &QueryTableBindingStore,
    input_schema: &arrow::datatypes::SchemaRef,
) -> Result<SqlTableBindingId, String> {
    crate::query_execution::rewrite_group_read::admit_table_execute_scan_binding(
        bindings,
        &frozen_rewrite_identity(),
        input_schema,
    )
}

/// Build the minimal physical source for one procedure cohort's group read.
/// Preparation freezes the relation the group names; no normal table lookup
/// may run.
pub(crate) fn rewrite_group_scan_physical_plan(
    input_schema: &arrow::datatypes::SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    crate::query_execution::rewrite_group_read::table_execute_scan_physical_plan(
        &frozen_rewrite_identity(),
        input_schema,
        binding,
    )
}

pub(crate) fn rewrite_group_read_resolver(
    binding: SqlTableBindingId,
    read: QueryRewriteGroupRead,
) -> crate::query_execution::rewrite_group_read::RewriteGroupReadResolver {
    crate::query_execution::rewrite_group_read::RewriteGroupReadResolver::new(
        binding,
        frozen_rewrite_identity(),
        read,
    )
}

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
    plan: ConnectorDistributedRewritePlan,
    lease: ConnectorDistributedRewriteLease,
    write_session: Option<Arc<ConnectorWriteSession>>,
}

impl ConnectorDistributedRewriteSession {
    /// Validate a provider-frozen plan and open one write session covering
    /// every frozen group.
    ///
    /// The provider re-derives its own branches at begin, so this checks that
    /// the target set it sealed agrees with the group set this plan froze. A
    /// disagreement means the table moved between planning and begin, and the
    /// two halves would silently write past each other -- so it fails closed
    /// here rather than at commit.
    /// A plan that froze no group. It writes nothing, so it deliberately has no
    /// write session and never derives a write-stack lease -- there is nothing
    /// for one to admit.
    pub fn noop(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
    ) -> Result<Self, ConnectorError> {
        lease.validate_plan(&plan)?;
        if !plan.cohorts().is_empty() {
            return Err(invalid(
                "distributed rewrite plan froze groups, so it is not a no-op",
            ));
        }
        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session: None,
            }),
        })
    }

    pub fn try_begin(
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: crate::connector::control_host::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        lease.validate_plan(&plan)?;

        let write_session = if plan.cohorts().is_empty() {
            None
        } else {
            let write_lease = lease.derive_write_lease()?;
            let session = crate::query_execution::write_session::begin_connector_write_session(
                write_stack,
                &write_lease,
                rewrite_begin_request(&plan, table, context).map_err(invalid)?,
            )
            .map_err(invalid)?;
            let sealed = session.expected_targets().len();
            if sealed != plan.cohorts().len() {
                return Err(invalid(format!(
                    "distributed rewrite sealed {sealed} write targets for {} frozen groups",
                    plan.cohorts().len()
                )));
            }
            Some(session)
        };

        Ok(Self {
            inner: Arc::new(ConnectorDistributedRewriteSessionInner {
                plan,
                lease,
                write_session,
            }),
        })
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

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.inner.plan.operation_id()
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

    /// Take one finished group's prepared write set into the session.
    ///
    /// Each group runs as its own distributed query, so the session collects
    /// their prepared sets and commits the union once. Budgets are charged on
    /// that union, not per group.
    pub(crate) fn accumulate(
        &self,
        prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) -> Result<(), ConnectorError> {
        self.require_write_session()?.accumulate(prepared)
    }

    /// Commit every accumulated group through the same exact control lease.
    pub fn commit(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.require_write_session()?.finish_accumulated(context)
    }

    /// Abort the whole rewrite through the same control lease.
    pub fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.require_write_session()?.abort(context)
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
fn rewrite_begin_request(
    plan: &ConnectorDistributedRewritePlan,
    table: &novarocks_spi::connector::ConnectorTableMetadata,
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
                format!("{}.{}", table.identity.namespace, table.identity.table).as_str(),
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
            ),
            base: None,
            flavor:
                novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::DistributedRewrite(
                    plan.shape(),
                ),
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
    WriteTargetOrdinal::try_new(ordinal)
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion, ConnectorCancellation,
        ConnectorControlBinding, ConnectorDistributedRewrite,
        ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewritePlanSummary,
        ConnectorDistributedRewritePlanningRequest, ConnectorExecutionDistribution,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMetadata,
        ConnectorProviderBinding, ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorScanPlanning, ConnectorTableHandle, ConnectorWriteBaseVersion,
        ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteIntent,
        ConnectorWritePreparation, ProviderBindingEpoch,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
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
    ) -> ConnectorWritePreparation {
        let binding = |index: usize, field: &Field| {
            ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([index as u8 + 1; 32]),
                field.clone(),
            )
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
        novarocks_spi::connector::ConnectorTableMetadata {
            identity: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance.clone(),
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            schema: Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new("_file", DataType::Utf8, true),
                Field::new("_pos", DataType::Int64, true),
                Field::new("_row_id", DataType::Int64, true),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ])),
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
            definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(instance, Bytes::from_static(b"table")).unwrap(),
        }
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
        ) -> Result<novarocks_spi::connector::ConnectorTableMetadata, ConnectorError> {
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
        ) -> Result<ConnectorDistributedRewritePlan, ConnectorError> {
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
                ConnectorDistributedRewriteCohortPlan::try_new(
                    ConnectorWriteCohortId::derive(operation_id, b"test", digest).unwrap(),
                    novarocks_spi::connector::ConnectorRewriteCohortRead::DeleteArtifactGroup(
                        novarocks_spi::connector::ConnectorFrozenRewriteGroup::try_new(
                            "db",
                            "orders",
                            "s3://warehouse/db/orders/_rewrite/0199",
                            digest,
                        )
                        .unwrap(),
                    ),
                    schema.clone(),
                    [3; 32],
                    preparation(key.clone(), table.clone(), schema),
                    digest,
                )
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
        let session = ConnectorDistributedRewriteSession::noop(plan, lease).unwrap();
        assert!(session.is_noop());
        assert!(session.write_session().is_none());
    }

    #[test]
    fn a_plan_with_frozen_groups_is_refused_as_a_noop() {
        let (plan, lease) = fixture(1);
        let Err(error) = ConnectorDistributedRewriteSession::noop(plan, lease) else {
            panic!("a plan that froze a group must open a write session");
        };
        assert!(error.to_string().contains("not a no-op"), "{error}");
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
            table.schema.fields().len()
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
}
