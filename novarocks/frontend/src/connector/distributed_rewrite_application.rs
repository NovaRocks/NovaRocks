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

//! Provider-neutral application session for a frozen distributed rewrite.
//!
//! This layer owns exact-generation acquisition, strict table loading, and
//! C1 operation sealing.  Provider-specific source execution is deliberately
//! injected by the concrete engine implementation after this session exists.
//!
//! Sealing a frozen provider plan into a live distributed operation is query
//! assembly work, not a connector fact.  Its owner is therefore injected
//! through [`DistributedRewriteSealing`] rather than named from here.

use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorDistributedRewriteOperation,
    ConnectorDistributedRewritePlan, ConnectorDistributedRewriteResolver, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, ConnectorWriteOperationId,
};
use sha2::{Digest, Sha256};

use novarocks_query_application::admitted_query_context::QueryExecutionContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedRewriteIntent {
    DataFiles {
        rewrite_all: bool,
    },
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

/// The frozen-plan facts this application layer reads back from a sealed
/// distributed rewrite operation.
///
/// The operation stays opaque here: this module never inspects its cohort
/// executions, provider checkpoints, or C1 commit decision.  It only needs the
/// sealed SPI plan it handed over and whether that plan sealed to a no-op.
pub trait SealedDistributedRewrite {
    fn plan(&self) -> &ConnectorDistributedRewritePlan;

    fn is_noop(&self) -> bool;
}

/// Seal one provider-frozen distributed rewrite into an owned operation.
///
/// Acquiring the exact composite lease, strictly loading the target table, and
/// freezing the provider plan are connector facts and stay in this module.
/// Turning that frozen plan into a live distributed operation is query
/// assembly, so its owner implements this port and is passed in explicitly.
///
/// The port names only sealed SPI values plus an operation type the assembly
/// owner defines, so it survives relocation of either side: moving query
/// assembly out of this package, and later moving the connector adapters
/// themselves, leave both the trait and its single call site unchanged.
pub trait DistributedRewriteSealing {
    /// The sealed operation the query assembly owner produces.
    type Sealed: SealedDistributedRewrite;

    fn seal_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
        write_stack: novarocks_catalog_application::ConnectorWriteStackLease,
        table: &novarocks_spi::connector::ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Self::Sealed, String>;

    /// Seal a plan that froze no group. It is a separate entry point rather
    /// than an argument because a no-op has no write-stack lease to be given.
    fn seal_noop_distributed_rewrite(
        &self,
        plan: ConnectorDistributedRewritePlan,
        lease: ConnectorDistributedRewriteLease,
    ) -> Result<Self::Sealed, String>;
}

pub struct DistributedRewriteApplicationSession<S> {
    session: S,
    context: ConnectorRequestContext,
    execution: QueryExecutionContext,
}

impl<S: SealedDistributedRewrite> DistributedRewriteApplicationSession<S> {
    pub fn plan(&self) -> &ConnectorDistributedRewritePlan {
        self.session.plan()
    }

    pub fn is_noop(&self) -> bool {
        self.session.is_noop()
    }

    /// Stable durable digest for the sealed cohort membership.  It contains
    /// no provider payload or source path; those remain provider-private in
    /// the plan artifact.
    pub fn cohort_set_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"novarocks.distributed-rewrite.cohort-set.v1\0");
        hash.update((self.session.plan().cohorts().len() as u64).to_be_bytes());
        for cohort in self.session.plan().cohorts() {
            hash.update(cohort.cohort_id().to_bytes());
            hash.update(cohort.group_digest());
        }
        hash.finalize().into()
    }

    pub fn session(&self) -> &S {
        &self.session
    }

    pub fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }
}

/// Plan exactly once.  The caller captures topology before this function and
/// retains the returned session through every staged cohort and terminal C1
/// commit.  No current-generation lookup is available after this point.
#[expect(
    clippy::too_many_arguments,
    reason = "The frozen frontend boundary keeps independently validated inputs explicit."
)]
pub fn plan_distributed_rewrite_session<S: DistributedRewriteSealing>(
    sealing: &S,
    resolver: &dyn ConnectorDistributedRewriteResolver,
    control_host: &novarocks_catalog_application::ConnectorControlHost,
    instance_id: &ConnectorInstanceId,
    table: ConnectorTableIdentity,
    operation_id: ConnectorWriteOperationId,
    intent: DistributedRewriteIntent,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
) -> Result<DistributedRewriteApplicationSession<S::Sealed>, String> {
    if table.instance_id != *instance_id {
        return Err(
            "distributed rewrite table does not belong to requested connector instance".to_string(),
        );
    }
    let lease = resolver
        .acquire_current_distributed_rewrite(instance_id)
        .map_err(|error| format!("acquire distributed rewrite exact lease: {error}"))?;
    let metadata = lease
        .metadata()
        .load_table(ConnectorTableRequest {
            table: table.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .map_err(|error| format!("load distributed rewrite target metadata: {error}"))?;
    if metadata.identity != table || metadata.table.owner() != instance_id {
        return Err("distributed rewrite metadata returned a foreign table handle".to_string());
    }
    let operation = match intent {
        DistributedRewriteIntent::DataFiles { rewrite_all } => {
            ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table: metadata.table.clone(),
                rewrite_all,
            }
        }
        DistributedRewriteIntent::PositionDeletes {
            rewrite_all,
            min_input_files,
        } => ConnectorDistributedRewriteOperation::RewritePositionDeletes {
            table: metadata.table.clone(),
            rewrite_all,
            min_input_files,
        },
    };
    let plan = lease
        .plan_operation(operation_id, operation, context.clone())
        .map_err(|error| format!("plan distributed rewrite: {error}"))?;
    // A rewrite that froze no group writes nothing, so it must not acquire a
    // write-stack lease at all -- there would be nothing for one to admit. The
    // lease is derived only on the writing path, on the same generation that
    // resolved the rewrite, so its writer recipes cannot outlive the metadata
    // they were planned against.
    let session = if plan.cohorts().is_empty() {
        sealing.seal_noop_distributed_rewrite(plan, lease)?
    } else {
        let write_stack = crate::connector::write_target::derive_write_stack_lease(
            control_host,
            &lease.planning_lease(),
        )?;
        sealing.seal_distributed_rewrite(plan, lease, write_stack, &metadata, context.clone())?
    };
    Ok(DistributedRewriteApplicationSession {
        session,
        context,
        execution,
    })
}
