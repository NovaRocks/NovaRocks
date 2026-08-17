// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native write assembly for the Frontend-owned MV refresh lifecycle.
//!
//! The MV application module owns refresh domain facts; this module owns the
//! assembly vocabulary those facts are dispatched through.  Keeping the two
//! apart lets the MV application port stay with the MV domain while the
//! sealed encoding carrier and its provider activation port travel with the
//! rest of query assembly.

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_spi::connector::{
    ConnectorCommittedPartitioning, ConnectorControlPlanningLease, ConnectorRequestContext,
    ConnectorWriteCohortId, ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWriteReceipt,
};

use crate::mv::application::{
    MvRefreshCommittedFacts, MvRefreshPublicationIntent, PreparedMvRefreshWrite,
};
use crate::mv::persistence::schema::MvPartitionContract;
use crate::query_execution::contract::ConnectorWriteOperationRegistration;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::post_compile::NativeFragmentEncodingInput;
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::request_context::QueryExecutionContext;

/// Exact Core-retained inputs for one Frontend-owned MV native assembly.
///
/// The frontend may read the immutable input only to encode the native
/// fragment bundle.  Finishing consumes the same retained pair, so neither a
/// newer binding nor a replacement prepared fragment set can reach dispatch.
pub struct PreparedMvNativeWriteAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    registration: ConnectorWriteOperationRegistration,
    cohort_id: ConnectorWriteCohortId,
    lease: ConnectorWriteLease,
}

impl PreparedMvNativeWriteAssembly {
    pub(crate) fn new(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        registration: ConnectorWriteOperationRegistration,
        cohort_id: ConnectorWriteCohortId,
        lease: ConnectorWriteLease,
    ) -> Self {
        Self {
            encoding,
            query_options,
            registration,
            cohort_id,
            lease,
        }
    }

    pub fn native_encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    pub fn write_operation_id(&self) -> ConnectorWriteOperationId {
        self.registration.operation_id()
    }

    pub fn write_cohort_id(&self) -> ConnectorWriteCohortId {
        self.cohort_id
    }

    pub fn finish(
        self,
        native_bundle: NativeFragmentAttachment,
    ) -> Result<PreparedDistributedWriteRequest, String> {
        if !self.encoding.matches_native_attachment(&native_bundle) {
            return Err(
                "native fragment bundle does not match the sealed MV encoding input".into(),
            );
        }
        let (_, prepared) = self.encoding.into_parts();
        PreparedDistributedWriteRequest::new(
            prepared,
            native_bundle,
            self.query_options,
            self.registration,
            self.cohort_id,
            self.lease,
        )
        .map_err(|error| error.to_string())
    }
}

/// Provider activation and native fragment preparation for a SQL-shaped
/// refresh artifact. The frontend owns intent persistence, write-session
/// admission, native assembly, execution, commit, publication, and cleanup;
/// the port returns only an exact sealed encoding carrier after the lease is
/// retained.
pub trait MvRefreshProviderActivation: Send + Sync {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, String>;

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String>;

    /// Project a provider-committed repartition contract into the lake-owned
    /// MV descriptor. The atomic table commit is already durable when this is
    /// called; a failure therefore leaves the frontend refresh fenced for
    /// recovery and must be safe to retry. `committed_partitioning` is the
    /// provider-produced exact CAS guard and must be forwarded unchanged, not
    /// reconstructed from the application partition contract.
    fn sync_repartition_descriptor(
        &self,
        mv_id: i64,
        partition_spec: MvPartitionContract,
        committed_partitioning: ConnectorCommittedPartitioning,
        connector_context: &ConnectorRequestContext,
    ) -> Result<(), String>;
}

/// Composition sink installed before the activation adapter exists. The
/// adapter is bound only after connector control and the engine state are
/// available, avoiding a direct all-in-one call path.
pub trait MvRefreshProviderActivationSink: Send + Sync {
    fn bind_mv_refresh_provider_activation(
        &self,
        activation: std::sync::Arc<dyn MvRefreshProviderActivation>,
    ) -> Result<(), String>;
}
