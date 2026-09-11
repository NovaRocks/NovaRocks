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

//! Native write assembly for the Frontend-owned MV refresh lifecycle.
//!
//! The MV application module owns refresh domain facts; this module owns the
//! assembly vocabulary those facts are dispatched through.  Keeping the two
//! apart lets the MV application port stay with the MV domain while the
//! sealed encoding carrier and its provider activation port travel with the
//! rest of query assembly.

use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteLease, ConnectorWriteReceipt, MvLakePackageObservation,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvRefreshCommittedFacts, MvRefreshPublicationIntent,
};
use crate::query_execution::mv_assembly::refresh_handoff::PreparedMvRefreshWrite;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::post_compile::NativeFragmentEncodingInput;

/// Exact Core-retained inputs for one Frontend-owned MV native assembly.
///
/// The frontend may read the immutable input only to encode the native
/// fragment bundle.  Finishing consumes the same retained pair, so neither a
/// newer binding nor a replacement prepared fragment set can reach dispatch.
///
/// Every MV data write -- first refresh and incremental alike -- commits through
/// the write session that admitted it. The session sealed the recipes this
/// plan's writer nodes carry, so the two travel together and no operation,
/// cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvNativeWriteAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvNativeWriteAssembly {
    pub(crate) fn session(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> Self {
        Self {
            encoding,
            query_options,
            session: write_session,
        }
    }

    pub fn native_encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    /// The commit authority of this write, so a caller that fails between
    /// assembly and dispatch can release it rather than leaving the provider
    /// holding a session for a plan that will never run.
    pub(crate) fn write_session(
        &self,
    ) -> &std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        &self.session
    }

    pub fn finish(
        self,
        native_bundle: NativeFragmentAttachment,
    ) -> Result<PreparedMvSessionWrite, String> {
        Ok(PreparedMvSessionWrite {
            encoding: self.encoding,
            native_bundle,
            query_options: self.query_options,
            session: self.session,
        })
    }
}

/// A session-driven MV write, one step away from dispatch.
///
/// The session rides along as the request's single commit authority, so no
/// operation, cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvSessionWrite {
    encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput,
    native_bundle: NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvSessionWrite {
    pub(crate) fn into_request(
        self,
        execution: &QueryExecutionContext,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        let request =
            crate::query_execution::contract::build_distributed_query_request_with_execution(
                self.encoding,
                self.native_bundle,
                self.query_options,
                crate::query_execution::contract::DistributedQueryIntent::Write,
                execution,
            )
            .map_err(|error| error.to_string())?;
        crate::query_execution::contract::with_connector_write_session(request, self.session)
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

    /// Reobserve the complete lake-owned package after a known publication.
    /// The caller supplies the retained exact lease and snapshot identity it
    /// already proved; implementations reject a missing, stale, or advanced
    /// head before returning the package for Accelerator convergence.
    fn observe_published_package(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        connector_context: &ConnectorRequestContext,
    ) -> Result<MvLakePackageObservation, String>;
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
