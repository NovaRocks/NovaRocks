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

//! Concrete original facts moved into one MV write-preparation source.
//!
//! Public only because the installed activation trait is public. Construction
//! remains inside this crate; these fields do not grant Root, result, CPU,
//! Provider, or StateStore authority.

use std::sync::Arc;

use crate::{
    mv::domain::refresh::target_binding::MvTargetBinding,
    query_execution::{
        completion_facts::CompletionFactOwners,
        mv_assembly::{
            refresh_handoff::PreparedMvRefreshWrite,
            refresh_preparation::{driver::MvRefreshTurnCustody, io_gate::MvRefreshPreparationIo},
        },
        write_session::ConnectorWriteSession,
    },
};
use novarocks_query_application::admitted_query_context::{
    QueryExecutionContext, QueryResultCapacityBinding,
};
use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorRequestContext};

/// The caller has already received these exact facts in its original Action.
/// Moving this request preserves the same session and preparation receivers;
/// it does not perform a lookup, open a session, or start a physical operation.
pub struct OriginalMvWritePreparationRequest {
    pub(crate) prepared: PreparedMvRefreshWrite,
    pub(crate) target: MvTargetBinding,
    pub(crate) planning: ConnectorControlPlanningLease,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) session: Arc<ConnectorWriteSession>,
    pub(crate) io: Arc<MvRefreshPreparationIo>,
    pub(crate) turn: MvRefreshTurnCustody,
    pub(crate) owners: CompletionFactOwners,
    pub(crate) capacity: QueryResultCapacityBinding,
    pub(crate) requests: Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>,
}
