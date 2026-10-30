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

//! Rewrite borrows the SAME positive metadata Root payload for formation.
use super::io_admission::MetadataIoAdmission;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorOperationControlView, ConnectorOriginalResultScope,
    ConnectorOriginalResultScopeCheck, ConnectorRequestContext, OriginalResultCheckClass as Class,
    OriginalResultCheckError as Failure,
};
use std::{sync::Arc, time::Instant};

struct OriginalRewriteCapture {
    control: ConnectorOperationControlView,
    // Obtained ONLY from MIO's existing positive RetainedPayloadGrant.
    // A plain ResultWindowAlias/u64 cannot construct this author.
    binding: QueryResultCapacityBinding,
}
impl ConnectorOriginalResultScopeCheck for OriginalRewriteCapture {
    fn original_deadline(&self) -> Instant {
        self.control.deadline()
    }
    fn check_active(&self) -> Result<(), Failure> {
        self.control
            .check_active()
            .map_err(|e| Failure::new(Class::Control, e))?;
        self.binding
            .scope()
            .check()
            .map_err(|e| Failure::new(Class::Admission, e))?;
        self.binding
            .try_window_alias()
            .map(drop)
            .map_err(|e| Failure::new(Class::Coverage, e))
    }
}

pub(super) fn borrow_positive_capture(
    io: &MetadataIoAdmission,
    context: &ConnectorRequestContext,
) -> Result<ConnectorOriginalResultScope, Failure> {
    let binding = io
        .root_capacity()
        .map_err(|e| Failure::new(Class::Admission, e))?;
    context
        .check_active()
        .map_err(|e| Failure::new(Class::Control, e))?;
    binding
        .scope()
        .check()
        .map_err(|e| Failure::new(Class::Admission, e))?;
    let original = ConnectorOriginalResultScope::from_original(Arc::new(OriginalRewriteCapture {
        control: context.original_operation_control(),
        binding,
    }));
    original.check_active()?;
    Ok(original)
}
