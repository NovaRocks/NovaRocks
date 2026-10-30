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

//! Original owned-source provenance at the FE COW request boundary.
//! This check creates no allowance and predicts no Provider or compiler graph.

use super::cow_necessary_before_begin::Error;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
    ConnectorRowMutationSelection,
};

pub(super) fn before_request(
    selection: &ConnectorRowMutationSelection,
    binding: &QueryResultCapacityBinding,
    context: &ConnectorRequestContext,
) -> Result<(), Error> {
    context.check_active().map_err(Error::Control)?;
    binding.scope().check().map_err(Error::Scope)?;
    crate::query_execution::internal_result_cpu::require_internal_result_capacity(
        binding.scope(),
        &binding.window_alias(),
    )
    .map_err(Error::ExistingCoverage)?;
    if !selection.has_owned_sources() {
        return Err(Error::Control(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "COW request requires original owned-source receipts",
        )));
    }
    context.check_active().map_err(Error::Control)?;
    binding.scope().check().map_err(Error::Scope)?;
    Ok(())
}
