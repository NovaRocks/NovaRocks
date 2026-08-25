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

//! Core-owned assembly after the SQL compiler has produced sealed facts.
//!
//! This module deliberately accepts a sealed distributed plan together with
//! the application materializer that admitted it.  It keeps the exact binding
//! store, fragment preparation, and native-request finalization outside the
//! SQL compiler and does not expose a route to substitute a newer binding.

use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_proto::lifecycle::QueryOptions;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::preparation::PreparedFragmentSet;
use crate::query_execution::{PreparedQueryCompletion, PreparedQueryOperation};
use novarocks_sql::plan_read::DistributedPlan;

/// Select the completion formatter paired with one post-compile assembly.
/// SQL owns the plan facts; Core owns the profile formatter and its
/// connector-planning observations.
pub enum PostCompileIntent {
    Result,
    Profile {
        planning_elapsed: std::time::Duration,
        execution_started_at: std::time::Instant,
    },
}

/// Exact plan/preparation pair frozen by Core for one Frontend-owned native
/// assembly step. It has no constructor and exposes only immutable encoder
/// inputs, so callers cannot replace bindings or acquire a newer generation.
pub struct NativeFragmentEncodingInput {
    distributed_plan: DistributedPlan,
    prepared: PreparedFragmentSet,
    provenance: u64,
}

impl NativeFragmentEncodingInput {
    pub(crate) fn new(distributed_plan: DistributedPlan, prepared: PreparedFragmentSet) -> Self {
        Self {
            distributed_plan,
            prepared,
            provenance: next_native_encoding_provenance(),
        }
    }

    pub fn distributed_plan(&self) -> &DistributedPlan {
        &self.distributed_plan
    }

    pub fn prepared(&self) -> &PreparedFragmentSet {
        &self.prepared
    }

    pub fn encoding_view(
        &self,
    ) -> crate::query_execution::native_fragment::NativeFragmentEncodingView<'_> {
        crate::query_execution::native_fragment::NativeFragmentEncodingView::sealed(
            &self.distributed_plan,
            &self.prepared,
            self.provenance,
        )
    }

    pub(crate) fn matches_native_attachment(
        &self,
        native_attachment: &crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> bool {
        native_attachment.matches_provenance(self.provenance)
    }

    pub(crate) fn into_parts(self) -> (DistributedPlan, PreparedFragmentSet) {
        (self.distributed_plan, self.prepared)
    }
}

fn next_native_encoding_provenance() -> u64 {
    static NEXT_PROVENANCE: AtomicU64 = AtomicU64::new(1);
    loop {
        let provenance = NEXT_PROVENANCE.fetch_add(1, Ordering::Relaxed);
        if provenance != 0 {
            return provenance;
        }
    }
}

/// Core-owned request finalizer for one Frontend-encoded distributed query.
/// Frontend supplies the only native bundle after reading the exact sealed
/// pair; Core retains lifecycle request construction and completion pairing.
pub struct PreparedDistributedQueryAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    intent: crate::query_execution::contract::DistributedQueryIntent,
    execution: QueryExecutionContext,
}

impl PreparedDistributedQueryAssembly {
    pub(crate) fn new(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        intent: crate::query_execution::contract::DistributedQueryIntent,
        execution: QueryExecutionContext,
    ) -> Self {
        Self {
            encoding,
            query_options,
            intent,
            execution,
        }
    }

    pub fn encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    pub fn finish(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        if !self.encoding.matches_native_attachment(&native_attachment) {
            return Err(
                "native fragment bundle does not match the sealed query encoding input".into(),
            );
        }
        let (_, prepared) = self.encoding.into_parts();
        crate::query_execution::contract::build_distributed_query_request_with_execution(
            prepared,
            native_attachment,
            self.query_options,
            self.intent,
            &self.execution,
        )
        .map_err(|error| error.to_string())
    }

    pub fn into_operation(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
        completion: PreparedQueryCompletion,
    ) -> Result<PreparedQueryOperation, String> {
        let request = self.finish(native_attachment)?;
        Ok(PreparedQueryOperation::Distributed(
            crate::query_execution::PreparedQueryDistributedOperation::new(request, completion),
        ))
    }
}

/// Prepare one SQL compiler result against the exact materializer that
/// admitted its bindings.  The Frontend calls the compiler itself, then hands
/// the sealed result to this Core-only preparation step; no caller can supply
/// a separate binding store or reacquire a current connector generation.
#[allow(clippy::too_many_arguments)]
pub fn prepare_compiled_distributed_query(
    distributed_plan: DistributedPlan,
    query_kernel: &crate::query_execution::kernels::QueryPreparationKernel,
    analyzer_catalog: &crate::catalog_application::query_materializer::CatalogServiceMaterializer<
        '_,
    >,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    query_options: Option<QueryOptions>,
    execution: &QueryExecutionContext,
    completion_intent: PostCompileIntent,
) -> Result<(PreparedDistributedQueryAssembly, PreparedQueryCompletion), String> {
    crate::query_execution::compiler::ensure_mainline_distributed_execution(
        false,
        query_kernel.exchange_port(),
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        query_kernel.connector_control().as_ref(),
        connector_context,
        Some(analyzer_catalog.query_table_bindings().as_ref()),
        None,
        crate::query_execution::compiler::scan_preparation_options(
            execution.optimizer_settings(),
            execution,
        )?,
    )?;
    let connector_static_planning =
        crate::query_execution::compiler::connector_static_planning_metrics(&prepared)?;
    let distributed_intent = match &completion_intent {
        PostCompileIntent::Result => {
            crate::query_execution::contract::DistributedQueryIntent::Result
        }
        PostCompileIntent::Profile { .. } => {
            crate::query_execution::contract::DistributedQueryIntent::Profile
        }
    };
    let completion = match completion_intent {
        PostCompileIntent::Result => PreparedQueryCompletion::result(),
        PostCompileIntent::Profile {
            planning_elapsed,
            execution_started_at,
        } => PreparedQueryCompletion::profile(
            distributed_plan.clone(),
            planning_elapsed,
            execution_started_at,
            connector_static_planning,
        ),
    };
    let assembly = PreparedDistributedQueryAssembly::new(
        NativeFragmentEncodingInput::new(distributed_plan, prepared),
        query_options,
        distributed_intent,
        execution.clone(),
    );
    Ok((assembly, completion))
}
