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

//! Frontend provider effects for statement-owned maintenance.

use novarocks_table_maintenance::product::{
    DistributedRewriteSession, RewriteIntent, TableMaintenanceEffectPort,
};
use novarocks_table_maintenance::runtime::TerminalError as OptimizeTerminalError;
use novarocks_table_maintenance::{
    AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceEffectId, MaintenanceTarget,
};

use crate::query_execution::maintenance::TableMaintenanceEngine;

/// Frontend provider/native-query adapter for the neutral maintenance product.
/// It keeps opaque connector sessions and fragment encoding here, while the
/// product crate owns all target-gate, cohort and terminal business decisions.
pub(crate) struct LegacyMaintenanceEffects<'a> {
    engine: &'a dyn TableMaintenanceEngine,
    state: Option<&'a crate::query_execution::maintenance::MaintenanceActionState>,
}

impl<'a> LegacyMaintenanceEffects<'a> {
    pub(crate) fn new(
        engine: &'a dyn TableMaintenanceEngine,
        state: &'a crate::query_execution::maintenance::MaintenanceActionState,
    ) -> Self {
        Self {
            engine,
            state: Some(state),
        }
    }
}

impl TableMaintenanceEffectPort for LegacyMaintenanceEffects<'_> {
    fn reject_user_action_on_mv<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            self.engine
                .reject_user_action_on_mv(target)
                .await
                .map_err(|error| {
                    let terminal =
                        crate::query_execution::maintenance::rewrite_job::guard_terminal_error(
                            error,
                        );
                    if self.state.is_some_and(|state| state.is_trino_expiry()) {
                        crate::query_execution::maintenance::iceberg::retention_format_error(
                            terminal,
                            "expire_snapshots",
                            target,
                        )
                    } else {
                        terminal
                    }
                })
        })
    }

    fn execute_metadata<'a>(
        &'a self,
        request: MaintenanceActionRequest,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<MaintenanceActionOutcome, OptimizeTerminalError>,
    > {
        Box::pin(async move {
            let state = self.state.ok_or_else(|| {
                OptimizeTerminalError::pre_dispatch_failed(
                    "rewrite effect cannot dispatch metadata",
                )
            })?;
            self.engine.execute_action(state, request).await
        })
    }

    fn execute_metadata_with_id<'a>(
        &'a self,
        request: MaintenanceActionRequest,
        effect_id: MaintenanceEffectId,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<AutomaticMaintenanceOutcome, OptimizeTerminalError>,
    > {
        Box::pin(async move {
            let state = self.state.ok_or_else(|| {
                OptimizeTerminalError::pre_dispatch_failed(
                    "rewrite effect cannot dispatch metadata",
                )
            })?;
            self.engine
                .execute_automatic_metadata_action(state, request, effect_id)
                .await
        })
    }

    fn begin_rewrite<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _intent: RewriteIntent,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<Box<dyn DistributedRewriteSession + 'a>, String>,
    > {
        // Metadata commands cannot create the governed rewrite Source.
        Box::pin(async {
            Err("metadata command effect cannot dispatch a distributed rewrite".into())
        })
    }
}

pub(crate) fn rewrite_compile_terminal(
    error: novarocks_sql::compiler::SqlCompileError,
) -> OptimizeTerminalError {
    match error {
        novarocks_sql::compiler::SqlCompileError::Cancelled => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::Cancelled,
            )
        }
        novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            )
        }
        novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        }
        novarocks_sql::compiler::SqlCompileError::Analyze(error) => match error.control_error() {
            Some(error) => OptimizeTerminalError::from_compile_control(error),
            None => OptimizeTerminalError::pre_dispatch_failed(error.to_string()),
        },
        novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => {
            OptimizeTerminalError::from_compile_control(
                novarocks_type_contract::CompileControlError::Source(source),
            )
        }
        error => OptimizeTerminalError::pre_dispatch_failed(error.to_string()),
    }
}
