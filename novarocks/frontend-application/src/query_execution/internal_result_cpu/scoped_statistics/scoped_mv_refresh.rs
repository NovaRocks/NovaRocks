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

//! Closed MV turns on the existing creator queue and actual-exit packet.
use super::*;

pub(crate) enum MvRefreshCpuTurn {
    Preparation(Arc<crate::query_execution::mv_assembly::refresh_preparation::async_original::MvRefreshPreparationOriginal>),
    Activation(Arc<crate::query_execution::mv_assembly::activation_cpu::MvWriteActivationOriginal>),
}
impl MvRefreshCpuTurn {
    pub(super) fn run_once(&self) {
        match self {
            Self::Preparation(original) => original.run_once(),
            Self::Activation(original) => original.run_once(),
        }
    }
}

impl InternalResultCpu {
    pub(crate) fn submit_mv_refresh_turn(
        &self,
        turn: MvRefreshCpuTurn,
        window: ResultWindowAlias,
        source: InternalCpuActionSource,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> MvCpuJob {
        MvCpuJob {
            original: self.submit_creator_input(
                ClosedCreatorInput::MvRefresh(turn),
                window,
                None,
                Some(source),
                cancellation,
                runtime,
                wake,
            ),
        }
    }
}

pub(crate) type MvCpuJob = crate::query_execution::internal_result_cpu::CreatorCpuJob;
