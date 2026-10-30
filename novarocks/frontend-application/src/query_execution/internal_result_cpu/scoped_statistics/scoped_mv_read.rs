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

//! MV read commands' concrete pure turn on the existing creator queue and exit packet.
use super::*;
impl InternalResultCpu {
    pub(crate) fn submit_mv_read(
        &self,
        original: Arc<crate::query::mv_read_action::MvReadOriginal>,
        window: ResultWindowAlias,
        source: InternalCpuActionSource,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> scoped_mv_refresh::MvCpuJob {
        scoped_mv_refresh::MvCpuJob::from_original(self.submit_creator_input(
            ClosedCreatorInput::MvRead(original),
            window,
            None,
            Some(source),
            cancellation,
            runtime,
            wake,
        ))
    }
}
