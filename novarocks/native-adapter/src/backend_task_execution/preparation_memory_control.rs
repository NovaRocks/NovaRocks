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

//! Borrow the original Worker stop/wait loan through Execution's neutral port.
use novarocks_execution::runtime::preparation_memory::{
    PreparationMemoryStop, SynchronousPreparationControl,
};
use novarocks_worker::{PreparationControlLoan, PreparationStop};

pub(crate) struct WorkerPreparationMemoryControl<'a> {
    pub(crate) preparation: &'a PreparationControlLoan<'a>,
}
impl SynchronousPreparationControl for WorkerPreparationMemoryControl<'_> {
    fn checkpoint(&self) -> Result<(), PreparationMemoryStop> {
        self.preparation.checkpoint().map_err(neutral_stop)
    }
    fn wait(&self) -> Result<(), PreparationMemoryStop> {
        self.preparation.wait().map_err(neutral_stop)
    }
}
fn neutral_stop(stop: PreparationStop) -> PreparationMemoryStop {
    match stop {
        PreparationStop::Cancel(cause) => PreparationMemoryStop::Cancel(cause),
        PreparationStop::Abort(cause) => PreparationMemoryStop::Abort(cause),
    }
}
pub(crate) fn worker_stop(stop: PreparationMemoryStop) -> PreparationStop {
    match stop {
        PreparationMemoryStop::Cancel(cause) => PreparationStop::Cancel(cause),
        PreparationMemoryStop::Abort(cause) => PreparationStop::Abort(cause),
    }
}
