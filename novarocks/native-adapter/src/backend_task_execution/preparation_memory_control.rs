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
use novarocks_execution::runtime::preparation_memory::SynchronousPreparationControl;
use novarocks_worker::PreparationControlLoan;

pub(crate) struct WorkerPreparationMemoryControl<'a> {
    pub(crate) preparation: &'a PreparationControlLoan<'a>,
}
impl SynchronousPreparationControl for WorkerPreparationMemoryControl<'_> {
    fn checkpoint(&self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.preparation.checkpoint()
    }
    fn wait(&self) -> novarocks_execution::runtime::task::ExecutionResult<()> {
        self.preparation.wait()
    }
}
