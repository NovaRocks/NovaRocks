// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original Task binds a separately funded domain return at the actual
//! application boundary. Admitting its cut earlier could fence its own RPC.

use super::*;
pub(crate) struct DomainReturnRefusal {
    pub error: CausalError,
    pub role: AdmittedCompletionRole,
    pub admission: CutSourceAdmission,
}
impl TaskHostRecord {
    pub(crate) fn production_domain_descriptor(
        &self,
    ) -> Option<novarocks_execution_contract::TaskDescriptor> {
        self.resources
            .production
            .as_ref()?
            .state
            .lock()
            .expect("original domain Task descriptor")
            .descriptor
            .clone()
    }
    pub(crate) fn bind_domain_return(
        &mut self,
        role: AdmittedCompletionRole,
        admission: CutSourceAdmission,
    ) -> Result<(CausalInputCustody, InputAuthor), DomainReturnRefusal> {
        let (cut, sender) =
            match self
                .resources
                .causal
                .admit_input(&role.work, 1, self.pending.clone(), admission)
            {
                Ok(original) => original,
                Err(refusal) => {
                    return Err(DomainReturnRefusal {
                        error: refusal.error,
                        role,
                        admission: refusal.admission,
                    });
                }
            };
        Ok(CausalInputCustody::from_admitted(
            role,
            &mut self.resources.causal,
            cut,
            sender,
            CausalInputBinding {
                task: self.task,
                pending: self.pending.clone(),
                health: self.health.clone(),
                unresolved: novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CausalInputUnresolved.reason(),
                ),
            },
        ))
    }
}
