// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Kernel-local runtime-filter carriers.
//!
//! Runtime-filter semantics are frozen in `novarocks-execution`.  Core retains
//! only the expression and operator coordinates required to invoke that
//! contract from a pipeline kernel.

use crate::runtime_filter as execution;
pub use execution::RuntimeFilterExecutionContract;
pub use execution::RuntimeFilterReduction as RuntimeFilterExecutionReduction;

use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerBinding {
    pub expr_id: ExprId,
    pub contract: execution::RuntimeFilterConsumerContract,
    /// Present only for a connector scan whose FE-pinned source boundary is
    /// eligible for scan-unit pre-reader evaluation. Core carries this sealed
    /// value but does not interpret scan-domain facts or decisions.
    pub scan_domain: Option<execution::scan_domain::RuntimeFilterScanDomainBinding>,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterConsumerNode {
    pub input: Box<ExecNode>,
    pub owner_node_id: i32,
    pub bindings: Vec<RuntimeFilterConsumerBinding>,
}

impl RuntimeFilterConsumerNode {
    pub fn new(
        input: ExecNode,
        owner_node_id: i32,
        bindings: Vec<RuntimeFilterConsumerBinding>,
    ) -> Self {
        Self {
            input: Box::new(input),
            owner_node_id,
            bindings,
        }
    }

    pub fn input(&self) -> &ExecNode {
        &self.input
    }

    pub fn input_mut(&mut self) -> &mut ExecNode {
        &mut self.input
    }
}

impl RuntimeFilterConsumerBinding {
    pub const fn new(
        expr_id: ExprId,
        contract: execution::RuntimeFilterConsumerContract,
        scan_domain: Option<execution::scan_domain::RuntimeFilterScanDomainBinding>,
    ) -> Self {
        Self {
            expr_id,
            contract,
            scan_domain,
        }
    }

    pub const fn contract(&self) -> &execution::RuntimeFilterConsumerContract {
        &self.contract
    }

    pub const fn binding_id(&self) -> u32 {
        self.contract.binding_id().get()
    }

    pub const fn channel_id(&self) -> u32 {
        self.contract.channel_id().get()
    }

    pub const fn activation(&self) -> execution::ConsumerActivation {
        self.contract.activation()
    }

    pub const fn execution_contract(&self) -> &execution::RuntimeFilterExecutionContract {
        self.contract.contract()
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime_filter::{
        ConsumerActivation, RuntimeFilterBindingId, RuntimeFilterChannelId,
        RuntimeFilterConsumerContract, RuntimeFilterExecutionContract,
        RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics,
    };
    use arrow::datatypes::DataType;

    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode};
    use novarocks_types::SlotId;

    #[test]
    fn consumer_carrier_retains_only_kernel_coordinate_and_execution_contract() {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let contract = RuntimeFilterConsumerContract::membership_blocking(
            RuntimeFilterBindingId::new(1),
            RuntimeFilterChannelId::new(2),
            RuntimeFilterExecutionContract::Membership(
                RuntimeFilterMembershipSchema::new(
                    &DataType::Int64,
                    RuntimeFilterNullSemantics::NeverMatches,
                )
                .expect("membership schema"),
            ),
        )
        .expect("membership consumer contract");
        let binding = RuntimeFilterConsumerBinding::new(expr_id, contract, None);

        assert_eq!(binding.expr_id, expr_id);
        assert_eq!(binding.contract().binding_id().get(), 1);
        assert_eq!(binding.contract().channel_id().get(), 2);
        assert_eq!(
            binding.contract().activation(),
            ConsumerActivation::BlockingSnapshot
        );
    }
}
