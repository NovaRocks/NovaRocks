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

//! Shared retained Exchange route cost and neutral record layout.

use novarocks_execution_contract::{
    ExchangeInbound, ExchangeSource, ExchangeStreamClose, TaskDescriptor, TaskIdentity,
};
use std::mem::size_of;

/// The bounded, payload-free remainder of an installed receiver that stopped.
#[derive(Debug)]
pub struct RetainedInboundRecord {
    pub destination: TaskIdentity,
    pub inbound: Vec<ExchangeInbound>,
    /// A failure fence retains only the exact roster.
    pub closes: Option<Box<[ExchangeStreamClose]>>,
}

pub fn record_bytes(descriptor: &TaskDescriptor, retained_owner_bytes: usize) -> Option<usize> {
    if descriptor.topology().inbound().is_empty() {
        return Some(0);
    }
    descriptor.topology().inbound().iter().try_fold(
        size_of::<RetainedInboundRecord>()
            .checked_add(128)?
            .checked_add(retained_owner_bytes)?,
        |bytes, node| {
            bytes
                .checked_add(size_of::<ExchangeInbound>() + 64)?
                .checked_add(node.sources().len().checked_mul(
                    size_of::<ExchangeSource>() + size_of::<ExchangeStreamClose>() + 16,
                )?)
        },
    )
}
