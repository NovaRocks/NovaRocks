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
//! Generic join-build sink wrapper.
//!
//! Responsibilities:
//! - Provides common sink lifecycle around concrete join-build state implementations.
//! - Delegates chunk ingestion and completion publishing to join-specific build-state objects.
//!
//! Key exported interfaces:
//! - Types: `JoinBuildSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::chunk::Chunk;
use crate::exec::node::join::JoinType;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::exchange;
use crate::runtime::runtime_state::RuntimeState;

/// Factory wrapper for generic join build sinks over pluggable build-state implementations.
pub struct JoinBuildSinkFactory {
    name: String,
    join_type: JoinType,
    exchange_key: exchange::ExchangeKey,
    dep: DependencyHandle,
}

impl JoinBuildSinkFactory {
    #[allow(dead_code)]
    pub(crate) fn new(
        join_type: JoinType,
        exchange_key: exchange::ExchangeKey,
        dep: DependencyHandle,
    ) -> Self {
        Self {
            name: "JoinBuildSink".to_string(),
            join_type,
            exchange_key,
            dep,
        }
    }
}

impl OperatorFactory for JoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(JoinBuildSinkOperator {
            name: self.name.clone(),
            join_type: self.join_type,
            exchange_key: self.exchange_key,
            sender_id: driver_id,
            be_number: 0,
            dep: self.dep.clone(),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct JoinBuildSinkOperator {
    name: String,
    join_type: JoinType,
    exchange_key: exchange::ExchangeKey,
    sender_id: i32,
    be_number: i32,
    dep: DependencyHandle,
    finished: bool,
}

impl Operator for JoinBuildSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for JoinBuildSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if self.join_type != JoinType::Inner {
            return Err("only INNER join is supported".to_string());
        }
        if !chunk.is_empty() {
            exchange::push_chunks(
                self.exchange_key,
                self.sender_id,
                self.be_number,
                vec![chunk],
                false,
            );
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        exchange::push_chunks(
            self.exchange_key,
            self.sender_id,
            self.be_number,
            Vec::new(),
            true,
        );
        self.dep.set_ready();
        Ok(())
    }
}
