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
use std::time::Duration;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding;

#[derive(Clone, Debug)]
pub struct ExchangeSourceNode {
    pub node_id: i32,
    pub timeout: Duration,
    pub expected_chunk_schema: ChunkSchemaRef,
    pub(crate) native_runtime_filter_specs: Vec<RuntimeFilterConsumerBinding>,
}

impl ExchangeSourceNode {
    pub fn new(node_id: i32, timeout: Duration, expected_chunk_schema: ChunkSchemaRef) -> Self {
        Self {
            node_id,
            timeout,
            expected_chunk_schema,
            native_runtime_filter_specs: Vec::new(),
        }
    }

    pub fn profile_name(&self) -> String {
        format!("EXCHANGE_SOURCE (id={})", self.node_id)
    }

    pub(crate) fn native_runtime_filter_specs(&self) -> &[RuntimeFilterConsumerBinding] {
        &self.native_runtime_filter_specs
    }

    pub fn expected_chunk_schema(&self) -> ChunkSchemaRef {
        self.expected_chunk_schema.clone()
    }

    pub(crate) fn set_native_runtime_filter_specs(
        &mut self,
        specs: Vec<RuntimeFilterConsumerBinding>,
    ) {
        self.native_runtime_filter_specs = specs;
    }

    pub fn with_runtime_filter_consumers(
        mut self,
        specs: Vec<RuntimeFilterConsumerBinding>,
    ) -> Self {
        self.native_runtime_filter_specs = specs;
        self
    }
}
