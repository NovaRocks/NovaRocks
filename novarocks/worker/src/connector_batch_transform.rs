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

//! Execution-domain projection applied to a typed connector page.

/// Worker-owned projection applied after a typed connector reader yields a
/// batch and before execution materializes its output Chunk.
///
/// This deliberately exposes no connector registry, provider identity, or
/// query lifecycle capability. Typed scan decoding resolves those exclusively
/// through its query-leased `CatalogHandle` runtime.
pub trait ConnectorBatchTransform: Send + Sync {
    fn transform(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String>;
}
