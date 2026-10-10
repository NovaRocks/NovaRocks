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

use crate::commit::model::{ArtifactWriter, OperationIntent};
use crate::iceberg::spec::TableMetadata;

/// Each preparer borrows exactly the canonical prefix and the attempt IO owner.
pub struct StagedView<'a> {
    metadata: &'a TableMetadata,
    intent: &'a OperationIntent,
    artifacts: &'a dyn ArtifactWriter,
}

impl<'a> StagedView<'a> {
    pub(super) fn new(
        metadata: &'a TableMetadata,
        intent: &'a OperationIntent,
        artifacts: &'a dyn ArtifactWriter,
    ) -> Self {
        Self {
            metadata,
            intent,
            artifacts,
        }
    }
    pub fn metadata(&self) -> &TableMetadata {
        self.metadata
    }
    pub fn intent(&self) -> &OperationIntent {
        self.intent
    }
    pub fn artifacts(&self) -> &dyn ArtifactWriter {
        self.artifacts
    }
    pub fn target_ref(&self) -> &str {
        self.intent.target_ref()
    }
}
