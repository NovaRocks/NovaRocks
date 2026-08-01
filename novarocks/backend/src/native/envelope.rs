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

//! Backend-owned native fragment envelope validation.

use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks_protocol::plan;

pub(crate) fn require_root(
    fragment: &plan::PlanFragment,
) -> Result<&plan::DistributedNode, ProtocolError> {
    fragment.root.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("root"),
            "native PlanFragment requires root",
        )
    })
}

pub(crate) fn require_sink(
    fragment: &plan::PlanFragment,
) -> Result<&plan::DataSink, ProtocolError> {
    fragment.sink.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("sink"),
            "native PlanFragment requires sink",
        )
    })
}

fn error(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(
        ProtocolFamily::Native,
        path,
        ProtocolErrorKind::MissingField,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use super::{require_root, require_sink};
    use novarocks_protocol::plan;

    #[test]
    fn preserves_missing_root_error() {
        let error = require_root(&plan::PlanFragment::default()).expect_err("root is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root (missing field): native PlanFragment requires root"
        );
    }

    #[test]
    fn preserves_missing_sink_error() {
        let error = require_sink(&plan::PlanFragment::default()).expect_err("sink is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.sink (missing field): native PlanFragment requires sink"
        );
    }
}
