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

//! Read-only recognition of legacy NovaRocks write-fence metadata.

use crate::iceberg::spec::Summary;

/// Prefix used by legacy provider-private write-fence refs.
pub const WRITE_FENCE_REF_PREFIX: &str = "novarocks-write-fence-";

const FENCE_PROP_VERSION: &str = "novarocks.write-fence.version";

/// Whether a snapshot carries the legacy fence-marker layout tag.
pub fn is_fence_marker_snapshot(summary: &Summary) -> bool {
    summary
        .additional_properties
        .contains_key(FENCE_PROP_VERSION)
}

/// Whether `ref_name` belongs to the legacy NovaRocks fence namespace.
pub fn is_fence_ref(ref_name: &str) -> bool {
    ref_name.starts_with(WRITE_FENCE_REF_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::Operation;

    #[test]
    fn prefix_and_marker_tag_are_recognized_without_writing_any_state() {
        assert!(is_fence_ref("novarocks-write-fence-op-1"));
        assert!(!is_fence_ref("main"));
        assert!(is_fence_marker_snapshot(&Summary {
            operation: Operation::Append,
            additional_properties: [(FENCE_PROP_VERSION.into(), "1".into())].into(),
        }));
        assert!(!is_fence_marker_snapshot(&Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        }));
    }
}
