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

//! Stable row-identity helpers used by execution expressions and MV flows.

use sha2::{Digest, Sha256};

/// Produce a deterministic identifier for a pair of stable source rows.
///
/// This deliberately has no SQL, catalog, or provider dependency. Callers
/// decide how the identity is persisted or surfaced; execution owns only the
/// canonical byte layout and digest rendering.
pub fn stable_join_row_key(
    left_uuid: &str,
    left_row_id: i64,
    right_uuid: &str,
    right_row_id: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(left_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(left_row_id.to_be_bytes());
    hasher.update([0]);
    hasher.update(right_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(right_row_id.to_be_bytes());
    let digest = hasher.finalize();
    let mut output = String::with_capacity("v1:".len() + 32);
    output.push_str("v1:");
    for byte in &digest[..16] {
        use std::fmt::Write;
        write!(&mut output, "{byte:02x}").expect("write to String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::stable_join_row_key;

    #[test]
    fn stable_join_row_key_is_deterministic_and_versioned() {
        let first = stable_join_row_key("left-uuid", 11, "right-uuid", 22);
        assert_eq!(
            first,
            stable_join_row_key("left-uuid", 11, "right-uuid", 22)
        );
        assert_eq!(first, "v1:929fd0cb9ddedffee0f213805f322e78");
    }

    #[test]
    fn stable_join_row_key_distinguishes_row_identity() {
        let base = stable_join_row_key("left-uuid", 11, "right-uuid", 22);
        assert_ne!(
            base,
            stable_join_row_key("other-left", 11, "right-uuid", 22)
        );
        assert_ne!(base, stable_join_row_key("left-uuid", 12, "right-uuid", 22));
        assert_ne!(
            base,
            stable_join_row_key("left-uuid", 11, "other-right", 22)
        );
        assert_ne!(base, stable_join_row_key("left-uuid", 11, "right-uuid", 23));
    }
}
