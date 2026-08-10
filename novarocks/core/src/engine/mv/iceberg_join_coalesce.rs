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

pub(crate) fn stable_join_row_key(
    left_uuid: &str,
    left_row_id: i64,
    right_uuid: &str,
    right_row_id: i64,
) -> String {
    novarocks_execution::exec::mv::stable_join_row_key(
        left_uuid,
        left_row_id,
        right_uuid,
        right_row_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_join_row_key_is_deterministic() {
        let first = stable_join_row_key("left-uuid", 11, "right-uuid", 22);
        let second = stable_join_row_key("left-uuid", 11, "right-uuid", 22);

        assert_eq!(first, second);
        assert!(first.starts_with("v1:"));
        assert_eq!(first.len(), "v1:".len() + 32);
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
