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

//! A library upgrade must re-audit the bytes request model before publication.
//! This compile-time guard examines the locked dependency, allocates nothing
//! at runtime and establishes no allocator, live-byte or host grant bound.

const fn matches_at(haystack: &[u8], needle: &[u8], start: usize) -> bool {
    if start > haystack.len() || needle.len() > haystack.len() - start {
        return false;
    }
    let mut offset = 0;
    while offset < needle.len() {
        if haystack[start + offset] != needle[offset] {
            return false;
        }
        offset += 1;
    }
    true
}

pub(crate) const fn locked_bytes_matches(haystack: &[u8]) -> bool {
    let name = b"\nname = \"bytes\"\n";
    let expected = b"\nname = \"bytes\"\nversion = \"1.11.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"1e748733b7cbc798e1434b6ac524f0c1ff2ab456fe201501e6497c8417a4fc33\"\n";
    let mut start = 0;
    let mut found = false;
    while start < haystack.len() {
        if matches_at(haystack, name, start) {
            if found || !matches_at(haystack, expected, start) {
                return false;
            }
            found = true;
        }
        start += 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_request_profile_rejects_missing_upgraded_forked_and_mixed_dependencies() {
        let lock = include_str!("../../../../Cargo.lock");
        assert!(locked_bytes_matches(lock.as_bytes()));
        for (from, to) in [
            ("name = \"bytes\"", "name = \"other-bytes\""),
            (
                "name = \"bytes\"\nversion = \"1.11.1\"",
                "name = \"bytes\"\nversion = \"1.12.0\"",
            ),
            (
                "name = \"bytes\"\nversion = \"1.11.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"",
                "name = \"bytes\"\nversion = \"1.11.1\"\nsource = \"git+https://example.invalid/fork\"",
            ),
            (
                "1e748733b7cbc798e1434b6ac524f0c1ff2ab456fe201501e6497c8417a4fc33",
                "changed-source-checksum",
            ),
        ] {
            assert!(lock.contains(from));
            assert!(!locked_bytes_matches(lock.replace(from, to).as_bytes()));
        }
        let mixed = format!("{lock}\n[[package]]\nname = \"bytes\"\nversion = \"1.12.0\"\n");
        assert!(!locked_bytes_matches(mixed.as_bytes()));
        let duplicate = format!(
            "{lock}\n[[package]]\nname = \"bytes\"\nversion = \"1.11.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"1e748733b7cbc798e1434b6ac524f0c1ff2ab456fe201501e6497c8417a4fc33\"\n"
        );
        assert!(!locked_bytes_matches(duplicate.as_bytes()));
        assert!(!locked_bytes_matches(b""));
        assert!(!locked_bytes_matches(
            b"\nname = \"bytes\"\nversion = \"1.11.1\"\n"
        ));
    }
}
