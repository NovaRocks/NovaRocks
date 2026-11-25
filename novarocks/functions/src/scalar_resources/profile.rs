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

//! A source upgrade requires a new request proof before this model compiles.
//! This receipt fixes source identity, not dependency feature resolution.

const SOURCES: [(&[u8], &[u8]); 6] = [
    (b"\nname = \"arrow-array\"\n", b"\nname = \"arrow-array\"\nversion = \"60.2.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"a7a75080e8066219ed2d1e3b8fd2eb0b1a4b35d03b6fa87fb1035e93bd8dac95\"\n"),
    (b"\nname = \"arrow-buffer\"\n", b"\nname = \"arrow-buffer\"\nversion = \"60.2.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"6c6bf2edf52662586214410c6030204215453eb86b8715037e8951326e173b19\"\n"),
    (b"\nname = \"arrow-data\"\n", b"\nname = \"arrow-data\"\nversion = \"60.2.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"d0a7becabdb9bf3fa058b55845c52e0f792bedc88fd0e28d6580c9730a902b60\"\n"),
    (b"\nname = \"arrow-schema\"\n", b"\nname = \"arrow-schema\"\nversion = \"60.2.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"3e7574c4175cc731e0d26a8384bc25d455c5e220250988a772c1af843592444b\"\n"),
    (b"\nname = \"arrow-cast\"\n", b"\nname = \"arrow-cast\"\nversion = \"60.2.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"fcad6713dda348c245cd6ef36a578b4362c143c3a9dd829aa19e89f407c3e48b\"\n"),
    (b"\nname = \"num-traits\"\n", b"\nname = \"num-traits\"\nversion = \"0.2.19\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"071dfc062690e90b734c0b2273ce72ad0ffa95f0c74596bc250dcfd960262841\"\n"),
];

const fn matches_at(haystack: &[u8], needle: &[u8], at: usize) -> bool {
    if at > haystack.len() || needle.len() > haystack.len() - at {
        return false;
    }
    let mut offset = 0;
    while offset < needle.len() {
        if haystack[at + offset] != needle[offset] {
            return false;
        }
        offset += 1;
    }
    true
}

pub(crate) fn locked_sources_match(lock: &[u8]) -> bool {
    let mut source = 0;
    while source < SOURCES.len() {
        let (name, expected) = SOURCES[source];
        let mut found = false;
        let mut at = 0;
        while at < lock.len() {
            if matches_at(lock, name, at) {
                if found || !matches_at(lock, expected, at) {
                    return false;
                }
                found = true;
            }
            at += 1;
        }
        if !found {
            return false;
        }
        source += 1;
    }
    true
}

pub(crate) fn audited_compiler(actual: &str) -> bool {
    let mut release = actual.lines().filter(|line| line.starts_with("release: "));
    let mut commit = actual
        .lines()
        .filter(|line| line.starts_with("commit-hash: "));
    release.next() == Some("release: 1.98.1")
        && release.next().is_none()
        && commit.next() == Some("commit-hash: 48a229ceaefd4985c50990b14116b6d856af0985")
        && commit.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_resources_private_layout_guard_requires_the_exact_compiler_source() {
        let exact = "release: 1.98.1\ncommit-hash: 48a229ceaefd4985c50990b14116b6d856af0985\n";
        assert!(audited_compiler(exact));
        for actual in [
            String::new(),
            exact.replace("1.98.1", "1.98.0"),
            exact.replace(
                "48a229ceaefd4985c50990b14116b6d856af0985",
                "unreviewed-compiler",
            ),
            format!("{exact}release: 1.98.1\n"),
            format!("{exact}commit-hash: 48a229ceaefd4985c50990b14116b6d856af0985\n"),
        ] {
            assert!(!audited_compiler(&actual));
        }
    }

    #[test]
    fn scalar_resources_locked_profile_rejects_missing_changed_and_ambiguous_sources() {
        let lock = include_str!("../../../../Cargo.lock");
        assert!(locked_sources_match(lock.as_bytes()));
        for (name, expected) in SOURCES {
            let name = std::str::from_utf8(name).unwrap();
            let expected = std::str::from_utf8(expected).unwrap();
            let version = expected
                .lines()
                .find(|line| line.starts_with("version = "))
                .unwrap();
            assert!(!locked_sources_match(
                lock.replace(name, "\nname = \"unrelated\"\n").as_bytes()
            ));
            for (old, new) in [
                (version, "version = \"0.0.0\""),
                (
                    "registry+https://github.com/rust-lang/crates.io-index",
                    "git+https://example.invalid/fork",
                ),
                ("checksum = \"", "checksum = \"changed-"),
            ] {
                assert!(!locked_sources_match(
                    lock.replace(expected, &expected.replace(old, new))
                        .as_bytes()
                ));
            }
            assert!(!locked_sources_match(
                format!("{lock}\n[[package]]{expected}").as_bytes()
            ));
        }
        assert!(!locked_sources_match(b""));
    }
}
