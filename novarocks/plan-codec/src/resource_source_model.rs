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

//! Cargo-owned library identities for allocation-request and work projections.
//! Identity checks do not certify behavior or grant memory. Resource geometry
//! remains subject to the source audit and the corresponding upgrade gates.

const LOCK: &[u8] = include_bytes!("../../../Cargo.lock");

const fn at(source: &[u8], position: usize, needle: &[u8]) -> bool {
    if position > source.len() || needle.len() > source.len() - position {
        return false;
    }
    let mut index = 0;
    while index < needle.len() {
        if source[position + index] != needle[index] {
            return false;
        }
        index += 1;
    }
    true
}
const fn equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    at(left, 0, right)
}
const fn field<'a>(record: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let length = record.len();
    let mut cursor = 0;
    let mut found = None;
    while cursor < length {
        let start = cursor;
        while cursor < length && record[cursor] != b'\n' {
            cursor += 1;
        }
        if at(record, start, key) {
            if found.is_some() {
                return None;
            }
            let value_start = start + key.len();
            if cursor <= value_start || record[cursor - 1] != b'"' {
                return None;
            }
            found = Some(
                record
                    .split_at(value_start)
                    .1
                    .split_at(cursor - 1 - value_start)
                    .0,
            );
        }
        cursor += 1;
    }
    found
}
const fn checksum_valid(checksum: &[u8]) -> bool {
    if checksum.len() != 64 {
        return false;
    }
    let mut i = 0;
    while i < checksum.len() {
        if !(checksum[i] >= b'0' && checksum[i] <= b'9')
            && !(checksum[i] >= b'a' && checksum[i] <= b'f')
        {
            return false;
        }
        i += 1;
    }
    true
}
const fn package_version<'a>(source: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let length = source.len();
    let mut cursor = 0;
    let mut found = None;
    while cursor < length {
        if source[cursor] == b'[' && at(source, cursor, b"[[package]]\n") {
            let start = cursor + b"[[package]]\n".len();
            cursor = start;
            while cursor < length
                && !(source[cursor] == b'[' && at(source, cursor, b"[[package]]\n"))
            {
                cursor += 1;
            }
            let record = source.split_at(start).1.split_at(cursor - start).0;
            if let Some(actual) = field(record, b"name = \"")
                && equal(actual, name)
            {
                if found.is_some() {
                    return None;
                }
                found = field(record, b"version = \"");
                if found.is_none() {
                    return None;
                }
            }
        } else {
            cursor += 1;
        }
    }
    found
}
/// The version is borrowed from the one resolved Cargo identity, not a second
/// release policy. Absence/ambiguity cannot publish a usable compiled profile.
pub(crate) const LOCKED_ARROW_VERSION: &str = match package_version(LOCK, b"arrow") {
    Some(version) => match std::str::from_utf8(version) {
        Ok(version) if !version.is_empty() => version,
        _ => panic!("Cargo Arrow identity is not representable"),
    },
    None => panic!("Cargo Arrow identity is missing or ambiguous"),
};

pub(crate) const fn locked_family(source: &[u8]) -> bool {
    let names: [&[u8]; 7] = [
        b"arrow",
        b"arrow-array",
        b"arrow-buffer",
        b"arrow-data",
        b"arrow-ipc",
        b"arrow-schema",
        b"flatbuffers",
    ];
    let arrow_version = match package_version(source, b"arrow") {
        Some(version) if !version.is_empty() => version,
        _ => return false,
    };
    let length = source.len();
    let mut counts = [0usize; 7];
    let mut cursor = 0;
    while cursor < length {
        if source[cursor] != b'[' || !at(source, cursor, b"[[package]]\n") {
            cursor += 1;
            continue;
        }
        let start = cursor + b"[[package]]\n".len();
        cursor = start;
        while cursor < length && !(source[cursor] == b'[' && at(source, cursor, b"[[package]]\n")) {
            cursor += 1;
        }
        let record = source.split_at(start).1.split_at(cursor - start).0;
        let name = match field(record, b"name = \"") {
            Some(name) => name,
            None => return false,
        };
        let mut member = 0;
        while member < names.len() {
            if equal(name, names[member]) {
                counts[member] += 1;
                let version = match field(record, b"version = \"") {
                    Some(version) if !version.is_empty() => version,
                    _ => return false,
                };
                if member < 6 && !equal(version, arrow_version) {
                    return false;
                }
                match field(record, b"source = \"") {
                    Some(origin)
                        if equal(
                            origin,
                            b"registry+https://github.com/rust-lang/crates.io-index",
                        ) => {}
                    _ => return false,
                }
                match field(record, b"checksum = \"") {
                    Some(checksum) if checksum_valid(checksum) => {}
                    _ => return false,
                }
            }
            member += 1;
        }
    }
    let mut member = 0;
    while member < counts.len() {
        if counts[member] != 1 {
            return false;
        }
        member += 1;
    }
    true
}
pub(crate) const LOCKED_FAMILY: bool = locked_family(LOCK)
    && equal(
        arrow::ARROW_VERSION.as_bytes(),
        LOCKED_ARROW_VERSION.as_bytes(),
    );
pub(crate) use novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN;

#[cfg(test)]
mod tests {
    use super::{LOCKED_ARROW_VERSION, locked_family, package_version};

    #[test]
    fn lockfile_resource_sources_follow_cargo_identity_without_certifying_geometry() {
        let source = include_str!("../../../Cargo.lock");
        assert!(locked_family(source.as_bytes()));
        // i256 Display uses fixed limbs in Arrow 60. Unrelated num-bigint
        // identities no longer belong to the constant-reader source model.
        assert!(locked_family(
            format!("{source}\n[[package]]\nname = \"num-bigint\"\nversion = \"999.0.0\"\n")
                .as_bytes()
        ));
        assert_eq!(LOCKED_ARROW_VERSION, arrow::ARROW_VERSION);
        // A synchronized release change is an identity-consistency success,
        // not compatibility acceptance. No release/checksum is a guard policy.
        let current = format!("version = \"{LOCKED_ARROW_VERSION}\"");
        let changed = source.replace(&current, "version = \"999.0.0\"");
        assert!(locked_family(changed.as_bytes()));
        let split = source.replacen(
            &format!("name = \"arrow-array\"\n{current}"),
            "name = \"arrow-array\"\nversion = \"999.0.0\"",
            1,
        );
        assert!(!locked_family(split.as_bytes()));
        for member in ["arrow", "flatbuffers"] {
            let version =
                std::str::from_utf8(package_version(source.as_bytes(), member.as_bytes()).unwrap())
                    .unwrap();
            let duplicate =
                format!("{source}\n[[package]]\nname = \"{member}\"\nversion = \"{version}\"\n");
            assert!(!locked_family(duplicate.as_bytes()));
            assert!(!locked_family(
                source
                    .replacen(
                        &format!("name = \"{member}\""),
                        "name = \"missing-model-source\"",
                        1
                    )
                    .as_bytes()
            ));
        }
        // Select an actual required record rather than the first arbitrary crate.
        let arrow = source.find("name = \"arrow\"\n").unwrap();
        let tail = source[arrow..].replacen(
            "source = \"registry+https://github.com/rust-lang/crates.io-index\"",
            "source = \"git+https://example.invalid/fork\"",
            1,
        );
        let fork = format!("{}{tail}", &source[..arrow]);
        assert!(!locked_family(fork.as_bytes()));
        let tail = source[arrow..].replacen("checksum = \"", "checksum = \"invalid-", 1);
        assert!(!locked_family(
            format!("{}{tail}", &source[..arrow]).as_bytes()
        ));
    }
}
