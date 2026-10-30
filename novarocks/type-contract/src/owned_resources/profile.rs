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

//! Fixed-size build receipts from the actual compiled common source owner.
//! Compiler overrides and locked bytes drift fail at this crate's build gate.

pub const LOCKED_TOOLCHAIN: bool =
    matches_compiler(env!("NOVAROCKS_OWNED_RESOURCE_RUST").as_bytes());

const fn matches_compiler(actual: &[u8]) -> bool {
    let expected = b"1.98.1";
    if actual.len() != expected.len() {
        return false;
    }
    let mut at = 0;
    while at < expected.len() {
        if actual[at] != expected[at] {
            return false;
        }
        at += 1;
    }
    true
}

pub const fn require_locked_bytes_request_model() {
    let actual = env!("NOVAROCKS_OWNED_RESOURCE_BYTES").as_bytes();
    let expected = b"1.11.1";
    assert!(
        actual.len() == expected.len(),
        "bytes request model requires re-audit"
    );
    let mut at = 0;
    while at < expected.len() {
        assert!(
            actual[at] == expected[at],
            "bytes request model requires re-audit"
        );
        at += 1;
    }
}
const _: () = assert!(LOCKED_TOOLCHAIN);
const _: () = require_locked_bytes_request_model();
