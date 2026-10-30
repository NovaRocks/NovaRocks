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

use std::{env, process::Command};
#[path = "src/owned_resources/bytes_profile.rs"]
mod byte_request_model_profile;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Cargo fingerprints the compiler; RUSTC is its build-script input, not a user override.
    println!("cargo:rerun-if-changed=../../rust-toolchain.toml");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=src/owned_resources/bytes_profile.rs");
    assert!(
        byte_request_model_profile::locked_bytes_matches(include_bytes!("../../Cargo.lock")),
        "owned-resource bytes request model requires re-audit after a locked source change"
    );
    println!("cargo:rustc-env=NOVAROCKS_OWNED_RESOURCE_BYTES=1.11.1");
    let compiler = env::var_os("RUSTC").expect("Cargo must provide RUSTC");
    let result = Command::new(compiler)
        .arg("--version")
        .output()
        .expect("failed to inspect the actual owned-resource compiler");
    // Arc allocation layout and Vec/String growth requests in the supported
    // resource model are derived from this exact standard-library source.
    // A +toolchain override must not silently compile an unreviewed model.
    assert!(
        result.status.success() && result.stdout.starts_with(b"rustc 1.98.1 "),
        "owned-resource allocation model requires the audited Rust 1.98.1 compiler"
    );
    println!("cargo:rustc-env=NOVAROCKS_OWNED_RESOURCE_RUST=1.98.1");
}
