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

#[path = "src/scalar_resources/profile.rs"]
mod scalar_arrow_profile;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=src/scalar_resources/profile.rs");
    assert!(
        scalar_arrow_profile::locked_sources_match(include_bytes!("../../Cargo.lock")),
        "scalar Arrow request source model requires re-audit"
    );
    let compiler = std::env::var_os("RUSTC").expect("Cargo must provide RUSTC");
    let actual = std::process::Command::new(compiler)
        .arg("-Vv")
        .output()
        .expect("failed to inspect the actual scalar request compiler");
    assert!(
        actual.status.success()
            && std::str::from_utf8(&actual.stdout)
                .is_ok_and(scalar_arrow_profile::audited_compiler),
        "scalar Arrow request model requires the audited Rust compiler source"
    );
    // Private enum layout uses this exact compiler's audited tagged/niche
    // algorithm. Features/target geometry remain separate source prerequisites.
    println!("cargo:rustc-env=NOVAROCKS_SCALAR_ARROW_SOURCE=58.4.0");
}
