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

use std::env;
use std::path::{Path, PathBuf};

const STAROS_IDL: &str = "idl/staros_v1.proto";
const STORAGE_IDL: &str = "idl/storage_v1.proto";

fn main() {
    println!("cargo:rerun-if-changed={STAROS_IDL}");
    println!("cargo:rerun-if-changed={STORAGE_IDL}");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");
    unsafe {
        env::set_var("PROTOC", protoc);
    }

    let descriptor_path = PathBuf::from(
        env::var_os("OUT_DIR").expect("Cargo supplies OUT_DIR for connector codegen"),
    )
    .join("starrocks-storage-v1-descriptor.bin");
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(
            &[PathBuf::from(STAROS_IDL), PathBuf::from(STORAGE_IDL)],
            &[PathBuf::from(
                Path::new(STAROS_IDL)
                    .parent()
                    .expect("StarOS IDL parent directory"),
            )],
        )
        .expect("compile provider-private StarOS V1 read client");
}
