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

const IDL_DIR: &str = "../../idl/novarocks";
const NATIVE_PROTO_FILES: [&str; 5] = [
    "common.proto",
    "expr.proto",
    "filter.proto",
    "plan.proto",
    "service.proto",
];

fn main() {
    for file in NATIVE_PROTO_FILES {
        println!(
            "cargo:rerun-if-changed={}",
            Path::new(IDL_DIR).join(file).display()
        );
    }

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");
    unsafe {
        env::set_var("PROTOC", protoc);
    }

    // Native protobuf DTOs are generated exactly once by novarocks-proto.
    // The backend owns its ingress and outbound Tonic transport stubs
    // and refers to every request and response through canonical DTO modules.
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .codec_path("crate::rpc::codec::NativeProstCodec")
        .extern_path(".novarocks.common", "::novarocks_proto::common")
        .extern_path(".novarocks.expr", "::novarocks_proto::expr")
        .extern_path(".novarocks.filter", "::novarocks_proto::filter")
        .extern_path(".novarocks.plan", "::novarocks_proto::plan")
        .extern_path(".novarocks", "::novarocks_proto::novarocks")
        .compile_protos(
            &[PathBuf::from(IDL_DIR).join("service.proto")],
            &[PathBuf::from(IDL_DIR)],
        )
        .expect("compile native backend Tonic transport stubs against protocol DTOs");
}
