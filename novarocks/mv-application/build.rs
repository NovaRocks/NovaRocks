// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

#[path = "../../build-support/development.rs"]
mod development;

use std::env;
use std::path::{Path, PathBuf};

const IDL_DIR: &str = "idl";
const PROTO_FILES: &[&str] = &[
    "persistence/definition.proto",
    "persistence/interpretation.proto",
    "persistence/configuration.proto",
    "persistence/binding.proto",
    "persistence/marker.proto",
];

fn main() {
    development::emit();
    for file in PROTO_FILES.iter().copied() {
        println!(
            "cargo:rerun-if-changed={}",
            Path::new(IDL_DIR).join(file).display()
        );
    }

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");
    unsafe {
        env::set_var("PROTOC", protoc);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let proto_paths = PROTO_FILES
        .iter()
        .map(|file| Path::new(IDL_DIR).join(file))
        .collect::<Vec<_>>();
    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(out_dir.join("mv_persistence_descriptor.bin"));
    config
        .compile_protos(&proto_paths, &[PathBuf::from(IDL_DIR)])
        .expect("compile MV persistence private protobuf DTOs");
}
