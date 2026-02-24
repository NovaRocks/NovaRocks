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
use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

// Find a tool in PATH or fallback to thirdparty bin directory
// For protoc, prioritize thirdparty version to ensure version compatibility
fn find_tool(name: &str, thirdparty_bin: &std::path::Path) -> PathBuf {
    // For protoc, always use thirdparty version first to ensure compatibility
    if name == "protoc" {
        let tp_protoc = thirdparty_bin.join("protoc");
        if tp_protoc.exists() {
            return tp_protoc;
        }
    }
    // For other tools, try thirdparty first, then PATH
    let tp_tool = thirdparty_bin.join(name);
    if tp_tool.exists() {
        return tp_tool;
    }
    // Fallback to PATH
    if let Ok(path) = which::which(name) {
        return path;
    }
    // Last resort: return thirdparty path even if it doesn't exist
    // (will fail later with a better error message)
    thirdparty_bin.join(name)
}

fn has_thirdparty_layout(path: &Path) -> bool {
    path.join("include").exists() && (path.join("lib").exists() || path.join("lib64").exists())
}

fn normalize_thirdparty_root(path: &Path) -> Option<PathBuf> {
    if has_thirdparty_layout(path) {
        return Some(path.to_path_buf());
    }
    let installed = path.join("installed");
    if has_thirdparty_layout(&installed) {
        return Some(installed);
    }
    None
}

fn resolve_thirdparty_root(manifest_dir: &Path) -> Result<PathBuf, String> {
    if let Ok(raw) = env::var("STARROCKS_THIRDPARTY") {
        if !raw.trim().is_empty() {
            let configured = PathBuf::from(raw.trim());
            let resolved = if configured.is_absolute() {
                configured
            } else {
                manifest_dir.join(configured)
            };
            if let Some(root) = normalize_thirdparty_root(&resolved) {
                return Ok(root);
            }
            return Err(format!(
                "invalid STARROCKS_THIRDPARTY: '{}' (expected a thirdparty root or installed dir with include/ and lib|lib64/)",
                resolved.display()
            ));
        }
    }

    let default_root = manifest_dir.join("thirdparty");
    if let Some(root) = normalize_thirdparty_root(&default_root) {
        return Ok(root);
    }

    Err(format!(
        "thirdparty directory not found. expected default '{}' (or its installed subdir), or set STARROCKS_THIRDPARTY",
        default_root.display()
    ))
}

fn main() {
    println!("cargo:rerun-if-changed=cpp/compat.cpp");
    println!("cargo:rerun-if-changed=cpp/compat.h");
    println!("cargo:rerun-if-changed=cpp/brpc_server.cpp");
    println!("cargo:rerun-if-changed=idl/thrift/HeartbeatService.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/BackendService.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/InternalService.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/FrontendService.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Data.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Planner.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/PlanNodes.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Exprs.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Opcodes.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Status.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/StatusCode.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/Types.thrift");
    println!("cargo:rerun-if-changed=idl/thrift/StarrocksExternalService.thrift");
    println!("cargo:rerun-if-changed=idl/proto/internal_service.proto");
    println!("cargo:rerun-if-changed=idl/proto/data.proto");
    println!("cargo:rerun-if-changed=idl/proto/status.proto");
    println!("cargo:rerun-if-changed=idl/proto/types.proto");
    println!("cargo:rerun-if-changed=idl/proto/binlog.proto");
    println!("cargo:rerun-if-changed=idl/proto/descriptors.proto");
    println!("cargo:rerun-if-changed=idl/proto/olap_common.proto");
    println!("cargo:rerun-if-changed=idl/proto/olap_file.proto");
    println!("cargo:rerun-if-changed=idl/proto/lake_types.proto");
    println!("cargo:rerun-if-changed=idl/proto/lake_service.proto");
    println!("cargo:rerun-if-changed=idl/proto/tablet_schema.proto");
    println!("cargo:rerun-if-changed=idl/proto/starust_grpc.proto");
    println!("cargo:rerun-if-changed=idl/proto/staros/starlet.proto");
    println!("cargo:rerun-if-changed=idl/proto/staros/star_status.proto");
    println!("cargo:rerun-if-changed=idl/proto/staros/file_store.proto");
    println!("cargo:rerun-if-changed=idl/proto/staros/shard.proto");
    println!("cargo:rerun-if-changed=idl/proto/staros/worker.proto");
    println!("cargo:rerun-if-env-changed=STARROCKS_THIRDPARTY");

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let thrift_out = out_dir.join("thrift");
    let proto_out = out_dir.join("proto");
    let thrift_rs_out = out_dir.join("thrift_rs");

    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let project_thirdparty = resolve_thirdparty_root(&manifest_dir).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        eprintln!("  To build default thirdparty, run: ./thirdparty/build-thirdparty.sh");
        panic!("thirdparty directory not found");
    });
    println!(
        "cargo:warning=Using thirdparty root: {}",
        project_thirdparty.display()
    );

    let (tp_bin, tp_include, tp_lib, tp_lib64) = (
        project_thirdparty.join("bin"),
        project_thirdparty.join("include"),
        project_thirdparty.join("lib"),
        project_thirdparty.join("lib64"),
    );

    std::fs::create_dir_all(&thrift_out).expect("create thrift out dir");
    std::fs::create_dir_all(&proto_out).expect("create proto out dir");
    std::fs::create_dir_all(&thrift_rs_out).expect("create thrift_rs out dir");

    // Try to find thrift in PATH first, fallback to thirdparty
    let thrift_cmd = find_tool("thrift", &tp_bin);
    for thrift_file in [
        "idl/thrift/HeartbeatService.thrift",
        "idl/thrift/InternalService.thrift",
    ] {
        let thrift_status = std::process::Command::new(&thrift_cmd)
            .args(["-r", "-I", "idl/thrift", "--gen", "cpp", "-out"])
            .arg(&thrift_out)
            .arg(thrift_file)
            .status()
            .expect("run thrift");
        if !thrift_status.success() {
            panic!("thrift codegen failed for {thrift_file} with status={thrift_status}");
        }
    }

    for thrift_file in [
        "idl/thrift/InternalService.thrift",
        "idl/thrift/FrontendService.thrift",
        "idl/thrift/HeartbeatService.thrift",
        "idl/thrift/BackendService.thrift",
    ] {
        let thrift_rs_status = std::process::Command::new(&thrift_cmd)
            .args(["-r", "-I", "idl/thrift", "--gen", "rs", "-out"])
            .arg(&thrift_rs_out)
            .arg(thrift_file)
            .status()
            .expect("run thrift --gen rs");
        if !thrift_rs_status.success() {
            panic!("thrift rs codegen failed for {thrift_file} with status={thrift_rs_status}");
        }
    }

    patch_plan_nodes_rs(&thrift_rs_out);

    let mut module_files = Vec::new();
    for entry in fs::read_dir(&thrift_rs_out).expect("read thrift_rs out dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("rs file stem");
            module_files.push((stem.to_string(), path));
        }
    }
    module_files.sort_by(|a, b| a.0.cmp(&b.0));

    let mut wrapper = String::new();
    wrapper.push_str("// @generated by build.rs\n");
    for (stem, path) in module_files {
        let abs = path.canonicalize().expect("canonicalize thrift rs file");
        let lit = format!("{:?}", abs.to_string_lossy());
        let _ = writeln!(wrapper, "#[allow(unreachable_patterns, unused_variables)]");
        let _ = writeln!(wrapper, "#[path = {lit}]\npub mod {stem};\n");
    }
    fs::write(out_dir.join("thrift_root_mod.rs"), wrapper).expect("write thrift_root_mod.rs");

    let proto_files = [
        "idl/proto/status.proto",
        "idl/proto/types.proto",
        "idl/proto/data.proto",
        "idl/proto/binlog.proto",
        "idl/proto/olap_common.proto",
        "idl/proto/tablet_schema.proto",
        "idl/proto/olap_file.proto",
        "idl/proto/descriptors.proto",
        "idl/proto/lake_types.proto",
        "idl/proto/lake_service.proto",
        "idl/proto/internal_service.proto",
    ];

    // Try to find protoc in PATH first, fallback to thirdparty
    // Note: For Rust proto generation, we use protoc-bin-vendored (see below)
    let protoc_cmd = find_tool("protoc", &tp_bin);
    let mut protoc = std::process::Command::new(&protoc_cmd);
    protoc.arg("-I").arg("idl/proto");
    protoc.arg("--cpp_out").arg(&proto_out);
    for file in proto_files {
        protoc.arg(file);
    }
    let protoc_status = protoc.status().expect("run protoc");
    if !protoc_status.success() {
        panic!("protoc codegen failed with status={protoc_status}");
    }

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .file("cpp/compat.cpp")
        .file("cpp/brpc_server.cpp");

    for entry in std::fs::read_dir(&thrift_out).expect("read thrift out dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) == Some("cpp") {
            if path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|name| name.ends_with(".skeleton.cpp"))
            {
                continue;
            }
            build.file(path);
        }
    }

    for entry in std::fs::read_dir(&proto_out).expect("read proto out dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) == Some("cc") {
            build.file(path);
        }
    }

    build
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-Wno-deprecated-declarations")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-vla-cxx-extension")
        .flag_if_supported("-w") // Suppress all warnings for C++ code
        .define("GLOG_USE_GLOG_EXPORT", None) // Required for glog 0.7.1 static linking
        .define("GLOG_STATIC_DEFINE", None) // Required for glog 0.7.1 static linking
        .include("cpp")
        .include(&thrift_out)
        .include(&proto_out);

    // Check target platform for platform-specific configurations
    let target = std::env::var("TARGET").unwrap_or_default();
    let is_macos = target.contains("apple") || target.contains("darwin");

    // Add thirdparty include
    build.include(&tp_include);

    // macOS-specific include paths
    if is_macos {
        build.include("/opt/homebrew/opt/openssl/include");
        // Add boost include for thrift (thrift requires boost)
        build.include("/opt/homebrew/include");
    }
    build.compile("novarocks_compat");

    // Add library search paths
    println!("cargo:rustc-link-search=native={}", tp_lib.display());
    if tp_lib64.exists() {
        println!("cargo:rustc-link-search=native={}", tp_lib64.display());
    }

    // macOS-specific library search paths (for other libraries like gperftools)
    if is_macos {
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
    }

    // Link OpenSSL - use dynamic linking from thirdparty/installed
    // build_openssl() has copied system OpenSSL to thirdparty/installed/lib
    // Both macOS and Linux use the same OpenSSL from thirdparty/installed
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
    println!("cargo:rustc-link-lib=static=brpc");
    println!("cargo:rustc-link-lib=static=protobuf");
    println!("cargo:rustc-link-lib=static=glog");
    println!("cargo:rustc-link-lib=static=leveldb"); // Required by brpc
    println!("cargo:rustc-link-lib=static=gflags"); // Required by glog
    println!("cargo:rustc-link-lib=static=thrift"); // Use static thrift from thirdparty
    println!("cargo:rustc-link-lib=z");

    // macOS-specific libraries and paths
    if is_macos {
        // Optional: gperftools for profiling (brpc may reference but not require)
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
        println!("cargo:rustc-link-lib=static=profiler"); // Optional profiling support
        println!("cargo:rustc-link-lib=static=tcmalloc"); // Required for MallocExtension symbols
        println!("cargo:rustc-link-lib=objc");
        println!("cargo:rustc-link-lib=framework=Foundation");
    }

    println!("cargo:rustc-link-lib=pthread");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["idl/proto/starust_grpc.proto"], &["idl/proto"])
        .expect("compile novarocks_grpc.proto");

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(&["idl/proto/staros/starlet.proto"], &["idl/proto/staros"])
        .expect("compile staros starlet.proto");
}

fn patch_plan_nodes_rs(thrift_rs_out: &std::path::Path) {
    let path = thrift_rs_out.join("plan_nodes.rs");
    let Ok(mut contents) = fs::read_to_string(&path) else {
        return;
    };
    let broken = "pub const : TPlanNodeType = TPlanNodeType(39);";
    if contents.contains(broken) {
        contents = contents.replace(
            broken,
            "pub const RAW_VALUES_NODE: TPlanNodeType = TPlanNodeType(39);",
        );
        let _ = fs::write(path, contents);
    }
}
