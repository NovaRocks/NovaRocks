use std::env;
use std::path::{Path, PathBuf};

#[path = "build/resource_layout.rs"]
mod resource_layout;

const IDL_DIR: &str = "../../idl/novarocks";
const PROTO_FILES: [&str; 16] = [
    "catalog.proto",
    "common.proto",
    "connector_common.proto",
    "connector_read.proto",
    "connector_write.proto",
    "filter.proto",
    "execution_shared.proto",
    "arrow_type.proto",
    "call_request.proto",
    "physical_type_v2.proto",
    "value_type.proto",
    "physical_control_v2.proto",
    "physical_semantics_v2.proto",
    "physical_package_v2.proto",
    "service.proto",
    "result.proto",
];

fn main() {
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=build/resource_layout.rs");
    for file in PROTO_FILES {
        println!(
            "cargo:rerun-if-changed={}",
            Path::new(IDL_DIR).join(file).display()
        );
    }

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let proto_paths = PROTO_FILES
        .iter()
        .map(|file| Path::new(IDL_DIR).join(file))
        .collect::<Vec<_>>();
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.file_descriptor_set_path(out_dir.join("novarocks_descriptor.bin"));
    // Connector maps are generated as BTreeMap so map fields retain a
    // deterministic key order for canonical codecs and structural validation.
    config.btree_map([".novarocks.connector_read", ".novarocks.connector_write"]);
    // Root result packets are retained until an explicit frontend ACK. Bytes
    // lets the backend replay the same allocation through Tonic instead of
    // cloning an untracked Vec for every poll.
    config.bytes([
        ".novarocks.FetchResultResponse.result_arrow_ipc",
        ".novarocks.result.RootData.body",
        ".novarocks.CreateTaskRequest.frozen_fragment",
        ".novarocks.CreateTaskRequest.creation_metadata",
        ".novarocks.FrozenFragment.package",
        ".novarocks.ExchangeUnitData.payload",
    ]);
    let descriptors = config
        .load_fds(&proto_paths, &[PathBuf::from(IDL_DIR)])
        .expect("load NovaRocks native protobuf descriptors");
    let resources = resource_layout::Generator::new(&descriptors, &mut config);
    config
        .compile_fds(descriptors)
        .expect("compile NovaRocks native protobuf DTOs");
    resources.generate(&out_dir, Path::new("src/lib.rs"));
}
