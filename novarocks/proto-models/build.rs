use std::env;
use std::path::{Path, PathBuf};

const IDL_DIR: &str = "../../idl/novarocks";
const PROTO_FILES: [&str; 9] = [
    "catalog.proto",
    "common.proto",
    "connector_common.proto",
    "connector_read.proto",
    "connector_write.proto",
    "expr.proto",
    "filter.proto",
    "plan.proto",
    "service.proto",
];

fn main() {
    for file in PROTO_FILES {
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
    config.file_descriptor_set_path(out_dir.join("novarocks_descriptor.bin"));
    // Connector maps are generated as BTreeMap so map fields retain a
    // deterministic key order for canonical codecs and structural validation.
    config.btree_map([".novarocks.connector_read", ".novarocks.connector_write"]);
    // Root result packets are retained until an explicit frontend ACK. Bytes
    // lets the backend replay the same allocation through Tonic instead of
    // cloning an untracked Vec for every poll.
    config.bytes([".novarocks.FetchResultResponse.result_arrow_ipc"]);
    config
        .compile_protos(&proto_paths, &[PathBuf::from(IDL_DIR)])
        .expect("compile NovaRocks native protobuf DTOs");
}
