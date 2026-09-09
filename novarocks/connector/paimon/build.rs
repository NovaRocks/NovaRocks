use std::env;
use std::path::{Path, PathBuf};

const IDL_DIR: &str = "idl";
const PROTO_FILES: &[&str] = &["common.proto", "read.proto"];

fn main() {
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
    config.btree_map(["."]);
    config.file_descriptor_set_path(out_dir.join("paimon_connector_descriptor.bin"));
    config
        .compile_protos(&proto_paths, &[PathBuf::from(IDL_DIR)])
        .expect("compile Paimon connector private protobuf DTOs");
}
