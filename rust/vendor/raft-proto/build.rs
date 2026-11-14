// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

fn main() {
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("protos");
    std::fs::create_dir_all(&output).unwrap();
    protobuf_codegen_pure::Codegen::new().out_dir(output).include("proto").input("proto/eraftpb.proto")
        .customize(protobuf_codegen_pure::Customize { gen_mod_rs: Some(true), ..Default::default() })
        .run().expect("generate Raft wire protocol");
    println!("cargo:rerun-if-changed=proto/eraftpb.proto");
}
