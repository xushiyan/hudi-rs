use cxx_build::CFG;
fn main() {
    CFG.include_prefix = "hudi";

    cxx_build::bridge("src/bridge.rs")
        .include("include")
        .include("include/arrow/c")
        .flag_if_supported("-std=c++14")
        .compile("hudi");

    println!("cargo:rerun-if-changed=src/bridge.rs");
    println!("cargo:rerun-if-changed=include/arrow_bridge.h");
    println!("cargo:rerun-if-changed=include/arrow/c/api.h");

    println!("cargo:root={}", std::env::var("CARGO_MANIFEST_DIR").unwrap());
}