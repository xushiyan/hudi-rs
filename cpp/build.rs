use cxx_build::CFG;
use std::env;

fn main() {
    CFG.include_prefix = "hudi";

    let mut build = cxx_build::bridge("src/lib.rs");

    build
        .flag_if_supported("-std=c++20")
        .include("include");
    if let Ok(include_dir) = env::var("DEP_CXX_ASYNC_INCLUDE") {
        build.include(include_dir);
    } else {
        panic!("Where's the `cxx_async` `DEP`?")
    }

    build
        .compile("hudi");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=include/hudi.h");
}