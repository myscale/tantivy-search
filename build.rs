use std::collections::HashSet;
use std::env;

fn main() {
    let mut build = cxx_build::bridge("src/lib.rs");
    let target = env::var("TARGET").unwrap();
    if target.eq("aarch64-apple-darwin") {
        build.flag_if_supported("-std=c++17");
    }
    let flags = "-Wno-dollar-in-identifier-extension -Wno-unused-macros ";
    let unique_flags: Vec<&str> = flags
        .split_whitespace()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    for flag in unique_flags {
        if flag.len() == 0 {
            continue;
        }
        build.flag_if_supported(flag);
    }
    build.compile("tantivy_search");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=.cargo/config.toml");
}
