use std::env;
use std::path::PathBuf;

fn main() {
    let java_home = env::var("JAVA_HOME").expect("JAVA_HOME must be set");
    let java_include = PathBuf::from(&java_home).join("include");
    
    println!("cargo:rustc-link-search=native={}/lib", java_home);
    println!("cargo:rustc-link-lib=dylib=jvm");
    println!("cargo:rustc-link-search=native={}", java_include.display());
    
    // Platform-specific include paths
    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-search=native={}/linux", java_include.display());
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-search=native={}/darwin", java_include.display());
    } else if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-search=native={}/win32", java_include.display());
    }
    
    println!("cargo:rerun-if-env-changed=JAVA_HOME");
}