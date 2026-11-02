fn main() {
    let lib = std::env::var("CEPH_LIB").unwrap_or_else(|_| "/work/ceph/build/lib".to_string());
    println!("cargo:rustc-link-search=native={lib}");
    println!("cargo:rustc-link-lib=dylib=rados");
    println!("cargo:rerun-if-env-changed=CEPH_LIB");
}
