fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use bundled protoc
    std::env::set_var("PROTOC", protobuf_src::protoc());

    let proto_files = &["../../proto/control.proto", "../../proto/data.proto"];

    let dirs = &["../../proto"];

    // Ensure the output directory exists
    let out_dir = std::path::Path::new("src/generated");
    std::fs::create_dir_all(out_dir)?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(out_dir)
        .compile(proto_files, dirs)?;

    // Re-run if proto files change
    for proto in proto_files {
        println!("cargo:rerun-if-changed={}", proto);
    }

    Ok(())
}
