fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_path = "spark/connector/connect/common/src/main/protobuf";
    let proto_files = vec![
        "base.proto",
        "commands.proto",
        "expressions.proto",
        "relations.proto",
        "types.proto",
    ];
    let mut paths = Vec::new();
    for proto_file in proto_files {
        paths.push(format!("{}/spark/connect/{}", base_path, proto_file));
    }
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(paths.as_ref(), &[base_path])?;
    Ok(())
}
