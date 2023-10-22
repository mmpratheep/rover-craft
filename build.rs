fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec![
        "proto/health_check.proto",
        "proto/probe.proto",
    ];

    tonic_build::configure()
        .compile(&proto_files, &["proto/"])?;

    Ok(())
}
