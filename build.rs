fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let proto_files = vec![
    //     "proto/health_check.proto",
    //     "proto/probe_sync.proto",
    // ];
    //
    // tonic_build::configure()
    //     .compile(&proto_files, &["proto/"])?;
    tonic_build::compile_protos("./protos/probe_sync.proto")?;

    Ok(())
}
