fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/dataoutput.proto")?;
    tonic_build::compile_protos("proto/datasource.proto")?;
    Ok(())
}
