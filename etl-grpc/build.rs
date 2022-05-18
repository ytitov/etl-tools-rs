use std::{env, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // DataOutput servers and clients
    // These are data receiving servers and clients that interact with them
    //tonic_build::compile_protos("proto/dataoutput.proto")?;
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Shared data types by datasources and dataoutputs
    tonic_build::compile_protos("proto/datastore.proto")?;

    /*
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("dataoutput_descriptor.bin"))
        .compile(&["proto/dataoutput.proto"], &["proto"])
        .unwrap();
    */
    //tonic_build::compile_protos("proto/dataoutput_string.proto")?;


    // DataSource code
    //tonic_build::compile_protos("proto/datasource.proto")?;


    // for the transform
    tonic_build::compile_protos("proto/transform.proto")?;

    //build_json_codec_service();
    Ok(())
}
