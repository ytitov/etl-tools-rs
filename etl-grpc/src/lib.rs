pub mod proto {
    //currently server only implemented in python
    //pub(crate) const TRANSFORM_REFLECTION_DESCR: &[u8] =
    //    tonic::include_file_descriptor_set!("transform-reflection");
    pub(crate) const SIMPLESTORE_REFLECTION_DESCR: &[u8] =
        tonic::include_file_descriptor_set!("simplestore-reflection");
    pub mod etl_grpc {
        pub mod basetypes {
            pub mod ds_error {
                tonic::include_proto!("etl_grpc.basetypes.ds_error");
            }
            pub mod simplestore_error {
                tonic::include_proto!("etl_grpc.basetypes.simplestore_error");
            }
        }
        pub mod transformers {
            pub mod transform {
                tonic::include_proto!("etl_grpc.transformers.transform");
            }
        }
        pub mod simplestore {
            pub mod bytes_store {
                tonic::include_proto!("etl_grpc.simplestore.bytes_store");
            }
        }
    }
}

pub mod datastore;
//pub mod dataoutput;
pub mod simplestore;
pub mod transformer;

pub mod log_util {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;

    pub fn new_info() {
        SimpleLogger::new()
            .with_level(LevelFilter::Info)
            .env()
            .init()
            .unwrap();
    }
    pub fn new_debug() {
        SimpleLogger::new()
            .with_level(LevelFilter::Debug)
            .env()
            .init()
            .unwrap();
    }
}
