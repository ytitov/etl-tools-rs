use etl_grpc::simplestore::observer_server::ObservableStoreServerBuilder;
use etl_core::datastore::mock::MockJsonDataSource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    SimpleLogger::new()
        .with_level(LevelFilter::Error)
        //.with_level(LevelFilter::Info)
        // specific logging only for etl_core to be info
        .with_module_level("grpc_simple_store_server", LevelFilter::Info)
        .with_module_level("etl_grpc::simplestore::observer_server", LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    let simplestore = Box::new(MockJsonDataSource::default());
    
    let ip_addr = "[::]:50051".to_string();

    log::info!("starting {}", &ip_addr);

    ObservableStoreServerBuilder { ip_addr, buffer_size: 100, simplestore }.start()?.await??;



    Ok(())
}
