use etl_core::datastore::mock::MockJsonDataOutput;
use etl_core::datastore::DataOutput;
use etl_core::deps::bytes::Bytes;
use etl_core::task::apply::Apply;

use etl_grpc::datasource_server::DataSourceServerBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    etl_grpc::log_util::new_info();

    let builder = DataSourceServerBuilder {
        ip_addr: String::from("[::1]:50051"),
        buffer_size: 10,
    };
    let (data_source, server_jh) = builder.start()?;
    // this data store should never finish
    let _stats = Apply {
        state: (),
        input: data_source,
        apply_fn: Box::new(|_, item| {
            Box::pin(async move {
                println!("apply got: {:?}", &item);
                Ok(())
            })
        }),
    }
    .run()
    .await?;
    server_jh.await?.expect("Server failed");
    Ok(())
}
