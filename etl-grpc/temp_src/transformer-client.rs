use etl_grpc::transformer::transformer_client::TransformerClient;
use etl_grpc::transformer::TransformPayload;
use etl_grpc::transformer::*;
use etl_core::datastore::DataSource;
//use etl_core::transformer::Transformer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TransformerClient::connect("http://[::1]:50051").await?;

    /*
    let request = tonic::Request::new(TransformPayload {
        string_content: Some("Tonic".into()),
        ..Default::default()
    });
    */

    //let g = GrpcTransformerClient { grpc_client: client };

    //let t_ds = Box<dyn DataSource<(I, TransformerResultTx<O>)>>::from_datasource(Box::new(String::from("lots a lines") as Box<dyn DataSource<String>>));
    //Box::new(g).create();

    /*
    let response = client.transform(request).await?;

    println!("RESPONSE={:?}", response);
    */

    Ok(())
}
