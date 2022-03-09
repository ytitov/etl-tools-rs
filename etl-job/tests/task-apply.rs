use etl_core::decoder::string::StringDecoder;
use etl_core::task::apply::Apply;
use etl_core::deps::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_output_task() {
    let source_string = String::from(r"
    hi 
    this
    is cool");
    let output = Apply {
        input: StringDecoder::default().with_datasource(source_string),
        state: (),
        apply_fn: Box::new(|_, _s| Box::pin(async move {
            Ok(())
        })),
    }
    .run()
    .await.expect("Did not succeed correctly");
    println!("{:?}", output);
    assert_eq!(4, output.lines_written);
}
