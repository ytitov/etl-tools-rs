//#![cfg(feature = "registry")]

use etl_core::decoder::json::JsonDecoder;
use etl_core::deps::*;
use etl_core::task::apply::Apply;

use tracing_core::field;
use tracing_core::metadata::Metadata;
use tracing_core::span::*;
use tracing_core::Event;
use tracing_core::Subscriber;
use tracing_futures::{Instrument, WithSubscriber};
use tracing_subscriber::prelude::*;

struct MySubscriber {}

impl Subscriber for MySubscriber {
    fn enabled(&self, m: &Metadata) -> bool {
        let b = m.name() == "foo";
        println!("# enabled {}: metadata {:?}", &b, m);
        true
    }

    fn new_span(&self, a: &Attributes) -> Id {
        println!("# new_span attributes: {:?}", &a);
        Id::from_u64(123)
    }

    fn record(&self, _: &Id, r: &Record) {
        println!("# record record: {:?}", r);
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {
        todo!()
    }

    fn event(&self, _: &Event<'_>) {
        todo!()
    }

    fn enter(&self, id: &Id) {
        println!("# enter id: {:?}", id);
    }

    fn exit(&self, id: &Id) {
        println!("# exit id: {:?}", id);
    }
}

#[tokio::test]
async fn future_with_subscriber() {
    use simple_logger::SimpleLogger;
    use tracing::log::LevelFilter;

    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .with_level(LevelFilter::Error)
        .with_level(LevelFilter::Warn)
        .with_level(LevelFilter::Debug)
        .init()
        .expect("Could not initialize SimpleLogger");

    use serde_json::Value as JsonValue;

    let source_string = String::from(
        r#"
    {"key": 123} 
    {"key": 124} 
    {"key": 125} 
    this should error

    "#,
    );
    let output = Apply {
        input: JsonDecoder::default().with_datasource(source_string),
        state: (),
        apply_fn: Box::new(|_, _s: JsonValue| {
            Box::pin(async move {
                //apply_fn: Box::new(|_, _s: String| Box::pin(async move {
                Ok(())
            })
        }),
    }
    .run()
    .await
    .expect("Did not succeed correctly");
    println!("{:?}", output);
    //assert_eq!(4, output.lines_written);

    let _default = tracing_subscriber::registry().init();
    //log::trace!("example trace log");
    tracing::event!(target: "app_events", tracing::Level::WARN, "increment_count {}", "123");
    let span = tracing::info_span!("foo", mydata = field::Empty);
    let _e = span.enter();
    span.record("mydata", &"this is the data");

    let span = tracing::info_span!("bar");
    let _e = span.enter();
    span.record("stuff", &"have no clue");
    log::info!("this never shows up");
    log::error!("this never shows up");
    tokio::spawn(
        async {
            async {
                let span = tracing::Span::current();
                println!("--> {:?}", span);
            }
            .instrument(tracing::info_span!("instrument"))
            .await
        }
        //.with_subscriber(tracing_subscriber::registry()),
        .with_subscriber(MySubscriber{}),
    )
    .await
    .unwrap();
    let span = tracing::info_span!("horsey");
    let _e = span.enter();
}
