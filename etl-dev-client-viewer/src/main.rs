use actix_web::{get, web, web::Data, App, HttpServer, Responder};
use etl_core::datastore::fs::LocalFs;
use etl_core::datastore::simple::*;
use std::sync::Arc;

#[get("/file/{key}")]
async fn greet(
    loader: web::Data<Arc<PairedDataStore<String, String>>>,
    name: web::Path<String>,
) -> impl Responder {
    /*
    if let Ok(files) = loader.load(&name).await {
        format!("Found! {name}!")
    } else {
        format!("Not found {name}!")
    }
    */
    match loader.load(&name).await {
        Ok(r) => {
            format!("Found! {name}!")
        }
        Err(e) => {
            format!("Error! {name} {e}")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    use simple_logger::SimpleLogger;
    use log::LevelFilter;
    SimpleLogger::new().with_level(LevelFilter::Debug).env().init().unwrap();
    //dotenv().ok();

    //let config = crate::config::Config::from_env().unwrap();
    //let pool = config.pg.create_pool(None, NoTls).unwrap();
    let paired = PairedDataStore::<String, String> {
        left_ds: Box::new(LocalFs {
            home: "./left".to_string(),
            ..Default::default()
        }),
        right_ds: Box::new(LocalFs {
            home: "./right".to_string(),
            ..Default::default()
        }),
    };

    let loader = Arc::new(paired);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(loader.clone()))
            .service(greet)
    })
    .bind("localhost:8111")?
    .run();

    server.await
}
