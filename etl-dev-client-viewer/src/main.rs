// https://ace.c9.io/#nav=embedding for code editing
use actix_web::{get, web, /*web::Data,*/ App, HttpServer, HttpResponse, Responder};
use actix_files::NamedFile;
use etl_core::datastore::fs::LocalFs;
use etl_core::keystore::simple::*;
use std::sync::Arc;

#[get("/file/{key}")]
async fn greet(
    loader: web::Data<Arc<PairedDataStore<String, String>>>,
    name: web::Path<String>,
) -> impl Responder {
    let html_str = include_str!("../html/viewer-page-template.html");
    /*
    if let Ok(files) = loader.load(&name).await {
        format!("Found! {name}!")
    } else {
        format!("Not found {name}!")
    }
    */
    HttpResponse::Ok().content_type("text/html").body(match loader.load(&name).await {
        Ok(DataPair { left: Some(left), right: Some(right) }) => {
            html_str.replace("{{left-source}}", &left)
                .replace("{{right-source}}", &right)
        }
        _ => {
            format!("Error! {name} not all covered")
        }
        Err(e) => {
            format!("Error! {name} {e}")
        }
    })
}

#[get("/assets/{p:.*}")]
async fn serve_assets(
    loader: web::Data<Arc<PairedDataStore<String, String>>>,
    p: web::Path<String>,
) -> impl Responder {
    println!("Requested {p}");
    let p_str = p.into_inner();
    let p = format!("./assets/{p_str}");
    println!("Retrieving {p}");
    NamedFile::open_async(p).await
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
            .service(serve_assets)
            .service(greet)
    })
    .bind("localhost:8111")?
    .run();

    println!("navigate to localhost:8111/file/[filename]");

    server.await
}
