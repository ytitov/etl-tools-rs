use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_files::NamedFile;
use actix_web::{middleware::Logger, web, App, Error, HttpRequest, HttpServer, Responder};
use actix_web_actors::ws;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    unimplemented!("todo");
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // start chat server actor
    let server = server::ChatServer::default().start();

    // start TCP server in separate thread
    let srv = server.clone();
    session::tcp_server("127.0.0.1:12345", srv);

    log::info!("starting HTTP+WebSocket server at http://localhost:8080");

    HttpServer::new(move || {
        App::new()
            //.app_data(web::Data::new(server.clone()))
            // WebSocket UI HTML file
            //.service(web::resource("/").to(index))
            // websocket
            //.service(web::resource("/ws").to(chat_route))
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", 8181))?
    .workers(2)
    .run()
    .await
}
