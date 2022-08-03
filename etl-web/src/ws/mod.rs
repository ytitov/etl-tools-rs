use std::time::{Duration, Instant};

use actix::prelude::*;
use actix_files::NamedFile;
use actix_web::{middleware::Logger, web, App, Error, HttpRequest, HttpServer, Responder};
use actix_web_actors::ws;

pub mod event;
pub mod server;

pub struct WsClientSession {
    id: usize,
    hb: Instant,
    addr: Addr<server::EventServer>,
    framed: actix::io::FramedWrite<ChatResponse, WriteHalf<TcpStream>, ChatCodec>,
}

impl actix::io::WriteHandler<io::Error> for ChatSession {}

impl StreamHandler<Result<ClientRequest, io::Error>> for WsClientSession {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: Result<ClientRequest, io::Error>, ctx: &mut Context<Self>) {
    }
}
